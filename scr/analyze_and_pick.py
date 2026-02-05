import os, json
import pandas as pd
from scipy.stats import chi2_contingency
from sklearn.metrics import mutual_info_score

from utils_io import ensure_dirs, read_history_xlsx

DATA_DIR = "data/histories"
OUT_DIR  = "outputs"

# Config de fuentes (xlsx)
SOURCES = [
    {"file":"La primera history.xlsx", "loteria":"La Primera"},
    {"file":"La nacional history.xlsx", "loteria":"La Nacional"},
    {"file":"Anguilla history.xlsx", "loteria":"Anguilla"},
]

TOP_N = 12
TOP_PALES = 20

def load_all() -> pd.DataFrame:
    frames = []
    for s in SOURCES:
        path = os.path.join(DATA_DIR, s["file"])
        df = read_history_xlsx(path)
        if df.empty:
            continue
        # asegura loteria consistente
        df["loteria"] = s["loteria"]
        frames.append(df)
    if not frames:
        return pd.DataFrame(columns=["fecha","loteria","sorteo","n1","n2","n3"])
    df = pd.concat(frames, ignore_index=True)
    df["fecha_dt"] = pd.to_datetime(df["fecha"], errors="coerce")
    df = df.dropna(subset=["fecha_dt"]).sort_values(["fecha_dt","loteria","sorteo"]).reset_index(drop=True)
    return df

def explode_numbers(df: pd.DataFrame) -> pd.DataFrame:
    x = df.copy()
    x["nums"] = x[["n1","n2","n3"]].values.tolist()
    x = x.explode("nums").rename(columns={"nums":"num"})
    x["num"] = x["num"].astype(str).str.zfill(2)
    return x[["fecha_dt","fecha","loteria","sorteo","num"]]

def build_event_table(exp: pd.DataFrame, target_lottery: str, target_draw: str, lag_days: int):
    """
    Construye pares de observaciones para medir dependencia:
    Source event: "aparece num en cualquier otra lotería en fecha t"
    Target event: "aparece num en target (loteria/draw) en fecha t+lag"
    """
    tgt = exp[(exp["loteria"] == target_lottery) & (exp["sorteo"] == target_draw)].copy()
    if tgt.empty:
        return None

    # mapa target: fecha -> set(nums)
    tgt_map = tgt.groupby("fecha_dt")["num"].apply(set).to_dict()

    # source: todo lo demás
    src = exp[~((exp["loteria"] == target_lottery) & (exp["sorteo"] == target_draw))].copy()
    src_map = src.groupby("fecha_dt")["num"].apply(list).to_dict()

    rows = []
    for d, src_nums in src_map.items():
        d2 = d + pd.Timedelta(days=lag_days)
        if d2 not in tgt_map:
            continue
        tgt_nums = tgt_map[d2]

        # para cada número 00-99, evento binario: apareció en src ese día / apareció en target día d2
        src_set = set(src_nums)
        for n in range(0, 100):
            nn = str(n).zfill(2)
            rows.append((nn, int(nn in src_set), int(nn in tgt_nums)))

    if not rows:
        return None

    t = pd.DataFrame(rows, columns=["num","src_event","tgt_event"])
    return t

def chi_square_and_mi(table: pd.DataFrame):
    # tabla 2x2 para cada num (aparece/no aparece en source vs aparece/no aparece en target)
    out = []
    for num, sub in table.groupby("num"):
        a = int(((sub.src_event==1) & (sub.tgt_event==1)).sum())
        b = int(((sub.src_event==1) & (sub.tgt_event==0)).sum())
        c = int(((sub.src_event==0) & (sub.tgt_event==1)).sum())
        d = int(((sub.src_event==0) & (sub.tgt_event==0)).sum())
        contingency = [[a,b],[c,d]]

        # chi2 (si hay ceros extremos, puede fallar; lo manejamos)
        try:
            chi2, p, _, _ = chi2_contingency(contingency, correction=False)
        except Exception:
            chi2, p = 0.0, 1.0

        # MI (binaria)
        mi = mutual_info_score(sub["src_event"], sub["tgt_event"])

        out.append({"num":num, "chi2":chi2, "p_value":p, "mi":mi, "a11":a})
    return pd.DataFrame(out)

def base_frequency_scores(exp: pd.DataFrame):
    g = exp.groupby(["loteria","sorteo","num"]).size().reset_index(name="count")
    tot = exp.groupby(["loteria","sorteo"]).size().reset_index(name="total")
    m = g.merge(tot, on=["loteria","sorteo"], how="left")
    m["p_base"] = m["count"]/m["total"]
    return m

def recent_scores(exp: pd.DataFrame, last_days=30):
    cutoff = exp["fecha_dt"].max() - pd.Timedelta(days=last_days)
    sub = exp[exp["fecha_dt"] >= cutoff]
    g = sub.groupby(["loteria","sorteo","num"]).size().reset_index(name="r_count")
    tot = sub.groupby(["loteria","sorteo"]).size().reset_index(name="r_total")
    m = g.merge(tot, on=["loteria","sorteo"], how="left")
    m["p_recent"] = m["r_count"]/m["r_total"]
    return m[["loteria","sorteo","num","p_recent"]]

def make_pales(top_nums: list[str], limit: int):
    pales = []
    for i in range(len(top_nums)):
        for j in range(i+1, len(top_nums)):
            pales.append((top_nums[i], top_nums[j]))
    return pales[:limit]

def main():
    ensure_dirs(OUT_DIR)
    df = load_all()
    exp = explode_numbers(df)

    if exp.empty:
        raise SystemExit("No hay data en XLSX. Revisa data/histories.")

    # scores base + recientes
    base = base_frequency_scores(exp)
    recent = recent_scores(exp, last_days=30)

    # Cross-signals: elegimos targets que te importen (ejemplos)
    targets = [
        ("La Nacional", "Loteria Nacional- Gana Más"),
        ("La Nacional", "Loteria Nacional- Noche"),
        ("La Primera", "Principal"),
    ]

    cross_results = []
    for lot, dr in targets:
        for lag in [0, 1]:  # mismo día vs día siguiente
            t = build_event_table(exp, lot, dr, lag_days=lag)
            if t is None:
                continue
            stats = chi_square_and_mi(t)
            stats["loteria"] = lot
            stats["sorteo"] = dr
            stats["lag"] = lag
            cross_results.append(stats)

    cross = pd.concat(cross_results, ignore_index=True) if cross_results else pd.DataFrame(
        columns=["num","chi2","p_value","mi","a11","loteria","sorteo","lag"]
    )

    # “cross_score” simple: MI * (1 - p_value) y separa lag 0/1
    cross["cross_score"] = cross["mi"] * (1.0 - cross["p_value"].clip(0,1))

    # Construye recomendaciones por lotería/sorteo
    # Score final (ajustable): base + reciente + cross same-day + cross next-day
    recs = base.merge(recent, on=["loteria","sorteo","num"], how="left").fillna({"p_recent":0})
    # agrega cross lag0 y lag1
    c0 = cross[cross["lag"]==0][["loteria","sorteo","num","cross_score"]].rename(columns={"cross_score":"cross_same"})
    c1 = cross[cross["lag"]==1][["loteria","sorteo","num","cross_score"]].rename(columns={"cross_score":"cross_next"})
    recs = recs.merge(c0, on=["loteria","sorteo","num"], how="left").merge(c1, on=["loteria","sorteo","num"], how="left")
    recs = recs.fillna({"cross_same":0, "cross_next":0})

    recs["score"] = (
        0.45 * recs["p_base"] +
        0.35 * recs["p_recent"] +
        0.10 * recs["cross_same"] +
        0.10 * recs["cross_next"]
    )

    # Top números por target
    picks = []
    for (lot, dr), sub in recs.groupby(["loteria","sorteo"]):
        top = sub.sort_values("score", ascending=False).head(TOP_N)
        top_nums = top["num"].tolist()
        pales = make_pales(top_nums, TOP_PALES)

        picks.append({
            "loteria": lot,
            "sorteo": dr,
            "top_nums": top_nums,
            "pales": pales,
            "notes": {
                "cross_same_top": top.sort_values("cross_same", ascending=False).head(5)[["num","cross_same"]].to_dict("records"),
                "cross_next_top": top.sort_values("cross_next", ascending=False).head(5)[["num","cross_next"]].to_dict("records"),
            }
        })

    out_json = os.path.join(OUT_DIR, "picks.json")
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump({"generated_at": datetime.now().isoformat(), "picks": picks}, f, ensure_ascii=False, indent=2)

    # reporte txt rápido
    out_txt = os.path.join(OUT_DIR, "report.txt")
    with open(out_txt, "w", encoding="utf-8") as f:
        f.write("REPORTE CROSS-LOTTERY (Chi2 + MI)\n")
        f.write("="*60 + "\n\n")
        for p in picks:
            f.write(f"[{p['loteria']} | {p['sorteo']}]\n")
            f.write("Top nums: " + ", ".join(p["top_nums"]) + "\n")
            f.write("Top palés: " + " | ".join([f"{a}-{b}" for a,b in p["pales"][:10]]) + "\n")
            f.write("\n")

    print("[OK] Generado outputs/picks.json y outputs/report.txt")

if __name__ == "__main__":
    from datetime import datetime
    main()
