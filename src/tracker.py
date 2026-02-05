import os
import json
from datetime import datetime
import pandas as pd


def _ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def _mk_key(date_str: str, lottery: str, draw: str, time_rd: str) -> str:
    return f"{date_str}|{lottery}|{draw}|{time_rd}"


def log_candidates(outputs_dir: str, payload: dict):
    """
    Guarda TODOS los candidates del picks.json en data/picks_log.csv
    para poder evaluarlos cuando salga el resultado.
    """
    data_dir = "data"
    _ensure_dir(data_dir)

    log_path = os.path.join(data_dir, "picks_log.csv")
    generated_at = payload.get("generated_at")
    date_str = (generated_at or "")[:10] if generated_at else datetime.now().strftime("%Y-%m-%d")

    rows = []
    for c in payload.get("candidates_ranked", []):
        time_rd = c.get("time_rd", "")
        lottery = c.get("lottery", "")
        draw = c.get("draw", "")

        key = _mk_key(date_str, lottery, draw, time_rd)

        rows.append({
            "key": key,
            "date": date_str,
            "time_rd": time_rd,
            "lottery": lottery,
            "draw": draw,
            "generated_at": generated_at,
            "best_score": c.get("best_score"),
            "best_signal": c.get("best_signal"),
            "best_a11": c.get("best_a11"),
            "ok_alert": c.get("ok_alert"),
            "top12": json.dumps(c.get("top_nums", []), ensure_ascii=False),
            "pales10": json.dumps(c.get("pales", []), ensure_ascii=False),
            "graded": 0,
        })

    if not rows:
        return

    new_df = pd.DataFrame(rows)

    if os.path.exists(log_path):
        old = pd.read_csv(log_path, dtype=str)
        # Merge / upsert por key (mantener 1 registro por target)
        merged = old.merge(new_df, on="key", how="outer", suffixes=("_old", ""))
        # Preferir valores nuevos si existen
        out = pd.DataFrame()
        out["key"] = merged["key"]

        for col in ["date", "time_rd", "lottery", "draw", "generated_at",
                    "best_score", "best_signal", "best_a11", "ok_alert",
                    "top12", "pales10"]:
            out[col] = merged[col].fillna(merged.get(f"{col}_old"))

        out["graded"] = merged["graded"].fillna(merged.get("graded_old")).fillna("0")
        out.to_csv(log_path, index=False, encoding="utf-8")
    else:
        new_df.to_csv(log_path, index=False, encoding="utf-8")


def grade_picks_from_histories(outputs_dir: str, xlsx_files: dict):
    """
    Revisa data/picks_log.csv, busca el resultado real en los historiales,
    y calcula hits para QUINIELA y PALE. Guarda en outputs/performance.csv
    """
    log_path = os.path.join("data", "picks_log.csv")
    if not os.path.exists(log_path):
        return

    df = pd.read_csv(log_path, dtype=str)
    if df.empty:
        return

    # Solo los que no estén graded
    pending = df[df["graded"].fillna("0") != "1"].copy()
    if pending.empty:
        return

    perf_rows = []
    any_graded = False

    # Cache de historiales ya cargados
    hist_cache = {}

    def load_hist(lottery: str):
        if lottery in hist_cache:
            return hist_cache[lottery]
        path = xlsx_files.get(lottery)
        if not path or not os.path.exists(path):
            hist_cache[lottery] = pd.DataFrame()
            return hist_cache[lottery]

        # lee primera hoja por defecto (tu io_xlsx ya lo hace, pero aquí vamos directo)
        hx = pd.read_excel(path)
        # normaliza columnas esperadas
        hx.columns = [str(c).strip().lower() for c in hx.columns]
        for col in ["fecha", "sorteo", "primero", "segundo", "tercero"]:
            if col not in hx.columns:
                hist_cache[lottery] = pd.DataFrame()
                return hist_cache[lottery]
        hx["fecha"] = hx["fecha"].astype(str).str.slice(0, 10)
        hx["sorteo"] = hx["sorteo"].astype(str)
        # números como texto (00..99)
        for col in ["primero", "segundo", "tercero"]:
            hx[col] = hx[col].astype(str).str.extract(r"(\d{1,2})")[0].fillna("").str.zfill(2)
        hist_cache[lottery] = hx
        return hx

    def hits_topk(nums_list, drawn_set, k):
        s = set(nums_list[:k])
        return len(s.intersection(drawn_set))

    def pale_hits(pales, drawn_nums):
        # Palé se considera hit si está en cualquier par de los 3 números (sin orden)
        drawn = sorted(list(drawn_nums))
        pairs = {f"{drawn[0]}-{drawn[1]}", f"{drawn[0]}-{drawn[2]}", f"{drawn[1]}-{drawn[2]}"}
        # normaliza pales a "AA-BB"
        norm = []
        for p in pales:
            try:
                a, b = str(p).split("-")
                a = a.strip().zfill(2)
                b = b.strip().zfill(2)
                aa, bb = sorted([a, b])
                norm.append(f"{aa}-{bb}")
            except Exception:
                continue
        return len(set(norm).intersection(pairs))

    for _, r in pending.iterrows():
        date = r.get("date", "")
        lottery = r.get("lottery", "")
        draw = r.get("draw", "")
        time_rd = r.get("time_rd", "")
        key = r.get("key", "")

        hx = load_hist(lottery)
        if hx.empty:
            continue

        match = hx[(hx["fecha"] == date) & (hx["sorteo"] == draw)]
        if match.empty:
            # resultado todavía no existe en el historial (aún no publicado o no actualizado)
            continue

        row = match.iloc[-1]
        drawn = {row["primero"], row["segundo"], row["tercero"]}

        top12 = json.loads(r.get("top12", "[]") or "[]")
        pales10 = json.loads(r.get("pales10", "[]") or "[]")

        # Hits de quiniela (cuántos de tus nums aparecen en los 3 reales)
        h6 = hits_topk(top12, drawn, 6)
        h8 = hits_topk(top12, drawn, 8)
        h12 = hits_topk(top12, drawn, 12)

        # Hits de palé (cuántos palés de los 10 pegaron algún par real)
        ph = pale_hits(pales10, drawn)

        perf_rows.append({
            "key": key,
            "date": date,
            "time_rd": time_rd,
            "lottery": lottery,
            "draw": draw,
            "result": f"{row['primero']}-{row['segundo']}-{row['tercero']}",
            "hits_top6": h6,
            "hits_top8": h8,
            "hits_top12": h12,
            "pale_hits_top10": ph,
            "best_signal": r.get("best_signal"),
            "best_a11": r.get("best_a11"),
            "ok_alert": r.get("ok_alert"),
        })

        # marcar como graded
        df.loc[df["key"] == key, "graded"] = "1"
        any_graded = True

    if perf_rows:
        _ensure_dir(outputs_dir)
        perf_path = os.path.join(outputs_dir, "performance.csv")
        perf_df = pd.DataFrame(perf_rows)

        if os.path.exists(perf_path):
            oldp = pd.read_csv(perf_path, dtype=str)
            outp = pd.concat([oldp, perf_df], ignore_index=True)
            outp = outp.drop_duplicates(subset=["key"], keep="last")
            outp.to_csv(perf_path, index=False, encoding="utf-8")
        else:
            perf_df.to_csv(perf_path, index=False, encoding="utf-8")

    if any_graded:
        df.to_csv(log_path, index=False, encoding="utf-8")