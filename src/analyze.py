from typing import List, Tuple, Optional
import pandas as pd
from scipy.stats import chi2_contingency
from sklearn.metrics import mutual_info_score

NUMBER_RANGE = range(0, 100)

def z2(n: int) -> str:
    return str(n).zfill(2)

def explode(df: pd.DataFrame, lottery: str) -> pd.DataFrame:
    x = df.copy()
    x["lottery"] = lottery
    x["fecha_dt"] = pd.to_datetime(x["fecha"], errors="coerce")
    x = x.dropna(subset=["fecha_dt"])
    x["nums"] = x[["primero","segundo","tercero"]].values.tolist()
    x = x.explode("nums").rename(columns={"nums":"num"})
    x["num"] = x["num"].astype(str).str.strip().str.zfill(2)
    return x[["fecha_dt","fecha","lottery","sorteo","num"]]

def build_pairs(exp: pd.DataFrame, src_filter, tgt_filter, lag_days: int) -> Optional[pd.DataFrame]:
    src = exp[src_filter(exp)].copy()
    tgt = exp[tgt_filter(exp)].copy()
    if src.empty or tgt.empty:
        return None

    src_map = src.groupby("fecha_dt")["num"].apply(set).to_dict()
    tgt_map = tgt.groupby("fecha_dt")["num"].apply(set).to_dict()

    rows = []
    for d, src_nums in src_map.items():
        d2 = d + pd.Timedelta(days=lag_days)
        if d2 not in tgt_map:
            continue
        tgt_nums = tgt_map[d2]
        for n in NUMBER_RANGE:
            nn = z2(n)
            rows.append((nn, int(nn in src_nums), int(nn in tgt_nums)))

    if not rows:
        return None
    return pd.DataFrame(rows, columns=["num","src_event","tgt_event"])

def stats_per_num(pairs: pd.DataFrame) -> pd.DataFrame:
    out = []
    for num, sub in pairs.groupby("num"):
        a = int(((sub.src_event==1) & (sub.tgt_event==1)).sum())
        b = int(((sub.src_event==1) & (sub.tgt_event==0)).sum())
        c = int(((sub.src_event==0) & (sub.tgt_event==1)).sum())
        d = int(((sub.src_event==0) & (sub.tgt_event==0)).sum())

        try:
            chi2, p, _, _ = chi2_contingency([[a,b],[c,d]], correction=False)
        except Exception:
            chi2, p = 0.0, 1.0

        mi = mutual_info_score(sub["src_event"], sub["tgt_event"])
        out.append({"num": num, "chi2": float(chi2), "p_value": float(p), "mi": float(mi), "a11": a})

    df = pd.DataFrame(out)
    df["signal"] = df["mi"] * (1.0 - df["p_value"].clip(0,1))
    return df

def recommend_for_target(exp: pd.DataFrame,
                         src_filter,
                         tgt_lottery: str,
                         tgt_draw: str,
                         lag_days: int,
                         top_n: int = 12) -> pd.DataFrame:
    tgt_filter = lambda e: (e["lottery"]==tgt_lottery) & (e["sorteo"]==tgt_draw)

    pairs = build_pairs(exp, src_filter, tgt_filter, lag_days=lag_days)
    if pairs is None:
        return pd.DataFrame(columns=["num","signal","mi","p_value","a11","score"])

    st = stats_per_num(pairs)

    tgt = exp[tgt_filter(exp)]
    base = tgt.groupby("num").size().reset_index(name="count")
    base["p_base"] = base["count"] / max(len(tgt), 1)

    out = st.merge(base[["num","p_base"]], on="num", how="left").fillna({"p_base":0})
    out["score"] = 0.70*out["signal"] + 0.30*out["p_base"]
    return out.sort_values("score", ascending=False).head(top_n)

def top_pales(nums: List[str], k: int) -> List[Tuple[str,str]]:
    pales = []
    for i in range(len(nums)):
        for j in range(i+1, len(nums)):
            pales.append((nums[i], nums[j]))
    return pales[:k]

def should_alert(recs: pd.DataFrame, min_signal: float, min_count_hits: int) -> bool:
    if recs.empty:
        return False
    strong = recs[(recs["signal"] >= min_signal) | (recs["a11"] >= min_count_hits)]
    return len(strong) >= 3
