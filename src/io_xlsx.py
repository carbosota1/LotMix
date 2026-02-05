import os
import pandas as pd

COLS = ["fecha", "sorteo", "primero", "segundo", "tercero"]

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def normalize_2d(x: str) -> str:
    s = str(x).strip()
    digits = "".join([c for c in s if c.isdigit()])
    if digits == "":
        return ""
    return digits.zfill(2)

def read_history_xlsx(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame(columns=COLS)

    df = pd.read_excel(path, sheet_name="history", dtype=str, engine="openpyxl")
    for c in COLS:
        if c not in df.columns:
            df[c] = ""
    df = df[COLS].fillna("")

    # preserva 00/07 como texto
    for c in ["primero", "segundo", "tercero"]:
        df[c] = df[c].astype(str).map(normalize_2d)

    df["fecha"] = df["fecha"].astype(str).str.strip()
    df["sorteo"] = df["sorteo"].astype(str).str.strip()
    return df

def upsert_history_xlsx(path: str, new_rows: pd.DataFrame):
    """
    new_rows columnas: fecha, sorteo, primero, segundo, tercero (strings con zfill(2))
    Unicidad por (fecha, sorteo)
    """
    old = read_history_xlsx(path)
    df = pd.concat([old, new_rows], ignore_index=True).fillna("")
    df = df.drop_duplicates(subset=["fecha","sorteo"], keep="last")
    df = df.sort_values(["fecha","sorteo"]).reset_index(drop=True)

    with pd.ExcelWriter(path, engine="openpyxl", mode="w") as w:
        df.to_excel(w, sheet_name="history", index=False)
