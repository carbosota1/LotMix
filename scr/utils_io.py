import os
import pandas as pd

def ensure_dirs(*paths):
    for p in paths:
        os.makedirs(p, exist_ok=True)

def read_history_xlsx(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame(columns=["fecha","loteria","sorteo","n1","n2","n3"])
    df = pd.read_excel(path, sheet_name="history", dtype=str, engine="openpyxl")
    # normaliza
    for c in ["n1","n2","n3"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip().str.zfill(2)
    df["fecha"] = df["fecha"].astype(str).str.strip()
    df["loteria"] = df["loteria"].astype(str).str.strip()
    df["sorteo"] = df["sorteo"].astype(str).str.strip()
    return df

def upsert_history_xlsx(path: str, new_rows: pd.DataFrame):
    # new_rows debe venir con columnas fecha,loteria,sorteo,n1,n2,n3 (strings)
    old = read_history_xlsx(path)
    df = pd.concat([old, new_rows], ignore_index=True)

    # llave única por (fecha,loteria,sorteo)
    df = df.drop_duplicates(subset=["fecha","loteria","sorteo"], keep="last")
    df = df.sort_values(["fecha","loteria","sorteo"]).reset_index(drop=True)

    with pd.ExcelWriter(path, engine="openpyxl", mode="w") as w:
        df.to_excel(w, sheet_name="history", index=False)
