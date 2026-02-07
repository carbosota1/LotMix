# src/runner.py
import os
import sys
import json
import hashlib
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd

# ✅ Asegura que /src esté en el path (funciona local y en Actions)
sys.path.insert(0, os.path.dirname(__file__))

from io_xlsx import ensure_dir, read_history_xlsx, upsert_history_xlsx, normalize_2d
from analyze import explode, recommend_for_target, should_alert, top_pales
from telegram import send_telegram

TZ = ZoneInfo("America/Santo_Domingo")

DATA_DIR = "data"
HIST_DIR = os.path.join(DATA_DIR, "histories")
STATE_PATH = os.path.join(DATA_DIR, "state.json")
OUT_DIR = "outputs"

# =========================
# HISTORIAL XLSX (DEBEN EXISTIR EN EL REPO)
# =========================
XLSX_FILES = {
    "La Primera": os.path.join(HIST_DIR, "La Primera History.xlsx"),
    "Anguilla":   os.path.join(HIST_DIR, "Anguilla history.xlsx"),
    "La Nacional": os.path.join(HIST_DIR, "La nacional history.xlsx"),
    "La Suerte":  os.path.join(HIST_DIR, "La suerte history.xlsx"),
}

# =========================
# SCHEDULE (draw == EXACTO columna "sorteo")
# ✅ Nacional Noche = 21:00
# =========================
UPDATE_AFTER = 15

SCHEDULE = [
    # Anguilla (4)
    {"lottery": "Anguilla", "draw": "Anguila 10AM", "time": "10:00", "update_after_minutes": UPDATE_AFTER},
    {"lottery": "Anguilla", "draw": "Anguila 1PM",  "time": "13:00", "update_after_minutes": UPDATE_AFTER},
    {"lottery": "Anguilla", "draw": "Anguila 6PM",  "time": "18:00", "update_after_minutes": UPDATE_AFTER},
    {"lottery": "Anguilla", "draw": "Anguila 9PM",  "time": "21:00", "update_after_minutes": UPDATE_AFTER},

    # La Primera (2)
    {"lottery": "La Primera", "draw": "Quiniela La Primera",       "time": "12:00", "update_after_minutes": UPDATE_AFTER},
    {"lottery": "La Primera", "draw": "Quiniela La Primera Noche", "time": "19:00", "update_after_minutes": UPDATE_AFTER},

    # La Nacional (2)
    {"lottery": "La Nacional", "draw": "Loteria Nacional- Gana Más", "time": "14:30", "update_after_minutes": UPDATE_AFTER},
    {"lottery": "La Nacional", "draw": "Loteria Nacional- Noche",    "time": "21:00", "update_after_minutes": UPDATE_AFTER},

    # La Suerte (2)
    {"lottery": "La Suerte", "draw": "Quiniela La Suerte",     "time": "12:30", "update_after_minutes": UPDATE_AFTER},
    {"lottery": "La Suerte", "draw": "Quiniela La Suerte 6PM", "time": "18:00", "update_after_minutes": UPDATE_AFTER},
]

# =========================
# PRECISIÓN QUIRÚRGICA (Telegram)
# =========================
TOPK_QUINIELA = 3
TOPK_FULL     = 12
PALES_OUT     = 3

# =========================
# UMBRALES "EDGE"
# =========================
MIN_SIGNAL = 0.010
MIN_A11    = 10

LOOKAHEAD_MINUTES = 16 * 60
UPCOMING_GRACE_SECONDS = 120  # tolerancia si el job llega un poco tarde

FORCE_NOTIFY = os.getenv("FORCE_NOTIFY", "0").strip() == "1"


# -----------------------------
# Helpers
# -----------------------------
def now_rd() -> datetime:
    return datetime.now(TZ)

def today_str() -> str:
    return now_rd().strftime("%Y-%m-%d")

def draw_datetime_today(time_hhmm: str) -> datetime:
    h, m = map(int, time_hhmm.split(":"))
    n = now_rd()
    return n.replace(hour=h, minute=m, second=0, microsecond=0)

def _ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def _norm2(x: str) -> str:
    s = str(x).strip()
    if s.isdigit():
        return s.zfill(2)
    return s

def _norm_pair(a: str, b: str) -> str:
    a = _norm2(a)
    b = _norm2(b)
    aa, bb = sorted([a, b])
    return f"{aa}-{bb}"

def format_pales(pales_raw):
    """Devuelve palés válidos en formato 'AA-BB' (sin repetidos AA-AA)."""
    out = []
    if not pales_raw:
        return out
    seen = set()
    for p in pales_raw:
        try:
            if isinstance(p, (tuple, list)) and len(p) >= 2:
                a, b = str(p[0]).strip(), str(p[1]).strip()
            else:
                s = str(p).strip()
                if "-" not in s:
                    continue
                a, b = s.split("-", 1)
                a, b = a.strip(), b.strip()

            a = _norm2(a)
            b = _norm2(b)
            if not a or not b:
                continue
            if a == b:
                continue

            pair = _norm_pair(a, b)
            if pair in seen:
                continue
            seen.add(pair)
            out.append(pair)
        except Exception:
            continue
    return out

def fingerprint(topq, pales):
    s = "|".join(topq) + "||" + "|".join(pales)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]


# -----------------------------
# State (robusto)
# -----------------------------
def _fresh_state():
    return {
        "last_updates": {},           # date|lottery|draw -> done
        "last_event_key": "",
        "sent_by_target_fp": {},      # target_key -> fingerprint
        "last_wait_key": "",
    }

def load_state():
    if not os.path.exists(STATE_PATH):
        return _fresh_state()
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            raw = f.read().strip()
            if not raw:
                raise ValueError("state.json vacío")
            st = json.loads(raw)
        if not isinstance(st, dict):
            raise ValueError("state.json no es dict")
    except Exception:
        return _fresh_state()

    st.setdefault("last_updates", {})
    st.setdefault("last_event_key", "")
    st.setdefault("sent_by_target_fp", {})
    st.setdefault("last_wait_key", "")
    return st

def save_state(state):
    ensure_dir(DATA_DIR)
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    os.replace(tmp, STATE_PATH)


# -----------------------------
# Scraper hooks (load by file path)
# -----------------------------
def fetch_result(lottery: str, draw: str, date: str):
    import importlib.util

    file_map = {
        "Anguilla": "anguilla_scraper.py",
        "La Primera": "laprimera_scraper.py",
        "La Nacional": "lanacional_scraper.py",
        "La Suerte": "lasuerte_scraper.py",
    }
    if lottery not in file_map:
        raise ValueError(f"Lottery no soportada: {lottery}")

    scrapers_dir = os.path.join(os.path.dirname(__file__), "scrapers")
    file_path = os.path.join(scrapers_dir, file_map[lottery])
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"No existe el scraper en: {file_path}")

    spec = importlib.util.spec_from_file_location(f"{lottery}_scraper", file_path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)

    if not hasattr(module, "get_result"):
        raise AttributeError(f"El scraper {file_path} no tiene get_result(draw, date)")
    return module.get_result(draw, date)


# -----------------------------
# DUE logic
# -----------------------------
def _due_dt(item) -> datetime:
    return draw_datetime_today(item["time"]) + timedelta(minutes=item["update_after_minutes"])

def _is_due(item, now: datetime) -> bool:
    return now >= _due_dt(item)


# -----------------------------
# XLSX truth checks
# -----------------------------
def _has_row_for_date(lottery: str, draw: str, date_str: str) -> bool:
    path = XLSX_FILES.get(lottery)
    if not path or not os.path.exists(path):
        return False
    try:
        df = pd.read_excel(path)
    except Exception:
        return False

    df.columns = [str(c).strip().lower() for c in df.columns]
    need = {"fecha", "sorteo", "primero", "segundo", "tercero"}
    if not need.issubset(set(df.columns)):
        return False

    df["fecha"] = df["fecha"].astype(str).str.slice(0, 10)
    df["sorteo"] = df["sorteo"].astype(str)
    m = df[(df["fecha"] == date_str) & (df["sorteo"] == draw)]
    return not m.empty

def _get_row_for_date(lottery: str, draw: str, date_str: str):
    path = XLSX_FILES.get(lottery)
    if not path or not os.path.exists(path):
        return None
    try:
        df = pd.read_excel(path)
    except Exception:
        return None
    df.columns = [str(c).strip().lower() for c in df.columns]
    need = {"fecha", "sorteo", "primero", "segundo", "tercero"}
    if not need.issubset(set(df.columns)):
        return None
    df["fecha"] = df["fecha"].astype(str).str.slice(0, 10)
    df["sorteo"] = df["sorteo"].astype(str)
    m = df[(df["fecha"] == date_str) & (df["sorteo"] == draw)]
    if m.empty:
        return None
    r = m.iloc[-1]
    return (normalize_2d(str(r["primero"])), normalize_2d(str(r["segundo"])), normalize_2d(str(r["tercero"])))


# -----------------------------
# Normal update (HOY, solo DUE)
# -----------------------------
def try_update_one(item, state) -> bool:
    n = now_rd()
    date_str = today_str()

    # Solo cuando está DUE
    if not _is_due(item, n):
        return False

    key = f"{date_str}|{item['lottery']}|{item['draw']}"
    last_updates = state.get("last_updates", {})
    if last_updates.get(key) == "done":
        # si state dice done pero XLSX no tiene fila, no confiamos en state
        if _has_row_for_date(item["lottery"], item["draw"], date_str):
            return False

    p1, p2, p3 = fetch_result(item["lottery"], item["draw"], date_str)
    p1, p2, p3 = normalize_2d(p1), normalize_2d(p2), normalize_2d(p3)

    # Anti-invención HOY: si devuelve exactamente lo mismo que AYER, no insertamos aún (ventana 90min)
    yday = (now_rd().date() - timedelta(days=1)).strftime("%Y-%m-%d")
    yres = _get_row_for_date(item["lottery"], item["draw"], yday)
    if yres is not None and (p1, p2, p3) == yres:
        due = _due_dt(item)
        if n < (due + timedelta(minutes=90)):
            raise RuntimeError("Resultado aún no publicado (mismo que ayer). Skipping insert.")

    new_row = pd.DataFrame([{
        "fecha": date_str,
        "sorteo": item["draw"],
        "primero": p1,
        "segundo": p2,
        "tercero": p3,
    }])

    ensure_dir(HIST_DIR)
    upsert_history_xlsx(XLSX_FILES[item["lottery"]], new_row)

    last_updates[key] = "done"
    state["last_updates"] = last_updates
    return True


# -----------------------------
# FORCE REFRESH + BACKFILL (HOY + AYER)
# HOY: SOLO DUE
# AYER: todos faltantes
# -----------------------------
def _missing_for_date(date_str: str) -> list[dict]:
    missing = []
    n = now_rd()
    today = today_str()
    for item in SCHEDULE:
        if date_str == today and (not _is_due(item, n)):
            continue
        if not _has_row_for_date(item["lottery"], item["draw"], date_str):
            missing.append(item)
    return missing

def _try_update_for_date(item, date_str: str, state: dict) -> bool:
    key = f"{date_str}|{item['lottery']}|{item['draw']}"
    last_updates = state.get("last_updates", {})

    if _has_row_for_date(item["lottery"], item["draw"], date_str):
        last_updates[key] = "done"
        state["last_updates"] = last_updates
        return False

    p1, p2, p3 = fetch_result(item["lottery"], item["draw"], date_str)
    p1, p2, p3 = normalize_2d(p1), normalize_2d(p2), normalize_2d(p3)

    # Anti-invención solo HOY
    if date_str == today_str():
        n = now_rd()
        yday = (n.date() - timedelta(days=1)).strftime("%Y-%m-%d")
        yres = _get_row_for_date(item["lottery"], item["draw"], yday)
        if yres is not None and (p1, p2, p3) == yres:
            due = _due_dt(item)
            if n < (due + timedelta(minutes=90)):
                raise RuntimeError("Resultado aún no publicado (mismo que ayer). Skipping insert.")

    new_row = pd.DataFrame([{
        "fecha": date_str,
        "sorteo": item["draw"],
        "primero": p1,
        "segundo": p2,
        "tercero": p3,
    }])

    ensure_dir(HIST_DIR)
    upsert_history_xlsx(XLSX_FILES[item["lottery"]], new_row)

    last_updates[key] = "done"
    state["last_updates"] = last_updates
    return True

def force_refresh_backfill(state: dict, days_back: int = 1, max_attempts: int = 5, backoff_seconds=None) -> dict:
    if backoff_seconds is None:
        backoff_seconds = [2, 5, 10, 20, 30]

    base = now_rd().date()
    dates = [(base - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(0, days_back + 1)]

    for attempt in range(max_attempts):
        any_fixed = False

        for ds in dates:
            missing_items = _missing_for_date(ds)
            if not missing_items:
                continue

            print(f"[INFO] FORCE_REFRESH date={ds} missing={len(missing_items)} attempt={attempt+1}/{max_attempts}")
            for item in missing_items:
                try:
                    did = _try_update_for_date(item, ds, state)
                    if did:
                        any_fixed = True
                        print(f"[OK] Backfilled: {ds} | {item['lottery']} {item['draw']}")
                except Exception as e:
                    print(f"[WARN] Backfill skip/fail: {ds} | {item['lottery']} {item['draw']}: {e}")

        if attempt < max_attempts - 1 and not any_fixed:
            wait = backoff_seconds[min(attempt, len(backoff_seconds) - 1)]
            print(f"[INFO] FORCE_REFRESH waiting {wait}s before retry...")
            time.sleep(wait)

    return state


# -----------------------------
# GATE: no adivinar
# -----------------------------
def missing_due_updates_before_target(target_dt: datetime) -> list[str]:
    n = now_rd()
    date_str = today_str()
    missing = []

    for item in SCHEDULE:
        draw_dt = draw_datetime_today(item["time"])
        if draw_dt.date() != target_dt.date():
            continue
        if draw_dt >= target_dt:
            continue

        if _is_due(item, n):
            if not _has_row_for_date(item["lottery"], item["draw"], date_str):
                missing.append(f"{item['lottery']} | {item['draw']} (due {_due_dt(item).strftime('%H:%M')})")
    return missing

def missing_due_updates_global_today() -> list[str]:
    n = now_rd()
    date_str = today_str()
    missing = []
    for item in SCHEDULE:
        if _is_due(item, n):
            if not _has_row_for_date(item["lottery"], item["draw"], date_str):
                missing.append(f"{item['lottery']} | {item['draw']} (due {_due_dt(item).strftime('%H:%M')})")
    return missing


# -----------------------------
# Next targets: si hay empate en hora (18:00 o 21:00), procesa TODOS
# -----------------------------
def next_targets_same_time():
    n = now_rd()
    candidates = []

    for item in SCHEDULE:
        dt = draw_datetime_today(item["time"])
        if dt.date() != n.date():
            continue

        if dt < (n - timedelta(seconds=UPCOMING_GRACE_SECONDS)):
            continue

        if (dt - n).total_seconds() > LOOKAHEAD_MINUTES * 60:
            continue

        candidates.append((dt, item))

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[0])
    dt_min = candidates[0][0]
    same = [it for (dt, it) in candidates if dt == dt_min]
    return dt_min, same


# -----------------------------
# Picks logging + grading
# -----------------------------
def log_pick(payload: dict):
    _ensure_dir(DATA_DIR)
    log_path = os.path.join(DATA_DIR, "picks_log.csv")

    generated_at = payload.get("generated_at")
    date_str = (generated_at or "")[:10] if generated_at else today_str()

    bp = payload.get("best_play", {})
    time_rd = bp.get("time_rd", "")
    lottery = bp.get("lottery", "")
    draw = bp.get("draw", "")

    key = f"{date_str}|{lottery}|{draw}|{time_rd}"

    row = {
        "key": key,
        "date": date_str,
        "time_rd": time_rd,
        "lottery": lottery,
        "draw": draw,
        "generated_at": generated_at,
        "best_signal": bp.get("best_signal"),
        "best_a11": bp.get("best_a11"),
        "ok_alert": bp.get("ok_alert"),
        "top12": json.dumps(bp.get("top12", []), ensure_ascii=False),
        "topq": json.dumps(bp.get("topq", []), ensure_ascii=False),
        "pales": json.dumps(bp.get("pales", []), ensure_ascii=False),
        "graded": "0",
    }

    new_df = pd.DataFrame([row])

    if os.path.exists(log_path):
        old = pd.read_csv(log_path, dtype=str)
        combined = pd.concat([old, new_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["key"], keep="last")
        combined.to_csv(log_path, index=False, encoding="utf-8")
    else:
        new_df.to_csv(log_path, index=False, encoding="utf-8")

def grade_picks_from_histories():
    log_path = os.path.join(DATA_DIR, "picks_log.csv")
    if not os.path.exists(log_path):
        return

    df = pd.read_csv(log_path, dtype=str)
    if df.empty:
        return

    pending = df[df["graded"].fillna("0") != "1"].copy()
    if pending.empty:
        return

    hist_cache = {}

    def load_hist(lottery: str):
        if lottery in hist_cache:
            return hist_cache[lottery]
        path = XLSX_FILES.get(lottery)
        if not path or not os.path.exists(path):
            hist_cache[lottery] = pd.DataFrame()
            return hist_cache[lottery]

        hx = pd.read_excel(path)
        hx.columns = [str(c).strip().lower() for c in hx.columns]
        need = {"fecha", "sorteo", "primero", "segundo", "tercero"}
        if not need.issubset(set(hx.columns)):
            hist_cache[lottery] = pd.DataFrame()
            return hist_cache[lottery]

        hx["fecha"] = hx["fecha"].astype(str).str.slice(0, 10)
        hx["sorteo"] = hx["sorteo"].astype(str)
        for col in ["primero", "segundo", "tercero"]:
            hx[col] = hx[col].astype(str).str.extract(r"(\d{1,2})")[0].fillna("").str.zfill(2)

        hist_cache[lottery] = hx
        return hx

    def hits(nums_list, drawn_set):
        return len(set(nums_list).intersection(drawn_set))

    def pale_hits(pales_list, drawn_nums):
        drawn = sorted(list(drawn_nums))
        if len(drawn) < 3:
            return 0
        pairs = {f"{drawn[0]}-{drawn[1]}", f"{drawn[0]}-{drawn[2]}", f"{drawn[1]}-{drawn[2]}"}
        norm = set(format_pales(pales_list))
        return len(norm.intersection(pairs))

    perf_rows = []
    any_graded = False

    for _, r in pending.iterrows():
        date_s = r.get("date", "")
        lottery = r.get("lottery", "")
        draw = r.get("draw", "")
        time_rd = r.get("time_rd", "")
        key = r.get("key", "")

        hx = load_hist(lottery)
        if hx.empty:
            continue

        match = hx[(hx["fecha"] == date_s) & (hx["sorteo"] == draw)]
        if match.empty:
            continue

        row = match.iloc[-1]
        drawn = {row["primero"], row["segundo"], row["tercero"]}

        top12 = json.loads(r.get("top12", "[]") or "[]")
        topq  = json.loads(r.get("topq", "[]") or "[]")
        pales = json.loads(r.get("pales", "[]") or "[]")

        perf_rows.append({
            "key": key,
            "date": date_s,
            "time_rd": time_rd,
            "lottery": lottery,
            "draw": draw,
            "result": f"{row['primero']}-{row['segundo']}-{row['tercero']}",
            "hits_quiniela_topq": hits(topq, drawn),
            "hits_quiniela_top12": hits(top12, drawn),
            "pale_hits": pale_hits(pales, drawn),
        })

        df.loc[df["key"] == key, "graded"] = "1"
        any_graded = True

    if perf_rows:
        ensure_dir(OUT_DIR)
        perf_path = os.path.join(OUT_DIR, "performance.csv")
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


# -----------------------------
# Build exploded history
# -----------------------------
def build_exploded_history():
    frames = []
    for lot, path in XLSX_FILES.items():
        df = read_history_xlsx(path)
        if not df.empty:
            frames.append(explode(df, lot))
    if not frames:
        return None
    exp = pd.concat(frames, ignore_index=True).sort_values("fecha_dt").reset_index(drop=True)
    exp["fecha_dt"] = pd.to_datetime(exp["fecha_dt"], errors="coerce")
    exp = exp.dropna(subset=["fecha_dt"])
    return exp


# -----------------------------
# Intradía analysis per target (MI/Chi² ONLY for intraday)
# -----------------------------
def analyze_target_and_maybe_notify(exp, event_key: str, target_dt: datetime, target_item: dict, state: dict):
    target = target_item
    print("[INFO] Target:", target_dt.strftime("%Y-%m-%d %H:%M"), target["lottery"], target["draw"])

    # ✅ GATE: no picks si falta data previa debida (cross-match incompleto)
    missing = missing_due_updates_before_target(target_dt)
    if missing and (not FORCE_NOTIFY):
        print("[INFO] Missing due updates before this target. Skipping picks.")
        for m in missing:
            print("[INFO] Missing:", m)
        return None

    target_dt_naive = target_dt.replace(tzinfo=None)

    # ---------- HIST ----------
    src_filter_hist = lambda e: ~((e["lottery"] == target["lottery"]) & (e["sorteo"] == target["draw"]))
    rec_hist = recommend_for_target(exp, src_filter_hist, target["lottery"], target["draw"], lag_days=0, top_n=TOPK_FULL)

    if rec_hist is None or rec_hist.empty:
        print("[INFO] Not enough data to compute recommendations for target.")
        return None

    # ---------- TODAY (intradía SOLO si MI/Chi² da señal) ----------
    today_date = target_dt_naive.date()
    exp_today = exp[(exp["fecha_dt"].dt.date == today_date) & (exp["fecha_dt"] < target_dt_naive)].copy()

    rec_today = None
    if not exp_today.empty:
        mask_idx = set(exp_today.index.tolist())
        src_filter_today = lambda e, _m=mask_idx: e.index.isin(_m)
        rec_today = recommend_for_target(
            exp,
            src_filter_today,
            target["lottery"],
            target["draw"],
            lag_days=0,
            top_n=TOPK_FULL
        )

    # ✅ Solo intradía si hay señal MI/Chi² real (df no vacío)
    use_intraday = (rec_today is not None) and (not rec_today.empty)

    # ---------- BLEND ----------
    def _prep(df):
        d = df.copy()
        d["num"] = d["num"].astype(str)
        d["signal"] = pd.to_numeric(d.get("signal", 0), errors="coerce").fillna(0.0)
        d["a11"] = pd.to_numeric(d.get("a11", 0), errors="coerce").fillna(0).astype(int)
        return d[["num", "signal", "a11"]]

    hist = _prep(rec_hist)

    best_signal_today = None
    best_a11_today = None

    if use_intraday:
        tday = _prep(rec_today)

        def _norm(s):
            mx = float(s.max()) if len(s) else 0.0
            return (s / mx) if mx > 0 else s

        hist["sig_n_h"] = _norm(hist["signal"])
        tday["sig_n_t"] = _norm(tday["signal"])

        m = pd.merge(hist, tday, on="num", how="outer", suffixes=("_h", "_t")).fillna(0)

        # intradía manda, histórico respalda
        W_TODAY = 0.70
        W_HIST  = 0.30
        m["score"] = W_TODAY*m["sig_n_t"] + W_HIST*m["sig_n_h"]
        m["score"] = m["score"] + 0.0005*m["a11_t"] + 0.0002*m["a11_h"]

        blended = m.sort_values("score", ascending=False)
        top12 = blended["num"].tolist()[:TOPK_FULL]

        best_signal_today = float(tday["signal"].max()) if not tday.empty else None
        best_a11_today = int(tday["a11"].max()) if not tday.empty else None
    else:
        top12 = hist.sort_values(["signal", "a11"], ascending=False)["num"].tolist()[:TOPK_FULL]

    topq = top12[:TOPK_QUINIELA]
    pales = format_pales(top_pales(top12[:10], 40))[:PALES_OUT]

    ok = should_alert(rec_hist, min_signal=MIN_SIGNAL, min_count_hits=MIN_A11)

    best_signal_hist = float(hist["signal"].max()) if not hist.empty else None
    best_a11_hist = int(hist["a11"].max()) if not hist.empty else None

    fp = fingerprint(topq, pales)

    payload = {
        "generated_at": now_rd().isoformat(),
        "event_key": event_key,
        "target": {
            "time_rd": target_dt.strftime("%Y-%m-%d %H:%M"),
            "lottery": target["lottery"],
            "draw": target["draw"],
        },
        "best_play": {
            "time_rd": target_dt.strftime("%Y-%m-%d %H:%M"),
            "lottery": target["lottery"],
            "draw": target["draw"],
            "top12": top12,
            "topq": topq,
            "pales": pales,
            "fingerprint": fp,
            "ok_alert": bool(ok),
            "best_signal": best_signal_hist,
            "best_a11": best_a11_hist,
            "debug": {
                "best_signal_hist": best_signal_hist,
                "best_a11_hist": best_a11_hist,
                "best_signal_today": best_signal_today,
                "best_a11_today": best_a11_today,
                "has_intraday_sources": int(use_intraday),
                "intraday_events": int(len(exp_today)),
            }
        }
    }

    # Log SIEMPRE
    log_pick(payload)

    # Telegram solo si cumple o TEST
    if (not ok) and (not FORCE_NOTIFY):
        print("[INFO] Thresholds not met. No message sent for this target.")
        return payload

    target_key = f"{payload['target']['time_rd']}|{payload['target']['lottery']}|{payload['target']['draw']}"
    sent_map = state.get("sent_by_target_fp", {})
    if sent_map.get(target_key, "") == fp and (not FORCE_NOTIFY):
        print("[INFO] Same fingerprint already sent for this target.")
        return payload

    msg = []
    msg.append("🚨 OPV INTRADÍA (Cross-Match Real / Data Real)")
    msg.append(f"🧩 Señal nueva: {event_key}")
    msg.append(f"🎯 Target: {target['lottery']} | {target['draw']}")
    msg.append(f"⏰ Hora: {target_dt.strftime('%H:%M')} RD")
    msg.append("")
    msg.append(f"✅ QUINIELA Top{len(topq)}:")
    msg.append(", ".join(topq))
    msg.append("")
    msg.append(f"🎲 PALE Top{len(pales)}:")
    msg.append(" | ".join(pales))
    msg.append("")
    msg.append("📊 Debug:")
    msg.append(f"hist best_signal={best_signal_hist} best_a11={best_a11_hist}")
    msg.append(f"today best_signal={best_signal_today} best_a11={best_a11_today}")
    msg.append(f"has_intraday_sources={int(use_intraday)} intraday_events={len(exp_today)}")

    send_telegram("\n".join(msg))
    print("[OK] Telegram sent for target.")

    sent_map[target_key] = fp
    state["sent_by_target_fp"] = sent_map
    return payload


# -----------------------------
# MAIN
# -----------------------------
def main():
    ensure_dir(DATA_DIR)
    ensure_dir(HIST_DIR)
    ensure_dir(OUT_DIR)

    state = load_state()

    updated_today = []

    # 1) Update pass normal (solo HOY, solo DUE)
    for item in SCHEDULE:
        try:
            if try_update_one(item, state):
                print("[OK] Updated:", item["lottery"], item["draw"])
                updated_today.append(item)
        except Exception as e:
            print(f"[WARN] update failed {item['lottery']}|{item['draw']}: {e}")

    # 2) FORCE REFRESH + BACKFILL (HOY + AYER)
    try:
        state = force_refresh_backfill(state, days_back=1, max_attempts=5)
    except Exception as e:
        print(f"[WARN] force_refresh_backfill failed: {e}")

    # 3) Grading siempre
    try:
        grade_picks_from_histories()
        print("[OK] Grading pass completed.")
    except Exception as e:
        print(f"[WARN] grading failed: {e}")

    # 4) Si faltan updates "due" de HOY, NO se analiza (no adivinar)
    missing_due_today = missing_due_updates_global_today()
    if missing_due_today and (not FORCE_NOTIFY):
        print("[INFO] Still missing due updates today. Skipping analysis.")
        for m in missing_due_today:
            print("[INFO] Missing:", m)

        wait_key = f"{today_str()}|WAIT|{len(missing_due_today)}"
        if state.get("last_wait_key") != wait_key:
            try:
                msg = []
                msg.append("⏳ OPV (Esperando resultados)")
                msg.append("No se generarán picks hasta que se actualicen TODOS los sorteos debidos de HOY.")
                msg.append("")
                msg.append("Faltan:")
                msg.extend([f"• {x}" for x in missing_due_today[:20]])
                send_telegram("\n".join(msg))
                state["last_wait_key"] = wait_key
            except Exception as e:
                print(f"[WARN] Telegram wait message failed: {e}")

        save_state(state)
        print("[OK] runner finished")
        return

    # 5) Si no hubo updates hoy y no es test, no spam
    if not updated_today and (not FORCE_NOTIFY):
        print("[INFO] No new updates today. Skipping intraday analysis/notify.")
        save_state(state)
        print("[OK] runner finished")
        return

    # 6) Evento
    if FORCE_NOTIFY and not updated_today:
        event_key = f"{today_str()}|TEST|NO-UPDATE"
    else:
        updated_sorted = sorted(updated_today, key=lambda x: draw_datetime_today(x["time"]))
        last_event = updated_sorted[-1]
        event_key = f"{today_str()}|{last_event['lottery']}|{last_event['draw']}"

    if state.get("last_event_key") == event_key and (not FORCE_NOTIFY):
        print("[INFO] Latest event already processed. Skipping.")
        save_state(state)
        print("[OK] runner finished")
        return

    # 7) Build history + process next targets (handles dual 18:00 y dual 21:00)
    exp = build_exploded_history()
    if exp is None:
        print("[INFO] No history loaded. Exiting.")
        save_state(state)
        print("[OK] runner finished")
        return

    nxt = next_targets_same_time()
    if not nxt:
        print("[INFO] No next targets.")
        state["last_event_key"] = event_key
        save_state(state)
        print("[OK] runner finished")
        return

    dt_min, targets = nxt
    print(f"[INFO] Next time slot: {dt_min.strftime('%H:%M')} targets={len(targets)}")

    picks_all = []
    for t in targets:
        try:
            payload = analyze_target_and_maybe_notify(exp, event_key, dt_min, t, state)
            if payload:
                picks_all.append(payload)
        except Exception as e:
            print(f"[WARN] target analysis failed {t['lottery']}|{t['draw']}: {e}")

    # 8) Outputs
    ensure_dir(OUT_DIR)
    if picks_all:
        with open(os.path.join(OUT_DIR, "picks_all.json"), "w", encoding="utf-8") as f:
            json.dump({"generated_at": now_rd().isoformat(), "event_key": event_key, "items": picks_all}, f, ensure_ascii=False, indent=2)

        # compat: picks.json = último payload
        with open(os.path.join(OUT_DIR, "picks.json"), "w", encoding="utf-8") as f:
            json.dump(picks_all[-1], f, ensure_ascii=False, indent=2)

        print("[OK] Wrote outputs/picks_all.json and outputs/picks.json")
    else:
        print("[INFO] No payloads produced.")

    state["last_event_key"] = event_key
    save_state(state)
    print("[OK] runner finished")


if __name__ == "__main__":
    main()
