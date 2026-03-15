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
# =========================
UPDATE_AFTER = 2

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
    # 👇 Nacional Noche cambia los domingos a 18:00 (se maneja dinámico abajo)
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
# UMBRALES "EDGE" (modo ganador)
# =========================
MIN_SIGNAL = 0.0075
MIN_A11    = 11

LOOKAHEAD_MINUTES = 5 * 60
UPCOMING_GRACE_SECONDS = 10 * 60  # si ejecutas tarde manual, aún procesa

FORCE_NOTIFY = os.getenv("FORCE_NOTIFY", "0").strip() == "1"

# =========================
# ✅ NUEVO (SIN TOCAR LO DEMÁS):
# - Recorta histórico cuando se “diluye” por demasiadas filas usadas
# - Regla especial para el PRIMER target del día (ej Anguilla 10AM)
# =========================
MAX_SOURCE_ROWS = 3000          # cap duro de filas usadas en histórico
RECENT_DAYS_CAP = 180           # si hay demasiadas filas, limitar a últimos N días
FIRST_TARGET_RECENT_DAYS = 120  # primer sorteo del día usa recencia para variar picks


# -----------------------------
# Helpers
# -----------------------------
def now_rd() -> datetime:
    return datetime.now(TZ)

def today_str() -> str:
    return now_rd().strftime("%Y-%m-%d")

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

def fingerprint(topq, top12, pales):
    s = "|".join(topq) + "||" + "|".join(top12) + "||" + "|".join(pales)
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
# Dynamic time: Nacional Noche domingo = 18:00
# -----------------------------
def item_time(item: dict) -> str:
    """
    Ajusta horarios por reglas especiales.
    - Nacional Noche: domingo 18:00 (6PM), otros días usa el time definido.
    """
    try:
        n = now_rd()
        is_sun = (n.weekday() == 6)  # Monday=0 ... Sunday=6
        if item["lottery"] == "La Nacional" and item["draw"] == "Loteria Nacional- Noche" and is_sun:
            return "18:00"
    except Exception:
        pass
    return item["time"]

def draw_datetime_today_from_item(item: dict) -> datetime:
    hhmm = item_time(item)
    h, m = map(int, hhmm.split(":"))
    n = now_rd()
    return n.replace(hour=h, minute=m, second=0, microsecond=0)


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
    return draw_datetime_today_from_item(item) + timedelta(minutes=item["update_after_minutes"])

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

    if not _is_due(item, n):
        return False

    key = f"{date_str}|{item['lottery']}|{item['draw']}"
    last_updates = state.get("last_updates", {})
    if last_updates.get(key) == "done":
        if _has_row_for_date(item["lottery"], item["draw"], date_str):
            return False

    p1, p2, p3 = fetch_result(item["lottery"], item["draw"], date_str)
    p1, p2, p3 = normalize_2d(p1), normalize_2d(p2), normalize_2d(p3)

    # Anti-invención HOY: si devuelve exactamente lo mismo que AYER antes de tiempo, no insertamos.
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
        draw_dt = draw_datetime_today_from_item(item)
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
# Next targets: empate en hora => procesa TODOS
# -----------------------------
def next_targets_same_time():
    n = now_rd()
    candidates = []

    for item in SCHEDULE:
        dt = draw_datetime_today_from_item(item)
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
# Picks logging + grading (performance incluye ganadores + best_signal)
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
        "fingerprint": bp.get("fingerprint", ""),
        "source_rows_hist_used": bp.get("debug", {}).get("source_rows_hist_used"),
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

        try:
            bs = float(r.get("best_signal")) if r.get("best_signal") not in (None, "", "nan") else None
        except Exception:
            bs = None
        try:
            ba = int(float(r.get("best_a11"))) if r.get("best_a11") not in (None, "", "nan") else None
        except Exception:
            ba = None

        perf_rows.append({
            "key": key,
            "date": date_s,
            "time_rd": time_rd,
            "lottery": lottery,
            "draw": draw,
            "result": f"{row['primero']}-{row['segundo']}-{row['tercero']}",
            "best_signal": bs,
            "best_a11": ba,
            "ok_alert": r.get("ok_alert", ""),
            "hits_quiniela_topq": hits(topq, drawn),
            "hits_quiniela_top12": hits(top12, drawn),
            "pale_hits": pale_hits(pales, drawn),
            "source_rows_hist_used": float(r.get("source_rows_hist_used")) 
              if r.get("source_rows_hist_used") not in (None, "", "nan") else None,
            
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
# NEW: obtener números observados HOY en sorteos previos (cross-match real)
# -----------------------------
def observed_nums_today_before(target_dt: datetime) -> set[str]:
    date_str = today_str()
    obs = set()
    for it in SCHEDULE:
        dt = draw_datetime_today_from_item(it)
        if dt.date() != target_dt.date():
            continue
        if dt >= target_dt:
            continue
        if not _has_row_for_date(it["lottery"], it["draw"], date_str):
            continue
        r = _get_row_for_date(it["lottery"], it["draw"], date_str)
        if r:
            obs.update({r[0], r[1], r[2]})
    return obs


# -----------------------------
# Analysis per target (MI/Chi² histórico pero CONDICIONADO por secuencia de HOY)
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

    # 1) Fuentes permitidas = sorteos previos DEL DÍA (por hora) (secuencia)
    prior_pairs = set()
    for it in SCHEDULE:
        dt = draw_datetime_today_from_item(it)
        if dt.date() != target_dt.date():
            continue
        if dt >= target_dt:
            continue
        prior_pairs.add((it["lottery"], it["draw"]))

    # 2) Números observados HOY en esos sorteos previos
    obs_nums = observed_nums_today_before(target_dt)

    # ✅ Regla especial: PRIMER target del día (no hay prior_pairs / obs)
    #    -> usa MI/Chi² pero con recencia fija para evitar repetición eterna.
    if not prior_pairs:
        target_dt_naive = target_dt.replace(tzinfo=None)
        cutoff = target_dt_naive - timedelta(days=FIRST_TARGET_RECENT_DAYS)
        recent_mask = exp["fecha_dt"] >= cutoff

        def src_filter_first(e, _rm=recent_mask):
            return _rm & ~((e["lottery"] == target["lottery"]) & (e["sorteo"] == target["draw"]))

        rec_hist = recommend_for_target(
            exp,
            src_filter_first,
            target["lottery"],
            target["draw"],
            lag_days=0,
            top_n=TOPK_FULL
        )
        used_rows = int(recent_mask.sum())
        used_pairs = 0
    else:
        # 3) Construir máscara de fuentes dentro del histórico:
        #    - SOLO eventos cuyo (lottery,sorteo) está en prior_pairs
        #    - y además num ∈ obs_nums (si existe) para forzar cross-match real
        base_mask = exp.apply(lambda r: (r.get("lottery"), r.get("sorteo")) in prior_pairs, axis=1)
        if obs_nums:
            num_mask = exp["num"].astype(str).isin(obs_nums)
            mask = base_mask & num_mask
        else:
            mask = base_mask

        # ✅ CAP anti-dilución (si hay demasiado histórico usado)
        target_dt_naive = target_dt.replace(tzinfo=None)
        used_pairs = len(prior_pairs)

        if mask.sum() > MAX_SOURCE_ROWS:
            cutoff = target_dt_naive - timedelta(days=RECENT_DAYS_CAP)
            mask = mask & (exp["fecha_dt"] >= cutoff)

        if mask.sum() > MAX_SOURCE_ROWS:
            tmp = exp[mask].sort_values("fecha_dt")
            tail_idx = tmp.tail(MAX_SOURCE_ROWS).index
            mask = exp.index.isin(tail_idx)

        mask_idx = set(exp[mask].index.tolist())
        used_rows = len(mask_idx)

        # Fallback: si no hay suficiente fuente, usa solo prior_pairs sin filtrar por num
        if len(mask_idx) < 10 and prior_pairs:
            mask2 = base_mask
            if mask2.sum() > MAX_SOURCE_ROWS:
                cutoff = target_dt_naive - timedelta(days=RECENT_DAYS_CAP)
                mask2 = mask2 & (exp["fecha_dt"] >= cutoff)
            if mask2.sum() > MAX_SOURCE_ROWS:
                tmp2 = exp[mask2].sort_values("fecha_dt")
                tail_idx2 = tmp2.tail(MAX_SOURCE_ROWS).index
                mask2 = exp.index.isin(tail_idx2)

            mask_idx = set(exp[mask2].index.tolist())
            used_rows = len(mask_idx)

        # Fallback final: si sigue vacío, vuelve a lo clásico (excluir solo el mismo target) PERO con recencia
        if not mask_idx:
            cutoff = target_dt_naive - timedelta(days=RECENT_DAYS_CAP)
            recent_mask = exp["fecha_dt"] >= cutoff
            def src_filter(e, _rm=recent_mask):
                return _rm & ~((e["lottery"] == target["lottery"]) & (e["sorteo"] == target["draw"]))
            rec_hist = recommend_for_target(
                exp,
                src_filter,
                target["lottery"],
                target["draw"],
                lag_days=0,
                top_n=TOPK_FULL
            )
            used_rows = int(recent_mask.sum())
        else:
            src_filter = lambda e, _m=mask_idx: e.index.isin(_m)
            rec_hist = recommend_for_target(
                exp,
                src_filter,
                target["lottery"],
                target["draw"],
                lag_days=0,
                top_n=TOPK_FULL
            )

    if rec_hist is None or rec_hist.empty:
        print("[INFO] Not enough data to compute recommendations for target.")
        return None

    rec = rec_hist.copy()
    rec["num"] = rec["num"].astype(str)
    rec["signal"] = pd.to_numeric(rec.get("signal", 0), errors="coerce").fillna(0.0)
    rec["a11"] = pd.to_numeric(rec.get("a11", 0), errors="coerce").fillna(0).astype(int)

    rec = rec.sort_values(["signal", "a11"], ascending=False)
    top12 = rec["num"].tolist()[:TOPK_FULL]
    topq = top12[:TOPK_QUINIELA]
    pales = format_pales(top_pales(top12[:10], 40))[:PALES_OUT]

    best_signal_hist = float(rec["signal"].max()) if not rec.empty else None
    best_a11_hist = int(rec["a11"].max()) if not rec.empty else None

    # -------------------------------------------------
    # THRESHOLDS DINÁMICOS POR LOTERÍA
    # -------------------------------------------------

    lottery_name = target["lottery"]

    min_signal = MIN_SIGNAL
    min_a11 = MIN_A11

    if lottery_name == "La Nacional":
       min_signal = 0.007
       min_a11 = 6

    elif lottery_name == "Anguilla":
         min_signal = 0.018
         min_a11 = 7

    elif lottery_name == "La Primera":
        min_signal = 0.020
        min_a11 = 3

    elif lottery_name == "La Suerte":
        min_a11 = 8

    ok = should_alert(rec_hist, min_signal=min_signal, min_count_hits=min_a11)
    fp = fingerprint(topq, top12, pales)

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
                "has_intraday_sources": 0,
                "intraday_events": 0,
                "today_observed_nums": len(obs_nums),
                "source_pairs_today": int(used_pairs),
                "source_rows_hist_used": int(used_rows),
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
    msg.append("🚨 OPV (Cross-Match SECUENCIAL / MI + Chi² HISTÓRICO)")
    msg.append(f"🧩 Señal nueva: {event_key}")
    msg.append(f"🎯 Target: {target['lottery']} | {target['draw']}")
    msg.append(f"⏰ Hora: {target_dt.strftime('%H:%M')} RD")
    msg.append("")
    msg.append(f"✅ QUINIELA Top{len(topq)}:")
    msg.append(", ".join(topq))
    msg.append("")
    msg.append("📌 Top12:")
    msg.append(", ".join(top12))
    msg.append("")
    msg.append(f"🎲 PALE Top{len(pales)}:")
    msg.append(" | ".join(pales))
    msg.append("")
    msg.append("📊 Debug:")
    msg.append(f"best_signal={best_signal_hist} best_a11={best_a11_hist} ok_alert={bool(ok)}")
    msg.append(f"today_observed_nums={len(obs_nums)} source_rows_hist_used={int(used_rows)}")

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
        print("[INFO] No new updates today. Skipping analysis/notify.")
        save_state(state)
        print("[OK] runner finished")
        return

    # 6) Evento
    if FORCE_NOTIFY and not updated_today:
        event_key = f"{today_str()}|TEST|NO-UPDATE"
    else:
        updated_sorted = sorted(updated_today, key=lambda x: draw_datetime_today_from_item(x))
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
            json.dump({"generated_at": now_rd().isoformat(), "event_key": event_key, "items": picks_all},
                      f, ensure_ascii=False, indent=2)

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
