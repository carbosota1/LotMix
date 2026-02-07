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
}

# =========================
# SCHEDULE (draw == EXACTO columna "sorteo")
# update_after_minutes = 15 ✅ (según tu cambio)
# Nacional Noche = 21:00 ✅ (según tu corrección)
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
UPCOMING_GRACE_SECONDS = 120

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
    return {"last_updates": {}, "last_event_key": "", "sent_by_target_fp": {}, "last_wait_key": ""}

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
        # si se corrompe, arrancamos limpio (evita crash)
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

def try_update_one(item, state) -> bool:
    n = now_rd()
    date_str = today_str()
    draw_dt = draw_datetime_today(item["time"])
    due = draw_dt + timedelta(minutes=item["update_after_minutes"])

    key = f"{date_str}|{item['lottery']}|{item['draw']}"
    last_updates = state.get("last_updates", {})

    if n < due:
        return False
    if last_updates.get(key) == "done":
        return False

    p1, p2, p3 = fetch_result(item["lottery"], item["draw"], date_str)
    p1, p2, p3 = normalize_2d(p1), normalize_2d(p2), normalize_2d(p3)

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
# GATE: cross-match REAL (no adivinar)
# -----------------------------
def _due_dt(item) -> datetime:
    return draw_datetime_today(item["time"]) + timedelta(minutes=item["update_after_minutes"])

def _is_due(item, now: datetime) -> bool:
    return now >= _due_dt(item)

def _has_today_row(lottery: str, draw: str, date_str: str) -> bool:
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

def missing_due_updates_before_target(target_dt: datetime) -> list[str]:
    """Sorteos previos al target que ya deberían estar en histórico HOY y faltan."""
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
            if not _has_today_row(item["lottery"], item["draw"], date_str):
                missing.append(f"{item['lottery']} | {item['draw']} (due {_due_dt(item).strftime('%H:%M')})")
    return missing

def missing_due_updates_global() -> list[str]:
    """Todos los sorteos del día que ya deberían estar actualizados y faltan."""
    n = now_rd()
    date_str = today_str()
    missing = []
    for item in SCHEDULE:
        if _is_due(item, n):
            if not _has_today_row(item["lottery"], item["draw"], date_str):
                missing.append(f"{item['lottery']} | {item['draw']} (due {_due_dt(item).strftime('%H:%M')})")
    return missing


# -----------------------------
# Next target (TARGET=1) con GRACE
# -----------------------------
def next_upcoming_draw():
    n = now_rd()
    best = None
    for item in SCHEDULE:
        dt = draw_datetime_today(item["time"])
        if dt.date() != n.date():
            continue

        # tolerancia por segundos tarde
        if dt < (n - timedelta(seconds=UPCOMING_GRACE_SECONDS)):
            continue

        if (dt - n).total_seconds() > LOOKAHEAD_MINUTES * 60:
            continue

        if best is None or dt < best[0]:
            best = (dt, item)
    return best


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
# Intradía analysis + notify
# -----------------------------
def run_intraday_next_target(event_key: str, state: dict):
    exp = build_exploded_history()
    if exp is None:
        print("[INFO] No history data loaded yet.")
        return

    nxt = next_upcoming_draw()
    if not nxt:
        print("[INFO] No upcoming draw in lookahead window.")
        return

    target_dt, target = nxt
    print("[INFO] Next target:", target_dt.strftime("%Y-%m-%d %H:%M"), target["lottery"], target["draw"])

    # ✅ GATE: no picks si falta data previa debida (cross-match incompleto)
    missing = missing_due_updates_before_target(target_dt)
    if missing and (not FORCE_NOTIFY):
        print("[INFO] Missing due updates before target. Skipping picks.")
        for m in missing:
            print("[INFO] Missing:", m)

        wait_key = f"{target_dt.strftime('%Y-%m-%d %H:%M')}|{target['lottery']}|{target['draw']}"
        if state.get("last_wait_key") != wait_key:
            try:
                msg = []
                msg.append("⏳ OPV INTRADÍA (Esperando data)")
                msg.append(f"🎯 Target: {target['lottery']} | {target['draw']}")
                msg.append(f"⏰ Hora: {target_dt.strftime('%H:%M')} RD")
                msg.append("")
                msg.append("Faltan resultados previos del día (cross-match incompleto):")
                msg.extend([f"• {x}" for x in missing[:10]])
                send_telegram("\n".join(msg))
                state["last_wait_key"] = wait_key
            except Exception as e:
                print(f"[WARN] Telegram wait message failed: {e}")
        return

    target_dt_naive = target_dt.replace(tzinfo=None)

    # Histórico (cross-lottery)
    src_filter_hist = lambda e: ~((e["lottery"] == target["lottery"]) & (e["sorteo"] == target["draw"]))
    rec_hist = recommend_for_target(exp, src_filter_hist, target["lottery"], target["draw"], lag_days=0, top_n=TOPK_FULL)
    if rec_hist is None or rec_hist.empty:
        print("[INFO] Not enough data to compute historical recommendations.")
        return

    # Intradía: hoy, antes del target
    today = target_dt_naive.date()
    exp_today = exp[(exp["fecha_dt"].dt.date == today) & (exp["fecha_dt"] < target_dt_naive)].copy()

    rec_today = None
    if not exp_today.empty:
        mask_idx = set(exp_today.index.tolist())
        src_filter_today = lambda e, _m=mask_idx: e.index.isin(_m)
        rec_today = recommend_for_target(exp, src_filter_today, target["lottery"], target["draw"], lag_days=0, top_n=TOPK_FULL)

    # Combinar: por simplicidad, usamos histórico como base (quirúrgico)
    top12 = rec_hist["num"].astype(str).tolist()[:TOPK_FULL]
    topq = top12[:TOPK_QUINIELA]
    pales = format_pales(top_pales(top12[:10], 40))[:PALES_OUT]

    ok = should_alert(rec_hist, min_signal=MIN_SIGNAL, min_count_hits=MIN_A11)

    best_signal_hist = float(rec_hist["signal"].max()) if "signal" in rec_hist.columns else None
    best_a11_hist = int(rec_hist["a11"].max()) if "a11" in rec_hist.columns else None
    best_signal_today = float(rec_today["signal"].max()) if (rec_today is not None and "signal" in rec_today.columns and not rec_today.empty) else None
    best_a11_today = int(rec_today["a11"].max()) if (rec_today is not None and "a11" in rec_today.columns and not rec_today.empty) else None

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
                "has_intraday_sources": int(not exp_today.empty),
            }
        }
    }

    ensure_dir(OUT_DIR)
    with open(os.path.join(OUT_DIR, "picks.json"), "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print("[OK] Wrote outputs/picks.json")

    # ✅ NO mandar picks si no hay edge, a menos que sea TEST
    if (not ok) and (not FORCE_NOTIFY):
        print("[INFO] No valid opportunity (thresholds not met). No message sent.")
        return

    target_key = f"{payload['target']['time_rd']}|{payload['target']['lottery']}|{payload['target']['draw']}"
    sent_map = state.get("sent_by_target_fp", {})
    if sent_map.get(target_key, "") == fp and (not FORCE_NOTIFY):
        print("[INFO] Same picks fingerprint already sent for this target.")
        return

    msg = []
    msg.append("🚨 OPV INTRADÍA (Cross-Match Real / Data Real)")
    msg.append(f"🧩 Señal nueva: {event_key}")
    msg.append(f"🎯 Próximo target: {target['lottery']} | {target['draw']}")
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

    send_telegram("\n".join(msg))
    print("[OK] Telegram sent.")

    sent_map[target_key] = fp
    state["sent_by_target_fp"] = sent_map


# -----------------------------
# MAIN
# -----------------------------
def main():
    ensure_dir(DATA_DIR)
    ensure_dir(HIST_DIR)
    ensure_dir(OUT_DIR)

    state = load_state()

    updated = []

    # 1) Update pass normal
    for item in SCHEDULE:
        try:
            if try_update_one(item, state):
                print("[OK] Updated:", item["lottery"], item["draw"])
                updated.append(item)
        except Exception as e:
            print(f"[WARN] update failed {item['lottery']}|{item['draw']}: {e}")

    # 2) Reintento: si ya están DUE y faltan, intenta 2 veces más en este mismo run
    #    (esto evita que se quede atrás La Primera 12PM o La Nacional Noche)
    for attempt in range(2):
        missing_now = missing_due_updates_global()
        if not missing_now:
            break
        print(f"[INFO] Due missing updates detected. Retry pass {attempt+1}/2")
        time.sleep(2)

        for item in SCHEDULE:
            try:
                n = now_rd()
                if _is_due(item, n):
                    if not _has_today_row(item["lottery"], item["draw"], today_str()):
                        if try_update_one(item, state):
                            print("[OK] Updated (retry):", item["lottery"], item["draw"])
                            updated.append(item)
            except Exception as e:
                print(f"[WARN] retry update failed {item['lottery']}|{item['draw']}: {e}")

    # 3) Si faltan updates "due", NO se analiza (evita adivinar)
    missing_due = missing_due_updates_global()
    if missing_due and (not FORCE_NOTIFY):
        print("[INFO] Still missing due updates. Skipping analysis.")
        for m in missing_due:
            print("[INFO] Missing:", m)

        # Telegram de espera (1 vez por estado)
        wait_key = f"{today_str()}|WAIT|{len(missing_due)}"
        if state.get("last_wait_key") != wait_key:
            try:
                msg = []
                msg.append("⏳ OPV (Esperando resultados)")
                msg.append("No se generarán picks hasta que se actualicen TODOS los sorteos debidos.")
                msg.append("")
                msg.append("Faltan:")
                msg.extend([f"• {x}" for x in missing_due[:12]])
                send_telegram("\n".join(msg))
                state["last_wait_key"] = wait_key
            except Exception as e:
                print(f"[WARN] Telegram wait message failed: {e}")

        save_state(state)
        print("[OK] runner finished")
        return

    # 4) Si no hubo updates y no es test, no spam
    if not updated and (not FORCE_NOTIFY):
        print("[INFO] No new updates. Skipping intraday analysis/notify.")
        save_state(state)
        print("[OK] runner finished")
        return

    # 5) Intradía: evento = último update que entró (o TEST)
    if FORCE_NOTIFY and not updated:
        event_key = f"{today_str()}|TEST|NO-UPDATE"
    else:
        updated_sorted = sorted(updated, key=lambda x: draw_datetime_today(x["time"]))
        last_event = updated_sorted[-1]
        event_key = f"{today_str()}|{last_event['lottery']}|{last_event['draw']}"

    # Evita repetir procesamiento del mismo evento
    if state.get("last_event_key") == event_key and (not FORCE_NOTIFY):
        print("[INFO] Latest event already processed. Skipping.")
        save_state(state)
        print("[OK] runner finished")
        return

    try:
        run_intraday_next_target(event_key, state)
    except Exception as e:
        print(f"[WARN] intraday analysis/notify failed: {e}")

    state["last_event_key"] = event_key
    save_state(state)
    print("[OK] runner finished")


if __name__ == "__main__":
    main()
