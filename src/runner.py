import os
import sys
import json
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

# XLSX (tus nombres)
XLSX_FILES = {
    "La Primera": os.path.join(HIST_DIR, "La Primera History.xlsx"),
    "Anguilla":   os.path.join(HIST_DIR, "Anguilla history.xlsx"),
    "La Nacional":os.path.join(HIST_DIR, "La nacional history.xlsx"),
}

# ✅ AJUSTA horas y nombres EXACTOS de sorteos (como están en tu columna 'sorteo')
SCHEDULE = [
    # Anguilla (4)
    {"lottery":"Anguilla", "draw":"ANG-10AM", "time":"10:00", "update_after_minutes":30},
    {"lottery":"Anguilla", "draw":"ANG-1PM",  "time":"13:00", "update_after_minutes":30},
    {"lottery":"Anguilla", "draw":"ANG-5PM",  "time":"17:00", "update_after_minutes":30},
    {"lottery":"Anguilla", "draw":"ANG-9PM",  "time":"21:00", "update_after_minutes":30},

    # La Primera (2)
    {"lottery":"La Primera", "draw":"LP-MediaDia", "time":"12:00", "update_after_minutes":30},
    {"lottery":"La Primera", "draw":"LP-Noche",    "time":"20:00", "update_after_minutes":30},

    # La Nacional (2)
    {"lottery":"La Nacional", "draw":"Loteria Nacional- Gana Más", "time":"14:30", "update_after_minutes":30},
    {"lottery":"La Nacional", "draw":"Loteria Nacional- Noche",    "time":"20:30", "update_after_minutes":30},
]

# Umbrales de “oportunidad válida”
MIN_SIGNAL = 0.010
MIN_A11    = 10

# Ventana para considerar “próximo sorteo cercano”
LOOKAHEAD_MINUTES = 720  # 12h (para que casi siempre haya próximo sorteo)

def now_rd():
    return datetime.now(TZ)

def load_state():
    if not os.path.exists(STATE_PATH):
        return {"last_updates": {}, "last_alert_key": ""}
    with open(STATE_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def save_state(state):
    ensure_dir(DATA_DIR)
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

def draw_datetime_today(time_hhmm: str) -> datetime:
    h, m = map(int, time_hhmm.split(":"))
    n = now_rd()
    return n.replace(hour=h, minute=m, second=0, microsecond=0)

# -----------------------------
# SCRAPER HOOKS
# -----------------------------
def fetch_result(lottery: str, draw: str, date: str):
    """
    Debe devolver tuple (primero, segundo, tercero) como strings '00'..'99'
    """
    if lottery == "Anguilla":
        from scrapers.anguilla_scraper import get_result
        return get_result(draw, date)
    if lottery == "La Primera":
        from scrapers.laprimera_scraper import get_result
        return get_result(draw, date)
    if lottery == "La Nacional":
        from scrapers.lanacional_scraper import get_result
        return get_result(draw, date)
    raise ValueError("Lottery no soportada")

def try_update_one(item, state) -> bool:
    n = now_rd()
    date_str = n.strftime("%Y-%m-%d")
    due = draw_datetime_today(item["time"]) + timedelta(minutes=item["update_after_minutes"])

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

def next_upcoming_draw():
    n = now_rd()
    best = None
    for item in SCHEDULE:
        dt = draw_datetime_today(item["time"])
        if dt < n:
            continue
        if (dt - n).total_seconds() <= LOOKAHEAD_MINUTES * 60:
            if best is None or dt < best[0]:
                best = (dt, item)
    return best  # (datetime, item) o None

def run_analysis_and_maybe_alert(state):
    # Cargar data
    frames = []
    for lot, path in XLSX_FILES.items():
        df = read_history_xlsx(path)
        if not df.empty:
            frames.append(explode(df, lot))

    if not frames:
        print("[INFO] No history data loaded yet.")
        return

    exp = pd.concat(frames, ignore_index=True).sort_values("fecha_dt").reset_index(drop=True)

    nxt = next_upcoming_draw()
    if not nxt:
        print("[INFO] No upcoming draw in lookahead window.")
        return

    dt, target = nxt
    print("[INFO] Next target:", dt.strftime("%Y-%m-%d %H:%M"), target["lottery"], target["draw"])

    # Fuente = “todos los otros sorteos”
    src_filter = lambda e: ~((e["lottery"]==target["lottery"]) & (e["sorteo"]==target["draw"]))

    rec0 = recommend_for_target(exp, src_filter, target["lottery"], target["draw"], lag_days=0, top_n=12)
    rec1 = recommend_for_target(exp, src_filter, target["lottery"], target["draw"], lag_days=1, top_n=12)

    if rec0.empty:
        print("[INFO] Not enough data to compute recommendations for target.")
        return

    top_nums = rec0["num"].tolist()
    pales = top_pales(top_nums[:10], 20)

    # ✅ Guardar picks SIEMPRE (para que puedas revisar)
    picks_payload = {
        "generated_at": now_rd().isoformat(),
        "target": {
            "lottery": target["lottery"],
            "draw": target["draw"],
            "time_rd": dt.strftime("%Y-%m-%d %H:%M"),
        },
        "top_nums": top_nums[:12],
        "pales": pales,
        "debug": {
            "min_signal": MIN_SIGNAL,
            "min_a11": MIN_A11,
            "best_signal": float(rec0["signal"].max()) if "signal" in rec0.columns else None,
            "best_a11": int(rec0["a11"].max()) if "a11" in rec0.columns else None,
        }
    }

    ensure_dir(OUT_DIR)
    with open(os.path.join(OUT_DIR, "picks.json"), "w", encoding="utf-8") as f:
        json.dump(picks_payload, f, ensure_ascii=False, indent=2)

    print("[OK] Wrote outputs/picks.json")

    ok = should_alert(rec0, min_signal=MIN_SIGNAL, min_count_hits=MIN_A11)
    alert_key = f"{dt.strftime('%Y-%m-%d %H:%M')}|{target['lottery']}|{target['draw']}"

    if not ok:
        print("[INFO] No valid opportunity (thresholds not met).")
        return
    if state.get("last_alert_key") == alert_key:
        print("[INFO] Alert already sent for this target.")
        return

    msg = []
    msg.append("🚨 ALERTA OPV (Cross-Lottery)")
    msg.append(f"🎯 Próximo sorteo: {target['lottery']} | {target['draw']}")
    msg.append(f"⏰ Hora: {dt.strftime('%H:%M')} RD")
    msg.append("")
    msg.append("✅ Top números sugeridos:")
    msg.append(", ".join(top_nums[:12]))
    msg.append("")
    msg.append("🎲 Palés sugeridos:")
    msg.append(" | ".join([f"{a}-{b}" for a,b in pales[:10]]))
    msg.append("")
    if not rec1.empty:
        msg.append("📌 Next-day (lag 1) top 5:")
        msg.append(", ".join(rec1["num"].tolist()[:5]))

    send_telegram("\n".join(msg))
    state["last_alert_key"] = alert_key
    print("[OK] Telegram alert sent.")

def main():
    ensure_dir(DATA_DIR)
    ensure_dir(HIST_DIR)
    ensure_dir(OUT_DIR)

    state = load_state()

    # Updates
    for item in SCHEDULE:
        try:
            did = try_update_one(item, state)
            if did:
                print("[OK] Updated:", item["lottery"], item["draw"])
        except Exception as e:
            print(f"[WARN] update failed {item['lottery']}|{item['draw']}: {e}")

    # Analysis + possible alert
    try:
        run_analysis_and_maybe_alert(state)
    except Exception as e:
        print(f"[WARN] analysis/alert failed: {e}")

    save_state(state)
    print("[OK] runner finished")

if __name__ == "__main__":
    main()
