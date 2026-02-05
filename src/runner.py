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

# Tus XLSX históricos (deben existir en el repo)
XLSX_FILES = {
    "La Primera": os.path.join(HIST_DIR, "La Primera History.xlsx"),
    "Anguilla":   os.path.join(HIST_DIR, "Anguilla history.xlsx"),
    "La Nacional":os.path.join(HIST_DIR, "La nacional history.xlsx"),
}

# ✅ IMPORTANTE:
# draw == EXACTAMENTE el texto que aparece en la columna "sorteo" de tus historiales XLSX
# Esto garantiza que el análisis use DATA REAL del histórico que subiste.
SCHEDULE = [
    # Anguilla (4)
    {"lottery":"Anguilla", "draw":"Anguila 10AM", "time":"10:00", "update_after_minutes":30},
    {"lottery":"Anguilla", "draw":"Anguila 1PM",  "time":"13:00", "update_after_minutes":30},
    {"lottery":"Anguilla", "draw":"Anguila 6PM",  "time":"18:00", "update_after_minutes":30},
    {"lottery":"Anguilla", "draw":"Anguila 9PM",  "time":"21:00", "update_after_minutes":30},

    # La Primera (2)
    {"lottery":"La Primera", "draw":"Quiniela La Primera",       "time":"12:00", "update_after_minutes":30},
    {"lottery":"La Primera", "draw":"Quiniela La Primera Noche", "time":"20:00", "update_after_minutes":30},

    # La Nacional (2)
    {"lottery":"La Nacional", "draw":"Loteria Nacional- Gana Más", "time":"14:30", "update_after_minutes":30},
    {"lottery":"La Nacional", "draw":"Loteria Nacional- Noche",    "time":"20:30", "update_after_minutes":30},
]

# Umbrales para “oportunidad válida” (basado en MI y Chi²)
MIN_SIGNAL = 0.010
MIN_A11    = 10

# Ventana para buscar “próximo sorteo cercano”
LOOKAHEAD_MINUTES = 720  # 12h

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
# SCRAPER HOOKS (por archivo)
# -----------------------------
def fetch_result(lottery: str, draw: str, date: str):
    """
    Carga el scraper por ruta (NO depende de 'scrapers' como paquete).
    Devuelve (primero, segundo, tercero) como '00'..'99'
    """
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
        "sorteo": item["draw"],  # ✅ guarda el nombre REAL del sorteo (compatible con histórico)
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
    return best

def run_analysis_and_maybe_alert(state):
    # Cargar y explotar historiales (DATA REAL)
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

    # Fuente = todos los otros sorteos (cross-lottery)
    src_filter = lambda e: ~((e["lottery"] == target["lottery"]) & (e["sorteo"] == target["draw"]))

    # Same-day vs Next-day
    rec0 = recommend_for_target(exp, src_filter, target["lottery"], target["draw"], lag_days=0, top_n=12)
    rec1 = recommend_for_target(exp, src_filter, target["lottery"], target["draw"], lag_days=1, top_n=12)

    if rec0.empty:
        print("[INFO] Not enough data to compute recommendations for target.")
        return

    top_nums = rec0["num"].tolist()
    pales = top_pales(top_nums[:10], 20)

    # ✅ Guardar picks SIEMPRE (para revisar)
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
            "lag1_top5": rec1["num"].tolist()[:5] if not rec1.empty else [],
        }
    }

    ensure_dir(OUT_DIR)
    with open(os.path.join(OUT_DIR, "picks.json"), "w", encoding="utf-8") as f:
        json.dump(picks_payload, f, ensure_ascii=False, indent=2)

    print("[OK] Wrote outputs/picks.json")

    # Solo alerta si la señal supera umbrales (DATA REAL, no adivinanza)
    ok = should_alert(rec0, min_signal=MIN_SIGNAL, min_count_hits=MIN_A11)
    alert_key = f"{dt.strftime('%Y-%m-%d %H:%M')}|{target['lottery']}|{target['draw']}"

    if not ok:
        print("[INFO] No valid opportunity (thresholds not met).")
        return
    if state.get("last_alert_key") == alert_key:
        print("[INFO] Alert already sent for this target.")
        return

    msg = []
    msg.append("🚨 ALERTA OPV (Data Real / Cross-Lottery)")
    msg.append(f"🎯 Próximo sorteo: {target['lottery']} | {target['draw']}")
    msg.append(f"⏰ Hora: {dt.strftime('%H:%M')} RD")
    msg.append("")
    msg.append("✅ Top números sugeridos (MI + Chi²):")
    msg.append(", ".join(top_nums[:12]))
    msg.append("")
    msg.append("🎲 Palés sugeridos:")
    msg.append(" | ".join([f"{a}-{b}" for a, b in pales[:10]]))

    if not rec1.empty:
        msg.append("")
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

    # Updates (30 min después de cada sorteo)
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
