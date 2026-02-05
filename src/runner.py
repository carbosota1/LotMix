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
    "La Nacional": os.path.join(HIST_DIR, "La nacional history.xlsx"),
}

# ✅ draw == EXACTAMENTE lo que tienes en la columna "sorteo" de tus historiales XLSX
# (Esto es lo que evita problemas con historial)
SCHEDULE = [
    # Anguilla (4)
    {"lottery": "Anguilla", "draw": "Anguila 10AM", "time": "10:00", "update_after_minutes": 30},
    {"lottery": "Anguilla", "draw": "Anguila 1PM",  "time": "13:00", "update_after_minutes": 30},
    {"lottery": "Anguilla", "draw": "Anguila 6PM",  "time": "18:00", "update_after_minutes": 30},
    {"lottery": "Anguilla", "draw": "Anguila 9PM",  "time": "21:00", "update_after_minutes": 30},

    # La Primera (2)
    {"lottery": "La Primera", "draw": "Quiniela La Primera",       "time": "12:00", "update_after_minutes": 30},
    {"lottery": "La Primera", "draw": "Quiniela La Primera Noche", "time": "20:00", "update_after_minutes": 30},

    # La Nacional (2)
    {"lottery": "La Nacional", "draw": "Loteria Nacional- Gana Más", "time": "14:30", "update_after_minutes": 30},
    {"lottery": "La Nacional", "draw": "Loteria Nacional- Noche",    "time": "20:30", "update_after_minutes": 30},
]

# 🚨 UMBRALES para ALERTA premium (edge real)
MIN_SIGNAL = 0.010
MIN_A11    = 10

# Ventana para evaluar sorteos “cercanos”
LOOKAHEAD_MINUTES = 16 * 60  # 16h

def now_rd() -> datetime:
    return datetime.now(TZ)

def load_state():
    """
    last_updates: marca updates hechos hoy por sorteo
    sent_info: evita spam de INFO por "target actual líder"
    sent_alert: evita spam de ALERTA por target
    """
    if not os.path.exists(STATE_PATH):
        return {"last_updates": {}, "sent_info": {}, "sent_alert": {}}

    with open(STATE_PATH, "r", encoding="utf-8") as f:
        state = json.load(f)

    state.setdefault("last_updates", {})
    state.setdefault("sent_info", {})
    state.setdefault("sent_alert", {})
    return state

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
    """
    Actualiza el historial SOLO cuando ya pasó (hora del sorteo + 30 min).
    Esto garantiza resultados disponibles.
    """
    n = now_rd()
    date_str = n.strftime("%Y-%m-%d")
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
        "sorteo": item["draw"],  # ✅ igual al histórico
        "primero": p1,
        "segundo": p2,
        "tercero": p3,
    }])

    ensure_dir(HIST_DIR)
    upsert_history_xlsx(XLSX_FILES[item["lottery"]], new_row)

    last_updates[key] = "done"
    state["last_updates"] = last_updates
    return True

def upcoming_draws_today():
    """
    Devuelve TODOS los sorteos restantes del día (hoy) dentro del lookahead.
    (No solo uno). Esto habilita la “precisión quirúrgica”.
    """
    n = now_rd()
    out = []
    for item in SCHEDULE:
        dt = draw_datetime_today(item["time"])
        if dt.date() != n.date():
            continue
        if dt < n:
            continue
        if (dt - n).total_seconds() > LOOKAHEAD_MINUTES * 60:
            continue
        out.append((dt, item))
    return sorted(out, key=lambda x: x[0])

def build_exploded_history():
    frames = []
    for lot, path in XLSX_FILES.items():
        df = read_history_xlsx(path)
        if not df.empty:
            frames.append(explode(df, lot))

    if not frames:
        return None

    exp = pd.concat(frames, ignore_index=True).sort_values("fecha_dt").reset_index(drop=True)
    return exp

def evaluate_targets(exp: pd.DataFrame, targets):
    """
    Evalúa cada target con data real (cross-lottery).
    Devuelve lista de candidatos rankeados por score.
    """
    candidates = []

    for dt, target in targets:
        # Fuente = todos los otros sorteos
        src_filter = lambda e, t=target: ~((e["lottery"] == t["lottery"]) & (e["sorteo"] == t["draw"]))

        rec0 = recommend_for_target(exp, src_filter, target["lottery"], target["draw"], lag_days=0, top_n=12)
        rec1 = recommend_for_target(exp, src_filter, target["lottery"], target["draw"], lag_days=1, top_n=12)

        if rec0.empty:
            continue

        top_nums = rec0["num"].tolist()
        pales = top_pales(top_nums[:10], 20)

        best_signal = float(rec0["signal"].max()) if "signal" in rec0.columns else None
        best_a11 = int(rec0["a11"].max()) if "a11" in rec0.columns else None

        # criterio de alerta premium (edge real)
        ok_alert = should_alert(rec0, min_signal=MIN_SIGNAL, min_count_hits=MIN_A11)

        # score principal para ranking (usa el score ya calculado en analyze.py)
        best_score = float(rec0["score"].max()) if "score" in rec0.columns else (best_signal or 0.0)

        candidates.append({
            "dt": dt,
            "target": target,
            "top_nums": top_nums[:12],
            "pales": pales,
            "best_signal": best_signal,
            "best_a11": best_a11,
            "best_score": best_score,
            "lag1_top5": rec1["num"].tolist()[:5] if not rec1.empty else [],
            "ok_alert": bool(ok_alert),
        })

    candidates.sort(key=lambda x: x["best_score"], reverse=True)
    return candidates

def write_picks_json(payload: dict):
    ensure_dir(OUT_DIR)
    with open(os.path.join(OUT_DIR, "picks.json"), "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

def send_info_once(state, info_key: str, msg: str):
    if state["sent_info"].get(info_key) == "done":
        print("[INFO] INFO already sent for this key.")
        return
    send_telegram(msg)
    state["sent_info"][info_key] = "done"
    print("[OK] Telegram INFO sent.")

def send_alert_once(state, alert_key: str, msg: str):
    if state["sent_alert"].get(alert_key) == "done":
        print("[INFO] ALERT already sent for this target.")
        return
    send_telegram(msg)
    state["sent_alert"][alert_key] = "done"
    print("[OK] Telegram ALERT sent.")

def run_analysis_and_notify(state):
    exp = build_exploded_history()
    if exp is None:
        print("[INFO] No history data loaded yet.")
        return

    targets = upcoming_draws_today()
    if not targets:
        print("[INFO] No upcoming draws today in lookahead window.")
        return

    print(f"[INFO] Upcoming targets today: {len(targets)}")

    candidates = evaluate_targets(exp, targets)
    if not candidates:
        print("[INFO] Not enough data to compute recommendations for today’s targets.")
        return

    # Mejor oportunidad (precisión quirúrgica)
    best = candidates[0]

    # Guardar picks.json con TODAS las oportunidades evaluadas
    payload = {
        "generated_at": now_rd().isoformat(),
        "min_signal": MIN_SIGNAL,
        "min_a11": MIN_A11,
        "candidates_ranked": [
            {
                "time_rd": c["dt"].strftime("%Y-%m-%d %H:%M"),
                "lottery": c["target"]["lottery"],
                "draw": c["target"]["draw"],
                "best_score": c["best_score"],
                "best_signal": c["best_signal"],
                "best_a11": c["best_a11"],
                "ok_alert": c["ok_alert"],
                "top_nums": c["top_nums"],
                "pales": c["pales"][:10],
                "lag1_top5": c["lag1_top5"],
            }
            for c in candidates
        ],
        "best_play": {
            "time_rd": best["dt"].strftime("%Y-%m-%d %H:%M"),
            "lottery": best["target"]["lottery"],
            "draw": best["target"]["draw"],
            "top_nums": best["top_nums"],
            "pales": best["pales"][:10],
            "best_signal": best["best_signal"],
            "best_a11": best["best_a11"],
            "best_score": best["best_score"],
            "ok_alert": best["ok_alert"],
            "lag1_top5": best["lag1_top5"],
        }
    }

    write_picks_json(payload)
    print("[OK] Wrote outputs/picks.json")

    # -----------------------------
    # ℹ️ INFO: ranking del día + el #1 (una vez por “líder actual”)
    # -----------------------------
    leader_key = f"{best['dt'].strftime('%Y-%m-%d %H:%M')}|{best['target']['lottery']}|{best['target']['draw']}"
    try:
        lines = []
        lines.append("ℹ️ INFO PICKS (Precisión Quirúrgica / Data Real)")
        lines.append("Ranking de oportunidades (Top 3):")

        for i, c in enumerate(candidates[:3], start=1):
            lines.append(
                f"{i}) {c['dt'].strftime('%H:%M')} {c['target']['lottery']} | {c['target']['draw']} "
                f"(signal={c['best_signal']:.6f} a11={c['best_a11']} score={c['best_score']:.6f})"
                if c["best_signal"] is not None and c["best_a11"] is not None
                else f"{i}) {c['dt'].strftime('%H:%M')} {c['target']['lottery']} | {c['target']['draw']}"
            )

        lines.append("")
        lines.append("🎯 MEJOR JUGADA AHORA:")
        lines.append(f"Target: {best['target']['lottery']} | {best['target']['draw']}")
        lines.append(f"Hora: {best['dt'].strftime('%H:%M')} RD")
        lines.append("")
        lines.append("Top 12 (MI + Chi²):")
        lines.append(", ".join(best["top_nums"]))
        lines.append("")
        lines.append("Top Palés (10):")
        lines.append(" | ".join([f"{a}-{b}" for a, b in best["pales"][:10]]))
        lines.append("")
        lines.append(f"best_signal: {best['best_signal']}")
        lines.append(f"best_a11: {best['best_a11']}")

        send_info_once(state, leader_key, "\n".join(lines))
    except Exception as e:
        print(f"[WARN] Telegram INFO failed: {e}")

    # -----------------------------
    # 🚨 ALERTA: por CADA target con edge premium (una vez por target)
    # -----------------------------
    for c in candidates:
        if not c["ok_alert"]:
            continue

        alert_key = f"{c['dt'].strftime('%Y-%m-%d %H:%M')}|{c['target']['lottery']}|{c['target']['draw']}"
        try:
            msg = []
            msg.append("🚨 ALERTA OPV (EDGE REAL / Data Real)")
            msg.append(f"🎯 Target: {c['target']['lottery']} | {c['target']['draw']}")
            msg.append(f"⏰ Hora: {c['dt'].strftime('%H:%M')} RD")
            msg.append("")
            msg.append("✅ Top 12:")
            msg.append(", ".join(c["top_nums"]))
            msg.append("")
            msg.append("🎲 Palés (10):")
            msg.append(" | ".join([f"{a}-{b}" for a, b in c["pales"][:10]]))
            if c["lag1_top5"]:
                msg.append("")
                msg.append("📌 Next-day (lag 1) top 5:")
                msg.append(", ".join(c["lag1_top5"]))

            send_alert_once(state, alert_key, "\n".join(msg))
        except Exception as e:
            print(f"[WARN] Telegram ALERT failed for {alert_key}: {e}")

def main():
    ensure_dir(DATA_DIR)
    ensure_dir(HIST_DIR)
    ensure_dir(OUT_DIR)

    state = load_state()

    # 1) Updates: intenta actualizar TODO lo que ya esté “vencido” (+30 min)
    for item in SCHEDULE:
        try:
            did = try_update_one(item, state)
            if did:
                print("[OK] Updated:", item["lottery"], item["draw"])
        except Exception as e:
            # Normal si aún no publican resultados
            print(f"[WARN] update failed {item['lottery']}|{item['draw']}: {e}")

    # 2) Analysis + notify: evalúa todas las oportunidades restantes del día
    try:
        run_analysis_and_notify(state)
    except Exception as e:
        print(f"[WARN] analysis/notify failed: {e}")

    save_state(state)
    print("[OK] runner finished")

if __name__ == "__main__":
    main()
