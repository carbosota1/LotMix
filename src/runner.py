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

# 🎯 Precisión quirúrgica: jugable real
TOPK_QUINIELA = 6          # SOLO Top6 para quiniela
TOPK_FULL     = 12         # el modelo calcula Top12 para ranking/diagnóstico
PALES_OUT     = 10         # SOLO 10 palés (no dispersión)

# 🚨 UMBRALES para ALERTA premium (edge real)
MIN_SIGNAL = 0.010
MIN_A11    = 10

# Para no “all over the place”: máximo alertas por corrida
MAX_ALERTS_PER_RUN = 2

# Ventana para evaluar sorteos “cercanos”
LOOKAHEAD_MINUTES = 16 * 60  # 16h


# -----------------------------
# Utils / State
# -----------------------------
def now_rd() -> datetime:
    return datetime.now(TZ)

def draw_datetime_today(time_hhmm: str) -> datetime:
    h, m = map(int, time_hhmm.split(":"))
    n = now_rd()
    return n.replace(hour=h, minute=m, second=0, microsecond=0)

def load_state():
    """
    last_updates: marca updates hechos hoy por sorteo
    sent_info: evita spam del INFO por líder
    sent_alert: evita spam de alerta por target
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


# -----------------------------
# Scraper hooks (load by file path)
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


# -----------------------------
# Targets selection (ALL remaining today)
# -----------------------------
def upcoming_draws_today():
    """
    Devuelve TODOS los sorteos restantes del día dentro del lookahead.
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


# -----------------------------
# Tracking (picks_log + performance)
# -----------------------------
def _ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def _mk_key(date_str: str, lottery: str, draw: str, time_rd: str) -> str:
    return f"{date_str}|{lottery}|{draw}|{time_rd}"

def log_candidates(payload: dict):
    """
    Guarda candidates del picks.json en data/picks_log.csv para poder evaluarlos luego.
    """
    _ensure_dir(DATA_DIR)
    log_path = os.path.join(DATA_DIR, "picks_log.csv")

    generated_at = payload.get("generated_at")
    date_str = (generated_at or "")[:10] if generated_at else now_rd().strftime("%Y-%m-%d")

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
            "top6": json.dumps(c.get("top6", []), ensure_ascii=False),
            "pales10": json.dumps(c.get("pales", []), ensure_ascii=False),
            "graded": 0,
        })

    if not rows:
        return

    new_df = pd.DataFrame(rows)

    if os.path.exists(log_path):
        old = pd.read_csv(log_path, dtype=str)
        merged = old.merge(new_df, on="key", how="outer", suffixes=("_old", ""))

        out = pd.DataFrame()
        out["key"] = merged["key"]
        for col in ["date", "time_rd", "lottery", "draw", "generated_at",
                    "best_score", "best_signal", "best_a11", "ok_alert",
                    "top12", "top6", "pales10"]:
            out[col] = merged[col].fillna(merged.get(f"{col}_old"))

        out["graded"] = merged["graded"].fillna(merged.get("graded_old")).fillna("0")
        out.to_csv(log_path, index=False, encoding="utf-8")
    else:
        new_df.to_csv(log_path, index=False, encoding="utf-8")

def grade_picks_from_histories():
    """
    Revisa data/picks_log.csv, busca el resultado real en los historiales,
    y calcula hits (quiniela Top6/Top12) + palé (Top10).
    Guarda en outputs/performance.csv
    """
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
        for col in ["fecha", "sorteo", "primero", "segundo", "tercero"]:
            if col not in hx.columns:
                hist_cache[lottery] = pd.DataFrame()
                return hist_cache[lottery]

        hx["fecha"] = hx["fecha"].astype(str).str.slice(0, 10)
        hx["sorteo"] = hx["sorteo"].astype(str)

        for col in ["primero", "segundo", "tercero"]:
            hx[col] = hx[col].astype(str).str.extract(r"(\d{1,2})")[0].fillna("").str.zfill(2)

        hist_cache[lottery] = hx
        return hx

    def hits(nums_list, drawn_set, k):
        return len(set(nums_list[:k]).intersection(drawn_set))

    def pale_hits(pales, drawn_nums):
        drawn = sorted(list(drawn_nums))
        pairs = {f"{drawn[0]}-{drawn[1]}", f"{drawn[0]}-{drawn[2]}", f"{drawn[1]}-{drawn[2]}"}

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

    perf_rows = []
    any_graded = False

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
            continue  # todavía no hay resultado en el historial

        row = match.iloc[-1]
        drawn = {row["primero"], row["segundo"], row["tercero"]}

        top12 = json.loads(r.get("top12", "[]") or "[]")
        top6 = json.loads(r.get("top6", "[]") or "[]")
        pales10 = json.loads(r.get("pales10", "[]") or "[]")

        # Quiniela hits
        h6 = hits(top6, drawn, len(top6))
        h12 = hits(top12, drawn, 12)

        # Palé hits (cuántos palés pegan algún par real)
        ph = pale_hits(pales10, drawn)

        perf_rows.append({
            "key": key,
            "date": date,
            "time_rd": time_rd,
            "lottery": lottery,
            "draw": draw,
            "result": f"{row['primero']}-{row['segundo']}-{row['tercero']}",
            "hits_quiniela_top6": h6,
            "hits_quiniela_top12": h12,
            "pale_hits_top10": ph,
            "best_signal": r.get("best_signal"),
            "best_a11": r.get("best_a11"),
            "ok_alert": r.get("ok_alert"),
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
# Analysis
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
    return exp

def evaluate_targets(exp: pd.DataFrame, targets):
    candidates = []

    for dt, target in targets:
        src_filter = lambda e, t=target: ~((e["lottery"] == t["lottery"]) & (e["sorteo"] == t["draw"]))

        rec0 = recommend_for_target(exp, src_filter, target["lottery"], target["draw"], lag_days=0, top_n=TOPK_FULL)
        rec1 = recommend_for_target(exp, src_filter, target["lottery"], target["draw"], lag_days=1, top_n=TOPK_FULL)

        if rec0.empty:
            continue

        top12 = rec0["num"].tolist()[:TOPK_FULL]
        top6 = top12[:TOPK_QUINIELA]

        # palés basados en top12, pero solo entregamos 10
        pales = top_pales(top12[:10], 20)[:PALES_OUT]

        best_signal = float(rec0["signal"].max()) if "signal" in rec0.columns else None
        best_a11 = int(rec0["a11"].max()) if "a11" in rec0.columns else None

        ok_alert = should_alert(rec0, min_signal=MIN_SIGNAL, min_count_hits=MIN_A11)

        best_score = float(rec0["score"].max()) if "score" in rec0.columns else (best_signal or 0.0)

        candidates.append({
            "dt": dt,
            "target": target,
            "top12": top12,
            "top6": top6,
            "pales10": pales,
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

    candidates = evaluate_targets(exp, targets)
    if not candidates:
        print("[INFO] Not enough data to compute recommendations for today’s targets.")
        return

    best = candidates[0]

    payload = {
        "generated_at": now_rd().isoformat(),
        "min_signal": MIN_SIGNAL,
        "min_a11": MIN_A11,
        "strategy": {
            "quiniela_topk": TOPK_QUINIELA,
            "pales_out": PALES_OUT,
            "max_alerts_per_run": MAX_ALERTS_PER_RUN,
        },
        "candidates_ranked": [
            {
                "time_rd": c["dt"].strftime("%Y-%m-%d %H:%M"),
                "lottery": c["target"]["lottery"],
                "draw": c["target"]["draw"],
                "best_score": c["best_score"],
                "best_signal": c["best_signal"],
                "best_a11": c["best_a11"],
                "ok_alert": c["ok_alert"],
                "top_nums": c["top12"],     # top12 para auditoría
                "top6": c["top6"],          # top6 jugable
                "pales": c["pales10"],      # top10 palés jugable
                "lag1_top5": c["lag1_top5"],
            }
            for c in candidates
        ],
        "best_play": {
            "time_rd": best["dt"].strftime("%Y-%m-%d %H:%M"),
            "lottery": best["target"]["lottery"],
            "draw": best["target"]["draw"],
            "top12": best["top12"],
            "top6": best["top6"],
            "pales10": best["pales10"],
            "best_signal": best["best_signal"],
            "best_a11": best["best_a11"],
            "best_score": best["best_score"],
            "ok_alert": best["ok_alert"],
            "lag1_top5": best["lag1_top5"],
        }
    }

    write_picks_json(payload)
    print("[OK] Wrote outputs/picks.json")

    # ✅ Log picks para grading posterior
    log_candidates(payload)

    # -----------------------------
    # ℹ️ INFO: SOLO la mejor jugada (Top6 + Top10 palés)
    # + ranking Top3 para contexto (no para jugar todo)
    # -----------------------------
    leader_key = f"{best['dt'].strftime('%Y-%m-%d %H:%M')}|{best['target']['lottery']}|{best['target']['draw']}"
    try:
        lines = []
        lines.append("ℹ️ INFO PICKS (Precisión Quirúrgica / Data Real)")
        lines.append("Ranking (Top 3) - contexto:")
        for i, c in enumerate(candidates[:3], start=1):
            if c["best_signal"] is not None and c["best_a11"] is not None:
                lines.append(
                    f"{i}) {c['dt'].strftime('%H:%M')} {c['target']['lottery']} | {c['target']['draw']} "
                    f"(signal={c['best_signal']:.6f} a11={c['best_a11']} score={c['best_score']:.6f})"
                )
            else:
                lines.append(f"{i}) {c['dt'].strftime('%H:%M')} {c['target']['lottery']} | {c['target']['draw']}")

        lines.append("")
        lines.append("🎯 JUGADA PRINCIPAL (JUGAR ESTA):")
        lines.append(f"Target: {best['target']['lottery']} | {best['target']['draw']}")
        lines.append(f"Hora: {best['dt'].strftime('%H:%M')} RD")
        lines.append("")
        lines.append(f"✅ QUINIELA Top{TOPK_QUINIELA}:")
        lines.append(", ".join(best["top6"]))
        lines.append("")
        lines.append("🎲 PALE Top 10:")
        lines.append(" | ".join(best["pales10"]))
        lines.append("")
        lines.append(f"best_signal: {best['best_signal']}")
        lines.append(f"best_a11: {best['best_a11']}")

        send_info_once(state, leader_key, "\n".join(lines))
    except Exception as e:
        print(f"[WARN] Telegram INFO failed: {e}")

    # -----------------------------
    # 🚨 ALERTAS: máximo 2 por corrida (no dispersión)
    # Solo las mejores por score que cumplen umbrales
    # -----------------------------
    alert_candidates = [c for c in candidates if c["ok_alert"]]
    alert_candidates.sort(key=lambda x: x["best_score"], reverse=True)
    alert_candidates = alert_candidates[:MAX_ALERTS_PER_RUN]

    for c in alert_candidates:
        alert_key = f"{c['dt'].strftime('%Y-%m-%d %H:%M')}|{c['target']['lottery']}|{c['target']['draw']}"
        try:
            msg = []
            msg.append("🚨 ALERTA OPV (EDGE REAL / Data Real)")
            msg.append(f"🎯 Target: {c['target']['lottery']} | {c['target']['draw']}")
            msg.append(f"⏰ Hora: {c['dt'].strftime('%H:%M')} RD")
            msg.append("")
            msg.append(f"✅ QUINIELA Top{TOPK_QUINIELA}:")
            msg.append(", ".join(c["top6"]))
            msg.append("")
            msg.append("🎲 PALE Top 10:")
            msg.append(" | ".join(c["pales10"]))
            if c["lag1_top5"]:
                msg.append("")
                msg.append("📌 Next-day (lag 1) top 5:")
                msg.append(", ".join(c["lag1_top5"]))

            send_alert_once(state, alert_key, "\n".join(msg))
        except Exception as e:
            print(f"[WARN] Telegram ALERT failed for {alert_key}: {e}")


# -----------------------------
# Main
# -----------------------------
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
            print(f"[WARN] update failed {item['lottery']}|{item['draw']}: {e}")

    # 2) Grading: si ya existen resultados reales para picks pasados, calcula hits
    try:
        grade_picks_from_histories()
        print("[OK] Grading pass completed.")
    except Exception as e:
        print(f"[WARN] grading failed: {e}")

    # 3) Analysis + notify: evalúa todas las oportunidades restantes del día
    try:
        run_analysis_and_notify(state)
    except Exception as e:
        print(f"[WARN] analysis/notify failed: {e}")

    save_state(state)
    print("[OK] runner finished")

if __name__ == "__main__":
    main()