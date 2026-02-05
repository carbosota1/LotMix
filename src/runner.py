import os
import sys
import json
import hashlib
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))

from io_xlsx import ensure_dir, read_history_xlsx, upsert_history_xlsx, normalize_2d
from analyze import explode, recommend_for_target, should_alert, top_pales
from telegram import send_telegram

TZ = ZoneInfo("America/Santo_Domingo")

DATA_DIR = "data"
HIST_DIR = os.path.join(DATA_DIR, "histories")
STATE_PATH = os.path.join(DATA_DIR, "state.json")
OUT_DIR = "outputs"

XLSX_FILES = {
    "La Primera": os.path.join(HIST_DIR, "La Primera History.xlsx"),
    "Anguilla":   os.path.join(HIST_DIR, "Anguilla history.xlsx"),
    "La Nacional": os.path.join(HIST_DIR, "La nacional history.xlsx"),
}

SCHEDULE = [
    {"lottery": "Anguilla", "draw": "Anguila 10AM", "time": "10:00", "update_after_minutes": 30},
    {"lottery": "Anguilla", "draw": "Anguila 1PM",  "time": "13:00", "update_after_minutes": 30},
    {"lottery": "Anguilla", "draw": "Anguila 6PM",  "time": "18:00", "update_after_minutes": 30},
    {"lottery": "Anguilla", "draw": "Anguila 9PM",  "time": "21:00", "update_after_minutes": 30},

    {"lottery": "La Primera", "draw": "Quiniela La Primera",       "time": "12:00", "update_after_minutes": 30},
    {"lottery": "La Primera", "draw": "Quiniela La Primera Noche", "time": "20:00", "update_after_minutes": 30},

    {"lottery": "La Nacional", "draw": "Loteria Nacional- Gana Más", "time": "14:30", "update_after_minutes": 30},
    {"lottery": "La Nacional", "draw": "Loteria Nacional- Noche",    "time": "21:30", "update_after_minutes": 30},
]

TOPK_QUINIELA = 3
TOPK_FULL     = 12
PALES_OUT     = 3

MIN_SIGNAL = 0.010
MIN_A11    = 10

LOOKAHEAD_MINUTES = 16 * 60
FORCE_NOTIFY = os.getenv("FORCE_NOTIFY", "0").strip() == "1"


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

def _mk_key(date_str: str, lottery: str, draw: str, time_rd: str) -> str:
    return f"{date_str}|{lottery}|{draw}|{time_rd}"

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
    """
    ✅ Palés válidos: a != b (nunca '63-63')
    Salida: lista de strings "AA-BB"
    """
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
                continue  # ✅ evita palé repetido

            pair = _norm_pair(a, b)
            if pair in seen:
                continue
            seen.add(pair)
            out.append(pair)

        except Exception:
            continue

    return out

def fingerprint(top6, pales10):
    s = "|".join(top6) + "||" + "|".join(pales10)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]


def _fresh_state():
    return {"last_updates": {}, "last_event_key": "", "sent_by_target_fp": {}}

def load_state():
    if not os.path.exists(STATE_PATH):
        return _fresh_state()

    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            raw = f.read().strip()
            if not raw:
                raise ValueError("state.json vacío")
            state = json.loads(raw)
        if not isinstance(state, dict):
            raise ValueError("state.json no es dict")

    except Exception as e:
        try:
            ensure_dir(DATA_DIR)
            ts = now_rd().strftime("%Y%m%d-%H%M%S")
            bad_path = os.path.join(DATA_DIR, f"state.json.bad-{ts}")
            os.replace(STATE_PATH, bad_path)
            print(f"[WARN] state.json corrupto -> movido a {bad_path} ({e})")
        except Exception as e2:
            print(f"[WARN] state.json corrupto y no se pudo mover ({e2})")
        return _fresh_state()

    state.setdefault("last_updates", {})
    state.setdefault("last_event_key", "")
    state.setdefault("sent_by_target_fp", {})
    return state

def save_state(state):
    ensure_dir(DATA_DIR)
    tmp_path = STATE_PATH + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, STATE_PATH)


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
        if dt.date() != n.date():
            continue
        if dt < n:
            continue
        if (dt - n).total_seconds() > LOOKAHEAD_MINUTES * 60:
            continue
        if best is None or dt < best[0]:
            best = (dt, item)
    return best


def log_pick(payload: dict):
    _ensure_dir(DATA_DIR)
    log_path = os.path.join(DATA_DIR, "picks_log.csv")

    bp = payload.get("best_play", {})
    generated_at = payload.get("generated_at")
    date_str = (generated_at or "")[:10] if generated_at else today_str()

    time_rd = bp.get("time_rd", "")
    lottery = bp.get("lottery", "")
    draw = bp.get("draw", "")
    key = _mk_key(date_str, lottery, draw, time_rd)

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
        "top6": json.dumps(bp.get("top6", []), ensure_ascii=False),
        "pales10": json.dumps(bp.get("pales10", []), ensure_ascii=False),
        "graded": 0,
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

    def hits(nums_list, drawn_set):
        return len(set(nums_list).intersection(drawn_set))

    def pale_hits(pales_list, drawn_nums):
        drawn = sorted(list(drawn_nums))
        pairs = {f"{drawn[0]}-{drawn[1]}", f"{drawn[0]}-{drawn[2]}", f"{drawn[1]}-{drawn[2]}"}
        norm = set(format_pales(pales_list))
        return len(norm.intersection(pairs))

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
            continue

        row = match.iloc[-1]
        drawn = {row["primero"], row["segundo"], row["tercero"]}

        top12 = json.loads(r.get("top12", "[]") or "[]")
        top6 = json.loads(r.get("top6", "[]") or "[]")
        pales10 = json.loads(r.get("pales10", "[]") or "[]")

        perf_rows.append({
            "key": key,
            "date": date,
            "time_rd": time_rd,
            "lottery": lottery,
            "draw": draw,
            "result": f"{row['primero']}-{row['segundo']}-{row['tercero']}",
            "hits_quiniela_top6": hits(top6, drawn),
            "hits_quiniela_top12": hits(top12, drawn),
            "pale_hits_top10": pale_hits(pales10, drawn),
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


def combine_hist_and_intraday(rec_hist: pd.DataFrame, rec_today: pd.DataFrame):
    if rec_hist is None or rec_hist.empty:
        return rec_today
    if rec_today is None or rec_today.empty:
        out = rec_hist.copy()
        out["score_combo"] = out["score"]
        return out

    h = rec_hist[["num", "score", "signal", "a11"]].copy()
    t = rec_today[["num", "score", "signal", "a11"]].copy()

    h = h.rename(columns={"score": "score_hist", "signal": "signal_hist", "a11": "a11_hist"})
    t = t.rename(columns={"score": "score_today", "signal": "signal_today", "a11": "a11_today"})

    m = h.merge(t, on="num", how="outer").fillna(0)
    w_hist, w_today = 0.75, 0.25
    m["score_combo"] = w_hist * m["score_hist"] + w_today * m["score_today"]
    m["signal_combo"] = m["signal_hist"] + 0.50 * m["signal_today"]
    m["a11_combo"] = m["a11_hist"].astype(int) + m["a11_today"].astype(int)
    return m.sort_values("score_combo", ascending=False).reset_index(drop=True)


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

    target_dt_naive = target_dt.replace(tzinfo=None)

    src_filter_hist = lambda e: ~((e["lottery"] == target["lottery"]) & (e["sorteo"] == target["draw"]))
    rec_hist = recommend_for_target(exp, src_filter_hist, target["lottery"], target["draw"], lag_days=0, top_n=TOPK_FULL)
    if rec_hist is None or rec_hist.empty:
        print("[INFO] Not enough data to compute historical recommendations.")
        return

    today = target_dt_naive.date()
    exp_today = exp[(exp["fecha_dt"].dt.date == today) & (exp["fecha_dt"] < target_dt_naive)].copy()

    rec_today = None
    if not exp_today.empty:
        mask_idx = set(exp_today.index.tolist())
        src_filter_today = lambda e, _m=mask_idx: e.index.isin(_m)
        rec_today = recommend_for_target(exp, src_filter_today, target["lottery"], target["draw"], lag_days=0, top_n=TOPK_FULL)

    combo = combine_hist_and_intraday(rec_hist, rec_today)

    top12 = combo["num"].astype(str).tolist()[:TOPK_FULL]
    top6 = top12[:TOPK_QUINIELA]
    pales10 = format_pales(top_pales(top12[:10], 30))[:PALES_OUT]  # ✅ más candidatos y filtra inválidos

    ok_hist = should_alert(rec_hist, min_signal=MIN_SIGNAL, min_count_hits=MIN_A11)
    ok_today = False
    if rec_today is not None and not rec_today.empty:
        ok_today = should_alert(rec_today, min_signal=MIN_SIGNAL, min_count_hits=MIN_A11)
    ok = bool(ok_hist or ok_today)

    best_signal_hist = float(rec_hist["signal"].max()) if "signal" in rec_hist.columns else None
    best_a11_hist = int(rec_hist["a11"].max()) if "a11" in rec_hist.columns else None
    best_signal_today = float(rec_today["signal"].max()) if (rec_today is not None and "signal" in rec_today.columns and not rec_today.empty) else None
    best_a11_today = int(rec_today["a11"].max()) if (rec_today is not None and "a11" in rec_today.columns and not rec_today.empty) else None

    fp = fingerprint(top6, pales10)

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
            "top6": top6,
            "pales10": pales10,
            "fingerprint": fp,
            "ok_alert": ok,
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

    log_pick(payload)

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
    msg.append(f"✅ QUINIELA Top{TOPK_QUINIELA}:")
    msg.append(", ".join(top6))
    msg.append("")
    msg.append("🎲 PALE Top {len(pale10)}:")
    msg.append(" | ".join(pales10))
    msg.append("")
    msg.append("📊 Debug:")
    msg.append(f"hist best_signal={best_signal_hist} best_a11={best_a11_hist}")
    msg.append(f"today best_signal={best_signal_today} best_a11={best_a11_today}")

    send_telegram("\n".join(msg))
    print("[OK] Telegram sent.")

    sent_map[target_key] = fp
    state["sent_by_target_fp"] = sent_map


def main():
    ensure_dir(DATA_DIR)
    ensure_dir(HIST_DIR)
    ensure_dir(OUT_DIR)

    state = load_state()

    updated = []
    for item in SCHEDULE:
        try:
            if try_update_one(item, state):
                print("[OK] Updated:", item["lottery"], item["draw"])
                updated.append(item)
        except Exception as e:
            print(f"[WARN] update failed {item['lottery']}|{item['draw']}: {e}")

    try:
        grade_picks_from_histories()
        print("[OK] Grading pass completed.")
    except Exception as e:
        print(f"[WARN] grading failed: {e}")

    if not updated and (not FORCE_NOTIFY):
        print("[INFO] No new updates. Skipping intraday analysis/notify.")
        save_state(state)
        print("[OK] runner finished")
        return

    if not updated and FORCE_NOTIFY:
        print("[TEST] FORCE_NOTIFY=1 -> running intraday analysis even without new updates.")
        event_key = f"{today_str()}|TEST|NO-UPDATE"
        try:
            run_intraday_next_target(event_key, state)
        except Exception as e:
            print(f"[WARN] intraday analysis/notify failed: {e}")
        save_state(state)
        print("[OK] runner finished")
        return

    updated_sorted = sorted(updated, key=lambda x: draw_datetime_today(x["time"]))
    last_event = updated_sorted[-1]
    event_key = f"{today_str()}|{last_event['lottery']}|{last_event['draw']}"

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
