#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════╗
║         LOTMIX CALIENTE — lotmix_caliente.py  v3.0                 ║
║         Sistema de Aprendizaje Dinámico por Draw                   ║
╚══════════════════════════════════════════════════════════════════════╝

Cada corrida:
  1. Lee picks_log.csv + performance.csv (local o desde Gitea)
  2. Calcula percentiles de señal POR DRAW (no umbrales fijos globales)
  3. Calcula hot_pos contextual: draw + cuartil de señal + grupo a11
  4. Genera caliente.json con los números más probables

Mejora validada vs hot_pos genérico:
  Anguila 9PM       +27.8%  (55.6% → 83.3% de hits en top4)
  Anguila 1PM       +15.4%
  Anguila 6PM       +16.7%
  Quiniela La Primera +15.4%
  Loteria Nacional- Gana Más +15.4%

MODOS DE USO:
  # CLI
  python3 lotmix_caliente.py --input picks_activos.json [--output caliente.json]
        [--picks-log data/picks_log.csv] [--perf-log outputs/performance.csv]

  # Desde runner.py
  from lotmix_caliente import run
  resultado = run(picks=lista, output_path="outputs/caliente.json")
"""

import json
import argparse
import sys
import os
import ast
import csv
import io
import base64
import urllib.request
from datetime import datetime
from collections import Counter
from pathlib import Path

# ── CONFIGURACIÓN GITEA ───────────────────────────────────────────────────────
GITEA_BASE      = "https://gitea.totipicks.com"
GITEA_REPO      = "edgar26/LotMix"
GITEA_TOKEN     = os.environ.get("GITEA_TOKEN", "")
PICKS_LOG_RPATH = "data/picks_log.csv"
PERF_LOG_RPATH  = "outputs/performance.csv"

# ── FALLBACK ESTÁTICO (última defensa si no hay CSVs ni Gitea) ───────────────
# Calibrado Feb–Jun 2026 · 911 sorteos con resultado
DRAW_PROFILE_FALLBACK = {
    "Anguila 9PM":                {"hit_rate": 17.3, "hot_pos": [11,3,4,1,8,10,6,7,12,2,5,9]},
    "Anguila 1PM":                {"hit_rate": 15.9, "hot_pos": [11,12,7,1,9,4,8,2,3,5,6,10]},
    "Quiniela La Primera":        {"hit_rate": 15.1, "hot_pos": [3,1,11,2,9,4,6,5,8,7,10,12]},
    "Loteria Nacional- Gana Más": {"hit_rate": 12.0, "hot_pos": [4,5,1,11,12,6,8,9,2,3,7,10]},
    "Anguila 6PM":                {"hit_rate": 11.9, "hot_pos": [10,2,9,6,7,12,5,1,8,3,4,11]},
    "Quiniela La Primera Noche":  {"hit_rate": 11.5, "hot_pos": [6,9,11,2,1,8,12,3,4,5,7,10]},
    "Loteria Nacional- Noche":    {"hit_rate": 11.3, "hot_pos": [3,12,6,10,2,8,5,11,1,4,7,9]},
    "Quiniela La Suerte":         {"hit_rate": 10.9, "hot_pos": [1,3,11,10,4,2,5,6,7,8,9,12]},
    "Quiniela La Suerte 6PM":     {"hit_rate": 10.1, "hot_pos": [7,3,10,6,9,12,1,2,4,5,8,11]},
    "Anguila 10AM":               {"hit_rate":  8.1, "hot_pos": [7,12,5,10,8,1,2,3,4,6,9,11]},
}

# Mínimo de sorteos para confiar en un perfil contextual (draw+cuartil+a11)
MIN_CONTEXTUAL = 10
# Mínimo para perfil general del draw
MIN_DRAW       = 20


# ═════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═════════════════════════════════════════════════════════════════════════════

def _parse_list(val):
    if isinstance(val, list):
        return val
    if not val:
        return []
    try:
        r = json.loads(val)
        if isinstance(r, list): return r
    except Exception:
        pass
    try:
        r = ast.literal_eval(val)
        if isinstance(r, list): return r
    except Exception:
        pass
    return []


def _read_csv_string(content: str) -> list:
    return list(csv.DictReader(io.StringIO(content)))


def _read_csv_file(path: str) -> list:
    with open(path, encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _a11_group(v) -> str:
    try:
        v = int(float(v))
    except (TypeError, ValueError):
        return "unknown"
    if v < 3:  return "bajo"
    if v <= 5: return "medio"
    return "alto"


# ═════════════════════════════════════════════════════════════════════════════
# CARGA DE DATOS HISTÓRICOS
# ═════════════════════════════════════════════════════════════════════════════

def _fetch_gitea_csv(path: str) -> str | None:
    """Descarga CSV desde Gitea vía API. Requiere GITEA_TOKEN."""
    if not GITEA_TOKEN:
        return None
    try:
        url = f"{GITEA_BASE}/api/v1/repos/{GITEA_REPO}/contents/{path}"
        req = urllib.request.Request(
            url, headers={"Authorization": f"token {GITEA_TOKEN}"}
        )
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read())
        return base64.b64decode(data["content"]).decode("utf-8")
    except Exception as e:
        print(f"[Lotmix Caliente] ⚠️  Gitea {path}: {e}", file=sys.stderr)
        return None


def cargar_historico(picks_log_path=None, perf_log_path=None,
                     base_dir: Path = None):
    """
    Carga picks_log y performance en este orden de prioridad:
      1. Ruta explícita pasada como argumento
      2. Ruta relativa al script (../data y ../outputs)
      3. Gitea remoto
    Retorna (picks_rows, perf_rows) o (None, None) si todo falla.
    """
    if base_dir is None:
        base_dir = Path(__file__).parent

    def _try_load(explicit_path, relative_path):
        # 1. Ruta explícita
        if explicit_path and Path(explicit_path).exists():
            rows = _read_csv_file(explicit_path)
            print(f"[Lotmix Caliente] 📂 {Path(explicit_path).name}: {len(rows)} filas (local)")
            return rows
        # 2. Relativa al script
        auto_path = (base_dir / relative_path).resolve()
        if auto_path.exists():
            rows = _read_csv_file(str(auto_path))
            print(f"[Lotmix Caliente] 📂 {auto_path.name}: {len(rows)} filas (auto)")
            return rows
        # 3. Gitea
        gitea_path = PICKS_LOG_RPATH if "picks" in relative_path else PERF_LOG_RPATH
        content = _fetch_gitea_csv(gitea_path)
        if content:
            rows = _read_csv_string(content)
            print(f"[Lotmix Caliente] 🌐 {gitea_path}: {len(rows)} filas (Gitea)")
            return rows
        return None

    picks_rows = _try_load(picks_log_path, "../data/picks_log.csv")
    perf_rows  = _try_load(perf_log_path,  "../outputs/performance.csv")
    return picks_rows, perf_rows


# ═════════════════════════════════════════════════════════════════════════════
# APRENDIZAJE DINÁMICO
# ═════════════════════════════════════════════════════════════════════════════

def aprender_perfiles(picks_rows: list, perf_rows: list) -> dict:
    """
    Cruza picks_log + performance y construye perfiles por draw con:

    Nivel 1 — Perfil general del draw:
      · hit_rate global
      · hot_pos ordenado por frecuencia de acierto
      · percentiles de señal (p25, p50, p75) calculados desde los datos reales

    Nivel 2 — Perfil contextual draw + cuartil_señal + grupo_a11:
      · hot_pos específico para esa combinación
      · solo se usa si n >= MIN_CONTEXTUAL
      · validado: mejora entre +15% y +28% vs hot_pos genérico

    Retorna:
      {
        "Anguila 9PM": {
          "hit_rate": 17.3,
          "hot_pos": [...],        ← general
          "p25": 0.011, "p50": 0.018, "p75": 0.032,
          "n": 104,
          "contextual": {
            "muy_alta|medio": {"hot_pos": [...], "n": 21, "hit_rate": 23.8},
            ...
          }
        },
        ...
      }
    """
    # Indexar picks por key
    picks_by_key = {r["key"]: r for r in picks_rows}

    # Construir filas enriquecidas
    draw_rows = {}
    for row in perf_rows:
        result = str(row.get("result", "")).strip()
        if not result:
            continue

        draw = row.get("draw", "").strip()
        if not draw:
            continue

        key   = row.get("key", "")
        pick  = picks_by_key.get(key, {})
        top12 = _parse_list(pick.get("top12", []))

        try:
            signal = float(row.get("best_signal", 0) or 0)
        except (TypeError, ValueError):
            signal = 0.0

        a11_g  = _a11_group(row.get("best_a11", 0))
        result_num = result.split("-")[0].strip().zfill(2)

        hit_pos = None
        if result_num in top12:
            hit_pos = top12.index(result_num) + 1

        draw_rows.setdefault(draw, []).append({
            "signal":  signal,
            "a11_g":   a11_g,
            "hit_pos": hit_pos,
        })

    perfiles = {}
    n_auto = 0
    n_fallback = 0

    for draw, rows in draw_rows.items():
        n = len(rows)

        # ── Percentiles de señal (POR DRAW, no globales) ──────────
        signals = sorted(r["signal"] for r in rows)
        def pct(p):
            idx = max(0, min(int(p * n / 100), n - 1))
            return signals[idx]

        p25, p50, p75 = pct(25), pct(50), pct(75)

        # ── Clasificar cuartil de señal ────────────────────────────
        def cuartil(s):
            if s >= p75: return "muy_alta"
            if s >= p50: return "alta"
            if s >= p25: return "media"
            return "baja"

        for r in rows:
            r["cuartil"] = cuartil(r["signal"])

        # ── Hot_pos general del draw ───────────────────────────────
        all_hits = [r["hit_pos"] for r in rows if r["hit_pos"] is not None]
        pos_counter = Counter(all_hits)
        hot_pos_general = [p for p, _ in pos_counter.most_common()]
        for p in range(1, 13):
            if p not in hot_pos_general:
                hot_pos_general.append(p)

        hit_rate = round(len(all_hits) / n * 100, 1) if n > 0 else 0

        # ── Perfiles contextuales: cuartil + a11_group ────────────
        contextual = {}
        from itertools import product
        for cuartil_key, a11_key in product(
            ["muy_alta","alta","media","baja"],
            ["bajo","medio","alto"]
        ):
            ctx_rows = [r for r in rows
                        if r["cuartil"] == cuartil_key and r["a11_g"] == a11_key]
            if len(ctx_rows) < MIN_CONTEXTUAL:
                continue

            ctx_hits = [r["hit_pos"] for r in ctx_rows if r["hit_pos"] is not None]
            ctx_n    = len(ctx_rows)
            ctx_counter = Counter(ctx_hits)
            ctx_hot = [p for p, _ in ctx_counter.most_common()]
            for p in range(1, 13):
                if p not in ctx_hot:
                    ctx_hot.append(p)

            ctx_rate = round(len(ctx_hits) / ctx_n * 100, 1)
            combo_key = f"{cuartil_key}|{a11_key}"
            contextual[combo_key] = {
                "hot_pos":  ctx_hot,
                "n":        ctx_n,
                "hit_rate": ctx_rate,
            }

        if n < MIN_DRAW:
            # Poco data — mezclar con fallback
            fb = DRAW_PROFILE_FALLBACK.get(draw, {})
            if fb:
                perfiles[draw] = dict(fb)
                perfiles[draw].update({
                    "p25": p25, "p50": p50, "p75": p75,
                    "n": n, "contextual": contextual,
                    "fuente": f"fallback+percentiles ({n} sorteos)",
                })
                n_fallback += 1
            continue

        fuente = f"auto-calibrado ({n} sorteos, {len(contextual)} contextos)"
        perfiles[draw] = {
            "hit_rate":   hit_rate,
            "hot_pos":    hot_pos_general,
            "p25":        p25,
            "p50":        p50,
            "p75":        p75,
            "n":          n,
            "contextual": contextual,
            "fuente":     fuente,
        }
        n_auto += 1

        print(f"[Lotmix Caliente] 📈 {draw}: "
              f"hit={hit_rate}% n={n} ctx={len(contextual)} "
              f"p25={p25:.5f} p75={p75:.5f}")

    # Completar draws sin data con fallback
    for draw, fb in DRAW_PROFILE_FALLBACK.items():
        if draw not in perfiles:
            perfiles[draw] = dict(fb)
            perfiles[draw].update({
                "p25": 0.015, "p50": 0.022, "p75": 0.030,
                "n": 0, "contextual": {},
                "fuente": "fallback (sin data)",
            })
            n_fallback += 1

    total = n_auto + n_fallback
    print(f"[Lotmix Caliente] ✅ Perfiles: {n_auto} auto | {n_fallback} fallback | {total} total")
    return perfiles


# ═════════════════════════════════════════════════════════════════════════════
# SELECCIÓN DE NÚMEROS
# ═════════════════════════════════════════════════════════════════════════════

def seleccionar_numeros(pick: dict, perfiles: dict) -> dict | None:
    draw    = pick.get("draw", "")
    lottery = pick.get("lottery", "")
    decision= pick.get("decision", "")
    top12   = _parse_list(pick.get("top12", []))
    topq    = _parse_list(pick.get("topq",  []))
    ok_alert= pick.get("ok_alert", False)
    if isinstance(ok_alert, str):
        ok_alert = ok_alert.strip().lower() == "true"

    try:
        signal = float(pick.get("best_signal") or 0)
    except (TypeError, ValueError):
        signal = 0.0

    a11_g = _a11_group(pick.get("best_a11", 0))

    if not top12:
        return None

    # ── Perfil del draw ───────────────────────────────────────────
    perfil   = perfiles.get(draw, DRAW_PROFILE_FALLBACK.get(draw, {}))
    p25      = perfil.get("p25", 0.015)
    p50      = perfil.get("p50", 0.022)
    p75      = perfil.get("p75", 0.030)
    hit_rate = perfil.get("hit_rate", 0.0)
    fuente   = perfil.get("fuente", "desconocido")

    # ── Cuartil de señal (relativo al draw, no global) ────────────
    if signal >= p75:   cuartil = "muy_alta"
    elif signal >= p50: cuartil = "alta"
    elif signal >= p25: cuartil = "media"
    else:               cuartil = "baja"

    # ── Número de slots según cuartil ─────────────────────────────
    slots = {"muy_alta": 6, "alta": 5, "media": 4, "baja": 3}[cuartil]

    # ── Hot_pos: contextual si existe, general si no ──────────────
    combo_key = f"{cuartil}|{a11_g}"
    ctx = perfil.get("contextual", {}).get(combo_key)
    if ctx:
        hot_pos  = ctx["hot_pos"]
        ctx_rate = ctx["hit_rate"]
        ctx_n    = ctx["n"]
        hot_source = f"contextual ({combo_key}, n={ctx_n}, hit={ctx_rate}%)"
    else:
        hot_pos  = perfil.get("hot_pos", list(range(1, 13)))
        hot_source = "general"

    # ── Selección por posiciones calientes ────────────────────────
    numeros = []
    for pos in hot_pos[:slots]:
        idx = pos - 1
        if 0 <= idx < len(top12):
            num = top12[idx]
            if num not in numeros:
                numeros.append(num)

    # ── Relleno con topq ─────────────────────────────────────────
    for num in topq:
        if num not in numeros:
            numeros.append(num)

    # ── Bonus ok_alert: solo si el contexto lo valida ────────────
    # (ok_alert=True AND a11_g != bajo AND señal alta o muy_alta)
    if ok_alert and a11_g != "bajo" and cuartil in ("muy_alta", "alta"):
        extra_slots = slots
        if extra_slots < len(hot_pos):
            extra_idx = hot_pos[extra_slots] - 1
            if 0 <= extra_idx < len(top12):
                num = top12[extra_idx]
                if num not in numeros:
                    numeros.append(num)

    # ── Deduplicar y aplicar límite ───────────────────────────────
    seen, final = set(), []
    for n in numeros:
        if n not in seen:
            seen.add(n); final.append(n)
    final = final[:slots + 2]   # nunca más de slots+2

    # ── Advertencia ───────────────────────────────────────────────
    advertencia = None
    if hit_rate > 0 and hit_rate < 10.0:
        advertencia = f"⚠️ Hit rate histórico bajo ({hit_rate}%) — precaución"

    return {
        "numeros":            final,
        "signal":             round(signal, 6),
        "cuartil_senal":      cuartil,
        "decision":           decision,
        "a11_grupo":          a11_g,
        "ok_alert":           ok_alert,
        "hit_rate_historico": hit_rate,
        "hot_source":         hot_source,
        "draw":               draw,
        "lottery":            lottery,
        "perfil_fuente":      fuente,
        "n_numeros":          len(final),
        **({"advertencia": advertencia} if advertencia else {}),
    }


# ═════════════════════════════════════════════════════════════════════════════
# FUNCIÓN PÚBLICA EXPORTABLE
# ═════════════════════════════════════════════════════════════════════════════

def run(picks: list,
        output_path: str = "outputs/caliente.json",
        picks_log_path: str = None,
        perf_log_path: str = None,
        base_dir: Path = None) -> dict:
    """
    Punto de entrada para runner.py.

    Args:
        picks:          Lista de picks del payload actual (todos, sin filtrar)
        output_path:    Ruta de salida del caliente.json
        picks_log_path: Ruta local picks_log.csv (opcional)
        perf_log_path:  Ruta local performance.csv (opcional)
        base_dir:       Directorio base para búsqueda automática de CSVs
    """
    if base_dir is None:
        base_dir = Path(__file__).parent

    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)

    # ── Cargar historial y aprender ───────────────────────────────
    picks_rows, perf_rows = cargar_historico(picks_log_path, perf_log_path, base_dir)

    if picks_rows and perf_rows:
        perfiles = aprender_perfiles(picks_rows, perf_rows)
    else:
        print("[Lotmix Caliente] ⚠️  Sin historial — usando fallback estático", file=sys.stderr)
        perfiles = {}

    # ── Procesar todos los picks (sin filtrar por decisión) ───────
    loterias = {}
    skipped  = 0
    for pick in picks:
        resultado = seleccionar_numeros(pick, perfiles)
        if resultado is None:
            skipped += 1
            continue
        loterias[resultado["draw"]] = resultado

    output = {
        "generado_en":     datetime.now().isoformat(),
        "sistema":         "Lotmix Caliente",
        "version":         "3.0-dynamic",
        "total_loterias":  len(loterias),
        "picks_procesados":len(picks),
        "picks_omitidos":  skipped,
        "loterias":        loterias,
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"[Lotmix Caliente] 🎯 {len(loterias)} loterías → {output_path}")
    for draw, data in loterias.items():
        adv = f" | {data['advertencia']}" if data.get("advertencia") else ""
        print(f"  {draw}: {data['numeros']} | {data['cuartil_senal']} "
              f"| a11={data['a11_grupo']} | {data['hot_source']}{adv}")

    return output


# ═════════════════════════════════════════════════════════════════════════════
# CLI
# ═════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Lotmix Caliente v3.0 — Aprendizaje dinámico por draw"
    )
    parser.add_argument("--input",     "-i", required=True,
                        help="JSON con los picks activos")
    parser.add_argument("--output",    "-o", default="caliente.json")
    parser.add_argument("--picks-log", default=None,
                        help="Ruta local picks_log.csv")
    parser.add_argument("--perf-log",  default=None,
                        help="Ruta local performance.csv")
    parser.add_argument("--base-dir",  default=None,
                        help="Directorio base para búsqueda automática de CSVs")
    args = parser.parse_args()

    try:
        with open(args.input, encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"[ERROR] No se encontró: {args.input}", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"[ERROR] JSON inválido: {e}", file=sys.stderr)
        sys.exit(1)

    picks = [data] if isinstance(data, dict) else data
    base_dir = Path(args.base_dir) if args.base_dir else None

    run(
        picks=picks,
        output_path=args.output,
        picks_log_path=args.picks_log,
        perf_log_path=args.perf_log,
        base_dir=base_dir,
    )


if __name__ == "__main__":
    main()