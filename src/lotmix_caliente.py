#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════╗
║           LOTMIX CALIENTE — lotmix_caliente.py                  ║
║   Sistema de Números Calientes por Posición Histórica           ║
║   Calibrado con data real: Feb–Jun 2026                         ║
╚══════════════════════════════════════════════════════════════════╝

Uso desde línea de comandos:
    python3 lotmix_caliente.py --input picks_activos.json --output caliente.json

Uso desde otro script (importando la función):
    from lotmix_caliente import run
    resultado = run(picks=lista_de_picks, output_path="outputs/caliente.json")
"""

import json
import argparse
import ast
from datetime import datetime
from pathlib import Path

# ─────────────────────────────────────────────────────────────────
# DRAW PROFILES — calibrados con ~4 meses de data real
# hit_rate: % histórico de acierto en top12
# hot_pos:  posiciones 1-indexadas ordenadas por frecuencia de acierto
# ─────────────────────────────────────────────────────────────────
DRAW_PROFILE = {
    "Anguila 9PM": {
        "hit_rate": 17.3,
        "hot_pos": [11, 3, 4, 1, 8, 10, 6, 7, 12, 2, 5, 9],
        "nota": "Mejor draw del sistema — priorizar"
    },
    "Anguila 1PM": {
        "hit_rate": 15.9,
        "hot_pos": [11, 12, 7, 1, 9, 4, 8, 2, 3, 5, 6, 10],
        "nota": "Segundo mejor draw"
    },
    "Quiniela La Primera": {
        "hit_rate": 15.1,
        "hot_pos": [3, 1, 11, 2, 9, 4, 6, 5, 8, 7, 10, 12],
        "nota": "Tercer mejor draw"
    },
    "Loteria Nacional- Gana Más": {
        "hit_rate": 12.0,
        "hot_pos": [4, 5, 1, 11, 12, 6, 8, 9, 2, 3, 7, 10],
        "nota": "Rendimiento aceptable"
    },
    "Anguila 6PM": {
        "hit_rate": 11.9,
        "hot_pos": [10, 2, 9, 6, 7, 12, 5, 1, 8, 3, 4, 11],
        "nota": "Señal predominantemente baja — aceptable"
    },
    "Quiniela La Primera Noche": {
        "hit_rate": 11.5,
        "hot_pos": [6, 9, 11, 2, 1, 8, 12, 3, 4, 5, 7, 10],
        "nota": "Señal media/alta — rendimiento moderado"
    },
    "Loteria Nacional- Noche": {
        "hit_rate": 11.3,
        "hot_pos": [3, 12, 6, 10, 2, 8, 5, 11, 1, 4, 7, 9],
        "nota": "Rendimiento moderado"
    },
    "Quiniela La Suerte": {
        "hit_rate": 10.9,
        "hot_pos": [1, 3, 11, 10, 4, 2, 5, 6, 7, 8, 9, 12],
        "nota": "Señal alta pero hit rate bajo — usar con precaución"
    },
    "Quiniela La Suerte 6PM": {
        "hit_rate": 10.1,
        "hot_pos": [7, 3, 10, 6, 9, 12, 1, 2, 4, 5, 8, 11],
        "nota": "Hit rate bajo — señal predominantemente baja"
    },
    "Anguila 10AM": {
        "hit_rate": 8.1,
        "hot_pos": [7, 12, 5, 10, 8, 1, 2, 3, 4, 6, 9, 11],
        "nota": "PRECAUCIÓN: señal fuerte pero peor hit rate del sistema"
    },
}

# ─────────────────────────────────────────────────────────────────
# TABLA DE SEÑAL — calibrada con data real
#
# HALLAZGOS DEL ANÁLISIS:
#   muy_alta (≥0.035): 14.8% hit rate  ← mejores resultados
#   media (0.015-0.025): 14.2%          ← sorpresivamente buena
#   baja (<0.015): 12.4%
#   alta (0.025-0.035): 8.0%            ← peor nivel — posible artefacto
#
# Por eso nivel MEDIA mantiene 4 posiciones (igual que en el diseño
# original) y nivel ALTA se reduce a 4 también para no penalizar.
# ─────────────────────────────────────────────────────────────────
SIGNAL_CONFIG = {
    #  nivel     umbral_min  posiciones_calientes  max_numeros
    "muy_alta": {"min": 0.035, "hot_slots": 6, "max_nums": 8},
    "alta":     {"min": 0.025, "hot_slots": 4, "max_nums": 6},   # reducido por bajo rendimiento
    "media":    {"min": 0.015, "hot_slots": 4, "max_nums": 6},
    "baja":     {"min": 0.000, "hot_slots": 3, "max_nums": 4},
}

# Decisiones que activan la generación de calientes
DECISIONES_ACTIVAS = {"⚠️ JUGAR", "🔥 JUGAR AGRESIVO"}

# ok_alert: el análisis mostró que ok_alert=True BAJA el hit rate (8.2% vs 12.9%)
# Por eso NO se añade posición extra por ok_alert — se ignora como bonus
# Si en el futuro cambia con más data, ajustar aquí:
OK_ALERT_BONUS = False  # cambiar a True si futura data lo valida


# ─────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────

def get_signal_level(signal: float) -> str:
    """Clasifica la señal en nivel."""
    if signal is None:
        return "baja"
    if signal >= 0.035:
        return "muy_alta"
    if signal >= 0.025:
        return "alta"
    if signal >= 0.015:
        return "media"
    return "baja"


def parse_list_field(value) -> list:
    """Parsea un campo que puede ser lista JSON, string con lista, o None."""
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            result = json.loads(value)
            if isinstance(result, list):
                return result
        except (json.JSONDecodeError, ValueError):
            pass
        try:
            result = ast.literal_eval(value)
            if isinstance(result, list):
                return result
        except (ValueError, SyntaxError):
            pass
    return []


def build_caliente(pick: dict) -> dict | None:
    """
    Procesa un pick individual y produce su entrada caliente.

    Parámetros esperados en pick:
        draw         (str)   nombre del sorteo
        lottery      (str)   nombre de la lotería
        decision     (str)   decisión del sistema principal
        best_signal  (float) mejor señal del pick
        best_a11     (int)   valor a11
        ok_alert     (bool)  alerta ok
        top12        (list)  12 números candidatos (strings "00"-"99")
        topq         (list)  top 3 quiniela (relleno)
        date         (str)   fecha del pick (opcional)

    Retorna dict con la entrada caliente, o None si el pick no aplica.
    """
    decision = pick.get("decision", "")
    draw = pick.get("draw", "")
    lottery = pick.get("lottery", "")
    signal = pick.get("best_signal") or 0.0
    a11 = pick.get("best_a11") or 0
    ok_alert = bool(pick.get("ok_alert", False))
    top12 = parse_list_field(pick.get("top12", []))
    topq = parse_list_field(pick.get("topq", []))

    if not top12:
        return None

    # ── Nivel de señal y configuración ──────────────────────────
    nivel = get_signal_level(signal)
    cfg = SIGNAL_CONFIG[nivel]
    hot_slots = cfg["hot_slots"]
    max_nums = cfg["max_nums"]

    # ── Perfil del draw ──────────────────────────────────────────
    profile = DRAW_PROFILE.get(draw)
    if profile:
        hot_pos = profile["hot_pos"]
        hit_rate = profile["hit_rate"]
    else:
        # Draw desconocido: usar posiciones naturales 1-12
        hot_pos = list(range(1, 13))
        hit_rate = 0.0

    # ── Selección de números por posiciones calientes ────────────
    numeros = []
    for pos in hot_pos[:hot_slots]:
        idx = pos - 1  # convertir a 0-indexado
        if 0 <= idx < len(top12):
            num = top12[idx]
            if num not in numeros:
                numeros.append(num)

    # ── Relleno con topq ─────────────────────────────────────────
    for num in topq:
        if num not in numeros:
            numeros.append(num)

    # ── Bonus: a11 < 3 en nivel media tiene 18.4% de hit rate ────
    # Basado en análisis: media + a11<3 = 18.4% vs media + a11>=3 = 13.3%
    # Se añade 1 posición caliente extra cuando aplica
    if nivel == "media" and a11 < 3 and len(hot_pos) > hot_slots:
        extra_pos = hot_pos[hot_slots] - 1  # siguiente posición caliente
        if 0 <= extra_pos < len(top12):
            num = top12[extra_pos]
            if num not in numeros:
                numeros.append(num)

    # ── ok_alert: actualmente desactivado (ver nota en config) ───
    if OK_ALERT_BONUS and ok_alert and a11 >= 3:
        next_slot = hot_slots
        if next_slot < len(hot_pos):
            extra_pos = hot_pos[next_slot] - 1
            if 0 <= extra_pos < len(top12):
                num = top12[extra_pos]
                if num not in numeros:
                    numeros.append(num)

    # ── Aplicar límite máximo ────────────────────────────────────
    numeros = numeros[:max_nums]

    # ── Advertencia para draws de bajo rendimiento ───────────────
    advertencia = None
    if profile and hit_rate < 10.0:
        advertencia = f"⚠️ Draw con hit rate histórico bajo ({hit_rate}%) — usar con precaución"

    return {
        "numeros": numeros,
        "signal": round(float(signal), 6),
        "nivel_senal": nivel,
        "decision": decision,
        "a11": int(a11),
        "ok_alert": ok_alert,
        "hit_rate_historico": hit_rate,
        "draw": draw,
        "lottery": lottery,
        "n_numeros": len(numeros),
        **({"advertencia": advertencia} if advertencia else {}),
    }


# ─────────────────────────────────────────────────────────────────
# FUNCIÓN PRINCIPAL EXPORTABLE
# ─────────────────────────────────────────────────────────────────

def run(picks: list, output_path: str = "caliente.json") -> dict:
    """
    Genera el archivo caliente.json a partir de una lista de picks.

    Args:
        picks:       Lista de dicts con los picks del sistema principal.
        output_path: Ruta del archivo de salida.

    Returns:
        Dict con la estructura completa del caliente.json generado.
    """
    loterias = {}
    skipped = 0

    for pick in picks:
        resultado = build_caliente(pick)
        if resultado is None:
            skipped += 1
            continue

        draw = resultado["draw"]
        # Si hay múltiples picks del mismo draw, queda el último procesado
        loterias[draw] = resultado

    output = {
        "generado_en": datetime.now().isoformat(),
        "sistema": "Lotmix Caliente",
        "version": "1.0",
        "total_loterias": len(loterias),
        "picks_procesados": len(picks),
        "picks_activos": len(loterias),
        "picks_omitidos": skipped,
        "loterias": loterias,
    }

    # Guardar archivo
    out_path = Path(output_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"[Lotmix Caliente] ✅ {len(loterias)} loterías generadas → {output_path}")
    for draw, data in loterias.items():
        advertencia = data.get("advertencia", "")
        print(f"  {draw}: {data['numeros']} | {data['nivel_senal']} | hit_rate={data['hit_rate_historico']}%"
              + (f" | {advertencia}" if advertencia else ""))

    return output


# ─────────────────────────────────────────────────────────────────
# MODO LÍNEA DE COMANDOS
# ─────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Lotmix Caliente — Genera caliente.json con números calientes por lotería"
    )
    parser.add_argument(
        "--input", "-i",
        required=True,
        help="Ruta al JSON con los picks activos (generado por el sistema principal)"
    )
    parser.add_argument(
        "--output", "-o",
        default="caliente.json",
        help="Ruta del archivo de salida (default: caliente.json)"
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        print(f"[Lotmix Caliente] ❌ No se encontró el archivo: {args.input}")
        return

    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Acepta tanto lista directa como dict con clave "picks" o "loterias"
    if isinstance(data, list):
        picks = data
    elif isinstance(data, dict):
        picks = data.get("picks") or data.get("loterias") or list(data.values())
    else:
        print(f"[Lotmix Caliente] ❌ Formato de input no reconocido")
        return

    run(picks=picks, output_path=args.output)


if __name__ == "__main__":
    main()
