"""
run_sunday_aware.py - Ejecuta un runner aplicando horarios especiales de domingo
sin modificar el runner original.

Uso:  python src/run_sunday_aware.py [runner|runner_v2]

Domingo: La Suerte 6PM sale a las 21:00 (9PM) en vez de 18:00.
Con eso el runner espera hasta las 21:02 para buscar el resultado, genera el
pick junto al slot de las 9PM y lo califica con el resultado correcto.
"""
import importlib
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# (loteria, sorteo) -> hora RD en domingo
SUNDAY_OVERRIDES = {
    ("La Suerte", "Quiniela La Suerte 6PM"): "21:00",
}


def apply(mod) -> None:
    if not hasattr(mod, "item_time") or not hasattr(mod, "now_rd"):
        raise RuntimeError(f"{mod.__name__}: no tiene item_time/now_rd; no se puede aplicar el override de domingo")
    orig = mod.item_time

    def item_time(item: dict) -> str:
        try:
            if mod.now_rd().weekday() == 6:
                t = SUNDAY_OVERRIDES.get((item["lottery"], item["draw"]))
                if t:
                    return t
        except Exception:
            pass
        return orig(item)

    mod.item_time = item_time
    print(f"[PATCH] {mod.__name__}: overrides de domingo activos -> {SUNDAY_OVERRIDES}")


if __name__ == "__main__":
    name = sys.argv[1] if len(sys.argv) > 1 else "runner"
    module = importlib.import_module(name)
    apply(module)
    module.main()
