import os
import requests
import concurrent.futures

_executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)


def _post(url: str, payload: dict):
    # timeout aquí cubre la conexión TCP y la lectura de la respuesta,
    # pero NO cubre una resolución DNS colgada — por eso el hard-timeout externo
    return requests.post(url, json=payload, timeout=15)


def send_telegram(text: str):
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        raise RuntimeError("Faltan TELEGRAM_BOT_TOKEN o TELEGRAM_CHAT_ID")

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text}

    future = _executor.submit(_post, url, payload)
    try:
        r = future.result(timeout=20)  # hard-timeout real, cubre incluso un DNS colgado
        r.raise_for_status()
    except concurrent.futures.TimeoutError:
        print("[WARN] send_telegram: timeout duro (posible DNS colgado). Se omite este envío.")
    except requests.RequestException as e:
        print(f"[WARN] send_telegram falló: {e}")