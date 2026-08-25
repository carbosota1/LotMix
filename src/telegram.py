import os
import requests


def send_telegram(text: str):
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        raise RuntimeError("Faltan TELEGRAM_BOT_TOKEN o TELEGRAM_CHAT_ID")

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        r = requests.post(
            url,
            json={"chat_id": chat_id, "text": text},
            timeout=15,  # (segundos) evita que el job se cuelgue indefinidamente
        )
        r.raise_for_status()
    except requests.RequestException as e:
        print(f"[WARN] send_telegram falló: {e}")