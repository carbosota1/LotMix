import os, json
import requests

PICKS_PATH = "outputs/picks.json"

def send_telegram(text: str):
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        raise RuntimeError("Faltan TELEGRAM_BOT_TOKEN o TELEGRAM_CHAT_ID")

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    r = requests.post(url, json={"chat_id": chat_id, "text": text})
    r.raise_for_status()

def main():
    with open(PICKS_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    lines = []
    lines.append("🎯 PICKS DEL DÍA (Cross-Lottery)")
    lines.append("")
    for p in data["picks"]:
        lines.append(f"📌 {p['loteria']} | {p['sorteo']}")
        lines.append("Top: " + ", ".join(p["top_nums"][:10]))
        top_pales = p["pales"][:10]
        lines.append("Palés: " + " | ".join([f"{a}-{b}" for a,b in top_pales]))
        lines.append("")

    send_telegram("\n".join(lines))

if __name__ == "__main__":
    main()
