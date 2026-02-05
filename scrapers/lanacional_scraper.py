import re
import base64
from datetime import datetime, date as dt_date
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

TZ_RD = ZoneInfo("America/Santo_Domingo")

BASE_URL = "https://www.loteriadominicana.com.do/Lottery/National"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari"
    )
}

# En La Nacional, en tu runner ya usas los títulos reales:
# - "Loteria Nacional- Gana Más"
# - "Loteria Nacional- Noche"
# Aun así, dejo alias por si quieres usar nombres cortos después.
DRAW_ALIASES = {
    "LN-GanaMas": "Loteria Nacional- Gana Más",
    "LN-Noche":   "Loteria Nacional- Noche",
}

def z2(x: str) -> str:
    """Normaliza a 2 dígitos SIN perder 00."""
    s = str(x).strip()
    if re.fullmatch(r"\d{2}", s):
        return s
    m = re.search(r"\d+", s)
    return m.group(0).zfill(2) if m else ""

def encode_d_param(d: dt_date) -> str:
    """
    Genera el ?d= que usa loteriadominicana.com.do
    ddmmyyyy -> invertir -> decimal->HEX -> base64(HEX)
    """
    ddmmyyyy = d.strftime("%d%m%Y")
    rev = ddmmyyyy[::-1]
    hx = format(int(rev), "X")
    return base64.b64encode(hx.encode("utf-8")).decode("utf-8")

def build_url_for_date(d: dt_date) -> str:
    return f"{BASE_URL}?d={encode_d_param(d)}"

def _parse_date(date_str: str) -> dt_date:
    return datetime.strptime(date_str, "%Y-%m-%d").date()

def _extract_numbers_near_h4(h4) -> list[str]:
    """
    Desde el <h4> del sorteo, sube por el DOM hasta
    encontrar el contenedor con las bolas.
    """
    container = h4.parent
    for _ in range(8):
        if container is None:
            break
        if container.find(class_=re.compile(r"result-item-ball-content|ball")):
            break
        container = container.parent

    if not container:
        return []

    balls = container.select("div.ball span")
    nums = [z2(b.get_text(strip=True)) for b in balls if b.get_text(strip=True)]

    if len(nums) < 3:
        txt_nums = re.findall(r"\b\d{1,2}\b", container.get_text(" ", strip=True))
        nums = [z2(x) for x in txt_nums]

    return [n for n in nums if n][:3]

def get_result(draw: str, date: str) -> tuple[str, str, str]:
    """
    draw: nombre del sorteo (idealmente el mismo del <h4>)
    date: 'YYYY-MM-DD'
    return: ('84','23','82')
    """
    target_title = DRAW_ALIASES.get(draw, draw).strip()
    d = _parse_date(date)
    url = build_url_for_date(d)

    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")

    h4_target = None
    for h4 in soup.find_all("h4"):
        title = (h4.get_text(strip=True) or "").strip()
        if title == target_title:
            h4_target = h4
            break

    if h4_target is None:
        raise ValueError(f"[La Nacional] No encontré el sorteo '{target_title}' en la página para {date}.")

    nums = _extract_numbers_near_h4(h4_target)
    if len(nums) < 3:
        raise ValueError(f"[La Nacional] Resultado aún no publicado para '{target_title}' ({date}).")

    return (nums[0], nums[1], nums[2])
