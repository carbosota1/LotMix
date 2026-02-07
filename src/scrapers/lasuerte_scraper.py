# src/scrapers/lasuerte_scraper.py
import re
import base64
from datetime import date as dt_date
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

TZ_RD = ZoneInfo("America/Santo_Domingo")

BASE_URL = "https://www.loteriadominicana.com.do/Lottery/DominicanLuck"

TARGET_DRAWS = {
    "Quiniela La Suerte",
    "Quiniela La Suerte 6PM",
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari"
    )
}

def z2(x: str) -> str:
    """
    Normaliza a 2 dígitos SIN perder 00.
    """
    s = str(x).strip()
    if re.fullmatch(r"\d{2}", s):
        return s
    m = re.search(r"\d+", s)
    return m.group(0).zfill(2) if m else ""

def encode_d_param(d: dt_date) -> str:
    """
    Genera el valor de ?d= que usa loteriadominicana.com.do
    - ddmmyyyy (ej: 07062024)
    - invertir -> 42026070
    - decimal a HEX (uppercase)
    - base64 del HEX
    """
    ddmmyyyy = d.strftime("%d%m%Y")
    rev = ddmmyyyy[::-1]
    hx = format(int(rev), "X")
    return base64.b64encode(hx.encode("utf-8")).decode("utf-8")

def build_url_for_date(d: dt_date) -> str:
    key = encode_d_param(d)
    return f"{BASE_URL}?d={key}"

def parse_date_from_text(txt: str) -> dt_date | None:
    """
    La web suele incluir fecha como dd-mm-yyyy en el bloque.
    """
    m = re.search(r"\b(\d{2})-(\d{2})-(\d{4})\b", txt)
    if not m:
        return None
    dd, mm, yyyy = map(int, m.groups())
    return dt_date(yyyy, mm, dd)

def fetch_day_results(date_str: str) -> list[dict]:
    """
    date_str: 'YYYY-MM-DD'
    Retorna lista de dicts con sorteos encontrados.
    """
    d = datetime.strptime(date_str, "%Y-%m-%d").date()
    url = build_url_for_date(d)

    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")

    out = []
    for h4 in soup.find_all("h4"):
        title = (h4.get_text(strip=True) or "")
        if title not in TARGET_DRAWS:
            continue

        # Buscar contenedor cercano con bolas
        container = h4.parent
        for _ in range(8):
            if container is None:
                break
            if container.find(class_=re.compile(r"result-item-ball-content|ball")):
                break
            container = container.parent

        if not container:
            continue

        # Extraer bolas (lo más confiable)
        balls = container.select("div.result-item-ball-content div.ball span, div.ball span")
        nums = [z2(b.get_text(strip=True)) for b in balls if b.get_text(strip=True)]

        # Fallback si cambia HTML
        if len(nums) < 3:
            txt_nums = re.findall(r"\b\d{1,2}\b", container.get_text(" ", strip=True))
            nums = [z2(x) for x in txt_nums][:3]

        if len(nums) < 3:
            continue

        block_text = container.get_text(" ", strip=True)
        block_date = parse_date_from_text(block_text) or d

        out.append({
            "fecha": block_date.isoformat(),
            "sorteo": title,
            "primero": nums[0],
            "segundo": nums[1],
            "tercero": nums[2],
            "scraped_at_rd": datetime.now(TZ_RD).strftime("%Y-%m-%d %H:%M:%S"),
            "url": url,
        })

    return out

def get_result(draw: str, date: str) -> tuple[str, str, str]:
    """
    draw: nombre EXACTO del sorteo (columna 'sorteo' en XLSX)
    date: 'YYYY-MM-DD'
    return: ('00','07','84') siempre con 2 dígitos
    """
    draw = str(draw).strip()

    rows = fetch_day_results(date)
    for r in rows:
        if str(r.get("sorteo", "")).strip() == draw:
            return (z2(r["primero"]), z2(r["segundo"]), z2(r["tercero"]))

    raise RuntimeError(f"No result found for draw='{draw}' date='{date}' (quizá aún no publicado)")
