import re
import base64
from datetime import datetime, date as ddate
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

TZ_RD = ZoneInfo("America/Santo_Domingo")

BASE_URL = "https://www.loteriadominicana.com.do/Lottery/Lotodom"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari"
    )
}

# ✅ Debe coincidir con la columna "sorteo" del XLSX / runner
TARGET_DRAWS = {
    "Quiniela La Primera",
    "Quiniela La Primera Noche",
}


def _z2(x: str) -> str:
    s = str(x).strip()
    if re.fullmatch(r"\d{2}", s):
        return s
    m = re.search(r"\d+", s)
    return m.group(0).zfill(2) if m else ""


def _encode_d_param(dt: ddate) -> str:
    # ddmmyyyy -> reverse -> int -> hex -> base64(hex)
    ddmmyyyy = dt.strftime("%d%m%Y")
    rev = ddmmyyyy[::-1]
    hx = format(int(rev), "X")
    return base64.b64encode(hx.encode("utf-8")).decode("utf-8")


def _build_url_for_date(dt: ddate) -> str:
    key = _encode_d_param(dt)
    return f"{BASE_URL}?d={key}"


def _norm_title(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", " ", s)
    return s


def _parse_numbers(container) -> list[str]:
    # 1) Preferido: div.ball span
    balls = container.select("div.ball span")
    nums = [_z2(b.get_text(strip=True)) for b in balls if b.get_text(strip=True)]
    nums = [n for n in nums if n]

    # 2) Fallback: números en el texto del bloque
    if len(nums) < 3:
        txt = container.get_text(" ", strip=True)
        txt_nums = re.findall(r"\b\d{1,2}\b", txt)
        nums = [_z2(x) for x in txt_nums if _z2(x)]

    return nums[:3]


def get_result(draw: str, date: str):
    """
    draw: "Quiniela La Primera" o "Quiniela La Primera Noche"
    date: "YYYY-MM-DD"
    return: (primero, segundo, tercero) como "00".."99"
    """
    draw = _norm_title(draw)
    if draw not in TARGET_DRAWS:
        raise ValueError(f"Draw no reconocido: '{draw}'. Esperado: {sorted(TARGET_DRAWS)}")

    try:
        dt = datetime.strptime(date, "%Y-%m-%d").date()
    except Exception:
        raise ValueError("date debe ser 'YYYY-MM-DD'")

    url = _build_url_for_date(dt)
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")

    # Encontrar el H4 exacto del sorteo
    h4_match = None
    for h4 in soup.find_all("h4"):
        title = _norm_title(h4.get_text(strip=True))
        if title == draw:
            h4_match = h4
            break

    if not h4_match:
        titles = [_norm_title(h.get_text(strip=True)) for h in soup.find_all("h4")]
        raise RuntimeError(f"No encontré '{draw}' en la página. H4 vistos: {titles[:30]}")

    # Subir a un contenedor que tenga bolas
    container = h4_match.parent
    for _ in range(10):
        if container is None:
            break
        if container.find(class_=re.compile(r"result-item-ball-content|ball")):
            break
        container = container.parent

    if not container:
        raise RuntimeError(f"Encontré '{draw}' pero no el contenedor de bolas.")

    nums = _parse_numbers(container)
    if len(nums) < 3:
        raise RuntimeError(f"No pude extraer 3 números para '{draw}' ({date}).")

    return nums[0], nums[1], nums[2]