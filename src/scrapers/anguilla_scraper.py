import re
import base64
from datetime import datetime, date as dt_date
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup

TZ_RD = ZoneInfo("America/Santo_Domingo")

BASE_URL = "https://www.loteriadominicana.com.do/Lottery/Anguilla"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari"
    )
}

# Si en runner usas nombres distintos (ANG-10AM, etc.),
# aquí los traducimos al nombre real del <h4> en la web:
DRAW_ALIASES = {
    "ANG-10AM": "Anguila 10AM",
    "ANG-1PM":  "Anguila 1PM",
    "ANG-6PM":  "Anguila 6PM",
    "ANG-9PM":  "Anguila 9PM",
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
    Genera el valor de ?d= que usa loteriadominicana.com.do
    Patrón:
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

def _parse_date(date_str: str) -> dt_date:
    # date_str: 'YYYY-MM-DD'
    return datetime.strptime(date_str, "%Y-%m-%d").date()

def _extract_numbers_near_h4(h4) -> list[str]:
    """
    Dado un <h4> del sorteo, busca el contenedor más cercano
    que tenga las bolas y extrae 3 números.
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

    # Método principal: bolas
    balls = container.select("div.ball span")
    nums = [z2(b.get_text(strip=True)) for b in balls if b.get_text(strip=True)]

    # Fallback si cambia el HTML
    if len(nums) < 3:
        txt_nums = re.findall(r"\b\d{1,2}\b", container.get_text(" ", strip=True))
        nums = [z2(x) for x in txt_nums]

    # deja solo los primeros 3
    nums = [n for n in nums if n != ""][:3]
    return nums

def get_result(draw: str, date: str) -> tuple[str, str, str]:
    """
    draw: nombre del sorteo (de runner) o alias (ANG-10AM, etc.)
    date: 'YYYY-MM-DD'
    return: ('02','03','04') siempre con 2 dígitos
    """
    target_title = DRAW_ALIASES.get(draw, draw).strip()
    d = _parse_date(date)
    url = build_url_for_date(d)

    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")

    # Buscar el h4 exacto del sorteo
    h4_target = None
    for h4 in soup.find_all("h4"):
        title = (h4.get_text(strip=True) or "").strip()
        if title == target_title:
            h4_target = h4
            break

    if h4_target is None:
        raise ValueError(f"[Anguilla] No encontré el sorteo '{target_title}' en la página para {date}.")

    nums = _extract_numbers_near_h4(h4_target)
    if len(nums) < 3:
        # Esto es normal si el resultado aún no está publicado
        raise ValueError(f"[Anguilla] Resultado aún no publicado para '{target_title}' ({date}).")

    return (nums[0], nums[1], nums[2])
