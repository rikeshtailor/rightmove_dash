# ============================================================
# scraper_alternative.py
# BROWSERLESS RIGHTMOVE SCRAPER (aiohttp only, no Playwright)
#
# URL collection uses Rightmove's internal search API instead
# of a real browser, eliminating the Playwright dependency.
# Detail scraping is unchanged from scraper_optimized_v4.py.
# ============================================================

import os
import re
import sys
import json
import time
import random
import asyncio
import aiohttp
import subprocess
import pandas as pd
import threading
from threading import Thread
from multiprocessing import Process, Queue, freeze_support
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from math import radians, sin, cos, sqrt, atan2

RETRY_FAILED     = "--retry-failed"     in sys.argv
REBUILD_FROM_ALL = "--rebuild-from-all" in sys.argv
ANALYSE_ONLY     = "--analyse-only"     in sys.argv

# --time-limit N: exit cleanly after N minutes (for chunked CI runs)
_time_limit_arg = next((sys.argv[i+1] for i, a in enumerate(sys.argv) if a == "--time-limit"), None)
TIME_LIMIT_SECS = int(_time_limit_arg) * 60 if _time_limit_arg else None
RUN_START       = time.time()

# --workers N: number of parallel detail-scraping processes (default: CPU count, capped at 6)
_workers_arg  = next((sys.argv[i+1] for i, a in enumerate(sys.argv) if a == "--workers"), None)
DETAIL_WORKERS = int(_workers_arg) if _workers_arg else min(os.cpu_count() or 4, 8)

def time_is_up() -> bool:
    return TIME_LIMIT_SECS is not None and (time.time() - RUN_START) >= TIME_LIMIT_SECS

# ============================================================
# CONFIG
# ============================================================

BASE_DIR    = Path(__file__).resolve().parent
PARQUET_DIR = BASE_DIR / "parquet"
DATA_DIR    = BASE_DIR / "data"
PARQUET_DIR.mkdir(exist_ok=True)
DATA_DIR.mkdir(exist_ok=True)
SHARD_ROWS  = 5000
STATE_FILE  = BASE_DIR / "state.json"

OUTCODE_CSV = os.environ.get(
    "OUTCODE_CSV",
    r"C:\rightmove_monitor\scraper\outercode_to_postcode_master.csv",
)

PAGE_SIZE   = 24
MAX_PAGES   = 50

# HMO analysis
HMO_EMAIL_TO        = os.environ.get("HMO_EMAIL_TO", "")
HMO_PRICE_THRESHOLD = 220_000
# Property types that can physically be HMOs (exclude flats/maisonettes)
_HOUSE_TYPE_PAT = r"detach|semi|terrace|bungalow|town.house|cottage|villa|barn"

# ============================================================
# REFERENCE DATA  (for HMO hotspot scoring)
# ============================================================

# (name, lat, lon)
UK_UNIVERSITIES = [
    # Yorkshire
    ("University of Leeds",           53.8067, -1.5550),
    ("Leeds Beckett University",       53.8008, -1.5531),
    ("University of Bradford",         53.7895, -1.7527),
    ("University of Huddersfield",     53.6462, -1.7830),
    ("University of Sheffield",        53.3811, -1.4886),
    ("Sheffield Hallam University",    53.3780, -1.4684),
    ("University of York",             53.9479, -1.0498),
    ("York St John University",        53.9618, -1.0734),
    ("University of Hull",             53.7707, -0.3665),
    ("Leeds Trinity University",       53.8330, -1.6340),
    # North East
    ("Newcastle University",           54.9788, -1.6131),
    ("Northumbria University",         54.9786, -1.6147),
    ("Durham University",              54.7743, -1.5756),
    ("University of Sunderland",       54.9046, -1.3822),
    ("Teesside University",            54.5761, -1.2295),
    # North West
    ("University of Manchester",       53.4668, -2.2339),
    ("Manchester Metropolitan Univ",   53.4709, -2.2374),
    ("University of Salford",          53.4875, -2.2901),
    ("University of Bolton",           53.5796, -2.4290),
    ("University of Chester",          53.1901, -2.8939),
    ("University of Central Lancashire",53.7632, -2.7113),
    ("Lancaster University",           54.0104, -2.7878),
    ("Liverpool University",           53.4066, -2.9665),
    ("Liverpool John Moores Univ",     53.4053, -2.9769),
    ("Edge Hill University",           53.5501, -2.8797),
    ("Keele University",               53.0014, -2.2724),
    ("Staffordshire University",       52.9986, -2.1798),
    # West Midlands
    ("Aston University",               52.4862, -1.8876),
    ("University of Birmingham",       52.4508, -1.9305),
    ("Birmingham City University",     52.4862, -1.8913),
    ("University of Warwick",          52.3790, -1.5613),
    ("Coventry University",            52.4073, -1.5026),
    ("University of Wolverhampton",    52.5887, -2.1318),
    # East Midlands
    ("University of Nottingham",       52.9382, -1.1974),
    ("Nottingham Trent University",    52.9560, -1.1513),
    ("University of Leicester",        52.6219, -1.1290),
    ("De Montfort University",         52.6313, -1.1358),
    ("Loughborough University",        52.7659, -1.2247),
    ("University of Derby",            52.9190, -1.4809),
    ("University of Lincoln",          53.2307, -0.5434),
    ("University of Northampton",      52.2477, -0.8987),
    # East of England
    ("University of Cambridge",        52.2044,  0.1153),
    ("Anglia Ruskin University",       52.2018,  0.1349),
    ("University of East Anglia",      52.6216,  1.2394),
    # South East
    ("University of Oxford",           51.7548, -1.2544),
    ("Oxford Brookes University",      51.7539, -1.2278),
    ("University of Reading",          51.4413, -0.9406),
    ("University of Surrey",           51.2440, -0.5875),
    ("University of Sussex",           50.8673, -0.0878),
    ("University of Brighton",         50.8230, -0.1403),
    ("University of Kent",             51.2980,  1.0649),
    ("Canterbury Christ Church Univ",  51.2824,  1.0750),
    ("University of Portsmouth",       50.7984, -1.0965),
    ("University of Southampton",      50.9340, -1.3958),
    ("Brunel University",              51.5327, -0.4769),
    ("Royal Holloway University",      51.4255, -0.5640),
    ("University of Hertfordshire",    51.7520, -0.2438),
    # London
    ("University College London",      51.5246, -0.1340),
    ("King's College London",          51.5116, -0.1162),
    ("Queen Mary Univ of London",      51.5244, -0.0402),
    ("City University of London",      51.5281, -0.1020),
    ("London Metropolitan University", 51.5244, -0.0770),
    ("London South Bank University",   51.5009, -0.1022),
    ("University of East London",      51.5090,  0.0493),
    ("University of Greenwich",        51.4825,  0.0036),
    ("Kingston University",            51.4104, -0.3000),
    ("University of Westminster",      51.5183, -0.1436),
    ("University of Roehampton",       51.4644, -0.2261),
    # South West
    ("University of Bristol",          51.4584, -2.6032),
    ("UWE Bristol",                    51.4996, -2.5476),
    ("University of Bath",             51.3783, -2.3280),
    ("Bath Spa University",            51.3873, -2.3617),
    ("University of Exeter",           50.7358, -3.5337),
    ("University of Plymouth",         50.3755, -4.1432),
    ("Bournemouth University",         50.7420, -1.8959),
    ("University of Gloucestershire",  51.8582, -2.2435),
    # Wales
    ("Cardiff University",             51.4876, -3.1827),
    ("Cardiff Metropolitan University",51.4959, -3.2125),
    ("University of South Wales",      51.5957, -3.3416),
    ("Swansea University",             51.6214, -3.8877),
    ("Aberystwyth University",         52.4153, -4.0658),
    ("Bangor University",              53.2277, -4.1271),
    # Scotland
    ("University of Edinburgh",        55.9445, -3.1892),
    ("Heriot-Watt University",         55.9093, -3.3226),
    ("Edinburgh Napier University",    55.9260, -3.2188),
    ("University of Glasgow",          55.8718, -4.2888),
    ("University of Strathclyde",      55.8613, -4.2445),
    ("Glasgow Caledonian University",  55.8671, -4.2499),
    ("University of Aberdeen",         57.1657, -2.1033),
    ("Robert Gordon University",       57.1407, -2.1264),
    ("University of Dundee",           56.4577, -2.9796),
    ("University of St Andrews",       56.3399, -2.9778),
    ("University of Stirling",         56.1447, -3.9199),
    # Northern Ireland
    ("Queen's University Belfast",     54.5845, -5.9344),
    ("Ulster University",              54.9980, -6.6538),
]

UK_HOSPITALS = [
    # Yorkshire
    ("Leeds General Infirmary",                53.8027, -1.5491),
    ("St James's University Hospital Leeds",   53.8036, -1.5197),
    ("Bradford Royal Infirmary",               53.8022, -1.7718),
    ("Pinderfields Hospital Wakefield",        53.6819, -1.4821),
    ("Calderdale Royal Hospital Halifax",      53.7108, -1.8574),
    ("Huddersfield Royal Infirmary",           53.6442, -1.7810),
    ("York Hospital",                          53.9603, -1.0771),
    ("Hull Royal Infirmary",                   53.7458, -0.3466),
    ("Sheffield Teaching Hospitals",           53.3797, -1.4868),
    ("Rotherham Hospital",                     53.4344, -1.3643),
    ("Doncaster Royal Infirmary",              53.5256, -1.0960),
    ("Barnsley Hospital",                      53.5561, -1.4752),
    # North East
    ("Newcastle RVI",                          54.9839, -1.6169),
    ("Freeman Hospital Newcastle",             55.0019, -1.6056),
    ("James Cook University Hospital",         54.5568, -1.2244),
    ("Sunderland Royal Hospital",              54.9046, -1.4056),
    ("University Hospital of North Durham",    54.7788, -1.5701),
    # North West
    ("Manchester Royal Infirmary",             53.4622, -2.2261),
    ("Salford Royal Hospital",                 53.4867, -2.3420),
    ("Wythenshawe Hospital Manchester",        53.3883, -2.2953),
    ("Royal Liverpool Hospital",               53.4065, -2.9682),
    ("Aintree University Hospital",            53.4695, -2.9627),
    ("Alder Hey Children's Hospital",          53.4204, -2.9219),
    ("Blackpool Victoria Hospital",            53.8180, -3.0504),
    ("Royal Preston Hospital",                 53.7607, -2.7040),
    ("Stoke Royal Hospital",                   53.0006, -2.1804),
    # West Midlands
    ("University Hospital Birmingham",         52.4527, -1.9434),
    ("Birmingham City Hospital",               52.5088, -1.9745),
    ("Heartlands Hospital Birmingham",         52.4758, -1.8275),
    ("Walsall Manor Hospital",                 52.5859, -1.9716),
    ("University Hospital Coventry",           52.3970, -1.4983),
    ("New Cross Hospital Wolverhampton",       52.5851, -2.1420),
    # East Midlands
    ("Nottingham City Hospital",               52.9797, -1.1771),
    ("Queen's Medical Centre Nottingham",      52.9427, -1.1852),
    ("University Hospitals of Leicester",      52.6273, -1.1371),
    ("Derby Royal Hospital",                   52.9153, -1.4765),
    ("Lincoln County Hospital",                53.2374, -0.5343),
    # East of England
    ("Addenbrooke's Hospital Cambridge",       52.1762,  0.1410),
    ("Norfolk and Norwich Hospital",           52.6218,  1.2390),
    # South East
    ("John Radcliffe Hospital Oxford",         51.7625, -1.2203),
    ("Southampton General Hospital",           50.9339, -1.4244),
    ("Portsmouth QA Hospital",                 50.8499, -1.0626),
    ("Royal Sussex County Hospital Brighton",  50.8222, -0.1401),
    ("East Kent Hospitals",                    51.2770,  1.0894),
    ("Royal Berkshire Hospital Reading",       51.4517, -0.9740),
    # London
    ("University College Hospital London",     51.5254, -0.1356),
    ("Guy's Hospital London",                  51.5034, -0.0879),
    ("King's College Hospital London",         51.4681, -0.0938),
    ("St Thomas' Hospital London",             51.4991, -0.1187),
    ("St George's Hospital London",            51.4282, -0.1756),
    ("Royal Free Hospital London",             51.5530, -0.1659),
    ("Royal London Hospital Whitechapel",      51.5186, -0.0594),
    ("Charing Cross Hospital",                 51.4893, -0.2196),
    # South West
    ("Bristol Royal Infirmary",                51.4583, -2.5983),
    ("Southmead Hospital Bristol",             51.5017, -2.6043),
    ("Royal United Hospital Bath",             51.3890, -2.3793),
    ("Derriford Hospital Plymouth",            50.4189, -4.1201),
    ("Royal Devon and Exeter Hospital",        50.7299, -3.5264),
    ("Bournemouth Royal Hospital",             50.7249, -1.8341),
    # Wales
    ("University Hospital of Wales Cardiff",   51.4851, -3.1914),
    ("Morriston Hospital Swansea",             51.6570, -3.9307),
    # Scotland
    ("Royal Infirmary of Edinburgh",           55.9201, -3.1357),
    ("Western General Hospital Edinburgh",     55.9601, -3.2205),
    ("Glasgow Royal Infirmary",                55.8638, -4.2391),
    ("Queen Elizabeth Univ Hospital Glasgow",  55.8584, -4.3162),
    ("Aberdeen Royal Infirmary",               57.1571, -2.1270),
    ("Ninewells Hospital Dundee",              56.4637, -3.0076),
    # Northern Ireland
    ("Belfast City Hospital",                  54.5882, -5.9464),
    ("Royal Victoria Hospital Belfast",        54.5972, -5.9678),
]

UK_STATIONS = [
    # Yorkshire
    ("Leeds",                   53.7956, -1.5487),
    ("Sheffield",               53.3779, -1.4634),
    ("York",                    53.9581, -1.0929),
    ("Hull",                    53.7440, -0.3462),
    ("Bradford Interchange",    53.7921, -1.7518),
    ("Huddersfield",            53.6476, -1.7849),
    ("Halifax",                 53.7225, -1.8608),
    ("Wakefield Westgate",      53.6806, -1.5015),
    ("Doncaster",               53.5219, -1.1341),
    ("Barnsley",                53.5576, -1.4786),
    ("Rotherham Central",       53.4349, -1.3628),
    # North East
    ("Newcastle",               54.9683, -1.6175),
    ("Sunderland",              54.9058, -1.3791),
    ("Durham",                  54.7774, -1.5730),
    ("Darlington",              54.5235, -1.5533),
    ("Middlesbrough",           54.5773, -1.2349),
    # North West
    ("Manchester Piccadilly",   53.4773, -2.2310),
    ("Manchester Victoria",     53.4841, -2.2416),
    ("Liverpool Lime Street",   53.4069, -2.9777),
    ("Preston",                 53.7574, -2.7066),
    ("Blackpool North",         53.8218, -3.0524),
    ("Wigan North Western",     53.5460, -2.6347),
    ("Bolton",                  53.5776, -2.4296),
    ("Lancaster",               54.0474, -2.8068),
    ("Carlisle",                54.8888, -2.9331),
    ("Crewe",                   53.0885, -2.4346),
    ("Chester",                 53.1896, -2.8802),
    ("Stoke-on-Trent",          53.0047, -2.1758),
    # Midlands
    ("Birmingham New Street",   52.4776, -1.9004),
    ("Wolverhampton",           52.5897, -2.1241),
    ("Coventry",                52.4013, -1.5110),
    ("Nottingham",              52.9479, -1.1454),
    ("Leicester",               52.6333, -1.1257),
    ("Derby",                   52.9145, -1.4762),
    ("Lincoln",                 53.2270, -0.5422),
    ("Shrewsbury",              52.7113, -2.7547),
    # East
    ("Cambridge",               52.1940,  0.1373),
    ("Norwich",                 52.6274,  1.3079),
    ("Peterborough",            52.5765, -0.2509),
    # London
    ("London Euston",           51.5281, -0.1338),
    ("London King's Cross",     51.5307, -0.1228),
    ("London Paddington",       51.5154, -0.1755),
    ("London Liverpool St",     51.5178, -0.0823),
    ("London Waterloo",         51.5031, -0.1132),
    ("London Victoria",         51.4952, -0.1439),
    ("London Bridge",           51.5054, -0.0864),
    # South East
    ("Reading",                 51.4587, -0.9718),
    ("Oxford",                  51.7534, -1.2690),
    ("Southampton Central",     50.9094, -1.4142),
    ("Portsmouth Harbour",      50.7990, -1.1071),
    ("Brighton",                50.8290, -0.1414),
    ("Gatwick Airport",         51.1565, -0.1619),
    # South West
    ("Bristol Temple Meads",    51.4491, -2.5813),
    ("Bath Spa",                51.3786, -2.3600),
    ("Exeter St Davids",        50.7270, -3.5347),
    ("Plymouth",                50.3780, -4.1430),
    ("Bournemouth",             50.7259, -1.8758),
    # Wales
    ("Cardiff Central",         51.4762, -3.1792),
    ("Swansea",                 51.6213, -3.9436),
    # Scotland
    ("Edinburgh Waverley",      55.9518, -3.1903),
    ("Glasgow Central",         55.8584, -4.2573),
    ("Aberdeen",                57.1439, -2.0975),
    ("Dundee",                  56.4572, -2.9699),
    ("Inverness",               57.4772, -4.2276),
    # Northern Ireland
    ("Belfast Central",         54.5974, -5.9169),
]

# Estimated monthly room rent (£) by outcode alpha-prefix
OUTCODE_ROOM_RENTS: dict[str, int] = {
    # London inner
    "E": 950, "EC": 1050, "N": 900, "NW": 900,
    "SE": 900, "SW": 950, "W": 1000, "WC": 1050,
    # London outer
    "BR": 750, "CR": 700, "DA": 700, "EN": 700,
    "HA": 700, "IG": 700, "KT": 750, "RM": 700,
    "SM": 700, "TW": 750, "UB": 700, "WD": 720,
    # South East
    "AL": 650, "BN": 600, "CT": 550, "GU": 650,
    "HP": 650, "LU": 600, "ME": 550, "MK": 580,
    "OX": 700, "PO": 600, "RG": 650, "RH": 620,
    "SG": 640, "SL": 700, "SN": 560, "SO": 600,
    "SS": 600, "TN": 580,
    # South West
    "BA": 580, "BH": 580, "BS": 600, "DT": 500,
    "EX": 520, "GL": 560, "PL": 490, "TA": 480,
    "TQ": 510, "TR": 490,
    # East
    "CB": 680, "CM": 600, "CO": 550, "IP": 520,
    "NR": 500, "PE": 500,
    # East Midlands
    "CV": 500, "DE": 480, "LE": 500, "LN": 460,
    "NG": 500, "NN": 480,
    # West Midlands
    "B": 520, "DY": 480, "ST": 460, "TF": 450,
    "WR": 470, "WS": 480, "WV": 480,
    # Yorkshire & Humber
    "BD": 430, "DN": 430, "HD": 420, "HG": 450,
    "HU": 430, "HX": 420, "LS": 520, "S": 460,
    "WF": 430, "YO": 460,
    # North West
    "BB": 420, "BL": 430, "CA": 430, "CH": 450,
    "CW": 450, "FY": 420, "L": 480, "LA": 440,
    "M": 600, "OL": 430, "PR": 430, "SK": 480,
    "WA": 450, "WN": 430,
    # North East
    "DH": 380, "DL": 380, "NE": 400, "SR": 380, "TS": 390,
    # Wales
    "CF": 460, "LD": 400, "LL": 420, "NP": 440,
    "SA": 430, "SY": 420,
    # Scotland
    "AB": 490, "DD": 460, "DG": 400, "EH": 600,
    "FK": 450, "G": 520, "IV": 420, "KA": 420,
    "KY": 450, "ML": 420, "PA": 430, "PH": 440, "TD": 400,
    # Northern Ireland
    "BT": 420,
}

# ── Scoring breakpoints ───────────────────────────────────────────────────────
# Each list is (threshold, score); first threshold that the value is ≤ (for
# distance / price) or ≥ (for yield / density) wins; fallback is 0.

_UNI_BP    = [(1, 10), (2, 9), (3, 8), (5, 6), (8, 4), (12, 2)]   # km
_HOSP_BP   = [(1, 10), (2, 8), (3, 6), (5, 4), (8, 2)]             # km
_STA_BP    = [(0.5, 10), (1, 9), (2, 7), (3, 5), (5, 3), (8, 1)]  # km
_YIELD_BP  = [(15, 10), (12, 8), (10, 6), (8, 4), (6, 2)]          # % gross yield
_DENS_BP   = [(20, 10), (15, 8), (10, 6), (5, 4), (2, 2)]          # % already-HMO
_AFFORD_BP = [                                                       # £ median
    (100_000, 10), (130_000, 9), (150_000, 8), (170_000, 7),
    (190_000, 6),  (210_000, 5), (220_000, 4), (250_000, 3), (300_000, 1),
]

_SCORE_WEIGHTS = {
    "university":    0.30,
    "yield":         0.25,
    "hmo_density":   0.20,
    "transport":     0.15,
    "hospital":      0.07,
    "affordability": 0.03,
}

# URL collection concurrency
URL_CONCURRENCY = 20

# Detail scraping concurrency per worker process
# 8 workers × 20 = 160 total connections — aggressive but not likely to trigger 429s
START_CONCURRENCY = 20
MAX_CONCURRENCY   = 30


USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/122 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Safari/605.1.15",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:122.0) Firefox/122.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/121 Safari/537.36",
]

AUCTION_RE = re.compile(r"\bauction\b", re.IGNORECASE)
HMO_RE     = re.compile(r"\bH\.?\s*M\.?\s*O\.?\b", re.IGNORECASE)
TAG_RE     = re.compile(r"<[^>]+>")


# ============================================================
# HELPERS
# ============================================================

def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    return R * 2 * atan2(sqrt(a), sqrt(1 - a))


def _nearest_km(lat: float, lon: float, pois: list) -> float:
    """Minimum haversine distance in km from (lat, lon) to any POI in list."""
    return min(haversine_km(lat, lon, p[1], p[2]) for p in pois)


def _score_lower_better(value: float, breakpoints: list) -> int:
    """Return score where a lower value is better (distances, prices)."""
    for threshold, score in breakpoints:
        if value <= threshold:
            return score
    return 0


def _score_higher_better(value: float, breakpoints: list) -> int:
    """Return score where a higher value is better (yield, density)."""
    for threshold, score in sorted(breakpoints, reverse=True):
        if value >= threshold:
            return score
    return 0


def _get_room_rent(outcode: str) -> int:
    """Look up estimated monthly room rent (£) for an outcode."""
    prefix = re.match(r"^([A-Z]+)", (outcode or "").upper())
    if not prefix:
        return 420
    key = prefix.group(1)
    # Try 2-letter prefix first (e.g. "NW"), then 1-letter fallback
    return OUTCODE_ROOM_RENTS.get(key, OUTCODE_ROOM_RENTS.get(key[:1], 420))


def html_to_text(s: str) -> str:
    if not s:
        return ""
    s = TAG_RE.sub(" ", s)
    s = s.replace("&nbsp;", " ").replace("&amp;", "&")
    return re.sub(r"\s+", " ", s).strip()


def get_description_text_from_model(model: dict) -> str:
    if not model:
        return ""
    prop = model.get("propertyData") or {}
    candidates = [
        prop.get("text") or {},
        prop.get("details") or {},
        prop,
        model.get("propertyData") or {},
    ]
    keys_to_try = [
        "description", "propertyDescription", "fullDescription",
        "summaryDescription", "marketingDescription", "shortDescription",
        "htmlDescription", "descriptionHtml",
    ]
    for c in candidates:
        if isinstance(c, dict):
            for k in keys_to_try:
                v = c.get(k)
                if isinstance(v, str) and v.strip():
                    return html_to_text(v)
    return ""


# ============================================================
# STATE
# ============================================================

def load_state():
    if STATE_FILE.exists():
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                d = json.load(f)
        except Exception:
            d = {}
    else:
        d = {}
    return {
        "completed_outcodes": set(d.get("completed_outcodes", [])),
        "collected_urls":     set(d.get("collected_urls", [])),
        "seen_urls":          set(d.get("seen_urls", [])),
    }


def save_state(state):
    tmp = str(STATE_FILE) + ".tmp"
    data = {
        "completed_outcodes": sorted(state["completed_outcodes"]),
        "collected_urls":     sorted(state["collected_urls"]),
        "seen_urls":          sorted(state["seen_urls"]),
    }
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, STATE_FILE)


# ============================================================
# LOAD OUTCODES
# ============================================================

def load_outcodes():
    df = pd.read_csv(OUTCODE_CSV)
    outcodes = [f"OUTCODE^{int(x)}" for x in df["OuterCode"]]
    print(f"Loaded {len(outcodes)} outcode identifiers")
    return outcodes


# ============================================================
# URL COLLECTION  (HTML-based, no browser)
# ============================================================

# Matches /properties/12345678 hrefs in search result HTML
PROPERTY_ID_RE = re.compile(r'href="(/properties/(\d+))[^"]*"')

SEARCH_URL = "https://www.rightmove.co.uk/property-for-sale/find.html"


async def fetch_search_page(session, outcode_id, index):
    """
    Fetch a search results HTML page and extract property URLs via regex.
    Rightmove server-renders the property card links in the initial HTML,
    so no browser is needed.
    """
    params = {
        "locationIdentifier": outcode_id,
        "sortType":           1,
        "index":              index,
    }
    headers = {
        "User-Agent":      random.choice(USER_AGENTS),
        "Accept":          "text/html,application/xhtml+xml",
        "Accept-Language": "en-GB,en;q=0.9",
        "Referer":         "https://www.rightmove.co.uk/",
    }
    for attempt in range(3):
        try:
            async with session.get(SEARCH_URL, params=params, headers=headers) as resp:
                if resp.status in (429, 403, 503):
                    await asyncio.sleep(3.0 + attempt * 3.0 + random.uniform(0, 1))
                    continue
                if resp.status != 200:
                    return set(), resp.status, 0

                html = await resp.text()
                ids  = {m.group(2) for m in PROPERTY_ID_RE.finditer(html)}
                urls = {f"https://www.rightmove.co.uk/properties/{i}" for i in ids}
                return urls, resp.status, 0

        except Exception as e:
            if attempt == 0:
                print(f"\nSearch page error for {outcode_id} index={index}: {e}")
            await asyncio.sleep(0.5 + attempt * 0.5)
    return set(), None, 0


async def collect_urls_for_outcode(session, outcode_id, sem):
    async with sem:
        all_urls = set()
        for page_idx in range(MAX_PAGES):
            index = page_idx * PAGE_SIZE
            urls, status, _ = await fetch_search_page(session, outcode_id, index)

            if status in (None, 403, 503):
                return outcode_id, all_urls, False

            new_urls = urls - all_urls
            if not new_urls:
                # Empty page or all duplicates — end of results
                break

            all_urls.update(new_urls)

            # If the page returned fewer than a full page, this was the last page
            if len(urls) < PAGE_SIZE:
                break

            await asyncio.sleep(random.uniform(0.3, 0.7))

        return outcode_id, all_urls, True


async def collect_all_urls(outcodes, state):
    collected = state["collected_urls"]
    seen      = state["seen_urls"]
    completed = state["completed_outcodes"]

    connector = aiohttp.TCPConnector(limit=100, ssl=False)
    timeout   = aiohttp.ClientTimeout(total=30)
    sem       = asyncio.Semaphore(URL_CONCURRENCY)

    total     = len(outcodes)
    done      = 0
    start     = time.time()

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = [collect_urls_for_outcode(session, oc, sem) for oc in outcodes]

        for coro in asyncio.as_completed(tasks):
            outcode_id, urls, ok = await coro
            done += 1

            if ok:
                new = {u for u in urls if u not in seen and u not in collected}
                collected.update(new)
                completed.add(outcode_id)

            if done % 25 == 0:
                save_state(state)

            elapsed = time.time() - start
            eta = (elapsed / done * total) - elapsed if done > 5 else 0
            eta = min(max(eta, 0), 7200)
            pct = int(done / total * 100)
            fill = int(30 * done / total)
            bar  = "#" * fill + "-" * (30 - fill)
            print(
                f"[URLs {bar}] {pct}% ({done}/{total}) "
                f"| PENDING={len(collected):,} "
                f"| ETA={int(eta//60):02d}:{int(eta%60):02d}",
                end="\r", flush=True,
            )

            if time_is_up():
                print("\nTime limit reached during URL collection — saving state.")
                break

    save_state(state)
    print(f"\nURL collection done — {len(collected):,} pending URLs")


# ============================================================
# DETAIL SCRAPER  (unchanged from v4)
# ============================================================

def extract_page_model(html: str):
    key = "window.PAGE_MODEL"
    pos = html.find(key)
    if pos == -1:
        return None
    eq = html.find("=", pos)
    if eq == -1:
        return None
    start = html.find("{", eq)
    if start == -1:
        return None

    depth, in_str, esc = 0, False, False
    for i in range(start, len(html)):
        ch = html[i]
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == '"':
                in_str = False
            continue
        if ch == '"':
            in_str = True
        elif ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                try:
                    return json.loads(html[start:i+1])
                except Exception:
                    return None
    return None


def normalize_row(url, model, status, description_text=""):
    desc = description_text or ""
    potential_auction = bool(AUCTION_RE.search(desc))
    potential_hmo     = bool(HMO_RE.search(desc))

    if not model:
        return {
            "url": url, "address": None, "postcode": None,
            "price": None, "bedrooms": None, "property_type": None,
            "floor_area": None, "latitude": None, "longitude": None,
            "status": int(status) if str(status).isdigit() else None,
            "potential_auction": potential_auction,
            "potential_hmo": potential_hmo,
        }

    prop      = model.get("propertyData", {}) or {}
    analytics = (model.get("analyticsInfo", {}) or {}).get("analyticsProperty", {}) or {}
    addr      = prop.get("address", {}) or {}
    outcode   = addr.get("outcode", "") or ""
    incode    = addr.get("incode", "") or ""

    fa = None
    for s in prop.get("sizings", []) or []:
        if s.get("display"):
            fa = s["display"]
            break

    loc = prop.get("location") or {}
    lat = loc.get("latitude") or loc.get("lat")
    lon = loc.get("longitude") or loc.get("lng")
    if lat is None or lon is None:
        mv  = model.get("mapView") or model.get("map") or {}
        lat = mv.get("latitude") or mv.get("lat") or lat
        lon = mv.get("longitude") or mv.get("lng") or lon
    if lat is None or lon is None:
        lat = analytics.get("latitude") or analytics.get("lat") or lat
        lon = analytics.get("longitude") or analytics.get("lng") or lon

    return {
        "url":              url,
        "address":          addr.get("displayAddress"),
        "postcode":         analytics.get("postcode") or f"{outcode} {incode}".strip(),
        "price":            analytics.get("price"),
        "bedrooms":         prop.get("bedrooms"),
        "property_type":    prop.get("propertySubType") or prop.get("propertyType"),
        "floor_area":       fa,
        "latitude":         lat,
        "longitude":        lon,
        "status":           int(status) if str(status).isdigit() else None,
        "potential_auction": potential_auction,
        "potential_hmo":    potential_hmo,
    }


async def fetch_detail(session, url):
    headers = {
        "User-Agent":      random.choice(USER_AGENTS),
        "Accept-Language": "en-GB,en;q=0.9",
    }
    for attempt in range(4):
        try:
            async with session.get(url, headers=headers) as resp:
                status = resp.status
                if status != 200:
                    if attempt == 0:
                        print(f"\nERROR {status} for {url}")
                    wait = (2.0 + attempt * 2.5 + random.uniform(0, 1)) if status in (429, 403, 503) \
                           else (0.25 + attempt * 0.3)
                    await asyncio.sleep(wait)
                    continue

                html  = await resp.text()
                model = extract_page_model(html)
                desc  = get_description_text_from_model(model)
                if not desc:
                    m = re.search(r'property-description[^>]*>(.*?)</', html, re.IGNORECASE | re.DOTALL)
                    if m:
                        desc = html_to_text(m.group(1))
                if model is None and attempt == 0:
                    print(f"\nPAGE_MODEL not found for {url} (len={len(html):,})")
                return normalize_row(url, model, status, description_text=desc)

        except Exception as e:
            if attempt == 0:
                print(f"\nNETWORK ERROR for {url}: {e}")
            await asyncio.sleep(0.3 + attempt * 0.4)

    return normalize_row(url, None, "fail", description_text="")


async def scrape_details(urls, label, concurrency, result_list, state, progress_queue=None):
    """
    Scrape detail pages. If progress_queue is provided (multiprocessing mode),
    progress updates are sent through it instead of printed directly.
    """
    urls  = list(set(urls))
    total = len(urls)
    if not total:
        return []

    if progress_queue is None:
        print(f"\nStarting {label} — {total:,} URLs")

    completed, errors = 0, 0
    retry_urls = []
    start_time = time.time()

    sem       = asyncio.Semaphore(concurrency)
    connector = aiohttp.TCPConnector(limit=300, limit_per_host=25, ssl=False)
    timeout   = aiohttp.ClientTimeout(total=25)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        async def run_one(u):
            nonlocal completed, errors
            async with sem:
                row = await fetch_detail(session, u)
                result_list.append(row)

                if row["status"] == 200:
                    state["seen_urls"].add(u)
                    state["collected_urls"].discard(u)
                else:
                    retry_urls.append(u)
                    errors += 1

                completed += 1
                if completed % 500 == 0 and progress_queue is None:
                    save_state(state)

                elapsed = time.time() - start_time
                frac    = completed / total
                eta     = (elapsed / max(frac, 1e-9)) - elapsed if completed > 50 else 0
                eta     = min(max(eta, 0), 7200)

                if progress_queue is not None:
                    progress_queue.put({
                        "_type":     "progress",
                        "label":     label,
                        "completed": completed,
                        "total":     total,
                        "errors":    errors,
                        "eta_secs":  int(eta),
                    })
                else:
                    fill = int(30 * frac)
                    bar  = "#" * fill + "-" * (30 - fill)
                    print(
                        f"[{label} {bar}] {int(frac*100)}% ({completed:,}/{total:,}) "
                        f"| ERR={errors:,} | ETA={int(eta//60):02d}:{int(eta%60):02d}",
                        end="\r", flush=True,
                    )

        for coro in asyncio.as_completed([run_one(u) for u in urls]):
            await coro
            if time_is_up():
                if progress_queue is None:
                    print(f"\nTime limit reached during {label} — saving state.")
                    save_state(state)
                return retry_urls

    if progress_queue is None:
        print(f"\n{label} done — {completed:,} rows, {errors:,} errors")
    return retry_urls


def detail_worker(urls, worker_id, result_queue, state_snapshot):
    """Runs in a worker process — own asyncio loop, sends rows + progress to result_queue."""
    async def _run():
        state = {
            "collected_urls":     set(state_snapshot["collected_urls"]),
            "seen_urls":          set(state_snapshot["seen_urls"]),
            "completed_outcodes": set(),
        }

        async def scrape_to_queue(urls_in, label, concurrency):
            rows: list = []
            retry = await scrape_details(
                urls_in, label, concurrency, rows, state,
                progress_queue=result_queue,
            )
            for row in rows:
                result_queue.put(row)
            return retry

        retry1 = await scrape_to_queue(urls,   f"W{worker_id}-P1", START_CONCURRENCY)
        retry2 = await scrape_to_queue(retry1, f"W{worker_id}-P2", 30) if retry1 else []
        final  = await scrape_to_queue(retry2, f"W{worker_id}-P3", 10) if retry2 else []

        if final:
            out = BASE_DIR / f"failed_urls_worker{worker_id}.txt"
            out.write_text("\n".join(final), encoding="utf-8")

        result_queue.put(f"DONE-{worker_id}")

    asyncio.run(_run())


# ============================================================
# PROGRESS TABLE  (rendered by the writer thread)
# ============================================================

def _render_table(workers: dict, table_printed: bool) -> None:
    n_rows = len(workers) + 2  # header + divider + one row per worker
    if table_printed:
        # Move cursor up to overwrite previous table
        sys.stdout.write(f"\033[{n_rows}A")

    header = f"{'Worker':<10} {'Pass':<5} {'Progress':<32} {'Done':>8} {'Total':>8} {'ERR':>5} {'ETA':>6}"
    sys.stdout.write(f"\033[2K{header}\n")
    sys.stdout.write(f"\033[2K{'-' * len(header)}\n")

    for wid in sorted(workers):
        w    = workers[wid]
        frac = w["completed"] / w["total"] if w["total"] else 0
        fill = int(28 * frac)
        bar  = "#" * fill + "-" * (28 - fill)
        eta  = w["eta_secs"]
        sys.stdout.write(
            f"\033[2K{w['label']:<10} {w['pass']:<5} [{bar}]"
            f" {w['completed']:>8,} {w['total']:>8,} {w['errors']:>5}"
            f" {int(eta//60):02d}:{int(eta%60):02d}\n"
        )
    sys.stdout.flush()


def writer_thread_func(result_queue, shard_dir, expected_done, state):
    """Collects rows + progress updates from workers, renders table, writes shards."""
    rows_buf     = []
    shard_index  = 0
    done_count   = 0
    processed    = 0
    workers      = {}       # worker_id -> progress state
    table_printed = False

    def flush():
        nonlocal shard_index
        if not rows_buf:
            return
        df    = pd.DataFrame(rows_buf)
        table = pa.Table.from_pandas(df, preserve_index=False)
        out   = shard_dir / f"rightmove_part_{shard_index:05d}.parquet"
        pq.write_table(table, out, compression=None)
        # Print below the progress table
        sys.stdout.write(f"Wrote shard -> {out.name} ({len(df):,} rows)\n")
        sys.stdout.flush()
        rows_buf.clear()
        shard_index += 1

    while True:
        item = result_queue.get()

        # ---- worker finished ----
        if isinstance(item, str) and item.startswith("DONE-"):
            done_count += 1
            if done_count >= expected_done:
                break
            continue

        # ---- progress update ----
        if isinstance(item, dict) and item.get("_type") == "progress":
            label = item["label"]
            # label is like "W3-P1" — extract worker id and pass
            parts = label.split("-")
            wid   = int(parts[0][1:])
            pass_ = parts[1] if len(parts) > 1 else "P1"
            workers[wid] = {
                "label":     label,
                "pass":      pass_,
                "completed": item["completed"],
                "total":     item["total"],
                "errors":    item["errors"],
                "eta_secs":  item["eta_secs"],
            }
            _render_table(workers, table_printed)
            table_printed = True
            continue

        # ---- property row ----
        rows_buf.append(item)
        url = item.get("url")
        if url and item.get("status") == 200:
            state["seen_urls"].add(url)
            state["collected_urls"].discard(url)

        processed += 1
        if len(rows_buf) >= SHARD_ROWS:
            flush()
        if processed % 2000 == 0:
            save_state(state)

    flush()
    save_state(state)
    print(f"\nDetail scraping complete — {processed:,} rows written")


# ============================================================
# PARQUET WRITER
# ============================================================

def final_parquet_name():
    ts = time.strftime("%Y-%m-%d")
    return DATA_DIR / f"rightmove_{ts}.parquet"


def consolidate_shards(shard_dir, output_path):
    shard_files = sorted(shard_dir.glob("*.parquet"))
    if not shard_files:
        print("No shards to consolidate.")
        return
    print(f"\nConsolidating {len(shard_files)} shards -> {output_path}")

    df = pd.concat(
        [pd.read_parquet(f) for f in shard_files],
        ignore_index=True,
    )
    df.to_parquet(output_path, index=False, compression="snappy")
    print(f"{len(df):,} rows written to {output_path}")


def flush_shard(rows, shard_dir, shard_index):
    df    = pd.DataFrame(rows)
    table = pa.Table.from_pandas(df, preserve_index=False)
    out   = shard_dir / f"rightmove_part_{shard_index:05d}.parquet"
    pq.write_table(table, out, compression=None)
    print(f"\nWrote shard -> {out} ({len(df):,} rows)")
    return shard_index + 1


# ============================================================
# POST-RUN HELPERS
# ============================================================

def clear_state():
    """Reset state.json and remove all parquet shards."""
    empty = {"completed_outcodes": [], "collected_urls": [], "seen_urls": []}
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(empty, f, indent=2)
    print("State cleared.")

    shards_root = PARQUET_DIR / "shards"
    if shards_root.exists():
        import shutil
        shutil.rmtree(shards_root)
        print(f"Shards directory removed.")


def push_to_github(parquet_path: Path):
    """Stage the output parquet and push to origin/main."""
    def run(cmd):
        result = subprocess.run(cmd, cwd=BASE_DIR, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"git error: {result.stderr.strip()}")
            return False
        return True

    datestamp = time.strftime("%Y-%m-%d")
    run(["git", "config", "user.email", "scraper@rightmove-dash"])
    run(["git", "config", "user.name",  "scraper"])
    run(["git", "add",    str(parquet_path)])
    committed = run(["git", "commit", "-m", f"data: scrape {datestamp}"])
    if not committed:
        print("Nothing to commit — parquet unchanged.")
        return
    if run(["git", "push"]):
        print(f"Pushed {parquet_path.name} to GitHub.")
    else:
        print("Push failed — check git credentials.")


# ============================================================
# HMO ANALYSIS & EMAIL
# ============================================================

def analyse_hmo_opportunities(df: pd.DataFrame) -> dict:
    """
    Identify HMO conversion candidates and score outcode hotspots.

    Candidate criteria:
      - 3+ bedrooms, house type (not flat), not already marketed as HMO

    Hotspot composite score (0–10, weighted sum of six signals):
      university proximity  30%  — tenant demand from students
      estimated gross yield 25%  — (room_rent × beds × 12) / price
      HMO market density    20%  — % of houses in outcode already listed as HMO
                                   (proves demand without implying saturation)
      transport proximity   15%  — distance to nearest major rail/metro station
      hospital proximity     7%  — demand from NHS staff / key workers
      affordability          3%  — median price vs £220 k cap
    """
    df = df.copy()
    df["price_num"] = pd.to_numeric(df["price"], errors="coerce")
    df["beds_num"]  = pd.to_numeric(df["bedrooms"], errors="coerce")
    df["is_house"]  = (
        df["property_type"].fillna("")
        .str.contains(_HOUSE_TYPE_PAT, case=False, regex=True)
    )
    df["outcode"] = df["postcode"].fillna("").str.split().str[0].str.upper()

    hmo_candidates = df[
        (df["beds_num"] >= 3) &
        df["is_house"] &
        (~df["potential_hmo"].fillna(False))
    ].copy()

    five_plus = df[df["beds_num"] >= 5].copy()

    # ── Per-outcode stats ─────────────────────────────────────────────────────
    all_houses = df[df["is_house"] & df["outcode"].notna() & (df["outcode"] != "")]

    # Centroid: average lat/lon of all scraped properties per outcode
    centroids = (
        all_houses.dropna(subset=["latitude", "longitude"])
        .groupby("outcode")
        .agg(lat=("latitude", "mean"), lon=("longitude", "mean"))
    )

    # HMO density: already-HMO houses as % of all houses in outcode
    hmo_counts   = all_houses[all_houses["potential_hmo"].fillna(False)].groupby("outcode").size()
    house_counts = all_houses.groupby("outcode").size()
    hmo_density  = (hmo_counts / house_counts * 100).fillna(0).rename("hmo_density_pct")

    valid_cands = hmo_candidates[
        hmo_candidates["outcode"].notna() & (hmo_candidates["outcode"] != "")
    ]
    agg = (
        valid_cands.groupby("outcode")
        .agg(
            opportunities=("url",       "count"),
            median_price =("price_num", "median"),
            avg_beds     =("beds_num",  "mean"),
            under_220k   =("price_num", lambda x: (x < HMO_PRICE_THRESHOLD).sum()),
        )
    )

    # ── Scoring ───────────────────────────────────────────────────────────────
    rows = []
    for oc, row in agg.iterrows():
        if oc not in centroids.index:
            continue
        lat, lon = centroids.loc[oc, "lat"], centroids.loc[oc, "lon"]
        med_price = row["median_price"]
        avg_beds  = row["avg_beds"]
        room_rent = _get_room_rent(oc)
        density   = hmo_density.get(oc, 0.0)

        # Gross yield estimate: (annual room income) / purchase price × 100
        est_yield = (
            (room_rent * avg_beds * 12) / med_price * 100
            if pd.notna(med_price) and med_price > 0 else 0.0
        )

        dist_uni  = _nearest_km(lat, lon, UK_UNIVERSITIES)
        dist_hosp = _nearest_km(lat, lon, UK_HOSPITALS)
        dist_sta  = _nearest_km(lat, lon, UK_STATIONS)

        s_uni    = _score_lower_better(dist_uni,   _UNI_BP)
        s_hosp   = _score_lower_better(dist_hosp,  _HOSP_BP)
        s_sta    = _score_lower_better(dist_sta,   _STA_BP)
        s_yield  = _score_higher_better(est_yield, _YIELD_BP)
        s_dens   = _score_higher_better(density,   _DENS_BP)
        s_afford = _score_lower_better(med_price or 999_999, _AFFORD_BP)

        composite = (
            s_uni    * _SCORE_WEIGHTS["university"]
            + s_yield  * _SCORE_WEIGHTS["yield"]
            + s_dens   * _SCORE_WEIGHTS["hmo_density"]
            + s_sta    * _SCORE_WEIGHTS["transport"]
            + s_hosp   * _SCORE_WEIGHTS["hospital"]
            + s_afford * _SCORE_WEIGHTS["affordability"]
        )

        rows.append({
            "outcode":       oc,
            "opportunities": int(row["opportunities"]),
            "median_price":  med_price,
            "avg_beds":      avg_beds,
            "under_220k":    int(row["under_220k"]),
            "hmo_density":   round(density, 1),
            "est_yield":     round(est_yield, 1),
            "dist_uni_km":   round(dist_uni, 1),
            "dist_hosp_km":  round(dist_hosp, 1),
            "dist_sta_km":   round(dist_sta, 1),
            "score_uni":     s_uni,
            "score_hosp":    s_hosp,
            "score_transport": s_sta,
            "score_yield":   s_yield,
            "score_density": s_dens,
            "score_afford":  s_afford,
            "composite_score": round(composite, 2),
        })

    hotspots = (
        pd.DataFrame(rows)
        .sort_values("composite_score", ascending=False)
        .head(20)
        .reset_index(drop=True)
    )

    # Attach outcode score to affordable candidates for email
    score_map = hotspots.set_index("outcode")["composite_score"].to_dict()
    affordable = hmo_candidates[
        hmo_candidates["price_num"] < HMO_PRICE_THRESHOLD
    ].copy()
    affordable["hmo_score"] = affordable["outcode"].map(score_map)
    affordable = affordable.sort_values(
        ["hmo_score", "price_num"], ascending=[False, True]
    )

    return {
        "total":          len(df),
        "hmo_candidates": hmo_candidates,
        "five_plus":      five_plus,
        "affordable":     affordable,
        "hotspots":       hotspots,
    }


def print_metrics_report(analysis: dict) -> None:
    cands     = analysis["hmo_candidates"]
    five_plus = analysis["five_plus"]
    aff       = analysis["affordable"]
    hot       = analysis["hotspots"]

    p = cands["price_num"]
    band_mid  = ((p >= HMO_PRICE_THRESHOLD) & (p < 350_000)).sum()
    band_high = (p >= 350_000).sum()

    print("\n" + "=" * 64)
    print("  HMO OPPORTUNITY ANALYSIS")
    print("=" * 64)
    print(f"  Total properties scraped      : {analysis['total']:>8,}")
    print(f"  HMO conversion candidates     : {len(cands):>8,}  (3+ bed house, non-HMO)")
    print(f"    Under £{HMO_PRICE_THRESHOLD:,}               : {len(aff):>8,}")
    print(f"    £{HMO_PRICE_THRESHOLD:,} – £350,000          : {band_mid:>8,}")
    print(f"    Over £350,000                : {band_high:>8,}")
    print(f"  5+ bed properties (all types) : {len(five_plus):>8,}")
    if len(cands) > 0 and p.notna().any():
        print(f"\n  Price stats (HMO candidates):")
        print(f"    Median  : £{p.median():>10,.0f}")
        print(f"    Mean    : £{p.mean():>10,.0f}")
        print(f"    Min     : £{p.min():>10,.0f}")
        print(f"    Max     : £{p.max():>10,.0f}")

    if len(hot) > 0:
        print(f"\n  TOP HMO HOTSPOTS  (ranked by composite investment score)")
        hdr = (
            f"  {'#':<3}  {'Area':<7}  {'Score':>5}  {'Count':>5}  "
            f"{'Median':>9}  {'Yield':>5}  {'HMO%':>4}  "
            f"{'Uni km':>6}  {'Sta km':>6}  {'<220k':>5}"
        )
        print(hdr)
        print("  " + "-" * (len(hdr) - 2))
        for _, row in hot.iterrows():
            med = f"£{row['median_price']:,.0f}" if pd.notna(row["median_price"]) else "N/A"
            print(
                f"  {_+1:<3}  {row['outcode']:<7}  {row['composite_score']:>5.2f}"
                f"  {row['opportunities']:>5,}  {med:>9}"
                f"  {row['est_yield']:>4.1f}%  {row['hmo_density']:>4.1f}%"
                f"  {row['dist_uni_km']:>6.1f}  {row['dist_sta_km']:>6.1f}"
                f"  {row['under_220k']:>5,}"
            )
    print("=" * 64 + "\n")


def _build_email_html(affordable: pd.DataFrame, hotspots: pd.DataFrame) -> str:
    date_str = time.strftime("%d %B %Y")

    hotspot_rows = ""
    for i, row in hotspots.iterrows():
        med    = f"£{row['median_price']:,.0f}" if pd.notna(row["median_price"]) else "—"
        score  = row["composite_score"]
        # Colour-code score: green ≥7, amber ≥4, red <4
        sc     = int(score * 10)
        colour = "#27ae60" if score >= 7 else ("#e67e22" if score >= 4 else "#c0392b")
        hotspot_rows += (
            f"<tr>"
            f"<td>{i+1}</td>"
            f"<td><b>{row['outcode']}</b></td>"
            f"<td style='color:{colour};font-weight:bold'>{score:.2f}</td>"
            f"<td>{int(row['opportunities']):,}</td>"
            f"<td>{med}</td>"
            f"<td>{row['est_yield']:.1f}%</td>"
            f"<td>{row['hmo_density']:.1f}%</td>"
            f"<td>{row['dist_uni_km']:.1f} km</td>"
            f"<td>{row['dist_sta_km']:.1f} km</td>"
            f"<td>{int(row['under_220k']):,}</td>"
            f"</tr>\n"
        )

    property_rows = ""
    for _, r in affordable.head(100).iterrows():
        price  = f"£{int(r['price_num']):,}" if pd.notna(r["price_num"]) else "—"
        beds   = str(int(r["beds_num"])) if pd.notna(r["beds_num"]) else "?"
        addr   = r.get("address") or "View listing"
        ptype  = r.get("property_type") or "—"
        pc     = r.get("postcode") or "—"
        score  = r.get("hmo_score")
        score_str = f"{score:.1f}" if pd.notna(score) else "—"
        auction_flag = " &#9873;" if r.get("potential_auction") else ""
        property_rows += (
            f"<tr>"
            f"<td><a href='{r['url']}'>{addr}{auction_flag}</a></td>"
            f"<td>{pc}</td><td>{price}</td><td>{beds}</td><td>{ptype}</td>"
            f"<td style='text-align:center'>{score_str}</td>"
            f"</tr>\n"
        )

    return f"""<html><body style="font-family:Arial,sans-serif;color:#222;max-width:960px;margin:auto;">
<h2 style="color:#c0392b;border-bottom:2px solid #c0392b;padding-bottom:6px;">
  HMO Opportunity Report &mdash; {date_str}
</h2>
<p>HMO conversion candidates (3+ bed house, not already HMO) priced under
<b>£{HMO_PRICE_THRESHOLD:,}</b>. Properties are sorted by area investment score, then price.</p>
<p><b>{len(affordable):,} properties</b> meet the criteria.</p>

<h3 style="margin-top:1.8em;">Top HMO Hotspots</h3>
<p style="font-size:0.85em;color:#555;">
  Composite score (0–10) weights: university proximity 30% · estimated gross yield 25% ·
  HMO market density 20% · transport 15% · hospital proximity 7% · affordability 3%.<br>
  HMO density = % of houses in that outcode already listed as HMO — signals proven tenant demand.
</p>
<table border="1" cellpadding="7" cellspacing="0"
       style="border-collapse:collapse;width:100%;font-size:0.88em;">
  <tr style="background:#2c3e50;color:#fff;">
    <th>#</th><th>Area</th><th>Score</th><th>Listings</th>
    <th>Median Price</th><th>Est. Yield</th><th>HMO Density</th>
    <th>Uni Dist</th><th>Station Dist</th><th>Under £220k</th>
  </tr>
  {hotspot_rows}
</table>

<h3 style="margin-top:2em;">Top 100 Properties Under £{HMO_PRICE_THRESHOLD:,}</h3>
<p style="font-size:0.85em;color:#555;">
  Showing top 100 of {len(affordable):,} total candidates, ranked by area score then price.
  &#9873; = auction listing &nbsp;|&nbsp; Score = outcode composite score
</p>
<table border="1" cellpadding="7" cellspacing="0"
       style="border-collapse:collapse;width:100%;font-size:0.88em;">
  <tr style="background:#c0392b;color:#fff;">
    <th>Address</th><th>Postcode</th><th>Price</th><th>Beds</th><th>Type</th><th>Score</th>
  </tr>
  {property_rows}
</table>

<p style="color:#999;margin-top:2em;font-size:0.8em;">
  Generated by Rightmove HMO Scraper &mdash; {time.strftime('%Y-%m-%d %H:%M')} UTC<br>
  Yield is estimated gross (room rent × beds × 12 ÷ price). Room rents are regional averages.
</p>
</body></html>"""


def send_hmo_email(analysis: dict) -> None:
    """Send HMO opportunity email via Gmail SMTP (needs GMAIL_USER + GMAIL_APP_PASSWORD env vars)."""
    gmail_user = os.environ.get("GMAIL_USER", "").strip()
    gmail_pass = os.environ.get("GMAIL_APP_PASSWORD", "").strip()

    if not gmail_user or not gmail_pass or not HMO_EMAIL_TO:
        print("GMAIL_USER / GMAIL_APP_PASSWORD / HMO_EMAIL_TO not set — skipping HMO email.")
        return

    affordable = analysis["affordable"]
    if affordable.empty:
        print("No properties under £{:,} found — skipping email.".format(HMO_PRICE_THRESHOLD))
        return

    msg = MIMEMultipart("alternative")
    msg["Subject"] = (
        f"HMO Opportunities Under £{HMO_PRICE_THRESHOLD:,} "
        f"({len(affordable):,} properties) — {time.strftime('%d %b %Y')}"
    )
    msg["From"] = gmail_user
    msg["To"]   = HMO_EMAIL_TO

    html = _build_email_html(affordable, analysis["hotspots"])
    msg.attach(MIMEText(html, "html"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(gmail_user, gmail_pass)
            smtp.sendmail(gmail_user, HMO_EMAIL_TO, msg.as_string())
        print(f"HMO email sent to {HMO_EMAIL_TO} — {len(affordable):,} properties listed.")
    except Exception as exc:
        print(f"Email send failed: {exc}")


# ============================================================
# MAIN
# ============================================================

async def run():
    # Clean data/ so old parquet files don't accumulate
    for f in DATA_DIR.glob("*.parquet"):
        f.unlink()

    state = load_state()

    if REBUILD_FROM_ALL:
        print("Rebuilding collected_urls from collected_urls + seen_urls")
        all_urls = state["collected_urls"] | state["seen_urls"]
        state["collected_urls"] = set(all_urls)
        state["seen_urls"].clear()
        save_state(state)

    outcodes  = load_outcodes()
    remaining = [oc for oc in outcodes if oc not in state["completed_outcodes"]]

    print(f"Outcodes remaining : {len(remaining)}")
    print(f"Seen URLs          : {len(state['seen_urls']):,}")
    print(f"Pending URLs       : {len(state['collected_urls']):,}")

    # ---- URL COLLECTION ----
    if remaining:
        await collect_all_urls(remaining, state)

    print(f"\nURL COLLECTION COMPLETE — {len(state['collected_urls']):,} pending")

    # ---- DETAIL SCRAPING ----
    pending = list(state["collected_urls"] - state["seen_urls"])
    if not pending:
        print("No pending URLs to scrape.")
        return

    print(f"\nStarting detail scraping — {len(pending):,} URLs across {DETAIL_WORKERS} workers")

    shard_dir = PARQUET_DIR / "shards" / time.strftime("%Y-%m-%d")
    shard_dir.mkdir(parents=True, exist_ok=True)

    # Snapshot of state to pass into worker processes (sets aren't shared across processes)
    state_snapshot = {
        "collected_urls": list(state["collected_urls"]),
        "seen_urls":      list(state["seen_urls"]),
    }

    chunks = [pending[i::DETAIL_WORKERS] for i in range(DETAIL_WORKERS)]
    result_q = Queue()

    writer_thread = Thread(
        target=writer_thread_func,
        args=(result_q, shard_dir, DETAIL_WORKERS, state),
        daemon=True,
    )
    writer_thread.start()

    procs = [
        Process(target=detail_worker, args=(chunk, i, result_q, state_snapshot))
        for i, chunk in enumerate(chunks)
    ]
    for p in procs:
        p.start()
    for p in procs:
        p.join()

    writer_thread.join()

    output = final_parquet_name()
    consolidate_shards(shard_dir, output)

    # ---- HMO ANALYSIS & EMAIL ----
    if output.exists():
        df_all   = pd.read_parquet(output)
        analysis = analyse_hmo_opportunities(df_all)
        print_metrics_report(analysis)
        send_hmo_email(analysis)

    # Only clean state + push if every URL was scraped (no pending left)
    still_pending = len(state["collected_urls"] - state["seen_urls"])
    if still_pending == 0:
        clear_state()
        # On GitHub Actions the workflow step handles the push
        if os.environ.get("GITHUB_ACTIONS"):
            print("\nAll URLs scraped — state cleared. Workflow will push parquet.")
        else:
            print("\nAll URLs scraped — clearing state and pushing to GitHub.")
            push_to_github(output)
    else:
        print(f"\n{still_pending:,} URLs still pending — state kept for resume.")

    print("\nALL DONE!")
    print(f"Output : {output}")
    print(f"Seen   : {len(state['seen_urls']):,}")
    print(f"Pending: {still_pending:,}")


def run_analyse_only():
    """Re-run HMO analysis on the most recent parquet without re-scraping."""
    parquets = sorted(DATA_DIR.glob("*.parquet"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not parquets:
        print("No parquet files found in data/ — run the scraper first.")
        return
    output = parquets[0]
    print(f"Analysing {output.name} …")
    df_all   = pd.read_parquet(output)
    analysis = analyse_hmo_opportunities(df_all)
    print_metrics_report(analysis)
    send_hmo_email(analysis)


async def run_retry_failed():
    state = load_state()
    failed = []
    for fn in os.listdir(BASE_DIR):
        if fn.startswith("failed_urls_worker") and fn.endswith(".txt"):
            with open(BASE_DIR / fn, "r", encoding="utf-8") as f:
                failed += [u.strip() for u in f if u.strip()]

    failed = [u for u in set(failed) if u not in state["seen_urls"]]
    if not failed:
        print("No failed URLs to retry.")
        return

    print(f"Retrying {len(failed):,} failed URLs...")
    rows_buf = []
    await scrape_details(failed, "RETRY", START_CONCURRENCY, rows_buf, state)

    if rows_buf:
        shard_dir = PARQUET_DIR / "shards" / time.strftime("%Y-%m-%d")
        shard_dir.mkdir(parents=True, exist_ok=True)
        flush_shard(rows_buf, shard_dir, 0)
        consolidate_shards(shard_dir, final_parquet_name())

    save_state(state)
    print("Retry complete!")


if __name__ == "__main__":
    freeze_support()
    if ANALYSE_ONLY:
        run_analyse_only()
    elif RETRY_FAILED:
        asyncio.run(run_retry_failed())
    else:
        asyncio.run(run())
