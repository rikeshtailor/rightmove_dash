# ============================================================
# scraper_alternative.py
# UNIFIED RIGHTMOVE + SPAREROOM SCRAPER
#
# Rightmove: browserless search-API URL collection + async detail scraping
# SpareRoom: async offered/wanted room listing scraper
# HMO Analysis: composite hotspot scoring + email report
#
# All data is written under data/
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
from math import radians, sin, cos, sqrt, atan2
from urllib.parse import urlparse, parse_qs
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

try:
    from bs4 import BeautifulSoup
    _BS4_AVAILABLE = True
except ImportError:
    _BS4_AVAILABLE = False

try:
    from shapely.geometry import shape, Point
    from shapely.strtree import STRtree
    _SHAPELY_AVAILABLE = True
except ImportError:
    _SHAPELY_AVAILABLE = False

# ============================================================
# CLI FLAGS
# ============================================================

IS_CI            = bool(os.environ.get("GITHUB_ACTIONS"))
RETRY_FAILED     = "--retry-failed"     in sys.argv
REBUILD_FROM_ALL = "--rebuild-from-all" in sys.argv
ANALYSE_ONLY     = "--analyse-only"     in sys.argv
SCRAPE_ONLY      = "--scrape-only"      in sys.argv
RIGHTMOVE_ONLY   = "--rightmove-only"   in sys.argv
SPAREROOM_ONLY   = "--spareroom-only"   in sys.argv

_time_limit_arg = next((sys.argv[i+1] for i, a in enumerate(sys.argv) if a == "--time-limit"), None)
TIME_LIMIT_SECS = int(_time_limit_arg) * 60 if _time_limit_arg else None
RUN_START       = time.time()

_workers_arg   = next((sys.argv[i+1] for i, a in enumerate(sys.argv) if a == "--workers"), None)
DETAIL_WORKERS = int(_workers_arg) if _workers_arg else min(os.cpu_count() or 4, 8)

_sr_conc_arg   = next((sys.argv[i+1] for i, a in enumerate(sys.argv) if a == "--spareroom-concurrency"), None)
SR_CONCURRENCY = int(_sr_conc_arg) if _sr_conc_arg else 30

_sr_csv_arg  = next((sys.argv[i+1] for i, a in enumerate(sys.argv) if a == "--spareroom-csv"), None)

def time_is_up() -> bool:
    return TIME_LIMIT_SECS is not None and (time.time() - RUN_START) >= TIME_LIMIT_SECS


# ============================================================
# CONFIG
# ============================================================

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

ARTICLE4_PARQUET      = DATA_DIR / "article4_areas.parquet"
ARTICLE4_MAX_AGE_DAYS = 7

SHARD_ROWS = 5000
STATE_FILE = BASE_DIR / "state.json"

OUTCODE_CSV = os.environ.get(
    "OUTCODE_CSV",
    r"C:\rightmove_monitor\scraper\outercode_to_postcode_master.csv",
)

SPAREROOM_CSV = (
    _sr_csv_arg
    or os.environ.get("SPAREROOM_CSV", "")
    or str(BASE_DIR / "postcodes.csv")
)

PAGE_SIZE = 24
MAX_PAGES = 50

# HMO analysis
HMO_EMAIL_TO        = os.environ.get("HMO_EMAIL_TO", "")
HMO_PRICE_THRESHOLD = 220_000
_HOUSE_TYPE_PAT     = r"detach|semi|terrace|bungalow|town.house|cottage|\bvilla\b|barn"
_LAND_TYPE_PAT      = r"\bland\b|plot|development\s+site"

# URL collection concurrency
URL_CONCURRENCY = 20

# Detail scraping concurrency per worker process
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

# Estimated monthly room rent (£) by outcode alpha-prefix (fallback when no SpareRoom data)
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

# ── Article 4 outcode overrides ──────────────────────────────────────────────
# Many councils with Article 4 HMO restrictions haven't submitted their
# boundaries to the national planning register (planning.data.gov.uk).
# List outcodes here that are known Article 4 areas to ensure they are
# excluded even when missing from the API dataset.
KNOWN_ARTICLE4_OUTCODES: set[str] = {
    # Middlesbrough (council-wide HMO Article 4 – not in national register)
    "TS1", "TS3", "TS4", "TS5",
    # Sunderland
    "SR1", "SR2", "SR4",
    # Hull
    "HU3", "HU5",
    # Nottingham (partial – NMC Article 4 covers city centre wards)
    "NG1", "NG7",
    # Sheffield (parts of S1–S10 under Article 4)
    "S1", "S2", "S3", "S6", "S10",
    # Leeds (Hyde Park / Headingley Article 4)
    "LS6",
    # Liverpool (parts of L6, L7)
    "L6", "L7",
    # Coventry
    "CV1",
    # Bristol (parts of BS6, BS7)
    "BS6", "BS7",
}

# ── Scoring breakpoints ───────────────────────────────────────────────────────
_UNI_BP    = [(1, 10), (2, 9), (3, 8), (5, 6), (8, 4), (12, 2)]
_HOSP_BP   = [(1, 10), (2, 8), (3, 6), (5, 4), (8, 2)]
_STA_BP    = [(0.5, 10), (1, 9), (2, 7), (3, 5), (5, 3), (8, 1)]
_YIELD_BP  = [(15, 10), (12, 8), (10, 6), (8, 4), (6, 2)]
_DENS_BP   = [(20, 10), (15, 8), (10, 6), (5, 4), (2, 2)]
_AFFORD_BP = [
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
    return min(haversine_km(lat, lon, p[1], p[2]) for p in pois)


def _score_lower_better(value: float, breakpoints: list) -> int:
    for threshold, score in breakpoints:
        if value <= threshold:
            return score
    return 0


def _score_higher_better(value: float, breakpoints: list) -> int:
    for threshold, score in sorted(breakpoints, reverse=True):
        if value >= threshold:
            return score
    return 0


def _get_room_rent(outcode: str) -> int:
    if not outcode or not isinstance(outcode, str):
        return 420
    prefix = re.match(r"^([A-Z]+)", outcode.upper())
    if not prefix:
        return 420
    key = prefix.group(1)
    return OUTCODE_ROOM_RENTS.get(key, OUTCODE_ROOM_RENTS.get(key[:1], 420))


def _safe_float(x) -> float | None:
    if x is None:
        return None
    try:
        v = float(x)
        return None if v != v else v  # NaN != NaN
    except Exception:
        return None


def _parse_floor_area_sqm(fa_str) -> float | None:
    """Parse '120 sq m', '1,200 sq ft', '120m²' etc. into square metres."""
    if not fa_str or str(fa_str).strip() in ("", "None", "nan"):
        return None
    s = str(fa_str).lower().replace(",", "")
    m = re.search(r"([\d.]+)\s*(?:sq\.?\s*m(?:etres?)?|m²|sqm)\b", s)
    if m:
        v = float(m.group(1))
        return v if 10 < v < 2_000 else None
    m = re.search(r"([\d.]+)\s*(?:sq\.?\s*f(?:ee|oo)?t|ft²|sqft)\b", s)
    if m:
        v = float(m.group(1)) * 0.0929
        return v if 10 < v < 2_000 else None
    return None


def _estimate_hmo_rooms(
    beds_num,
    floor_sqm,
    bathrooms=None,
    reception_rooms=None,
    has_ensuite: bool = False,
) -> tuple[int, int]:
    """
    Return (potential_rooms, ensuite_capable_rooms).

    Priority order for room count:
      1. Floor area (when scraped): usable = total * 0.65, room = 10 sq m each
      2. Known reception_rooms from keyFeatures: beds + receptions
      3. Bedroom-count heuristic: 3-bed → +1, 4-bed → +2, 5+ → +1

    Priority order for en-suite estimate:
      1. Floor area: rooms ≥ 14 sq m can take a wet room
      2. Bathrooms ≥ 2 or existing en-suite flag → 40% of rooms en-suite capable
      3. Default: 1 in 3 rooms
    """
    beds = int(beds_num) if pd.notna(beds_num) and beds_num > 0 else 3

    # ── Potential rooms ──────────────────────────────────────────────────────
    if floor_sqm and floor_sqm > 30:
        usable = floor_sqm * 0.65
        pot    = max(beds, min(int(usable / 10), beds + 4))
    elif reception_rooms is not None and pd.notna(reception_rooms):
        pot = beds + int(reception_rooms)
    else:
        bonus = 1 if beds <= 3 else (2 if beds == 4 else 1)
        pot   = beds + bonus

    # ── En-suite capable rooms ───────────────────────────────────────────────
    if floor_sqm and floor_sqm > 30:
        ensuite = min(int(floor_sqm * 0.65 / 14), pot)
    elif (bathrooms is not None and pd.notna(bathrooms) and bathrooms >= 2) or has_ensuite:
        ensuite = max(1, int(pot * 0.4))   # en-suite friendly property
    else:
        ensuite = max(1, pot // 3)

    return pot, ensuite


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
# RIGHTMOVE STATE
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
# RIGHTMOVE URL COLLECTION
# ============================================================

PROPERTY_ID_RE = re.compile(r'href="(/properties/(\d+))[^"]*"')
SEARCH_URL = "https://www.rightmove.co.uk/property-for-sale/find.html"


async def fetch_search_page(session, outcode_id, index):
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
                break
            all_urls.update(new_urls)
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

    total = len(outcodes)
    done  = 0
    start = time.time()

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

            if not IS_CI:
                elapsed = time.time() - start
                eta = (elapsed / done * total) - elapsed if done > 5 else 0
                eta = min(max(eta, 0), 7200)
                pct  = int(done / total * 100)
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
# RIGHTMOVE DETAIL SCRAPER
# ============================================================

def _extract_json_object(html: str, pos: int) -> dict | None:
    """Extract the JSON object starting at the first '{' after pos."""
    start = html.find("{", pos)
    if start == -1:
        return None
    depth, in_str, esc = 0, False, False
    for i in range(start, min(start + 400_000, len(html))):
        ch = html[i]
        if in_str:
            if esc:   esc = False
            elif ch == "\\": esc = True
            elif ch == '"':  in_str = False
            continue
        if   ch == '"': in_str = True
        elif ch == "{": depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                try:    return json.loads(html[start:i + 1])
                except: return None
    return None


def _decode_rightmove_compressed(outer: dict) -> dict | None:
    """
    Decode Rightmove's indexed-array JSON compression (window.__PAGE_MODEL).
    The outer object has a 'data' key whose value is a JSON-encoded array.
    Array[0] is the root template; integer values are indices into the array.
    Primitives stored at those indices are returned as-is; complex types recurse.
    """
    data_str = outer.get("data")
    if not isinstance(data_str, str):
        return None
    try:
        arr = json.loads(data_str)
    except Exception:
        return None

    def resolve(val, depth=0):
        if depth > 40:
            return val
        if isinstance(val, int) and 0 < val < len(arr):
            inner = arr[val]
            return resolve(inner, depth + 1) if isinstance(inner, (dict, list)) else inner
        if isinstance(val, dict):
            return {k: resolve(v, depth + 1) for k, v in val.items()}
        if isinstance(val, list):
            return [resolve(v, depth + 1) for v in val]
        return val

    try:
        return resolve(arr[0])
    except Exception:
        return None


def extract_page_model(html: str) -> dict | None:
    """
    Extract and decode the Rightmove page model.
    Supports both the old window.PAGE_MODEL and the new window.__PAGE_MODEL
    compressed-array format.
    """
    for key in ("window.__PAGE_MODEL", "window.PAGE_MODEL"):
        pos = html.find(key)
        if pos == -1:
            continue
        outer = _extract_json_object(html, pos)
        if not outer:
            continue
        # New compressed format
        if "data" in outer and isinstance(outer.get("data"), str):
            decoded = _decode_rightmove_compressed(outer)
            if decoded:
                return decoded
        # Old flat format
        if "propertyData" in outer or "analyticsInfo" in outer:
            return outer
    return None


_RECEPTION_RE = re.compile(r'(\d+)\s+reception', re.IGNORECASE)
_ENSUITE_RE   = re.compile(r'en.?suite', re.IGNORECASE)


def _parse_key_features(features) -> tuple[int | None, bool]:
    """
    Parse Rightmove keyFeatures list.
    Returns (reception_rooms, has_ensuite).
    reception_rooms is None when not mentioned explicitly.
    """
    if not features:
        return None, False
    text = " ".join(str(f) for f in features)
    m = _RECEPTION_RE.search(text)
    receptions = int(m.group(1)) if m else None
    return receptions, bool(_ENSUITE_RE.search(text))


def normalize_row(url, model, status, description_text=""):
    desc = description_text or ""
    potential_auction = bool(AUCTION_RE.search(desc))
    potential_hmo     = bool(HMO_RE.search(desc))

    _null = {
        "url": url, "address": None, "postcode": None,
        "price": None, "bedrooms": None, "bathrooms": None,
        "reception_rooms": None, "has_ensuite": False,
        "property_type": None, "floor_area": None,
        "latitude": None, "longitude": None,
        "status": int(status) if str(status).isdigit() else None,
        "potential_auction": potential_auction,
        "potential_hmo": potential_hmo,
    }
    if not model:
        return _null

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

    reception_rooms, has_ensuite = _parse_key_features(prop.get("keyFeatures"))

    return {
        "url":               url,
        "address":           addr.get("displayAddress"),
        "postcode":          analytics.get("postcode") or f"{outcode} {incode}".strip(),
        "price":             analytics.get("price"),
        "bedrooms":          prop.get("bedrooms"),
        "bathrooms":         prop.get("bathrooms"),
        "reception_rooms":   reception_rooms,
        "has_ensuite":       has_ensuite,
        "property_type":     prop.get("propertySubType") or prop.get("propertyType"),
        "floor_area":        fa,
        "latitude":          lat,
        "longitude":         lon,
        "status":            int(status) if str(status).isdigit() else None,
        "potential_auction": potential_auction,
        "potential_hmo":     potential_hmo,
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
                elif not IS_CI:
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
# RIGHTMOVE PROGRESS TABLE
# ============================================================

def _render_table(workers: dict, table_printed: bool) -> None:
    n_rows = len(workers) + 2
    if table_printed:
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
    rows_buf      = []
    shard_index   = 0
    done_count    = 0
    processed     = 0
    workers       = {}
    table_printed = False

    def flush():
        nonlocal shard_index
        if not rows_buf:
            return
        df  = pd.DataFrame(rows_buf)
        out = shard_dir / f"rightmove_part_{shard_index:05d}.parquet"
        df.to_parquet(out, index=False, compression=None)
        if not IS_CI:
            sys.stdout.write(f"Wrote shard -> {out.name} ({len(df):,} rows)\n")
            sys.stdout.flush()
        rows_buf.clear()
        shard_index += 1

    while True:
        item = result_queue.get()

        if isinstance(item, str) and item.startswith("DONE-"):
            done_count += 1
            if done_count >= expected_done:
                break
            continue

        if isinstance(item, dict) and item.get("_type") == "progress":
            label = item["label"]
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
            if not IS_CI:
                _render_table(workers, table_printed)
                table_printed = True
            continue

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
# RIGHTMOVE PARQUET HELPERS
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
    df  = pd.DataFrame(rows)
    out = shard_dir / f"rightmove_part_{shard_index:05d}.parquet"
    df.to_parquet(out, index=False, compression=None)
    print(f"\nWrote shard -> {out} ({len(df):,} rows)")
    return shard_index + 1


# ============================================================
# SPAREROOM SCRAPER
# ============================================================

SR_BASE_URL   = "https://www.spareroom.co.uk/flatshare/search.pl"
SR_PAGE_SIZE  = 200

_SR_HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept-Language": "en-GB,en;q=0.9",
    "Referer":         "https://www.spareroom.co.uk/",
}


def _sr_extract_flatshare_id(url: str) -> str | None:
    if not url:
        return None
    qs = parse_qs(urlparse(url).query)
    return qs.get("flatshare_id", [None])[0]



def _sr_load_state(state_file: Path) -> dict:
    if state_file.exists():
        try:
            with open(state_file, "r", encoding="utf-8") as f:
                d = json.load(f)
            return {
                "completed_postcodes": set(d.get("completed_postcodes", [])),
                "failed_postcodes":    list(d.get("failed_postcodes", [])),
            }
        except Exception:
            pass
    return {"completed_postcodes": set(), "failed_postcodes": []}


def _sr_save_state(state: dict, state_file: Path):
    data = {
        "completed_postcodes": sorted(list(state["completed_postcodes"])),
        "failed_postcodes":    state["failed_postcodes"],
    }
    with open(state_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def _sr_load_postcodes(csv_path: str) -> list[str]:
    df = pd.read_csv(csv_path)
    col = "Postcode" if "Postcode" in df.columns else df.columns[0]
    return df[col].dropna().astype(str).str.strip().str.upper().unique().tolist()



class _SpareRoomScraper:
    DOMAIN = "https://www.spareroom.co.uk"

    def __init__(self, postcode: str, session: aiohttp.ClientSession, flatshare_type: str):
        self.postcode       = postcode
        self.session        = session
        self.flatshare_type = flatshare_type

    async def _fetch_page(self, offset: int) -> tuple[int, str | None]:
        params = {
            "action":          "search",
            "flatshare_type":  self.flatshare_type,
            "search":          self.postcode,
            "max_per_page":    SR_PAGE_SIZE,
            "offset":          offset,
        }
        headers = dict(_SR_HEADERS)
        headers["User-Agent"] = random.choice(USER_AGENTS)
        try:
            async with self.session.get(SR_BASE_URL, params=params, headers=headers) as r:
                return r.status, (await r.text() if r.status == 200 else None)
        except Exception:
            return 0, None

    def _parse_listing(self, li, soup_module) -> dict | None:
        try:
            title     = li.find("h2",   class_="listing-card__title")
            link      = li.find("a",    class_="listing-card__link")
            price     = li.find("p",    class_="listing-card__price")
            location  = li.find("p",    class_="listing-card__location")
            room_type = li.find("span", class_="listing-card__room")
            avail     = li.find("span", class_="listing-card__availability")

            url = None
            if link and link.has_attr("href"):
                href = link["href"]
                url  = href if href.startswith("http") else (self.DOMAIN + href)

            flatshare_id = _sr_extract_flatshare_id(url)

            price_text = price.get_text(" ", strip=True) if price else ""
            pw_price = pcm_price = None
            if price_text:
                lt  = price_text.lower()
                tok = price_text.split()[0] if price_text.split() else None
                if tok:
                    if "pw"  in lt: pw_price  = tok
                    elif "pcm" in lt: pcm_price = tok

            return {
                "postcode":     self.postcode,
                "title":        title.get_text(strip=True) if title else None,
                "url":          url,
                "flatshare_id": flatshare_id,
                "pw_price":     pw_price,
                "pcm_price":    pcm_price,
                "location":     location.get_text(strip=True) if location else None,
                "room_type":    room_type.get_text(strip=True) if room_type else None,
                "available":    avail.get_text(strip=True) if avail else None,
            }
        except Exception:
            return None

    async def scrape_all(self) -> tuple[list[dict], int]:
        if not _BS4_AVAILABLE:
            print("bs4 not installed — skipping SpareRoom scrape. Run: pip install beautifulsoup4")
            return [], 0

        rows: list[dict] = []
        seen_ids: set[str] = set()
        last_status = 200
        offset = 0
        no_new_streak = 0

        for page in range(200):
            status, html = await self._fetch_page(offset)
            last_status = status or last_status
            if not html:
                break

            soup     = BeautifulSoup(html, "html.parser")
            listings = soup.find_all("li", class_="listing-result")
            if not listings:
                break

            new_this_page = 0
            for li in listings:
                row = self._parse_listing(li, BeautifulSoup)
                if not row:
                    continue
                fid = row.get("flatshare_id")
                if not fid or fid in seen_ids:
                    continue
                seen_ids.add(fid)
                rows.append(row)
                new_this_page += 1

            no_new_streak = 0 if new_this_page else no_new_streak + 1
            if no_new_streak >= 3:
                break

            offset += SR_PAGE_SIZE
            if page % 5 == 0:
                await asyncio.sleep(random.uniform(0.05, 0.10))

        return rows, last_status


def _sr_normalize(row: dict) -> dict:
    return {
        "source":       "spareroom",
        "postcode":     str(row.get("postcode") or ""),
        "title":        str(row.get("title") or ""),
        "url":          str(row.get("url") or ""),
        "flatshare_id": str(row.get("flatshare_id") or ""),
        "pw_price":     str(row.get("pw_price") or ""),
        "pcm_price":    str(row.get("pcm_price") or ""),
        "location":     str(row.get("location") or ""),
        "room_type":    str(row.get("room_type") or ""),
        "available":    str(row.get("available") or ""),
        "scraped_at":   pd.Timestamp.utcnow(),
    }


def _consolidate_spareroom(flatshare_type: str, shard_dir: Path) -> None:
    """Merge all shards for a flatshare_type into a single parquet in data/."""
    shards = sorted(shard_dir.glob("spareroom_shard_*.parquet"))
    if not shards:
        return
    out = DATA_DIR / f"spareroom_{flatshare_type}.parquet"
    df = pd.concat([pd.read_parquet(f) for f in shards], ignore_index=True)
    df.to_parquet(out, index=False, compression="snappy")
    print(f"SpareRoom {flatshare_type} consolidated — {len(df):,} rows -> {out.name}")


async def run_spareroom(flatshare_type: str, retry_failed: bool = False):
    """Scrape SpareRoom for one flatshare_type ('offered' or 'wanted')."""
    if not Path(SPAREROOM_CSV).exists():
        print(f"SpareRoom CSV not found: {SPAREROOM_CSV} — skipping.")
        return

    output_dir = DATA_DIR / f"spareroom_{flatshare_type}"
    output_dir.mkdir(exist_ok=True)
    state_file = BASE_DIR / f"state_spareroom_{flatshare_type}.json"

    postcodes = _sr_load_postcodes(SPAREROOM_CSV)
    state     = _sr_load_state(state_file)

    if retry_failed and state["failed_postcodes"]:
        remaining = state["failed_postcodes"]
        state["failed_postcodes"] = []
    else:
        remaining = [pc for pc in postcodes if pc not in state["completed_postcodes"]]

    total = len(remaining)
    if total == 0:
        print(f"SpareRoom {flatshare_type}: nothing to scrape.")
        return

    print(f"\nSpareRoom {flatshare_type} — {total:,} postcodes, concurrency={SR_CONCURRENCY}")

    batch:     list[dict] = []
    seen_keys: set[str]   = set()
    deduped    = 0
    done       = 0
    total_rows = 0
    shard_idx  = 0
    start_ts   = time.time()

    def flush_batch():
        nonlocal shard_idx, batch
        if not batch:
            return
        df  = pd.DataFrame(batch)
        ts  = int(time.time() * 1000)
        out = output_dir / f"spareroom_shard_{ts}.parquet"
        df.to_parquet(str(out) + ".tmp", index=False)
        os.replace(str(out) + ".tmp", out)
        if not IS_CI:
            print(f"\nWrote SpareRoom shard -> {out.name} ({len(df):,} rows)")
        batch = []
        shard_idx += 1

    sem       = asyncio.Semaphore(SR_CONCURRENCY)
    connector = aiohttp.TCPConnector(limit=max(20, SR_CONCURRENCY * 2), limit_per_host=max(4, SR_CONCURRENCY))
    timeout   = aiohttp.ClientTimeout(total=45, sock_connect=10, sock_read=30)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:

        async def worker(pc: str):
            nonlocal done, total_rows, deduped

            async with sem:
                scraper = _SpareRoomScraper(pc, session, flatshare_type)
                rows, status = await scraper.scrape_all()

                for r in rows:
                    fid = r.get("flatshare_id")
                    key = ("spareroom", str(fid), str(pc))
                    if not fid or key in seen_keys:
                        deduped += 1
                        continue
                    seen_keys.add(key)
                    batch.append(_sr_normalize(r))
                    if len(batch) >= 5000:
                        flush_batch()

                if status == 200:
                    state["completed_postcodes"].add(pc)
                else:
                    state["failed_postcodes"].append(pc)

                total_rows += len(rows)
                done += 1

                if not IS_CI:
                    frac = done / total
                    elapsed = time.time() - start_ts
                    eta = max(0, int((elapsed / max(frac, 1e-9)) - elapsed)) if done > 5 else 0
                    fill = int(30 * frac)
                    bar  = "#" * fill + "-" * (30 - fill)
                    print(
                        f"[SR-{flatshare_type} {bar}] {int(frac*100)}% ({done:,}/{total:,})"
                        f" | ETA {eta//60:02d}:{eta%60:02d}",
                        end="\r", flush=True,
                    )

                if done % 25 == 0:
                    _sr_save_state(state, state_file)

        await asyncio.gather(*(worker(pc) for pc in remaining))

    flush_batch()
    _sr_save_state(state, state_file)
    print(f"\nSpareRoom {flatshare_type} done — {total_rows:,} rows, {deduped:,} deduped")
    _consolidate_spareroom(flatshare_type, output_dir)


# ============================================================
# ARTICLE 4 DIRECTION AREAS
# ============================================================

def fetch_article4_areas() -> pd.DataFrame:
    """
    Download all Article 4 direction areas from planning.data.gov.uk and
    cache them to data/article4_areas.parquet.  Cache is reused for 7 days.
    Returns a DataFrame with columns: entity, name, reference, geometry_json, point_wkt.
    """
    import urllib.request
    import urllib.parse

    if ARTICLE4_PARQUET.exists():
        age_days = (time.time() - ARTICLE4_PARQUET.stat().st_mtime) / 86400
        if age_days < ARTICLE4_MAX_AGE_DAYS:
            print(f"Article 4 cache is {age_days:.1f} days old — loading from parquet.")
            return pd.read_parquet(ARTICLE4_PARQUET)

    print("Fetching Article 4 direction areas from planning.data.gov.uk ...")
    base_url = "https://www.planning.data.gov.uk/entity.json"
    limit    = 500
    offset   = 0
    all_rows: list[dict] = []

    while True:
        params = urllib.parse.urlencode({
            "dataset": "article-4-direction-area",
            "limit":   limit,
            "offset":  offset,
        })
        try:
            with urllib.request.urlopen(f"{base_url}?{params}", timeout=30) as resp:
                data = json.loads(resp.read().decode())
        except Exception as exc:
            print(f"\nArticle 4 API error at offset {offset}: {exc}")
            break

        entities = data.get("entities", [])
        if not entities:
            break

        for ent in entities:
            geom = ent.get("geometry")
            all_rows.append({
                "entity":        ent.get("entity"),
                "name":          ent.get("name") or "",
                "reference":     ent.get("reference") or "",
                "geometry_json": json.dumps(geom) if geom else None,
                "point_wkt":     ent.get("point") or "",
            })

        print(f"  Fetched {len(all_rows):,} areas...", end="\r", flush=True)
        if len(entities) < limit:
            break
        offset += limit

    print(f"\nFetched {len(all_rows):,} Article 4 direction areas total.")

    if not all_rows:
        empty = pd.DataFrame(columns=["entity", "name", "reference", "geometry_json", "point_wkt"])
        return empty

    df = pd.DataFrame(all_rows)
    df.to_parquet(ARTICLE4_PARQUET, index=False, compression="snappy")
    print(f"Saved Article 4 areas -> {ARTICLE4_PARQUET.name}")
    return df


def build_article4_index(df: pd.DataFrame):
    """
    Build a shapely STRtree spatial index from Article 4 MultiPolygon geometries.
    Returns (STRtree, list[geom]) or (None, []) if shapely is unavailable.
    """
    if not _SHAPELY_AVAILABLE:
        print("shapely not installed — Article 4 filtering disabled. Run: pip install shapely")
        return None, []

    if df.empty:
        return None, []

    from shapely import wkt as _shapely_wkt

    geoms: list = []
    for geom_json in df["geometry_json"].dropna():
        try:
            parsed = json.loads(geom_json)
            if isinstance(parsed, str):
                # WKT string (planning.data.gov.uk returns WKT, not GeoJSON)
                geoms.append(_shapely_wkt.loads(parsed))
            elif isinstance(parsed, dict) and parsed:
                # GeoJSON object
                geoms.append(shape(parsed))
        except Exception:
            pass

    if not geoms:
        return None, []

    tree = STRtree(geoms)
    print(f"Article 4 spatial index built — {len(geoms):,} polygons")
    return tree, geoms


def is_in_article4(lat: float, lon: float, tree, geoms: list) -> bool:
    """Return True if (lat, lon) falls inside any Article 4 direction area polygon."""
    if tree is None or not geoms:
        return False
    pt = Point(lon, lat)  # shapely uses (x=lon, y=lat)
    for candidate in tree.query(pt):
        try:
            geom = geoms[int(candidate)]
        except (TypeError, ValueError):
            geom = candidate
        if geom.contains(pt):
            return True
    return False


def _flag_article4(df: pd.DataFrame, tree, geoms: list) -> pd.Series:
    """Return a boolean Series indicating which rows fall inside Article 4 areas."""
    flags = []
    for _, row in df.iterrows():
        lat = _safe_float(row.get("latitude"))
        lon = _safe_float(row.get("longitude"))
        if lat is None or lon is None:
            flags.append(False)
        else:
            flags.append(is_in_article4(lat, lon, tree, geoms))
    return pd.Series(flags, index=df.index, dtype=bool)


# ============================================================
# HMO ANALYSIS & EMAIL
# ============================================================

def load_spareroom_rents(min_listings: int = 3) -> dict[str, float]:
    """
    Return outcode -> median monthly rent (£) from SpareRoom offered data.
    Reads consolidated parquet if present, otherwise falls back to shards.
    """
    consolidated = DATA_DIR / "spareroom_offered.parquet"
    offered_dir  = DATA_DIR / "spareroom_offered"

    if consolidated.exists():
        sources = [consolidated]
    else:
        sources = sorted(offered_dir.glob("spareroom_shard_*.parquet")) if offered_dir.exists() else []

    if not sources:
        return {}

    dfs = []
    for f in sources:
        try:
            dfs.append(pd.read_parquet(f, columns=["postcode", "pcm_price", "pw_price"]))
        except Exception:
            pass
    if not dfs:
        return {}

    df = pd.concat(dfs, ignore_index=True)

    def _to_monthly(pcm_str, pw_str) -> float | None:
        def _clean(s):
            if not s or str(s).strip() in ("", "None", "nan"):
                return None
            return re.sub(r"[£,\s]", "", str(s))

        pcm = _clean(pcm_str)
        if pcm:
            try:
                v = float(pcm)
                if 50 < v < 5_000:
                    return v
            except ValueError:
                pass

        pw = _clean(pw_str)
        if pw:
            try:
                v = float(pw)
                if 10 < v < 1_500:
                    return round(v * 52 / 12, 2)
            except ValueError:
                pass
        return None

    df["monthly_rent"] = df.apply(lambda r: _to_monthly(r["pcm_price"], r["pw_price"]), axis=1)
    df = df.dropna(subset=["monthly_rent"])
    df["outcode"] = df["postcode"].fillna("").str.split().str[0].str.upper()
    df = df[df["outcode"].notna() & (df["outcode"] != "")]

    rents: dict[str, float] = {}
    for oc, grp in df.groupby("outcode"):
        if len(grp) >= min_listings:
            rents[oc] = round(grp["monthly_rent"].median(), 0)
    return rents


def analyse_hmo_opportunities(df: pd.DataFrame) -> dict:
    """
    Identify HMO conversion candidates and score outcode hotspots.

    Candidate criteria:
      - 3+ bedrooms, house type (not flat), not already marketed as HMO

    Hotspot composite score (0-10, weighted sum of six signals):
      university proximity  30%  - tenant demand from students
      estimated gross yield 25%  - (room_rent x beds x 12) / price
      HMO market density    20%  - % of houses in outcode already listed as HMO
      transport proximity   15%  - distance to nearest major rail/metro station
      hospital proximity     7%  - demand from NHS staff / key workers
      affordability          3%  - median price vs £220k cap

    Room rent uses live SpareRoom outcode medians where available,
    falling back to the regional OUTCODE_ROOM_RENTS table.
    """
    df = df.copy()
    df["price_num"] = pd.to_numeric(df["price"], errors="coerce")
    df["beds_num"]  = pd.to_numeric(df["bedrooms"], errors="coerce")
    df["is_house"]  = (
        df["property_type"].fillna("")
        .str.contains(_HOUSE_TYPE_PAT, case=False, regex=True)
    )
    df["outcode"] = df["postcode"].fillna("").str.split().str[0].str.upper()

    df["is_land"] = (
        df["property_type"].fillna("")
        .str.contains(_LAND_TYPE_PAT, case=False, regex=True)
    )

    hmo_candidates = df[
        (df["beds_num"] >= 3) &
        (df["beds_num"] <= 20) &
        (df["price_num"] >= 5_000) &
        df["is_house"] &
        (~df["is_land"]) &
        (~df["potential_hmo"].fillna(False))
    ].copy()

    # Article 4 filter — exclude properties in restricted areas
    # Two-layer check: (1) polygon lookup against national register,
    # (2) outcode override list for councils not yet in the register.
    a4_df   = fetch_article4_areas()
    a4_tree, a4_geoms = build_article4_index(a4_df)
    a4_excluded = 0
    if not hmo_candidates.empty:
        # Layer 1: polygon check from national register
        if a4_tree is not None:
            print("Checking Article 4 restrictions (national register) ...")
            hmo_candidates["in_article4"] = _flag_article4(hmo_candidates, a4_tree, a4_geoms)
        else:
            hmo_candidates["in_article4"] = False

        # Layer 2: known outcode overrides (councils not in national register)
        outcode_override = hmo_candidates["outcode"].isin(KNOWN_ARTICLE4_OUTCODES)
        override_count   = int((outcode_override & ~hmo_candidates["in_article4"]).sum())
        hmo_candidates["in_article4"] = hmo_candidates["in_article4"] | outcode_override

        a4_excluded = int(hmo_candidates["in_article4"].sum())
        print(f"  Excluded {a4_excluded:,} candidates in Article 4 areas"
              f" ({override_count:,} via outcode override list)")
        hmo_candidates = hmo_candidates[~hmo_candidates["in_article4"]].copy()

    five_plus = df[df["beds_num"] >= 5].copy()

    spareroom_rents = load_spareroom_rents()

    all_houses = df[df["is_house"] & df["outcode"].notna() & (df["outcode"] != "")]

    centroids = (
        all_houses.dropna(subset=["latitude", "longitude"])
        .groupby("outcode")
        .agg(lat=("latitude", "mean"), lon=("longitude", "mean"))
    )

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

    rows = []
    for oc, row in agg.iterrows():
        if oc not in centroids.index:
            continue
        lat, lon  = centroids.loc[oc, "lat"], centroids.loc[oc, "lon"]
        med_price = row["median_price"]
        avg_beds  = row["avg_beds"]

        if oc in spareroom_rents:
            room_rent   = spareroom_rents[oc]
            rent_source = "spareroom"
        else:
            room_rent   = _get_room_rent(oc)
            rent_source = "regional"

        density = hmo_density.get(oc, 0.0)

        est_yield = (
            (room_rent * avg_beds * 12) / med_price * 100
            if pd.notna(med_price) and med_price > 0 else 0.0
        )

        dist_uni  = _nearest_km(lat, lon, UK_UNIVERSITIES)
        dist_hosp = _nearest_km(lat, lon, UK_HOSPITALS)
        dist_sta  = _nearest_km(lat, lon, UK_STATIONS)

        s_uni    = _score_lower_better(dist_uni,              _UNI_BP)
        s_hosp   = _score_lower_better(dist_hosp,             _HOSP_BP)
        s_sta    = _score_lower_better(dist_sta,              _STA_BP)
        s_yield  = _score_higher_better(est_yield,            _YIELD_BP)
        s_dens   = _score_higher_better(density,              _DENS_BP)
        s_afford = _score_lower_better(med_price or 999_999,  _AFFORD_BP)

        composite = (
            s_uni    * _SCORE_WEIGHTS["university"]
            + s_yield  * _SCORE_WEIGHTS["yield"]
            + s_dens   * _SCORE_WEIGHTS["hmo_density"]
            + s_sta    * _SCORE_WEIGHTS["transport"]
            + s_hosp   * _SCORE_WEIGHTS["hospital"]
            + s_afford * _SCORE_WEIGHTS["affordability"]
        )

        rows.append({
            "outcode":         oc,
            "opportunities":   int(row["opportunities"]),
            "median_price":    med_price,
            "avg_beds":        avg_beds,
            "under_220k":      int(row["under_220k"]),
            "hmo_density":     round(density, 1),
            "room_rent":       int(room_rent),
            "rent_source":     rent_source,
            "est_yield":       round(est_yield, 1),
            "dist_uni_km":     round(dist_uni, 1),
            "dist_hosp_km":    round(dist_hosp, 1),
            "dist_sta_km":     round(dist_sta, 1),
            "score_uni":       s_uni,
            "score_hosp":      s_hosp,
            "score_transport": s_sta,
            "score_yield":     s_yield,
            "score_density":   s_dens,
            "score_afford":    s_afford,
            "composite_score": round(composite, 2),
        })

    hotspots = (
        pd.DataFrame(rows)
        .sort_values("composite_score", ascending=False)
        .head(20)
        .reset_index(drop=True)
    )

    score_map = hotspots.set_index("outcode")["composite_score"].to_dict()
    affordable = hmo_candidates[hmo_candidates["price_num"] < HMO_PRICE_THRESHOLD].copy()
    affordable["hmo_score"] = affordable["outcode"].map(score_map)
    affordable = affordable.sort_values(["hmo_score", "price_num"], ascending=[False, True])

    # Per-property HMO income estimates
    affordable["room_rent_est"] = affordable["outcode"].apply(
        lambda oc: int(spareroom_rents.get(oc, _get_room_rent(oc)))
    )
    affordable["floor_sqm"] = affordable["floor_area"].apply(_parse_floor_area_sqm)
    _hmo_est = affordable.apply(
        lambda r: _estimate_hmo_rooms(
            r["beds_num"],
            r["floor_sqm"],
            bathrooms=r.get("bathrooms"),
            reception_rooms=r.get("reception_rooms"),
            has_ensuite=bool(r.get("has_ensuite") or False),
        ),
        axis=1,
    )
    affordable["pot_rooms"]     = _hmo_est.apply(lambda x: x[0])
    affordable["ensuite_rooms"] = _hmo_est.apply(lambda x: x[1])
    affordable["est_monthly"]   = affordable["room_rent_est"] * affordable["pot_rooms"]
    affordable["est_yield_pct"] = (
        affordable["est_monthly"] * 12 / affordable["price_num"] * 100
    ).where(affordable["price_num"] > 0)
    affordable = affordable.sort_values("est_yield_pct", ascending=False)

    is_auction = affordable["potential_auction"].fillna(False).astype(bool)
    affordable_standard = affordable[~is_auction].copy()
    affordable_auction  = affordable[is_auction].copy()

    return {
        "total":               len(df),
        "hmo_candidates":      hmo_candidates,
        "five_plus":           five_plus,
        "affordable":          affordable_standard,
        "affordable_auction":  affordable_auction,
        "hotspots":            hotspots,
        "a4_excluded":         a4_excluded,
    }


def print_metrics_report(analysis: dict) -> None:
    cands     = analysis["hmo_candidates"]
    five_plus = analysis["five_plus"]
    aff       = analysis["affordable"]
    hot       = analysis["hotspots"]

    p = cands["price_num"]
    band_mid  = ((p >= HMO_PRICE_THRESHOLD) & (p < 350_000)).sum()
    band_high = (p >= 350_000).sum()

    print("\n" + "=" * 72)
    print("  HMO OPPORTUNITY ANALYSIS")
    print("=" * 72)
    print(f"  Total properties scraped      : {analysis['total']:>8,}")
    print(f"  HMO conversion candidates     : {len(cands):>8,}  (3+ bed house, non-HMO)")
    if analysis.get("a4_excluded", 0) > 0:
        print(f"  Article 4 excluded            : {analysis['a4_excluded']:>8,}  (planning restriction)")
    print(f"    Under £{HMO_PRICE_THRESHOLD:,}               : {len(aff):>8,}")
    print(f"    £{HMO_PRICE_THRESHOLD:,} - £350,000          : {band_mid:>8,}")
    print(f"    Over £350,000                : {band_high:>8,}")
    print(f"  5+ bed properties (all types) : {len(five_plus):>8,}")
    if len(cands) > 0 and p.notna().any():
        print(f"\n  Price stats (HMO candidates):")
        print(f"    Median  : £{p.median():>10,.0f}")
        print(f"    Mean    : £{p.mean():>10,.0f}")
        print(f"    Min     : £{p.min():>10,.0f}")
        print(f"    Max     : £{p.max():>10,.0f}")

    if len(hot) > 0:
        sr_count = (hot["rent_source"] == "spareroom").sum() if "rent_source" in hot.columns else 0
        print(f"\n  Rent data: {sr_count}/{len(hot)} outcodes use live SpareRoom data"
              f" ({len(hot) - sr_count} use regional fallback)")
        print(f"\n  TOP HMO HOTSPOTS  (ranked by composite investment score)")
        hdr = (
            f"  {'#':<3}  {'Area':<7}  {'Score':>5}  {'Count':>5}  "
            f"{'Median':>9}  {'Rent/rm':>7}  {'Src':<3}  {'Yield':>5}  {'HMO%':>4}  "
            f"{'Uni km':>6}  {'Sta km':>6}  {'<220k':>5}"
        )
        print(hdr)
        print("  " + "-" * (len(hdr) - 2))
        for i, row in hot.iterrows():
            med = f"£{row['median_price']:,.0f}" if pd.notna(row["median_price"]) else "N/A"
            src = "SR" if row.get("rent_source") == "spareroom" else "est"
            print(
                f"  {i+1:<3}  {row['outcode']:<7}  {row['composite_score']:>5.2f}"
                f"  {row['opportunities']:>5,}  {med:>9}"
                f"  £{row['room_rent']:>5,}  {src:<3}  {row['est_yield']:>4.1f}%"
                f"  {row['hmo_density']:>4.1f}%"
                f"  {row['dist_uni_km']:>6.1f}  {row['dist_sta_km']:>6.1f}"
                f"  {row['under_220k']:>5,}"
            )
    print("=" * 72 + "\n")


def _property_table_rows(df: pd.DataFrame, n: int = 100) -> str:
    rows = ""
    for _, r in df.head(n).iterrows():
        price       = f"£{int(r['price_num']):,}" if pd.notna(r["price_num"]) else "-"
        beds        = str(int(r["beds_num"])) if pd.notna(r["beds_num"]) else "?"
        addr        = r.get("address") or "View listing"
        ptype       = r.get("property_type") or "-"
        pc          = r.get("postcode") or "-"
        score       = r.get("hmo_score")
        score_str   = f"{score:.1f}" if pd.notna(score) else "-"
        rr          = r.get("room_rent_est")
        rr_str      = f"£{int(rr):,}" if pd.notna(rr) else "-"
        pot         = r.get("pot_rooms")
        pot_str     = str(int(pot)) if pd.notna(pot) else "-"
        ens         = r.get("ensuite_rooms")
        ens_str     = str(int(ens)) if pd.notna(ens) else "-"
        monthly     = r.get("est_monthly")
        monthly_str = f"£{int(monthly):,}" if pd.notna(monthly) else "-"
        gross_yield = r.get("est_yield_pct")
        yield_str   = f"{gross_yield:.1f}%" if pd.notna(gross_yield) else "-"
        yield_col   = (
            "#27ae60" if pd.notna(gross_yield) and gross_yield >= 10 else
            ("#e67e22" if pd.notna(gross_yield) and gross_yield >= 7 else "#c0392b")
        )
        rows += (
            f"<tr>"
            f"<td><a href='{r['url']}'>{addr}</a></td>"
            f"<td>{pc}</td><td>{price}</td><td>{beds}</td><td>{ptype}</td>"
            f"<td style='text-align:center'>{score_str}</td>"
            f"<td style='text-align:center'>{rr_str}</td>"
            f"<td style='text-align:center'>{pot_str}</td>"
            f"<td style='text-align:center'>{ens_str}</td>"
            f"<td style='text-align:right;font-weight:bold'>{monthly_str}</td>"
            f"<td style='text-align:center;font-weight:bold;color:{yield_col}'>{yield_str}</td>"
            f"</tr>\n"
        )
    return rows


def _property_table_html(df: pd.DataFrame, heading: str, header_colour: str, n: int = 100) -> str:
    total = len(df)
    if total == 0:
        return f"<h3 style='margin-top:2em;'>{heading}</h3><p>No properties.</p>"
    note = (
        f"Showing top {min(n, total):,} of {total:,} total, ranked by estimated gross yield.<br>"
        "Score = outcode composite score &nbsp;|&nbsp; "
        "<b>Pot. Rooms</b> = estimated HMO rooms after converting reception rooms. "
        "<b>En-suite</b> = rooms estimated large enough to add a wet room. "
        "<b>Est. Monthly</b> = Rent/rm &times; Pot. Rooms. All figures are estimates only."
    )
    rows = _property_table_rows(df, n)
    return f"""
<h3 style="margin-top:2em;">{heading}</h3>
<p style="font-size:0.85em;color:#555;">{note}</p>
<table border="1" cellpadding="7" cellspacing="0"
       style="border-collapse:collapse;width:100%;font-size:0.88em;">
  <tr style="background:{header_colour};color:#fff;">
    <th>Address</th><th>Postcode</th><th>Price</th><th>Beds</th><th>Type</th>
    <th>Score</th><th>Rent/rm</th><th>Pot. Rooms</th><th>En-suite</th>
    <th>Est. Monthly</th><th>Est. Yield</th>
  </tr>
  {rows}
</table>"""


def _build_email_html(affordable: pd.DataFrame, hotspots: pd.DataFrame, affordable_auction: pd.DataFrame | None = None) -> str:
    date_str = time.strftime("%d %B %Y")
    auction_df = affordable_auction if affordable_auction is not None else pd.DataFrame()

    hotspot_rows = ""
    for i, row in hotspots.iterrows():
        med    = f"£{row['median_price']:,.0f}" if pd.notna(row["median_price"]) else "-"
        score  = row["composite_score"]
        colour = "#27ae60" if score >= 7 else ("#e67e22" if score >= 4 else "#c0392b")
        src_badge = (
            "<span style='background:#27ae60;color:#fff;padding:1px 5px;border-radius:3px;"
            "font-size:0.8em'>SpareRoom</span>"
            if row.get("rent_source") == "spareroom" else
            "<span style='background:#95a5a6;color:#fff;padding:1px 5px;border-radius:3px;"
            "font-size:0.8em'>est.</span>"
        )
        hotspot_rows += (
            f"<tr>"
            f"<td>{i+1}</td><td><b>{row['outcode']}</b></td>"
            f"<td style='color:{colour};font-weight:bold'>{score:.2f}</td>"
            f"<td>{int(row['opportunities']):,}</td><td>{med}</td>"
            f"<td>£{int(row['room_rent']):,}/mo {src_badge}</td>"
            f"<td>{row['est_yield']:.1f}%</td><td>{row['hmo_density']:.1f}%</td>"
            f"<td>{row['dist_uni_km']:.1f} km</td><td>{row['dist_sta_km']:.1f} km</td>"
            f"<td>{int(row['under_220k']):,}</td>"
            f"</tr>\n"
        )

    standard_table = _property_table_html(
        affordable, f"Top 100 Properties Under £{HMO_PRICE_THRESHOLD:,}", "#c0392b"
    )
    auction_table = _property_table_html(
        auction_df, f"Auction Properties Under £{HMO_PRICE_THRESHOLD:,}", "#8e44ad"
    ) if not auction_df.empty else ""

    return f"""<html><body style="font-family:Arial,sans-serif;color:#222;max-width:960px;margin:auto;">
<h2 style="color:#c0392b;border-bottom:2px solid #c0392b;padding-bottom:6px;">
  HMO Opportunity Report &mdash; {date_str}
</h2>
<p>HMO conversion candidates (3+ bed house, not already HMO, not land) priced under
<b>£{HMO_PRICE_THRESHOLD:,}</b>. Properties in <b>Article 4 direction areas</b>
have been excluded. Auction properties are listed separately below.</p>
<p><b>{len(affordable):,} standard</b> + <b>{len(auction_df):,} auction</b> candidates.</p>

<h3 style="margin-top:1.8em;">Top HMO Hotspots</h3>
<p style="font-size:0.85em;color:#555;">
  Score (0-10): university proximity 30% &middot; yield 25% &middot;
  HMO density 20% &middot; transport 15% &middot; hospital 7% &middot; affordability 3%.<br>
  HMO density = % of houses in that outcode already listed as HMO (proven demand signal).
</p>
<table border="1" cellpadding="7" cellspacing="0"
       style="border-collapse:collapse;width:100%;font-size:0.88em;">
  <tr style="background:#2c3e50;color:#fff;">
    <th>#</th><th>Area</th><th>Score</th><th>Listings</th>
    <th>Median Price</th><th>Room Rent</th><th>Est. Yield</th><th>HMO Density</th>
    <th>Uni Dist</th><th>Station Dist</th><th>Under £220k</th>
  </tr>
  {hotspot_rows}
</table>

{standard_table}
{auction_table}

<p style="color:#999;margin-top:2em;font-size:0.8em;">
  Generated by Rightmove + SpareRoom HMO Scraper &mdash; {time.strftime('%Y-%m-%d %H:%M')} UTC<br>
  Yield = gross estimate (room rent &times; beds &times; 12 &divide; price).
  <b>SpareRoom</b> = live median asking rent. <b>est.</b> = regional average fallback.
</p>
</body></html>"""


def send_hmo_email(analysis: dict) -> None:
    gmail_user = os.environ.get("GMAIL_USER", "").strip()
    gmail_pass = os.environ.get("GMAIL_APP_PASSWORD", "").strip()

    if not gmail_user or not gmail_pass or not HMO_EMAIL_TO:
        print("GMAIL_USER / GMAIL_APP_PASSWORD / HMO_EMAIL_TO not set — skipping HMO email.")
        return

    affordable = analysis["affordable"]
    if affordable.empty:
        print(f"No properties under £{HMO_PRICE_THRESHOLD:,} found — skipping email.")
        return

    msg = MIMEMultipart("alternative")
    msg["Subject"] = (
        f"HMO Opportunities Under £{HMO_PRICE_THRESHOLD:,} "
        f"({len(affordable):,} properties) — {time.strftime('%d %b %Y')}"
    )
    msg["From"] = gmail_user
    msg["To"]   = HMO_EMAIL_TO
    msg.attach(MIMEText(_build_email_html(affordable, analysis["hotspots"], analysis.get("affordable_auction")), "html"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(gmail_user, gmail_pass)
            smtp.sendmail(gmail_user, HMO_EMAIL_TO, msg.as_string())
        print(f"HMO email sent to {HMO_EMAIL_TO} — {len(affordable):,} properties listed.")
    except Exception as exc:
        print(f"Email send failed: {exc}")


# ============================================================
# POST-RUN HELPERS
# ============================================================

def clear_state():
    empty = {"completed_outcodes": [], "collected_urls": [], "seen_urls": []}
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(empty, f, indent=2)
    print("Rightmove state cleared.")

    shards_root = DATA_DIR / "shards"
    if shards_root.exists():
        import shutil
        shutil.rmtree(shards_root)
        print("Rightmove shards directory removed.")


def push_to_github(parquet_path: Path):
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
# MAIN ENTRY POINTS
# ============================================================

async def run_rightmove():
    """Full Rightmove scrape: URL collection + detail scraping + analysis + email."""
    for f in DATA_DIR.glob("rightmove_*.parquet"):
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

    if remaining:
        await collect_all_urls(remaining, state)

    print(f"\nURL COLLECTION COMPLETE — {len(state['collected_urls']):,} pending")

    pending = list(state["collected_urls"] - state["seen_urls"])
    if not pending:
        print("No pending URLs to scrape.")
        return None

    print(f"\nStarting detail scraping — {len(pending):,} URLs across {DETAIL_WORKERS} workers")

    shard_dir = DATA_DIR / "shards" / time.strftime("%Y-%m-%d")
    shard_dir.mkdir(parents=True, exist_ok=True)

    state_snapshot = {
        "collected_urls": list(state["collected_urls"]),
        "seen_urls":      list(state["seen_urls"]),
    }

    chunks   = [pending[i::DETAIL_WORKERS] for i in range(DETAIL_WORKERS)]
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
    return output


async def run():
    """Default: Rightmove + SpareRoom + analysis + email."""
    output = await run_rightmove()

    if SCRAPE_ONLY:
        if output:
            print(f"\nScrape-only complete. Parquet written: {output}")
        return

    if not RIGHTMOVE_ONLY:
        await run_spareroom("offered", retry_failed=RETRY_FAILED)
        await run_spareroom("wanted",  retry_failed=RETRY_FAILED)

    if output and output.exists():
        df_all   = pd.read_parquet(output)
        analysis = analyse_hmo_opportunities(df_all)
        print_metrics_report(analysis)
        send_hmo_email(analysis)

    state = load_state()
    still_pending = len(state["collected_urls"] - state["seen_urls"])
    if still_pending == 0:
        clear_state()
        if os.environ.get("GITHUB_ACTIONS"):
            print("\nAll URLs scraped — state cleared. Workflow will push parquet.")
        else:
            print("\nAll URLs scraped — clearing state and pushing to GitHub.")
            if output:
                push_to_github(output)
    else:
        print(f"\n{still_pending:,} URLs still pending — state kept for resume.")

    if output:
        print(f"\nALL DONE!\nOutput : {output}")


def run_analyse_only():
    """SpareRoom scrape + HMO analysis on the most recent Rightmove parquet."""
    asyncio.run(run_spareroom("offered", retry_failed=RETRY_FAILED))
    if not IS_CI:
        asyncio.run(run_spareroom("wanted", retry_failed=RETRY_FAILED))

    parquets = sorted(
        DATA_DIR.glob("rightmove_*.parquet"),
        key=lambda p: p.stat().st_mtime, reverse=True,
    )
    if not parquets:
        print("No Rightmove parquet files found in data/ — run the scraper first.")
        return
    output = parquets[0]
    print(f"Analysing {output.name} ...")
    df_all   = pd.read_parquet(output)
    analysis = analyse_hmo_opportunities(df_all)
    print_metrics_report(analysis)
    send_hmo_email(analysis)


async def run_retry_failed():
    state  = load_state()
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
        shard_dir = DATA_DIR / "shards" / time.strftime("%Y-%m-%d")
        shard_dir.mkdir(parents=True, exist_ok=True)
        flush_shard(rows_buf, shard_dir, 0)
        consolidate_shards(shard_dir, final_parquet_name())

    save_state(state)
    print("Retry complete!")


if __name__ == "__main__":
    freeze_support()
    if ANALYSE_ONLY:
        run_analyse_only()
    elif SPAREROOM_ONLY:
        asyncio.run(run_spareroom("offered", retry_failed=RETRY_FAILED))
        asyncio.run(run_spareroom("wanted",  retry_failed=RETRY_FAILED))
    elif RETRY_FAILED and not RIGHTMOVE_ONLY:
        asyncio.run(run_retry_failed())
    else:
        asyncio.run(run())
