# ============================================================
# SPAREROOM ASYNC SCRAPER (FULL SCRIPT)
# - Postcodes from CSV
# - Robust pagination (offset until empty)
# - Parquet shards
# - Resume-safe + retry failed postcodes
# - Rightmove-style progress bar + ETA
# ============================================================

import argparse
import asyncio
import aiohttp
import json
import os
import random
import re
import time
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup


import math

def extract_total_results(soup) -> int | None:
    nav = soup.find("p", class_="navcurrent")
    if not nav:
        return None

    strongs = nav.find_all("strong")
    if len(strongs) >= 2:
        try:
            return int(strongs[1].get_text(strip=True).replace(",", ""))
        except:
            return None
    return None

# ============================================================
# CONFIG
# ============================================================
PAGE_SIZE = 50
SOURCE_NAME = "spareroom"
_SEEN_KEYS = set()
BASE_DIR = Path(__file__).resolve().parent
PARQUET_DIR = BASE_DIR / "parquet"
PARQUET_DIR.mkdir(exist_ok=True)
CSV_PATH = r'C:\rightmove_monitor\spareroom\postcodes.csv'
STATE_FILE = BASE_DIR / "state.json"

BASE_URL = "https://www.spareroom.co.uk/flatshare/search.pl"

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept-Language": "en-GB,en;q=0.9",
    "Referer": "https://www.spareroom.co.uk/",
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17 Safari/605.1.15",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121 Safari/537.36",
]

# Parquet shard batching
BATCH_SIZE = 5000
_BATCH = []


from urllib.parse import urlparse, parse_qs

def extract_flatshare_id(url: str) -> str | None:
    qs = parse_qs(urlparse(url).query)
    return qs.get("flatshare_id", [None])[0]

# ============================================================
# STATE (resume-safe)
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
        "completed_postcodes": set(d.get("completed_postcodes", [])),
        "failed_postcodes": list(d.get("failed_postcodes", [])),
    }


def save_state(state):
    data = {
        "completed_postcodes": sorted(list(state["completed_postcodes"])),
        "failed_postcodes": state["failed_postcodes"],
    }
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

# ============================================================
# PARQUET SHARDS
# ============================================================

def _normalize_row(row: dict) -> dict:
    # Keep schema stable
    return {
        "source": SOURCE_NAME,
        "postcode": str(row.get("postcode") or ""),
        "title": str(row.get("title") or ""),
        "url": str(row.get("url") or ""),
        "price_text": str(row.get("price_text") or ""),
        "location": str(row.get("location") or ""),
        "room_type": str(row.get("room_type") or ""),
        "available": str(row.get("available") or ""),
        "scraped_at": pd.Timestamp.utcnow(),
        "status": int(row.get("status") or 0),
    }


def flush_parquet_shard():
    global _BATCH
    if not _BATCH:
        return

    df = pd.DataFrame(_BATCH)
    ts = int(time.time() * 1000)
    path = PARQUET_DIR / f"spareroom_shard_{ts}.parquet"
    tmp = str(path) + ".tmp"

    df.to_parquet(tmp, index=False)
    os.replace(tmp, path)

    print(f"\n🧱 Wrote shard → {path.name} ({len(df):,} rows)")
    _BATCH = []

DEDUPED = 0

def add_row(row: dict):
    global _BATCH, _SEEN_KEYS, DEDUPED

    listing_id = row.get("flatshare_id")
    postcode = row.get("postcode")

    if not listing_id or not postcode:
        return

    key = ("spareroom", listing_id, postcode)

    if key in _SEEN_KEYS:
        DEDUPED += 1
        return

    _SEEN_KEYS.add(key)
    _BATCH.append(_normalize_row(row))

    if len(_BATCH) >= BATCH_SIZE:
        flush_parquet_shard()

# ============================================================
# PROGRESS BAR
# ============================================================

def print_progress(done: int, total: int, start_ts: float):
    frac = done / total if total else 1.0
    bar_len = 30
    fill = int(bar_len * frac)
    bar = "#" * fill + "-" * (bar_len - fill)

    elapsed = time.time() - start_ts
    if done > 5:
        eta = (elapsed / max(frac, 1e-9)) - elapsed
    else:
        eta = 0

    eta = max(0, int(eta))
    mins, secs = divmod(eta, 60)

    print(
        f"[{bar}] {int(frac*100)}% ({done:,}/{total:,}) | ETA {mins:02d}:{secs:02d}",
        end="\r",
        flush=True,
    )

# ============================================================
# CSV LOADING
# ============================================================

def load_postcodes_from_csv(csv_path: str, column: str | None = None) -> list[str]:
    df = pd.read_csv(csv_path)

    if column and column in df.columns:
        ser = df[column]
    else:
        # If user gives OuterCode, Postcode, take Postcode; else take first column
        if "Postcode" in df.columns:
            ser = df["Postcode"]
        else:
            ser = df.iloc[:, 0]

    postcodes = (
        ser.dropna()
        .astype(str)
        .str.strip()
        .str.upper()
        .unique()
        .tolist()
    )
    return postcodes

# ============================================================
# SPAREROOM SCRAPER
# ============================================================

class AsyncSpareRoomScraper:
    DOMAIN = "https://www.spareroom.co.uk"

    def __init__(self, postcode: str, session: aiohttp.ClientSession):
        self.postcode = postcode
        self.session = session

    async def fetch_page(self, offset: int) -> tuple[int, str | None]:
        params = {
            "action": "search",
            "flatshare_type": "offered",
            "search": self.postcode,
            "max_per_page": PAGE_SIZE,   # 🔥 REQUIRED
            "offset": offset,
        }

        headers = dict(DEFAULT_HEADERS)
        headers["User-Agent"] = random.choice(USER_AGENTS)

        try:
            async with self.session.get(BASE_URL, params=params, headers=headers) as r:
                status = r.status
                if status != 200:
                    return status, None
                return status, await r.text()
        except Exception:
            return 0, None

    def parse_listing(self, li) -> dict | None:
        try:
            title = li.find("h2", class_="listing-card__title")
            link = li.find("a", class_="listing-card__link")
            price = li.find("p", class_="listing-card__price")
            location = li.find("p", class_="listing-card__location")
            room_type = li.find("span", class_="listing-card__room")
            avail = li.find("span", class_="listing-card__availability")

            url = None
            if link and link.has_attr("href"):
                href = link["href"]
                url = href if href.startswith("http") else (self.DOMAIN + href)
            flatshare_id = extract_flatshare_id(url)
            return {
                "source": "spareroom",
                "flatshare_id": flatshare_id,
                "postcode": self.postcode,
                "title": title.get_text(strip=True) if title else None,
                "url": url,
                "price_text": price.get_text(" ", strip=True) if price else None,
                "location": location.get_text(strip=True) if location else None,
                "room_type": room_type.get_text(strip=True) if room_type else None,
                "available": avail.get_text(strip=True) if avail else None,
            }
        except Exception:
            return None

    async def scrape_all(self) -> tuple[list[dict], int]:
        rows: list[dict] = []
        last_status = 200
        seen_urls: set[str] = set()

        status, html = await self.fetch_page(0)
        last_status = status or last_status
        if not html:
            return rows, last_status

        soup = BeautifulSoup(html, "html.parser")
        listings = soup.find_all("li", class_="listing-result")
        if not listings:
            return rows, last_status

        total_results = extract_total_results(soup)
        if total_results:
            total_pages = max(1, math.ceil(total_results / PAGE_SIZE))
        else:
            total_pages = 30  # safe fallback

        # page 0
        for li in listings:
            row = self.parse_listing(li)
            if not row:
                continue
            u = row.get("url")
            if u and u not in seen_urls:
                seen_urls.add(u)
                rows.append(row)

        if len(listings) < PAGE_SIZE:
            return rows, last_status

        # remaining pages
        for page_idx in range(1, total_pages):
            offset = page_idx * PAGE_SIZE
            await asyncio.sleep(random.uniform(0.08, 0.15))

            status, html = await self.fetch_page(offset)
            last_status = status or last_status
            if not html:
                continue

            soup = BeautifulSoup(html, "html.parser")
            listings = soup.find_all("li", class_="listing-result")
            if not listings:
                break

            for li in listings:
                row = self.parse_listing(li)
                if not row:
                    continue
                u = row.get("url")
                if u and u not in seen_urls:
                    seen_urls.add(u)
                    rows.append(row)

            if len(listings) < PAGE_SIZE:
                break

        return rows, last_status




# ============================================================
# POSTCODE TASK
# ============================================================

async def scrape_one_postcode(postcode: str, session: aiohttp.ClientSession) -> tuple[bool, int, int]:
    """
    Returns: (success, rows_count, status_code)
    """
    scraper = AsyncSpareRoomScraper(postcode, session)
    rows, status = await scraper.scrape_all()

    for r in rows:
        r["status"] = status
        add_row(r)

    # Consider success even with 0 rows if we got a 200 (valid empty area)
    if status == 200:
        return True, len(rows), status

    return False, len(rows), status

# ============================================================
# MAIN ORCHESTRATION
# ============================================================

async def run(csv_path: str, csv_column: str | None, concurrency: int, limit: int | None, retry_failed: bool):
    postcodes = load_postcodes_from_csv(csv_path, csv_column)

    if limit:
        postcodes = postcodes[:limit]

    print(f"📍 Loaded {len(postcodes):,} postcodes")

    state = load_state()

    if retry_failed and state["failed_postcodes"]:
        remaining = state["failed_postcodes"]
        state["failed_postcodes"] = []
        print(f"🔁 RETRY MODE — {len(remaining):,} postcodes")
    else:
        remaining = [pc for pc in postcodes if pc not in state["completed_postcodes"]]

    total = len(remaining)
    if total == 0:
        print("🎉 Nothing to scrape (all postcodes completed).")
        return

    start_ts = time.time()
    done = 0
    total_rows = 0

    connector = aiohttp.TCPConnector(limit=max(20, concurrency * 2), limit_per_host=max(4, concurrency))
    timeout = aiohttp.ClientTimeout(total=30, sock_connect=10, sock_read=20)

    sem = asyncio.Semaphore(concurrency)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:

        async def worker_task(pc: str):
            nonlocal done, total_rows

            async with sem:
                ok, rows_count, status = await scrape_one_postcode(pc, session)
                total_rows += rows_count

                if ok:
                    state["completed_postcodes"].add(pc)
                else:
                    state["failed_postcodes"].append(pc)

                done += 1
                print_progress(done, total, start_ts)

                # persist state periodically
                if done % 25 == 0:
                    save_state(state)

        tasks = [asyncio.create_task(worker_task(pc)) for pc in remaining]
        await asyncio.gather(*tasks)

    flush_parquet_shard()
    save_state(state)

    print("\n\n✅ DONE")
    print(f"   Postcodes processed: {done:,}/{total:,}")
    print(f"   Total rows scraped:  {total_rows:,}")
    print(f"   Deduped rows dropped: {DEDUPED:,}")
    print(f"   Failed postcodes:    {len(state['failed_postcodes']):,}")
    print(f"   Output dir:          {PARQUET_DIR}")

# ============================================================
# ENTRY POINT
# ============================================================

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default=str(CSV_PATH), help="Path to CSV containing postcodes")
    ap.add_argument("--column", default=None, help="CSV column name to read postcodes from (optional)")
    ap.add_argument("--concurrency", type=int, default=8, help="Concurrent postcodes to scrape")
    ap.add_argument("--limit", type=int, default=None, help="Limit number of postcodes (testing)")
    ap.add_argument("--retry-failed", action="store_true", help="Retry failed postcodes from state.json")
    return ap.parse_args()


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(run(args.csv, args.column, args.concurrency, args.limit, args.retry_failed))
