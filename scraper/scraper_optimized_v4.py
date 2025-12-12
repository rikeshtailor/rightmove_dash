# ============================================================
# scraper_optimized_v2_retry-enabled.py
# FULL HIGH-PERFORMANCE RIGHTMOVE SCRAPER (Playwright + Async)
# ============================================================

import os
import re
import gc
import sys
import json
import time
import queue
import socket
import random
import asyncio
import aiohttp
import pandas as pd
from pathlib import Path
from threading import Thread
from bs4 import BeautifulSoup
from multiprocessing import Process, Queue, freeze_support
from playwright.async_api import async_playwright

import pyarrow as pa
import pyarrow.parquet as pq

# ============================================================
# FLAGS
# ============================================================

RETRY_FAILED = "--retry-failed" in sys.argv

# ============================================================
# CONFIG
# ============================================================

BASE_DIR = Path(__file__).resolve().parent
PARQUET_DIR = BASE_DIR / "parquet"
PARQUET_DIR.mkdir(exist_ok=True)

STATE_FILE = BASE_DIR / "state.json"

OUTCODE_CSV = r"C:\rightmove_monitor\scraper\outercode_to_postcode_master.csv"

OUTCODE_WORKERS = 8
DETAIL_WORKERS = 8

PAGE_SIZE = 24
MAX_PAGES = 50

# DETAIL CONCURRENCY
START_CONCURRENCY = 8
MAX_CONCURRENCY = 15
MIN_CONCURRENCY = 8

socket.setdefaulttimeout(20)  # Windows fix

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/122 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Safari/605.1.15",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:122.0) Firefox/122.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/121 Safari/537.36",
]


# ============================================================
# STATE HANDLING
# ============================================================

def load_state():
    if STATE_FILE.exists():
        try:
            with open(STATE_FILE, "r") as f:
                d = json.load(f)
        except:
            d = {}
        return {
            "completed_outcodes": set(d.get("completed_outcodes", [])),
            "collected_urls": set(d.get("collected_urls", [])),
        }
    return {"completed_outcodes": set(), "collected_urls": set()}


def save_state(completed, urls):
    with open(STATE_FILE, "w") as f:
        json.dump({
            "completed_outcodes": list(completed),
            "collected_urls": list(urls),
        }, f, indent=2)


# ============================================================
# LOAD OUTCODES
# ============================================================

def load_outcodes():
    df = pd.read_csv(OUTCODE_CSV)
    outcodes = [f"OUTCODE^{int(x)}" for x in df["OuterCode"]]
    print(f"📍 Loaded {len(outcodes)} OUTCODE identifiers")
    return outcodes


# ============================================================
# URL PROGRESS
# ============================================================

def print_url_progress(current, total, count, start_time):
    frac = current / total
    pct = int(frac * 100)
    bar_len = 30
    fill = int(bar_len * frac)
    bar = "#" * fill + "-" * (bar_len - fill)

    elapsed = time.time() - start_time
    if current > 20:
        eta = (elapsed / frac) - elapsed
    else:
        eta = 99999

    eta = min(max(eta, 0), 7200)
    eta_str = f"{int(eta//60):02d}:{int(eta%60):02d}"

    print(
        f"[URL {bar}] {pct}% ({current}/{total}) "
        f"| URLs={count:,} | ETA={eta_str}",
        end="\r",
        flush=True
    )


# ============================================================
# PLAYWRIGHT BLOCKER
# ============================================================

async def block_assets(route, request):
    if request.resource_type in ("image", "media", "font", "stylesheet"):
        return await route.abort()
    await route.continue_()


async def fetch_page_urls(page, outcode_id, index):
    url = (
        "https://www.rightmove.co.uk/property-for-sale/find.html?"
        f"locationIdentifier={outcode_id}&sortType=1&index={index}"
    )
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=15000)
    except:
        return set()

    anchors = await page.query_selector_all("a.propertyCard-link")
    urls = set()

    for a in anchors:
        href = await a.get_attribute("href")
        if href and "/properties/" in href:
            urls.add("https://www.rightmove.co.uk" + href.split("#")[0])

    return urls


async def scrape_outcodes_with_browser(outcodes, result_queue):
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)

        for oc in outcodes:
            await asyncio.sleep(random.uniform(0.05, 0.15))

            context = await browser.new_context()
            await context.route("**/*", block_assets)
            page = await context.new_page()

            urls = set()

            for p in range(MAX_PAGES):
                index = p * PAGE_SIZE
                try:
                    page_urls = await fetch_page_urls(page, oc, index)
                except:
                    await asyncio.sleep(0.2)
                    continue

                if p == 0 and not page_urls:
                    await asyncio.sleep(0.4)
                    continue

                if not page_urls:
                    break

                urls.update(page_urls)
                if len(page_urls) < PAGE_SIZE:
                    break

            await context.close()
            result_queue.put({"outcode": oc, "urls": list(urls)})

        result_queue.put("DONE")


def outcode_worker(outcodes, result_queue):
    asyncio.run(scrape_outcodes_with_browser(outcodes, result_queue))


# ============================================================
# DETAIL SCRAPER
# ============================================================

PAGE_MODEL_RE = re.compile(
    r"window\.PAGE_MODEL\s*=\s*({.*?})\s*;</script>",
    re.DOTALL
)

def extract_page_model(html):
    m = PAGE_MODEL_RE.search(html)
    if not m:
        return None
    try:
        return json.loads(m.group(1))
    except:
        return None


def normalize_row(url, model, status):
    if not model:
        return {
            "url": url,
            "address": None,
            "postcode": None,
            "price": None,
            "bedrooms": None,
            "property_type": None,
            "floor_area": None,
            "status": int(status) if str(status).isdigit() else None,
        }

    prop = model.get("propertyData", {})
    analytics = model.get("analyticsInfo", {}).get("analyticsProperty", {})
    addr = prop.get("address", {})

    outcode = addr.get("outcode", "")
    incode = addr.get("incode", "")

    # Floor area
    fa = None
    for s in prop.get("sizings", []) or []:
        disp = s.get("display")
        if disp:
            fa = disp
            break

    return {
        "url": url,
        "address": addr.get("displayAddress"),
        "postcode": analytics.get("postcode") or f"{outcode} {incode}".strip(),
        "price": analytics.get("price"),
        "bedrooms": prop.get("bedrooms"),
        "property_type": prop.get("propertySubType") or prop.get("propertyType"),
        "floor_area": fa,
        "status": int(status) if str(status).isdigit() else None,
    }


async def fetch_detail(session, url):
    for attempt in range(4):
        try:
            headers = {
                "User-Agent": random.choice(USER_AGENTS),
                "Accept-Language": "en-GB,en;q=0.9",
            }
            async with session.get(url, headers=headers) as resp:
                status = resp.status

                if status != 200:
                    if attempt == 0:
                        print(f"\nERROR {status} for {url}")
                    await asyncio.sleep(0.25 + attempt * 0.3)
                    continue

                html = await resp.text()
                model = extract_page_model(html)
                return normalize_row(url, model, status)

        except Exception as e:
            if attempt == 0:
                print(f"\nNETWORK ERROR for {url}: {e}")
            await asyncio.sleep(0.3 + attempt * 0.4)

    return normalize_row(url, None, "fail")


async def detail_pass(urls, label, start_conc, max_conc, result_queue):
    urls = list(set(urls))
    total = len(urls)

    print(f"\n🔎 Starting {label} — {total:,} URLs")

    concurrency = start_conc
    MINC = MIN_CONCURRENCY
    MAXC = max_conc

    completed = 0
    errors = 0
    retry_urls = []
    latencies = []

    start_time = time.time()

    connector = aiohttp.TCPConnector(limit=60, limit_per_host=6, ssl=False)
    timeout = aiohttp.ClientTimeout(total=30)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:

        i = 0
        while i < total:

            batch = urls[i:i + concurrency]
            sem = asyncio.Semaphore(concurrency)

            async def run_one(u):
                async with sem:
                    t0 = time.perf_counter()
                    row = await fetch_detail(session, u)
                    dt = time.perf_counter() - t0

                    latencies.append(dt)
                    if len(latencies) > 300:
                        latencies.pop(0)

                    nonlocal errors
                    if row["status"] != 200:
                        retry_urls.append(u)
                        errors += 1

                    result_queue.put(row)
                    return row

            tasks = [run_one(u) for u in batch]

            for coro in asyncio.as_completed(tasks):
                await coro
                completed += 1

                frac = completed / total
                pct = int(frac * 100)
                bar_len = 30
                fill = int(bar_len * frac)
                bar = "#" * fill + "-" * (bar_len - fill)

                if completed < 300:
                    eta_str = "--:--"
                else:
                    elapsed = time.time() - start_time
                    eta = (elapsed / frac) - elapsed
                    eta = min(max(eta, 0), 7200)
                    eta_str = f"{int(eta//60):02d}:{int(eta%60):02d}"

                print(
                    f"[{label} {bar}] {pct}% ({completed:,}/{total:,}) "
                    f"| ERR={errors:,} | CONC={concurrency} | ETA={eta_str}",
                    end="\r",
                    flush=True
                )

            # adaptive tuning
            if latencies:
                avg = sum(latencies)/len(latencies)
            else:
                avg = 0.25

            err_rate = errors / max(1, completed)

            if completed < 300:
                concurrency = START_CONCURRENCY
            else:
                if avg < 0.18 and err_rate < 0.008:
                    concurrency = min(MAXC, concurrency + 2)
                elif avg > 0.28 or err_rate > 0.015:
                    concurrency = max(MINC, concurrency - 10)

            i += concurrency
            gc.collect()

    print(f"\n{label} done → {completed:,} rows, {errors:,} errors")
    return retry_urls


async def detail_worker_async(urls, worker_id, result_queue):
    print(f"\n🧵 Worker {worker_id} starting with {len(urls):,} URLs")

    # PASS1
    retry1 = await detail_pass(
        urls, f"W{worker_id}-PASS1",
        start_conc=START_CONCURRENCY, max_conc=MAX_CONCURRENCY,
        result_queue=result_queue
    )

    # PASS2
    retry2 = []
    if retry1:
        retry2 = await detail_pass(
            retry1, f"W{worker_id}-PASS2",
            start_conc=30, max_conc=60,
            result_queue=result_queue
        )

    # PASS3
    retry3 = []
    if retry2:
        retry3 = await detail_pass(
            retry2, f"W{worker_id}-PASS3",
            start_conc=10, max_conc=20,
            result_queue=result_queue
        )

    if retry3:
        fn = f"failed_urls_worker{worker_id}.txt"
        with open(fn, "w") as f:
            for u in retry3:
                f.write(u + "\n")
        print(f"⚠️ Worker {worker_id}: {len(retry3):,} final failures → {fn}")

    result_queue.put(f"DONE-{worker_id}")


def detail_worker(urls, worker_id, queue):
    asyncio.run(detail_worker_async(urls, worker_id, queue))


# ============================================================
# PARQUET WRITER
# ============================================================

def final_parquet_name():
    ts = time.strftime("%Y-%m-%d")
    return PARQUET_DIR / f"rightmove_{ts}.parquet"


class ParquetCollector:
    def __init__(self):
        self.rows = []

    def add_row(self, row):
        self.rows.append(row)

    def write(self, out):
        if not self.rows:
            print("⚠️ No rows to write!")
            return
        df = pd.DataFrame(self.rows)
        table = pa.Table.from_pandas(df, preserve_index=False)

        pq.write_table(table, out, compression=None)
        print(f"\n📦 Wrote final parquet → {out} ({len(df):,} rows)")


def writer_thread_func(in_queue, writer, expected_done):
    done = 0
    while True:
        item = in_queue.get()
        if isinstance(item, str) and item.startswith("DONE-"):
            done += 1
            if done == expected_done:
                break
            continue
        writer.add_row(item)

    writer.write(final_parquet_name())


# ============================================================
# FAILED URL RETRY SUPPORT
# ============================================================

def load_failed_urls():
    failed = set()
    for fn in os.listdir(BASE_DIR):
        if fn.startswith("failed_urls_worker") and fn.endswith(".txt"):
            with open(BASE_DIR / fn, "r") as f:
                for url in f:
                    url = url.strip()
                    if url:
                        failed.add(url)
    return list(failed)


def run_retry_failed():
    failed = load_failed_urls()
    if not failed:
        print("✔ No failed URLs found.")
        return

    print(f"🔁 Retrying {len(failed):,} failed URLs...")

    result_queue = Queue()
    writer = ParquetCollector()

    thread = Thread(target=writer_thread_func, args=(result_queue, writer, 1), daemon=True)
    thread.start()

    p = Process(target=detail_worker, args=(failed, 0, result_queue))
    p.start()
    p.join()

    thread.join()
    print("🎉 Retry complete!")


# ============================================================
# MAIN
# ============================================================

def main():

    if RETRY_FAILED:
        return run_retry_failed()

    # Load state
    state = load_state()
    completed = state["completed_outcodes"]
    collected_urls = state["collected_urls"]

    outcodes = load_outcodes()
    remaining = [oc for oc in outcodes if oc not in completed]

    print(f"➡️ Outcodes remaining: {len(remaining)}")

    # -----------------------------
    # URL COLLECTION
    # -----------------------------

    if remaining:
        chunks = [remaining[i::OUTCODE_WORKERS] for i in range(OUTCODE_WORKERS)]
        q = Queue()
        procs = []

        for chunk in chunks:
            p = Process(target=outcode_worker, args=(chunk, q))
            p.start()
            procs.append(p)

        count = 0
        done = 0
        start = time.time()

        while done < OUTCODE_WORKERS:
            msg = q.get()
            if msg == "DONE":
                done += 1
                continue

            oc = msg["outcode"]
            urls = msg["urls"]
            completed.add(oc)
            collected_urls.update(urls)
            count += 1

            save_state(completed, collected_urls)

            print_url_progress(count, len(remaining), len(collected_urls), start)

        for p in procs:
            p.join()

    print("\n\n🌍 URL COLLECTION COMPLETE")
    print(f"Total URLs: {len(collected_urls):,}")

    # -----------------------------
    # DETAIL SCRAPING
    # -----------------------------
    print("\n🔧 Starting detail scraping...")

    urls = list(collected_urls)
    chunks = [urls[i::DETAIL_WORKERS] for i in range(DETAIL_WORKERS)]

    q = Queue()
    writer = ParquetCollector()
    writer_thread = Thread(target=writer_thread_func, args=(q, writer, DETAIL_WORKERS), daemon=True)
    writer_thread.start()

    procs = []
    for i, chunk in enumerate(chunks):
        p = Process(target=detail_worker, args=(chunk, i, q))
        p.start()
        procs.append(p)

    for p in procs:
        p.join()

    writer_thread.join()

    print("\n🎉 ALL DONE!")
    print(f"Data stored: {PARQUET_DIR}")


# ============================================================
# ENTRY
# ============================================================

if __name__ == "__main__":
    freeze_support()
    main()
