# ============================================================
# PART 1 — IMPORTS, CONFIG, STATE, CSV, PLAYWRIGHT URL COLLECTOR
# ============================================================

import os
import re
import time
import json
import random
import asyncio
import gc
import socket
import pandas as pd
import aiohttp
from pathlib import Path
from bs4 import BeautifulSoup
from multiprocessing import Process, Queue, freeze_support
from playwright.async_api import async_playwright

# Windows networking stability
socket.setdefaulttimeout(20)

# ============================================================
# CONFIGURATION
# ============================================================

BASE_DIR = Path(__file__).resolve().parent
PARQUET_DIR = BASE_DIR / "parquet"
PARQUET_DIR.mkdir(exist_ok=True)

STATE_FILE = BASE_DIR / "state.json"

OUTCODE_CSV = r"C:\rightmove_monitor\scraper\outercode_to_postcode_master.csv"

# Worker counts
OUTCODE_WORKERS = 8     # Playwright URL collectors
DETAIL_WORKERS = 4       # Async detail workers

PAGE_SIZE = 24
MAX_PAGES = 50           # Max pagination per outcode

# Detail scraping concurrency
START_CONCURRENCY = 8
MAX_CONCURRENCY = 15
MIN_CONCURRENCY = 8

# Parquet batching
BATCH_SIZE = 5000        # Large batches reduce I/O overhead


# ============================================================
# USER AGENT POOL — randomised to reduce 410 errors
# ============================================================

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17 Safari/605.1.15",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121 Safari/537.36",
]

HEADERS = {
    "User-Agent": USER_AGENTS[0],
    "Accept-Language": "en-GB,en;q=0.9"
}


# ============================================================
# STATE MANAGEMENT
# ============================================================

def load_state():
    """Load scraping state safely."""
    if STATE_FILE.exists():
        try:
            with open(STATE_FILE, "r") as f:
                data = json.load(f)
        except:
            data = {}

        completed = data.get("completed_outcodes", data.get("completed_locations", []))
        urls = data.get("collected_urls", [])

        return {
            "completed_outcodes": set(completed),
            "collected_urls": set(urls)
        }

    return {
        "completed_outcodes": set(),
        "collected_urls": set()
    }


def save_state(completed_outcodes, collected_urls):
    data = {
        "completed_outcodes": list(completed_outcodes),
        "collected_urls": list(collected_urls),
    }
    with open(STATE_FILE, "w") as f:
        json.dump(data, f, indent=2)


# ============================================================
# OUTCODE LOADING
# ============================================================

def load_outcodes():
    df = pd.read_csv(OUTCODE_CSV)
    outcodes = [f"OUTCODE^{int(x)}" for x in df["OuterCode"]]
    print(f"📍 Loaded {len(outcodes)} OUTCODE identifiers")
    return outcodes


# ============================================================
# PROGRESS BAR WITH ETA
# ============================================================

def print_url_progress(current, total, url_count, start_time):
    fraction = current / total
    percent = int(fraction * 100)
    bar_len = 30
    fill = int(bar_len * fraction)
    bar = "#" * fill + "-" * (bar_len - fill)

    elapsed = time.time() - start_time
    if current > 10:
        eta = (elapsed / fraction) - elapsed
    else:
        eta = 99999  # unstable early phase

    eta = min(max(eta, 0), 7200)  # clamp to 0–2h

    mins = int(eta // 60)
    secs = int(eta % 60)
    eta_str = f"{mins:02d}:{secs:02d}"

    print(
        f"[URL {bar}] {percent}% ({current}/{total}) | URLs={url_count:,} | ETA={eta_str}",
        end="\r",
        flush=True,
    )


# ============================================================
# PLAYWRIGHT REQUEST BLOCKER — reduces page load by ~80%
# ============================================================

async def block_assets(route, request):
    if request.resource_type != "document":
        return await route.abort()
    await route.continue_()


# ============================================================
# FETCH LISTING URLs PER PAGE
# ============================================================

async def fetch_page_urls(page, outcode_id, index):
    url = (
        "https://www.rightmove.co.uk/property-for-sale/find.html?"
        f"locationIdentifier={outcode_id}&sortType=1&useLocationIdentifier=true&index={index}"
    )

    try:
        await page.goto(url, timeout=15000)
    except Exception:
        return set()

    anchors = await page.query_selector_all("a.propertyCard-link")
    urls = set()

    for a in anchors:
        href = await a.get_attribute("href")
        if href and "/properties/" in href:
            urls.add("https://www.rightmove.co.uk" + href.split("#")[0])

    return urls


# ============================================================
# PLAYWRIGHT WORKER — URL COLLECTION FOR OUTCODES
# ============================================================

# ============================================================
# SUPER-OPTIMIZED PLAYWRIGHT URL SCRAPER (PATCHED VERSION)
# ============================================================

async def block_assets(route, request):
    """
    BLOCK EVERYTHING EXCEPT HTML DOCUMENTS.
    This reduces page load time by 70–90%.
    """
    if request.resource_type != "document":
        return await route.abort()
    await route.continue_()


async def fetch_page_urls(page, outcode_id, index):
    """
    FAST URL extractor – no JS, no waiting.
    Uses regex to extract property IDs from raw HTML.
    """

    url = (
        "https://www.rightmove.co.uk/property-for-sale/find.html?"
        f"locationIdentifier={outcode_id}&sortType=1"
        f"&useLocationIdentifier=true&index={index}"
    )

    try:
        # DO NOT wait for JS or network – fastest navigation
        await page.goto(url, timeout=8000)
    except Exception:
        return set()

    # Extract raw HTML (very fast)
    try:
        html = await page.content()
    except:
        return set()

    # Extract property URLs via regex
    matches = re.findall(r"/properties/\d+", html)
    if not matches:
        return set()

    return {"https://www.rightmove.co.uk" + m for m in matches}


async def scrape_outcodes_with_browser(outcodes, result_queue):
    """
    EXTREMELY FAST URL SCRAPER:
    - Reuses ONE browser, ONE context, ONE page.
    - JavaScript disabled
    - Blocks all subresources
    """

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)

        # Context reused across ALL outcodes (big speed win)
        context = await browser.new_context(
            java_script_enabled=False,
            user_agent=random.choice(USER_AGENTS)
        )
        await context.route("**/*", block_assets)

        page = await context.new_page()

        for oc in outcodes:

            # Random jitter prevents server throttling
            await asyncio.sleep(random.uniform(0.02, 0.10))

            urls = set()

            for p in range(MAX_PAGES):
                index = p * PAGE_SIZE
                page_urls = await fetch_page_urls(page, oc, index)

                # Retry empty first page once (sometimes RM glitches)
                if p == 0 and not page_urls:
                    await asyncio.sleep(0.15)
                    page_urls = await fetch_page_urls(page, oc, index)

                if not page_urls:
                    break

                urls.update(page_urls)

                if len(page_urls) < PAGE_SIZE:
                    break

            # Send results to main process
            result_queue.put({"outcode": oc, "urls": list(urls)})

        result_queue.put("DONE")


def outcode_worker(outcodes, result_queue):
    """
    Worker wrapper — starts an event loop and runs the Playwright scraper.
    """
    asyncio.set_event_loop(asyncio.new_event_loop())
    loop = asyncio.get_event_loop()
    loop.run_until_complete(scrape_outcodes_with_browser(outcodes, result_queue))

# ============================================================
# PART 2 — ULTRA-FAST ASYNC DETAIL SCRAPER (8 WORKERS)
# ============================================================

# ------------------------------------------------------------
# REGEX PAGE_MODEL extraction (fastest)
# ------------------------------------------------------------

PAGE_MODEL_RE = re.compile(
    r"window\.PAGE_MODEL\s*=\s*({.*?})\s*;</script>",
    re.DOTALL
)

def extract_page_model_fast(html):
    m = PAGE_MODEL_RE.search(html)
    if not m:
        return None
    try:
        return json.loads(m.group(1))
    except:
        return None


# ------------------------------------------------------------
# NORMALISE OUTPUT ROWS
# ------------------------------------------------------------

def normalize_detail_row(url, model, status):
    if not model:
        return {
            "url": url,
            "address": None,
            "postcode": None,
            "price": None,
            "bedrooms": None,
            "property_type": None,
            "floor_area": None,
            "status": status
        }

    prop = model.get("propertyData", {})
    analytics = model.get("analyticsInfo", {}).get("analyticsProperty", {})
    addr = prop.get("address", {})

    outcode = addr.get("outcode", "")
    incode = addr.get("incode", "")

    # Floor area
    floor_area = None
    for s in prop.get("sizings", []) or []:
        disp = s.get("display")
        if disp:
            floor_area = disp
            break

    return {
        "url": str(row.get("url") or ""),
        "address": row.get("address") or "",
        "postcode": row.get("postcode") or "",
        "price": row.get("price"),
        "bedrooms": row.get("bedrooms"),
        "property_type": row.get("property_type") or "",
        "floor_area": row.get("floor_area") or "",
        "status": str(row.get("status")),
        "scraped_at": pd.Timestamp.utcnow(),
    }


# ------------------------------------------------------------
# FETCH PROPERTY DETAIL (with retry & jitter)
# ------------------------------------------------------------

async def fetch_detail(session, url):
    """Fetch PAGE_MODEL for a property with retries."""
    for attempt in range(4):
        try:
            dyn_headers = {
                "User-Agent": random.choice(USER_AGENTS),
                "Accept-Language": "en-GB,en;q=0.9",
                "Referer": "https://www.rightmove.co.uk/",
                "Connection": "keep-alive"
            }

            async with session.get(url, headers=dyn_headers) as resp:
                status = resp.status

                # Rate-limit signals → wait & retry
                if status in (429, 503):
                    await asyncio.sleep(0.5 + attempt * 0.7)
                    continue

                # Non-200 → retry
                if status != 200:
                    if attempt == 0:
                        print(f"\nERROR {status} for {url}")
                    await asyncio.sleep(0.25 + attempt * 0.3)
                    continue

                html = await resp.text()
                model = extract_page_model_fast(html)
                return normalize_detail_row(url, model, status)

        except Exception as e:
            if attempt == 0:
                print(f"\nNETWORK ERROR for {url}: {e}")
            await asyncio.sleep(random.uniform(0.25, 0.80) + attempt * 0.40)

    # Complete failure
    return normalize_detail_row(url, None, "fail")


# ------------------------------------------------------------
# ONE SCRAPING PASS (adaptive concurrency)
# ------------------------------------------------------------

async def detail_pass(urls, label, start_conc, max_conc, result_queue):
    total = len(urls)
    urls = list(set(urls))

    print(f"\n🔎 Starting {label} — {total:,} URLs")

    concurrency = start_conc
    MINC = MIN_CONCURRENCY
    MAXC = max_conc

    completed = 0
    errors = 0
    retry_urls = []
    latencies = []

    start_time = time.time()

    # Windows-optimized TCPConnector
    connector = aiohttp.TCPConnector(
        limit=60,
        limit_per_host=6,
        enable_cleanup_closed=True,
        ttl_dns_cache=300,
        force_close=False,
        ssl=False,
    )
    timeout = aiohttp.ClientTimeout(total=30)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:

        i = 0
        while i < total:

            # batch jitter
            await asyncio.sleep(random.uniform(0.01, 0.10))

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

            tasks = [run_one(url) for url in batch]

            # Consume tasks as they complete
            for coro in asyncio.as_completed(tasks):
                await coro
                completed += 1

                # --------------- PROGRESS BAR + ETA ------------------

                frac = completed / total
                pct = int(frac * 100)

                elapsed = time.time() - start_time

                # ----- Improved ETA logic -----
                # Suppress ETA until enough data has accumulated to avoid garbage values
                if completed < 500:
                    eta_str = "--:--"
                else:
                    eta = (elapsed / frac) - elapsed
                    eta = min(max(eta, 0), 7200)  # clamp to <= 2 hours

                    mins = int(eta // 60)
                    secs = int(eta % 60)
                    eta_str = f"{mins:02d}:{secs:02d}"
                # ------------------------------

                bar_len = 30
                fill = int(bar_len * frac)
                bar = "#" * fill + "-" * (bar_len - fill)

                print(
                    f"[{label} {bar}] {pct}% ({completed:,}/{total:,}) "
                    f"| ERR={errors:,} | CONC={concurrency} | ETA={eta_str}",
                    end="\r",
                    flush=True
                )
                # -----------------------------------------------------


            # Adaptive concurrency logic
            if latencies:
                avg = sum(latencies) / len(latencies)
            else:
                avg = 0.22

            err_rate = errors / max(1, completed)

            if completed < 500:
                concurrency = START_CONCURRENCY
            else:
                # Adaptive concurrency tuning
                if avg < 0.18 and err_rate < 0.008:
                    concurrency = min(MAXC, concurrency + 2)
                elif avg > 0.28 or err_rate > 0.015:
                    concurrency = max(MINC, concurrency - 10)

            i += concurrency
            gc.collect()

    print(f"\n{label} done → {completed:,} rows, {errors:,} errors")
    return retry_urls


# ------------------------------------------------------------
# MULTI-PASS SCRAPING: PASS1 → PASS2 → PASS3
# ------------------------------------------------------------

async def detail_worker_async(urls, worker_id, result_queue):
    print(f"\n🧵 Worker {worker_id} starting with {len(urls):,} URLs")

    await asyncio.sleep(random.uniform(1.0, 3.0))  # worker startup jitter

    # PASS 1: Fast mode
    retry1 = await detail_pass(
        urls,
        label=f"W{worker_id}-PASS1",
        start_conc=START_CONCURRENCY,
        max_conc=MAX_CONCURRENCY,
        result_queue=result_queue
    )

    # PASS 2: Safe mode
    retry2 = []
    if retry1:
        retry2 = await detail_pass(
            retry1,
            label=f"W{worker_id}-PASS2",
            start_conc=30,
            max_conc=60,
            result_queue=result_queue
        )

    # PASS 3: Ultra-safe mode
    retry3 = []
    if retry2:
        retry3 = await detail_pass(
            retry2,
            label=f"W{worker_id}-PASS3",
            start_conc=10,
            max_conc=20,
            result_queue=result_queue
        )

    # Save failures
    if retry3:
        fn = f"failed_urls_worker{worker_id}.txt"
        with open(fn, "w") as f:
            for u in retry3:
                f.write(u + "\n")
        print(f"⚠️ Worker {worker_id}: {len(retry3):,} final failures saved → {fn}")

    print(f"🧵 Worker {worker_id} finished.")
    result_queue.put(f"DONE-{worker_id}")


def detail_worker(urls, worker_id, result_queue):
    asyncio.run(detail_worker_async(urls, worker_id, result_queue))

# ============================================================
# PART 3 — PARQUET WRITER + MAIN ORCHESTRATION
# ============================================================

import queue
from threading import Thread
import pyarrow as pa
import pyarrow.parquet as pq


# ============================================================
# FINAL PARQUET FILE NAME (DATE-BASED)
# ============================================================

def final_parquet_name():
    ts = time.strftime("%Y-%m-%d")
    return PARQUET_DIR / f"rightmove_{ts}.parquet"


# ============================================================
# PARQUET COLLECTOR (large in-memory buffer)
# ============================================================

class ParquetCollector:
    """
    Collects rows produced by detail workers and writes one final Parquet file.
    Reduces disk I/O massively and ensures consistent schema.
    """
    def __init__(self):
        self.buffer = []

    def add_row(self, row):
        self.buffer.append(row)

    def write_final(self, output_path):
        if not self.buffer:
            print("⚠️ No rows to write.")
            return

        df = pd.DataFrame(self.buffer)

        table = pa.Table.from_pandas(df, preserve_index=False)

        pq.write_table(
            table,
            output_path,
            compression=None  # user requested uncompressed
        )

        print(f"\n📦 Final Parquet written → {output_path} ({len(df):,} rows)")


# ============================================================
# QUEUE CONSUMER THREAD (runs in background)
# ============================================================

def writer_thread_func(result_queue, writer, worker_count):
    """
    Runs in its own thread:
    - Consumes rows from result_queue
    - Detects DONE signals from the 8 workers
    - Writes final Parquet file when all workers complete
    """
    done_signals = 0

    while True:
        item = result_queue.get()

        if isinstance(item, str) and item.startswith("DONE-"):
            done_signals += 1
            print(f"\nWriter: received {item}")
            if done_signals == worker_count:
                print("Writer: All workers finished.")
                break
            continue

        writer.add_row(item)

    # All workers done — save output
    output_path = final_parquet_name()
    writer.write_final(output_path)


# ============================================================
# MAIN ORCHESTRATION
# ============================================================

def main():
    state = load_state()
    completed_outcodes = state["completed_outcodes"]
    collected_urls = state["collected_urls"]

    outcodes = load_outcodes()
    remaining = [oc for oc in outcodes if oc not in completed_outcodes]

    print(f"➡️ Remaining OUTCODEs to scrape: {len(remaining)}")

    # --------------------------------------------------------
    # 1) URL COLLECTION (Playwright workers)
    # --------------------------------------------------------
    if remaining:
        chunks = [remaining[i::OUTCODE_WORKERS] for i in range(OUTCODE_WORKERS)]
        result_queue = Queue()
        processes = []

        for chunk in chunks:
            p = Process(target=outcode_worker, args=(chunk, result_queue))
            p.start()
            processes.append(p)

        processed = 0
        total_needed = len(remaining)
        start_time = time.time()
        done_count = 0

        while done_count < OUTCODE_WORKERS:
            msg = result_queue.get()

            if msg == "DONE":
                done_count += 1
                continue

            oc = msg["outcode"]
            urls = msg["urls"]

            collected_urls.update(urls)
            completed_outcodes.add(oc)
            processed += 1

            save_state(completed_outcodes, collected_urls)

            print_url_progress(
                processed,
                total_needed,
                len(collected_urls),
                start_time
            )

        for p in processes:
            p.join()

    print("\n\n🌍 URL COLLECTION COMPLETE")
    print(f"Total unique URLs collected: {len(collected_urls):,}")

    # --------------------------------------------------------
    # 2) DETAIL SCRAPING (8 async workers)
    # --------------------------------------------------------
    print("\n🔧 Starting detail scraping with 8 async workers...")

    urls = list(collected_urls)
    url_chunks = [urls[i::DETAIL_WORKERS] for i in range(DETAIL_WORKERS)]

    result_queue = Queue()
    writer = ParquetCollector()

    # Start writer thread
    writer_thread = Thread(
        target=writer_thread_func,
        args=(result_queue, writer, DETAIL_WORKERS),
        daemon=True
    )
    writer_thread.start()

    # Start detail worker processes
    processes = []
    for worker_id, chunk in enumerate(url_chunks):
        p = Process(target=detail_worker, args=(chunk, worker_id, result_queue))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

    writer_thread.join()

    print("\n🎉 ALL DONE! Final Parquet is ready inside:")
    print(f"   {PARQUET_DIR}\n")


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    freeze_support()
    main()
