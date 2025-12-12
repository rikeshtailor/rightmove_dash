# ============================================================
# RIGHTMOVE SCRAPER — OPTIMIZED v2 (FULL COMBINED SCRIPT)
# Includes:
#  • Resume-safe Playwright URL collection
#  • Multi-worker async detail scraping (3-pass)
#  • Adaptive concurrency per worker
#  • Single final Parquet output (efficient)
#  • Type-stable schema
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

# Stability for Windows networking
socket.setdefaulttimeout(20)

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

# Detail concurrency (per async worker)
START_CONCURRENCY = 8
MAX_CONCURRENCY = 15
MIN_CONCURRENCY = 8

BATCH_SIZE = 5000   # batch limit for shard writing (unused in final collector)

# Rotating user agent pool
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17 Safari/605.1.15",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121 Safari/537.36",
]

HEADERS = {"User-Agent": USER_AGENTS[0], "Accept-Language": "en-GB,en;q=0.9"}

# ============================================================
# STATE HANDLING
# ============================================================

def load_state():
    """Load state.json and recover resume state."""
    if STATE_FILE.exists():
        try:
            data = json.load(open(STATE_FILE, "r"))
        except:
            data = {}

        return {
            "completed_outcodes": set(data.get("completed_outcodes", [])),
            "collected_urls": set(data.get("collected_urls", [])),
        }
    return {"completed_outcodes": set(), "collected_urls": set()}


def save_state(completed_outcodes, collected_urls):
    json.dump(
        {
            "completed_outcodes": list(completed_outcodes),
            "collected_urls": list(collected_urls),
        },
        open(STATE_FILE, "w"),
        indent=2,
    )

# ============================================================
# OUTCODE LOADING
# ============================================================

def load_outcodes():
    df = pd.read_csv(OUTCODE_CSV)
    outcodes = [f"OUTCODE^{int(x)}" for x in df["OuterCode"]]
    print(f"📍 Loaded {len(outcodes)} OUTCODE identifiers")
    return outcodes

# ============================================================
# PROGRESS BAR FOR URL COLLECTION
# ============================================================

def print_url_progress(current, total, url_count, start_time):
    fraction = current / max(total, 1)
    percent = int(fraction * 100)
    bar_len = 30
    fill = int(bar_len * fraction)
    bar = "#" * fill + "-" * (bar_len - fill)

    elapsed = time.time() - start_time
    eta = 99999 if current < 10 else (elapsed / fraction) - elapsed
    eta = min(max(eta, 0), 7200)

    mins = int(eta // 60)
    secs = int(eta % 60)

    print(
        f"[URL {bar}] {percent}% ({current}/{total}) | URLs={url_count:,} | ETA={mins:02d}:{secs:02d}",
        end="\r",
    )

# ============================================================
# PLAYWRIGHT REQUEST BLOCKER (speeds up load)
# ============================================================

async def block_assets(route, request):
    if request.resource_type in ("image", "media", "font", "stylesheet"):
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

# ============================================================
# PLAYWRIGHT OUTCODE SCRAPER WORKER
# ============================================================

async def scrape_outcodes_with_browser(outcodes, result_queue):
    async with async_playwright() as pw:
        # Use persistent context → major speed boost
        browser = await pw.chromium.launch_persistent_context(
            user_data_dir=str(BASE_DIR / f"pwcache_{random.randint(1,9999)}"),
            headless=True,
            viewport={"width": 1280, "height": 800},
            locale="en-GB",
        )
        page = await browser.new_page()
        await page.route("**/*", block_assets)

        for oc in outcodes:
            await asyncio.sleep(random.uniform(0.02, 0.10))

            urls = set()

            for p in range(MAX_PAGES):
                index = p * PAGE_SIZE
                try:
                    page_urls = await fetch_page_urls(page, oc, index)
                except:
                    await asyncio.sleep(0.2)
                    continue

                if p == 0 and not page_urls:
                    await asyncio.sleep(random.uniform(0.2, 0.5))
                    continue

                if not page_urls:
                    break

                urls.update(page_urls)

                if len(page_urls) < PAGE_SIZE:
                    break

            result_queue.put({"outcode": oc, "urls": list(urls)})

        await browser.close()

    result_queue.put("DONE")

def outcode_worker(outcodes, result_queue):
    asyncio.set_event_loop(asyncio.new_event_loop())
    asyncio.get_event_loop().run_until_complete(
        scrape_outcodes_with_browser(outcodes, result_queue)
    )

# ============================================================
# DETAIL SCRAPER - PAGE_MODEL extraction
# ============================================================

PAGE_MODEL_RE = re.compile(
    r"window\.PAGE_MODEL\s*=\s*({.*?})\s*;</script>",
    re.DOTALL
)

def extract_page_model_fast(html):
    """
    Robust PAGE_MODEL extractor that works even if Rightmove changes formatting.
    """
    # Locate the start
    start_index = html.find("window.PAGE_MODEL")
    if start_index == -1:
        return None

    # Find first { after PAGE_MODEL
    start_brace = html.find("{", start_index)
    if start_brace == -1:
        return None

    depth = 0
    end_brace = None

    for i in range(start_brace, len(html)):
        char = html[i]

        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                end_brace = i
                break

    if end_brace is None:
        return None

    json_text = html[start_brace:end_brace+1]

    try:
        return json.loads(json_text)
    except Exception:
        # Attempt cleanup
        try:
            cleaned = json_text.replace("\n", "").replace("\r", "")
            return json.loads(cleaned)
        except:
            return None

# ============================================================
# NORMALISE DETAIL OUTPUT (TYPE-STABLE)
# ============================================================

def normalize_detail_row(url, model, status):
    """Convert scraped dict into a type-safe row."""
    address = postcode = price = bedrooms = property_type = floor_area = None

    if model:
        prop = model.get("propertyData", {})
        analytics = model.get("analyticsInfo", {}).get("analyticsProperty", {})
        addr = prop.get("address", {})

        address = addr.get("displayAddress") or None
        outcode = addr.get("outcode", "")
        incode = addr.get("incode", "")

        postcode = analytics.get("postcode") or f"{outcode} {incode}".strip() or None
        price = analytics.get("price")
        price = int(price) if isinstance(price, (int, float, str)) and str(price).isdigit() else None

        bedrooms = prop.get("bedrooms")
        if isinstance(bedrooms, str) and bedrooms.isdigit():
            bedrooms = int(bedrooms)

        property_type = prop.get("propertySubType") or prop.get("propertyType") or None

        floor_area = None
        for s in prop.get("sizings", []) or []:
            disp = s.get("display")
            if disp:
                floor_area = disp
                break

    return {
        "url": str(url),
        "address": address,
        "postcode": postcode,
        "price": price,
        "bedrooms": bedrooms,
        "property_type": property_type,
        "floor_area": floor_area,
        "status": str(status) if status is not None else None,
    }

# ============================================================
# FETCH PROPERTY DETAIL WITH RETRY
# ============================================================

async def fetch_detail(session, url):
    for attempt in range(4):
        try:
            headers = {
                "User-Agent": random.choice(USER_AGENTS),
                "Accept-Language": "en-GB,en;q=0.9",
                "Referer": "https://www.rightmove.co.uk/"
            }

            async with session.get(url, headers=headers) as resp:
                status = resp.status

                if status in (429, 503):
                    await asyncio.sleep(0.4 + attempt * 0.5)
                    continue

                if status != 200:
                    if attempt == 0:
                        print(f"\nERROR {status} for {url}")
                    await asyncio.sleep(0.3 + attempt * 0.2)
                    continue

                html = await resp.text()
                model = extract_page_model_fast(html)
                if model is None:
                    with open("debug_no_model.html", "w", encoding="utf-8") as f:
                        f.write(html[:20000])
                return normalize_detail_row(url, model, status)

        except Exception as e:
            if attempt == 0:
                print(f"\nNETWORK ERROR for {url}: {e}")
            await asyncio.sleep(0.3 + attempt * 0.2)

    return normalize_detail_row(url, None, "fail")

# ============================================================
# DETAIL SCRAPING PASS
# ============================================================

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

    connector = aiohttp.TCPConnector(
        limit=80,
        limit_per_host=8,
        enable_cleanup_closed=True,
        ttl_dns_cache=300,
        ssl=False,
        force_close=False
    )
    timeout = aiohttp.ClientTimeout(total=30)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:

        i = 0
        while i < total:

            await asyncio.sleep(random.uniform(0.01, 0.08))

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

                    if row["status"] != "200" and row["status"] != 200:
                        retry_urls.append(u)
                        nonlocal errors
                        errors += 1

                    result_queue.put(row)
                    return row

            tasks = [run_one(u) for u in batch]

            for coro in asyncio.as_completed(tasks):
                await coro
                completed += 1

                frac = completed / total
                pct = int(frac * 100)

                if completed < 500:
                    eta_str = "--:--"
                else:
                    elapsed = time.time() - start_time
                    eta = (elapsed / frac) - elapsed
                    eta = min(max(eta, 0), 7200)
                    mins = int(eta // 60)
                    secs = int(eta % 60)
                    eta_str = f"{mins:02d}:{secs:02d}"

                bar_len = 30
                fill = int(bar_len * frac)
                bar = "#" * fill + "-" * (bar_len - fill)

                print(
                    f"[{label} {bar}] {pct}% ({completed:,}/{total:,}) "
                    f"| ERR={errors:,} | CONC={concurrency} | ETA={eta_str}",
                    end="\r",
                    flush=True
                )

            if latencies:
                avg = sum(latencies) / len(latencies)
            else:
                avg = 0.25

            err_rate = errors / max(1, completed)

            if completed < 500:
                concurrency = START_CONCURRENCY
            else:
                if avg < 0.18 and err_rate < 0.01:
                    concurrency = min(MAXC, concurrency + 2)
                elif avg > 0.30 or err_rate > 0.015:
                    concurrency = max(MINC, concurrency - 10)

            i += concurrency
            gc.collect()

    print(f"\n{label} done → {completed:,} rows, {errors:,} errors")
    return retry_urls

# ============================================================
# MULTI-PASS DETAIL SCRAPER WORKER
# ============================================================

async def detail_worker_async(urls, worker_id, result_queue):
    print(f"\n🧵 Worker {worker_id} starting with {len(urls):,} URLs")
    await asyncio.sleep(random.uniform(1.5, 3.5))

    retry1 = await detail_pass(urls, f"W{worker_id}-PASS1",
                               START_CONCURRENCY, MAX_CONCURRENCY, result_queue)

    retry2 = retry3 = []
    if retry1:
        retry2 = await detail_pass(retry1, f"W{worker_id}-PASS2",
                                   30, 60, result_queue)

    if retry2:
        retry3 = await detail_pass(retry2, f"W{worker_id}-PASS3",
                                   10, 20, result_queue)

    if retry3:
        fn = f"failed_urls_worker{worker_id}.txt"
        with open(fn, "w") as f:
            for u in retry3:
                f.write(u + "\n")
        print(f"⚠️ Worker {worker_id}: {len(retry3)} final failures saved to {fn}")

    print(f"🧵 Worker {worker_id} finished.")
    result_queue.put(f"DONE-{worker_id}")


def detail_worker(urls, worker_id, result_queue):
    asyncio.run(detail_worker_async(urls, worker_id, result_queue))

# ============================================================
# PART 3 — STREAMING PARQUET SHARD WRITER + MAIN ORCHESTRATION
# MODE: A (Continuous shards — no final merge)
# ============================================================

import queue
from threading import Thread
import pyarrow as pa
import pyarrow.parquet as pq


# ============================================================
# SHARD WRITER (streaming, no final combine)
# ============================================================

SHARD_BATCH = []
SHARD_SIZE = 5000   # writes a shard every 5000 rows


def cast_row_types(row):
    """Ensure consistent schema and avoid Arrow conversion errors."""
    return {
        "url": str(row.get("url") or ""),
        "address": str(row.get("address") or ""),
        "postcode": str(row.get("postcode") or ""),
        "price": int(row["price"]) if row.get("price") not in (None, "") else None,
        "bedrooms": int(row["bedrooms"]) if row.get("bedrooms") not in (None, "") else None,
        "property_type": str(row.get("property_type") or ""),
        "floor_area": str(row.get("floor_area") or ""),
        "status": str(row.get("status") or ""),
        "scraped_at": pd.Timestamp.utcnow()
    }


def write_shard(rows):
    """Write one parquet shard safely."""
    df = pd.DataFrame(rows)

    ts = int(time.time() * 1000)
    path = PARQUET_DIR / f"shard_{ts}.parquet"
    tmp = str(path) + ".tmp"

    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, tmp, compression=None)  # uncompressed for speed
    os.replace(tmp, path)

    print(f"\n📝 Wrote shard {path.name} ({len(df)} rows)")


def add_row_to_shard(row):
    """Append row & flush shard if large enough."""
    global SHARD_BATCH
    SHARD_BATCH.append(cast_row_types(row))

    if len(SHARD_BATCH) >= SHARD_SIZE:
        write_shard(SHARD_BATCH)
        SHARD_BATCH = []


def flush_remaining_shard():
    """Flush final rows when workers finish."""
    global SHARD_BATCH
    if SHARD_BATCH:
        write_shard(SHARD_BATCH)
        SHARD_BATCH = []


# ============================================================
# QUEUE CONSUMER THREAD (reads from detail workers)
# ============================================================

def writer_thread_func(result_queue, worker_count):
    """
    Listens for rows from workers.
    Writes shards on-the-fly.
    """
    done = 0

    while True:
        item = result_queue.get()

        # Worker finished
        if isinstance(item, str) and item.startswith("DONE-"):
            done += 1
            print(f"\nWriter received {item} ({done}/{worker_count})")

            if done == worker_count:
                print("Writer: All workers completed. Flushing final shard...")
                flush_remaining_shard()
                break
            continue

        # Normal row
        add_row_to_shard(item)


# ============================================================
# MAIN ORCHESTRATION (unchanged except writer)
# ============================================================

def main():
    state = load_state()
    completed_outcodes = state["completed_outcodes"]
    collected_urls = state["collected_urls"]

    outcodes = load_outcodes()
    remaining = [oc for oc in outcodes if oc not in completed_outcodes]

    print(f"➡️ Remaining OUTCODEs to scrape: {len(remaining)}")

    # --------------------------------------------------------
    # 1) URL COLLECTION
    # --------------------------------------------------------
    if remaining:
        chunks = [remaining[i::OUTCODE_WORKERS] for i in range(OUTCODE_WORKERS)]
        result_queue = Queue()
        procs = []

        for chunk in chunks:
            p = Process(target=outcode_worker, args=(chunk, result_queue))
            p.start()
            procs.append(p)

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

            print_url_progress(processed, total_needed,
                                len(collected_urls), start_time)

        for p in procs:
            p.join()

    print("\n🌍 URL COLLECTION COMPLETE")
    print(f"Total unique URLs collected: {len(collected_urls):,}")

    # --------------------------------------------------------
    # 2) DETAIL SCRAPING — streaming shards
    # --------------------------------------------------------
    print("\n🔧 Starting detail scraping with workers...")

    urls = list(collected_urls)
    url_chunks = [urls[i::DETAIL_WORKERS] for i in range(DETAIL_WORKERS)]

    result_queue = Queue()

    # Start writer thread
    writer_thread = Thread(
        target=writer_thread_func,
        args=(result_queue, DETAIL_WORKERS),
        daemon=True
    )
    writer_thread.start()

    # Spawn detail workers
    procs = []
    for wid, chunk in enumerate(url_chunks):
        p = Process(target=detail_worker, args=(chunk, wid, result_queue))
        p.start()
        procs.append(p)

    # Wait for workers
    for p in procs:
        p.join()

    writer_thread.join()

    print("\n🎉 ALL DONE — shards written inside:")
    print(PARQUET_DIR)



# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    freeze_support()
    main()
