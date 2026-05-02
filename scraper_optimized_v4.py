# ============================================================
# scraper_optimized_v3_resume+cache-drain.py
# FULL HIGH-PERFORMANCE RIGHTMOVE SCRAPER (Playwright + Async)
# - Resume-safe URL collection (completed_outcodes)
# - Resume-safe detail queue (collected_urls as pending)
# - Cache drains as details succeed (collected_urls -> seen_urls)
# - Retry failed URL support
# - Fixes detail_pass() indexing bug (no URL skipping)
# ============================================================

import os
import re
import gc
import sys
import json
import time
import socket
import random
import asyncio
import aiohttp
import pandas as pd
from pathlib import Path
import threading
from threading import Thread
from multiprocessing import Process, Queue, freeze_support
from playwright.async_api import async_playwright
from playwright._impl._errors import TargetClosedError
import pyarrow as pa
import pyarrow.parquet as pq


REBUILD_FROM_ALL = "--rebuild-from-all" in sys.argv

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
SHARD_ROWS = 5000
STATE_FILE = BASE_DIR / "state.json"

# NOTE: Windows path; consider overriding via env var if needed
OUTCODE_CSV = os.environ.get(
    "OUTCODE_CSV",
    r"C:\rightmove_monitor\scraper\outercode_to_postcode_master.csv",
)

OUTCODE_WORKERS = 4      # each process spawns TABS_PER_WORKER concurrent tabs
TABS_PER_WORKER = 5      # concurrent Playwright tabs per browser process
DETAIL_WORKERS = 6

PAGE_SIZE = 24
MAX_PAGES = 50

# DETAIL CONCURRENCY
START_CONCURRENCY = 30
MAX_CONCURRENCY = 60
MIN_CONCURRENCY = 20

socket.setdefaulttimeout(20)  # Windows fix

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/122 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Safari/605.1.15",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:122.0) Firefox/122.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/121 Safari/537.36",
]

def shard_parquet_name(shard_idx: int):
    ts = time.strftime("%Y-%m-%d")
    return PARQUET_DIR / f"rightmove_{ts}_part{shard_idx:05d}.parquet"


AUCTION_RE = re.compile(r"\bauction\b", re.IGNORECASE)
HMO_RE = re.compile(r"\bH\.?\s*M\.?\s*O\.?\b", re.IGNORECASE)
TAG_RE = re.compile(r"<[^>]+>")

def html_to_text(s: str) -> str:
    if not s:
        return ""
    s = TAG_RE.sub(" ", s)
    s = s.replace("&nbsp;", " ").replace("&amp;", "&")
    return re.sub(r"\s+", " ", s).strip()

def get_description_text_from_model(model: dict) -> str:
    """
    Try several known Rightmove PAGE_MODEL shapes for description.
    Returns plain text (HTML stripped).
    """
    if not model:
        return ""

    prop = model.get("propertyData") or {}
    # Common candidates (Rightmove has varied these over time)
    candidates = [
        prop.get("text") or {},                      # sometimes holds description
        prop.get("details") or {},                   # sometimes holds description
        prop,                                        # sometimes description sits directly here
        (model.get("propertyData") or {}),          # redundant but safe
    ]

    keys_to_try = [
        "description", "propertyDescription", "fullDescription",
        "summaryDescription", "marketingDescription", "shortDescription",
        "htmlDescription", "descriptionHtml",
    ]

    # Also sometimes nested like {"text": {"description": "..."}}
    for c in candidates:
        if isinstance(c, dict):
            for k in keys_to_try:
                v = c.get(k)
                if isinstance(v, str) and v.strip():
                    return html_to_text(v)

    return ""


# ============================================================
# STATE HANDLING (resume-safe, cache-draining)
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
        "collected_urls": set(d.get("collected_urls", [])),  # pending details
        "seen_urls": set(d.get("seen_urls", [])),            # successfully scraped
    }


def save_state(state):
    tmp = str(STATE_FILE) + ".tmp"
    data = {
        "completed_outcodes": sorted(list(state["completed_outcodes"])),
        "collected_urls": sorted(list(state["collected_urls"])),
        "seen_urls": sorted(list(state["seen_urls"])),
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
    print(f"📍 Loaded {len(outcodes)} OUTCODE identifiers")
    return outcodes


# ============================================================
# URL PROGRESS
# ============================================================

def print_url_progress(current, total, count, start_time):
    frac = current / total if total else 1.0
    pct = int(frac * 100)
    bar_len = 30
    fill = int(bar_len * frac)
    bar = "#" * fill + "-" * (bar_len - fill)

    elapsed = time.time() - start_time
    if current > 20 and frac > 0:
        eta = (elapsed / frac) - elapsed
    else:
        eta = 99999

    eta = min(max(eta, 0), 7200)
    eta_str = f"{int(eta//60):02d}:{int(eta%60):02d}"

    print(
        f"[URL {bar}] {pct}% ({current}/{total}) "
        f"| PENDING URLs={count:,} | ETA={eta_str}",
        end="\r",
        flush=True
    )


# ============================================================
# PLAYWRIGHT BLOCKER
# ============================================================

async def block_assets(route, request):
    # If you get empty pages sometimes, try removing "stylesheet" from the block list
    if request.resource_type in ("image", "media", "font"):
        return await route.abort()
    await route.continue_()


async def fetch_page_urls(page, outcode_id, index):
    url = (
        "https://www.rightmove.co.uk/property-for-sale/find.html?"
        f"locationIdentifier={outcode_id}&sortType=1&index={index}"
    )
    try:
        resp = await page.goto(url, wait_until="domcontentloaded", timeout=20000)
        status = resp.status if resp else None

        if status in (429, 403, 503):
            return set(), status, "throttled"

        try:
            await page.wait_for_selector("a.propertyCard-link", timeout=8000)
        except:
            return set(), status, "no_selector"

        # Single JS round-trip instead of N individual get_attribute calls
        hrefs = await page.evaluate(
            "Array.from(document.querySelectorAll('a.propertyCard-link'))"
            ".map(a => a.href).filter(h => h.includes('/properties/'))"
        )
        urls = {h.split("#")[0] for h in hrefs}
        return urls, status, "ok"

    except TargetClosedError:
        return set(), None, "target_closed"
    except Exception:
        return set(), None, "error"



async def scrape_outcodes_with_browser(outcodes, result_queue):
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=random.choice(USER_AGENTS),
            locale="en-GB",
        )
        await context.route("**/*", block_assets)

        sem = asyncio.Semaphore(TABS_PER_WORKER)

        async def scrape_one(oc):
            async with sem:
                page = None
                try:
                    page = await context.new_page()
                    urls = set()
                    ok_outcode = False

                    for p_idx in range(MAX_PAGES):
                        page_urls, status, flag = await fetch_page_urls(page, oc, p_idx * PAGE_SIZE)

                        if flag == "target_closed":
                            break

                        if flag == "throttled":
                            await asyncio.sleep(random.uniform(4, 10))
                            page_urls, status, flag = await fetch_page_urls(page, oc, p_idx * PAGE_SIZE)

                        if p_idx == 0:
                            ok_outcode = (flag in ("ok", "no_selector") and status not in (429, 403, 503))
                            if flag in ("error", "target_closed") or status in (429, 403, 503):
                                ok_outcode = False
                                break

                        if not page_urls:
                            break

                        urls.update(page_urls)
                        if len(page_urls) < PAGE_SIZE:
                            break

                    result_queue.put({"outcode": oc, "urls": list(urls), "ok": ok_outcode})

                except Exception:
                    result_queue.put({"outcode": oc, "urls": [], "ok": False})
                finally:
                    if page:
                        try:
                            await page.close()
                        except Exception:
                            pass

        try:
            await asyncio.gather(*[scrape_one(oc) for oc in outcodes])
        finally:
            try:
                await context.close()
            except Exception:
                pass
            try:
                await browser.close()
            except Exception:
                pass
            result_queue.put("DONE")



def outcode_worker(outcodes, result_queue):
    try:
        asyncio.run(scrape_outcodes_with_browser(outcodes, result_queue))
    except Exception as e:
        # crash-proof: never let parent hang waiting for DONE
        try:
            result_queue.put("DONE")
        except:
            pass
        raise


# ============================================================
# DETAIL SCRAPER
# ============================================================

PAGE_MODEL_RE = re.compile(
    r"window\.PAGE_MODEL\s*=\s*({.*?})\s*;</script>",
    re.DOTALL
)


def extract_page_model(html: str):
    """
    Robustly extract window.PAGE_MODEL = {...}; using brace matching
    instead of regex (Rightmove frequently changes formatting).
    """
    key = "window.PAGE_MODEL"
    pos = html.find(key)
    if pos == -1:
        return None

    # Find the '=' after window.PAGE_MODEL
    eq = html.find("=", pos)
    if eq == -1:
        return None

    # Find the first '{' after '='
    start = html.find("{", eq)
    if start == -1:
        return None

    # Brace-match the JSON object
    depth = 0
    in_str = False
    esc = False

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

        # not in string
        if ch == '"':
            in_str = True
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                json_text = html[start:i+1]
                try:
                    return json.loads(json_text)
                except Exception:
                    return None

    return None



def normalize_row(url, model, status, description_text=""):
    desc = description_text or ""
    potential_auction = bool(AUCTION_RE.search(desc))
    potential_hmo = bool(HMO_RE.search(desc))

    if not model:
        return {
            "url": url,
            "address": None,
            "postcode": None,
            "price": None,
            "bedrooms": None,
            "property_type": None,
            "floor_area": None,
            "latitude": None,
            "longitude": None,
            "status": int(status) if str(status).isdigit() else None,
            "potential_auction": potential_auction,
            "potential_hmo": potential_hmo,
        }

    prop = model.get("propertyData", {}) or {}
    analytics = (model.get("analyticsInfo", {}) or {}).get("analyticsProperty", {}) or {}
    addr = prop.get("address", {}) or {}

    outcode = addr.get("outcode", "") or ""
    incode = addr.get("incode", "") or ""

    fa = None
    for s in prop.get("sizings", []) or []:
        disp = s.get("display")
        if disp:
            fa = disp
            break

    lat = None
    lon = None

    loc = prop.get("location") or {}
    lat = loc.get("latitude") or loc.get("lat") or lat
    lon = loc.get("longitude") or loc.get("lng") or lon

    if lat is None or lon is None:
        mv = model.get("mapView") or model.get("map") or {}
        lat = mv.get("latitude") or mv.get("lat") or lat
        lon = mv.get("longitude") or mv.get("lng") or lon

    if lat is None or lon is None:
        lat = analytics.get("latitude") or analytics.get("lat") or lat
        lon = analytics.get("longitude") or analytics.get("lng") or lon

    return {
        "url": url,
        "address": addr.get("displayAddress"),
        "postcode": analytics.get("postcode") or f"{outcode} {incode}".strip(),
        "price": analytics.get("price"),
        "bedrooms": prop.get("bedrooms"),
        "property_type": prop.get("propertySubType") or prop.get("propertyType"),
        "floor_area": fa,
        "latitude": lat,
        "longitude": lon,
        "status": int(status) if str(status).isdigit() else None,
        "potential_auction": potential_auction,
        "potential_hmo": potential_hmo,
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
                    if status in (429, 403, 503):
                        await asyncio.sleep(2.0 + attempt * 2.5 + random.uniform(0.0, 1.0))
                    else:
                        await asyncio.sleep(0.25 + attempt * 0.3)
                    continue

                html = await resp.text()
                model = extract_page_model(html)

                # ✅ description-only detection
                description_text = get_description_text_from_model(model)

                # Fallback if PAGE_MODEL missing description:
                # try a very light HTML-based extraction (only common description containers)
                if not description_text:
                    # try to capture the description section if present in markup
                    m = re.search(r'property-description[^>]*>(.*?)</', html, re.IGNORECASE | re.DOTALL)
                    if m:
                        description_text = html_to_text(m.group(1))

                if model is None and attempt == 0:
                    print(f"\n⚠️ PAGE_MODEL not found for {url} (html len={len(html):,})")

                return normalize_row(url, model, status, description_text=description_text)

        except Exception as e:
            if attempt == 0:
                print(f"\nNETWORK ERROR for {url}: {e}")
            await asyncio.sleep(0.3 + attempt * 0.4)

    return normalize_row(url, None, "fail", description_text="")




async def detail_pass(urls, label, start_conc, max_conc, result_queue):
    urls = list(set(urls))
    total = len(urls)
    if not total:
        return []

    print(f"\n🔎 Starting {label} — {total:,} URLs")

    completed = 0
    errors = 0
    retry_urls = []
    start_time = time.time()

    # Semaphore sliding window: no batch stalls — fast URLs never wait on slow ones
    sem = asyncio.Semaphore(start_conc)
    connector = aiohttp.TCPConnector(limit=300, limit_per_host=25, ssl=False)
    timeout = aiohttp.ClientTimeout(total=25)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:

        async def run_one(u):
            nonlocal completed, errors
            async with sem:
                row = await fetch_detail(session, u)

                if row["status"] != 200:
                    retry_urls.append(u)
                    errors += 1

                result_queue.put(row)
                completed += 1

                frac = completed / total
                pct = int(frac * 100)
                fill = int(30 * frac)
                bar = "#" * fill + "-" * (30 - fill)

                if completed < 100:
                    eta_str = "--:--"
                else:
                    elapsed = time.time() - start_time
                    eta = (elapsed / max(frac, 1e-9)) - elapsed
                    eta = min(max(eta, 0), 7200)
                    eta_str = f"{int(eta//60):02d}:{int(eta%60):02d}"

                print(
                    f"[{label} {bar}] {pct}% ({completed:,}/{total:,}) "
                    f"| ERR={errors:,} | CONC={start_conc} | ETA={eta_str}",
                    end="\r",
                    flush=True,
                )

        await asyncio.gather(*[run_one(u) for u in urls])

    print(f"\n{label} done → {completed:,} rows, {errors:,} errors")
    return retry_urls


async def detail_worker_async(urls, worker_id, result_queue):
    print(f"\n🧵 Worker {worker_id} starting with {len(urls):,} URLs")

    retry1 = await detail_pass(
        urls, f"W{worker_id}-PASS1",
        start_conc=START_CONCURRENCY, max_conc=MAX_CONCURRENCY,
        result_queue=result_queue
    )

    retry2 = []
    if retry1:
        retry2 = await detail_pass(
            retry1, f"W{worker_id}-PASS2",
            start_conc=30, max_conc=60,
            result_queue=result_queue
        )

    retry3 = []
    if retry2:
        retry3 = await detail_pass(
            retry2, f"W{worker_id}-PASS3",
            start_conc=10, max_conc=20,
            result_queue=result_queue
        )

    if retry3:
        fn = f"failed_urls_worker{worker_id}.txt"
        with open(BASE_DIR / fn, "w", encoding="utf-8") as f:
            for u in retry3:
                f.write(u + "\n")
        print(f"⚠️ Worker {worker_id}: {len(retry3):,} final failures → {fn}")

    result_queue.put(f"DONE-{worker_id}")


def detail_worker(urls, worker_id, q):
    asyncio.run(detail_worker_async(urls, worker_id, q))


# ============================================================
# PARQUET WRITER
# ============================================================

def final_parquet_name():
    ts = time.strftime("%Y-%m-%d")
    return PARQUET_DIR / f"rightmove_{ts}.parquet"


def consolidate_shards(shard_dir, output_path):
    shard_files = sorted(shard_dir.glob("*.parquet"))
    if not shard_files:
        print("No shards to consolidate.")
        return

    print(f"\n📎 Consolidating {len(shard_files)} shards → {output_path}")

    # Unify schemas first — shards can drift (e.g. status: int64 vs double)
    # when a shard contains only null values in a numeric column
    schemas = [pq.read_schema(f) for f in shard_files]
    unified = pa.unify_schemas(schemas, promote_options="default")

    writer = None
    total_rows = 0
    try:
        for f in shard_files:
            table = pq.read_table(f).cast(unified)
            if writer is None:
                writer = pq.ParquetWriter(output_path, unified, compression="snappy")
            writer.write_table(table)
            total_rows += len(table)
    finally:
        if writer:
            writer.close()

    print(f"✅ {total_rows:,} rows written to {output_path}")


class ParquetShardWriter:
    def __init__(self, shard_dir, shard_size=5000):
        self.shard_dir = shard_dir
        self.shard_size = shard_size
        self.rows = []
        self.shard_index = 0
        self.lock = threading.Lock()

    def add_row(self, row):   # ✅ ONLY row
        with self.lock:
            self.rows.append(row)
            if len(self.rows) >= self.shard_size:
                self._flush()

    def _flush(self):
        if not self.rows:
            return

        df = pd.DataFrame(self.rows)
        table = pa.Table.from_pandas(df, preserve_index=False)

        out = self.shard_dir / f"rightmove_part_{self.shard_index:05d}.parquet"
        pq.write_table(table, out, compression=None)

        print(f"\n📦 Wrote shard → {out} ({len(df):,} rows)")

        self.rows.clear()
        self.shard_index += 1

    def close(self):
        self._flush()




def writer_thread_func(in_queue, writer, expected_done, state):
    """
    Receives detail rows and writes parquet shards continuously.
    Also drains cache: when a URL is successfully scraped (status 200),
    move it from collected_urls -> seen_urls and persist periodically.
    """
    done = 0
    processed = 0

    while True:
        item = in_queue.get()

        if isinstance(item, str) and item.startswith("DONE-"):
            done += 1
            if done == expected_done:
                break
            continue

        writer.add_row(item)

        url = item.get("url")
        if url and item.get("status") == 200:
            state["seen_urls"].add(url)
            state["collected_urls"].discard(url)

        processed += 1
        if processed % 2000 == 0:
            save_state(state)

    # final flush + state write
    save_state(state)
    writer.close()


# ============================================================
# FAILED URL RETRY SUPPORT
# ============================================================

def load_failed_urls():
    failed = set()
    for fn in os.listdir(BASE_DIR):
        if fn.startswith("failed_urls_worker") and fn.endswith(".txt"):
            with open(BASE_DIR / fn, "r", encoding="utf-8") as f:
                for url in f:
                    url = url.strip()
                    if url:
                        failed.add(url)
    return list(failed)


def run_retry_failed():
    state = load_state()
    failed = load_failed_urls()
    if not failed:
        print("✔ No failed URLs found.")
        return

    # Filter out already-seen
    failed = [u for u in failed if u not in state["seen_urls"]]
    if not failed:
        print("✔ All failed URLs are already marked seen.")
        return

    print(f"🔁 Retrying {len(failed):,} failed URLs...")

    result_queue = Queue()
    writer = ParquetShardWriter()

    thread = Thread(
        target=writer_thread_func,
        args=(result_queue, writer, 1, state),
        daemon=True
    )
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

    state = load_state()
    if REBUILD_FROM_ALL:
        print("🔁 Rebuilding collected_urls from collected_urls ∪ seen_urls")

        all_urls = state["collected_urls"] | state["seen_urls"]
        state["collected_urls"] = set(all_urls)
        state["seen_urls"].clear()

        save_state(state)
    completed = state["completed_outcodes"]
    collected_urls = state["collected_urls"]
    seen_urls = state["seen_urls"]

    outcodes = load_outcodes()
    remaining = [oc for oc in outcodes if oc not in completed]

    print(f"➡️ Outcodes remaining: {len(remaining)}")
    print(f"🧠 Seen URLs: {len(seen_urls):,}")
    print(f"📥 Pending URLs (from previous runs): {len(collected_urls):,}")

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
        start = time.time()


        finished = set()  # worker indices finished (DONE received OR process exited)

        while len(finished) < len(procs):
            # Mark exited processes as finished
            for idx, p in enumerate(procs):
                if idx in finished:
                    continue
                if not p.is_alive() and p.exitcode is not None:
                    finished.add(idx)
                    if p.exitcode != 0:
                        print(f"\n⚠️ Outcode worker {idx} exited with code {p.exitcode} (marking finished)")
                    else:
                        print(f"\nℹ️ Outcode worker {idx} exited cleanly (marking finished)")

            # If all finished, stop waiting on queue
            if len(finished) == len(procs):
                break

            # Try read queue
            try:
                msg = q.get(timeout=2.0)
            except Exception:
                continue

            if msg == "DONE":
                # We don't know which worker sent it; just ignore (process-exit tracking is authoritative)
                continue

            oc = msg["outcode"]
            urls = msg["urls"]
            ok = msg.get("ok", False)

            if ok:
                new_urls = [u for u in urls if (u not in seen_urls and u not in collected_urls)]
                collected_urls.update(new_urls)
                completed.add(oc)

            count += 1
            if count % 25 == 0:
                save_state(state)
            print_url_progress(count, len(remaining), len(collected_urls), start)

        for p in procs:
            p.join()
        save_state(state)

    print("\n\n🌍 URL COLLECTION COMPLETE")
    print(f"Pending URLs: {len(collected_urls):,}")

    # -----------------------------
    # DETAIL SCRAPING
    # -----------------------------
    pending = list(collected_urls - seen_urls)
    if not pending:
        print("\n✅ No pending URLs to scrape.")
        return

    print("\n🔧 Starting detail scraping...")
    urls = pending
    chunks = [urls[i::DETAIL_WORKERS] for i in range(DETAIL_WORKERS)]

    q = Queue()
    shard_dir = PARQUET_DIR / "shards" / time.strftime("%Y-%m-%d")
    shard_dir.mkdir(parents=True, exist_ok=True)
    writer = ParquetShardWriter(
    shard_dir=shard_dir,
    shard_size=SHARD_ROWS
)
    writer_thread = Thread(
        target=writer_thread_func,
        args=(q, writer, DETAIL_WORKERS, state),
        daemon=True
    )
    writer_thread.start()

    procs = []
    for i, chunk in enumerate(chunks):
        p = Process(target=detail_worker, args=(chunk, i, q))
        p.start()
        procs.append(p)

    for p in procs:
        p.join()

    writer_thread.join()

    consolidate_shards(shard_dir, final_parquet_name())

    print("\n🎉 ALL DONE!")
    print(f"Data stored: {PARQUET_DIR}")
    print(f"Seen URLs: {len(state['seen_urls']):,}")
    print(f"Pending URLs: {len(state['collected_urls']):,}")



# ============================================================
# ENTRY
# ============================================================

if __name__ == "__main__":
    freeze_support()
    main()
