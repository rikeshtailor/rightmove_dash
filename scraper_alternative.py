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

RETRY_FAILED     = "--retry-failed"     in sys.argv
REBUILD_FROM_ALL = "--rebuild-from-all" in sys.argv

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

    # Numeric columns drift between int64 and float64 when a shard contains
    # only nulls (pandas stores NaN as float). Force them to a fixed type.
    FORCE_TYPES = {"status": pa.int64(), "price": pa.int64(), "bedrooms": pa.int64()}

    def normalise(schema):
        return pa.schema([
            pa.field(f.name, FORCE_TYPES[f.name], nullable=True)
            if f.name in FORCE_TYPES else f
            for f in schema
        ])

    schemas = [normalise(pq.read_schema(f)) for f in shard_files]
    unified = pa.unify_schemas(schemas, promote_options="default")
    writer  = None
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
    print(f"{total_rows:,} rows written to {output_path}")


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

    # Only clean state + push if every URL was scraped (no pending left)
    still_pending = len(state["collected_urls"] - state["seen_urls"])
    if still_pending == 0:
        print("\nAll URLs scraped — clearing state and pushing to GitHub.")
        clear_state()
        push_to_github(output)
    else:
        print(f"\n{still_pending:,} URLs still pending — state kept for resume.")

    print("\nALL DONE!")
    print(f"Output : {output}")
    print(f"Seen   : {len(state['seen_urls']):,}")
    print(f"Pending: {still_pending:,}")


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
    if RETRY_FAILED:
        asyncio.run(run_retry_failed())
    else:
        asyncio.run(run())
