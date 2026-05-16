#!/usr/bin/env python3
"""
Build data/nomis_outcode_stats.csv from NOMIS Census 2021 data.
Run once; output is committed to the repo and read by the main scraper.

Datasets used (all Census 2021, England & Wales, MSOA level):
  NM_2072_1  TS054 - Tenure of households
  NM_2085_1  TS068 - Schoolchildren and full-time students
  NM_2083_1  TS066 - Economic activity status
"""

import json
import time
import urllib.request
import urllib.parse
import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).parent
OUTPUT   = BASE_DIR / "data" / "nomis_outcode_stats.csv"
NOMIS    = "https://www.nomisweb.co.uk/api/v01/dataset"

# (dataset_id, total_code, target_code, output_col)
DATASETS = [
    ("NM_2072_1", 0,  1004, "pct_private_rented"),  # TS054: private rented
    ("NM_2085_1", 0,  1,    "pct_students"),          # TS068: full-time students
    ("NM_2083_1", 0,  1001, "pct_employed"),          # TS066: economically active
]


def nomis_get(dataset, params):
    url = f"{NOMIS}/{dataset}.data.json?" + urllib.parse.urlencode(params)
    with urllib.request.urlopen(url, timeout=120) as r:
        data = json.loads(r.read())
    if "error" in data:
        raise RuntimeError(f"NOMIS error: {data['error']}")
    return data.get("obs", [])


def postcodes_batch(postcodes):
    body = json.dumps({"postcodes": postcodes}).encode()
    req  = urllib.request.Request(
        "https://api.postcodes.io/postcodes",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        data = json.loads(r.read())
    return [item["result"] for item in data.get("result", []) if item.get("result")]


# ── Step 1: Load outcodes and representative postcodes ────────────────────────
print("Loading Rightmove postcodes...")
rm_files = sorted(BASE_DIR.glob("data/rightmove_*.parquet"),
                  key=lambda p: p.stat().st_mtime, reverse=True)
if not rm_files:
    raise FileNotFoundError("No rightmove parquet found in data/")

df_rm = pd.read_parquet(rm_files[0], columns=["postcode"])
df_rm["outcode"] = df_rm["postcode"].fillna("").str.split().str[0].str.upper()
df_rm = df_rm[df_rm["outcode"].notna() & (df_rm["outcode"] != "")]
df_rm["postcode"] = df_rm["postcode"].str.strip()

# One representative postcode per outcode (longest sample for reliability)
outcode_pc = (
    df_rm.groupby("outcode")["postcode"]
    .apply(lambda s: s.dropna().iloc[0] if len(s.dropna()) > 0 else None)
    .dropna()
)
pc_to_outcode = {pc: oc for oc, pc in outcode_pc.items()}
all_postcodes  = list(outcode_pc.values)
print(f"  {len(all_postcodes):,} unique outcodes to map")


# ── Step 2: Map postcodes → MSOA via postcodes.io batch API ──────────────────
print("Querying postcodes.io for MSOA codes...")
outcode_msoa: dict[str, str] = {}
for i in range(0, len(all_postcodes), 100):
    batch = [p for p in all_postcodes[i:i+100] if p and str(p).strip()]
    if not batch:
        continue
    try:
        results = postcodes_batch(batch)
        for result in results:
            pc   = result.get("postcode", "").strip()
            msoa = result.get("codes", {}).get("msoa", "")
            oc   = pc_to_outcode.get(pc)
            if oc and msoa:
                outcode_msoa[oc] = msoa
    except Exception as e:
        print(f"  Warning: batch {i//100} failed — {e}")
    if i % 1000 == 0:
        print(f"  {i:,}/{len(all_postcodes):,} processed, {len(outcode_msoa):,} mapped")
    time.sleep(0.05)

print(f"  Mapped {len(outcode_msoa):,} outcodes to MSOAs")


# ── Step 3: Download NOMIS Census 2021 data for all England+Wales MSOAs ──────
print("Downloading NOMIS Census 2021 data...")
msoa_stats: dict[str, dict] = {}

for dataset_id, total_code, target_code, col in DATASETS:
    print(f"  {col} ({dataset_id})...")

    # Discover the category dimension name from a sample observation
    sample = nomis_get(dataset_id, {"date": "latest", "geography": "637536440", "measures": "20100"})
    dim_key = next(k for k in sample[0] if k.startswith("c2021"))

    # Download all England+Wales MSOAs for total and target category
    obs = nomis_get(dataset_id, {
        "date":        "latest",
        "geography":   "TYPE152",
        "measures":    "20100",
        dim_key:       f"{total_code},{target_code}",
        "recordlimit": "30000",
    })
    print(f"    {len(obs):,} observations")

    for o in obs:
        msoa_code = o["geography"]["geogcode"]
        cat_val   = o[dim_key]["value"]
        obs_val   = o["obs_value"]["value"]
        if msoa_code not in msoa_stats:
            msoa_stats[msoa_code] = {}
        if cat_val == total_code:
            msoa_stats[msoa_code][f"{col}_total"]  = obs_val
        elif cat_val == target_code:
            msoa_stats[msoa_code][f"{col}_target"] = obs_val

    time.sleep(0.5)

# Compute percentages
for msoa, vals in msoa_stats.items():
    for _, _, _, col in DATASETS:
        total  = vals.get(f"{col}_total",  0)
        target = vals.get(f"{col}_target", 0)
        vals[col] = round(target / total * 100, 1) if total > 0 else None


# ── Step 4: Aggregate to outcode level ───────────────────────────────────────
print("Aggregating to outcode level...")
rows = []
for outcode, msoa in outcode_msoa.items():
    vals = msoa_stats.get(msoa, {})
    rows.append({
        "outcode":            outcode,
        "pct_private_rented": vals.get("pct_private_rented"),
        "pct_students":       vals.get("pct_students"),
        "pct_employed":       vals.get("pct_employed"),
    })

result = pd.DataFrame(rows).dropna(
    subset=["pct_private_rented", "pct_students", "pct_employed"], how="all"
)
result.to_csv(OUTPUT, index=False)
print(f"\nSaved {len(result):,} outcode rows to {OUTPUT.name}")
print(result[["pct_private_rented", "pct_students", "pct_employed"]].describe().round(1).to_string())
