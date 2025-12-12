# ============================================================
# RIGHTMOVE DASHBOARD — TABLE + FILTERS + MAP VIEW
# ============================================================

import streamlit as st
import pandas as pd
import time
from pathlib import Path
from geopy.geocoders import Nominatim

# ------------------------------------------------------------
# PAGE CONFIG
# ------------------------------------------------------------

st.set_page_config(
    page_title="Rightmove Dashboard",
    layout="wide",
)

# ------------------------------------------------------------
# LOAD DATA
# ------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent
PARQUET_DIR = BASE_DIR / "parquet"

parquet_files = sorted(PARQUET_DIR.glob("*.parquet"))

if not parquet_files:
    st.error("No Parquet files found in /parquet")
    st.stop()

df = pd.concat(
    (pd.read_parquet(p) for p in parquet_files),
    ignore_index=True
)

# ------------------------------------------------------------
# SIDEBAR (CLEAN + SIMPLE)
# ------------------------------------------------------------

st.sidebar.title("🔎 Filters")

price_min, price_max = st.sidebar.slider(
    "Price (£)",
    min_value=int(df["price"].min() or 0),
    max_value=int(df["price"].max() or 1_000_000),
    value=(
        int(df["price"].min() or 0),
        int(df["price"].max() or 1_000_000),
    ),
    step=5000,
)

bedrooms = st.sidebar.multiselect(
    "Bedrooms",
    sorted(df["bedrooms"].dropna().unique())
)

ptype = st.sidebar.multiselect(
    "Property Type",
    sorted(df["property_type"].dropna().unique())
)

# ------------------------------------------------------------
# APPLY FILTERS
# ------------------------------------------------------------

filtered = df.copy()

filtered = filtered[
    (filtered["price"].fillna(0) >= price_min) &
    (filtered["price"].fillna(0) <= price_max)
]

if bedrooms:
    filtered = filtered[filtered["bedrooms"].isin(bedrooms)]

if ptype:
    filtered = filtered[filtered["property_type"].isin(ptype)]

# ------------------------------------------------------------
# MAIN TABLE
# ------------------------------------------------------------

st.title("🏠 Rightmove Property Dashboard")

st.caption(f"Showing {len(filtered):,} properties")

st.dataframe(
    filtered.sort_values("price", ascending=False),
    use_container_width=True,
    height=500
)

# ============================================================
# MAP VIEW (FIXED & WORKING)
# ============================================================

st.divider()
st.header("🗺 Property Map")

# ------------------------------------------------------------
# Extract OUTCODE safely
# ------------------------------------------------------------

def extract_outcode(pc):
    if not isinstance(pc, str):
        return None
    pc = pc.strip().upper()
    return pc.split(" ")[0] if pc else None

filtered["outcode"] = filtered["postcode"].apply(extract_outcode)

# ------------------------------------------------------------
# Cached geocoding (OUTCODE → lat/lon)
# ------------------------------------------------------------

@st.cache_data(show_spinner=False)
def geocode_outcodes(outcodes):
    geolocator = Nominatim(user_agent="rightmove-dashboard")
    latlon = {}

    progress = st.progress(0)
    total = len(outcodes)

    for i, oc in enumerate(outcodes, 1):
        try:
            loc = geolocator.geocode(f"{oc}, UK", timeout=10)
            if loc:
                latlon[oc] = (loc.latitude, loc.longitude)
        except:
            pass

        if i % 5 == 0:
            progress.progress(i / total)

        time.sleep(1)  # required to avoid being blocked

    progress.empty()
    return latlon

# ------------------------------------------------------------
# Apply lat/lon
# ------------------------------------------------------------

valid_outcodes = sorted(filtered["outcode"].dropna().unique())

if not valid_outcodes:
    st.error("No valid postcodes found for mapping.")
    st.stop()

st.caption("Geocoding postcodes (cached on first run)…")

latlon_map = geocode_outcodes(valid_outcodes)

filtered["lat"] = filtered["outcode"].map(lambda x: latlon_map.get(x, (None, None))[0])
filtered["lon"] = filtered["outcode"].map(lambda x: latlon_map.get(x, (None, None))[1])

map_df = filtered.dropna(subset=["lat", "lon"])

# ------------------------------------------------------------
# Render map
# ------------------------------------------------------------

if map_df.empty:
    st.error("No properties could be mapped after geocoding.")
else:
    st.caption(f"Mapped properties: {len(map_df):,}")
    st.map(map_df[["lat", "lon"]])
