# ============================================================
# RIGHTMOVE + SPAREROOM DASHBOARD
# Unified listings viewer with filters + SQL
# ============================================================

import streamlit as st
import pandas as pd
import duckdb
import re
from pathlib import Path

# ============================================================
# PAGE CONFIG
# ============================================================

st.set_page_config(
    page_title="Property Listings Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("🏠 Property Listings Dashboard")
st.caption("Rightmove + SpareRoom | Fast Parquet backend")

# ============================================================
# PATHS
# ============================================================

RIGHTMOVE_PARQUET = Path("../scraper/parquet")
SPAREROOM_PARQUET = Path("../spareroom/parquet")

# ============================================================
# DATA LOADING
# ============================================================

@st.cache_data(show_spinner=True)
def load_all_data():
    dfs = []

    if RIGHTMOVE_PARQUET.exists():
        try:
            rm = pd.read_parquet(RIGHTMOVE_PARQUET)
            rm["source"] = "rightmove"
            dfs.append(rm)
        except Exception as e:
            st.warning(f"Failed to load Rightmove data: {e}")

    if SPAREROOM_PARQUET.exists():
        try:
            sr = pd.read_parquet(SPAREROOM_PARQUET)
            sr["source"] = "spareroom"
            dfs.append(sr)
        except Exception as e:
            st.warning(f"Failed to load SpareRoom data: {e}")

    if not dfs:
        return pd.DataFrame()

    df = pd.concat(dfs, ignore_index=True)

    # Normalize text columns
    for col in ["postcode", "title", "address", "location"]:
        if col in df.columns:
            df[col] = df[col].astype(str)

    # --------------------------------------------------------
    # Normalised price column
    # --------------------------------------------------------

    def extract_price(x):
        if pd.isna(x):
            return None
        m = re.search(r"£([\d,]+)", str(x))
        return int(m.group(1).replace(",", "")) if m else None

    if "price" in df.columns:
        df["price_norm"] = df["price"]
    else:
        df["price_norm"] = None

    if "price_text" in df.columns:
        df.loc[df["price_norm"].isna(), "price_norm"] = (
            df["price_text"].apply(extract_price)
        )

    df["price_norm"] = pd.to_numeric(df["price_norm"], errors="coerce")

    return df


df = load_all_data()

if df.empty:
    st.error("No data found. Make sure parquet files exist.")
    st.stop()

# ============================================================
# SIDEBAR
# ============================================================

st.sidebar.header("Filters")

# Source filter
sources = sorted(df["source"].dropna().unique())
source_filter = st.sidebar.multiselect(
    "Source",
    options=sources,
    default=sources,
)

df = df[df["source"].isin(source_filter)]

# Postcode prefix filter
postcode_prefix = st.sidebar.text_input(
    "Postcode starts with",
    placeholder="e.g. SW, E1, M",
).upper()

if postcode_prefix:
    df = df[df["postcode"].str.startswith(postcode_prefix, na=False)]

# Price filter
min_price, max_price = st.sidebar.slider(
    "Price range (£)",
    min_value=int(df["price_norm"].min() or 0),
    max_value=int(df["price_norm"].max() or 5000),
    value=(
        int(df["price_norm"].min() or 0),
        int(df["price_norm"].max() or 5000),
    ),
)

df = df[
    (df["price_norm"].isna()) |
    ((df["price_norm"] >= min_price) & (df["price_norm"] <= max_price))
]

st.sidebar.markdown("---")

# ============================================================
# INSTANT TEXT SEARCH
# ============================================================

search_text = st.text_input(
    "🔍 Instant text search",
    placeholder="Address, title, postcode, description…",
)

if search_text:
    pattern = re.escape(search_text.lower())
    df = df[
        df.astype(str)
        .apply(lambda row: row.str.lower().str.contains(pattern, na=False))
        .any(axis=1)
    ]

# ============================================================
# SQL QUERYING
# ============================================================

st.markdown("### 🧠 SQL Query")

default_sql = """SELECT *
FROM listings
ORDER BY scraped_at DESC
LIMIT 500
"""

sql_query = st.text_area(
    "DuckDB SQL (table name: listings)",
    value=default_sql,
    height=140,
)

run_sql = st.button("Run SQL")

if run_sql:
    try:
        con = duckdb.connect()
        con.register("listings", df)
        df_sql = con.execute(sql_query).df()
        st.success(f"Query returned {len(df_sql):,} rows")
        df = df_sql
    except Exception as e:
        st.error(f"SQL error: {e}")

# ============================================================
# RESULTS TABLE
# ============================================================

st.markdown("### 📊 Results")

st.caption(f"{len(df):,} listings")

display_cols = [
    col for col in [
        "source",
        "postcode",
        "price_norm",
        "title",
        "address",
        "location",
        "url",
        "scraped_at",
    ]
    if col in df.columns
]

st.dataframe(
    df[display_cols],
    use_container_width=True,
    height=700,
)

# ============================================================
# FOOTER
# ============================================================

st.markdown("---")
st.caption("Parquet-backed | Cached | Local-first | No external APIs")
