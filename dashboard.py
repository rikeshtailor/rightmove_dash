# ============================================================
# RIGHTMOVE DASHBOARD
# Sidebar filters + Instant search + Saved presets + SQL
# ============================================================

import json
from pathlib import Path
import pandas as pd
import streamlit as st
import duckdb

# ============================================================
# CONFIG
# ============================================================

PARQUET_DIR = Path("parquet")
SAVED_FILTERS_FILE = Path("saved_filters.json")

st.set_page_config(
    page_title="Rightmove Dashboard",
    layout="wide",
)

# ============================================================
# DATA LOADING
# ============================================================

@st.cache_data(show_spinner=True)
def load_data():
    files = sorted(PARQUET_DIR.glob("*.parquet"))
    if not files:
        return pd.DataFrame()

    dfs = [pd.read_parquet(f) for f in files]
    df = pd.concat(dfs, ignore_index=True)

    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["bedrooms"] = pd.to_numeric(df["bedrooms"], errors="coerce")
    df["scraped_at"] = pd.to_datetime(df["scraped_at"], errors="coerce")

    return df


df = load_data()

st.title("🏡 Rightmove Property Dashboard")

if df.empty:
    st.warning("No Parquet data found in ./parquet/")
    st.stop()

# ============================================================
# DUCKDB SETUP
# ============================================================

con = duckdb.connect(database=":memory:")
con.register("properties", df)

# ============================================================
# SAVED FILTERS
# ============================================================

def load_saved_filters():
    if SAVED_FILTERS_FILE.exists():
        with open(SAVED_FILTERS_FILE, "r") as f:
            return json.load(f)
    return {}


def save_saved_filters(filters):
    with open(SAVED_FILTERS_FILE, "w") as f:
        json.dump(filters, f, indent=2)


saved_filters = load_saved_filters()

# ============================================================
# SIDEBAR — FILTERS
# ============================================================

st.sidebar.header("🔎 Filters")

search_text = st.sidebar.text_input(
    "Instant search (address / postcode / URL)",
    placeholder="e.g. SW1A, London, terrace"
)

price_min, price_max = st.sidebar.slider(
    "Price (£)",
    int(df["price"].min(skipna=True)),
    int(df["price"].max(skipna=True)),
    (
        int(df["price"].min(skipna=True)),
        int(df["price"].max(skipna=True)),
    )
)

bedroom_options = sorted(df["bedrooms"].dropna().unique())
bedrooms = st.sidebar.multiselect("Bedrooms", bedroom_options)

property_type_options = sorted(df["property_type"].dropna().unique())
property_types = st.sidebar.multiselect("Property type", property_type_options)

# ============================================================
# SAVED PRESETS
# ============================================================

st.sidebar.markdown("---")
st.sidebar.subheader("💾 Saved filters")

preset_names = ["—"] + list(saved_filters.keys())
selected_preset = st.sidebar.selectbox("Load preset", preset_names)

if selected_preset != "—":
    preset = saved_filters[selected_preset]
    search_text = preset["search_text"]
    price_min, price_max = preset["price"]
    bedrooms = preset["bedrooms"]
    property_types = preset["property_types"]

preset_name = st.sidebar.text_input("Save current filters as")

if st.sidebar.button("💾 Save filters") and preset_name:
    saved_filters[preset_name] = {
        "search_text": search_text,
        "price": [price_min, price_max],
        "bedrooms": bedrooms,
        "property_types": property_types,
    }
    save_saved_filters(saved_filters)
    st.sidebar.success(f"Saved '{preset_name}'")

# ============================================================
# APPLY SIDEBAR FILTERS (BASE VIEW)
# ============================================================

filtered = df.copy()

filtered = filtered[
    (filtered["price"] >= price_min) &
    (filtered["price"] <= price_max)
]

if bedrooms:
    filtered = filtered[filtered["bedrooms"].isin(bedrooms)]

if property_types:
    filtered = filtered[filtered["property_type"].isin(property_types)]

if search_text:
    q = search_text.lower()
    filtered = filtered[
        filtered["address"].str.lower().str.contains(q, na=False) |
        filtered["postcode"].str.lower().str.contains(q, na=False) |
        filtered["url"].str.lower().str.contains(q, na=False)
    ]

# ============================================================
# SQL QUERY PANEL
# ============================================================

st.markdown("---")
st.subheader("🧾 SQL Query")

sql_query = st.text_area(
    "Query the data using SQL (table name: properties)",
    height=160,
    value="""SELECT
    postcode,
    COUNT(*) AS listings,
    AVG(price) AS avg_price
FROM properties
WHERE price IS NOT NULL
GROUP BY postcode
ORDER BY avg_price DESC
LIMIT 20;
"""
)

run_sql = st.button("▶ Run SQL query")

sql_result = None
sql_error = None

if run_sql:
    try:
        sql_result = con.execute(sql_query).df()
    except Exception as e:
        sql_error = str(e)

# ============================================================
# DISPLAY RESULTS
# ============================================================

if sql_error:
    st.error(sql_error)

elif sql_result is not None:
    st.subheader(f"📊 SQL Results ({len(sql_result):,} rows)")
    st.dataframe(sql_result, use_container_width=True, height=700)

else:
    st.subheader(f"📊 Results ({len(filtered):,} properties)")
    st.dataframe(
        filtered.sort_values("price", ascending=True),
        use_container_width=True,
        height=700,
    )

# ============================================================
# FOOTER METRICS
# ============================================================

st.markdown("---")

col1, col2, col3, col4 = st.columns(4)

col1.metric("Total properties", f"{len(df):,}")
col2.metric("Filtered", f"{len(filtered):,}")
col3.metric("Min price", f"£{int(filtered['price'].min()):,}" if not filtered.empty else "—")
col4.metric("Max price", f"£{int(filtered['price'].max()):,}" if not filtered.empty else "—")