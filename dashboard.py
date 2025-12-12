# ============================================================
# PROPERTY DASHBOARD — RIGHTMOVE + SPAREROOM
# Sidebar filters + Tabs + SQL
# ============================================================

import streamlit as st
import pandas as pd
import duckdb
import re
from pathlib import Path

# ============================================================
# PAGE CONFIG
# ============================================================


BASE_DIR = Path(__file__).resolve().parent

RIGHTMOVE_PARQUET = BASE_DIR / "scraper" / "parquet"
SPAREROOM_PARQUET = BASE_DIR / "spareroom" / "parquet"

# --- TEMP DEBUG (can remove later) ---
st.write("DEBUG — file:", __file__)
st.write("DEBUG — base dir:", BASE_DIR)
st.write("DEBUG — rightmove path:", RIGHTMOVE_PARQUET)
st.write("DEBUG — rightmove exists:", RIGHTMOVE_PARQUET.exists())
st.write("DEBUG — rightmove files:", list(RIGHTMOVE_PARQUET.glob("*.parquet")))
st.write("DEBUG — spareroom path:", SPAREROOM_PARQUET)
st.write("DEBUG — spareroom exists:", SPAREROOM_PARQUET.exists())
st.write("DEBUG — spareroom files:", list(SPAREROOM_PARQUET.glob("*.parquet")))

st.set_page_config(
    page_title="Property Listings Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("🏠 Property Listings Dashboard")
st.caption("Rightmove & SpareRoom | Parquet-backed | SQL-enabled")

# ============================================================
# PATHS
# ============================================================

RIGHTMOVE_PARQUET = Path("../scraper/parquet")
SPAREROOM_PARQUET = Path("../spareroom/parquet")

# ============================================================
# HELPERS
# ============================================================

def load_parquet_dir(path: Path) -> pd.DataFrame:
    files = sorted(path.glob("*.parquet"))
    if not files:
        return pd.DataFrame()
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)


def has_link_column():
    return hasattr(st.column_config, "LinkColumn")


# ============================================================
# DATA LOADERS
# ============================================================

@st.cache_data(show_spinner=True)
def load_rightmove():
    df = load_parquet_dir(RIGHTMOVE_PARQUET)
    if df.empty:
        return df

    df["price"] = pd.to_numeric(df.get("price"), errors="coerce")
    df["bedrooms"] = pd.to_numeric(df.get("bedrooms"), errors="coerce")
    df["scraped_at"] = pd.to_datetime(df.get("scraped_at"), errors="coerce")

    for col in ["postcode", "address", "property_type", "url"]:
        if col in df.columns:
            df[col] = df[col].astype("string")

    return df


@st.cache_data(show_spinner=True)
def load_spareroom():
    df = load_parquet_dir(SPAREROOM_PARQUET)
    if df.empty:
        return df

    df = df.rename(columns={"locatiom": "location"})

    def extract_price(x):
        if pd.isna(x):
            return None
        m = re.search(r"£([\d,]+)", str(x))
        return int(m.group(1).replace(",", "")) if m else None

    df["price_norm"] = df.get("price_text").apply(extract_price)
    df["scraped_at"] = pd.to_datetime(df.get("scraped_at"), errors="coerce")

    for col in ["postcode", "title", "location", "room_type", "available", "url"]:
        if col in df.columns:
            df[col] = df[col].astype("string")

    return df


rightmove_df = load_rightmove()
spareroom_df = load_spareroom()

if rightmove_df.empty and spareroom_df.empty:
    st.error("No data found. Check parquet paths.")
    st.stop()

# ============================================================
# DUCKDB
# ============================================================

con = duckdb.connect(database=":memory:")

if not rightmove_df.empty:
    con.register("rightmove", rightmove_df)

if not spareroom_df.empty:
    con.register("spareroom", spareroom_df)

# ============================================================
# TABS
# ============================================================

tab_rm, tab_sr = st.tabs(["🏡 Rightmove", "🛏 SpareRoom"])

# ============================================================
# RIGHTMOVE TAB
# ============================================================

with tab_rm:
    st.subheader("🏡 Rightmove Listings")

    if rightmove_df.empty:
        st.info("No Rightmove data.")
    else:
        df = rightmove_df.copy()

        # =========================
        # SIDEBAR FILTERS
        # =========================
        with st.sidebar:
            st.header("Rightmove Filters")

            pc = st.text_input("Postcode starts with", key="rm_pc").upper()

            property_types = st.multiselect(
                "Property type",
                sorted(df["property_type"].dropna().unique()),
            )

            st.markdown("**Bedrooms**")
            min_bed = st.number_input("Min bedrooms", value=None, step=1, key="rm_min_bed")
            max_bed = st.number_input("Max bedrooms", value=None, step=1, key="rm_max_bed")

            st.markdown("**Price (£)**")
            min_price = st.number_input("Min price", value=None, step=5000, key="rm_min_price")
            max_price = st.number_input("Max price", value=None, step=5000, key="rm_max_price")

        # =========================
        # APPLY FILTERS
        # =========================
        if pc:
            df = df[df["postcode"].str.startswith(pc, na=False)]

        if property_types:
            df = df[df["property_type"].isin(property_types)]

        if min_bed is not None:
            df = df[df["bedrooms"] >= min_bed]

        if max_bed is not None:
            df = df[df["bedrooms"] <= max_bed]

        if min_price is not None:
            df = df[df["price"] >= min_price]

        if max_price is not None:
            df = df[df["price"] <= max_price]

        # =========================
        # SQL
        # =========================
        st.markdown("### 🧠 SQL (table: rightmove)")

        sql = st.text_area(
            "SQL",
            """SELECT postcode, COUNT(*) AS listings, AVG(price) AS avg_price
FROM rightmove
GROUP BY postcode
ORDER BY listings DESC
LIMIT 20;
""",
            height=140,
            key="rm_sql",
        )

        if st.button("Run SQL", key="rm_sql_btn"):
            df = con.execute(sql).df()

        # =========================
        # RESULTS
        # =========================
        st.caption(f"{len(df):,} listings")

        cols = [
            "postcode",
            "price",
            "bedrooms",
            "property_type",
            "address",
            "url",
            "scraped_at",
        ]
        cols = [c for c in cols if c in df.columns]

        config = {}
        if has_link_column() and "url" in cols:
            config["url"] = st.column_config.LinkColumn(
                "URL", display_text="Open"
            )

        st.dataframe(
            df[cols].sort_values("scraped_at", ascending=False),
            use_container_width=True,
            height=650,
            column_config=config if config else None,
        )

# ============================================================
# SPAREROOM TAB
# ============================================================

with tab_sr:
    st.subheader("🛏 SpareRoom Listings")

    if spareroom_df.empty:
        st.info("No SpareRoom data.")
    else:
        df = spareroom_df.copy()

        # =========================
        # SIDEBAR FILTERS
        # =========================
        with st.sidebar:
            st.header("SpareRoom Filters")

            pc = st.text_input("Postcode starts with", key="sr_pc").upper()

            room_types = st.multiselect(
                "Room type",
                sorted(df["room_type"].dropna().unique()),
            )

            availability = st.multiselect(
                "Available",
                sorted(df["available"].dropna().unique()),
            )

            st.markdown("**Price (£)**")
            min_price = st.number_input("Min price", value=None, step=100, key="sr_min_price")
            max_price = st.number_input("Max price", value=None, step=100, key="sr_max_price")

        # =========================
        # APPLY FILTERS
        # =========================
        if pc:
            df = df[df["postcode"].str.startswith(pc, na=False)]

        if room_types:
            df = df[df["room_type"].isin(room_types)]

        if availability:
            df = df[df["available"].isin(availability)]

        if min_price is not None:
            df = df[df["price_norm"] >= min_price]

        if max_price is not None:
            df = df[df["price_norm"] <= max_price]

        # =========================
        # SQL
        # =========================
        st.markdown("### 🧠 SQL (table: spareroom)")

        sql = st.text_area(
            "SQL",
            """SELECT postcode, COUNT(*) AS rooms, AVG(price_norm) AS avg_price
FROM spareroom
GROUP BY postcode
ORDER BY rooms DESC
LIMIT 20;
""",
            height=140,
            key="sr_sql",
        )

        if st.button("Run SQL", key="sr_sql_btn"):
            df = con.execute(sql).df()

        # =========================
        # RESULTS
        # =========================
        st.caption(f"{len(df):,} listings")

        cols = [
            "postcode",
            "price_norm",
            "room_type",
            "location",
            "url",
            "scraped_at",
        ]
        cols = [c for c in cols if c in df.columns]

        config = {}
        if has_link_column() and "url" in cols:
            config["url"] = st.column_config.LinkColumn(
                "URL", display_text="Open"
            )

        st.dataframe(
            df[cols].sort_values("scraped_at", ascending=False),
            use_container_width=True,
            height=650,
            column_config=config if config else None,
        )

# ============================================================
# FOOTER
# ============================================================

st.markdown("---")
st.caption("Rightmove and SpareRoom are handled independently to preserve schema integrity.")
