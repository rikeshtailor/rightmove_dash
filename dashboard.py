import json
import base64
from pathlib import Path

import streamlit as st
import pandas as pd
import folium
from streamlit_folium import st_folium


# ----------------------------
# CONFIG
# ----------------------------
st.set_page_config(page_title="Property Dashboard", layout="wide")

UK_CENTER = [54.5, -3.0]
UK_ZOOM = 6
DETAIL_ZOOM = 15

DATA_DIR = Path(__file__).parent / "data"
VIEWS_PATH = DATA_DIR / "views.json"


# ----------------------------
# HELPERS
# ----------------------------
def _read_any_path(path: Path) -> pd.DataFrame:
    name = path.name.lower()
    if name.endswith(".parquet"):
        return pd.read_parquet(path)
    if name.endswith(".csv"):
        return pd.read_csv(path)
    raise ValueError("Unsupported file type. Use .parquet or .csv")


def _read_any_upload(uploaded_file) -> pd.DataFrame:
    name = (uploaded_file.name or "").lower()
    if name.endswith(".parquet"):
        return pd.read_parquet(uploaded_file)
    if name.endswith(".csv"):
        return pd.read_csv(uploaded_file)
    raise ValueError("Unsupported file type. Use .parquet or .csv")


def _safe_str(s: pd.Series) -> pd.Series:
    return s.astype("string")


def _to_num(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")

def _to_bool(series: pd.Series) -> pd.Series:
    # handles real bool, 1/0, "true"/"false", "yes"/"no", etc.
    if pd.api.types.is_bool_dtype(series):
        return series
    s = _safe_str(series).str.strip().str.lower()
    return s.isin(["true", "t", "1", "yes", "y"])

def _parse_price(series: pd.Series) -> pd.Series:
    # If it's already numeric, don't stringify & regex it (avoids the ".0" -> extra zero bug)
    if pd.api.types.is_numeric_dtype(series):
        return pd.to_numeric(series, errors="coerce")

    s = _safe_str(series).str.strip()

    # Remove common currency/thousand separators but KEEP decimals
    s = (
        s.str.replace("£", "", regex=False)
         .str.replace(",", "", regex=False)
    )

    # Extract the first number with optional decimals (handles "£1,200 pcm", "100000.0", etc.)
    num = s.str.extract(r"(-?\d+(?:\.\d+)?)", expand=False)
    return pd.to_numeric(num, errors="coerce")


def _ensure_latlon(
    df: pd.DataFrame,
    lat_candidates=("lat", "latitude"),
    lon_candidates=("lon", "lng", "longitude"),
):
    lat_col = next((c for c in df.columns if c.lower() in lat_candidates), None)
    lon_col = next((c for c in df.columns if c.lower() in lon_candidates), None)

    if lat_col is None or lon_col is None:
        lat_col = lat_col or next((c for c in df.columns if "lat" in c.lower()), None)
        lon_col = lon_col or next(
            (c for c in df.columns if "lon" in c.lower() or "lng" in c.lower()), None
        )

    if lat_col is None or lon_col is None:
        return df

    df["lat_num"] = _to_num(df[lat_col])
    df["lon_num"] = _to_num(df[lon_col])
    return df


def _postcode_outcode(series: pd.Series) -> pd.Series:
    s = _safe_str(series).str.strip().str.upper()
    return s.str.split().str[0]


def build_map(center, zoom, rm_df, sr_offer_df, max_points_each: int):
    m = folium.Map(location=center, zoom_start=zoom, tiles="OpenStreetMap", control_scale=True)

    fg_rm = folium.FeatureGroup(name="Rightmove (for sale)", show=True)
    fg_sr = folium.FeatureGroup(name="SpareRoom (offered)", show=True)

    # Rightmove (red)
    if rm_df is not None and {"lat_num", "lon_num"}.issubset(rm_df.columns):
        pts = rm_df.dropna(subset=["lat_num", "lon_num"]).head(max_points_each)
        for _, r in pts.iterrows():
            popup = folium.Popup(
                f"""
                <b>{r.get('address','')}</b><br>
                Postcode: {r.get('postcode','')}<br>
                Price: {r.get('price','')}<br>
                Beds: {r.get('bedrooms','')}<br>
                Type: {r.get('property_type','')}<br>
                <a href="{r.get('url','')}" target="_blank">Open listing</a>
                """,
                max_width=350,
            )
            folium.CircleMarker(
                location=[float(r["lat_num"]), float(r["lon_num"])],
                radius=3,
                color="#e74c3c",
                fill=True,
                fill_color="#e74c3c",
                fill_opacity=0.7,
                popup=popup,
            ).add_to(fg_rm)

    # SpareRoom offered (blue) — NO POPUPS if you prefer? (you said earlier you don't need them)
    # We'll keep markers only (no popup) for performance/clarity.
    if sr_offer_df is not None and {"lat_num", "lon_num"}.issubset(sr_offer_df.columns):
        pts = sr_offer_df.dropna(subset=["lat_num", "lon_num"]).head(max_points_each)
        for _, r in pts.iterrows():
            folium.CircleMarker(
                location=[float(r["lat_num"]), float(r["lon_num"])],
                radius=3,
                color="#2e86de",
                fill=True,
                fill_color="#2e86de",
                fill_opacity=0.7,
            ).add_to(fg_sr)

    fg_rm.add_to(m)
    fg_sr.add_to(m)
    folium.LayerControl(collapsed=False).add_to(m)
    return m


@st.cache_data(show_spinner=False)
def load_rightmove_from_path(path_str: str) -> pd.DataFrame:
    path = Path(path_str)
    df = _read_any_path(path)
    df["price_num"] = _parse_price(df["price"]) if "price" in df.columns else pd.Series(dtype="float")
    df["bedrooms_num"] = _to_num(df["bedrooms"]) if "bedrooms" in df.columns else pd.Series(dtype="float")
    df = _ensure_latlon(df)
    if "postcode" in df.columns:
        df["outcode"] = _postcode_outcode(df["postcode"])
    return df


@st.cache_data(show_spinner=False)
def load_generic_from_path(path_str: str) -> pd.DataFrame:
    path = Path(path_str)
    df = _read_any_path(path)
    df = _ensure_latlon(df)
    if "postcode" in df.columns:
        df["outcode"] = _postcode_outcode(df["postcode"])
    if "price" in df.columns:
        df["price_num"] = _parse_price(df["price"])
    elif "rent" in df.columns:
        df["price_num"] = _parse_price(df["rent"])
    return df


def _list_data_files(extensions=(".parquet", ".csv")):
    if not DATA_DIR.exists():
        return []
    files = []
    for ext in extensions:
        files.extend(sorted(DATA_DIR.glob(f"*{ext}")))
    return files


def _ensure_data_dir():
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def _load_views() -> dict:
    _ensure_data_dir()
    if not VIEWS_PATH.exists():
        return {}
    try:
        return json.loads(VIEWS_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_views(views: dict) -> None:
    _ensure_data_dir()
    VIEWS_PATH.write_text(json.dumps(views, indent=2), encoding="utf-8")


def _view_payload_from_state() -> dict:
    return {
        "filters": {
            "rm_price_min": st.session_state.get("rm_price_min"),
            "rm_price_max": st.session_state.get("rm_price_max"),
            "rm_beds_min": st.session_state.get("rm_beds_min"),
            "rm_beds_max": st.session_state.get("rm_beds_max"),
            "rm_property_types": st.session_state.get("rm_property_types", []),
            "sr_price_min": st.session_state.get("sr_price_min"),
            "sr_price_max": st.session_state.get("sr_price_max"),
            "sr_outcodes": st.session_state.get("sr_outcodes", []),
            "rm_potential_auction": st.session_state.get("rm_potential_auction", False),
            "rm_potential_hmo": st.session_state.get("rm_potential_hmo", False),
        },
        "map": {
            "center": st.session_state.get("map_center", UK_CENTER),
            "zoom": st.session_state.get("map_zoom", UK_ZOOM),
        },
        "selected_outcode": st.session_state.get("selected_outcode"),
    }


def _apply_view_payload(payload: dict) -> None:
    f = (payload or {}).get("filters", {})
    m = (payload or {}).get("map", {})

    st.session_state["rm_price_min"] = f.get("rm_price_min")
    st.session_state["rm_price_max"] = f.get("rm_price_max")
    st.session_state["rm_beds_min"] = f.get("rm_beds_min")
    st.session_state["rm_beds_max"] = f.get("rm_beds_max")
    st.session_state["rm_property_types"] = f.get("rm_property_types", [])

    st.session_state["sr_price_min"] = f.get("sr_price_min")
    st.session_state["sr_price_max"] = f.get("sr_price_max")
    st.session_state["sr_outcodes"] = f.get("sr_outcodes", [])

    st.session_state["map_center"] = m.get("center", UK_CENTER)
    st.session_state["map_zoom"] = m.get("zoom", UK_ZOOM)

    st.session_state["selected_outcode"] = payload.get("selected_outcode")
    st.session_state["rm_potential_auction"] = f.get("rm_potential_auction", False)
    st.session_state["rm_potential_hmo"] = f.get("rm_potential_hmo", False)

def _encode_payload(payload: dict) -> str:
    raw = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii")


def _decode_payload(s: str) -> dict | None:
    try:
        raw = base64.urlsafe_b64decode(s.encode("ascii"))
        return json.loads(raw.decode("utf-8"))
    except Exception:
        return None


# ----------------------------
# STATE
# ----------------------------
if "rm_df" not in st.session_state:
    st.session_state.rm_df = None
if "sr_offer_df" not in st.session_state:
    st.session_state.sr_offer_df = None
if "sr_wanted_df" not in st.session_state:
    st.session_state.sr_wanted_df = None

if "map_center" not in st.session_state:
    st.session_state.map_center = UK_CENTER
if "map_zoom" not in st.session_state:
    st.session_state.map_zoom = UK_ZOOM

if "selected_outcode" not in st.session_state:
    st.session_state.selected_outcode = None

# Rightmove filter state
for k in ["rm_price_min", "rm_price_max", "rm_beds_min", "rm_beds_max", "rm_property_types",
          "rm_potential_auction", "rm_potential_hmo"]:
    if k not in st.session_state:
        if k == "rm_property_types":
            st.session_state[k] = []
        else:
            st.session_state[k] = False

# SpareRoom offered filter state
for k in ["sr_price_min", "sr_price_max", "sr_outcodes"]:
    if k not in st.session_state:
        st.session_state[k] = None if k != "sr_outcodes" else []


# ----------------------------
# URL view restore (shareable)
# ----------------------------
q = st.query_params
if "view_b64" in q and q["view_b64"]:
    payload = _decode_payload(q["view_b64"])
    if payload:
        _apply_view_payload(payload)
        st.query_params.clear()


# ----------------------------
# SIDEBAR: DATA SOURCE
# ----------------------------
st.sidebar.title("Data")

data_source = st.sidebar.radio(
    "Load data from…",
    ["Repo files (recommended for deployment)", "Upload files (local)"],
    index=0,
)

st.sidebar.caption(f"Data directory: `{DATA_DIR.as_posix()}`")
max_points_each = st.sidebar.slider("Max points per layer (performance)", 500, 8000, 3000, step=500)
st.sidebar.divider()


def _load_from_repo_ui():
    files = _list_data_files(extensions=(".parquet", ".csv"))
    if not files:
        st.sidebar.warning("No data files found in ./data. Add your parquet/csv files to the repo under `data/`.")
        return

    file_names = [f.name for f in files]
    name_to_path = {f.name: str(f) for f in files}

    st.sidebar.subheader("Rightmove")
    rm_choice = st.sidebar.selectbox("Rightmove file", ["(none)"] + file_names, index=0, key="rm_repo_choice")
    rm_load = st.sidebar.button("Load Rightmove", key="rm_repo_load")

    st.sidebar.subheader("SpareRoom Offered")
    sr_offer_choice = st.sidebar.selectbox(
        "SpareRoom Offered file", ["(none)"] + file_names, index=0, key="sr_offer_repo_choice"
    )
    sr_offer_load = st.sidebar.button("Load SpareRoom Offered", key="sr_offer_repo_load")

    st.sidebar.subheader("SpareRoom Wanted")
    sr_wanted_choice = st.sidebar.selectbox(
        "SpareRoom Wanted file", ["(none)"] + file_names, index=0, key="sr_wanted_repo_choice"
    )
    sr_wanted_load = st.sidebar.button("Load SpareRoom Wanted", key="sr_wanted_repo_load")

    if rm_load and rm_choice != "(none)":
        try:
            df = load_rightmove_from_path(name_to_path[rm_choice])
            st.session_state.rm_df = df
            st.sidebar.success(f"Loaded Rightmove: {len(df):,} rows")
        except Exception as e:
            st.sidebar.error(f"Rightmove load failed: {e}")

    if sr_offer_load and sr_offer_choice != "(none)":
        try:
            df = load_generic_from_path(name_to_path[sr_offer_choice])
            st.session_state.sr_offer_df = df
            st.sidebar.success(f"Loaded SpareRoom Offered: {len(df):,} rows")
        except Exception as e:
            st.sidebar.error(f"SpareRoom Offered load failed: {e}")

    if sr_wanted_load and sr_wanted_choice != "(none)":
        try:
            df = load_generic_from_path(name_to_path[sr_wanted_choice])
            if "postcode" in df.columns:
                df["outcode"] = _postcode_outcode(df["postcode"])
            elif "location" in df.columns:
                df["outcode"] = _postcode_outcode(df["location"])
            st.session_state.sr_wanted_df = df
            st.sidebar.success(f"Loaded SpareRoom Wanted: {len(df):,} rows")
        except Exception as e:
            st.sidebar.error(f"SpareRoom Wanted load failed: {e}")


def _load_from_upload_ui():
    rm_file = st.sidebar.file_uploader("Rightmove (parquet)", type=["parquet"], key="rm_uploader")
    rm_load = st.sidebar.button("Load Rightmove", key="rm_load")

    sr_offer_file = st.sidebar.file_uploader(
        "SpareRoom Offered (parquet/csv)", type=["parquet", "csv"], key="sr_offer_uploader"
    )
    sr_offer_load = st.sidebar.button("Load SpareRoom Offered", key="sr_offer_load")

    sr_wanted_file = st.sidebar.file_uploader(
        "SpareRoom Wanted (parquet/csv)", type=["parquet", "csv"], key="sr_wanted_uploader"
    )
    sr_wanted_load = st.sidebar.button("Load SpareRoom Wanted", key="sr_wanted_load")

    if rm_load:
        if rm_file is None:
            st.sidebar.warning("Select a Rightmove parquet first.")
        else:
            try:
                df = pd.read_parquet(rm_file)
                df["price_num"] = _parse_price(df["price"]) if "price" in df.columns else pd.Series(dtype="float")
                df["bedrooms_num"] = _to_num(df["bedrooms"]) if "bedrooms" in df.columns else pd.Series(dtype="float")
                df = _ensure_latlon(df)
                if "postcode" in df.columns:
                    df["outcode"] = _postcode_outcode(df["postcode"])
                st.session_state.rm_df = df
                st.sidebar.success(f"Loaded Rightmove: {len(df):,} rows")
            except Exception as e:
                st.sidebar.error(f"Rightmove load failed: {e}")

    if sr_offer_load:
        if sr_offer_file is None:
            st.sidebar.warning("Select a SpareRoom Offered file first.")
        else:
            try:
                df = _read_any_upload(sr_offer_file)
                df = _ensure_latlon(df)
                if "postcode" in df.columns:
                    df["outcode"] = _postcode_outcode(df["postcode"])
                if "price" in df.columns:
                    df["price_num"] = _parse_price(df["price"])
                elif "rent" in df.columns:
                    df["price_num"] = _parse_price(df["rent"])
                st.session_state.sr_offer_df = df
                st.sidebar.success(f"Loaded SpareRoom Offered: {len(df):,} rows")
            except Exception as e:
                st.sidebar.error(f"SpareRoom Offered load failed: {e}")

    if sr_wanted_load:
        if sr_wanted_file is None:
            st.sidebar.warning("Select a SpareRoom Wanted file first.")
        else:
            try:
                df = _read_any_upload(sr_wanted_file)
                if "postcode" in df.columns:
                    df["outcode"] = _postcode_outcode(df["postcode"])
                elif "location" in df.columns:
                    df["outcode"] = _postcode_outcode(df["location"])
                st.session_state.sr_wanted_df = df
                st.sidebar.success(f"Loaded SpareRoom Wanted: {len(df):,} rows")
            except Exception as e:
                st.sidebar.error(f"SpareRoom Wanted load failed: {e}")


if data_source == "Repo files (recommended for deployment)":
    _load_from_repo_ui()
else:
    _load_from_upload_ui()


# ----------------------------
# SIDEBAR: VIEWS (PERSIST + SHARE)
# ----------------------------
st.sidebar.divider()
st.sidebar.subheader("Views")

views = _load_views()
view_names = ["(none)"] + sorted(views.keys())
selected_view = st.sidebar.selectbox("Saved views", view_names, index=0, key="views_select")
colv1, colv2, colv3 = st.sidebar.columns(3)

with colv1:
    if st.button("Load", use_container_width=True, disabled=(selected_view == "(none)")):
        payload = views.get(selected_view)
        if payload:
            _apply_view_payload(payload)
            st.rerun()

with colv2:
    if st.button("Delete", use_container_width=True, disabled=(selected_view == "(none)")):
        if selected_view in views:
            views.pop(selected_view, None)
            _save_views(views)
            st.rerun()

with colv3:
    if st.button("Share", use_container_width=True, disabled=(selected_view == "(none)")):
        payload = views.get(selected_view)
        if payload:
            st.query_params.clear()
            st.query_params["view_b64"] = _encode_payload(payload)
            st.sidebar.success("Shareable URL set (copy browser address bar).")

new_view_name = st.sidebar.text_input("New view name", value="", key="new_view_name").strip()
if st.sidebar.button("Save current as view", use_container_width=True, disabled=(new_view_name == "")):
    payload = _view_payload_from_state()
    views[new_view_name] = payload
    _save_views(views)
    st.sidebar.success(f"Saved view: {new_view_name}")
    st.rerun()


# ----------------------------
# MAIN UI
# ----------------------------
st.title("🏠 Property Dashboard")

rm_df = st.session_state.rm_df
sr_offer_df = st.session_state.sr_offer_df
sr_wanted_df = st.session_state.sr_wanted_df

if rm_df is None and sr_offer_df is None and sr_wanted_df is None:
    st.info("Load at least one dataset from the sidebar.")
    st.stop()


# ----------------------------
# RIGHTMOVE FILTERS (STATE-DRIVEN, ONE PASS)
# ----------------------------
filtered_rm = rm_df
if rm_df is not None:
    st.sidebar.divider()
    st.sidebar.subheader("Rightmove filters")

    filtered_rm = rm_df.copy()

    # Property type filter (Select all / Clear all) — FIXED
    if "property_type" in rm_df.columns:
        options = sorted(
            rm_df["property_type"]
            .astype("string")
            .fillna("")
            .replace("nan", "")
            .loc[lambda s: s.str.strip() != ""]
            .unique()
            .tolist()
        )

        if options:
            # initialise once
            if not st.session_state["rm_property_types"]:
                st.session_state["rm_property_types"] = options.copy()

            st.sidebar.markdown("**Property type**")
            c1, c2 = st.sidebar.columns(2)

            if c1.button("Select all", key="rm_pt_select_all"):
                st.session_state["rm_property_types"] = options.copy()
                st.rerun()

            if c2.button("Clear all", key="rm_pt_clear_all"):
                st.session_state["rm_property_types"] = []
                st.rerun()

            # widget READS state, does NOT write it
            selected_types = st.sidebar.multiselect(
                " ",
                options=options,
                default=st.session_state["rm_property_types"],
                label_visibility="collapsed",
            )

            # apply filter using local value
            if selected_types:
                filtered_rm = filtered_rm[
                    filtered_rm["property_type"]
                    .astype("string")
                    .isin(selected_types)
                ]
            else:
                pass

            # persist for views
            st.session_state["rm_property_types"] = selected_types


    # Price min/max — initialise from FULL rm_df (not already-filtered)
    if "price_num" in rm_df.columns and rm_df["price_num"].notna().any():
        pmin_data = float(rm_df["price_num"].min())
        pmax_data = float(rm_df["price_num"].max())

        # Initialise / clamp BEFORE widget creation
        cur_min = st.session_state.get("rm_price_min")
        cur_max = st.session_state.get("rm_price_max")

        if cur_min is None or cur_min <= 0 or cur_min < pmin_data or cur_min > pmax_data:
            st.session_state["rm_price_min"] = int(pmin_data)
        if cur_max is None or cur_max <= 0 or cur_max > pmax_data or cur_max < pmin_data:
            st.session_state["rm_price_max"] = int(pmax_data)

        pcol1, pcol2 = st.sidebar.columns(2)
        with pcol1:
            st.number_input("Min price (£)", min_value=0, step=1000, key="rm_price_min")
        with pcol2:
            st.number_input("Max price (£)", min_value=0, step=1000, key="rm_price_max")

        lo = float(st.session_state["rm_price_min"])
        hi = float(st.session_state["rm_price_max"])
        if hi < lo:
            lo, hi = hi, lo

        filtered_rm = filtered_rm[filtered_rm["price_num"].between(lo, hi)]

    # Bedrooms min/max — initialise from FULL rm_df
    if "bedrooms_num" in rm_df.columns and rm_df["bedrooms_num"].notna().any():
        bmin_data = float(rm_df["bedrooms_num"].min())
        bmax_data = float(rm_df["bedrooms_num"].max())

        cur_min = st.session_state.get("rm_beds_min")
        cur_max = st.session_state.get("rm_beds_max")

        if cur_min is None or cur_min < bmin_data or cur_min > bmax_data:
            st.session_state["rm_beds_min"] = bmin_data
        if cur_max is None or cur_max > bmax_data or cur_max < bmin_data:
            st.session_state["rm_beds_max"] = bmax_data

        bcol1, bcol2 = st.sidebar.columns(2)
        with bcol1:
            st.number_input("Min bedrooms", min_value=0, value=int(st.session_state["rm_beds_min"]), step=1, key="rm_beds_min")
        with bcol2:
            st.number_input("Max bedrooms", min_value=0, value=int(st.session_state["rm_beds_max"]), step=1, key="rm_beds_max")

        lo = float(st.session_state["rm_beds_min"])
        hi = float(st.session_state["rm_beds_max"])
        if hi < lo:
            lo, hi = hi, lo

        filtered_rm = filtered_rm[filtered_rm["bedrooms_num"].between(lo, hi)]
            
    c1, c2 = st.sidebar.columns(2)

    if "potential_auction" in rm_df.columns:
        c1.checkbox(
            "Potential auction",
            key="rm_potential_auction",
            value=bool(st.session_state.get("rm_potential_auction", False)),
        )
        if st.session_state["rm_potential_auction"]:
            filtered_rm = filtered_rm[_to_bool(filtered_rm["potential_auction"])]

    if "potential_hmo" in rm_df.columns:
        c2.checkbox(
            "Potential HMO",
            key="rm_potential_hmo",
            value=bool(st.session_state.get("rm_potential_hmo", False)),
        )
        if st.session_state["rm_potential_hmo"]:
            filtered_rm = filtered_rm[_to_bool(filtered_rm["potential_hmo"])]

    # keep only mappable rows for map layer
    if filtered_rm is not None and {"lat_num", "lon_num"}.issubset(filtered_rm.columns):
        filtered_rm = filtered_rm.dropna(subset=["lat_num", "lon_num"])


# ----------------------------
# SPAREROOM OFFERED FILTERS (STATE-DRIVEN)
# ----------------------------
filtered_sr_offer = sr_offer_df
if sr_offer_df is not None:
    st.sidebar.divider()
    st.sidebar.subheader("SpareRoom Offered filters")

    filtered_sr_offer = sr_offer_df.copy()

    # Rent filter (uses price_num if present)
    if "price_num" in sr_offer_df.columns and sr_offer_df["price_num"].notna().any():
        smin_data = float(sr_offer_df["price_num"].min())
        smax_data = float(sr_offer_df["price_num"].max())

        if st.session_state.get("sr_price_min") is None:
            st.session_state["sr_price_min"] = smin_data
        if st.session_state.get("sr_price_max") is None:
            st.session_state["sr_price_max"] = smax_data

        scol1, scol2 = st.sidebar.columns(2)
        with scol1:
            st.number_input("Min rent (£ pcm)", min_value=0, value=int(st.session_state["sr_price_min"]), step=50, key="sr_price_min")
        with scol2:
            st.number_input("Max rent (£ pcm)", min_value=0, value=int(st.session_state["sr_price_max"]), step=50, key="sr_price_max")

        lo = float(st.session_state["sr_price_min"])
        hi = float(st.session_state["sr_price_max"])
        if hi < lo:
            lo, hi = hi, lo

        if "price_num" in filtered_sr_offer.columns:
            filtered_sr_offer = filtered_sr_offer[filtered_sr_offer["price_num"].between(lo, hi)]

    # Outcodes with Select all / Clear all — FIXED
    if "outcode" in sr_offer_df.columns:
        outcodes = sorted(sr_offer_df["outcode"].dropna().unique().tolist())
        if outcodes:
            st.sidebar.markdown("**Outcode**")
            c1, c2 = st.sidebar.columns(2)

            if c1.button("Select all", key="sr_oc_select_all"):
                st.session_state["sr_outcodes"] = outcodes.copy()
                st.rerun()

            if c2.button("Clear all", key="sr_oc_clear_all"):
                st.session_state["sr_outcodes"] = []
                st.rerun()

            selected_outcodes = st.sidebar.multiselect(
                " ",
                options=outcodes,
                default=st.session_state["sr_outcodes"],
                label_visibility="collapsed",
            )

            if selected_outcodes:
                filtered_sr_offer = filtered_sr_offer[
                    filtered_sr_offer["outcode"].isin(selected_outcodes)
                ]

            # persist for views
            st.session_state["sr_outcodes"] = selected_outcodes


    # only plot mappable rows
    if {"lat_num", "lon_num"}.issubset(filtered_sr_offer.columns):
        filtered_sr_offer = filtered_sr_offer.dropna(subset=["lat_num", "lon_num"])


# ----------------------------
# MAP
# ----------------------------
st.subheader("Map")
rm_count = len(filtered_rm) if filtered_rm is not None else 0
sr_count = len(filtered_sr_offer) if filtered_sr_offer is not None else 0
st.caption(f"Rightmove: {rm_count:,} | SpareRoom Offered: {sr_count:,}")

m = build_map(
    center=st.session_state.map_center,
    zoom=st.session_state.map_zoom,
    rm_df=filtered_rm,
    sr_offer_df=filtered_sr_offer,
    max_points_each=max_points_each,
)

st_folium(
    m,
    height=520,
    use_container_width=True,
    key="map_view",
    returned_objects=[],
)

st.divider()


# ----------------------------
# TABLES
# ----------------------------
col1, col2 = st.columns([1.25, 1])

with col1:
    st.subheader("Rightmove Properties")

    if rm_df is None:
        st.info("Load Rightmove to enable selection + postcode filtering.")
    else:
        show_cols = [
            "address", "postcode", "price", "bedrooms", "property_type",
            "potential_auction", "potential_hmo",
            "url",
        ] 
        show_cols = [c for c in show_cols if c in filtered_rm.columns]

        table_df = filtered_rm.reset_index(drop=True)
        event = st.dataframe(
            table_df[show_cols] if show_cols else table_df,
            use_container_width=True,
            hide_index=True,
            selection_mode="single-row",
            on_select="rerun",
            key="rm_table",
            column_config={
                "url": st.column_config.LinkColumn(
                    "URL",
                    display_text="Open",
                ),
            },
        )

        selected_rows = []
        if isinstance(event, dict):
            selected_rows = event.get("selection", {}).get("rows", [])
        else:
            state_val = st.session_state.get("rm_table")
            if isinstance(state_val, dict):
                selected_rows = state_val.get("selection", {}).get("rows", [])

        if selected_rows:
            idx = selected_rows[0]
            row = table_df.iloc[idx]

            if "lat_num" in table_df.columns and "lon_num" in table_df.columns:
                lat = row.get("lat_num")
                lon = row.get("lon_num")
                if pd.notna(lat) and pd.notna(lon):
                    st.session_state.map_center = [float(lat), float(lon)]
                    st.session_state.map_zoom = DETAIL_ZOOM

            outcode = None
            if "postcode" in table_df.columns and pd.notna(row.get("postcode")):
                outcode = str(row.get("postcode")).strip().upper().split()[0]
            st.session_state.selected_outcode = outcode

            st.success(f"Selected: {row.get('address','(no address)')}  |  Outcode: {outcode or 'n/a'}")

with col2:
    st.subheader("SpareRoom Wanted (filtered by outcode)")

    if sr_wanted_df is None:
        st.info("Load SpareRoom Wanted to see this table.")
    else:
        wanted = sr_wanted_df.copy()

        outcode = st.session_state.selected_outcode
        if outcode and "outcode" in wanted.columns:
            wanted = wanted[wanted["outcode"] == outcode]

        st.caption(f"Showing {len(wanted):,} wanted-room rows" + (f" for outcode **{outcode}**" if outcode else ""))

        wanted_cols_pref = ["title", "postcode", "location", "budget", "price", "rent", "bedrooms", "move_in", "url"]
        wanted_cols = [c for c in wanted_cols_pref if c in wanted.columns]
        if not wanted_cols:
            wanted_cols = list(wanted.columns)[:10]

        st.dataframe(
            wanted[wanted_cols],
            use_container_width=True,
            hide_index=True,
        )
