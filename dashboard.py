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
    if pd.api.types.is_bool_dtype(series):
        return series
    s = _safe_str(series).str.strip().str.lower()
    return s.isin(["true", "t", "1", "yes", "y"])


def _parse_price(series: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(series):
        return pd.to_numeric(series, errors="coerce")
    s = _safe_str(series).str.strip()
    s = s.str.replace("£", "", regex=False).str.replace(",", "", regex=False)
    num = s.str.extract(r"(-?\d+(?:\.\d+)?)", expand=False)
    return pd.to_numeric(num, errors="coerce")


def _ensure_latlon(df: pd.DataFrame) -> pd.DataFrame:
    lat_candidates = ("lat", "latitude")
    lon_candidates = ("lon", "lng", "longitude")
    lat_col = next((c for c in df.columns if c.lower() in lat_candidates), None)
    lon_col = next((c for c in df.columns if c.lower() in lon_candidates), None)
    if lat_col is None:
        lat_col = next((c for c in df.columns if "lat" in c.lower()), None)
    if lon_col is None:
        lon_col = next((c for c in df.columns if "lon" in c.lower() or "lng" in c.lower()), None)
    if lat_col is None or lon_col is None:
        return df
    df["lat_num"] = _to_num(df[lat_col])
    df["lon_num"] = _to_num(df[lon_col])
    return df


def _postcode_outcode(series: pd.Series) -> pd.Series:
    return _safe_str(series).str.strip().str.upper().str.split().str[0]




# ----------------------------
# MAP  (GeoJson path: one Python object per layer, Leaflet renders N markers)
# ----------------------------
def build_map(center, zoom, rm_df, sr_offer_df, max_points_each: int):
    m = folium.Map(
        location=center,
        zoom_start=zoom,
        min_zoom=5,
        tiles="CartoDB positron",
        control_scale=True,
    )
    # Restrict panning/zooming to UK region
    m.options["maxBounds"] = [[49.0, -9.5], [61.5, 3.5]]
    m.options["maxBoundsViscosity"] = 1.0

    def _add_layer(df, name: str, color: str, with_popup: bool = True):
        if df is None or df.empty:
            return
        pts = df.dropna(subset=["lat_num", "lon_num"]).head(max_points_each)
        if pts.empty:
            return

        prop_cols = [
            c for c in ["address", "postcode", "price", "bedrooms", "property_type", "url"]
            if c in pts.columns
        ]
        keep = prop_cols + ["lat_num", "lon_num"]

        # Build GeoJSON with a list comprehension — no iterrows
        records = pts[keep].fillna("").to_dict("records")

        def _prop_value(k, v):
            s = str(v)
            if k == "url" and s.startswith("http"):
                return f'<a href="{s}" target="_blank" rel="noopener noreferrer">View listing ↗</a>'
            return s

        geojson = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [r["lon_num"], r["lat_num"]],
                    },
                    "properties": {k: _prop_value(k, r[k]) for k in prop_cols},
                }
                for r in records
            ],
        }

        layer = folium.GeoJson(
            geojson,
            name=name,
            marker=folium.CircleMarker(
                radius=5,
                fill_color=color,
                color=color,
                fill_opacity=0.7,
                weight=1,
            ),
        )
        if with_popup and prop_cols:
            folium.GeoJsonPopup(
                fields=prop_cols,
                aliases=[
                    "Link" if c == "url" else c.replace("_", " ").title()
                    for c in prop_cols
                ],
                max_width=320,
            ).add_to(layer)

        fg = folium.FeatureGroup(name=name, show=True)
        layer.add_to(fg)
        fg.add_to(m)

    _add_layer(rm_df, "Rightmove (for sale)", "#e74c3c")
    _add_layer(sr_offer_df, "SpareRoom (offered)", "#2e86de", with_popup=False)
    folium.LayerControl(collapsed=False).add_to(m)
    return m


# ----------------------------
# DATA LOADING (cached)
# ----------------------------
@st.cache_data(show_spinner=False)
def load_rightmove_from_path(path_str: str) -> pd.DataFrame:
    df = _read_any_path(Path(path_str))
    df["price_num"] = _parse_price(df["price"]) if "price" in df.columns else pd.Series(dtype="float")
    df["bedrooms_num"] = _to_num(df["bedrooms"]) if "bedrooms" in df.columns else pd.Series(dtype="float")
    df = _ensure_latlon(df)
    if "postcode" in df.columns:
        df["outcode"] = _postcode_outcode(df["postcode"])
    return df


@st.cache_data(show_spinner=False)
def load_generic_from_path(path_str: str) -> pd.DataFrame:
    df = _read_any_path(Path(path_str))
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


# ----------------------------
# VIEWS
# ----------------------------
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
    st.session_state["rm_potential_auction"] = f.get("rm_potential_auction", False)
    st.session_state["rm_potential_hmo"] = f.get("rm_potential_hmo", False)
    st.session_state["map_center"] = m.get("center", UK_CENTER)
    st.session_state["map_zoom"] = m.get("zoom", UK_ZOOM)
    st.session_state["selected_outcode"] = payload.get("selected_outcode")


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
# SESSION STATE INIT
# ----------------------------
_DEFAULTS = {
    "rm_df": None,
    "sr_offer_df": None,
    "sr_wanted_df": None,
    "map_center": UK_CENTER,
    "map_zoom": UK_ZOOM,
    "selected_outcode": None,
    "rm_price_min": None,
    "rm_price_max": None,
    "rm_beds_min": None,
    "rm_beds_max": None,
    "rm_property_types": [],
    "rm_potential_auction": False,
    "rm_potential_hmo": False,
}
for _k, _v in _DEFAULTS.items():
    if _k not in st.session_state:
        st.session_state[_k] = _v


# ----------------------------
# URL view restore
# ----------------------------
_q = st.query_params
if "view_b64" in _q and _q["view_b64"]:
    _payload = _decode_payload(_q["view_b64"])
    if _payload:
        _apply_view_payload(_payload)
        st.query_params.clear()


# ----------------------------
# SIDEBAR: DATA LOADING
# ----------------------------
st.sidebar.title("Property Dashboard")
st.sidebar.caption(f"`{DATA_DIR.as_posix()}`")
st.sidebar.divider()

_source = st.sidebar.radio("Load from", ["Repo files", "Upload"], horizontal=True)


def _repo_loader():
    files = _list_data_files()
    if not files:
        st.sidebar.warning("No .parquet/.csv files found in ./data")
        return
    names = [f.name for f in files]
    name_to_path = {f.name: str(f) for f in files}

    st.sidebar.subheader("Rightmove")
    rm_choice = st.sidebar.selectbox(
        "RM file", ["(none)"] + names, key="rm_repo_choice", label_visibility="collapsed"
    )
    if st.sidebar.button("Load", key="rm_repo_load", use_container_width=True) and rm_choice != "(none)":
        try:
            st.session_state.rm_df = load_rightmove_from_path(name_to_path[rm_choice])
            st.sidebar.success(f"{len(st.session_state.rm_df):,} rows")
        except Exception as e:
            st.sidebar.error(str(e))

    st.sidebar.subheader("SpareRoom Offered")
    sro_choice = st.sidebar.selectbox(
        "SRO file", ["(none)"] + names, key="sro_repo_choice", label_visibility="collapsed"
    )
    if st.sidebar.button("Load", key="sro_repo_load", use_container_width=True) and sro_choice != "(none)":
        try:
            st.session_state.sr_offer_df = load_generic_from_path(name_to_path[sro_choice])
            st.sidebar.success(f"{len(st.session_state.sr_offer_df):,} rows")
        except Exception as e:
            st.sidebar.error(str(e))

    st.sidebar.subheader("SpareRoom Wanted")
    srw_choice = st.sidebar.selectbox(
        "SRW file", ["(none)"] + names, key="srw_repo_choice", label_visibility="collapsed"
    )
    if st.sidebar.button("Load", key="srw_repo_load", use_container_width=True) and srw_choice != "(none)":
        try:
            df = load_generic_from_path(name_to_path[srw_choice])
            if "location" in df.columns and "outcode" not in df.columns:
                df["outcode"] = _postcode_outcode(df["location"])
            st.session_state.sr_wanted_df = df
            st.sidebar.success(f"{len(df):,} rows")
        except Exception as e:
            st.sidebar.error(str(e))


def _upload_loader():
    rm_file = st.sidebar.file_uploader("Rightmove (.parquet)", type=["parquet"])
    if st.sidebar.button("Load Rightmove", use_container_width=True) and rm_file:
        try:
            df = pd.read_parquet(rm_file)
            df["price_num"] = _parse_price(df["price"]) if "price" in df.columns else pd.Series(dtype="float")
            df["bedrooms_num"] = _to_num(df["bedrooms"]) if "bedrooms" in df.columns else pd.Series(dtype="float")
            df = _ensure_latlon(df)
            if "postcode" in df.columns:
                df["outcode"] = _postcode_outcode(df["postcode"])
            st.session_state.rm_df = df
            st.sidebar.success(f"{len(df):,} rows")
        except Exception as e:
            st.sidebar.error(str(e))

    sro_file = st.sidebar.file_uploader("SpareRoom Offered (.parquet/.csv)", type=["parquet", "csv"])
    if st.sidebar.button("Load SR Offered", use_container_width=True) and sro_file:
        try:
            df = _read_any_upload(sro_file)
            df = _ensure_latlon(df)
            if "postcode" in df.columns:
                df["outcode"] = _postcode_outcode(df["postcode"])
            if "price" in df.columns:
                df["price_num"] = _parse_price(df["price"])
            elif "rent" in df.columns:
                df["price_num"] = _parse_price(df["rent"])
            st.session_state.sr_offer_df = df
            st.sidebar.success(f"{len(df):,} rows")
        except Exception as e:
            st.sidebar.error(str(e))

    srw_file = st.sidebar.file_uploader("SpareRoom Wanted (.parquet/.csv)", type=["parquet", "csv"])
    if st.sidebar.button("Load SR Wanted", use_container_width=True) and srw_file:
        try:
            df = _read_any_upload(srw_file)
            if "postcode" in df.columns:
                df["outcode"] = _postcode_outcode(df["postcode"])
            elif "location" in df.columns:
                df["outcode"] = _postcode_outcode(df["location"])
            st.session_state.sr_wanted_df = df
            st.sidebar.success(f"{len(df):,} rows")
        except Exception as e:
            st.sidebar.error(str(e))


if _source == "Repo files":
    _repo_loader()
else:
    _upload_loader()

st.sidebar.divider()

# ----------------------------
# SIDEBAR: VIEWS
# ----------------------------
st.sidebar.subheader("Saved views")
_views = _load_views()
_view_names = ["(none)"] + sorted(_views.keys())
_sel_view = st.sidebar.selectbox("View", _view_names, key="views_select", label_visibility="collapsed")

_vc1, _vc2, _vc3 = st.sidebar.columns(3)
if _vc1.button("Load", use_container_width=True, disabled=(_sel_view == "(none)")):
    _p = _views.get(_sel_view)
    if _p:
        _apply_view_payload(_p)
        st.rerun()
if _vc2.button("Delete", use_container_width=True, disabled=(_sel_view == "(none)")):
    _views.pop(_sel_view, None)
    _save_views(_views)
    st.rerun()
if _vc3.button("Share", use_container_width=True, disabled=(_sel_view == "(none)")):
    _p = _views.get(_sel_view)
    if _p:
        st.query_params["view_b64"] = _encode_payload(_p)
        st.sidebar.info("Copy the URL from the address bar.")

_new_name = st.sidebar.text_input("Save current as…", key="new_view_name").strip()
if st.sidebar.button("💾 Save view", use_container_width=True, disabled=(not _new_name)):
    _views[_new_name] = _view_payload_from_state()
    _save_views(_views)
    st.sidebar.success(f'Saved "{_new_name}"')
    st.rerun()


# ----------------------------
# MAIN — guard
# ----------------------------
st.title("🏠 Property Dashboard")

rm_df = st.session_state.rm_df
sr_offer_df = st.session_state.sr_offer_df
sr_wanted_df = st.session_state.sr_wanted_df

if rm_df is None and sr_offer_df is None and sr_wanted_df is None:
    st.info("👈 Load at least one dataset from the sidebar to get started.")
    st.stop()


# ----------------------------
# FILTERS  (3-column expander in main area)
# ----------------------------
filtered_rm = rm_df
filtered_sr_offer = sr_offer_df

with st.expander("🔍 Filters", expanded=True):
    fc0, fc1 = st.columns(2)

    # ── Column 0: Rightmove price + bedrooms ──────────────────────────────────
    with fc0:
        st.markdown("##### Rightmove")

        if rm_df is not None and "price_num" in rm_df.columns and rm_df["price_num"].notna().any():
            _prices = rm_df["price_num"].dropna()
            # Initialise from percentiles on first load to avoid outlier-dominated defaults
            if st.session_state["rm_price_min"] is None:
                st.session_state["rm_price_min"] = int(_prices.quantile(0.02))
            if st.session_state["rm_price_max"] is None:
                st.session_state["rm_price_max"] = int(_prices.quantile(0.98))
            _pc1, _pc2 = st.columns(2)
            with _pc1:
                st.number_input("Min price (£)", min_value=0, step=5000, key="rm_price_min")
            with _pc2:
                st.number_input("Max price (£)", min_value=0, step=5000, key="rm_price_max")
            _lo = float(st.session_state["rm_price_min"])
            _hi = float(st.session_state["rm_price_max"])
            if _hi < _lo:
                _lo, _hi = _hi, _lo
            filtered_rm = filtered_rm[filtered_rm["price_num"].between(_lo, _hi)]
            st.caption(f"Range: £{int(_prices.min()):,} – £{int(_prices.max()):,}")

        if rm_df is not None and "bedrooms_num" in rm_df.columns and rm_df["bedrooms_num"].notna().any():
            _beds = rm_df["bedrooms_num"].dropna()
            if st.session_state["rm_beds_min"] is None:
                st.session_state["rm_beds_min"] = int(_beds.quantile(0.02))
            if st.session_state["rm_beds_max"] is None:
                st.session_state["rm_beds_max"] = int(_beds.quantile(0.98))
            _bc1, _bc2 = st.columns(2)
            with _bc1:
                st.number_input("Min beds", min_value=0, step=1, key="rm_beds_min")
            with _bc2:
                st.number_input("Max beds", min_value=0, step=1, key="rm_beds_max")
            _lo = float(st.session_state["rm_beds_min"])
            _hi = float(st.session_state["rm_beds_max"])
            if _hi < _lo:
                _lo, _hi = _hi, _lo
            filtered_rm = filtered_rm[filtered_rm["bedrooms_num"].between(_lo, _hi)]
            st.caption(f"Range: {int(_beds.min())} – {int(_beds.max())}")

    # ── Column 1: Property type + auction/HMO flags ───────────────────────────
    with fc1:
        st.markdown("##### Type & flags")

        if rm_df is not None and "property_type" in rm_df.columns:
            _opts = sorted(
                rm_df["property_type"].astype("string").fillna("")
                .replace("nan", "").loc[lambda s: s.str.strip() != ""].unique().tolist()
            )
            if _opts:
                if not st.session_state["rm_property_types"]:
                    st.session_state["rm_property_types"] = _opts.copy()

                _pa, _pb = st.columns(2)
                if _pa.button("All", use_container_width=True, key="rm_pt_all"):
                    st.session_state["rm_property_types"] = _opts.copy()
                    st.rerun()
                if _pb.button("Clear", use_container_width=True, key="rm_pt_clear"):
                    st.session_state["rm_property_types"] = []
                    st.rerun()

                _sel_types = st.multiselect(
                    "Property types", options=_opts,
                    default=st.session_state["rm_property_types"],
                    label_visibility="collapsed",
                )
                st.session_state["rm_property_types"] = _sel_types
                if _sel_types and filtered_rm is not None:
                    filtered_rm = filtered_rm[
                        filtered_rm["property_type"].astype("string").isin(_sel_types)
                    ]

        if rm_df is not None:
            _fa, _fb = st.columns(2)
            if "potential_auction" in rm_df.columns:
                if _fa.checkbox("Auction only", key="rm_potential_auction"):
                    filtered_rm = filtered_rm[_to_bool(filtered_rm["potential_auction"])]
            if "potential_hmo" in rm_df.columns:
                if _fb.checkbox("HMO only", key="rm_potential_hmo"):
                    filtered_rm = filtered_rm[_to_bool(filtered_rm["potential_hmo"])]


# Drop rows without coordinates before passing to map
if filtered_rm is not None and {"lat_num", "lon_num"}.issubset(filtered_rm.columns):
    filtered_rm = filtered_rm.dropna(subset=["lat_num", "lon_num"])
if filtered_sr_offer is not None and {"lat_num", "lon_num"}.issubset(filtered_sr_offer.columns):
    filtered_sr_offer = filtered_sr_offer.dropna(subset=["lat_num", "lon_num"])


# ----------------------------
# MAP HEADER
# ----------------------------
_hc1, _hc2 = st.columns([4, 1])
rm_count = len(filtered_rm) if filtered_rm is not None else 0
sr_count = len(filtered_sr_offer) if filtered_sr_offer is not None else 0

with _hc1:
    st.caption(
        f"🔴 Rightmove **{rm_count:,}** matching &nbsp;|&nbsp; "
        f"🔵 SpareRoom Offered **{sr_count:,}** matching"
    )
with _hc2:
    max_points_each = st.select_slider(
        "Max points/layer",
        options=[500, 1000, 2000, 3000, 5000, 8000],
        value=3000,
        label_visibility="collapsed",
    )


# ----------------------------
# MAP
# ----------------------------
m = build_map(
    center=st.session_state.map_center,
    zoom=st.session_state.map_zoom,
    rm_df=filtered_rm,
    sr_offer_df=filtered_sr_offer,
    max_points_each=max_points_each,
)
_lat, _lon = st.session_state.map_center
_map_key = f"map_{_lat:.5f}_{_lon:.5f}_{st.session_state.map_zoom}"
st_folium(m, height=540, use_container_width=True, key=_map_key, returned_objects=[])

st.divider()


# ----------------------------
# TABLES
# ----------------------------
col1, col2 = st.columns([1.3, 1])

with col1:
    st.subheader("Rightmove listings")
    if rm_df is None:
        st.info("Load Rightmove data to see listings here.")
    else:
        _show = [
            c for c in
            ["address", "postcode", "price", "bedrooms", "property_type",
             "potential_auction", "potential_hmo", "url"]
            if c in (filtered_rm.columns if filtered_rm is not None else [])
        ]
        _table = (filtered_rm if filtered_rm is not None else pd.DataFrame()).reset_index(drop=True)
        _event = st.dataframe(
            _table[_show] if _show else _table,
            use_container_width=True,
            hide_index=True,
            selection_mode="single-row",
            on_select="rerun",
            key="rm_table",
            column_config={"url": st.column_config.LinkColumn("URL", display_text="Open")},
        )

        _sel_rows = []
        if isinstance(_event, dict):
            _sel_rows = _event.get("selection", {}).get("rows", [])
        else:
            _sv = st.session_state.get("rm_table")
            if isinstance(_sv, dict):
                _sel_rows = _sv.get("selection", {}).get("rows", [])

        if _sel_rows:
            _row = _table.iloc[_sel_rows[0]]
            if "lat_num" in _table.columns and pd.notna(_row.get("lat_num")):
                st.session_state.map_center = [float(_row["lat_num"]), float(_row["lon_num"])]
                st.session_state.map_zoom = DETAIL_ZOOM
            _outcode = None
            if "postcode" in _table.columns and pd.notna(_row.get("postcode")):
                _outcode = str(_row["postcode"]).strip().upper().split()[0]
            st.session_state.selected_outcode = _outcode
            st.success(
                f"**{_row.get('address', '(no address)')}** &nbsp;|&nbsp; Outcode: {_outcode or 'n/a'}"
            )

        _dl_df = _table[_show] if _show else _table
        st.download_button(
            label=f"⬇ Download ({len(_dl_df):,} rows)",
            data=_dl_df.to_csv(index=False).encode("utf-8"),
            file_name="rightmove_listings.csv",
            mime="text/csv",
            use_container_width=True,
        )

with col2:
    st.subheader("SpareRoom Wanted")
    if sr_wanted_df is None:
        st.info("Load SpareRoom Wanted data to see this table.")
    else:
        _wanted = sr_wanted_df
        _outcode = st.session_state.selected_outcode
        if _outcode and "outcode" in _wanted.columns:
            _wanted = _wanted[_wanted["outcode"] == _outcode]

        st.caption(
            f"{len(_wanted):,} rows"
            + (f" for outcode **{_outcode}**" if _outcode else " — select a row on the left to filter")
        )
        _wcols_pref = ["title", "postcode", "location", "budget", "price", "rent",
                       "bedrooms", "move_in", "url"]
        _wcols = [c for c in _wcols_pref if c in _wanted.columns] or list(_wanted.columns)[:10]
        st.dataframe(_wanted[_wcols], use_container_width=True, hide_index=True)
