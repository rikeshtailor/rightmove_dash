"""Add the ingested HMO Article 4 areas to the dashboard's folium map.

Deliberately dependency-free beyond folium + stdlib json, so it doesn't drag
geopandas into the Streamlit app. It consumes the GeoJSON that `ingest.py`
writes (data/england_hmo_article4.geojson) and renders it as a single,
toggleable polygon layer that sits *beneath* the property markers.
"""
from __future__ import annotations

import json
from pathlib import Path

import folium

HMO_FILL = "#7b1fa2"   # purple — reads as a zone under the red/blue property dots
HMO_LINE = "#4a148c"
LAYER_NAME = "HMO Article 4 areas"

# Tooltip fields, in preference order; only those present are shown.
_TOOLTIP_FIELDS = ["organisation", "name", "reference", "start-date"]


def load_hmo_geojson(path) -> dict | None:
    """Read the HMO GeoJSON from disk. Returns None if missing or empty."""
    p = Path(path)
    if not p.exists():
        return None
    try:
        with open(p, "r", encoding="utf-8") as fh:
            gj = json.load(fh)
    except (json.JSONDecodeError, OSError):
        return None
    return gj if gj.get("features") else None


def hmo_feature_count(gj: dict | None) -> int:
    return len(gj.get("features", [])) if gj else 0


def add_hmo_layer(m: folium.Map, gj: dict | None, show: bool = True,
                  name: str = LAYER_NAME) -> None:
    """Add the HMO polygons to map `m` as a toggleable FeatureGroup.

    Call this BEFORE adding the property-marker layers so the polygons render
    underneath them. No-op if `gj` is None/empty.
    """
    if not gj or not gj.get("features"):
        return

    props0 = gj["features"][0].get("properties", {}) or {}
    fields = [f for f in _TOOLTIP_FIELDS if f in props0]
    aliases = [("Council" if f == "organisation" else f.replace("-", " ").title())
               for f in fields]

    fg = folium.FeatureGroup(name=name, show=show)
    gj_layer = folium.GeoJson(
        gj,
        name=name,
        style_function=lambda _f: {
            "fillColor": HMO_FILL,
            "color": HMO_LINE,
            "weight": 1,
            "fillOpacity": 0.18,
        },
        highlight_function=lambda _f: {"weight": 2, "fillOpacity": 0.30},
    )
    if fields:
        folium.GeoJsonTooltip(fields=fields, aliases=aliases, sticky=True).add_to(gj_layer)
    gj_layer.add_to(fg)
    fg.add_to(m)
