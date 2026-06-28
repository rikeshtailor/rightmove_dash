"""Ingest England-wide HMO Article 4 directions from planning.data.gov.uk.

Downloads the national `article-4-direction-area` dataset, keeps the HMO
(C3 -> C4) directions, fixes geometry, optionally drops expired directions, and
writes the result to a GeoPackage (and optionally a PostGIS table). It then
prints a coverage report -- how many HMO areas, from how many councils, and the
per-council breakdown -- so you can see exactly what "all councils" amounts to
today and where the gaps are.

Usage:
    python ingest.py                      # -> data/england_hmo_article4.gpkg
    python ingest.py --keep-expired       # include directions with a past end-date
    python ingest.py --postgis "postgresql://user:pw@localhost/planning"

Data source: planning.data.gov.uk -- (c) Crown copyright and database right,
released under the Open Government Licence v3.0. Attribute it in anything you ship.
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, datetime
from pathlib import Path

import requests

from hmo_filter import is_hmo_article4, looks_non_hmo

GEOJSON_URL = "https://files.planning.data.gov.uk/dataset/article-4-direction-area.geojson"
DEFAULT_GPKG = Path("data/england_hmo_article4.gpkg")
DEFAULT_GEOJSON_OUT = Path("data/england_hmo_article4.geojson")
LAYER = "hmo_article4"


def download(url: str, dest: Path) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    print(f"Downloading {url}", file=sys.stderr)
    with requests.get(url, stream=True, timeout=600) as r:
        r.raise_for_status()
        with open(dest, "wb") as fh:
            for chunk in r.iter_content(chunk_size=1 << 20):
                fh.write(chunk)
    print(f"  saved {dest} ({dest.stat().st_size / 1e6:.1f} MB)", file=sys.stderr)
    return dest


def _parse_date(value):
    if value in (None, "", "nan"):
        return None
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            return datetime.strptime(str(value)[:19], fmt).date()
        except ValueError:
            continue
    return None


def in_force(end_value, today: date) -> bool:
    end = _parse_date(end_value)
    return end is None or end >= today


def run(geojson_path: Path, out_gpkg: Path, keep_expired: bool, postgis: str | None,
        geojson_out: Path | None = None, simplify: float | None = None):
    # Heavy GIS imports kept local so the rest of the toolkit (and selftest.py)
    # doesn't require GDAL just to import this module.
    import geopandas as gpd
    from shapely import make_valid

    print(f"Reading {geojson_path}", file=sys.stderr)
    gdf = gpd.read_file(geojson_path)
    if gdf.crs is None:
        gdf.set_crs(4326, inplace=True)
    elif gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(4326)

    total = len(gdf)

    # --- filter to HMO directions -------------------------------------------
    records = gdf.drop(columns="geometry").to_dict("records")
    hmo_mask = [is_hmo_article4(rec) for rec in records]
    gdf_hmo = gdf[hmo_mask].copy()

    # --- drop expired (optional) --------------------------------------------
    today = date.today()
    end_col = "end-date" if "end-date" in gdf_hmo.columns else "end_date"
    if not keep_expired and end_col in gdf_hmo.columns:
        live = gdf_hmo[end_col].apply(lambda v: in_force(v, today))
        dropped = int((~live).sum())
        gdf_hmo = gdf_hmo[live].copy()
        if dropped:
            print(f"  dropped {dropped} expired HMO direction(s)", file=sys.stderr)

    # --- repair geometry -----------------------------------------------------
    def fix(geom):
        if geom is None:
            return None
        return geom if geom.is_valid else make_valid(geom)

    gdf_hmo["geometry"] = gdf_hmo.geometry.apply(fix)
    gdf_hmo = gdf_hmo[~gdf_hmo.geometry.is_empty & gdf_hmo.geometry.notna()].copy()

    # --- write ---------------------------------------------------------------
    out_gpkg.parent.mkdir(parents=True, exist_ok=True)
    gdf_hmo.to_file(out_gpkg, layer=LAYER, driver="GPKG")
    print(f"Wrote {len(gdf_hmo)} HMO areas -> {out_gpkg} (layer '{LAYER}')")

    # GeoJSON copy for lightweight, geopandas-free consumers (e.g. a folium map).
    if geojson_out is not None:
        export = gdf_hmo
        if simplify:
            export = gdf_hmo.copy()
            export["geometry"] = export.geometry.simplify(simplify, preserve_topology=True)
        geojson_out.parent.mkdir(parents=True, exist_ok=True)
        # overwrite cleanly; GeoJSON driver appends if the file already exists
        if geojson_out.exists():
            geojson_out.unlink()
        export.to_file(geojson_out, driver="GeoJSON")
        print(f"Wrote {len(export)} HMO areas -> {geojson_out}")

    if postgis:
        from sqlalchemy import create_engine

        engine = create_engine(postgis)
        gdf_hmo.to_postgis(LAYER, engine, if_exists="replace", index=False)
        print(f"Wrote {len(gdf_hmo)} HMO areas -> PostGIS table '{LAYER}'")

    _coverage_report(gdf_hmo, total, out_gpkg.parent / "coverage.csv")


def _coverage_report(gdf_hmo, total_areas: int, csv_path: Path):
    org_col = "organisation" if "organisation" in gdf_hmo.columns else None
    print("\n=== COVERAGE REPORT =====================================")
    print(f"Article 4 areas in national dataset : {total_areas}")
    print(f"Classified as HMO (C3->C4)          : {len(gdf_hmo)}")
    if org_col:
        by_org = gdf_hmo[org_col].fillna("(unknown)").value_counts()
        print(f"Councils (data providers) with HMO  : {by_org.shape[0]}")
        print("\nTop councils by HMO area count:")
        for org, n in by_org.head(20).items():
            print(f"  {n:>4}  {org}")
        by_org.rename("hmo_area_count").to_csv(csv_path, header=True)
        print(f"\nFull per-council breakdown written to {csv_path}")
    # honest gap note
    print(
        "\nNOTE: England has ~317 local planning authorities. The platform warns it\n"
        "does not yet cover all of England, so this is maximal *automated* coverage,\n"
        "not every council. Councils absent here publish their boundaries only as\n"
        "PDFs/portals -- add them as a second GeoPackage layer to close the gap."
    )
    # flag borderline records to audit the filter
    borderline = sum(looks_non_hmo(r) for r in gdf_hmo.drop(columns="geometry").to_dict("records"))
    if borderline:
        print(
            f"\nAUDIT: {borderline} kept area(s) contain non-HMO wording too -- worth a\n"
            "spot-check that the filter hasn't over-matched. See hmo_filter.py."
        )
    print("=========================================================")


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--source-geojson", type=Path, default=Path("data/article-4-direction-area.geojson"),
                    help="Path to download/read the national source GeoJSON.")
    ap.add_argument("--out", type=Path, default=DEFAULT_GPKG, help="Output GeoPackage path.")
    ap.add_argument("--geojson-out", type=Path, default=DEFAULT_GEOJSON_OUT,
                    help="Output GeoJSON path for the HMO areas (for the dashboard map). "
                         "Pass 'none' to skip.")
    ap.add_argument("--simplify", type=float, default=None,
                    help="Optional geometry simplification tolerance in degrees (e.g. 0.0001 "
                         "~= 11m) to lighten the GeoJSON for faster map rendering.")
    ap.add_argument("--keep-expired", action="store_true", help="Keep directions with a past end-date.")
    ap.add_argument("--postgis", default=None, help="Optional SQLAlchemy URL to also load PostGIS.")
    ap.add_argument("--no-download", action="store_true", help="Reuse an existing local source GeoJSON.")
    args = ap.parse_args()

    if not args.no_download or not args.source_geojson.exists():
        download(GEOJSON_URL, args.source_geojson)

    geojson_out = None if str(args.geojson_out).lower() == "none" else args.geojson_out
    run(args.source_geojson, args.out, args.keep_expired, args.postgis,
        geojson_out=geojson_out, simplify=args.simplify)


if __name__ == "__main__":
    main()
