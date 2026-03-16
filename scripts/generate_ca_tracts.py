"""
generate_ca_tracts.py
---------------------
Downloads the 2010 Census TIGER cartographic boundary file for California
Census tracts, simplifies geometries, and writes a GeoJSON file to
dashboard/src/data/ca-tracts.json for use by the Observable Framework dashboard.

Requires: geopandas, requests
Run with: /path/to/python3 scripts/generate_ca_tracts.py

Uses the prop13_paper venv if available:
  /Users/ryanvaughn/Projects/prop13_paper/.venv/bin/python3 scripts/generate_ca_tracts.py
"""

import sys
import json
import zipfile
import io
from pathlib import Path

try:
    import requests
    import geopandas as gpd
except ImportError:
    print("ERROR: Install geopandas and requests. Try the prop13_paper venv:")
    print("  /Users/ryanvaughn/Projects/prop13_paper/.venv/bin/python3 scripts/generate_ca_tracts.py")
    sys.exit(1)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_PATH = PROJECT_ROOT / "dashboard" / "src" / "data" / "ca-tracts.json"

# Census 2010 500k cartographic boundary for California (FIPS 06) tracts
# genz2010_06_tract_500k.zip → gz_2010_06_140_00_500k.shp
CENSUS_URL = "https://www2.census.gov/geo/tiger/GENZ2010/gz_2010_06_140_00_500k.zip"


def main():
    print(f"Downloading Census TIGER 2010 CA tract boundaries...")
    resp = requests.get(CENSUS_URL, timeout=120)
    resp.raise_for_status()
    print(f"  Downloaded {len(resp.content) / 1024:.0f} KB")

    # Read shapefile from ZIP into geopandas
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        shp_name = next(n for n in zf.namelist() if n.endswith(".shp"))
        print(f"  Reading {shp_name}")
        with zf.open(shp_name) as f:
            # Write all ZIP members to a temp dir so fiona can read
            import tempfile, os
            with tempfile.TemporaryDirectory() as tmpdir:
                zf.extractall(tmpdir)
                shp_path = os.path.join(tmpdir, shp_name)
                gdf = gpd.read_file(shp_path)

    print(f"  Loaded {len(gdf)} features")
    print(f"  Columns: {list(gdf.columns)}")

    # Normalize GEOID: the 2010 cartographic boundary uses GEO_ID like
    # "1400000US06001400100" or GEOID10 = "06001400100" (11 digits).
    # We want the 11-digit form that matches our parquet files.
    if "GEOID10" in gdf.columns:
        gdf["geoid"] = gdf["GEOID10"].str.zfill(11)
    elif "GEO_ID" in gdf.columns:
        # Strip prefix "1400000US"
        gdf["geoid"] = gdf["GEO_ID"].str.replace("1400000US", "").str.zfill(11)
    else:
        raise ValueError(f"No GEOID column found. Available: {list(gdf.columns)}")

    # Keep only the geoid property — drop everything else
    gdf = gdf[["geoid", "geometry"]].copy()

    # Ensure WGS84 (EPSG:4326)
    if gdf.crs is None or gdf.crs.to_epsg() != 4326:
        print(f"  Reprojecting from {gdf.crs} to WGS84")
        gdf = gdf.to_crs(epsg=4326)

    # Simplify geometries for web display (tolerance in degrees ≈ 0.001° ≈ 100m)
    print("  Simplifying geometries (tolerance = 0.001°)...")
    gdf["geometry"] = gdf["geometry"].simplify(0.001, preserve_topology=True)

    # Drop invalid/empty geometries
    n_before = len(gdf)
    gdf = gdf[gdf.geometry.is_valid & ~gdf.geometry.is_empty].copy()
    if len(gdf) < n_before:
        print(f"  Dropped {n_before - len(gdf)} invalid/empty geometries")

    print(f"  Writing {len(gdf)} features to {OUTPUT_PATH.relative_to(PROJECT_ROOT)}")
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_file(OUTPUT_PATH, driver="GeoJSON")

    # Report file size
    size_kb = OUTPUT_PATH.stat().st_size / 1024
    print(f"  Output size: {size_kb:.0f} KB")
    if size_kb > 8000:
        print("  WARNING: File > 8 MB — consider increasing simplification tolerance")

    print("Done.")


if __name__ == "__main__":
    main()
