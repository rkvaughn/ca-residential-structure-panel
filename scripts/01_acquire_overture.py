"""
01_acquire_overture.py
======================
Download Overture Maps residential building footprints for California and
spatially join them to 2010 Census tracts to produce a tract-level residential
structure count anchored to 2024 (the most recent Overture release).

This count is used as the 2024 anchor for the hind-cast in 04_build_structure_panel.py.

Pipeline
--------
1. Install overturemaps CLI if not present (pip install overturemaps).
2. Download CA building footprints via overturemaps CLI → geoparquet.
3. Filter to subtype == "residential"; report null subtype rate.
4. Reproject to EPSG:3310 (CA Albers) and replace geometry with centroids.
5. Spatial join (point-in-polygon) to 2010 Census tract polygons.
6. Count residential buildings per tract; assign 0 for tracts with no match.
7. Save tract-level counts to data/clean/tract_residential_counts_2024.parquet.

Distributional assumption
--------------------------
The within-county proportional share of residential structures belonging to
each tract is assumed stable over time. This assumption is most defensible in
rural, low-construction tracts where new development is slow. Conservative bias
from fire demolitions (Overture 2024 undercounts pre-fire structures in
wildfire-affected counties) is documented in the paper.

Inputs
------
  data/raw/shapefiles/tl_2010_06_tract10/tl_2010_06_tract10.shp
    2010 TIGER tract polygons — downloaded automatically on first run.

Outputs
-------
  data/raw/overture/ca_buildings.geoparquet   (large; gitignored)
  data/clean/tract_residential_counts_2024.parquet
    Columns: geoid, county_FIPS, overture_residential_count_2024,
             overture_null_subtype_share

Usage
-----
  python scripts/01_acquire_overture.py

Dependencies
------------
  overturemaps  (installed automatically if absent)
  geopandas, pandas, pyarrow
"""

import subprocess
import sys
from pathlib import Path

import geopandas as gpd
import pandas as pd

# ---------------------------------------------------------------------------
# Paths and constants
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_OVERTURE  = PROJECT_ROOT / "data" / "raw" / "overture"
RAW_SHAPES    = PROJECT_ROOT / "data" / "raw" / "shapefiles"
CLEAN_DIR     = PROJECT_ROOT / "data" / "clean"

RAW_OVERTURE.mkdir(parents=True, exist_ok=True)
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

CRS_CA = "EPSG:3310"   # CA Albers Equal Area — used for all spatial operations

# California bounding box (lon_min, lat_min, lon_max, lat_max)
CA_BBOX = "-124.48,32.53,-114.13,42.01"

TIGER_SHP = RAW_SHAPES / "tl_2010_06_tract10" / "tl_2010_06_tract10.shp"

OUT_RAW   = RAW_OVERTURE / "ca_buildings.geoparquet"
OUT_CLEAN = CLEAN_DIR / "tract_residential_counts_2024.parquet"

# Null subtype warning threshold
NULL_RATE_WARN = 0.20  # pre-specified: flag if > 20% of buildings have no subtype


# ---------------------------------------------------------------------------
# Step 0: Ensure overturemaps is installed
# ---------------------------------------------------------------------------

def ensure_overturemaps() -> Path:
    """
    Check that the overturemaps CLI is available. Install via pip if missing.

    Returns the path to the overturemaps executable in the current Python
    environment's bin directory.
    """
    cli_path = Path(sys.executable).parent / "overturemaps"
    if cli_path.exists():
        print(f"  [ok] overturemaps CLI found: {cli_path}")
        return cli_path

    # Try importing as a module check
    try:
        import importlib
        importlib.import_module("overturemaps")
        # Module present but CLI not in path — use module invocation
        print("  [ok] overturemaps module found (will invoke via -m)")
        return None  # signals use of -m invocation
    except ImportError:
        pass

    print("  [install] overturemaps not found — installing via pip...")
    subprocess.run(
        [sys.executable, "-m", "pip", "install", "overturemaps"],
        check=True,
    )
    print("  [ok] overturemaps installed")

    cli_path = Path(sys.executable).parent / "overturemaps"
    return cli_path if cli_path.exists() else None


# ---------------------------------------------------------------------------
# Step 1: Download CA buildings from Overture Maps
# ---------------------------------------------------------------------------

def download_overture_buildings(out_path: Path, cli_path) -> Path:
    """
    Download California building footprints from Overture Maps as geoparquet.

    Skip if the output file already exists (skip-if-exists pattern).
    The download covers all building types; residential filtering happens
    after download.

    Parameters
    ----------
    out_path : Path
        Destination for the raw geoparquet file.
    cli_path : Path or None
        Path to the overturemaps CLI binary. If None, invokes via python -m.

    Returns
    -------
    Path
        out_path (the saved file).
    """
    if out_path.exists():
        size_mb = out_path.stat().st_size / (1 << 20)
        print(f"  [skip] {out_path.name} already exists ({size_mb:.0f} MB)")
        return out_path

    print(f"  [download] Downloading CA buildings from Overture Maps")
    print(f"  Bounding box: {CA_BBOX}")
    print(f"  Output: {out_path}")
    print(f"  NOTE: This file may be several GB. Download may take 10-30+ minutes.")

    if cli_path is not None:
        cmd = [str(cli_path)]
    else:
        cmd = [sys.executable, "-m", "overturemaps"]

    cmd += [
        "download",
        f"--bbox={CA_BBOX}",
        "-f", "geoparquet",
        "--type=building",
        "-o", str(out_path),
    ]

    print(f"  Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)

    if not out_path.exists():
        raise FileNotFoundError(
            f"overturemaps download completed but output not found at {out_path}. "
            "Check CLI output for errors."
        )

    size_mb = out_path.stat().st_size / (1 << 20)
    print(f"  [ok] Download complete — {size_mb:.0f} MB → {out_path}")
    return out_path


# ---------------------------------------------------------------------------
# Step 2: Filter to residential buildings
# ---------------------------------------------------------------------------

def filter_residential(geoparquet_path: Path):
    """
    Load Overture buildings geoparquet, report subtype null rate, and
    filter to residential buildings.

    Parameters
    ----------
    geoparquet_path : Path
        Path to the raw ca_buildings.geoparquet file.

    Returns
    -------
    tuple[gpd.GeoDataFrame, float]
        (residential_gdf, global_null_subtype_share)
    """
    print(f"\n  Loading buildings from {geoparquet_path.name} ...")
    gdf = gpd.read_parquet(geoparquet_path)
    n_total = len(gdf)
    print(f"  Total buildings loaded: {n_total:,}  CRS: {gdf.crs}")

    # Identify the subtype column — Overture may use 'subtype' or 'class'
    subtype_col = None
    for candidate in ("subtype", "class", "primary_use"):
        if candidate in gdf.columns:
            subtype_col = candidate
            break

    if subtype_col is None:
        print(f"  [WARN] No subtype/class column found. Available: {list(gdf.columns)}")
        print(f"  Cannot filter to residential — using all buildings as proxy.")
        null_share = 1.0  # treat everything as unclassified
        return gdf, null_share

    # Null rate (global diagnostic)
    n_null = gdf[subtype_col].isna().sum()
    null_share = n_null / n_total if n_total > 0 else 0.0
    print(f"  Subtype column: '{subtype_col}'")
    print(f"  Null subtype: {n_null:,} / {n_total:,} ({null_share:.1%})")

    if null_share > NULL_RATE_WARN:
        print(
            f"  [WARN] > {NULL_RATE_WARN:.0%} null subtypes. The residential building "
            f"count will undercount structures whose subtype is unclassified. "
            f"Flag this limitation in the paper (affects pre-period denominator)."
        )

    # Distribution of non-null subtypes
    value_counts = gdf[subtype_col].dropna().value_counts().head(10)
    print(f"  Subtype distribution (top 10):\n{value_counts.to_string()}")

    # Filter to residential
    res = gdf[gdf[subtype_col] == "residential"].copy()
    n_res = len(res)
    print(f"\n  Residential buildings: {n_res:,} ({n_res/n_total:.1%} of total)")

    if n_res == 0:
        raise ValueError(
            "No residential buildings found after filtering. "
            f"Check that subtype='{subtype_col}' contains 'residential' entries."
        )

    return res, null_share


# ---------------------------------------------------------------------------
# Step 3: Load 2010 TIGER tract polygons
# ---------------------------------------------------------------------------

def load_tiger_tracts() -> gpd.GeoDataFrame:
    """
    Load 2010 TIGER/Line CA tract polygons and normalise the GEOID column.

    Returns GeoDataFrame in EPSG:4326 with columns: geoid, geometry.
    """
    if not TIGER_SHP.exists():
        raise FileNotFoundError(
            f"2010 TIGER shapefile not found at {TIGER_SHP}. "
            "Download from https://www2.census.gov/geo/tiger/TIGER2010/TRACT/2010/ "
            "(tl_2010_06_tract10.zip) and extract to data/raw/shapefiles/."
        )

    tracts = gpd.read_file(TIGER_SHP)
    print(f"\n  2010 TIGER tracts loaded: {len(tracts):,} rows  CRS: {tracts.crs}")

    # Normalise GEOID column (TIGER 2010 uses GEOID10)
    geoid_col = next((c for c in tracts.columns if "GEOID" in c.upper()), None)
    if geoid_col is None:
        raise KeyError(f"No GEOID column in TIGER shapefile. Columns: {list(tracts.columns)}")

    tracts = tracts[[geoid_col, "geometry"]].rename(columns={geoid_col: "geoid"})
    # Confirm CA-only (state FIPS 06)
    tracts = tracts[tracts["geoid"].str.startswith("06")].copy()
    print(f"  CA tracts: {len(tracts):,}")
    return tracts


# ---------------------------------------------------------------------------
# Step 4: Spatial join — building centroids to tract polygons
# ---------------------------------------------------------------------------

def count_buildings_per_tract(
    res_gdf: gpd.GeoDataFrame,
    tracts_gdf: gpd.GeoDataFrame,
    null_share: float,
) -> pd.DataFrame:
    """
    Spatially join residential building centroids to 2010 tract polygons and
    count buildings per tract.

    Both layers are reprojected to EPSG:3310 (CA Albers) before joining.
    Building polygon geometry is replaced with centroids for point-in-polygon
    matching; this handles irregular building footprints near tract boundaries.

    Parameters
    ----------
    res_gdf : GeoDataFrame
        Residential buildings (any CRS).
    tracts_gdf : GeoDataFrame
        2010 CA tract polygons (any CRS).
    null_share : float
        Global null subtype share — stored as a constant column in output.

    Returns
    -------
    pd.DataFrame
        One row per tract with columns:
        geoid, county_FIPS, overture_residential_count_2024,
        overture_null_subtype_share
    """
    print("\n  Reprojecting to EPSG:3310 (CA Albers)...")
    tracts_alb = tracts_gdf.to_crs(CRS_CA)
    res_alb    = res_gdf.to_crs(CRS_CA)

    # Replace footprint polygons with centroids for point-in-polygon join
    print("  Computing building centroids...")
    res_pts = res_alb.copy()
    res_pts["geometry"] = res_alb.geometry.centroid

    print(f"  Spatial join: {len(res_pts):,} building centroids → {len(tracts_alb):,} tracts ...")
    print("  (This may take several minutes for large building datasets)")

    joined = gpd.sjoin(
        res_pts[["geometry"]],
        tracts_alb[["geoid", "geometry"]],
        how="left",
        predicate="within",
    )

    # Count buildings per tract
    matched   = joined[joined["geoid"].notna()]
    unmatched = joined[joined["geoid"].isna()]
    n_matched   = len(matched)
    n_unmatched = len(unmatched)
    print(f"  Matched: {n_matched:,} buildings  |  Unmatched (outside CA tracts): {n_unmatched:,}")
    if n_unmatched / len(joined) > 0.05:
        print(f"  [WARN] >{n_unmatched/len(joined):.1%} buildings unmatched — check bounding box alignment")

    building_counts = (
        matched.groupby("geoid").size().reset_index(name="overture_residential_count_2024")
    )

    # Merge onto full tract list so that tracts with zero buildings get 0 (not NaN)
    all_geoids = tracts_gdf[["geoid"]].copy()
    result = all_geoids.merge(building_counts, on="geoid", how="left")
    result["overture_residential_count_2024"] = (
        result["overture_residential_count_2024"].fillna(0).astype(int)
    )

    n_zero = (result["overture_residential_count_2024"] == 0).sum()
    pct_zero = n_zero / len(result)
    print(f"\n  Tracts with zero residential buildings: {n_zero:,} ({pct_zero:.1%})")
    if pct_zero > 0.05:
        print(
            f"  [NOTE] {pct_zero:.1%} of tracts have zero Overture count. These tracts "
            f"will receive county-proportional uniform imputation in build_structure_panel.py."
        )

    # Add derived columns
    result["county_FIPS"] = result["geoid"].str[:5]
    result["overture_null_subtype_share"] = round(null_share, 4)

    print(f"\n  Building count distribution:")
    print(result["overture_residential_count_2024"].describe().to_string())

    return result[["geoid", "county_FIPS", "overture_residential_count_2024",
                   "overture_null_subtype_share"]]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 65)
    print("acquire_overture_buildings.py — Overture 2024 residential anchor")
    print("=" * 65)

    if OUT_CLEAN.exists():
        print(f"\n[skip] {OUT_CLEAN.name} already exists. Delete to rebuild.")
        return

    # ── 0. Ensure overturemaps CLI is available ───────────────────────────────
    print("\n--- Step 0: Check overturemaps installation ---")
    cli_path = ensure_overturemaps()

    # ── 1. Download buildings ─────────────────────────────────────────────────
    print("\n--- Step 1: Download CA buildings from Overture Maps ---")
    download_overture_buildings(OUT_RAW, cli_path)

    # ── 2. Filter to residential ──────────────────────────────────────────────
    print("\n--- Step 2: Filter to residential buildings ---")
    res_gdf, null_share = filter_residential(OUT_RAW)

    # ── 3. Load 2010 TIGER tracts ─────────────────────────────────────────────
    print("\n--- Step 3: Load 2010 TIGER tract polygons ---")
    tracts_gdf = load_tiger_tracts()

    # ── 4. Spatial join and count ─────────────────────────────────────────────
    print("\n--- Step 4: Count buildings per tract (centroid join) ---")
    tract_counts = count_buildings_per_tract(res_gdf, tracts_gdf, null_share)

    # ── 5. Save ───────────────────────────────────────────────────────────────
    print("\n--- Step 5: Save output ---")
    tract_counts.to_parquet(OUT_CLEAN, index=False)
    print(f"[saved] {OUT_CLEAN.name}")
    print(f"        {len(tract_counts):,} tracts × {len(tract_counts.columns)} columns")
    print(f"        county_FIPS values: {tract_counts['county_FIPS'].nunique()} unique counties")
    print(
        f"        Count range: "
        f"min={tract_counts['overture_residential_count_2024'].min()} "
        f"median={tract_counts['overture_residential_count_2024'].median():.0f} "
        f"max={tract_counts['overture_residential_count_2024'].max()}"
    )

    print("\n" + "=" * 65)
    print("Done. Run 02_acquire_bps.py next.")
    print("=" * 65)


if __name__ == "__main__":
    main()
