"""
05_bootstrap_structure_panel.py
================================
Build a bootstrapped distribution over the true residential housing stock per
California tract × year, addressing the 69% Overture null subtype rate.

Method
------
Step 0 — County building statistics (cached to data/temp/)
  Spatial join of all 15.6M Overture buildings to county polygons (TIGER tract
  dissolve) to compute per-county:
    R_c = count of labeled-residential buildings
    N_c = count of null-subtype buildings
    L_c = count of labeled non-residential buildings
    r_frac_c = R_c / (R_c + L_c)  [labeled residential fraction — data-derived]
  Cached to data/temp/county_building_stats.parquet (skip-if-exists).

Step 0a — ACS external absorption calibration
  f_c_external = (ACS_units_c − R_c) / max(N_c, 1)
  Problem: ACS counts *housing units*; Overture counts *structures*. In dense
  urban counties (≥15 CA counties), ACS_units >> structures → f_c clips to 0.99.

Step 0b — Arruda hybrid override (for ACS-clipped counties)
  For counties where Step 0a clips to MU_CLIP_HI (unit/structure mismatch),
  replace f_c_external with Arruda-derived absorption fraction:
    f_c_arruda = max(0, Arruda_RES_c − R_c) / max(N_c, 1)
  Arruda et al. (2024) classify OSM buildings as RES/NON_RES — same unit of
  measurement as Overture (buildings, not housing units), eliminating the
  unit/structure mismatch. OSM undercoverage in rural/suburban areas means
  Arruda is less reliable there; ACS is retained for non-clipping counties.

  Requires: output/tables/arruda_ca_county_counts.parquet (from script 07).
  If file is absent, ACS calibration is kept for all counties with a warning.

  Validation: counties where Arruda_RES_c < R_c yield negative f_c_arruda.
  These are flagged and fall back to the endogenous r_frac_c.

Step 1 — Per-county Beta calibration (Phase 1 + Phase 2 check)
  For each county c, calibrate Beta(α_c, β_c) over the null-subtype absorption
  fraction f_c (= share of null buildings that are truly residential).

  Initial condition:  Beta(2, 5)  [PI-confirmed 2026-03-01]
    mean = 2/7 ≈ 28.6%,  P80 ≈ 48%,  P95 ≈ 64%
    Justification: most null-subtype buildings are garages, ADUs, sheds, and
    storage structures; a smaller fraction are unclassified multi-family
    residential. Beta(2,5) is right-skewed, consistent with "most nulls are
    non-residential."

  Calibration target per county (external ACS or endogenous fallback):
    T_target_c = R_c + f_target_c × N_c
  where f_target_c is the ACS-external absorption fraction (Step 0a) when
  available — f_c_external = (ACS_units_c − R_c) / max(N_c, 1) — or the
  endogenous r_frac_c = R_c / (R_c + L_c) from labeled Overture buildings
  when ACS is unavailable or undercounts the labeled residential stock.

  Forward mean at Beta mean μ = α/(α+β):
    forward_mean_c = R_c + μ × N_c

  Phase 1 (calibrate μ via bisection toward f_target_c):
    Concentration κ = α+β = 7 held fixed. Iterate until convergence:
      |forward_mean_c − T_target_c| / T_target_c < 1% (CALIB_TOL)
    Overshoot  (forward_mean > T_target_c): reduce μ toward r_frac_c.
    Undershoot > 10% (UNDERSHOOT_THR): increase μ toward r_frac_c.
    Each iteration adjusts both α_c = μ×κ and β_c = (1−μ)×κ.

  Phase 2 (P95 check vs hard upper bound T_c = R_c + N_c):
    P95 of Beta(α_c, β_c) × N_c + R_c vs T_c.
    Satisfied by construction (Beta ∈ [0,1]); result logged for transparency.

Step 2 — Vectorised bootstrap (B = 500 per county)
  For each county c, draw B samples from calibrated Beta(α_c, β_c):
    f_c[b] ~ Beta(α_c, β_c)
    A_c[b] = R_c + f_c[b] × N_c          [bootstrapped 2024 anchor]
    For each year t:
      net_after_t = Σ_{s=t+1..2024}(structures_permitted_s − dins_destroyed_s)
      hind_c_t[b] = max(1, A_c[b] − net_after_t)
      Apply noise:  ε_t ~ N(0, 0.005 × hind_c_t[b])   [0.5% CV — residual non-fire demolitions]
      hind_c_t[b] = max(1, hind_c_t[b] + ε_t)
      Truncate:     hind_c_t[b] = min(hind_c_t[b], max(1, T_c − net_after_t))
  DINS wildfire demolitions are subtracted directly in net_after_t, replacing the
  previous 1% CV noise proxy. The residual 0.5% CV covers non-fire demolitions
  (code violations, age-related teardowns) that DINS does not capture.
  Compute county-level P5, P50, P95, IQR across B iterations per year.

Step 3 — Downscale to tract × year
  bootstrap_count_it[q] = p{q}_county_ct × tract_share_i
  (same tract_share as tract_structure_panel.parquet)

Outputs
-------
  data/clean/tract_structure_panel_bootstrap.parquet
    geoid, county_FIPS, year,
    p5_residential_count, p50_residential_count,
    p95_residential_count, iqr_residential_count,
    alpha_c, beta_c

  data/clean/external_absorption_fractions.parquet
    county_FIPS, acs_units, f_c_external, calibration_source, diff_from_rfrac

  output/tables/bootstrap_calibration_log.csv
    county_FIPS, phase, iteration, alpha_old, beta_old, alpha_new, beta_new,
    forward_mean, T_target_c, T_c, N_c, R_c, r_frac,
    f_target, calib_target_source, bias_pct, adjustment_direction, converged

Usage
-----
  python scripts/05_bootstrap_structure_panel.py

Dependencies
------------
  geopandas, pandas, numpy, scipy, pyarrow
  (scipy available via statsmodels dependency in .venv)
"""

import sys
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from scipy import stats

sys.path.insert(0, str(Path(__file__).parent / "utils"))
from census_api import fetch_acs_batch

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PROJECT_ROOT  = Path(__file__).resolve().parents[1]
RAW_OVERTURE  = PROJECT_ROOT / "data" / "raw" / "overture"
CLEAN_DIR     = PROJECT_ROOT / "data" / "clean"
TEMP_DIR      = PROJECT_ROOT / "data" / "temp"
OUT_TABLES    = PROJECT_ROOT / "output" / "tables"

IN_RAW_BUILDINGS = RAW_OVERTURE / "ca_buildings.geoparquet"
IN_TRACT_PANEL   = CLEAN_DIR / "tract_structure_panel.parquet"
IN_BPS_PERMITS   = CLEAN_DIR / "county_permits_ca_2010_2024.parquet"
IN_DINS          = CLEAN_DIR / "dins_county_destroyed_residential.parquet"
TIGER_SHP        = (
    PROJECT_ROOT / "data" / "raw" / "shapefiles"
    / "tl_2010_06_tract10" / "tl_2010_06_tract10.shp"
)

CACHE_COUNTY_STATS    = TEMP_DIR / "county_building_stats.parquet"
OUT_BOOTSTRAP         = CLEAN_DIR / "tract_structure_panel_bootstrap.parquet"
OUT_CALIB_LOG         = OUT_TABLES / "bootstrap_calibration_log.csv"
OUT_EXT_ABSORPTION    = CLEAN_DIR / "external_absorption_fractions.parquet"

# Arruda county counts written by script 07 — used for hybrid calibration (Step 0b)
IN_ARRUDA_COUNTY      = OUT_TABLES / "arruda_ca_county_counts.parquet"

# ACS vintage for external calibration. Using 2022 5-year (2018-2022) — closest
# available to the 2024 Overture anchor that is well-established.
ACS_HOUSING_YEAR = 2022

# ---------------------------------------------------------------------------
# Bootstrap parameters — initial condition confirmed by PI 2026-03-01
# ---------------------------------------------------------------------------

# Beta(2,5) prior: mean ≈ 28.6%, P80 ≈ 48%, P95 ≈ 64%.
# Justification in CLAUDE.md Issue 3. Not derived from project data;
# confirmed by PI before running.
INITIAL_ALPHA  = 2.0   # calibration confirmed by PI 2026-03-01
INITIAL_BETA   = 5.0   # calibration confirmed by PI 2026-03-01
INITIAL_KAPPA  = INITIAL_ALPHA + INITIAL_BETA  # = 7.0; fixed through Phase 1

B_SAMPLES      = 500   # bootstrap iterations; pre-specified in Research Plan
NOISE_CV       = 0.005  # 0.5% CV for residual non-fire demolition uncertainty
                        # (was 1.0%; reduced because DINS now handles wildfire demolitions)

CALIB_TOL      = 0.01  # Phase 1 convergence: 1% relative bias
CALIB_MAX_ITER = 20    # maximum Phase 1 iterations per county
UNDERSHOOT_THR = 0.10  # flag undershoot if > 10% below calibration target

MU_CLIP_LO = 0.01  # minimum μ for Beta numerical stability
MU_CLIP_HI = 0.99  # maximum μ for Beta numerical stability

CRS_CA   = "EPSG:3310"
YEARS    = list(range(2010, 2025))
RNG_SEED = 42

FIRE_COUNTIES = {
    "Butte":  "06007",  # Camp Fire 2018
    "Sonoma": "06097",  # Tubbs Fire 2017
    "Shasta": "06089",  # Carr Fire 2018
}


# ---------------------------------------------------------------------------
# Step 0a: ACS external calibration (county-level housing unit counts)
# ---------------------------------------------------------------------------

def fetch_acs_housing_calibration(
    county_stats: pd.DataFrame,
) -> pd.DataFrame:
    """
    Compute per-county external absorption fraction f_c_external using ACS
    B25001 (total housing units) as the ground-truth residential count.

    Formula
    -------
    f_c_external = (ACS_units_c − R_c) / max(N_c, 1)

    Interpretation: the fraction of null-subtype Overture buildings that must
    be residential to reconcile the Overture labeled count (R_c) with the ACS
    total housing unit count.

    Unit/structure caveat
    ---------------------
    ACS counts housing units; Overture counts structures. In single-family-
    dominated counties (rural CA, high-FHSZ treatment counties), units ≈
    structures so the calibration is valid. In dense urban counties (LA, SF),
    ACS units >> Overture structures → f_c_external clips to MU_CLIP_HI. These
    these dense urban counties are logged; the overcorrection does not affect
    rural/wildfire-county estimates where the calibration is most valid.

    Fallback
    --------
    Counties where ACS_units < R_c (ACS undercounts relative to Overture
    labeled residential — unusual but possible in dense areas where Overture
    flags many commercial buildings as residential) fall back to the endogenous
    r_frac_c from Overture.

    Parameters
    ----------
    county_stats : pd.DataFrame
        county_FIPS, R_c, N_c, r_frac (from compute_county_building_stats)

    Returns
    -------
    pd.DataFrame
        county_FIPS, acs_units, f_c_external, calibration_source, diff_from_rfrac
        One row per county.
        calibration_source ∈ {'acs', 'acs_clipped', 'fallback_acs_undercount',
                               'fallback_acs_unavailable'}
    """
    print(f"  Fetching ACS {ACS_HOUSING_YEAR} 5-year B25001 (total housing units) "
          f"at county level...")

    try:
        raw = fetch_acs_batch(
            year=ACS_HOUSING_YEAR,
            variables=["B25001_001E"],
            state_fips="06",
            geography="county",
        )
    except Exception as exc:
        print(f"  [warn] ACS fetch failed: {exc}")
        print(f"  [warn] Falling back to endogenous r_frac_c for all counties.")
        result = county_stats[["county_FIPS", "r_frac"]].copy()
        result["acs_units"]          = pd.NA
        result["f_c_external"]       = result["r_frac"]
        result["calibration_source"] = "fallback_acs_unavailable"
        result["diff_from_rfrac"]    = 0.0
        return result

    # Build county_FIPS and coerce units to numeric
    raw["county_FIPS"] = "06" + raw["county"].str.zfill(3)
    raw["acs_units"]   = pd.to_numeric(raw["B25001_001E"], errors="coerce")
    acs = raw[["county_FIPS", "acs_units"]].copy()

    # Merge with county building stats
    merged = county_stats[["county_FIPS", "R_c", "N_c", "r_frac"]].merge(
        acs, on="county_FIPS", how="left"
    )

    rows = []
    for _, row in merged.iterrows():
        fips    = row["county_FIPS"]
        R_c     = float(row["R_c"])
        N_c     = float(row["N_c"])
        r_frac  = float(row["r_frac"])
        acs_u   = row["acs_units"]

        if pd.isna(acs_u):
            rows.append({
                "county_FIPS": fips, "acs_units": pd.NA,
                "f_c_external": r_frac,
                "calibration_source": "fallback_acs_unavailable",
                "diff_from_rfrac": 0.0,
            })
            continue

        acs_u = float(acs_u)

        if acs_u < R_c:
            # ACS unit count < Overture labeled residential: unusual; fall back.
            rows.append({
                "county_FIPS": fips, "acs_units": acs_u,
                "f_c_external": r_frac,
                "calibration_source": "fallback_acs_undercount",
                "diff_from_rfrac": 0.0,
            })
            continue

        f_raw = (acs_u - R_c) / max(N_c, 1.0)

        if f_raw >= MU_CLIP_HI:
            # ACS units >> Overture buildings: dense urban unit/structure mismatch.
            # Clip to MU_CLIP_HI and flag.
            f_ext = MU_CLIP_HI
            src   = "acs_clipped"
        else:
            f_ext = max(MU_CLIP_LO, f_raw)
            src   = "acs"

        rows.append({
            "county_FIPS": fips,
            "acs_units":   acs_u,
            "f_c_external": round(f_ext, 4),
            "calibration_source": src,
            "diff_from_rfrac": round(abs(f_ext - r_frac), 4),
        })

    result = pd.DataFrame(rows)

    n_acs     = (result["calibration_source"] == "acs").sum()
    n_clipped = (result["calibration_source"] == "acs_clipped").sum()
    n_fall    = result["calibration_source"].str.startswith("fallback").sum()
    n_large_diff = (result["diff_from_rfrac"] > 0.15).sum()

    print(f"  ACS calibration results ({len(result)} counties):")
    print(f"    acs (clean):  {n_acs}   acs_clipped: {n_clipped}   fallback: {n_fall}")
    print(f"    Counties where |f_c_acs − r_frac_c| > 15pp: {n_large_diff}")
    if n_large_diff > 0:
        large = result[result["diff_from_rfrac"] > 0.15].sort_values(
            "diff_from_rfrac", ascending=False
        )
        print(large[["county_FIPS", "f_c_external", "diff_from_rfrac",
                      "calibration_source"]].to_string(index=False))

    print(f"  f_c_external distribution (all counties):")
    fe = result["f_c_external"]
    print(f"    min={fe.min():.3f}  p25={fe.quantile(0.25):.3f}  "
          f"median={fe.median():.3f}  p75={fe.quantile(0.75):.3f}  "
          f"max={fe.max():.3f}")

    return result


# ---------------------------------------------------------------------------
# Step 0: County building statistics (cached)
# ---------------------------------------------------------------------------

def compute_county_building_stats() -> pd.DataFrame:
    """
    Spatial join all Overture buildings (including null-subtype) to county
    polygons and compute per-county R_c, N_c, L_c, r_frac.

    The raw geoparquet is ~2.75 GB. On first run this step takes 5–15 minutes;
    the result is cached to data/temp/county_building_stats.parquet.

    Returns
    -------
    pd.DataFrame
        One row per county with columns:
        county_FIPS, R_c, N_c, L_c, r_frac
        where:
          R_c    = residential-labeled building count
          N_c    = null-subtype building count
          L_c    = labeled non-residential building count
          r_frac = R_c / (R_c + L_c), clipped to [MU_CLIP_LO, MU_CLIP_HI]
    """
    TEMP_DIR.mkdir(parents=True, exist_ok=True)

    if CACHE_COUNTY_STATS.exists():
        df = pd.read_parquet(CACHE_COUNTY_STATS)
        print(f"  [cached] county_building_stats.parquet ({len(df)} counties)")
        return df

    print(f"  Loading raw Overture buildings (geometry + subtype only)...")
    print(f"  NOTE: ~2.75 GB file; first run takes 5–15 min. "
          f"Result will be cached to data/temp/.")

    # Read only the two columns needed — minimises memory footprint
    gdf = gpd.read_parquet(IN_RAW_BUILDINGS, columns=["geometry", "subtype"])
    n_total = len(gdf)
    print(f"  Loaded: {n_total:,} buildings  CRS: {gdf.crs}")

    # Build county polygons by dissolving TIGER 2010 tracts
    print("  Dissolving TIGER tracts to county polygons...")
    tracts = gpd.read_file(TIGER_SHP)
    geoid_col = next(c for c in tracts.columns if "GEOID" in c.upper())
    tracts["county_FIPS"] = tracts[geoid_col].str[:5]
    tracts = tracts[tracts[geoid_col].str.startswith("06")].copy()
    counties = (
        tracts[["county_FIPS", "geometry"]]
        .dissolve(by="county_FIPS")
        .reset_index()
    )
    print(f"  County polygons: {len(counties)}")

    # Reproject both layers to CA Albers
    print("  Reprojecting to EPSG:3310...")
    counties_alb = counties.to_crs(CRS_CA)
    gdf_alb      = gdf.to_crs(CRS_CA)

    # Replace polygon footprints with centroids — drastically reduces memory
    # and speeds up the spatial join
    print("  Computing building centroids...")
    gdf_alb["geometry"] = gdf_alb.geometry.centroid

    # Spatial join: building centroid → county polygon
    # 58 county polygons vs 8,057 tract polygons → ~140× fewer comparisons
    print(f"  Spatial join: {n_total:,} centroids → {len(counties_alb)} "
          f"county polygons...")
    joined = gpd.sjoin(
        gdf_alb[["geometry", "subtype"]],
        counties_alb[["county_FIPS", "geometry"]],
        how="left",
        predicate="within",
    )

    n_matched   = joined["county_FIPS"].notna().sum()
    n_unmatched = joined["county_FIPS"].isna().sum()
    print(f"  Matched: {n_matched:,}  Unmatched: {n_unmatched:,} "
          f"({n_unmatched / n_total:.1%})")

    matched = joined[joined["county_FIPS"].notna()].copy()

    # Classify each building into one of three categories
    matched["subtype_cat"] = "labeled_nonres"
    matched.loc[matched["subtype"] == "residential", "subtype_cat"] = "residential"
    matched.loc[matched["subtype"].isna(), "subtype_cat"] = "null"

    # Pivot to wide: one row per county, one column per category
    counts = (
        matched.groupby(["county_FIPS", "subtype_cat"])
        .size()
        .unstack(fill_value=0)
        .reset_index()
    )
    for col in ("residential", "null", "labeled_nonres"):
        if col not in counts.columns:
            counts[col] = 0

    counts = counts.rename(columns={
        "residential":   "R_c",
        "null":          "N_c",
        "labeled_nonres": "L_c",
    })

    # Labeled residential fraction — derived from project data
    labeled_total = (counts["R_c"] + counts["L_c"]).clip(lower=1)
    counts["r_frac"] = (counts["R_c"] / labeled_total).clip(
        lower=MU_CLIP_LO, upper=MU_CLIP_HI
    )

    counts.to_parquet(CACHE_COUNTY_STATS, index=False)
    print(f"  [saved] county_building_stats.parquet: {len(counts)} counties")
    print(f"  Totals — R_c: {counts['R_c'].sum():,}  "
          f"N_c: {counts['N_c'].sum():,}  L_c: {counts['L_c'].sum():,}")
    print(f"  r_frac (labeled res. fraction) — "
          f"min={counts['r_frac'].min():.3f}  "
          f"mean={counts['r_frac'].mean():.3f}  "
          f"max={counts['r_frac'].max():.3f}")
    return counts


# ---------------------------------------------------------------------------
# Step 0b: Arruda hybrid override for ACS-clipped counties
# ---------------------------------------------------------------------------

def apply_arruda_hybrid_calibration(
    ext_absorption: pd.DataFrame,
    county_stats: pd.DataFrame,
) -> pd.DataFrame:
    """
    For counties where ACS calibration clips to MU_CLIP_HI (unit/structure
    mismatch in dense urban counties), replace f_c_external with the
    Arruda-derived absorption fraction:

        f_c_arruda = max(0, Arruda_RES_c - R_c) / max(N_c, 1)

    Arruda et al. (2024) count residential *buildings* (same unit as Overture),
    eliminating the ACS unit/structure mismatch in dense urban counties.
    OSM undercoverage in rural/suburban areas makes Arruda less reliable there,
    so ACS is retained for non-clipping counties.

    Validation
    ----------
    Counties where Arruda_RES_c < R_c yield f_c_arruda < 0 (Arruda observes
    fewer residential buildings than Overture already labels). These are flagged,
    reported, and fall back to the endogenous r_frac_c.

    Requires
    --------
    output/tables/arruda_ca_county_counts.parquet  (from script 07).
    If absent, this step is skipped with a warning.

    Parameters
    ----------
    ext_absorption : pd.DataFrame
        Output of fetch_acs_housing_calibration() — columns:
        county_FIPS, acs_units, f_c_external, calibration_source, diff_from_rfrac
    county_stats : pd.DataFrame
        county_FIPS, R_c, N_c, r_frac  (from compute_county_building_stats)

    Returns
    -------
    pd.DataFrame
        Same schema as ext_absorption. For acs_clipped counties:
          calibration_source → 'arruda_direct' (override applied)
                            → 'arruda_negative_fallback' (Arruda_RES < R_c)
        All other rows unchanged.
    """
    if not IN_ARRUDA_COUNTY.exists():
        print(f"  [skip] Arruda county counts not found: {IN_ARRUDA_COUNTY}")
        print(f"  [skip] Run 07_acquire_arruda_comparison.py first to enable Arruda hybrid.")
        print(f"  [skip] Keeping ACS calibration for all counties.")
        return ext_absorption

    arruda = pd.read_parquet(IN_ARRUDA_COUNTY)[["county_FIPS", "arruda_res_count"]].copy()
    print(f"  Loaded Arruda county counts: {len(arruda)} counties")

    # Merge ext_absorption with county building stats (R_c, N_c) and Arruda counts
    merged = (
        ext_absorption
        .merge(county_stats[["county_FIPS", "R_c", "N_c", "r_frac"]], on="county_FIPS", how="left")
        .merge(arruda, on="county_FIPS", how="left")
    )

    clipping_mask = merged["calibration_source"] == "acs_clipped"
    n_clipping = clipping_mask.sum()
    print(f"  Counties with ACS clipping (unit/structure mismatch): {n_clipping}")

    if n_clipping == 0:
        print(f"  [ok] No clipping counties — Arruda override not needed.")
        return ext_absorption

    result = ext_absorption.copy()
    override_rows    = []
    negative_rows    = []
    missing_rows     = []

    for idx, row in merged[clipping_mask].iterrows():
        fips       = row["county_FIPS"]
        R_c        = float(row["R_c"])
        N_c        = float(row["N_c"])
        r_frac     = float(row["r_frac"])
        arruda_res = row["arruda_res_count"]

        if pd.isna(arruda_res):
            missing_rows.append(fips)
            print(f"  [warn] {fips}: Arruda data missing; retaining acs_clipped calibration.")
            continue

        arruda_res = float(arruda_res)

        # Validation check: negative f_c_arruda if Arruda_RES < R_c
        if arruda_res < R_c:
            negative_rows.append({
                "county_FIPS":   fips,
                "arruda_res":    int(arruda_res),
                "R_c":           int(R_c),
                "f_c_arruda_raw": round((arruda_res - R_c) / max(N_c, 1.0), 4),
                "note": "Arruda_RES < Overture_labeled_residential; falls back to r_frac_c",
            })
            mask = result["county_FIPS"] == fips
            result.loc[mask, "f_c_external"]       = r_frac
            result.loc[mask, "calibration_source"] = "arruda_negative_fallback"
            result.loc[mask, "diff_from_rfrac"]    = 0.0
            continue

        # Valid override: f_c_arruda ≥ 0
        f_raw    = (arruda_res - R_c) / max(N_c, 1.0)
        f_arruda = max(MU_CLIP_LO, min(MU_CLIP_HI, f_raw))
        override_rows.append({
            "county_FIPS":        fips,
            "arruda_res":         int(arruda_res),
            "R_c":                int(R_c),
            "N_c":                int(N_c),
            "f_c_arruda":         round(f_arruda, 4),
            "f_c_acs_was_clipped": MU_CLIP_HI,
        })
        mask = result["county_FIPS"] == fips
        result.loc[mask, "f_c_external"]       = round(f_arruda, 4)
        result.loc[mask, "calibration_source"] = "arruda_direct"
        result.loc[mask, "diff_from_rfrac"]    = round(abs(f_arruda - r_frac), 4)

    # ── Validation report ────────────────────────────────────────────────────
    print()
    print("  ── Arruda hybrid calibration validation ──────────────────────────")

    if negative_rows:
        print(f"  [WARN] {len(negative_rows)} county/ies with NEGATIVE f_c_arruda "
              f"(Arruda_RES < Overture labeled residential):")
        print(f"  {'county_FIPS':<12} {'arruda_res':>12} {'R_c':>10} "
              f"{'f_c_arruda_raw':>15}  note")
        for r in negative_rows:
            print(f"  {r['county_FIPS']:<12} {r['arruda_res']:>12,} {r['R_c']:>10,} "
                  f"  {r['f_c_arruda_raw']:>14.4f}  {r['note']}")
        print(f"  → These counties fall back to endogenous r_frac_c.")
    else:
        print(f"  [ok] No negative f_c_arruda among clipping counties "
              f"(Arruda_RES ≥ R_c for all).")

    if override_rows:
        print(f"\n  [ok] Arruda override applied to {len(override_rows)} "
              f"acs_clipped counties:")
        print(f"  {'county_FIPS':<12} {'R_c':>10} {'N_c':>10} {'arruda_res':>12} "
              f"{'f_c_arruda':>11}")
        for r in override_rows:
            print(f"  {r['county_FIPS']:<12} {r['R_c']:>10,} {r['N_c']:>10,} "
                  f"{r['arruda_res']:>12,}  {r['f_c_arruda']:>10.4f}")

    # Calibration source summary
    src_counts = result["calibration_source"].value_counts()
    print(f"\n  Calibration source summary (post Arruda hybrid):")
    for src, cnt in src_counts.items():
        print(f"    {src:<35}: {cnt:>3}")

    return result


# ---------------------------------------------------------------------------
# Step 1: Per-county Beta calibration
# ---------------------------------------------------------------------------

def calibrate_county_betas(
    county_stats: pd.DataFrame,
    external_absorption: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Calibrate Beta(α_c, β_c) per county via iterative forward-forecast
    comparison. Returns calibrated parameters and a full iteration log.

    Phase 1 — calibrate μ (affects both α and β):
      Start: μ = INITIAL_ALPHA / INITIAL_KAPPA ≈ 0.286
      Target: forward_mean = R_c + μ × N_c  ≈  T_target_c = R_c + f_target_c × N_c
      where f_target_c = f_c_external (ACS-derived) if available and not fallback,
      otherwise r_frac_c (endogenous Overture labeled fraction).
      Adjust μ via bisection toward f_target_c until |bias| < CALIB_TOL.
      α_c = μ × κ,  β_c = (1−μ) × κ  (concentration κ held at INITIAL_KAPPA).

    Phase 2 — P95 check vs hard upper bound T_c = R_c + N_c:
      P95(Beta(α_c,β_c)) × N_c + R_c  vs  T_c.
      Satisfied by construction; logged for transparency.

    Parameters
    ----------
    county_stats : pd.DataFrame
        county_FIPS, R_c, N_c, L_c, r_frac
    external_absorption : pd.DataFrame or None
        Output of fetch_acs_housing_calibration(). If None, falls back to
        endogenous r_frac_c for all counties.

    Returns
    -------
    calib_params : pd.DataFrame
        county_FIPS, alpha_c, beta_c, mu_c, kappa_c, R_c, N_c, T_target_c, T_c,
        f_target, calib_target_source
    calib_log : pd.DataFrame
        Iteration-level log; columns documented in script header.
    """
    log_rows    = []
    param_rows  = []

    for _, row in county_stats.iterrows():
        fips     = row["county_FIPS"]
        R_c      = float(row["R_c"])
        N_c      = float(row["N_c"])
        r_frac_c = float(row["r_frac"])

        # Determine calibration target: external ACS fraction if available,
        # otherwise endogenous r_frac_c from Overture labeled buildings.
        if external_absorption is not None:
            ext_row = external_absorption[
                external_absorption["county_FIPS"] == fips
            ]
            if (
                len(ext_row) > 0
                and not str(ext_row["calibration_source"].values[0]).startswith("fallback")
            ):
                f_target      = float(ext_row["f_c_external"].values[0])
                calib_tgt_src = str(ext_row["calibration_source"].values[0])
            else:
                f_target      = r_frac_c
                calib_tgt_src = "endogenous"
        else:
            f_target      = r_frac_c
            calib_tgt_src = "endogenous"

        T_target_c = R_c + f_target * N_c   # calibration target (ACS or endogenous)
        T_c        = R_c + N_c               # hard upper bound

        # Starting parameters
        alpha = INITIAL_ALPHA
        beta  = INITIAL_BETA
        kappa = INITIAL_KAPPA
        mu    = alpha / kappa

        # ── Phase 1: calibrate μ ─────────────────────────────────────────────
        converged = False

        if N_c == 0:
            # Absorption fraction irrelevant: all bootstrap samples give A=R_c
            log_rows.append({
                "county_FIPS": fips, "phase": 1, "iteration": 0,
                "alpha_old": alpha, "beta_old": beta,
                "alpha_new": alpha, "beta_new": beta,
                "forward_mean": R_c, "T_target_c": T_target_c, "T_c": T_c,
                "N_c": int(N_c), "R_c": int(R_c), "r_frac": r_frac_c,
                "f_target": round(f_target, 4), "calib_target_source": calib_tgt_src,
                "bias_pct": 0.0, "adjustment_direction": "skip_N_c_zero",
                "converged": True,
            })
            converged = True

        else:
            for i in range(1, CALIB_MAX_ITER + 1):
                forward_mean = R_c + mu * N_c
                bias_pct = (
                    (forward_mean - T_target_c) / T_target_c
                    if T_target_c > 0 else 0.0
                )

                alpha_old = alpha
                beta_old  = beta

                if abs(bias_pct) < CALIB_TOL:
                    direction = "converged"
                    converged = True
                elif forward_mean > T_target_c:
                    # Overshoot: reduce μ toward f_target via bisection
                    direction = "decrease"
                    mu = 0.5 * (mu + f_target)
                    mu = max(MU_CLIP_LO, min(MU_CLIP_HI, mu))
                elif (T_target_c - forward_mean) / max(T_target_c, 1) > UNDERSHOOT_THR:
                    # Undershoot > 10%: increase μ toward f_target via bisection
                    direction = "increase"
                    mu = 0.5 * (mu + f_target)
                    mu = max(MU_CLIP_LO, min(MU_CLIP_HI, mu))
                else:
                    # 0% < undershoot ≤ 10%: within acceptable band
                    direction = "within_band"
                    converged = True

                # Update both α and β from new μ (keeping κ fixed)
                alpha = mu * kappa
                beta  = (1.0 - mu) * kappa

                log_rows.append({
                    "county_FIPS": fips, "phase": 1, "iteration": i,
                    "alpha_old": round(alpha_old, 4),
                    "beta_old":  round(beta_old,  4),
                    "alpha_new": round(alpha, 4),
                    "beta_new":  round(beta,  4),
                    "forward_mean": round(forward_mean, 1),
                    "T_target_c":   round(T_target_c, 1),
                    "T_c":          round(T_c, 1),
                    "N_c": int(N_c), "R_c": int(R_c),
                    "r_frac": round(r_frac_c, 4),
                    "f_target": round(f_target, 4),
                    "calib_target_source": calib_tgt_src,
                    "bias_pct": round(bias_pct, 6),
                    "adjustment_direction": direction,
                    "converged": converged,
                })

                if converged:
                    break

            if not converged:
                print(f"  [warn] {fips}: Phase 1 did not converge after "
                      f"{CALIB_MAX_ITER} iterations (final bias={bias_pct:.3%})")

        # ── Phase 2: P95 check vs hard upper bound T_c ───────────────────────
        p95_f = stats.beta(alpha, beta).ppf(0.95)
        p95_A = R_c + p95_f * N_c
        p2_ok = bool(p95_A <= T_c)  # always True since Beta ≤ 1 and T_c = R_c + N_c

        log_rows.append({
            "county_FIPS": fips, "phase": 2, "iteration": 0,
            "alpha_old": round(alpha, 4), "beta_old":  round(beta, 4),
            "alpha_new": round(alpha, 4), "beta_new":  round(beta, 4),
            "forward_mean": round(p95_A, 1),
            "T_target_c":   round(T_c, 1),
            "T_c":          round(T_c, 1),
            "N_c": int(N_c), "R_c": int(R_c), "r_frac": round(r_frac_c, 4),
            "f_target": round(f_target, 4),
            "calib_target_source": calib_tgt_src,
            "bias_pct": round((p95_A - T_c) / max(T_c, 1), 6),
            "adjustment_direction": f"P95_check_pass={p2_ok}",
            "converged": p2_ok,
        })

        param_rows.append({
            "county_FIPS": fips,
            "alpha_c":    round(alpha, 4),
            "beta_c":     round(beta,  4),
            "mu_c":       round(mu,    4),
            "kappa_c":    round(kappa, 4),
            "R_c":        int(R_c),
            "N_c":        int(N_c),
            "T_target_c": round(T_target_c, 1),
            "T_c":        round(T_c, 1),
            "f_target":           round(f_target, 4),
            "calib_target_source": calib_tgt_src,
        })

    calib_params = pd.DataFrame(param_rows)
    calib_log    = pd.DataFrame(log_rows)

    # Summary
    n_conv = (
        calib_log[calib_log["phase"] == 1]
        .groupby("county_FIPS")["converged"]
        .any()
        .sum()
    )
    print(f"  Phase 1 converged: {n_conv}/{len(calib_params)} counties")
    print(f"  Calibrated μ — "
          f"min={calib_params['mu_c'].min():.3f}  "
          f"mean={calib_params['mu_c'].mean():.3f}  "
          f"max={calib_params['mu_c'].max():.3f}")
    print(f"  Calibrated α — "
          f"min={calib_params['alpha_c'].min():.3f}  "
          f"max={calib_params['alpha_c'].max():.3f}")
    print(f"  Calibrated β — "
          f"min={calib_params['beta_c'].min():.3f}  "
          f"max={calib_params['beta_c'].max():.3f}")

    return calib_params, calib_log


# ---------------------------------------------------------------------------
# Step 2: Vectorised county-level bootstrap
# ---------------------------------------------------------------------------

def run_county_bootstrap(
    calib_params: pd.DataFrame,
    permits: pd.DataFrame,
    dins: pd.DataFrame,
) -> pd.DataFrame:
    """
    Draw B = B_SAMPLES bootstrap iterations per county and compute hind-cast
    distribution percentiles for each county × year.

    Vectorised over B: all samples for a county are drawn at once and
    processed as a (B × T) matrix. No Python loop over iterations.

    Parameters
    ----------
    calib_params : pd.DataFrame
        county_FIPS, alpha_c, beta_c, R_c, N_c, T_c
    permits : pd.DataFrame
        county_FIPS, year, structures_permitted
    dins : pd.DataFrame
        county_FIPS, year, structures_destroyed

    Returns
    -------
    pd.DataFrame
        One row per county × year with columns:
        county_FIPS, year, p5_county, p50_county, p95_county, iqr_county
    """
    # Compute net_after_t: for year t, Σ_{s=t+1..2024}(structures_permitted - dins_destroyed).
    # In fire years, dins_destroyed > structures_permitted → net_after_t is smaller (or
    # negative), which adds pre-fire structures back when subtracted from the anchor.
    perm = permits.sort_values(["county_FIPS", "year"]).copy()
    perm = perm.merge(
        dins[["county_FIPS", "year", "structures_destroyed"]],
        on=["county_FIPS", "year"],
        how="left",
    )
    perm["structures_destroyed"] = perm["structures_destroyed"].fillna(0)
    perm["net_structures_change"] = perm["structures_permitted"] - perm["structures_destroyed"]

    perm["rev_cumsum"] = (
        perm.groupby("county_FIPS")["net_structures_change"]
        .transform(lambda x: x[::-1].cumsum()[::-1])
    )
    perm["net_after_t"] = (
        perm.groupby("county_FIPS")["rev_cumsum"]
        .transform(lambda x: x.shift(-1, fill_value=0))
    )
    perm = perm[["county_FIPS", "year", "net_after_t"]].copy()

    rng     = np.random.default_rng(RNG_SEED)
    results = []

    for _, row in calib_params.iterrows():
        fips    = row["county_FIPS"]
        alpha_c = float(row["alpha_c"])
        beta_c  = float(row["beta_c"])
        R_c     = float(row["R_c"])
        N_c     = float(row["N_c"])
        T_c     = float(row["T_c"])

        # permits_after_t for this county, sorted by year ascending
        cperm = perm[perm["county_FIPS"] == fips].sort_values("year")

        if len(cperm) == 0:
            print(f"  [warn] No permits data for {fips} — using zero net change.")
            cperm = pd.DataFrame({
                "county_FIPS": [fips] * len(YEARS),
                "year":        YEARS,
                "net_after_t": [0.0] * len(YEARS),
            })

        net_arr = cperm["net_after_t"].to_numpy(dtype=float)        # shape (T,)
        years   = cperm["year"].values                               # shape (T,)

        # ── Draw B samples ───────────────────────────────────────────────────
        f_samples = rng.beta(alpha_c, beta_c, size=B_SAMPLES)      # shape (B,)
        A_samples = R_c + f_samples * N_c                           # shape (B,)
        A_samples = np.clip(A_samples, R_c, T_c)                   # A ∈ [R_c, T_c]

        # ── Hind-cast matrix: shape (B, T) ───────────────────────────────────
        # Broadcasting: (B,1) − (1,T) → (B,T)
        # net_arr may be negative in fire years, which adds pre-fire structures back.
        hind = A_samples[:, None] - net_arr[None, :]

        # Hard upper bound at each year: max(1, T_c − net_after_t)
        upper_t = np.maximum(1.0, T_c - net_arr)[None, :]          # shape (1,T)
        hind    = np.clip(hind, 1.0, upper_t)                      # shape (B,T)

        # ── Annual noise: σ_t = NOISE_CV × hind_t ───────────────────────────
        noise = rng.normal(0.0, NOISE_CV * hind)                   # shape (B,T)
        hind  = np.clip(hind + noise, 1.0, None)

        # ── Percentiles across B iterations ──────────────────────────────────
        p5  = np.percentile(hind, 5,  axis=0)                      # shape (T,)
        p25 = np.percentile(hind, 25, axis=0)
        p50 = np.percentile(hind, 50, axis=0)
        p75 = np.percentile(hind, 75, axis=0)
        p95 = np.percentile(hind, 95, axis=0)
        iqr = p75 - p25

        for t_idx, year in enumerate(years):
            results.append({
                "county_FIPS": fips,
                "year":        int(year),
                "p5_county":   float(p5[t_idx]),
                "p50_county":  float(p50[t_idx]),
                "p95_county":  float(p95[t_idx]),
                "iqr_county":  float(iqr[t_idx]),
            })

    return pd.DataFrame(results)


# ---------------------------------------------------------------------------
# Step 3: Downscale county percentiles to tract × year
# ---------------------------------------------------------------------------

def build_bootstrap_panel(
    county_bootstrap: pd.DataFrame,
    tract_panel: pd.DataFrame,
    calib_params: pd.DataFrame,
) -> pd.DataFrame:
    """
    Downscale county-level bootstrap percentiles to tract level using the
    same tract_share as tract_structure_panel.parquet.

    For tract i in county c, year t:
        p{q}_residential_count_it = p{q}_county_ct × tract_share_i

    Parameters
    ----------
    county_bootstrap : pd.DataFrame
        county_FIPS, year, p5_county, p50_county, p95_county, iqr_county
    tract_panel : pd.DataFrame
        geoid, county_FIPS, year, tract_share  (from tract_structure_panel)
    calib_params : pd.DataFrame
        county_FIPS, alpha_c, beta_c  (annotated in output)

    Returns
    -------
    pd.DataFrame
        Tract × year panel with columns:
        geoid, county_FIPS, year,
        p5_residential_count, p50_residential_count,
        p95_residential_count, iqr_residential_count,
        alpha_c, beta_c
    """
    panel = tract_panel[["geoid", "county_FIPS", "year", "tract_share"]].merge(
        county_bootstrap,
        on=["county_FIPS", "year"],
        how="left",
    )

    n_missing = panel["p50_county"].isna().sum()
    if n_missing > 0:
        print(f"  [warn] {n_missing} tract-year rows missing county bootstrap.")

    # Linear downscale; floor at 1 (consistent with build_structure_panel.py)
    panel["p5_residential_count"]  = (
        panel["p5_county"]  * panel["tract_share"]
    ).clip(lower=1.0).round(2)

    panel["p50_residential_count"] = (
        panel["p50_county"] * panel["tract_share"]
    ).clip(lower=1.0).round(2)

    panel["p95_residential_count"] = (
        panel["p95_county"] * panel["tract_share"]
    ).clip(lower=1.0).round(2)

    # IQR: no floor at 1 — zero is meaningful (no spread)
    panel["iqr_residential_count"] = (
        panel["iqr_county"] * panel["tract_share"]
    ).clip(lower=0.0).round(2)

    # Annotate with calibrated Beta parameters (county-level, constant over years)
    panel = panel.merge(
        calib_params[["county_FIPS", "alpha_c", "beta_c"]],
        on="county_FIPS",
        how="left",
    )

    return panel[[
        "geoid", "county_FIPS", "year",
        "p5_residential_count", "p50_residential_count",
        "p95_residential_count", "iqr_residential_count",
        "alpha_c", "beta_c",
    ]].sort_values(["geoid", "year"])


# ---------------------------------------------------------------------------
# Diagnostics
# ---------------------------------------------------------------------------

def print_diagnostics(
    bootstrap_panel: pd.DataFrame,
    calib_params: pd.DataFrame,
) -> None:
    """Print summary statistics for calibration and bootstrap outputs."""
    print("\n" + "=" * 65)
    print("DIAGNOSTICS")
    print("=" * 65)

    print(f"\n  Panel: {bootstrap_panel['geoid'].nunique():,} tracts × "
          f"{bootstrap_panel['year'].nunique()} years = {len(bootstrap_panel):,} rows")

    # Mean p50 by year (should increase toward 2024)
    print("\n  Mean p50_residential_count by year (should increase toward 2024):")
    yr = bootstrap_panel.groupby("year")["p50_residential_count"].mean().round(1)
    print(yr.to_string())

    # Uncertainty: IQR / p50
    bootstrap_panel = bootstrap_panel.copy()
    bootstrap_panel["iqr_frac"] = (
        bootstrap_panel["iqr_residential_count"]
        / bootstrap_panel["p50_residential_count"].clip(lower=1)
    )
    print(f"\n  Bootstrap uncertainty (IQR / p50) across all tract-years:")
    print(f"  mean={bootstrap_panel['iqr_frac'].mean():.3f}  "
          f"median={bootstrap_panel['iqr_frac'].median():.3f}  "
          f"p75={bootstrap_panel['iqr_frac'].quantile(0.75):.3f}  "
          f"max={bootstrap_panel['iqr_frac'].max():.3f}")

    # Calibrated μ distribution
    print(f"\n  Calibrated μ_c distribution ({len(calib_params)} counties):")
    mu = calib_params["mu_c"]
    print(f"  min={mu.min():.3f}  p25={mu.quantile(0.25):.3f}  "
          f"median={mu.median():.3f}  p75={mu.quantile(0.75):.3f}  "
          f"max={mu.max():.3f}")

    # Fire-county spot-check: 2024 county-level p50 vs R_c (point estimate anchor)
    print(f"\n  Fire-county spot-check (2024 county-level totals):")
    print(f"  {'County':<12}  {'R_c':>8}  {'mu_c':>6}  "
          f"{'p50_sum':>10}  {'p95_sum':>10}  {'p50/R_c':>8}")
    for county_name, fips in FIRE_COUNTIES.items():
        sub = bootstrap_panel[
            (bootstrap_panel["county_FIPS"] == fips)
            & (bootstrap_panel["year"] == 2024)
        ]
        if len(sub) == 0:
            print(f"  {county_name:<12}  [not found]")
            continue
        p50_sum = sub["p50_residential_count"].sum()
        p95_sum = sub["p95_residential_count"].sum()
        cp = calib_params[calib_params["county_FIPS"] == fips]
        R_c = int(cp["R_c"].values[0]) if len(cp) > 0 else -1
        mu_c = float(cp["mu_c"].values[0]) if len(cp) > 0 else float("nan")
        ratio = p50_sum / max(R_c, 1)
        print(f"  {county_name:<12}  {R_c:>8,}  {mu_c:>6.3f}  "
              f"{p50_sum:>10,.0f}  {p95_sum:>10,.0f}  {ratio:>8.2f}x")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 65)
    print("bootstrap_structure_panel.py — Bootstrapped residential stock")
    print("=" * 65)

    if OUT_BOOTSTRAP.exists() and OUT_CALIB_LOG.exists():
        print(f"\n[skip] Output files already exist. Delete to rebuild:")
        print(f"  {OUT_BOOTSTRAP}")
        print(f"  {OUT_CALIB_LOG}")
        return

    # ── Check required inputs ──────────────────────────────────────────────
    for path in [IN_RAW_BUILDINGS, IN_TRACT_PANEL, IN_BPS_PERMITS, IN_DINS, TIGER_SHP]:
        if not path.exists():
            raise FileNotFoundError(
                f"Required input missing: {path}\n"
                f"Check that all upstream scripts have been run."
            )

    OUT_TABLES.mkdir(parents=True, exist_ok=True)

    # ── Step 0: County building statistics ────────────────────────────────
    print("\n--- Step 0: County building statistics (cached) ---")
    county_stats = compute_county_building_stats()

    # ── Step 0a: ACS external absorption calibration ──────────────────────
    print("\n--- Step 0a: ACS external absorption calibration ---")
    ext_absorption = fetch_acs_housing_calibration(county_stats)

    # ── Step 0b: Arruda hybrid override for ACS-clipped counties ──────────
    print("\n--- Step 0b: Arruda hybrid calibration override ---")
    ext_absorption = apply_arruda_hybrid_calibration(ext_absorption, county_stats)

    ext_absorption.to_parquet(OUT_EXT_ABSORPTION, index=False)
    print(f"\n[saved] {OUT_EXT_ABSORPTION.name}  ({len(ext_absorption)} counties)")

    # ── Step 1: Calibrate Beta parameters per county ──────────────────────
    print("\n--- Step 1: Calibrate Beta(α_c, β_c) per county ---")
    calib_params, calib_log = calibrate_county_betas(county_stats, ext_absorption)
    print(f"  Calibration log: {len(calib_log)} rows "
          f"({len(calib_log[calib_log['phase']==1])} Phase 1, "
          f"{len(calib_log[calib_log['phase']==2])} Phase 2)")

    # ── Step 2: Bootstrap ─────────────────────────────────────────────────
    print(f"\n--- Step 2: Bootstrap (B={B_SAMPLES}) ---")
    permits = pd.read_parquet(IN_BPS_PERMITS)
    print(f"  Permits loaded: {len(permits):,} county × year rows")
    dins = pd.read_parquet(IN_DINS)
    print(f"  DINS loaded: {len(dins):,} county-year fire events "
          f"({dins['structures_destroyed'].sum():,} destroyed residential)")
    county_bootstrap = run_county_bootstrap(calib_params, permits, dins)
    print(f"  County bootstrap panel: {len(county_bootstrap):,} rows")

    # ── Step 3: Downscale to tract × year ─────────────────────────────────
    print("\n--- Step 3: Downscale to tract × year ---")
    tract_panel = pd.read_parquet(IN_TRACT_PANEL)
    print(f"  Tract panel loaded: {len(tract_panel):,} rows")
    bootstrap_panel = build_bootstrap_panel(county_bootstrap, tract_panel, calib_params)
    print(f"  Bootstrap panel: {len(bootstrap_panel):,} tract × year rows")

    # ── Diagnostics ───────────────────────────────────────────────────────
    print_diagnostics(bootstrap_panel, calib_params)

    # ── Save outputs ──────────────────────────────────────────────────────
    print("\n--- Saving outputs ---")

    bootstrap_panel.to_parquet(OUT_BOOTSTRAP, index=False)
    print(f"[saved] {OUT_BOOTSTRAP.name}")
    print(f"        {len(bootstrap_panel):,} rows × {len(bootstrap_panel.columns)} columns")
    print(f"        Columns: {list(bootstrap_panel.columns)}")

    calib_log.to_csv(OUT_CALIB_LOG, index=False)
    print(f"[saved] {OUT_CALIB_LOG.name}")
    print(f"        {len(calib_log):,} iteration log rows")

    print("\n" + "=" * 65)
    print("Done. Next steps:")
    print("  1. Use p50_residential_count as the primary denominator in downstream analyses.")
    print("  2. Compare to residential_count_hindcast (point estimate) as robustness check.")
    print("  3. Run 06_build_acs_challenger.py for direct ACS comparison.")
    print("=" * 65)


if __name__ == "__main__":
    main()
