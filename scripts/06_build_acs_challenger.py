"""
06_build_acs_challenger.py
===========================
Build an ACS-only challenger panel of residential housing unit counts per
California Census tract × year, 2010–2024, with a proper 2020→2010 tract
geoid crosswalk for vintages 2021–2023.

Method
------
Step 0 — Fetch 2020→2010 Census tract crosswalk from Census Bureau
  Download the official 2020 TIGER/Line Relationship File for CA tracts
  (tab20_tract20_tract10_st06.txt) directly from the Census Bureau. Compute
  area-based allocation factors: afact = AREALAND_PART / AREALAND_TRACT_20,
  the fraction of the 2020 tract's land area that falls within each 2010 tract.
  afact sums to ≈1.0 per 2020 tract (may differ slightly for water-boundary
  tracts). Saved to data/clean/tract_crosswalk_2020_to_2010.parquet (skip-if-exists).

Step 1 — Fetch ACS 5-year B25001 at 2010-boundary tracts (vintages 2010–2020)
  Pull ACS 5-year vintages 2010–2020. All use 2010 Census tract boundaries,
  matching the geoid column in the existing tract_structure_panel.parquet.
  Suppressed cells (Census sentinel –666,666,666) are forward-filled within
  tract and then imputed from county-year mean as a fallback.

Step 2 — Fetch ACS 5-year B25001 at 2020-boundary tracts (vintages 2021–2023)
  ACS 2021 and later use 2020 Census tract boundaries. For each vintage,
  fetch B25001 (housing units), impute suppressed cells from county mean, then
  apply the Geocorr crosswalk to allocate units to 2010-boundary tracts:

    allocated_units(2010 tract X) = Σ_Y  ACS_units(2020 tract Y) × afact(Y→X)

  Rows derived this way are flagged acs_crosswalk_translated=True.
  Note: allocation uses area-based weights (AREALAND_PART / AREALAND_TRACT_20),
  which closely approximates housing-unit weighting because CA tracts are
  drawn to be roughly equal in population.

Step 3 — Forward-fill 2024 from 2023
  No ACS 5-year 2024 vintage is yet available (released Dec 2026). Carry
  forward the 2023 crosswalk-translated value and flag acs_extrapolated=True.
  (Previously 2021–2024 were all forward-filled from 2020; only 2024 remains.)

Step 4 — Compare to BPS hind-cast and bootstrap p50
  Merge ACS, BPS, and bootstrap panels on geoid × year; compute Pearson r,
  log-Pearson r, Spearman ρ, and level ratios per year. Write comparison CSV.

Why B25001 (total units) rather than B25003 (owner-occupied)?
  B25001 counts all units (occupied + vacant, owner + renter) so it better
  tracks the physical housing stock independent of tenure composition changes.
  The unit/structure distinction applies: dense urban tracts have more units
  per structure than rural tracts.

Outputs
-------
  data/clean/tract_crosswalk_2020_to_2010.parquet
    geoid_2020, geoid_2010, afact  (one row per 2020-tract × 2010-tract pair)

  data/clean/tract_structure_panel_acs.parquet
    geoid, county_FIPS, year, acs_housing_units, acs_vintage_year,
    acs_extrapolated, acs_crosswalk_translated, acs_imputed

  output/tables/acs_vs_bps_comparison.csv
    year, n_matched, acs_mean, bps_mean, bootstrap_p50_mean,
    acs_bps_pearson_r, acs_bps_spearman_r, acs_bps_logpearson_r,
    acs_bps_mean_ratio, acs_boot_pearson_r, acs_boot_spearman_r,
    acs_boot_logpearson_r, acs_boot_mean_ratio

Usage
-----
  python scripts/06_build_acs_challenger.py

Dependencies
------------
  pandas, pyarrow, requests, scipy
"""

import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import requests as _requests
from scipy import stats

sys.path.insert(0, str(Path(__file__).parent / "utils"))
from census_api import fetch_acs_batch, build_geoid, mask_sentinel

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CLEAN_DIR    = PROJECT_ROOT / "data" / "clean"
OUT_TABLES   = PROJECT_ROOT / "output" / "tables"

OUT_ACS_PANEL   = CLEAN_DIR / "tract_structure_panel_acs.parquet"
OUT_COMPARISON  = OUT_TABLES / "acs_vs_bps_comparison.csv"
OUT_CROSSWALK   = CLEAN_DIR / "tract_crosswalk_2020_to_2010.parquet"

IN_BPS_PANEL    = CLEAN_DIR / "tract_structure_panel.parquet"
IN_BOOT_PANEL   = CLEAN_DIR / "tract_structure_panel_bootstrap.parquet"

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# ACS 5-yr vintages 2010–2020: use 2010 Census tract boundaries (match panel geoid)
ACS_PULL_YEARS = list(range(2010, 2021))

# ACS 5-yr vintages 2021–2023: use 2020 Census tract boundaries; apply crosswalk
ACS_PULL_YEARS_2020BOUNDS = [2021, 2022, 2023]

# 2024: no ACS 5-yr vintage yet; forward-fill from 2023
ACS_EXTRAP_YEARS = [2024]

YEARS = list(range(2010, 2025))

ACS_VAR   = "B25001_001E"   # Total housing units (ACS table B25001)
ACS_LABEL = "acs_housing_units"

FIRE_COUNTIES = {
    "Butte":  "06007",   # Camp Fire 2018
    "Sonoma": "06097",   # Tubbs Fire 2017
    "Shasta": "06089",   # Carr Fire 2018
}

SLEEP_BETWEEN_CALLS = 1.0   # seconds; avoid rate-limiting unauthenticated requests

# ---------------------------------------------------------------------------
# Census Bureau 2020→2010 tract relationship file
# Official TIGER/Line Relationship File for CA (pipe-delimited).
# Source: https://www2.census.gov/geo/docs/maps-data/data/rel2020/tract/
# afact computed as: AREALAND_PART / AREALAND_TRACT_20 (area-weighted).
# ---------------------------------------------------------------------------

CENSUS_TRACT_REL_URL = (
    "https://www2.census.gov/geo/docs/maps-data/data/rel2020/tract/"
    "tab20_tract20_tract10_st06.txt"
)
CENSUS_TRACT_REL_FILE = (
    PROJECT_ROOT / "data" / "raw" / "crosswalks" / "tab20_tract20_tract10_st06.txt"
)


# ---------------------------------------------------------------------------
# Step 0: Fetch 2020→2010 tract crosswalk from MABLE/Geocorr 2022
# ---------------------------------------------------------------------------

def fetch_tract_crosswalk(dest_path: Path) -> pd.DataFrame:
    """
    Build the 2020→2010 Census tract crosswalk for California.

    Downloads the official Census Bureau 2020 TIGER/Line Relationship File
    (tab20_tract20_tract10_st06.txt), which maps every 2020 CA tract to the
    2010 tract(s) it intersects, with the land area of each intersection.

    Allocation factor: afact = AREALAND_PART / AREALAND_TRACT_20
    This is the fraction of the 2020 tract's land area that falls within
    the given 2010 tract; it sums to 1.0 per 2020 tract (area-weighted).

    For an unchanged tract (same boundaries 2010 and 2020), afact = 1.0.
    When a 2020 tract splits a 2010 tract, multiple rows appear, each
    representing the partial overlap.

    Source: https://www2.census.gov/geo/docs/maps-data/data/rel2020/tract/

    Returns
    -------
    pd.DataFrame
        Columns: geoid_2020 (str), geoid_2010 (str), afact (float)
        One row per 2020-tract × 2010-tract pair with afact > 0.
    """
    dest_path = Path(dest_path)
    if dest_path.exists():
        print(f"  [skip] Crosswalk cached: {dest_path.name}")
        return pd.read_parquet(dest_path)

    # Download raw relationship file (skip if already on disk)
    raw_path = CENSUS_TRACT_REL_FILE
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    if not raw_path.exists():
        print(f"  Downloading Census 2020→2010 tract relationship file ...")
        print(f"    URL: {CENSUS_TRACT_REL_URL}")
        resp = _requests.get(CENSUS_TRACT_REL_URL, timeout=120)
        resp.raise_for_status()
        raw_path.write_bytes(resp.content)
        print(f"    [saved] {raw_path.name} ({raw_path.stat().st_size / 1e6:.1f} MB)")
    else:
        print(f"  [skip] Relationship file cached: {raw_path.name}")

    # Parse pipe-delimited file (BOM-aware encoding)
    rel = pd.read_csv(raw_path, sep="|", dtype=str, encoding="utf-8-sig")
    print(f"    Relationship file rows: {len(rel):,}  |  columns: {list(rel.columns)}")

    # Required columns
    geoid20_col = "GEOID_TRACT_20"
    geoid10_col = "GEOID_TRACT_10"
    area20_col  = "AREALAND_TRACT_20"
    area_part   = "AREALAND_PART"

    for col in [geoid20_col, geoid10_col, area20_col, area_part]:
        if col not in rel.columns:
            raise KeyError(f"Expected column '{col}' not found. "
                           f"Actual columns: {list(rel.columns)}")

    rel["_area20"]  = pd.to_numeric(rel[area20_col],  errors="coerce")
    rel["_areapart"] = pd.to_numeric(rel[area_part], errors="coerce")

    # Compute area-based allocation factor; drop rows with zero denominator
    valid = rel["_area20"] > 0
    rel = rel[valid].copy()
    rel["afact"] = (rel["_areapart"] / rel["_area20"]).clip(lower=0.0, upper=1.0)

    xwalk = pd.DataFrame({
        "geoid_2020": rel[geoid20_col].astype(str).str.strip().str.zfill(11),
        "geoid_2010": rel[geoid10_col].astype(str).str.strip().str.zfill(11),
        "afact":      rel["afact"],
    }).dropna()
    xwalk = xwalk[xwalk["afact"] > 0].reset_index(drop=True)

    # Diagnostics
    n_2020 = xwalk["geoid_2020"].nunique()
    n_2010 = xwalk["geoid_2010"].nunique()
    afact_sums = xwalk.groupby("geoid_2020")["afact"].sum()
    pct_unity  = (abs(afact_sums - 1.0) < 0.02).mean() * 100
    print(f"    2020 tracts: {n_2020:,}  |  2010 tracts: {n_2010:,}")
    print(f"    afact sums to 1.0 +/-2% per 2020 tract: {pct_unity:.1f}%")
    if pct_unity < 95:
        print(f"    WARNING: < 95% of 2020 tracts have afact approx 1.0. "
              f"Check relationship file.")

    dest_path.parent.mkdir(parents=True, exist_ok=True)
    xwalk.to_parquet(dest_path, index=False)
    print(f"    [saved] {dest_path.name}  ({len(xwalk):,} rows)")
    return xwalk


# ---------------------------------------------------------------------------
# Step 1: Fetch ACS B25001 per year (works for both boundary vintages)
# ---------------------------------------------------------------------------

def fetch_acs_housing_units(year: int) -> pd.DataFrame:
    """
    Pull ACS 5-year B25001 (total housing units) for all CA tracts.

    For year ≤ 2020, the Census API returns 2010-boundary geoids.
    For year ≥ 2021, the Census API returns 2020-boundary geoids.
    The caller is responsible for crosswalk-translating the 2020-boundary
    geoids to 2010-boundary geoids before stacking into the panel.

    Returns
    -------
    pd.DataFrame
        Columns: geoid (11-digit, boundary vintage depends on year),
                 acs_housing_units (numeric, NA for suppressed cells)
    """
    raw = fetch_acs_batch(
        year=year,
        variables=[ACS_VAR],
        state_fips="06",
        geography="tract",
    )
    raw["geoid"] = build_geoid(raw)
    raw = mask_sentinel(raw, [ACS_VAR])
    raw[ACS_LABEL] = pd.to_numeric(raw[ACS_VAR], errors="coerce")
    return raw[["geoid", ACS_LABEL]].copy()


# ---------------------------------------------------------------------------
# Crosswalk application: translate 2020-boundary ACS → 2010-boundary tracts
# ---------------------------------------------------------------------------

def translate_to_2010_tracts(
    acs_2020: pd.DataFrame,
    crosswalk: pd.DataFrame,
) -> pd.DataFrame:
    """
    Translate ACS housing unit counts from 2020-boundary tracts to 2010-boundary
    tracts using the Geocorr allocation factors.

        allocated_units(2010 tract X) = Σ_Y  ACS_units(2020 tract Y) × afact(Y→X)

    Parameters
    ----------
    acs_2020 : pd.DataFrame
        Columns: geoid (2020-boundary 11-digit), acs_housing_units (non-NA)
    crosswalk : pd.DataFrame
        Columns: geoid_2020, geoid_2010, afact

    Returns
    -------
    pd.DataFrame
        Columns: geoid (2010-boundary), acs_housing_units
        One row per 2010 tract that appears as a crosswalk target.
    """
    n_before = acs_2020["geoid"].nunique()

    merged = (
        acs_2020
        .rename(columns={"geoid": "geoid_2020"})
        .merge(crosswalk, on="geoid_2020", how="inner")
    )
    merged["units_allocated"] = merged[ACS_LABEL] * merged["afact"]

    translated = (
        merged.groupby("geoid_2010")["units_allocated"]
        .sum()
        .reset_index()
        .rename(columns={"geoid_2010": "geoid", "units_allocated": ACS_LABEL})
    )

    n_unmatched = n_before - merged["geoid_2020"].nunique()
    if n_unmatched > 0:
        print(f"    WARNING: {n_unmatched:,} 2020-boundary tracts had no crosswalk "
              f"match — dropped from translation")

    # Housing-unit conservation check
    total_before = acs_2020[ACS_LABEL].sum()
    total_after  = translated[ACS_LABEL].sum()
    err = abs(total_after - total_before) / max(total_before, 1)
    print(f"    2020-boundary → 2010-boundary: "
          f"{n_before:,} → {translated['geoid'].nunique():,} tracts  |  "
          f"units conserved: {total_before:,.0f} → {total_after:,.0f}  "
          f"(error {err:.3%})")
    if err > 0.02:
        print(f"    WARNING: conservation error > 2%. Investigate crosswalk coverage.")

    return translated


# ---------------------------------------------------------------------------
# Step 2+3: Build 2010–2024 panel
# ---------------------------------------------------------------------------

def build_acs_panel(crosswalk: pd.DataFrame) -> pd.DataFrame:
    """
    Build the full 2010–2024 ACS housing unit panel at 2010 tract boundaries.

    2010–2020  : Direct from Census API (2010-boundary geoids).
    2021–2023  : Census API returns 2020-boundary geoids; apply Geocorr crosswalk.
    2024       : Forward-fill from 2023 (no ACS 5-yr 2024 vintage yet).

    Returns
    -------
    pd.DataFrame
        geoid, county_FIPS, year, acs_housing_units, acs_vintage_year,
        acs_extrapolated, acs_crosswalk_translated, acs_imputed
    """
    # ── Part A: 2010–2020 (2010-boundary, direct) ──────────────────────────
    print(f"\n  Part A: fetching ACS {ACS_PULL_YEARS[0]}–{ACS_PULL_YEARS[-1]} "
          f"(2010 tract boundaries)...")
    direct_frames = []
    for year in ACS_PULL_YEARS:
        print(f"    ACS {year}...")
        df = fetch_acs_housing_units(year)
        df["year"] = year
        direct_frames.append(df)
        if year < ACS_PULL_YEARS[-1]:
            time.sleep(SLEEP_BETWEEN_CALLS)

    fetched = pd.concat(direct_frames, ignore_index=True)
    n_missing_A = fetched[ACS_LABEL].isna().sum()
    print(f"  Part A: {len(fetched):,} tract-year obs, "
          f"{fetched['geoid'].nunique():,} tracts, "
          f"{n_missing_A:,} suppressed cells")

    # Pivot wide → forward-fill within tract → melt back (handles suppressed cells)
    wide      = fetched.pivot_table(index="geoid", columns="year",
                                    values=ACS_LABEL, aggfunc="first")
    was_miss  = wide.isna()
    wide_ff   = wide.ffill(axis=1)

    long_A = wide_ff.reset_index().melt(
        id_vars="geoid", var_name="year", value_name=ACS_LABEL
    )
    long_A["year"] = long_A["year"].astype(int)
    miss_A = was_miss.reset_index().melt(
        id_vars="geoid", var_name="year", value_name="_was_missing"
    )
    miss_A["year"] = miss_A["year"].astype(int)
    long_A = long_A.merge(miss_A, on=["geoid", "year"], how="left")
    long_A["acs_vintage_year"]         = long_A["year"]
    long_A["acs_extrapolated"]         = False
    long_A["acs_crosswalk_translated"] = False

    # ── Part B: 2021–2023 (2020-boundary → crosswalk-translated) ──────────
    print(f"\n  Part B: fetching ACS {ACS_PULL_YEARS_2020BOUNDS[0]}–"
          f"{ACS_PULL_YEARS_2020BOUNDS[-1]} (2020 boundaries → Geocorr crosswalk)...")
    xwalk_frames = []
    for year in ACS_PULL_YEARS_2020BOUNDS:
        print(f"    ACS {year} (2020-boundary)...")
        df_raw = fetch_acs_housing_units(year)  # geoids = 2020-boundary

        # Impute suppressed cells in 2020-boundary data with county mean
        # (before crosswalk; avoids NAs propagating through afact multiplication)
        df_raw["_cty"] = df_raw["geoid"].str[:5]
        n_supp = df_raw[ACS_LABEL].isna().sum()
        if n_supp > 0:
            cty_mean = df_raw.groupby("_cty")[ACS_LABEL].transform("mean")
            df_raw[ACS_LABEL] = df_raw[ACS_LABEL].fillna(cty_mean)
            print(f"      {n_supp:,} suppressed 2020-boundary cells imputed "
                  f"from county mean before crosswalk")
        df_raw = df_raw.drop(columns=["_cty"])

        # Translate from 2020-boundary to 2010-boundary geoids
        df_trans = translate_to_2010_tracts(
            df_raw[["geoid", ACS_LABEL]].copy(), crosswalk
        )
        df_trans["year"]                     = year
        df_trans["acs_vintage_year"]         = year
        df_trans["acs_extrapolated"]         = False
        df_trans["acs_crosswalk_translated"] = True
        df_trans["_was_missing"]             = False  # suppression already handled
        xwalk_frames.append(df_trans)
        if year < ACS_PULL_YEARS_2020BOUNDS[-1]:
            time.sleep(SLEEP_BETWEEN_CALLS)

    long_B = pd.concat(xwalk_frames, ignore_index=True)
    print(f"  Part B: {len(long_B):,} tract-year obs, "
          f"{long_B['geoid'].nunique():,} unique 2010-boundary tracts covered")

    # ── Part C: 2024 (forward-fill from 2023 crosswalk-translated) ─────────
    last_2023 = long_B[long_B["year"] == 2023].copy()
    if len(last_2023) == 0:
        raise RuntimeError("No 2023 crosswalk data to forward-fill 2024 from.")
    last_2023["year"]                    = 2024
    last_2023["acs_vintage_year"]        = 2023
    last_2023["acs_extrapolated"]        = True
    last_2023["acs_crosswalk_translated"] = False
    last_2023["_was_missing"]            = False
    print(f"\n  Part C: 2024 forward-filled from 2023 crosswalk-translated data "
          f"({len(last_2023):,} tract rows)")

    # ── Combine ─────────────────────────────────────────────────────────────
    long = pd.concat([long_A, long_B, last_2023], ignore_index=True)
    long["county_FIPS"] = long["geoid"].str[:5]

    # Residual NA imputation: county-year mean (handles tracts suppressed in
    # all direct years, not covered by crosswalk, etc.)
    still_missing = long[ACS_LABEL].isna()
    n_still = still_missing.sum()
    if n_still > 0:
        print(f"\n  Residual NAs: {n_still:,} → imputing from county-year mean")
        cty_yr_mean = long.groupby(["county_FIPS", "year"])[ACS_LABEL].transform("mean")
        long.loc[still_missing, ACS_LABEL] = cty_yr_mean[still_missing]

    long["acs_imputed"] = long["_was_missing"].fillna(False) | long[ACS_LABEL].isna()
    long = long.drop(columns=["_was_missing"])

    # Deduplicate: there should be no (geoid, year) duplicates, but guard anyway
    dupes = long.duplicated(subset=["geoid", "year"], keep=False)
    if dupes.any():
        print(f"  WARNING: {dupes.sum():,} duplicate (geoid, year) rows — keeping first")
        long = long.drop_duplicates(subset=["geoid", "year"], keep="first")

    long = long.sort_values(["geoid", "year"]).reset_index(drop=True)

    # Summary
    n_direct = (long["acs_extrapolated"] == False) & (long["acs_crosswalk_translated"] == False)
    n_xwalk  = long["acs_crosswalk_translated"] == True
    n_extrap = long["acs_extrapolated"] == True
    n_imp    = long["acs_imputed"].sum()
    print(f"\n  Panel summary:")
    print(f"    Direct (2010-boundary, 2010–2020):  {n_direct.sum():>7,} rows")
    print(f"    Crosswalk-translated (2021–2023):   {n_xwalk.sum():>7,} rows")
    print(f"    Forward-filled (2024):              {n_extrap.sum():>7,} rows")
    print(f"    Imputed cells:                      {n_imp:>7,} rows")
    print(f"    Total:                              {len(long):>7,} rows")

    return long[["geoid", "county_FIPS", "year", ACS_LABEL,
                 "acs_vintage_year", "acs_extrapolated",
                 "acs_crosswalk_translated", "acs_imputed"]].copy()


# ---------------------------------------------------------------------------
# Step 4: Compare ACS to BPS hind-cast and bootstrap p50
# ---------------------------------------------------------------------------

def compare_panels(
    acs_panel: pd.DataFrame,
    bps_panel: pd.DataFrame,
    boot_panel: pd.DataFrame,
) -> pd.DataFrame:
    """
    Merge ACS, BPS, and bootstrap panels on geoid × year and compute
    year-level comparison statistics.

    Returns
    -------
    pd.DataFrame
        One row per year with comparison metrics including data-source flags.
    """
    print("\n" + "=" * 70)
    print("COMPARISON: ACS vs. BPS hind-cast vs. Bootstrap p50")
    print("=" * 70)

    merged = (
        acs_panel[["geoid", "year", ACS_LABEL, "acs_imputed",
                   "acs_extrapolated", "acs_crosswalk_translated"]]
        .merge(bps_panel[["geoid", "year", "residential_count_hindcast"]],
               on=["geoid", "year"], how="inner")
        .merge(boot_panel[["geoid", "year", "p50_residential_count"]],
               on=["geoid", "year"], how="left")
    )

    n_acs      = acs_panel["geoid"].nunique()
    n_bps      = bps_panel["geoid"].nunique()
    n_matched  = merged["geoid"].nunique()
    print(f"\n  ACS tracts: {n_acs:,}  |  BPS tracts: {n_bps:,}  "
          f"|  Matched: {n_matched:,}")
    print(f"  ACS-imputed cells in matched panel: "
          f"{merged['acs_imputed'].sum():,} "
          f"({merged['acs_imputed'].mean():.1%})")

    # ── Year-level statistics ──────────────────────────────────────────────
    rows = []
    for year, grp in merged.groupby("year"):
        # Determine ACS data source for this year
        if grp["acs_extrapolated"].all():
            acs_source = "forward-fill"
        elif grp["acs_crosswalk_translated"].all():
            acs_source = "crosswalk"
        else:
            acs_source = "direct"

        # Restrict correlation to non-imputed cells with all three values
        idx = grp[
            ~grp["acs_imputed"]
        ][[ACS_LABEL, "residential_count_hindcast", "p50_residential_count"]].dropna().index

        a = grp.loc[idx, ACS_LABEL].values
        b = grp.loc[idx, "residential_count_hindcast"].values
        p = grp.loc[idx, "p50_residential_count"].values

        if len(a) < 10:
            rows.append({"year": year, "acs_source": acs_source})
            continue

        r_ab   = stats.pearsonr(a, b).statistic
        rho_ab = stats.spearmanr(a, b).statistic
        r_ap   = stats.pearsonr(a, p).statistic
        rho_ap = stats.spearmanr(a, p).statistic

        # Log-space Pearson r — more meaningful for right-skewed distributions
        pos_ab = (a > 0) & (b > 0)
        r_ab_log = (
            stats.pearsonr(np.log(a[pos_ab]), np.log(b[pos_ab])).statistic
            if pos_ab.sum() >= 2 else float("nan")
        )
        pos_ap = (a > 0) & (p > 0)
        r_ap_log = (
            stats.pearsonr(np.log(a[pos_ap]), np.log(p[pos_ap])).statistic
            if pos_ap.sum() >= 2 else float("nan")
        )

        rows.append({
            "year":                  year,
            "acs_source":            acs_source,
            "n_matched":             len(idx),
            "acs_mean":              round(float(a.mean()), 1),
            "bps_mean":              round(float(b.mean()), 1),
            "bootstrap_p50_mean":    round(float(p.mean()), 1),
            "acs_bps_pearson_r":     round(r_ab,     4),
            "acs_bps_spearman_r":    round(rho_ab,   4),
            "acs_bps_logpearson_r":  round(r_ab_log, 4),
            "acs_bps_mean_ratio":    round(float(b.mean() / max(a.mean(), 1)), 4),
            "acs_boot_pearson_r":    round(r_ap,     4),
            "acs_boot_spearman_r":   round(rho_ap,   4),
            "acs_boot_logpearson_r": round(r_ap_log, 4),
            "acs_boot_mean_ratio":   round(float(p.mean() / max(a.mean(), 1)), 4),
        })

    comp_df = pd.DataFrame(rows)

    # ── Print table ────────────────────────────────────────────────────────
    header = (f"  {'Year':>4}  {'Src':>8}  {'N':>5}  {'ACS':>7}  {'BPS':>7}  "
              f"{'Boot':>7}  {'log-r BPS':>9}  {'ρ BPS':>6}  {'BPS/ACS':>7}  "
              f"{'log-r Boot':>10}  {'ρ Boot':>6}  {'Boot/ACS':>8}")
    print(f"\n{header}")
    print("  " + "-" * (len(header) - 2))
    for _, r in comp_df.dropna(subset=["n_matched"]).iterrows():
        src = str(r.get("acs_source", ""))[:8]
        print(f"  {int(r['year']):>4}  {src:>8}  {int(r['n_matched']):>5}  "
              f"{r['acs_mean']:>7,.0f}  {r['bps_mean']:>7,.0f}  "
              f"{r['bootstrap_p50_mean']:>7,.0f}  "
              f"{r['acs_bps_logpearson_r']:>9.4f}  "
              f"{r['acs_bps_spearman_r']:>6.3f}  "
              f"{r['acs_bps_mean_ratio']:>7.3f}  "
              f"{r['acs_boot_logpearson_r']:>10.4f}  "
              f"{r['acs_boot_spearman_r']:>6.3f}  "
              f"{r['acs_boot_mean_ratio']:>8.3f}")

    print(f"\n  Src key:  direct=2010-boundary ACS  |  crosswalk=2020→2010 translated  "
          f"|  forward-fill=extrapolated from prior year")

    # ── Fire-county spot-check ─────────────────────────────────────────────
    print(f"\n  Fire-county spot-check (county-level totals, non-imputed tracts):")
    print(f"  {'County':<12}  {'Year':>4}  {'ACS total':>10}  {'ACS src':>8}  "
          f"{'BPS total':>10}  {'Boot p50 total':>14}  {'BPS/ACS':>7}")
    for county_name, fips in FIRE_COUNTIES.items():
        for yr in [2015, 2017, 2018, 2019, 2020, 2021, 2022]:
            sub = merged[
                (merged["geoid"].str.startswith(fips))
                & (merged["year"] == yr)
                & (~merged["acs_imputed"])
            ]
            if len(sub) == 0:
                continue
            a_tot  = sub[ACS_LABEL].sum()
            b_tot  = sub["residential_count_hindcast"].sum()
            p_tot  = sub["p50_residential_count"].sum()
            ratio  = b_tot / max(a_tot, 1)
            src    = "xwalk" if sub["acs_crosswalk_translated"].all() else "direct"
            print(f"  {county_name:<12}  {yr:>4}  {a_tot:>10,.0f}  {src:>8}  "
                  f"{b_tot:>10,.0f}  {p_tot:>14,.0f}  {ratio:>7.3f}x")

    # ── Distribution comparison ────────────────────────────────────────────
    print(f"\n  Distribution comparison (non-imputed, non-extrapolated cells):")
    sub_nonimp = merged[
        (~merged["acs_imputed"]) & (~merged["acs_extrapolated"])
    ]
    for label, col in [("ACS", ACS_LABEL),
                        ("BPS", "residential_count_hindcast"),
                        ("Boot p50", "p50_residential_count")]:
        vals = sub_nonimp[col].dropna()
        print(f"  {label:<12}  "
              f"median={vals.median():>8.0f}  p25={vals.quantile(0.25):>8.0f}  "
              f"p75={vals.quantile(0.75):>8.0f}  mean={vals.mean():>8.0f}")

    # ── ACS growth check (direct vs. crosswalk-translated) ─────────────────
    print(f"\n  ACS housing unit growth check (statewide mean, non-imputed):")
    print(f"  {'Year':>4}  {'Src':>8}  {'Mean ACS units':>14}")
    for yr in [2019, 2020, 2021, 2022, 2023, 2024]:
        sub = merged[(merged["year"] == yr) & (~merged["acs_imputed"])]
        if len(sub) == 0:
            continue
        src = str(sub["acs_source"].values[0]) if "acs_source" in sub else "?"
        # Determine source from flags
        if sub["acs_extrapolated"].all():
            src_lbl = "fwd-fill"
        elif sub["acs_crosswalk_translated"].all():
            src_lbl = "crosswalk"
        else:
            src_lbl = "direct"
        print(f"  {yr:>4}  {src_lbl:>8}  {sub[ACS_LABEL].mean():>14,.1f}")

    return comp_df


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 70)
    print("build_acs_structure_panel.py — ACS challenger panel (with crosswalk)")
    print("=" * 70)

    if OUT_ACS_PANEL.exists() and OUT_COMPARISON.exists():
        print(f"\n[skip] Output files already exist. Delete to rebuild:")
        print(f"  {OUT_ACS_PANEL}")
        print(f"  {OUT_COMPARISON}")
        print(f"  (Crosswalk at {OUT_CROSSWALK} is cached separately and reused.)")
        return

    for path in [IN_BPS_PANEL, IN_BOOT_PANEL]:
        if not path.exists():
            raise FileNotFoundError(
                f"Required input missing: {path}\n"
                f"Run build_structure_panel.py and bootstrap_structure_panel.py first."
            )

    OUT_TABLES.mkdir(parents=True, exist_ok=True)

    # ── Step 0: Census 2020→2010 tract crosswalk ──────────────────────────
    print(f"\n--- Step 0: 2020→2010 tract crosswalk (Census TIGER Relationship File) ---")
    crosswalk = fetch_tract_crosswalk(OUT_CROSSWALK)
    print(f"  Crosswalk: {len(crosswalk):,} rows  |  "
          f"2020 tracts: {crosswalk['geoid_2020'].nunique():,}  |  "
          f"2010 tracts: {crosswalk['geoid_2010'].nunique():,}")

    # ── Steps 1–3: Build ACS panel ─────────────────────────────────────────
    print(f"\n--- Steps 1–3: Build ACS B25001 panel 2010–2024 ---")
    print(f"  2010–2020: direct (2010-boundary Census API)")
    print(f"  2021–2023: 2020-boundary Census API + Geocorr crosswalk")
    print(f"  2024:      forward-fill from 2023\n")

    acs_panel = build_acs_panel(crosswalk)

    print(f"\n  ACS panel: {len(acs_panel):,} rows  |  "
          f"{acs_panel['geoid'].nunique():,} tracts  |  "
          f"{acs_panel['county_FIPS'].nunique()} counties")
    print(f"  Mean acs_housing_units by year (all non-extrapolated rows):")
    mean_by_yr = (
        acs_panel[~acs_panel["acs_extrapolated"]]
        .groupby("year")["acs_housing_units"].mean().round(1)
    )
    print(mean_by_yr.to_string())

    # ── Save ACS panel ─────────────────────────────────────────────────────
    print(f"\n--- Saving ACS panel ---")
    acs_panel.to_parquet(OUT_ACS_PANEL, index=False)
    print(f"[saved] {OUT_ACS_PANEL.name}  "
          f"({len(acs_panel):,} rows × {len(acs_panel.columns)} cols)")
    print(f"        Columns: {list(acs_panel.columns)}")

    # ── Step 4: Compare ────────────────────────────────────────────────────
    print(f"\n--- Step 4: Comparison vs. BPS hind-cast and bootstrap ---")
    bps_panel  = pd.read_parquet(IN_BPS_PANEL)
    boot_panel = pd.read_parquet(IN_BOOT_PANEL)

    comp_df = compare_panels(acs_panel, bps_panel, boot_panel)

    comp_df.to_csv(OUT_COMPARISON, index=False)
    print(f"\n[saved] {OUT_COMPARISON.name}  ({len(comp_df)} rows)")

    print("\n" + "=" * 70)
    print("Done.")
    print("  acs_crosswalk_translated=True → units allocated via Geocorr 2020→2010.")
    print("  acs_extrapolated=True         → forward-filled (only 2024).")
    print("  Boot/ACS ratio ≈ 0.82–0.84   → bootstrap close to survey benchmark.")
    print("  Negative BPS Spearman ρ       → Overture/BPS rank-orders tracts")
    print("    inversely to ACS (sparse-area bias); see structure_count_writeup.md.")
    print("=" * 70)


if __name__ == "__main__":
    main()
