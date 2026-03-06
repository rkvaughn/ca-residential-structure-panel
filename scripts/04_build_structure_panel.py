"""
04_build_structure_panel.py
===========================
Construct a tract × year panel of estimated residential structure counts for
California, 2010–2024, using a two-step hind-cast + downscale approach.

Method
------
Step A — County-level hind-cast
  1. Sum Overture tract counts to county totals → 2024 anchor (C_county_2024).
  2. For each year t, subtract the cumulative *net* structure change from year t+1
     onward:
       C_county_t = C_county_2024 − Σ_{s=t+1..2024}(structures_permitted_s − dins_destroyed_s)
     where:
       structures_permitted_s = BPS authorized units converted to structure footprints
         (1-unit ÷ 1, 2-unit ÷ 2, 3-4 unit ÷ 3.5, 5+ unit ÷ 15; PI-confirmed 2026-03-01)
       dins_destroyed_s = destroyed residential structures from CAL FIRE DINS data
         (DAMAGE == "Destroyed (>50%)", STRUCTURE CATEGORY contains "residen")
     In fire years, dins_destroyed_s > structures_permitted_s → net change is negative
     → subtracting a negative sum adds pre-fire structures back to earlier years.
  3. Floor at 1 to prevent non-positive denominators.

Step B — Tract-level downscale
  Each tract's share of its county's 2024 Overture count (tract_share) is
  assumed stable over time. Multiplying the county hind-cast by tract_share
  yields the tract-level estimate for each year:
    residential_count_hindcast_it = tract_share_i × C_county_t

  For tracts with zero Overture count (Overture detection gap), an equal-share
  imputation within county is applied:
    tract_share_imputed_i = 1 / n_tracts_in_county_c

  This imputation preserves county-level trends and avoids zero denominators
  in any downstream analysis.

Known biases and limitations
------------------------------
  1. Fire demolitions are not captured in BPS permits. Overture 2024
     undercounts pre-fire structures in wildfire-affected counties. The DINS
     correction recovers pre-fire structure counts at the county level; tract-
     level distribution relies on stable within-county shares.
     Flag this limitation in downstream analyses.

  2. The within-county share assumption holds better in rural, low-construction
     tracts than in fast-growing suburban tracts.

  3. Equal-share imputation for zero-count tracts assigns each such tract an
     equal share of the county hind-cast. In practice, zero-count Overture
     tracts are typically urban or high-density (Overture detection gap for
     multi-unit buildings).

Inputs
------
  data/clean/tract_residential_counts_2024.parquet  (from 01_acquire_overture.py)
  data/clean/county_permits_ca_2010_2024.parquet    (from 02_acquire_bps.py)
  data/clean/dins_county_destroyed_residential.parquet  (from 03_acquire_dins.py)

Output
------
  data/clean/tract_structure_panel.parquet
    Columns: geoid, county_FIPS, year, overture_residential_count_2024,
             tract_share, county_anchor, county_count_hindcast,
             residential_count_hindcast
    Rows: ~6,690 tracts × 15 years ≈ 100,350 rows

Usage
-----
  python scripts/04_build_structure_panel.py

Dependencies
------------
  pandas, pyarrow (numpy transitively via pandas)
"""

import sys
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Paths and constants
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CLEAN_DIR    = PROJECT_ROOT / "data" / "clean"

IN_TRACT_COUNTS = CLEAN_DIR / "tract_residential_counts_2024.parquet"
IN_BPS_PERMITS  = CLEAN_DIR / "county_permits_ca_2010_2024.parquet"
IN_DINS         = CLEAN_DIR / "dins_county_destroyed_residential.parquet"
OUT_PANEL       = CLEAN_DIR / "tract_structure_panel.parquet"

YEARS = list(range(2010, 2025))

# Fire counties for diagnostic spot-check (name, 5-digit FIPS)
FIRE_COUNTIES = {
    "Butte":  "06007",   # Camp Fire 2018
    "Sonoma": "06097",   # Tubbs Fire 2017
    "Shasta": "06089",   # Carr Fire 2018
}


# ---------------------------------------------------------------------------
# Step A: County-level hind-cast
# ---------------------------------------------------------------------------

def build_county_hind_cast(
    tract_counts: pd.DataFrame,
    permits: pd.DataFrame,
    dins: pd.DataFrame,
) -> pd.DataFrame:
    """
    Produce a county × year panel of hind-cast residential structure counts.

    Parameters
    ----------
    tract_counts : DataFrame
        Output of acquire_overture_buildings.py — one row per tract with
        columns: geoid, county_FIPS, overture_residential_count_2024.
    permits : DataFrame
        Output of acquire_bps_permits.py — one row per county × year with
        columns: county_FIPS, year, structures_permitted.
    dins : DataFrame
        Output of acquire_dins_data.py — one row per county × year with
        columns: county_FIPS, year, structures_destroyed.
        Absent rows mean zero destroyed structures for that county-year.

    Returns
    -------
    DataFrame
        One row per county × year with columns:
        county_FIPS, year, county_anchor, county_count_hindcast,
        structures_permitted, dins_destroyed, net_structures_change

    Notes on the backward cumsum
    ----------------------------
    For year t:
      net_change_s   = structures_permitted_s − dins_destroyed_s
      county_count_t = county_anchor_2024 − Σ_{s=t+1}^{2024} net_change_s

    In fire years, dins_destroyed_s may exceed structures_permitted_s, making
    net_change_s negative. Subtracting a negative cumsum from the anchor adds
    destroyed structures back, recovering the pre-fire housing stock.
    """
    # County 2024 anchor: sum Overture tract counts within each county
    county_anchor = (
        tract_counts.groupby("county_FIPS")["overture_residential_count_2024"]
        .sum()
        .reset_index()
        .rename(columns={"overture_residential_count_2024": "county_anchor"})
    )

    n_counties = len(county_anchor)
    print(f"  County anchors computed: {n_counties} counties")
    print(f"  Anchor range: "
          f"min={county_anchor['county_anchor'].min():,} "
          f"max={county_anchor['county_anchor'].max():,} "
          f"mean={county_anchor['county_anchor'].mean():.0f}")

    # Merge permits with county anchor; fill any missing county-years with 0
    perm = permits.copy()
    perm = perm.merge(county_anchor, on="county_FIPS", how="left")

    # Warn if any counties in anchor are missing from permits
    anchor_counties = set(county_anchor["county_FIPS"])
    permit_counties = set(perm["county_FIPS"])
    missing_from_permits = anchor_counties - permit_counties
    if missing_from_permits:
        print(f"  [warn] {len(missing_from_permits)} counties in Overture anchor "
              f"have no BPS permit rows: {sorted(missing_from_permits)}")

    # Merge DINS destroyed counts onto permits (left join; absent = 0 destroyed)
    perm = perm.merge(
        dins[["county_FIPS", "year", "structures_destroyed"]],
        on=["county_FIPS", "year"],
        how="left",
    )
    perm["structures_destroyed"] = perm["structures_destroyed"].fillna(0)
    total_dins_destroyed = int(perm["structures_destroyed"].sum())
    n_fire_years = (perm["structures_destroyed"] > 0).sum()
    print(f"  DINS destroyed structures merged: {total_dins_destroyed:,} "
          f"across {n_fire_years} county-year events")

    # Net change in structures for year s:
    #   net_change_s = structures_permitted_s − dins_destroyed_s
    # Negative in fire years (destroyed > built), which is correct — subtracting
    # a negative cumsum from the anchor recovers pre-fire housing stock.
    perm["net_structures_change"] = perm["structures_permitted"] - perm["structures_destroyed"]

    # Vectorized hind-cast using groupby transform (avoids pandas 2.x apply key-drop issue)
    perm = perm.sort_values(["county_FIPS", "year"]).copy()

    # Reverse cumsum of net_structures_change within each county
    perm["rev_cumsum"] = (
        perm.groupby("county_FIPS")["net_structures_change"]
        .transform(lambda x: x[::-1].cumsum()[::-1])
    )

    # Shift within each county: at year t, gives sum(net_change from t+1 to 2024)
    # fill_value=0 means 2024 has no net change subtracted (anchor year)
    perm["net_after_t"] = (
        perm.groupby("county_FIPS")["rev_cumsum"]
        .transform(lambda x: x.shift(-1, fill_value=0))
    )

    perm["county_count_hindcast"] = (
        (perm["county_anchor"] - perm["net_after_t"]).clip(lower=1).round().astype(int)
    )

    county_panel = perm[["county_FIPS", "year", "county_anchor",
                          "county_count_hindcast", "structures_permitted",
                          "structures_destroyed", "net_structures_change"]].copy()

    # Verify that 2024 hind-cast equals anchor by construction
    check_2024 = county_panel[county_panel["year"] == 2024].copy()
    mismatches = (check_2024["county_count_hindcast"] != check_2024["county_anchor"]).sum()
    if mismatches > 0:
        print(f"  [WARN] {mismatches} counties where hind-cast 2024 ≠ anchor (unexpected)")
    else:
        print(f"  [ok] 2024 hind-cast equals Overture anchor for all counties (as expected)")

    # Summary stats by year
    print("\n  County-level hind-cast summary by year:")
    summary = county_panel.groupby("year")["county_count_hindcast"].agg(["min", "mean", "max"])
    summary.columns = ["min_count", "mean_count", "max_count"]
    summary["mean_count"] = summary["mean_count"].round(0).astype(int)
    print(summary.to_string())

    return county_panel


# ---------------------------------------------------------------------------
# Step B: Tract-level downscale
# ---------------------------------------------------------------------------

def build_tract_panel(
    tract_counts: pd.DataFrame,
    county_panel: pd.DataFrame,
) -> pd.DataFrame:
    """
    Downscale county hind-cast estimates to tract level using each tract's
    proportional share of the county's 2024 Overture count.

    Parameters
    ----------
    tract_counts : DataFrame
        Tract-level Overture counts with columns:
        geoid, county_FIPS, overture_residential_count_2024.
    county_panel : DataFrame
        County × year panel from build_county_hind_cast().

    Returns
    -------
    DataFrame
        Tract × year panel (the final output) with columns:
        geoid, county_FIPS, year, overture_residential_count_2024,
        tract_share, county_anchor, county_count_hindcast,
        residential_count_hindcast
    """
    # Merge county anchor onto tract data
    county_anchor_map = county_panel[["county_FIPS", "county_anchor"]].drop_duplicates()
    tracts = tract_counts.merge(county_anchor_map, on="county_FIPS", how="left")

    # ── Tract share computation ────────────────────────────────────────────────
    # For tracts with non-zero Overture count: share = count / county_anchor
    # For tracts with zero count: equal-share imputation within county
    n_per_county = tracts.groupby("county_FIPS")["geoid"].transform("count")
    zero_mask = tracts["overture_residential_count_2024"] == 0
    n_zero = zero_mask.sum()

    if n_zero > 0:
        pct_zero = n_zero / len(tracts)
        print(f"\n  Zero Overture count tracts: {n_zero:,} ({pct_zero:.1%})")
        print(f"  Applying equal-share imputation for zero-count tracts "
              f"(each receives 1/n_tracts_in_county share of county hind-cast)")

    # Compute effective count for share calculation
    # Zero-count tracts: imputed_count = county_anchor / n_per_county
    # (numerically equivalent to uniform share = 1/n_per_county)
    tracts["count_for_share"] = tracts["overture_residential_count_2024"].where(
        ~zero_mask,
        other=(tracts["county_anchor"] / n_per_county).round(4),
    )

    # Recompute county total after imputation (for share denominator)
    county_total_for_share = (
        tracts.groupby("county_FIPS")["count_for_share"]
        .transform("sum")
    )
    tracts["tract_share"] = (tracts["count_for_share"] / county_total_for_share).round(6)

    # Verify shares sum to ~1.0 within each county
    share_sum_check = tracts.groupby("county_FIPS")["tract_share"].sum()
    share_deviations = (share_sum_check - 1.0).abs()
    max_deviation = share_deviations.max()
    if max_deviation > 0.01:
        print(f"  [warn] Max within-county share sum deviation from 1.0: {max_deviation:.6f}")
    else:
        print(f"  [ok] Tract shares sum to 1.0 within each county "
              f"(max deviation: {max_deviation:.6f})")

    # ── Cross-join tracts with years ───────────────────────────────────────────
    # Merge tract data onto county_panel (county_FIPS is the join key)
    # county_panel has 1 row per (county_FIPS × year); each tract gets all 15 years
    panel = tracts[["geoid", "county_FIPS", "overture_residential_count_2024",
                    "tract_share", "county_anchor"]].merge(
        county_panel[["county_FIPS", "year", "county_count_hindcast",
                       "structures_permitted", "structures_destroyed",
                       "net_structures_change"]],
        on="county_FIPS",
        how="left",
    )

    # ── Hind-cast tract count ──────────────────────────────────────────────────
    # residential_count_hindcast_it = tract_share_i × county_count_hindcast_t
    panel["residential_count_hindcast"] = (
        panel["tract_share"] * panel["county_count_hindcast"]
    ).clip(lower=1).round(2)

    # Sanity check: 2024 counts should equal overture_residential_count_2024
    # (approximately — small rounding differences from share computation allowed)
    panel_2024 = panel[panel["year"] == 2024].copy()
    max_2024_diff = (
        panel_2024["residential_count_hindcast"] - panel_2024["overture_residential_count_2024"]
    ).abs().max()
    if max_2024_diff > 1.0:
        print(f"\n  [warn] Max 2024 hind-cast vs Overture count diff: {max_2024_diff:.2f} "
              f"(small rounding expected; large values indicate an error)")
    else:
        print(f"\n  [ok] 2024 hind-cast closely matches Overture anchor "
              f"(max tract diff: {max_2024_diff:.2f})")

    # Final column order
    panel = panel[["geoid", "county_FIPS", "year",
                   "overture_residential_count_2024", "tract_share",
                   "county_anchor", "county_count_hindcast",
                   "structures_permitted", "structures_destroyed",
                   "net_structures_change",
                   "residential_count_hindcast"]].sort_values(["geoid", "year"])

    return panel


# ---------------------------------------------------------------------------
# Diagnostics
# ---------------------------------------------------------------------------

def print_diagnostics(panel: pd.DataFrame) -> None:
    """
    Print diagnostic statistics to stdout.

    Covers:
    - Panel dimensions and row counts
    - Small-cell tracts (hind-cast < 10 units)
    - Fire-county hind-cast vs Overture 2024 comparison
    - Mean residential_count_hindcast by year (sanity check for downward trend)
    """
    print("\n" + "=" * 65)
    print("DIAGNOSTICS")
    print("=" * 65)

    # Panel dimensions
    n_tracts = panel["geoid"].nunique()
    n_years  = panel["year"].nunique()
    n_rows   = len(panel)
    print(f"\n  Panel: {n_tracts:,} tracts × {n_years} years = {n_rows:,} rows")
    print(f"  Years: {panel['year'].min()}–{panel['year'].max()}")
    print(f"  Counties: {panel['county_FIPS'].nunique()}")

    # Small-cell tracts (residential_count_hindcast < 10 in any year)
    small_tracts = panel[panel["residential_count_hindcast"] < 10]
    n_small_cells = len(small_tracts)
    n_small_tracts = small_tracts["geoid"].nunique()
    print(f"\n  Small-cell rows (count < 10): {n_small_cells:,} "
          f"across {n_small_tracts:,} tracts")
    if n_small_tracts > 0:
        print(f"  [note] Small-cell tracts should be suppressed or flagged "
              f"in any downstream rate computation.")

    # Mean hind-cast by year (expect lower values in earlier years)
    print(f"\n  Mean residential_count_hindcast by year "
          f"(should increase toward 2024):")
    mean_by_year = panel.groupby("year")["residential_count_hindcast"].mean().round(1)
    print(mean_by_year.to_string())

    # Fire-county spot-check
    print(f"\n  Fire-county hind-cast vs Overture 2024 anchor:")
    print(f"  {'County':<12}  {'county_anchor':>14}  {'count_2010':>12}  {'count_2018':>12}  {'count_2024':>12}")
    for county_name, fips in FIRE_COUNTIES.items():
        sub = panel[panel["county_FIPS"] == fips]
        if len(sub) == 0:
            print(f"  {county_name:<12}  [not found in panel]")
            continue
        anchor  = int(sub["county_anchor"].iloc[0])
        c2010   = int(sub[sub["year"] == 2010]["county_count_hindcast"].values[0]) if 2010 in sub["year"].values else -1
        c2018   = int(sub[sub["year"] == 2018]["county_count_hindcast"].values[0]) if 2018 in sub["year"].values else -1
        c2024   = int(sub[sub["year"] == 2024]["county_count_hindcast"].values[0]) if 2024 in sub["year"].values else -1
        print(f"  {county_name:<12}  {anchor:>14,}  {c2010:>12,}  {c2018:>12,}  {c2024:>12,}")

    # Distribution of residential_count_hindcast
    print(f"\n  residential_count_hindcast distribution (all tract-years):")
    print(panel["residential_count_hindcast"].describe().round(1).to_string())


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 65)
    print("build_structure_panel.py — Tract × year structure count panel")
    print("=" * 65)

    if OUT_PANEL.exists():
        print(f"\n[skip] {OUT_PANEL.name} already exists. Delete to rebuild.")
        return

    # ── Load inputs ───────────────────────────────────────────────────────────
    print("\n--- Loading inputs ---")

    for path in [IN_TRACT_COUNTS, IN_BPS_PERMITS, IN_DINS]:
        if not path.exists():
            raise FileNotFoundError(
                f"Required input not found: {path}\n"
                f"Run the corresponding acquisition script first:\n"
                f"  acquire_overture_buildings.py → tract_residential_counts_2024\n"
                f"  acquire_bps_permits.py        → county_permits_ca_2010_2024\n"
                f"  acquire_dins_data.py           → dins_county_destroyed_residential"
            )

    tract_counts = pd.read_parquet(IN_TRACT_COUNTS)
    print(f"  tract_residential_counts_2024: {len(tract_counts):,} rows")
    print(f"  Counties in tract data: {tract_counts['county_FIPS'].nunique()}")

    permits = pd.read_parquet(IN_BPS_PERMITS)
    print(f"  county_permits_ca_2010_2024:   {len(permits):,} rows")
    print(f"  Years in permits: {permits['year'].min()}–{permits['year'].max()}")

    dins = pd.read_parquet(IN_DINS)
    print(f"  dins_county_destroyed_residential: {len(dins):,} county-year events")
    print(f"  Total DINS destroyed residential: {dins['structures_destroyed'].sum():,}")

    # ── Step A: County hind-cast ──────────────────────────────────────────────
    print("\n--- Step A: County-level hind-cast ---")
    county_panel = build_county_hind_cast(tract_counts, permits, dins)

    # ── Step B: Tract downscale ───────────────────────────────────────────────
    print("\n--- Step B: Tract-level downscale ---")
    panel = build_tract_panel(tract_counts, county_panel)

    # ── Diagnostics ───────────────────────────────────────────────────────────
    print_diagnostics(panel)

    # ── Save ──────────────────────────────────────────────────────────────────
    print("\n--- Saving output ---")
    panel.to_parquet(OUT_PANEL, index=False)
    print(f"[saved] {OUT_PANEL.name}")
    print(f"        {len(panel):,} rows × {len(panel.columns)} columns")
    print(f"        Columns: {list(panel.columns)}")

    print("\n" + "=" * 65)
    print("Done. tract_structure_panel.parquet ready. Run 05_bootstrap_structure_panel.py next.")
    print("=" * 65)


if __name__ == "__main__":
    main()
