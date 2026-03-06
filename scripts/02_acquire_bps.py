"""
02_acquire_bps.py
=================
Download and parse Census Bureau Building Permits Survey (BPS) county-level
annual files for California, years 2010–2024.

The BPS provides the number of new residential units authorized by building
permits, disaggregated by structure type (1-unit, 2-unit, 3-4 unit, 5+ unit).
These counts are used in 04_build_structure_panel.py to hind-cast county-level
residential structure counts backward from the 2024 Overture Maps anchor.

Source
------
  https://www2.census.gov/econ/bps/County/co{YEAR}a.txt

File format (confirmed from co2024a.txt)
-----------------------------------------
  Two header rows; data begins on row 3. After skipping headers:
    Col 0:  survey_date   4-digit year
    Col 1:  state_fips    2-digit, zero-padded
    Col 2:  county_fips   3-digit, zero-padded
    Col 3:  region_code
    Col 4:  division_code
    Col 5:  county_name
    Cols 6–8:   1-unit  Bldgs / Units / Value
    Cols 9–11:  2-unit  Bldgs / Units / Value
    Cols 12–14: 3-4 unit Bldgs / Units / Value
    Cols 15–17: 5+ unit Bldgs / Units / Value

  "Units" columns (7, 10, 13, 16) count total housing units authorized —
  preferred over "Bldgs" for multi-family structures.

Outputs
-------
  data/raw/bps/co{YEAR}a.txt        — 15 raw annual files (gitignored)
  data/clean/county_permits_ca_2010_2024.parquet
    Columns: county_FIPS (5-char), year, county_name,
             units_1, units_2, units_34, units_5plus, units_all_res

Usage
-----
  python scripts/02_acquire_bps.py

Dependencies
------------
  utils/download_utils.py, pandas, pyarrow
"""

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent / "utils"))
from download_utils import download_file

# ---------------------------------------------------------------------------
# Paths and constants
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_BPS   = PROJECT_ROOT / "data" / "raw" / "bps"
CLEAN_DIR = PROJECT_ROOT / "data" / "clean"

RAW_BPS.mkdir(parents=True, exist_ok=True)
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

YEARS = list(range(2010, 2025))
BPS_URL_TEMPLATE = "https://www2.census.gov/econ/bps/County/co{year}a.txt"

OUT_CLEAN = CLEAN_DIR / "county_permits_ca_2010_2024.parquet"

# Column positions in BPS county annual files (confirmed from co2024a.txt).
# Positions are 0-indexed after the two header rows are skipped.
# "Units" sub-column (index 1 within each building-type group) is used — not Bldgs.
_COL_SURVEY_DATE = 0
_COL_STATE_FIPS  = 1
_COL_COUNTY_FIPS = 2
_COL_COUNTY_NAME = 5
# Units columns per building type (Bldgs=+0, Units=+1, Value=+2; first group starts at col 6)
_COL_UNITS_1     = 7    # 1-unit structures
_COL_UNITS_2     = 10   # 2-unit structures
_COL_UNITS_34    = 13   # 3-4 unit structures
_COL_UNITS_5PLUS = 16   # 5+ unit structures

CA_STATE_FIPS = "06"

# Unit-to-structure conversion ratios.
# BPS reports authorized *units*, but the Overture anchor counts *structures*.
# Dividing by the ratio converts permitted units → permitted structure footprints
# before the backward cumulative sum.
#
#   1-unit:   1 unit  = 1 structure  (identity, by definition)
#   2-unit:   2 units = 1 structure  (exact, by definition)
#   3-4 unit: (3+4)/2 = 3.5 units/structure  (midpoint, deterministic)
#   5+ unit:  15 units/structure  (PI-confirmed 2026-03-01)
#             Justification: consistent with Census AHS 2021 national median
#             for large multifamily buildings; appropriate for CA urban stock.
_RATIO_1UNIT  = 1.0   # by definition
_RATIO_2UNIT  = 2.0   # by definition
_RATIO_34UNIT = 3.5   # deterministic midpoint
_RATIO_5PLUS  = 15.0  # calibration confirmed by PI 2026-03-01: CA multifamily avg


# ---------------------------------------------------------------------------
# Parse one BPS county annual file
# ---------------------------------------------------------------------------

def parse_bps_file(path: Path, year: int) -> pd.DataFrame:
    """
    Parse a single BPS county annual .txt file and return CA rows only.

    Parameters
    ----------
    path : Path
        Local path to the downloaded co{YEAR}a.txt file.
    year : int
        Survey year (used to assign the 'year' column in output).

    Returns
    -------
    pd.DataFrame
        Rows for California with columns:
        county_FIPS, year, county_name,
        units_1, units_2, units_34, units_5plus, units_all_res

    Notes
    -----
    The BPS files have a two-row header followed by data. skiprows=2 discards
    both header rows; column names are assigned manually from confirmed positions.
    Commas within numeric values (thousands separators) are stripped before
    conversion. Rows that are footnotes, totals, or state-level summaries are
    excluded by the CA state_fips filter.
    """
    # Read all columns as strings; skip two header rows
    try:
        raw = pd.read_csv(
            path,
            skiprows=2,
            header=None,
            dtype=str,
            na_filter=False,
            low_memory=False,
        )
    except Exception as exc:
        print(f"  [warn] Could not read {path.name}: {exc}")
        return pd.DataFrame()

    n_cols = len(raw.columns)
    if n_cols < 18:
        # Some years may have extra columns (rep/est flags); 18 is the minimum.
        print(f"  [warn] {path.name}: expected ≥18 columns, got {n_cols} — skipping year {year}")
        return pd.DataFrame()

    # Sanity check: col 0 of first data row should be a 4-digit year
    first_val = raw.iloc[0, _COL_SURVEY_DATE].strip() if len(raw) > 0 else ""
    if not first_val.isdigit() or len(first_val) != 4:
        print(
            f"  [warn] {path.name}: col 0 of first row is '{first_val}' (expected 4-digit year). "
            f"The two-header-row assumption may be wrong for this year. Attempting to parse anyway."
        )

    # Extract the columns we need
    needed = {
        "state_fips":  _COL_STATE_FIPS,
        "county_fips": _COL_COUNTY_FIPS,
        "county_name": _COL_COUNTY_NAME,
        "units_1":     _COL_UNITS_1,
        "units_2":     _COL_UNITS_2,
        "units_34":    _COL_UNITS_34,
        "units_5plus": _COL_UNITS_5PLUS,
    }
    df = raw.iloc[:, list(needed.values())].copy()
    df.columns = list(needed.keys())

    # Strip whitespace from string fields
    for col in ["state_fips", "county_fips", "county_name"]:
        df[col] = df[col].str.strip()

    # Filter to California
    ca = df[df["state_fips"].str.zfill(2) == CA_STATE_FIPS].copy()
    if len(ca) == 0:
        print(f"  [warn] No CA rows found in {path.name} (state_fips == '{CA_STATE_FIPS}')")
        return pd.DataFrame()

    # Parse unit counts: strip commas (thousands separators), coerce to int
    unit_cols = ["units_1", "units_2", "units_34", "units_5plus"]
    for col in unit_cols:
        ca[col] = (
            pd.to_numeric(ca[col].str.replace(",", "", regex=False), errors="coerce")
            .fillna(0)
            .astype(int)
        )

    # Build output
    ca["county_FIPS"]    = CA_STATE_FIPS + ca["county_fips"].str.zfill(3)
    ca["year"]           = year
    ca["units_all_res"]  = ca[unit_cols].sum(axis=1)

    # Convert authorized units → authorized structure footprints.
    # Multi-unit categories are divided by their unit-per-structure ratio so
    # the backward hind-cast subtracts structures, not units, from the
    # Overture 2024 structure anchor.
    ca["structures_permitted"] = (
        ca["units_1"]     / _RATIO_1UNIT
        + ca["units_2"]   / _RATIO_2UNIT
        + ca["units_34"]  / _RATIO_34UNIT
        + ca["units_5plus"] / _RATIO_5PLUS
    ).round(2)

    out = ca[["county_FIPS", "year", "county_name",
              "units_1", "units_2", "units_34", "units_5plus",
              "units_all_res", "structures_permitted"]].copy()

    # Validate: county_FIPS should be 5-char string starting with "06"
    bad_fips = out[~out["county_FIPS"].str.match(r"^06\d{3}$")]
    if len(bad_fips) > 0:
        print(f"  [warn] {len(bad_fips)} rows with unexpected county_FIPS: {bad_fips['county_FIPS'].unique()}")
        out = out[out["county_FIPS"].str.match(r"^06\d{3}$")].copy()

    return out


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 65)
    print("acquire_bps_permits.py — Census BPS county permits 2010–2024")
    print("=" * 65)

    if OUT_CLEAN.exists():
        print(f"\n[skip] {OUT_CLEAN.name} already exists. Delete to rebuild.")
        return

    all_years = []

    for year in YEARS:
        url   = BPS_URL_TEMPLATE.format(year=year)
        fname = f"co{year}a.txt"
        dest  = RAW_BPS / fname

        print(f"\n--- Year {year} ---")
        download_file(url=url, dest_path=dest)

        year_df = parse_bps_file(dest, year)
        if len(year_df) == 0:
            print(f"  [skip] No usable rows for {year}")
            continue

        n_counties = year_df["county_FIPS"].nunique()
        total_units = year_df["units_all_res"].sum()
        print(
            f"  {n_counties} CA counties  |  "
            f"{total_units:,} total residential units authorized"
        )
        all_years.append(year_df)

    if not all_years:
        raise RuntimeError("No BPS data parsed for any year. Check downloads and file format.")

    panel = pd.concat(all_years, ignore_index=True)

    # Validate panel dimensions
    n_expected_rows = len(YEARS) * 58   # 15 years × 58 CA counties
    n_actual_rows   = len(panel)
    n_county_years  = panel.groupby(["county_FIPS", "year"]).size()
    n_dupes         = (n_county_years > 1).sum()

    print("\n--- Panel summary ---")
    print(f"  Rows: {n_actual_rows:,} (expected ~{n_expected_rows:,} for 58 counties × {len(YEARS)} years)")
    if n_dupes > 0:
        print(f"  [warn] {n_dupes} duplicate county-year combinations — check raw files")
    print(f"  Years: {panel['year'].min()}–{panel['year'].max()}")
    print(f"  Counties: {panel['county_FIPS'].nunique()}")
    print(f"\n  units_all_res by year:")
    annual_totals = panel.groupby("year")["units_all_res"].sum()
    print(annual_totals.to_string())

    # Spot-check Butte County (FIPS 06007) — should show high activity post-2018
    # (Camp Fire rebuild in 2019+)
    butte = panel[panel["county_FIPS"] == "06007"].sort_values("year")
    if len(butte) > 0:
        print(f"\n  Butte County (06007) — spot check (Camp Fire rebuild expected post-2018):")
        print(butte[["year", "units_1", "units_5plus", "units_all_res"]].to_string(index=False))

    panel.to_parquet(OUT_CLEAN, index=False)
    print(f"\n[saved] {OUT_CLEAN.name}")
    print(f"        {n_actual_rows:,} rows × {len(panel.columns)} columns")

    print("\n" + "=" * 65)
    print("Done. Run 03_acquire_dins.py next.")
    print("=" * 65)


if __name__ == "__main__":
    main()
