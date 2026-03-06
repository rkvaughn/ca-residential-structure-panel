"""
03_acquire_dins.py
====================
Download and process CAL FIRE Damage Inspection (DINS) data — the official
record of structures damaged or destroyed by wildfires in California.

Source
------
California Natural Resources Agency (CNRA) GIS Portal:
  https://gis.data.cnra.ca.gov/datasets/CALFIRE-Forestry::cal-fire-damage-inspection-dins-data

Coverage: 2013–present. Prior to 2018, only damaged/destroyed structures
were collected (so the destroyed filter is complete across our full period).
From 2018 onward, all structures in or near fire perimeters are recorded.

Methodology note
----------------
DINS records individual structure inspections with a damage rating and
structure type. We filter to:
  - damagerating == "Destroyed"              (50–100% damage)
  - structuretype contains "Residential"     (case-insensitive)
Then aggregate to county_FIPS × year counts of destroyed residential structures.

This output is used by build_structure_panel.py and bootstrap_structure_panel.py
to correct the backward hind-cast: for years before a fire event, the hind-cast
must add back destroyed structures to recover the pre-fire housing stock.

Outputs
-------
  data/raw/dins/dins_ca.csv                     — raw download (gitignored)
  data/clean/dins_county_destroyed_residential.parquet
    Columns: county_FIPS, year, structures_destroyed
    One row per county × year with ≥1 destroyed residential structure.
    Counties × years with no fires are absent (treated as 0 in merge).

Usage
-----
  python scripts/01_build/03_acquire_dins.py

Dependencies
------------
  pandas, pyarrow, requests (via download_utils)
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
RAW_DINS     = PROJECT_ROOT / "data" / "raw" / "dins"
CLEAN_DIR    = PROJECT_ROOT / "data" / "clean"

RAW_DINS.mkdir(parents=True, exist_ok=True)
CLEAN_DIR.mkdir(parents=True, exist_ok=True)

RAW_CSV  = RAW_DINS / "dins_ca.csv"
OUT_PARQUET = CLEAN_DIR / "dins_county_destroyed_residential.parquet"

# CNRA GIS Portal direct CSV download — full DINS dataset
DINS_CSV_URL = (
    "https://gis.data.cnra.ca.gov/api/download/v1/items/"
    "994d3dc4569640caadbbc3198d5a3da1/csv?layers=0"
)

# CA county name → 5-digit FIPS mapping (Census Bureau standard, 2010 vintage)
# Source: https://www.census.gov/library/reference/code-lists/ansi.html
CA_COUNTY_FIPS = {
    "Alameda": "06001", "Alpine": "06003", "Amador": "06005",
    "Butte": "06007", "Calaveras": "06009", "Colusa": "06011",
    "Contra Costa": "06013", "Del Norte": "06015", "El Dorado": "06017",
    "Fresno": "06019", "Glenn": "06021", "Humboldt": "06023",
    "Imperial": "06025", "Inyo": "06027", "Kern": "06029",
    "Kings": "06031", "Lake": "06033", "Lassen": "06035",
    "Los Angeles": "06037", "Madera": "06039", "Marin": "06041",
    "Mariposa": "06043", "Mendocino": "06045", "Merced": "06047",
    "Modoc": "06049", "Mono": "06051", "Monterey": "06053",
    "Napa": "06055", "Nevada": "06057", "Orange": "06059",
    "Placer": "06061", "Plumas": "06063", "Riverside": "06065",
    "Sacramento": "06067", "San Benito": "06069", "San Bernardino": "06071",
    "San Diego": "06073", "San Francisco": "06075", "San Joaquin": "06077",
    "San Luis Obispo": "06079", "San Mateo": "06081", "Santa Barbara": "06083",
    "Santa Clara": "06085", "Santa Cruz": "06087", "Shasta": "06089",
    "Sierra": "06091", "Siskiyou": "06093", "Solano": "06095",
    "Sonoma": "06097", "Stanislaus": "06099", "Sutter": "06101",
    "Tehama": "06103", "Trinity": "06105", "Tulare": "06107",
    "Tuolumne": "06109", "Ventura": "06111", "Yolo": "06113",
    "Yuba": "06115",
}

# Damage rating string that denotes total destruction.
# Confirmed from DINS field documentation: "Destroyed" corresponds to 50–100% damage.
DESTROYED_LABEL = "destroyed"   # matched case-insensitively

# Residential structure type filter.
# STRUCTURE CATEGORY values confirmed from the downloaded data:
#   "Single Residence", "Multiple Residence", "Mixed Commercial/Residential"
# Matching on "residen" captures all three (covers "residence" and "residential").
RESIDENTIAL_SUBSTRING = "residen"   # matched case-insensitively


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _normalize_county(name: str) -> str:
    """Strip whitespace and ' County' suffix for FIPS lookup."""
    return name.strip().removesuffix(" County").strip()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 65)
    print("03_acquire_dins.py — CAL FIRE DINS destroyed structures")
    print("=" * 65)

    if OUT_PARQUET.exists():
        print(f"\n[skip] {OUT_PARQUET.name} already exists. Delete to rebuild.")
        return

    # ── Download raw CSV ──────────────────────────────────────────────────────
    print("\n--- Downloading DINS CSV ---")
    download_file(url=DINS_CSV_URL, dest_path=RAW_CSV, timeout=300)

    # ── Load and inspect ──────────────────────────────────────────────────────
    print("\n--- Loading DINS CSV ---")
    df = pd.read_csv(RAW_CSV, low_memory=False)
    print(f"  Raw rows:    {len(df):,}")
    print(f"  Columns ({len(df.columns)}): {list(df.columns)}")

    # Normalize column names to uppercase for consistent access
    df.columns = [c.upper().strip() for c in df.columns]

    # Identify the relevant columns.
    # The DINS CSV uses human-readable field names with asterisks for required fields
    # (e.g., "* Damage", "* Structure Type", "Incident Start Date") — after uppercasing
    # these become "* DAMAGE", "* STRUCTURE TYPE", "INCIDENT START DATE".
    date_col   = next((c for c in df.columns if "INCIDENT START DATE" in c), None)
    damage_col = next((c for c in df.columns if "DAMAGE" in c and "OUTBUILD" not in c), None)
    # Prefer "STRUCTURE CATEGORY" (broader) over "* STRUCTURE TYPE" if both present
    type_col   = next(
        (c for c in df.columns if c == "STRUCTURE CATEGORY"),
        next((c for c in df.columns if "STRUCTURE TYPE" in c), None),
    )
    county_col = next((c for c in df.columns if c == "COUNTY"), None)

    print(f"\n  Date column:   {date_col}")
    print(f"  Damage column: {damage_col}")
    print(f"  Type column:   {type_col}")
    print(f"  County column: {county_col}")

    if not all([date_col, damage_col, type_col, county_col]):
        raise RuntimeError(
            "Could not locate all required columns. "
            "Check column names above and update the script."
        )

    # Print unique values to verify filter strings before applying them
    print(f"\n  Unique DAMAGE values:")
    for v in sorted(df[damage_col].dropna().unique()):
        print(f"    '{v}'")

    print(f"\n  Unique STRUCTURETYPE values (top 20):")
    for v in sorted(df[type_col].dropna().unique())[:20]:
        print(f"    '{v}'")

    # ── Extract incident year ─────────────────────────────────────────────────
    print("\n--- Extracting incident year ---")
    df["incident_year"] = pd.to_datetime(
        df[date_col], errors="coerce"
    ).dt.year
    n_null_date = df["incident_year"].isna().sum()
    if n_null_date > 0:
        print(f"  [warn] {n_null_date:,} rows with unparseable date — excluded")
    df = df[df["incident_year"].notna()].copy()
    df["incident_year"] = df["incident_year"].astype(int)
    print(f"  Years present: {df['incident_year'].min()}–{df['incident_year'].max()}")

    # ── Filter: destroyed residential ─────────────────────────────────────────
    print("\n--- Filtering to destroyed residential structures ---")
    damage_mask = df[damage_col].str.lower().str.contains(
        DESTROYED_LABEL, na=False
    )
    type_mask = df[type_col].str.lower().str.contains(
        RESIDENTIAL_SUBSTRING, na=False
    )
    filtered = df[damage_mask & type_mask].copy()

    print(f"  Total rows:              {len(df):,}")
    print(f"  After damage filter:     {damage_mask.sum():,}")
    print(f"  After type filter:       {type_mask.sum():,}")
    print(f"  Destroyed residential:   {len(filtered):,}")

    if len(filtered) == 0:
        raise RuntimeError(
            "No destroyed residential records found after filtering. "
            "Check DAMAGE and STRUCTURETYPE values printed above."
        )

    # ── Join county name → FIPS ────────────────────────────────────────────────
    print("\n--- Mapping county names to FIPS ---")
    filtered["county_norm"] = filtered[county_col].apply(
        lambda x: _normalize_county(str(x)) if pd.notna(x) else ""
    )
    filtered["county_FIPS"] = filtered["county_norm"].map(CA_COUNTY_FIPS)

    n_unmatched = filtered["county_FIPS"].isna().sum()
    if n_unmatched > 0:
        unmatched_vals = filtered.loc[filtered["county_FIPS"].isna(), "county_norm"].unique()
        print(f"  [warn] {n_unmatched:,} rows with unmatched county name:")
        for v in sorted(unmatched_vals):
            print(f"    '{v}'")
        filtered = filtered[filtered["county_FIPS"].notna()].copy()

    print(f"  Matched: {len(filtered):,} rows across "
          f"{filtered['county_FIPS'].nunique()} counties")

    # ── Aggregate to county × year ────────────────────────────────────────────
    print("\n--- Aggregating to county × year ---")
    agg = (
        filtered.groupby(["county_FIPS", "incident_year"])
        .size()
        .reset_index(name="structures_destroyed")
    )
    agg = agg.rename(columns={"incident_year": "year"})

    # Spot-check known fire events
    spot_checks = [
        ("Butte",  "06007", 2018, "Camp Fire"),
        ("Sonoma", "06097", 2017, "Tubbs Fire"),
        ("Shasta", "06089", 2018, "Carr Fire"),
        ("Lake",   "06033", 2015, "Valley Fire"),
    ]
    print(f"\n  Spot-check known fire events:")
    print(f"  {'County':<12}  {'Year':>6}  {'Fire':<18}  {'Destroyed':>10}")
    for county_name, fips, yr, fire in spot_checks:
        row = agg[(agg["county_FIPS"] == fips) & (agg["year"] == yr)]
        count = int(row["structures_destroyed"].values[0]) if len(row) > 0 else 0
        print(f"  {county_name:<12}  {yr:>6}  {fire:<18}  {count:>10,}")

    print(f"\n  Total county-year records with ≥1 destroyed: {len(agg):,}")
    print(f"  Counties affected: {agg['county_FIPS'].nunique()}")
    print(f"  Years span: {agg['year'].min()}–{agg['year'].max()}")
    print(f"  Total destroyed residential structures: {agg['structures_destroyed'].sum():,}")

    # ── Save ──────────────────────────────────────────────────────────────────
    print("\n--- Saving output ---")
    agg.to_parquet(OUT_PARQUET, index=False)
    print(f"[saved] {OUT_PARQUET.name}")
    print(f"        {len(agg):,} rows × {len(agg.columns)} columns")
    print(f"        Columns: {list(agg.columns)}")

    print("\n" + "=" * 65)
    print("Done. Run 04_build_structure_panel.py next to apply DINS correction.")
    print("=" * 65)


if __name__ == "__main__":
    main()
