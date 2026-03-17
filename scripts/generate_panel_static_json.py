"""
generate_panel_static_json.py
-----------------------------
Generates static JSON files for the ACS, Point, and Arruda panels so the
dashboard can load them via FileAttachment instead of runtime Supabase fetches.

This eliminates CORS risk and row-limit pagination for all non-hybrid panels,
following the same pattern established for panel-hybrid.json.

Output files (committed to dashboard/src/data/):
  panel-acs.json    — ACS B25001 housing units by tract × year
  panel-point.json  — Deterministic hindcast by tract × year
  panel-arruda.json — Arruda-anchored hindcast by tract × year

Usage:
    /Users/ryanvaughn/Projects/prop13_paper/.venv/bin/python3 \\
        scripts/generate_panel_static_json.py
"""

import json
import sys
from pathlib import Path

try:
    import pandas as pd
except ImportError:
    print("ERROR: pandas not available. Use prop13_paper venv.")
    sys.exit(1)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_CLEAN   = PROJECT_ROOT / "data" / "clean"
OUT_DIR      = PROJECT_ROOT / "dashboard" / "src" / "data"

PANELS = [
    {
        "key":     "acs",
        "parquet": DATA_CLEAN / "tract_structure_panel_acs.parquet",
        "cols":    ["geoid", "year", "acs_housing_units"],
        "output":  OUT_DIR / "panel-acs.json",
    },
    {
        "key":     "point",
        "parquet": DATA_CLEAN / "tract_structure_panel.parquet",
        "cols":    ["geoid", "year", "residential_count_hindcast"],
        "output":  OUT_DIR / "panel-point.json",
    },
    {
        "key":     "arruda",
        "parquet": DATA_CLEAN / "tract_structure_panel_arruda.parquet",
        "cols":    ["geoid", "year", "residential_count_hindcast"],
        "output":  OUT_DIR / "panel-arruda.json",
    },
]


def generate(panel: dict) -> None:
    key = panel["key"]
    parquet = panel["parquet"]
    cols = panel["cols"]
    output = panel["output"]

    if not parquet.exists():
        print(f"  ERROR: {parquet.name} not found — skipping {key}")
        return

    print(f"  Reading {parquet.name} ...")
    df = pd.read_parquet(parquet, columns=cols)
    print(f"    {len(df):,} rows")

    # Round floats to 2 decimal places; convert NaN → None (JSON null)
    val_col = cols[2]
    df[val_col] = pd.to_numeric(df[val_col], errors="coerce").round(2)
    df = df.where(pd.notnull(df), None)

    records = df.to_dict("records")

    output.parent.mkdir(parents=True, exist_ok=True)
    with open(output, "w") as f:
        json.dump(records, f, allow_nan=False, separators=(",", ":"))

    size_mb = output.stat().st_size / (1024 * 1024)
    print(f"    Written: {output.relative_to(PROJECT_ROOT)} ({size_mb:.1f} MB)")


def main():
    print("Generating static panel JSON files ...\n")
    for panel in PANELS:
        print(f"Panel: {panel['key']}")
        generate(panel)
        print()

    print("Done. Commit these files:")
    for panel in PANELS:
        if panel["output"].exists():
            print(f"  git add {panel['output'].relative_to(PROJECT_ROOT)}")


if __name__ == "__main__":
    main()
