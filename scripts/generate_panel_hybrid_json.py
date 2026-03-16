"""
generate_panel_hybrid_json.py
-----------------------------
Generates dashboard/src/data/panel-hybrid.json from the Arruda hybrid panel parquet.
Run locally whenever the panel data changes. Commit the generated JSON to the repo.

Observable Framework serves the committed JSON as a static asset via FileAttachment.
This avoids runtime Supabase fetches (and CORS issues) for the default map panel.

Usage:
    /Users/ryanvaughn/Projects/prop13_paper/.venv/bin/python3 \
        scripts/generate_panel_hybrid_json.py
"""

import json
import sys
from pathlib import Path

try:
    import pandas as pd
except ImportError:
    print("ERROR: pandas not available. Use prop13_paper venv:")
    print("  /Users/ryanvaughn/Projects/prop13_paper/.venv/bin/python3 scripts/generate_panel_hybrid_json.py")
    sys.exit(1)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_CLEAN   = PROJECT_ROOT / "data" / "clean"
OUTPUT_PATH  = PROJECT_ROOT / "dashboard" / "src" / "data" / "panel-hybrid.json"

PARQUET = DATA_CLEAN / "tract_structure_panel_arruda_hybrid.parquet"
COLUMNS = ["geoid", "year",
           "p5_residential_count", "p50_residential_count", "p95_residential_count"]


def main():
    if not PARQUET.exists():
        print(f"ERROR: parquet not found: {PARQUET}")
        print("Download from GitHub Releases v1.1 or re-run script 08.")
        sys.exit(1)

    print(f"Reading {PARQUET.name}...")
    df = pd.read_parquet(PARQUET, columns=COLUMNS)
    print(f"  {len(df):,} rows, {len(df.columns)} columns")

    # Round float columns to 2 decimal places (structure counts don't need more precision)
    float_cols = ["p5_residential_count", "p50_residential_count", "p95_residential_count"]
    for col in float_cols:
        df[col] = df[col].round(2)

    # Convert NaN → None (JSON null)
    df = df.where(pd.notnull(df), None)

    # Use columnar format for ~40% smaller output vs row-of-objects format
    # Client reads: data.geoid[i], data.year[i], data.p50[i]  (zip by index)
    # OR: reconstruct row objects client-side via zip
    # For simplicity, output row-of-objects but with compact separators
    records = df.to_dict("records")

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    print(f"Writing {OUTPUT_PATH.relative_to(PROJECT_ROOT)}...")
    with open(OUTPUT_PATH, "w") as f:
        json.dump(records, f, allow_nan=False, separators=(",", ":"))

    size_mb = OUTPUT_PATH.stat().st_size / (1024 * 1024)
    print(f"  Done: {len(records):,} records, {size_mb:.1f} MB")
    print()
    print("Commit this file to the repo:")
    print(f"  git add {OUTPUT_PATH.relative_to(PROJECT_ROOT)}")


if __name__ == "__main__":
    main()
