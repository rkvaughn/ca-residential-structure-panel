"""
import_to_supabase.py
--------------------
One-time script to import the four panel parquet files into a Supabase
PostgreSQL database for use by the Observable Framework dashboard.

PREREQUISITES
=============
1. Create a new Supabase project at https://supabase.com
   Suggested name: ca-residential-panel

2. In the Supabase SQL Editor, run the CREATE TABLE statements below
   (copy from the SQL block in this file's docstring).

3. Set environment variables (or edit the CONFIG block below):
     export SUPABASE_URL="https://xxxxxxxxxxxx.supabase.co"
     export SUPABASE_SERVICE_KEY="eyJhbGci..."   # Settings → API → service_role key

4. Run with the prop13_paper venv (has pandas + pyarrow + requests):
     /Users/ryanvaughn/Projects/prop13_paper/.venv/bin/python3 scripts/import_to_supabase.py

5. After import, update dashboard/src/components/supabase-client.js with:
     SUPABASE_URL = "https://xxxxxxxxxxxx.supabase.co"
     SUPABASE_ANON_KEY = "eyJhbGci..."    # Settings → API → anon public key

SQL TO RUN IN SUPABASE SQL EDITOR BEFORE RUNNING THIS SCRIPT
=============================================================

-- Panel: Arruda Hybrid (P5/P50/P95 bootstrap with Arruda calibration)
CREATE TABLE panel_hybrid (
    geoid        TEXT        NOT NULL,
    county_fips  TEXT,
    year         SMALLINT    NOT NULL,
    p5_residential_count   REAL,
    p50_residential_count  REAL,
    p95_residential_count  REAL,
    iqr_residential_count  REAL,
    alpha_c      REAL,
    beta_c       REAL,
    PRIMARY KEY (geoid, year)
);
CREATE INDEX panel_hybrid_year_idx ON panel_hybrid (year);

-- Panel: ACS B25001 challenger
CREATE TABLE panel_acs (
    geoid                    TEXT     NOT NULL,
    county_fips              TEXT,
    year                     SMALLINT NOT NULL,
    acs_housing_units        REAL,
    acs_vintage_year         SMALLINT,
    acs_extrapolated         BOOLEAN,
    acs_crosswalk_translated BOOLEAN,
    acs_imputed              BOOLEAN,
    PRIMARY KEY (geoid, year)
);
CREATE INDEX panel_acs_year_idx ON panel_acs (year);

-- Panel: Point estimate (deterministic hindcast)
CREATE TABLE panel_point (
    geoid                        TEXT     NOT NULL,
    county_fips                  TEXT,
    year                         SMALLINT NOT NULL,
    overture_residential_count_2024 REAL,
    tract_share                  REAL,
    county_anchor                REAL,
    county_count_hindcast        REAL,
    structures_permitted         REAL,
    structures_destroyed         REAL,
    net_structures_change        REAL,
    residential_count_hindcast   REAL,
    PRIMARY KEY (geoid, year)
);
CREATE INDEX panel_point_year_idx ON panel_point (year);

-- Panel: Arruda hindcast
CREATE TABLE panel_arruda (
    geoid                      TEXT     NOT NULL,
    county_fips                TEXT,
    year                       SMALLINT NOT NULL,
    tract_share                REAL,
    county_anchor              REAL,
    county_count_hindcast      REAL,
    residential_count_hindcast REAL,
    PRIMARY KEY (geoid, year)
);
CREATE INDEX panel_arruda_year_idx ON panel_arruda (year);

-- Aggregated view for Overview page statewide time-series (15 rows)
CREATE VIEW panel_hybrid_annual AS
SELECT
    year,
    SUM(p5_residential_count)  AS p5_total,
    SUM(p50_residential_count) AS p50_total,
    SUM(p95_residential_count) AS p95_total,
    COUNT(DISTINCT geoid)      AS n_tracts,
    COUNT(DISTINCT county_fips) AS n_counties
FROM panel_hybrid
GROUP BY year
ORDER BY year;

-- Row Level Security: enable read access for anon key
ALTER TABLE panel_hybrid  ENABLE ROW LEVEL SECURITY;
ALTER TABLE panel_acs     ENABLE ROW LEVEL SECURITY;
ALTER TABLE panel_point   ENABLE ROW LEVEL SECURITY;
ALTER TABLE panel_arruda  ENABLE ROW LEVEL SECURITY;

CREATE POLICY "public read" ON panel_hybrid  FOR SELECT USING (true);
CREATE POLICY "public read" ON panel_acs     FOR SELECT USING (true);
CREATE POLICY "public read" ON panel_point   FOR SELECT USING (true);
CREATE POLICY "public read" ON panel_arruda  FOR SELECT USING (true);

-- Note: Views inherit RLS from underlying tables.
-- Grant select on the view to the anon role:
GRANT SELECT ON panel_hybrid_annual TO anon;
"""

import os
import sys
import json
import math
import time
from pathlib import Path

try:
    import pandas as pd
    import requests
except ImportError:
    print("ERROR: Install pandas and requests.")
    print("  /Users/ryanvaughn/Projects/prop13_paper/.venv/bin/python3 scripts/import_to_supabase.py")
    sys.exit(1)

# ── CONFIG ─────────────────────────────────────────────────────────────────────
SUPABASE_URL = os.environ.get("SUPABASE_URL", "YOUR_SUPABASE_URL")
SERVICE_KEY  = os.environ.get("SUPABASE_SERVICE_KEY", "YOUR_SERVICE_ROLE_KEY")
BATCH_SIZE   = 500  # rows per POST request
# ──────────────────────────────────────────────────────────────────────────────

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR     = PROJECT_ROOT / "data" / "clean"

PANELS = {
    "panel_hybrid": {
        "file": "tract_structure_panel_arruda_hybrid.parquet",
        "columns": [
            "geoid", "county_FIPS", "year",
            "p5_residential_count", "p50_residential_count",
            "p95_residential_count", "iqr_residential_count",
            "alpha_c", "beta_c",
        ],
        "rename": {"county_FIPS": "county_fips"},
    },
    "panel_acs": {
        "file": "tract_structure_panel_acs.parquet",
        "columns": [
            "geoid", "county_FIPS", "year",
            "acs_housing_units", "acs_vintage_year",
            "acs_extrapolated", "acs_crosswalk_translated", "acs_imputed",
        ],
        "rename": {"county_FIPS": "county_fips"},
    },
    "panel_point": {
        "file": "tract_structure_panel.parquet",
        "columns": [
            "geoid", "county_FIPS", "year",
            "overture_residential_count_2024", "tract_share",
            "county_anchor", "county_count_hindcast",
            "structures_permitted", "structures_destroyed",
            "net_structures_change", "residential_count_hindcast",
        ],
        "rename": {"county_FIPS": "county_fips"},
    },
    "panel_arruda": {
        "file": "tract_structure_panel_arruda.parquet",
        "columns": [
            "geoid", "county_FIPS", "year",
            "tract_share", "county_anchor",
            "county_count_hindcast", "residential_count_hindcast",
        ],
        "rename": {"county_FIPS": "county_fips"},
    },
}


def check_config():
    if "YOUR_" in SUPABASE_URL or "YOUR_" in SERVICE_KEY:
        print("ERROR: Set SUPABASE_URL and SUPABASE_SERVICE_KEY environment variables.")
        print("  export SUPABASE_URL='https://xxxx.supabase.co'")
        print("  export SUPABASE_SERVICE_KEY='eyJhbGci...'")
        sys.exit(1)


def make_headers():
    return {
        "apikey": SERVICE_KEY,
        "Authorization": f"Bearer {SERVICE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal,resolution=ignore-duplicates",
    }


def insert_batch(table: str, records: list, headers: dict):
    """POST a batch of records to a Supabase table via PostgREST."""
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    payload = json.dumps(records, default=_json_default)
    r = requests.post(url, headers=headers, data=payload, timeout=30)
    if r.status_code not in (200, 201):
        raise RuntimeError(f"Insert failed ({r.status_code}): {r.text[:300]}")


def _json_default(obj):
    """Convert numpy/pandas scalars to Python native types."""
    import numpy as np
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        return None if math.isnan(obj) else float(obj)
    if isinstance(obj, (np.bool_,)):
        return bool(obj)
    raise TypeError(f"Unserializable: {type(obj)}")


def import_panel(table: str, config: dict, headers: dict):
    path = DATA_DIR / config["file"]
    if not path.exists():
        print(f"  SKIP — file not found: {path}")
        print(f"         Download from GitHub Releases first.")
        return

    print(f"\nImporting {table} from {config['file']}...")
    df = pd.read_parquet(path)

    # Keep only the columns we need (some parquets have extra columns)
    cols = [c for c in config["columns"] if c in df.columns]
    df = df[cols].rename(columns=config.get("rename", {}))

    # Replace NaN with None (JSON null)
    df = df.where(pd.notnull(df), None)

    n = len(df)
    n_batches = math.ceil(n / BATCH_SIZE)
    print(f"  {n:,} rows → {n_batches} batches of {BATCH_SIZE}")

    for i in range(n_batches):
        batch = df.iloc[i * BATCH_SIZE:(i + 1) * BATCH_SIZE].to_dict("records")
        insert_batch(table, batch, headers)
        if (i + 1) % 20 == 0 or (i + 1) == n_batches:
            pct = (i + 1) / n_batches * 100
            print(f"  [{pct:5.1f}%] batch {i+1}/{n_batches}", end="\r")
        time.sleep(0.02)  # gentle rate limiting

    print(f"\n  Done: {n:,} rows inserted into {table}")


def main():
    check_config()
    headers = make_headers()

    print("=" * 60)
    print("CA Residential Structure Panel — Supabase Import")
    print(f"Target: {SUPABASE_URL}")
    print("=" * 60)
    print()
    print("IMPORTANT: Make sure you have run the CREATE TABLE SQL in the")
    print("Supabase SQL Editor first (see this script's docstring).")
    print()
    input("Press Enter to continue, or Ctrl-C to abort...")

    for table, config in PANELS.items():
        import_panel(table, config, headers)

    print()
    print("=" * 60)
    print("Import complete.")
    print()
    print("Next steps:")
    print("1. Get your anon (public) key from Supabase → Settings → API")
    print("2. Fill in SUPABASE_URL and SUPABASE_ANON_KEY in:")
    print("   dashboard/src/components/supabase-client.js")
    print("3. Run: cd dashboard && npm run dev")
    print("=" * 60)


if __name__ == "__main__":
    main()
