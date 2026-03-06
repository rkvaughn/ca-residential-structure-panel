"""
census_api.py
=============
Helpers for pulling American Community Survey (ACS) data via the Census Bureau
Data API (https://api.census.gov).

Functions
---------
fetch_acs_batch(year, variables, state_fips, geography, api_key)
    Fetch a single batch of ACS variables for a given geography and return a
    raw DataFrame with Census API column headers.

fetch_acs_tracts(year, variable_batches, state_fips, api_key, variable_labels)
    Fetch one or more batches, merge on geo columns, build a standard GEOID,
    rename variables, and mask Census sentinel values (-666666666).

build_geoid(df, state_col="state", county_col="county", tract_col="tract")
    Construct an 11-digit Census tract GEOID string from component columns.

mask_sentinel(df, columns, sentinel=-666666666)
    Replace Census sentinel values with pd.NA and coerce columns to numeric.

Dependencies
------------
    requests  (pip install requests)
    pandas    (pip install pandas)

Usage example
-------------
    from census_api import fetch_acs_tracts

    BATCHES = [
        ["B25038_001E",   # total owner-occupied units
         "B25038_002E",   # owner occ: moved in 2015 or later
         "B25038_003E",   # owner occ: moved in 2010–2014
         "B25038_006E",   # owner occ: moved in 2000–2009
         "B25038_010E"],  # owner occ: moved in 1999 or earlier
    ]
    LABELS = {
        "B25038_001E": "owner_occ_total",
        "B25038_002E": "moved_in_2015plus",
        "B25038_003E": "moved_in_2010_2014",
        "B25038_006E": "moved_in_2000_2009",
        "B25038_010E": "moved_in_pre2000",
    }

    df_2020 = fetch_acs_tracts(
        year=2020,
        variable_batches=BATCHES,
        state_fips="06",
        api_key="YOUR_KEY",          # or set CENSUS_API_KEY env var
        variable_labels=LABELS,
    )

Census API notes
----------------
- Free API key signup: https://api.census.gov/data/key_signup.html
- Without a key, unauthenticated requests are rate-limited to ~500/day.
- Set the key as an environment variable: export CENSUS_API_KEY=your_key
- The API returns sentinel value -666666666 for missing/suppressed cells.
- The API limits each request to 50 variables; use variable_batches to split
  large variable lists across multiple requests.
- ACS 5-year vintages: year refers to the end year (e.g., 2020 = 2016–2020).
"""

import os
import time
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

try:
    import requests as _requests
    _HAS_REQUESTS = True
except ImportError:
    _HAS_REQUESTS = False

CENSUS_BASE = "https://api.census.gov/data/{year}/acs/acs5"
SENTINEL = -666666666


def _get_api_key(api_key: Optional[str]) -> str:
    """Return api_key if provided, else check CENSUS_API_KEY env var."""
    if api_key:
        return api_key
    env_key = os.environ.get("CENSUS_API_KEY", "")
    if not env_key:
        print(
            "WARNING: CENSUS_API_KEY not set. Unauthenticated requests are rate-limited.\n"
            "Get a free key at https://api.census.gov/data/key_signup.html\n"
            "Then: export CENSUS_API_KEY=your_key_here"
        )
    return env_key


def fetch_acs_batch(
    year: int,
    variables: List[str],
    state_fips: str = "06",
    geography: str = "tract",
    api_key: Optional[str] = None,
) -> pd.DataFrame:
    """
    Fetch one batch of ACS 5-year variables for all geographies of type
    *geography* within *state_fips*.

    Parameters
    ----------
    year : int
        ACS 5-year end-year (e.g., 2020 pulls the 2016–2020 estimates).
    variables : list of str
        ACS variable codes (e.g., ["B25038_001E", "B19013_001E"]).
        Maximum 50 per request (Census API limit).
    state_fips : str
        Two-digit state FIPS code. Default "06" (California).
    geography : str
        Census geography type. Default "tract".
        Other options: "county", "block group", "zip code tabulation area".
    api_key : str, optional
        Census API key. Falls back to CENSUS_API_KEY environment variable.

    Returns
    -------
    pd.DataFrame
        Raw DataFrame with Census API column headers (variable codes + geo cols).

    Raises
    ------
    requests.HTTPError
        If the API returns a non-2xx status.
    """
    if not _HAS_REQUESTS:
        raise ImportError("requests is required: pip install requests")
    import requests

    key = _get_api_key(api_key)
    url = CENSUS_BASE.format(year=year)
    params: dict = {
        "get": "NAME," + ",".join(variables),
        "for": f"{geography}:*",
        "in": f"state:{state_fips}",
    }
    if key:
        params["key"] = key

    print(f"    Fetching {len(variables)} variables for {geography} level ({year} ACS 5-yr)...")
    resp = requests.get(url, params=params, timeout=60)
    if resp.status_code == 400 and "key" in resp.text.lower():
        print(
            "    NOTE: Census API rate limit hit without key.\n"
            "    Get a free key at https://api.census.gov/data/key_signup.html"
        )
    resp.raise_for_status()

    data = resp.json()
    return pd.DataFrame(data[1:], columns=data[0])


def build_geoid(
    df: pd.DataFrame,
    state_col: str = "state",
    county_col: str = "county",
    tract_col: str = "tract",
) -> pd.Series:
    """
    Build a standard 11-digit Census tract GEOID from component columns.

    Concatenates state (2 digits) + county (3 digits) + tract (6 digits).

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing the geo component columns as strings.
    state_col, county_col, tract_col : str
        Column names for state, county, and tract FIPS codes.

    Returns
    -------
    pd.Series
        Series of 11-character GEOID strings.
    """
    return df[state_col].astype(str) + df[county_col].astype(str) + df[tract_col].astype(str)


def mask_sentinel(
    df: pd.DataFrame,
    columns: List[str],
    sentinel: int = SENTINEL,
) -> pd.DataFrame:
    """
    Replace Census sentinel values with pd.NA and coerce columns to numeric.

    The Census API uses -666666666 to indicate suppressed or unavailable cells.
    This function coerces columns to numeric and replaces the sentinel with pd.NA.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame (modified in-place).
    columns : list of str
        Column names to process. Missing columns are silently skipped.
    sentinel : int
        Sentinel value to replace. Default -666666666.

    Returns
    -------
    pd.DataFrame
        The modified DataFrame (same object, returned for chaining).
    """
    for col in columns:
        if col not in df.columns:
            continue
        df[col] = pd.to_numeric(df[col], errors="coerce")
        df[col] = df[col].where(df[col] != sentinel, other=pd.NA)
    return df


def fetch_acs_tracts(
    year: int,
    variable_batches: List[List[str]],
    state_fips: str = "06",
    api_key: Optional[str] = None,
    variable_labels: Optional[Dict[str, str]] = None,
    sleep_between_batches: float = 1.0,
) -> pd.DataFrame:
    """
    Fetch multiple batches of ACS variables for all Census tracts in a state,
    merge them into a single DataFrame, build GEOIDs, rename variables, and
    mask sentinel values.

    Parameters
    ----------
    year : int
        ACS 5-year end-year (e.g., 2020 for 2016–2020 estimates).
    variable_batches : list of list of str
        Variable codes split into batches of ≤50 (Census API limit).
        Example: [["B25038_001E", "B25038_002E"], ["B19013_001E"]]
    state_fips : str
        Two-digit state FIPS code. Default "06" (California).
    api_key : str, optional
        Census API key. Falls back to CENSUS_API_KEY env var.
    variable_labels : dict, optional
        Mapping from ACS variable code to human-readable column name.
        Codes not in the dict are left as-is.
    sleep_between_batches : float
        Seconds to sleep between API requests to avoid rate limiting.
        Default 1.0.

    Returns
    -------
    pd.DataFrame
        Columns: geoid (11-digit str), NAME, acs_year,
                 + all requested variables (renamed if variable_labels provided).
    """
    print(f"  Fetching ACS {year} 5-year estimates — {sum(len(b) for b in variable_batches)} variables...")

    geo_cols = ["state", "county", "tract"]
    all_batches = []

    for i, batch in enumerate(variable_batches):
        df_batch = fetch_acs_batch(year, batch, state_fips=state_fips, api_key=api_key)
        all_batches.append(df_batch)
        if i < len(variable_batches) - 1:
            time.sleep(sleep_between_batches)

    # Merge batches on geo columns
    df = all_batches[0]
    for extra in all_batches[1:]:
        drop_cols = [c for c in extra.columns if c in ("NAME",)]
        df = df.merge(extra.drop(columns=drop_cols, errors="ignore"), on=geo_cols)

    # Build GEOID
    df["geoid"] = build_geoid(df)
    df["acs_year"] = year

    # Rename variables
    if variable_labels:
        df = df.rename(columns=variable_labels)

    # Mask sentinel values on all variable columns
    all_var_codes = [v for batch in variable_batches for v in batch]
    renamed = [variable_labels.get(v, v) for v in all_var_codes] if variable_labels else all_var_codes
    mask_sentinel(df, renamed)

    keep = ["geoid", "NAME", "acs_year"] + [c for c in renamed if c in df.columns]
    return df[keep].reset_index(drop=True)
