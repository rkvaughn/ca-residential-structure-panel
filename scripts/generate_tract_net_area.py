"""
generate_tract_net_area.py
--------------------------
Generates dashboard/src/data/tract_net_area.json — a GEOID → net residential
land area (sq miles) lookup — using 2010 Census block-level data.

Method (Option A: Census Block ALAND):
  - Acquire the TIGER/Line 2010 California block attribute file (DBF only,
    extracted from the 419 MB ZIP via HTTP range request — downloads ~16 MB).
    Provides GEOID10 and ALAND10 (land area in sq meters, water already
    excluded by the Census Bureau) for all 710,145 CA blocks.
  - Acquire block-level housing unit counts (H001001) from the 2010 Census
    Decennial SF1 API, querying all 58 CA counties.
  - Filter to blocks where H001001 > 0. Blocks inside national parks,
    national forests, BLM land, state parks, military reservations, and water
    bodies have 0 housing units and are excluded automatically.
  - Aggregate ALAND10 to the tract level (first 11 chars of the 15-char
    block GEOID = state + county + tract).
  - Convert sq meters → sq miles and write a compact JSON mapping.

The resulting "net residential land area" excludes both water AND public/
federal lands. This is the appropriate denominator for residential structure
density displayed in the dashboard choropleth.

Usage:
    /Users/ryanvaughn/Projects/prop13_paper/.venv/bin/python3 \\
        scripts/generate_tract_net_area.py
"""

import io
import json
import struct
import sys
import time
from pathlib import Path

try:
    import pandas as pd
    import requests
except ImportError:
    print("ERROR: pandas/requests not available. Use prop13_paper venv:")
    print("  /Users/ryanvaughn/Projects/prop13_paper/.venv/bin/python3 scripts/generate_tract_net_area.py")
    sys.exit(1)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR      = PROJECT_ROOT / "data" / "raw"
OUTPUT_PATH  = PROJECT_ROOT / "dashboard" / "src" / "data" / "tract_net_area.json"

TIGER_ZIP_URL = (
    "https://www2.census.gov/geo/tiger/TIGER2010/TABBLOCK/2010/"
    "tl_2010_06_tabblock10.zip"
)
DBF_CACHE = RAW_DIR / "tl_2010_06_tabblock10.dbf"
CENSUS_API = "https://api.census.gov/data/2010/dec/sf1"

# 1 sq meter = 3.86102e-7 sq miles
SQ_M_TO_SQ_MI = 3.86102e-7


# ---------------------------------------------------------------------------
# Step 1: acquire the TIGER block DBF via HTTP range request
# ---------------------------------------------------------------------------

def acquire_block_dbf():
    """Download just the DBF from the CA TIGER block ZIP (range requests).

    The ZIP structure has the DBF as the first file at offset 0. We read the
    end-of-central-directory to locate it, then fetch only the compressed DBF
    portion (~16 MB) rather than the full 419 MB ZIP.
    """
    if DBF_CACHE.exists():
        print(f"  Using cached DBF: {DBF_CACHE.name} ({DBF_CACHE.stat().st_size / 1e6:.1f} MB)")
        return

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    print(f"  Fetching ZIP directory from {TIGER_ZIP_URL} ...")

    # File size
    head = requests.head(TIGER_ZIP_URL, timeout=30)
    head.raise_for_status()
    total_size = int(head.headers["Content-Length"])

    # End-of-central-directory (last 64 KB)
    eocd_fetch = min(65536, total_size)
    r = requests.get(TIGER_ZIP_URL,
                     headers={"Range": f"bytes={total_size - eocd_fetch}-{total_size - 1}"},
                     timeout=60)
    r.raise_for_status()
    eocd_buf = r.content

    # Locate EOCD signature
    idx = eocd_buf.rfind(b"PK\x05\x06")
    if idx < 0:
        raise ValueError("End-of-central-directory not found in ZIP")
    cd_size, cd_offset = struct.unpack_from("<II", eocd_buf, idx + 12)

    # Central directory
    r2 = requests.get(TIGER_ZIP_URL,
                      headers={"Range": f"bytes={cd_offset}-{cd_offset + cd_size - 1}"},
                      timeout=60)
    r2.raise_for_status()
    cd = r2.content

    # Find the DBF entry (should be the first one, but search to be safe)
    pos, dbf_comp_size, dbf_local_offset = 0, None, None
    while pos < len(cd):
        if cd[pos:pos + 4] != b"PK\x01\x02":
            break
        comp_size   = struct.unpack_from("<I", cd, pos + 20)[0]
        local_off   = struct.unpack_from("<I", cd, pos + 42)[0]
        fname_len   = struct.unpack_from("<H", cd, pos + 28)[0]
        extra_len   = struct.unpack_from("<H", cd, pos + 30)[0]
        comment_len = struct.unpack_from("<H", cd, pos + 32)[0]
        fname = cd[pos + 46: pos + 46 + fname_len].decode("utf-8", errors="replace")
        if fname.endswith(".dbf"):
            dbf_comp_size    = comp_size
            dbf_local_offset = local_off
        pos += 46 + fname_len + extra_len + comment_len

    if dbf_comp_size is None:
        raise ValueError("DBF file not found in ZIP central directory")

    # Local file header — read 100 bytes to get extra_len
    r3 = requests.get(TIGER_ZIP_URL,
                      headers={"Range": f"bytes={dbf_local_offset}-{dbf_local_offset + 99}"},
                      timeout=30)
    r3.raise_for_status()
    local_hdr = r3.content
    lfname_len = struct.unpack_from("<H", local_hdr, 26)[0]
    lextra_len = struct.unpack_from("<H", local_hdr, 28)[0]
    data_start = dbf_local_offset + 30 + lfname_len + lextra_len
    data_end   = data_start + dbf_comp_size - 1

    print(f"  Downloading compressed DBF ({dbf_comp_size / 1e6:.1f} MB) ...")
    r4 = requests.get(TIGER_ZIP_URL,
                      headers={"Range": f"bytes={data_start}-{data_end}"},
                      timeout=300,
                      stream=True)
    r4.raise_for_status()
    compressed = b"".join(r4.iter_content(1 << 20))

    import zlib
    raw = zlib.decompress(compressed, -15)   # deflate (no header)
    print(f"  Decompressed: {len(raw) / 1e6:.1f} MB")

    with open(DBF_CACHE, "wb") as f:
        f.write(raw)
    print(f"  Saved: {DBF_CACHE.name}")


# ---------------------------------------------------------------------------
# Step 2: parse the DBF into a pandas DataFrame
# ---------------------------------------------------------------------------

def read_dbf(path: Path) -> pd.DataFrame:
    """Read a dBASE III DBF file into a pandas DataFrame.

    Reads only GEOID10 and ALAND10 columns for efficiency.
    """
    with open(path, "rb") as f:
        header  = f.read(32)
        n_recs  = struct.unpack_from("<I", header, 4)[0]
        hdr_len = struct.unpack_from("<H", header, 8)[0]
        rec_len = struct.unpack_from("<H", header, 10)[0]

        # Parse field descriptors
        fields = []
        while True:
            fd = f.read(32)
            if not fd or fd[0] == 0x0D:
                break
            name  = fd[:11].rstrip(b"\x00").decode("ascii")
            ftype = chr(fd[11])
            flen  = fd[16]
            fields.append({"name": name, "type": ftype, "len": flen})

        # Compute byte offsets within each record (first byte is deletion flag)
        offset = 1
        for fld in fields:
            fld["offset"] = offset
            offset += fld["len"]

        # Identify columns we want
        want = {"GEOID10", "ALAND10"}
        keep = [f for f in fields if f["name"] in want]

        # Seek to start of records
        f.seek(hdr_len)
        raw_records = f.read(n_recs * rec_len)

    # Extract records using vectorised string slicing via memoryview
    rows = {"GEOID10": [], "ALAND10": []}
    mv = memoryview(raw_records)
    for i in range(n_recs):
        base = i * rec_len
        for fld in keep:
            val = bytes(mv[base + fld["offset"]: base + fld["offset"] + fld["len"]]).decode("ascii").strip()
            rows[fld["name"]].append(val)

    df = pd.DataFrame(rows)
    df["ALAND10"] = pd.to_numeric(df["ALAND10"], errors="coerce").fillna(0).astype("int64")
    return df


# ---------------------------------------------------------------------------
# Step 3: get block-level housing units from the Census API
# ---------------------------------------------------------------------------

def fetch_block_hu(county_fips_list: list[str]) -> pd.DataFrame:
    """Fetch H001001 (total housing units) per block for CA via Census SF1 API.

    Queries one county at a time (API requirement for block-level data).
    Returns DataFrame with columns: GEOID10 (15-char), HU10.
    """
    frames = []
    n = len(county_fips_list)
    for i, county in enumerate(county_fips_list, 1):
        params = {
            "get": "H001001",
            "for": "block:*",
            "in": f"state:06 county:{county}",
        }
        for attempt in range(3):
            try:
                r = requests.get(CENSUS_API, params=params, timeout=60)
                r.raise_for_status()
                data = r.json()
                break
            except Exception as exc:
                if attempt == 2:
                    print(f"  WARNING: county {county} failed after 3 attempts: {exc}")
                    data = None
                time.sleep(2 ** attempt)

        if not data or len(data) < 2:
            continue

        cols = data[0]
        rows = data[1:]
        df_c = pd.DataFrame(rows, columns=cols)
        # Reconstruct 15-char block GEOID: state(2) + county(3) + tract(6) + block(4)
        df_c["GEOID10"] = df_c["state"] + df_c["county"] + df_c["tract"] + df_c["block"]
        df_c["HU10"] = pd.to_numeric(df_c["H001001"], errors="coerce").fillna(0).astype("int32")
        frames.append(df_c[["GEOID10", "HU10"]])

        if i % 10 == 0 or i == n:
            print(f"  Fetched {i}/{n} counties ...")

    return pd.concat(frames, ignore_index=True)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("Generating tract_net_area.json ...")

    # Step 1: TIGER block DBF (GEOID10, ALAND10)
    print("\nStep 1: Acquiring TIGER 2010 CA block DBF ...")
    acquire_block_dbf()

    print("\nStep 2: Parsing DBF ...")
    df_dbf = read_dbf(DBF_CACHE)
    print(f"  {len(df_dbf):,} blocks read; ALAND10 range: "
          f"{df_dbf['ALAND10'].min():,} – {df_dbf['ALAND10'].max():,} sq m")

    # Step 3: Census API housing units
    print("\nStep 3: Fetching block-level housing units from Census API ...")
    county_list = sorted(df_dbf["GEOID10"].str[2:5].unique().tolist())
    print(f"  {len(county_list)} CA counties to query")
    df_hu = fetch_block_hu(county_list)
    print(f"  {len(df_hu):,} block records returned")

    # Step 4: Join and filter
    print("\nStep 4: Joining ALAND and housing units; filtering to residential blocks ...")
    df = df_dbf.merge(df_hu, on="GEOID10", how="left")
    df["HU10"] = df["HU10"].fillna(0).astype("int32")

    total = len(df)
    df_res = df[df["HU10"] > 0].copy()
    excluded = total - len(df_res)
    print(f"  {total:,} total blocks")
    print(f"  {len(df_res):,} residential blocks (HU10 > 0)")
    print(f"  {excluded:,} excluded (parks, forests, water, military, vacant land)")

    # Step 5: Aggregate to tract level
    print("\nStep 5: Aggregating to tract GEOID ...")
    df_res["tract_geoid"] = df_res["GEOID10"].str[:11]
    tract_aland = df_res.groupby("tract_geoid")["ALAND10"].sum()
    tract_sqmi  = (tract_aland * SQ_M_TO_SQ_MI).round(6)
    print(f"  {len(tract_sqmi):,} tracts with at least one residential block")

    # Step 6: Write JSON
    print(f"\nStep 6: Writing {OUTPUT_PATH.relative_to(PROJECT_ROOT)} ...")
    output = tract_sqmi.to_dict()
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, separators=(",", ":"))

    size_kb = OUTPUT_PATH.stat().st_size / 1024
    print(f"  Done: {len(output):,} tracts, {size_kb:.0f} KB")
    print()
    print("Commit this file:")
    print(f"  git add {OUTPUT_PATH.relative_to(PROJECT_ROOT)}")


if __name__ == "__main__":
    main()
