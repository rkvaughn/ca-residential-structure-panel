"""
07_acquire_arruda_comparison.py
================================
External validation using Arruda et al. (2024) OSM-based residential building
classification dataset.

Citation: Arruda et al. (2024), Sci Data 11:1210. DOI:10.1038/s41597-024-03960-3
OSF repository: https://osf.io/utgae/

Actual OSF file layout (discovered 2026-03-08)
----------------------------------------------
Root osfstorage:
  metropolitan.zip   (7.5 GB)
  micropolitan.zip   (472 MB)
  other.zip          (467 MB)
  metropolitan/      (folder of CBSA-range sub-ZIPs)
    10180-17980.zip  (1.6 GB)
    18020-25980.zip  (1.1 GB)
    26140-32900.zip  (1.3 GB)
    33100-40980.zip  (2.0 GB)
    41060-48900.zip  (1.5 GB)
    49020-49740.zip  (88 MB — Sutter + Yuba counties, CBSA 49700 Yuba City)

File naming convention inside ZIPs (verified 2026-03-08):
  <root_dir>/<cbsa>/<raw_fips>_<County_Name>_<ST>.gpkg
  e.g. micropolitan/39780/6103_Tehama_CA.gpkg
  IMPORTANT: County FIPS drops leading zero for states 01-09.
  CA state FIPS = 06 → files named 6XXX_..._CA.gpkg (4-digit FIPS).
  Standard 5-digit FIPS = raw_fips.zfill(5).

Download strategy (efficient — avoids 8 GB download)
------------------------------------------------------
OSF supports HTTP Range requests on download URLs.
For each source ZIP:
  1. Read EOCD from last 65 KB → get central directory offset + size
  2. Download just the central directory → list all entries
  3. Find CA entries: endswith("_CA.gpkg") and not startswith("__MACOSX")
  4. For each CA entry: read 30-byte local file header to get data_start
  5. Range-download only the compressed GPKG data; decompress with zlib

This reduces downloads from ~8 GB to ~200 MB of CA GPKG data.

What it does (6 steps)
-----------------------
Step 1 — OSF API discovery  (resolve download URLs for each source ZIP)
Step 2 — Range-extract CA county GPKGs to data/raw/arruda/
Step 3 — Count RES buildings per county  (pyogrio, type column only)
Step 4 — Build comparison table  (Overture / Arruda / Bootstrap / ACS)
Step 5 — Arruda-anchored hind-cast tract panel
Step 6 — Two-panel comparison figure (300 DPI)

Outputs
-------
  data/raw/arruda/                             gitignored; CA GPKG files
  output/tables/arruda_ca_county_counts.parquet
  output/tables/arruda_comparison.csv
  data/clean/tract_structure_panel_arruda.parquet
  output/figures/fig_arruda_comparison.png

Usage
-----
  python scripts/07_acquire_arruda_comparison.py

Dependencies
------------
  pandas, pyarrow, pyogrio, requests, matplotlib, scipy
"""

import struct
import sys
import warnings
import zlib
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
import pyogrio
import requests
from scipy import stats

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(Path(__file__).parent / "utils"))

CLEAN_DIR   = PROJECT_ROOT / "data" / "clean"
RAW_DIR     = PROJECT_ROOT / "data" / "raw"
ARRUDA_DIR  = RAW_DIR / "arruda"
OUT_TABLES  = PROJECT_ROOT / "output" / "tables"
OUT_FIGURES = PROJECT_ROOT / "output" / "figures"

IN_TRACT_COUNTS = CLEAN_DIR / "tract_residential_counts_2024.parquet"
IN_BOOTSTRAP    = CLEAN_DIR / "tract_structure_panel_bootstrap.parquet"
IN_ACS          = CLEAN_DIR / "tract_structure_panel_acs.parquet"
IN_BPS_PERMITS  = CLEAN_DIR / "county_permits_ca_2010_2024.parquet"
IN_DINS         = CLEAN_DIR / "dins_county_destroyed_residential.parquet"
IN_PANEL        = CLEAN_DIR / "tract_structure_panel.parquet"

OUT_COUNTY_COUNTS = OUT_TABLES / "arruda_ca_county_counts.parquet"
OUT_COMPARISON    = OUT_TABLES  / "arruda_comparison.csv"
OUT_ARRUDA_PANEL  = CLEAN_DIR / "tract_structure_panel_arruda.parquet"
OUT_FIGURE        = OUT_FIGURES / "fig_arruda_comparison.png"

YEARS = list(range(2010, 2025))

# OSF node ID and API base
OSF_NODE   = "utgae"
OSF_API    = f"https://api.osf.io/v2/nodes/{OSF_NODE}/files/osfstorage/"


# ---------------------------------------------------------------------------
# Step 1 — OSF API discovery: resolve download URLs for each source ZIP
# ---------------------------------------------------------------------------

def discover_source_urls(session: requests.Session) -> dict[str, str]:
    """
    Query OSF API to get direct download URLs for each source ZIP.

    Returns dict: {source_name: direct_download_url}
    Sources: micropolitan.zip, other.zip, and 5 metro range ZIPs from
    the metropolitan/ folder (excluding 49020-49740.zip — no CA counties).
    """
    print(f"  Querying OSF API: {OSF_API}")
    r = session.get(OSF_API, timeout=30)
    r.raise_for_status()
    root_items = r.json()["data"]

    urls = {}

    for item in root_items:
        attrs = item["attributes"]
        name  = attrs["name"]
        kind  = attrs["kind"]

        if kind == "file" and name in ("micropolitan.zip", "other.zip"):
            # Follow redirect to get the storage URL
            dl_url = item["links"]["download"]
            head = session.head(dl_url, timeout=15, allow_redirects=True)
            source_name = name.replace(".zip", "")
            urls[source_name] = head.url
            print(f"    {name}: {head.url[:60]}...")

        elif kind == "folder" and name == "metropolitan":
            folder_url = item["relationships"]["files"]["links"]["related"]["href"]
            r2 = session.get(folder_url, timeout=30)
            r2.raise_for_status()
            for fi in r2.json()["data"]:
                fname = fi["attributes"]["name"]
                if not fname.endswith(".zip"):
                    continue
                # All 6 metro range ZIPs included; 49020-49740 has Sutter + Yuba (CBSA 49700)
                # (Earlier false negative was due to wrong CA filter — corrected 2026-03-09)
                dl_url = fi["links"]["download"]
                head = session.head(dl_url, timeout=15, allow_redirects=True)
                source_name = "metro_" + fname.replace(".zip", "").replace("-", "_")
                urls[source_name] = head.url
                size_mb = (fi["attributes"].get("size") or 0) / 1048576
                print(f"    {fname} ({size_mb:.0f} MB): {head.url[:60]}...")

    print(f"  Total sources: {len(urls)}")
    return urls


# ---------------------------------------------------------------------------
# Step 2 — Range-request ZIP extraction
# ---------------------------------------------------------------------------

def _read_eocd(url: str, content_length: int,
               session: requests.Session) -> tuple[int, int]:
    """
    Read the last 65 KB of the ZIP to find the end-of-central-directory record.

    Returns (cd_offset, cd_size) — both as file byte offsets.
    """
    start = max(0, content_length - 65536)
    r = session.get(url, headers={"Range": f"bytes={start}-"}, timeout=60)
    r.raise_for_status()
    data = r.content

    eocd_sig = b"PK\x05\x06"
    idx = data.rfind(eocd_sig)
    if idx < 0:
        raise ValueError(f"EOCD signature not found in last 65 KB of {url}")

    # EOCD fields after 4-byte signature
    # Offset: 4=disk_num(2), 6=cd_disk(2), 8=cd_recs_disk(2),
    #         10=cd_recs_total(2), 12=cd_size(4), 16=cd_offset(4), 20=comment_len(2)
    cd_size   = struct.unpack_from("<I", data, idx + 12)[0]
    cd_offset = struct.unpack_from("<I", data, idx + 16)[0]

    if cd_size == 0xFFFFFFFF or cd_offset == 0xFFFFFFFF:
        raise ValueError(f"ZIP64 format detected — not currently handled: {url}")

    return cd_offset, cd_size


def _read_central_directory(url: str, cd_offset: int, cd_size: int,
                             session: requests.Session) -> list[dict]:
    """
    Download and parse the central directory of a ZIP file.

    Returns list of dicts with keys:
    fname, comp_method, comp_size, uncomp_size, local_hdr_offset
    """
    r = session.get(
        url,
        headers={"Range": f"bytes={cd_offset}-{cd_offset + cd_size - 1}"},
        timeout=120,
    )
    r.raise_for_status()
    cd_data = r.content

    cd_sig = b"PK\x01\x02"
    off = 0
    entries = []

    while True:
        i = cd_data.find(cd_sig, off)
        if i < 0:
            break
        if i + 46 > len(cd_data):
            break

        comp_method       = struct.unpack_from("<H", cd_data, i + 10)[0]
        comp_size         = struct.unpack_from("<I", cd_data, i + 20)[0]
        uncomp_size       = struct.unpack_from("<I", cd_data, i + 24)[0]
        fname_len         = struct.unpack_from("<H", cd_data, i + 28)[0]
        extra_len         = struct.unpack_from("<H", cd_data, i + 30)[0]
        comment_len       = struct.unpack_from("<H", cd_data, i + 32)[0]
        local_hdr_offset  = struct.unpack_from("<I", cd_data, i + 42)[0]

        if i + 46 + fname_len > len(cd_data):
            break

        fname = cd_data[i + 46: i + 46 + fname_len].decode("utf-8", errors="replace")
        entries.append({
            "fname":            fname,
            "comp_method":      comp_method,
            "comp_size":        comp_size,
            "uncomp_size":      uncomp_size,
            "local_hdr_offset": local_hdr_offset,
        })

        off = i + 46 + fname_len + extra_len + comment_len

    return entries


def _is_ca_gpkg(fname: str) -> bool:
    """
    Return True if this ZIP entry is a CA county GPKG (not metadata).

    Files inside ZIPs are named: <dir>/<cbsa>/<raw_fips>_<County>_CA.gpkg
    CA counties: state FIPS 06 → files end with _CA.gpkg.
    Excludes __MACOSX metadata entries.
    """
    if fname.startswith("__MACOSX"):
        return False
    basename = fname.split("/")[-1]
    return basename.endswith("_CA.gpkg") and not basename.startswith("._")


def _decompress_deflate(data: bytes) -> bytes:
    """Decompress raw DEFLATE data (wbits=-15, no zlib/gzip header)."""
    return zlib.decompress(data, wbits=-15)


def _extract_ca_entry(
    url: str,
    entry: dict,
    dest_path: Path,
    session: requests.Session,
    retries: int = 3,
) -> None:
    """
    Extract a single CA GPKG from a ZIP using HTTP range requests.

    1. Read the local file header to get the exact data start offset.
    2. Download the compressed data with retries.
    3. Decompress if method == 8 (DEFLATE).
    4. Write to dest_path.
    """
    lh_offset = entry["local_hdr_offset"]

    # Read local file header (up to 300 bytes is more than enough)
    r = session.get(url, headers={"Range": f"bytes={lh_offset}-{lh_offset + 299}"},
                    timeout=30)
    r.raise_for_status()
    lh = r.content

    if lh[:4] != b"PK\x03\x04":
        raise ValueError(f"Local file header signature mismatch for {entry['fname']}")

    lh_fname_len  = struct.unpack_from("<H", lh, 26)[0]
    lh_extra_len  = struct.unpack_from("<H", lh, 28)[0]
    data_start    = lh_offset + 30 + lh_fname_len + lh_extra_len
    data_end      = data_start + entry["comp_size"] - 1

    # Download compressed data (with retries for network interruptions)
    for attempt in range(1, retries + 1):
        try:
            r2 = session.get(
                url,
                headers={"Range": f"bytes={data_start}-{data_end}"},
                timeout=300,
            )
            r2.raise_for_status()
            compressed = r2.content
            break
        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout) as exc:
            if attempt == retries:
                raise
            print(f"    [retry {attempt}/{retries}] {entry['fname']}: {exc}")

    # Decompress
    if entry["comp_method"] == 8:
        raw = _decompress_deflate(compressed)
    elif entry["comp_method"] == 0:
        raw = compressed  # stored (no compression)
    else:
        raise ValueError(f"Unsupported compression method {entry['comp_method']} "
                         f"for {entry['fname']}")

    dest_path.parent.mkdir(parents=True, exist_ok=True)
    dest_path.write_bytes(raw)


def _sentinel(source_name: str) -> Path:
    return ARRUDA_DIR / f".extracted_{source_name}"


def extract_ca_from_source(
    source_name: str,
    url: str,
    session: requests.Session,
) -> list[Path]:
    """
    Range-extract all CA county GPKGs from one source ZIP URL.

    Returns list of extracted GPKG paths.
    Skips extraction if sentinel file already exists.
    """
    sentinel = _sentinel(source_name)
    if sentinel.exists():
        existing = sorted(ARRUDA_DIR.glob("*.gpkg"))
        print(f"    [skip] {source_name} already extracted")
        return existing

    # Step 1: content-length
    head = session.head(url, timeout=15, allow_redirects=True)
    content_length = int(head.headers.get("Content-Length", 0))
    print(f"    Size: {content_length / 1048576:.0f} MB")

    # Step 2: EOCD
    cd_offset, cd_size = _read_eocd(url, content_length, session)
    print(f"    Central directory: {cd_size:,} bytes at offset {cd_offset:,}")

    # Step 3: Central directory
    all_entries = _read_central_directory(url, cd_offset, cd_size, session)
    ca_entries  = [e for e in all_entries if _is_ca_gpkg(e["fname"])]
    print(f"    Total entries: {len(all_entries)}, CA GPKGs: {len(ca_entries)}")

    extracted = []
    ARRUDA_DIR.mkdir(parents=True, exist_ok=True)

    for e in ca_entries:
        basename = e["fname"].split("/")[-1]      # e.g. "6103_Tehama_CA.gpkg"
        dest = ARRUDA_DIR / basename
        if dest.exists():
            print(f"      [skip] {basename}")
            extracted.append(dest)
            continue
        uncomp_mb = e["uncomp_size"] / 1048576
        print(f"      [extract] {basename} ({uncomp_mb:.1f} MB) ...", end=" ", flush=True)
        _extract_ca_entry(url, e, dest, session)
        actual_mb = dest.stat().st_size / 1048576
        print(f"done ({actual_mb:.1f} MB)")
        extracted.append(dest)

    n_extracted = len([e for e in ca_entries
                       if (ARRUDA_DIR / e["fname"].split("/")[-1]).exists()])
    sentinel.write_text(f"Extracted {n_extracted} CA GPKGs from {source_name}\n")
    return extracted


def collect_ca_gpkgs(source_urls: dict[str, str],
                     session: requests.Session) -> list[Path]:
    """
    Range-extract CA GPKGs from all source ZIPs.
    Returns sorted list of all extracted GPKG paths.
    """
    for i, (name, url) in enumerate(source_urls.items(), 1):
        print(f"\n  Source {i}/{len(source_urls)}: {name}")
        extract_ca_from_source(name, url, session)

    gpkgs = sorted(ARRUDA_DIR.glob("[0-9]*.gpkg"))
    print(f"\n  Total CA county GPKGs: {len(gpkgs)}")
    return gpkgs


# ---------------------------------------------------------------------------
# Step 3 — Count RES buildings per county
# ---------------------------------------------------------------------------

def _parse_gpkg_county_fips(stem: str) -> tuple[str, str]:
    """
    Parse county_FIPS and county_name from a GPKG filename stem.

    Filename format: <raw_fips>_<County_Name>_CA
    e.g. "6103_Tehama_CA" → ("06103", "Tehama")
         "6037_Los_Angeles_CA" → ("06037", "Los Angeles")

    raw_fips has the leading zero dropped for CA (state 06);
    zero-pad to 5 digits to recover the standard 5-digit FIPS.
    """
    parts = stem.split("_")
    raw_fips = parts[0]
    county_fips = raw_fips.zfill(5)

    # Everything between raw_fips and the trailing "CA" is the county name
    if len(parts) >= 3 and parts[-1] == "CA":
        county_name = " ".join(parts[1:-1])
    else:
        county_name = " ".join(parts[1:])

    return county_fips, county_name


def count_res_buildings(local_paths: list[Path]) -> pd.DataFrame:
    """
    Read each GPKG (type column only, no geometry) and count RES vs total rows.

    Returns DataFrame: county_FIPS, county_name, arruda_res_count,
                       arruda_total_count, pct_residential
    """
    records = []

    for path in local_paths:
        county_fips, county_name = _parse_gpkg_county_fips(path.stem)

        try:
            df = pyogrio.read_dataframe(path, columns=["type"], use_arrow=False)
            res_count   = int((df["type"] == "RES").sum())
            total_count = int(len(df))
            pct_res = res_count / total_count if total_count > 0 else 0.0
            print(f"  {county_fips} {county_name:<22s}  "
                  f"RES={res_count:>7,}  total={total_count:>7,}  pct={pct_res:.1%}")
        except Exception as exc:
            warnings.warn(f"Could not read {path.name}: {exc}")
            res_count   = None
            total_count = None
            pct_res     = None

        records.append({
            "county_FIPS":        county_fips,
            "county_name":        county_name,
            "arruda_res_count":   res_count,
            "arruda_total_count": total_count,
            "pct_residential":    pct_res,
        })

    df_counts = pd.DataFrame(records)
    df_counts = df_counts.sort_values("county_FIPS").reset_index(drop=True)
    return df_counts


# ---------------------------------------------------------------------------
# Step 4 — Build comparison table
# ---------------------------------------------------------------------------

def build_comparison_table(
    arruda_counts: pd.DataFrame,
) -> tuple[pd.DataFrame, float | None]:
    """
    Join Arruda county RES counts with Overture, Bootstrap P50, and ACS.

    Returns (comparison DataFrame, Spearman rho or None).
    """
    tract_counts = pd.read_parquet(IN_TRACT_COUNTS)
    overture = (
        tract_counts.groupby("county_FIPS")["overture_residential_count_2024"]
        .sum()
        .reset_index()
        .rename(columns={"overture_residential_count_2024": "overture_labeled"})
    )

    bootstrap = pd.read_parquet(IN_BOOTSTRAP)
    boot_2024 = (
        bootstrap[bootstrap["year"] == 2024]
        .groupby("county_FIPS")["p50_residential_count"]
        .sum()
        .reset_index()
        .rename(columns={"p50_residential_count": "bootstrap_p50"})
    )

    acs = pd.read_parquet(IN_ACS)
    acs_2020 = (
        acs[acs["year"] == 2020]
        .groupby("county_FIPS")["acs_housing_units"]
        .sum()
        .reset_index()
        .rename(columns={"acs_housing_units": "acs_units"})
    )

    comp = (
        overture
        .merge(boot_2024,  on="county_FIPS", how="outer")
        .merge(acs_2020,   on="county_FIPS", how="outer")
        .merge(
            arruda_counts[["county_FIPS", "county_name",
                           "arruda_res_count", "pct_residential"]],
            on="county_FIPS",
            how="outer",
        )
    )
    comp = comp.sort_values("county_FIPS").reset_index(drop=True)

    valid = comp.dropna(subset=["arruda_res_count", "bootstrap_p50"])
    rho = None
    if len(valid) > 1:
        ratio      = valid["arruda_res_count"] / valid["bootstrap_p50"]
        rho, pval  = stats.spearmanr(valid["arruda_res_count"], valid["bootstrap_p50"])
        print(f"\n  Summary:")
        print(f"    Counties with Arruda data: {valid['county_FIPS'].nunique()}")
        print(f"    Mean Arruda / Bootstrap P50:   {ratio.mean():.3f}")
        print(f"    Median Arruda / Bootstrap P50: {ratio.median():.3f}")
        print(f"    Spearman rho: {rho:.4f}  (p={pval:.2e})")
    else:
        print("  [warn] Too few valid counties for Spearman correlation")

    return comp, rho


# ---------------------------------------------------------------------------
# Step 5 — Arruda-anchored hind-cast tract panel
# ---------------------------------------------------------------------------

def build_arruda_panel(arruda_counts: pd.DataFrame) -> pd.DataFrame:
    """
    Build a tract × year panel using Arruda 2024 county RES counts as anchor.

    Same backward hind-cast as 04_build_structure_panel.py:
      net_change_s = structures_permitted_s - dins_destroyed_s
      net_after_t  = Σ_{s=t+1..2024} net_change_s (reverse cumsum, shifted by 1)
      county_count_t = max(arruda_county_2024 - net_after_t, 1)

    Tract downscale:
      residential_count_hindcast_it = tract_share_i × county_count_hindcast_t
    where tract_share comes from tract_structure_panel.parquet (Overture-derived).
    """
    permits = pd.read_parquet(IN_BPS_PERMITS)
    dins    = pd.read_parquet(IN_DINS)

    required = {"county_FIPS", "year", "structures_permitted"}
    missing  = required - set(permits.columns)
    if missing:
        raise ValueError(f"county_permits missing columns: {missing}")
    print(f"  [ok] county_permits columns: {list(permits.columns)}")

    perm = permits.merge(
        dins[["county_FIPS", "year", "structures_destroyed"]],
        on=["county_FIPS", "year"],
        how="left",
    )
    perm["structures_destroyed"]  = perm["structures_destroyed"].fillna(0)
    perm["net_structures_change"] = (
        perm["structures_permitted"] - perm["structures_destroyed"]
    )

    # Arruda 2024 anchor
    arruda_anchor = (
        arruda_counts[arruda_counts["arruda_res_count"].notna()]
        [["county_FIPS", "arruda_res_count"]]
        .rename(columns={"arruda_res_count": "county_anchor"})
        .copy()
    )
    print(f"  Arruda anchors: {len(arruda_anchor)} counties")

    missing_perm = set(perm["county_FIPS"].unique()) - set(arruda_anchor["county_FIPS"])
    if missing_perm:
        print(f"  [warn] {len(missing_perm)} permit counties missing Arruda anchor "
              f"(will be dropped from panel)")

    perm = perm.merge(arruda_anchor, on="county_FIPS", how="inner")

    perm = perm.sort_values(["county_FIPS", "year"]).copy()
    perm["rev_cumsum"] = (
        perm.groupby("county_FIPS")["net_structures_change"]
        .transform(lambda x: x[::-1].cumsum()[::-1])
    )
    perm["net_after_t"] = (
        perm.groupby("county_FIPS")["rev_cumsum"]
        .transform(lambda x: x.shift(-1, fill_value=0))
    )
    perm["county_count_hindcast"] = (
        (perm["county_anchor"] - perm["net_after_t"]).clip(lower=1).round().astype(int)
    )

    county_panel = perm[["county_FIPS", "year", "county_anchor",
                          "county_count_hindcast"]].copy()

    check = county_panel[county_panel["year"] == 2024]
    mm = (check["county_count_hindcast"] !=
          check["county_anchor"].round().astype(int)).sum()
    if mm > 0:
        print(f"  [WARN] {mm} counties: Arruda 2024 hind-cast ≠ anchor")
    else:
        print(f"  [ok] 2024 Arruda hind-cast equals anchor for all counties")

    # Tract downscale
    panel_base   = pd.read_parquet(IN_PANEL)
    tract_shares = (
        panel_base[panel_base["year"] == 2024]
        [["geoid", "county_FIPS", "tract_share"]]
        .drop_duplicates()
    )

    arruda_tract = tract_shares.merge(
        county_panel[["county_FIPS", "year",
                      "county_anchor", "county_count_hindcast"]],
        on="county_FIPS",
        how="inner",
    )
    arruda_tract["residential_count_hindcast"] = (
        arruda_tract["tract_share"] * arruda_tract["county_count_hindcast"]
    ).clip(lower=1).round(2)

    arruda_tract = arruda_tract[
        ["geoid", "county_FIPS", "year", "tract_share",
         "county_anchor", "county_count_hindcast", "residential_count_hindcast"]
    ].sort_values(["geoid", "year"]).reset_index(drop=True)

    n_tracts = arruda_tract["geoid"].nunique()
    n_years  = arruda_tract["year"].nunique()
    print(f"  Panel: {n_tracts:,} tracts × {n_years} years = {len(arruda_tract):,} rows")

    return arruda_tract


# ---------------------------------------------------------------------------
# Step 6 — Comparison figure
# ---------------------------------------------------------------------------

def make_comparison_figure(comp: pd.DataFrame, rho: float | None) -> None:
    """
    Two-panel figure:
      A: County scatter — Arruda vs Bootstrap P50 (Spearman rho annotated)
      B: Horizontal bar — top 20 counties by ACS housing units
    """
    valid = comp.dropna(subset=["arruda_res_count", "bootstrap_p50"]).copy()
    for col in ["arruda_res_count", "bootstrap_p50", "overture_labeled"]:
        valid[col] = valid[col].astype(float)
    valid["overture_labeled"] = valid["overture_labeled"].fillna(0)

    top20 = (
        comp.dropna(subset=["acs_units"])
        .nlargest(20, "acs_units")
        .sort_values("acs_units", ascending=True)
        .copy()
    )

    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    fig.patch.set_facecolor("white")

    # ── Panel A: scatter ─────────────────────────────────────────────────────
    ax = axes[0]
    ax.set_facecolor("white")

    # Compute ratio = Arruda / Bootstrap P50 to identify outliers for labeling
    valid["ratio"] = valid["arruda_res_count"] / valid["bootstrap_p50"].replace(0, float("nan"))
    q1, q3 = valid["ratio"].quantile(0.25), valid["ratio"].quantile(0.75)
    iqr = q3 - q1
    outlier_mask = (valid["ratio"] < q1 - 1.5 * iqr) | (valid["ratio"] > q3 + 1.5 * iqr)
    # Also label the 5 largest counties (by bootstrap_p50) for navigation context
    top5_idx = valid.nlargest(5, "bootstrap_p50").index
    label_mask = outlier_mask | valid.index.isin(top5_idx)

    # Plot all points
    ax.scatter(valid.loc[~label_mask, "bootstrap_p50"] / 1000,
               valid.loc[~label_mask, "arruda_res_count"] / 1000,
               s=28, alpha=0.65, color="#2c7bb6", edgecolors="none")
    ax.scatter(valid.loc[label_mask, "bootstrap_p50"] / 1000,
               valid.loc[label_mask, "arruda_res_count"] / 1000,
               s=38, alpha=0.85, color="#d7191c", edgecolors="none",
               zorder=3)

    # Label outliers + top-5 with county name (short)
    for _, row in valid[label_mask].iterrows():
        x = row["bootstrap_p50"] / 1000
        y = row["arruda_res_count"] / 1000
        name = row["county_name"].replace(" County", "").strip()
        ax.annotate(
            name,
            xy=(x, y),
            xytext=(4, 2),
            textcoords="offset points",
            fontsize=6.5,
            color="#333333",
            va="bottom",
        )

    lim_max = max(valid["bootstrap_p50"].max(),
                  valid["arruda_res_count"].max()) / 1000 * 1.05
    ax.plot([0, lim_max], [0, lim_max], "--", color="#888888",
            linewidth=1.0, label="1:1 line")
    if rho is not None:
        ax.text(0.05, 0.92, f"Spearman \u03c1 = {rho:.3f}",
                transform=ax.transAxes, fontsize=9, color="#555555")
    ax.set_xlabel("Bootstrap P50 (thousands)", fontsize=10)
    ax.set_ylabel("Arruda RES buildings (thousands)", fontsize=10)
    ax.set_title("A  Arruda vs Bootstrap P50 — County Level",
                 fontsize=11, loc="left", fontweight="bold")
    ax.legend(fontsize=8, frameon=False)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.0f}k"))
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.0f}k"))
    ax.tick_params(labelsize=8)

    # ── Panel B: bar chart ────────────────────────────────────────────────────
    ax = axes[1]
    ax.set_facecolor("white")
    labels = top20["county_name"].str.strip()
    y_pos  = np.arange(len(labels))
    # 4 bars per county: offsets centered at 0 with bar_h=0.2 spacing
    bar_h  = 0.2
    offsets = [-0.3, -0.1, 0.1, 0.3]
    color_map = [
        ("overture_labeled", "#4dac26", "Overture labeled"),
        ("arruda_res_count", "#2c7bb6", "Arruda RES"),
        ("bootstrap_p50",    "#d7191c", "Bootstrap P50"),
        ("acs_units",        "#f4a442", "ACS B25001 (units)"),
    ]
    for offset, (col, color, label) in zip(offsets, color_map):
        vals = top20[col].fillna(0) / 1000
        ax.barh(y_pos + offset, vals,
                height=bar_h * 0.9, color=color, alpha=0.8, label=label)
    ax.set_yticks(y_pos)
    ax.set_yticklabels(labels, fontsize=7)
    ax.set_xlabel("Residential structures / housing units (thousands)", fontsize=10)
    ax.set_title("B  Top 20 Counties by ACS Housing Units",
                 fontsize=11, loc="left", fontweight="bold")
    ax.legend(fontsize=8, frameon=False, loc="lower right")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:.0f}k"))
    ax.tick_params(axis="x", labelsize=8)

    plt.suptitle(
        "Arruda et al. (2024) OSM Classification \u2014 California County Comparison",
        fontsize=12, y=1.01,
    )
    plt.tight_layout()
    OUT_FIGURES.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUT_FIGURE, dpi=300, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"  [saved] {OUT_FIGURE.name}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 70)
    print("07_acquire_arruda_comparison.py — Arruda et al. (2024)")
    print("=" * 70)

    session = requests.Session()
    session.headers["User-Agent"] = "ca-residential-structure-panel/1.1 (research)"

    # ── Step 1: Resolve download URLs ─────────────────────────────────────────
    print("\n--- Step 1: OSF API discovery ---")
    source_urls = discover_source_urls(session)
    if not source_urls:
        raise RuntimeError("No source ZIPs found via OSF API.")

    # ── Step 2: Range-extract CA GPKGs ────────────────────────────────────────
    print("\n--- Step 2: Range-extracting CA county GPKGs ---")
    print("  (Only CA files are downloaded — avoids 8 GB full ZIP downloads)")
    local_paths = collect_ca_gpkgs(source_urls, session)

    if not local_paths:
        raise RuntimeError(
            "No CA county GPKGs extracted. "
            "Check _is_ca_gpkg() filter and OSF file naming convention."
        )

    # ── Step 3: Count RES buildings ───────────────────────────────────────────
    print("\n--- Step 3: Counting RES buildings per county ---")
    arruda_counts = count_res_buildings(local_paths)
    n_valid   = arruda_counts["arruda_res_count"].notna().sum()
    total_res = int(arruda_counts["arruda_res_count"].sum())
    print(f"\n  Valid counties: {n_valid} / {len(arruda_counts)}")
    print(f"  Total CA Arruda RES buildings: {total_res:,}")

    OUT_TABLES.mkdir(parents=True, exist_ok=True)
    arruda_counts.to_parquet(OUT_COUNTY_COUNTS, index=False)
    print(f"  [saved] {OUT_COUNTY_COUNTS.name}")

    # ── Step 4: Comparison table ──────────────────────────────────────────────
    print("\n--- Step 4: Building comparison table ---")
    comp, rho = build_comparison_table(arruda_counts)
    comp.to_csv(OUT_COMPARISON, index=False)
    print(f"  [saved] {OUT_COMPARISON.name}")
    print(f"\n  Preview (top 5 by ACS units):")
    preview = ["county_FIPS", "county_name", "overture_labeled",
               "arruda_res_count", "bootstrap_p50", "acs_units"]
    with pd.option_context("display.max_columns", 10, "display.width", 120):
        print(comp.nlargest(5, "acs_units")[preview].to_string(index=False))

    # ── Step 5: Arruda panel ──────────────────────────────────────────────────
    print("\n--- Step 5: Building Arruda-anchored hind-cast panel ---")
    arruda_panel = build_arruda_panel(arruda_counts)
    arruda_panel.to_parquet(OUT_ARRUDA_PANEL, index=False)
    print(f"  [saved] {OUT_ARRUDA_PANEL.name}")

    # ── Step 6: Figure ────────────────────────────────────────────────────────
    print("\n--- Step 6: Generating comparison figure ---")
    make_comparison_figure(comp, rho)

    print("\n" + "=" * 70)
    print("Done.")
    print(f"  County counts:   {OUT_COUNTY_COUNTS}")
    print(f"  Comparison:      {OUT_COMPARISON}")
    print(f"  Arruda panel:    {OUT_ARRUDA_PANEL}")
    print(f"  Figure:          {OUT_FIGURE}")
    print("=" * 70)


if __name__ == "__main__":
    main()
