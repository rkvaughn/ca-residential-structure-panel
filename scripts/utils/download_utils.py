"""
download_utils.py
=================
General-purpose file download and extraction helpers.

Functions
---------
download_zip(url, dest_dir, name, session=None)
    Download a ZIP archive and extract it to dest_dir/name/.
    Skips if the directory already exists and is non-empty.
    Returns the extraction directory path.

download_file(url, dest_path, session=None)
    Download a single file to dest_path.
    Skips if dest_path already exists.
    Returns dest_path.

Dependencies
------------
    requests  (pip install requests)
    Standard library: io, zipfile, pathlib

Usage example
-------------
    from download_utils import download_zip, download_file

    # Download and extract a Census TIGER shapefile
    shp_dir = download_zip(
        url="https://www2.census.gov/geo/tiger/TIGER2020/TRACT/tl_2020_06_tract.zip",
        dest_dir=Path("data/raw/shapefiles"),
        name="tl_2020_06_tract",
    )

    # Download a plain-text relationship file
    txt_path = download_file(
        url="https://www2.census.gov/geo/docs/maps-data/data/rel2020/zcta520/tab20_zcta520_tract20_natl.txt",
        dest_path=Path("data/raw/shapefiles/tab20_zcta520_tract20_natl.txt"),
    )

Notes
-----
- Both functions use skip-if-exists logic so they are safe to call repeatedly in
  reproducible pipelines.
- Pass a requests.Session for connection pooling / custom headers / auth.
- Chunk size defaults to 1 MB; increase for very large files over fast connections.
"""

import io
import zipfile
from pathlib import Path
from typing import Optional

try:
    import requests as _requests
    _HAS_REQUESTS = True
except ImportError:
    _HAS_REQUESTS = False


def _get_session(session):
    if session is not None:
        return session
    if not _HAS_REQUESTS:
        raise ImportError("requests is required: pip install requests")
    import requests
    return requests.Session()


def download_zip(
    url: str,
    dest_dir: Path,
    name: str,
    session=None,
    chunk_size: int = 1 << 20,
    timeout: int = 300,
) -> Path:
    """
    Download a ZIP archive from *url* and extract it to *dest_dir*/*name*/.

    Parameters
    ----------
    url : str
        Full URL of the ZIP file.
    dest_dir : Path
        Parent directory in which to create the extraction subdirectory.
    name : str
        Name of the subdirectory to extract into (also used as a label in logs).
    session : requests.Session, optional
        Pre-configured session. A new session is created if None.
    chunk_size : int
        Download chunk size in bytes. Default 1 MB.
    timeout : int
        Request timeout in seconds. Default 300.

    Returns
    -------
    Path
        Path to the extraction directory (*dest_dir*/*name*/).

    Raises
    ------
    requests.HTTPError
        If the server returns a non-2xx status code.
    """
    dest_dir = Path(dest_dir)
    out_dir = dest_dir / name
    if out_dir.exists() and any(out_dir.iterdir()):
        print(f"  [skip] {name} already downloaded")
        return out_dir

    out_dir.mkdir(parents=True, exist_ok=True)
    sess = _get_session(session)
    print(f"  [download] {name} from {url} ...")
    resp = sess.get(url, timeout=timeout, stream=True)
    resp.raise_for_status()

    content = b"".join(resp.iter_content(chunk_size=chunk_size))
    with zipfile.ZipFile(io.BytesIO(content)) as zf:
        zf.extractall(out_dir)

    print(f"  [ok] extracted to {out_dir}")
    return out_dir


def download_file(
    url: str,
    dest_path: Path,
    session=None,
    chunk_size: int = 1 << 20,
    timeout: int = 300,
) -> Path:
    """
    Download a single file from *url* to *dest_path*.

    Parameters
    ----------
    url : str
        Full URL of the file to download.
    dest_path : Path
        Destination file path (parent directory must exist or will be created).
    session : requests.Session, optional
        Pre-configured session. A new session is created if None.
    chunk_size : int
        Download chunk size in bytes. Default 1 MB.
    timeout : int
        Request timeout in seconds. Default 300.

    Returns
    -------
    Path
        *dest_path* (the saved file).

    Raises
    ------
    requests.HTTPError
        If the server returns a non-2xx status code.
    """
    dest_path = Path(dest_path)
    if dest_path.exists():
        print(f"  [skip] {dest_path.name} already downloaded")
        return dest_path

    dest_path.parent.mkdir(parents=True, exist_ok=True)
    sess = _get_session(session)
    print(f"  [download] {dest_path.name} from {url} ...")
    resp = sess.get(url, timeout=timeout, stream=True)
    resp.raise_for_status()

    with open(dest_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=chunk_size):
            f.write(chunk)

    size_mb = dest_path.stat().st_size / (1 << 20)
    print(f"  [ok] saved {size_mb:.1f} MB → {dest_path}")
    return dest_path
