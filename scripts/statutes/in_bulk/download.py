"""Download + extract the official Indiana Code bulk HTML ZIP.

iga.in.gov geo-fences the ZIP, it does not bot-wall the file itself: a non-US
client (including the scraper box's direct German egress) gets a 691-byte SPA
shell served with HTTP 200, while a US exit gets the real ~43 MB
``application/zip``. So we ALWAYS fetch through the Webshare US-rotate proxy and
validate the response is a real ZIP (zip content-type + ``PK`` magic bytes),
rotating to a fresh US exit and retrying until it is. The file URL is
year-templated:

    https://iga.in.gov/ic/{year}/{year}-Indiana-Code-html.zip

The downloads page (iga.in.gov/laws/ic/downloads) is a JS SPA that never exposes
this file URL to a non-browser client, which is why the URL is hardcoded here
rather than discovered. Because the URL is year-templated, refresh tries the
current edition year and falls back to the prior year across the annual rollover
(a new edition posts mid-year), so no manual bump is needed.
"""

from __future__ import annotations

import shutil
import sys
import time
import zipfile
from pathlib import Path

import requests

# state_scrapers holds the shared proxy/UA helpers; make sure it is importable
# whether this module is run via the orchestrator or imported directly. Walk up to
# find it rather than assume a fixed nesting depth.
for _p in Path(__file__).resolve().parents:
    _cand = _p / "scripts" / "us_corpus" / "state_scrapers"
    if _cand.is_dir():
        if str(_cand) not in sys.path:
            sys.path.insert(0, str(_cand))
        break

from vaquill_pipeline.http_client import _proxy_for, _random_profile

_URL_TMPL = "https://iga.in.gov/ic/{year}/{year}-Indiana-Code-html.zip"
_ZIP_MAGIC = b"PK\x03\x04"


def zip_url(year: int) -> str:
    return _URL_TMPL.format(year=year)


def _looks_like_zip(content_type: str, body: bytes) -> bool:
    ct = (content_type or "").lower()
    return "zip" in ct and body[:4] == _ZIP_MAGIC


def download_zip(year: int, dest: Path, retries: int = 8) -> Path:
    """Fetch the year's IC ZIP via the US proxy, validate it is a real ZIP, save it.

    Retries on the geo-fence shell (HTTP 200 tiny text/html) and on transient
    proxy/network errors, each attempt rotating to a fresh US exit. Raises
    SystemExit if no valid ZIP is obtained.
    """
    proxies = _proxy_for("us")
    if not proxies:
        raise SystemExit(
            "WEBSHARE_USERNAME / WEBSHARE_PASSWORD not set: cannot reach the "
            "geo-fenced Indiana Code ZIP. Set them, or pass --src with a local copy."
        )
    url = zip_url(year)
    last = ""
    for attempt in range(1, retries + 1):
        prof = _random_profile()
        headers = {"User-Agent": prof["ua"], "Accept": "*/*"}
        try:
            r = requests.get(
                url, headers=headers, proxies=proxies, timeout=180, allow_redirects=True
            )
        except Exception as exc:
            last = f"{type(exc).__name__}: {str(exc)[:120]}"
            print(f"  [in-dl] attempt {attempt}/{retries} error: {last}", flush=True)
            time.sleep(min(2 * attempt, 10))
            continue
        ct = r.headers.get("Content-Type", "")
        body = r.content
        if r.status_code == 200 and _looks_like_zip(ct, body):
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(body)
            print(
                f"  [in-dl] got {len(body):,} bytes ({ct}) on attempt {attempt} -> {dest}",
                flush=True,
            )
            return dest
        # Geo-fence shell or transient block: rotate the exit and retry.
        last = f"HTTP {r.status_code} {ct} {len(body)}B"
        print(f"  [in-dl] attempt {attempt}/{retries} not-a-zip: {last} (rotating)", flush=True)
        time.sleep(min(2 * attempt, 10))
    raise SystemExit(
        f"could not download a valid Indiana Code ZIP for {year} after {retries} "
        f"attempts (last: {last})"
    )


def _extract_root(extract_dir: Path) -> Path:
    """The dir holding the per-title *.html files inside the extracted ZIP."""
    if any(extract_dir.glob("*.html")):
        return extract_dir
    html_dirs = [p for p in extract_dir.rglob("*") if p.is_dir() and any(p.glob("*.html"))]
    for p in html_dirs:
        if "indiana_code_html" in p.name.lower():
            return p
    if html_dirs:
        return max(html_dirs, key=lambda d: len(list(d.glob("*.html"))))
    return extract_dir


def download_and_extract(year: int, workdir: Path) -> Path:
    """Download the IC ZIP for ``year`` and extract it under ``workdir``.

    Returns the directory that directly holds the per-title *.html files. The
    workdir is wiped first so each refresh starts from a clean download.
    """
    workdir = Path(workdir)
    if workdir.exists():
        shutil.rmtree(workdir)
    workdir.mkdir(parents=True, exist_ok=True)
    zip_path = workdir / f"{year}-Indiana-Code-html.zip"
    download_zip(year, zip_path)
    extract_dir = workdir / "extracted"
    extract_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(extract_dir)
    root = _extract_root(extract_dir)
    print(f"  [in-dl] extracted to {root}", flush=True)
    return root


def resolve_and_download(session: str, workdir: Path) -> tuple[Path, str]:
    """Download the IC ZIP, trying the session year then the prior year.

    Returns ``(html_root, resolved_year)``. The prior-year fallback covers the
    annual rollover, when the new edition has not been posted yet.
    """
    base_year = int(session) if str(session).isdigit() else time.gmtime().tm_year
    errs: list[str] = []
    for yr in (base_year, base_year - 1):
        try:
            return download_and_extract(yr, workdir), str(yr)
        except SystemExit as exc:
            errs.append(f"{yr}: {exc}")
            print(f"  [in-dl] year {yr} unavailable, trying prior year", flush=True)
    raise SystemExit(f"no Indiana Code ZIP for {base_year} or {base_year - 1}: " + " | ".join(errs))
