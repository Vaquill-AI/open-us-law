#!/usr/bin/env python3
"""
Download the United States Code from GovInfo API.

Source: https://api.govinfo.gov (Government Publishing Office)
Data:   All 54 USC titles, section-level granules with HTML text + PDF
Auth:   Free API key from api.data.gov (or DEMO_KEY for testing)
Rate:   36,000 req/hr, 1,200 req/min, 40 req/sec

Pipeline:
  1. List all USCODE packages (one per title per year)
  2. For each title: download the ZIP (contains all sections as HTML)
  3. Extract and parse individual section HTML into structured records
  4. Store parsed sections as JSONL at: data/parsed/usc_sections.jsonl

Usage:
    # Download latest year for all titles
    python scripts/federal/download_usc.py

    # Download specific title
    python scripts/federal/download_usc.py --title 42

    # Dry run (list packages only)
    python scripts/federal/download_usc.py --dry-run
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import io
import json
import os
import re
import sys
import time
import zipfile
from dataclasses import asdict, dataclass, field
from html.parser import HTMLParser
from pathlib import Path

import httpx

# Force unbuffered output for background/pipe execution
sys.stdout.reconfigure(line_buffering=True)  # type: ignore[attr-defined]
sys.stderr.reconfigure(line_buffering=True)  # type: ignore[attr-defined]

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

GOVINFO_API_BASE = "https://api.govinfo.gov"
GOVINFO_API_KEY = os.environ.get("GOVINFO_API_KEY", "DEMO_KEY")

# Local storage
DATA_DIR = _PROJECT_ROOT / "data" / "us_corpus"
RAW_DIR = DATA_DIR / "usc" / "raw"
PARSED_DIR = DATA_DIR / "parsed"

# Rate limiting
# GovInfo limits: 36,000 req/hr, 1,200 req/min, 40 req/sec
# We use a semaphore for concurrency + small per-request delay for safety
MAX_CONCURRENT_REQUESTS = 20  # Parallel in-flight requests (safe margin below 40/sec)
REQUEST_DELAY = 0.0  # No per-request delay (semaphore handles pacing)
GRANULE_PAGE_SIZE = 100  # Max granules per API page
SECTION_PARALLEL_BATCH = 20  # Sections fetched in parallel within a title

# We want the latest year's data
TARGET_YEARS = [2024, 2023]  # Prefer 2024, fall back to 2023


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class USCSection:
    """A single section of the United States Code."""

    act_id: str  # e.g., "USC_T42_S1983"
    title_number: int  # e.g., 42
    title_name: str  # e.g., "THE PUBLIC HEALTH AND WELFARE"
    chapter: str  # e.g., "21"
    chapter_name: str  # e.g., "CIVIL RIGHTS"
    subchapter: str  # e.g., "I"
    subchapter_name: str  # e.g., "GENERALLY"
    section_number: str  # e.g., "1983"
    section_title: str  # e.g., "Civil action for deprivation of rights"
    text: str  # Full section text (HTML stripped)
    html: str  # Original HTML
    year: int  # e.g., 2024
    granule_id: str  # GovInfo granule ID
    package_id: str  # GovInfo package ID
    source_url: str  # URL to section on govinfo.gov
    pdf_url: str  # URL to PDF
    last_modified: str  # ISO timestamp
    category: str = "usc"
    document_type: str = "statute"
    jurisdiction: str = "US"
    state: str = "federal"
    act_status: str = "in_force"

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class DownloadStats:
    """Track download progress."""

    titles_found: int = 0
    titles_downloaded: int = 0
    granules_found: int = 0
    granules_downloaded: int = 0
    sections_parsed: int = 0
    errors: list[str] = field(default_factory=list)
    start_time: float = field(default_factory=time.time)

    def elapsed(self) -> str:
        return f"{time.time() - self.start_time:.1f}s"


# ---------------------------------------------------------------------------
# HTML text extractor
# ---------------------------------------------------------------------------


class HTMLTextExtractor(HTMLParser):
    """Extract clean text from USC section HTML."""

    def __init__(self):
        super().__init__()
        self._text_parts: list[str] = []
        self._skip = False

    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style", "head"):
            self._skip = True
        elif tag in ("p", "div", "br", "li", "tr", "h1", "h2", "h3", "h4"):
            self._text_parts.append("\n")

    def handle_endtag(self, tag):
        if tag in ("script", "style", "head"):
            self._skip = False
        elif tag in ("p", "div", "li", "tr"):
            self._text_parts.append("\n")

    def handle_data(self, data):
        if not self._skip:
            self._text_parts.append(data)

    def get_text(self) -> str:
        raw = "".join(self._text_parts)
        # Normalize whitespace
        raw = re.sub(r"[ \t]+", " ", raw)
        raw = re.sub(r"\n{3,}", "\n\n", raw)
        return raw.strip()


def html_to_text(html: str) -> str:
    """Convert HTML to clean text."""
    parser = HTMLTextExtractor()
    parser.feed(html)
    return parser.get_text()


# ---------------------------------------------------------------------------
# GovInfo API client
# ---------------------------------------------------------------------------

# Module-level semaphore to cap concurrent in-flight requests.
# Initialized in download_usc() since it requires an event loop.
_api_semaphore: asyncio.Semaphore | None = None


def _get_semaphore() -> asyncio.Semaphore:
    global _api_semaphore
    if _api_semaphore is None:
        _api_semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    return _api_semaphore


async def _api_get(
    client: httpx.AsyncClient, url: str, params: dict | None = None, max_retries: int = 5
) -> dict:
    """Make a GovInfo API request with semaphore-based rate limiting and retries."""
    if params is None:
        params = {}
    params["api_key"] = GOVINFO_API_KEY

    sem = _get_semaphore()
    for attempt in range(max_retries):
        async with sem:
            try:
                resp = await client.get(url, params=params, timeout=60)
            except (httpx.ConnectError, httpx.ReadTimeout, httpx.ConnectTimeout) as e:
                wait = min(2 ** (attempt + 1), 60)
                print(f"  [Retry] Connection error on attempt {attempt + 1}: {type(e).__name__}, waiting {wait}s...")
                await asyncio.sleep(wait)
                continue
            if resp.status_code in (429, 500, 502, 503, 504):
                wait = min(2 ** (attempt + 1), 30)
                # Quiet the noisy 500s that happen mid-batch (only log first + last)
                if attempt == 0 or attempt == max_retries - 1:
                    print(f"  [Retry] HTTP {resp.status_code} attempt {attempt + 1}, waiting {wait}s ({url[-80:]})")
                await asyncio.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()

    raise httpx.HTTPStatusError(
        "Max retries exceeded", request=resp.request, response=resp
    )


async def _api_get_bytes(
    client: httpx.AsyncClient, url: str, max_retries: int = 5
) -> bytes:
    """Download binary content (ZIP, PDF, HTML) from GovInfo with retry."""
    params = {"api_key": GOVINFO_API_KEY}

    sem = _get_semaphore()
    for attempt in range(max_retries):
        async with sem:
            try:
                resp = await client.get(url, params=params, timeout=120, follow_redirects=True)
            except (httpx.ConnectError, httpx.ReadTimeout, httpx.ConnectTimeout) as e:
                wait = min(2 ** (attempt + 1), 60)
                print(f"  [Retry] Connection error on bytes download attempt {attempt + 1}: {type(e).__name__}, waiting {wait}s...")
                await asyncio.sleep(wait)
                continue
            if resp.status_code in (429, 500, 502, 503, 504):
                wait = min(2 ** (attempt + 1), 30)
                if attempt == 0 or attempt == max_retries - 1:
                    print(f"  [Retry] HTTP {resp.status_code} on bytes, waiting {wait}s...")
                await asyncio.sleep(wait)
                continue
        resp.raise_for_status()
        return resp.content

    raise httpx.HTTPStatusError(
        "Max retries exceeded (429)", request=resp.request, response=resp
    )


async def list_usc_packages(
    client: httpx.AsyncClient, target_years: list[int] | None = None
) -> list[dict]:
    """List all USCODE packages by probing per-package endpoints.

    The collection listing API is unreliable (500 errors on pagination),
    so we construct package IDs directly: USCODE-{year}-title{n}.
    Probe each to check if it exists.
    """
    if target_years is None:
        target_years = TARGET_YEARS

    # USC has titles 1-54 (with some gaps)
    ALL_TITLE_NUMBERS = list(range(1, 55))

    print(f"[USC] Probing USCODE packages for {len(ALL_TITLE_NUMBERS)} titles...")
    packages = []

    for title_num in ALL_TITLE_NUMBERS:
        found = False
        for year in target_years:
            pid = f"USCODE-{year}-title{title_num}"
            try:
                data = await _api_get(
                    client,
                    f"{GOVINFO_API_BASE}/packages/{pid}/summary",
                )
                packages.append({
                    "packageId": pid,
                    "title": data.get("title", ""),
                    "pages": data.get("pages", "?"),
                    "dateIssued": data.get("dateIssued", ""),
                    "lastModified": data.get("lastModified", ""),
                    "download": data.get("download", {}),
                })
                print(f"  Title {title_num:2d} ({year}): {data.get('title', '?')} ({data.get('pages', '?')} pages)")
                found = True
                break  # Use the first (most recent) year found
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    continue  # Try next year
                # Other errors: skip this title
                print(f"  Title {title_num:2d} ({year}): HTTP {e.response.status_code}, skipping")
                break

        if not found:
            print(f"  Title {title_num:2d}: not found for years {target_years}")

    print(f"[USC] Found {len(packages)} titles")
    return packages


async def _fetch_one_section(
    client: httpx.AsyncClient,
    granule: dict,
    package_id: str,
    title_number: int,
    title_name: str,
    year: int,
    stats: DownloadStats,
) -> USCSection | None:
    """Fetch HTML + summary for one granule in parallel. Returns None on failure/skip."""
    gid = granule["granuleId"]
    parts = _parse_granule_id(gid, title_number, title_name)

    try:
        # Fetch HTML and summary concurrently
        html_url = f"{GOVINFO_API_BASE}/packages/{package_id}/granules/{gid}/htm"
        summary_url = granule["granuleLink"]

        html_bytes_task = _api_get_bytes(client, html_url)
        summary_task = _api_get(client, summary_url)

        html_bytes, summary = await asyncio.gather(html_bytes_task, summary_task)
        html_content = html_bytes.decode("utf-8", errors="replace")

        text = html_to_text(html_content)
        if len(text.strip()) < 20:
            return None  # Repealed/reserved

        section = USCSection(
            act_id=f"USC_T{title_number}_S{parts['section_number']}",
            title_number=title_number,
            title_name=title_name,
            chapter=parts["chapter"],
            chapter_name=parts["chapter_name"],
            subchapter=parts["subchapter"],
            subchapter_name=parts["subchapter_name"],
            section_number=parts["section_number"],
            section_title=granule.get("title", ""),
            text=text,
            html=html_content,
            year=year,
            granule_id=gid,
            package_id=package_id,
            source_url=summary.get("detailsLink", ""),
            pdf_url=(summary.get("download", {}).get("pdfLink", "") + f"?api_key={GOVINFO_API_KEY}"),
            last_modified=summary.get("lastModified", ""),
        )
        stats.granules_downloaded += 1
        return section

    except Exception as e:
        stats.errors.append(f"Section {gid}: {type(e).__name__}: {str(e)[:100]}")
        return None


async def download_title_granules(
    client: httpx.AsyncClient,
    package_id: str,
    title_name: str,
    stats: DownloadStats,
) -> list[USCSection]:
    """Download all section-level granules for a USC title in parallel batches.

    Strategy:
      1. Paginate through granule list to collect all LEAF granules
      2. Fetch HTML + summary for each in parallel (bounded by semaphore)
      3. Build USCSection records with rich metadata
    """
    sections: list[USCSection] = []
    next_url = f"{GOVINFO_API_BASE}/packages/{package_id}/granules"
    next_params: dict = {"pageSize": GRANULE_PAGE_SIZE, "offsetMark": "*"}

    # Extract year and title number from package ID
    match = re.match(r"USCODE-(\d{4})-title(\d+)", package_id)
    year = int(match.group(1)) if match else 2024
    title_number = int(match.group(2)) if match else 0

    # Step 1: Collect all LEAF granules (sections)
    leaf_granules: list[dict] = []
    while next_url:
        try:
            data = await _api_get(client, next_url, next_params)
        except httpx.HTTPStatusError as e:
            stats.errors.append(f"Granule list failed for {package_id}: HTTP {e.response.status_code}")
            break

        for g in data.get("granules", []):
            if g.get("granuleClass") == "LEAF":
                leaf_granules.append(g)
                stats.granules_found += 1

        raw_next = data.get("nextPage")
        if raw_next:
            next_url = raw_next
            next_params = {}
        else:
            next_url = None

    total = len(leaf_granules)
    if total == 0:
        return sections

    print(f"    [{stats.elapsed()}] Title {title_number}: fetching {total} sections in parallel (concurrency={MAX_CONCURRENT_REQUESTS})...")

    # Step 2: Fetch all sections in parallel batches
    batch_start_time = time.time()
    for batch_start in range(0, total, SECTION_PARALLEL_BATCH):
        batch = leaf_granules[batch_start:batch_start + SECTION_PARALLEL_BATCH]
        tasks = [
            _fetch_one_section(client, g, package_id, title_number, title_name, year, stats)
            for g in batch
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for r in results:
            if isinstance(r, USCSection):
                sections.append(r)
            elif isinstance(r, Exception):
                stats.errors.append(f"Section batch error: {type(r).__name__}: {str(r)[:80]}")

        # Progress update every batch
        done = min(batch_start + SECTION_PARALLEL_BATCH, total)
        if batch_start % (SECTION_PARALLEL_BATCH * 5) == 0 or done == total:
            elapsed = time.time() - batch_start_time
            rate = done / elapsed if elapsed > 0 else 0
            eta_sec = (total - done) / rate if rate > 0 else 0
            print(
                f"    [{stats.elapsed()}] Title {title_number}: "
                f"{done}/{total} sections ({rate:.1f}/sec, ETA {eta_sec:.0f}s)"
            )

    return sections


def _parse_granule_id(granule_id: str, title_number: int, title_name: str) -> dict:
    """Extract structural info from a granule ID.

    Example: USCODE-2024-title42-chap21-subchapI-sec1983
    """
    result = {
        "chapter": "",
        "chapter_name": "",
        "subchapter": "",
        "subchapter_name": "",
        "section_number": "",
    }

    # Extract section number
    sec_match = re.search(r"-sec(\S+)$", granule_id)
    if sec_match:
        result["section_number"] = sec_match.group(1)

    # Extract chapter
    chap_match = re.search(r"-chap(\d+[A-Za-z]*)", granule_id)
    if chap_match:
        result["chapter"] = chap_match.group(1)

    # Extract subchapter
    subchap_match = re.search(r"-subchap([IVXLCDM]+|\d+[A-Za-z]*)", granule_id)
    if subchap_match:
        result["subchapter"] = subchap_match.group(1)

    return result


# ---------------------------------------------------------------------------
# JSONL output
# ---------------------------------------------------------------------------


def write_sections_jsonl(sections: list[USCSection], output_path: Path) -> int:
    """Write parsed sections to JSONL file (without HTML to save space)."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with open(output_path, "a", encoding="utf-8") as f:
        for section in sections:
            record = section.to_dict()
            # Don't include full HTML in JSONL
            record.pop("html", None)
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
            count += 1
    return count


# ---------------------------------------------------------------------------
# Main download orchestrator
# ---------------------------------------------------------------------------


async def download_usc(
    title_filter: int | None = None,
    dry_run: bool = False,
    max_titles: int | None = None,
    resume: bool = False,
) -> DownloadStats:
    """Download and parse the US Code from GovInfo API.

    Args:
        title_filter: Download only this title number (e.g., 42)
        dry_run: List packages only, don't download
        max_titles: Limit number of titles to download (for testing)
        resume: Skip titles that already have sections in the JSONL

    Returns:
        DownloadStats with counts and errors
    """
    stats = DownloadStats()

    # Ensure output directories exist
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    PARSED_DIR.mkdir(parents=True, exist_ok=True)

    output_path = PARSED_DIR / "usc_sections.jsonl"

    # Load completed titles from existing JSONL (for resume)
    completed_titles: set[int] = set()
    if resume and output_path.exists():
        with open(output_path, "r") as f:
            for line in f:
                try:
                    rec = json.loads(line)
                    tn = rec.get("title_number")
                    if tn:
                        completed_titles.add(int(tn))
                except (json.JSONDecodeError, ValueError, TypeError):
                    continue
        print(f"[USC] Resume mode: {len(completed_titles)} titles already complete: {sorted(completed_titles)}")

    # Clear previous output if starting fresh (not resume, not filter)
    elif output_path.exists() and not title_filter:
        output_path.unlink()
        print(f"[USC] Cleared previous output: {output_path}")

    # High connection pool to handle parallel requests
    limits = httpx.Limits(
        max_connections=MAX_CONCURRENT_REQUESTS + 10,
        max_keepalive_connections=MAX_CONCURRENT_REQUESTS,
        keepalive_expiry=30.0,
    )
    timeout = httpx.Timeout(connect=10.0, read=120.0, write=30.0, pool=30.0)
    async with httpx.AsyncClient(follow_redirects=True, limits=limits, timeout=timeout) as client:
        # Step 1: List all USC packages
        packages = await list_usc_packages(client)
        stats.titles_found = len(packages)

        if title_filter:
            packages = [p for p in packages if re.search(rf"-title{title_filter}\b", p["packageId"])]
            if not packages:
                print(f"[USC] No package found for title {title_filter}")
                return stats

        if max_titles:
            packages = packages[:max_titles]

        if dry_run:
            print(f"\n[USC] DRY RUN - {len(packages)} titles would be downloaded:")
            for pkg in packages:
                print(f"  {pkg['packageId']}: {pkg['title']} ({pkg.get('pages', '?')} pages)")
            return stats

        # Step 2: Download each title's sections
        for i, pkg in enumerate(packages, 1):
            pid = pkg["packageId"]
            title_name = pkg.get("title", "")

            # Skip titles already downloaded (resume mode)
            title_match = re.search(r"-title(\d+)", pid)
            pkg_title_num = int(title_match.group(1)) if title_match else 0
            if resume and pkg_title_num in completed_titles:
                print(f"\n[USC] [{i}/{len(packages)}] SKIP (already done): {pid}: {title_name}")
                continue

            print(f"\n[USC] [{i}/{len(packages)}] Downloading {pid}: {title_name}")

            try:
                sections = await download_title_granules(client, pid, title_name, stats)
            except Exception as e:
                print(f"  [FATAL] Title {pkg_title_num} crashed: {type(e).__name__}: {str(e)[:200]}")
                stats.errors.append(f"Title {pkg_title_num} crashed: {type(e).__name__}: {str(e)[:200]}")
                # Wait a bit then continue with next title
                await asyncio.sleep(30)
                continue

            stats.titles_downloaded += 1
            stats.sections_parsed += len(sections)

            # Write to JSONL
            written = write_sections_jsonl(sections, output_path)
            print(f"  -> {written} sections written to {output_path}")

            # Save raw ZIP for archival
            try:
                zip_url = pkg.get("download", {}).get("zipLink", "")
                if zip_url:
                    match = re.search(r"title(\d+)", pid)
                    title_num = match.group(1) if match else "unknown"
                    year_match = re.search(r"(\d{4})", pid)
                    year = year_match.group(1) if year_match else "unknown"

                    zip_path = RAW_DIR / year / f"title-{title_num}.zip"
                    zip_path.parent.mkdir(parents=True, exist_ok=True)

                    zip_bytes = await _api_get_bytes(client, zip_url)
                    zip_path.write_bytes(zip_bytes)
                    print(f"  -> Raw ZIP saved: {zip_path} ({len(zip_bytes) / 1024 / 1024:.1f} MB)")
            except Exception as e:
                stats.errors.append(f"ZIP download for {pid}: {e}")
                print(f"  -> ZIP download failed: {e}")

    # Summary
    print(f"\n{'='*60}")
    print(f"[USC] Download Complete ({stats.elapsed()})")
    print(f"  Titles found:       {stats.titles_found}")
    print(f"  Titles downloaded:  {stats.titles_downloaded}")
    print(f"  Granules found:     {stats.granules_found}")
    print(f"  Granules downloaded:{stats.granules_downloaded}")
    print(f"  Sections parsed:    {stats.sections_parsed}")
    print(f"  Output:             {output_path}")
    if stats.errors:
        print(f"  Errors:             {len(stats.errors)}")
        for err in stats.errors[:10]:
            print(f"    - {err}")
        if len(stats.errors) > 10:
            print(f"    ... and {len(stats.errors) - 10} more")
    print(f"{'='*60}")

    return stats


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="Download US Code from GovInfo API")
    parser.add_argument("--title", type=int, help="Download only this title number (e.g., 42)")
    parser.add_argument("--dry-run", action="store_true", help="List packages only, don't download")
    parser.add_argument("--max-titles", type=int, help="Limit number of titles (for testing)")
    parser.add_argument("--resume", action="store_true", help="Skip titles already in JSONL (resume after crash)")
    parser.add_argument(
        "--api-key", type=str, default=None, help="GovInfo API key (or set GOVINFO_API_KEY env)"
    )
    args = parser.parse_args()

    if args.api_key:
        global GOVINFO_API_KEY
        GOVINFO_API_KEY = args.api_key

    stats = asyncio.run(
        download_usc(
            title_filter=args.title,
            dry_run=args.dry_run,
            max_titles=args.max_titles,
            resume=args.resume,
        )
    )

    sys.exit(1 if stats.errors else 0)


if __name__ == "__main__":
    main()
