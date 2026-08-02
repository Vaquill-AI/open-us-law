#!/usr/bin/env python3
"""
Fast USC downloader: fetches one ZIP per title via GovInfo package API.

The ZIP contains:
- All section HTMLs (thousands per title for big ones like Title 42)
- Full MODS XML with rich metadata for every granule
- METS manifest

This avoids the slow per-section API approach and its pagination 500 errors.

One ZIP download per title = orders of magnitude faster than granule-by-granule.

Usage:
    # Download all 53 titles
    python scripts/us_corpus/download_usc_zips.py

    # Resume (skip titles already downloaded)
    python scripts/us_corpus/download_usc_zips.py --resume

    # Download specific title
    python scripts/us_corpus/download_usc_zips.py --title 42

    # Parallel downloads (4 concurrent)
    python scripts/us_corpus/download_usc_zips.py --concurrency 4
"""

from __future__ import annotations

import argparse
import asyncio
import os
import re
import sys
import time
from pathlib import Path

import httpx

sys.stdout.reconfigure(line_buffering=True)  # type: ignore[attr-defined]

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

GOVINFO_API_BASE = "https://api.govinfo.gov"
GOVINFO_API_KEY = os.environ.get("GOVINFO_API_KEY", "DEMO_KEY")

DATA_DIR = _PROJECT_ROOT / "data" / "us_corpus" / "usc" / "raw"

TARGET_YEARS = [2024, 2023]
ALL_TITLES = list(range(1, 55))


async def download_zip(
    client: httpx.AsyncClient,
    title_num: int,
    semaphore: asyncio.Semaphore,
    skip_existing: bool = True,
) -> tuple[int, str, int]:
    """Download one USC title ZIP. Returns (title_num, status, size_bytes)."""
    async with semaphore:
        # Find which year has this title
        pid = None
        pages = "?"
        title_name = "?"
        for year in TARGET_YEARS:
            test_pid = f"USCODE-{year}-title{title_num}"
            try:
                summary_resp = await client.get(
                    f"{GOVINFO_API_BASE}/packages/{test_pid}/summary",
                    params={"api_key": GOVINFO_API_KEY},
                    timeout=30,
                )
                if summary_resp.status_code == 404:
                    continue
                summary_resp.raise_for_status()
                data = summary_resp.json()
                pid = test_pid
                pages = data.get("pages", "?")
                title_name = data.get("title", "?")
                zip_url = data.get("download", {}).get("zipLink", "")
                year_found = year
                break
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    continue
                return (title_num, f"summary failed HTTP {e.response.status_code}", 0)
            except Exception as e:
                return (title_num, f"summary error: {type(e).__name__}", 0)
        else:
            return (title_num, "not found in any year", 0)

        if not pid or not zip_url:
            return (title_num, "no zipLink in summary", 0)

        # Check if already downloaded
        zip_path = DATA_DIR / str(year_found) / f"title-{title_num}.zip"
        if skip_existing and zip_path.exists() and zip_path.stat().st_size > 1024:
            return (title_num, f"SKIP ({zip_path.stat().st_size / 1024 / 1024:.1f} MB)", zip_path.stat().st_size)

        # Download ZIP - STREAMING to disk (no full-body buffer in RAM)
        try:
            t0 = time.time()
            zip_path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = zip_path.with_suffix(".zip.tmp")
            total_bytes = 0
            async with client.stream(
                "GET",
                zip_url,
                params={"api_key": GOVINFO_API_KEY},
                timeout=httpx.Timeout(connect=30, read=600, write=60, pool=30),
                follow_redirects=True,
            ) as resp:
                resp.raise_for_status()
                with open(tmp_path, "wb") as f:
                    async for chunk in resp.aiter_bytes(chunk_size=1024 * 1024):  # 1 MB chunks
                        f.write(chunk)
                        total_bytes += len(chunk)
            # Atomic rename once complete
            tmp_path.rename(zip_path)

            elapsed = time.time() - t0
            size_mb = total_bytes / 1024 / 1024
            return (title_num, f"{title_name} ({pages}p, {size_mb:.1f} MB, {elapsed:.1f}s)", total_bytes)

        except Exception as e:
            return (title_num, f"download error: {type(e).__name__}: {str(e)[:80]}", 0)


async def main_async(
    title_filter: int | None = None,
    resume: bool = False,
    concurrency: int = 4,
):
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    titles = [title_filter] if title_filter else ALL_TITLES

    print(f"[USC-ZIP] Downloading {len(titles)} USC titles (concurrency={concurrency}, resume={resume})")

    sem = asyncio.Semaphore(concurrency)
    limits = httpx.Limits(max_connections=concurrency + 2, max_keepalive_connections=concurrency)
    timeout = httpx.Timeout(connect=10, read=600, write=60, pool=30)

    async with httpx.AsyncClient(follow_redirects=True, limits=limits, timeout=timeout) as client:
        t0 = time.time()
        tasks = [download_zip(client, n, sem, skip_existing=resume) for n in titles]

        total_bytes = 0
        done = 0
        failed = 0
        skipped = 0

        for coro in asyncio.as_completed(tasks):
            title_num, status, size = await coro
            done += 1
            if "SKIP" in status:
                skipped += 1
                print(f"  [{done}/{len(titles)}] Title {title_num:2d}: {status}")
            elif "error" in status.lower() or "failed" in status.lower() or "not found" in status:
                failed += 1
                print(f"  [{done}/{len(titles)}] Title {title_num:2d}: FAIL - {status}")
            else:
                total_bytes += size
                print(f"  [{done}/{len(titles)}] Title {title_num:2d}: {status}")

        elapsed = time.time() - t0
        print(f"\n{'='*60}")
        print(f"[USC-ZIP] Done in {elapsed:.1f}s ({elapsed/60:.1f} min)")
        print(f"  Downloaded:  {done - skipped - failed} titles ({total_bytes / 1024 / 1024:.1f} MB)")
        print(f"  Skipped:     {skipped}")
        print(f"  Failed:      {failed}")
        print(f"{'='*60}")


def main():
    parser = argparse.ArgumentParser(description="Fast USC ZIP downloader")
    parser.add_argument("--title", type=int, help="Download only this title")
    parser.add_argument("--resume", action="store_true", help="Skip already-downloaded ZIPs")
    parser.add_argument("--concurrency", type=int, default=4, help="Parallel downloads (default 4)")
    args = parser.parse_args()

    asyncio.run(main_async(
        title_filter=args.title,
        resume=args.resume,
        concurrency=args.concurrency,
    ))


if __name__ == "__main__":
    main()
