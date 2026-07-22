#!/usr/bin/env python3
"""
Download the Electronic Code of Federal Regulations (eCFR).

Source: https://www.ecfr.gov/api/ (no authentication, no rate limits published)
Data:   All 50 CFR titles, full XML, updated daily
Auth:   NONE required

Pipeline:
  1. List all 50 CFR titles with metadata (last-amended dates)
  2. Download full XML for each title (current as of latest issue date)
  4. Store raw XML locally at: data/us_corpus/ecfr/raw/{date}/title-{n}.xml
  5. Parse sections from XML into structured records
  6. Output parsed sections as JSONL: data/parsed/ecfr_sections.jsonl

Usage:
    # Download all 50 titles
    python scripts/federal/download_ecfr.py

    # Download specific title
    python scripts/federal/download_ecfr.py --title 17


    # Dry run
    python scripts/federal/download_ecfr.py --dry-run
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from xml.etree import ElementTree as ET

import httpx

# Add project root to path
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

ECFR_API_BASE = "https://www.ecfr.gov/api"
REQUEST_DELAY = 0.3  # 300ms between requests (polite, no published limit)

# Local storage
DATA_DIR = _PROJECT_ROOT / "data" / "us_corpus"
RAW_DIR = DATA_DIR / "ecfr" / "raw"
PARSED_DIR = DATA_DIR / "parsed"


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class ECFRTitle:
    """Metadata about a CFR title from the eCFR versioner."""

    number: int
    name: str
    latest_issue_date: str  # YYYY-MM-DD
    latest_amended_on: str  # YYYY-MM-DD
    up_to_date_as_of: str  # YYYY-MM-DD
    reserved: bool = False


@dataclass
class ECFRSection:
    """A single section of the Code of Federal Regulations."""

    act_id: str  # e.g., "CFR_T17_P240_S10b-5"
    cfr_title_number: int  # e.g., 17
    cfr_title_name: str  # e.g., "Commodity and Securities Exchanges"
    chapter: str  # e.g., "II"
    chapter_name: str  # e.g., "Securities and Exchange Commission"
    part: str  # e.g., "240"
    part_name: str  # e.g., "General Rules and Regulations, Securities Exchange Act of 1934"
    subpart: str  # e.g., ""
    subpart_name: str  # e.g., ""
    section_number: str  # e.g., "240.10b-5"
    section_title: str  # e.g., "Employment of manipulative and deceptive devices"
    text: str  # Full section text
    xml_fragment: str  # Original XML fragment for this section
    issue_date: str  # YYYY-MM-DD
    authority: str  # e.g., "15 U.S.C. 78a et seq."
    source: str  # e.g., "45 FR 68375, Oct. 15, 1980"
    source_url: str  # URL to ecfr.gov
    category: str = "ecfr"
    document_type: str = "regulation"
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
    titles_skipped: int = 0
    sections_parsed: int = 0
    bytes_downloaded: int = 0
    errors: list[str] = field(default_factory=list)
    start_time: float = field(default_factory=time.time)

    def elapsed(self) -> str:
        return f"{time.time() - self.start_time:.1f}s"


# ---------------------------------------------------------------------------
# eCFR API client
# ---------------------------------------------------------------------------


async def list_ecfr_titles(client: httpx.AsyncClient) -> list[ECFRTitle]:
    """List all 50 CFR titles with metadata."""
    print("[eCFR] Fetching title metadata...")
    await asyncio.sleep(REQUEST_DELAY)
    resp = await client.get(f"{ECFR_API_BASE}/versioner/v1/titles.json", timeout=30)
    resp.raise_for_status()
    data = resp.json()

    titles = []
    for t in data.get("titles", []):
        titles.append(
            ECFRTitle(
                number=t["number"],
                name=t["name"],
                latest_issue_date=t.get("latest_issue_date", ""),
                latest_amended_on=t.get("latest_amended_on", ""),
                up_to_date_as_of=t.get("up_to_date_as_of", ""),
                reserved=t.get("reserved", False),
            )
        )

    print(f"[eCFR] Found {len(titles)} titles")
    return titles


async def download_title_xml(
    client: httpx.AsyncClient, title: ECFRTitle
) -> bytes | None:
    """Download the full XML for a CFR title as of its latest issue date."""
    if title.reserved:
        return None

    issue_date = title.latest_issue_date
    if not issue_date:
        return None

    url = f"{ECFR_API_BASE}/versioner/v1/full/{issue_date}/title-{title.number}.xml"
    print(f"  Downloading Title {title.number} ({issue_date})... ", end="", flush=True)

    await asyncio.sleep(REQUEST_DELAY)
    try:
        resp = await client.get(url, timeout=300)  # Large titles can be slow
        resp.raise_for_status()
        xml_bytes = resp.content
        size_mb = len(xml_bytes) / 1024 / 1024
        print(f"{size_mb:.1f} MB")
        return xml_bytes
    except httpx.HTTPStatusError as e:
        print(f"HTTP {e.response.status_code}")
        return None
    except httpx.TimeoutException:
        print("TIMEOUT")
        return None


# ---------------------------------------------------------------------------
# XML parser
# ---------------------------------------------------------------------------


def parse_ecfr_xml(xml_bytes: bytes, title: ECFRTitle) -> list[ECFRSection]:
    """Parse eCFR XML into section-level records.

    eCFR XML structure:
      <ECFR>
        <DIV1 N="I" TYPE="CHAPTER">           -- Chapter
          <DIV4 N="A" TYPE="SUBCHAP">          -- Subchapter (optional)
            <DIV5 N="240" TYPE="PART">          -- Part
              <AUTH><HED>Authority:</HED>...</AUTH>
              <SOURCE><HED>Source:</HED>...</SOURCE>
              <DIV8 N="240.10b-5" TYPE="SECTION"> -- Section (our target)
                <HEAD>...</HEAD>
                <P>paragraph text...</P>
              </DIV8>
            </DIV5>
          </DIV4>
        </DIV1>
      </ECFR>
    """
    sections: list[ECFRSection] = []

    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as e:
        print(f"  XML parse error for Title {title.number}: {e}")
        return sections

    # Build parent_map ONCE for the entire title (O(n) instead of O(n) per section)
    print(f"  Building parent map for parse...")
    t0 = time.time()
    parent_map: dict = {child: parent for parent in root.iter() for child in parent}
    part_info_cache: dict = {}  # PART element -> (authority, source) tuple
    print(f"  Parent map built ({len(parent_map)} nodes) in {time.time()-t0:.1f}s")

    # Walk the tree to find all DIV8 (section) elements
    for div8 in root.iter():
        if div8.tag != "DIV8" and div8.tag != "SECTNO":
            # We only care about DIV8 (sections) and sometimes SECTION elements
            if not (div8.tag.startswith("DIV") and div8.get("TYPE", "").upper() == "SECTION"):
                continue

        section_num = div8.get("N", "")
        if not section_num:
            continue

        # Extract heading
        head_el = div8.find("HEAD")
        section_title = ""
        if head_el is not None:
            section_title = _get_element_text(head_el).strip()
            # Remove the section number prefix from heading (e.g., "240.10b-5 Employment of...")
            section_title = re.sub(r"^[\d\.\-a-zA-Z]+\s+", "", section_title, count=1)

        # Extract all text content from the section
        text_parts = []
        for el in div8.iter():
            if el.tag in ("HEAD", "SECTNO", "SUBJECT"):
                continue  # Skip header elements
            if el.text:
                text_parts.append(el.text.strip())
            if el.tail:
                text_parts.append(el.tail.strip())

        text = "\n".join(p for p in text_parts if p)
        if not text or len(text) < 10:
            continue

        # Get XML fragment for storage
        xml_fragment = ET.tostring(div8, encoding="unicode", method="xml")

        # Walk up to find parent context (chapter, part, subpart)
        # Uses pre-built parent_map and part_info_cache (O(depth) instead of O(n))
        chapter, chapter_name = _walk_ancestor(div8, parent_map, "CHAPTER")
        part, part_name = _walk_ancestor(div8, parent_map, "PART")
        subpart, subpart_name = _walk_ancestor(div8, parent_map, "SUBPART")

        # Authority and source from nearest parent PART (cached)
        authority, source_ref = _walk_part_info(div8, parent_map, part_info_cache)

        # Build act_id
        part_clean = part.replace(".", "_").replace("-", "_")
        sec_clean = section_num.replace(".", "_").replace("-", "_")
        act_id = f"CFR_T{title.number}_P{part_clean}_S{sec_clean}"

        # Source URL
        source_url = (
            f"https://www.ecfr.gov/current/title-{title.number}"
            f"/part-{part}/section-{section_num}"
        )

        section = ECFRSection(
            act_id=act_id,
            cfr_title_number=title.number,
            cfr_title_name=title.name,
            chapter=chapter,
            chapter_name=chapter_name,
            part=part,
            part_name=part_name,
            subpart=subpart,
            subpart_name=subpart_name,
            section_number=section_num,
            section_title=section_title,
            text=text,
            xml_fragment=xml_fragment[:5000],  # Cap fragment size for storage
            issue_date=title.latest_issue_date,
            authority=authority,
            source=source_ref,
            source_url=source_url,
        )
        sections.append(section)

    return sections


def _get_element_text(el: ET.Element) -> str:
    """Get all text content from an element and its children."""
    parts = []
    if el.text:
        parts.append(el.text)
    for child in el:
        parts.append(_get_element_text(child))
        if child.tail:
            parts.append(child.tail)
    return " ".join(parts)


def _walk_ancestor(
    target: ET.Element, parent_map: dict, div_type: str
) -> tuple[str, str]:
    """Walk up the tree using a pre-built parent_map (O(depth), not O(n))."""
    current = target
    target_type = div_type.upper()
    depth = 0
    while current is not None and depth < 15:
        el_type = current.get("TYPE", "").upper()
        if el_type == target_type:
            number = current.get("N", "")
            head = current.find("HEAD")
            name = _get_element_text(head).strip() if head is not None else ""
            # Clean name: remove "CHAPTER I--" or "PART 240--" prefix
            name = re.sub(r"^(CHAPTER|PART|SUBPART|SUBCHAPTER)\s+[\dIVXLCDMA-Z]+\s*[-\u2014]+\s*", "", name, flags=re.IGNORECASE)
            return number, name
        current = parent_map.get(current)
        depth += 1

    return "", ""


def _walk_part_info(
    target: ET.Element, parent_map: dict, cache: dict
) -> tuple[str, str]:
    """Find AUTH and SOURCE from nearest parent PART, with per-PART cache."""
    current = target
    depth = 0
    while current is not None and depth < 15:
        if current.get("TYPE", "").upper() == "PART":
            if current in cache:
                return cache[current]
            auth_el = current.find(".//AUTH")
            source_el = current.find(".//SOURCE")
            auth = _get_element_text(auth_el).strip() if auth_el is not None else ""
            source = _get_element_text(source_el).strip() if source_el is not None else ""
            auth = re.sub(r"^Authority:\s*", "", auth, flags=re.IGNORECASE)
            source = re.sub(r"^Source:\s*", "", source, flags=re.IGNORECASE)
            cache[current] = (auth, source)
            return auth, source
        current = parent_map.get(current)
        depth += 1

    return "", ""


# ---------------------------------------------------------------------------
# JSONL output
# ---------------------------------------------------------------------------


def write_sections_jsonl(sections: list[ECFRSection], output_path: Path) -> int:
    """Write parsed sections to JSONL file."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with open(output_path, "a", encoding="utf-8") as f:
        for section in sections:
            record = section.to_dict()
            # Don't include XML fragment in JSONL
            record.pop("xml_fragment", None)
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
            count += 1
    return count


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------


async def download_ecfr(
    title_filter: int | None = None,
    dry_run: bool = False,
    max_titles: int | None = None,
    resume: bool = False,
    parse_only: bool = False,
) -> DownloadStats:
    """Download and parse the eCFR.

    Args:
        title_filter: Download only this title number (e.g., 17)
        dry_run: List titles only, don't download
        max_titles: Limit number of titles (for testing)
        resume: Skip titles already in JSONL
        parse_only: Only parse existing XML files, don't download
    """
    stats = DownloadStats()

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    PARSED_DIR.mkdir(parents=True, exist_ok=True)

    output_path = PARSED_DIR / "ecfr_sections.jsonl"

    # Load completed titles for resume
    completed_titles: set[int] = set()
    if resume and output_path.exists():
        with open(output_path, "r") as f:
            for line in f:
                try:
                    rec = json.loads(line)
                    tn = rec.get("cfr_title_number")
                    if tn:
                        completed_titles.add(int(tn))
                except (json.JSONDecodeError, ValueError, TypeError):
                    continue
        print(f"[eCFR] Resume mode: {len(completed_titles)} titles already in JSONL: {sorted(completed_titles)}")

    # Clear previous output if starting fresh (not resume, not filter)
    elif output_path.exists() and not title_filter:
        output_path.unlink()
        print(f"[eCFR] Cleared previous output: {output_path}")

    async with httpx.AsyncClient(follow_redirects=True) as client:
        # Step 1: List titles
        titles = await list_ecfr_titles(client)
        stats.titles_found = len(titles)

        if title_filter:
            titles = [t for t in titles if t.number == title_filter]

        # Skip reserved titles
        titles = [t for t in titles if not t.reserved]

        if max_titles:
            titles = titles[:max_titles]

        if dry_run:
            print(f"\n[eCFR] DRY RUN - {len(titles)} titles would be downloaded:")
            for t in titles:
                status = "RESERVED" if t.reserved else f"issued {t.latest_issue_date}"
                print(f"  Title {t.number:2d}: {t.name} ({status})")
            return stats

        # Step 2: Download and parse each title
        for i, title in enumerate(titles, 1):
            print(f"\n[eCFR] [{i}/{len(titles)}] Title {title.number}: {title.name}")

            # Resume: skip titles already in JSONL
            if resume and title.number in completed_titles:
                print(f"  SKIP (already in JSONL)")
                continue

            # Check if XML already on disk (from previous run)
            existing_xml = None
            date_dir = RAW_DIR / title.latest_issue_date
            xml_path = date_dir / f"title-{title.number}.xml"
            if xml_path.exists() and xml_path.stat().st_size > 1024:
                existing_xml = xml_path
                print(f"  Using existing XML: {xml_path} ({xml_path.stat().st_size / 1024 / 1024:.1f} MB)")

            if existing_xml:
                xml_bytes = existing_xml.read_bytes()
                stats.titles_downloaded += 1
                stats.bytes_downloaded += len(xml_bytes)
            elif parse_only:
                print(f"  SKIP (parse-only mode, no XML on disk)")
                stats.titles_skipped += 1
                continue
            else:
                xml_bytes = await download_title_xml(client, title)
                if xml_bytes is None:
                    stats.titles_skipped += 1
                    continue

                stats.titles_downloaded += 1
                stats.bytes_downloaded += len(xml_bytes)

                # Save raw XML locally
                date_dir.mkdir(parents=True, exist_ok=True)
                xml_path.write_bytes(xml_bytes)
                print(f"  -> Raw XML saved: {xml_path}")

            # Parse sections
            print(f"  -> Parsing XML...")
            t0 = time.time()
            sections = parse_ecfr_xml(xml_bytes, title)
            stats.sections_parsed += len(sections)
            print(f"  -> Parsed {len(sections)} sections in {time.time()-t0:.1f}s")

            # Write to JSONL
            written = write_sections_jsonl(sections, output_path)
            print(f"  -> {written} sections written to JSONL")

    # Summary
    print(f"\n{'='*60}")
    print(f"[eCFR] Download Complete ({stats.elapsed()})")
    print(f"  Titles found:       {stats.titles_found}")
    print(f"  Titles downloaded:  {stats.titles_downloaded}")
    print(f"  Titles skipped:     {stats.titles_skipped}")
    print(f"  Sections parsed:    {stats.sections_parsed}")
    print(f"  Bytes downloaded:   {stats.bytes_downloaded / 1024 / 1024:.1f} MB")
    print(f"  Output:             {output_path}")
    if stats.errors:
        print(f"  Errors:             {len(stats.errors)}")
        for err in stats.errors[:10]:
            print(f"    - {err}")
    print(f"{'='*60}")

    return stats


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="Download eCFR from ecfr.gov API")
    parser.add_argument("--title", type=int, help="Download only this title (e.g., 17)")
    parser.add_argument("--dry-run", action="store_true", help="List titles only")
    parser.add_argument("--max-titles", type=int, help="Limit titles (for testing)")
    parser.add_argument("--resume", action="store_true", help="Skip titles already in JSONL")
    parser.add_argument("--parse-only", action="store_true", help="Only parse existing XML files, skip downloads")
    args = parser.parse_args()

    stats = asyncio.run(
        download_ecfr(
            title_filter=args.title,
            dry_run=args.dry_run,
            max_titles=args.max_titles,
            resume=args.resume,
            parse_only=args.parse_only,
        )
    )

    sys.exit(1 if stats.errors else 0)


if __name__ == "__main__":
    main()
