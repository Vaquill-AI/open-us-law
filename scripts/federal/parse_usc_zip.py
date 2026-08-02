#!/usr/bin/env python3
"""
Parse USC ZIP files into section JSONL with rich metadata.

Each USC title ZIP (downloaded from GovInfo API) contains:
- html/USCODE-{year}-title{n}-chap{c}-sec{s}.htm  (section HTML text)
- mods.xml  (rich METS/MODS metadata for every granule)
- dip.xml   (METS manifest)

This is the fast path: one ZIP download per title, extract everything locally.
No per-section API calls needed.

Usage:
    # Parse all downloaded ZIPs
    python scripts/federal/parse_usc_zip.py

    # Parse a specific title
    python scripts/federal/parse_usc_zip.py --title 42

    # Use a custom input/output
    python scripts/federal/parse_usc_zip.py --input data/us_corpus/usc/raw --output data/us_corpus/parsed/usc_sections.jsonl
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
import zipfile
from dataclasses import asdict, dataclass, field
from pathlib import Path
from xml.etree import ElementTree as ET

sys.stdout.reconfigure(line_buffering=True)  # type: ignore[attr-defined]

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
# Reuse the HTML extractor from the sibling download_usc.py
sys.path.insert(0, str(Path(__file__).resolve().parent))
from download_usc import USCSection, html_to_text  # noqa: E402

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DEFAULT_INPUT_DIR = _PROJECT_ROOT / "data" / "us_corpus" / "usc" / "raw"
DEFAULT_OUTPUT_PATH = _PROJECT_ROOT / "data" / "us_corpus" / "parsed" / "usc_sections.jsonl"

# MODS XML namespace
MODS_NS = {"mods": "http://www.loc.gov/mods/v3"}


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------


@dataclass
class ParseStats:
    titles_parsed: int = 0
    sections_parsed: int = 0
    sections_skipped_empty: int = 0
    errors: list[str] = field(default_factory=list)
    start_time: float = field(default_factory=time.time)

    def elapsed(self) -> str:
        return f"{time.time() - self.start_time:.1f}s"


# ---------------------------------------------------------------------------
# MODS XML metadata parser
# ---------------------------------------------------------------------------


def parse_mods_metadata(mods_xml: bytes) -> dict[str, dict]:
    """Parse mods.xml and build a dict mapping granule ID -> metadata.

    Returns:
        {
            "USCODE-2024-title1-chap1-sec1": {
                "title": "Words denoting number, gender...",
                "part_name": "Sec. 1",
                "parent_id": "id-USCODE-2024-title1-chap1",
                "html_url": "https://...",
                "pdf_url": "https://...",
                "details_url": "https://...",
                "former_id": "1USC1",
                ...
            },
            ...
        }
    """
    metadata: dict[str, dict] = {}

    try:
        root = ET.fromstring(mods_xml)
    except ET.ParseError as e:
        print(f"  MODS parse error: {e}")
        return metadata

    # Find all relatedItem elements with granule IDs
    for item in root.iter("{http://www.loc.gov/mods/v3}relatedItem"):
        rid = item.get("ID", "")
        if not rid.startswith("id-USCODE"):
            continue

        granule_id = rid[len("id-"):]  # Strip "id-" prefix

        entry: dict = {}

        # Title info
        title_info = item.find("mods:titleInfo", MODS_NS)
        if title_info is not None:
            t = title_info.find("mods:title", MODS_NS)
            if t is not None and t.text:
                entry["section_title"] = t.text.strip()
            pn = title_info.find("mods:partName", MODS_NS)
            if pn is not None and pn.text:
                entry["part_name"] = pn.text.strip()

        # Parent ID (for building hierarchy)
        for ident in item.findall("mods:identifier", MODS_NS):
            itype = ident.get("type", "")
            if itype == "Parent Id" and ident.text:
                entry["parent_id"] = ident.text.strip()
            elif itype == "uri" and ident.text:
                entry["details_url"] = ident.text.strip()
            elif itype == "former granule identifier" and ident.text:
                entry["former_id"] = ident.text.strip()

        # Related URLs (HTML, PDF)
        for related in item.findall("mods:relatedItem", MODS_NS):
            href = related.get("{http://www.w3.org/1999/xlink}href", "")
            if ".htm" in href:
                entry["html_url"] = href
            elif ".pdf" in href:
                entry["pdf_url"] = href

        # Origin info (date)
        origin = item.find("mods:originInfo", MODS_NS)
        if origin is not None:
            date_el = origin.find("mods:dateIssued", MODS_NS)
            if date_el is not None and date_el.text:
                entry["date_issued"] = date_el.text.strip()

        metadata[granule_id] = entry

    return metadata


# ---------------------------------------------------------------------------
# Granule ID parser
# ---------------------------------------------------------------------------


def parse_granule_id_details(granule_id: str) -> dict:
    """Extract structural info from a granule ID.

    Examples:
      USCODE-2024-title1-chap1-sec1
      USCODE-2024-title42-chap21-subchapI-sec1983
      USCODE-2024-title26-subtitleA-chap1-subchapA-partI-subpartA-sec1
    """
    result = {
        "year": None,
        "title_number": None,
        "subtitle": "",
        "chapter": "",
        "subchapter": "",
        "part": "",
        "subpart": "",
        "section_number": "",
    }

    # Year and title
    m = re.match(r"USCODE-(\d{4})-title(\d+)", granule_id)
    if m:
        result["year"] = int(m.group(1))
        result["title_number"] = int(m.group(2))

    # Subtitle
    m = re.search(r"-subtitle([A-Z])", granule_id)
    if m:
        result["subtitle"] = m.group(1)

    # Chapter
    m = re.search(r"-chap(\d+[A-Za-z]*)", granule_id)
    if m:
        result["chapter"] = m.group(1)

    # Subchapter
    m = re.search(r"-subchap([IVXLCDM]+|\d+[A-Za-z]*)", granule_id)
    if m:
        result["subchapter"] = m.group(1)

    # Part
    m = re.search(r"-part([IVXLCDM]+|\d+[A-Za-z]*)", granule_id)
    if m:
        result["part"] = m.group(1)

    # Subpart
    m = re.search(r"-subpart([IVXLCDM]+|\d+[A-Za-z]*|[A-Z])", granule_id)
    if m:
        result["subpart"] = m.group(1)

    # Section (everything after -sec)
    m = re.search(r"-sec([^\-]+)$", granule_id)
    if m:
        result["section_number"] = m.group(1)

    return result


def find_ancestor_title(metadata_by_id: dict, granule_id: str, ancestor_type: str) -> tuple[str, str]:
    """Walk up the hierarchy using parent_id refs to find ancestor name.

    ancestor_type: "chap", "subchap", "part", "subpart", "subtitle"
    """
    current_id = granule_id
    for _ in range(10):  # Max depth
        parent_id = metadata_by_id.get(current_id, {}).get("parent_id", "")
        if not parent_id:
            break
        parent_clean = parent_id.replace("id-", "")
        if f"-{ancestor_type}" in parent_clean and not any(
            f"-{deeper}" in parent_clean.split(f"-{ancestor_type}", 1)[1]
            for deeper in ["chap", "subchap", "part", "subpart", "sec"]
            if deeper != ancestor_type
        ):
            # Found it - get the name
            parent_meta = metadata_by_id.get(parent_clean, {})
            name = parent_meta.get("section_title", "")
            # Extract identifier (e.g., "chap1" -> "1")
            m = re.search(rf"-{ancestor_type}([^\-]+?)(?:-|$)", parent_clean)
            number = m.group(1) if m else ""
            return number, name
        current_id = parent_clean

    return "", ""


# ---------------------------------------------------------------------------
# ZIP parser
# ---------------------------------------------------------------------------


def parse_usc_zip(zip_path: Path, stats: ParseStats) -> list[USCSection]:
    """Parse a single USC title ZIP into section records."""
    sections: list[USCSection] = []

    # Extract title number and year from filename: title-42.zip in dir 2024/
    year_str = zip_path.parent.name
    year = int(year_str) if year_str.isdigit() else 2024

    title_match = re.match(r"title-(\d+)\.zip", zip_path.name)
    if not title_match:
        stats.errors.append(f"Bad ZIP filename: {zip_path.name}")
        return sections
    title_number = int(title_match.group(1))

    package_id = f"USCODE-{year}-title{title_number}"
    package_prefix = f"{package_id}/"

    try:
        with zipfile.ZipFile(zip_path) as z:
            # Step 1: Parse mods.xml for metadata
            mods_path = f"{package_prefix}mods.xml"
            metadata_by_id: dict = {}
            title_name = ""
            try:
                mods_bytes = z.read(mods_path)
                metadata_by_id = parse_mods_metadata(mods_bytes)

                # Find title name from the title-level entry
                title_entry = metadata_by_id.get(package_id, {})
                title_name = title_entry.get("section_title", f"TITLE {title_number}")
                # Or try the titleInfo at the top level of MODS
                if not title_name or title_name.startswith("TITLE"):
                    try:
                        root = ET.fromstring(mods_bytes)
                        for ti in root.iter("{http://www.loc.gov/mods/v3}titleInfo"):
                            t = ti.find("mods:title", MODS_NS)
                            if t is not None and t.text and "title" in t.text.lower():
                                title_name = t.text.strip()
                                break
                    except Exception:
                        pass
            except KeyError:
                print(f"  [WARN] {zip_path.name}: no mods.xml")

            # Step 2: Find all section HTML files
            section_files = [
                name for name in z.namelist()
                if re.search(r"/html/USCODE-\d+-title\d+.*-sec[^/]+\.htm$", name)
            ]

            for html_path in section_files:
                # Extract granule ID from filename
                # USCODE-2024-title1/html/USCODE-2024-title1-chap1-sec1.htm
                gid_match = re.search(r"(USCODE-\d+-title\d+[-\w]*-sec[^/]+)\.htm$", html_path)
                if not gid_match:
                    continue
                granule_id = gid_match.group(1)

                # Read HTML content
                try:
                    html_content = z.read(html_path).decode("utf-8", errors="replace")
                except Exception as e:
                    stats.errors.append(f"Read HTML {html_path}: {e}")
                    continue

                text = html_to_text(html_content)
                if len(text.strip()) < 20:
                    stats.sections_skipped_empty += 1
                    continue

                # Parse granule ID for structural info
                parts = parse_granule_id_details(granule_id)
                section_number = parts["section_number"]
                if not section_number:
                    continue

                # Get MODS metadata
                meta = metadata_by_id.get(granule_id, {})

                # Build source URLs
                details_url = meta.get("details_url", f"https://www.govinfo.gov/app/details/{package_id}/{granule_id}")
                html_url = meta.get("html_url", f"https://www.govinfo.gov/content/pkg/{package_id}/html/{granule_id}.htm")
                pdf_url = meta.get("pdf_url", f"https://www.govinfo.gov/content/pkg/{package_id}/pdf/{granule_id}.pdf")
                section_title = meta.get("section_title", "")

                # Build chapter name by looking up the parent
                chapter_name = ""
                chapter_num = parts["chapter"]
                if chapter_num:
                    # Try to find chapter-level metadata
                    chapter_gid = f"{package_id}-chap{chapter_num}"
                    chapter_meta = metadata_by_id.get(chapter_gid, {})
                    chapter_name = chapter_meta.get("section_title", "")

                subchapter_name = ""
                subchapter_num = parts["subchapter"]
                if subchapter_num and chapter_num:
                    sub_gid = f"{package_id}-chap{chapter_num}-subchap{subchapter_num}"
                    sub_meta = metadata_by_id.get(sub_gid, {})
                    subchapter_name = sub_meta.get("section_title", "")

                section = USCSection(
                    act_id=f"USC_T{title_number}_S{section_number}",
                    title_number=title_number,
                    title_name=title_name or f"TITLE {title_number}",
                    chapter=chapter_num,
                    chapter_name=chapter_name,
                    subchapter=subchapter_num,
                    subchapter_name=subchapter_name,
                    section_number=section_number,
                    section_title=section_title,
                    text=text,
                    html=html_content,
                    year=year,
                    granule_id=granule_id,
                    package_id=package_id,
                    source_url=details_url,
                    pdf_url=pdf_url,
                    last_modified="",  # Would need dateIssued from MODS
                )
                sections.append(section)
                stats.sections_parsed += 1

    except Exception as e:
        stats.errors.append(f"ZIP {zip_path.name}: {type(e).__name__}: {str(e)[:150]}")

    return sections


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def write_sections_jsonl(sections: list[USCSection], output_path: Path, append: bool = True):
    """Append parsed sections to JSONL file."""
    mode = "a" if append else "w"
    with open(output_path, mode, encoding="utf-8") as f:
        for section in sections:
            record = asdict(section)
            record.pop("html", None)  # Don't store full HTML in JSONL
            f.write(json.dumps(record, ensure_ascii=False) + "\n")


def main():
    parser = argparse.ArgumentParser(description="Parse USC ZIP files into JSONL")
    parser.add_argument("--input", type=str, default=str(DEFAULT_INPUT_DIR),
                        help="Directory containing USC ZIPs (organized by year)")
    parser.add_argument("--output", type=str, default=str(DEFAULT_OUTPUT_PATH),
                        help="Output JSONL path")
    parser.add_argument("--title", type=int, help="Parse only this title number")
    parser.add_argument("--clear", action="store_true", help="Clear output file before parsing")
    args = parser.parse_args()

    input_dir = Path(args.input)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if args.clear and output_path.exists():
        output_path.unlink()
        print(f"Cleared {output_path}")

    # Find all ZIP files
    zip_files = sorted(input_dir.rglob("title-*.zip"))

    if args.title:
        zip_files = [z for z in zip_files if z.name == f"title-{args.title}.zip"]

    if not zip_files:
        print(f"No ZIP files found in {input_dir}")
        sys.exit(1)

    print(f"Found {len(zip_files)} USC title ZIPs to parse")

    stats = ParseStats()

    for i, zip_path in enumerate(zip_files, 1):
        print(f"\n[{i}/{len(zip_files)}] Parsing {zip_path.name} ({zip_path.stat().st_size / 1024 / 1024:.1f} MB)...")
        sections = parse_usc_zip(zip_path, stats)
        stats.titles_parsed += 1
        write_sections_jsonl(sections, output_path, append=True)
        print(f"  -> {len(sections)} sections written ({stats.elapsed()} elapsed)")

    print(f"\n{'='*60}")
    print(f"Parse Complete ({stats.elapsed()})")
    print(f"  Titles parsed:       {stats.titles_parsed}")
    print(f"  Sections parsed:     {stats.sections_parsed}")
    print(f"  Skipped (empty):     {stats.sections_skipped_empty}")
    print(f"  Output:              {output_path}")
    if stats.errors:
        print(f"  Errors:              {len(stats.errors)}")
        for err in stats.errors[:10]:
            print(f"    - {err}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
