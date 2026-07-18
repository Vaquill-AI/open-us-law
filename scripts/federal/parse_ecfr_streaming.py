#!/usr/bin/env python3
"""
Stream-parse eCFR XML files that are too large for in-memory parsing.

Uses xml.etree.ElementTree.iterparse to process sections one at a time,
writing to JSONL as they're encountered and clearing memory.

Essential for Title 40 (Environment, 149MB XML) which OOMs with the
default parser.

Usage:
    # Parse a single title's XML
    python scripts/us_corpus/parse_ecfr_streaming.py \\
        --xml data/us_corpus/ecfr/raw/2026-04-13/title-40.xml \\
        --title-num 40 --title-name "Protection of Environment" \\
        --issue-date 2026-04-13

    # Parse all already-downloaded XML files that aren't in JSONL yet
    python scripts/us_corpus/parse_ecfr_streaming.py --auto
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from pathlib import Path
from xml.etree import ElementTree as ET

sys.stdout.reconfigure(line_buffering=True)  # type: ignore[attr-defined]
sys.stderr.reconfigure(line_buffering=True)  # type: ignore[attr-defined]

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = _PROJECT_ROOT / "data" / "us_corpus"
RAW_DIR = DATA_DIR / "ecfr" / "raw"
JSONL_PATH = DATA_DIR / "parsed" / "ecfr_sections.jsonl"


def _text_from_elem(el: ET.Element) -> str:
    """Extract all text from element, skipping HEAD/SECTNO/SUBJECT."""
    parts = []
    if el.text:
        parts.append(el.text)
    for child in el:
        if child.tag in ("HEAD", "SECTNO", "SUBJECT"):
            if child.tail:
                parts.append(child.tail)
            continue
        parts.append(_text_from_elem(child))
        if child.tail:
            parts.append(child.tail)
    return " ".join(p.strip() for p in parts if p and p.strip())


def _clean_name(raw: str) -> str:
    return re.sub(
        r"^(CHAPTER|PART|SUBPART|SUBCHAPTER|SUBCHAP)\s+[\dIVXLCDMA-Z]+\s*[-\u2014]+\s*",
        "",
        raw,
        flags=re.IGNORECASE,
    ).strip()


def parse_streaming(
    xml_path: Path,
    title_num: int,
    title_name: str,
    issue_date: str,
    jsonl_path: Path = JSONL_PATH,
) -> int:
    """Stream-parse a single eCFR title XML into JSONL.

    Returns number of sections written.
    """
    # Context tracked as we descend the tree
    ancestor_stack: list[tuple[str, str, str]] = []  # (type, number, name)
    current_authority = ""
    current_source = ""

    sections_written = 0
    jsonl_path.parent.mkdir(parents=True, exist_ok=True)

    start_time = time.time()
    print(f"[streaming-parse] Title {title_num} from {xml_path}", flush=True)

    with open(jsonl_path, "a", encoding="utf-8") as out_f:
        try:
            context = ET.iterparse(str(xml_path), events=("start", "end"))
            event, root = next(context)  # first event is 'start' of root

            for event, elem in context:
                tag = elem.tag
                el_type = elem.get("TYPE", "").upper()

                if event == "start":
                    # Track on entering DIV ancestors
                    if tag.startswith("DIV") and el_type in (
                        "CHAPTER", "SUBCHAP", "SUBCHAPTER", "PART", "SUBPART"
                    ):
                        ancestor_stack.append((el_type, elem.get("N", ""), ""))

                elif event == "end":
                    # A section!
                    if tag == "DIV8" or (
                        tag.startswith("DIV") and el_type == "SECTION"
                    ):
                        section_num = elem.get("N", "")
                        if not section_num:
                            elem.clear()
                            continue

                        # Section title
                        head_el = elem.find("HEAD")
                        section_title = ""
                        if head_el is not None:
                            section_title = _text_from_elem(head_el).strip()
                            section_title = re.sub(
                                r"^[\d\.\-a-zA-Z]+\s+", "", section_title, count=1
                            )

                        # Section text
                        text = _text_from_elem(elem)
                        if not text or len(text) < 10:
                            elem.clear()
                            continue

                        # Resolve ancestor context
                        chapter_n, chapter_name = "", ""
                        part_n, part_name = "", ""
                        subpart_n, subpart_name = "", ""
                        for a_type, a_num, a_name in ancestor_stack:
                            if a_type in ("CHAPTER", "SUBCHAP", "SUBCHAPTER") and not chapter_n:
                                chapter_n, chapter_name = a_num, a_name
                            elif a_type == "PART":
                                part_n, part_name = a_num, a_name
                            elif a_type == "SUBPART":
                                subpart_n, subpart_name = a_num, a_name

                        # Record
                        part_clean = part_n.replace(".", "_").replace("-", "_")
                        sec_clean = section_num.replace(".", "_").replace("-", "_")
                        act_id = f"CFR_T{title_num}_P{part_clean}_S{sec_clean}"
                        source_url = (
                            f"https://www.ecfr.gov/current/title-{title_num}"
                            f"/part-{part_n}/section-{section_num}"
                        )

                        record = {
                            "act_id": act_id,
                            "cfr_title_number": title_num,
                            "cfr_title_name": title_name,
                            "chapter": chapter_n,
                            "chapter_name": chapter_name,
                            "part": part_n,
                            "part_name": part_name,
                            "subpart": subpart_n,
                            "subpart_name": subpart_name,
                            "section_number": section_num,
                            "section_title": section_title,
                            "text": text,
                            "issue_date": issue_date,
                            "authority": current_authority,
                            "source": current_source,
                            "source_url": source_url,
                            "category": "ecfr",
                            "document_type": "regulation",
                            "jurisdiction": "US",
                            "state": "federal",
                            "act_status": "in_force",
                        }
                        out_f.write(json.dumps(record, ensure_ascii=False) + "\n")
                        sections_written += 1

                        if sections_written % 1000 == 0:
                            elapsed = time.time() - start_time
                            rate = sections_written / elapsed if elapsed > 0 else 0
                            print(
                                f"  Title {title_num}: {sections_written} sections parsed "
                                f"({rate:.1f}/sec)",
                                flush=True,
                            )

                        # Clear element to free memory (critical for large XMLs)
                        elem.clear()

                    elif tag == "AUTH":
                        auth_text = _text_from_elem(elem)
                        current_authority = re.sub(
                            r"^Authority:\s*", "", auth_text, flags=re.IGNORECASE
                        ).strip()
                        elem.clear()

                    elif tag == "SOURCE":
                        source_text = _text_from_elem(elem)
                        current_source = re.sub(
                            r"^Source:\s*", "", source_text, flags=re.IGNORECASE
                        ).strip()
                        elem.clear()

                    elif tag.startswith("DIV") and el_type in (
                        "CHAPTER", "SUBCHAP", "SUBCHAPTER", "PART", "SUBPART"
                    ):
                        # Resolve HEAD text
                        head_el = elem.find("HEAD")
                        head_text = (
                            _clean_name(_text_from_elem(head_el))
                            if head_el is not None
                            else ""
                        )
                        # Update entry in stack with resolved name
                        for i in range(len(ancestor_stack) - 1, -1, -1):
                            if (
                                ancestor_stack[i][0] == el_type
                                and ancestor_stack[i][1] == elem.get("N", "")
                                and ancestor_stack[i][2] == ""
                            ):
                                ancestor_stack[i] = (el_type, elem.get("N", ""), head_text)
                                break
                        # Pop if this is the top
                        while (
                            ancestor_stack
                            and ancestor_stack[-1][0] == el_type
                            and ancestor_stack[-1][1] == elem.get("N", "")
                        ):
                            ancestor_stack.pop()
                            break
                        # Clear PART-scoped authority/source
                        if el_type == "PART":
                            current_authority = ""
                            current_source = ""
                        elem.clear()

                    else:
                        # Periodically clear the root to free accumulated children
                        if sections_written > 0 and sections_written % 500 == 0:
                            root.clear()

        except ET.ParseError as e:
            print(f"  XML parse error for Title {title_num}: {e}", flush=True)

    elapsed = time.time() - start_time
    print(
        f"  Title {title_num}: {sections_written} sections written in {elapsed:.1f}s",
        flush=True,
    )
    return sections_written


def _find_xml_files() -> list[tuple[Path, int, str]]:
    """Find all downloaded eCFR XML files. Returns (path, title_num, issue_date)."""
    files = []
    for xml_path in RAW_DIR.glob("*/title-*.xml"):
        issue_date = xml_path.parent.name  # e.g., "2026-04-13"
        m = re.search(r"title-(\d+)\.xml$", xml_path.name)
        if m:
            files.append((xml_path, int(m.group(1)), issue_date))
    return sorted(files, key=lambda x: x[1])


def _sections_already_parsed() -> set[int]:
    """Return set of title_numbers already in the JSONL."""
    if not JSONL_PATH.exists():
        return set()
    found = set()
    with open(JSONL_PATH, "r", encoding="utf-8") as f:
        for line in f:
            try:
                d = json.loads(line)
                if d.get("category") == "ecfr":
                    tn = d.get("cfr_title_number")
                    if tn:
                        found.add(int(tn))
            except Exception:
                continue
    return found


# Title names (for CLI auto mode; derived from eCFR API titles.json)
TITLE_NAMES = {
    1: "General Provisions",
    2: "Federal Financial Assistance",
    3: "The President",
    4: "Accounts",
    5: "Administrative Personnel",
    6: "Domestic Security",
    7: "Agriculture",
    8: "Aliens and Nationality",
    9: "Animals and Animal Products",
    10: "Energy",
    11: "Federal Elections",
    12: "Banks and Banking",
    13: "Business Credit and Assistance",
    14: "Aeronautics and Space",
    15: "Commerce and Foreign Trade",
    16: "Commercial Practices",
    17: "Commodity and Securities Exchanges",
    18: "Conservation of Power and Water Resources",
    19: "Customs Duties",
    20: "Employees' Benefits",
    21: "Food and Drugs",
    22: "Foreign Relations",
    23: "Highways",
    24: "Housing and Urban Development",
    25: "Indians",
    26: "Internal Revenue",
    27: "Alcohol, Tobacco Products and Firearms",
    28: "Judicial Administration",
    29: "Labor",
    30: "Mineral Resources",
    31: "Money and Finance: Treasury",
    32: "National Defense",
    33: "Navigation and Navigable Waters",
    34: "Education",
    35: "Panama Canal [Reserved]",
    36: "Parks, Forests, and Public Property",
    37: "Patents, Trademarks, and Copyrights",
    38: "Pensions, Bonuses, and Veterans' Relief",
    39: "Postal Service",
    40: "Protection of Environment",
    41: "Public Contracts and Property Management",
    42: "Public Health",
    43: "Public Lands: Interior",
    44: "Emergency Management and Assistance",
    45: "Public Welfare",
    46: "Shipping",
    47: "Telecommunication",
    48: "Federal Acquisition Regulations System",
    49: "Transportation",
    50: "Wildlife and Fisheries",
}


def main():
    parser = argparse.ArgumentParser(description="Stream-parse eCFR XML files")
    parser.add_argument("--xml", type=str, help="Path to a single XML file")
    parser.add_argument("--title-num", type=int, help="Title number")
    parser.add_argument("--title-name", type=str, help="Title name")
    parser.add_argument("--issue-date", type=str, help="Issue date (YYYY-MM-DD)")
    parser.add_argument(
        "--auto",
        action="store_true",
        help="Find all downloaded XMLs and parse those not in JSONL yet",
    )
    parser.add_argument(
        "--output", type=str, default=None,
        help="Output JSONL path. Default: append to data/us_corpus/parsed/ecfr_sections.jsonl. "
             "Use a temp path for incremental refresh to avoid polluting the main JSONL.",
    )
    args = parser.parse_args()

    if args.auto:
        files = _find_xml_files()
        already_parsed = _sections_already_parsed()
        print(
            f"[auto] Found {len(files)} XML files. Already parsed: {sorted(already_parsed)}",
            flush=True,
        )
        total = 0
        for xml_path, title_num, issue_date in files:
            if title_num in already_parsed:
                print(f"  Skipping Title {title_num} (already parsed)", flush=True)
                continue
            title_name = TITLE_NAMES.get(title_num, f"Title {title_num}")
            count = parse_streaming(xml_path, title_num, title_name, issue_date)
            total += count
        print(f"\n[auto] Total new sections: {total}")
    else:
        if not args.xml or not args.title_num:
            print("Usage: --xml PATH --title-num N --title-name NAME --issue-date DATE")
            print("   or: --auto")
            sys.exit(1)
        xml_path = Path(args.xml)
        title_name = args.title_name or TITLE_NAMES.get(args.title_num, f"Title {args.title_num}")
        issue_date = args.issue_date or "2026-04-15"
        out_path = Path(args.output) if args.output else JSONL_PATH
        count = parse_streaming(xml_path, args.title_num, title_name, issue_date, jsonl_path=out_path)
        print(f"Parsed {count} sections to {out_path}")


if __name__ == "__main__":
    main()
