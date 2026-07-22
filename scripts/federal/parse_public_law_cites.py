#!/usr/bin/env python3
"""
P1: parse USC source_credit_raw into Bluebook-structured public law cites.

Raw source_credit looks like:
    "R.S. \u00a71979; Pub. L. 96-170, \u00a71, Dec. 29, 1979, 93 Stat. 1284;
     Pub. L. 104-317, title III, \u00a7309(c), Oct. 19, 1996, 110 Stat. 3853."

We split on ";" and parse each segment into a structured dict like:
    [
      {"type":"revised_statutes","display":"R.S. \u00a71979"},
      {"type":"public_law","law":"96-170","section":"1",
       "date":"1979-12-29","stat_at_large":"93 Stat. 1284",
       "display":"Pub. L. 96-170, \u00a71, Dec. 29, 1979, 93 Stat. 1284"},
      {"type":"public_law","law":"104-317","title":"III","section":"309(c)",
       "date":"1996-10-19","stat_at_large":"110 Stat. 3853",
       "display":"Pub. L. 104-317, title III, \u00a7309(c), Oct. 19, 1996, 110 Stat. 3853"}
    ]

This mirrors Westlaw's "Credits" field and Lexis's "History" segment. It's
what lawyers need to pin-cite an amendment for a brief.

Usage:
    python scripts/us_corpus/parse_public_law_cites.py
    python scripts/us_corpus/parse_public_law_cites.py --dry-run
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path

sys.stdout.reconfigure(line_buffering=True)  # type: ignore[attr-defined]

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
PARSED_DIR = _PROJECT_ROOT / "data" / "us_corpus" / "parsed"


# ---------------------------------------------------------------------------
# Cite parser
# ---------------------------------------------------------------------------

_MONTHS = {
    "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04", "May": "05", "Jun": "06",
    "Jul": "07", "Aug": "08", "Sep": "09", "Sept": "09", "Oct": "10",
    "Nov": "11", "Dec": "12",
}

_RE_PL = re.compile(r"Pub\.\s*L\.\s*(\d+)[\-\u2013](\d+)", re.IGNORECASE)
_RE_DATE = re.compile(
    r"\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z\.]*\s+(\d{1,2}),\s+(\d{4})\b"
)
_RE_STAT = re.compile(r"\b(\d+)\s+Stat\.\s+(\d+)\b")
_RE_TITLE_ROMAN = re.compile(
    r"\btitle\s+([IVXLCDM]+)\b",
    re.IGNORECASE,
)
_RE_SECTION = re.compile(r"\u00a7\s*(\d[\w\.\-]*(?:\([\w\.\-]+\))*)")
_RE_RS = re.compile(r"\bR\.?\s*S\.?\s*\u00a7\s*(\d+)", re.IGNORECASE)
# Pre-Public-Law-era acts (prior to 1957): cited by date + chapter + Stat
# page, e.g., "July 30, 1947, ch. 388, 61 Stat. 633". Sometimes prefaced with
# "Act" / "Act of". Detect a segment that has both a long date and "ch." +
# Stat cite but no Pub. L. token.
_RE_ACT_OF = re.compile(
    r"(?:Act\s+(?:of\s+)?)?"  # optional leading "Act" or "Act of"
    r"((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z\.]*\s+\d{1,2},\s+\d{4})"
    r"[^;]*?ch\.\s*\d+",
    re.IGNORECASE,
)
_RE_CH = re.compile(r"\bch\.\s*(\d+\w*)")


def _parse_date(text: str) -> str:
    """Return first ISO-8601 date found, or empty string."""
    m = _RE_DATE.search(text)
    if not m:
        return ""
    month = _MONTHS.get(m.group(1)[:3].capitalize(), "")
    if not month:
        return ""
    day = m.group(2).zfill(2)
    year = m.group(3)
    return f"{year}-{month}-{day}"


def parse_source_credit(raw: str) -> list[dict]:
    """Parse a source_credit string into a list of structured cite dicts."""
    if not raw:
        return []

    # Split top-level on semicolons. Each segment is one enacting/amending act.
    segments = [s.strip().rstrip(".") for s in raw.split(";") if s.strip()]
    out: list[dict] = []

    for seg in segments:
        # Revised Statutes: "R.S. §1979"
        rs_m = _RE_RS.search(seg)
        if rs_m and not _RE_PL.search(seg):
            out.append({
                "type": "revised_statutes",
                "section": rs_m.group(1),
                "display": f"R.S. \u00a7{rs_m.group(1)}",
            })
            continue

        # Public Law: "Pub. L. 96-170, §1, Dec. 29, 1979, 93 Stat. 1284"
        pl_m = _RE_PL.search(seg)
        if pl_m:
            congress, law = int(pl_m.group(1)), int(pl_m.group(2))
            law_display = f"{congress}-{law}"
            entry: dict = {
                "type": "public_law",
                "congress": congress,
                "law": law,
                "display_id": f"Pub. L. {law_display}",
            }
            iso = _parse_date(seg)
            if iso:
                entry["date"] = iso
            stat_m = _RE_STAT.search(seg)
            if stat_m:
                entry["stat_volume"] = int(stat_m.group(1))
                entry["stat_page"] = int(stat_m.group(2))
                entry["stat_at_large"] = f"{stat_m.group(1)} Stat. {stat_m.group(2)}"
            title_m = _RE_TITLE_ROMAN.search(seg)
            if title_m:
                entry["title_roman"] = title_m.group(1).upper()
            section_m = _RE_SECTION.search(seg)
            if section_m:
                entry["section"] = section_m.group(1)
            entry["display"] = seg  # preserve original Bluebook-ready string
            out.append(entry)
            continue

        # Pre-Public-Law acts: "Act July 30, 1947, ch. 388, 61 Stat. 633"
        ao_m = _RE_ACT_OF.search(seg)
        if ao_m:
            entry = {
                "type": "act",
                "date_text": ao_m.group(1),
                "display": seg,
            }
            iso = _parse_date(seg)
            if iso:
                entry["date"] = iso
            stat_m = _RE_STAT.search(seg)
            if stat_m:
                entry["stat_volume"] = int(stat_m.group(1))
                entry["stat_page"] = int(stat_m.group(2))
                entry["stat_at_large"] = f"{stat_m.group(1)} Stat. {stat_m.group(2)}"
            ch_m = _RE_CH.search(seg)
            if ch_m:
                entry["chapter"] = ch_m.group(1)
            out.append(entry)
            continue

        # Catch-all: record as unparsed
        out.append({"type": "other", "display": seg})

    return out


# ---------------------------------------------------------------------------
# File processing
# ---------------------------------------------------------------------------


def process(path: Path, dry_run: bool) -> None:
    print(f"\n[USC] Parsing source_credit on {path.name}")
    t0 = time.time()
    total = 0
    with_credit = 0
    sample_shown = 0

    tmp = path.with_suffix(path.suffix + ".tmp")
    fin = open(path, "r", encoding="utf-8")
    fout = None if dry_run else open(tmp, "w", encoding="utf-8")

    try:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            total += 1
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            raw = rec.get("source_credit_raw", "") or ""
            cites = parse_source_credit(raw)
            rec["public_law_cites"] = cites
            rec["public_law_cites_count"] = len(cites)
            if raw:
                with_credit += 1

            if sample_shown < 3 and cites:
                print(f"  sample {rec['act_id']}:")
                print(f"    raw: {raw[:120]!r}")
                for c in cites:
                    print(f"      {c}")
                sample_shown += 1

            if fout is not None:
                fout.write(json.dumps(rec, ensure_ascii=False) + "\n")

            if total % 10000 == 0:
                rate = total / (time.time() - t0)
                print(f"  ... {total:,} records ({rate:.0f}/s)")

    finally:
        fin.close()
        if fout is not None:
            fout.close()

    if dry_run:
        print(f"  DRY RUN: {total:,} records. with_credit: {with_credit:,}")
    else:
        tmp.replace(path)
        elapsed = time.time() - t0
        print(f"  -> {total:,} in {elapsed:.1f}s ({total/elapsed:.0f}/s). "
              f"with_credit: {with_credit:,} ({100*with_credit/total:.1f}%)")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else "")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    print("=== Parse public law cites ===")
    print(f"Dry run: {args.dry_run}")

    p = PARSED_DIR / "usc_sections.jsonl"
    if p.exists():
        process(p, args.dry_run)
    else:
        print(f"[USC] SKIP: {p} not found")

    print("\n=== Done ===")


if __name__ == "__main__":
    main()
