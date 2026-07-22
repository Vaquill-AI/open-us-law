#!/usr/bin/env python3
"""
P1: parse eCFR ``authority`` field into structured citations and build a
reverse "implementing regulations" index on USC.

Every CFR Part begins with an Authority line listing the enabling statutes,
executive orders, Federal Register pages, and prior CFR references that
give the agency legal authority to promulgate the regulations. In our
eCFR JSONL, this is the free-text ``authority`` field (99.8% populated).

Example raw authority:
    "44 U.S.C. 1506; sec. 6, E.O. 10530, 19 FR 2709; 3 CFR,
     1954-1958 Comp., p. 189; 1 U.S.C. 112, 113."

This script parses that into a structured list and attaches it back to
each eCFR record as ``statutory_authority``:
    [
      {"type":"usc","title":44,"section":"1506","display":"44 U.S.C. 1506"},
      {"type":"executive_order","number":"10530","display":"E.O. 10530"},
      {"type":"federal_register","cite":"19 FR 2709","display":"19 FR 2709"},
      {"type":"cfr","title":3,"ref":"1954-1958 Comp., p. 189",
       "display":"3 CFR, 1954-1958 Comp., p. 189"},
      {"type":"usc","title":1,"section":"112","display":"1 U.S.C. 112"},
      {"type":"usc","title":1,"section":"113","display":"1 U.S.C. 113"}
    ]

Then a second pass builds a reverse map: every USC section that is cited
by any CFR part as statutory authority collects the list of CFR parts
implementing it. That list is attached to the USC record as
``implementing_regulations``:
    [
      {"cfr_title":17, "part":"240", "part_name":"GENERAL RULES AND
       REGULATIONS, SECURITIES EXCHANGE ACT OF 1934",
       "display":"17 CFR Part 240"}
    ]

This enables the UI's "Regulations Implementing This Statute" tab and
the reverse "Statutory Authority" chips on CFR section pages.

Usage:
    python scripts/us_corpus/parse_authority_citations.py
    python scripts/us_corpus/parse_authority_citations.py --dry-run
    python scripts/us_corpus/parse_authority_citations.py --source usc
    python scripts/us_corpus/parse_authority_citations.py --source ecfr
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from collections import defaultdict
from pathlib import Path

sys.stdout.reconfigure(line_buffering=True)  # type: ignore[attr-defined]

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
PARSED_DIR = _PROJECT_ROOT / "data" / "us_corpus" / "parsed"


# ---------------------------------------------------------------------------
# Citation parser
# ---------------------------------------------------------------------------

# Normalize non-breaking spaces, various dashes, and multi-whitespace.
_RE_NB = re.compile(r"[\u00a0\u2013\u2014]")
_RE_WS = re.compile(r"\s+")


def _normalize(text: str) -> str:
    text = _RE_WS.sub(" ", _RE_NB.sub("-", text)).strip()
    # Replace "et seq." with a comma so section-list regex captures the
    # follow-on sections ("15 U.S.C. 78a et seq., 78c, 78d" -> enumerate all).
    text = re.sub(r"\s+et\s+seq\.?\s*,?", ",", text, flags=re.IGNORECASE)
    return text


# USC citations: "44 U.S.C. 1506", "44 U.S.C. 1506, 1507, 1508",
# "44 U.S.C. 1506-1509". We greedily capture the section list then split on
# comma/range to enumerate individual sections.
_RE_USC_CITE = re.compile(
    r"\b(\d{1,2})\s+U\.?\s*S\.?\s*C\.?(?:\s*\u00a7+)?\s*"
    r"(\d[\w\.\-]*(?:\s*(?:,|-|and)\s*\d[\w\.\-]*)*)",
)

# Executive Orders: "E.O. 10530", "Executive Order 13771".
_RE_EO_CITE = re.compile(
    r"\b(?:E\.?\s*O\.?|Executive\s+Order)\s+(\d{3,6})",
    re.IGNORECASE,
)

# Federal Register page cites: "19 FR 2709", "85 FR 12345-12350".
_RE_FR_CITE = re.compile(r"\b(\d+)\s+FR\s+(\d{2,6}(?:-\d{2,6})?)")

# Public Law: "Pub. L. 104-317", "Pub. L. 118-42".
_RE_PL_CITE = re.compile(
    r"\bPub\.\s*L\.\s*(\d+)[\-\u2013](\d+)(?:,\s*\u00a7\s*(\d[\w\.\-]*))?",
)

# Statutes at Large: "93 Stat. 1284".
_RE_STAT_CITE = re.compile(r"\b(\d+)\s+Stat\.\s+(\d+)")

# CFR cites: "3 CFR, 1954-1958 Comp., p. 189", "17 CFR 240.10b-5",
# "17 CFR Part 240". Captures an optional trailing "ref" block up to the
# next semicolon or end-of-string.
_RE_CFR_CITE = re.compile(
    r"\b(\d{1,2})\s+CFR\b(?:[,\s]+([^;]{1,100}?))?(?=\s*(?:;|$|\.$))",
)


def _enumerate_sections(raw: str) -> list[str]:
    """Turn "1506, 1507, 1508" or "1506-1509" into ['1506','1507','1508'(,'1509')]."""
    raw = raw.strip().rstrip(",;. ")
    parts = re.split(r"\s*(?:,|\band\b)\s*", raw)
    sections: list[str] = []
    for p in parts:
        p = p.strip()
        if not p:
            continue
        # Range like "1506-1509" -> enumerate if pure numeric
        range_m = re.match(r"^(\d+)\-(\d+)$", p)
        if range_m:
            start, end = int(range_m.group(1)), int(range_m.group(2))
            if end - start <= 50:  # safety
                sections.extend(str(n) for n in range(start, end + 1))
            else:
                sections.append(p)  # too large, keep as-is
        else:
            sections.append(p)
    return sections


def parse_authority(auth: str) -> list[dict]:
    """Return a list of structured citation dicts extracted from a raw
    authority string. Order of matches follows order of appearance to keep
    the UI display stable."""
    if not auth:
        return []

    auth = _normalize(auth)
    # Strip trailing explanatory notes like "interpret or apply..." that
    # sometimes appear after the cite list.
    auth = re.split(
        r"(?:interpret|apply|and)\s+[a-z\s]{2,30}\s+(?:sec\.|\u00a7)",
        auth,
        maxsplit=1,
    )[0]

    results: list[dict] = []
    seen_display: set[str] = set()

    def _add(d: dict) -> None:
        disp = d.get("display", "")
        if disp and disp not in seen_display:
            seen_display.add(disp)
            results.append(d)

    # USC
    for m in _RE_USC_CITE.finditer(auth):
        t_num = int(m.group(1))
        sections_raw = m.group(2)
        for sec in _enumerate_sections(sections_raw):
            _add({
                "type": "usc",
                "title": t_num,
                "section": sec,
                "display": f"{t_num} U.S.C. {sec}",
            })

    # Executive Orders
    for m in _RE_EO_CITE.finditer(auth):
        num = m.group(1)
        _add({"type": "executive_order", "number": num, "display": f"E.O. {num}"})

    # Federal Register
    for m in _RE_FR_CITE.finditer(auth):
        vol, page = m.group(1), m.group(2)
        _add({"type": "federal_register", "volume": vol, "page": page,
              "display": f"{vol} FR {page}"})

    # Public Laws
    for m in _RE_PL_CITE.finditer(auth):
        congress, law = m.group(1), m.group(2)
        section = m.group(3)
        disp = f"Pub. L. {congress}-{law}"
        entry = {"type": "public_law", "congress": int(congress),
                 "law": int(law), "display": disp}
        if section:
            entry["section"] = section
        _add(entry)

    # Statutes at Large
    for m in _RE_STAT_CITE.finditer(auth):
        vol, page = m.group(1), m.group(2)
        _add({"type": "statutes_at_large", "volume": vol, "page": page,
              "display": f"{vol} Stat. {page}"})

    # CFR references (typically "3 CFR, 1954-1958 Comp., p. 189" -- EO compilations)
    for m in _RE_CFR_CITE.finditer(auth):
        t_num = int(m.group(1))
        ref = (m.group(2) or "").strip()
        disp = f"{t_num} CFR" + (f", {ref}" if ref else "")
        _add({"type": "cfr", "title": t_num, "ref": ref, "display": disp})

    return results


# ---------------------------------------------------------------------------
# eCFR pass: attach statutory_authority to each record
# ---------------------------------------------------------------------------


def process_ecfr(path: Path, dry_run: bool) -> dict[str, list[dict]]:
    """Parse authority on every eCFR record. Return a reverse-index dict
    mapping usc_act_id -> [{cfr_title, part, part_name, display}].
    """
    print(f"\n[eCFR] Parsing authority on {path.name}")
    t0 = time.time()
    total = 0
    non_empty = 0
    usc_cites_total = 0
    sample_shown = 0

    # Reverse index: (usc_title, usc_section) -> set of (cfr_title, part, part_name)
    reverse: dict[tuple[int, str], set[tuple[int, str, str]]] = defaultdict(set)

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

            auth_raw = rec.get("authority", "") or ""
            cites = parse_authority(auth_raw)
            rec["statutory_authority"] = cites
            if auth_raw:
                non_empty += 1

            # Accumulate the reverse map
            cfr_title = rec.get("cfr_title_number")
            part = str(rec.get("part", "") or "")
            part_name = rec.get("part_name", "") or ""
            if cfr_title and part:
                for c in cites:
                    if c["type"] == "usc":
                        key = (int(c["title"]), str(c["section"]))
                        reverse[key].add((int(cfr_title), part, part_name))
                        usc_cites_total += 1

            if sample_shown < 3 and cites:
                print(f"  sample {rec['act_id']}:")
                print(f"    authority_raw: {auth_raw[:150]!r}")
                print(f"    parsed: {cites[:5]}")
                sample_shown += 1

            if fout is not None:
                fout.write(json.dumps(rec, ensure_ascii=False) + "\n")

            if total % 25000 == 0:
                rate = total / (time.time() - t0)
                print(f"  ... {total:,} records ({rate:.0f}/s)")

    finally:
        fin.close()
        if fout is not None:
            fout.close()

    if dry_run:
        print(f"  DRY RUN: {total:,} records. authority non-empty: {non_empty:,}. "
              f"usc cites extracted: {usc_cites_total:,}. reverse keys: {len(reverse):,}")
    else:
        tmp.replace(path)
        elapsed = time.time() - t0
        print(f"  -> {total:,} in {elapsed:.1f}s. authority non-empty: {non_empty:,}. "
              f"usc cites extracted: {usc_cites_total:,}. reverse keys: {len(reverse):,}")

    # Collapse reverse index sets -> sorted list of dicts
    out: dict[str, list[dict]] = {}
    for (usc_title, usc_sec), cfrs in reverse.items():
        usc_key = f"{usc_title}:{usc_sec}"
        out[usc_key] = [
            {
                "cfr_title": t,
                "part": p,
                "part_name": name,
                "display": f"{t} CFR Part {p}",
            }
            for (t, p, name) in sorted(cfrs)
        ]
    return out


# ---------------------------------------------------------------------------
# USC pass: attach implementing_regulations using the reverse map
# ---------------------------------------------------------------------------


def process_usc(path: Path, reverse_map: dict[str, list[dict]], dry_run: bool) -> None:
    print(f"\n[USC] Attaching implementing_regulations to {path.name}")
    t0 = time.time()
    total = 0
    matched = 0

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

            title_num = rec.get("title_number")
            section = str(rec.get("section_number") or "")
            key = f"{title_num}:{section}"
            regs = reverse_map.get(key, [])
            rec["implementing_regulations"] = regs
            rec["implementing_regulations_count"] = len(regs)
            if regs:
                matched += 1

            if fout is not None:
                fout.write(json.dumps(rec, ensure_ascii=False) + "\n")

            if total % 10000 == 0:
                rate = total / (time.time() - t0)
                print(f"  ... {total:,} records ({rate:.0f}/s) matched={matched:,}")

    finally:
        fin.close()
        if fout is not None:
            fout.close()

    if dry_run:
        print(f"  DRY RUN: {total:,} records. matched={matched:,} (have implementing regs)")
    else:
        tmp.replace(path)
        elapsed = time.time() - t0
        print(f"  -> {total:,} in {elapsed:.1f}s. matched={matched:,} "
              f"({100*matched/total if total else 0:.1f}%)")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else "")
    parser.add_argument("--source", choices=["all", "usc", "ecfr"], default="all")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    print("=== Parse authority citations ===")
    print(f"Source:   {args.source}")
    print(f"Dry run:  {args.dry_run}")

    ecfr_path = PARSED_DIR / "ecfr_sections.jsonl"
    usc_path = PARSED_DIR / "usc_sections.jsonl"

    reverse_map: dict[str, list[dict]] = {}
    if args.source in ("all", "ecfr"):
        if not ecfr_path.exists():
            print(f"[eCFR] SKIP: {ecfr_path} not found")
        else:
            reverse_map = process_ecfr(ecfr_path, args.dry_run)

    if args.source in ("all", "usc"):
        if not usc_path.exists():
            print(f"[USC] SKIP: {usc_path} not found")
        elif not reverse_map and args.source == "usc":
            # Running USC alone needs the reverse map; rebuild it
            print("  (Rebuilding reverse map from eCFR first...)")
            reverse_map = process_ecfr(ecfr_path, dry_run=True)  # re-parse, no write
        if usc_path.exists():
            process_usc(usc_path, reverse_map, args.dry_run)

    print("\n=== Done ===")


if __name__ == "__main__":
    main()
