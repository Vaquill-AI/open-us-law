"""Parse the ILGA `Section Sequence.txt` manifest into section records.

Illinois publishes the entire ILCS as a static file tree under
https://www.ilga.gov/ftp/ILCS/ , and ships an authoritative ordered manifest of
every document at .../aReadMe/Section Sequence.txt . That manifest is why IL is
tractable: it lists every section up front, so there is no discovery crawl and
no chance of missing a chapter (the gap we currently have -- 19 of 67 chapters).

Manifest line format (fixed-width prefix `ccccaaaaat` then the section id):
    cccc   4-digit Chapter, left-zero-padded.        0005 -> Chapter 5
    aaaaa  5 digits for the Act. First 4 are the Act number; a non-zero 5th
           digit is a decimal suffix.  00050 -> Act 5 ; 00055 -> Act 5.5 ;
           01200 -> Act 120
    t      document type: A=Chapter header, F=Act title, H=Article heading,
           K=Section (the ones with text we ingest)
    rest   the section (or article) number, verbatim (e.g. 15-108.1, 0.01, 1a)

The reproduced act_id must match the existing IL scraper exactly so reconcile
can clean the chapters we already hold:
    STATE_IL_C{chapter}_A{act}_S{section}      citation  {chapter} ILCS {act}/{section}
    verified: manifest 004000050K15-108.1  ->  STATE_IL_C40_A5_S15-108.1  (40 ILCS 5/15-108.1)
"""
from __future__ import annotations

from dataclasses import dataclass

# Document types in the manifest. We ingest sections (K); the others carry the
# hierarchy/headings and are used only to reconstruct structure if needed.
TYPE_SECTION = "K"
TYPE_ACT_TITLE = "F"
TYPE_CHAPTER_HEADER = "A"
TYPE_ARTICLE = "H"
_TYPES = frozenset({"A", "F", "H", "K"})


@dataclass(frozen=True)
class ManifestEntry:
    raw: str          # original manifest line
    chapter: str      # "40"      (leading zeros stripped)
    act: str          # "5" or "5.5" or "120"
    doc_type: str     # A / F / H / K
    section: str      # "15-108.1"  ("" for A/F rows that have no section id)

    @property
    def is_section(self) -> bool:
        return self.doc_type == TYPE_SECTION

    def act_id(self) -> str:
        return f"STATE_IL_C{self.chapter}_A{self.act}_S{self.section}"

    def citation(self) -> str:
        return f"{self.chapter} ILCS {self.act}/{self.section}"


def _act_from_code(aaaaa: str) -> str:
    """5-digit act code -> display act number. 00050->5, 00055->5.5, 01200->120."""
    first4 = int(aaaaa[:4])
    fifth = aaaaa[4]
    return str(first4) if fifth == "0" else f"{first4}.{fifth}"


def parse_line(line: str) -> ManifestEntry | None:
    """One manifest line -> ManifestEntry, or None for headers/blank/garbage."""
    s = line.rstrip("\r\n")
    # A valid entry is at least cccc(4) + aaaaa(5) + t(1) = 10 chars, the 10th a
    # known type letter. Everything else (the file's prose header, blanks) is skipped.
    if len(s) < 10 or not s[:9].isdigit() or s[9] not in _TYPES:
        return None
    chapter = str(int(s[0:4]))
    act = _act_from_code(s[4:9])
    doc_type = s[9]
    section = s[10:].strip()
    return ManifestEntry(raw=s, chapter=chapter, act=act, doc_type=doc_type, section=section)


def parse_manifest(text: str) -> list[ManifestEntry]:
    out: list[ManifestEntry] = []
    for line in text.splitlines():
        e = parse_line(line)
        if e is not None:
            out.append(e)
    return out


def sections(entries: list[ManifestEntry]) -> list[ManifestEntry]:
    return [e for e in entries if e.is_section]
