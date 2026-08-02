"""Regex-driven enrichment for state-statute chunks.

Mirrors the post-extraction passes that ``scripts/us_corpus/`` runs for eCFR
and USC (extract_critical_fields, extract_html_status_metadata, etc.) but
inlined for our scraper output. Bumps state payloads from the lean baseline
(40 fields, mostly-empty arrays) up to parity with the federal corpus
(48 fields, populated arrays).

Populates these previously-empty fields on every content chunk:

    subsection_letters       e.g. ['a', 'b', 'c']
    subsection_count         e.g. 3
    numbered_paragraph_count e.g. 2  (number of "(1) " / "(2) " markers)
    cross_references_usc     e.g. ['18:1030', '5:552a']
    cross_references_cfr     e.g. ['12:226.5']
    cross_references_count   total of usc + cfr + state self-refs
    amendment_years          e.g. [2018, 2022]
    last_amended_year        e.g. 2022
    public_laws_referenced   e.g. ['Pub. L. 117-103']
"""

from __future__ import annotations

import re
from typing import Any

# Subsection markers at start-of-line: "(a)", "(b)", "(1)", "(i)"
_LETTER_SUBSEC = re.compile(r"(?:^|\n)\s*\(([a-z]{1,2})\)\s", re.MULTILINE)
_NUM_SUBSEC = re.compile(r"(?:^|\n)\s*\((\d+)\)\s", re.MULTILINE)
_ROMAN_SUBSEC = re.compile(r"(?:^|\n)\s*\(([ivxlcdm]{1,5})\)\s", re.MULTILINE)

# Cross-references — USC and CFR inline
# Matches "18 U.S.C. § 1030", "18 USC 1030", "12 C.F.R. § 226.5", etc.
_USC_REF = re.compile(
    r"\b(\d{1,2})\s*U\.?\s*S\.?\s*C\.?\s*(?:§+|sec(?:tion)?\.?)\s*"
    r"(\d{1,5}[A-Za-z0-9.\-]*)",
    re.IGNORECASE,
)
_CFR_REF = re.compile(
    r"\b(\d{1,2})\s*C\.?\s*F\.?\s*R\.?\s*(?:§+|sec(?:tion)?\.?)\s*"
    r"(\d{1,4}\.[0-9A-Za-z\-]+)",
    re.IGNORECASE,
)

# Public Laws — "Pub. L. 117-103"
_PUB_L = re.compile(r"\bPub\.?\s*L\.?\s*(?:No\.?\s*)?(\d{2,3}-\d{1,4})", re.IGNORECASE)

# Amendments — common patterns across state codes:
#   "(Added 1979, No. 17, § 1.)"        Vermont
#   "Amended by 2018 Acts, ch. 401"     Maryland
#   "L 1892, c 57, §5"                   Hawaii
#   "(L. 1947, c. 25, § 1, eff. Jan. 1, 1948)"  generic
_AMEND_YEAR = re.compile(
    r"\b(?:added|amended|enacted|repealed|effective|L\.?)\s*[^0-9]{0,40}\b((?:1[7-9]|20)\d{2})\b",
    re.IGNORECASE,
)
# Standalone "(YEAR" near amendment context (broader catch)
_PAREN_YEAR = re.compile(r"\((?:[A-Za-z. ,]{1,40})((?:1[7-9]|20)\d{2})\b")

# Repealed / reserved markers.
#
# IMPORTANT: these check the HEAD of the section text only (first ~300
# chars after whitespace and any leading section-number prefix). Matching
# anywhere in the body produces a flood of false positives, every one of
# which silently hides an in-force statute from RAG. Empirical audit
# (2026-06-01): of 277 CA rows previously flagged 'repealed', only 1 was
# actually repealed. False positives included:
#   - Sunset clauses ("This section shall remain in effect only until
#     January 1, 2030, and as of that date is repealed.")
#   - Future-operative sections with a sunset on their successor.
#   - Code savings clauses ("any of the acts repealed by this code").
#   - Corporations Code bylaw mechanics ("bylaws may be adopted, amended
#     or repealed by the board").
#   - APA references ("regulations may be adopted, amended or repealed").
#   - Hypothetical-review language ("as if scheduled to be repealed").
#   - Historical one-time repeal acts (the section itself is in force).
#   - Conditional repeals tied to events that haven't occurred.
#
# The fix below requires the marker AT THE START of the section text,
# which is where every actual repeal marker actually appears in the
# corpus (statutory drafting convention: a repealed section's text is
# replaced by "[Repealed]" or "Repealed by Stats. <year>, ch. <chap>...").
_REPEALED_HEAD = re.compile(
    r"^\s*(?:"
    r"\[Repealed[\]\s]"  # [Repealed] or [Repealed by ...]
    r"|\(Repealed\s+by\b"  # (Repealed by Stats. ...
    r"|Repealed\s*\.(?:\s|$)"  # Repealed.\n or Repealed. (text follows)
    r"|Repealed\s+by\b"  # Repealed by Stats. ...
    r")",
    re.IGNORECASE,
)
_RESERVED_HEAD = re.compile(
    r"^\s*(?:\[Reserved\]|Reserved\s*\.(?:\s|$))",
    re.IGNORECASE,
)
# Drop a leading section-number prefix so "1234.5. [Repealed]" or
# "1234.5 (Repealed by ...)" still match. The corpus stores section text
# with the number prefix in several formats.
_LEADING_NUMBER = re.compile(r"^[\d.\-:()§\s]+", re.IGNORECASE)


def _strip_to_head(full_text: str, limit: int = 300) -> str:
    """Return the first `limit` chars of `full_text` after stripping
    leading whitespace and any section-number prefix.

    Used by the repeal / reserved checks below so we evaluate only the
    "head" of the section -- where statutory drafting places repeal /
    reserved markers -- not arbitrary body prose.
    """
    if not full_text:
        return ""
    head = full_text.lstrip()[:limit]
    head = _LEADING_NUMBER.sub("", head, count=1).lstrip()
    return head[:limit]


def _section_is_repealed(full_text: str) -> bool:
    """True iff the section text opens with a definitive repeal marker."""
    return bool(_REPEALED_HEAD.match(_strip_to_head(full_text)))


def _section_is_reserved(full_text: str) -> bool:
    """True iff the section text opens with a definitive reserved marker."""
    return bool(_RESERVED_HEAD.match(_strip_to_head(full_text)))


def enrich_payload(payload: dict[str, Any], full_text: str) -> dict[str, Any]:
    """Mutate ``payload`` in-place with extracted fields. Returns it for chaining.

    ``full_text`` should be the *complete* section text (not a chunk slice)
    so cross-references aren't truncated mid-citation. Caller passes the
    pre-chunked combined text.
    """
    if not full_text:
        return payload

    # --- Subsection markers ---
    letters = sorted({m.group(1).lower() for m in _LETTER_SUBSEC.finditer(full_text)})
    nums = list({m.group(1) for m in _NUM_SUBSEC.finditer(full_text)})
    romans = list({m.group(1).lower() for m in _ROMAN_SUBSEC.finditer(full_text)})

    payload["subsection_letters"] = letters
    payload["subsection_count"] = len(letters)
    payload["numbered_paragraph_count"] = len(nums) + len(romans)

    # --- Cross-references ---
    usc_refs: set[str] = set()
    for m in _USC_REF.finditer(full_text):
        usc_refs.add(f"{m.group(1)}:{m.group(2)}")
    cfr_refs: set[str] = set()
    for m in _CFR_REF.finditer(full_text):
        cfr_refs.add(f"{m.group(1)}:{m.group(2)}")

    payload["cross_references_usc"] = sorted(usc_refs)
    payload["cross_references_cfr"] = sorted(cfr_refs)
    payload["cross_references_count"] = len(usc_refs) + len(cfr_refs)

    # --- Public Laws ---
    pls = sorted({f"Pub. L. {m.group(1)}" for m in _PUB_L.finditer(full_text)})
    payload["public_laws_referenced"] = pls
    payload["public_laws_count"] = len(pls)

    # --- Amendment years ---
    # The amendment-context regexes also match forward-looking dates in the
    # body ("repealed effective January 1, 2050", sunset clauses), so a raw
    # max() would report a future "last amended" year. Clamp to the edition
    # year (the snapshot the row was scraped from) so a section can never be
    # "amended" after the data was captured, and drop pre-1789 garble.
    years: set[int] = set()
    for m in _AMEND_YEAR.finditer(full_text):
        years.add(int(m.group(1)))
    if not years:
        for m in _PAREN_YEAR.finditer(full_text):
            years.add(int(m.group(1)))
    edition_year = payload.get("year") if isinstance(payload.get("year"), int) else None
    hi = edition_year if (edition_year and edition_year >= 1789) else 2100
    years_sorted = sorted(y for y in years if 1789 <= y <= hi)
    payload["amendment_years"] = years_sorted
    payload["amendments_count"] = len(years_sorted)
    payload["last_amended_year"] = years_sorted[-1] if years_sorted else None

    # --- Status hints ---
    # Only flip in_force -> repealed/reserved when the section text
    # OPENS with a definitive marker. Matching anywhere in the body
    # caused a confirmed ~99% false-positive rate on CA's BPC + CCP +
    # CIV sunset-clause sections. See _REPEALED_HEAD docstring above.
    if payload.get("act_status") in (None, "", "in_force"):
        if _section_is_repealed(full_text):
            payload["act_status"] = "repealed"
        elif _section_is_reserved(full_text):
            payload["act_status"] = "reserved"

    return payload


__all__ = ["enrich_payload"]
