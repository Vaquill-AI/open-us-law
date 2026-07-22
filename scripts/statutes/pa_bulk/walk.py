"""Pennsylvania Consolidated Statutes section model + node_id / act_id path.

act_id scheme (reproduces the existing PA corpus structure so a cutover is
reconcilable): ``STATE_PA_T{title}_C{chapter}_S{section}`` where

  - ``title``   is the numeric consolidated title (e.g. 18, 42, 75, 23),
  - ``chapter`` is DERIVED from the section number by the same heuristic the
    original Findlaw scraper used (leading digits of the section minus the
    trailing two, floor one digit) -- the whole PA corpus already keys on this
    derived chapter, so we reproduce it rather than the real chapter to keep the
    node structure identical, and
  - ``section`` is the canonical DOT-form section number as the official code
    prints it (e.g. ``2502``, ``3132.1``, ``9799.11``).

The one difference from the legacy Findlaw act_ids is the section separator: the
Findlaw slug encoded the decimal as a hyphen (``3132-1``) whereas the official
code prints a dot (``3132.1``). The bulk therefore emits canonical dotted
citations (``20 Pa.C.S. § 3132.1``) and a state-scoped reconcile replaces the
old hyphen-form points -- see ingest_pa_bulk.py / verify_act_ids.py.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

COUNTRY = "us"
STATE = "pa"
CORPUS = "statutes"

_LEAD_DIGITS_RE = re.compile(r"^(\d+)")


def derive_chapter(section_number: str) -> str:
    """Chapter grouping derived from a section number.

    Mirrors the original scraper's ``_derive_chapter_from_section``: take the
    leading digits, drop the trailing two (the within-chapter section index),
    with a one-digit floor; sections whose leading run is <= 2 digits use the
    whole run. Falls back to "0" when there is no leading digit.

    Examples: 2502 -> 25, 1543 -> 15, 101 -> 1, 9799.11 -> 97, 5 -> 5.
    """
    m = _LEAD_DIGITS_RE.match(section_number)
    if not m:
        return "0"
    digits = m.group(1)
    if len(digits) <= 2:
        return digits
    return digits[:-2]


@dataclass(frozen=True)
class PASection:
    title_number: str  # numeric title, e.g. "18"
    title_name: str
    section_number: str  # canonical dot form, e.g. "2502", "3132.1"
    section_title: str
    status: str | None = None

    @property
    def chapter(self) -> str:
        return derive_chapter(self.section_number)

    def citation(self) -> str:
        return f"{self.title_number} Pa.C.S. § {self.section_number}"

    def node_id(self) -> str:
        return (
            f"{COUNTRY}/{STATE}/{CORPUS}"
            f"/title={self.title_number}"
            f"/chapter={self.chapter}"
            f"/section={self.section_number}"
        )
