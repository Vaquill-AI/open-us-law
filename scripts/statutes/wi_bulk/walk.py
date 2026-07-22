"""Wisconsin Statutes section model + act_id path construction.

Wisconsin's hierarchy in ``statutes_us`` is flat: ``chapter=N/section=S`` (there
is no title level, and subchapters are structural headings only, not part of the
citation). The section number itself carries the chapter as its integer prefix
(e.g. ``940.01`` -> chapter ``940``), so the chapter is derived deterministically
from the section number and always matches the chapter page the section was
crawled from.

The resulting act_id reproduces the existing scraper's scheme exactly:

    1.01     (chapter 1)    -> STATE_WI_C1_S1.01
    940.01   (chapter 940)  -> STATE_WI_C940_S940.01
    104.015  (chapter 104)  -> STATE_WI_C104_S104.015

verified against Qdrant by verify_act_ids.py (the existing 812 statute act_ids
are 100% in this form). Citation is rendered ``Wis. Stat. § <section>``.
"""

from __future__ import annotations

from dataclasses import dataclass

COUNTRY = "us"
STATE = "wi"
CORPUS = "statutes"


def chapter_of(section_number: str) -> str:
    """Chapter number = the integer segment before the first dot of a section.

    Wisconsin section numbers are ``<chapter>.<rest>`` (e.g. ``940.225`` ->
    chapter ``940``). Every existing WI statute act_id in Qdrant follows this,
    and it matches the chapter page each section is rendered on.
    """
    return section_number.split(".", 1)[0].strip()


@dataclass(frozen=True)
class WISection:
    section_number: str
    section_title: str

    @property
    def chapter(self) -> str:
        return chapter_of(self.section_number)

    def citation(self) -> str:
        return f"Wis. Stat. § {self.section_number}"

    def node_id(self) -> str:
        return f"{COUNTRY}/{STATE}/{CORPUS}/chapter={self.chapter}/section={self.section_number}"
