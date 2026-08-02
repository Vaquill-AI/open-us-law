"""Missouri section model + node id / act_id path construction.

The node id path has no title level (RSMo groups chapters under roman-numeral
titles on the TOC but publishes no title landing pages, and the existing corpus
stores sections as ``us/mo/statutes/chapter={chapter}/section={section}``). The
chapter is the section number's integer prefix, which is also the chapter page
the section was enumerated from, so the two always agree.

    565.020  (chapter 565)  -> node us/mo/statutes/chapter=565/section=565.020
                            -> act_id STATE_MO_C565_S565.020
                            -> cite   Mo. Rev. Stat. § 565.020

verified against Qdrant by verify_act_ids.py before any full run.
"""

from __future__ import annotations

from dataclasses import dataclass

COUNTRY = "us"
STATE = "mo"
CORPUS = "statutes"

# Section headnote markers that mean "no live text here" (matches the scraper's
# RESERVED_KEYWORDS). Such sections carry a status and are dropped downstream by
# node_to_chunks (no body text), so they never enter the corpus as chunks.
_RESERVED_KEYWORDS = ("repealed", "reserved", "expired", "renumbered", "transferred")


@dataclass(frozen=True)
class MOSection:
    chapter: str
    section_number: str
    section_title: str

    def citation(self) -> str:
        return f"Mo. Rev. Stat. § {self.section_number}"

    def node_name(self) -> str:
        # Mirror the scraper: "<number> <title>" (cosmetic; not part of point_id).
        return f"{self.section_number} {self.section_title}".strip()

    def node_id(self) -> str:
        return (
            f"{COUNTRY}/{STATE}/{CORPUS}"
            f"/chapter={self.chapter}/section={self.section_number}"
        )

    def source_url(self) -> str:
        return f"https://revisor.mo.gov/main/OneSection.aspx?section={self.section_number}"

    def status(self) -> str | None:
        low = self.node_name().lower()
        for kw in _RESERVED_KEYWORDS:
            if kw in low:
                return "repealed" if "repealed" in low else "reserved"
        return None
