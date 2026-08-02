"""Iowa Code section model + act_id path construction.

Iowa's hierarchy is a flat Title -> Chapter -> Section (no subtitles, parts, or
articles), so the act_id path has exactly three component pairs after
country/jurisdiction/corpus. The title is a Roman numeral (e.g. XVI), the chapter
may carry a letter suffix (256H, 8E, 147I), and the section number is the full
``chapter.section`` token as Iowa cites it (707.2, 256H.1, 8E.205).

Reproduces the existing scraper's scheme exactly, e.g.
    707.2   (Title XVI, Chapter 707)  -> STATE_IA_TXVI_C707_S707.2
    294.4   (Title VII, Chapter 294)  -> STATE_IA_TVII_C294_S294.4
    256H.1  (Title VII, Chapter 256H) -> STATE_IA_TVII_C256H_S256H.1
verified against Qdrant by verify_act_ids.py before any full run.
"""

from __future__ import annotations

from dataclasses import dataclass

COUNTRY = "us"
STATE = "ia"
CORPUS = "statutes"


@dataclass(frozen=True)
class IASection:
    title_number: str  # Roman numeral, e.g. "XVI"
    chapter: str  # e.g. "707" or "256H"
    section_number: str  # full token, e.g. "707.2" or "256H.1"
    section_title: str

    def citation(self) -> str:
        return f"Iowa Code § {self.section_number}"

    def node_id_pairs(self) -> list[tuple[str, str]]:
        """Ordered (classifier, number) pairs after country/jurisdiction/corpus.

        Iowa is uniformly Title -> Chapter -> Section, so exactly three pairs.
        """
        return [
            ("title", self.title_number),
            ("chapter", self.chapter),
            ("section", self.section_number),
        ]

    def node_id(self) -> str:
        head = f"{COUNTRY}/{STATE}/{CORPUS}"
        tail = "/".join(f"{cls_}={num}" for cls_, num in self.node_id_pairs())
        return f"{head}/{tail}"
