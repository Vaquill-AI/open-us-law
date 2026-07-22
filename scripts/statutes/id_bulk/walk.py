"""Idaho Code section model + act_id path construction.

The Idaho hierarchy is a clean three levels: Title -> Chapter -> Section. The
section number itself encodes the title (e.g. ``18-4003`` is in Title 18) but not
the chapter, so the chapter is taken from the chapter page the section was
enumerated under. Sub-chapter sections (rare; e.g. detachment / annexation rows)
are flattened into their parent chapter, matching the scraper.

The node id path is ``us/id/statutes/title={T}/chapter={C}/section={sec}`` and the
resulting act_id reproduces the existing scraper's scheme exactly, e.g.

    18-4003 (title 18, chapter 40) -> STATE_ID_T18_C40_S18-4003
    9-405   (title 9,  chapter 4)  -> STATE_ID_T9_C4_S9-405

verified against Qdrant by verify_act_ids.py before any full run. The citation is
``Idaho Code § {section}``, matching the existing Idaho statute points.
"""

from __future__ import annotations

from dataclasses import dataclass

COUNTRY = "us"
STATE = "id"
CORPUS = "statutes"


@dataclass(frozen=True)
class IDSection:
    title_number: str
    chapter_number: str
    section_number: str
    section_desc: str
    url: str
    reserved: bool = False

    def citation(self) -> str:
        return f"Idaho Code § {self.section_number}"

    def node_name(self) -> str:
        # Mirrors the scraper: "{sec_label} {section_desc}".
        return f"{self.section_number} {self.section_desc}".strip()

    def node_id(self) -> str:
        return (
            f"{COUNTRY}/{STATE}/{CORPUS}"
            f"/title={self.title_number}"
            f"/chapter={self.chapter_number}"
            f"/section={self.section_number}"
        )

    def parent_id(self) -> str:
        return "/".join(self.node_id().split("/")[:-1])
