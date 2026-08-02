"""SDCL section model + act_id path construction.

South Dakota's hierarchy is a flat Title -> Chapter -> Section (no subtitles,
parts, or articles), so the act_id path has exactly three component pairs after
country/jurisdiction/corpus. The title is a plain integer 1-62, the chapter may
carry a letter suffix (1A, 16B, 16G), and the section number is the full
``title-chapter-section`` token as SDCL cites it (22-16-4, 1-1-1.1, 10-45-1).

Because an SDCL section number literally embeds its title and chapter
(``{title}-{chapter}-{rest}``), the chapter is derived deterministically from the
section number's middle segment rather than re-parsed from a separate TOC. This
reproduces the existing scraper's node path exactly, e.g.
    22-16-4  (Title 22, Chapter 16)  -> STATE_SD_T22_C16_S22-16-4
    1-1-1.1  (Title 1,  Chapter 1)   -> STATE_SD_T1_C1_S1-1-1.1
    1-1A-1   (Title 1,  Chapter 1A)  -> STATE_SD_T1_C1A_S1-1A-1
verified against Qdrant by verify_act_ids.py before any full run.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

COUNTRY = "us"
STATE = "sd"
CORPUS = "statutes"

# A full SDCL section number: title-chapter-section, where title is digits, the
# chapter may carry an alpha suffix, and the section tail may carry alpha and
# dotted decimals (22-16-20.2, 1-1A-1, 10-45-1). Anchored so a heading line's
# citation prefix is captured without the trailing period + headnote.
SECTION_NUM_RE = re.compile(r"^(\d+[A-Za-z]*-\d+[A-Za-z]*-\d+[A-Za-z0-9]*(?:\.\d+)*)")


def chapter_from_section(section_number: str) -> str | None:
    """The chapter token embedded in an SDCL section number (its middle segment).

    ``22-16-4`` -> ``16``; ``1-1A-1`` -> ``1A``; ``10-45-1`` -> ``45``. Returns
    None when the token is not a well-formed title-chapter-section triple.
    """
    parts = section_number.split("-")
    if len(parts) < 3 or not parts[0] or not parts[1]:
        return None
    return parts[1]


def title_from_section(section_number: str) -> str | None:
    parts = section_number.split("-")
    if len(parts) < 3 or not parts[0]:
        return None
    return parts[0]


@dataclass(frozen=True)
class SDSection:
    title_number: str  # plain integer as a string, e.g. "22"
    chapter: str  # e.g. "16", "1A", "16B"
    section_number: str  # full token, e.g. "22-16-4" or "1-1-1.1"
    section_title: str

    def citation(self) -> str:
        # Reproduces the existing SDCL scraper's citation string exactly so the
        # cutover keeps every section's display citation consistent.
        return f"S.D. Codified Laws § {self.section_number}"

    def node_id_pairs(self) -> list[tuple[str, str]]:
        """Ordered (classifier, number) pairs after country/jurisdiction/corpus.

        SDCL is uniformly Title -> Chapter -> Section, so exactly three pairs.
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
