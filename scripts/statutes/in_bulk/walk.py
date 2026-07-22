"""Indiana Code section model + Title-Article-Chapter-Section node_id path.

The Indiana Code is coded by Title, Article, Chapter, Section (TACS): ``35-42-1-1``
is Title 35, Article 42, Chapter 1, Section 1 (murder). The public citation uses
the full number: ``Ind. Code § 35-42-1-1``.

Source is the Commonwealth's official bulk export (the "Current Indiana Code (all
titles): ZIP (HTML only)" from iga.in.gov/laws/ic/downloads). Each section in that
HTML carries its full TACS id on the div, e.g. ``<div class="section"
id="35-42-1-1">``, so the hierarchy is read directly, never guessed.

Levels are separated by ``-``; sub-numbers use ``.`` (Title ``7.1``, Chapter
``3.1``, Section ``16.8``). So a section id splits on ``-`` into exactly four
components. A small number of sections (583 of 83,148 in 2026) are published in
multiple simultaneously-effective versions and carry a trailing ``-{letter}``
(``35-42-2-1-b`` is version b of Battery). Those are real, distinct law, so each
version becomes its own point (the version letter is kept in the node id so the
act_id / point_id / R2 key stay unique), while the citation renders the base
four-part number without the version suffix.

The node_id path mirrors every other state ingest so node_to_payload derives the
act_id / point_id / breadcrumb identically:

    us/in/statutes/title=35/article=42/chapter=1/section=35-42-1-1
        -> act_id  STATE_IN_T35_A42_C1_S35-42-1-1   -> Ind. Code § 35-42-1-1
    us/in/statutes/title=35/article=42/chapter=2/section=35-42-2-1-b
        -> act_id  STATE_IN_T35_A42_C2_S35-42-2-1-b -> Ind. Code § 35-42-2-1
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

COUNTRY = "us"
STATE = "in"
CORPUS = "statutes"

_VERSION_RE = re.compile(r"^[a-z]$")


def parse_section_id(section_id: str) -> Optional["INSection"]:
    """Parse a raw ``<div class="section" id=...>`` id into an INSection stub.

    Returns None for ids that do not resolve to a Title-Article-Chapter-Section
    shape (the caller logs and skips these; none were observed in the 2026 code
    beyond the version case handled here).
    """
    sid = (section_id or "").strip()
    parts = sid.split("-")
    version: Optional[str] = None
    if len(parts) == 5 and _VERSION_RE.match(parts[4]):
        version = parts[4]
        parts = parts[:4]
    if len(parts) != 4 or not all(p.strip() for p in parts):
        return None
    title, article, chapter, section = (p.strip() for p in parts)
    citation_number = f"{title}-{article}-{chapter}-{section}"
    # The node-path section component keeps the version so versioned sections do
    # not collide with the base section on act_id / point_id / R2 key.
    path_section = citation_number if version is None else f"{citation_number}-{version}"
    return INSection(
        title=title,
        article=article,
        chapter=chapter,
        path_section=path_section,
        citation_number=citation_number,
        version=version,
    )


@dataclass(frozen=True)
class INSection:
    title: str            # e.g. "35"  (also "7.1")
    article: str          # e.g. "42"
    chapter: str          # e.g. "1"   (also "3.1")
    path_section: str     # full node-path section component, e.g. "35-42-1-1" / "35-42-2-1-b"
    citation_number: str  # base four-part TACS for the citation, e.g. "35-42-2-1"
    version: Optional[str] = None
    section_title: str = ""
    status: Optional[str] = None  # "repealed" / "reserved" / None

    def with_content(self, section_title: str, status: Optional[str]) -> "INSection":
        return INSection(
            title=self.title,
            article=self.article,
            chapter=self.chapter,
            path_section=self.path_section,
            citation_number=self.citation_number,
            version=self.version,
            section_title=section_title,
            status=status,
        )

    def citation(self) -> str:
        return f"Ind. Code § {self.citation_number}"

    def node_id_pairs(self) -> list[tuple[str, str]]:
        return [
            ("title", self.title),
            ("article", self.article),
            ("chapter", self.chapter),
            ("section", self.path_section),
        ]

    def node_id(self) -> str:
        head = f"{COUNTRY}/{STATE}/{CORPUS}"
        tail = "/".join(f"{cls_}={num}" for cls_, num in self.node_id_pairs())
        return f"{head}/{tail}"

    def public_url(self, session: str) -> str:
        """Human-facing IGA page for the section's title (the citation target)."""
        return f"https://iga.in.gov/laws/{session}/ic/titles/{self.title}"
