"""Florida Statutes section model + act_id / node_id path construction.

Florida's hierarchy in ``statutes_us`` is Title -> Chapter -> [Part] -> Section,
where the Title and (when present) the Part are ROMAN numerals and the Chapter is
the integer prefix of the section number. The section number itself is
``<chapter>.<rest>`` (e.g. ``782.04`` -> chapter ``782``), but the chapter is
carried from the source TOC rather than derived, so it always matches the chapter
page the section was crawled from.

The resulting act_id reproduces the existing scraper's scheme EXACTLY (verified
against Qdrant by verify_act_ids.py):

    782.04   (Title XLVI, Ch 782)              -> STATE_FL_TXLVI_C782_S782.04
    440.09   (Title XXXI, Ch 440)              -> STATE_FL_TXXXI_C440_S440.09
    627.0653 (Title XXXVII, Ch 627, Part I)    -> STATE_FL_TXXXVII_C627_PI_S627.0653
    627.40952(Title XXXVII, Ch 627, Part II)   -> STATE_FL_TXXXVII_C627_PII_S627.40952

Reproducing the Title (roman) and Part (roman) levels is what makes an
act_id-scoped ``--reconcile`` surgical: the rebuilt act_ids match the existing
points, so superseded chunks are retired within the touched act_ids and the 188
Florida constitution points (document_type=constitution, same ``state=fl`` tag)
are never in scope. A flat ``STATE_FL_C<chapter>_S<section>`` would NOT match any
existing act_id and would silently duplicate the whole state. Citation is
rendered ``Fla. Stat. § <section>``.
"""

from __future__ import annotations

from dataclasses import dataclass

COUNTRY = "us"
STATE = "fl"
CORPUS = "statutes"


def chapter_of(section_number: str) -> str:
    """Chapter = the integer segment before the first dot of a section number.

    ``627.0653`` -> ``627``. Used only as a cross-check against the chapter the
    section was crawled from; the crawl carries the authoritative chapter.
    """
    return section_number.split(".", 1)[0].strip()


@dataclass(frozen=True)
class FLSection:
    title_roman: str
    chapter: str
    section_number: str
    section_title: str = ""
    part_roman: str | None = None
    status: str | None = None

    def citation(self) -> str:
        return f"Fla. Stat. § {self.section_number}"

    def node_id(self) -> str:
        segs = [
            COUNTRY,
            STATE,
            CORPUS,
            f"title={self.title_roman}",
            f"chapter={self.chapter}",
        ]
        if self.part_roman:
            segs.append(f"part={self.part_roman}")
        segs.append(f"section={self.section_number}")
        return "/".join(segs)
