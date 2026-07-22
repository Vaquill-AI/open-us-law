"""Tennessee Code section model + act_id path construction.

Tennessee's hierarchy in ``statutes_us`` is flat ``title=T/chapter=C/section=S``
(Justia's Part level is structural only, not part of the citation and not in the
existing scraper's node path). The TCA section number itself is
``<title>-<chapter>-<rest>`` (e.g. ``39-13-202`` -> title ``39`` chapter ``13``),
so title and chapter are taken authoritatively from the Justia URL path (which
always carries ``title-<T>/chapter-<C>/.../section-<slug>``) and never guessed
from the slug alone.

The resulting act_id reproduces the existing scraper's scheme exactly:

    39-13-202  (title 39, chapter 13)  -> STATE_TN_T39_C13_S39-13-202
    56-7-101   (title 56, chapter 7)   -> STATE_TN_T56_C7_S56-7-101
    47-2-201   (title 47, chapter 2)   -> STATE_TN_T47_C2_S47-2-201

verified against Qdrant by verify_act_ids.py (the existing TN statute act_ids,
titles 1-40, are in this form -> an act_id-scoped ``--reconcile`` is safe: it
never touches the 148 ``document_type=constitution`` points that share the
``state=tn`` tag). Citation is rendered ``Tenn. Code Ann. § <section>``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional

COUNTRY = "us"
STATE = "tn"
CORPUS = "statutes"

# A Justia TN code URL, optionally year-prefixed, down to the section leaf.
# Part / subpart / subchapter segments between chapter and section are ignored
# (they are not in the citation or the existing node path).
_URL_RE = re.compile(
    r"/codes/tennessee/(?:\d{4}/)?"
    r"title-(?P<title>[0-9]+[A-Za-z]?)/"
    r"(?:[a-z]+-[\w.-]+/)*?"
    r"chapter-(?P<chapter>[\w.-]+)/"
    r"(?:[a-z]+-[\w.-]+/)*?"
    r"section-(?P<section>[\w.-]+)/?$"
)


@dataclass(frozen=True)
class TNSection:
    """One Tennessee Code section keyed by its Justia URL.

    ``title`` / ``chapter`` come from the URL path; ``section_number`` is the
    section slug verbatim (the full ``T-C-...`` citation token).
    """

    url: str
    title: str
    chapter: str
    section_number: str
    section_name: str = ""

    def citation(self) -> str:
        return f"Tenn. Code Ann. § {self.section_number}"

    def node_id(self) -> str:
        return (
            f"{COUNTRY}/{STATE}/{CORPUS}/"
            f"title={self.title}/chapter={self.chapter}/section={self.section_number}"
        )


def section_from_url(url: str, section_name: str = "") -> Optional[TNSection]:
    """Build a TNSection from a Justia section URL, or None if it is not one.

    Robust to year-prefixed URLs and to intervening part/subchapter/subpart
    segments. The section belongs to ``title`` iff the slug's first dash-segment
    equals the URL title (guards against cross-title reference links leaking in).
    """
    m = _URL_RE.search(url.split("?", 1)[0])
    if not m:
        return None
    title = m.group("title")
    chapter = m.group("chapter")
    section = m.group("section")
    # The section slug must start with the title number, e.g. title-39 -> 39-...
    # This drops cross-references to sections in other titles that appear as
    # links on a page (a section page cites siblings across the code).
    first_seg = section.split("-", 1)[0]
    if first_seg != title:
        return None
    return TNSection(
        url=f"https://law.justia.com{m.group(0)}" if url.startswith("/") else url.split("?", 1)[0],
        title=title,
        chapter=chapter,
        section_number=section,
        section_name=section_name,
    )
