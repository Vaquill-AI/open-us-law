"""Michigan Compiled Laws section model + act_id path construction.

The MCL hierarchy the scraper (state_scrapers/.../mi/statutes/scrapeMI.py)
recorded is three levels: Chapter -> Act -> Section. The section number itself
encodes the chapter (e.g. ``750.316`` sits in Chapter 750) and the act comes from
the enacting "Act N of YYYY" the section belongs to. Intermediate MCL divisions
(``MCLDivisionInfo``, e.g. "CHAPTER I" inside the Penal Code) are flattened into
their parent act, matching the scraper, which listed every section of an act in a
single flat table with no division level.

The node id path is ``us/mi/statutes/chapter={C}/act={act_slug}/section={sec}``
and the resulting act_id reproduces the existing scraper's scheme exactly, e.g.

    750.316 (chapter 750, Act 328 of 1931) -> STATE_MI_C750_AAct-328-of-1931_S750.316
    18.405  (chapter 18,  Act 541 of 1978) -> STATE_MI_C18_AAct-541-of-1978_S18.405
    388.997 (chapter 388, E.R.O. No. 2003-2) -> STATE_MI_C388_AE-R-O-No-2003-2_S388.997

verified against Qdrant by verify_act_ids.py before any full run. The citation is
``Mich. Comp. Laws § {section}`` and the source_url is the canonical stateless
``/Laws/MCL?objectName=mcl-{section-with-dashes}`` page, both matching the
existing Michigan statute points.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

COUNTRY = "us"
STATE = "mi"
CORPUS = "statutes"

BASE_URL = "https://www.legislature.mi.gov"


def act_slug(name: str) -> str:
    """Reproduce the scraper's act slug from an MCL act ``Name``.

    The scraper took the slug from the chapter page's ``objectName=mcl-<slug>``
    href with the leading ``mcl-`` stripped; the slug is the act's name with each
    run of non-alphanumeric characters (spaces, dots, parentheses) collapsed to a
    single dash, while existing internal dashes (year ranges like ``2003-2``) are
    preserved. A TRAILING dash is kept, not stripped: the site's own slug retains
    it, so an act named "Act 31 of 1948 (1st Ex. Sess.)" is
    ``Act-31-of-1948-1st-Ex-Sess-`` in the existing corpus (verified against
    Qdrant by verify_act_ids.py). Only a leading dash is trimmed (defensive; act
    names always start with a letter).

        "Act 328 of 1931"            -> "Act-328-of-1931"
        "E.R.O. No. 2003-2"          -> "E-R-O-No-2003-2"
        "Act 31 of 1948 (1st Ex. Sess.)" -> "Act-31-of-1948-1st-Ex-Sess-"
    """
    return re.sub(r"[^0-9A-Za-z-]+", "-", (name or "").strip()).lstrip("-")


@dataclass(frozen=True)
class MISection:
    chapter_number: str
    act_slug: str
    section_number: str
    catchline: str
    paragraphs: tuple[str, ...]
    repealed: bool = False

    def citation(self) -> str:
        return f"Mich. Comp. Laws § {self.section_number}"

    def node_name(self) -> str:
        # Mirrors the scraper: the section description, else a "§ N" fallback.
        return self.catchline or f"§ {self.section_number}"

    def object_name(self) -> str:
        # Section objectName: dots in the MCL number become dashes,
        # e.g. "750.316a" -> "mcl-750-316a".
        return "mcl-" + self.section_number.replace(".", "-")

    def url(self) -> str:
        return f"{BASE_URL}/Laws/MCL?objectName={self.object_name()}"

    def node_id(self) -> str:
        return (
            f"{COUNTRY}/{STATE}/{CORPUS}"
            f"/chapter={self.chapter_number}"
            f"/act={self.act_slug}"
            f"/section={self.section_number}"
        )

    def parent_id(self) -> str:
        return "/".join(self.node_id().split("/")[:-1])
