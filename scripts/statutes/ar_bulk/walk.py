"""Arkansas section model + node id path, citation, and URL classification.

The Arkansas Code is Title -> [Subtitle ->] Chapter -> [Subchapter ->] Section.
Justia's current-edition URLs mirror that tree, with the subtitle and subchapter
levels OPTIONAL, e.g.

    /codes/arkansas/title-5/subtitle-2/chapter-10/section-5-10-102/
    /codes/arkansas/title-5/subtitle-2/chapter-10/subchapter-2/section-5-10-201/

Section numbers are the full dashed citation number ("5-10-102"), which already
encodes title-chapter, so it is globally unique on its own. We build the node id
path from title / chapter / [subchapter] / section (the subtitle level is a
coarse grouping and is dropped, matching the existing AR act_id shape), so
``node_to_payload.node_to_chunks`` derives:

    us/ar/statutes/title=5/chapter=10/section=5-10-102
        -> STATE_AR_T5_C10_S5-10-102        Ark. Code Ann. § 5-10-102
    us/ar/statutes/title=5/chapter=10/subchapter=2/section=5-10-201
        -> STATE_AR_T5_C10_S2_S5-10-201     Ark. Code Ann. § 5-10-201

The existing 46 Qdrant statute points are FindLaw-format stubs whose act_ids are
inconsistent (some carry a subchapter, some do not) and will NOT reproduce, so
the cutover reconciles state-scoped (document_type=statute), which preserves the
236 Ark. constitution points under state=ar. The full section number in the
act_id keeps it unique and stable even when Justia reshuffles a subchapter.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

COUNTRY = "us"
STATE = "ar"
CORPUS = "statutes"

# One section page URL. Subtitle and subchapter are optional; the section slug is
# the full dashed number (e.g. "5-10-102", "5-10-102.1", "26-51-1601A").
SECTION_URL_RE = re.compile(
    r"/codes/arkansas/"
    r"title-(?P<title>[0-9]+[A-Za-z]?)/"
    r"(?:subtitle-(?P<subtitle>[^/]+)/)?"
    r"chapter-(?P<chapter>[0-9]+[A-Za-z]?)/"
    r"(?:subchapter-(?P<subchapter>[^/]+)/)?"
    r"section-(?P<section>[0-9A-Za-z][0-9A-Za-z.\-]*)/?$",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class ARSection:
    title: str  # "5"
    chapter: str  # "10"
    subchapter: str  # "2" or "" when the section sits directly under a chapter
    number: str  # full dashed section number, e.g. "5-10-102"
    heading: str  # section heading (node_name), e.g. "Murder in the first degree"

    def citation(self) -> str:
        return f"Ark. Code Ann. § {self.number}"

    def node_id_pairs(self) -> list[tuple[str, str]]:
        pairs: list[tuple[str, str]] = [("title", self.title), ("chapter", self.chapter)]
        if self.subchapter:
            pairs.append(("subchapter", self.subchapter))
        pairs.append(("section", self.number))
        return pairs

    def node_id(self) -> str:
        head = f"{COUNTRY}/{STATE}/{CORPUS}"
        tail = "/".join(f"{cls_}={num}" for cls_, num in self.node_id_pairs())
        return f"{head}/{tail}"

    def top_level_title(self) -> str:
        return self.title


def section_from_url(url: str) -> ARSection | None:
    """Build an ARSection skeleton (no heading/text yet) from a section URL.

    Returns None if the URL is not a section page (title/chapter/subchapter TOC).
    """
    m = SECTION_URL_RE.search(url)
    if not m:
        return None
    return ARSection(
        title=m.group("title"),
        chapter=m.group("chapter"),
        subchapter=(m.group("subchapter") or ""),
        number=m.group("section"),
        heading="",
    )


def is_section_url(url: str) -> bool:
    return SECTION_URL_RE.search(url) is not None


_YEAR_SEG_RE = re.compile(r"(/codes/arkansas/)(?:19|20)\d{2}/")


def to_current_url(url: str) -> str:
    """Strip a Justia edition-year segment so the URL points at the current code.

    Wayback captures carry the edition year (``/codes/arkansas/2020/title-5/...``);
    the current edition is the no-year form, which is what Exa's cache is keyed on.
    """
    u = _YEAR_SEG_RE.sub(r"\1", url)
    return u.split("#", 1)[0].split("?", 1)[0]
