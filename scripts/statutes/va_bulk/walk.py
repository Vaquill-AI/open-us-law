"""Code of Virginia section model + act_id path construction.

Section numbers are enumerated from the vacode HTML (see parse.py); the section
hierarchy and body come from the JSON section-detail endpoint, whose
``ChapterList[0]`` carries the full ancestry:

    {SubtitleNum, PartNum, ChapterNum, SubPartNum, ArticleNum, SectionNumber, Body}

Empty levels are omitted from the node id path. The resulting act_id reproduces
the existing scraper's scheme exactly, e.g.
    18.2-32  (chapter 4, article 1)          -> STATE_VA_T18.2_C4_A1_S18.2-32
    3.2-3220 (subtitle III, chapter 32)      -> STATE_VA_T3.2_SIII_C32_S3.2-3220
    10.1-1149 (subtitle II, chapter 11, art 6)-> STATE_VA_T10.1_SII_C11_A6_S10.1-1149
    1-300    (chapter 3.1)                    -> STATE_VA_T1_C3.1_S1-300
verified against Qdrant by verify_act_ids.py.

The section-list endpoint was NOT used for the hierarchy: it drops decimal
chapters and mis-groups some sections (10.1-1149 is absent from the chapter-11
list yet present in detail). The detail endpoint is the authoritative per-section
source.
"""

from __future__ import annotations

from dataclasses import dataclass

COUNTRY = "us"
STATE = "va"
CORPUS = "statutes"


def _clean(v) -> str:
    return (v or "").strip()


@dataclass(frozen=True)
class VASection:
    title_number: str
    title_name: str
    subtitle: str
    part: str
    chapter: str
    subpart: str
    article: str
    section_number: str
    section_title: str

    def citation(self) -> str:
        return f"Va. Code Ann. § {self.section_number}"

    def node_id_pairs(self) -> list[tuple[str, str]]:
        """Ordered (classifier, number) pairs after country/jurisdiction/corpus.

        Order mirrors the detail endpoint's hierarchy fields: title -> subtitle
        -> part -> chapter -> subpart -> article -> section. Only non-empty
        levels are included.
        """
        pairs: list[tuple[str, str]] = [("title", self.title_number)]
        if self.subtitle:
            pairs.append(("subtitle", self.subtitle))
        if self.part:
            pairs.append(("part", self.part))
        if self.chapter:
            pairs.append(("chapter", self.chapter))
        if self.subpart:
            pairs.append(("subpart", self.subpart))
        if self.article:
            pairs.append(("article", self.article))
        pairs.append(("section", self.section_number))
        return pairs

    def node_id(self) -> str:
        head = f"{COUNTRY}/{STATE}/{CORPUS}"
        tail = "/".join(f"{cls_}={num}" for cls_, num in self.node_id_pairs())
        return f"{head}/{tail}"


def section_from_detail(detail_json: dict) -> tuple[VASection | None, str]:
    """Build a VASection + body HTML from a section-detail JSON response.

    Returns (None, "") when the response has no section (e.g. a section that no
    longer exists in the current Code).
    """
    cl = detail_json.get("ChapterList") or []
    if not cl:
        return None, ""
    c = cl[0]
    secnum = _clean(c.get("SectionNumber"))
    if not secnum:
        return None, ""
    sec = VASection(
        title_number=_clean(detail_json.get("TitleNumber")),
        title_name=_clean(detail_json.get("TitleName")),
        subtitle=_clean(c.get("SubtitleNum")),
        part=_clean(c.get("PartNum")),
        chapter=_clean(c.get("ChapterNum")),
        subpart=_clean(c.get("SubPartNum")),
        article=_clean(c.get("ArticleNum")),
        section_number=secnum,
        section_title=_clean(c.get("SectionTitle")),
    )
    body = c.get("Body") or c.get("SectionText") or ""
    return sec, body
