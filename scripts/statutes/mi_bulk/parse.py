"""Parse a Michigan Compiled Laws chapter XML document into sections.

MCL chapter XML shape (verified 2026-07-19 against the live tree):

    <MCLChapterInfo>
      <Name>750</Name>                     chapter number
      <Title>...</Title>
      <MCLDocumentInfoCollection>
        <MCLStatuteInfo>                    an ACT ("Act 328 of 1931")
          <Name>Act 328 of 1931</Name>
          <MCLDocumentInfoCollection>
            <MCLDivisionInfo>               structural only ("CHAPTER I") -> flattened
              <MCLDocumentInfoCollection>
                <MCLSectionInfo>            a SECTION
                  <MCLNumber>750.316</MCLNumber>
                  <CatchLine>First degree murder; ...</CatchLine>
                  <Repealed>false</Repealed>
                  <BodyText>&lt;Section-Body&gt;&lt;Section-Number&gt;Sec. 316.&lt;/Section-Number&gt;
                            &lt;Paragraph&gt;&lt;P&gt;(1) ...&lt;/P&gt;&lt;/Paragraph&gt;...</BodyText>
                  <HistoryText>&lt;HistoryData&gt;1931, Act 328, ...&lt;/HistoryData&gt;...</HistoryText>
                </MCLSectionInfo>

``BodyText`` / ``HistoryText`` hold escaped inner markup (their text value is a
mini HTML/XML document with ``<Section-Number>`` / ``<Paragraph>`` / ``<P>`` /
``<Emph>`` / ``<HistoryData>`` tags). We parse that inner markup as HTML and pull
the ordered paragraph text out, so retrieval sees clean statute prose.

Divisions are transparent: sections are assigned to the nearest enclosing act, so
the chapter/act/section triple (hence the act_id) matches the scraper regardless
of division nesting.
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET

from bs4 import BeautifulSoup

from .walk import MISection, act_slug

RESERVED_KEYWORDS = ("repealed", "reserved", "expired", "renumbered", "transferred")


def clean(raw: str) -> str:
    """Normalise whitespace and strip non-breaking spaces / smart quotes."""
    text = (raw or "").replace("\xa0", " ").replace("’", "'").replace("‘", "'")
    text = text.replace("—", "-").replace("–", "-")
    return re.sub(r"\s+", " ", text).strip()


def is_reserved(text: str) -> bool:
    lower = (text or "").lower()
    return any(kw in lower for kw in RESERVED_KEYWORDS)


def _markup_to_paragraphs(markup: str) -> list[str]:
    """Turn a BodyText inner-markup string into ordered paragraph strings.

    ``<Section-Number>`` (the "Sec. N." label, kept as its own leading paragraph,
    matching the HTML scraper) and each leaf ``<P>`` become one paragraph. ``<P>``
    is a leaf that only carries inline ``<Emph>``, so ``find_all`` over the two tag
    names yields every paragraph exactly once in reading order (including ``<P>``s
    nested inside tables). Falls back to the whole cleaned text if the markup has
    no recognised paragraph tags.
    """
    if not markup or not markup.strip():
        return []
    soup = BeautifulSoup(markup, "html.parser")
    paras: list[str] = []
    for el in soup.find_all(["section-number", "p"]):
        t = clean(el.get_text(" "))
        if t:
            paras.append(t)
    if not paras:
        t = clean(soup.get_text(" "))
        if t:
            paras.append(t)
    return paras


def _history_paragraph(markup: str) -> list[str]:
    """Collapse HistoryText's ``<HistoryData>`` blocks into one history paragraph.

    Kept as a trailing paragraph (prefixed ``History:``) so amendment-year
    enrichment and the good-law / currency signals survive, mirroring the Idaho
    bulk. Returns [] when there is no history.
    """
    if not markup or not markup.strip():
        return []
    soup = BeautifulSoup(markup, "html.parser")
    parts = [clean(el.get_text(" ")) for el in soup.find_all("historydata")]
    parts = [p for p in parts if p]
    if not parts:
        whole = clean(soup.get_text(" "))
        parts = [whole] if whole else []
    if not parts:
        return []
    return [f"History: {'; '.join(parts)}"]


def _section_from(el: ET.Element, chapter_name: str, cur_act: str | None) -> MISection | None:
    if cur_act is None:
        return None
    number = clean(el.findtext("MCLNumber") or "")
    if not number:
        return None
    catchline = clean(el.findtext("CatchLine") or "")
    repealed = (el.findtext("Repealed") or "").strip().lower() == "true"
    paragraphs = _markup_to_paragraphs(el.findtext("BodyText") or "")
    paragraphs += _history_paragraph(el.findtext("HistoryText") or "")
    if not paragraphs:
        # Reserved / repealed / empty sections carry no body -> no chunk.
        return None
    return MISection(
        chapter_number=chapter_name,
        act_slug=cur_act,
        section_number=number,
        catchline=catchline,
        paragraphs=tuple(paragraphs),
        repealed=repealed,
    )


def parse_chapter(xml_text: str, chapter_hint: str) -> tuple[list[MISection], int]:
    """Parse one chapter XML -> (sections, act_less_skipped).

    ``act_less_skipped`` counts any section that could not be attributed to an
    enclosing act (would produce a non-reproducing act_id); it should be 0.
    """
    root = ET.fromstring(xml_text)
    chapter_name = clean(root.findtext("Name") or "") or str(chapter_hint)

    sections: list[MISection] = []
    act_less = 0

    def recurse(node: ET.Element, cur_act: str | None) -> None:
        nonlocal act_less
        for child in node:
            tag = child.tag
            if tag == "MCLStatuteInfo":
                recurse(child, act_slug(child.findtext("Name") or ""))
            elif tag in ("MCLDivisionInfo", "MCLDocumentInfoCollection"):
                recurse(child, cur_act)
            elif tag == "MCLSectionInfo":
                sec = _section_from(child, chapter_name, cur_act)
                if sec is not None:
                    sections.append(sec)
                elif cur_act is None and (child.findtext("MCLNumber") or "").strip():
                    act_less += 1
            # scalar metadata tags (Name / Title / History / ...) are ignored

    recurse(root, None)
    return sections, act_less
