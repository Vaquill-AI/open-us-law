"""HTML parsing for the leg.state.fl.us (Online Sunshine) 2025 Florida Statutes.

Three surfaces (see client.py):

  - TOC (``Mode=View Statutes``): links carry ``Title_Request=<roman>`` for each of
    the 49 Titles. ``title_romans`` returns them in order.
  - Title index (``Display_Index&Title_Request=<roman>``): a ``ChapterTOC`` with an
    anchor per chapter whose href is ``...URL=<band>/<pad>/<pad>ContentsIndex.html``.
    ``title_chapters`` returns the chapter numbers for that title, so the Title ->
    Chapter map (hence the act_id Title level) comes from the source, not a guess.
  - Chapter page (``<pad>.html``): the COMPLETE chapter. ``div.Section`` blocks carry
    ``SectionNumber`` / ``CatchlineText`` / ``SectionBody`` / ``History``.
    Part-bearing chapters wrap their sections in ``div.Part`` whose ``PartNumber``
    ("PART I") gives the Part roman. Not windowed (Chapter 627 -> all 628 sections
    across 22 parts, matching flsenate.gov's independent count).

Online Sunshine publishes the same statute text with the same HTML classes as
flsenate.gov, so the section/part extraction is identical; only the enumeration
surfaces (TOC + title index) differ.
"""

from __future__ import annotations

import re

from bs4 import BeautifulSoup

_WS_RE = re.compile(r"\s+")
_ROMAN_RE = re.compile(r"(?:title|part)\s+([IVXLCDM]+)", re.IGNORECASE)
_TITLE_REQ_RE = re.compile(r"Title_Request=([IVXLCDM]+)")
# Chapter link on a title index: ...URL=<band>/<pad>/<pad>ContentsIndex.html (or
# <pad>.html). We only need the padded chapter token (group 2).
_CH_HREF_RE = re.compile(r"URL=(\d{4}-\d{4})/(\d{4})/\d{4}\w*\.html", re.IGNORECASE)

# FL marks dead sections with bracketed status tags in the catchline, e.g.
# "1.05 [Repealed by s. 7, ch. 99-3.]".
_RESERVED_KEYWORDS = (
    "[repealed",
    "[reserved",
    "[expired",
    "[transferred",
    "[renumbered",
    "[former",
)
_HIST_LEAD_RE = re.compile(r"^\s*history\.?\s*[—\-:]*\s*", re.IGNORECASE)


def clean_text(raw: str) -> str:
    text = (raw or "").replace("\xa0", " ").replace("\u200b", "")
    return _WS_RE.sub(" ", text).strip()


def _roman(label: str) -> str | None:
    m = _ROMAN_RE.search(label or "")
    return m.group(1).upper() if m else None


def _has_reserved_marker(text: str) -> bool:
    low = (text or "").lower()
    return any(kw in low for kw in _RESERVED_KEYWORDS)


def title_romans(toc_html: str) -> list[str]:
    """The 49 Title roman numerals, in document order, from the statutes TOC."""
    out: list[str] = []
    seen: set[str] = set()
    for m in _TITLE_REQ_RE.finditer(toc_html):
        r = m.group(1).upper()
        if r not in seen:
            seen.add(r)
            out.append(r)
    return out


def title_chapters(index_html: str) -> list[str]:
    """Chapter numbers listed on a Title index page (deduped, source order).

    The chapter links live in an unclassed table, so we match the padded chapter
    token straight from the page's ``Display_Statute&URL=<band>/<pad>/<pad>*.html``
    hrefs and normalize it to its integer form (``0782`` -> ``782``). A Title index
    page lists only its own title's chapters (verified: the XLVI index yields 49
    chapters, all in the 775-899 band), so no cross-title filtering is needed.
    """
    out: list[str] = []
    seen: set[str] = set()
    for m in _CH_HREF_RE.finditer(index_html):
        number = str(int(m.group(2)))
        if number in seen:
            continue
        seen.add(number)
        out.append(number)
    return out


def _section_paragraphs(section_div) -> list[str]:
    """Body paragraphs of a ``div.Section`` from its ``.SectionBody``.

    Prefers direct-child block texts (subsection/paragraph structure); falls back
    to the flat body text for single-block sections. The number and catchline live
    in separate spans, so the body is body-only (no de-duplication needed).
    """
    body = section_div.find(class_="SectionBody")
    if body is None:
        return []
    paras: list[str] = []
    for child in body.find_all(recursive=False):
        t = clean_text(child.get_text(" "))
        if t:
            paras.append(t)
    if not paras:
        flat = clean_text(body.get_text(" "))
        if flat:
            paras.append(flat)
    return paras


def _section_history(section_div) -> str:
    hist = section_div.find(class_="History")
    if hist is None:
        return ""
    raw = clean_text(hist.get_text(" "))
    return _HIST_LEAD_RE.sub("", raw).strip()


def _parse_section_div(section_div, chapter: str, part_roman: str | None) -> dict | None:
    num_el = section_div.find(class_="SectionNumber")
    if num_el is None:
        return None
    number = clean_text(num_el.get_text(" "))
    if not number:
        return None
    cat_el = section_div.find(class_="CatchlineText")
    catchline = clean_text(cat_el.get_text(" ")) if cat_el is not None else ""
    status = "reserved" if _has_reserved_marker(catchline) else None
    paras = [] if status else _section_paragraphs(section_div)
    history = "" if status else _section_history(section_div)
    return {
        "number": number,
        "catchline": catchline,
        "chapter": chapter,
        "part_roman": part_roman,
        "paragraphs": paras,
        "history": history,
        "status": status,
    }


def parse_chapter_all(html: str, chapter: str) -> list[dict]:
    """Every section in a chapter page, with its Part (roman) resolved.

    Part-bearing chapters wrap sections in ``div.Part`` (roman from ``PartNumber``,
    fallback ``PartTitle``); partless chapters hold ``div.Section`` at the top
    level (part_roman=None). Each ``div.Section`` is attributed to its nearest
    enclosing ``div.Part`` so the mapping is exact rather than range-based.
    """
    soup = BeautifulSoup(html, "html.parser")
    out: list[dict] = []
    seen: set[str] = set()

    for section_div in soup.find_all("div", class_="Section"):
        part_div = section_div.find_parent("div", class_="Part")
        part_roman = None
        if part_div is not None:
            pn = part_div.find(class_="PartNumber") or part_div.find(class_="PartTitle")
            part_roman = _roman(pn.get_text(" ", strip=True)) if pn is not None else None
        rec = _parse_section_div(section_div, chapter, part_roman)
        if rec is None:
            continue
        if rec["number"] in seen:
            continue
        seen.add(rec["number"])
        out.append(rec)
    return out
