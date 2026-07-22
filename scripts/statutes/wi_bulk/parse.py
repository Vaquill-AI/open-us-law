"""HTML parsing for the docs.legis.wisconsin.gov statutes viewer.

The viewer renders a document as a FLAT sequence of ``div`` blocks under
``#document`` (not a nested tree). For a section the blocks are, in order:

    div.qsatxt_1sect[data-section=S]     -- number span + title span (+ prose
                                            for sections with no subsections)
    div.qsatxt_2subsect[data-section=S]  -- subsection "(1) ..."
    div.qsatxt_3para[data-section=S]     -- paragraph "(a) ..."
    div.qsatxt_4subdiv[data-section=S]   -- subdivision "1. ..."
    div.qsnote_history[data-section=S]   -- "History: ..." (kept as addendum)
    div.qsnote_annot / qsnote_* [S]      -- case annotations / cross-refs (DROPPED)

So a section's body is the concatenation of every ``div.qsatxt_*`` sharing that
``data-section`` (NOT just the qsatxt_1sect wrapper -- the old scraper read only
the wrapper and captured title-only text for every structured section). Case
annotations live in separate ``qsnote_*`` siblings and are excluded.

The viewer is windowed: a section page renders a small centered window of
consecutive sections and a wider (~45-entry) table-of-contents anchor list.
``section_anchors`` harvests the anchor list (used to enumerate a chapter);
``parse_page`` harvests the fully-rendered section bodies present on the page.
"""

from __future__ import annotations

import copy
import re

from bs4 import BeautifulSoup

from .walk import chapter_of

_WS_RE = re.compile(r"\s+")
# Section-number anchor in the TOC / cross-reference lists: /document/statutes/N.MM
_SEC_ANCHOR_RE = re.compile(r"/document/statutes/(\d+\.\d+\w*)(?:[/#?]|$)")
# Leading "<secnum> History History:" redundancy the viewer prints before the note.
_HIST_LEAD_RE = re.compile(r"^\s*[\d.]+\w*\s+History\s+History:\s*", re.IGNORECASE)

_RESERVED_KEYWORDS = ("[repealed]", "[reserved]", "[expired]", "(repealed)", "(reserved)")


def clean_text(raw: str) -> str:
    text = raw.replace("\xa0", " ").replace(" ", " ").replace("\u200b", "")
    return _WS_RE.sub(" ", text).strip()


def _soup(html: str) -> BeautifulSoup:
    soup = BeautifulSoup(html, "html.parser")
    return soup.find(id="document") or soup


def section_anchors(html: str, chapter: str | None = None) -> set[str]:
    """All section numbers linked from a page's TOC / cross-reference anchors.

    If ``chapter`` is given, only section numbers whose chapter matches are
    returned (cross-references into other chapters are excluded).
    """
    doc = _soup(html)
    out: set[str] = set()
    for a in doc.find_all("a", href=True):
        m = _SEC_ANCHOR_RE.search(a["href"])
        if not m:
            continue
        sec = m.group(1)
        if chapter is not None and chapter_of(sec) != chapter:
            continue
        out.add(sec)
    return out


def _is_qsatxt(cls_list) -> bool:
    return any(c.startswith("qsatxt_") for c in (cls_list or []))


def _status_for(title: str, body: str) -> str | None:
    probe = f"{title} {body[:200]}".lower()
    for kw in _RESERVED_KEYWORDS:
        if kw in probe:
            return "repealed" if "repeal" in kw else "reserved"
    return None


def parse_page(html: str, chapter: str) -> tuple[list[str], dict[str, dict]]:
    """Harvest every fully-rendered section body for ``chapter`` on this page.

    Returns ``(ordered, sections)`` where ``ordered`` is the in-chapter
    data-section values in document order (so callers can distinguish the
    possibly-truncated last section at the window's forward edge) and
    ``sections[secnum]`` = ``{"title", "paragraphs", "history", "status"}``.
    """
    doc = _soup(html)

    ordered: list[str] = []
    seen: set[str] = set()
    paras_by_sec: dict[str, list[str]] = {}
    title_by_sec: dict[str, str] = {}

    for div in doc.find_all("div"):
        cls = div.get("class") or []
        if not _is_qsatxt(cls):
            continue
        sec = (div.get("data-section") or "").strip()
        if not sec or chapter_of(sec) != chapter:
            continue
        if sec not in seen:
            seen.add(sec)
            ordered.append(sec)

        div_copy = copy.copy(div)
        title_span = div_copy.find("span", class_="qstitle_sect")
        if title_span is not None and sec not in title_by_sec:
            title_by_sec[sec] = clean_text(title_span.get_text(" "))
        # Drop the redundant section-number span, the section title span (kept
        # separately as node_name), and inline cross-reference anchors.
        for sp in div_copy.find_all("span", class_="qsnum_sect"):
            sp.decompose()
        for sp in div_copy.find_all("span", class_="qstitle_sect"):
            sp.decompose()
        for a in div_copy.find_all("a", class_="reference"):
            a.decompose()
        text = clean_text(div_copy.get_text(" "))
        if text:
            paras_by_sec.setdefault(sec, []).append(text)

    hist_by_sec: dict[str, str] = {}
    for div in doc.find_all("div", class_="qsnote_history"):
        sec = (div.get("data-section") or "").strip()
        if not sec or chapter_of(sec) != chapter:
            continue
        raw = clean_text(div.get_text(" "))
        raw = _HIST_LEAD_RE.sub("", raw).strip()
        if raw:
            hist_by_sec[sec] = (hist_by_sec.get(sec, "") + " " + raw).strip()

    sections: dict[str, dict] = {}
    for sec in ordered:
        paras = paras_by_sec.get(sec, [])
        title = title_by_sec.get(sec, "")
        body_join = " ".join(paras)
        sections[sec] = {
            "title": title,
            "paragraphs": paras,
            "history": hist_by_sec.get(sec, ""),
            "status": _status_for(title, body_join),
        }
    return ordered, sections


_PDF_HEADER_RE = re.compile(
    r"updated|published and certified|electronically scanned|wis\. stats?\.", re.IGNORECASE
)


def pdf_front_toc_sections(pdf_text: str, chapter: str) -> set[str]:
    """The chapter's CURRENT sections from a chapter PDF's front table of contents.

    The completeness signal for verify_act_ids.py. Renumbered / repealed section
    numbers appear only in the PDF BODY (in renumbering notes and cross-references),
    never in the front TOC, so counting all ``\\d+\\.\\d+`` tokens over-counts by
    those phantoms (that made an earlier check falsely report missing sections).

    The front TOC is everything before the first ``History:`` line (the first
    History belongs to the first body section, which follows the whole TOC). We
    drop the repeating page-header/footer lines within that region, then collect
    the in-chapter section numbers that name a real TOC entry.
    """
    lower = pdf_text.lower()
    cut = lower.find("history:")
    region = pdf_text[:cut] if cut != -1 else pdf_text
    out: set[str] = set()
    for line in region.splitlines():
        if _PDF_HEADER_RE.search(line):
            continue
        for m in re.finditer(r"\b(\d+\.\d+\w*)\b", line):
            sec = m.group(1)
            if chapter_of(sec) == chapter:
                out.add(sec)
    return out
