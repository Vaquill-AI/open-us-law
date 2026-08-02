"""Parsers for the Iowa Code bulk surfaces.

Two jobs:
  1. Enumeration from the chapter-listing HTML: the Title -> Chapter mapping and
     the RESERVED flag (reserved chapters have no XML body and are skipped).
  2. Per-chapter slim-XML parsing into ordered (section, paragraphs, status)
     tuples. Section identifiers, headnotes, nested subsection/letteredPara body
     text, and amendment history all come straight from the XML, so the output
     is deterministic: identical XML -> identical paragraphs -> identical
     content-addressed point_id across runs.
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ET

from bs4 import BeautifulSoup

from .walk import IASection

_WS = re.compile(r"\s+")

# Roman-numeral Title token as it appears in the TOC ("Title XVI - ...").
_TITLE_RE = re.compile(r"Title\s+([IVXLC]+)\b", re.IGNORECASE)
# Chapter token in a chapter-listing row ("Chapter 256H - ...", "Chapter 8E -").
_CHAPTER_RE = re.compile(r"Chapter\s+(\S+)", re.IGNORECASE)

# xhtml block containers whose ``heading > identifier`` prefixes its body.
_CONTAINER_CLASSES = {
    "subsection",
    "letteredPara",
    "numberedPara",
    "paragraph",
    "unnumberedPara",
    "subparagraph",
    "subparagraphPart",
    "item",
    "subitem",
}


def _clean(raw: str) -> str:
    text = raw.replace("\xa0", " ").replace(" ", " ").replace("\r", " ")
    return _WS.sub(" ", text).strip()


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _text_of(el: ET.Element) -> str:
    return _WS.sub(" ", "".join(el.itertext())).strip()


# ---------------------------------------------------------------------------
# Enumeration (HTML)
# ---------------------------------------------------------------------------


def titles_from_toc(html: str) -> list[str]:
    """Roman-numeral Title tokens from the Iowa Code root TOC, in document order."""
    soup = BeautifulSoup(html, "html.parser")
    il = soup.find(id="iacList")
    if il is None or il.find("tbody") is None:
        return []
    out: list[str] = []
    seen: set[str] = set()
    for row in il.find("tbody").find_all("tr"):
        tds = row.find_all("td")
        if not tds:
            continue
        m = _TITLE_RE.match(_clean(tds[0].get_text()))
        if m and m.group(1).upper() not in seen:
            seen.add(m.group(1).upper())
            out.append(m.group(1).upper())
    return out


def chapters_from_listing(html: str) -> list[tuple[str, bool]]:
    """(chapter_number, is_reserved) pairs from a Title's chapter-listing page.

    ``is_reserved`` chapters carry no statute text (their XML has no sections);
    the caller skips fetching them.
    """
    soup = BeautifulSoup(html, "html.parser")
    il = soup.find(id="iacList")
    if il is None or il.find("tbody") is None:
        return []
    out: list[tuple[str, bool]] = []
    seen: set[str] = set()
    for row in il.find("tbody").find_all("tr"):
        tds = row.find_all("td")
        if not tds:
            continue
        name = _clean(tds[0].get_text())
        m = _CHAPTER_RE.match(name)
        if not m:
            continue
        ch = m.group(1).rstrip("-")
        if ch in seen:
            continue
        seen.add(ch)
        out.append((ch, "RESERVED" in name.upper()))
    return out


# ---------------------------------------------------------------------------
# Body (per-chapter slim XML)
# ---------------------------------------------------------------------------


def _identifier_and_headnote(heading_el: ET.Element) -> tuple[str | None, str]:
    identifier: str | None = None
    headnote = ""
    for sp in heading_el:
        if _local(sp.tag) != "span":
            continue
        cls = sp.get("class")
        if cls == "identifier":
            identifier = _text_of(sp)
        elif cls == "headnote":
            headnote = _text_of(sp)
    return identifier, headnote


def _first_heading(el: ET.Element) -> ET.Element | None:
    for c in el:
        if _local(c.tag) == "div" and c.get("class") == "heading":
            return c
    return None


def _render(el: ET.Element, id_stack: list[str], out_paras: list[str], hist: list[str]) -> None:
    """Depth-first render of a section body into identifier-prefixed paragraphs.

    Structural identifiers (subsection "1", letteredPara "a", ...) are inlined as
    ``(1) (a) `` prefixes on their paragraph text; ``history`` blocks are diverted
    to ``hist`` so the caller can append them as a single trailing paragraph.
    """
    tag = _local(el.tag)
    cls = el.get("class", "")
    if tag == "div" and cls == "history":
        t = _text_of(el)
        if t:
            hist.append(t)
        return
    if tag == "div" and cls == "heading":
        return  # identifier heading is consumed by its parent container
    if tag == "p":
        t = _text_of(el)
        if t:
            prefix = "".join(f"({i}) " for i in id_stack if i)
            out_paras.append((prefix + t).strip())
        return
    if tag == "div" and cls in _CONTAINER_CLASSES:
        heading = _first_heading(el)
        ident = None
        if heading is not None:
            ident, _ = _identifier_and_headnote(heading)
        new_stack = id_stack + [ident] if ident else id_stack
        for c in el:
            if c is heading:
                continue
            _render(c, new_stack, out_paras, hist)
        return
    # Unknown wrapper: recurse without touching the identifier stack.
    for c in el:
        _render(c, id_stack, out_paras, hist)


def parse_chapter_sections(
    xml_bytes: bytes, title_roman: str, chapter: str
) -> list[tuple[IASection, list[str], str | None]]:
    """Parse one chapter's slim XML into ordered (section, paragraphs, status).

    ``status`` is "reserved"/"repealed" when the headnote says so, else None.
    Sections whose body renders empty are still returned; node_to_chunks drops
    them (text < 20 chars), so reserved/repealed stubs never become chunks.
    """
    root = ET.fromstring(xml_bytes)
    out: list[tuple[IASection, list[str], str | None]] = []
    for sec in root.iter():
        if _local(sec.tag) != "Section":
            continue
        heading = _first_heading(sec)
        identifier: str | None = None
        headnote = ""
        if heading is not None:
            identifier, headnote = _identifier_and_headnote(heading)
        secnum = (identifier or (sec.get("id") or "").replace("sec", "", 1)).strip()
        if not secnum:
            continue

        paras: list[str] = []
        hist: list[str] = []
        for c in sec:
            if c is heading:
                continue
            _render(c, [], paras, hist)
        if hist:
            paras.append(" ".join(hist))

        low = headnote.lower()
        status = "repealed" if "repealed" in low else ("reserved" if "reserved" in low else None)

        section = IASection(
            title_number=title_roman,
            chapter=chapter,
            section_number=secnum,
            section_title=headnote,
        )
        out.append((section, paras, status))
    return out
