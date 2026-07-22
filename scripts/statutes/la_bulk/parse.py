"""HTML parsing helpers for legis.la.gov TOC and Law pages.

Deterministic by construction: identical page -> identical paragraphs ->
identical content-addressed point_id across runs.
"""

from __future__ import annotations

import re

from bs4 import BeautifulSoup

from . import walk as W

_WS = re.compile(r"[\s ]+")

_DOCID_RE = re.compile(r"Law\.aspx\?d=(\d+)")
_HEADER_RE = re.compile(r'id="ctl00_ctl00_PageBody_PageContent_LabelHeader"[^>]*>([^<]{0,80})')

# LabelName: "<PREFIX> <number>". Longer prefixes first so CCP/CCRP win over CC.
_LABEL_RE = re.compile(r"^(RS|CCRP|CCP|CHC|CE|CC)\s+(.+?)\s*$")

# A section/article heading block: "§30.  First degree murder" or
# "Art. 1.  Sources of law" (the number may carry dots/letters).
_HEADING_RE = re.compile(r"^(?:§|Art\.)\s*[0-9][0-9A-Za-z.\-]*\.?\s*(?P<title>.*)$")


def folder_header(html: str) -> str:
    """The LabelHeader text of a TOC page (the body name), or ''."""
    m = _HEADER_RE.search(html)
    return m.group(1).strip() if m else ""


def toc_docids(html: str) -> list[str]:
    """Ordered, de-duplicated Law.aspx doc ids on a TOC page."""
    seen: set[str] = set()
    out: list[str] = []
    for m in _DOCID_RE.finditer(html):
        d = m.group(1)
        if d not in seen:
            seen.add(d)
            out.append(d)
    return out


def label_name(html: str) -> str:
    """The LabelName text of a Law page (e.g. 'RS 14:30'), or ''."""
    s = BeautifulSoup(html, "html.parser")
    el = s.find(id="ctl00_PageBody_LabelName")
    return el.get_text(" ", strip=True) if el else ""


def parse_label(label: str) -> tuple[str, str, str] | None:
    """Parse a LabelName into (body_prefix, title, number).

    Returns None for non-section rows: an unknown prefix, or a Revised-Statutes
    title-heading doc (``RS 14`` with no ``:section``), or an article row with no
    numeric article. ``title`` is '' for the article codes.
    """
    m = _LABEL_RE.match(label.strip())
    if not m:
        return None
    body, rest = m.group(1), m.group(2).strip()
    if body not in W.BODIES:
        return None
    if body == "RS":
        if ":" not in rest:
            return None  # title-heading doc (e.g. "RS 14"), not a section
        title, _, number = rest.partition(":")
        title, number = title.strip(), number.strip()
        if not title or not number:
            return None
        return body, title, number
    # article codes: the whole remainder is the article number; require a digit.
    if not re.match(r"^[0-9]", rest):
        return None
    return body, "", rest


def document_blocks(html: str) -> list[str]:
    """The LabelDocument text as an ordered list of whitespace-collapsed blocks.

    LabelDocument wraps the law text in a ``<div id="WPMainDoc">`` whose children
    are ``<p>`` blocks (structural headers, the section heading, each subsection,
    and the trailing history/credit line). Each block becomes one paragraph so
    the downstream chunker can break on paragraph boundaries.
    """
    s = BeautifulSoup(html, "html.parser")
    doc = s.find(id="ctl00_PageBody_LabelDocument")
    if doc is None:
        return []
    blocks = doc.find_all(["p", "li", "blockquote"])
    out: list[str] = []
    if blocks:
        for b in blocks:
            text = _WS.sub(" ", b.get_text(" ")).strip()
            if text:
                out.append(text)
    else:
        text = _WS.sub(" ", doc.get_text(" ")).strip()
        if text:
            out.append(text)
    return out


def heading_and_body(blocks: list[str]) -> tuple[str, list[str]]:
    """Split blocks into (section heading, body paragraphs).

    Leading structural headers (TITLE / CHAPTER / BOOK / PART ... — the blocks
    before the "§N." / "Art. N." heading) are navigational and repeat across the
    body's sections, so they are dropped; the breadcrumb/display path already
    encodes the container hierarchy. The heading text becomes node_name; every
    block after it (including the trailing "Acts ..." history line) is body text.
    """
    for i, b in enumerate(blocks):
        m = _HEADING_RE.match(b)
        if m:
            heading = m.group("title").strip()
            return heading, [x for x in blocks[i + 1 :] if x]
    # No explicit "§N."/"Art. N." heading found: keep all blocks as body.
    return "", [x for x in blocks if x]
