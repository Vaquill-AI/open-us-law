"""Helpers: section body HTML -> text, title dedup, human vacode URL."""

from __future__ import annotations

import re

from bs4 import BeautifulSoup

_WS = re.compile(r"[ \t ]+")

# The Body ends with a boilerplate disclaimer paragraph the site adds to every
# section; it is not part of the statute text, so we drop it.
_SIDENOTE_HINT = "may not constitute a comprehensive list"


def vacode_url(section_number: str) -> str:
    """Human-facing page for a section (what a citation link should open)."""
    return f"https://law.lis.virginia.gov/vacode/{section_number}/"


# Section table-of-contents link on a chapter page (quote style varies across
# pages, so match either): <a href='/vacode/title18.2/chapter4/section18.2-32/'>.
# Body cross-references use the SHORT form /vacode/{num}/, never /section.../, so
# this pattern picks up only this chapter's own section list.
_SECTION_ANCHOR_RE = re.compile(r"/section([0-9][^/\"']+)/")


def container_links(html: str, title_number: str) -> list[str]:
    """All sub-container page paths for a title found in a vacode HTML page.

    Titles are organized differently: most by ``chapterN``, the UCC titles (8.x)
    by ``partN``, some with ``subtitle`` / ``article`` / nested ``part`` levels.
    So enumeration crawls any container link ``/vacode/title{T}/.../`` rather than
    assuming ``chapter``. Section links (``.../section.../``) and the title root
    itself are excluded; a breadth-first crawl over these (deduped by a visited
    set) reaches every section regardless of the title's internal structure.
    Quote style varies across pages, so the href quote is matched loosely.
    """
    te = re.escape(title_number)
    root = f"/vacode/title{title_number}/"
    pat = re.compile(r"['\"](/vacode/title" + te + r"/(?:[^'\"/]+/)+)['\"]")
    seen: set[str] = set()
    out: list[str] = []
    for m in pat.finditer(html):
        u = m.group(1)
        if "/section" in u or u == root:
            continue
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def section_numbers(chapter_html: str) -> list[str]:
    """All section numbers on a chapter page, from the section TOC anchors.

    Uses the ``/section{num}/`` links rather than the row checkboxes: the
    checkbox attribute order / quote style is inconsistent across pages, but the
    section anchor is uniform and matches the JSON section list exactly.
    """
    out: list[str] = []
    seen: set[str] = set()
    for m in _SECTION_ANCHOR_RE.finditer(chapter_html):
        sid = m.group(1).strip()
        if sid in seen:
            continue
        seen.add(sid)
        out.append(sid)
    return out


def dedupe_titles(rows: list[dict]) -> list[dict]:
    """The titles endpoint returns duplicate rows for some titles that differ
    only in dash style (hyphen vs em dash in the name), e.g. two '8.2' rows.
    Keep the first row per distinct TitleNumber, preserving API order.
    """
    seen: set[str] = set()
    out: list[dict] = []
    for r in rows:
        tn = (r.get("TitleNumber") or "").strip()
        if not tn or tn in seen:
            continue
        seen.add(tn)
        out.append(r)
    return out


def body_to_paragraphs(body_html: str) -> list[str]:
    """Extract the section text as an ordered list of paragraph strings.

    The section ``Body`` is a run of block elements (mostly ``<p>``) that
    includes the enacting text and a trailing history/credit paragraph. We keep
    everything except the site's boilerplate ``sidenote`` disclaimer. Each block
    becomes one paragraph so the downstream chunker can break on paragraph
    boundaries; whitespace within a paragraph is collapsed. Deterministic:
    identical Body -> identical paragraphs -> identical content-addressed
    point_id across runs.
    """
    if not body_html or not body_html.strip():
        return []
    soup = BeautifulSoup(body_html, "html.parser")

    blocks = soup.find_all(["p", "li", "blockquote"])
    paras: list[str] = []
    if blocks:
        for b in blocks:
            cls = " ".join(b.get("class") or [])
            if "sidenote" in cls:
                continue
            text = b.get_text(separator=" ")
            text = _WS.sub(" ", text).strip()
            if text and _SIDENOTE_HINT not in text:
                paras.append(text)
    else:
        # No block markup: fall back to the whole body text.
        text = _WS.sub(" ", soup.get_text(separator=" ")).strip()
        if text:
            paras.append(text)
    return paras
