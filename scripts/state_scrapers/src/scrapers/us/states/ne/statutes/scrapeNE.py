"""Nebraska Revised Statutes scraper.

Source: https://nebraskalegislature.gov/laws/browse-statutes.php

Hierarchy:
    corpus  (us/ne/statutes)
    chapter (us/ne/statutes/chapter=N)
    section (us/ne/statutes/chapter=N/section=N-NNN)

Citation format: "Neb. Rev. Stat. § <SECTION>" (e.g., "Neb. Rev. Stat. § 1-101")

NE section numbers embed the chapter as a prefix (e.g., 1-101 is chapter 1,
section 101), so the hierarchy is two levels below corpus: chapter, then section.
"""
from __future__ import annotations

import logging
import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Path bootstrap (mirrors ME/DE pattern so imports work from any cwd).
# ---------------------------------------------------------------------------
_current_file = Path(__file__).resolve()
_src_dir = _current_file.parent
while _src_dir.name != "src" and _src_dir.parent != _src_dir:
    _src_dir = _src_dir.parent
_project_root = _src_dir.parent
if str(_project_root) not in sys.path:
    sys.path.append(str(_project_root))

from src.utils.pydanticModels import Addendum, AddendumType, Node, NodeText
from src.utils.scrapingHelpers import (
    get_url_as_soup,
    insert_jurisdiction_and_corpus_node,
    insert_node,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
COUNTRY = "us"
JURISDICTION = "ne"
CORPUS = "statutes"
TABLE_NAME = f"{COUNTRY}_{JURISDICTION}_{CORPUS}"

BASE_URL = "https://nebraskalegislature.gov"
TOC_URL = f"{BASE_URL}/laws/browse-statutes.php"
CHAPTER_URL = f"{BASE_URL}/laws/browse-chapters.php?chapter={{chapter}}"
SECTION_URL = f"{BASE_URL}/laws/statutes.php?statute={{statute}}"

RESERVED_PATTERNS = re.compile(
    r"\brepealed\b|\bexpired\b|\breserved\b|\brenumbered\b|\bunconstitutional\b"
    r"|\btransferred to\b",
    re.IGNORECASE,
)

# Body-text repealed/transferred indicator (matches short stub bodies like
# "Repealed. Laws 1972, LB 1284, § 9." that don't surface in TOC anchor text.)
BODY_STATUS_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"\brepealed\b", re.IGNORECASE), "repealed"),
    (re.compile(r"\btransferred to\b", re.IGNORECASE), "transferred"),
    (re.compile(r"\brenumbered\b", re.IGNORECASE), "renumbered"),
    (re.compile(r"\bheld unconstitutional\b", re.IGNORECASE), "unconstitutional"),
]


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    corpus_node: Node = insert_jurisdiction_and_corpus_node(COUNTRY, JURISDICTION, CORPUS)
    scrape_all_chapters(corpus_node)


# ---------------------------------------------------------------------------
# Resume helpers (chapter-level, mirrors DE titles_done pattern)
# ---------------------------------------------------------------------------


def _chapters_done_path():
    from vaquill_pipeline.config import SETTINGS
    return SETTINGS.chunks_dir / "state_ne_chapters_done.txt"


def _load_chapters_done() -> set[str]:
    path = _chapters_done_path()
    if not path.exists():
        return set()
    return {l.strip() for l in path.read_text().splitlines() if l.strip()}


def _mark_chapter_done(number: str) -> None:
    path = _chapters_done_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a") as fh:
        fh.write(f"{number}\n")
        fh.flush()


# ---------------------------------------------------------------------------
# Chapter level
# ---------------------------------------------------------------------------


def scrape_all_chapters(corpus_node: Node) -> None:
    """Fetch the TOC page and iterate over every chapter link in parallel."""
    soup = get_url_as_soup(TOC_URL)

    # Chapter links look like: /laws/browse-chapters.php?chapter=1 OR ?chapter=76A
    # NE has alpha-suffixed chapters (e.g. 76A) -- widen the char class.
    chapter_pattern = re.compile(r"/laws/browse-chapters\.php\?chapter=([\w\-]+)$")

    chapters_done = (
        set() if os.environ.get("VAQUILL_FORCE_RESCRAPE") else _load_chapters_done()
    )
    if chapters_done:
        print(
            f"[scrapeNE] resume: {len(chapters_done)} chapters already done",
            flush=True,
        )

    seen: set[str] = set()
    work: list[Node] = []
    for link_tag in soup.find_all("a", href=True):
        href = link_tag["href"].strip()
        m = chapter_pattern.search(href)
        if not m:
            continue
        chapter_num = m.group(1)
        if chapter_num in seen:
            continue
        seen.add(chapter_num)

        node_name = _clean(link_tag.get_text())
        if not node_name:
            node_name = f"Chapter {chapter_num}"

        chapter_link = BASE_URL + href if href.startswith("/") else href
        node_id = f"{corpus_node.node_id}/chapter={chapter_num}"

        chapter_node = Node(
            id=node_id,
            link=chapter_link,
            top_level_title=chapter_num,
            node_type="structure",
            level_classifier="chapter",
            number=chapter_num,
            node_name=node_name,
            parent=corpus_node.node_id,
        )
        insert_node(chapter_node, TABLE_NAME, ignore_duplicate=True, debug_mode=False)
        if chapter_num in chapters_done:
            continue
        work.append(chapter_node)

    workers = int(os.environ.get("VAQUILL_TITLE_WORKERS", "8"))
    print(
        f"[scrapeNE] running {len(work)} chapters with {workers} parallel workers",
        flush=True,
    )

    def _do_chapter(chap_node: Node):
        try:
            scrape_chapter(chap_node)
            _mark_chapter_done(str(chap_node.number))
            return (chap_node.number, "ok", None)
        except Exception as e:
            return (chap_node.number, "fail", str(e)[:200])

    with ThreadPoolExecutor(max_workers=workers) as ex:
        for fut in as_completed(ex.submit(_do_chapter, c) for c in work):
            num, status, err = fut.result()
            if status == "fail":
                print(f"[scrapeNE] chapter {num}: {status}: {err}", flush=True)
            else:
                print(f"[scrapeNE] chapter {num}: {status}", flush=True)


# ---------------------------------------------------------------------------
# Section level
# ---------------------------------------------------------------------------


_ARTICLE_HEADING_RE = re.compile(r"^\s*ARTICLE\s+([\w\-]+)\b[\.\s\-:]*(.*)$", re.IGNORECASE)


def _maybe_article_node(text: str, chapter_node: Node) -> Node | None:
    """If `text` looks like an Article heading, return an Article structure node."""
    m = _ARTICLE_HEADING_RE.match(text)
    if not m:
        return None
    article_num = m.group(1)
    title = _clean(m.group(2) or "")
    node_name = f"Article {article_num}" + (f" - {title}" if title else "")
    node_id = f"{chapter_node.node_id}/article={article_num}"
    return Node(
        id=node_id,
        link=chapter_node.link,
        top_level_title=chapter_node.top_level_title,
        node_type="structure",
        level_classifier="article",
        number=article_num,
        node_name=node_name,
        parent=chapter_node.node_id,
    )


def scrape_chapter(chapter_node: Node) -> None:
    """Fetch a chapter page and insert each section.

    Also detects intervening "ARTICLE N" headings (text nodes between section
    links) and emits Article structure nodes so the hierarchy reflects NE's
    chapter -> article -> section organization where present.
    """
    soup = get_url_as_soup(str(chapter_node.link))

    # Section links look like: /laws/statutes.php?statute=1-101 or 25-2740.04
    # or 169A.20 -- the section id can contain dots, so widen from `\w` (which
    # excludes `.`) to `[\w.\-]+`. The previous `\w` regex silently dropped
    # every dotted-section in NE.
    section_pattern = re.compile(r"/laws/statutes\.php\?statute=([\w.\-]+)$")

    # Walk the chapter body in document order. Article headings (e.g. "ARTICLE 1
    # - General Provisions") promote current_parent so subsequent sections
    # attach beneath the article, not the chapter. Anchors that aren't section
    # links are ignored.
    current_parent: Node = chapter_node
    seen_articles: set[str] = set()
    seen_sections: set[str] = set()

    for element in soup.find_all(
        ["a", "h1", "h2", "h3", "h4", "h5", "h6", "strong", "b", "p", "li"]
    ):
        if element.name != "a":
            text = _clean(element.get_text(" "))
            if not text:
                continue
            art = _maybe_article_node(text, chapter_node)
            if art is not None and str(art.number) not in seen_articles:
                seen_articles.add(str(art.number))
                insert_node(art, TABLE_NAME, ignore_duplicate=True, debug_mode=False)
                current_parent = art
            continue

        link_tag = element
        href = (link_tag.get("href") or "").strip()
        m = section_pattern.search(href)
        if not m:
            continue
        statute_id = m.group(1)
        if statute_id in seen_sections:
            continue
        seen_sections.add(statute_id)

        node_name = _clean(link_tag.get_text())
        section_link = BASE_URL + href if href.startswith("/") else href
        node_id = f"{current_parent.node_id}/section={statute_id}"
        citation = f"Neb. Rev. Stat. § {statute_id}"

        status = _reserved_status(node_name)

        if status:
            section_node = Node(
                id=node_id,
                link=section_link,
                top_level_title=chapter_node.top_level_title,
                node_type="content",
                level_classifier="section",
                number=statute_id,
                node_name=node_name,
                parent=current_parent.node_id,
                citation=citation,
                status=status,
            )
            insert_node(section_node, TABLE_NAME, ignore_duplicate=True, debug_mode=False)
            continue

        node_text, addendum, body_status = _fetch_section_content(section_link)
        # Body-based status detection catches stub bodies like "Repealed. Laws ..."
        # that don't surface in TOC anchor text.
        final_status = body_status or None

        section_node = Node(
            id=node_id,
            link=section_link,
            top_level_title=chapter_node.top_level_title,
            node_type="content",
            level_classifier="section",
            number=statute_id,
            node_name=node_name,
            parent=current_parent.node_id,
            citation=citation,
            status=final_status,
            node_text=node_text,
            addendum=addendum,
        )
        insert_node(section_node, TABLE_NAME, ignore_duplicate=True, debug_mode=False)


# ---------------------------------------------------------------------------
# Section content
# ---------------------------------------------------------------------------


# Candidate selectors for the section body wrapper. We try them in order
# and fall back to a broad container if none match. Note: live DOM inspection
# of nebraskalegislature.gov was attempted (timed out from this network);
# selectors below cover the variants observed historically and the wrapper
# id/class names rendered by the site's Bootstrap card layout.
_SECTION_BODY_SELECTORS: list[tuple[str, dict]] = [
    ("div", {"id": "statute_text"}),
    ("div", {"id": "statuteText"}),
    ("div", {"class_": "statute-body"}),
    ("div", {"class_": "statute_body"}),
    ("div", {"class_": "card-body"}),
    ("div", {"class_": "statute"}),
    ("div", {"class_": re.compile(r"statute|StatuteBody|section-body", re.I)}),
]


def _find_section_body(soup):
    for name, kwargs in _SECTION_BODY_SELECTORS:
        node = soup.find(name, **kwargs)
        if node is not None:
            return node, f"{name}[{kwargs}]"
    # Defensive fallback: main content region. Returns a tuple so caller can
    # log which selector hit (or that the fallback was used).
    node = (
        soup.find("main")
        or soup.find("div", id=re.compile(r"content|main", re.I))
        or soup.find("div", role="main")
    )
    if node is not None:
        return node, "fallback:main/content"
    return None, None


def _detect_body_status(text: str) -> str | None:
    """Detect repealed/transferred/etc status from the body text itself."""
    if not text:
        return None
    # Only match status markers near the start of the body to avoid false
    # positives from history notes mentioning "Repealed by Laws ..." for a
    # later amendment.
    snippet = text[:200]
    for pat, label in BODY_STATUS_PATTERNS:
        if pat.search(snippet):
            return label
    return None


def _fetch_section_content(url: str):
    """Fetch a single section page; return (NodeText|None, Addendum|None, body_status|None)."""
    soup = get_url_as_soup(url)

    statute_div, matched_selector = _find_section_body(soup)
    if statute_div is None:
        logger.warning("[scrapeNE] no section body container matched for %s", url)
        return None, None, None

    node_text = NodeText()
    history_parts: list[str] = []
    body_text_buf: list[str] = []

    for element in statute_div.find_all(["p", "div", "li"], recursive=True):
        cls_list = element.get("class") or []
        cls_str = " ".join(cls_list).lower()

        # History / source notes: NE renders these inside an element flagged
        # with "history", "source", "annotation", or an icon list.
        if any(k in cls_str for k in ("history", "source", "fa-ul", "annotation")):
            text = _clean(element.get_text(separator=" "))
            if text:
                history_parts.append(text)
            continue

        # Skip heading / card-header rows that just echo the section number.
        if any(k in cls_str for k in ("heading", "section-head", "card-header", "statute-head")):
            continue

        if element.name in ("p", "li"):
            text = _clean(element.get_text(separator=" "))
            if not text:
                continue
            node_text.add_paragraph(text=text)
            body_text_buf.append(text)

    # If selectors missed and we landed on the broad fallback, paragraphs may
    # include page chrome. Warn so we notice in logs without dropping data
    # silently.
    if matched_selector and matched_selector.startswith("fallback") and not node_text.paragraphs:
        logger.warning(
            "[scrapeNE] fallback container had no <p>/<li> body text for %s", url
        )

    addendum: Addendum | None = None
    if history_parts:
        addendum = Addendum()
        addendum.history = AddendumType(type="history", text=" ".join(history_parts))

    body_status = _detect_body_status(" ".join(body_text_buf))

    if not node_text.paragraphs:
        return None, addendum, body_status

    return node_text, addendum, body_status


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _clean(raw: str) -> str:
    """Normalise whitespace and strip non-breaking spaces."""
    text = raw.replace("\xa0", " ").replace(" ", " ").replace(" ", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _reserved_status(text: str) -> str | None:
    if RESERVED_PATTERNS.search(text):
        return "reserved"
    return None


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    main()
