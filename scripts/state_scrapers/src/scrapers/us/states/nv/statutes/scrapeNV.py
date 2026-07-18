"""Nevada Revised Statutes scraper.

Source: https://codes.findlaw.com/nv/ (FindLaw "codes" mirror).

The previously-used Onecle mirror (law.onecle.com/nevada/) and the official
Nevada Legislature site (www.leg.state.nv.us/NRS/) both now return HTTP 403
to our http_client (Cloudflare / WAF block). FindLaw's codes property is the
fresh, accessible mirror: it exposes every NRS section as its own
crawlable HTML page and publishes a sitemap that enumerates every section
URL, which lets us avoid any JS-rendered chapter TOC.

Hierarchy:  us/nv/statutes/title=<N>/chapter=<C>/section=<C.S>
Citation:   Nev. Rev. Stat. § <CHAPTER>.<SECTION>   (e.g. Nev. Rev. Stat. § 1.010)

Discovery flow
--------------
1. Pull https://codes.findlaw.com/sitemap_index.xml. Find every per-state NV
   shard (sitemapcodes/v4/nv/sitemapN.xml).
2. For each shard, pull every <loc> with the section URL pattern
       /nv/{title-slug}/nv-rev-st-{chapter}-{section}/
   where chapter is `\\d+[a-z]?` (alpha-suffix chapters like 41a, 116a, 118a
   are preserved) and section is `\\d+[a-z]?`.
3. Group every section URL by (title-slug, chapter) and dispatch each chapter
   to a ThreadPoolExecutor. Chapter-level resume is persisted to
   state_nv_chapters_done.txt under SETTINGS.chunks_dir (mirrors AK/DE).

Section page layout (codes.findlaw.com)
---------------------------------------
    <h1>Nevada Revised Statutes Title 1. ... § 1.010. Courts of justice</h1>
    <div class="codes-content section">
      <p>...body...</p>
      <div class="subsection"><p>1. ...</p></div>
      <div class="subsection"><p>2. ...</p></div>
      ...
      <div class="codes-controls">...</div>            <-- skip
      <div class="cite-this-article">...</div>         <-- skip
    </div>

Citation text "Cite this article: FindLaw.com ... last updated <DATE>" is
captured into addendum.history when present.
"""
from __future__ import annotations

import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional
from xml.etree import ElementTree as ET

from bs4 import BeautifulSoup

# --- sys.path bootstrap (mirrors all other scrapers in this repo) -----------
_current_file = Path(__file__).resolve()
_src_dir = _current_file.parent
while _src_dir.name != "src" and _src_dir.parent != _src_dir:
    _src_dir = _src_dir.parent
_project_root = _src_dir.parent
if str(_project_root) not in sys.path:
    sys.path.append(str(_project_root))
# ---------------------------------------------------------------------------

from src.utils.pydanticModels import Addendum, AddendumType, Node, NodeText
from src.utils.scrapingHelpers import (
    get_url_as_soup,
    insert_jurisdiction_and_corpus_node,
    insert_node,
)

COUNTRY = "us"
JURISDICTION = "nv"
CORPUS = "statutes"
TABLE_NAME = f"{COUNTRY}_{JURISDICTION}_{CORPUS}"

BASE_URL = "https://codes.findlaw.com"
SITEMAP_INDEX_URL = f"{BASE_URL}/sitemap_index.xml"

RESERVED_KEYWORDS = [
    "(reserved)",
    "(repealed)",
    "(expired)",
    "(renumbered)",
    "(deleted)",
    "reserved.",
    "repealed.",
    "[repealed",
    "[reserved",
    "[expired",
]

# FindLaw NV section URLs:
#   /nv/title-1-state-judicial-department/nv-rev-st-1-010/
#   /nv/title-13-guardianships-conservatorships-trusts/nv-rev-st-166a-340/
#   /nv/title-11-domestic-relations/nv-rev-st-125-450a/
# Chapter prefix may carry a trailing letter (a, b, c...) -- preserve it,
# normalized to UPPER for the canonical NRS chapter id (41A, 116A, 118A, ...).
_SECTION_URL_RE = re.compile(
    r"^https?://codes\.findlaw\.com/nv/(?P<title_slug>[a-z0-9\-]+)/"
    r"nv-rev-st-(?P<chapter>\d+[a-z]?)-(?P<section>\d+[a-z]?)/?$",
    re.IGNORECASE,
)

# NV sitemap shards live at /sitemapcodes/v4/nv/sitemap{N}.xml.
_NV_SITEMAP_RE = re.compile(
    r"^https?://codes\.findlaw\.com/sitemapcodes/v\d+/nv/sitemap\d+\.xml$",
    re.IGNORECASE,
)

# Pulls "title-1-state-judicial-department" -> ("1", "State Judicial Department")
_TITLE_SLUG_RE = re.compile(r"^title-(\d+)-(.+)$")


# ---------------------------------------------------------------------------
# Chapter-level resume bookkeeping (mirrors AK/DE)
# ---------------------------------------------------------------------------

def _chapters_done_path() -> Path:
    """Where we persist the set of {title}/{chapter} pairs already done."""
    try:
        from vaquill_pipeline.config import SETTINGS  # type: ignore
        return SETTINGS.chunks_dir / "state_nv_chapters_done.txt"
    except Exception:
        return Path(__file__).parent / "state_nv_chapters_done.txt"


def _load_chapters_done() -> set[str]:
    path = _chapters_done_path()
    if not path.exists():
        return set()
    return {l.strip() for l in path.read_text().splitlines() if l.strip()}


def _mark_chapter_done(key: str) -> None:
    path = _chapters_done_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a") as fh:
        fh.write(f"{key}\n")
        fh.flush()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    corpus_node: Node = insert_jurisdiction_and_corpus_node(COUNTRY, JURISDICTION, CORPUS)
    _scrape_all(corpus_node)


# ---------------------------------------------------------------------------
# Sitemap discovery
# ---------------------------------------------------------------------------

def _discover_nv_sitemap_shards() -> list[str]:
    """Return every NV sitemap shard URL from the FindLaw sitemap index."""
    soup = get_url_as_soup(SITEMAP_INDEX_URL)
    raw = str(soup)
    # ET handles XML namespaces cleanly. The page IS XML even though
    # get_url_as_soup parses as HTML; round-tripping via str() is enough
    # to pull the <loc> values out.
    locs = re.findall(r"<loc>\s*([^<\s]+)\s*</loc>", raw)
    return [loc for loc in locs if _NV_SITEMAP_RE.match(loc)]


def _discover_section_urls(shard_urls: list[str]) -> list[str]:
    """Pull every NV section URL across all shard sitemaps."""
    section_urls: list[str] = []
    for shard in shard_urls:
        try:
            soup = get_url_as_soup(shard)
        except Exception as exc:
            print(f"[scrapeNV] shard fetch failed {shard}: {exc!s}", flush=True)
            continue
        raw = str(soup)
        for loc in re.findall(r"<loc>\s*([^<\s]+)\s*</loc>", raw):
            if _SECTION_URL_RE.match(loc):
                section_urls.append(loc)
    # Deduplicate while preserving order (sitemaps occasionally repeat).
    seen: set[str] = set()
    deduped: list[str] = []
    for url in section_urls:
        if url not in seen:
            seen.add(url)
            deduped.append(url)
    return deduped


# ---------------------------------------------------------------------------
# Top-level driver
# ---------------------------------------------------------------------------

def _scrape_all(corpus_node: Node) -> None:
    shard_urls = _discover_nv_sitemap_shards()
    print(
        f"[scrapeNV] discovered {len(shard_urls)} NV sitemap shards",
        flush=True,
    )
    section_urls = _discover_section_urls(shard_urls)
    print(
        f"[scrapeNV] discovered {len(section_urls)} NV section URLs",
        flush=True,
    )

    # Group by (title_slug, chapter). title_slug carries title number + name
    # in its slug, so we can mint title nodes without fetching anything more.
    by_chapter: dict[tuple[str, str], list[tuple[str, str]]] = {}
    for url in section_urls:
        m = _SECTION_URL_RE.match(url)
        if not m:
            continue
        title_slug = m.group("title_slug").lower()
        chapter = m.group("chapter").upper()  # 41A, 116A, 118A, etc.
        section = m.group("section").upper()
        by_chapter.setdefault((title_slug, chapter), []).append((section, url))

    # Stably sort sections within each chapter.
    for sections in by_chapter.values():
        sections.sort(key=_section_sort_key)

    # Title nodes: one per distinct title_slug.
    title_nodes: dict[str, Node] = {}
    for title_slug, _chapter in by_chapter:
        if title_slug in title_nodes:
            continue
        title_nodes[title_slug] = _build_title_node(corpus_node, title_slug)

    force = bool(os.environ.get("VAQUILL_FORCE_RESCRAPE"))
    chapters_done: set[str] = set() if force else _load_chapters_done()

    work: list[tuple[Node, str, list[tuple[str, str]]]] = []
    for (title_slug, chapter), sections in by_chapter.items():
        key = f"{title_slug}/{chapter}"
        if key in chapters_done:
            continue
        work.append((title_nodes[title_slug], chapter, sections))

    # VAQUILL_NV_WORKERS stays the state-specific override, but fall back to the
    # fleet-wide VAQUILL_TITLE_WORKERS the refresh tasks actually set. Without
    # the fallback, setting TITLE_WORKERS across the fleet silently does nothing
    # here.
    workers = int(
        os.environ.get("VAQUILL_NV_WORKERS")
        or os.environ.get("VAQUILL_TITLE_WORKERS", "8")
    )
    print(
        f"[scrapeNV] running {len(work)} chapters with {workers} parallel workers "
        f"(resumed past {len(chapters_done)})",
        flush=True,
    )

    def _do_chapter(item: tuple[Node, str, list[tuple[str, str]]]) -> tuple[str, str, Optional[str]]:
        title_node, chapter, sections = item
        key = f"{title_node.number}/{chapter}"
        try:
            _scrape_chapter(title_node, chapter, sections)
            _mark_chapter_done(f"{_slug_from_title_node(title_node)}/{chapter}")
            return (key, "ok", None)
        except Exception as exc:
            return (key, "fail", str(exc)[:200])

    with ThreadPoolExecutor(max_workers=workers) as ex:
        for fut in as_completed(ex.submit(_do_chapter, w) for w in work):
            key, status, err = fut.result()
            if status == "fail":
                print(f"[scrapeNV] chapter {key}: {status}: {err}", flush=True)


# ---------------------------------------------------------------------------
# Title / chapter / section construction
# ---------------------------------------------------------------------------

def _build_title_node(corpus_node: Node, title_slug: str) -> Node:
    """Mint a title node from the slug ('title-1-state-judicial-department')."""
    m = _TITLE_SLUG_RE.match(title_slug)
    if m:
        title_num = m.group(1)
        title_name = _slug_to_title(m.group(2))
    else:
        # Non-numeric titles (e.g. "preliminary-chapter", "enabling-act",
        # "nevada-constitution"). Keep them under a stable id.
        title_num = title_slug
        title_name = _slug_to_title(title_slug)

    node_id = f"{corpus_node.node_id}/title={title_num}"
    title_node = Node(
        id=node_id,
        link=f"{BASE_URL}/nv/{title_slug}/",
        top_level_title=title_num,
        node_type="structure",
        level_classifier="title",
        number=title_num,
        node_name=f"Title {title_num} - {title_name}" if m else title_name,
        parent=corpus_node.node_id,
        status=None,
    )
    insert_node(title_node, TABLE_NAME, ignore_duplicate=True, debug_mode=False)
    return title_node


def _slug_from_title_node(title_node: Node) -> str:
    """Rebuild the URL slug from a title node for resume key consistency."""
    if title_node.link:
        m = re.search(r"/nv/([a-z0-9\-]+)/", title_node.link)
        if m:
            return m.group(1)
    return str(title_node.number)


def _scrape_chapter(
    title_node: Node,
    chapter: str,
    sections: list[tuple[str, str]],
) -> None:
    """Create the chapter node, then walk every section URL beneath it."""
    chapter_node_id = f"{title_node.node_id}/chapter={chapter}"
    chapter_node = Node(
        id=chapter_node_id,
        link=None,
        top_level_title=title_node.top_level_title,
        node_type="structure",
        level_classifier="chapter",
        number=chapter,
        node_name=f"Chapter {chapter}",
        parent=title_node.node_id,
        status=None,
    )
    insert_node(chapter_node, TABLE_NAME, ignore_duplicate=True, debug_mode=False)

    for section, url in sections:
        try:
            _scrape_section(chapter_node, chapter, section, url)
        except Exception as exc:
            print(
                f"[scrapeNV] section {chapter}.{section} ({url}) failed: {exc!s}",
                flush=True,
            )


def _scrape_section(
    chapter_node: Node,
    chapter: str,
    section: str,
    url: str,
) -> None:
    """Fetch one section page and insert a content node."""
    soup = get_url_as_soup(url)

    h1 = soup.find("h1")
    h1_text = _clean_text(h1.get_text(" ")) if h1 else ""
    node_name = _extract_section_name(h1_text, chapter, section)

    sec_number = f"{chapter}.{section}"
    citation = f"Nev. Rev. Stat. § {sec_number}"
    node_id = f"{chapter_node.node_id}/section={sec_number}"

    status = _check_reserved(h1_text)

    node_text, addendum = _parse_section_content(soup)

    # If body text says repealed/reserved, surface that as a node-level
    # status even when the heading didn't reveal it.
    if status is None and node_text is not None:
        joined = " ".join(p.text for p in node_text.paragraphs.values())[:500].lower()
        if _check_reserved(joined):
            status = "reserved"

    section_node = Node(
        id=node_id,
        link=url,
        top_level_title=chapter_node.top_level_title,
        node_type="content",
        level_classifier="section",
        number=sec_number,
        node_name=node_name,
        parent=chapter_node.node_id,
        citation=citation,
        node_text=node_text,
        addendum=addendum,
        status=status,
    )
    insert_node(section_node, TABLE_NAME, ignore_duplicate=True, debug_mode=False)


# ---------------------------------------------------------------------------
# Section body parsing
# ---------------------------------------------------------------------------

def _parse_section_content(
    soup: BeautifulSoup,
) -> tuple[Optional[NodeText], Optional[Addendum]]:
    """Pull statutory paragraphs and the "last updated" history note.

    div.codes-content.section holds the body:
        <p>...lead-in...</p>
        <div class="subsection"><p>1. ...</p></div>
        <div class="subsection"><p>2. ...</p></div>
        ...
        <div class="codes-controls">...</div>        -- skip
        <div class="cite-this-article">...</div>     -- skip
    """
    content = soup.select_one("div.codes-content")
    if content is None:
        return None, None

    node_text = NodeText()
    history_text = ""

    for child in content.find_all(recursive=False):
        classes = child.get("class") or []
        if any(c in classes for c in ("codes-controls", "ad-container")):
            continue
        if "cite-this-article" in classes:
            # "Cite this article: FindLaw.com - ... last updated MMM DD, YYYY | <url>"
            raw = _clean_text(child.get_text(" "))
            # Trim the trailing URL.
            raw = re.sub(r"\|\s*https?://\S+\s*$", "", raw).strip()
            if raw:
                history_text = raw
            continue
        if child.name == "p":
            text = _clean_text(child.get_text(" "))
            if text:
                node_text.add_paragraph(text=text)
        elif child.name == "div":
            text = _clean_text(child.get_text(" "))
            if text:
                node_text.add_paragraph(text=text)
        # else: skip <script>, <ins>, etc.

    addendum: Optional[Addendum] = None
    if history_text:
        addendum = Addendum()
        addendum.history = AddendumType(type="history", text=history_text)

    if not node_text.paragraphs:
        return None, addendum
    return node_text, addendum


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _section_sort_key(item: tuple[str, str]) -> tuple[int, str]:
    """Sort sections numerically when possible (e.g. 010 < 110 < 1031),
    falling back to lexical for alpha-suffix sections.
    """
    section, _url = item
    m = re.match(r"^(\d+)([A-Z]?)$", section)
    if m:
        return (int(m.group(1)), m.group(2))
    return (10**9, section)


def _extract_section_name(h1_text: str, chapter: str, section: str) -> str:
    """Pull the section name out of the FindLaw H1.

    H1 format:
        "Nevada Revised Statutes Title 1. State Judicial Department § 1.010. Courts of justice"

    We return everything after the "§ <chapter>.<section>." token. If the H1
    deviates (rare), fall back to the raw H1 text.
    """
    if not h1_text:
        return f"NRS {chapter}.{section}"
    # The chapter token in the H1 keeps original case (1, 41A, 116A...).
    pattern = re.compile(
        rf"§\s*{re.escape(chapter)}\.{re.escape(section)}\.?\s*(.*)$",
        re.IGNORECASE,
    )
    m = pattern.search(h1_text)
    if m and m.group(1).strip():
        return m.group(1).strip()
    return h1_text


def _slug_to_title(slug: str) -> str:
    """'state-judicial-department' -> 'State Judicial Department'."""
    return " ".join(word.capitalize() for word in slug.split("-") if word)


def _check_reserved(text: str) -> Optional[str]:
    lower = text.lower()
    for kw in RESERVED_KEYWORDS:
        if kw in lower:
            return "reserved"
    return None


def _clean_text(raw: str) -> str:
    if not raw:
        return ""
    text = raw.replace("\xa0", " ").replace(" ", " ").replace("–", "-")
    text = re.sub(r"\s+", " ", text).strip()
    return text


if __name__ == "__main__":
    main()
