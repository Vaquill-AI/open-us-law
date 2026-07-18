"""North Carolina General Statutes (NCGS) scraper.

Source: https://codes.findlaw.com/nc/  (FindLaw NC mirror)
Hierarchy: corpus -> chapter -> section
Citation: "N.C. Gen. Stat. § <chapter>-<section_suffix>"

Source migration history:
  - Originally scraped from law.onecle.com/north-carolina/ which exposed an
    Article-level hierarchy in static HTML. Onecle started returning HTTP 403
    on 2026-05-11 (Cloudflare / bot challenge), so it is no longer usable.
  - ncleg.gov (official) and law.justia.com both block the vaquill_pipeline
    HTTP client with 403 as well.
  - FindLaw (codes.findlaw.com/nc/) serves the same statute corpus and is
    accessible. Its chapter TOC pages render Subchapter/Article via JS
    (the static HTML only carries an opaque `tid` list), so we cannot
    reconstruct the Article level from FindLaw alone. Sections are
    enumerated cheaply via FindLaw's sitemap, which gives us a flat
    chapter-keyed list of every section URL. We therefore drop the Article
    structural layer and parent sections directly under the Chapter node.

FindLaw URL structure:
  TOC:      https://codes.findlaw.com/nc/
  Chapter:  https://codes.findlaw.com/nc/chapter-<N>-<slug>/
  Section:  https://codes.findlaw.com/nc/chapter-<N>-<slug>/nc-gen-st-sect-<chap>-<sec>/

Section enumeration uses the public sitemap index at
https://codes.findlaw.com/sitemap_index.xml -> /sitemapcodes/v3/nc/sitemapN.xml.
The sitemap lists every section URL in a single flat namespace, so we group
by chapter slug and dispatch chapters to a ThreadPoolExecutor.
"""
from __future__ import annotations

import os
import re
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Optional, Tuple

# ---------------------------------------------------------------------------
# Project-root bootstrap (mirrors ME/ND/DE pattern)
# ---------------------------------------------------------------------------
current_file = Path(__file__).resolve()
src_directory = current_file.parent
while src_directory.name != "src" and src_directory.parent != src_directory:
    src_directory = src_directory.parent
project_root = src_directory.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

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
JURISDICTION = "nc"
CORPUS = "statutes"
TABLE_NAME = f"{COUNTRY}_{JURISDICTION}_{CORPUS}"

BASE_URL = "https://codes.findlaw.com"
TOC_URL = f"{BASE_URL}/nc/"
SITEMAP_INDEX_URL = f"{BASE_URL}/sitemap_index.xml"

RESERVED_KEYWORDS = ["repealed", "transferred", "recodified", "expired", "reserved"]

# Chapter href, e.g. /nc/chapter-1-civil-procedure/ or /nc/chapter-15a-criminal-procedure-act/
_CHAPTER_HREF_RE = re.compile(
    r"^https?://codes\.findlaw\.com/nc/chapter-([\w\d]+)-([\w\d\-]+?)/?$",
    re.IGNORECASE,
)
# Section URL, e.g. .../chapter-1-civil-procedure/nc-gen-st-sect-1-1/ or
# .../nc-gen-st-sect-105-449-105b/. The last path segment also carries the
# section number canonically. We re-derive the section number from the
# segment rather than the chapter slug for fidelity.
_SECTION_HREF_RE = re.compile(
    r"^https?://codes\.findlaw\.com/nc/chapter-([\w\d]+)-[\w\d\-]+/nc-gen-st-sect-([\w\d\-]+?)/?$",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Resume bookkeeping (chapter-level)
# ---------------------------------------------------------------------------

def _chapters_done_path() -> Path:
    try:
        from vaquill_pipeline.config import SETTINGS  # type: ignore
        return SETTINGS.chunks_dir / "state_nc_chapters_done.txt"
    except Exception:
        return Path(__file__).parent / "state_nc_chapters_done.txt"


def _load_chapters_done() -> set[str]:
    path = _chapters_done_path()
    if not path.exists():
        return set()
    return {ln.strip() for ln in path.read_text().splitlines() if ln.strip()}


def _mark_chapter_done(number: str) -> None:
    path = _chapters_done_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a") as fh:
        fh.write(f"{number}\n")
        fh.flush()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    # Install vaquill_pipeline patch (JsonlSink, r2_sync, etc.) so emitted
    # nodes flow into the same chunk pipeline as other US state scrapers.
    try:
        from vaquill_pipeline import patch as vq_patch
        vq_patch.install()
    except Exception as e:
        print(f"[scrapeNC] vaquill_pipeline.patch unavailable: {e}", flush=True)

    corpus_node: Node = insert_jurisdiction_and_corpus_node(COUNTRY, JURISDICTION, CORPUS)
    _scrape_all_chapters(corpus_node)


# ---------------------------------------------------------------------------
# Chapter discovery + dispatch
# ---------------------------------------------------------------------------

def _scrape_all_chapters(corpus_node: Node) -> None:
    """Discover chapters from the NC TOC, group sitemap section URLs by
    chapter, then dispatch chapters to a ThreadPoolExecutor.

    Resume: chapters already in ``state_nc_chapters_done.txt`` are skipped.
    Override with ``VAQUILL_FORCE_RESCRAPE=1``. Concurrency is set by
    ``VAQUILL_TITLE_WORKERS`` (default 8), matching the DE/AK scrapers.
    """
    chapters_done = (
        set()
        if os.environ.get("VAQUILL_FORCE_RESCRAPE")
        else _load_chapters_done()
    )
    if chapters_done:
        print(
            f"[scrapeNC] resume: {len(chapters_done)} chapters already done",
            flush=True,
        )

    # 1. Chapter list (TOC links).
    chapters = _discover_chapters()
    print(f"[scrapeNC] discovered {len(chapters)} chapters from TOC", flush=True)

    # 2. Section enumeration via sitemap (one bulk pass; cheaper than crawling
    #    each chapter page's JS tree). Group by chapter number.
    sections_by_chapter = _enumerate_sections_via_sitemap()
    print(
        f"[scrapeNC] sitemap yielded sections for "
        f"{len(sections_by_chapter)} chapters "
        f"(total {sum(len(v) for v in sections_by_chapter.values())} sections)",
        flush=True,
    )

    # 3. Build per-chapter work, inserting the chapter structure node up front.
    work: List[Tuple[Node, List[Tuple[str, str]]]] = []
    for chapter_number, chapter_slug, chapter_name in chapters:
        node_id = f"{corpus_node.node_id}/chapter={chapter_number}"
        chapter_url = f"{BASE_URL}/nc/chapter-{chapter_number.lower()}-{chapter_slug}/"
        status = _check_reserved(chapter_name)

        chapter_node = Node(
            id=node_id,
            link=chapter_url,
            top_level_title=chapter_number,
            node_type="structure",
            level_classifier="chapter",
            number=chapter_number,
            node_name=chapter_name,
            parent=corpus_node.node_id,
            status=status,
        )
        insert_node(chapter_node, TABLE_NAME, ignore_duplicate=True, debug_mode=False)

        if status:
            continue
        if chapter_number in chapters_done:
            continue

        # Lowercase chapter key matches sitemap-derived keys.
        sections = sections_by_chapter.get(chapter_number.lower(), [])
        if not sections:
            # No sections in sitemap for this chapter (e.g. fully repealed).
            _mark_chapter_done(chapter_number)
            continue
        work.append((chapter_node, sections))

    def _do_chapter(item: Tuple[Node, List[Tuple[str, str]]]) -> Tuple[str, str, Optional[str]]:
        chapter_node, sections = item
        try:
            _scrape_chapter(chapter_node, sections)
            _mark_chapter_done(chapter_node.number)
            return (chapter_node.number, "ok", None)
        except Exception as exc:
            return (chapter_node.number, "fail", str(exc)[:200])

    workers = int(os.environ.get("VAQUILL_TITLE_WORKERS", "8"))
    print(
        f"[scrapeNC] running {len(work)} chapters with {workers} parallel workers",
        flush=True,
    )
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [ex.submit(_do_chapter, item) for item in work]
        for fut in as_completed(futures):
            num, status, err = fut.result()
            if status == "fail":
                print(f"[scrapeNC] chapter {num}: {status}: {err}", flush=True)
            else:
                print(f"[scrapeNC] chapter {num}: {status}", flush=True)


def _discover_chapters() -> List[Tuple[str, str, str]]:
    """Return [(chapter_number, slug_tail, chapter_name)] for every NC chapter.

    Chapter number is canonicalized to uppercase letter suffix
    (e.g. "1", "1A", "15A", "143B"). ``slug_tail`` is the URL slug minus the
    leading "chapter-<number>-" prefix, used to reconstruct chapter URLs.
    """
    soup = get_url_as_soup(TOC_URL)
    found: list[tuple[str, str, str]] = []
    seen: set[str] = set()
    for a in soup.find_all("a", href=True):
        href: str = a["href"].strip()
        m = _CHAPTER_HREF_RE.match(href)
        if not m:
            continue
        number_raw = m.group(1)
        slug_tail = m.group(2)
        # Uppercase trailing letters: "15a" -> "15A", "143b" -> "143B".
        number = re.sub(
            r"([a-z]+)$", lambda x: x.group(1).upper(), number_raw
        )
        name = _clean_text(a.get_text())
        if not name:
            continue
        if number in seen:
            continue
        seen.add(number)
        found.append((number, slug_tail, name))
    return found


def _enumerate_sections_via_sitemap() -> dict[str, list[tuple[str, str]]]:
    """Walk the FindLaw NC sitemaps and return a mapping of
    lowercase-chapter-number -> [(section_number, section_url), ...].

    There are ~6 NC sitemap shards. We fetch them sequentially (small, fast)
    and filter to section URLs only. Section number is taken from the URL
    segment, NOT reconstructed from the chapter slug, so we preserve the
    full granularity (e.g. "1-440-19", "105-449.105B").
    """
    import re as _re
    grouped: dict[str, list[tuple[str, str]]] = defaultdict(list)

    try:
        index_soup = get_url_as_soup(SITEMAP_INDEX_URL)
    except Exception as exc:
        print(f"[scrapeNC] sitemap index fetch failed: {exc}", flush=True)
        return {}

    # Sitemap index is XML; bs4 wraps <loc> tags identically.
    locs = [el.get_text(strip=True) for el in index_soup.find_all("loc")]
    nc_maps = [u for u in locs if "/nc/sitemap" in u]
    if not nc_maps:
        print("[scrapeNC] no NC sitemaps found in index", flush=True)
        return {}

    seen_urls: set[str] = set()
    for sm_url in nc_maps:
        try:
            sm_soup = get_url_as_soup(sm_url)
        except Exception as exc:
            print(f"[scrapeNC] sitemap fetch failed: {sm_url}: {exc}", flush=True)
            continue
        for el in sm_soup.find_all("loc"):
            url = el.get_text(strip=True)
            if url in seen_urls:
                continue
            seen_urls.add(url)
            m = _SECTION_HREF_RE.match(url)
            if not m:
                continue
            chapter_lower = m.group(1).lower()
            section_seg = m.group(2)  # e.g. "1-1", "105-449-105b"
            # FindLaw URL slugs use hyphens between every numeric/letter
            # boundary. Canonical citation form uses "<chapter>-<rest>"
            # where ``rest`` may contain dots, hyphens, or letter suffixes.
            # We restore dots where the segment had ".N" originally by
            # leaving the raw URL form intact; downstream consumers can
            # normalize. For now, store the URL form as section_number.
            grouped[chapter_lower].append((section_seg, url))
    return grouped


# ---------------------------------------------------------------------------
# Per-chapter section scraping
# ---------------------------------------------------------------------------

def _scrape_chapter(chapter_node: Node, sections: List[Tuple[str, str]]) -> None:
    """Walk every section URL for a chapter and emit a section node.

    Sections parent directly under the chapter (Article-level structure is
    not exposed by FindLaw's static HTML; recovering it would require a
    JS-rendered crawl).
    """
    seen: set[str] = set()
    for section_segment, section_url in sections:
        if section_segment in seen:
            continue
        seen.add(section_segment)
        try:
            _emit_section(
                section_segment=section_segment,
                section_url=section_url,
                chapter_node=chapter_node,
            )
        except Exception as exc:
            # Don't let one bad section kill the whole chapter.
            print(
                f"[scrapeNC] section {section_segment} failed: {exc}",
                flush=True,
            )


def _emit_section(
    section_segment: str,
    section_url: str,
    chapter_node: Node,
) -> None:
    """Build and insert one section node from a FindLaw section page."""
    # Canonical section number: drop the chapter prefix, then rejoin. The
    # URL segment for §1-440.19 is "1-440-19" - FindLaw flattens dots to
    # hyphens. We surface BOTH forms: the URL form is the node ``number``
    # (uniquely keys this node), and the citation uses the same form for
    # consistency. Consumers that need dotted form can normalize.
    chap_lower = chapter_node.number.lower()
    if section_segment.lower().startswith(chap_lower + "-"):
        sec_suffix = section_segment[len(chap_lower) + 1 :]
    else:
        sec_suffix = section_segment
    canonical_section = f"{chapter_node.number}-{sec_suffix}".upper() if any(
        c.isalpha() for c in section_segment
    ) else f"{chapter_node.number}-{sec_suffix}"

    citation = f"N.C. Gen. Stat. § {canonical_section}"
    node_id = f"{chapter_node.node_id}/section={canonical_section}"

    node_text, addendum, node_name, body_status = _fetch_section_content(
        section_url, citation
    )

    section_node = Node(
        id=node_id,
        link=section_url,
        top_level_title=chapter_node.top_level_title,
        node_type="content",
        level_classifier="section",
        number=canonical_section,
        node_name=node_name or citation,
        parent=chapter_node.node_id,
        citation=citation,
        node_text=node_text,
        addendum=addendum,
        status=body_status,
    )
    insert_node(section_node, TABLE_NAME, ignore_duplicate=True, debug_mode=False)


def _fetch_section_content(
    url: str,
    citation: str,
) -> tuple[Optional[NodeText], Optional[Addendum], Optional[str], Optional[str]]:
    """
    Fetch a single FindLaw section page and return
    (NodeText, Addendum, node_name, status).

    FindLaw section page structure:
      <main>
        <h1>North Carolina General Statutes Chapter 1. Civil Procedure § 1-1. Remedies</h1>
        ...
        <p>Current as of January 01, 2023 | Updated by Findlaw Staff</p>
        <p>Body paragraph 1...</p>
        <p>(1) ...</p>
        <p>Cite this article: ...</p>      (skip)
      </main>
    """
    try:
        soup = get_url_as_soup(url)
    except Exception as exc:
        print(f"[scrapeNC] fetch failed {url}: {exc}", flush=True)
        return None, None, None, None

    main = soup.find("main") or soup.find(id="main-content")
    if main is None:
        return None, None, None, None

    # Node name from H1. The H1 carries the full citation header; we extract
    # the trailing section-name portion after "§ <num>.".
    h1 = main.find("h1")
    node_name: Optional[str] = None
    if h1:
        h1_text = _clean_text(h1.get_text())
        # "...Chapter 1. Civil Procedure § 1-1. Remedies" -> "§ 1-1. Remedies"
        m = re.search(r"(§\s*[\w\.\-]+\.?\s*.*)$", h1_text)
        if m:
            node_name = m.group(1).strip()
        else:
            node_name = h1_text

    # Reserved/repealed detection from H1 + first body paragraph.
    status: Optional[str] = _check_reserved(node_name or "")

    # Body paragraphs: take all <p> inside main, skip nav/cite/last-update
    # noise.
    node_text = NodeText()
    history_parts: list[str] = []
    for p_tag in main.find_all("p"):
        raw = p_tag.get_text(separator=" ")
        text = _clean_text(raw)
        if not text:
            continue
        low = text.lower()

        # Skip well-known FindLaw chrome paragraphs.
        if low.startswith("cite this article"):
            continue
        if low.startswith("read this complete document"):
            continue
        if low.startswith("findlaw codes may not reflect"):
            continue
        # "Current as of January 01, 2023 | Updated by Findlaw Staff" - this
        # acts as a publication-date marker; record it as history rather
        # than dropping silently so downstream tooling can surface vintage.
        if low.startswith("current as of") or low.startswith("updated by"):
            history_parts.append(text)
            continue
        # Status detection from body if H1 was uninformative.
        if status is None:
            for kw in RESERVED_KEYWORDS:
                if kw in low:
                    status = "reserved"
                    break

        node_text.add_paragraph(text=text)

    addendum: Optional[Addendum] = None
    if history_parts:
        addendum = Addendum()
        addendum.history = AddendumType(
            type="history", text=" ".join(history_parts)
        )

    if not node_text.paragraphs and addendum is None:
        return None, None, node_name, status

    return (
        node_text if node_text.paragraphs else None,
        addendum,
        node_name,
        status,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _check_reserved(text: str) -> Optional[str]:
    """Return 'reserved' if text contains a reserved/repealed keyword."""
    lower = (text or "").lower()
    for kw in RESERVED_KEYWORDS:
        if kw in lower:
            return "reserved"
    return None


def _clean_text(raw: str) -> str:
    """Normalize whitespace and remove non-breaking spaces."""
    if not raw:
        return ""
    text = raw.replace("\xa0", " ").replace(" ", " ").replace(" ", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


# ---------------------------------------------------------------------------
# CLI entry
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    main()
