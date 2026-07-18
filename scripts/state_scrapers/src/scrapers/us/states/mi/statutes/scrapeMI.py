"""Michigan Compiled Laws (MCL) scraper.

Real site structure (verified 2026-05-11 via Webshare US proxy):

  Chapter Index:  https://www.legislature.mi.gov/Laws/ChapterIndex
    -> Chapter page: /Home/GetObject?objectName=mcl-chapN
         -> Act page: /Laws/MCL?objectName=mcl-Act-NNN-of-YYYY
              -> Section page: /Laws/MCL?objectName=mcl-2-1

The site is an ASP.NET Core Razor app.  All navigation uses stateless
?objectName= query parameters -- no session tokens needed.

OLD BUGS (why the scraper hung for 180s+):
  1. Chapter index URL used legacy "mileg.aspx?page=ChapterIndex" -- that URL
     still redirects and loads, but the old ID selectors
     (frg_chapterindex_ChapterList_Results, frg_getmcldocument_*) no longer
     exist on any page.  With the fallback to soup, every link matched and the
     scraper tried to fetch hundreds of unrelated hrefs endlessly.
  2. The three-level hierarchy (Chapter -> Act -> Section) was collapsed to two
     levels.  Acts like "Act 78 of 1945" were invisible; the scraper never
     found the section table rows.
  3. Old section URL form was "objectname=mcl-750-316" (lowercase, mileg.aspx).
     New form is "objectName=mcl-750-316" under /Laws/MCL -- different path.
  4. Section content selectors (frg_getmcldocument_MclContent / MclChildren)
     do not exist.  Content lives in <div class="sectionWrapper"> with history
     in <div class="editorials">.

HIERARCHY:
  us/mi/statutes/chapter=N/act=<act_slug>/section=S

CITATION: "Mich. Comp. Laws § <S>"  (e.g. "Mich. Comp. Laws § 2.1")

CHAPTER INDEX URL (canonical, stateless):
  https://www.legislature.mi.gov/Laws/ChapterIndex

ACT SLUG: the objectName query value with the leading "mcl-" stripped.
  e.g. objectName=mcl-Act-78-of-1945  ->  act_slug = "Act-78-of-1945"

SESSION TOKENS: none needed.  /Laws/MCL?objectName=... is stateless.
"""
from __future__ import annotations

import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional, Tuple

# ---------------------------------------------------------------------------
# Project-root bootstrap (mirrors ME/VT pattern)
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
JURISDICTION = "mi"
CORPUS = "statutes"
TABLE_NAME = f"{COUNTRY}_{JURISDICTION}_{CORPUS}"

BASE_URL = "https://www.legislature.mi.gov"
CHAPTER_INDEX_URL = f"{BASE_URL}/Laws/ChapterIndex"
LAWS_MCL_URL = f"{BASE_URL}/Laws/MCL?objectName="
GET_OBJECT_URL = f"{BASE_URL}/Home/GetObject?objectName="

RESERVED_KEYWORDS = [
    "repealed",
    "reserved",
    "expired",
    "renumbered",
    "transferred",
]


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    corpus_node: Node = insert_jurisdiction_and_corpus_node(COUNTRY, JURISDICTION, CORPUS)
    _scrape_all_chapters(corpus_node)


# ---------------------------------------------------------------------------
# Chapter level
# ---------------------------------------------------------------------------

def _scrape_all_chapters(corpus_node: Node) -> None:
    """Fetch the MCL chapter index and dispatch each chapter in parallel.

    Real chapter links:  /Home/GetObject?objectName=mcl-chapN
    The page also has nav links with "chap" in them (Previous/Next Chapter) --
    we filter strictly on the mcl-chap prefix.

    Chapter is the outermost independent level: each chapter is a self-contained
    subtree (chapter page -> acts -> sections) that shares no state with its
    siblings, so chapters fan out to a ThreadPoolExecutor sized by
    VAQUILL_TITLE_WORKERS (default 8, matching scrapeWA/scrapeAK). The crawl is
    latency-bound at one request per section, so the pool is the whole win here.

    NOTE: MI deliberately has no chapters_done resume file -- every run
    re-crawls in full. That is what lets an amended section be re-fetched and
    re-chunked into a fresh content-addressed point_id. Do not add a
    chapters_done skip here to save time without replacing that freshness some
    other way -- it would make MI amendment-blind.
    """
    soup = get_url_as_soup(CHAPTER_INDEX_URL)
    main = soup.find(id="main") or soup

    work: list[Node] = []
    for a in main.find_all("a", href=True):
        href: str = a["href"].strip()

        # Only chapter TOC links: /Home/GetObject?objectName=mcl-chapN
        m = re.search(r"objectName=mcl-chap(\S+)", href, re.IGNORECASE)
        if not m:
            continue

        chapter_number = m.group(1).strip()
        node_name = _clean_text(a.get_text()) or f"Chapter {chapter_number}"

        chapter_url = _make_absolute(href)
        node_id = f"{corpus_node.node_id}/chapter={chapter_number}"
        status = _check_reserved(node_name)

        chapter_node = Node(
            id=node_id,
            link=chapter_url,
            top_level_title=chapter_number,
            node_type="structure",
            level_classifier="chapter",
            number=chapter_number,
            node_name=node_name,
            parent=corpus_node.node_id,
            status=status,
        )
        insert_node(chapter_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)

        if not status:
            work.append(chapter_node)

    def _do_chapter(chapter_node: Node) -> tuple[str, str, str | None]:
        # One chapter's failure must not abort the other workers, so each is
        # wrapped and reported; the run continues with the remaining chapters.
        try:
            _scrape_chapter(chapter_node)
            return (chapter_node.number, "ok", None)
        except Exception as exc:
            return (chapter_node.number, "fail", str(exc)[:200])

    workers = int(os.environ.get("VAQUILL_TITLE_WORKERS", "8"))
    print(
        f"[scrapeMI] running {len(work)} chapters with {workers} parallel workers",
        flush=True,
    )
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for fut in as_completed(ex.submit(_do_chapter, item) for item in work):
            num, status, err = fut.result()
            if status == "fail":
                print(f"[scrapeMI] chapter {num}: {status}: {err}", flush=True)
            else:
                print(f"[scrapeMI] chapter {num}: {status}", flush=True)


# ---------------------------------------------------------------------------
# Act level (intermediate between chapter and section)
# ---------------------------------------------------------------------------

def _scrape_chapter(chapter_node: Node) -> None:
    """Fetch a chapter page and iterate over its Act links.

    Real act links on chapter page:  /Laws/MCL?objectName=mcl-Act-NNN-of-YYYY
    The table has columns: Document | Type | Description
    Description carries the section range, e.g. "STATE AREA (2.1-2.2)".
    We skip entries that are not "Statute" type (J.R., E.R.O., H.C.R., etc.)
    when they have no section content -- though we still insert them as acts.
    """
    try:
        soup = get_url_as_soup(str(chapter_node.link))
    except Exception as exc:
        print(f"[WARN] Could not fetch chapter {chapter_node.node_id}: {exc}", flush=True)
        return

    main = soup.find(id="main") or soup
    table = main.find("table")
    if table is None:
        return

    for row in table.find_all("tr"):
        link_tag = row.find("a", href=True)
        if link_tag is None:
            continue

        href: str = link_tag["href"].strip()
        # Act links: /Laws/MCL?objectName=mcl-Act-NNN-of-YYYY
        # Exclude chapter nav links (objectName=mcl-chapN)
        act_m = re.search(r"objectName=(mcl-(?!chap)\S+)", href, re.IGNORECASE)
        if not act_m:
            continue

        object_name = act_m.group(1)  # e.g. "mcl-Act-78-of-1945"
        # Strip leading "mcl-" to get the slug used in node IDs
        act_slug = re.sub(r"^mcl-", "", object_name, flags=re.IGNORECASE)

        act_name = _clean_text(link_tag.get_text()) or act_slug
        act_url = _make_absolute(href)
        node_id = f"{chapter_node.node_id}/act={act_slug}"
        status = _check_reserved(act_name)

        act_node = Node(
            id=node_id,
            link=act_url,
            top_level_title=chapter_node.top_level_title,
            node_type="structure",
            level_classifier="act",
            number=act_slug,
            node_name=act_name,
            parent=chapter_node.node_id,
            status=status,
        )
        insert_node(act_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)

        if not status:
            _scrape_act(act_node)


# ---------------------------------------------------------------------------
# Section level
# ---------------------------------------------------------------------------

def _scrape_act(act_node: Node) -> None:
    """Fetch an Act page and iterate over its section links.

    Real section links:  /Laws/MCL?objectName=mcl-2-1
    Table columns: Document | Type | Description
    """
    try:
        soup = get_url_as_soup(str(act_node.link))
    except Exception as exc:
        print(f"[WARN] Could not fetch act {act_node.node_id}: {exc}", flush=True)
        return

    main = soup.find(id="main") or soup
    table = main.find("table")
    if table is None:
        return

    for row in table.find_all("tr"):
        link_tag = row.find("a", href=True)
        if link_tag is None:
            continue

        href: str = link_tag["href"].strip()
        # Section links: /Laws/MCL?objectName=mcl-2-1 or mcl-750-316
        # The objectName for sections looks like mcl-N-M (no "Act-", no "chap")
        sec_m = re.search(r"objectName=mcl-([\d]+)-([\w\.]+)$", href, re.IGNORECASE)
        if not sec_m:
            continue

        # "mcl-2-1" -> sec_number "2.1", "mcl-750-316a" -> "750.316a"
        sec_number = f"{sec_m.group(1)}.{sec_m.group(2)}"

        # Description is in the third <td> when present
        cells = row.find_all("td")
        description = _clean_text(cells[2].get_text()) if len(cells) > 2 else ""
        node_name = description or f"§ {sec_number}"

        sec_url = _make_absolute(href)
        node_id = f"{act_node.node_id}/section={sec_number}"
        status = _check_reserved(node_name)
        citation = f"Mich. Comp. Laws § {sec_number}"

        if status:
            section_node = Node(
                id=node_id,
                link=sec_url,
                top_level_title=act_node.top_level_title,
                node_type="content",
                level_classifier="section",
                number=sec_number,
                node_name=node_name,
                parent=act_node.node_id,
                citation=citation,
                status=status,
            )
            insert_node(section_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)
            continue

        node_text, addendum = _fetch_section_content(sec_url)

        section_node = Node(
            id=node_id,
            link=sec_url,
            top_level_title=act_node.top_level_title,
            node_type="content",
            level_classifier="section",
            number=sec_number,
            node_name=node_name,
            parent=act_node.node_id,
            citation=citation,
            node_text=node_text,
            addendum=addendum,
        )
        insert_node(section_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)


# ---------------------------------------------------------------------------
# Section content fetch
# ---------------------------------------------------------------------------

def _fetch_section_content(url: str) -> Tuple[Optional[NodeText], Optional[Addendum]]:
    """Fetch a single MCL section page and return (NodeText, Addendum).

    Real section page layout (inside <main id="main">):
        <div class="sectionWrapper">
            <div class="excerpt"> ... act heading ... </div>
            <h1 class="h4">2.1 Area of state; basis.</h1>
            <p class="margin8Px">Sec. 1.</p>
            <p>   Body text here ... </p>
            <div class="editorials margin8Px">
                <p><span ...>History:</span> 1945, Act 78 ... </p>
            </div>
        </div>
    """
    try:
        soup = get_url_as_soup(url)
    except Exception as exc:
        print(f"[WARN] Could not fetch section {url}: {exc}", flush=True)
        return None, None

    main = soup.find(id="main") or soup
    wrapper = main.find("div", class_="sectionWrapper")
    if wrapper is None:
        return None, None

    node_text = NodeText()
    history_parts: list[str] = []

    for elem in wrapper.find_all(recursive=False):
        tag = getattr(elem, "name", None)
        cls = " ".join(elem.get("class", []))

        # Skip excerpt/heading divs and h-level headings
        if "excerpt" in cls:
            continue
        if tag in ("h1", "h2", "h3", "h4", "h5"):
            continue

        # editorials div holds the history / compiler notes
        if "editorials" in cls:
            history_text = _clean_text(elem.get_text(separator=" "))
            if history_text:
                history_parts.append(history_text)
            continue

        # For <p> tags inside the wrapper (not nested in editorials)
        if tag == "p":
            text = _clean_text(elem.get_text(separator=" "))
            if text:
                node_text.add_paragraph(text=text)
            continue

        # Nested divs that are NOT sectionWrapper itself
        if tag == "div":
            for p in elem.find_all("p", recursive=False):
                text = _clean_text(p.get_text(separator=" "))
                if text:
                    node_text.add_paragraph(text=text)

    addendum: Optional[Addendum] = None
    if history_parts:
        addendum = Addendum()
        addendum.history = AddendumType(
            type="history", text=" ".join(history_parts)
        )

    if not node_text.paragraphs:
        node_text = None  # type: ignore[assignment]

    return node_text, addendum


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_absolute(href: str) -> str:
    """Convert a root-relative or absolute href to a full URL."""
    if href.startswith("http"):
        return href
    if href.startswith("/"):
        return BASE_URL + href
    return BASE_URL + "/" + href


def _check_reserved(text: str) -> Optional[str]:
    lower = text.lower()
    for kw in RESERVED_KEYWORDS:
        if kw in lower:
            return "reserved"
    return None


def _clean_text(raw: str) -> str:
    text = raw.replace("\xa0", " ").replace("—", "-").replace("–", "-")
    text = re.sub(r"\s+", " ", text).strip()
    return text


# ---------------------------------------------------------------------------
# CLI entry
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    main()
