"""Idaho Code scraper.

Source:   https://legislature.idaho.gov/statutesrules/idstat/
Hierarchy: title -> chapter -> section

TOC page lists all 74 titles. Each title page lists chapters. Each chapter
page lists sections as rows with the section number and a link to the
section page. Section text lives in a ``pgbrk`` div; the first 4 sub-divs
are breadcrumb headers (title name, title desc, chapter name, chapter desc)
and are skipped.

Reserved / repealed entries have no anchor link in the table row.
"""
from __future__ import annotations

import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Optional

from bs4 import BeautifulSoup, Tag

# Project-root bootstrap (mirrors the ME / DE canonical pattern).
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
JURISDICTION = "id"
CORPUS = "statutes"
TABLE_NAME = f"{COUNTRY}_{JURISDICTION}_{CORPUS}"

BASE_URL = "https://legislature.idaho.gov"
TOC_URL = f"{BASE_URL}/statutesrules/idstat/"

RESERVED_KEYWORDS = ["[repealed]", "[expired]", "[reserved]", "redesignated"]

# Number of leading divs inside .pgbrk that are breadcrumb headers to skip.
_HEADER_DIV_COUNT = 4


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    corpus_node: Node = insert_jurisdiction_and_corpus_node(COUNTRY, JURISDICTION, CORPUS)
    _scrape_toc(corpus_node)


# ---------------------------------------------------------------------------
# TOC level
# ---------------------------------------------------------------------------


def _titles_done_path():
    """Where we persist the set of titles already fully scraped for resume."""
    from vaquill_pipeline.config import SETTINGS
    return SETTINGS.chunks_dir / "state_id_titles_done.txt"


def _load_titles_done() -> set:
    path = _titles_done_path()
    if not path.exists():
        return set()
    return {ln.strip() for ln in path.read_text().splitlines() if ln.strip()}


def _mark_title_done(number: str) -> None:
    path = _titles_done_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a") as fh:
        fh.write(f"{number}\n")
        fh.flush()


def _scrape_toc(corpus_node: Node) -> None:
    """Parse the Idaho Code TOC and dispatch per title.

    Titles are independent of one another, so they run in a
    ``ThreadPoolExecutor`` (default 8 workers, override via
    ``VAQUILL_TITLE_WORKERS``). Title-level resume persists completed titles
    to ``state_id_titles_done.txt``. Set ``VAQUILL_FORCE_RESCRAPE=1`` to
    re-scrape everything.
    """
    soup = get_url_as_soup(TOC_URL)
    container = _main_container(soup)
    if container is None:
        raise RuntimeError(f"Could not find main content container on {TOC_URL}")

    titles_done = set() if os.environ.get("VAQUILL_FORCE_RESCRAPE") else _load_titles_done()
    if titles_done:
        print(
            f"[scrapeID] resume: {len(titles_done)} titles already done",
            flush=True,
        )

    work: List[Node] = []
    for row in container.find_all("tr"):
        tds = row.find_all("td")
        if len(tds) < 3:
            continue

        # td[0] contains the "TITLE N" text and the anchor.
        link_tag = tds[0].find("a", href=True)
        if link_tag is None:
            continue

        title_label = _clean(tds[0].get_text())  # e.g. "TITLE 1"
        words = title_label.split()
        if len(words) < 2:
            continue
        number = words[1]  # "1", "9A", "63", etc.

        title_desc = _clean(tds[2].get_text())
        node_name = f"{title_label} {title_desc}"
        link = BASE_URL + link_tag["href"].rstrip("/") + "/"

        node_id = f"{corpus_node.node_id}/title={number}"
        status = _check_reserved(node_name)

        title_node = Node(
            id=node_id,
            link=link,
            top_level_title=number,
            node_type="structure",
            level_classifier="title",
            number=number,
            node_name=node_name,
            parent=corpus_node.node_id,
            status=status,
        )
        insert_node(title_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)

        if status:
            continue
        if number in titles_done:
            continue
        work.append(title_node)

    def _do_title(node: Node):
        try:
            _scrape_title(node)
            _mark_title_done(str(node.number))
            return (node.number, "ok", None)
        except Exception as e:  # noqa: BLE001
            return (node.number, "fail", str(e)[:200])

    workers = int(os.environ.get("VAQUILL_TITLE_WORKERS", "8"))
    print(
        f"[scrapeID] running {len(work)} titles with {workers} parallel workers",
        flush=True,
    )
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [ex.submit(_do_title, item) for item in work]
        for fut in as_completed(futures):
            num, status_str, err = fut.result()
            if status_str == "fail":
                print(f"[scrapeID] title {num}: fail: {err}", flush=True)
            else:
                print(f"[scrapeID] title {num}: ok", flush=True)


# ---------------------------------------------------------------------------
# Title level
# ---------------------------------------------------------------------------


def _scrape_title(title_node: Node) -> None:
    """Parse a title page and dispatch per chapter."""
    soup = get_url_as_soup(str(title_node.link))
    container = _main_container(soup)
    if container is None:
        return

    for row in container.find_all("tr"):
        tds = row.find_all("td")
        if len(tds) < 3:
            continue

        chapter_label = _clean(tds[0].get_text())  # e.g. "CHAPTER 1"
        words = chapter_label.split()
        if len(words) < 2:
            continue
        ch_number = words[1]  # "1", "10", "16", etc.

        chapter_desc = _clean(tds[2].get_text())
        node_name = f"{chapter_label} {chapter_desc}"

        link_tag = tds[0].find("a", href=True)
        status = _check_reserved(node_name)

        if link_tag is None:
            # No link means repealed / reserved chapter.
            status = status or "reserved"
            link = str(title_node.link)
        else:
            link = BASE_URL + link_tag["href"].rstrip("/") + "/"

        node_id = f"{title_node.node_id}/chapter={ch_number}"

        chapter_node = Node(
            id=node_id,
            link=link,
            top_level_title=title_node.top_level_title,
            node_type="structure",
            level_classifier="chapter",
            number=ch_number,
            node_name=node_name,
            parent=title_node.node_id,
            status=status,
        )
        insert_node(chapter_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)

        if not status:
            _scrape_chapter(chapter_node)


# ---------------------------------------------------------------------------
# Chapter level
# ---------------------------------------------------------------------------


def _scrape_chapter(chapter_node: Node) -> None:
    """Parse a chapter page and dispatch per section."""
    soup = get_url_as_soup(str(chapter_node.link))
    container = _main_container(soup)
    if container is None:
        return

    for row in container.find_all("tr"):
        tds = row.find_all("td")
        if len(tds) < 3:
            continue

        sec_label = _clean(tds[0].get_text())  # e.g. "1-101"
        if not sec_label:
            continue

        # Check whether this row links to a section or a sub-chapter.
        link_tag = tds[0].find("a", href=True)
        if link_tag is None:
            # Reserved / repealed row with no link.
            section_desc = _clean(tds[2].get_text())
            node_name = f"{sec_label} {section_desc}".strip()
            node_id = f"{chapter_node.node_id}/section={sec_label}"
            citation = f"Idaho Code § {sec_label}"
            section_node = Node(
                id=node_id,
                link=str(chapter_node.link),
                top_level_title=chapter_node.top_level_title,
                node_type="content",
                level_classifier="section",
                number=sec_label,
                node_name=node_name,
                parent=chapter_node.node_id,
                citation=citation,
                status="reserved",
            )
            insert_node(section_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)
            continue

        href = link_tag["href"]

        # Detect sub-chapter pages (href contains "SCH" but not "SECT").
        if "SCH" in href and "SECT" not in href:
            _scrape_subchapter(chapter_node, BASE_URL + href.rstrip("/") + "/")
            continue

        # Normal section row.
        # sec_label is the full section number like "1-101".
        # The section number we store is just the part after the last title prefix,
        # which for Idaho is the full dotted number (e.g. "1-101", "63-3201A").
        section_desc = _clean(tds[2].get_text())
        node_name = f"{sec_label} {section_desc}".strip()
        sec_number = sec_label  # use as-is; unique within corpus hierarchy

        node_id = f"{chapter_node.node_id}/section={sec_number}"
        sec_link = BASE_URL + href.rstrip("/") + "/"
        citation = f"Idaho Code § {sec_number}"
        status = _check_reserved(node_name)

        if status:
            section_node = Node(
                id=node_id,
                link=sec_link,
                top_level_title=chapter_node.top_level_title,
                node_type="content",
                level_classifier="section",
                number=sec_number,
                node_name=node_name,
                parent=chapter_node.node_id,
                citation=citation,
                status=status,
            )
            insert_node(section_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)
            continue

        node_text, addendum = _fetch_section_content(sec_link)

        section_node = Node(
            id=node_id,
            link=sec_link,
            top_level_title=chapter_node.top_level_title,
            node_type="content",
            level_classifier="section",
            number=sec_number,
            node_name=node_name,
            parent=chapter_node.node_id,
            citation=citation,
            node_text=node_text,
            addendum=addendum,
        )
        insert_node(section_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)


# ---------------------------------------------------------------------------
# Sub-chapter level (e.g. Title 40 Chapter 16 has DETACHMENT / ANNEXATION)
# ---------------------------------------------------------------------------


def _scrape_subchapter(chapter_node: Node, subchapter_url: str) -> None:
    """Parse a sub-chapter page: treat its rows as sections of the parent chapter."""
    soup = get_url_as_soup(subchapter_url)
    container = _main_container(soup)
    if container is None:
        return

    for row in container.find_all("tr"):
        tds = row.find_all("td")
        if len(tds) < 3:
            continue

        sec_label = _clean(tds[0].get_text())
        if not sec_label:
            continue

        link_tag = tds[0].find("a", href=True)
        if link_tag is None:
            continue

        href = link_tag["href"]
        if "SECT" not in href:
            continue

        section_desc = _clean(tds[2].get_text())
        node_name = f"{sec_label} {section_desc}".strip()
        sec_number = sec_label

        node_id = f"{chapter_node.node_id}/section={sec_number}"
        sec_link = BASE_URL + href.rstrip("/") + "/"
        citation = f"Idaho Code § {sec_number}"
        status = _check_reserved(node_name)

        if status:
            section_node = Node(
                id=node_id,
                link=sec_link,
                top_level_title=chapter_node.top_level_title,
                node_type="content",
                level_classifier="section",
                number=sec_number,
                node_name=node_name,
                parent=chapter_node.node_id,
                citation=citation,
                status=status,
            )
            insert_node(section_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)
            continue

        node_text, addendum = _fetch_section_content(sec_link)

        section_node = Node(
            id=node_id,
            link=sec_link,
            top_level_title=chapter_node.top_level_title,
            node_type="content",
            level_classifier="section",
            number=sec_number,
            node_name=node_name,
            parent=chapter_node.node_id,
            citation=citation,
            node_text=node_text,
            addendum=addendum,
        )
        insert_node(section_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)


# ---------------------------------------------------------------------------
# Section content
# ---------------------------------------------------------------------------


def _fetch_section_content(
    url: str,
) -> tuple[Optional[NodeText], Optional[Addendum]]:
    """Fetch a single section page and return (NodeText, Addendum)."""
    try:
        soup = get_url_as_soup(url)
    except Exception as exc:
        print(f"[WARN] Could not fetch section {url}: {exc}", flush=True)
        return None, None

    container = soup.find(class_="pgbrk")
    if container is None:
        return None, None

    divs = container.find_all("div", recursive=False)
    # Skip the first _HEADER_DIV_COUNT breadcrumb divs (title name, title desc,
    # chapter name, chapter desc).
    content_divs = divs[_HEADER_DIV_COUNT:]

    node_text = NodeText()
    history_parts: list[str] = []
    in_history = False

    for div in content_divs:
        text = _clean(div.get_text())
        if not text:
            continue

        if text.startswith("History:") or in_history:
            in_history = True
            history_parts.append(text)
        else:
            node_text.add_paragraph(text=text)

    addendum: Optional[Addendum] = None
    if history_parts:
        addendum = Addendum()
        addendum.history = AddendumType(
            type="history", text=" ".join(history_parts)
        )

    return node_text, addendum


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


# WPBakery / Visual Composer wrapper class. The Idaho legislature template
# historically shipped with a typo (``vc-column-innner-wrapper``, three n's).
# Some snapshots / pages render the correct spelling (two n's). Try the
# canonical spelling first, fall back to the typo, and log which one matched
# the first time so silent-empty-corpus bugs become loud.
_WRAPPER_CLASSES = ("vc-column-inner-wrapper", "vc-column-innner-wrapper")
_WRAPPER_CLASS_LOGGED: Optional[str] = None


def _main_container(soup: BeautifulSoup) -> Optional[Tag]:
    """Return the second wrapper div (the data container).

    Tolerant of either CSS class spelling. Logs the spelling that first
    yields a match so an operator can spot upstream HTML drift.
    """
    global _WRAPPER_CLASS_LOGGED
    for cls in _WRAPPER_CLASSES:
        containers = soup.find_all("div", class_=cls)
        if len(containers) >= 2:
            if _WRAPPER_CLASS_LOGGED != cls:
                print(f"[scrapeID] wrapper class detected: {cls!r}", flush=True)
                _WRAPPER_CLASS_LOGGED = cls
            return containers[1]
    return None


def _check_reserved(text: str) -> Optional[str]:
    """Return 'reserved' if any reserved keyword appears in the text."""
    lower = text.lower()
    for kw in RESERVED_KEYWORDS:
        if kw in lower:
            return "reserved"
    return None


def _clean(raw: str) -> str:
    """Normalise whitespace and strip non-breaking spaces."""
    text = raw.replace("\xa0", " ").replace("’", "'").replace("‘", "'")
    text = re.sub(r"\s+", " ", text).strip()
    return text


if __name__ == "__main__":
    main()
