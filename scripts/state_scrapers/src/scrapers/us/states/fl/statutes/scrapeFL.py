import os
import sys
# BeautifulSoup imports
from bs4 import BeautifulSoup
from bs4.element import Tag

# Selenium imports
from selenium.webdriver.common.actions.wheel_input import ScrollOrigin
from selenium.webdriver import ActionChains
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement

from typing import List, Optional, Tuple
import time
import json
import re

from pathlib import Path

DIR = os.path.dirname(os.path.realpath(__file__))
# Get the current file's directory
current_file = Path(__file__).resolve()

# Find the 'src' directory
src_directory = current_file.parent
while src_directory.name != 'src' and src_directory.parent != src_directory:
    src_directory = src_directory.parent

# Get the parent directory of 'src'
project_root = src_directory.parent

# Add the project root to sys.path
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from src.utils.pydanticModels import NodeID, Node, Addendum, AddendumType, NodeText, Paragraph, ReferenceHub, Reference, DefinitionHub, Definition, IncorporatedTerms
from src.utils.scrapingHelpers import insert_jurisdiction_and_corpus_node, insert_node, get_url_as_soup



SKIP_TITLE = 0 # If you want to skip the first n titles, set this to n
COUNTRY = "us"
# State code for states, 'federal' otherwise
JURISDICTION = "fl"
# 'statutes' is current default
CORPUS = "statutes"
# No need to change this
TABLE_NAME =  f"{COUNTRY}_{JURISDICTION}_{CORPUS}"
BASE_URL = "https://www.flsenate.gov"
TOC_URL = "https://www.flsenate.gov/Laws/Statutes"
SKIP_TITLE = 0
# FL marks dead sections with bracketed status tags in the CatchlineText, e.g.
# "1.05  [Repealed by s. 7, ch. 99-3.]". Detected case-insensitively.
RESERVED_KEYWORDS = ["[repealed", "[reserved", "[expired", "[transferred", "[renumbered", "[former"]

# Pattern: "Title 1" / "Chapter 12" / "Part II" — first token is label, second
# is the number. Hardened: if the split fails we skip the entry with a warning
# instead of crashing the whole title.
_NUMBER_TOKEN_RE = re.compile(r"^\S+\s+(\S+)")


def _extract_number(node_name: str) -> Optional[str]:
    """Return the number token (e.g. ``1`` from ``Title 1`` / ``II`` from
    ``Part II``) or ``None`` if the heading is malformed. Replaces the bare
    ``node_name.split(" ")[1]`` calls that previously crashed with
    ``IndexError`` on empty / single-word headings.
    """
    if not node_name:
        return None
    m = _NUMBER_TOKEN_RE.match(node_name.strip())
    if not m:
        return None
    return m.group(1)


def _has_reserved_marker(text: str) -> bool:
    if not text:
        return False
    low = text.lower()
    return any(kw in low for kw in RESERVED_KEYWORDS)


# ---------------------------------------------------------------------------
# Title-level resume (mirrors scrapeDE.py).
# ---------------------------------------------------------------------------

def _titles_done_path():
    from vaquill_pipeline.config import SETTINGS
    return SETTINGS.chunks_dir / "state_fl_titles_done.txt"


def _load_titles_done() -> set:
    path = _titles_done_path()
    if not path.exists():
        return set()
    return {l.strip() for l in path.read_text().splitlines() if l.strip()}


def _mark_title_done(number: str) -> None:
    path = _titles_done_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a") as fh:
        fh.write(f"{number}\n")
        fh.flush()


def main():
    # Install the vaquill_pipeline patches (JsonlSink, r2_sync, http_client
    # routing for get_url_as_soup) so this scraper emits chunks + R2 mirrors
    # the same way scrapeDE does.
    try:
        from vaquill_pipeline import patch
        patch.install()
    except Exception as e:
        print(f"[scrapeFL] patch.install skipped: {e}", flush=True)

    corpus_node: Node = insert_jurisdiction_and_corpus_node(COUNTRY, JURISDICTION, CORPUS)
    scrape_all_titles(corpus_node)


def _build_title_work(node_parent: Node) -> List[Tuple[Node, str]]:
    """Parse the top-level TOC into a list of (title_node, number) tuples.

    Sequential and cheap (one HTTP fetch + pure parse). Every node-attribute
    access is guarded so a malformed/added entry warns and is skipped rather
    than crashing the whole scrape.
    """
    work: List[Tuple[Node, str]] = []
    soup = get_url_as_soup(TOC_URL)
    statutes_container = soup.find("div", class_="statutesTOC") if soup else None
    if statutes_container is None:
        print("[scrapeFL] WARN: statutesTOC container missing on TOC; nothing to scrape", flush=True)
        return work

    all_title_containers = statutes_container.find_all("a")

    for i, title_container in enumerate(all_title_containers):
        if i < SKIP_TITLE:
            continue

        href = title_container.get("href")
        if not href:
            print(f"[scrapeFL] WARN: title #{i} missing href; skipping", flush=True)
            continue
        link = f"{BASE_URL}/{href}"

        title_spans = title_container.find_all("span")
        if len(title_spans) < 2:
            print(f"[scrapeFL] WARN: title #{i} has <2 spans ({len(title_spans)}); skipping", flush=True)
            continue

        node_name = title_spans[0].get_text().strip()
        number = _extract_number(node_name)
        if not number:
            print(f"[scrapeFL] WARN: cannot parse title number from {node_name!r}; skipping", flush=True)
            continue

        node_name = f"{node_name} {title_spans[1].get_text().strip()}"

        core_metadata = None
        if len(title_spans) >= 3:
            core_metadata = {"chapterRange": title_spans[2].get_text().strip()}

        parent = node_parent.node_id
        node_id = f"{parent}/title={number}"

        title_node = Node(
            id=node_id,
            link=link,
            top_level_title=number,
            node_type="structure",
            level_classifier="title",
            number=number,
            node_name=node_name,
            parent=parent,
            core_metadata=core_metadata,
        )
        insert_node(title_node, TABLE_NAME, ignore_duplicate=True, debug_mode=False)
        work.append((title_node, number))

    return work


def scrape_all_titles(node_parent: Node):
    """Walk all FL titles in parallel.

    Concurrency via ``VAQUILL_TITLE_WORKERS`` (default 8). Title-level resume
    via ``state_fl_titles_done.txt``; set ``VAQUILL_FORCE_RESCRAPE=1`` to
    re-scrape titles already on disk.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    titles_done = set() if os.environ.get("VAQUILL_FORCE_RESCRAPE") else _load_titles_done()
    if titles_done:
        print(f"[scrapeFL] resume: {len(titles_done)} titles already done: {sorted(titles_done)}", flush=True)

    all_work = _build_title_work(node_parent)
    work = [(tn, num) for (tn, num) in all_work if num not in titles_done]

    def _do_title(item):
        title_node, number = item
        try:
            scrape_chapters(title_node)
            _mark_title_done(number)
            return (number, "ok", None)
        except Exception as e:
            return (number, "fail", f"{type(e).__name__}: {e}"[:200])

    workers = int(os.environ.get("VAQUILL_TITLE_WORKERS", "8"))
    print(f"[scrapeFL] running {len(work)} titles with {workers} parallel workers", flush=True)
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for fut in as_completed(ex.submit(_do_title, item) for item in work):
            num, status, err = fut.result()
            if status == "fail":
                print(f"[scrapeFL] title {num}: {status}: {err}", flush=True)
            else:
                print(f"[scrapeFL] title {num}: {status}", flush=True)


def scrape_chapters(node_parent: Node):
    soup = get_url_as_soup(str(node_parent.link))
    statutes_container = soup.find("div", class_="statutesTOC") if soup else None
    if statutes_container is None:
        print(f"[scrapeFL] WARN: title {node_parent.number} has no statutesTOC; skipping", flush=True)
        return

    chapter_parent = statutes_container.find("ol", class_="chapter")
    if chapter_parent is None:
        print(f"[scrapeFL] WARN: title {node_parent.number} has no chapter list; skipping", flush=True)
        return

    all_chapter_containers: List[Tag] = chapter_parent.find_all("a")

    for i, chapter_container in enumerate(all_chapter_containers):
        href = chapter_container.get("href")
        if not href:
            continue
        # Avoid incorrectly processing Parts as Chapters. Should have previously been scraped by scrape_parts
        if "Part" in href:
            continue
        link = f"{BASE_URL}/{href}"

        chapter_spans = chapter_container.find_all("span")
        if len(chapter_spans) < 2:
            print(f"[scrapeFL] WARN: title {node_parent.number} chapter #{i} has <2 spans; skipping", flush=True)
            continue

        node_name = chapter_spans[0].get_text().strip()
        number = _extract_number(node_name)
        if not number:
            print(f"[scrapeFL] WARN: cannot parse chapter number from {node_name!r}; skipping", flush=True)
            continue

        node_name = f"{node_name} {chapter_spans[1].get_text().strip()}"

        status = "reserved" if _has_reserved_marker(node_name) else None

        parent = node_parent.node_id
        node_id = f"{parent}/chapter={number}"

        chapter_node = Node(
            id=node_id,
            link=link,
            top_level_title=node_parent.top_level_title,
            node_type="structure",
            level_classifier="chapter",
            number=number,
            node_name=node_name,
            parent=parent,
            status=status,
        )
        insert_node(chapter_node, TABLE_NAME, debug_mode=False)

        if status:
            # No sections to fetch for repealed/reserved chapters.
            continue

        possible_part_parent = chapter_container.parent.find("ol", class_="part") if chapter_container.parent else None
        if possible_part_parent is not None:
            scrape_parts(chapter_node, possible_part_parent)
        else:
            find_section_links(chapter_node)


def scrape_parts(node_parent: Node, soup: BeautifulSoup):
    all_part_containers = soup.find_all("a")

    for i, part_container in enumerate(all_part_containers):
        href = part_container.get("href")
        if not href:
            continue
        link = f"{BASE_URL}/{href}"

        part_spans = part_container.find_all("span")
        if len(part_spans) < 2:
            print(f"[scrapeFL] WARN: chapter {node_parent.number} part #{i} has <2 spans; skipping", flush=True)
            continue

        node_name = part_spans[0].get_text().strip()
        number = _extract_number(node_name)
        if not number:
            print(f"[scrapeFL] WARN: cannot parse part number from {node_name!r}; skipping", flush=True)
            continue

        node_name = f"{node_name} {part_spans[1].get_text().strip()}"

        core_metadata = None
        if len(part_spans) >= 3:
            core_metadata = {"sectionRange": part_spans[2].get_text().strip()}

        parent = node_parent.node_id
        node_id = f"{parent}/part={number}"
        part_node = Node(
            id=node_id,
            link=link,
            top_level_title=node_parent.top_level_title,
            node_type="structure",
            level_classifier="part",
            number=number,
            node_name=node_name,
            parent=parent,
            core_metadata=core_metadata,
        )
        insert_node(part_node, TABLE_NAME, debug_mode=False)
        find_section_links(part_node)


def find_section_links(node_parent: Node):
    soup = get_url_as_soup(str(node_parent.link))
    section_parent = soup.find("div", class_="CatchlineIndex") if soup else None
    if section_parent is None:
        print(f"[scrapeFL] WARN: {node_parent.node_id} has no CatchlineIndex; skipping", flush=True)
        return

    for section_container in section_parent.find_all("a"):
        href = section_container.get("href")
        if not href:
            continue
        link = f"{BASE_URL}/{href}"
        try:
            scrape_section(node_parent, link)
        except Exception as e:
            print(f"[scrapeFL] WARN: section {link} failed: {type(e).__name__}: {e}", flush=True)


def scrape_section(node_parent: Node, link: str):
    soup = get_url_as_soup(link)
    main_container = soup.find("div", id="main") if soup else None
    section_container = main_container.find("div", class_="Section") if main_container else None
    if section_container is None:
        print(f"[scrapeFL] WARN: {link} has no Section container; skipping", flush=True)
        return

    number_container = section_container.find("span", class_="SectionNumber")
    name_container = section_container.find("span", class_="CatchlineText")
    if number_container is None or name_container is None:
        print(f"[scrapeFL] WARN: {link} missing SectionNumber/CatchlineText; skipping", flush=True)
        return

    number = number_container.get_text().strip()
    catchline = name_container.get_text().strip()
    if not number:
        print(f"[scrapeFL] WARN: {link} empty section number; skipping", flush=True)
        return

    node_name = f"{number} {catchline}"
    parent = node_parent.node_id
    node_id = f"{parent}/section={number}"
    citation = f"Fla. Stat. § {number}"

    # FL marks dead sections by appending e.g. "[Repealed by s. 7, ch. 99-3.]"
    # to the CatchlineText. Treat any such marker as ``reserved`` and skip the
    # body fetch (often empty / missing History container, which previously
    # crashed on ``addendum_container.get_text()``).
    status = "reserved" if _has_reserved_marker(catchline) else None

    node_text = None
    addendum = None
    core_metadata = None

    if not status:
        node_text = NodeText()
        text_container = section_container.find("span", class_="SectionBody")
        if text_container is not None:
            for paragraph_container in text_container.find_all(recursive=False):
                text = paragraph_container.get_text().strip()
                if text:
                    node_text.add_paragraph(text=text)

        addendum_container = section_container.find("div", class_="History")
        if addendum_container is not None:
            addendum = Addendum()
            addendum.history = AddendumType(type="history", text=addendum_container.get_text().strip())

        note_container = section_container.find("div", class_="Note")
        if note_container is not None:
            core_metadata = {"Note": note_container.get_text().strip()}

    section_node = Node(
        id=node_id,
        link=link,
        citation=citation,
        top_level_title=node_parent.top_level_title,
        node_type="content",
        level_classifier="section",
        number=number,
        node_name=node_name,
        parent=node_parent.node_id,
        status=status,
        node_text=node_text,
        addendum=addendum,
        core_metadata=core_metadata,
    )

    insert_node(section_node, TABLE_NAME, debug_mode=False)


if __name__ == "__main__":
    main()
