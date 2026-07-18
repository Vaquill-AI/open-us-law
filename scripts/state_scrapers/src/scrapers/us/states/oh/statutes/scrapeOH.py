"""Ohio Revised Code scraper.

Hierarchy: us/oh/statutes/title=N/chapter=M/section=S

Source: https://codes.ohio.gov/ohio-revised-code/
  - TOC page lists title links (e.g. /ohio-revised-code/title-1)
  - Title page lists chapter links via class="data-grid laws-table" + class="name-cell"
  - Chapter page contains all sections inline:
      section header: class="content-head-text" <a>
      section body:   class="laws-body"
      addendum info:  class="laws-section-info-module"

Full scrape: walks every title from the live TOC at codes.ohio.gov.
Title-level resume via ``state_oh_titles_done.txt`` in
``vaquill_pipeline.config.SETTINGS.chunks_dir``. Concurrency via
``VAQUILL_TITLE_WORKERS`` (default 8). Force a full re-scrape with
``VAQUILL_FORCE_RESCRAPE=1``.
"""
from __future__ import annotations

import os
import re
import sys
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Optional, Tuple

from bs4 import BeautifulSoup

# Path resolution: find project root (parent of 'src')
_current_file = Path(__file__).resolve()
_src_dir = _current_file.parent
while _src_dir.name != "src" and _src_dir.parent != _src_dir:
    _src_dir = _src_dir.parent
_project_root = _src_dir.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.utils.pydanticModels import Addendum, AddendumType, Node, NodeText
from src.utils.scrapingHelpers import (
    get_url_as_soup,
    insert_jurisdiction_and_corpus_node,
    insert_node,
)

COUNTRY = "us"
JURISDICTION = "oh"
CORPUS = "statutes"
TABLE_NAME = f"{COUNTRY}_{JURISDICTION}_{CORPUS}"

BASE_URL = "https://codes.ohio.gov"
TOC_URL = "https://codes.ohio.gov/ohio-revised-code"

RESERVED_KEYWORDS = ["[Repealed", "[Expired", "[Reserved", "[Renumbered"]


def _titles_done_path():
    """Where we persist the set of titles already fully scraped for resume."""
    from vaquill_pipeline.config import SETTINGS
    return SETTINGS.chunks_dir / "state_oh_titles_done.txt"


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


def main() -> None:
    corpus_node: Node = insert_jurisdiction_and_corpus_node(COUNTRY, JURISDICTION, CORPUS)
    scrape_all_titles(corpus_node)


def scrape_all_titles(corpus_node: Node) -> None:
    """Walk every title from the live Ohio Revised Code TOC in parallel.

    Each title is independent (no shared state at the title level), so we
    hand them to a ThreadPoolExecutor. Concurrency: ``VAQUILL_TITLE_WORKERS``
    (default 8). HTTP keep-alive + UA rotation comes from
    ``vaquill_pipeline.http_client`` via ``get_url_as_soup``.

    Resume: titles previously completed are persisted in
    ``state_oh_titles_done.txt`` and skipped on re-runs. Set
    ``VAQUILL_FORCE_RESCRAPE=1`` to override.
    """
    titles_done = set() if os.environ.get("VAQUILL_FORCE_RESCRAPE") else _load_titles_done()
    if titles_done:
        print(
            f"[scrapeOH] resume: {len(titles_done)} titles already done: "
            f"{sorted(titles_done, key=lambda x: int(x) if x.isdigit() else 999)}",
            flush=True,
        )

    soup: BeautifulSoup = get_url_as_soup(TOC_URL)
    table = soup.find(class_="data-grid laws-table")
    if table is None:
        # Fallback: search all <a> tags whose href matches ohio-revised-code/title-
        all_links = soup.find_all("a", href=re.compile(r"ohio-revised-code/title-\d+"))
    else:
        all_links = table.find_all("a", href=True)

    # Build the title work list (dedupe by number, since the TOC can repeat links).
    seen: set = set()
    work: List[Node] = []
    for a in all_links:
        href: str = a["href"].strip()
        m = re.search(r"ohio-revised-code/title-(\d+)$", href)
        if not m:
            continue
        number = m.group(1)
        if number in seen:
            continue
        seen.add(number)

        node_name_raw = a.get_text(separator=" ", strip=True)
        node_name = node_name_raw if node_name_raw else f"Title {number}"

        # TOC hrefs are like "ohio-revised-code/title-1" (already include the
        # /ohio-revised-code/ prefix), so resolve relative to the site root.
        link = urljoin(BASE_URL + "/", href)
        node_id = f"{corpus_node.node_id}/title={number}"

        title_node = Node(
            id=node_id,
            link=link,
            top_level_title=number,
            node_type="structure",
            level_classifier="title",
            number=number,
            node_name=node_name,
            parent=corpus_node.node_id,
        )
        # Insert title structure node up front (cheap, idempotent).
        insert_node(title_node, TABLE_NAME, ignore_duplicate=True, debug_mode=False)
        if number in titles_done:
            continue
        work.append(title_node)

    def _do_title(title_node: Node) -> Tuple[str, str, Optional[str]]:
        try:
            scrape_chapters(title_node)
            _mark_title_done(str(title_node.number))
            return (str(title_node.number), "ok", None)
        except Exception as e:
            return (str(title_node.number), "fail", str(e)[:200])

    workers = int(os.environ.get("VAQUILL_TITLE_WORKERS", "8"))
    print(
        f"[scrapeOH] discovered {len(seen)} titles, scraping {len(work)} "
        f"with {workers} parallel workers",
        flush=True,
    )
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for fut in as_completed(ex.submit(_do_title, item) for item in work):
            num, status, err = fut.result()
            if status == "fail":
                print(f"[scrapeOH] title {num}: {status}: {err}", flush=True)
            else:
                print(f"[scrapeOH] title {num}: {status}", flush=True)


def scrape_chapters(title_node: Node) -> None:
    soup: BeautifulSoup = get_url_as_soup(str(title_node.link))
    table = soup.find(class_="data-grid laws-table")
    if table is None:
        print(f"[warn] no data-grid table on {title_node.link}", flush=True)
        return

    name_cells = table.find_all(class_="name-cell")
    for cell in name_cells:
        a = cell.find("a", href=True)
        if a is None:
            continue

        href: str = a["href"].strip()
        m = re.search(r"chapter-(\d+)$", href)
        if not m:
            continue
        chapter_number = m.group(1)

        node_name_raw = a.get_text(separator=" ", strip=True)
        # e.g. "Chapter 101 | General Assembly"
        node_name = node_name_raw if node_name_raw else f"Chapter {chapter_number}"

        # Resolve relative to the parent title page so bare "chapter-101"
        # becomes "/ohio-revised-code/chapter-101", not "/chapter-101".
        link = urljoin(str(title_node.link) + "/", href)
        node_id = f"{title_node.node_id}/chapter={chapter_number}"

        chapter_node = Node(
            id=node_id,
            link=link,
            top_level_title=title_node.top_level_title,
            node_type="structure",
            level_classifier="chapter",
            number=chapter_number,
            node_name=node_name,
            parent=title_node.node_id,
        )
        insert_node(chapter_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)
        scrape_sections(chapter_node)


def scrape_sections(chapter_node: Node) -> None:
    soup: BeautifulSoup = get_url_as_soup(str(chapter_node.link))
    table = soup.find(class_="data-grid laws-table")
    if table is None:
        print(f"[warn] no sections table on {chapter_node.link}", flush=True)
        return

    name_cells = table.find_all(class_="name-cell")
    for cell in name_cells:
        _scrape_section_from_cell(cell, chapter_node)


def _scrape_section_from_cell(cell, chapter_node: Node) -> None:
    # Section header link
    head_div = cell.find(class_="content-head-text")
    if head_div is None:
        # Older layout: look for any <a> whose href matches section-NNN.NN
        a = cell.find("a", href=re.compile(r"/section-"))
    else:
        a = head_div.find("a", href=True)

    if a is None:
        return

    href: str = a["href"].strip()
    m = re.search(r"section-([0-9A-Za-z.\-]+)$", href)
    if not m:
        return
    section_number = m.group(1)

    node_name_raw = a.get_text(separator=" ", strip=True)
    # e.g. "Section 101.01 | Regular session of the general assembly."
    node_name = node_name_raw if node_name_raw else f"Section {section_number}"

    link = urljoin(str(chapter_node.link) + "/", href)

    status: Optional[str] = None
    for kw in RESERVED_KEYWORDS:
        if kw.lower() in node_name.lower():
            status = "reserved"
            break

    citation = f"Ohio Rev. Code § {section_number}"
    node_id = f"{chapter_node.node_id}/section={section_number}"

    node_text: Optional[NodeText] = None
    addendum: Optional[Addendum] = None

    if not status:
        node_text = NodeText()

        # Section body paragraphs
        body = cell.find(class_="laws-body")
        if body is not None:
            for elem in body.find_all(["p", "div"], recursive=False):
                raw = elem.get_text(separator=" ", strip=True)
                text = re.sub(r"\s+", " ", raw).strip()
                if text:
                    node_text.add_paragraph(text=text)

            # If no block-level children found, fall back to direct text children
            if not node_text.paragraphs:
                raw = body.get_text(separator="\n", strip=True)
                for line in raw.splitlines():
                    line = re.sub(r"\s+", " ", line).strip()
                    if line:
                        node_text.add_paragraph(text=line)

        # Addendum (effective date, legislation history)
        addendum_text = ""
        for info_mod in cell.find_all(class_="laws-section-info-module"):
            chunk = re.sub(r"\s+", " ", info_mod.get_text(separator=" ", strip=True)).strip()
            if chunk:
                addendum_text += chunk + " "
        addendum_text = addendum_text.strip()
        if addendum_text:
            addendum = Addendum(history=AddendumType(type="history", text=addendum_text))

    section_node = Node(
        id=node_id,
        link=link,
        citation=citation,
        top_level_title=chapter_node.top_level_title,
        node_type="content",
        level_classifier="section",
        number=section_number,
        node_name=node_name,
        parent=chapter_node.node_id,
        status=status,
        node_text=node_text,
        addendum=addendum,
    )
    insert_node(section_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)


if __name__ == "__main__":
    main()
