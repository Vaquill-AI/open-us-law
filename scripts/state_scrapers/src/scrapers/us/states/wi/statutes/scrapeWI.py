"""Wisconsin Statutes scraper.

Source:
  TOC:     https://docs.legis.wisconsin.gov/statutes/statutes/
  Chapter: https://docs.legis.wisconsin.gov/document/statutes/<N>
  Section  (canonical link stored in node): https://docs.legis.wisconsin.gov/document/statutes/<N.NN>

The publisher renders all sections for a chapter on the chapter page at
/document/statutes/<chapter_number> (NOT /statutes/statutes/<N>).

Each section lives in a div with class "qsatxt_1sect" that carries a
data-section attribute (e.g. "1.01"). History annotations appear in sibling
div.qsnote_history elements with matching data-section attributes.

Old blind-selector bugs fixed:
  1. Chapter content URL was /statutes/statutes/<N> -- actual content is at
     /document/statutes/<N>.
  2. Section div class was "qs_atxt_1section_" -- real class is "qsatxt_1sect".
  3. History div class was "qs_note_history_" -- real class is "qsnote_history".
  4. Section title span class was "qs_title_section_" -- real class is "qstitle_sect".
  5. Section number span class was "qs_num_sectnum_" -- real class is "qsnum_sect".

Hierarchy: us/wi/statutes/chapter=N/section=S
Citation:  "Wis. Stat. § <SECTION>"  (e.g. "Wis. Stat. § 1.01")
"""
from __future__ import annotations

import copy
import re
import sys
from pathlib import Path
from typing import Optional

from bs4 import BeautifulSoup

current_file = Path(__file__).resolve()
src_directory = current_file.parent
while src_directory.name != "src" and src_directory.parent != src_directory:
    src_directory = src_directory.parent
project_root = src_directory.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from src.utils.pydanticModels import NodeID, Node, NodeText, Addendum, AddendumType
from src.utils.scrapingHelpers import (
    insert_jurisdiction_and_corpus_node,
    insert_node,
    get_url_as_soup,
)

COUNTRY = "us"
JURISDICTION = "wi"
CORPUS = "statutes"
TABLE_NAME = f"{COUNTRY}_{JURISDICTION}_{CORPUS}"

BASE_URL = "https://docs.legis.wisconsin.gov"
TOC_URL = "https://docs.legis.wisconsin.gov/statutes/statutes/"

RESERVED_KEYWORDS = ["[Repealed]", "[Reserved]", "[Expired]", "(Repealed)", "(Reserved)"]


def main() -> None:
    corpus_node: Node = insert_jurisdiction_and_corpus_node(COUNTRY, JURISDICTION, CORPUS)
    scrape_all_chapters(corpus_node)


def _chapters_done_path():
    """Where we persist the set of chapters already fully scraped for resume."""
    from vaquill_pipeline.config import SETTINGS
    return SETTINGS.chunks_dir / "state_wi_chapters_done.txt"


def _load_chapters_done() -> set:
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


def scrape_all_chapters(corpus_node: Node) -> None:
    """Fetch the TOC page and iterate over all chapter links.

    Chapter scrapes run in parallel via ThreadPoolExecutor. Each chapter is
    fully independent (HTTP + parse + insert). Concurrency controlled by env
    var ``VAQUILL_CHAPTER_WORKERS`` (default 8). Resume support:
    completed chapters persisted in ``state_wi_chapters_done.txt`` and
    skipped on re-runs. Set ``VAQUILL_FORCE_RESCRAPE=1`` to override.
    """
    import os
    from concurrent.futures import ThreadPoolExecutor, as_completed

    chapters_done = set() if os.environ.get("VAQUILL_FORCE_RESCRAPE") else _load_chapters_done()
    if chapters_done:
        print(f"[scrapeWI] resume: {len(chapters_done)} chapters already done", flush=True)

    soup = get_url_as_soup(TOC_URL)

    seen_chapters: set[str] = set()
    work: list[Node] = []

    for p_tag in soup.find_all("p"):
        chapter_link_tag = None
        for a in p_tag.find_all("a", href=True):
            href = a["href"].strip()
            if re.search(r"/document/statutes/(\d+)$", href):
                chapter_link_tag = a
                break

        if chapter_link_tag is None:
            continue

        m = re.search(r"/document/statutes/(\d+)$", chapter_link_tag["href"])
        if not m:
            continue
        chapter_number = m.group(1)

        if chapter_number in seen_chapters:
            continue
        seen_chapters.add(chapter_number)

        chapter_text = p_tag.get_text().strip()
        node_name = _extract_chapter_name(chapter_text, chapter_number)

        # Chapter content is served at /document/statutes/<N>
        chapter_url = f"{BASE_URL}/document/statutes/{chapter_number}"
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
        # Insert chapter structure node up front (cheap, idempotent).
        insert_node(chapter_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)

        if status:
            continue
        if chapter_number in chapters_done:
            continue
        work.append(chapter_node)

    def _do_chapter(node: Node):
        try:
            scrape_chapter(node)
            _mark_chapter_done(node.number)
            return (node.number, "ok", None)
        except Exception as e:
            return (node.number, "fail", str(e)[:200])

    # WI's unit is the chapter, so VAQUILL_CHAPTER_WORKERS is the specific knob,
    # but fall back to the fleet-wide VAQUILL_TITLE_WORKERS the refresh tasks
    # actually set. Without the fallback, setting TITLE_WORKERS across the fleet
    # silently does nothing here.
    workers = int(
        os.environ.get("VAQUILL_CHAPTER_WORKERS")
        or os.environ.get("VAQUILL_TITLE_WORKERS", "8")
    )
    print(f"[scrapeWI] running {len(work)} chapters with {workers} parallel workers", flush=True)
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for fut in as_completed(ex.submit(_do_chapter, n) for n in work):
            num, status_str, err = fut.result()
            if status_str == "fail":
                print(f"[scrapeWI] chapter {num}: {status_str}: {err}", flush=True)
            else:
                print(f"[scrapeWI] chapter {num}: {status_str}", flush=True)


def scrape_chapter(chapter_node: Node) -> None:
    """Fetch a chapter page and extract all section nodes from inline content."""
    soup = get_url_as_soup(str(chapter_node.link))

    # All section and history content lives inside #document.
    document_div = soup.find(id="document")
    if document_div is None:
        document_div = soup

    # Walk content divs matching sections and their histories.
    # Real classes (verified 2026-05-11):
    #   section body:   "qsatxt_1sect"
    #   history note:   "qsnote_history"
    content_divs = document_div.find_all(
        "div",
        class_=lambda c: c and (
            "qsatxt_1sect" in c or "qsnote_history" in c
        ),
    )

    current_section_div: Optional[BeautifulSoup] = None
    history_map: dict[str, str] = {}
    section_order: list[BeautifulSoup] = []

    for div in content_divs:
        cls_list = div.get("class") or []
        if "qsatxt_1sect" in cls_list:
            current_section_div = div
            section_order.append(div)
        elif "qsnote_history" in cls_list and current_section_div is not None:
            sec_num = div.get("data-section", "")
            raw = _clean_text(div.get_text(separator=" "))
            if sec_num:
                history_map[sec_num] = (history_map.get(sec_num, "") + " " + raw).strip()

    for section_div in section_order:
        section_number = section_div.get("data-section", "").strip()
        if not section_number:
            continue

        section_url = f"{BASE_URL}/document/statutes/{section_number}"
        node_id = f"{chapter_node.node_id}/section={section_number}"

        # Title span class is "qstitle_sect" (was wrongly "qs_title_section_")
        title_span = section_div.find("span", class_="qstitle_sect")
        if title_span:
            section_title = _clean_text(title_span.get_text(separator=" "))
        else:
            section_title = ""

        node_name = (
            f"§ {section_number}. {section_title}"
            if section_title
            else f"§ {section_number}"
        )

        citation = f"Wis. Stat. § {section_number}"
        status = _check_reserved(node_name)

        node_text: Optional[NodeText] = None
        addendum: Optional[Addendum] = None

        if not status:
            node_text = _extract_section_text(section_div)
            history_raw = history_map.get(section_number, "").strip()
            if history_raw:
                addendum = Addendum()
                addendum.history = AddendumType(type="history", text=history_raw)

        section_node = Node(
            id=node_id,
            link=section_url,
            top_level_title=chapter_node.top_level_title,
            node_type="content",
            level_classifier="section",
            number=section_number,
            node_name=node_name,
            parent=chapter_node.node_id,
            citation=citation,
            status=status,
            node_text=node_text,
            addendum=addendum,
        )
        insert_node(section_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)


def _extract_section_text(section_div: BeautifulSoup) -> Optional[NodeText]:
    """Return a NodeText with the body text of a section div.

    Strips the section-number span (qsnum_sect) and title span (qstitle_sect)
    plus the .reference anchor so only body prose remains.
    """
    node_text = NodeText()

    div_copy = copy.copy(section_div)

    # Strip number and title spans -- real classes are qsnum_sect / qstitle_sect
    for span in div_copy.find_all("span", class_="qsnum_sect"):
        span.decompose()
    for span in div_copy.find_all("span", class_="qstitle_sect"):
        span.decompose()

    # Strip the leading reference anchor (e.g. <a class="reference" href="...">1.01</a>)
    for a in div_copy.find_all("a", class_="reference"):
        a.decompose()

    raw = div_copy.get_text(separator=" ")
    text = _clean_text(raw)
    if text:
        node_text.add_paragraph(text=text)

    return node_text if node_text.paragraphs else None


def _extract_chapter_name(full_text: str, chapter_number: str) -> str:
    """Return a clean chapter name from a TOC paragraph.

    Input:  "Chapter 1 (PDF: ) - Sovereignty And Jurisdiction Of The State"
    Output: "Chapter 1 - Sovereignty And Jurisdiction Of The State"
    """
    text = re.sub(r"\(PDF:[^)]*\)", "", full_text)
    text = re.sub(r"\s+", " ", text).strip().strip(" -").strip()
    return text


def _check_reserved(text: str) -> Optional[str]:
    upper = text.upper()
    for kw in RESERVED_KEYWORDS:
        if kw.upper() in upper:
            return "reserved"
    return None


def _clean_text(raw: str) -> str:
    text = raw.replace("\xa0", " ").replace(" ", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


if __name__ == "__main__":
    main()
