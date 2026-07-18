"""Missouri Revised Statutes scraper.

Source:  https://revisor.mo.gov/main/Home.aspx  (ASP.NET, static HTML)
Hierarchy: Title (roman numeral grouping) -> Chapter -> Section

The TOC page presents chapters inside <details> elements grouped by Title.
Chapter pages list sections in a table. Section text is on individual
OneSection.aspx pages.

Node-id path: us/mo/statutes/chapter=N/section=S
Citation format: Mo. Rev. Stat. § <SECTION>
"""
from __future__ import annotations

import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Project-root bootstrap (mirrors ME/ND pattern)
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
JURISDICTION = "mo"
CORPUS = "statutes"
TABLE_NAME = f"{COUNTRY}_{JURISDICTION}_{CORPUS}"

BASE_URL = "https://revisor.mo.gov"
TOC_URL = f"{BASE_URL}/main/Home.aspx"
CHAPTER_URL = f"{BASE_URL}/main/OneChapter.aspx"
SECTION_URL = f"{BASE_URL}/main/OneSection.aspx"

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
    _scrape_toc(corpus_node)


# ---------------------------------------------------------------------------
# TOC: Home.aspx -> <details> per title group, <a> per chapter
# ---------------------------------------------------------------------------

def _scrape_toc(corpus_node: Node) -> None:
    """Parse the main TOC and dispatch each chapter in parallel.

    MO groups chapters into roman-numeral Titles on the TOC page but does NOT
    have separate Title landing pages. The spec requires path
    ``us/mo/statutes/chapter=N/section=S`` (no title segment), so we skip
    inserting title structure nodes and go straight to chapters.

    Chapter is therefore the outermost independent level: each chapter page and
    its sections share no state with sibling chapters, so chapters fan out to a
    ThreadPoolExecutor sized by VAQUILL_TITLE_WORKERS (default 8, matching
    scrapeWA/scrapeAK). The crawl is latency-bound at one request per section.

    NOTE: MO deliberately has no chapters_done resume file -- every run
    re-crawls in full. That is what lets an amended section be re-fetched and
    re-chunked into a fresh content-addressed point_id. Do not add a
    chapters_done skip here to save time without replacing that freshness some
    other way -- it would make MO amendment-blind.
    """
    soup = get_url_as_soup(TOC_URL)
    bottom = soup.find(id="BOTTOM")
    if bottom is None:
        raise RuntimeError(f"Cannot find #BOTTOM anchor on {TOC_URL}")

    outer = bottom.find_previous_sibling()
    if outer is None:
        raise RuntimeError("Cannot find content container on TOC page")

    top_children = outer.find_all(recursive=False)
    if len(top_children) < 2:
        raise RuntimeError("Unexpected TOC layout: fewer than 2 top-level children")

    details_container = top_children[1]
    all_details = details_container.find_all("details")

    work: list[Node] = []
    for detail in all_details:
        chapter_links = detail.find_all("a", href=True)
        for a_tag in chapter_links:
            href: str = a_tag["href"].strip()
            # Only chapter links; href contains 'OneChapter.aspx?chapter='
            if "OneChapter.aspx" not in href and "chapter=" not in href:
                continue

            m = re.search(r"chapter=([\w\.]+)", href)
            if not m:
                continue
            chapter_number = m.group(1)

            # Raw link text: "1 Laws in Force..." -- first token is chapter number
            raw_text = _clean_text(a_tag.get_text())
            # Remove leading chapter number (may be surrounded by em-spaces)
            node_name = re.sub(rf"^\s*{re.escape(chapter_number)}\s*", "", raw_text).strip()
            node_name = f"Chapter {chapter_number} {node_name}"

            # Build canonical (non-Wayback) URL
            chapter_url = f"{CHAPTER_URL}?chapter={chapter_number}"
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
        f"[scrapeMO] running {len(work)} chapters with {workers} parallel workers",
        flush=True,
    )
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for fut in as_completed(ex.submit(_do_chapter, item) for item in work):
            num, status, err = fut.result()
            if status == "fail":
                print(f"[scrapeMO] chapter {num}: {status}: {err}", flush=True)
            else:
                print(f"[scrapeMO] chapter {num}: {status}", flush=True)


# ---------------------------------------------------------------------------
# Chapter: OneChapter.aspx?chapter=N -> table of sections
# ---------------------------------------------------------------------------

def _scrape_chapter(chapter_node: Node) -> None:
    """Fetch a chapter page and iterate over its section rows."""
    soup = get_url_as_soup(str(chapter_node.link))
    bottom = soup.find(id="BOTTOM")
    if bottom is None:
        return

    outer = bottom.find_previous_sibling()
    if outer is None:
        return

    table = outer.find("table")
    if table is None:
        return

    for row in table.find_all("tr"):
        tds = row.find_all("td")
        if not tds:
            continue

        link_tag = tds[0].find("a", href=True)
        if link_tag is None:
            # Category / sub-heading row with no link; skip.
            continue

        # Section number from link text (e.g. "1.010")
        section_number = _clean_text(link_tag.get_text())
        if not section_number:
            continue

        # Section title from second column
        sec_title = _clean_text(tds[1].get_text()) if len(tds) > 1 else ""
        # Strip trailing effective date like "(8/28/2015)"
        sec_title = re.sub(r"\s*\(\d{1,2}/\d{1,2}/\d{4}\)\s*$", "", sec_title).strip()
        node_name = f"{section_number} {sec_title}".strip()

        node_id = f"{chapter_node.node_id}/section={section_number}"
        section_url = f"{SECTION_URL}?section={section_number}"
        citation = f"Mo. Rev. Stat. § {section_number}"

        status = _check_reserved(node_name)

        if status:
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
            )
            insert_node(section_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)
            continue

        node_text, addendum = _fetch_section_content(section_url)

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
            node_text=node_text,
            addendum=addendum,
        )
        insert_node(section_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)


# ---------------------------------------------------------------------------
# Section: OneSection.aspx?section=X.XXX
# ---------------------------------------------------------------------------

def _fetch_section_content(
    url: str,
) -> tuple[Optional[NodeText], Optional[Addendum]]:
    """Fetch a single section page and return (NodeText | None, Addendum | None).

    Page layout under #BOTTOM's previous sibling:
      div.lr-f-all
        div (breadcrumb + nav)
        div.norm
          p.norm ...  (section body paragraphs)
          div.foot    (history / source note)
    """
    try:
        soup = get_url_as_soup(url)
    except Exception as exc:
        print(f"[WARN] Could not fetch section {url}: {exc}", flush=True)
        return None, None

    bottom = soup.find(id="BOTTOM")
    if bottom is None:
        return None, None

    outer = bottom.find_previous_sibling()
    if outer is None:
        return None, None

    # The first direct child holds both nav and the norm div.
    first_child = outer.find(recursive=False)
    if first_child is None:
        return None, None

    norm_div = first_child.find("div", class_="norm")
    if norm_div is None:
        return None, None

    node_text = NodeText()
    history_text = ""

    for element in norm_div.find_all(recursive=False):
        tag_name = element.name
        cls_list = element.get("class", [])

        if tag_name == "div" and "foot" in cls_list:
            raw = element.get_text(separator=" ")
            history_text = _clean_text(raw)
            # Strip leading separator dashes
            history_text = re.sub(r"^[\-\xad\s]+", "", history_text).strip()
            continue

        if tag_name == "p":
            text = _clean_text(element.get_text(separator=" "))
            if text:
                node_text.add_paragraph(text=text)

    addendum: Optional[Addendum] = None
    if history_text:
        addendum = Addendum()
        addendum.history = AddendumType(type="history", text=history_text)

    return node_text, addendum


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _check_reserved(text: str) -> Optional[str]:
    """Return 'reserved' if the text contains a reserved/repealed keyword."""
    lower = text.lower()
    for kw in RESERVED_KEYWORDS:
        if kw in lower:
            return "reserved"
    return None


def _clean_text(raw: str) -> str:
    """Normalize whitespace, remove non-breaking and em-space characters."""
    text = raw.replace("\xa0", " ").replace(" ", " ").replace(" ", " ")
    text = text.replace("\xad", "")  # soft hyphen
    text = re.sub(r"\s+", " ", text).strip()
    return text


# ---------------------------------------------------------------------------
# CLI entry
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    main()
