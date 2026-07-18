"""
Tennessee Code Annotated (TCA) scraper.

Source: FindLaw (https://codes.findlaw.com/tn/)
Hierarchy: us/tn/statutes/title=N/chapter=M/section=S
Citation format: Tenn. Code Ann. § {title}-{chapter}-{section}
"""

import json
import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
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

from src.utils.pydanticModels import Addendum, AddendumType, Node, NodeText
from src.utils.scrapingHelpers import (
    get_url_as_soup,
    insert_jurisdiction_and_corpus_node,
    insert_node,
)

COUNTRY = "us"
JURISDICTION = "tn"
CORPUS = "statutes"
TABLE_NAME = f"{COUNTRY}_{JURISDICTION}_{CORPUS}"

BASE_URL = "https://codes.findlaw.com"
TOC_URL = f"{BASE_URL}/tn/"

RESERVED_KEYWORDS = [
    "(repealed)",
    "(expired)",
    "(reserved)",
    "(renumbered)",
    "(deleted)",
    "reserved.",
]

# Boilerplate div classes to skip when collecting paragraphs.
_SKIP_DIV_CLASSES = {"codes-controls", "cite-this-article", "wasThisHelpful"}


def main() -> None:
    corpus_node: Node = insert_jurisdiction_and_corpus_node(COUNTRY, JURISDICTION, CORPUS)
    scrape_all_titles(corpus_node)


# ---------------------------------------------------------------------------
# Top level: title list
# ---------------------------------------------------------------------------


def scrape_all_titles(corpus_node: Node) -> None:
    """Enumerate titles from the FindLaw TOC and run them in parallel.

    Each title is an independent subtree (title page -> chapters -> sections)
    sharing no state with its siblings, so titles fan out to a
    ThreadPoolExecutor sized by VAQUILL_TITLE_WORKERS (default 8, matching the
    other FindLaw scrapers: MS, NJ, NM, WY). codes.findlaw.com is a shared
    upstream host, so we stay at the same worker count the siblings already run
    against it rather than pushing it higher.

    Title is the outermost parallel level and also the innermost safe one:
    within a chapter, scrape_chapter walks sections by following "Next" links,
    a linked list whose next URL is only known after fetching the current page.
    That walk cannot be parallelized.

    NOTE: TN deliberately has no titles_done resume file -- every run re-crawls
    in full. That is what lets an amended section be re-fetched and re-chunked
    into a fresh content-addressed point_id. Do not add a titles_done skip here
    to save time without replacing that freshness some other way -- it would
    make TN amendment-blind.
    """
    soup = get_url_as_soup(TOC_URL)

    work: list[tuple[Node, str]] = []
    for link_tag in soup.find_all("a", href=True):
        href = link_tag["href"].strip()
        # e.g. https://codes.findlaw.com/tn/title-1-code-and-statutes/
        m = re.match(r"https://codes\.findlaw\.com/tn/(title-(\d+[a-zA-Z]?)-[^/]+)/?$", href)
        if not m:
            continue

        title_slug = m.group(1)   # title-1-code-and-statutes
        title_num = m.group(2)    # 1

        node_name = _clean_text(link_tag.get_text())
        if not node_name:
            continue

        node_id = f"{corpus_node.node_id}/title={title_num}"
        status = _check_reserved(node_name)

        title_node = Node(
            id=node_id,
            link=href,
            top_level_title=title_num,
            node_type="structure",
            level_classifier="title",
            number=title_num,
            node_name=node_name,
            parent=corpus_node.node_id,
            status=status,
        )
        insert_node(title_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)

        if not status:
            work.append((title_node, title_slug))

    def _do_title(item: tuple[Node, str]):
        # One title's failure must not abort the other workers, so each is
        # wrapped and reported; the run continues with the remaining titles.
        title_node, title_slug = item
        try:
            scrape_title(title_node, title_slug)
            return (title_node.number, "ok", None)
        except Exception as exc:
            return (title_node.number, "fail", f"{type(exc).__name__}: {exc}"[:200])

    workers = int(os.environ.get("VAQUILL_TITLE_WORKERS", "8"))
    print(
        f"[scrapeTN] running {len(work)} titles with {workers} parallel workers",
        flush=True,
    )
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for fut in as_completed(ex.submit(_do_title, item) for item in work):
            num, status, err = fut.result()
            if status == "fail":
                print(f"[scrapeTN] title {num}: {status}: {err}", flush=True)
            else:
                print(f"[scrapeTN] title {num}: {status}", flush=True)


# ---------------------------------------------------------------------------
# Title: extract chapter list via categoriesContent JS variable
# ---------------------------------------------------------------------------


def scrape_title(title_node: Node, title_slug: str) -> None:
    title_url = f"{BASE_URL}/tn/{title_slug}/"
    soup = get_url_as_soup(title_url)

    chapters = _extract_categories(soup)
    if not chapters:
        print(f"[WARN] No chapters found for title {title_node.number}", flush=True)
        return

    for ch_title in chapters:
        ch_num = _chapter_number_from_title(ch_title)
        if ch_num is None:
            continue

        node_id = f"{title_node.node_id}/chapter={ch_num}"
        status = _check_reserved(ch_title)

        # Build the first-section URL for this chapter.
        # TN FindLaw section pattern: tn-code-sect-{title}-{chapter}-{section}
        # Sections within a chapter start at X01 (e.g. 1-1-101, 2-3-301).
        first_sec_url = (
            f"{BASE_URL}/tn/{title_slug}/"
            f"tn-code-sect-{title_node.number}-{ch_num}-101/"
        )

        chapter_node = Node(
            id=node_id,
            link=first_sec_url,
            top_level_title=title_node.top_level_title,
            node_type="structure",
            level_classifier="chapter",
            number=ch_num,
            node_name=ch_title,
            parent=title_node.node_id,
            status=status,
        )
        insert_node(chapter_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)

        if not status:
            scrape_chapter(chapter_node, title_slug)


# ---------------------------------------------------------------------------
# Chapter: walk sections by following "Next" links
# ---------------------------------------------------------------------------


def scrape_chapter(chapter_node: Node, title_slug: str) -> None:
    ch_num = chapter_node.number

    current_url: Optional[str] = str(chapter_node.link)
    visited: set[str] = set()

    while current_url:
        norm = _normalise_url(current_url)
        if norm in visited:
            break
        visited.add(norm)

        # Verify this URL still belongs to the same title (slug guard).
        if f"/tn/{title_slug}/" not in norm:
            break

        # Extract the section number from the URL.
        sec_num = _section_number_from_url(norm)
        if sec_num is None:
            break

        # Stop if we've crossed into a different chapter (different middle segment).
        sec_parts = sec_num.split("-")
        if len(sec_parts) >= 2 and sec_parts[1].upper() != ch_num.upper():
            break

        try:
            soup = get_url_as_soup(norm)
        except Exception as exc:
            print(f"[SKIP] {norm}: {exc}", flush=True)
            break

        main_el = soup.find("main")
        if main_el is None:
            break

        node_name, node_text, addendum = _parse_section_content(main_el, sec_num)
        status = _check_reserved(node_name or "")
        citation = f"Tenn. Code Ann. § {sec_num}"
        node_id = f"{chapter_node.node_id}/section={sec_num}"

        section_node = Node(
            id=node_id,
            link=norm,
            top_level_title=chapter_node.top_level_title,
            node_type="content",
            level_classifier="section",
            number=sec_num,
            node_name=node_name or f"§ {sec_num}",
            parent=chapter_node.node_id,
            citation=citation,
            node_text=node_text if not status else None,
            addendum=addendum if not status else None,
            status=status,
        )
        insert_node(section_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)

        current_url = _get_next_url(main_el, title_slug)


# ---------------------------------------------------------------------------
# Content parsers
# ---------------------------------------------------------------------------


def _parse_section_content(
    main_el: BeautifulSoup,
    sec_num: str,
) -> tuple[Optional[str], Optional[NodeText], Optional[Addendum]]:
    """Return (node_name, node_text, addendum) from a FindLaw TN section page."""
    h1 = main_el.find("h1")
    node_name: Optional[str] = None
    if h1:
        raw = h1.get_text(separator=" ")
        node_name = _clean_text(raw)
        # Strip the long prefix, e.g. "Tennessee Code Title 1. Code and Statutes § 1-1-101"
        # Keep only the short name after the section symbol if present.
        m = re.search(r"§\s*[\d\-A-Za-z\.]+\s*(.*)", node_name)
        if m and m.group(1).strip():
            node_name = f"§ {sec_num}. {m.group(1).strip()}"
        else:
            node_name = f"§ {sec_num}"

    codes_div = main_el.find("div", class_="codes-content")
    if codes_div is None:
        return node_name, None, None

    node_text = NodeText()
    history_parts: list[str] = []

    for el in codes_div.find_all(True, recursive=False):
        el_classes = set(el.get("class", []))

        # Skip navigation, citation boilerplate divs.
        if el_classes & _SKIP_DIV_CLASSES:
            continue

        # Paragraphs live inside div.subsection; grab their text.
        if "subsection" in el_classes:
            raw = el.get_text(separator=" ")
            text = _clean_text(raw)
            if not text:
                continue
            if _looks_like_history(text):
                history_parts.append(text)
            else:
                node_text.add_paragraph(text=text)
            continue

        # Handle loose <p> tags at the top level of codes-content.
        if el.name == "p":
            raw = el.get_text(separator=" ")
            text = _clean_text(raw)
            if not text:
                continue
            if text.startswith("FindLaw Codes may not reflect") or text.lower().startswith("cite this"):
                continue
            if _looks_like_history(text):
                history_parts.append(text)
            else:
                node_text.add_paragraph(text=text)

    if not node_text.paragraphs:
        node_text = None

    addendum: Optional[Addendum] = None
    if history_parts:
        addendum = Addendum()
        addendum.history = AddendumType(
            type="history", text=" ".join(history_parts)
        )

    return node_name, node_text, addendum


def _get_next_url(main_el: BeautifulSoup, title_slug: str) -> Optional[str]:
    """Return the URL of the 'Next' section link, or None if not found or out of title."""
    for a in main_el.find_all("a", href=True):
        txt = a.get_text().strip()
        href = a["href"].strip()
        if "Next" in txt and "findlaw.com/tn/" in href:
            # Only follow if still within the same title slug.
            if f"/tn/{title_slug}/" in href:
                return _normalise_url(href)
    return None


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------


def _extract_categories(soup: BeautifulSoup) -> list[str]:
    """
    Return chapter title strings from the categoriesContent JS variable
    embedded in the title page.
    """
    for script in soup.find_all("script"):
        txt = script.string or ""
        m = re.search(r"categoriesContent\s*=\s*(\[.*?\]);", txt, re.DOTALL)
        if m:
            try:
                data = json.loads(m.group(1))
                return [item["title"] for item in data if "title" in item]
            except (json.JSONDecodeError, KeyError):
                pass
    return []


def _chapter_number_from_title(title: str) -> Optional[str]:
    """
    Extract chapter number from titles like 'Chapter 1. Code Commission'
    or 'Chapter 14-A. Something'.
    """
    m = re.match(r"Chapter\s+([\d]+[A-Za-z]?)[.\s]", title, re.IGNORECASE)
    if m:
        return m.group(1)
    return None


def _section_number_from_url(url: str) -> Optional[str]:
    """
    Extract section number from a FindLaw TN URL.

    Examples:
      .../tn-code-sect-1-1-101.html  -> 1-1-101
      .../tn-code-sect-39-13-101/    -> 39-13-101
      .../tn-code-sect-1-3-5a/       -> 1-3-5A
    """
    m = re.search(r"tn-code-sect-([\d]+[a-zA-Z]*-[\d]+[a-zA-Z]*-[\d]+[a-zA-Z]*)(?:\.html|/?$)", url)
    if not m:
        return None
    raw = m.group(1)
    # Uppercase any trailing alpha suffix on each segment.
    parts = [p.upper() if re.search(r"[a-zA-Z]", p) else p for p in raw.split("-")]
    return "-".join(parts)


def _normalise_url(url: str) -> str:
    url = url.rstrip("/") + "/"
    return url


def _looks_like_history(text: str) -> bool:
    stripped = text.strip()
    if stripped.startswith("History:") or stripped.startswith("Acts "):
        return True
    if re.match(r"^\d{4}\s+(Pub\. Acts|Acts|c\.|No\.)", stripped):
        return True
    return False


def _check_reserved(text: str) -> Optional[str]:
    lower = text.lower()
    for kw in RESERVED_KEYWORDS:
        if kw in lower:
            return "reserved"
    return None


def _clean_text(raw: str) -> str:
    text = raw.replace("\xa0", " ").replace(" ", " ").replace("’", "'")
    text = re.sub(r"\s+", " ", text).strip()
    return text


if __name__ == "__main__":
    main()
