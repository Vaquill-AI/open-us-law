"""Mississippi Code Annotated scraper.

Source: codes.findlaw.com/ms  (FindLaw mirror, direct HTTP, no Selenium)

Navigation strategy:
  - Title list from TOC page (raw-HTML regex over codes.findlaw.com/ms/ links)
  - Chapter list from embedded ``categoriesContent`` JS on each title page
  - Section list by following the "Next" pagination link starting from the
    first section found via a probe loop (sections are not always numbered
    consecutively from 1)

Citation format: ``Miss. Code Ann. § {title}-{chapter}-{section}``
Node-ID hierarchy: ``us/ms/statutes/title={N}/chapter={M}/section={S}``
"""

from __future__ import annotations

import json
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

from src.utils.pydanticModels import Addendum, AddendumType, Node, NodeText
from src.utils.scrapingHelpers import (
    get_url_as_soup,
    insert_jurisdiction_and_corpus_node,
    insert_node,
)

COUNTRY = "us"
JURISDICTION = "ms"
CORPUS = "statutes"
TABLE_NAME = f"{COUNTRY}_{JURISDICTION}_{CORPUS}"

BASE_URL = "https://codes.findlaw.com"
TOC_URL = f"{BASE_URL}/ms/"

RESERVED_KEYWORDS = [
    "repealed",
    "reserved",
    "expired",
    "renumbered",
    "deleted",
    "vacant",
    "blank",
]

# Boilerplate div classes to skip when extracting paragraph text
_SKIP_CLASSES = {"codes-controls", "cite-this-article", "wasThisHelpful"}

# Section probe limits. FindLaw exposes no per-chapter TOC of section
# numbers (categoriesContent gives chapter titles only and Cloudflare blocks
# scraping any official MS index), so we probe candidate section numbers and
# rely on the "Next" link to walk the rest. We sweep 1..PROBE_HARD_CEILING
# but bail as soon as we see PROBE_CONSEC_MISSES consecutive 404s, so
# chapters whose first section number is high (e.g. § 97-29-101) are found
# while wasted requests stay bounded.
_PROBE_HARD_CEILING = 600
_PROBE_CONSEC_MISSES = 40


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    corpus_node: Node = insert_jurisdiction_and_corpus_node(COUNTRY, JURISDICTION, CORPUS)
    scrape_all_titles(corpus_node)


# ---------------------------------------------------------------------------
# Title list
# ---------------------------------------------------------------------------


def _titles_done_path():
    """Where we persist the set of titles already fully scraped for resume."""
    from vaquill_pipeline.config import SETTINGS
    return SETTINGS.chunks_dir / "state_ms_titles_done.txt"


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


def scrape_all_titles(corpus_node: Node) -> None:
    """Walk the MS TOC and scrape every title in parallel.

    Titles are independent at the persistence layer (insert_node is
    idempotent and serialized by the chunk sink), so we fan them out to a
    ``ThreadPoolExecutor``. Concurrency is set via env
    ``VAQUILL_TITLE_WORKERS`` (default 8). Completed titles are persisted
    to ``state_ms_titles_done.txt`` so re-runs skip them; set
    ``VAQUILL_FORCE_RESCRAPE=1`` to override.
    """
    import os
    from concurrent.futures import ThreadPoolExecutor, as_completed

    from vaquill_pipeline.http_client import fetch_html

    html = fetch_html(TOC_URL)

    # Title links: https://codes.findlaw.com/ms/title-{N}-{slug}/
    title_urls = list(
        dict.fromkeys(
            re.findall(r"https://codes\.findlaw\.com/ms/title-[^\"\s<>]+/", html)
        )
    )

    titles_done = (
        set() if os.environ.get("VAQUILL_FORCE_RESCRAPE") else _load_titles_done()
    )
    if titles_done:
        print(
            f"[scrapeMS] resume: {len(titles_done)} titles already done",
            flush=True,
        )

    work: list[Node] = []
    for title_url in title_urls:
        title_number = _title_number_from_url(title_url)
        if title_number is None:
            continue

        title_name = _slug_to_name(_title_slug(title_url), title_number, "Title")
        node_id = f"{corpus_node.node_id}/title={title_number}"
        status = _check_reserved(title_name)

        title_node = Node(
            id=node_id,
            link=title_url,
            top_level_title=title_number,
            node_type="structure",
            level_classifier="title",
            number=title_number,
            node_name=title_name,
            parent=corpus_node.node_id,
            status=status,
        )
        insert_node(title_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)

        if status:
            continue
        if title_number in titles_done:
            continue
        work.append(title_node)

    def _do_title(tn: Node):
        try:
            scrape_title(tn)
            _mark_title_done(tn.number)
            return (tn.number, "ok", None)
        except Exception as e:
            return (tn.number, "fail", str(e)[:200])

    workers = int(os.environ.get("VAQUILL_TITLE_WORKERS", "8"))
    print(
        f"[scrapeMS] running {len(work)} titles with {workers} parallel workers",
        flush=True,
    )
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for fut in as_completed(ex.submit(_do_title, tn) for tn in work):
            num, st, err = fut.result()
            if st == "fail":
                print(f"[scrapeMS] title {num}: fail: {err}", flush=True)
            else:
                print(f"[scrapeMS] title {num}: ok", flush=True)


# ---------------------------------------------------------------------------
# Chapter list (via categoriesContent JS)
# ---------------------------------------------------------------------------


def scrape_title(title_node: Node) -> None:
    """Fetch a title page, extract chapters from JS, walk each chapter."""
    from vaquill_pipeline.http_client import fetch_html

    title_url = str(title_node.link)
    html = fetch_html(title_url)
    chapters = _extract_categories(html)

    if not chapters:
        # No categoriesContent: treat title as a single flat chapter
        first_url = _probe_first_section(
            title_url, title_node.number, title_node.number, "1"
        )
        if first_url:
            _walk_sections(title_node, title_node.number, title_url, first_url)
        return

    for ch_title_str in chapters:
        ch_number = _chapter_number_from_title(ch_title_str)
        if ch_number is None:
            continue

        node_id = f"{title_node.node_id}/chapter={ch_number}"
        status = _check_reserved(ch_title_str)

        chapter_node = Node(
            id=node_id,
            link=title_url,
            top_level_title=title_node.top_level_title,
            node_type="structure",
            level_classifier="chapter",
            number=ch_number,
            node_name=ch_title_str,
            parent=title_node.node_id,
            status=status,
        )
        insert_node(chapter_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)

        if not status:
            first_url = _probe_first_section(
                title_url, title_node.number, ch_number, "1"
            )
            if first_url:
                _walk_sections(chapter_node, title_node.number, title_url, first_url)


# ---------------------------------------------------------------------------
# Section walker
# ---------------------------------------------------------------------------


def _walk_sections(
    parent_node: Node,
    title_number: str,
    title_url: str,
    start_url: str,
) -> None:
    """Follow next-section links from start_url, stopping on title boundary."""
    title_path_fragment = f"/ms/title-{title_number.lower()}-"

    current_url: Optional[str] = _normalise_url(start_url)
    visited: set[str] = set()

    while current_url:
        norm = _normalise_url(current_url)

        if norm in visited:
            break
        visited.add(norm)

        # Stop when we leave this title's URL space
        if title_path_fragment not in norm and f"/ms/title-{title_number.lower()}/" not in norm:
            break

        try:
            soup = get_url_as_soup(norm)
        except Exception as exc:
            print(f"[SKIP] {norm}: {exc}", flush=True)
            break

        main_el = soup.find("main")
        if main_el is None:
            break

        # Detect 404 pages
        h1 = main_el.find("h1")
        if h1 and ("404" in h1.get_text() or "not found" in h1.get_text().lower()):
            break

        sec_number = _section_number_from_url(norm, title_number)
        if sec_number is None:
            break

        citation = f"Miss. Code Ann. § {title_number}-{sec_number}"
        node_name, node_text, addendum = _parse_section(main_el, title_number, sec_number)
        status = _check_reserved(node_name or "")

        node_id = f"{parent_node.node_id}/section={sec_number}"

        section_node = Node(
            id=node_id,
            link=norm,
            top_level_title=parent_node.top_level_title,
            node_type="content",
            level_classifier="section",
            number=sec_number,
            node_name=node_name or f"Miss. Code Ann. § {title_number}-{sec_number}",
            parent=parent_node.node_id,
            citation=citation,
            node_text=node_text if not status else None,
            addendum=addendum if not status else None,
            status=status,
        )
        insert_node(section_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)

        current_url = _get_next_url(main_el)


# ---------------------------------------------------------------------------
# Content parser
# ---------------------------------------------------------------------------


def _parse_section(
    main_el: BeautifulSoup,
    title_number: str,
    sec_number: str,
) -> tuple[Optional[str], Optional[NodeText], Optional[Addendum]]:
    """Extract node_name, NodeText, and Addendum from a FindLaw section page."""
    h1 = main_el.find("h1")
    node_name = _clean_text(h1.get_text()) if h1 else None
    if node_name:
        # Strip long prefix: "Mississippi Code Title N. <Name> § N-M-P"
        node_name = re.sub(
            r"^Mississippi Code\s+Title\s+[\d\w]+\.\s+.*?§\s*[\d\w\-\.]+\s*",
            "",
            node_name,
        ).strip()
        if not node_name:
            node_name = f"Miss. Code Ann. § {title_number}-{sec_number}"

    codes_div = main_el.find("div", class_="codes-content")
    if codes_div is None:
        return node_name, None, None

    node_text = NodeText()
    history_parts: list[str] = []

    for p in codes_div.find_all("p", recursive=True):
        if _should_skip_paragraph(p):
            continue
        raw = p.get_text(separator=" ")
        text = _clean_text(raw)
        if not text:
            continue
        if text.startswith("FindLaw Codes may not reflect"):
            continue
        if _looks_like_history(text):
            history_parts.append(text)
        else:
            node_text.add_paragraph(text=text)

    final_text: Optional[NodeText] = node_text if node_text.paragraphs else None

    addendum: Optional[Addendum] = None
    if history_parts:
        addendum = Addendum()
        addendum.history = AddendumType(type="history", text=" ".join(history_parts))

    return node_name, final_text, addendum


def _should_skip_paragraph(p: BeautifulSoup) -> bool:
    return any(
        set(ancestor.get("class", [])) & _SKIP_CLASSES
        for ancestor in p.parents
        if hasattr(ancestor, "get")
    )


def _get_next_url(main_el: BeautifulSoup) -> Optional[str]:
    for a in main_el.find_all("a", href=True):
        txt = a.get_text().strip()
        href = a["href"].strip()
        if "Next" in txt and "findlaw.com" in href and "/ms/" in href:
            return _normalise_url(href)
    return None


# ---------------------------------------------------------------------------
# URL construction and probing
# ---------------------------------------------------------------------------


def _build_section_url(title_url: str, title_number: str, chapter: str, section: str) -> str:
    """
    Construct the FindLaw URL for a section.

    Pattern: ms-code-sect-{title}-{chapter}-{section}
    Examples:
      title=97, chapter=1, section=1  => .../ms-code-sect-97-1-1/
      title=1,  chapter=1, section=7  => .../ms-code-sect-1-1-7/
    """
    t = title_number.lower()
    c = chapter.lower()
    s = section.lower()
    slug = f"ms-code-sect-{t}-{c}-{s}"
    base = title_url.rstrip("/")
    return f"{base}/{slug}/"


def _probe_first_section(
    title_url: str,
    title_number: str,
    chapter: str,
    start_section: str = "1",
) -> Optional[str]:
    """
    Find the first valid section URL for a given title/chapter by probing
    consecutive section numbers (1, 2, 3, ...). MS Code chapters frequently
    begin at non-1 numbers (e.g. § 97-29-101) so a hardcoded probe cap of
    51 silently dropped them. We now sweep up to ``_PROBE_HARD_CEILING``
    and bail only after ``_PROBE_CONSEC_MISSES`` consecutive 404s.
    """
    misses = 0
    for n in range(1, _PROBE_HARD_CEILING + 1):
        url = _build_section_url(title_url, title_number, chapter, str(n))
        try:
            soup = get_url_as_soup(url)
        except Exception:
            misses += 1
            if misses >= _PROBE_CONSEC_MISSES:
                return None
            continue
        main_el = soup.find("main")
        if main_el is None:
            misses += 1
            if misses >= _PROBE_CONSEC_MISSES:
                return None
            continue
        h1 = main_el.find("h1")
        if h1 and ("404" in h1.get_text() or "not found" in h1.get_text().lower()):
            misses += 1
            if misses >= _PROBE_CONSEC_MISSES:
                return None
            continue
        # Valid section page found
        return _normalise_url(url)

    return None


# ---------------------------------------------------------------------------
# Helper utilities
# ---------------------------------------------------------------------------


def _extract_categories(html: str) -> list[str]:
    """Return chapter title strings from the categoriesContent JS variable."""
    m = re.search(r"let categoriesContent\s*=\s*(\[.*?\]);", html, re.DOTALL)
    if not m:
        return []
    try:
        data = json.loads(m.group(1))
        return [item["title"] for item in data if "title" in item]
    except (json.JSONDecodeError, KeyError):
        return []


def _chapter_number_from_title(title: str) -> Optional[str]:
    """
    Extract chapter number from a category title string.

    Examples:
      'Chapter 1. Code of 1972'  -> '1'
      'Chapter 3A. Miscellaneous' -> '3A'
    """
    m = re.match(r"Chapter\s+([\dA-Za-z]+)[.\s]", title)
    if m:
        return m.group(1).upper()
    return None


def _title_number_from_url(url: str) -> Optional[str]:
    """
    Extract title number from a FindLaw MS title URL.

    https://codes.findlaw.com/ms/title-1-laws-and-statutes/  => '1'
    https://codes.findlaw.com/ms/title-11a-something/          => '11A'
    """
    m = re.search(r"/ms/title-([\dA-Za-z]+)-", url)
    if m:
        return m.group(1).upper()
    return None


def _title_slug(url: str) -> str:
    """Extract the full slug segment: 'title-1-laws-and-statutes'."""
    m = re.search(r"/ms/(title-[^/]+)/?", url)
    return m.group(1) if m else ""


def _slug_to_name(slug: str, number: str, level: str) -> str:
    """Convert a URL slug like 'title-1-laws-and-statutes' to 'Title 1. Laws and Statutes'."""
    # Strip the level prefix and number: 'title-1-' or 'chapter-3a-'
    name_part = re.sub(rf"^{level.lower()}-{re.escape(number.lower())}-", "", slug)
    name_part = name_part.replace("-", " ").title()
    return f"{level} {number}. {name_part}".strip()


def _section_number_from_url(url: str, title_number: str) -> Optional[str]:
    """
    Extract the chapter-section portion from a FindLaw MS section URL.

    URL: .../ms-code-sect-97-1-1/    title=97  => '1-1'
    URL: .../ms-code-sect-1-1-7/     title=1   => '1-7'
    URL: .../ms-code-sect-1-3-1/     title=1   => '3-1'

    Returns the chapter-section string (without the title prefix).
    """
    m = re.search(r"ms-code-sect-([\da-zA-Z][-\da-zA-Z.]+?)(?:/|\.html|$)", url)
    if not m:
        return None

    raw = m.group(1)
    title_prefix = title_number.lower()

    if not raw.lower().startswith(title_prefix + "-"):
        return None

    remainder = raw[len(title_prefix) + 1:]
    parts = remainder.split("-")

    # 3 parts: chapter-section-subsection => chapter-section.subsection
    if len(parts) == 3:
        return f"{parts[0]}-{parts[1]}.{parts[2]}"
    # 2 parts: chapter-section (normal case)
    if len(parts) == 2:
        return f"{parts[0]}-{parts[1]}"
    if len(parts) == 1:
        return parts[0]
    return "-".join(parts)


def _normalise_url(url: str) -> str:
    """Ensure URL ends with '/' (FindLaw section URLs accept both forms)."""
    url = url.rstrip("/")
    if not url.endswith(".html"):
        url += "/"
    return url


def _looks_like_history(text: str) -> bool:
    """Identify legislative history lines."""
    stripped = text.strip()
    if re.match(r"^HISTORY:", stripped, re.IGNORECASE):
        return True
    if re.match(r"^Laws\s+\d{4}", stripped):
        return True
    if re.match(r"^\(\s*(Codes\s+\d{4}|Laws\s+\d{4}|\d{4}\s+Code)", stripped):
        return True
    if stripped.startswith("Cross References"):
        return True
    return False


def _check_reserved(text: str) -> Optional[str]:
    lower = text.lower()
    for kw in RESERVED_KEYWORDS:
        if kw in lower:
            return "reserved"
    return None


def _clean_text(raw: str) -> str:
    text = raw.replace("\xa0", " ").replace(" ", " ").replace(" ", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


if __name__ == "__main__":
    main()
