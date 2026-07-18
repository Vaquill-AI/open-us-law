"""Wyoming Statutes Annotated scraper.

Source: FindLaw (https://codes.findlaw.com/wy/)
Hierarchy: us/wy/statutes/title=N/chapter=M/section=S
Citation format: "Wyo. Stat. § T-C-S"

Strategy:
- Enumerate titles from the FindLaw WY table of contents.
- For each title, extract chapter names from the embedded `categoriesContent` JS variable.
- Walk all sections within the title via "Next Part of Code" navigation links, starting
  from the first reachable section (probing wy-st-sect-{T}-1-{N} where N in 101..130).
- Infer the current chapter from the URL segment (title-chapter-section).
- Parse section content from the `.codes-content` div; collect history addendum.
"""

import json
import os
import re
import sys
import time
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
JURISDICTION = "wy"
CORPUS = "statutes"
TABLE_NAME = f"{COUNTRY}_{JURISDICTION}_{CORPUS}"

BASE_URL = "https://codes.findlaw.com"
TOC_URL = f"{BASE_URL}/wy/"

RESERVED_KEYWORDS = [
    "(reserved)",
    "(repealed)",
    "(expired)",
    "(renumbered)",
    "(deleted)",
    "reserved.",
    "repealed.",
]

# First-section probe bounds. Some titles reserve chapter 1 entirely, or the
# first numbered section is past 130 (e.g. recodified titles), so we sweep
# chapters 1..5 and section offsets 1..200, bailing only after a long run of
# consecutive 404s.
_FIRST_SECTION_PROBE_CHAPTERS = (1, 2, 3, 4, 5)
_FIRST_SECTION_PROBE_MAX = 200
_FIRST_SECTION_PROBE_MIN = 1
_FIRST_SECTION_404_STREAK = 30

# Boilerplate CSS classes to skip inside codes-content.
_SKIP_CLASSES = {"codes-controls", "cite-this-article", "wasThisHelpful"}


def main() -> None:
    # Install the vaquill_pipeline patch (R2 sync, JsonlSink, etc.) if present.
    try:
        from vaquill_pipeline.patch import install as _install_patch  # type: ignore
        _install_patch()
    except Exception as _exc:
        print(f"[scrapeWY] vaquill_pipeline.patch not installed: {_exc}", flush=True)
    corpus_node: Node = insert_jurisdiction_and_corpus_node(COUNTRY, JURISDICTION, CORPUS)
    scrape_all_titles(corpus_node)


# ---------------------------------------------------------------------------
# Title-level resume
# ---------------------------------------------------------------------------


def _titles_done_path():
    """Where we persist the set of titles already fully scraped for resume."""
    from vaquill_pipeline.config import SETTINGS  # type: ignore
    return SETTINGS.chunks_dir / "state_wy_titles_done.txt"


def _load_titles_done() -> set:
    try:
        path = _titles_done_path()
    except Exception:
        return set()
    if not path.exists():
        return set()
    return {ln.strip() for ln in path.read_text().splitlines() if ln.strip()}


def _mark_title_done(number: str) -> None:
    try:
        path = _titles_done_path()
    except Exception:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a") as fh:
        fh.write(f"{number}\n")
        fh.flush()


# ---------------------------------------------------------------------------
# Top-level: title list
# ---------------------------------------------------------------------------


def scrape_all_titles(corpus_node: Node) -> None:
    """Enumerate titles and fan out title-level scraping across a thread pool.

    Each title is independent (no shared state at the title level), so we hand
    them to a ThreadPoolExecutor. Concurrency: env ``VAQUILL_TITLE_WORKERS``
    (default 8). Resume: titles already in ``state_wy_titles_done.txt`` are
    skipped entirely; set ``VAQUILL_FORCE_RESCRAPE=1`` to override.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    titles_done = set() if os.environ.get("VAQUILL_FORCE_RESCRAPE") else _load_titles_done()
    if titles_done:
        print(f"[scrapeWY] resume: {len(titles_done)} titles already done", flush=True)

    soup = get_url_as_soup(TOC_URL)

    # Build the work list sequentially (cheap, pure parse) so we can insert
    # title structure nodes up front and dedupe by title number.
    work: list[tuple[Node, str, str]] = []
    seen_titles: set[str] = set()
    for link_tag in soup.find_all("a", href=True):
        href = link_tag["href"].strip()
        m = re.match(r"https://codes\.findlaw\.com/wy/(title-[\w.-]+)/?$", href)
        if not m:
            continue

        title_slug = m.group(1)
        title_num = _title_number_from_slug(title_slug)
        if title_num is None:
            continue
        if title_num in seen_titles:
            continue
        seen_titles.add(title_num)

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

        if status:
            continue
        if title_num in titles_done:
            continue
        work.append((title_node, title_slug, title_num))

    def _do_title(item: tuple[Node, str, str]):
        tnode, tslug, tnum = item
        try:
            scrape_title(tnode, tslug)
            _mark_title_done(tnum)
            return (tnum, "ok", None)
        except Exception as exc:
            return (tnum, "fail", str(exc)[:200])

    workers = int(os.environ.get("VAQUILL_TITLE_WORKERS", "8"))
    print(f"[scrapeWY] running {len(work)} titles with {workers} parallel workers", flush=True)
    if workers <= 1 or len(work) <= 1:
        for item in work:
            num, status_, err = _do_title(item)
            print(f"[scrapeWY] title {num}: {status_}{(': ' + err) if err else ''}", flush=True)
        return
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [ex.submit(_do_title, item) for item in work]
        for fut in as_completed(futures):
            num, status_, err = fut.result()
            print(f"[scrapeWY] title {num}: {status_}{(': ' + err) if err else ''}", flush=True)


# ---------------------------------------------------------------------------
# Title: build chapter map, find first section, walk all sections
# ---------------------------------------------------------------------------


def scrape_title(title_node: Node, title_slug: str) -> None:
    title_url = f"{BASE_URL}/wy/{title_slug}/"
    try:
        soup = get_url_as_soup(title_url)
    except Exception as exc:
        print(f"[SKIP title] {title_url}: {exc}", flush=True)
        return

    # Build chapter name map from the embedded categoriesContent JS variable.
    chapter_map = _extract_chapter_map(soup)

    # Insert chapter structure nodes.
    chapter_nodes: dict[str, Node] = {}
    for ch_num, ch_name in chapter_map.items():
        ch_node_id = f"{title_node.node_id}/chapter={ch_num}"
        status = _check_reserved(ch_name)
        ch_node = Node(
            id=ch_node_id,
            link=title_url,
            top_level_title=title_node.top_level_title,
            node_type="structure",
            level_classifier="chapter",
            number=ch_num,
            node_name=ch_name,
            parent=title_node.node_id,
            status=status,
        )
        insert_node(ch_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)
        chapter_nodes[ch_num] = ch_node

    # Find the first reachable section URL in this title.
    title_num_raw = title_node.top_level_title
    first_url = _find_first_section_url(title_num_raw, title_slug)
    if first_url is None:
        print(f"[WARN] Could not find first section for title {title_num_raw}", flush=True)
        return

    # Walk all sections via "Next Part of Code" links, staying within this title.
    _walk_sections(title_node, title_slug, chapter_nodes, chapter_map, first_url)


# ---------------------------------------------------------------------------
# Section walker
# ---------------------------------------------------------------------------


def _walk_sections(
    title_node: Node,
    title_slug: str,
    chapter_nodes: dict[str, Node],
    chapter_map: dict[str, str],
    first_url: str,
) -> None:
    current_url: Optional[str] = first_url
    visited: set[str] = set()

    while current_url:
        norm = _normalise_url(current_url)

        if norm in visited:
            break
        visited.add(norm)

        # Stop if we have wandered outside this title's URL space.
        if f"/wy/{title_slug}/" not in norm:
            break

        # Fetch with one retry + backoff before giving up on this URL.
        soup = None
        last_exc: Optional[BaseException] = None
        for attempt in range(2):
            try:
                soup = get_url_as_soup(norm)
                break
            except Exception as exc:
                last_exc = exc
                if attempt == 0:
                    time.sleep(1.5)
        if soup is None:
            print(f"[SKIP][WY] HTTP fail (giving up after retry) {norm}: {last_exc}", flush=True)
            # We have no Next link to follow without a parsed page; stop
            # this chain. Title-level resume + outer retries pick this up.
            break

        main_el = soup.find("main")
        if main_el is None:
            # No <main>: cannot navigate further from this page.
            print(f"[SKIP][WY] no <main> on {norm}", flush=True)
            break

        # Check for a valid section URL pattern. On parse failure, do NOT
        # abort the title -- try to follow the Next link and continue.
        sec_info = _parse_section_url(norm)
        if sec_info is None:
            print(f"[WARN][WY] unparseable section URL, continuing: {norm}", flush=True)
            current_url = _get_next_url(main_el, title_slug)
            continue

        title_num_url, chapter_num_url, sec_number = sec_info

        # Determine the parent chapter node, creating it on-the-fly if unknown.
        ch_node = _get_or_create_chapter_node(
            chapter_num_url, title_node, chapter_nodes, chapter_map
        )

        # Skip sections whose chapter is marked reserved.
        if ch_node.status:
            current_url = _get_next_url(main_el, title_slug)
            continue

        node_name, node_text, addendum = _parse_section_content(main_el, sec_number)
        status = _check_reserved(node_name) if node_name else None

        # A page without codes-content is a placeholder/redirect; skip content but
        # still attempt to follow the Next link so we do not lose the chain.
        codes_div = main_el.find("div", class_="codes-content")
        if codes_div is None:
            current_url = _get_next_url(main_el, title_slug)
            continue

        citation = f"Wyo. Stat. § {sec_number}"
        node_id = f"{ch_node.node_id}/section={sec_number}"

        section_node = Node(
            id=node_id,
            link=norm,
            top_level_title=title_node.top_level_title,
            node_type="content",
            level_classifier="section",
            number=sec_number,
            node_name=node_name or f"§ {sec_number}",
            parent=ch_node.node_id,
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
    sec_number: str,
) -> tuple[Optional[str], Optional[NodeText], Optional[Addendum]]:
    h1 = main_el.find("h1")
    raw_h1 = _clean_text(h1.get_text()) if h1 else ""
    # Strip the boilerplate prefix: "Wyoming Statutes Title N. ... § S. "
    node_name: Optional[str] = None
    if raw_h1:
        m = re.search(r"§\s*[\d\w.-]+\.\s*(.+)$", raw_h1)
        node_name = m.group(1).strip() if m else raw_h1
        node_name = f"§ {sec_number}. {node_name}" if node_name else f"§ {sec_number}"

    codes_div = main_el.find("div", class_="codes-content")
    if codes_div is None:
        return node_name, None, None

    node_text = NodeText()
    history_parts: list[str] = []

    for p in codes_div.find_all("p", recursive=True):
        # Skip paragraphs inside boilerplate containers.
        skip = any(
            set(ancestor.get("class", [])) & _SKIP_CLASSES
            for ancestor in p.parents
            if hasattr(ancestor, "get")
        )
        if skip:
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

    if not node_text.paragraphs:
        node_text = None  # type: ignore[assignment]

    addendum: Optional[Addendum] = None
    if history_parts:
        addendum = Addendum()
        addendum.history = AddendumType(
            type="history", text=" ".join(history_parts)
        )

    return node_name, node_text, addendum


def _get_next_url(main_el: BeautifulSoup, title_slug: str) -> Optional[str]:
    for a in main_el.find_all("a", href=True):
        txt = a.get_text().strip()
        href = a["href"].strip()
        if "Next Part" in txt and "findlaw.com" in href and f"/wy/{title_slug}/" in href:
            return _normalise_url(href)
    return None


# ---------------------------------------------------------------------------
# First-section finder
# ---------------------------------------------------------------------------


def _find_first_section_url(title_num: str, title_slug: str) -> Optional[str]:
    """Probe for the first reachable section of a title.

    Walk candidate chapters (1..5) and within each chapter sweep section
    offsets 1..200 (and the 100-style numbering 101..200 used by many WY
    titles). Bail out of a chapter only after a streak of consecutive 404s.
    Loud-log if every probe fails so a title is never silently dropped.
    """
    title_prefix = f"{BASE_URL}/wy/{title_slug}/"
    # FindLaw's section URLs use the dotted title form (e.g. 34.1) where
    # applicable; the title_num we receive is already in that form.
    attempted = 0
    for chapter in _FIRST_SECTION_PROBE_CHAPTERS:
        # Try the conventional 100-block first (cheap heuristic, most titles),
        # then fall back to 1..200 for atypically numbered titles.
        ranges = (range(101, _FIRST_SECTION_PROBE_MAX + 1),
                  range(_FIRST_SECTION_PROBE_MIN, 101))
        for rng in ranges:
            streak = 0
            for n in rng:
                attempted += 1
                candidate = f"{title_prefix}wy-st-sect-{title_num}-{chapter}-{n}/"
                try:
                    soup = get_url_as_soup(candidate)
                except Exception:
                    streak += 1
                    if streak >= _FIRST_SECTION_404_STREAK:
                        break
                    continue
                streak = 0
                main_el = soup.find("main")
                if main_el is None:
                    continue
                codes_div = main_el.find("div", class_="codes-content")
                if codes_div is not None:
                    return _normalise_url(candidate)
                nxt = _get_next_url(main_el, title_slug)
                if nxt:
                    return _normalise_url(nxt)
    print(
        f"[WARN][WY] _find_first_section_url EXHAUSTED for title {title_num} "
        f"(slug={title_slug}, attempts={attempted}) -- title will be SKIPPED",
        flush=True,
    )
    return None


# ---------------------------------------------------------------------------
# Chapter helpers
# ---------------------------------------------------------------------------


def _extract_chapter_map(soup: BeautifulSoup) -> dict[str, str]:
    """Return {chapter_num: chapter_name} from the categoriesContent JS variable."""
    for script in soup.find_all("script"):
        txt = script.string or ""
        m = re.search(r"categoriesContent\s*=\s*(\[.*?\]);", txt, re.DOTALL)
        if not m:
            continue
        try:
            data = json.loads(m.group(1))
        except json.JSONDecodeError:
            continue
        result: dict[str, str] = {}
        for item in data:
            title_str = item.get("title", "")
            ch_num = _chapter_number_from_title(title_str)
            if ch_num:
                result[ch_num] = _clean_text(title_str)
        return result
    return {}


def _chapter_number_from_title(title_str: str) -> Optional[str]:
    """Extract chapter number from strings like 'Chapter 2. Oaths' or 'Article 3. ...'."""
    m = re.match(r"(?:Chapter|Article)\s+([\w.]+)[.\s]", title_str, re.IGNORECASE)
    if m:
        return m.group(1).upper()
    return None


def _get_or_create_chapter_node(
    ch_num: str,
    title_node: Node,
    chapter_nodes: dict[str, Node],
    chapter_map: dict[str, str],
) -> Node:
    if ch_num in chapter_nodes:
        return chapter_nodes[ch_num]
    # Create a synthetic chapter node for chapters not listed in categoriesContent
    # (e.g., chapters only found via section walking).
    ch_name = chapter_map.get(ch_num, f"Chapter {ch_num}")
    ch_node_id = f"{title_node.node_id}/chapter={ch_num}"
    ch_node = Node(
        id=ch_node_id,
        link=str(title_node.link),
        top_level_title=title_node.top_level_title,
        node_type="structure",
        level_classifier="chapter",
        number=ch_num,
        node_name=ch_name,
        parent=title_node.node_id,
    )
    insert_node(ch_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)
    chapter_nodes[ch_num] = ch_node
    return ch_node


# ---------------------------------------------------------------------------
# URL utilities
# ---------------------------------------------------------------------------


def _parse_section_url(url: str) -> Optional[tuple[str, str, str]]:
    """
    Parse a WY FindLaw section URL.

    Returns (title_num, chapter_num, section_number) or None.

    Example:
        .../wy/title-1-code-of-civil-procedure/wy-st-sect-1-1-101/
        -> ("1", "1", "1-1-101")
    """
    # Allow `.` (dotted titles like 34.1 UCC), tolerate trailing punctuation
    # (e.g. `.1` suffix on subsection IDs like 6-2-101.1) and optional .html.
    m = re.search(
        r"/wy/title-[\w.-]+/wy-st-sect-([\w.-]+?)/?(?:\.html)?/?$",
        url,
    )
    if not m:
        return None
    raw = m.group(1).strip("-.")  # e.g. "1-1-101", "34.1-1-101", "6-2-101.1"
    parts = raw.split("-")
    if len(parts) < 3:
        return None
    title_num = parts[0]
    chapter_num = parts[1]
    # Section number is the full T-C-S string.
    section_number = raw
    return title_num, chapter_num, section_number


def _title_number_from_slug(slug: str) -> Optional[str]:
    """
    Extract title number from slug.

    Plain titles: 'title-1-code-of-civil-procedure' -> '1'.
    Dotted titles (WY UCC): 'title-34-1-uniform-commercial-code' -> '34.1'
    (the second numeric segment is fused with a dot so Title 34.1 does NOT
    collapse onto Title 34, which would corrupt downstream node ids).
    """
    # Dotted form: title-<N>-<M>-<alpha-rest>  where N and M are digits.
    m_dot = re.match(r"^title-(\d+)-(\d+)-([a-zA-Z].*)$", slug)
    if m_dot:
        return f"{m_dot.group(1)}.{m_dot.group(2)}"
    m = re.match(r"^title-([\w]+)(?:-|$)", slug)
    if m:
        return m.group(1)
    return None


def _normalise_url(url: str) -> str:
    url = url.rstrip("/")
    if url.endswith(".html"):
        return url + "/"
    if not url.endswith("/"):
        url += "/"
    return url


# ---------------------------------------------------------------------------
# Text utilities
# ---------------------------------------------------------------------------


def _looks_like_history(text: str) -> bool:
    stripped = text.strip()
    if stripped.startswith("History:") or stripped.startswith("Source:"):
        return True
    if re.match(r"^\d{4}\s+(Laws|Wyo\.|W\.S\.|Ch\.|Amendment)", stripped):
        return True
    if re.match(r"^Laws\s+\d{4}", stripped):
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
