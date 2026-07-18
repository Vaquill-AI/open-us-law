"""Montana Code Annotated (MCA) scraper.

Hierarchy: us/mt/statutes/title=N/chapter=M/part=P/section=S
Citation:  Mont. Code Ann. § <title>-<chapter>-<section>
Source:    https://mca.legmt.gov/bills/mca/
"""
import os
import re
import sys
from pathlib import Path
from typing import List, Optional, Tuple

from bs4 import BeautifulSoup

current_file = Path(__file__).resolve()
src_directory = current_file.parent
while src_directory.name != "src" and src_directory.parent != src_directory:
    src_directory = src_directory.parent
project_root = src_directory.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.utils.pydanticModels import NodeID, Node, NodeText, Paragraph, Addendum, AddendumType
from src.utils.scrapingHelpers import (
    insert_jurisdiction_and_corpus_node,
    insert_node,
    get_url_as_soup,
)

COUNTRY = "us"
JURISDICTION = "mt"
CORPUS = "statutes"
TABLE_NAME = f"{COUNTRY}_{JURISDICTION}_{CORPUS}"

# Canonical host: leg.mt.gov/bills/mca/ now 301-redirects (twice) to mca.legmt.gov.
# Hardcode the redirect target to avoid silent breakage if the redirect is ever
# turned off or if redirects are stripped by an upstream proxy.
BASE_URL = "https://mca.legmt.gov/bills/mca"
TOC_URL = "https://mca.legmt.gov/bills/mca/"

RESERVED_KEYWORDS = ["reserved", "repealed", "expired", "transferred", "renumbered"]


def main() -> None:
    corpus_node: Node = insert_jurisdiction_and_corpus_node(COUNTRY, JURISDICTION, CORPUS)
    scrape_all_titles(corpus_node)


def _titles_done_path():
    """Where we persist the set of titles already fully scraped for resume."""
    from vaquill_pipeline.config import SETTINGS
    return SETTINGS.chunks_dir / "state_mt_titles_done.txt"


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


def _parse_title_numbers_from_text(text: str) -> List[str]:
    """Parse one or more title numbers from anchor / reserved-span text.

    Examples:
        "TITLE 5. LEGISLATIVE BRANCH"        -> ["5"]
        "TITLE 4. Reserved"                  -> ["4"]
        "TITLES 8 AND 9. Reserved"           -> ["8", "9"]
        "TITLES 11 AND 12. Reserved"         -> ["11", "12"]
        "THE CONSTITUTION OF THE STATE..."   -> []  (no number, caller skips)
    """
    # Multi-title: "TITLES N AND M."
    m = re.match(r"(?i)\s*TITLES\s+(\d+)\s+AND\s+(\d+)\b", text)
    if m:
        return [m.group(1), m.group(2)]
    # Multi-title range: "TITLES N THROUGH M." (defensive; not seen in live DOM today)
    m = re.match(r"(?i)\s*TITLES\s+(\d+)\s+THROUGH\s+(\d+)\b", text)
    if m:
        lo, hi = int(m.group(1)), int(m.group(2))
        return [str(n) for n in range(lo, hi + 1)]
    # Single title: "TITLE N."
    m = re.match(r"(?i)\s*TITLE\s+(\d+)\b", text)
    if m:
        return [m.group(1)]
    return []


def _build_title_work_items(corpus_node: Node) -> List[Tuple[Node, bool]]:
    """Walk the root TOC and emit (title_node, is_reserved) work items.

    Inserts each title structure node up front (idempotent). Reserved titles
    are inserted with status="reserved" and do NOT need further recursion.
    Multi-title reserved spans like "TITLES 8 AND 9. Reserved" are expanded
    into one node per title number.
    """
    soup = get_url_as_soup(TOC_URL)
    toc_nav = soup.find(class_="mca-toc-nav") or soup.find(class_="mca-content mca-toc") or soup
    container = toc_nav.find(class_="title-toc-content") or toc_nav

    items: List[Tuple[Node, bool]] = []
    for li in container.find_all("li"):
        link_tag = li.find("a")
        span_tag = li.find("span", class_="reserved")

        # Anchor-text-first parsing (data-titlenumber not guaranteed; covers
        # multi-title spans correctly).
        raw_text = (link_tag or span_tag or li).get_text()
        node_name = _clean_text(raw_text)
        numbers = _parse_title_numbers_from_text(node_name)
        if not numbers:
            # Skips "THE CONSTITUTION OF THE STATE OF MONTANA" (data-titlenumber=0)
            # and any non-title list items. Constitution is handled separately
            # (or simply omitted, as in the original).
            continue

        is_reserved = span_tag is not None or _check_reserved(node_name) is not None
        if link_tag and link_tag.get("href"):
            link = _resolve_url(TOC_URL, link_tag["href"].strip())
        else:
            link = TOC_URL

        for number in numbers:
            # When expanding a multi-title span, rewrite the human-readable
            # name so each emitted node is unambiguous.
            if len(numbers) > 1:
                per_title_name = f"TITLE {number}. Reserved"
            else:
                per_title_name = node_name

            node_id = f"{corpus_node.node_id}/title={number}"
            title_node = Node(
                id=node_id,
                link=link,
                top_level_title=number,
                node_type="structure",
                level_classifier="title",
                number=number,
                node_name=per_title_name,
                parent=corpus_node.node_id,
                status="reserved" if is_reserved else None,
            )
            insert_node(title_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)
            items.append((title_node, is_reserved))
    return items


def scrape_all_titles(corpus_node: Node) -> None:
    """Walk all MCA titles in parallel.

    Titles are independent at the title level, so we hand them to a
    ``ThreadPoolExecutor``. Concurrency is set by ``VAQUILL_TITLE_WORKERS``
    (default 8). Title-level resume is persisted to
    ``state_mt_titles_done.txt``; set ``VAQUILL_FORCE_RESCRAPE=1`` to override.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    titles_done = set() if os.environ.get("VAQUILL_FORCE_RESCRAPE") else _load_titles_done()
    if titles_done:
        print(
            f"[scrapeMT] resume: {len(titles_done)} titles already done: "
            f"{sorted(titles_done, key=lambda x: int(x) if x.isdigit() else 9999)}",
            flush=True,
        )

    all_items = _build_title_work_items(corpus_node)
    work: List[Node] = []
    for title_node, is_reserved in all_items:
        if is_reserved:
            # Already inserted with status="reserved"; nothing to recurse.
            continue
        if title_node.number in titles_done:
            continue
        work.append(title_node)

    def _do_title(node: Node):
        try:
            scrape_chapters(node)
            _mark_title_done(node.number)
            return (node.number, "ok", None)
        except Exception as e:
            return (node.number, "fail", str(e)[:200])

    workers = int(os.environ.get("VAQUILL_TITLE_WORKERS", "8"))
    print(f"[scrapeMT] running {len(work)} titles with {workers} parallel workers", flush=True)
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for fut in as_completed(ex.submit(_do_title, n) for n in work):
            num, status, err = fut.result()
            if status == "fail":
                print(f"[scrapeMT] title {num}: {status}: {err}", flush=True)
            else:
                print(f"[scrapeMT] title {num}: {status}", flush=True)


def scrape_chapters(title_node: Node) -> None:
    soup = get_url_as_soup(str(title_node.link))
    container = soup.find(class_="chapter-toc-content")
    if container is None:
        return

    title_base = str(title_node.link).rsplit("/", 1)[0]

    for li in container.find_all("li", class_="line"):
        link_tag = li.find("a")
        span_tag = li.find("span", class_="reserved")

        if link_tag and link_tag.get("href"):
            node_name = _clean_text(link_tag.get_text())
            href = link_tag["href"].strip()
            link = _resolve_url(str(title_node.link), href)
            status = _check_reserved(node_name)
        elif span_tag:
            node_name = _clean_text(span_tag.get_text())
            link = str(title_node.link)
            status = "reserved"
        else:
            node_name = _clean_text(li.get_text())
            link = str(title_node.link)
            status = _check_reserved(node_name)

        number = _extract_structure_number(node_name, "chapter")
        if number is None:
            continue

        node_id = f"{title_node.node_id}/chapter={number}"

        chapter_node = Node(
            id=node_id,
            link=link,
            top_level_title=title_node.top_level_title,
            node_type="structure",
            level_classifier="chapter",
            number=number,
            node_name=node_name,
            parent=title_node.node_id,
            status=status,
        )
        insert_node(chapter_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)

        if not status:
            scrape_parts(chapter_node)


def scrape_parts(chapter_node: Node) -> None:
    soup = get_url_as_soup(str(chapter_node.link))
    container = soup.find(class_="part-toc-content")
    if container is None:
        return

    for li in container.find_all("li", class_="heading"):
        link_tag = li.find("a")
        span_tag = li.find("span", class_="reserved")

        if link_tag and link_tag.get("href"):
            node_name = _clean_text(link_tag.get_text())
            href = link_tag["href"].strip()
            link = _resolve_url(str(chapter_node.link), href)
            status = _check_reserved(node_name)
        elif span_tag:
            node_name = _clean_text(span_tag.get_text())
            link = str(chapter_node.link)
            status = "reserved"
        else:
            node_name = _clean_text(li.get_text())
            link = str(chapter_node.link)
            status = _check_reserved(node_name)

        number = _extract_structure_number(node_name, "part")
        if number is None:
            continue

        node_id = f"{chapter_node.node_id}/part={number}"

        part_node = Node(
            id=node_id,
            link=link,
            top_level_title=chapter_node.top_level_title,
            node_type="structure",
            level_classifier="part",
            number=number,
            node_name=node_name,
            parent=chapter_node.node_id,
            status=status,
        )
        insert_node(part_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)

        if not status:
            scrape_sections(part_node)


def scrape_sections(part_node: Node) -> None:
    soup = get_url_as_soup(str(part_node.link))
    container = soup.find(class_="section-toc-content")
    if container is None:
        return

    for li in container.find_all("li", class_="line"):
        link_tag = li.find("a")
        span_tag = li.find("span", class_="reserved")

        citation_span = li.find("span", class_="citation")
        section_citation = _clean_text(citation_span.get_text()) if citation_span else None

        if link_tag and link_tag.get("href"):
            node_name = _clean_text(link_tag.get_text())
            href = link_tag["href"].strip()
            link = _resolve_url(str(part_node.link), href)
            status = _check_reserved(node_name)
        elif span_tag:
            node_name = _clean_text(span_tag.get_text())
            link = str(part_node.link)
            status = "reserved"
        else:
            node_name = _clean_text(li.get_text())
            link = str(part_node.link)
            status = _check_reserved(node_name)

        # Section number comes from the citation span (e.g. "1-1-101").
        # Fall back to parsing node_name if no citation span.
        if section_citation:
            sec_number = section_citation  # full dotted form e.g. "1-1-101"
        else:
            sec_number = _extract_section_number_from_name(node_name)

        if sec_number is None:
            continue

        # Build citation in "Mont. Code Ann. § X-Y-Z" form.
        citation = f"Mont. Code Ann. § {sec_number}"

        node_id = f"{part_node.node_id}/section={sec_number}"

        if status:
            section_node = Node(
                id=node_id,
                link=link,
                top_level_title=part_node.top_level_title,
                node_type="content",
                level_classifier="section",
                number=sec_number,
                node_name=node_name,
                parent=part_node.node_id,
                citation=citation,
                status=status,
            )
            insert_node(section_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)
            continue

        node_text, addendum = _fetch_section_content(link)

        section_node = Node(
            id=node_id,
            link=link,
            top_level_title=part_node.top_level_title,
            node_type="content",
            level_classifier="section",
            number=sec_number,
            node_name=node_name,
            parent=part_node.node_id,
            citation=citation,
            node_text=node_text,
            addendum=addendum,
        )
        insert_node(section_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)


def _fetch_section_content(url: str) -> Tuple[Optional[NodeText], Optional[Addendum]]:
    """Fetch a single MCA section page and return (NodeText, Addendum)."""
    soup = get_url_as_soup(url)

    text_div = soup.find(class_="section-content")
    history_div = soup.find(class_="history-content")

    node_text: Optional[NodeText] = None
    if text_div is not None:
        node_text = NodeText()
        for elem in text_div.find_all(recursive=False):
            raw = elem.get_text(separator=" ")
            text = _clean_text(raw)
            if text:
                node_text.add_paragraph(text=text)

    addendum: Optional[Addendum] = None
    if history_div is not None:
        history_text = _clean_text(history_div.get_text(separator=" "))
        if history_text:
            addendum = Addendum()
            addendum.history = AddendumType(type="history", text=history_text)

    return node_text, addendum


def _resolve_url(base: str, href: str) -> str:
    """Resolve a relative href against a base URL.

    Handles both leading-dot (./chapter_0010/...) and bare-slash patterns.
    """
    if href.startswith("http"):
        return href
    # Strip the filename from base so we work against the directory.
    if "/" in base:
        base_dir = base.rsplit("/", 1)[0]
    else:
        base_dir = base

    # Normalise href: remove leading ./
    if href.startswith("./"):
        href = href[2:]
    elif href.startswith("/"):
        # Absolute path on same host.
        from urllib.parse import urlparse
        parsed = urlparse(base)
        return f"{parsed.scheme}://{parsed.netloc}{href}"

    # Walk up for each "../" prefix.
    while href.startswith("../"):
        href = href[3:]
        base_dir = base_dir.rsplit("/", 1)[0]

    return f"{base_dir}/{href}"


def _extract_structure_number(node_name: str, level: str) -> Optional[str]:
    """Extract the number from a structure node name.

    Examples:
        "CHAPTER 1. GENERAL PROVISIONS"  -> "1"
        "CHAPTER 14-A. SOMETHING"         -> "14-A"
        "Part 1. Meaning of Law"          -> "1"
        "Part 2. General Definitions"     -> "2"
        "CHAPTERS 7 THROUGH 10 RESERVED"  -> None  (multi-range reserved)
    """
    # Match "CHAPTER|PART N." or "CHAPTER|PART N-X." or "Chapter|Part N."
    pattern = rf"(?i)\b{level}\s+([\w][\w\-]*?)(?:\.|$|\s)"
    m = re.search(pattern, node_name)
    if m:
        raw = m.group(1).rstrip(".")
        # Skip multi-chapter spans like "CHAPTERS 7 THROUGH 10 RESERVED"
        # which result in a number that is just an integer but immediately
        # followed by "THROUGH" in the original string.
        if re.search(r"(?i)\bthrough\b", node_name):
            return None
        return raw
    return None


def _extract_section_number_from_name(node_name: str) -> Optional[str]:
    """Extract section number like '1-1-101' from a section listing text."""
    # Matches forms like "1-1-101", "15-30-2112", etc.
    m = re.search(r"\b(\d+(?:-\d+){1,3})\b", node_name)
    if m:
        return m.group(1)
    return None


def _check_reserved(text: str) -> Optional[str]:
    lower = text.lower()
    for kw in RESERVED_KEYWORDS:
        if kw in lower:
            return "reserved"
    return None


def _clean_text(raw: str) -> str:
    text = raw.replace("\xa0", " ").replace(" ", " ").replace(" ", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


if __name__ == "__main__":
    main()
