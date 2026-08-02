import os
import sys
import re
import time
from pathlib import Path
from typing import List, Optional

from bs4 import BeautifulSoup

current_file = Path(__file__).resolve()
src_directory = current_file.parent
while src_directory.name != "src" and src_directory.parent != src_directory:
    src_directory = src_directory.parent
project_root = src_directory.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from src.utils.pydanticModels import NodeID, Node, NodeText, Paragraph, Addendum, AddendumType
from src.utils.scrapingHelpers import (
    insert_jurisdiction_and_corpus_node,
    insert_node,
    get_url_as_soup,
)

COUNTRY = "us"
JURISDICTION = "me"
CORPUS = "statutes"
TABLE_NAME = f"{COUNTRY}_{JURISDICTION}_{CORPUS}"

BASE_URL = "https://legislature.maine.gov/legis/statutes"
TOC_URL = "https://legislature.maine.gov/statutes/"

RESERVED_KEYWORDS = ["(REPEALED)", "(EXPIRED)", "(RESERVED)", "(RENUMBERED)"]


# Markers that indicate latin-1/utf-8 mojibake. Copied from scrapeDE.py.
_MOJIBAKE_MARKERS = (
    "\xc2",
    "\xe2\x80",
    "\xe2\x82",
    "\xe2\x84",
    "\xe2\x86",
    "â€",
)


def _fix_encoding(s: str) -> str:
    """Undo latin-1/utf-8 mojibake only when markers indicate it occurred.

    BeautifulSoup decodes content correctly in most cases; blindly applying a
    latin-1 -> utf-8 round-trip mangles already-correct non-ASCII chars. Match
    the DE scraper's guarded approach.
    """
    if not s:
        return s
    if not any(m in s for m in _MOJIBAKE_MARKERS):
        return s
    try:
        fixed = s.encode("latin-1", errors="ignore").decode("utf-8", errors="ignore")
    except Exception:
        return s
    if sum(m in fixed for m in _MOJIBAKE_MARKERS) < sum(m in s for m in _MOJIBAKE_MARKERS):
        return fixed
    return s


def _get_soup_with_retry(url: str, max_retries: int = 3, backoff: float = 1.5):
    """Wrap get_url_as_soup with a small retry loop. Try vaquill_pipeline's
    http_client first (R2 mirroring + connection pooling) and fall back to
    the project-local helper.
    """
    last_exc: Optional[Exception] = None
    for attempt in range(max_retries):
        try:
            try:
                from vaquill_pipeline.http_client import fetch_text  # type: ignore
                html = fetch_text(url, timeout=60, max_retries=2)
                if html:
                    return BeautifulSoup(html, "html.parser")
            except Exception:
                pass
            soup = get_url_as_soup(url)
            if soup is not None:
                return soup
        except Exception as e:
            last_exc = e
        time.sleep(backoff * (attempt + 1))
    if last_exc is not None:
        print(f"[scrapeME] giving up on {url}: {last_exc}", flush=True)
    return None


def main() -> None:
    corpus_node: Node = insert_jurisdiction_and_corpus_node(COUNTRY, JURISDICTION, CORPUS)
    scrape_all_titles(corpus_node)


# --- Title-level resume bookkeeping -----------------------------------------

def _titles_done_path():
    from vaquill_pipeline.config import SETTINGS  # type: ignore
    return SETTINGS.chunks_dir / "state_me_titles_done.txt"


def _load_titles_done() -> set:
    try:
        path = _titles_done_path()
    except Exception:
        return set()
    if not path.exists():
        return set()
    return {l.strip() for l in path.read_text().splitlines() if l.strip()}


def _mark_title_done(number: str) -> None:
    try:
        path = _titles_done_path()
    except Exception:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a") as fh:
        fh.write(f"{number}\n")
        fh.flush()


def scrape_all_titles(corpus_node: Node) -> None:
    from concurrent.futures import ThreadPoolExecutor, as_completed

    soup = _get_soup_with_retry(TOC_URL)
    if soup is None:
        print("[scrapeME] could not load top-level TOC", flush=True)
        return
    links = soup.find_all("a", href=True)

    titles_done = set() if os.environ.get("VAQUILL_FORCE_RESCRAPE") else _load_titles_done()
    if titles_done:
        print(f"[scrapeME] resume: {len(titles_done)} titles already done", flush=True)

    work: List[Node] = []
    for link_tag in links:
        href = link_tag["href"].strip()
        # Title TOC links look like: 1/title1ch0sec0.html  or  9-A/title9-Ach0sec0.html
        if not re.match(r"^[\w\-]+/title[\w\-]+ch0sec0\.html$", href):
            continue

        node_name = _fix_encoding(link_tag.get_text().strip())
        parts = node_name.split(":", 1)
        title_label = parts[0].strip()
        title_words = title_label.split()
        if len(title_words) < 2:
            continue
        number = title_words[1]

        link = f"{BASE_URL}/{href}"
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

    workers = int(os.environ.get("VAQUILL_TITLE_WORKERS", "8"))
    print(f"[scrapeME] running {len(work)} titles with {workers} parallel workers", flush=True)

    def _do_title(tn: Node):
        try:
            scrape_title(tn)
            _mark_title_done(tn.number)
            return (tn.number, "ok", None)
        except Exception as e:
            return (tn.number, "fail", str(e)[:200])

    with ThreadPoolExecutor(max_workers=workers) as ex:
        for fut in as_completed(ex.submit(_do_title, tn) for tn in work):
            num, status, err = fut.result()
            if status == "fail":
                print(f"[scrapeME] title {num}: fail: {err}", flush=True)
            else:
                print(f"[scrapeME] title {num}: ok", flush=True)


def _parse_intermediate_header(div, title_node: Node, ancestor_id: str) -> Optional[Node]:
    """If `div` is a Part/Subtitle/Subpart container, build & insert a structure
    node for it and return it. Otherwise return None.

    Container classes seen on ME TOC pages:
      - MRSPart_toclist     -> level_classifier="part"
      - MRSSubTitle_toclist -> level_classifier="subtitle"
      - MRSSubpart_toclist  -> level_classifier="subpart"  (defensive)
    Header text lives in a child <h2> like "Part 1: STATE DEPARTMENTS".
    """
    cls = " ".join(div.get("class", []))
    if "MRSPart_toclist" in cls:
        level = "part"
    elif "MRSSubTitle_toclist" in cls or "MRSSubtitle_toclist" in cls:
        level = "subtitle"
    elif "MRSSubpart_toclist" in cls:
        level = "subpart"
    else:
        return None

    header = div.find("h2")
    if header is None:
        return None
    header_text = _fix_encoding(header.get_text().strip())
    # e.g. "Part 1: STATE DEPARTMENTS"
    label_parts = header_text.split(":", 1)
    label = label_parts[0].strip()
    words = label.split()
    if len(words) < 2:
        return None
    number = words[1]

    status = _check_reserved(header_text)
    if not status and "right_nav_repealed" in cls:
        status = "reserved"

    node_id = f"{ancestor_id}/{level}={number}"
    node = Node(
        id=node_id,
        link=title_node.link,
        top_level_title=title_node.top_level_title,
        node_type="structure",
        level_classifier=level,
        number=number,
        node_name=header_text,
        parent=ancestor_id,
        status=status,
    )
    insert_node(node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)
    return node


def scrape_title(title_node: Node) -> None:
    """Fetch a title TOC page. Emit Part/Subtitle structure nodes when present
    and parent chapters under the most recent such container; otherwise chapters
    parent directly under the title (titles like 24-A, 38, 22-A's flat ones).
    """
    soup = _get_soup_with_retry(str(title_node.link))
    if soup is None:
        return
    toc_div = soup.find("div", class_=re.compile(r"title_toc"))
    if toc_div is None:
        toc_div = soup

    # Walk direct children of toc_div in order so we can track the current
    # Part/Subtitle container.
    current_parent: Node = title_node
    for child in toc_div.find_all("div", recursive=False):
        cls = " ".join(child.get("class", []))

        # Intermediate container: Part / Subtitle / Subpart
        if any(k in cls for k in ("MRSPart_toclist", "MRSSubTitle_toclist", "MRSSubtitle_toclist", "MRSSubpart_toclist")):
            intermediate = _parse_intermediate_header(child, title_node, title_node.node_id)
            if intermediate is not None and not intermediate.status:
                current_parent = intermediate
                # Chapters live as nested MRSChapter_toclist divs inside.
                for ch_div in child.find_all("div", class_=re.compile(r"MRSChapter_toclist")):
                    _ingest_chapter(ch_div, title_node, current_parent)
            # If the Part/Subtitle itself is repealed/reserved, still skip its children.
            elif intermediate is not None:
                current_parent = title_node  # reset
            continue

        # Top-level chapter (flat titles like 24-A, 38)
        if "MRSChapter_toclist" in cls:
            _ingest_chapter(child, title_node, current_parent)


def _ingest_chapter(ch_div, title_node: Node, parent_node: Node) -> None:
    """Build a chapter node from a MRSChapter_toclist div under `parent_node`
    and recurse into its sections.
    """
    link_tag = ch_div.find("a", href=True)
    if link_tag is None:
        return
    href = link_tag["href"].strip()
    # Chapter links: ./title1ch1sec0.html (NOT ch0sec0 -- that's the title TOC itself)
    if not re.match(r"^\./title[\w\-]+ch[\w\-]+sec0\.html$", href):
        return
    if re.match(r"^\./title[\w\-]+ch0sec0\.html$", href):
        # Defensive: never re-ingest the title TOC self-link as a chapter.
        return

    node_name = _fix_encoding(link_tag.get_text().strip())
    ch_parts = node_name.split(":", 1)
    ch_label = ch_parts[0].strip()
    ch_words = ch_label.split()
    if len(ch_words) < 2:
        return
    ch_number = ch_words[1]

    ch_href_clean = href.replace("./", "")
    ch_link = f"{BASE_URL}/{title_node.top_level_title}/{ch_href_clean}"
    node_id = f"{parent_node.node_id}/chapter={ch_number}"

    status = _check_reserved(node_name)
    cls = " ".join(ch_div.get("class", []))
    if not status and "right_nav_repealed" in cls:
        status = "reserved"

    chapter_node = Node(
        id=node_id,
        link=ch_link,
        top_level_title=title_node.top_level_title,
        node_type="structure",
        level_classifier="chapter",
        number=ch_number,
        node_name=node_name,
        parent=parent_node.node_id,
        status=status,
    )
    insert_node(chapter_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)

    if not status:
        scrape_chapter(chapter_node)


def scrape_chapter(chapter_node: Node) -> None:
    """Fetch a chapter page and iterate over its section links."""
    soup = _get_soup_with_retry(str(chapter_node.link))
    if soup is None:
        return

    toc_div = soup.find("div", class_=re.compile(r"chapter_toclist|title_toc"))
    if toc_div is None:
        toc_div = soup

    for link_tag in toc_div.find_all("a", href=True):
        href = link_tag["href"].strip()
        # Section links: ./title1sec1.html  or  ./title1sec15-A.html
        if not re.match(r"^\./title[\w\-]+sec[\w\-]+\.html$", href):
            continue
        # Reject the chapter/title TOC self-link (sec0 or chN-sec0 patterns).
        if re.search(r"sec0\.html$", href):
            continue
        if re.search(r"ch0sec[\w\-]+\.html$", href):
            continue

        node_name = _fix_encoding(link_tag.get_text().strip())
        if not node_name:
            continue

        sec_number = _extract_section_number(node_name, href)
        if sec_number is None or sec_number == "0":
            continue

        sec_href_clean = href.replace("./", "")
        sec_link = f"{BASE_URL}/{chapter_node.top_level_title}/{sec_href_clean}"
        node_id = f"{chapter_node.node_id}/section={sec_number}"

        status = _check_reserved(node_name)
        citation = f"{chapter_node.top_level_title} M.R.S. § {sec_number}"

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


def _fetch_section_content(url: str):
    """Fetch a single section page and return (NodeText | None, Addendum | None)."""
    soup = _get_soup_with_retry(url)
    if soup is None:
        return None, None
    sec_div = soup.find("div", class_=re.compile(r"MRSSection"))
    if sec_div is None:
        return None, None

    node_text = NodeText()
    history_text = ""

    for element in sec_div.find_all(recursive=False):
        cls_str = " ".join(element.get("class", []))

        if "heading_section" in cls_str:
            continue

        if "qhistory" in cls_str:
            raw = element.get_text(separator=" ")
            history_text = _clean_text(raw)
            continue

        raw = element.get_text(separator=" ")
        text = _clean_text(raw)
        if text:
            node_text.add_paragraph(text=text)

    addendum = None
    if history_text:
        addendum = Addendum()
        addendum.history = AddendumType(type="history", text=history_text)

    return node_text, addendum


def _extract_section_number(node_name: str, href: str) -> Optional[str]:
    """Extract a clean section number from link text or href.

    Link text examples:
        "1 §1. Extent of sovereignty..."
        "1 §15-A. Consent of Legislature..."
    href fallback: ./title1sec15-A.html -> 15-A
    """
    m = re.search(r"§\s*([\w\-]+)\.", node_name)
    if m:
        return m.group(1)
    m2 = re.search(r"sec([\w\-]+)\.html$", href)
    if m2:
        return m2.group(1)
    return None


def _check_reserved(text: str) -> Optional[str]:
    upper = text.upper()
    for kw in RESERVED_KEYWORDS:
        if kw.upper() in upper:
            return "reserved"
    return None


def _clean_text(raw: str) -> str:
    text = _fix_encoding(raw)
    text = text.replace("\xa0", " ").replace(" ", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


if __name__ == "__main__":
    main()
