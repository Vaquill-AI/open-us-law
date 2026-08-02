import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

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
    insert_jurisdiction_and_corpus_node,
    insert_node,
)

COUNTRY = "us"
JURISDICTION = "or"
CORPUS = "statutes"
TABLE_NAME = f"{COUNTRY}_{JURISDICTION}_{CORPUS}"

BASE_URL = "https://oregon.public.law"
TOC_URL = f"{BASE_URL}/statutes"

RESERVED_KEYWORDS = [
    "repealed",
    "renumbered",
    "reserved",
    "expired",
    "former provisions",
]


def _get_soup(url: str) -> BeautifulSoup:
    """Fetch URL using the vaquill HTTP client with brotli excluded from
    Accept-Encoding. oregon.public.law returns brotli-encoded responses when
    'br' appears in Accept-Encoding, but the requests library cannot decode br
    without the optional brotli/brotlicffi package installed."""
    try:
        from vaquill_pipeline.http_client import fetch_soup
        return fetch_soup(url, extra_headers={"Accept-Encoding": "gzip, deflate"})
    except Exception:
        from src.utils.scrapingHelpers import get_url_as_soup
        return get_url_as_soup(url)


def main() -> None:
    corpus_node: Node = insert_jurisdiction_and_corpus_node(COUNTRY, JURISDICTION, CORPUS)
    scrape_all_volumes(corpus_node)


def scrape_all_volumes(corpus_node: Node) -> None:
    """Fetch the statutes TOC, then dispatch each title in parallel.

    ORS hierarchy: Volume -> Title -> Chapter -> Section.
    Volumes are not represented as nodes; we iterate volumes to discover titles.
    Hierarchy emitted: us/or/statutes/title=N/chapter=M/section=S
    Citation format: ORS sec S (e.g. ORS sec 1.001)

    Discovery walks the ~18 volume pages sequentially (they are only an index
    layer, so this is cheap) and collects every title node. Each title is then
    an independent subtree (title page -> chapters -> sections) with no shared
    state, and the JSONL sink + counters in vaquill_pipeline.patch are
    lock-protected, so titles fan out to a ThreadPoolExecutor sized by
    VAQUILL_TITLE_WORKERS.

    Why the default is 4 and not 8 like the other states: this scrapes
    oregon.public.law, a third-party volunteer-run mirror rather than an
    official state .gov datacenter. We are a guest on someone else's small
    server, so we stay deliberately gentle. Raise VAQUILL_TITLE_WORKERS only if
    you have a reason to believe the host can take it.

    NOTE: OR deliberately has no titles_done resume file -- every run re-crawls
    in full. That is what lets an amended section be re-fetched and re-chunked
    into a fresh content-addressed point_id (the JSONL skipset suppresses the
    write for unchanged sections, so a re-crawl is cheap in output but still
    catches amendments). Do not add a titles_done skip here to save time
    without replacing that freshness some other way -- it would make OR
    amendment-blind the way the titles_done states are.
    """
    soup = _get_soup(TOC_URL)

    work: list[tuple[Node, str]] = []
    for link_tag in soup.find_all("a", href=True):
        href = link_tag["href"].strip()
        m = re.search(r"ors_volume_(\d+)$", href)
        if not m:
            continue
        volume_number = m.group(1)
        volume_url = f"{BASE_URL}/statutes/ors_volume_{volume_number}"
        work.extend(scrape_volume(corpus_node, volume_number, volume_url))

    def _do_title(item: tuple[Node, str]) -> tuple[str, str, str | None]:
        # One title's failure must not abort the other workers, so each is
        # wrapped and reported; the run continues with the remaining titles.
        title_node, title_url = item
        try:
            scrape_title(title_node, title_url)
            return (title_node.number, "ok", None)
        except Exception as exc:
            return (title_node.number, "fail", str(exc)[:200])

    workers = int(os.environ.get("VAQUILL_TITLE_WORKERS", "4"))
    print(
        f"[scrapeOR] running {len(work)} titles with {workers} parallel workers",
        flush=True,
    )
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for fut in as_completed(ex.submit(_do_title, item) for item in work):
            num, status, err = fut.result()
            if status == "fail":
                print(f"[scrapeOR] title {num}: {status}: {err}", flush=True)
            else:
                print(f"[scrapeOR] title {num}: {status}", flush=True)


def scrape_volume(corpus_node: Node, volume_number: str, volume_url: str) -> list[tuple[Node, str]]:
    """Fetch a volume page and insert its title links.

    Returns the (title_node, title_url) pairs that still need scraping, for the
    caller to dispatch in parallel.
    """
    soup = _get_soup(volume_url)

    work: list[tuple[Node, str]] = []

    for link_tag in soup.find_all("a", href=True):
        href = link_tag["href"].strip()
        # Title links are relative: ors_title_N
        m = re.search(r"ors_title_(\d+(?:-\w+)?)$", href)
        if not m:
            continue
        title_number = m.group(1)

        raw_name = _clean_text(link_tag.get_text())
        node_name = raw_name or f"Title {title_number}"
        status = _check_reserved(node_name)

        title_url = f"{BASE_URL}/statutes/ors_title_{title_number}"
        node_id = f"{corpus_node.node_id}/title={title_number}"

        title_node = Node(
            id=node_id,
            link=title_url,
            top_level_title=title_number,
            node_type="structure",
            level_classifier="title",
            number=title_number,
            node_name=node_name,
            parent=corpus_node.node_id,
            status=status,
        )
        insert_node(title_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)

        if not status:
            work.append((title_node, title_url))

    return work


def scrape_title(title_node: Node, title_url: str) -> None:
    """Fetch a title page and iterate over chapter links."""
    soup = _get_soup(title_url)

    for link_tag in soup.find_all("a", href=True):
        href = link_tag["href"].strip()
        # Chapter links are relative: ors_chapter_N
        m = re.search(r"ors_chapter_([\w]+)$", href)
        if not m:
            continue
        ch_number = m.group(1)

        raw_name = _clean_text(link_tag.get_text())
        node_name = raw_name or f"Chapter {ch_number}"
        status = _check_reserved(node_name)

        ch_url = f"{BASE_URL}/statutes/ors_chapter_{ch_number}"
        node_id = f"{title_node.node_id}/chapter={ch_number}"

        chapter_node = Node(
            id=node_id,
            link=ch_url,
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
            scrape_chapter(chapter_node, ch_url, ch_number)


def scrape_chapter(chapter_node: Node, chapter_url: str, ch_number: str) -> None:
    """Fetch a chapter page and iterate over section links."""
    soup = _get_soup(chapter_url)

    for link_tag in soup.find_all("a", href=True):
        href = link_tag["href"].strip()
        # Section links are relative: ors_1.001 or ors_131A.005
        m = re.match(r"^ors_([\w]+\.[\w]+)$", href)
        if not m:
            continue

        sec_number = m.group(1)  # e.g. "1.001" or "131A.005"

        # Confirm section belongs to this chapter by matching prefix
        sec_prefix = sec_number.split(".")[0]
        if sec_prefix.upper() != ch_number.upper():
            continue

        raw_name = _clean_text(link_tag.get_text())
        if not raw_name:
            continue

        status = _check_reserved(raw_name)
        sec_url = f"{BASE_URL}/statutes/ors_{sec_number}"
        node_id = f"{chapter_node.node_id}/section={sec_number}"
        citation = f"ORS § {sec_number}"

        if status:
            section_node = Node(
                id=node_id,
                link=sec_url,
                top_level_title=chapter_node.top_level_title,
                node_type="content",
                level_classifier="section",
                number=sec_number,
                node_name=raw_name,
                parent=chapter_node.node_id,
                citation=citation,
                status=status,
            )
            insert_node(section_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)
            continue

        node_text, addendum = _fetch_section_content(sec_url)

        section_node = Node(
            id=node_id,
            link=sec_url,
            top_level_title=chapter_node.top_level_title,
            node_type="content",
            level_classifier="section",
            number=sec_number,
            node_name=raw_name,
            parent=chapter_node.node_id,
            citation=citation,
            node_text=node_text,
            addendum=addendum,
        )
        insert_node(section_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)


def _fetch_section_content(url: str):
    """Fetch a single section page and return (NodeText | None, Addendum | None)."""
    try:
        soup = _get_soup(url)
    except Exception:
        return None, None

    body_div = soup.find(id="leaf-statute-body")
    if body_div is None:
        return None, None

    node_text = NodeText()

    # Collect text from all block-level children; prefer <section> tags which
    # oregon.public.law uses for ORS subsection formatting.
    for element in body_div.find_all(["p", "section"], recursive=True):
        raw = element.get_text(separator=" ")
        text = _clean_text(raw)
        if text:
            node_text.add_paragraph(text=text)

    # If section/p approach found nothing, fall back to the full div text.
    if not node_text.paragraphs:
        full_text = _clean_text(body_div.get_text(separator=" "))
        if full_text:
            node_text.add_paragraph(text=full_text)

    # Source note: <p class="small"> outside the body
    addendum = None
    source_p = soup.find("p", class_="small")
    if source_p:
        source_text = _clean_text(source_p.get_text())
        if source_text:
            addendum = Addendum()
            addendum.source = AddendumType(type="source", text=source_text)

    if not node_text.paragraphs:
        node_text = None

    return node_text, addendum


def _check_reserved(text: str) -> str | None:
    lower = text.lower()
    for kw in RESERVED_KEYWORDS:
        if kw in lower:
            return "reserved"
    return None


def _clean_text(raw: str) -> str:
    text = raw.replace("\xa0", " ").replace("\u200b", "").replace(" ", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


if __name__ == "__main__":
    main()
