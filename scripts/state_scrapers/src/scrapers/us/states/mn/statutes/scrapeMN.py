import os
import sys
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Tuple

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
JURISDICTION = "mn"
CORPUS = "statutes"
TABLE_NAME = f"{COUNTRY}_{JURISDICTION}_{CORPUS}"

BASE_URL = "https://www.revisor.mn.gov"
TOC_URL = "https://www.revisor.mn.gov/statutes/"

RESERVED_KEYWORDS = ["repealed", "renumbered", "expired", "reserved"]

# MN section IDs include dots and (rarely) hyphens, e.g. "1.01", "169A.20",
# "116J.395", "256B.0625". Chapter IDs may carry alpha suffixes like "2A".
# Anchoring with [\w.\-]+ accepts all real MN cites and rejects empty/garbage
# strings that would crash downstream (was the original will-crash failure
# mode: `link_tag.get_text()` could return "" and yield malformed node IDs).
_SECTION_ID_RE = re.compile(r"^[\w.\-]+$")
_CHAPTER_ID_RE = re.compile(r"^[\w.\-]+$")


# --------------------------------------------------------------------------- #
# Resume state                                                                #
# --------------------------------------------------------------------------- #

def _chapters_done_path() -> Path:
    from vaquill_pipeline.config import SETTINGS
    return SETTINGS.chunks_dir / "state_mn_chapters_done.txt"


def _load_chapters_done() -> set:
    path = _chapters_done_path()
    if not path.exists():
        return set()
    return {ln.strip() for ln in path.read_text().splitlines() if ln.strip()}


def _mark_chapter_done(number: str) -> None:
    path = _chapters_done_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a") as fh:
        fh.write(f"{number}\n")
        fh.flush()


# --------------------------------------------------------------------------- #
# Entry point                                                                 #
# --------------------------------------------------------------------------- #

def main() -> None:
    corpus_node: Node = insert_jurisdiction_and_corpus_node(COUNTRY, JURISDICTION, CORPUS)
    scrape_all_parts(corpus_node)


def scrape_all_parts(corpus_node: Node) -> None:
    """Fetch the top-level TOC and parallelize over part rows."""
    soup = get_url_as_soup(TOC_URL)
    toc_table = soup.find(id="toc_table")
    if toc_table is None:
        print("[MN] toc_table not found on TOC page", flush=True)
        return

    chapters_done = (
        set() if os.environ.get("VAQUILL_FORCE_RESCRAPE") else _load_chapters_done()
    )
    if chapters_done:
        print(f"[scrapeMN] resume: {len(chapters_done)} chapters already done", flush=True)

    work: List[Node] = []
    for row in toc_table.find_all("tr"):
        tds = row.find_all("td")
        if not tds:
            continue
        link_tag = tds[0].find("a")
        if link_tag is None:
            continue
        href = (link_tag.get("href") or "").strip()
        if not href:
            continue

        chapter_range = link_tag.get_text().strip()  # e.g. "1 - 2A"
        if not chapter_range:
            continue
        part_name = tds[1].get_text().strip() if len(tds) > 1 else ""

        part_number = chapter_range.replace(" ", "").replace("-", "_")
        link = href if href.startswith("http") else f"{BASE_URL}{href}"

        node_id = f"{corpus_node.node_id}/part={part_number}"
        node_name = f"{chapter_range} {part_name}".strip()

        part_node = Node(
            id=node_id,
            link=link,
            top_level_title=part_number,
            node_type="structure",
            level_classifier="part",
            number=part_number,
            node_name=node_name,
            parent=corpus_node.node_id,
        )
        insert_node(part_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)
        work.append(part_node)

    workers = int(os.environ.get("VAQUILL_TITLE_WORKERS", "8"))
    print(f"[scrapeMN] running {len(work)} parts with {workers} parallel workers", flush=True)

    def _do_part(part_node: Node) -> Tuple[str, str, str | None]:
        try:
            scrape_part(part_node, chapters_done)
            return (part_node.number, "ok", None)
        except Exception as e:
            return (part_node.number, "fail", str(e)[:200])

    with ThreadPoolExecutor(max_workers=workers) as ex:
        for fut in as_completed(ex.submit(_do_part, p) for p in work):
            num, status, err = fut.result()
            if status == "fail":
                print(f"[scrapeMN] part {num}: {status}: {err}", flush=True)
            else:
                print(f"[scrapeMN] part {num}: {status}", flush=True)


def scrape_part(part_node: Node, chapters_done: set) -> None:
    """Fetch a part page and iterate over its chapter rows."""
    soup = get_url_as_soup(str(part_node.link))
    chapters_table = soup.find(id="chapters_table")
    if chapters_table is None:
        print(f"[MN] chapters_table not found on part page: {part_node.link}", flush=True)
        return

    for row in chapters_table.find_all("tr"):
        tds = row.find_all("td")
        if not tds:
            continue
        link_tag = tds[0].find("a")
        if link_tag is None:
            continue
        href = (link_tag.get("href") or "").strip()
        if not href:
            continue

        chapter_number = link_tag.get_text().strip()
        # Validate chapter id matches expected pattern (e.g. "1", "2A",
        # "169A"). Drop anything that doesn't, rather than crashing later.
        if not chapter_number or not _CHAPTER_ID_RE.match(chapter_number):
            continue

        chapter_name = tds[1].get_text().strip() if len(tds) > 1 else ""

        node_id = f"{part_node.node_id}/chapter={chapter_number}"
        node_name = f"Chapter {chapter_number} {chapter_name}".strip()
        link = href if href.startswith("http") else f"{BASE_URL}{href}"

        chapter_node = Node(
            id=node_id,
            link=link,
            top_level_title=chapter_number,
            node_type="structure",
            level_classifier="chapter",
            number=chapter_number,
            node_name=node_name,
            parent=part_node.node_id,
        )
        insert_node(chapter_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)

        if chapter_number in chapters_done:
            continue
        try:
            scrape_chapter(chapter_node)
            _mark_chapter_done(chapter_number)
        except Exception as e:
            print(f"[scrapeMN] chapter {chapter_number} failed: {e!s:.200}", flush=True)


def scrape_chapter(chapter_node: Node) -> None:
    """Fetch a chapter page and iterate over its section rows."""
    soup = get_url_as_soup(str(chapter_node.link))
    chapter_analysis = soup.find(id="chapter_analysis")
    if chapter_analysis is None:
        print(f"[MN] chapter_analysis not found on chapter page: {chapter_node.link}", flush=True)
        return

    table = chapter_analysis.find("table")
    if table is None:
        return

    for row in table.find_all("tr"):
        # Rows with a class attribute are category headings, not sections.
        if row.get("class"):
            continue
        tds = row.find_all("td")
        if not tds:
            continue
        link_tag = tds[0].find("a")
        if link_tag is None:
            continue

        href = (link_tag.get("href") or "").strip()
        if not href:
            continue

        sec_number = link_tag.get_text().strip()
        # Widened to accept dotted/alpha-suffix IDs like "169A.20",
        # "116J.395", "256B.0625". The earlier ``\w+``-style regex would have
        # dropped these (or crashed on a None match).
        if not sec_number or not _SECTION_ID_RE.match(sec_number):
            continue

        sec_name = tds[1].get_text().strip() if len(tds) > 1 else ""

        node_id = f"{chapter_node.node_id}/section={sec_number}"
        node_name = f"§ {sec_number} {sec_name}".strip()
        link = href if href.startswith("http") else f"{BASE_URL}{href}"
        citation = f"Minn. Stat. § {sec_number}"

        status = _check_reserved(sec_name)

        if status:
            section_node = Node(
                id=node_id,
                link=link,
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

        node_text, addendum = _fetch_section_content(link)

        section_node = Node(
            id=node_id,
            link=link,
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
    soup = get_url_as_soup(url)
    section_div = soup.find(class_="section")
    if section_div is None:
        return None, None

    node_text = NodeText()
    history_text = ""

    for element in section_div.find_all(recursive=False):
        cls_list = element.get("class") or []
        cls_str = " ".join(cls_list)

        if element.name in ("h1", "h2", "h3") and "shn" in cls_str:
            continue

        if "history" in cls_str:
            raw = element.get_text(separator=" ")
            history_text = _clean_text(raw)
            continue

        raw = element.get_text(separator=" ")
        text = _clean_text(raw)
        if text:
            node_text.add_paragraph(text=text)

    if not history_text:
        history_tag = soup.find(class_="history")
        if history_tag:
            history_text = _clean_text(history_tag.get_text(separator=" "))

    addendum = None
    if history_text:
        addendum = Addendum()
        addendum.history = AddendumType(type="history", text=history_text)

    return node_text, addendum


def _check_reserved(text: str) -> str | None:
    lower = text.lower()
    for kw in RESERVED_KEYWORDS:
        if kw in lower:
            return "reserved"
    return None


def _clean_text(raw: str) -> str:
    text = raw.replace("\xa0", " ").replace(" ", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


if __name__ == "__main__":
    main()
