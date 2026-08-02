"""Kentucky Revised Statutes (KRS) scraper.

Source: https://apps.legislature.ky.gov/law/statutes/
Hierarchy: title -> chapter -> section

The TOC page (Panel1 > span > ul) alternates between:
  <li><span id="title">TITLE I ...</span></li>
  <ul type="square"><li><a class="chapter" href="chapter.aspx?id=N">CHAPTER M ...</a></li></ul>

Chapter pages expose section links as <a class="statute" href="statute.aspx?id=N">.
Each statute.aspx?id=N URL returns a PDF directly (no HTML wrapper).

Note: KRS uses Roman-numeral title labels in the display text (TITLE I, II, ...) but
section numbers use the integer chapter prefix (e.g. 1.100, 6A.010). We store the
Roman-numeral label as the title number for display fidelity.
"""
from __future__ import annotations

import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO
from pathlib import Path

import pdfplumber

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
JURISDICTION = "ky"
CORPUS = "statutes"
TABLE_NAME = f"{COUNTRY}_{JURISDICTION}_{CORPUS}"

BASE_URL = "https://apps.legislature.ky.gov/law/statutes"
TOC_URL = f"{BASE_URL}/"

RESERVED_KEYWORDS = [
    "not yet utilized",
    "repealed",
    "reserved",
    "superseded",
    "expired",
    "renumbered",
]

# KRS section numbers look like: "1.100", "6A.010", "224A.050"
# The PDF first line is: "<chapter>.<digits> <Section heading text>."
_SECTION_FIRST_LINE_RE = re.compile(r"^(\d[\dA-Za-z]*\.\d+)\s+(.+)$")

# History / effective markers signal the addendum portion.
_HISTORY_START_RE = re.compile(
    r"^(Effective:|History:|HISTORY:|EFFECTIVE:)", re.IGNORECASE
)


def main() -> None:
    corpus_node: Node = insert_jurisdiction_and_corpus_node(COUNTRY, JURISDICTION, CORPUS)
    _scrape_toc(corpus_node)


def _scrape_toc(corpus_node: Node) -> None:
    """Parse the main TOC, then dispatch each chapter in parallel.

    The whole title/chapter skeleton lives on the single TOC page, so discovery
    costs one fetch. Every chapter below it is an independent subtree (chapter
    page -> statute PDFs) with no shared state, and the JSONL sink + counters in
    vaquill_pipeline.patch are lock-protected, so chapters fan out to a
    ThreadPoolExecutor sized by VAQUILL_TITLE_WORKERS (default 8, matching
    scrapeWA / scrapeAK).

    Why chapter and not title: KRS titles are very unevenly sized, and a title
    is only a label on the TOC page, so fanning out per chapter gives the pool
    far more units to balance across.

    Why it pays off: KY fetches one PDF per section and parses it with
    pdfplumber, so each section costs a network round trip plus CPU. pdfplumber
    holds the GIL for much of the parse, so threads mainly overlap the network
    wait rather than the parse. That is still the dominant cost per section, so
    the win is large even though it will not be a clean 8x. Threads (not
    processes) are required here: the JSONL sink + counters in
    vaquill_pipeline/patch.py are thread-locked, NOT process-safe.

    NOTE: KY deliberately has no titles_done resume file -- every run re-crawls
    in full. That is what lets an amended section be re-fetched and re-chunked
    into a fresh content-addressed point_id (the JSONL skipset suppresses the
    write for unchanged sections, so a re-crawl is cheap in output but still
    catches amendments). Do not add a chapters_done skip here to save time
    without replacing that freshness some other way -- it would make KY
    amendment-blind the way the titles_done states are.
    """
    soup = get_url_as_soup(TOC_URL)
    panel = soup.find(id="Panel1")
    if panel is None:
        raise RuntimeError(f"Panel1 not found on {TOC_URL}")

    outer_ul = panel.find("span").find("ul")
    if outer_ul is None:
        raise RuntimeError(f"Outer <ul> not found inside Panel1 span on {TOC_URL}")

    # The outer_ul contains alternating children:
    #   <li>  with <span id="title">  -> title node
    #   <ul type="square">            -> chapters belonging to that title
    #   <p>                           -> separator (ignored)
    current_title_node: Node | None = None

    # Sequential discovery pass: insert every title + chapter node and collect
    # the chapters that still need their sections fetched.
    work: list[tuple[Node, str]] = []

    for child in outer_ul.children:
        if not hasattr(child, "name") or not child.name:
            continue

        if child.name == "li":
            title_span = child.find("span", id="title")
            if title_span is None:
                continue
            current_title_node = _insert_title(corpus_node, title_span.get_text().strip())

        elif child.name == "ul" and current_title_node is not None:
            work.extend(_scrape_chapter_list(current_title_node, child))

    def _do_chapter(item: tuple[Node, str]) -> tuple[str, str, str | None]:
        # One chapter's failure must not abort the other workers, so each is
        # wrapped and reported; the run continues with the remaining chapters.
        chapter_node, chapter_url = item
        try:
            _scrape_chapter(chapter_node, chapter_url)
            return (chapter_node.number, "ok", None)
        except Exception as exc:
            return (chapter_node.number, "fail", str(exc)[:200])

    workers = int(os.environ.get("VAQUILL_TITLE_WORKERS", "8"))
    print(
        f"[scrapeKY] running {len(work)} chapters with {workers} parallel workers",
        flush=True,
    )
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for fut in as_completed(ex.submit(_do_chapter, item) for item in work):
            num, status, err = fut.result()
            if status == "fail":
                print(f"[scrapeKY] chapter {num}: {status}: {err}", flush=True)
            else:
                print(f"[scrapeKY] chapter {num}: {status}", flush=True)


def _insert_title(corpus_node: Node, raw_name: str) -> Node:
    """Create and insert a title node. Returns the node."""
    # raw_name: "TITLE I SOVEREIGNTY AND JURISDICTION OF THE COMMONWEALTH"
    parts = raw_name.split(None, 2)
    # parts[0] = "TITLE", parts[1] = "I" (Roman numeral), parts[2] = description
    number = parts[1] if len(parts) >= 2 else raw_name
    node_id = f"{corpus_node.node_id}/title={number}"
    status = _check_reserved(raw_name)

    title_node = Node(
        id=node_id,
        link=TOC_URL,
        top_level_title=number,
        node_type="structure",
        level_classifier="title",
        number=number,
        node_name=raw_name,
        parent=corpus_node.node_id,
        status=status,
    )
    insert_node(title_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)
    return title_node


def _scrape_chapter_list(title_node: Node, chapters_ul) -> list[tuple[Node, str]]:
    """Insert the <a class="chapter"> links inside a chapter <ul>.

    Returns the (chapter_node, chapter_url) pairs whose sections still need
    fetching, for the caller to dispatch in parallel.
    """
    work: list[tuple[Node, str]] = []

    for li in chapters_ul.find_all("li", recursive=False):
        link_tag = li.find("a", class_="chapter")
        if link_tag is None:
            continue

        raw_name = _clean_text(link_tag.get_text())
        href = link_tag.get("href", "").strip()
        status = _check_reserved(raw_name)

        # raw_name: "CHAPTER 6A ORGANIZATIONAL SESSIONS OF THE GENERAL ASSEMBLY"
        ch_parts = raw_name.split(None, 2)
        ch_number = ch_parts[1].rstrip(".") if len(ch_parts) >= 2 else raw_name

        node_id = f"{title_node.node_id}/chapter={ch_number}"

        if href:
            chapter_url = f"{BASE_URL}/{href}"
        else:
            chapter_url = TOC_URL

        chapter_node = Node(
            id=node_id,
            link=chapter_url,
            top_level_title=title_node.top_level_title,
            node_type="structure",
            level_classifier="chapter",
            number=ch_number,
            node_name=raw_name,
            parent=title_node.node_id,
            status=status,
        )
        insert_node(chapter_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)

        if not status and href:
            work.append((chapter_node, chapter_url))

    return work


def _scrape_chapter(chapter_node: Node, chapter_url: str) -> None:
    """Fetch the chapter page and iterate over statute PDF links."""
    try:
        soup = get_url_as_soup(chapter_url)
    except Exception as exc:
        print(f"[WARN] Could not fetch chapter {chapter_url}: {exc}", flush=True)
        return

    panel = soup.find(id="Panel1")
    if panel is None:
        return

    for link_tag in panel.find_all("a", class_="statute"):
        raw_label = _clean_text(link_tag.get_text())
        href = link_tag.get("href", "").strip()
        if not href:
            continue

        section_url = f"{BASE_URL}/{href}"

        # raw_label: ".100  Boundary with Virginia and West Virginia."
        # Section number is chapter_number + the decimal digits e.g. "1.100"
        sec_number = _parse_section_number(chapter_node.number, raw_label)
        if sec_number is None:
            continue

        node_name = _build_section_name(sec_number, raw_label)
        node_id = f"{chapter_node.node_id}/section={sec_number}"
        citation = f"KRS § {sec_number}"
        status = _check_reserved(raw_label)

        if status:
            section_node = Node(
                id=node_id,
                link=section_url,
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

        node_text, addendum = _fetch_section_pdf(section_url)

        section_node = Node(
            id=node_id,
            link=section_url,
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


def _fetch_section_pdf(url: str) -> tuple[NodeText | None, Addendum | None]:
    """
    Download a statute PDF and return (NodeText, Addendum).

    The statute.aspx?id=N endpoint returns a PDF directly. We use
    vaquill_pipeline.http_client.fetch_bytes so the raw PDF is mirrored to R2
    and the pipeline can attach an r2_pdf_url.
    """
    try:
        from vaquill_pipeline.http_client import fetch_bytes
        body, _ct = fetch_bytes(url, timeout=30)
    except Exception as exc:
        print(f"[WARN] Could not download PDF {url}: {exc}", flush=True)
        return None, None

    try:
        pdf = pdfplumber.open(BytesIO(body))
    except Exception as exc:
        print(f"[WARN] Could not open PDF {url}: {exc}", flush=True)
        return None, None

    lines: list[str] = []
    for page in pdf.pages:
        page_text = page.extract_text() or ""
        for line in page_text.splitlines():
            stripped = line.strip()
            if stripped:
                lines.append(stripped)

    return _build_node_text(lines)


def _build_node_text(
    lines: list[str],
) -> tuple[NodeText | None, Addendum | None]:
    """Split raw PDF lines into body NodeText and history Addendum."""
    if not lines:
        return None, None

    node_text = NodeText()
    history_lines: list[str] = []
    in_history = False

    for line in lines:
        text = _clean_text(line)
        if not text:
            continue
        if _HISTORY_START_RE.match(text):
            in_history = True
        if in_history:
            history_lines.append(text)
        else:
            node_text.add_paragraph(text=text)

    addendum: Addendum | None = None
    if history_lines:
        addendum = Addendum()
        addendum.history = AddendumType(
            type="history", text=" ".join(history_lines)
        )

    return node_text, addendum


def _parse_section_number(chapter_number: str, raw_label: str) -> str | None:
    """
    Derive the full KRS section number from the chapter number and the section label.

    raw_label examples:
      ".100  Boundary with Virginia and West Virginia."
      ".010  Legislative intent..."
      "6A.010  ..."  (some labels already include the chapter prefix)

    chapter_number examples: "1", "6A", "224A"
    """
    # Strip leading dot and grab the numeric suffix before whitespace.
    m = re.match(r"^\.(\d[\w]*)\s", raw_label.strip())
    if m:
        return f"{chapter_number}.{m.group(1)}"

    # If the label already includes a chapter-style prefix (e.g. "6A.010 ...")
    m2 = re.match(r"^(\d[\w]*\.\d+)\s", raw_label.strip())
    if m2:
        return m2.group(1)

    return None


def _build_section_name(sec_number: str, raw_label: str) -> str:
    """Build a clean section name: 'KRS 1.100 Boundary with Virginia...'"""
    # raw_label is like ".100  Boundary with Virginia and West Virginia."
    # Strip the leading .NNN prefix to get just the title text.
    title_text = re.sub(r"^\.\d+\s+", "", raw_label.strip()).rstrip(".")
    if not title_text:
        title_text = raw_label.strip()
    return f"{sec_number} {title_text}"


def _check_reserved(text: str) -> str | None:
    lower = text.lower()
    for kw in RESERVED_KEYWORDS:
        if kw in lower:
            return "reserved"
    return None


def _clean_text(raw: str) -> str:
    text = raw.replace("\xa0", " ").replace("–", "-").replace("—", "-")
    text = re.sub(r"\s+", " ", text).strip()
    return text


if __name__ == "__main__":
    main()
