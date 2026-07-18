"""Iowa Code scraper.

Hierarchy: Title -> Chapter -> Section
Source:    https://www.legis.iowa.gov/law/iowaCode  (HTML TOC + per-section RTF)
Citation:  Iowa Code § <section_number>  (e.g. Iowa Code § 1.1)

No Selenium required. The site serves sections as RTF files which are fetched
via vaquill_pipeline.http_client.fetch_bytes and parsed with striprtf.
"""

import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO
from pathlib import Path

current_file = Path(__file__).resolve()
src_directory = current_file.parent
while src_directory.name != "src" and src_directory.parent != src_directory:
    src_directory = src_directory.parent
project_root = src_directory.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from src.utils.pydanticModels import Node, NodeText, Addendum, AddendumType
from src.utils.scrapingHelpers import (
    insert_jurisdiction_and_corpus_node,
    insert_node,
    get_url_as_soup,
)

COUNTRY = "us"
JURISDICTION = "ia"
CORPUS = "statutes"
TABLE_NAME = f"{COUNTRY}_{JURISDICTION}_{CORPUS}"

BASE_URL = "https://www.legis.iowa.gov"
TOC_URL = f"{BASE_URL}/law/iowaCode"
CODE_YEAR = "2026"

RESERVED_KEYWORDS = [
    "Reserved.",
    "Repealed by ",
    "[Repealed",
    "[Reserved",
    "[Renumbered",
]


def main() -> None:
    corpus_node: Node = insert_jurisdiction_and_corpus_node(COUNTRY, JURISDICTION, CORPUS)
    scrape_all_titles(corpus_node)


def scrape_all_titles(corpus_node: Node) -> None:
    """Walk the Iowa Code TOC and dispatch every chapter in parallel.

    Two passes. The discovery pass is sequential and cheap: one TOC fetch for
    the title list, then one chapter-listing fetch per title (~16 requests
    total). It inserts the title + chapter structure nodes and collects the
    chapter nodes. The second pass fans the chapters out to a
    ThreadPoolExecutor sized by VAQUILL_TITLE_WORKERS (default 8, matching
    scrapeWA / scrapeAK).

    Why chapters and not titles: each chapter is an independent subtree
    (chapter listing -> per-section RTF fetch) sharing no state, and the JSONL
    sink + counters in vaquill_pipeline.patch are lock-protected. Iowa has
    only ~16 titles but ~1000 chapters, and the titles are wildly uneven
    (Title XVI dwarfs Title I), so a title-level pool would leave workers idle
    waiting on one long tail. Chapters are the level that keeps all 8 workers
    saturated. The crawl is latency-bound at one RTF per section, so this is
    the knob that matters.

    NOTE: IA deliberately has no titles_done resume file -- every run
    re-crawls in full. That is what lets an amended section be re-fetched and
    re-chunked into a fresh content-addressed point_id (the JSONL skipset
    suppresses the write for unchanged sections, so a re-crawl is cheap in
    output but still catches amendments). Do not add a titles_done skip here
    to save time without replacing that freshness some other way -- it would
    make IA amendment-blind.
    """
    soup = get_url_as_soup(TOC_URL)
    iac_list = soup.find(id="iacList")
    if iac_list is None:
        raise RuntimeError(f"Could not find #iacList on {TOC_URL}")

    work: list[Node] = []

    rows = iac_list.find("tbody").find_all("tr")
    for row in rows:
        tds = row.find_all("td")
        if not tds:
            continue
        a_tag = row.find("a")
        if not a_tag:
            continue

        raw_name = _clean_text(tds[0].get_text())
        # "Title I - STATE SOVEREIGNTY AND MANAGEMENT (Ch. 1 - 38D)"
        # Extract roman numeral: "Title I" -> "I"
        m = re.match(r"Title\s+([^\s\-]+)", raw_name, re.IGNORECASE)
        if not m:
            continue
        title_number = m.group(1)

        chapters_url = BASE_URL + a_tag["href"]
        node_id = f"{corpus_node.node_id}/title={title_number}"

        status = _check_reserved(raw_name)
        title_node = Node(
            id=node_id,
            link=chapters_url,
            top_level_title=title_number,
            node_type="structure",
            level_classifier="title",
            number=title_number,
            node_name=raw_name,
            parent=corpus_node.node_id,
            status=status,
        )
        insert_node(title_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)

        if not status:
            # A single title's listing failing must not sink the discovery
            # pass; the run continues with the chapters found so far.
            try:
                work.extend(scrape_title(title_node))
            except Exception as exc:
                print(
                    f"[scrapeIA] title {title_number}: discovery failed: {exc!s:.200}",
                    flush=True,
                )

    def _do_chapter(chapter_node: Node) -> tuple[str, str, str | None]:
        # One chapter's failure must not abort the other workers, so each is
        # wrapped and reported; the run continues with the remaining chapters.
        try:
            scrape_chapter(chapter_node)
            return (chapter_node.number, "ok", None)
        except Exception as exc:
            return (chapter_node.number, "fail", str(exc)[:200])

    workers = int(os.environ.get("VAQUILL_TITLE_WORKERS", "8"))
    print(
        f"[scrapeIA] running {len(work)} chapters with {workers} parallel workers",
        flush=True,
    )
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for fut in as_completed(ex.submit(_do_chapter, item) for item in work):
            num, status, err = fut.result()
            if status == "fail":
                print(f"[scrapeIA] chapter {num}: {status}: {err}", flush=True)
            else:
                print(f"[scrapeIA] chapter {num}: {status}", flush=True)


def scrape_title(title_node: Node) -> list[Node]:
    """Fetch the chapters-listing page for a title.

    Inserts each chapter structure node and returns the unreserved chapter
    nodes for the caller to dispatch; the chapters themselves are crawled in
    parallel by scrape_all_titles.
    """
    chapters: list[Node] = []

    soup = get_url_as_soup(str(title_node.link))
    iac_list = soup.find(id="iacList")
    if iac_list is None:
        return chapters

    rows = iac_list.find("tbody").find_all("tr")
    for row in rows:
        tds = row.find_all("td")
        if not tds:
            continue
        a_tag = row.find("a")
        if not a_tag:
            continue

        raw_name = _clean_text(tds[0].get_text())
        # "Chapter 1 - SOVEREIGNTY AND JURISDICTION OF THE STATE"
        m = re.match(r"Chapter\s+(\S+)", raw_name, re.IGNORECASE)
        if not m:
            continue
        ch_number = m.group(1).rstrip("-")

        # href: /law/iowaCode/sections?codeChapter=1&year=2026
        sections_url = BASE_URL + a_tag["href"]
        node_id = f"{title_node.node_id}/chapter={ch_number}"

        status = _check_reserved(raw_name)
        chapter_node = Node(
            id=node_id,
            link=sections_url,
            top_level_title=title_node.top_level_title,
            node_type="structure",
            level_classifier="chapter",
            number=ch_number,
            node_name=raw_name,
            parent=title_node.node_id,
            status=status,
        )
        insert_node(chapter_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)

        if not status:
            chapters.append(chapter_node)

    return chapters


def scrape_chapter(chapter_node: Node) -> None:
    """Fetch the section-listing page for a chapter and iterate over sections."""
    soup = get_url_as_soup(str(chapter_node.link))
    iac_list = soup.find(id="iacList")
    if iac_list is None:
        return

    rows = iac_list.find("tbody").find_all("tr")
    for row in rows:
        tds = row.find_all("td")
        if not tds:
            continue

        raw_name = _clean_text(tds[0].get_text())
        # "§1.1 - State boundaries."
        m = re.match(r"§([\d\w\.]+)", raw_name)
        if not m:
            continue
        # full citation token: "1.1", "218.99A", etc.
        sec_token = m.group(1).rstrip(".")

        # Section number is the part after the chapter prefix: "1.1" -> "1"
        # Iowa uses <chapter>.<section> numbering; we store the full token
        # as the number so IDs remain globally unique within the chapter.
        sec_number = sec_token

        # RTF link is the 2nd td (index 1) first <a>, or fall back to PDF
        rtf_url: str | None = None
        pdf_url: str | None = None
        for td in tds[1:]:
            a = td.find("a")
            if a:
                href = a["href"]
                if href.endswith(".rtf"):
                    rtf_url = BASE_URL + href
                elif href.endswith(".pdf"):
                    pdf_url = BASE_URL + href

        node_id = f"{chapter_node.node_id}/section={sec_number}"
        citation = f"Iowa Code § {sec_token}"
        status = _check_reserved(raw_name)

        if status:
            section_node = Node(
                id=node_id,
                link=rtf_url or pdf_url or str(chapter_node.link),
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

        node_text, addendum = _fetch_rtf_content(rtf_url, pdf_url, sec_token)

        # If still reserved after content fetch, mark it
        if node_text is None:
            status = "reserved"

        section_node = Node(
            id=node_id,
            link=rtf_url or pdf_url or str(chapter_node.link),
            top_level_title=chapter_node.top_level_title,
            node_type="content",
            level_classifier="section",
            number=sec_number,
            node_name=raw_name,
            parent=chapter_node.node_id,
            citation=citation,
            status=status,
            node_text=node_text,
            addendum=addendum,
        )
        insert_node(section_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)


def _fetch_rtf_content(
    rtf_url: str | None,
    pdf_url: str | None,
    sec_token: str,
) -> tuple[NodeText | None, Addendum | None]:
    """Fetch section text from RTF (preferred) or PDF fallback."""
    if rtf_url:
        try:
            return _parse_rtf(rtf_url)
        except Exception as exc:
            print(f"[warn] RTF failed for {rtf_url}: {exc}", flush=True)

    if pdf_url:
        try:
            return _parse_pdf(pdf_url, sec_token)
        except Exception as exc:
            print(f"[warn] PDF failed for {pdf_url}: {exc}", flush=True)

    return None, None


def _parse_rtf(url: str) -> tuple[NodeText | None, Addendum | None]:
    from striprtf.striprtf import rtf_to_text
    from vaquill_pipeline.http_client import fetch_bytes

    raw_bytes, _ = fetch_bytes(url)
    text = rtf_to_text(raw_bytes.decode("utf-8", errors="replace"))
    return _extract_from_text(text)


def _parse_pdf(url: str, sec_token: str) -> tuple[NodeText | None, Addendum | None]:
    import pdfplumber
    from vaquill_pipeline.http_client import fetch_bytes

    raw_bytes, _ = fetch_bytes(url)
    full_text_parts: list[str] = []
    with pdfplumber.open(BytesIO(raw_bytes)) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text() or ""
            full_text_parts.append(page_text)
    combined = "\n".join(full_text_parts)
    return _extract_from_text(combined)


def _extract_from_text(
    raw: str,
) -> tuple[NodeText | None, Addendum | None]:
    """Parse raw RTF/PDF text into NodeText + Addendum."""
    lines = [_clean_text(ln) for ln in raw.splitlines()]
    lines = [ln for ln in lines if ln]

    if not lines:
        return None, None

    # Check reserved status from content
    joined = " ".join(lines)
    for kw in RESERVED_KEYWORDS:
        if kw in joined:
            return None, None

    node_text = NodeText()
    history_lines: list[str] = []
    addendum_started = False

    for line in lines:
        if not line:
            continue
        # History / amendment lines are bracketed "[C51, ...]" or start with year Acts
        if re.match(r"^\[", line) or re.match(r"^\d{4}\s+Acts", line):
            history_lines.append(line)
            addendum_started = True
            continue
        # "Referred to in ..." lines are also addendum
        if line.startswith("Referred to in"):
            history_lines.append(line)
            addendum_started = True
            continue
        # Once addendum started, trailing lines stay in addendum
        if addendum_started:
            history_lines.append(line)
            continue
        node_text.add_paragraph(text=line)

    if node_text.paragraphs is None or len(node_text.paragraphs) == 0:
        return None, None

    addendum: Addendum | None = None
    if history_lines:
        addendum = Addendum()
        addendum.history = AddendumType(
            type="history",
            text=" ".join(history_lines),
        )

    return node_text, addendum


def _check_reserved(text: str) -> str | None:
    upper = text.upper()
    for kw in RESERVED_KEYWORDS:
        if kw.upper() in upper:
            return "reserved"
    return None


def _clean_text(raw: str) -> str:
    text = raw.replace("\xa0", " ").replace(" ", " ").replace("\r", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


if __name__ == "__main__":
    main()
