"""North Dakota Century Code (NDCC) scraper.

Source: https://ndlegis.gov/general-information/north-dakota-century-code/classic.html
Hierarchy: title -> chapter -> section
Section text is embedded in per-chapter PDFs served at
https://ndlegis.gov/cencode/tNNcNN.pdf.
Chapter HTML pages (tNNcNN.html) list all sections with names.
"""
from __future__ import annotations

import re
import sys
import time
from io import BytesIO
from pathlib import Path
from typing import Optional

import pdfplumber
import requests

# ---------------------------------------------------------------------------
# Project-root bootstrap (mirrors the ME/DE pattern)
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
JURISDICTION = "nd"
CORPUS = "statutes"
TABLE_NAME = f"{COUNTRY}_{JURISDICTION}_{CORPUS}"

BASE_URL = "https://ndlegis.gov"
CENCODE_URL = f"{BASE_URL}/cencode"
TOC_URL = f"{BASE_URL}/general-information/north-dakota-century-code/classic.html"

# Keywords that flag a chapter or section as reserved/repealed
RESERVED_KEYWORDS = [
    "repealed",
    "reserved",
    "expired",
    "renumbered",
]

# Word-boundary regex compiled from RESERVED_KEYWORDS to avoid substring false
# positives (e.g. "preserved", "expiration date", "irrepealable").
_RESERVED_RE = re.compile(
    r"\b(?:" + "|".join(re.escape(k) for k in RESERVED_KEYWORDS) + r")\b",
    re.IGNORECASE,
)

# Regex that matches a section-header line in a chapter PDF.
# NDCC section numbers look like:
#   "1-01-01"        -> plain three-part number
#   "1-01-01.1"      -> decimal sub-section
#   "10-19.1-01"     -> decimal in chapter component
#   "4.1-01-01"      -> decimal in title component
# Must contain a hyphen so we don't falsely match numbered list items like "1. Foo".
_SEC_HEADER_RE = re.compile(
    r"^(\d[\d\.]*(?:-[\d\.]+)+)\.\s+(.+)$"
)

# Matches "Page No. N" footers so we can strip them.
_PAGE_FOOTER_RE = re.compile(r"^Page No\.\s+\d+\s*$", re.IGNORECASE)

# Matches running headers/titles at the top of PDF pages, e.g.
#   "CHAPTER 1-01"
#   "TITLE 1"
#   "GENERAL PROVISIONS"  (all-caps running titles).
# Also strip "TABLE OF CONTENTS" headings.
_RUNNING_HEADER_RE = re.compile(
    r"^(?:CHAPTER\s+\d[\d\.\-]*|TITLE\s+\d[\d\.]*|TABLE OF CONTENTS)\s*$",
    re.IGNORECASE,
)

# Browser-like headers for direct requests (PDF downloads bypass the proxy helper).
_REQ_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "application/pdf,*/*",
}


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    corpus_node: Node = insert_jurisdiction_and_corpus_node(COUNTRY, JURISDICTION, CORPUS)
    _scrape_all_titles(corpus_node)


# ---------------------------------------------------------------------------
# Title-level
# ---------------------------------------------------------------------------

def _titles_done_path():
    """Where we persist the set of titles already fully scraped for resume."""
    from vaquill_pipeline.config import SETTINGS
    return SETTINGS.chunks_dir / "state_nd_titles_done.txt"


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


def _scrape_all_titles(corpus_node: Node) -> None:
    """Scrape the main TOC and dispatch per title.

    Titles are independent. We collect work first (small, sequential parse)
    then dispatch to a ThreadPoolExecutor. Resume: titles already in
    ``state_nd_titles_done.txt`` are skipped (set ``VAQUILL_FORCE_RESCRAPE=1``
    to override). Concurrency via ``VAQUILL_TITLE_WORKERS`` (default 8).
    """
    import os
    from concurrent.futures import ThreadPoolExecutor, as_completed

    titles_done = (
        set() if os.environ.get("VAQUILL_FORCE_RESCRAPE") else _load_titles_done()
    )
    if titles_done:
        print(
            f"[scrapeND] resume: {len(titles_done)} titles already done",
            flush=True,
        )

    soup = get_url_as_soup(TOC_URL)
    grid = soup.find(class_="titles-grid")
    if grid is None:
        raise RuntimeError(f"Could not find titles-grid on {TOC_URL}")

    work: list[tuple[Node, str, bool]] = []  # (title_node, full_url, is_pdf)

    for item in grid.find_all("div", class_="title-item"):
        link_tag = item.find("a", href=True)
        if link_tag is None:
            continue

        href: str = link_tag["href"].strip()
        num_elem = item.find(class_=re.compile(r"title-number"))
        if num_elem is None:
            continue
        title_number = num_elem.get_text(strip=True)

        node_name_raw = item.get_text(separator=" ", strip=True)
        # node_name_raw looks like "1 General Provisions" because the number
        # and the label are adjacent. Rebuild a clean name.
        title_name = f"Title {title_number} {_strip_leading_number(node_name_raw, title_number)}"

        node_id = f"{corpus_node.node_id}/title={title_number}"

        # Build absolute URL. Href is always an absolute path like /cencode/t01.html.
        full_url = BASE_URL + href if href.startswith("/") else href

        status = _check_reserved(title_name)

        title_node = Node(
            id=node_id,
            link=full_url,
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

        work.append((title_node, full_url, href.endswith(".pdf")))

    def _do_title(item):
        title_node, full_url, is_pdf = item
        num = title_node.number
        try:
            if is_pdf:
                _scrape_single_chapter_title(title_node, full_url)
            else:
                _scrape_title_html(title_node, full_url)
            _mark_title_done(num)
            return (num, "ok", None)
        except Exception as e:
            return (num, "fail", str(e)[:200])

    workers = int(os.environ.get("VAQUILL_TITLE_WORKERS", "8"))
    print(
        f"[scrapeND] running {len(work)} titles with {workers} parallel workers",
        flush=True,
    )
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for fut in as_completed(ex.submit(_do_title, item) for item in work):
            num, status, err = fut.result()
            if status == "fail":
                print(f"[scrapeND] title {num}: {status}: {err}", flush=True)
            else:
                print(f"[scrapeND] title {num}: {status}", flush=True)


# ---------------------------------------------------------------------------
# Chapter-level
# ---------------------------------------------------------------------------

def _scrape_title_html(title_node: Node, title_url: str) -> None:
    """Parse a title HTML page and iterate chapters."""
    soup = get_url_as_soup(title_url)
    field = soup.find(class_=re.compile(r"field--name-field-pwv-custom-content"))
    if field is None:
        return
    table = field.find("table")
    if table is None:
        return

    for row in table.find_all("tr"):
        tds = row.find_all("td")
        if len(tds) < 2:
            continue

        # Column layout: chapter-number | link-or-NA | chapter-name
        # Sometimes only 2 columns when there is no link.
        chapter_number_raw = tds[0].get_text(strip=True)  # e.g. "1-01" or "2-01"
        chapter_number = chapter_number_raw.strip()
        if not chapter_number:
            continue

        link_td = tds[1] if len(tds) > 1 else None
        name_td = tds[2] if len(tds) > 2 else (tds[1] if len(tds) > 1 else None)

        chapter_name_raw = name_td.get_text(strip=True) if name_td else chapter_number
        chapter_name = f"Chapter {chapter_number} {chapter_name_raw}"

        link_tag = link_td.find("a", href=True) if link_td else None
        if link_tag is None:
            # No link means the chapter is reserved/repealed.
            status = "reserved"
            chapter_url = title_url
        else:
            href = link_tag["href"].strip()
            # href is relative to /cencode/; it may end in .pdf or .html depending
            # on whether the Drupal template rewrites links.
            chapter_url = f"{CENCODE_URL}/{href.lstrip('/')}"
            status = _check_reserved(chapter_name)

        node_id = f"{title_node.node_id}/chapter={chapter_number}"

        chapter_node = Node(
            id=node_id,
            link=chapter_url,
            top_level_title=title_node.top_level_title,
            node_type="structure",
            level_classifier="chapter",
            number=chapter_number,
            node_name=chapter_name,
            parent=title_node.node_id,
            status=status,
        )
        insert_node(chapter_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)

        if status:
            continue

        # Normalize to both HTML and PDF variants.
        # The Drupal-rendered page may link to either .html or .pdf.
        # The HTML page lists sections; the PDF contains section text.
        base = chapter_url.split("#")[0]
        chapter_html_url = base.replace(".pdf", ".html")
        chapter_pdf_url = base.replace(".html", ".pdf")
        _scrape_chapter(chapter_node, chapter_html_url, chapter_pdf_url)


def _scrape_single_chapter_title(title_node: Node, pdf_url: str) -> None:
    """Handle a title that links directly to a chapter PDF (e.g., Title 7)."""
    # Derive chapter number from title number.
    chapter_number = f"{title_node.top_level_title}-01"
    chapter_name = f"Chapter {chapter_number}"
    node_id = f"{title_node.node_id}/chapter={chapter_number}"

    chapter_node = Node(
        id=node_id,
        link=pdf_url,
        top_level_title=title_node.top_level_title,
        node_type="structure",
        level_classifier="chapter",
        number=chapter_number,
        node_name=chapter_name,
        parent=title_node.node_id,
    )
    insert_node(chapter_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)

    # Normalize to both HTML and PDF variants.
    base = pdf_url.split("#")[0]
    html_url = base.replace(".pdf", ".html")
    actual_pdf_url = base.replace(".html", ".pdf")
    _scrape_chapter(chapter_node, html_url, actual_pdf_url)


# ---------------------------------------------------------------------------
# Section-level
# ---------------------------------------------------------------------------

def _scrape_chapter(
    chapter_node: Node,
    chapter_html_url: str,
    chapter_pdf_url: str,
) -> None:
    """
    Parse the chapter HTML for section metadata, then extract section text
    from the chapter PDF.
    """
    # Step 1: get section list from the HTML page.
    section_meta = _get_section_meta_from_html(chapter_html_url)

    # Step 2: extract all section text blocks from the PDF.
    section_texts = _extract_sections_from_pdf(chapter_pdf_url)

    # Fallback: if the HTML page failed (empty meta) but the PDF parsed,
    # derive section numbers + names from the PDF's first-line headers so
    # we still emit content nodes for the chapter.
    if not section_meta and section_texts:
        print(
            f"[INFO] HTML meta empty for {chapter_html_url}; "
            f"falling back to PDF-derived section headers",
            flush=True,
        )
        for sec_number, body in section_texts.items():
            first = body[0] if body else sec_number
            # Stored as "<num>. <name>"; strip the leading number to keep
            # the name portion only.
            name = first.split(". ", 1)[1] if ". " in first else first
            section_meta[sec_number] = _clean_text(name)

    # Step 3: merge and insert.
    for sec_number, sec_name in section_meta.items():
        node_id = f"{chapter_node.node_id}/section={sec_number}"
        citation = f"N.D. Cent. Code § {sec_number}"
        sec_link = f"{chapter_pdf_url}#nameddest={_number_to_anchor(sec_number)}"

        status = _check_reserved(sec_name)

        body_text = section_texts.get(sec_number)

        if status or body_text is None:
            section_node = Node(
                id=node_id,
                link=sec_link,
                top_level_title=chapter_node.top_level_title,
                node_type="content",
                level_classifier="section",
                number=sec_number,
                node_name=f"{sec_number}. {sec_name}",
                parent=chapter_node.node_id,
                citation=citation,
                status=status or None,
            )
        else:
            node_text, addendum = _build_node_text(body_text)
            section_node = Node(
                id=node_id,
                link=sec_link,
                top_level_title=chapter_node.top_level_title,
                node_type="content",
                level_classifier="section",
                number=sec_number,
                node_name=f"{sec_number}. {sec_name}",
                parent=chapter_node.node_id,
                citation=citation,
                node_text=node_text,
                addendum=addendum,
            )

        insert_node(section_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)


def _get_section_meta_from_html(chapter_html_url: str) -> dict[str, str]:
    """
    Fetch the chapter HTML page and return {section_number: section_name}.
    """
    try:
        soup = get_url_as_soup(chapter_html_url)
    except Exception as exc:
        print(f"[WARN] Could not fetch chapter HTML {chapter_html_url}: {exc}", flush=True)
        return {}

    field = soup.find(class_=re.compile(r"field--name-field-pwv-custom-content"))
    if field is None:
        return {}
    table = field.find("table")
    if table is None:
        return {}

    result: dict[str, str] = {}
    for row in table.find_all("tr"):
        tds = row.find_all("td")
        if len(tds) < 2:
            continue
        link_tag = tds[0].find("a")
        if link_tag is None:
            continue
        sec_number = link_tag.get_text(strip=True)
        sec_name = tds[1].get_text(strip=True) if len(tds) > 1 else sec_number
        if sec_number:
            result[sec_number] = _clean_text(sec_name)
    return result


def _extract_sections_from_pdf(pdf_url: str) -> dict[str, list[str]]:
    """
    Download the chapter PDF and split it into per-section text blocks.
    Returns {section_number: [paragraph, ...]}.
    """
    # Vaquill: use fetch_bytes so the PDF lands in R2 and chunks pick up
    # an r2_pdf_url via the stem index. Retry with exponential backoff
    # because the ndlegis.gov host occasionally 5xxs on bursty traffic.
    body = None
    last_exc: Optional[Exception] = None
    for attempt in range(4):
        try:
            from vaquill_pipeline.http_client import fetch_bytes
            body, _ct = fetch_bytes(pdf_url, timeout=45)
            break
        except Exception as exc:
            last_exc = exc
            sleep_s = min(2 ** attempt, 8)
            print(
                f"[WARN] PDF fetch attempt {attempt + 1}/4 failed {pdf_url}: {exc}; "
                f"sleeping {sleep_s}s",
                flush=True,
            )
            time.sleep(sleep_s)
    if body is None:
        print(f"[WARN] Could not download PDF {pdf_url}: {last_exc}", flush=True)
        return {}

    try:
        pdf = pdfplumber.open(BytesIO(body))
    except Exception as exc:
        print(f"[WARN] Could not open PDF {pdf_url}: {exc}", flush=True)
        return {}

    full_lines: list[str] = []
    for page in pdf.pages:
        page_text = page.extract_text() or ""
        page_lines = page_text.splitlines()
        # Strip up to the first 3 lines if they match the running-header
        # pattern (chapter/title heading repeated at every page top).
        head_strip = 0
        for ln in page_lines[:3]:
            if _RUNNING_HEADER_RE.match(ln.strip()):
                head_strip += 1
            else:
                break
        for line in page_lines[head_strip:]:
            stripped = line.strip()
            # Drop page footers and any surviving running-header lines.
            if _PAGE_FOOTER_RE.match(stripped):
                continue
            if _RUNNING_HEADER_RE.match(stripped):
                continue
            full_lines.append(stripped)

    return _split_into_sections(full_lines)


def _split_into_sections(lines: list[str]) -> dict[str, list[str]]:
    """
    Split raw PDF lines into a dict mapping section number -> list of text lines.

    Section headers look like:  "1-01-01. This act - How referred to."
    The header line itself becomes the first paragraph.
    """
    # First, locate the first "real" section start. NDCC chapter PDFs begin
    # with a Table of Contents that lists every section header back-to-back
    # with no body text between them. Walk the lines and find the first
    # header that is followed (within a small window) by a non-header,
    # non-empty body line. That is the start of the section bodies; treat
    # everything before it as TOC and discard.
    body_start = _find_body_start(lines)

    sections: dict[str, list[str]] = {}
    current_number: Optional[str] = None
    current_lines: list[str] = []

    for line in lines[body_start:]:
        m = _SEC_HEADER_RE.match(line)
        if m:
            if current_number is not None and current_lines:
                sections[current_number] = current_lines
            current_number = m.group(1)
            header_title = m.group(2).rstrip(".")
            current_lines = [f"{current_number}. {header_title}"]
        elif current_number is not None:
            if line:
                current_lines.append(line)
        # Pre-first-header content (running titles, etc.) is discarded.

    if current_number is not None and current_lines:
        sections[current_number] = current_lines

    return sections


def _find_body_start(lines: list[str]) -> int:
    """Return the index of the first section header that opens a real body.

    A real body header is one followed within the next few lines by at
    least one non-empty line that is NOT itself a section header. TOC
    entries fail this test because consecutive TOC lines are all headers.
    """
    n = len(lines)
    for i, line in enumerate(lines):
        if not _SEC_HEADER_RE.match(line):
            continue
        # Look ahead a small window for a body line.
        body_seen = False
        for j in range(i + 1, min(i + 6, n)):
            nxt = lines[j].strip()
            if not nxt:
                continue
            if _SEC_HEADER_RE.match(nxt):
                # Another header right after - still TOC-like.
                break
            body_seen = True
            break
        if body_seen:
            return i
    # No body region found; behave like the old code and scan from start.
    return 0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_node_text(
    lines: list[str],
) -> tuple[Optional[NodeText], Optional[Addendum]]:
    """Convert a list of text lines into NodeText + optional Addendum."""
    if not lines:
        return None, None

    node_text = NodeText()
    history_lines: list[str] = []
    in_history = False

    # Heuristic: lines starting with "Source:" or looking like amendment
    # history ("S.L. YYYY, ch. N, § N.") go into the addendum.
    _HISTORY_START_RE = re.compile(
        r"^(Source:|History:|S\.L\.\s+\d{4}|Amended\s+by\s+S\.L\.)", re.IGNORECASE
    )

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

    addendum: Optional[Addendum] = None
    if history_lines:
        addendum = Addendum()
        addendum.history = AddendumType(
            type="history", text=" ".join(history_lines)
        )

    return node_text, addendum


def _check_reserved(text: str) -> Optional[str]:
    """Return 'reserved' if the text contains a reserved/repealed keyword
    (matched as a whole word so substrings like "preserved" do not trigger).
    """
    if _RESERVED_RE.search(text or ""):
        return "reserved"
    return None


def _clean_text(raw: str) -> str:
    """Normalize whitespace and remove non-breaking spaces."""
    text = raw.replace("\xa0", " ").replace("‑", "-").replace("‒", "-")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _strip_leading_number(text: str, number: str) -> str:
    """Remove the leading number from a text string like '1 General Provisions'."""
    prefix = re.escape(number)
    return re.sub(rf"^\s*{prefix}\s*", "", text).strip()


def _number_to_anchor(sec_number: str) -> str:
    """
    Convert a section number to the PDF named destination format.
    e.g. "1-01-01" -> "1-01-01"
         "1-01-35.1" -> "1-01-35p1"
    """
    # The ND PDF anchors use "pN" for decimal parts: 1-01-35.1 -> 1-01-35p1
    return re.sub(r"\.(\d+)", r"p\1", sec_number)


# ---------------------------------------------------------------------------
# CLI entry
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    main()
