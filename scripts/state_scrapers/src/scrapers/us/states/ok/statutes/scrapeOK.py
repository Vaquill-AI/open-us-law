"""Oklahoma Statutes scraper.

Source: Oklahoma Legislature complete-title PDFs (www.oklegislature.gov).
Mirror used: https://www.oklegislature.gov/OK_Statutes/CompleteTitles/os{N}.pdf

Why this source (2026-05-12):
    The previous implementation depended on ``search.oklegislature.gov`` which
    is currently unreachable from our scrape network (TCP connection refused /
    Max retries exceeded). The official titles TOC at ``www.oklegislature.gov``
    is reachable and links to one PDF per title containing every section's
    full text. We parse those PDFs directly with pdfplumber.

Hierarchy: us/ok/statutes/title=N/section=S
Citation:   "Okla. Stat. tit. <T>, § <S>"  (e.g., "Okla. Stat. tit. 1, § 1-20")

Note on hierarchy:
    OK statutes are organized as title -> section (no native chapter level
    exposed in the official complete-title PDFs). The earlier search-portal
    scraper also flattened to title -> section. We preserve that here; section
    numbers carry the canonical "T-S" form (e.g., "1-20", "15-598.2") which
    matches the citation format Okla. Stat. tit. T, § T-S.

Performance:
    Titles are independent of each other, so we dispatch one worker per title
    via ThreadPoolExecutor (VAQUILL_TITLE_WORKERS env, default 6). A title-
    level resume file (state_ok_titles_done.txt) lets reruns skip completed
    titles.
"""

from __future__ import annotations

import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO
from pathlib import Path
from typing import Optional

from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Path setup
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
    insert_jurisdiction_and_corpus_node,
    insert_node,
)

try:
    import pdfplumber
except ImportError as exc:
    raise ImportError("pdfplumber is required: pip install pdfplumber") from exc

from vaquill_pipeline.http_client import fetch_bytes, fetch_html

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

COUNTRY = "us"
JURISDICTION = "ok"
CORPUS = "statutes"
TABLE_NAME = f"{COUNTRY}_{JURISDICTION}_{CORPUS}"

TITLES_HTML_URL = "https://www.oklegislature.gov/osStatuesTitle.html"
COMPLETE_TITLE_BASE = "https://www.oklegislature.gov/OK_Statutes/CompleteTitles"

RESERVED_KEYWORDS = [
    "(reserved)",
    "(repealed)",
    "(expired)",
    "(renumbered)",
    "(deleted)",
]

# Section heading: §T-S. Name.   T can have letter suffix (e.g. 3A, 74E).
# S can be alphanumeric with optional dots (e.g. 598.2, 233A).
_SECTION_HEADING_RE = re.compile(
    r"^§\s*([0-9]+[A-Za-z]?)\s*[-‑]\s*([0-9][0-9A-Za-z.\-]*)\s*\.\s*(.*)$"
)

# Legislative-history starters (one of these begins the trailing addendum).
_HISTORY_START_RE = re.compile(
    r"^("
    r"R\.L\.\d{4}"
    r"|Laws\s+\d{4}"
    r"|Added\s+by\s+Laws"
    r"|Amended\s+by\s+Laws"
    r"|Renumbered\s+(by|from)\s+Laws"
    r"|Repealed\s+by\s+Laws"
    r"|Transferred\s+by\s+Laws"
    r")",
    re.IGNORECASE,
)

# Lines that look like TOC dot-leaders, e.g. "Section name ........... 12".
_TOC_DOTS_RE = re.compile(r"\.\s*\.\s*\.\s*\.\s*\.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    corpus_node: Node = insert_jurisdiction_and_corpus_node(COUNTRY, JURISDICTION, CORPUS)

    title_specs = _discover_titles(corpus_node)

    titles_done = (
        set()
        if os.environ.get("VAQUILL_FORCE_RESCRAPE")
        else _load_titles_done()
    )
    if titles_done:
        print(
            f"[scrapeOK] resume: {len(titles_done)} titles already done: "
            f"{sorted(titles_done)}",
            flush=True,
        )

    work: list[tuple[Node, str]] = []
    for title_node, pdf_url in title_specs:
        if title_node.status:
            continue
        if title_node.number in titles_done:
            continue
        work.append((title_node, pdf_url))

    workers = int(os.environ.get("VAQUILL_TITLE_WORKERS", "6"))
    print(
        f"[scrapeOK] running {len(work)} titles with {workers} parallel workers",
        flush=True,
    )

    def _do_title(item: tuple[Node, str]) -> tuple[str, str, Optional[str]]:
        title_node, pdf_url = item
        try:
            _scrape_title(title_node, pdf_url)
            _mark_title_done(title_node.number)
            return (title_node.number, "ok", None)
        except Exception as exc:  # noqa: BLE001
            return (title_node.number, "fail", str(exc)[:200])

    with ThreadPoolExecutor(max_workers=workers) as ex:
        for fut in as_completed(ex.submit(_do_title, item) for item in work):
            num, status, err = fut.result()
            if status == "fail":
                print(f"[scrapeOK] title {num}: {status}: {err}", flush=True)
            else:
                print(f"[scrapeOK] title {num}: {status}", flush=True)


# ---------------------------------------------------------------------------
# Title discovery
# ---------------------------------------------------------------------------


def _discover_titles(corpus_node: Node) -> list[tuple[Node, str]]:
    """Parse the titles TOC HTML, insert title nodes, and return (node, pdf_url) pairs."""
    html = fetch_html(TITLES_HTML_URL)
    soup = BeautifulSoup(html, "html.parser")

    seen: set[str] = set()
    out: list[tuple[Node, str]] = []

    for link_tag in soup.find_all("a", href=True):
        href = link_tag["href"].strip()
        m = re.search(r"/os(\d+[A-Ea-e]?)\.pdf$", href, re.IGNORECASE)
        if not m:
            continue
        raw_number = m.group(1).upper()
        if raw_number in seen:
            continue
        seen.add(raw_number)

        raw_name = link_tag.get_text(separator=" ").strip()
        node_name = _clean_text(raw_name) or f"Title {raw_number}"

        # Normalize PDF URL to the canonical complete-titles location.
        pdf_url = f"{COMPLETE_TITLE_BASE}/os{raw_number}.pdf"

        status = _check_reserved(node_name)

        title_node = Node(
            id=f"{corpus_node.node_id}/title={raw_number}",
            link=pdf_url,
            top_level_title=raw_number,
            node_type="structure",
            level_classifier="title",
            number=raw_number,
            node_name=node_name,
            parent=corpus_node.node_id,
            status=status,
        )
        insert_node(title_node, TABLE_NAME, ignore_duplicate=True, debug_mode=False)
        out.append((title_node, pdf_url))

    return out


# ---------------------------------------------------------------------------
# Title-level resume bookkeeping
# ---------------------------------------------------------------------------


def _titles_done_path() -> Path:
    try:
        from vaquill_pipeline.config import SETTINGS  # type: ignore
        return SETTINGS.chunks_dir / "state_ok_titles_done.txt"
    except Exception:
        return Path(__file__).parent / "state_ok_titles_done.txt"


def _load_titles_done() -> set[str]:
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


# ---------------------------------------------------------------------------
# Per-title PDF parsing
# ---------------------------------------------------------------------------


def _scrape_title(title_node: Node, pdf_url: str) -> None:
    """Download one complete-title PDF, parse all sections, insert section nodes."""
    body, _ct = fetch_bytes(pdf_url, timeout=120)

    with pdfplumber.open(BytesIO(body)) as pdf:
        page_lines: list[str] = []
        for page in pdf.pages:
            text = page.extract_text() or ""
            for line in text.splitlines():
                stripped = line.strip()
                if stripped:
                    page_lines.append(stripped)

    # Strip the leading TOC: drop every line until we see a heading whose
    # following block is NOT a TOC dot-leader line.
    sections = _split_sections(page_lines, title_node.number)

    for sec_number, node_name, body_lines, history_lines in sections:
        node_id = f"{title_node.node_id}/section={sec_number}"
        citation = f"Okla. Stat. tit. {title_node.number}, § {sec_number}"

        status = _check_reserved(node_name) or _check_reserved(
            " ".join(history_lines[:1])
        )

        node_text: Optional[NodeText] = None
        if body_lines and not status:
            node_text = NodeText()
            for para in body_lines:
                node_text.add_paragraph(text=para)

        addendum: Optional[Addendum] = None
        if history_lines and not status:
            addendum = Addendum()
            addendum.history = AddendumType(
                type="history",
                text=" ".join(history_lines),
            )

        section_node = Node(
            id=node_id,
            link=pdf_url,
            top_level_title=title_node.top_level_title,
            node_type="content",
            level_classifier="section",
            number=sec_number,
            node_name=node_name or f"§ {sec_number}",
            parent=title_node.node_id,
            citation=citation,
            node_text=node_text,
            addendum=addendum,
            status=status,
        )
        insert_node(section_node, TABLE_NAME, ignore_duplicate=True, debug_mode=False)


def _split_sections(
    lines: list[str],
    title_number: str,
) -> list[tuple[str, str, list[str], list[str]]]:
    """Split a flat list of PDF text lines into per-section tuples.

    Returns (section_number, node_name, body_lines, history_lines) for every
    real section in the title. TOC entries (lines containing long dot-leaders)
    are ignored because their "body" is empty.
    """
    results: list[tuple[str, str, list[str], list[str]]] = []

    current: Optional[dict] = None

    def _flush() -> None:
        if current is None:
            return
        # Drop entries that look like TOC stubs (no body, no history).
        if not current["body"] and not current["history"]:
            return
        results.append(
            (
                current["number"],
                current["name"],
                current["body"],
                current["history"],
            )
        )

    for raw in lines:
        line = raw.replace("‑", "-")  # non-breaking hyphen
        # Skip pure TOC dot-leader lines (e.g. "§1-20. Short title. ......... 3")
        if _TOC_DOTS_RE.search(line):
            continue

        m = _SECTION_HEADING_RE.match(line)
        if m and m.group(1).upper() == title_number.upper():
            # New section starts.
            _flush()
            current = {
                "number": f"{m.group(1).upper()}-{m.group(2)}",
                "name": _clean_text(m.group(3).rstrip(". ")),
                "body": [],
                "history": [],
                "in_history": False,
            }
            continue

        if current is None:
            # Pre-section preamble (cover page, etc.) -- ignore.
            continue

        cleaned = _clean_text(line)
        if not cleaned:
            continue

        if _HISTORY_START_RE.match(cleaned):
            current["in_history"] = True

        if current["in_history"]:
            current["history"].append(cleaned)
        else:
            current["body"].append(cleaned)

    _flush()
    return results


# ---------------------------------------------------------------------------
# Text helpers
# ---------------------------------------------------------------------------


def _check_reserved(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    lower = text.lower()
    for kw in RESERVED_KEYWORDS:
        if kw in lower:
            return "reserved"
    # "Repealed by Laws" at the very start of the addendum is also a reserved
    # marker even without parentheses.
    if re.search(r"\brepealed\b", lower) and "by laws" in lower:
        return "reserved"
    return None


def _clean_text(raw: str) -> str:
    text = raw.replace("\xa0", " ").replace("​", "")
    text = text.replace("‑", "-")  # non-breaking hyphen
    text = re.sub(r"\s+", " ", text).strip()
    return text


if __name__ == "__main__":
    main()
