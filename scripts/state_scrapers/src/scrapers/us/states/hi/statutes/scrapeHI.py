"""Hawaii Revised Statutes (HRS) scraper.

Hierarchy scraped:
    TOC (capitol.hawaii.gov/docs/hrs.htm)
      -> Division (structure node, 5 DIVISIONs)
           -> Title (structure node, parents under current division)
                -> Chapter (structure node, one page per chapter)
                     -> Section (content node, one page per section)

Section pages are linked via sequential "Next" navigation (no inline
section links on chapter pages). Sections within a chapter are walked
by following "Next" until the link leaves the chapter prefix or returns
to a chapter index page (URL ends with "-.htm").

Node IDs:
    us/hi/statutes/division=D/title=N/chapter=M/section=S

Citation format:
    "Haw. Rev. Stat. § <SECTION>"  e.g. "Haw. Rev. Stat. § 1-1"
"""

from __future__ import annotations

import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Optional, Tuple

# ---------------------------------------------------------------------------
# Path bootstrap (matches ME/VT canonical pattern)
# ---------------------------------------------------------------------------

_current_file = Path(__file__).resolve()
_src_directory = _current_file.parent
while _src_directory.name != "src" and _src_directory.parent != _src_directory:
    _src_directory = _src_directory.parent
_project_root = _src_directory.parent
if str(_project_root) not in sys.path:
    sys.path.append(str(_project_root))

# ---------------------------------------------------------------------------
# Imports
# ---------------------------------------------------------------------------

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
JURISDICTION = "hi"
CORPUS = "statutes"
TABLE_NAME = f"{COUNTRY}_{JURISDICTION}_{CORPUS}"

BASE_URL = "https://www.capitol.hawaii.gov"
TOC_URL = "https://www.capitol.hawaii.gov/docs/hrs.htm"

RESERVED_KEYWORDS = [
    "(repealed)",
    "(expired)",
    "(reserved)",
    "(renumbered)",
    "--repealed",
]

# Lines that start with these strings are addendum / notes (not body text).
_NOTES_HEADINGS = {
    "attorney general opinions",
    "law journals and reviews",
    "case notes",
    "rules of court",
    "cross references",
    "definitions",
    "revision notes",
    "compiler's notes",
    "federal law and state law compared",
    "referred to:",
    "hawaii constitutional conventions discussions",
    "hawaii administrative rules",
    # Added per audit:
    "note",
    "notes",
    "previous",
    "effect of amendments",
    "amendment notes",
    "history",
    "history of section",
    "editor's note",
    "editor's notes",
    "source",
    "annotations",
    "related provisions",
    "research references",
}

# History line pattern: "(L 1892, ...) or "[L 1959...]"
_HISTORY_RE = re.compile(r"\[?[Ll]\s+\d{4}", re.IGNORECASE)

# Mojibake markers (UTF-8 mis-decoded as Latin-1).
_MOJIBAKE_MARKERS = (
    "\xc2",
    "\xe2\x80",
    "\xe2\x82",
    "\xe2\x84",
    "\xe2\x86",
    "â€",
)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    corpus_node: Node = insert_jurisdiction_and_corpus_node(COUNTRY, JURISDICTION, CORPUS)
    _scrape_toc(corpus_node)


# ---------------------------------------------------------------------------
# Resume support (chapter-level)
# ---------------------------------------------------------------------------


def _chapters_done_path() -> Path:
    """Where we persist the set of chapters already fully scraped for resume."""
    try:
        from vaquill_pipeline.config import SETTINGS  # type: ignore
        return SETTINGS.chunks_dir / "state_hi_chapters_done.txt"
    except Exception:
        return Path(__file__).parent / "state_hi_chapters_done.txt"


def _load_chapters_done() -> set:
    path = _chapters_done_path()
    if not path.exists():
        return set()
    return {line.strip() for line in path.read_text().splitlines() if line.strip()}


def _mark_chapter_done(key: str) -> None:
    path = _chapters_done_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a") as fh:
        fh.write(f"{key}\n")
        fh.flush()


# ---------------------------------------------------------------------------
# TOC parsing: build division->title->chapter graph, then parallel-scrape
# ---------------------------------------------------------------------------


def _scrape_toc(corpus_node: Node) -> None:
    """Parse the HRS TOC page and dispatch chapter scrapes in parallel.

    The TOC page lists links like:
        DIVISION 1. GOVERNMENT
        TITLE 1. GENERAL PROVISIONS
        Common Law; Construction of Laws -> chapter 1 URL
        ...
        DIVISION 2. ...
        TITLE 10. ...
    """
    soup = get_url_as_soup(TOC_URL)

    current_division_number: Optional[str] = None
    current_division_name: Optional[str] = None
    division_node: Optional[Node] = None
    seen_division_ids: set[str] = set()

    current_title_number: Optional[str] = None
    current_title_name: Optional[str] = None
    title_node: Optional[Node] = None
    seen_title_ids: set[str] = set()

    chapters_done = (
        set() if os.environ.get("VAQUILL_FORCE_RESCRAPE") else _load_chapters_done()
    )
    if chapters_done:
        print(f"[scrapeHI] resume: {len(chapters_done)} chapters already done", flush=True)

    work: List[Node] = []

    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"].strip()
        text = _clean_text(a_tag.get_text())

        # Detect TITLE or DIVISION headings (these may or may not end in -.htm).
        division_m = re.match(r"DIVISION\s+(\d+[\w\.]*)[.\s]+(.*)", text, re.IGNORECASE)
        title_m = re.match(r"TITLE\s+(\d+[\w\.]*)[.\s]+(.*)", text, re.IGNORECASE)

        if division_m:
            current_division_number = division_m.group(1).strip().rstrip(".")
            current_division_name = text
            division_node = None
            # Reset title tracking on division boundary (fix #2).
            current_title_number = None
            current_title_name = None
            title_node = None
            continue

        if title_m:
            current_title_number = title_m.group(1).strip().rstrip(".")
            current_title_name = text
            title_node = None
            continue

        if not href.endswith("-.htm"):
            continue

        chapter_url = _make_absolute(href)
        if current_title_number is None:
            continue

        chapter_number = _chapter_number_from_url(chapter_url)
        if chapter_number is None:
            continue

        chapter_name = text
        chapter_status = _check_reserved(chapter_name)

        # Lazily emit division node (fix #1).
        if current_division_number is not None and division_node is None:
            div_id = f"{corpus_node.node_id}/division={current_division_number}"
            division_node = Node(
                id=div_id,
                link=TOC_URL,
                top_level_title=current_division_number,
                node_type="structure",
                level_classifier="division",
                number=current_division_number,
                node_name=current_division_name or f"Division {current_division_number}",
                parent=corpus_node.node_id,
            )
            if div_id not in seen_division_ids:
                seen_division_ids.add(div_id)
                insert_node(division_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)

        # Parent for title: division if present, else corpus.
        title_parent_node = division_node if division_node is not None else corpus_node

        # Lazily insert the title node once per title.
        if title_node is None:
            title_id = f"{title_parent_node.node_id}/title={current_title_number}"
            title_node = Node(
                id=title_id,
                link=chapter_url,
                top_level_title=current_title_number,
                node_type="structure",
                level_classifier="title",
                number=current_title_number,
                node_name=current_title_name or f"Title {current_title_number}",
                parent=title_parent_node.node_id,
            )
            if title_id not in seen_title_ids:
                seen_title_ids.add(title_id)
                insert_node(title_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)

        chapter_id = f"{title_node.node_id}/chapter={chapter_number}"
        chapter_node = Node(
            id=chapter_id,
            link=chapter_url,
            top_level_title=current_title_number,
            node_type="structure",
            level_classifier="chapter",
            number=chapter_number,
            node_name=chapter_name,
            parent=title_node.node_id,
            status=chapter_status,
        )
        insert_node(chapter_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)

        if chapter_status:
            continue
        if chapter_id in chapters_done:
            continue
        work.append(chapter_node)

    workers = int(os.environ.get("VAQUILL_TITLE_WORKERS", "8"))
    print(f"[scrapeHI] running {len(work)} chapters with {workers} parallel workers", flush=True)

    def _do_chapter(chap: Node):
        try:
            _scrape_chapter_sections(chap)
            _mark_chapter_done(chap.node_id)
            return (chap.node_id, "ok", None)
        except Exception as e:
            return (chap.node_id, "fail", str(e)[:200])

    with ThreadPoolExecutor(max_workers=workers) as ex:
        for fut in as_completed(ex.submit(_do_chapter, c) for c in work):
            cid, status, err = fut.result()
            if status == "fail":
                print(f"[scrapeHI] chapter {cid}: {status}: {err}", flush=True)


# ---------------------------------------------------------------------------
# Section walking for one chapter
# ---------------------------------------------------------------------------


def _chapter_prefix(chapter_url: str) -> Optional[str]:
    """Return the chapter directory prefix from a chapter index URL.

    e.g. .../HRS0001/HRS_0001-.htm  -> .../HRS0001/
    Sections under this chapter all live at the same prefix.
    """
    m = re.match(r"^(.*/HRS\d+[A-Z]*/)", chapter_url)
    return m.group(1) if m else None


def _scrape_chapter_sections(chapter_node: Node) -> None:
    """Walk sections of a chapter via the sequential Next navigation."""
    chapter_url = str(chapter_node.link)
    chapter_prefix = _chapter_prefix(chapter_url)

    chapter_soup = get_url_as_soup(chapter_url)
    first_section_url = _find_next_link(chapter_soup, chapter_url)

    if first_section_url is None or first_section_url.endswith("-.htm"):
        print(f"[scrapeHI] chapter {chapter_node.node_id}: 0 sections (no first Next)", flush=True)
        return

    # Chapter-runaway guard (fix #3).
    if chapter_prefix and not first_section_url.startswith(chapter_prefix):
        print(
            f"[scrapeHI] chapter {chapter_node.node_id}: first Next leaves chapter prefix "
            f"({first_section_url}); stopping",
            flush=True,
        )
        return

    current_url: Optional[str] = first_section_url
    seen_urls: set = set()
    sections_emitted = 0

    while current_url is not None:
        if current_url in seen_urls:
            break
        seen_urls.add(current_url)

        section_soup = get_url_as_soup(current_url)
        _process_section_page(section_soup, current_url, chapter_node)
        sections_emitted += 1

        next_url = _find_next_link(section_soup, current_url)
        if next_url is None:
            break
        if next_url.endswith("-.htm"):
            break
        # Chapter-runaway guard (fix #3).
        if chapter_prefix and not next_url.startswith(chapter_prefix):
            print(
                f"[scrapeHI] chapter {chapter_node.node_id}: Next link leaves chapter prefix "
                f"({next_url}); stopping at {sections_emitted} sections",
                flush=True,
            )
            break
        current_url = next_url

    if sections_emitted == 0:
        print(f"[scrapeHI] chapter {chapter_node.node_id}: 0 sections after walk", flush=True)


# ---------------------------------------------------------------------------
# Section page parsing
# ---------------------------------------------------------------------------


def _process_section_page(soup, url: str, chapter_node: Node) -> None:
    """Parse one section page and emit a content Node."""
    paragraphs = soup.find_all("p")
    if not paragraphs:
        return

    first_p_raw = paragraphs[0].get_text(separator=" ")
    first_p = _clean_text(first_p_raw)

    sec_number = _extract_section_number(first_p)
    if sec_number is None:
        return

    sec_name = _extract_section_name(first_p)
    citation = f"Haw. Rev. Stat. § {sec_number}"
    node_id = f"{chapter_node.node_id}/section={sec_number}"
    status = _check_reserved(sec_name or first_p)

    if status:
        section_node = Node(
            id=node_id,
            link=url,
            top_level_title=chapter_node.top_level_title,
            node_type="content",
            level_classifier="section",
            number=sec_number,
            node_name=sec_name or sec_number,
            parent=chapter_node.node_id,
            citation=citation,
            status=status,
        )
        insert_node(section_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)
        return

    node_text, addendum = _parse_section_body(first_p, paragraphs)

    section_node = Node(
        id=node_id,
        link=url,
        top_level_title=chapter_node.top_level_title,
        node_type="content",
        level_classifier="section",
        number=sec_number,
        node_name=sec_name or sec_number,
        parent=chapter_node.node_id,
        citation=citation,
        node_text=node_text,
        addendum=addendum,
    )
    insert_node(section_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)


def _parse_section_body(
    first_p: str,
    paragraphs,
) -> Tuple[Optional[NodeText], Optional[Addendum]]:
    """Build NodeText and Addendum from section page paragraphs."""
    node_text = NodeText()
    history_text = ""

    hist_split = re.split(r"\s+(?=\[(?:Am\s+)?L\s+\d{4})", first_p, maxsplit=1)
    if len(hist_split) == 2:
        body_part = hist_split[0].strip()
        history_text = hist_split[1].strip()
    else:
        body_part = first_p

    body_part = _strip_section_heading(body_part)
    if body_part:
        node_text.add_paragraph(text=body_part)

    for p_tag in paragraphs[1:]:
        raw = p_tag.get_text(separator=" ")
        text = _clean_text(raw)
        if not text:
            continue

        # Stop at notes headings (fix #5, #6: removed premature-break heuristic).
        lower = text.lower().rstrip(":").rstrip(".")
        if lower in _NOTES_HEADINGS:
            break
        if any(lower.startswith(h) for h in _NOTES_HEADINGS):
            break

        if _HISTORY_RE.match(text) or text.startswith("[L ") or text.startswith("[Am"):
            history_text = text
            break

        node_text.add_paragraph(text=text)

    addendum: Optional[Addendum] = None
    if history_text:
        addendum = Addendum()
        addendum.history = AddendumType(type="history", text=history_text)

    if not node_text.paragraphs:
        node_text = None  # type: ignore[assignment]

    return node_text, addendum


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


# Case-insensitive Next variants (fix #4).
_NEXT_TEXT_RE = re.compile(r"^\s*next\s*(?:>+|»|&gt;)?\s*$", re.IGNORECASE)


def _find_next_link(soup, current_url: str) -> Optional[str]:
    """Return the absolute URL of the Next link, or None.

    Matches "Next", "Next >", "Next >>", "NEXT", " next ", etc.
    """
    for a_tag in soup.find_all("a", href=True):
        txt = a_tag.get_text().strip()
        if _NEXT_TEXT_RE.match(txt):
            href = a_tag["href"].strip()
            return _resolve_relative(href, current_url)
    return None


def _resolve_relative(href: str, base_url: str) -> str:
    if href.startswith("http"):
        return href
    if href.startswith("/"):
        return BASE_URL + href
    from urllib.parse import urljoin
    return urljoin(base_url, href)


def _make_absolute(href: str) -> str:
    if href.startswith("http"):
        return href
    if href.startswith("/"):
        return BASE_URL + href
    return BASE_URL + "/" + href


def _chapter_number_from_url(url: str) -> Optional[str]:
    m = re.search(r"/HRS(\d+[A-Z]*)[-/]", url)
    if m:
        raw = m.group(1)
        mm = re.match(r"^0*(\d+)([A-Z]*)$", raw)
        if mm:
            return mm.group(1) + mm.group(2)
    return None


def _extract_section_number(text: str) -> Optional[str]:
    normalised = re.sub(r"(\w)\s+-\s+(\w)", r"\1-\2", text)
    m = re.search(r"§\s*([\d][\w\-\.]*)", normalised)
    if m:
        return m.group(1).rstrip(".")
    return None


def _extract_section_name(text: str) -> Optional[str]:
    text = re.sub(r"(\w)\s+-\s+(\w)", r"\1-\2", text)
    stripped = re.sub(r"^\s*\[?§\s*[\d][\w\-\.]*\]?\s*", "", text).strip()
    m = re.match(r"^([^.]{1,150})\.\s+[A-Z(]", stripped)
    if m:
        return m.group(1).strip() + "."
    m2 = re.match(r"^([^.]{1,150})\.", stripped)
    if m2:
        return m2.group(1).strip() + "."
    return stripped[:150] if stripped else None


def _strip_section_heading(text: str) -> str:
    text = re.sub(r"(\w)\s+-\s+(\w)", r"\1-\2", text)
    m = re.match(
        r"^\s*\[?§\s*[\d][\w\-\.]*\]?\s+[^.]{1,200}\.\s+(.*)",
        text,
        re.DOTALL,
    )
    if m:
        return m.group(1).strip()
    return text


def _check_reserved(text: str) -> Optional[str]:
    lower = text.lower()
    for kw in RESERVED_KEYWORDS:
        if kw in lower:
            return "reserved"
    return None


def _fix_encoding(s: str) -> str:
    """Undo Latin-1/UTF-8 mojibake. Mirrors DE _fix_encoding (fix #8)."""
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


def _clean_text(raw: str) -> str:
    """Normalise whitespace; repair mojibake.

    Note: the previous version used byte-pair replacements (e.g.
    ``"\\xc2\\xa0"``) which were dead code on a Python str. Dropped (fix #8).
    """
    if raw is None:
        return ""
    text = _fix_encoding(raw)
    text = text.replace("\xa0", " ")
    text = text.replace("\r", " ").replace("\n", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


# ---------------------------------------------------------------------------

if __name__ == "__main__":
    main()
