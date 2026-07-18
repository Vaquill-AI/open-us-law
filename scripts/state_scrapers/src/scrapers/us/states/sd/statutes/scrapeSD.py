"""South Dakota Codified Laws (SDCL) scraper.

Source:
  Title TOC:   https://sdlegislature.gov/api/Statutes/{title}.html?all=true
  Chapter TOC: https://sdlegislature.gov/api/Statutes/{title}-{chapter}.html?all=true
  Section HTML: same chapter URL (sections are embedded in the chapter page)

The SD legislature site is a Vue SPA.  All substantive content is served through
a legacy SDLRC HTML API that pre-dates the SPA; no Selenium required.

Encoding:
  - Title and chapter ?all=true responses: UTF-16 LE (no BOM, raw bytes start 0x3C 0x00)
  - Individual section .html responses:    UTF-8

Hierarchy:  us/sd/statutes/title=N/chapter=M/section=S
Citation:   "S.D. Codified Laws § T-C-S"

SDCL title numbers run 1-62 (no gaps at endpoint level; the server returns 404
for non-existent titles).  Chapter numbers within each title use display strings
like "01", "01A", "16B" which map to citation prefixes "1", "1A", "16B" after
stripping leading zeros.
"""

from __future__ import annotations

import os
import re
import sys
import time
from pathlib import Path
from typing import Optional

import requests
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
JURISDICTION = "sd"
CORPUS = "statutes"
TABLE_NAME = f"{COUNTRY}_{JURISDICTION}_{CORPUS}"

BASE_API = "https://sdlegislature.gov/api/Statutes"

# Titles 1-62 exist in SDCL; the endpoint returns 404 for missing ones so we can
# probe all of them safely.
TITLE_RANGE = range(1, 63)

RESERVED_KEYWORDS = [
    "repealed",
    "transferred",
    "expired",
    "reserved",
    "superseded",
    "omitted",
]

# Section heading paragraph classes end exactly with "Normal" (no suffix).
# Body-text classes end with suffixes like "Normal-000000", "Statute",
# "StatuteNumber1", "NoIndent", etc.
# "Source:" lines typically use the "NoIndent" suffix.
_CLASS_HEADING_RE = re.compile(r"Normal$")
_SOURCE_RE = re.compile(r"^Source:", re.IGNORECASE)

# Match the section citation at the start of a heading paragraph.
# Examples:  "1-1-1.", "1-1-1.1.", "62-1-1.", "1-1A-1."
_SECTION_NUM_RE = re.compile(
    r"^(\d+[A-Za-z]*-\d+[A-Za-z]*-\d+[A-Za-z0-9]*(?:\.\d+)*)\."
)

# Chapter display number -> citation number (strip leading zeros, keep alpha suffix)
# e.g. "01" -> "1", "01A" -> "1A", "16B" -> "16B"
_LEADING_ZEROS_RE = re.compile(r"^0+(\d+[A-Za-z]*)$")


def main() -> None:
    corpus_node: Node = insert_jurisdiction_and_corpus_node(COUNTRY, JURISDICTION, CORPUS)
    _scrape_all_titles(corpus_node)


# ---------------------------------------------------------------------------
# Title level
# ---------------------------------------------------------------------------

def _titles_done_path() -> Path:
    """Where we persist the set of titles already fully scraped for resume."""
    try:
        from vaquill_pipeline.config import SETTINGS
        return SETTINGS.chunks_dir / "state_sd_titles_done.txt"
    except Exception:
        return Path(__file__).parent / "state_sd_titles_done.txt"


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
    """Scrape all SDCL titles in parallel with resume support.

    Concurrency via env ``VAQUILL_TITLE_WORKERS`` (default 8).
    Resume: titles previously completed are persisted in
    ``state_sd_titles_done.txt`` and skipped on re-runs.
    Set ``VAQUILL_FORCE_RESCRAPE=1`` to override.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    titles_done = set() if os.environ.get("VAQUILL_FORCE_RESCRAPE") else _load_titles_done()
    if titles_done:
        print(
            f"[scrapeSD] resume: {len(titles_done)} titles already done: "
            f"{sorted(titles_done, key=lambda x: int(x) if x.isdigit() else 999)}",
            flush=True,
        )

    work: list = []
    for title_num in TITLE_RANGE:
        url = f"{BASE_API}/{title_num}.html?all=true"
        raw = _fetch_raw(url)
        if raw is None:
            continue

        text = _decode(raw)
        if not text:
            continue

        soup = BeautifulSoup(text, "html.parser")
        paras = soup.find_all("p")
        if not paras:
            continue

        title_label = str(title_num)
        title_name = _extract_title_name(paras, title_num)

        node_id = f"{corpus_node.node_id}/title={title_label}"
        status = _check_reserved(title_name)

        title_node = Node(
            id=node_id,
            link=url,
            top_level_title=title_label,
            node_type="structure",
            level_classifier="title",
            number=title_label,
            node_name=title_name,
            parent=corpus_node.node_id,
            status=status,
        )
        insert_node(title_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)

        if status:
            continue
        if title_label in titles_done:
            continue
        work.append((title_node, paras, title_num, title_label))

    def _do_title(item):
        title_node, paras, title_num, title_label = item
        try:
            _scrape_title(title_node, paras, title_num)
            _mark_title_done(title_label)
            return (title_label, "ok", None)
        except Exception as e:
            return (title_label, "fail", str(e)[:200])

    workers = int(os.environ.get("VAQUILL_TITLE_WORKERS", "8"))
    print(f"[scrapeSD] running {len(work)} titles with {workers} parallel workers", flush=True)
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for fut in as_completed(ex.submit(_do_title, item) for item in work):
            num, status, err = fut.result()
            if status == "fail":
                print(f"[scrapeSD] title {num}: {status}: {err}", flush=True)
            else:
                print(f"[scrapeSD] title {num}: {status}", flush=True)


def _extract_title_name(paras: list, title_num: int) -> str:
    """Return the descriptive name for a title from its TOC HTML."""
    # Para 0: "TITLE{N}", para 1: descriptive name, para 2: empty, para 3: "Chapter"
    non_empty = [p.get_text(strip=True) for p in paras if p.get_text(strip=True)]
    if len(non_empty) >= 2:
        return non_empty[1]
    return f"Title {title_num}"


# ---------------------------------------------------------------------------
# Chapter level
# ---------------------------------------------------------------------------

def _scrape_title(title_node: Node, title_paras: list, title_num: int) -> None:
    """Extract chapter entries from the title TOC page and scrape each chapter."""
    # Chapter entries are in paragraphs whose class ends with 'B'.
    # Their text looks like: "01State Sovereignty And Jurisdiction"
    for p in title_paras:
        cls_list = p.get("class", [])
        if not cls_list:
            continue
        cls = cls_list[0]
        if not cls.endswith("B"):
            continue

        raw_text = p.get_text(strip=True)
        if not raw_text:
            continue

        ch_display, ch_name = _parse_chapter_entry(raw_text)
        if ch_display is None:
            continue

        ch_citation = _normalize_chapter_number(ch_display)
        ch_url = f"{BASE_API}/{title_num}-{ch_citation}.html?all=true"
        node_id = f"{title_node.node_id}/chapter={ch_citation}"
        status = _check_reserved(ch_name)

        chapter_node = Node(
            id=node_id,
            link=ch_url,
            top_level_title=title_node.top_level_title,
            node_type="structure",
            level_classifier="chapter",
            number=ch_citation,
            node_name=ch_name,
            parent=title_node.node_id,
            status=status,
        )
        insert_node(chapter_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)

        if not status:
            _scrape_chapter(chapter_node, ch_url)


def _parse_chapter_entry(raw: str) -> tuple[Optional[str], str]:
    """
    Parse a chapter TOC entry like '01State Sovereignty And Jurisdiction'.

    Returns (display_number, chapter_name) or (None, raw) on failure.
    display_number: the leading digits+alpha before the name, e.g. '01', '01A'.

    The tricky part: chapter alpha suffixes ('A', 'B', 'G', etc.) appear between
    the digit prefix and the chapter name, both of which start with uppercase letters.
    We use a lookahead to consume the alpha letter only when it is itself followed by
    another uppercase letter (i.e. the alpha is a suffix, not the start of the name).

    Examples:
      '01State...'             -> ('01', 'State...')
      '01AUnconstitutional...' -> ('01A', 'Unconstitutional...')
      '16GEconomic...'         -> ('16G', 'Economic...')
      '07Governor'             -> ('07', 'Governor')
    """
    # Consume the digit prefix plus an optional alpha-suffix letter, but only
    # when at least one more name character follows. This is robust to future
    # names starting with digits, spaces, or lowercase letters: we only treat
    # a trailing alpha as a chapter-letter suffix when the next char is also
    # an uppercase letter (the conventional case). Otherwise we keep the
    # alpha as part of the name.
    m = re.match(r"^(\d+(?:[A-Z](?=[A-Z]))?)(.+)$", raw)
    if m:
        display_num = m.group(1).strip()
        name = m.group(2).strip()
        return display_num, f"Chapter {display_num}: {name}"
    # Fallback: digits only, anything (incl. empty/space/lowercase/digit) after.
    m2 = re.match(r"^(\d+)(.*)$", raw)
    if m2 and m2.group(1):
        display_num = m2.group(1).strip()
        name = m2.group(2).strip()
        return display_num, f"Chapter {display_num}: {name}" if name else f"Chapter {display_num}"
    return None, raw


def _normalize_chapter_number(display: str) -> str:
    """Convert display chapter number '01A' -> '1A', '01' -> '1', '16B' -> '16B'."""
    m = _LEADING_ZEROS_RE.match(display)
    if m:
        return m.group(1)
    return display


# ---------------------------------------------------------------------------
# Section level - parse chapter all=true HTML
# ---------------------------------------------------------------------------

def _scrape_chapter(chapter_node: Node, url: str) -> None:
    """Fetch the chapter page and insert a section node for every section found."""
    raw = _fetch_raw(url)
    if raw is None:
        return

    text = _decode(raw)
    if not text:
        return

    soup = BeautifulSoup(text, "html.parser")
    sections = _parse_sections_from_chapter(soup)

    if not sections:
        # Could indicate SDLRC renamed the stylesheet hash prefix or changed
        # the heading class suffix. Fail loud so we notice.
        print(
            f"[scrapeSD][WARN] zero sections parsed for chapter "
            f"{chapter_node.node_id} url={url}",
            flush=True,
        )
        return

    for sec_num, sec_name, body_paras, source_text in sections:
        _insert_section(chapter_node, sec_num, sec_name, body_paras, source_text, url)


def _parse_sections_from_chapter(
    soup: BeautifulSoup,
) -> list[tuple[str, str, list[str], str]]:
    """
    Return a list of (section_number, section_name, body_paragraphs, source_text)
    tuples, one per section found in the combined chapter HTML.

    Strategy: each section heading paragraph has class ending exactly with 'Normal'
    and contains an anchor with a SENU-classed span holding the canonical citation.
    We extract the citation number from either the SENU span text or the href
    '?Statute=...' query param, both of which are more reliable than parsing
    the full paragraph text (which has whitespace inserted between spans).

    All subsequent paragraphs with the same CSS hash prefix belong to that section's
    body until the next 'Normal' heading from a different hash.
    """
    all_paras = soup.find_all("p")
    sections: list[tuple[str, str, list[str], str]] = []

    current_hash: Optional[str] = None
    current_sec_num: Optional[str] = None
    current_sec_name: Optional[str] = None
    current_body: list[str] = []
    current_source: str = ""

    def _flush() -> None:
        if current_sec_num is not None:
            sections.append(
                (current_sec_num, current_sec_name or current_sec_num, list(current_body), current_source)
            )

    for p in all_paras:
        cls_list = p.get("class", [])
        if not cls_list:
            continue
        cls = cls_list[0]

        # Skip TOC entries (class ends with 'B').
        if cls.endswith("B"):
            continue

        cls_hash = _extract_hash(cls)
        if cls_hash is None:
            continue

        if _CLASS_HEADING_RE.search(cls):
            # Potential section heading paragraph.  Extract the canonical
            # citation from the SENU span (e.g. <span class="...SENU">1-1-1.1</span>).
            sec_num = _extract_section_num_from_heading(p, cls_hash)
            if sec_num is not None:
                _flush()
                current_hash = cls_hash
                current_sec_num = sec_num
                # Build section name from plain text (strip=True avoids span spaces).
                plain = _clean_text(p.get_text(strip=True))
                # plain looks like '1-1-1.1.Heading text.' or '1-1-1.Heading text.'
                # Remove the leading citation prefix up to and including the
                # first dot after the citation number.
                name_tail = re.sub(r"^[\d\.\-]+\.\s*", "", plain).rstrip(".")
                current_sec_name = f"{sec_num}. {name_tail}" if name_tail else sec_num
                current_body = []
                current_source = ""
                continue
            # Not a section heading. Check if it's a Source: paragraph that
            # belongs to the current section (same hash).
            if cls_hash == current_hash and current_sec_num is not None:
                raw_text = _clean_text(p.get_text(separator=" "))
                if raw_text and _SOURCE_RE.match(raw_text):
                    current_source += raw_text + " "
            continue

        # Body paragraph: belongs to current section if hash matches.
        if cls_hash != current_hash or current_sec_num is None:
            continue

        raw_text = _clean_text(p.get_text(separator=" "))
        if not raw_text:
            continue

        if _SOURCE_RE.match(raw_text):
            current_source += raw_text + " "
        else:
            current_body.append(raw_text)

    _flush()
    return sections


def _extract_section_num_from_heading(p, cls_hash: str) -> Optional[str]:
    """
    Extract canonical section number from a heading paragraph.

    Tries in order:
    1. The SENU-classed span text (e.g. '1-1-1.1').
    2. The ?Statute=... query param in an <a> href.
    3. Fallback: regex on get_text(strip=True).
    """
    import urllib.parse

    # 1. SENU span: class contains hash + 'SENU'
    senu_class_suffix = "SENU"
    for span in p.find_all("span"):
        span_cls = " ".join(span.get("class", []))
        if senu_class_suffix in span_cls:
            num = span.get_text(strip=True)
            if num and _SECTION_NUM_RE.match(num + "."):
                return num

    # 2. Anchor href
    for a in p.find_all("a", href=True):
        href = a["href"]
        # Patterns: ?Statute=1-1-1.1  or  ?Type=Statute&Statute=1-1-1.1
        parsed = urllib.parse.urlparse(href)
        qs = urllib.parse.parse_qs(parsed.query)
        num = (qs.get("Statute") or [""])[0]
        if num and _SECTION_NUM_RE.match(num + "."):
            return num

    # 3. Regex on stripped text
    plain = p.get_text(strip=True)
    m = _SECTION_NUM_RE.match(plain)
    if m:
        return m.group(1)

    return None


def _extract_hash(cls: str) -> Optional[str]:
    """
    Extract the hex hash prefix from a CSS class name.

    Classes look like:
      s2030343Normal         -> hash 's2030343'
      s2030343Normal-000000  -> hash 's2030343'
      s8cea3b483ed344feb102c7b7238b1aecB -> hash 's8cea3b483ed344feb102c7b7238b1aec'

    We keep everything up to (but not including) any uppercase letter sequence
    that starts the suffix.  Since suffixes are: Normal, Statute, NoIndent, B,
    we split on the first uppercase character after the initial 's' and digits.
    """
    # Match hash: starts with 's', followed by hex chars.
    m = re.match(r"^(s[0-9a-f]+)", cls)
    if m:
        return m.group(1)
    return None


def _insert_section(
    chapter_node: Node,
    sec_num: str,
    sec_name: str,
    body_paras: list[str],
    source_text: str,
    chapter_url: str,
) -> None:
    """Build and insert a section Node."""
    node_id = f"{chapter_node.node_id}/section={sec_num}"
    citation = f"S.D. Codified Laws § {sec_num}"
    status = _check_reserved(sec_name)

    if status:
        section_node = Node(
            id=node_id,
            link=chapter_url,
            top_level_title=chapter_node.top_level_title,
            node_type="content",
            level_classifier="section",
            number=sec_num,
            node_name=sec_name,
            parent=chapter_node.node_id,
            citation=citation,
            status=status,
        )
        insert_node(section_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)
        return

    node_text: Optional[NodeText] = None
    addendum: Optional[Addendum] = None

    if body_paras:
        node_text = NodeText()
        for para in body_paras:
            if para:
                node_text.add_paragraph(text=para)

    source_clean = source_text.strip()
    if source_clean:
        addendum = Addendum()
        addendum.history = AddendumType(type="history", text=source_clean)

    section_node = Node(
        id=node_id,
        link=chapter_url,
        top_level_title=chapter_node.top_level_title,
        node_type="content",
        level_classifier="section",
        number=sec_num,
        node_name=sec_name,
        parent=chapter_node.node_id,
        citation=citation,
        node_text=node_text,
        addendum=addendum,
    )
    insert_node(section_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

_FETCH_MAX_ATTEMPTS = 4
_FETCH_BACKOFF_BASE = 1.5  # seconds: 1.5, 3, 6, 12...


def _fetch_raw(url: str) -> Optional[bytes]:
    """Fetch URL bytes with retry+exponential backoff on transient failure.

    Uses vaquill_pipeline.http_client when available, else requests.
    Returns None on 404 or after exhausting all retries.
    """
    try:
        from vaquill_pipeline.http_client import fetch_bytes
        _has_pipeline = True
    except ImportError:
        fetch_bytes = None  # type: ignore[assignment]
        _has_pipeline = False

    headers = {"User-Agent": "Mozilla/5.0 (compatible; VaquillBot/1.0)"}
    last_exc: Optional[Exception] = None

    for attempt in range(1, _FETCH_MAX_ATTEMPTS + 1):
        try:
            if _has_pipeline:
                body, _ct = fetch_bytes(url, timeout=30)  # type: ignore[misc]
                return body
            r = requests.get(url, timeout=30, headers=headers)
            if r.status_code == 404:
                return None
            # Retry on 5xx and 429; raise for other 4xx.
            if r.status_code >= 500 or r.status_code == 429:
                raise requests.HTTPError(f"status {r.status_code}")
            r.raise_for_status()
            return r.content
        except Exception as exc:
            last_exc = exc
            if attempt < _FETCH_MAX_ATTEMPTS:
                sleep_s = _FETCH_BACKOFF_BASE * (2 ** (attempt - 1))
                print(
                    f"[WARN] fetch attempt {attempt}/{_FETCH_MAX_ATTEMPTS} failed for "
                    f"{url}: {exc}; retrying in {sleep_s:.1f}s",
                    flush=True,
                )
                time.sleep(sleep_s)
            else:
                print(
                    f"[WARN] fetch giving up after {attempt} attempts for {url}: {exc}",
                    flush=True,
                )
    _ = last_exc
    return None


def _decode(raw: bytes) -> Optional[str]:
    """
    Decode raw HTTP response bytes to str.

    The SDLRC API returns two encoding variants:
      - UTF-16 LE (no BOM): bytes start 0x3C 0x00 ('<' in UTF-16).
        Identified by the second byte being 0x00.
      - UTF-8: regular ASCII/UTF-8 HTML.
    """
    if not raw:
        return None
    if len(raw) >= 2 and raw[1] == 0x00:
        # UTF-16 LE without BOM.
        try:
            return raw.decode("utf-16-le")
        except UnicodeDecodeError:
            pass
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        try:
            return raw.decode("latin-1")
        except UnicodeDecodeError:
            return None


# ---------------------------------------------------------------------------
# Text utilities
# ---------------------------------------------------------------------------

def _clean_text(raw: str) -> str:
    text = raw.replace("\xa0", " ").replace("\r\n", " ").replace("\r", " ").replace("\n", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


_RESERVED_TOKEN_RE = re.compile(
    r"\b(?:" + "|".join(re.escape(k) for k in RESERVED_KEYWORDS) + r")\b",
    re.IGNORECASE,
)


def _check_reserved(text: str) -> Optional[str]:
    """Whole-word match only. Avoids false positives like 'reserved powers'
    or 'reserved rights' triggering on substring 'reserved'.

    We require the keyword to appear as its own token AND for there to be no
    immediately following noun that turns it into a substantive phrase
    (e.g. 'reserved powers'). The simplest robust rule: match a reserved
    keyword followed by end-of-string, punctuation, or whitespace+end.
    """
    if not text:
        return None
    # Bail if there's no token-boundary hit at all.
    if not _RESERVED_TOKEN_RE.search(text):
        return None
    # Stricter: the keyword should appear in a terminal/standalone way, e.g.
    # "[Reserved]", "Repealed.", "Section 1-2-3. Repealed." rather than as an
    # adjective modifying a following noun ("reserved powers", "repealed act").
    # Heuristic: token must be followed by end, punctuation, ']', or another
    # reserved-style marker. Otherwise treat as non-reserved.
    pattern = (
        r"\b(" + "|".join(re.escape(k) for k in RESERVED_KEYWORDS) + r")\b"
        r"(?=$|[\s]*[\.\,\;\:\)\]\}]|\s*$)"
    )
    if re.search(pattern, text, re.IGNORECASE):
        return "reserved"
    return None


if __name__ == "__main__":
    main()
