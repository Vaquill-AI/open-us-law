"""Texas (TX) statutes scraper.

Source: Texas Constitution and Statutes site (statutes.capitol.texas.gov)
API:    https://tcss.legis.texas.gov/api/  (undocumented JSON API backing the Angular SPA)
HTML:   https://tcss.legis.texas.gov/resources/{CODE}/htm/{CODE}.{N}.htm

Strategy (HTTP + bs4 only, no Selenium):
1. Iterate ALL 26 Texas codes (Agriculture through Water).
2. Fetch chapter list via GetStatuteArray API per code.
3. For each chapter, fetch the static HTML from tcss.legis.texas.gov/resources/.
4. Parse <p class="left"> paragraphs:
   - Paragraphs starting with "Sec. " open a new content node.
   - Subsequent indented paragraphs are added to the current section's NodeText.
   - Non-indented paragraphs (history lines) are treated as addendum history.
5. Build Node objects (structure: code, chapter; content: section) and insert each.

Parallelism: codes run in parallel via ThreadPoolExecutor; concurrency is
controlled by env var ``VAQUILL_TITLE_WORKERS`` (default 8). Code-level
resume is persisted in ``state_tx_titles_done.txt`` so a crashed/interrupted
run skips codes that have already completed (set ``VAQUILL_FORCE_RESCRAPE=1``
to override).
"""
from __future__ import annotations

import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Optional, Tuple

from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Resolve project root and add to sys.path (mirrors DE pattern).
# ---------------------------------------------------------------------------
current_file = Path(__file__).resolve()
src_directory = current_file.parent
while src_directory.name != "src" and src_directory.parent != src_directory:
    src_directory = src_directory.parent
project_root = src_directory.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import requests
from requests.exceptions import HTTPError, ConnectionError as ReqConnectionError

# Vaquill: shared HTTP layer (proxy + UA rotation + Cloudflare bypass + pool).
from vaquill_pipeline.http_client import fetch_html

from src.utils.pydanticModels import (
    Addendum,
    AddendumType,
    Node,
    NodeText,
)
from src.utils.scrapingHelpers import (
    insert_jurisdiction_and_corpus_node,
    insert_node,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
COUNTRY = "us"
JURISDICTION = "tx"
CORPUS = "statutes"
TABLE_NAME = f"{COUNTRY}_{JURISDICTION}_{CORPUS}"

TCAS_API = "https://tcss.legis.texas.gov/api/"
TCAS_RESOURCES = "https://tcss.legis.texas.gov/resources/"
STATUTE_ORIGIN = "https://statutes.capitol.texas.gov"

# All 26 Texas codes. Verified 2026-05-11 by probing
# https://tcss.legis.texas.gov/resources/{CODE}/htm/{CODE}.*.htm.
# Note: the "Code Construction Act" is NOT a standalone code - it lives as
# chapters 311-312 inside the Government Code, so it is not listed here.
TX_CODES: List[Tuple[str, str]] = [
    ("AG", "Agriculture Code"),
    ("AL", "Alcoholic Beverage Code"),
    ("BC", "Business & Commerce Code"),
    ("BO", "Business Organizations Code"),
    ("CP", "Civil Practice and Remedies Code"),
    ("CR", "Code of Criminal Procedure"),
    ("CV", "Vernon's Civil Statutes"),
    ("ED", "Education Code"),
    ("EL", "Election Code"),
    ("ES", "Estates Code"),
    ("FA", "Family Code"),
    ("FI", "Finance Code"),
    ("GV", "Government Code"),
    ("HS", "Health and Safety Code"),
    ("HR", "Human Resources Code"),
    ("I1", "Insurance Code - Not Codified"),
    ("IN", "Insurance Code"),
    ("LA", "Labor Code"),
    ("LG", "Local Government Code"),
    ("NR", "Natural Resources Code"),
    ("OC", "Occupations Code"),
    ("PB", "Probate Code"),
    ("PE", "Penal Code"),
    ("PR", "Property Code"),
    ("PW", "Parks and Wildlife Code"),
    ("SD", "Special District Local Laws Code"),
    ("TX", "Tax Code"),
    ("TN", "Transportation Code"),
    ("UT", "Utilities Code"),
    ("WA", "Water Code"),
    ("WL", "Auxiliary Water Laws"),
]

RESERVED_KEYWORDS = ["[Repealed", "[Expired", "[Reserved", "Repealed"]

REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/html, */*",
    "Origin": STATUTE_ORIGIN,
    "Referer": STATUTE_ORIGIN + "/",
}
REQUEST_DELAY = 0.3  # seconds between requests to be a polite scraper


# ---------------------------------------------------------------------------
# Mojibake fixer (TX site occasionally serves curly quotes / dashes mis-encoded
# when Content-Type lacks an explicit charset). Mirrors the DE pattern.
# ---------------------------------------------------------------------------

_MOJIBAKE_MARKERS = (
    "\xc2",      # Â prefix
    "\xe2\x80",  # â\x80 (curly quotes, em/en dashes)
    "\xe2\x82",
    "\xe2\x84",
    "\xe2\x86",
    "â€",
)


def _fix_encoding(s: str) -> str:
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


# ---------------------------------------------------------------------------
# HTTP helpers (route through vaquill_pipeline.http_client for proxy + UA
# rotation + connection pooling + Cloudflare bypass).
# ---------------------------------------------------------------------------

def _get(url: str, as_json: bool = False, retries: int = 3, timeout: float = 30.0):
    """GET via fetch_html. Returns parsed JSON or text.

    ``timeout`` is the per-attempt HTTP timeout. Chapter-list endpoints for
    huge codes (SD has 1300+ chapters) need 60-120s; per-section HTML pages
    are small and fit in the default 30s.
    """
    last_exc: Optional[BaseException] = None
    for attempt in range(retries):
        try:
            body = fetch_html(
                url,
                country_code="us",
                timeout=timeout,
                max_retries=2,
                referer=STATUTE_ORIGIN + "/",
                extra_headers={
                    "Accept": "application/json, text/html, */*",
                    "Origin": STATUTE_ORIGIN,
                },
            )
            if as_json:
                return json.loads(body)
            return body
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
    if last_exc is not None:
        raise last_exc
    return None  # unreachable


def _get_soup(url: str) -> BeautifulSoup:
    html = _get(url, as_json=False)
    return BeautifulSoup(html, "html.parser")


# ---------------------------------------------------------------------------
# Resume: persist completed codes so a crashed run skips them on restart.
# ---------------------------------------------------------------------------

def _titles_done_path():
    from vaquill_pipeline.config import SETTINGS
    return SETTINGS.chunks_dir / "state_tx_titles_done.txt"


def _load_titles_done() -> set:
    path = _titles_done_path()
    if not path.exists():
        return set()
    return {l.strip() for l in path.read_text().splitlines() if l.strip()}


def _mark_title_done(code: str) -> None:
    path = _titles_done_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a") as fh:
        fh.write(f"{code}\n")
        fh.flush()


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

# codeID lookup for TX codes (used by the PopulateChapterList fallback).
# Sourced from https://statutes.capitol.texas.gov/assets/QuickCodes.json on
# 2026-05-27. The codeID is the TLC internal identifier referenced by the
# QuickSearch endpoints, which return ALL chapters even for codes too large
# for GetStatuteArray to enumerate (notably SD with 1300+ chapters).
TX_CODE_IDS: dict[str, str] = {
    "AG": "1", "AL": "2", "BC": "4", "BO": "32", "CN": "5", "CP": "6",
    "CR": "7", "CV": "29", "ED": "9", "EL": "10", "ES": "35", "FA": "11",
    "FI": "12", "GV": "13", "HR": "15", "HS": "14", "I1": "37", "IN": "17",
    "LA": "18", "LG": "19", "NR": "20", "OC": "21", "PB": "23", "PE": "22",
    "PR": "25", "PW": "26", "SD": "33", "TN": "27", "TX": "28",
    "UT": "16", "WA": "30", "WL": "31",
}


def _fetch_chapters_quicksearch(code: str) -> list:
    """Fallback chapter fetch via QuickSearch/PopulateChapterList.

    Used for codes too large for GetStatuteArray to enumerate within the
    request timeout (SD ~1300 chapters, etc.). Returns the same shape as
    ``_fetch_chapters`` ({name, url}) so callers don't care which path
    produced the list.
    """
    code_id = TX_CODE_IDS.get(code)
    if not code_id:
        return []
    url = TCAS_API + f"QuickSearch/PopulateChapterList/{code_id}/CH"
    try:
        items = _get(url, as_json=True, timeout=120.0)
    except Exception as exc:  # noqa: BLE001
        print(f"[TX] QuickSearch fallback failed for {code}: {exc}", flush=True)
        return []
    if not isinstance(items, list):
        return []
    # Normalize {text,value,url} -> {name,url} that downstream expects.
    out = []
    for it in items:
        name = (it.get("text") or "").strip()
        # url is like "SD.1" - build full HTML URL on the resources host.
        rel = (it.get("url") or "").strip()
        full = f"{TCAS_RESOURCES}{code}/htm/{rel}.htm" if rel else ""
        if name and full:
            out.append({"name": name, "url": full})
    return out


def _fetch_chapters(code: str):
    """Return list of chapter dicts: [{name, ahid, hid, url}, ...].

    The GetStatuteArray endpoint takes 11 path params after the handler prefix:
      code / chapter / artSec / p1 / p2 / p3 / p4 / p5 / p6 / p7 / docType
    Passing code twice (code/code) with all others null returns the full chapter list.

    Falls back to QuickSearch/PopulateChapterList when GetStatuteArray returns
    an empty list (e.g. on timeout for huge codes like SD with 1300+ chapters).
    """
    url = (
        TCAS_API
        + "GetStatuteArray/GetStatuteArray/"
        + f"{code}/{code}/null/null/null/null/null/null/null/null/htm"
    )
    try:
        # 120s timeout - TX API is intermittently slow from non-residential
        # IPs and large codes (CR, ED, GV) take 20-90s to enumerate.
        chapters = _get(url, as_json=True, timeout=120.0)
    except Exception as exc:  # noqa: BLE001
        print(f"[TX] GetStatuteArray failed for {code}: {exc} — trying QuickSearch fallback", flush=True)
        return _fetch_chapters_quicksearch(code)
    if not isinstance(chapters, list) or not chapters:
        print(f"[TX] GetStatuteArray returned empty for {code} — trying QuickSearch fallback", flush=True)
        return _fetch_chapters_quicksearch(code)
    return chapters


def _chapter_html_url(code: str, chapter_num: str) -> str:
    """Build URL for the chapter's static HTML file.
    e.g. code=BC, chapter_num=1 -> .../resources/BC/htm/BC.1.htm
    """
    return f"{TCAS_RESOURCES}{code}/htm/{code}.{chapter_num}.htm"


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def _clean_text(raw: str) -> str:
    text = raw.replace("\xa0", " ").replace(" ", " ")
    text = re.sub(r"\s+", " ", text)
    text = _fix_encoding(text)
    return text.strip()


def _is_history_line(text: str) -> bool:
    """Detect amendment/history lines (no text-indent or known prefixes)."""
    history_prefixes = (
        "Acts ", "Added by", "Amended by", "Redesignated", "Transferred",
        "Expired ", "Renumbered", "Reenacted",
    )
    return text.startswith(history_prefixes)


def _section_status(name: str) -> Optional[str]:
    for kw in RESERVED_KEYWORDS:
        if kw in name:
            return "reserved"
    return None


# ---------------------------------------------------------------------------
# Main parse: extract sections from a chapter HTML page
# ---------------------------------------------------------------------------

def _parse_chapter_sections(
    soup: BeautifulSoup,
    chapter_node: Node,
    code: str,
    chapter_num: str,
    code_name: str,
    chapter_html_url: str,
):
    """Parse all <p class='left'> paragraphs; yield section Nodes."""
    paras = [p for p in soup.find_all("p") if "left" in (p.get("class") or [])]

    current_number: Optional[str] = None
    current_name: Optional[str] = None
    current_node_text: Optional[NodeText] = None
    current_history: str = ""
    current_anchor: str = ""

    def _flush_section():
        nonlocal current_number, current_name, current_node_text, current_history, current_anchor
        if current_number is None:
            return

        status = _section_status(current_name or "")
        node_id = f"{chapter_node.node_id}/section={current_number}"
        citation = f"Tex. {code_name} § {current_number}"
        link = chapter_html_url
        if current_anchor:
            link = f"{chapter_html_url}#{current_anchor}"

        addendum = None
        if current_history.strip():
            addendum = Addendum(
                history=AddendumType(type="history", text=current_history.strip())
            )

        node = Node(
            id=node_id,
            link=link,
            citation=citation,
            top_level_title=chapter_node.top_level_title,
            node_type="content",
            level_classifier="section",
            number=current_number,
            node_name=current_name,
            parent=chapter_node.node_id,
            status=status,
            node_text=current_node_text if not status else None,
            addendum=addendum,
        )
        insert_node(node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)

        current_number = None
        current_name = None
        current_node_text = None
        current_history = ""
        current_anchor = ""

    for p in paras:
        style = p.get("style", "")
        text = _clean_text(p.get_text())
        if not text:
            continue

        # Anchor (id attribute) on section heading paragraphs
        anchor_id = p.get("id", "")

        # New section starts with "Sec. X.XXX." or "Art. X." pattern
        sec_match = re.match(r"^(Sec\.|Art\.)\s+(\d[\d.A-Z-]*)\.", text)
        if sec_match:
            _flush_section()
            raw_num = sec_match.group(2).rstrip(".")
            current_number = raw_num
            current_anchor = anchor_id or raw_num
            # Extract section title (everything before the body text)
            # Pattern: "Sec. X.XXX.  TITLE.  Body..."
            title_match = re.match(
                r"^(?:Sec\.|Art\.)\s+[\d.\w-]+\.\s+((?:[A-Z][A-Z\s;,\-\(\)\'\"&]+\.)+)\s*(.*)",
                text,
            )
            if title_match:
                current_name = f"§ {raw_num}. {title_match.group(1).strip()}"
                body_text = _clean_text(title_match.group(2))
            else:
                current_name = f"§ {raw_num}."
                # Body is rest of the paragraph after "Sec. X.XXX."
                body_text = text
            current_node_text = NodeText()
            if body_text:
                current_node_text.add_paragraph(body_text)
            continue

        if current_number is None:
            # Before any section (chapter/title/subchapter headers) -- skip
            continue

        # History / addendum lines: no indent or starts with known history prefixes
        has_indent = "text-indent" in style
        if not has_indent or _is_history_line(text):
            current_history += text + "\n"
        else:
            # Indented body paragraph belonging to current section
            if current_node_text is None:
                current_node_text = NodeText()
            current_node_text.add_paragraph(text)

    # Flush last section
    _flush_section()


# ---------------------------------------------------------------------------
# Main traversal
# ---------------------------------------------------------------------------

def _scrape_chapter(chapter_entry: dict, code_node: Node, code: str, code_name: str):
    """Fetch and parse a single chapter, building structure + content nodes."""
    raw_name: str = chapter_entry.get("name", "").strip()
    chapter_url: str = chapter_entry.get("url", "").strip()

    # Extract chapter number. Some codes (notably CR - Code of Criminal
    # Procedure) return a placeholder "Chapter Title Not Found" in the
    # ``name`` field instead of a real "CHAPTER N." string, but the ``url``
    # field still carries the correct chapter ID
    # (e.g. ``CR/htm/CR.1.htm``). Prefer the URL when the name regex would
    # extract a non-numeric token like "Title".
    chapter_num: Optional[str] = None
    url_match = re.search(r"/" + re.escape(code) + r"\.([0-9A-Za-z._-]+?)\.htm", chapter_url)
    if url_match:
        chapter_num = url_match.group(1)
    if chapter_num is None or not chapter_num[0].isdigit():
        num_match = re.match(r"CHAPTER\s+([\w.]+)", raw_name, re.IGNORECASE)
        if num_match:
            cand = num_match.group(1).rstrip(".")
            # Only use name-extracted token if it starts with a digit; "Title"
            # is the well-known placeholder we want to skip.
            if cand and cand[0].isdigit():
                chapter_num = cand
    if not chapter_num:
        print(f"  [skip] unrecognised chapter name/url: name={raw_name!r} url={chapter_url!r}")
        return
    chapter_node_id = f"{code_node.node_id}/chapter={chapter_num}"
    chapter_link = _chapter_html_url(code, chapter_num)

    chapter_node = Node(
        id=chapter_node_id,
        link=chapter_link,
        top_level_title=code.lower(),
        node_type="structure",
        level_classifier="chapter",
        number=chapter_num,
        node_name=raw_name,
        parent=code_node.node_id,
    )
    insert_node(chapter_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)

    time.sleep(REQUEST_DELAY)
    try:
        soup = _get_soup(chapter_link)
    except Exception as exc:
        print(f"  [error] failed to fetch {chapter_link}: {exc}")
        return

    _parse_chapter_sections(
        soup,
        chapter_node,
        code=code,
        chapter_num=chapter_num,
        code_name=code_name,
        chapter_html_url=chapter_link,
    )


def scrape_code(corpus_node: Node, code: str, code_name: str):
    """Scrape all chapters of a single TX code."""
    code_node_id = f"{corpus_node.node_id}/code={code.lower()}"
    code_node = Node(
        id=code_node_id,
        link=f"{STATUTE_ORIGIN}/?link={code}",
        top_level_title=code.lower(),
        node_type="structure",
        level_classifier="code",
        number=code.lower(),
        node_name=code_name,
        parent=corpus_node.node_id,
    )
    insert_node(code_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)

    print(f"[TX] Fetching chapter list for code={code} ({code_name})")
    chapters = _fetch_chapters(code)
    print(f"[TX] Found {len(chapters)} chapters in {code}")

    for chapter_entry in chapters:
        print(f"  > {chapter_entry.get('name', '?')!r}")
        _scrape_chapter(chapter_entry, code_node, code, code_name)


def main():
    """Walk all 26 Texas codes in parallel.

    Each code is fully independent (separate code-node subtree, separate API
    chapter list, separate static HTML files), so we hand them to a
    ThreadPoolExecutor. Concurrency is controlled by ``VAQUILL_TITLE_WORKERS``
    (default 8). The HTTP layer (vaquill_pipeline.http_client) uses a shared
    keep-alive connection pool so parallel requests don't open new sockets.

    Resume: completed codes are persisted in ``state_tx_titles_done.txt`` and
    skipped on re-runs. Set ``VAQUILL_FORCE_RESCRAPE=1`` to override.
    """
    corpus_node: Node = insert_jurisdiction_and_corpus_node(COUNTRY, JURISDICTION, CORPUS)

    titles_done = set() if os.environ.get("VAQUILL_FORCE_RESCRAPE") else _load_titles_done()
    if titles_done:
        print(
            f"[scrapeTX] resume: {len(titles_done)} codes already done: "
            f"{sorted(titles_done)}",
            flush=True,
        )

    work = [(code, name) for code, name in TX_CODES if code not in titles_done]

    def _do_code(item: Tuple[str, str]):
        code, name = item
        try:
            scrape_code(corpus_node, code, name)
            _mark_title_done(code)
            return (code, "ok", None)
        except Exception as e:  # noqa: BLE001
            return (code, "fail", str(e)[:200])

    workers = int(os.environ.get("VAQUILL_TITLE_WORKERS", "8"))
    print(
        f"[scrapeTX] running {len(work)} codes with {workers} parallel workers",
        flush=True,
    )
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for fut in as_completed(ex.submit(_do_code, item) for item in work):
            code, status, err = fut.result()
            if status == "fail":
                print(f"[scrapeTX] code {code}: {status}: {err}", flush=True)
            else:
                print(f"[scrapeTX] code {code}: {status}", flush=True)


if __name__ == "__main__":
    main()
