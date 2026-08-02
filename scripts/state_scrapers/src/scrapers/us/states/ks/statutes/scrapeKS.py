"""Kansas Statutes Annotated (KSA) scraper.

Source:  https://www.kslegislature.gov/b2025_26/laws/
Hierarchy: chapter -> article -> section
Node IDs:  us/ks/statutes/chapter=N/article=M/section=S
Citation:  K.S.A. § <SECTION>   (e.g. "K.S.A. § 1-201")

Notes:
- The site redirects from kslegislature.org to kslegislature.gov and
  serves Brotli-compressed responses. The pipeline http_client sends
  ``Accept-Encoding: gzip, deflate, br`` but the underlying ``requests``
  library does not decode Brotli without an optional extra dep.
  We therefore fetch with a local helper that omits ``br`` from the
  accept-encoding header so the server falls back to gzip, which
  ``requests`` handles natively.
- R2 upload is triggered opportunistically via the pipeline's
  ``r2_sync.upload_source`` helper (best-effort, no hard dependency).
"""
from __future__ import annotations

import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Project-root bootstrap (mirrors ME/ND pattern)
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

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
COUNTRY = "us"
JURISDICTION = "ks"
CORPUS = "statutes"
TABLE_NAME = f"{COUNTRY}_{JURISDICTION}_{CORPUS}"

SESSION = "b2025_26"
BASE_URL = "https://www.kslegislature.gov"
TOC_URL = f"{BASE_URL}/{SESSION}/laws/"

RESERVED_KEYWORDS = ["repealed", "reserved", "expired", "renumbered"]

# Brotli-safe browser headers for direct requests.
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate",
}

# Worker count for the parallel chapter pool. Read once at import so the
# session's connection pool can be sized to match.
_WORKERS = int(os.environ.get("VAQUILL_TITLE_WORKERS", "8"))

# The session is shared across the worker threads. That is safe here because
# the headers are set once below and never mutated per request, and a
# requests.Session holds no other per-request mutable state -- concurrent
# GETs each get their own connection from the pool. The default adapter keeps
# only 10 connections per host, so size the pool to the worker count instead;
# otherwise a larger VAQUILL_TITLE_WORKERS makes urllib3 discard and re-open
# a socket on nearly every request.
_SESSION = requests.Session()
_SESSION.headers.update(_HEADERS)
_ADAPTER = requests.adapters.HTTPAdapter(
    pool_connections=_WORKERS,
    pool_maxsize=_WORKERS,
)
_SESSION.mount("https://", _ADAPTER)
_SESSION.mount("http://", _ADAPTER)


# ---------------------------------------------------------------------------
# HTTP helper (no Brotli, with best-effort R2 upload)
# ---------------------------------------------------------------------------

def _get_soup(url: str, retries: int = 3, delay: float = 1.5) -> BeautifulSoup:
    """Fetch a URL and return a BeautifulSoup object.

    Uses ``requests`` directly (not the pipeline http_client) to avoid the
    Brotli decompression issue on kslegislature.gov.  Triggers a best-effort
    R2 source upload via the pipeline helper when available.
    """
    last_exc: Optional[BaseException] = None
    for attempt in range(1, retries + 1):
        try:
            resp = _SESSION.get(url, timeout=30, allow_redirects=True)
            resp.raise_for_status()
            # Best-effort R2 upload (no-op if pipeline is not configured).
            try:
                from vaquill_pipeline import r2_sync
                r2_sync.upload_source(
                    state=r2_sync._state_for_run,
                    url=url,
                    body=resp.content,
                    content_type=resp.headers.get("Content-Type"),
                )
            except Exception:
                pass
            return BeautifulSoup(resp.text, "html.parser")
        except Exception as exc:
            last_exc = exc
            print(f"[WARN] attempt {attempt} failed for {url}: {exc}", flush=True)
            time.sleep(delay * attempt)
    raise RuntimeError(f"Could not fetch {url} after {retries} attempts") from last_exc


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    corpus_node: Node = insert_jurisdiction_and_corpus_node(COUNTRY, JURISDICTION, CORPUS)
    _scrape_all_chapters(corpus_node)


# ---------------------------------------------------------------------------
# Chapter level
# ---------------------------------------------------------------------------

def _scrape_all_chapters(corpus_node: Node) -> None:
    """Fetch the KSA top-level TOC and dispatch each chapter in parallel.

    Each chapter is an independent subtree (chapter page -> articles ->
    sections) with no shared state, and the JSONL sink + counters in
    vaquill_pipeline.patch are lock-protected, so chapters fan out to a
    ThreadPoolExecutor sized by VAQUILL_TITLE_WORKERS (default 8, matching
    scrapeWA / scrapeAK). Chapter is the right level here: the ~90 chapters
    are numerous enough to keep every worker busy, and they are discovered up
    front from the single TOC page rather than by walking a linked list.

    Why: kslegislature.gov is one page fetch per chapter, per article and per
    section, so the crawl is latency-bound rather than server-bound and
    overlapping the chapters is what collapses the wall-clock.

    NOTE: KS deliberately has no titles_done resume file -- every run
    re-crawls in full. That is what lets an amended section be re-fetched and
    re-chunked into a fresh content-addressed point_id (the JSONL skipset
    suppresses the write for unchanged sections, so a re-crawl is cheap in
    output but still catches amendments). Do not add a titles_done skip here
    to save time without replacing that freshness some other way -- it would
    make KS amendment-blind.
    """
    soup = _get_soup(TOC_URL)
    tbl = soup.find(id="statute")
    if tbl is None:
        raise RuntimeError(f"Could not find #statute table on {TOC_URL}")

    work: list[Node] = []

    for tr in tbl.find_all("tr"):
        a = tr.find("a", href=True)
        if a is None:
            continue
        href: str = a["href"].strip()
        # Only chapter rows: href like "001_000_0000_chapter/"
        m = re.match(r"^(\d+)_\d+_\d+_chapter/?$", href)
        if m is None:
            continue

        chapter_num = str(int(m.group(1)))  # "001" -> "1"
        node_name_raw = tr.get_text(separator=" ", strip=True)
        node_name = _clean_text(node_name_raw)

        chapter_url = TOC_URL + href
        node_id = f"{corpus_node.node_id}/chapter={chapter_num}"
        status = _check_reserved(node_name)

        chapter_node = Node(
            id=node_id,
            link=chapter_url,
            top_level_title=chapter_num,
            node_type="structure",
            level_classifier="chapter",
            number=chapter_num,
            node_name=node_name,
            parent=corpus_node.node_id,
            status=status,
        )
        insert_node(chapter_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)

        if not status:
            work.append(chapter_node)

    def _do_chapter(chapter_node: Node) -> tuple[str, str, str | None]:
        # One chapter's failure must not abort the other workers, so each is
        # wrapped and reported; the run continues with the remaining chapters.
        try:
            _scrape_chapter(chapter_node)
            return (chapter_node.number, "ok", None)
        except Exception as exc:
            return (chapter_node.number, "fail", str(exc)[:200])

    print(
        f"[scrapeKS] running {len(work)} chapters with {_WORKERS} parallel workers",
        flush=True,
    )
    with ThreadPoolExecutor(max_workers=_WORKERS) as ex:
        for fut in as_completed(ex.submit(_do_chapter, item) for item in work):
            num, status, err = fut.result()
            if status == "fail":
                print(f"[scrapeKS] chapter {num}: {status}: {err}", flush=True)
            else:
                print(f"[scrapeKS] chapter {num}: {status}", flush=True)


# ---------------------------------------------------------------------------
# Article level
# ---------------------------------------------------------------------------

def _scrape_chapter(chapter_node: Node) -> None:
    soup = _get_soup(str(chapter_node.link))
    tbl = soup.find(id="statute")
    if tbl is None:
        print(f"[WARN] No #statute table on {chapter_node.link}", flush=True)
        return

    for tr in tbl.find_all("tr"):
        a = tr.find("a", href=True)
        if a is None:
            continue
        href: str = a["href"].strip()
        # Article rows: href like "001_002_0000_article/"
        m = re.match(r"^(\d+)_(\d+)_\d+_article/?$", href)
        if m is None:
            continue

        article_num = str(int(m.group(2)))  # "002" -> "2"
        node_name_raw = tr.get_text(separator=" ", strip=True)
        node_name = _clean_text(node_name_raw)

        article_url = str(chapter_node.link).rstrip("/") + "/" + href
        node_id = f"{chapter_node.node_id}/article={article_num}"
        status = _check_reserved(node_name)

        article_node = Node(
            id=node_id,
            link=article_url,
            top_level_title=chapter_node.top_level_title,
            node_type="structure",
            level_classifier="article",
            number=article_num,
            node_name=node_name,
            parent=chapter_node.node_id,
            status=status,
        )
        insert_node(article_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)

        if not status:
            _scrape_article(article_node)


# ---------------------------------------------------------------------------
# Section level
# ---------------------------------------------------------------------------

def _scrape_article(article_node: Node) -> None:
    soup = _get_soup(str(article_node.link))
    tbl = soup.find(id="statute")
    if tbl is None:
        print(f"[WARN] No #statute table on {article_node.link}", flush=True)
        return

    for tr in tbl.find_all("tr"):
        a = tr.find("a", href=True)
        if a is None:
            continue
        href: str = a["href"].strip()
        # Section hrefs are relative from the article's parent context:
        # ../../001_000_0000_chapter/001_002_0000_article/001_002_0001_section/001_002_0001_k/
        m = re.search(
            r"(\d+)_(\d+)_(\d+)_section/(\d+)_(\d+)_(\d+)_k/?$", href
        )
        if m is None:
            continue

        # The stat_5f_number span in the listing text gives the KSA number
        # (e.g. "1-201 - ..."), which is the authoritative section number.
        row_text = _clean_text(tr.get_text(separator=" ", strip=True))
        ksa_num = _extract_ksa_number(row_text)

        # Build the absolute section URL from the chapter/article base.
        # href starts with "../../" relative to the article page location, so
        # we resolve against TOC_URL.
        clean_href = re.sub(r"^\.\./\.\./", "", href)
        section_url = TOC_URL + clean_href

        node_name_raw = row_text
        node_name = node_name_raw if node_name_raw else f"Section {ksa_num}"
        status = _check_reserved(node_name)
        citation = f"K.S.A. § {ksa_num}"

        node_id = f"{article_node.node_id}/section={ksa_num}"

        if status:
            section_node = Node(
                id=node_id,
                link=section_url,
                top_level_title=article_node.top_level_title,
                node_type="content",
                level_classifier="section",
                number=ksa_num,
                node_name=node_name,
                parent=article_node.node_id,
                citation=citation,
                status=status,
            )
            insert_node(section_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)
            continue

        node_text, addendum = _fetch_section_content(section_url)

        section_node = Node(
            id=node_id,
            link=section_url,
            top_level_title=article_node.top_level_title,
            node_type="content",
            level_classifier="section",
            number=ksa_num,
            node_name=node_name,
            parent=article_node.node_id,
            citation=citation,
            node_text=node_text,
            addendum=addendum,
        )
        insert_node(section_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)


# ---------------------------------------------------------------------------
# Section content fetch
# ---------------------------------------------------------------------------

def _fetch_section_content(
    url: str,
) -> tuple[Optional[NodeText], Optional[Addendum]]:
    """Fetch a section page and return (NodeText | None, Addendum | None)."""
    try:
        soup = _get_soup(url)
    except Exception as exc:
        print(f"[WARN] Could not fetch section {url}: {exc}", flush=True)
        return None, None

    stat_body = soup.find(class_="statute-body")
    if stat_body is None:
        return None, None

    tables = stat_body.find_all("table")
    if len(tables) < 2:
        return None, None

    # Table[1]: main section content (p.p_pt tags).
    content_table = tables[1]
    td = content_table.find("td")
    if td is None:
        return None, None

    node_text = NodeText()
    for p in td.find_all("p"):
        text = _clean_text(p.get_text(separator=" "))
        if text:
            node_text.add_paragraph(text=text)

    if not node_text.paragraphs:
        # Fallback: grab all direct text from td
        raw = _clean_text(td.get_text(separator=" "))
        if raw:
            node_text.add_paragraph(text=raw)

    # Table[2]: history / addendum (when present).
    addendum: Optional[Addendum] = None
    if len(tables) >= 3:
        hist_raw = _clean_text(tables[2].get_text(separator=" "))
        if hist_raw:
            addendum = Addendum()
            addendum.history = AddendumType(type="history", text=hist_raw)

    return node_text, addendum


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _extract_ksa_number(row_text: str) -> str:
    """
    Extract the KSA section number from an article-listing row text.

    Row text examples:
        "1-201 - Membership; appointment..."
        "21-5413 - Robbery; severity level..."
    The KSA number is the first token before " - ".
    Fallback: strip trailing period from numeric token.
    """
    # Pattern: digits-digits(-digits)? at start, e.g. 1-201, 21-5413, 12-1667a
    m = re.match(r"^([\da-z]+-[\da-z]+(?:-[\da-z]+)*)\s*[-–]", row_text, re.IGNORECASE)
    if m:
        return m.group(1).rstrip(".")
    # Fallback: first whitespace-delimited token
    token = row_text.split()[0] if row_text else "unknown"
    return token.rstrip(".-")


def _check_reserved(text: str) -> Optional[str]:
    """Return 'reserved' if text contains a reserved/repealed keyword."""
    lower = text.lower()
    for kw in RESERVED_KEYWORDS:
        if kw in lower:
            return "reserved"
    return None


def _clean_text(raw: str) -> str:
    """Normalize whitespace and non-breaking spaces."""
    text = raw.replace("\xa0", " ").replace(" ", " ").replace("​", "")
    text = re.sub(r"\s+", " ", text).strip()
    return text


# ---------------------------------------------------------------------------
# CLI entry
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    main()
