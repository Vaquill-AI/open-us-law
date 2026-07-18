"""Alaska Statutes scraper.

Source: https://www.akleg.gov/basis/statutes.asp (Alaska Legislative Affairs Agency).
The official LAA infobase. The page title is "Alaska Statutes 2024" — current
through the 33rd Legislature (2024 regular session). The previously-used Onecle
mirror (law.onecle.com/alaska/) is frozen at 2016-11-15 and missing 8+ years of
amendments, so we switched.

Hierarchy: title -> chapter -> section
Citation: Alaska Stat. § <SECTION>  (e.g. Alaska Stat. § 01.05.006)

The site is JS-driven but exposes plain HTML fragments via three endpoints:
  TOC (titles):          ?media=js&type=TOC&title={N}            -> chapter list
  TOC (chapters):        ?media=js&type=TOC&title={NN.NN}        -> section list
  Section body:          ?media=print&secStart=X&secEnd=X        -> <div class="statute">
  Section history/refs:  ?type=xRef&sec=X                        -> history block

Resume + parallelism mirror the DE scraper: env VAQUILL_TITLE_WORKERS (default 8),
title-level resume via state_ak_titles_done.txt, and HTTP-pool reuse through
vaquill_pipeline.http_client (when available).
"""
from __future__ import annotations

import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

from bs4 import BeautifulSoup, NavigableString

# ---------------------------------------------------------------------------
# Project-root bootstrap (mirrors the DE/ME/ND pattern)
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
JURISDICTION = "ak"
CORPUS = "statutes"
TABLE_NAME = f"{COUNTRY}_{JURISDICTION}_{CORPUS}"

BASE = "https://www.akleg.gov/basis/statutes.asp"

# Titles 1..47 in Alaska Statutes. (Title 20 was repealed in 1986, kept as a
# reserved structural placeholder so citation lookups don't 404 on it.)
ALL_TITLE_NUMBERS: list[int] = list(range(1, 48))

RESERVED_KEYWORDS = ["repealed", "reserved", "expired", "renumbered"]


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    corpus_node: Node = insert_jurisdiction_and_corpus_node(COUNTRY, JURISDICTION, CORPUS)
    _scrape_all_titles(corpus_node)


# ---------------------------------------------------------------------------
# Title-level resume bookkeeping (mirrors DE)
# ---------------------------------------------------------------------------

def _titles_done_path() -> Path:
    try:
        from vaquill_pipeline.config import SETTINGS  # type: ignore
        return SETTINGS.chunks_dir / "state_ak_titles_done.txt"
    except Exception:
        return Path(__file__).parent / "state_ak_titles_done.txt"


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
# Title discovery + dispatch
# ---------------------------------------------------------------------------

def _scrape_all_titles(corpus_node: Node) -> None:
    """Fetch every title's TOC fragment and dispatch each title in parallel.

    Each title is independent (no shared state at the title level), so we hand
    them to a ThreadPoolExecutor sized by VAQUILL_TITLE_WORKERS (default 8).
    The HTTP layer uses a connection pool so concurrent requests share
    keep-alive sockets.
    """
    titles_done = (
        set() if os.environ.get("VAQUILL_FORCE_RESCRAPE") else _load_titles_done()
    )
    if titles_done:
        print(
            f"[scrapeAK] resume: {len(titles_done)} titles already done: "
            f"{sorted(titles_done, key=lambda x: int(x) if x.isdigit() else 999)}",
            flush=True,
        )

    work: list[tuple[Node, str]] = []
    for title_int in ALL_TITLE_NUMBERS:
        title_num = str(title_int)
        title_padded = f"{title_int:02d}"

        # Pull the title's chapter TOC fragment.
        toc_url = f"{BASE}?media=js&type=TOC&title={title_padded}"
        try:
            soup = get_url_as_soup(toc_url)
        except Exception as exc:
            print(f"[scrapeAK] title {title_num} TOC fetch failed: {exc}", flush=True)
            continue

        # First <a> in the fragment is the title header itself.
        first_a = soup.find("a")
        title_name = _clean_text(first_a.get_text()) if first_a else f"Title {title_num}"

        title_node = Node(
            id=f"{corpus_node.node_id}/title={title_num}",
            link=f"{BASE}?title={title_padded}",
            top_level_title=title_num,
            node_type="structure",
            level_classifier="title",
            number=title_num,
            node_name=title_name,
            parent=corpus_node.node_id,
            status=_check_reserved(title_name),
        )
        insert_node(title_node, TABLE_NAME, ignore_duplicate=True, debug_mode=False)

        if title_node.status:
            continue
        if title_num in titles_done:
            continue

        work.append((title_node, title_padded))

    def _do_title(item: tuple[Node, str]) -> tuple[str, str, Optional[str]]:
        title_node, title_padded = item
        try:
            _scrape_title(title_node, title_padded)
            _mark_title_done(title_node.number)
            return (title_node.number, "ok", None)
        except Exception as exc:
            return (title_node.number, "fail", str(exc)[:200])

    workers = int(os.environ.get("VAQUILL_TITLE_WORKERS", "8"))
    print(
        f"[scrapeAK] running {len(work)} titles with {workers} parallel workers",
        flush=True,
    )
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for fut in as_completed(ex.submit(_do_title, item) for item in work):
            num, status, err = fut.result()
            if status == "fail":
                print(f"[scrapeAK] title {num}: {status}: {err}", flush=True)
            else:
                print(f"[scrapeAK] title {num}: {status}", flush=True)


# ---------------------------------------------------------------------------
# Chapter-level (each title TOC enumerates chapters)
# ---------------------------------------------------------------------------

def _scrape_title(title_node: Node, title_padded: str) -> None:
    """For a given title, pull each chapter TOC and scrape its sections."""
    toc_url = f"{BASE}?media=js&type=TOC&title={title_padded}"
    soup = get_url_as_soup(toc_url)

    # Each <li> in the title TOC carries an `<a onclick='loadTOC("NN.NN");' ...>`
    # whose visible text is "Chapter NN. <Name>.".
    for li in soup.find_all("li"):
        link = li.find("a", onclick=True)
        if link is None:
            continue
        m = re.search(r'loadTOC\("(\d{2}\.\d{2})"\)', link.get("onclick", ""))
        if not m:
            continue
        chapter_dotted = m.group(1)  # e.g. "01.05"

        node_name = _clean_text(link.get_text())
        chapter_number = chapter_dotted.split(".", 1)[1].lstrip("0") or "0"
        chapter_node = Node(
            id=f"{title_node.node_id}/chapter={chapter_dotted}",
            link=f"{BASE}?title={title_padded}#{chapter_dotted}",
            top_level_title=title_node.top_level_title,
            node_type="structure",
            level_classifier="chapter",
            number=chapter_dotted,
            node_name=node_name,
            parent=title_node.node_id,
            status=_check_reserved(node_name),
        )
        insert_node(chapter_node, TABLE_NAME, ignore_duplicate=True, debug_mode=False)

        if chapter_node.status:
            continue
        _scrape_chapter(chapter_node, chapter_dotted)


# ---------------------------------------------------------------------------
# Section-level
# ---------------------------------------------------------------------------

def _scrape_chapter(chapter_node: Node, chapter_dotted: str) -> None:
    """For a chapter, fetch its section list and walk each section."""
    toc_url = f"{BASE}?media=js&type=TOC&title={chapter_dotted}"
    soup = get_url_as_soup(toc_url)

    for li in soup.find_all("li"):
        link = li.find("a", href=True)
        if link is None:
            continue
        href: str = link["href"]
        # Section anchors look like: "statutes.asp?year=2024&title=1#01.05.006"
        m = re.search(r"#(\d{2}\.\d{2}\.\d{3}[A-Za-z]?)$", href)
        if not m:
            continue
        sec_number = m.group(1)

        # Section name is the link text, taken DIRECTLY (no regex slicing
        # against the cleaned text — that loses the name).
        raw_link_text = _clean_text(link.get_text())
        node_name = _extract_section_name(raw_link_text)

        node_id = f"{chapter_node.node_id}/section={sec_number}"
        citation = f"Alaska Stat. § {sec_number}"
        sec_url = f"{BASE}?title={chapter_dotted.split('.')[0]}#{sec_number}"
        status = _check_reserved(raw_link_text)

        if status:
            section_node = Node(
                id=node_id,
                link=sec_url,
                top_level_title=chapter_node.top_level_title,
                node_type="content",
                level_classifier="section",
                number=sec_number,
                node_name=node_name,
                parent=chapter_node.node_id,
                citation=citation,
                status=status,
            )
            insert_node(section_node, TABLE_NAME, ignore_duplicate=True, debug_mode=False)
            continue

        node_text, addendum, body_status = _fetch_section_content(sec_number)

        section_node = Node(
            id=node_id,
            link=sec_url,
            top_level_title=chapter_node.top_level_title,
            node_type="content",
            level_classifier="section",
            number=sec_number,
            node_name=node_name,
            parent=chapter_node.node_id,
            citation=citation,
            node_text=node_text,
            addendum=addendum,
            # If the BODY text says "[Repealed by ...]" but the link text
            # didn't, surface that as reserved on the node (status from body).
            status=body_status,
        )
        insert_node(section_node, TABLE_NAME, ignore_duplicate=True, debug_mode=False)


def _fetch_section_content(
    sec_number: str,
) -> tuple[Optional[NodeText], Optional[Addendum], Optional[str]]:
    """Fetch body + history for a single section.

    Returns (node_text, addendum, status). `status` is set to "reserved" if the
    body text indicates the section is repealed/reserved (so callers can mark
    the node even when the link text didn't reveal it).
    """
    body_url = f"{BASE}?media=print&secStart={sec_number}&secEnd={sec_number}"
    try:
        body_soup = get_url_as_soup(body_url)
    except Exception as exc:
        print(f"[scrapeAK] body fetch failed for {sec_number}: {exc}", flush=True)
        return None, None, None

    # Strip the SectionHead <b>...<BR></b> prefix; body is the remainder.
    statute_div = body_soup.find("div", class_="statute")
    if statute_div is None:
        return None, None, None

    # Detect [Repealed ...] in the visible body text -> status="reserved".
    body_text_flat = _clean_text(statute_div.get_text(separator=" "))
    body_status: Optional[str] = None
    if re.search(r"\[\s*Repealed\b", body_text_flat, flags=re.IGNORECASE):
        body_status = "reserved"

    node_text = NodeText()
    # Drop the leading SectionHead (the <b> with the name) so it doesn't bleed
    # into the paragraph stream.
    head_b = statute_div.find("b")
    if head_b is not None:
        head_b.decompose()

    # The body uses <BR><BR> as a paragraph separator inside the single div.
    # Convert <br>s to newlines and split on blank lines.
    for br in statute_div.find_all("br"):
        br.replace_with("\n")
    raw_body = statute_div.get_text(separator=" ")
    for chunk in re.split(r"\n\s*\n+", raw_body):
        text = _clean_text(chunk)
        if not text:
            continue
        node_text.add_paragraph(text=text)

    # Pull history + cross-references from the xRef endpoint.
    history_lines: list[str] = []
    xref_url = f"{BASE}?type=xRef&sec={sec_number}"
    try:
        xref_soup = get_url_as_soup(xref_url)
        # The "History" block is rendered as `<h5>History</h5>` followed by
        # plain text/anchors. We grab everything after the <h5>History</h5>
        # marker up to the next <h5> or the disclaimer div.
        history_h5 = None
        for h5 in xref_soup.find_all("h5"):
            if "history" in h5.get_text(strip=True).lower():
                history_h5 = h5
                break
        if history_h5 is not None:
            for sibling in history_h5.next_siblings:
                if getattr(sibling, "name", None) == "h5":
                    break
                if getattr(sibling, "name", None) == "div":
                    # The yellow disclaimer div ends the history region.
                    break
                if isinstance(sibling, NavigableString):
                    text = _clean_text(str(sibling))
                else:
                    text = _clean_text(sibling.get_text(separator=" "))
                if not text:
                    continue
                # The xRef endpoint always appends a literal "here" sentinel
                # right after the history paragraph — drop it.
                if text.lower() == "here":
                    continue
                history_lines.append(text)
    except Exception as exc:
        # Non-fatal — body text is the load-bearing field.
        print(f"[scrapeAK] xRef fetch failed for {sec_number}: {exc}", flush=True)

    addendum: Optional[Addendum] = None
    if history_lines:
        addendum = Addendum()
        addendum.history = AddendumType(
            type="history", text=" ".join(history_lines).strip()
        )

    if not node_text.paragraphs and addendum is None:
        return None, None, body_status

    return (node_text if node_text.paragraphs else None), addendum, body_status


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _extract_section_name(link_text: str) -> str:
    """Pull a clean section name from the section's link text.

    Link text format: "Sec. 01.05.006.   Adoption of Alaska Statutes; ..."
    We strip the leading "Sec. <N>." prefix and return what remains, which IS
    the section name. The previous regex-based approach tried to strip after a
    double-space — but `_clean_text` already collapses runs of whitespace, so
    that double-space anchor never matched.
    """
    text = _clean_text(link_text)
    m = re.match(r"^Sec\.\s+[\d\.]+[A-Za-z]?\.\s*(.+)$", text)
    if m:
        return m.group(1).strip()
    return text


def _check_reserved(text: str) -> Optional[str]:
    lower = text.lower()
    for kw in RESERVED_KEYWORDS:
        if kw in lower:
            return "reserved"
    return None


def _clean_text(raw: str) -> str:
    """Normalize whitespace, NBSPs, and the LAA-style Unicode quote mojibake."""
    if not raw:
        return ""
    text = raw.replace("\xa0", " ").replace(" ", " ")
    # The akleg pages serve ISO-8859-1; if anything slipped through as
    # mojibake bytes (e.g. "â") leave it alone — get_url_as_soup decodes
    # via requests' apparent encoding. Just collapse whitespace.
    text = re.sub(r"\s+", " ", text).strip()
    return text


# ---------------------------------------------------------------------------
# CLI entry
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    main()
