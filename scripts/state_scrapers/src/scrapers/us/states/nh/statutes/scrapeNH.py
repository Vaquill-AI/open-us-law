"""New Hampshire RSA (Revised Statutes Annotated) scraper.

Source: https://www.gencourt.state.nh.us/rsa/html/

NOTE: The gencourt.state.nh.us site is behind FortiWeb Cloud WAF (Azure) and
geo-restricts non-US IPs. Run from a US host, or supply WEBSHARE_USERNAME /
WEBSHARE_PASSWORD (US rotating residential proxy). The vaquill_pipeline
http_client already routes this domain through the proxy when the env vars are
present.

Real URL patterns (verified 2026-05-11 via proxy):
  Master TOC:    .../rsa/html/NHTOC.htm
                 Links: NHTOC/NHTOC-{ROMAN}.htm  (one file per title, no chapter suffix)
  Title TOC:     .../rsa/html/NHTOC/NHTOC-{ROMAN}.htm
                 Links: NHTOC-{ROMAN}-{CHAPTER}.htm  (relative, same dir)
  Chapter TOC:   .../rsa/html/NHTOC/NHTOC-{ROMAN}-{CHAPTER}.htm
                 Links: ../{ROMAN}/{CHAPTER}/{CHAPTER}-{SECTION}.htm  (relative)
  Section page:  .../rsa/html/{ROMAN}/{CHAPTER}/{CHAPTER}-{SECTION}.htm
                 e.g.  .../rsa/html/I/1/1-1.htm  -> RSA 1:1

Citation format: "N.H. Rev. Stat. § {chapter}:{section}" (e.g. "N.H. Rev. Stat. § 1:1")
"""
from __future__ import annotations

import html as _html_mod
import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup

current_file = Path(__file__).resolve()
src_directory = current_file.parent
while src_directory.name != "src" and src_directory.parent != src_directory:
    src_directory = src_directory.parent
project_root = src_directory.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.utils.pydanticModels import Addendum, AddendumType, Node, NodeText
from src.utils.scrapingHelpers import (
    get_url_as_soup,
    insert_jurisdiction_and_corpus_node,
    insert_node,
)

COUNTRY = "us"
JURISDICTION = "nh"
CORPUS = "statutes"
TABLE_NAME = f"{COUNTRY}_{JURISDICTION}_{CORPUS}"

BASE_URL = "https://www.gencourt.state.nh.us/rsa/html"
TOC_URL = f"{BASE_URL}/NHTOC.htm"
NHTOC_BASE = f"{BASE_URL}/NHTOC"

RESERVED_KEYWORDS = ["[Repealed]", "[Expired]", "[Reserved]", "Repealed", "Reserved", "Expired"]


_MOJIBAKE_MARKERS = (
    "\xc2",      # Â
    "\xe2\x80",  # â\x80 (curly quotes / en-em dashes mis-decoded)
    "\xe2\x82",
    "\xe2\x84",
    "\xe2\x86",
    "â€",        # high-frequency double-encoded marker
)


def _fix_encoding(s: str) -> str:
    """Undo Latin-1/UTF-8 mojibake. Marker-gated: only attempts the round-trip
    when at least one classic mojibake prefix is present, and only keeps the
    fix if it strictly reduces marker count.
    """
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


def main() -> None:
    corpus_node: Node = insert_jurisdiction_and_corpus_node(COUNTRY, JURISDICTION, CORPUS)
    scrape_all_titles(corpus_node)


def _titles_done_path():
    """Where we persist the set of titles already fully scraped for resume."""
    from vaquill_pipeline.config import SETTINGS
    return SETTINGS.chunks_dir / "state_nh_titles_done.txt"


def _load_titles_done() -> set:
    path = _titles_done_path()
    if not path.exists():
        return set()
    return {l.strip() for l in path.read_text().splitlines() if l.strip()}


def _mark_title_done(roman: str) -> None:
    path = _titles_done_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a") as fh:
        fh.write(f"{roman}\n")
        fh.flush()


def scrape_all_titles(corpus_node: Node) -> None:
    """Fetch the master TOC and walk every title in parallel.

    Each title is independent. ThreadPoolExecutor with VAQUILL_TITLE_WORKERS
    (default 8). Title-level resume via state_nh_titles_done.txt. Set
    VAQUILL_FORCE_RESCRAPE=1 to override.
    """
    titles_done = set() if os.environ.get("VAQUILL_FORCE_RESCRAPE") else _load_titles_done()
    if titles_done:
        print(f"[scrapeNH] resume: {len(titles_done)} titles already done: {sorted(titles_done)}", flush=True)

    soup = get_url_as_soup(TOC_URL)

    work = []  # list of (title_node,) tuples to scrape
    seen_roman: set = set()

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        # Match: NHTOC/NHTOC-{ROMAN}.htm  (title page, no chapter number suffix)
        m = re.match(r"NHTOC/NHTOC-([A-Z][A-Z\-]*)\.htm$", href, re.IGNORECASE)
        if m is None:
            continue

        roman = m.group(1).upper()
        if roman in seen_roman:
            continue
        seen_roman.add(roman)

        node_name = _clean(a.get_text())
        if not node_name:
            continue

        title_url = f"{NHTOC_BASE}/{href.split('/')[-1]}"
        node_id = f"{corpus_node.node_id}/title={roman}"
        status = _check_reserved(node_name)

        title_node = Node(
            id=node_id,
            link=title_url,
            top_level_title=roman,
            node_type="structure",
            level_classifier="title",
            number=roman,
            node_name=node_name,
            parent=corpus_node.node_id,
            status=status,
        )
        insert_node(title_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)

        if status:
            continue
        if roman in titles_done:
            continue
        work.append(title_node)

    def _do_title(tnode: Node):
        try:
            scrape_title(tnode)
            _mark_title_done(str(tnode.number))
            return (tnode.number, "ok", None)
        except Exception as e:
            return (tnode.number, "fail", str(e)[:200])

    workers = int(os.environ.get("VAQUILL_TITLE_WORKERS", "8"))
    print(f"[scrapeNH] running {len(work)} titles with {workers} parallel workers", flush=True)
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for fut in as_completed(ex.submit(_do_title, t) for t in work):
            num, status, err = fut.result()
            if status == "fail":
                print(f"[scrapeNH] title {num}: {status}: {err}", flush=True)
            else:
                print(f"[scrapeNH] title {num}: {status}", flush=True)


def scrape_title(title_node: Node) -> None:
    """Fetch a title TOC page and walk its chapter links.

    Chapter links are relative to the NHTOC/ directory, e.g.:
      NHTOC-I-1.htm, NHTOC-I-1-A.htm, NHTOC-XIX-A-216-A.htm
    """
    soup = get_url_as_soup(str(title_node.link))
    title_base = str(title_node.link).rsplit("/", 1)[0] + "/"

    seen_chapters: set = set()
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        # Chapter links: NHTOC-{ROMAN}-{CHAPTER}.htm; same dir (no slash).
        if "/" in href:
            continue
        if not re.match(r"NHTOC-[A-Z][A-Z\-]*-[\dA-Z][\w\-]*\.htm$", href, re.IGNORECASE):
            continue
        if href in seen_chapters:
            continue
        seen_chapters.add(href)

        chapter_url = title_base + href
        _scrape_chapter_page(title_node, chapter_url)


def _scrape_chapter_page(title_node: Node, chapter_url: str) -> None:
    """Fetch a chapter TOC page and walk its section links."""
    soup = get_url_as_soup(chapter_url)

    chapter_name = ""
    for center in soup.find_all("center"):
        h2 = center.find("h2")
        if h2 is not None:
            candidate = _clean(h2.get_text())
            if re.match(r"CHAPTER\s+", candidate, re.IGNORECASE):
                chapter_name = candidate
                break

    if not chapter_name:
        for tag in soup.find_all(["h2", "h3", "b", "center"]):
            candidate = _clean(tag.get_text())
            if re.match(r"CHAPTER\s+", candidate, re.IGNORECASE):
                chapter_name = candidate
                break

    chapter_num = _extract_chapter_number(chapter_name)
    if chapter_num is None:
        m = re.search(r"NHTOC-[A-Z][A-Z\-]*-(\d+[\w\-]*)\.htm", chapter_url, re.IGNORECASE)
        chapter_num = m.group(1) if m else "unknown"

    node_id = f"{title_node.node_id}/chapter={chapter_num}"
    status = _check_reserved(chapter_name)

    chapter_node = Node(
        id=node_id,
        link=chapter_url,
        top_level_title=title_node.top_level_title,
        node_type="structure",
        level_classifier="chapter",
        number=chapter_num,
        node_name=chapter_name or f"Chapter {chapter_num}",
        parent=title_node.node_id,
        status=status,
    )
    insert_node(chapter_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)

    if status:
        return

    chapter_base = chapter_url.rsplit("/", 1)[0] + "/"

    seen_sections: set = set()
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href.startswith("../"):
            continue
        if not href.endswith(".htm"):
            continue
        if re.search(r"-mrg\.htm$", href, re.IGNORECASE):
            continue
        if href in seen_sections:
            continue
        seen_sections.add(href)

        section_url = urljoin(chapter_base, href)
        section_name_hint = _clean(a.get_text())
        _scrape_section(chapter_node, section_url, section_name_hint)


def _scrape_section(chapter_node: Node, section_url: str, section_name_hint: str) -> None:
    """Fetch a single section page and insert it as a content node."""
    soup = get_url_as_soup(section_url)
    body = soup.find("body")
    if body is None:
        return

    b_tag = body.find("b")
    node_name = _clean(b_tag.get_text()) if b_tag else section_name_hint

    # Strip trailing dash-like separators (ASCII -, en-dash, em-dash, horizontal bar).
    node_name = re.sub(r"\s*[\-–—―]+\s*$", "", node_name).strip()

    citation_raw = node_name.split()[0] if node_name else ""
    if not re.match(r"^\d", citation_raw):
        # Fallback: derive chapter:section from URL path e.g. .../I/1/1-1.htm
        m = re.search(r"/([\w\-]+)/([\w\-]+)\.htm$", section_url)
        if m:
            chap_url = m.group(1)
            sec_part = m.group(2)
            # Strip leading "{chap}-" prefix from section file stem.
            prefix = f"{chap_url}-"
            sec_only = sec_part[len(prefix):] if sec_part.startswith(prefix) else sec_part
            citation_raw = f"{chap_url}:{sec_only}"

    if ":" in citation_raw:
        section_num = citation_raw.split(":", 1)[1]
    else:
        section_num = citation_raw
    section_num = re.sub(r"\.$", "", section_num)

    citation = f"N.H. Rev. Stat. § {citation_raw}" if citation_raw else None
    node_id = f"{chapter_node.node_id}/section={section_num}"
    status = _check_reserved(node_name)

    node_text: Optional[NodeText] = None
    addendum: Optional[Addendum] = None

    if not status:
        codesect = body.find("codesect")
        sourcenote = body.find("sourcenote")

        node_text = NodeText()
        if codesect:
            codesect_text = _clean(codesect.get_text())
            if codesect_text:
                node_text.add_paragraph(text=codesect_text)

        if sourcenote:
            history_text = _clean(sourcenote.get_text())
            if history_text:
                addendum = Addendum()
                addendum.history = AddendumType(type="history", text=history_text)

    section_node = Node(
        id=node_id,
        link=section_url,
        citation=citation,
        top_level_title=chapter_node.top_level_title,
        node_type="content",
        level_classifier="section",
        number=section_num,
        node_name=node_name,
        parent=chapter_node.node_id,
        status=status,
        node_text=node_text,
        addendum=addendum,
    )
    insert_node(section_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _clean(raw: str) -> str:
    """Strip whitespace, decode HTML entities (&#150;, &ndash;, &nbsp;, ...),
    collapse runs, and fix common mojibake.

    NH HTML uses legacy Windows-1252 numeric entities like &#150; (en-dash) and
    &#151; (em-dash). BeautifulSoup's get_text() leaves bare entities alone if
    they were already decoded by the parser into raw bytes; html.unescape()
    handles any that survive. Then we map the Windows-1252 control-range code
    points (U+0080..U+009F) to their Unicode equivalents.
    """
    if raw is None:
        return ""
    text = _html_mod.unescape(raw)
    # Map Windows-1252 control-range chars (which is what &#150;, &#151;, &#147;
    # etc. decode to via unescape: U+0096, U+0097, U+0093, ...) to real Unicode.
    _CP1252_FIX = {
        "": "€", "": "‚", "": "ƒ",
        "": "„", "": "…", "": "†",
        "": "‡", "": "ˆ", "": "‰",
        "": "Š", "": "‹", "": "Œ",
        "": "Ž", "": "‘", "": "’",
        "": "“", "": "”", "": "•",
        "": "–", "": "—", "": "˜",
        "": "™", "": "š", "": "›",
        "": "œ", "": "ž", "": "Ÿ",
    }
    for k, v in _CP1252_FIX.items():
        if k in text:
            text = text.replace(k, v)
    text = text.replace("\xa0", " ").replace(" ", " ").replace(" ", " ")
    text = re.sub(r"\s+", " ", text).strip()
    text = _fix_encoding(text)
    return text


def _check_reserved(text: str) -> Optional[str]:
    upper = text.upper()
    for kw in RESERVED_KEYWORDS:
        if kw.upper() in upper:
            return "reserved"
    return None


def _extract_chapter_number(chapter_name: str) -> Optional[str]:
    """Extract chapter number from 'CHAPTER 1 ...' or 'CHAPTER 1-A ...'."""
    m = re.match(r"CHAPTER\s+([\w\-]+)", chapter_name, re.IGNORECASE)
    if m:
        return m.group(1).rstrip(":")
    return None


if __name__ == "__main__":
    main()
