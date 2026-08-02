r"""Rhode Island General Laws scraper.

Source: https://webserver.rilegislature.gov/Statutes/
Hierarchy: title -> chapter -> section
Citation: R.I. Gen. Laws § <SECTION>

REAL HTML landmarks (verified 2026-05-11 against live site via Webshare proxy):

  Master TOC  (https://webserver.rilegislature.gov/Statutes/):
      Bare fragment HTML with no <html>/<body> wrapper. Just <LI> elements.
      Links: <A HREF="TITLE1/INDEX.HTM"><B> TITLE 1  Aeronautics </B></A>
      Pattern: TITLE{N}/INDEX.HTM  (uppercase, e.g. TITLE6A/INDEX.HTM)
      NOTE: the old HTTP host webserver.rilin.state.ri.us redirects every
      sub-path back to the master TOC root, making sub-pages unreachable. The
      canonical base is https://webserver.rilegislature.gov.

  Title index  (e.g. .../Statutes/TITLE1/INDEX.HTM):
      Full HTML with <body>. Title name in <h1><center>.
      Chapter links: <p><a href="1-1/INDEX.htm">Chapter 1-1 ...</a></p>
      Pattern: {N}-{M}/INDEX.htm  (note lowercase .htm vs uppercase .HTM)

  Chapter index  (e.g. .../Statutes/TITLE1/1-2/INDEX.htm):
      Full HTML with <body>. Chapter name in <h2><center>.
      Section links: <p><a href="1-2-3.htm">§ 1-2-3. Title of section.</a></p>
      Decimal section numbers exist: 1-2-1.1.htm, 1-2-14.2.htm, etc.
      Pattern: {N}-{M}-{S}.htm or {N}-{M}-{S}.{D}.htm

  Section page  (e.g. .../Statutes/TITLE1/1-2/1-2-3.htm):
      Full HTML with <body>. Three top-level <div>s:
        divs[0]: <h1><center>Title N / Title Name</center></h1>
        divs[1]: <h2><center>Chapter M / Chapter Name</center></h2>
        divs[2]: Content div containing:
            <p style="margin-left:0px"><b>§ N-M-S. Section heading.</b></p>
            <p style="margin-left:0px"><b>(a)</b> Body text ...</p>
            ...
            <div><p>History of Section.<br/>P.L. ...</p></div>   <- last child div

OLD BUGS (why 0 chunks were emitted):
  1. BASE_URL used the old HTTP host webserver.rilin.state.ri.us, which
     redirects every sub-path back to the master TOC. Title and chapter index
     pages were never reached.
  2. Section filename regex r"^[\w\\-]+\\.HTM$" (with IGNORECASE) missed
     decimal section numbers like 1-2-1.1.htm because \\w and \\- don't cover
     the decimal dot in the stem. Sections 1-2-1.1, 1-2-14.2, etc. were skipped.
  3. The history heuristic searched the last div of the entire <body> but on the
     new site the history <div> is nested inside divs[2], not at body level.

No Selenium. Pure HTTP + BeautifulSoup via get_url_as_soup (UA rotation + proxy).
"""
from __future__ import annotations

import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Optional, Tuple

# ---------------------------------------------------------------------------
# Path bootstrap
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
JURISDICTION = "ri"
CORPUS = "statutes"
TABLE_NAME = f"{COUNTRY}_{JURISDICTION}_{CORPUS}"

# Canonical base: HTTPS on the new host. The old HTTP host
# webserver.rilin.state.ri.us redirects every sub-path back to the root TOC.
BASE_URL = "https://webserver.rilegislature.gov/Statutes"
TOC_URL = f"{BASE_URL}/"

RESERVED_KEYWORDS = ["[repealed]", "[expired]", "[reserved]", "[renumbered]", "repealed.", "reserved."]

# History paragraph inside the last nested <div> typically starts with this.
_HISTORY_PREFIX = "History of Section"


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    corpus_node: Node = insert_jurisdiction_and_corpus_node(COUNTRY, JURISDICTION, CORPUS)
    _scrape_all_titles(corpus_node)


# ---------------------------------------------------------------------------
# Title-level scraping
# ---------------------------------------------------------------------------


def _titles_done_path():
    """Where we persist the set of titles already fully scraped for resume."""
    from vaquill_pipeline.config import SETTINGS
    return SETTINGS.chunks_dir / "state_ri_titles_done.txt"


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
    """Fetch the master TOC and iterate over title links in parallel.

    Each title is independent of every other title, so we hand them to a
    ThreadPoolExecutor. Concurrency is set by env ``VAQUILL_TITLE_WORKERS``
    (default 8). Resume: titles previously completed are persisted in
    ``state_ri_titles_done.txt`` and skipped on re-runs. Set env
    ``VAQUILL_FORCE_RESCRAPE=1`` to override and re-scrape.
    """
    titles_done = set() if os.environ.get("VAQUILL_FORCE_RESCRAPE") else _load_titles_done()
    if titles_done:
        print(f"[scrapeRI] resume: {len(titles_done)} titles already done: {sorted(titles_done)}", flush=True)

    soup = get_url_as_soup(TOC_URL)

    work: List[Node] = []
    for a_tag in soup.find_all("a", href=True):
        href: str = a_tag["href"].strip()

        # Match TITLE{N}/INDEX.HTM (case-insensitive because the server
        # redirects correctly for both cases).
        m = re.match(r"^TITLE([\w\-]+)/INDEX\.HTM$", href, re.IGNORECASE)
        if not m:
            continue

        raw_number = m.group(1)  # e.g. "1", "6A", "9-A"
        # Preserve alphanumeric suffixes; strip leading zeros from pure digits.
        number = _normalise_number(raw_number)

        node_name = _clean_text(a_tag.get_text())
        if not node_name:
            continue

        # Href is relative to TOC_URL; join carefully.
        link = f"{BASE_URL}/{href}"
        node_id = f"{corpus_node.node_id}/title={number}"
        status = _check_reserved(node_name)

        title_node = Node(
            id=node_id,
            link=link,
            top_level_title=number,
            node_type="structure",
            level_classifier="title",
            number=number,
            node_name=node_name,
            parent=corpus_node.node_id,
            status=status,
        )
        insert_node(title_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)

        if status:
            continue
        if number in titles_done:
            continue
        work.append(title_node)

    def _do_title(title_node: Node):
        try:
            _scrape_title(title_node)
            _mark_title_done(title_node.number)
            return (title_node.number, "ok", None)
        except Exception as e:
            return (title_node.number, "fail", str(e)[:200])

    workers = int(os.environ.get("VAQUILL_TITLE_WORKERS", "8"))
    print(f"[scrapeRI] running {len(work)} titles with {workers} parallel workers", flush=True)
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for fut in as_completed(ex.submit(_do_title, item) for item in work):
            num, status_str, err = fut.result()
            if status_str == "fail":
                print(f"[scrapeRI] title {num}: {status_str}: {err}", flush=True)
            else:
                print(f"[scrapeRI] title {num}: {status_str}", flush=True)


# ---------------------------------------------------------------------------
# Chapter-level scraping
# ---------------------------------------------------------------------------


def _scrape_title(title_node: Node) -> None:
    """Fetch a title index page and iterate over chapter links.

    Title page lives at e.g.:
        https://webserver.rilegislature.gov/Statutes/TITLE1/INDEX.HTM

    Chapter links are relative to the title page directory:
        <a href="1-1/INDEX.htm">Chapter 1-1 ...</a>
        <a href="1-2/INDEX.htm">Chapter 1-2 ...</a>

    Pattern: {N}-{M}/INDEX.htm  (note lowercase .htm)
    """
    soup = get_url_as_soup(str(title_node.link))

    # Title name may be richer in <h1>.
    h1 = soup.find("h1")
    if h1:
        cleaned = _clean_text(h1.get_text())
        if cleaned:
            title_node.node_name = cleaned

    # Construct the base URL for this title directory.
    title_base = str(title_node.link).rsplit("/", 1)[0]

    for a_tag in soup.find_all("a", href=True):
        href: str = a_tag["href"].strip()

        # Chapter index links: e.g. "1-1/INDEX.htm" (case-insensitive match).
        if not re.match(r"^[\w\-]+/INDEX\.HTM$", href, re.IGNORECASE):
            continue

        # Chapter number is the directory component, e.g. "1-1", "6A-1".
        ch_number = href.split("/")[0]

        node_name = _clean_text(a_tag.get_text())
        if not node_name:
            continue

        link = f"{title_base}/{href}"
        node_id = f"{title_node.node_id}/chapter={ch_number}"
        status = _check_reserved(node_name)

        chapter_node = Node(
            id=node_id,
            link=link,
            top_level_title=title_node.top_level_title,
            node_type="structure",
            level_classifier="chapter",
            number=ch_number,
            node_name=node_name,
            parent=title_node.node_id,
            status=status,
        )
        insert_node(chapter_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)

        if not status:
            _scrape_chapter(chapter_node)


# ---------------------------------------------------------------------------
# Section-level scraping
# ---------------------------------------------------------------------------


def _scrape_chapter(chapter_node: Node) -> None:  # noqa: E501
    r"""Fetch a chapter index page and iterate over section links.

    Chapter page lives at e.g.:
        https://webserver.rilegislature.gov/Statutes/TITLE1/1-2/INDEX.htm

    Section links are relative to the chapter directory:
        <a href="1-2-1.htm">§ 1-2-1. Powers of the president ...</a>
        <a href="1-2-1.1.htm">§ 1-2-1.1. Powers relating to ...</a>

    IMPORTANT: decimal section numbers like 1-2-1.1 are common. The href
    pattern is {digits-and-dashes}.{decimal}.htm or {digits-and-dashes}.htm.
    Use r"^[\w\-\.]+\.htm$" (case-insensitive) to match all variants, then
    skip INDEX.htm explicitly.

    Section number is the filename stem (everything before the final ".htm"),
    e.g. "1-2-1.1" from "1-2-1.1.htm".
    """
    soup = get_url_as_soup(str(chapter_node.link))

    # Chapter name may be richer in <h2>.
    h2 = soup.find("h2")
    if h2:
        cleaned = _clean_text(h2.get_text())
        if cleaned:
            chapter_node.node_name = cleaned

    chapter_base = str(chapter_node.link).rsplit("/", 1)[0]

    for a_tag in soup.find_all("a", href=True):
        href: str = a_tag["href"].strip()

        # Section file pattern: {sec_number}.htm (with optional decimal suffix).
        # Must match lowercase and uppercase .htm/.HTM.
        if not re.match(r"^[\w\-\.]+\.htm$", href, re.IGNORECASE):
            continue
        # Skip chapter/title index back-links.
        if href.upper() == "INDEX.HTM":
            continue

        # Section number: strip the final ".htm" extension only.
        # "1-2-1.htm"   -> "1-2-1"
        # "1-2-1.1.htm" -> "1-2-1.1"
        sec_number = re.sub(r"\.htm$", "", href, flags=re.IGNORECASE)

        node_name = _clean_text(a_tag.get_text())
        if not node_name:
            continue

        link = f"{chapter_base}/{href}"
        node_id = f"{chapter_node.node_id}/section={sec_number}"
        status = _check_reserved(node_name)
        citation = f"R.I. Gen. Laws § {sec_number}"

        if status:
            section_node = Node(
                id=node_id,
                link=link,
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

        node_text, addendum = _fetch_section_content(link)

        section_node = Node(
            id=node_id,
            link=link,
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


# ---------------------------------------------------------------------------
# Section body fetching
# ---------------------------------------------------------------------------


def _fetch_section_content(url: str) -> Tuple[Optional[NodeText], Optional[Addendum]]:
    """Fetch a section page and return (NodeText | None, Addendum | None).

    Section page structure (verified 2026-05-11):
        <body>
            <div>                         <- divs[0]: title banner <h1>
            <div>                         <- divs[1]: chapter banner <h2>
            <div>                         <- divs[2]: content
                <p><b>§ N-M-S. Heading.</b></p>    <- heading, skip
                <p><b>(a)</b> Body text...</p>      <- content paragraphs
                ...
                <div>                              <- history nested div
                    <p>History of Section.<br/>P.L. ...</p>
                </div>
            </div>
        </body>

    The history <div> is the last child <div> inside the content div (divs[2]).
    """
    soup = get_url_as_soup(url)
    body = soup.find("body")
    if body is None:
        return None, None

    # Top-level <div>s directly under <body>.
    top_divs = body.find_all("div", recursive=False)
    if len(top_divs) >= 3:
        content_div = top_divs[2]
    elif top_divs:
        content_div = top_divs[-1]
    else:
        content_div = body

    node_text = NodeText()
    history_text = ""

    # Extract the history <div> (last nested div inside content_div) first so
    # we can identify and skip its paragraph(s) when iterating.
    nested_divs = content_div.find_all("div", recursive=False)
    history_div = nested_divs[-1] if nested_divs else None

    if history_div is not None:
        raw_hist = history_div.get_text(separator=" ")
        cleaned = _clean_text(raw_hist)
        if _HISTORY_PREFIX in cleaned or "P.L." in cleaned or "G.L." in cleaned:
            history_text = cleaned

    # Walk the direct <p> children of content_div (skip any inside history_div).
    for p in content_div.find_all("p", recursive=False):
        text = _clean_text(p.get_text(separator=" "))
        if not text:
            continue

        # Skip the section heading paragraph (bold "§ N-M-S. Name." with nothing else).
        b_tag = p.find("b")
        if b_tag:
            b_text = _clean_text(b_tag.get_text())
            # If the whole paragraph is just the bold heading text, skip it.
            remainder = text.replace(b_text, "").strip()
            if not remainder:
                continue

        node_text.add_paragraph(text=text)

    if not node_text.paragraphs:
        node_text = None  # type: ignore[assignment]

    addendum: Optional[Addendum] = None
    if history_text:
        addendum = Addendum()
        addendum.history = AddendumType(type="history", text=history_text)

    return node_text, addendum


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _normalise_number(raw: str) -> str:
    """Strip leading zeros from numeric prefix while preserving alpha suffixes.

    Examples:
        "1"   -> "1"
        "01"  -> "1"
        "6A"  -> "6A"
        "9-A" -> "9-A"
    """
    m = re.match(r"^0*(\d+)(.*)", raw)
    if m:
        return m.group(1) + m.group(2)
    return raw


def _check_reserved(text: str) -> Optional[str]:
    """Return 'reserved' if the text signals a repealed/reserved entry."""
    lower = text.lower()
    for kw in RESERVED_KEYWORDS:
        if kw in lower:
            return "reserved"
    return None


def _clean_text(raw: str) -> str:
    """Normalise whitespace, remove non-breaking spaces, and fix common mojibake."""
    text = raw.replace("\xa0", " ").replace(" ", " ").replace("—", " ").replace(" ", " ")
    # Strip the common mojibake sequence for § (Â§) that appears when UTF-8 is
    # decoded as Latin-1.
    text = text.replace("Â§", "§").replace("Â§", "§").replace("â", "")
    text = re.sub(r"\s+", " ", text).strip()
    return text


# ---------------------------------------------------------------------------

if __name__ == "__main__":
    main()
