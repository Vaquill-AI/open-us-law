"""
South Carolina Code of Laws scraper.

Source:   https://www.scstatehouse.gov/code/statmast.php
Hierarchy: us/sc/statutes/title=N/chapter=M/section=S
Citation:  S.C. Code Ann. § <SECTION>  (e.g. S.C. Code Ann. § 1-1-10)

Structure of the SC statehouse site:
  - TOC page:       statmast.php         -- links /code/titleN.php
  - Title page:     titleN.php           -- table rows with chapter name + /code/tNNcMMM.php
  - Chapter page:   tNNcMMM.php          -- flat inline HTML with:
      <div> ARTICLE N / Article name
      <span style="font-weight: bold;"> SECTION X-Y-Z.
      NavigableString  section title / body text
      NavigableString  HISTORY: ...

No Selenium required -- all content is server-rendered HTML.
"""

import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

from bs4 import BeautifulSoup, NavigableString, Tag

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

COUNTRY = "us"
JURISDICTION = "sc"
CORPUS = "statutes"
TABLE_NAME = f"{COUNTRY}_{JURISDICTION}_{CORPUS}"

BASE_URL = "https://www.scstatehouse.gov"
TOC_URL = "https://www.scstatehouse.gov/code/statmast.php"

# Keywords that mark a section or structural node as repealed/reserved/expired.
RESERVED_KEYWORDS = ["REPEALED", "RESERVED", "EXPIRED", "RENUMBERED"]


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    corpus_node: Node = insert_jurisdiction_and_corpus_node(COUNTRY, JURISDICTION, CORPUS)
    scrape_all_titles(corpus_node)


# ---------------------------------------------------------------------------
# Title-level
# ---------------------------------------------------------------------------

def _titles_done_path():
    """Where we persist the set of titles already fully scraped for resume."""
    from vaquill_pipeline.config import SETTINGS
    return SETTINGS.chunks_dir / "state_sc_titles_done.txt"


def _load_titles_done() -> set:
    try:
        path = _titles_done_path()
    except Exception:
        return set()
    if not path.exists():
        return set()
    return {l.strip() for l in path.read_text().splitlines() if l.strip()}


def _mark_title_done(number: str) -> None:
    try:
        path = _titles_done_path()
    except Exception:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a") as fh:
        fh.write(f"{number}\n")
        fh.flush()


def scrape_all_titles(corpus_node: Node) -> None:
    """Fetch the master TOC and iterate over all title links in parallel.

    Title-level resume via ``state_sc_titles_done.txt``. Concurrency set by
    ``VAQUILL_TITLE_WORKERS`` env var (default 8). Set
    ``VAQUILL_FORCE_RESCRAPE=1`` to ignore the resume file.
    """
    soup = get_url_as_soup(TOC_URL)

    titles_done = set() if os.environ.get("VAQUILL_FORCE_RESCRAPE") else _load_titles_done()
    if titles_done:
        print(f"[scrapeSC] resume: {len(titles_done)} titles already done: "
              f"{sorted(titles_done, key=lambda x: int(x) if x.isdigit() else 999)}",
              flush=True)

    work: list[Node] = []
    for a in soup.find_all("a", href=True):
        href: str = a["href"].strip()
        # Title links look like /code/titleN.php
        m = re.match(r"^/code/title(\d+)\.php$", href)
        if not m:
            continue

        number = m.group(1)
        title_url = f"{BASE_URL}{href}"

        node_name = _get_title_name(title_url, number)
        node_id = f"{corpus_node.node_id}/title={number}"
        status = _check_reserved(node_name)

        title_node = Node(
            id=node_id,
            link=title_url,
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

    def _do_title(tn: Node):
        try:
            scrape_title(tn)
            _mark_title_done(str(tn.number))
            return (tn.number, "ok", None)
        except Exception as e:
            return (tn.number, "fail", str(e)[:200])

    workers = int(os.environ.get("VAQUILL_TITLE_WORKERS", "8"))
    print(f"[scrapeSC] running {len(work)} titles with {workers} parallel workers", flush=True)
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for fut in as_completed(ex.submit(_do_title, tn) for tn in work):
            num, status_str, err = fut.result()
            if status_str == "fail":
                print(f"[scrapeSC] title {num}: {status_str}: {err}", flush=True)
            else:
                print(f"[scrapeSC] title {num}: {status_str}", flush=True)


def _get_title_name(title_url: str, number: str) -> str:
    """Return a clean title name like 'Title 1 - ADMINISTRATION OF THE GOVERNMENT'."""
    try:
        soup = get_url_as_soup(title_url)
        bh = soup.find(class_="barheader")
        if bh:
            raw = bh.get_text(" ", strip=True)
            # Strip site-wide prefix 'South Carolina Code of Laws'
            raw = re.sub(r"^South Carolina Code of Laws\s*", "", raw).strip()
            return _clean_text(raw)
    except Exception:
        pass
    return f"Title {number}"


# ---------------------------------------------------------------------------
# Chapter-level
# ---------------------------------------------------------------------------

def scrape_title(title_node: Node) -> None:
    """Fetch a title page and iterate over chapter rows."""
    soup = get_url_as_soup(str(title_node.link))
    content = soup.find(id="contentsection")
    if content is None:
        return

    for tr in content.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 2:
            continue

        chapter_name_raw = _clean_text(tds[0].get_text())
        # Chapter rows start with "CHAPTER N - ..."
        m = re.match(r"CHAPTER\s+([\w\-]+)", chapter_name_raw, re.IGNORECASE)
        if not m:
            continue

        ch_number = m.group(1)

        # The HTML link is in the second td
        html_link_tag = tds[1].find("a", href=True)
        if not html_link_tag:
            continue

        ch_url = BASE_URL + html_link_tag["href"].strip()
        node_id = f"{title_node.node_id}/chapter={ch_number}"
        status = _check_reserved(chapter_name_raw)

        chapter_node = Node(
            id=node_id,
            link=ch_url,
            top_level_title=title_node.top_level_title,
            node_type="structure",
            level_classifier="chapter",
            number=ch_number,
            node_name=chapter_name_raw,
            parent=title_node.node_id,
            status=status,
        )
        insert_node(chapter_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)

        if not status:
            scrape_chapter(chapter_node)


# ---------------------------------------------------------------------------
# Section-level
# ---------------------------------------------------------------------------

def scrape_chapter(chapter_node: Node) -> None:
    """
    Parse a chapter page and emit article structure nodes + section content nodes.

    The page is a flat stream of elements inside #contentsection:
      <div>  ARTICLE N      (two consecutive divs: label then name)
      <div>  Article name
      <span style="font-weight: bold;">  SECTION X-Y-Z.
      NavigableString  section title (same line, immediately after span)
      NavigableString / <br>  body paragraphs
      NavigableString  HISTORY: ...  (signals end of section body)
    """
    soup = get_url_as_soup(str(chapter_node.link))
    content = soup.find(id="contentsection")
    if content is None:
        return

    elements = list(content.contents)

    # Track current article context.
    current_article_id: str = chapter_node.node_id
    article_label_pending: Optional[str] = None  # "ARTICLE N" waiting for name
    sections_emitted = 0  # zero-section guard

    def _emit_pending_article(name_text: str) -> None:
        """Materialize a pending ARTICLE label with the given name text."""
        nonlocal article_label_pending, current_article_id
        if article_label_pending is None:
            return
        article_full = f"{article_label_pending} - {name_text}" if name_text else article_label_pending
        art_m = re.match(r"ARTICLE\s+([\w\-]+)", article_label_pending, re.IGNORECASE)
        art_number = art_m.group(1) if art_m else article_label_pending
        article_node_id = f"{chapter_node.node_id}/article={art_number}"
        art_status = _check_reserved(article_full)
        article_node = Node(
            id=article_node_id,
            link=str(chapter_node.link),
            top_level_title=chapter_node.top_level_title,
            node_type="structure",
            level_classifier="article",
            number=art_number,
            node_name=article_full,
            parent=chapter_node.node_id,
            status=art_status,
        )
        insert_node(article_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)
        current_article_id = article_node_id
        article_label_pending = None

    # Track current section being assembled.
    in_section = False
    sec_number: str = ""
    sec_citation: str = ""
    sec_link: str = ""
    sec_name_parts: list[str] = []
    sec_got_name = False
    sec_body_parts: list[str] = []
    sec_history = ""
    sec_status: Optional[str] = None
    sec_parent_id: str = chapter_node.node_id

    def flush_section() -> None:
        """Emit the accumulated section node, then reset state."""
        nonlocal in_section, sec_number, sec_citation, sec_link
        nonlocal sec_name_parts, sec_got_name, sec_body_parts
        nonlocal sec_history, sec_status, sec_parent_id, sections_emitted

        if not in_section or not sec_number:
            in_section = False
            return

        full_name = _clean_text(" ".join(sec_name_parts))
        node_id = f"{sec_parent_id}/section={sec_number}"
        status = sec_status or _check_reserved(full_name)

        node_text: Optional[NodeText] = None
        addendum: Optional[Addendum] = None

        if not status:
            combined = " ".join(sec_body_parts)
            combined = _clean_text(combined)
            if combined:
                node_text = NodeText()
                node_text.add_paragraph(text=combined)

            if sec_history:
                addendum = Addendum()
                addendum.history = AddendumType(
                    type="history", text=_clean_text(sec_history)
                )

        section_node = Node(
            id=node_id,
            link=sec_link,
            top_level_title=chapter_node.top_level_title,
            node_type="content",
            level_classifier="section",
            number=sec_number,
            node_name=full_name,
            parent=sec_parent_id,
            citation=sec_citation,
            status=status,
            node_text=node_text,
            addendum=addendum,
        )
        insert_node(section_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)
        sections_emitted += 1

        # Reset.
        in_section = False
        sec_number = ""
        sec_citation = ""
        sec_link = ""
        sec_name_parts = []
        sec_got_name = False
        sec_body_parts = []
        sec_history = ""
        sec_status = None
        sec_parent_id = current_article_id

    for elem in elements:
        # ----- NavigableString -----
        if isinstance(elem, NavigableString):
            text = _clean_text(str(elem))
            if not text:
                continue

            if not in_section:
                # Could be the article name following the ARTICLE label div,
                # delivered as a NavigableString rather than a <div>.
                if article_label_pending is not None:
                    _emit_pending_article(text)
                    continue
                continue

            # Inside a section: first non-empty string is the section title.
            if not sec_got_name:
                sec_name_parts.append(text)
                sec_got_name = True
                continue

            # HISTORY line signals end of body.
            if text.upper().startswith("HISTORY:"):
                sec_history = text
                continue

            sec_body_parts.append(text)
            continue

        # ----- Tag -----
        if not isinstance(elem, Tag):
            continue

        tag_name = elem.name
        style = elem.get("style", "")
        text = _clean_text(elem.get_text())

        # <br> -- skip (paragraph separators in body are handled at NavigableString level)
        if tag_name == "br":
            continue

        # Robust bold detection: explicit <strong>/<b>, CSS font-weight (any
        # spacing/casing), or a "bold" class. SC site is inconsistent.
        is_bold = (
            tag_name in ("strong", "b")
            or re.search(r"font-weight\s*:\s*bold", style, re.IGNORECASE) is not None
            or re.search(r"font-weight\s*:\s*[6-9]\d{2}", style) is not None
            or "bold" in " ".join(elem.get("class", [])).lower()
        )

        # Bold span/strong/b -- section header (or article-name follower).
        if tag_name in ("span", "strong", "b") and is_bold:
            # If we have a pending article label and this bold span is NOT
            # itself a SECTION header, treat its text as the article name.
            if (
                article_label_pending is not None
                and not re.match(r"SECTION\s+", text, re.IGNORECASE)
            ):
                _emit_pending_article(text)
                # Fall through to allow normal flow if this span turned out to
                # also start a section. (It shouldn't, but be safe.)
                continue

            # Flush any in-progress section first.
            flush_section()

            # Parse: "SECTION 1-1-10." -- accept any alphanumeric/dash/dot suffix
            # (e.g. 1-1-10, 12-36-2120, 16-3-20D, 38-71-2210.1, 1-23-630E).
            m = re.match(r"SECTION\s+([\w\-.]+?)\.", text, re.IGNORECASE)
            if not m:
                continue

            raw_number = m.group(1)
            sec_number = raw_number
            sec_citation = f"S.C. Code Ann. § {raw_number}"
            sec_link = str(chapter_node.link)
            sec_status = None
            sec_parent_id = current_article_id
            in_section = True
            sec_got_name = False
            sec_name_parts = [f"SECTION {raw_number}."]
            sec_body_parts = []
            sec_history = ""
            continue

        # <div> -- could be article label or article name or chapter-level header
        if tag_name == "div":
            text_upper = text.upper()

            # A bold centered div is the "Title N - ..." header at the top. Skip.
            if is_bold and not re.match(r"^ARTICLE\s+[\w\-]+$", text_upper):
                # If we have a pending article label, treat this bold div text
                # as the article name (some pages style names in bold).
                if article_label_pending is not None and text:
                    _emit_pending_article(text)
                continue

            # "ARTICLE N" label
            if re.match(r"^ARTICLE\s+[\w\-]+$", text_upper):
                flush_section()
                # If a previous ARTICLE label was never resolved with a name,
                # emit it now with an empty name so it isn't lost / leaked
                # into the next article's scope.
                if article_label_pending is not None:
                    _emit_pending_article("")
                article_label_pending = text
                continue

            # The div immediately after ARTICLE N div is the article name.
            if article_label_pending is not None and text:
                _emit_pending_article(text)
                continue

            # "CHAPTER N" heading inside chapter page (repeat of chapter label) -- skip.
            if text_upper.startswith("CHAPTER"):
                continue

            # "General Provisions" style subtitle after CHAPTER heading -- skip.
            # Just a presentation element, no structural meaning beyond chapter name.
            continue

    # Flush any remaining open section.
    flush_section()

    # Zero-section guard: log loudly if a non-reserved chapter produced nothing.
    if sections_emitted == 0:
        print(
            f"[scrapeSC][warn] chapter {chapter_node.node_id} produced 0 sections "
            f"(url={chapter_node.link}) -- possible layout change",
            flush=True,
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _check_reserved(text: str) -> Optional[str]:
    upper = text.upper()
    for kw in RESERVED_KEYWORDS:
        if kw in upper:
            return "reserved"
    return None


def _clean_text(raw: str) -> str:
    text = raw.replace("\xa0", " ").replace(" ", " ").replace(" ", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


if __name__ == "__main__":
    main()
