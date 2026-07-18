"""Washington State (WA) statutes scraper.

Source: https://apps.leg.wa.gov/RCW/ (official WA Legislature RCW portal)

Hierarchy:
    us/wa/statutes/title=N/chapter=M/section=S

Citation format: "RCW <SECTION>" e.g. "RCW 1.04.010"

DOM notes:
- TOC:        href matches ``default.aspx?Cite=<N>`` (title links)
- Title page: contentWrapper .title-page; chapter links are /rcw/default.aspx?cite=<N.NN>
- Chapter:    contentWrapper .chapter-page; section rows are table > tr with three <td>
              td[0] HTML/PDF buttons, td[1] section number link, td[2] section heading text
- Section:    contentWrapper .section-page; content is div[2] (zero-indexed top-level);
              history bracket is div[3] (margin-top:15pt); notes follow in div[4]+
"""
from __future__ import annotations

import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

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
JURISDICTION = "wa"
CORPUS = "statutes"
TABLE_NAME = f"{COUNTRY}_{JURISDICTION}_{CORPUS}"

BASE_URL = "https://apps.leg.wa.gov/RCW"
TOC_URL = f"{BASE_URL}/default.aspx"

RESERVED_KEYWORDS = [
    "reserved",
    "repealed",
    "expired",
    "renumbered",
    "deleted",
    "transferred",
    "recodified",
]


def main() -> None:
    corpus_node: Node = insert_jurisdiction_and_corpus_node(COUNTRY, JURISDICTION, CORPUS)
    scrape_all_titles(corpus_node)


def scrape_all_titles(corpus_node: Node) -> None:
    """Fetch the RCW top-level TOC and dispatch each title in parallel.

    Each title is an independent subtree (title page -> chapters -> sections)
    with no shared state, and the JSONL sink + counters in
    vaquill_pipeline.patch are lock-protected, so titles fan out to a
    ThreadPoolExecutor sized by VAQUILL_TITLE_WORKERS (default 8, matching
    scrapeAK).

    Why: apps.leg.wa.gov is a slow legacy backend (~3s/page), so a full crawl
    of ~78k sections is latency-bound, not server-bound -- it measured ~44h
    sequentially. The host serves 8 concurrent requests in ~3s wall-clock with
    no throttling or 429s, so 8 workers cuts the crawl to roughly 8h. This
    scraper was previously the only knob that did nothing: it ignored
    VAQUILL_TITLE_WORKERS entirely and crawled single-threaded.

    NOTE: WA deliberately has no titles_done resume file -- every run re-crawls
    in full. That is what lets an amended section be re-fetched and re-chunked
    into a fresh content-addressed point_id (the JSONL skipset suppresses the
    write for unchanged sections, so a re-crawl is cheap in output but still
    catches amendments). Do not add a titles_done skip here to save time
    without replacing that freshness some other way -- it would make WA
    amendment-blind the way the titles_done states are.
    """
    soup = get_url_as_soup(TOC_URL)

    work: list[Node] = []
    for link_tag in soup.find_all("a", href=True):
        href = link_tag["href"].strip()
        # Title links: default.aspx?Cite=1  or  default.aspx?Cite=9A
        m = re.match(r"^default\.aspx\?Cite=([\w]+)$", href, re.IGNORECASE)
        if not m:
            continue

        number = m.group(1)
        node_name = f"Title {number} RCW"
        link = f"{BASE_URL}/default.aspx?Cite={number}"
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

        if not status:
            work.append(title_node)

    def _do_title(title_node: Node) -> tuple[str, str, str | None]:
        # One title's failure must not abort the other workers, so each is
        # wrapped and reported; the run continues with the remaining titles.
        try:
            scrape_title(title_node)
            return (title_node.number, "ok", None)
        except Exception as exc:
            return (title_node.number, "fail", str(exc)[:200])

    workers = int(os.environ.get("VAQUILL_TITLE_WORKERS", "8"))
    print(
        f"[scrapeWA] running {len(work)} titles with {workers} parallel workers",
        flush=True,
    )
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for fut in as_completed(ex.submit(_do_title, item) for item in work):
            num, status, err = fut.result()
            if status == "fail":
                print(f"[scrapeWA] title {num}: {status}: {err}", flush=True)
            else:
                print(f"[scrapeWA] title {num}: {status}", flush=True)


def scrape_title(title_node: Node) -> None:
    """Fetch a title page and iterate over its chapter links."""
    soup = get_url_as_soup(str(title_node.link))
    cw = soup.find("div", id="contentWrapper")
    if cw is None:
        return

    for link_tag in cw.find_all("a", href=True):
        href = link_tag["href"].strip()
        # Chapter links: /rcw/default.aspx?cite=1.04 (two-segment cite)
        m = re.match(r"^/rcw/default\.aspx\?cite=([\w]+\.[\w]+)$", href, re.IGNORECASE)
        if not m:
            continue

        ch_cite = m.group(1)  # e.g. "1.04"
        # Chapter number is the part after the dot
        parts = ch_cite.split(".", 1)
        ch_number = parts[1] if len(parts) == 2 else ch_cite
        node_name_txt = _chapter_name_from_row(link_tag)
        node_name = f"Chapter {ch_cite} RCW{': ' + node_name_txt if node_name_txt else ''}"
        ch_link = f"{BASE_URL}/default.aspx?cite={ch_cite}"
        node_id = f"{title_node.node_id}/chapter={ch_number}"
        status = _check_reserved(node_name)

        chapter_node = Node(
            id=node_id,
            link=ch_link,
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
            scrape_chapter(chapter_node)


def scrape_chapter(chapter_node: Node) -> None:
    """Fetch a chapter page and iterate over its section rows."""
    soup = get_url_as_soup(str(chapter_node.link))
    cw = soup.find("div", id="contentWrapper")
    if cw is None:
        return

    for row in cw.find_all("tr"):
        cells = row.find_all("td")
        # Expect three cells: [HTML/PDF buttons, section number, heading text]
        if len(cells) < 3:
            continue

        # Section number link is in cells[1]
        sec_link_tag = cells[1].find("a", href=True)
        if sec_link_tag is None:
            continue

        sec_cite = sec_link_tag.get_text().strip()  # e.g. "1.04.010"
        if not re.match(r"^\d", sec_cite):
            continue

        # Section number is the last segment: "010"
        sec_segments = sec_cite.split(".")
        sec_number = sec_segments[-1] if sec_segments else sec_cite

        sec_heading = _clean_text(cells[2].get_text())
        node_name = f"RCW {sec_cite}: {sec_heading}" if sec_heading else f"RCW {sec_cite}"

        href = sec_link_tag["href"].strip()
        # Build absolute URL
        if href.startswith("/"):
            sec_link = f"https://apps.leg.wa.gov{href}"
        else:
            sec_link = href

        node_id = f"{chapter_node.node_id}/section={sec_number}"
        status = _check_reserved(node_name)
        citation = f"RCW {sec_cite}"

        if status:
            section_node = Node(
                id=node_id,
                link=sec_link,
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

        node_text, addendum = _fetch_section_content(sec_link)

        section_node = Node(
            id=node_id,
            link=sec_link,
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


def _fetch_section_content(url: str):
    """Fetch a section page and return (NodeText | None, Addendum | None).

    Section page DOM (contentWrapper .section-page), top-level divs:
        div[0] -- empty
        div[1] -- empty
        div[2] -- substantive text (contains nested divs with text-indent:0.5in)
        div[3] -- history bracket "[1951 c 5 s 2; ...]"  (style margin-top:15pt)
        div[4] -- "Notes:" heading
        div[5] -- notes body text
    """
    try:
        soup = get_url_as_soup(url)
    except Exception:
        return None, None

    cw = soup.find("div", id="contentWrapper")
    if cw is None:
        return None, None

    top_divs = cw.find_all("div", recursive=False)

    node_text = NodeText()
    history_text = ""
    notes_lines: list[str] = []
    in_notes = False

    for i, div in enumerate(top_divs):
        style = div.get("style", "")
        raw = div.get_text(separator="\n")
        text = _clean_text(raw)

        # Skip empties
        if not text:
            continue

        # History bracket div: style contains margin-top:15pt
        if "margin-top:15pt" in style or (text.startswith("[") and "]" in text):
            history_text = text
            in_notes = False
            continue

        # Notes heading
        if text.lower().strip() == "notes:":
            in_notes = True
            continue

        # Notes body
        if in_notes:
            if text:
                notes_lines.append(text)
            continue

        # Skip the first two empty divs by index (already handled via empty check)
        if i <= 1:
            continue

        # Substantive content
        node_text.add_paragraph(text=text)

    addendum = None
    history_combined = history_text
    if notes_lines:
        notes_body = " ".join(notes_lines)
        if history_combined:
            history_combined = f"{history_combined} NOTES: {notes_body}"
        else:
            history_combined = f"NOTES: {notes_body}"

    if history_combined:
        addendum = Addendum()
        addendum.history = AddendumType(type="history", text=history_combined)

    if not node_text.paragraphs:
        node_text = None

    return node_text, addendum


def _chapter_name_from_row(link_tag) -> str:
    """Given the <a> chapter link, find the adjacent chapter description text."""
    row = link_tag.find_parent("tr")
    if row is None:
        return ""
    cells = row.find_all("td")
    if len(cells) >= 2:
        return _clean_text(cells[-1].get_text())
    return ""


def _check_reserved(text: str) -> str | None:
    lower = text.lower()
    for kw in RESERVED_KEYWORDS:
        if kw in lower:
            return "reserved"
    return None


def _clean_text(raw: str) -> str:
    text = raw.replace("\xa0", " ").replace(" ", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


if __name__ == "__main__":
    main()
