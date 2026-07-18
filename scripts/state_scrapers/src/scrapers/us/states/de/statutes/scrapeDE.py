import os
import sys
# BeautifulSoup imports
from bs4 import BeautifulSoup
from bs4.element import Tag

# Selenium imports
from selenium.webdriver.common.actions.wheel_input import ScrollOrigin
from selenium.webdriver import ActionChains
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement

from typing import List, Tuple
import time
import json
import re

from pathlib import Path

DIR = os.path.dirname(os.path.realpath(__file__))
# Get the current file's directory
current_file = Path(__file__).resolve()

# Find the 'src' directory
src_directory = current_file.parent
while src_directory.name != 'src' and src_directory.parent != src_directory:
    src_directory = src_directory.parent

# Get the parent directory of 'src'
project_root = src_directory.parent

# Add the project root to sys.path
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from src.utils.pydanticModels import NodeID, Node, Addendum, AddendumType, NodeText, Paragraph, ReferenceHub, Reference, DefinitionHub, Definition, IncorporatedTerms
from src.utils.scrapingHelpers import insert_jurisdiction_and_corpus_node, insert_node, get_url_as_soup


_MOJIBAKE_MARKERS = (
    "\xc2",      # Â (capital A with circumflex prefix)
    "\xe2\x80",  # â\x80 (start of curly quotes / en-em dashes in mis-decoded UTF-8)
    "\xe2\x82",  # â\x82 (Euro signs etc.)
    "\xe2\x84",  # â\x84 (trademark)
    "\xe2\x86",  # â\x86 (arrows)
    "â€",        # â€ (the high-frequency double-encoded marker)
)


def _fix_encoding(s: str) -> str:
    """Undo Latin-1/UTF-8 mojibake from delcode.delaware.gov.

    The site serves UTF-8 but with ambiguous Content-Type, so requests/bs4
    sometimes interprets it as Latin-1. We detect any of the typical
    multi-byte UTF-8 prefixes present as separate Unicode chars (the
    classic ``\\xc2``, ``\\xe2`` start bytes) and round-trip via Latin-1.
    """
    if not s:
        return s
    if not any(m in s for m in _MOJIBAKE_MARKERS):
        return s
    try:
        fixed = s.encode("latin-1", errors="ignore").decode("utf-8", errors="ignore")
    except Exception:
        return s
    # Only accept the fix if it has FEWER mojibake markers than the original.
    if sum(m in fixed for m in _MOJIBAKE_MARKERS) < sum(m in s for m in _MOJIBAKE_MARKERS):
        return fixed
    return s



SKIP_TITLE = 0 # If you want to skip the first n titles, set this to n
COUNTRY = "us"
# State code for states, 'federal' otherwise
JURISDICTION = "de"
# 'statutes' is current default
CORPUS = "statutes"
# No need to change this
TABLE_NAME =  f"{COUNTRY}_{JURISDICTION}_{CORPUS}"
BASE_URL = "https://delcode.delaware.gov"
TOC_URL = "https://delcode.delaware.gov"
SKIP_TITLE = 0
RESERVED_KEYWORDS = ["[Repealed", "[Expired", "[Reserved"]

# MOST OF THE TIME: ["TITLE", "PART", "CHAPTER", "SECTION"]
# Going to chapter will bring you to the sections page (sometimes part)

def main():
    corpus_node: Node = insert_jurisdiction_and_corpus_node(COUNTRY, JURISDICTION, CORPUS)
    scrape_constitution(corpus_node)
    scrape_all_titles(corpus_node)


def _fetch_title_pdf(href: str, title_num: str) -> None:
    """Pull the Authenticated PDF for a title through fetch_bytes so it lands
    in R2 alongside the HTML. Per-section chunks pick it up via the stem
    index, exposing r2_pdf_url next to r2_html_url for cross-format highlight.
    """
    pdf_href = href.replace("index.html", f"title{title_num}.pdf")
    pdf_url = f"{BASE_URL}/{pdf_href}"
    try:
        from vaquill_pipeline.http_client import fetch_bytes
        fetch_bytes(pdf_url, timeout=60, max_retries=2)
    except Exception as e:
        # Non-fatal: HTML chunks are still good even if PDF mirror fails.
        print(f"[warn] DE title {title_num} PDF skipped: {e}", flush=True)


def scrape_constitution(node_parent: Node) -> None:
    """Delaware Constitution. Lives at /constitution/index.html with the
    Authenticated PDF at /constitution/constitution.pdf. Treated as a
    pseudo-title under the same corpus so it lands in statutes_us alongside
    the regular titles. Skipped historically by the upstream scraper.
    """
    const_node = Node(
        id=f"{node_parent.node_id}/title=Constitution",
        link=f"{BASE_URL}/constitution/index.html",
        top_level_title="Constitution",
        node_type="structure",
        level_classifier="title",
        number="Constitution",
        node_name="The Delaware Constitution",
        parent=node_parent.node_id,
    )
    insert_node(const_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)
    # PDF -> R2 (no chunks emitted from PDF; HTML chunks reference both)
    try:
        from vaquill_pipeline.http_client import fetch_bytes
        fetch_bytes(f"{BASE_URL}/constitution/constitution.pdf", timeout=60, max_retries=2)
    except Exception as e:
        print(f"[warn] DE constitution PDF skipped: {e}", flush=True)
    try:
        recursive_scrape(const_node)
    except Exception as e:
        print(f"[warn] DE constitution body skipped: {e}", flush=True)


def _titles_done_path():
    """Where we persist the set of titles already fully scraped for resume."""
    from vaquill_pipeline.config import SETTINGS
    return SETTINGS.chunks_dir / "state_de_titles_done.txt"


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


def scrape_all_titles(node_parent: Node):
    """Walk all 31 titles in parallel.

    Each title is fully independent of every other title (no shared state at
    the title level), so we hand them to a ThreadPoolExecutor. Concurrency
    is set by env var ``VAQUILL_TITLE_WORKERS`` (default 8). The HTTP layer
    uses a connection pool so concurrent requests share keep-alive sockets.
    JsonlSink + r2_sync + patch._state_lock all serialize the small critical
    sections - everything else (HTTP, parse) runs in parallel.

    Resume: titles previously completed are persisted in
    ``state_de_titles_done.txt`` and skipped entirely on re-runs (no HTTP
    re-fetch). Set env ``VAQUILL_FORCE_RESCRAPE=1`` to override and re-scrape.
    """
    import os
    from concurrent.futures import ThreadPoolExecutor, as_completed

    titles_done = set() if os.environ.get("VAQUILL_FORCE_RESCRAPE") else _load_titles_done()
    if titles_done:
        print(f"[scrapeDE] resume: {len(titles_done)} titles already done: {sorted(titles_done, key=lambda x: int(x) if x.isdigit() else 999)}", flush=True)

    soup = get_url_as_soup(TOC_URL).find(id="content")
    all_title_containers = soup.find_all("a")

    # Build the list of title work items first (small, sequential - pure parse).
    work: List[Node] = []
    for i, title_container in enumerate(all_title_containers):
        if i < SKIP_TITLE:
            continue
        href = title_container['href'].strip()
        if "/index.html" not in href:
            continue
        if "constitution" in href.lower():
            continue

        node_name = title_container.get_text().strip()
        try:
            if "Â" in node_name or "â\x80" in node_name:
                node_name = node_name.encode("latin-1").decode("utf-8")
        except Exception:
            pass
        number = node_name.split(" ")[1]
        link = f"{BASE_URL}/{href}"

        title_node = Node(
            id=f"{node_parent.node_id}/title={number}",
            link=link,
            top_level_title=number,
            node_type="structure",
            level_classifier="title",
            number=number,
            node_name=node_name,
            parent=node_parent.node_id,
        )
        # Insert the title structure node up front (cheap, idempotent).
        insert_node(title_node, TABLE_NAME, ignore_duplicate=True, debug_mode=False)
        if number in titles_done:
            # Title already fully scraped on a previous run.
            continue
        work.append((title_node, href, number))

    def _do_title(item):
        title_node, href, number = item
        try:
            _fetch_title_pdf(href, number)
            recursive_scrape(title_node)
            _mark_title_done(number)
            return (number, "ok", None)
        except Exception as e:
            return (number, "fail", str(e)[:200])

    workers = int(os.environ.get("VAQUILL_TITLE_WORKERS", "8"))
    print(f"[scrapeDE] running {len(work)} titles with {workers} parallel workers", flush=True)
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for fut in as_completed(ex.submit(_do_title, item) for item in work):
            num, status, err = fut.result()
            if status == "fail":
                print(f"[scrapeDE] title {num}: {status}: {err}", flush=True)
            else:
                print(f"[scrapeDE] title {num}: {status}", flush=True)



# Handles any generic structure node, routes sections to scrape_sections
def recursive_scrape(node_parent: Node):
    soup = get_url_as_soup(str(node_parent.link)).find(id="content")
    # Indicates page contains sections, send to section scrape function
    if soup.find(id="CodeBody"):
        scrape_sections(node_parent, soup)
    else:
        structure_node_containers = soup.find_all("div", class_="title-links")
        # Iterate over the container of the structure nodes
        for i, structure_container in enumerate(structure_node_containers):
            link_container = structure_container.find("a")
            href = link_container['href'].strip()


            link = href.replace("../","")
            link = f"{TOC_URL}/{link}"
            node_name = link_container.get_text().strip()
            level_classifier = node_name.split(" ")[0].lower()
            number = node_name.split(" ")[1]

            if number[-1] == ".":
                number=number[:-1]

            node_type = "structure"
            parent = node_parent.node_id
            top_level_title = node_parent.top_level_title

            status=None
            for word in RESERVED_KEYWORDS:
                if word in node_name:
                    status = word.lower()
                    break

            node_id = f"{parent}/{level_classifier}={number}"
            structure_node = Node(
                id=node_id,
                link=link,
                top_level_title=top_level_title,
                node_type=node_type,
                level_classifier=level_classifier,
                number=number,
                node_name=node_name,
                parent=node_parent.node_id,
                status=status
            )

            insert_node(structure_node, TABLE_NAME, debug_mode=True)
            recursive_scrape(structure_node)


def scrape_sections(node_parent: Node, soup: BeautifulSoup):
    # Scrape a section regularly

    section_containers = soup.find_all("div", class_="Section")

    for i, div in enumerate(section_containers):

        section_header = div.find("div", class_="SectionHead")
        node_name = section_header.get_text().strip()
        # Clean up super weird formatting
        node_name = node_name.replace("§", "")
        node_name = node_name.strip()
        node_name = f"§ {node_name}"

        # This is legacy code, I have no idea. Im not gonna touch it for now
        number = section_header['id']

        node_type = "content"
        level_classifier = "section"

        link = str(node_parent.link) + f"#{number}"

        number = number.replace(",", "-").rstrip(".")
        node_id = f"{node_parent.node_id}/{level_classifier}={number}"

        status = None
        for word in RESERVED_KEYWORDS:
            if word in node_name:
                status = "reserved"
                break

        node_text = None
        citation = f"{node_parent.top_level_title} Del. C. § {number}"

        # Finding addendum
        addendum = None
        core_metadata = None

        if not status:
            node_text = NodeText()
            addendum = Addendum()
            addendum.history = AddendumType(type="history", text="")
            addendum_references = ReferenceHub()
            for element in div.find_all(recursive=False):
                # Skip the sectionHead
                if 'class' in element.attrs and element['class'][0] == "SectionHead":
                    continue

                # I want to remove all &nbsp; and &ensp; from the elements text
                temp = element.get_text().strip()
                text = temp.replace('\xc2\xa0', '').replace('\u2002', '').replace('\n', '').replace('\r            ', '').strip()
                text = re.sub(r'\s+', ' ', text)
                # Vaquill: undo latin-1/utf-8 mojibake (curly quotes, em
                # dashes, etc.) that delcode.delaware.gov triggers via
                # ambiguous Content-Type.
                text = _fix_encoding(text)

                if text == "":
                    continue

                if element.name == "p":
                    node_text.add_paragraph(text=text)
                    continue

                # Assume any left over text without a <p> tag is the addendum
                addendum.history.text += text

                if element.name == "a":
                    addendum_references.references[element['href']] = Reference(text=text)

            if addendum_references.references == {}:
                addendum_references = None
            addendum.history.reference_hub = addendum_references
        if addendum and addendum.history.text == "":
            addendum = None

        section_node = Node(
            id=node_id,
            link=link,
            citation=citation,
            top_level_title=node_parent.top_level_title,
            node_type=node_type,
            level_classifier=level_classifier,
            number=number,
            node_name=node_name,
            parent=node_parent.node_id,
            status=status,
            node_text=node_text,
            addendum=addendum
        )

        insert_node(section_node, TABLE_NAME, debug_mode=True)

if __name__ == "__main__":
     main()
