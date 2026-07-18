
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



COUNTRY = "us"
# State code for states, 'federal' otherwise
JURISDICTION = "az"
# 'statutes' is current default
CORPUS = "statutes"
# No need to change this
TABLE_NAME =  f"{COUNTRY}_{JURISDICTION}_{CORPUS}"
BASE_URL = "https://www.azleg.gov"
TOC_URL = "https://www.azleg.gov/arstitle/"


def _titles_done_path():
    """Where we persist the set of titles already fully scraped for resume."""
    from vaquill_pipeline.config import SETTINGS
    return SETTINGS.chunks_dir / "state_az_titles_done.txt"


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


def _discover_title_urls() -> List[str]:
    """Pull the live ARS Table of Contents and return every arsDetail link.

    Arizona Revised Statutes nominally numbers 1..49, but several titles
    (e.g. 2, 24) are reserved/absent. We trust the live TOC rather than a
    stale committed JSON snapshot. The old top_level_title_links.json only
    held 47 entries AND scrapeAZ.py skipped the first 38 of them via
    SKIP_TITLE = 38, meaning 81 percent of the corpus never ingested.
    """
    soup = get_url_as_soup(TOC_URL)
    urls: List[str] = []
    seen: set = set()
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if "arsDetail?title=" not in href:
            continue
        if href.startswith("/"):
            href = BASE_URL + href
        elif not href.startswith("http"):
            href = f"{BASE_URL}/{href}"
        if href in seen:
            continue
        seen.add(href)
        urls.append(href)
    # Sort by numeric title for deterministic ordering / readable logs.
    def _key(u: str) -> int:
        try:
            return int(u.rsplit("=", 1)[1])
        except Exception:
            return 9999
    urls.sort(key=_key)
    return urls


def main():
    import os
    from concurrent.futures import ThreadPoolExecutor, as_completed

    corpus_node = insert_jurisdiction_and_corpus_node(COUNTRY, JURISDICTION, CORPUS)

    all_titles = _discover_title_urls()
    print(f"[scrapeAZ] discovered {len(all_titles)} titles from live TOC", flush=True)

    titles_done = set() if os.environ.get("VAQUILL_FORCE_RESCRAPE") else _load_titles_done()
    if titles_done:
        print(
            f"[scrapeAZ] resume: {len(titles_done)} titles already done: "
            f"{sorted(titles_done, key=lambda x: int(x) if x.isdigit() else 999)}",
            flush=True,
        )

    # Build pending work list (skip titles already marked done).
    work: List[Tuple[str, str]] = []
    for title_url in all_titles:
        try:
            number = title_url.rsplit("=", 1)[1]
        except Exception:
            number = title_url
        if number in titles_done:
            continue
        work.append((title_url, number))

    def _do_title(item):
        title_url, number = item
        try:
            scrape_per_title(corpus_node, title_url)
            _mark_title_done(number)
            return (number, "ok", None)
        except Exception as e:
            return (number, "fail", str(e)[:200])

    workers = int(os.environ.get("VAQUILL_TITLE_WORKERS", "8"))
    print(f"[scrapeAZ] running {len(work)} titles with {workers} parallel workers", flush=True)
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for fut in as_completed(ex.submit(_do_title, item) for item in work):
            num, status, err = fut.result()
            if status == "fail":
                print(f"[scrapeAZ] title {num}: {status}: {err}", flush=True)
            else:
                print(f"[scrapeAZ] title {num}: {status}", flush=True)


def scrape_per_title(corpus_node: Node, url: str):
    """
    TODO: Make this
    """

    soup: BeautifulSoup = get_url_as_soup(url)
    #print(soup.prettify())

    title_container = soup.find(class_="topTitle")
    title_name = title_container.get_text().strip()
    number = title_name.split(" ")[1]
    top_level_title = number
    parent = corpus_node.node_id
    level_classifier = "title"
    node_type = "structure"

    title_node_id = f"{parent}/{level_classifier}={top_level_title}"


    title_node = Node(
        id=title_node_id,
        link=url,
        top_level_title=top_level_title,
        node_type=node_type,
        level_classifier=level_classifier,
        node_name=title_name,
        parent=parent,
        number=number
    )

    insert_node(title_node, TABLE_NAME, True, True)

    chapter_container = title_container.parent.parent.parent

    for i, chapter in enumerate(chapter_container.find_all(class_="accordion", recursive=False)):

        header = chapter.find("h5")
        link = header.find("a")
        node_name_start = link.get_text().strip()
        node_number = node_name_start.split(" ")[1]
        node_link = url + "#" + chapter['id']
        node_name_end = link.next_sibling.get_text().strip()
        node_name = f"{node_name_start} {node_name_end}"

        level_classifier = "chapter"
        node_type = "structure"

        chapter_node_id = f"{title_node_id}/{level_classifier}={node_number}"
        chapter_node = Node(
            id=chapter_node_id,
            link=node_link,
            top_level_title=top_level_title,
            node_type=node_type,
            level_classifier=level_classifier,
            node_name=node_name,
            parent=title_node_id,
            number=node_number
        )

        insert_node(chapter_node, TABLE_NAME, debug_mode=True)


        article_container = header.next_sibling
        for i, article in enumerate(article_container.find_all(class_="article")):
            elements = article.find_all(recursive=False)
            link_container = elements[0]
            try:
                node_name_start = link_container.get_text().strip()
                node_number = node_name_start.split(" ")[1]
                name_container = elements[1]
                node_name_end = name_container.get_text().strip()
                node_name = f"{node_name_start} {node_name_end}"

                level_classifier = "article"
                node_type = "structure"
                article_node_id = f"{chapter_node_id}/{level_classifier}={node_number}"
                ### INSERT STRUCTURE NODE, if it's already there, skip it

                article_node = Node(
                    id=article_node_id,
                    link=node_link,
                    top_level_title=top_level_title,
                    node_type=node_type,
                    level_classifier=level_classifier,
                    node_name=node_name,
                    parent=chapter_node_id,
                    number=node_number
                )
                insert_node(article_node, TABLE_NAME, debug_mode=True)

            # There is no article, goes straight from chapter to section
            except:
                article_node_id = chapter_node_id
                print("No article")

            section_container = elements[2]
            for i, section in enumerate(section_container.find_all(recursive=False)):

                link_container = section.find(class_="colleft").find("a")
                node_name_start = link_container.get_text().strip()
                if node_name_start == "":
                    continue
                node_number = node_name_start.split("-")[1]
                node_link = BASE_URL + link_container['href']

                name_container = section.find(class_="colright")
                node_name_end = name_container.get_text().strip()
                node_level_classifier = "section"
                node_type = "content"
                node_name = f"{node_name_start} {node_name_end}"
                node_id = f"{article_node_id}/{node_level_classifier}={node_number}"

                node_text = NodeText()
                node_addendum = None
                node_citation = f"A.R.S. § {top_level_title}-{node_number}"

                # Get the separate html page for each Section to scrape
                section_soup = get_url_as_soup(node_link)

                text_container = section_soup.find(class_="content-sidebar-wrap").find(class_="first")
                # For all flat <p> tags, add them to node_text
                for p in text_container.find_all("p"):
                    txt = p.get_text().strip()
                    # Do not add empty <p> tags as paragraphs
                    if txt != "":
                        node_text.add_paragraph(text=txt)

                section_node = Node(
                    id=node_id,
                    link=node_link,
                    citation=node_citation,
                    top_level_title=top_level_title,
                    node_type=node_type,
                    level_classifier=node_level_classifier,
                    number=node_number,
                    node_name=node_name,
                    node_text=node_text,
                    addendum=node_addendum,
                    parent=article_node_id
                )

                insert_node(section_node, TABLE_NAME, debug_mode=True)




if __name__ == "__main__":
    main()
