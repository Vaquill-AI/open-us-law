


import os
import sys
from urllib.parse import urljoin
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
JURISDICTION = "ca"
# 'statutes' is current default
CORPUS = "statutes"
# No need to change this
TABLE_NAME =  f"{COUNTRY}_{JURISDICTION}_{CORPUS}"
BASE_URL = 'https://leginfo.legislature.ca.gov/'
TOC_URL = "https://leginfo.legislature.ca.gov/faces/codes.xhtml"
SKIP_TITLE = 0

all_codes: List[Tuple[str, str]] = [("BPC","https://leginfo.legislature.ca.gov/faces/codedisplayexpand.xhtml?tocCode=BPC" ),("CIV","https://leginfo.legislature.ca.gov/faces/codedisplayexpand.xhtml?tocCode=CIV" ),("CCP","https://leginfo.legislature.ca.gov/faces/codedisplayexpand.xhtml?tocCode=CCP" ),("COM","https://leginfo.legislature.ca.gov/faces/codedisplayexpand.xhtml?tocCode=COM" ),("CORP","https://leginfo.legislature.ca.gov/faces/codedisplayexpand.xhtml?tocCode=CORP" ),("EDC","https://leginfo.legislature.ca.gov/faces/codedisplayexpand.xhtml?tocCode=EDC" ),("ELEC","https://leginfo.legislature.ca.gov/faces/codedisplayexpand.xhtml?tocCode=ELEC" ),("EVID","https://leginfo.legislature.ca.gov/faces/codedisplayexpand.xhtml?tocCode=EVID" ),("FAM","https://leginfo.legislature.ca.gov/faces/codedisplayexpand.xhtml?tocCode=FAM" ),("FIN","https://leginfo.legislature.ca.gov/faces/codedisplayexpand.xhtml?tocCode=FIN" ),("FGC","https://leginfo.legislature.ca.gov/faces/codedisplayexpand.xhtml?tocCode=FGC" ),("FAC","https://leginfo.legislature.ca.gov/faces/codedisplayexpand.xhtml?tocCode=FAC" ),("GOV","https://leginfo.legislature.ca.gov/faces/codedisplayexpand.xhtml?tocCode=GOV" ),("HNC","https://leginfo.legislature.ca.gov/faces/codedisplayexpand.xhtml?tocCode=HNC" ),("HSC","https://leginfo.legislature.ca.gov/faces/codedisplayexpand.xhtml?tocCode=HSC" ),("INS","https://leginfo.legislature.ca.gov/faces/codedisplayexpand.xhtml?tocCode=INS" ),("LAB","https://leginfo.legislature.ca.gov/faces/codedisplayexpand.xhtml?tocCode=LAB" ),("MVC","https://leginfo.legislature.ca.gov/faces/codedisplayexpand.xhtml?tocCode=MVC" ),("PEN","https://leginfo.legislature.ca.gov/faces/codedisplayexpand.xhtml?tocCode=PEN" ),("PROB","https://leginfo.legislature.ca.gov/faces/codedisplayexpand.xhtml?tocCode=PROB" ),("PCC","https://leginfo.legislature.ca.gov/faces/codedisplayexpand.xhtml?tocCode=PCC" ),("PRC","https://leginfo.legislature.ca.gov/faces/codedisplayexpand.xhtml?tocCode=PRC" ),("PUC","https://leginfo.legislature.ca.gov/faces/codedisplayexpand.xhtml?tocCode=PUC" ),("RTC","https://leginfo.legislature.ca.gov/faces/codedisplayexpand.xhtml?tocCode=RTC" ),("SHC","https://leginfo.legislature.ca.gov/faces/codedisplayexpand.xhtml?tocCode=SHC" ),("UIC","https://leginfo.legislature.ca.gov/faces/codedisplayexpand.xhtml?tocCode=UIC" ),("VEH","https://leginfo.legislature.ca.gov/faces/codedisplayexpand.xhtml?tocCode=VEH" ),("WAT","https://leginfo.legislature.ca.gov/faces/codedisplayexpand.xhtml?tocCode=WAT" ),("WIC","https://leginfo.legislature.ca.gov/faces/codedisplayexpand.xhtml?tocCode=WIC" )]
ALLOWED_LEVELS = ["code", "division", "part", "title", "chapter", "article", "section"]
RESERVED_KEYWORDS = ["(reserved)", "[reserved]"]

def main():
    corpus_node: Node = insert_jurisdiction_and_corpus_node(COUNTRY, JURISDICTION, CORPUS)
    scrape(corpus_node)


# Schemas. CA reorders its hierarchy per-code; these orderings encode that.
DIVISION_CASE = ["division", "part", "title", "chapter", "article", "appendix"]
PART_CASE = ["part", "title", "division", "chapter", "article", "appendix"]
TITLE_CASE = ["title", "division", "part", "chapter", "article", "appendix"]
SHORT_CASE = ["division", "chapter", "article", "appendix"]
REGULAR_CASE = ["division", "part", "chapter", "article", "appendix"]
BASE = ["ca", "statutes", "code"]

# Explicit per-code schema dispatcher. Default for any code not listed is REGULAR_CASE.
# This covers all 29 California codes; previous if/elif chain only special-cased 11
# and silently routed the other 18 through REGULAR_CASE (correct for most, but the
# routing was implicit). Keep this dict authoritative.
CODE_SCHEMA: dict = {
    # DIVISION ordering
    "WAT": DIVISION_CASE,
    "CIV": DIVISION_CASE,
    # PART ordering
    "CCP": PART_CASE,
    "PEN": PART_CASE,
    # TITLE ordering
    "GOV": TITLE_CASE,
    "CORP": TITLE_CASE,
    "EDC": TITLE_CASE,
    # SHORT ordering
    "VEH": SHORT_CASE,
    "COM": SHORT_CASE,
    "FIN": SHORT_CASE,
    "EVID": SHORT_CASE,
}


def _titles_done_path():
    """Where we persist the set of codes already fully scraped, for resume."""
    from vaquill_pipeline.config import SETTINGS
    return SETTINGS.chunks_dir / "state_ca_titles_done.txt"


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


# Current
# class=treecodetitle
def scrape(corpus_node: Node):
    """Walk all 29 California codes in parallel.

    Each code is independent. ThreadPoolExecutor concurrency is set via
    ``VAQUILL_TITLE_WORKERS`` (default 8). Resume: codes previously completed
    are persisted in ``state_ca_titles_done.txt`` and skipped on re-runs.
    Set ``VAQUILL_FORCE_RESCRAPE=1`` to override.
    """
    import os
    from concurrent.futures import ThreadPoolExecutor, as_completed

    codes_done = set() if os.environ.get("VAQUILL_FORCE_RESCRAPE") else _load_titles_done()
    if codes_done:
        print(f"[scrapeCA] resume: {len(codes_done)} codes already done: {sorted(codes_done)}", flush=True)

    work = []
    for code_tup in all_codes:
        code: str = code_tup[0]
        url = code_tup[1]

        title_schema = BASE + CODE_SCHEMA.get(code, REGULAR_CASE)

        code_node = Node(
            id=f"{corpus_node.node_id}/code={code.lower()}",
            link=url,
            citation=f"Cal. {code.lower()}",
            parent=corpus_node.node_id,
            number=code.lower(),
            top_level_title=code.lower(),
            node_type="structure",
            level_classifier="code"
        )
        # Insert the code structure node up front (cheap, idempotent).
        insert_node(code_node, TABLE_NAME, ignore_duplicate=True, debug_mode=False)
        if code in codes_done:
            continue
        work.append((code, url, title_schema, code_node))

    def _do_code(item):
        code, url, title_schema, code_node = item
        try:
            print(f"[scrapeCA] starting code: {code}", flush=True)
            scrape_structure_nodes(url, title_schema, code_node)
            _mark_title_done(code)
            return (code, "ok", None)
        except Exception as e:
            return (code, "fail", str(e)[:200])

    workers = int(os.environ.get("VAQUILL_TITLE_WORKERS", "8"))
    print(f"[scrapeCA] running {len(work)} codes with {workers} parallel workers", flush=True)
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for fut in as_completed(ex.submit(_do_code, item) for item in work):
            code, status, err = fut.result()
            if status == "fail":
                print(f"[scrapeCA] code {code}: {status}: {err}", flush=True)
            else:
                print(f"[scrapeCA] code {code}: {status}", flush=True)

# For every root node, scrape all structure nodes, getting a list of all valid HTML div elements which have content node children
def scrape_structure_nodes(url: str, title_schema: List[str], node_parent: Node):
    """
    For every top_level_title node (level_classifier=code), scrape all structure nodes, getting a list of all valid HTML div elements.
    """
    soup = get_url_as_soup(url=url)

    # Find the HTML container for actual content
    codesbrchfrm = soup.find(id="codesbrchfrm")
    if (not codesbrchfrm):
        print("Couldn't find codesbrchform!")
        exit(1)

    branchChildren = codesbrchfrm.find_all(recursive=False)

    container = branchChildren[4].contents[0]

    if (not container):
        print("code container undefined!")
        exit(1)

    div_elements = []
    # vFind all div_elements in container
    for i, current_element in enumerate(container.find_all(recursive=False)):

        # Format Div
        if (i % 2 == 1):
            continue

        div_elements.append(current_element)


    scrape_per_code(div_elements, title_schema, node_parent)
    return node_parent


def scrape_per_code(structure_divs, title_schema: List[str], node_parent: Node):
    """
    For every code, process a list of HTML divs corresponding to structure nodes. Using title_schema, find each nodes correct parent, and add each node. If a div indicates a link to find children which are content_nodes, scrape those content nodes
    """
    current_node = node_parent
    # Find the container

    for i, div in enumerate(structure_divs):
        # Skip useless style divs
        new_partial_node: Node = get_structure_node_attributes(div, current_node)

        # Check node is not reserved
        if new_partial_node.status:

            new_partial_node.id = new_partial_node.id.add_level(new_partial_node.level_classifier, new_partial_node.number)
            print(f"Node status is none: {new_partial_node.node_id}")
            new_partial_node.parent = current_node.node_id

            insert_node(new_partial_node, TABLE_NAME, debug_mode=True)
            return

        # Not a valid level for this code
        if (new_partial_node.level_classifier not in title_schema):
            print(f"Not a valid level for this code: {new_partial_node.level_classifier}")
            # Assume node_parent is correct parent
            new_partial_node.id = new_partial_node.id.add_level(new_partial_node.level_classifier, new_partial_node.number)
            new_partial_node.parent = current_node.node_id

            insert_node(new_partial_node, TABLE_NAME, debug_mode=True)

        # Is a valid level or SUB variation of valid level
        if (new_partial_node.level_classifier  in title_schema):
            # Find the rank of the current and new node's level classifiers in title_schema
            currentRank = title_schema.index(current_node.level_classifier)
            newRank = title_schema.index(new_partial_node.level_classifier)

            # Determine the position of the newNode in the hierarchy
            if (newRank <= currentRank):
                temp_node_id = current_node.id

                # Find the correct parent of the new node
                while (title_schema.index(temp_node_id.current_level[0]) > newRank):
                    temp_node_id = temp_node_id.pop_level()
                    if (temp_node_id.current_level[0] == 'code'):
                        break


                current_node = temp_node_id
                currentRank = title_schema.index(current_node.current_level[0])
                # Set the ID of the newNode and add it to the current_node's children or siblings
                if (newRank == currentRank):
                    new_partial_node.id = NodeID(raw_id=f"{current_node.pop_level().raw_id}/{new_partial_node.level_classifier}={new_partial_node.number}")
                    new_partial_node.parent = current_node.pop_level().raw_id
                    current_node = new_partial_node
                else:
                    new_partial_node.id = temp_node_id.add_level(new_partial_node.level_classifier, new_partial_node.number)
                    new_partial_node.parent = current_node.raw_id
                    current_node = new_partial_node

            else:
                # Set the ID of the newNode and add it to the current_node's children
                new_partial_node.id = new_partial_node.id.add_level(new_partial_node.level_classifier, new_partial_node.number)
                new_partial_node.parent = current_node.node_id
                current_node = new_partial_node

            insert_node(current_node, TABLE_NAME, debug_mode=True)

        # New structure contains content node children, denoted by link value
        if ("codes_displayText" in str(new_partial_node.link)):
            scrape_content_node(new_partial_node)







def get_structure_node_attributes(current_element, node_parent: Node) -> Node:
    """
    For a structure node, initially populate the attributes into a Pydantic model. The true correct node_id and parent are uncertain, as the correct hierarchy has to be determined later.

    """
    # Find the "a" tag inside the current_element
    #print(current_element.name)
    a_tag = current_element.find("a")

    # Ensure the a_tag is found
    if (not a_tag):
        print("No 'a' tag found in the current element.")
        exit(1)
    # Extract href value
    # Use urljoin to avoid double slashes when href is absolute path (e.g. "/faces/...")
    href = a_tag["href"]
    if isinstance(href, list):
        href = href[0] if href else ""
    link = urljoin(BASE_URL, str(href).lstrip("/"))
    if (link is None):
        print("Invalid link!")
        exit(1)

    # Assume the first child element of a_tag contains the title and name
    node_name: str = a_tag.contents[0].get_text().strip()
    if (not node_name):
        print("No first child element with text found in the 'a' tag.")
        exit(1)

    # Check that the current node is not reserved, tag in status if it is
    status=None
    for word in RESERVED_KEYWORDS:
        if word in node_name:
            status="reserved"
            break

    # Handle cases where no level is specified, assume appendix
    level_classifier = node_name.split(" ")[0].lower()
    if level_classifier not in ALLOWED_LEVELS:
        level_classifier = "appendix"
        # Example: node_name = "GENERAL PROVISIONS"
        # number should be "GENERAL PROVISIONS", same as node_name
        number=node_name
    else:
        number = node_name.split(" ")[1]

    # Remove unneccesary punctuation
    if(number[-1] == "."):
        number = number[:-1]

    node_type = "structure"


    # Create a temporary node_id. This will be changed when the correct parent is found
    node_id = f"{node_parent.node_id}"
    citation = f"Cal. {node_parent.top_level_title.upper()} {level_classifier.capitalize()} {number}"

    partial_node_data = Node(
        id=node_id,
        link=link,
        citation=citation,
        number=number,
        parent=node_parent.node_id,
        top_level_title=node_parent.top_level_title,
        node_type=node_type,
        level_classifier=level_classifier,
        status=status,
        node_name=node_name
    )

    return partial_node_data



def scrape_content_node(node_parent: Node):
    """
    Scrape all individual sections.
    """

    soup = get_url_as_soup(url=node_parent.link)

    container = soup.find(id="manylawsections")
    if (not container):
        print("manylawsections container not found!")
        return

    divCon = container.contents[-1]
    if (not divCon):
        print("divCon element cannot be found!")
        exit(1)


    fontContainer = divCon.contents[0]
    if (not fontContainer):
        print("font element cannot be found!")
        exit(1)

    section_divs = fontContainer.find_all("div", recursive=False)

    # https://leginfo.legislature.ca.gov/faces/codes_displaySection.xhtml?lawCode=BPC&sectionNum=Section 115.
    # https://leginfo.legislature.ca.gov/faces/codes_displaySection.xhtml?lawCode=BPC&sectionNum=115.
    # https://leginfo.legislature.ca.gov/faces/codes_displaySection.xhtml?lawCode=HSC&sectionNum=1358.1.

    for i, div in enumerate(section_divs):
        if i == 0:
            continue
        #print(div)
        # find first child div element and extract the .textContent
        try:
            section_container = div.find("a")
            number = section_container.get_text().strip()

            link = f"https://leginfo.legislature.ca.gov/faces/codes_displaySection.xhtml?lawCode={node_parent.top_level_title.upper()}&sectionNum={number}"

            if number[-1] == ".":
                number = number[:-1]



            citation = f"Cal. {node_parent.top_level_title.upper()} § {number}"
            level_classifier = "section"
            node_id = f"{node_parent.node_id}/{level_classifier}={number}"




            node_name = "Section " + number
            node_type = "content"
            node_text = NodeText()
        except:
            continue

        # -Inserting: us/ca/statutes/code=bpc/division=3/chapter=4/article=8/section=6140.55
#-Inserting: us/ca/statutes/code=bpc/division=3/chapter=4/article=8/section=6140.55
#Adding duplicate version number for: us/ca/statutes/code=bpc/division=3/chapter=4/article=8/section=6140.5-v_2


        for i, p_tag in enumerate(div.find_all(recursive=True)):
            if i <= 1 or p_tag.name == "i":
                continue

            text_to_add = p_tag.get_text().strip()
            if(text_to_add == ""):
                continue

            node_text.add_paragraph(text=text_to_add)

        node_addendum_text = node_text.pop().text
        addendum = Addendum(history=AddendumType(text=node_addendum_text))
        section_node = Node(
            id=node_id,
            link=link,
            citation=citation,
            top_level_title=node_parent.top_level_title,
            node_type=node_type,
            level_classifier=level_classifier,
            node_name=node_name,
            number=number,
            node_text=node_text,
            addendum=addendum,
            parent=node_parent.node_id
        )

        insert_node(section_node, TABLE_NAME, debug_mode=True)

    return

if __name__ == "__main__":
    main()
