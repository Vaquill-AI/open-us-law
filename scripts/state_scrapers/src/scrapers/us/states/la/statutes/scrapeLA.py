"""Louisiana statutes scraper.

Source: codes.findlaw.com/la  (FindLaw mirror, direct HTTP, no Selenium needed)

Louisiana has multiple codes, all enumerated here:
    - Revised Statutes (La. R.S.)
    - Civil Code (La. Civ. Code)
    - Code of Civil Procedure (La. Code Civ. Proc.)
    - Code of Criminal Procedure (La. Code Crim. Proc.)
    - Code of Evidence (La. Code Evid.)
    - Children's Code (La. Ch. Code)

Navigation strategy: each section page has a "Next Part of Code" link that
chains through all sections in reading order across every code. We start from
the known first section of each code and walk forward until the "back to
chapter list" link leaves that code's URL space.

Citation formats:
    Revised Statutes       -> "La. R.S. § <TITLE>:<SECTION>"   e.g. La. R.S. § 1:1
    Civil Code             -> "La. Civ. Code art. <N>"          e.g. La. Civ. Code art. 1
    Code of Civil Proc.    -> "La. Code Civ. Proc. art. <N>"    e.g. La. Code Civ. Proc. art. 1
    Code of Criminal Proc. -> "La. Code Crim. Proc. art. <N>"
    Code of Evidence       -> "La. Code Evid. art. <N>"
    Children's Code        -> "La. Ch. Code art. <N>"

Node ID hierarchy:
    us/la/statutes/code=<slug>/title=<N>/section=<N>   (Revised Statutes)
    us/la/statutes/code=<slug>/article=<N>             (flat article codes)
    us/la/statutes/code=<slug>/title=<roman>/article=<N> (titled article codes)
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Optional

from bs4 import BeautifulSoup

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
JURISDICTION = "la"
CORPUS = "statutes"
TABLE_NAME = f"{COUNTRY}_{JURISDICTION}_{CORPUS}"

BASE_FINDLAW = "https://codes.findlaw.com/la"

RESERVED_KEYWORDS = [
    "repealed",
    "reserved",
    "expired",
    "renumbered",
    "blank",
    "vacant",
]

# ---------------------------------------------------------------------------
# Code registry: each entry is (code_slug, human_name, first_section_url_slug,
# citation_prefix, citation_style)
#
# citation_style:
#   "rs"      -> "La. R.S. § <title>:<section>"
#   "article" -> "<citation_prefix> art. <N>"
# ---------------------------------------------------------------------------
CODES: list[tuple[str, str, str, str, str]] = [
    (
        "revised-statutes",
        "Revised Statutes",
        "la-rev-stat-tit-1-sect-1",
        "La. R.S.",
        "rs",
    ),
    (
        "civil-code",
        "Civil Code",
        "la-civ-code-art-1",
        "La. Civ. Code",
        "article",
    ),
    (
        "code-of-civil-procedure",
        "Code of Civil Procedure",
        "la-code-civ-proc-tit-i-art-1",
        "La. Code Civ. Proc.",
        "article",
    ),
    (
        "code-of-criminal-procedure",
        "Code of Criminal Procedure",
        "la-code-crim-proc-tit-i-art-1",
        "La. Code Crim. Proc.",
        "article",
    ),
    (
        "code-of-evidence",
        "Code of Evidence",
        "la-code-evid-art-101",
        "La. Code Evid.",
        "article",
    ),
    (
        "childrens-code",
        "Children's Code",
        "la-ch-code-tit-i-art-100",
        "La. Ch. Code",
        "article",
    ),
]


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def main() -> None:
    corpus_node: Node = insert_jurisdiction_and_corpus_node(COUNTRY, JURISDICTION, CORPUS)
    for code_slug, code_name, first_slug, citation_prefix, citation_style in CODES:
        code_node = _insert_code_node(corpus_node, code_slug, code_name)
        first_url = f"{BASE_FINDLAW}/{code_slug}/{first_slug}/"
        _scrape_code(code_node, code_slug, first_url, citation_prefix, citation_style)


# ---------------------------------------------------------------------------
# Code-level node
# ---------------------------------------------------------------------------

def _insert_code_node(corpus_node: Node, code_slug: str, code_name: str) -> Node:
    node_id = f"{corpus_node.node_id}/code={code_slug}"
    code_node = Node(
        id=node_id,
        link=f"{BASE_FINDLAW}/{code_slug}/",
        top_level_title=code_slug,
        node_type="structure",
        level_classifier="code",
        number=code_slug,
        node_name=code_name,
        parent=corpus_node.node_id,
    )
    insert_node(code_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)
    return code_node


# ---------------------------------------------------------------------------
# Walk all sections of one code via Next link chain
# ---------------------------------------------------------------------------

def _scrape_code(
    code_node: Node,
    code_slug: str,
    start_url: str,
    citation_prefix: str,
    citation_style: str,
) -> None:
    url: Optional[str] = start_url
    seen: set[str] = set()

    # Track structure nodes already inserted so we avoid duplicate DB writes.
    inserted_structure: set[str] = set()

    while url and url not in seen:
        seen.add(url)
        try:
            soup = get_url_as_soup(url)
        except Exception as exc:
            print(f"[LA] fetch error {url}: {exc}", flush=True)
            break

        main_div = soup.find("main")
        if main_div is None:
            break

        # Check "Back to Chapter List" to confirm we're still in this code.
        back_link = main_div.find("a", title="Back to Chapter List")
        if back_link:
            back_href = back_link.get("href", "")
            if f"/la/{code_slug}/" not in back_href:
                # Navigated into a different code; stop.
                break

        h1_tag = main_div.find("h1")
        if h1_tag is None:
            url = _next_url(main_div, code_slug)
            continue

        h1_text = _clean_text(h1_tag.get_text())

        # Check reserved / repealed status.
        status = _check_reserved(h1_text)

        # Parse the URL slug to extract structural information.
        slug = _url_to_slug(url)
        parsed = _parse_slug(slug, code_slug)
        if parsed is None:
            url = _next_url(main_div, code_slug)
            continue

        # Ensure parent structure nodes exist.
        section_parent = _ensure_structure_nodes(
            code_node, parsed, inserted_structure
        )

        # Build the section node.
        section_node = _build_section_node(
            parent_node=section_parent,
            code_node=code_node,
            parsed=parsed,
            h1_text=h1_text,
            url=url,
            citation_prefix=citation_prefix,
            citation_style=citation_style,
            status=status,
            soup=main_div if not status else None,
        )
        if section_node is not None:
            insert_node(section_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)

        url = _next_url(main_div, code_slug)


# ---------------------------------------------------------------------------
# Structure node management
# ---------------------------------------------------------------------------

def _ensure_structure_nodes(
    code_node: Node,
    parsed: dict,
    inserted: set[str],
) -> Node:
    """Insert title/chapter structure nodes if needed and return the direct parent."""
    parent = code_node

    # Revised Statutes: title level.
    title_key = parsed.get("title")
    if title_key:
        node_id = f"{code_node.node_id}/title={title_key}"
        if node_id not in inserted:
            title_node = Node(
                id=node_id,
                link=f"{BASE_FINDLAW}/{code_node.number}/",
                top_level_title=title_key,
                node_type="structure",
                level_classifier="title",
                number=title_key,
                node_name=f"Title {title_key}",
                parent=code_node.node_id,
            )
            insert_node(title_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)
            inserted.add(node_id)
        parent = Node(
            id=node_id,
            link=f"{BASE_FINDLAW}/{code_node.number}/",
            top_level_title=title_key,
            node_type="structure",
            level_classifier="title",
            number=title_key,
            node_name=f"Title {title_key}",
            parent=code_node.node_id,
        )

    return parent


# ---------------------------------------------------------------------------
# Section node builder
# ---------------------------------------------------------------------------

def _build_section_node(
    parent_node: Node,
    code_node: Node,
    parsed: dict,
    h1_text: str,
    url: str,
    citation_prefix: str,
    citation_style: str,
    status: Optional[str],
    soup: Optional[BeautifulSoup],
) -> Optional[Node]:
    art_or_sect = parsed.get("art") or parsed.get("sect")
    if art_or_sect is None:
        return None

    title_num = parsed.get("title")

    # Build citation.
    if citation_style == "rs" and title_num:
        citation = f"La. R.S. § {title_num}:{art_or_sect}"
    else:
        citation = f"{citation_prefix} art. {art_or_sect}"

    # Build node_id.
    node_id = f"{parent_node.node_id}/section={art_or_sect}"

    # Node name is the H1 text stripped of the jurisdictional prefix.
    node_name = _strip_prefix(h1_text)

    node_text = None
    addendum = None
    if not status and soup is not None:
        node_text, addendum = _fetch_section_content(soup)

    return Node(
        id=node_id,
        link=url,
        top_level_title=title_num or code_node.number,
        node_type="content",
        level_classifier="section",
        number=art_or_sect,
        node_name=node_name,
        parent=parent_node.node_id,
        citation=citation,
        status=status,
        node_text=node_text,
        addendum=addendum,
    )


# ---------------------------------------------------------------------------
# Content extraction
# ---------------------------------------------------------------------------

def _fetch_section_content(main_div: BeautifulSoup):
    """Extract substantive paragraphs from an already-fetched main div."""
    content_div = main_div.find("div", class_="codes-content")
    if content_div is None:
        return None, None

    node_text = NodeText()
    history_parts: list[str] = []

    for elem in content_div.find_all(["p", "li"], recursive=True):
        # Skip navigation/cite-this controls inside codes-controls or cite-this-article.
        if elem.find_parent(class_=["codes-controls", "cite-this-article"]):
            continue

        raw = elem.get_text(separator=" ")
        text = _clean_text(raw)
        if not text:
            continue
        if _looks_like_history(text):
            history_parts.append(text)
        else:
            node_text.add_paragraph(text=text)

    addendum = None
    if history_parts:
        addendum = Addendum()
        addendum.history = AddendumType(
            type="history", text=" ".join(history_parts)
        )

    if not node_text.paragraphs:
        node_text = None

    return node_text, addendum


# ---------------------------------------------------------------------------
# URL / slug helpers
# ---------------------------------------------------------------------------

def _url_to_slug(url: str) -> str:
    """Extract the section slug from a FindLaw URL."""
    return url.rstrip("/").split("/")[-1]


def _parse_slug(slug: str, code_slug: str) -> Optional[dict]:
    """
    Parse a FindLaw section slug into its structural components.

    Returns a dict with keys: title (optional), art (optional), sect (optional).

    Revised Statutes patterns:
        la-rev-stat-tit-1-sect-1          -> title=1, sect=1
        la-rev-stat-tit-14-sect-95-1      -> title=14, sect=95-1

    Civil Code patterns (flat preliminary title):
        la-civ-code-art-1                 -> art=1
        la-civ-code-art-15                -> art=15

    Civil Code patterns (with title):
        la-civ-code-tit-i-art-24          -> title=i, art=24
        la-civ-code-tit-viii-art-3549     -> title=viii, art=3549

    CCP / CrCP patterns:
        la-code-civ-proc-tit-i-art-1      -> title=i, art=1
        la-code-crim-proc-tit-i-art-1     -> title=i, art=1
        la-code-crim-proc-tit-xxxv-art-1005 -> title=xxxv, art=1005

    Evidence / Children's Code patterns:
        la-code-evid-art-101              -> art=101
        la-ch-code-tit-i-art-100          -> title=i, art=100
    """
    # --- Revised Statutes ---
    m = re.match(r"la-rev-stat-tit-(\d+)-sect-([\w-]+)$", slug)
    if m:
        return {"title": m.group(1), "sect": m.group(2)}

    # --- Civil Code flat (preliminary title): la-civ-code-art-N ---
    m = re.match(r"la-civ-code-art-(\d+)$", slug)
    if m:
        return {"art": m.group(1)}

    # --- Civil Code with title: la-civ-code-tit-<roman>-art-N ---
    m = re.match(r"la-civ-code-tit-([\w-]+)-art-(\d+)$", slug)
    if m:
        return {"title": m.group(1), "art": m.group(2)}

    # --- CCP: la-code-civ-proc-tit-<roman>-art-N ---
    m = re.match(r"la-code-civ-proc-tit-([\w-]+)-art-([\w-]+)$", slug)
    if m:
        return {"title": m.group(1), "art": m.group(2)}

    # --- CrCP: la-code-crim-proc-tit-<roman>-art-N ---
    m = re.match(r"la-code-crim-proc-tit-([\w-]+)-art-([\w-]+)$", slug)
    if m:
        return {"title": m.group(1), "art": m.group(2)}

    # --- Code of Evidence flat: la-code-evid-art-N ---
    m = re.match(r"la-code-evid-art-([\w-]+)$", slug)
    if m:
        return {"art": m.group(1)}

    # --- Children's Code: la-ch-code-tit-<roman>-art-N ---
    m = re.match(r"la-ch-code-tit-([\w-]+)-art-([\w-]+)$", slug)
    if m:
        return {"title": m.group(1), "art": m.group(2)}

    # --- Children's Code flat: la-ch-code-art-N ---
    m = re.match(r"la-ch-code-art-([\w-]+)$", slug)
    if m:
        return {"art": m.group(1)}

    return None


def _next_url(main_div: BeautifulSoup, code_slug: str) -> Optional[str]:
    """Return next section URL if it stays within the same code, else None."""
    next_tag = main_div.find("a", title="Next Part of Code")
    if next_tag is None:
        return None
    href = next_tag.get("href", "")
    if f"/la/{code_slug}/" in href:
        return href
    return None


# ---------------------------------------------------------------------------
# Text helpers
# ---------------------------------------------------------------------------

def _strip_prefix(h1_text: str) -> str:
    """
    Remove the 'Louisiana <Code Name> <structural prefix>' from the H1.

    Examples:
        'Louisiana Revised Statutes Tit. 1, § 1. Revised Statutes; how cited'
          -> '§ 1. Revised Statutes; how cited'
        'Louisiana Civil Code Art. 1. Sources of law'
          -> 'Art. 1. Sources of law'
        'Louisiana Code of Evidence Art. 101. Scope'
          -> 'Art. 101. Scope'
    """
    # Strip leading 'Louisiana <...> ' up to the first 'Art.' / 'Tit.' / '§'
    m = re.search(r"((?:Art\.|Tit\.|§)\s*.+)$", h1_text)
    if m:
        return m.group(1).strip()
    return h1_text.strip()


def _looks_like_history(text: str) -> bool:
    """Return True for amendment history lines."""
    s = text.strip()
    if re.match(r"^\(\s*(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\.?\s+\d", s):
        return True
    if re.match(r"^\d{4}\s+(Amendment|Act|P\.L\.|No\.|La\.)", s):
        return True
    if s.startswith("Added by Acts"):
        return True
    if s.startswith("Acts ") and re.search(r"\d{4}", s):
        return True
    if s.startswith("Amended by"):
        return True
    return False


def _check_reserved(text: str) -> Optional[str]:
    lower = text.lower()
    for kw in RESERVED_KEYWORDS:
        if kw in lower:
            return "reserved"
    return None


def _clean_text(raw: str) -> str:
    text = raw.replace("\xa0", " ").replace(" ", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


if __name__ == "__main__":
    main()
