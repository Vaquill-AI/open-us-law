"""Utah Code scraper.

Source: https://le.utah.gov/xcode/

Site notes:
  - le.utah.gov TCP-times-out from non-US IPs. Run from a US host or use
    Webshare US residential proxy (configured in http_client.py).
  - Each wrapper .html page embeds an inline versionArr JS variable that names
    a versioned static content file relative to the wrapper page's directory
    (e.g. Title3/C3_1800010118000101.html). That content file carries the real
    #content div with #childtbl rows listing child links.
  - Section hrefs use a -S prefix: Title3/Chapter1/3-1-S1.html. The actual
    section citation number (e.g. 3-1-1) lives in the versioned content file's
    #secdiv > b:first-child text, not in the href filename.

URL patterns:
  TOC wrapper:       /xcode/code.html
  TOC content:       /xcode/C_VERSION.html             -> #content #childtbl
  Title wrapper:     /xcode/Title{T}/{T}.html
  Title content:     /xcode/Title{T}/C{T}_VERSION.html  -> #content #childtbl
  Chapter wrapper:   /xcode/Title{T}/Chapter{CH}/{T}-{CH}.html
  Chapter content:   /xcode/Title{T}/Chapter{CH}/C{T}-{CH}_VERSION.html
  Section wrapper:   /xcode/Title{T}/Chapter{CH}/{T}-{CH}-S{N}.html
  Section content:   /xcode/Title{T}/Chapter{CH}/C{T}-{CH}-S{N}_VERSION.html

Citation format: "Utah Code § SECTION" e.g. "Utah Code § 3-1-1"
Hierarchy: us/ut/statutes/title=T/chapter=CH/section=S
"""
from __future__ import annotations

import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Optional, Tuple
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
JURISDICTION = "ut"
CORPUS = "statutes"
TABLE_NAME = f"{COUNTRY}_{JURISDICTION}_{CORPUS}"

BASE_URL = "https://le.utah.gov/xcode"
TOC_WRAPPER_URL = f"{BASE_URL}/code.html"

RESERVED_KEYWORDS = ["Repealed", "Expired", "Reserved", "Renumbered"]


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    corpus_node: Node = insert_jurisdiction_and_corpus_node(COUNTRY, JURISDICTION, CORPUS)
    _scrape_toc(corpus_node)


# ---------------------------------------------------------------------------
# TOC level
# ---------------------------------------------------------------------------


def _titles_done_path():
    """Persist set of titles already fully scraped (resume support)."""
    try:
        from vaquill_pipeline.config import SETTINGS
        return SETTINGS.chunks_dir / "state_ut_titles_done.txt"
    except Exception:
        return Path(__file__).resolve().parent / "_state_ut_titles_done.txt"


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


def _scrape_toc(corpus_node: Node) -> None:
    """Fetch the code TOC wrapper, resolve versioned content, iterate titles in parallel."""
    content_soup = _load_content(TOC_WRAPPER_URL)
    if content_soup is None:
        return

    child_table = content_soup.find(id="childtbl")
    if child_table is None:
        return

    titles_done = set() if os.environ.get("VAQUILL_FORCE_RESCRAPE") else _load_titles_done()
    if titles_done:
        print(f"[UT] resume: {len(titles_done)} titles already done", flush=True)

    work: List[Tuple[Node, str]] = []
    for row in child_table.find_all("tr"):
        a = row.find("a", href=True)
        if a is None:
            continue

        # href: "Title3/3.html?v=C3_1800010118000101" -- strip query string
        href_path = a["href"].strip().split("?")[0]
        tds = row.find_all("td")
        title_label = _clean(a.get_text())
        title_name_suffix = _clean(tds[1].get_text()) if len(tds) > 1 else ""

        m = re.match(r"Title\s+(\S+)", title_label, re.IGNORECASE)
        if not m:
            continue
        title_num = m.group(1).rstrip(".")

        title_wrapper_url = f"{BASE_URL}/{href_path}"
        node_name = f"Title {title_num} {title_name_suffix}".strip()
        node_id = f"{corpus_node.node_id}/title={title_num}"
        status = _check_reserved(node_name)

        title_node = Node(
            id=node_id,
            link=title_wrapper_url,
            top_level_title=title_num,
            node_type="structure",
            level_classifier="title",
            number=title_num,
            node_name=node_name,
            parent=corpus_node.node_id,
            status=status,
        )
        insert_node(title_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)

        if status or title_num in titles_done:
            continue
        work.append((title_node, title_wrapper_url))

    def _do_title(item):
        tnode, turl = item
        try:
            _scrape_title(tnode, turl)
            _mark_title_done(tnode.number)
            return (tnode.number, "ok", None)
        except Exception as e:
            return (tnode.number, "fail", str(e)[:200])

    workers = int(os.environ.get("VAQUILL_TITLE_WORKERS", "8"))
    print(f"[UT] running {len(work)} titles with {workers} parallel workers", flush=True)
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for fut in as_completed(ex.submit(_do_title, item) for item in work):
            num, status_str, err = fut.result()
            if status_str == "fail":
                print(f"[UT] title {num}: {status_str}: {err}", flush=True)
            else:
                print(f"[UT] title {num}: {status_str}", flush=True)


# ---------------------------------------------------------------------------
# Title level
# ---------------------------------------------------------------------------


def _scrape_title(title_node: Node, wrapper_url: str) -> None:
    """Resolve a title's versioned content, iterate chapters."""
    content_soup = _load_content(wrapper_url)
    if content_soup is None:
        return

    child_table = content_soup.find(id="childtbl")
    if child_table is None:
        return

    for row in child_table.find_all("tr"):
        a = row.find("a", href=True)
        if a is None:
            continue

        # href: "../../Title3/Chapter1/3-1.html?v=C3-1_..." -- relative to content file
        # But content file is at Title3/C3_VERSION.html, so ../../ resolves to /xcode/
        # Strip query string for canonical wrapper URL.
        href_raw = a["href"].strip()
        href_path = href_raw.split("?")[0]

        link_text = _clean(a.get_text())
        tds = row.find_all("td")
        name_suffix = _clean(tds[1].get_text()) if len(tds) > 1 else ""

        # Only follow Chapter links
        if "chapter" not in href_path.lower():
            continue

        m = re.search(r"Chapter([^/]+)/", href_path, re.IGNORECASE)
        if not m:
            continue
        ch_raw = m.group(1)

        node_name = _clean(f"{link_text} {name_suffix}").strip() or f"Chapter {ch_raw}"

        # Reconstruct absolute chapter wrapper URL from the canonical Title path
        ch_wrapper_url = _resolve_href(wrapper_url, href_path)

        node_id = f"{title_node.node_id}/chapter={ch_raw}"
        status = _check_reserved(node_name)

        chapter_node = Node(
            id=node_id,
            link=ch_wrapper_url,
            top_level_title=title_node.top_level_title,
            node_type="structure",
            level_classifier="chapter",
            number=ch_raw,
            node_name=node_name,
            parent=title_node.node_id,
            status=status,
        )
        insert_node(chapter_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)

        if not status:
            _scrape_chapter(chapter_node, ch_wrapper_url)


# ---------------------------------------------------------------------------
# Chapter level
# ---------------------------------------------------------------------------


def _scrape_chapter(chapter_node: Node, wrapper_url: str) -> None:
    """Resolve a chapter's versioned content, dispatch to sections."""
    content_soup = _load_content(wrapper_url)
    if content_soup is None:
        return

    child_table = content_soup.find(id="childtbl")
    if child_table is None:
        return

    for row in child_table.find_all("tr"):
        a = row.find("a", href=True)
        if a is None:
            continue

        href_raw = a["href"].strip()
        href_path = href_raw.split("?")[0]
        link_text = _clean(a.get_text())
        tds = row.find_all("td")
        name_suffix = _clean(tds[1].get_text()) if len(tds) > 1 else ""
        node_name = _clean(f"{link_text} {name_suffix}").strip()

        section_wrapper_url = _resolve_href(wrapper_url, href_path)
        # Multi-version sections: emit each as ::v1/::v2 suffixed node.
        version_files = _parse_version_arr_all_from_url(section_wrapper_url)
        if len(version_files) > 1:
            for idx, vf in enumerate(version_files, start=1):
                versioned_url = _resolve_href(section_wrapper_url, vf)
                _emit_section(
                    chapter_node,
                    section_wrapper_url,
                    node_name,
                    version_suffix=f"::v{idx}",
                    versioned_content_url=versioned_url,
                )
        else:
            _emit_section(chapter_node, section_wrapper_url, node_name)


# ---------------------------------------------------------------------------
# Section emission
# ---------------------------------------------------------------------------


def _emit_section(
    parent_node: Node,
    section_wrapper_url: str,
    node_name: str,
    version_suffix: str = "",
    versioned_content_url: Optional[str] = None,
) -> None:
    """Fetch section content (to get the real citation number) and emit the Node.

    ``version_suffix`` (e.g. ``"::v1"``) is appended to node_id+number when a
    section has multiple versions in versionArr (matches IL fix pattern).
    ``versioned_content_url`` overrides the version-file lookup so each call
    parses a different version.
    """
    status = _check_reserved(node_name)

    if status:
        # We still need the section number for the node_id; derive it from URL.
        sec_num = _sec_num_from_url(section_wrapper_url)
        if sec_num is None:
            return
        node_id = f"{parent_node.node_id}/section={sec_num}{version_suffix}"
        citation = f"Utah Code § {sec_num}"
        section_node = Node(
            id=node_id,
            link=section_wrapper_url,
            top_level_title=parent_node.top_level_title,
            node_type="content",
            level_classifier="section",
            number=f"{sec_num}{version_suffix}",
            node_name=node_name,
            parent=parent_node.node_id,
            citation=citation,
            status=status,
        )
        insert_node(section_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)
        return

    node_text, addendum, sec_num = _fetch_section_content(
        section_wrapper_url, versioned_content_url=versioned_content_url
    )

    if sec_num is None:
        # Fall back to URL-derived number if content parse failed
        sec_num = _sec_num_from_url(section_wrapper_url)
    if sec_num is None:
        return

    node_id = f"{parent_node.node_id}/section={sec_num}{version_suffix}"
    citation = f"Utah Code § {sec_num}"

    section_node = Node(
        id=node_id,
        link=section_wrapper_url,
        top_level_title=parent_node.top_level_title,
        node_type="content",
        level_classifier="section",
        number=f"{sec_num}{version_suffix}",
        node_name=node_name or f"§ {sec_num}",
        parent=parent_node.node_id,
        citation=citation,
        node_text=node_text,
        addendum=addendum,
    )
    insert_node(section_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)


# ---------------------------------------------------------------------------
# Content fetching helpers
# ---------------------------------------------------------------------------


def _load_content(wrapper_url: str) -> Optional[BeautifulSoup]:
    """Fetch a wrapper page, parse its versionArr, then fetch the versioned
    content file (located in the same directory as the wrapper) and return it.

    The versioned content file is resolved relative to the wrapper URL, not
    relative to BASE_URL.  For example:
        wrapper: https://le.utah.gov/xcode/Title3/3.html
        version: C3_1800010118000101.html
        result:  https://le.utah.gov/xcode/Title3/C3_1800010118000101.html

    Returns None on failure.
    """
    try:
        wrapper_soup = get_url_as_soup(wrapper_url)
    except Exception as exc:
        print(f"[UT] failed to fetch wrapper {wrapper_url}: {exc}", flush=True)
        return None

    version_file = _parse_version_arr(wrapper_soup)
    if version_file is None:
        # No versionArr -- check if #content is already inline (rare)
        if wrapper_soup.find(id="content"):
            return wrapper_soup
        print(f"[UT] no versionArr and no #content in {wrapper_url}", flush=True)
        return None

    # Resolve version file URL relative to wrapper page's directory
    versioned_url = _resolve_href(wrapper_url, version_file)

    try:
        versioned_soup = get_url_as_soup(versioned_url)
    except Exception as exc:
        print(f"[UT] failed to fetch versioned {versioned_url}: {exc}", flush=True)
        return None

    if versioned_soup.find(id="content") is None:
        print(f"[UT] no #content in versioned file {versioned_url}", flush=True)
        return None

    return versioned_soup


# Captures EVERY versioned filename (single OR double quoted) in versionArr,
# not just the first. Section pages may carry multiple version rows.
_VERSION_ARR_FILE_RE = re.compile(r"""\[\s*['"]([^'"]+\.html)['"]""")


def _parse_version_arr_all(soup: BeautifulSoup) -> List[str]:
    """Extract ALL versioned filenames from the inline JS versionArr.

    Handles both single and double quotes. Returns filenames in document
    order; deduplicates while preserving order.
    """
    seen: set = set()
    out: List[str] = []
    for script in soup.find_all("script"):
        txt = script.string or ""
        if "versionArr" not in txt:
            continue
        # Slice from versionArr to bound the search and avoid unrelated arrays.
        idx = txt.find("versionArr")
        scope = txt[idx:]
        for m in _VERSION_ARR_FILE_RE.finditer(scope):
            name = m.group(1)
            if name not in seen:
                seen.add(name)
                out.append(name)
    return out


def _parse_version_arr(soup: BeautifulSoup) -> Optional[str]:
    """Backward-compatible accessor for the first versioned filename."""
    files = _parse_version_arr_all(soup)
    return files[0] if files else None


def _parse_version_arr_all_from_url(wrapper_url: str) -> List[str]:
    """Fetch a wrapper URL and return all versioned filenames found."""
    try:
        wrapper_soup = get_url_as_soup(wrapper_url)
    except Exception as exc:
        print(f"[UT] failed to fetch wrapper {wrapper_url}: {exc}", flush=True)
        return []
    return _parse_version_arr_all(wrapper_soup)


def _fetch_section_content(
    section_wrapper_url: str,
    versioned_content_url: Optional[str] = None,
):
    """Fetch a section wrapper, resolve versioned content, parse text and citation.

    If ``versioned_content_url`` is provided, fetch that file directly
    (bypassing versionArr lookup); used for multi-version emission.

    Returns (NodeText | None, Addendum | None, section_number | None).
    """
    if versioned_content_url is not None:
        try:
            content_soup = get_url_as_soup(versioned_content_url)
        except Exception as exc:
            print(f"[UT] failed to fetch versioned {versioned_content_url}: {exc}", flush=True)
            return None, None, None
        if content_soup.find(id="content") is None:
            return None, None, None
    else:
        content_soup = _load_content(section_wrapper_url)
    if content_soup is None:
        return None, None, None

    content_div = content_soup.find(id="content")
    if content_div is None:
        return None, None, None

    # Section number lives in #secdiv as a <b> tag whose text matches the
    # citation pattern (digits-digits-...). Some newly effective sections
    # have a first <b> that says "Effective M/D/YYYY" -- skip those.
    # Pattern: digits, hyphens, optional letters/dots (e.g. "3-1-1", "4-2-1101").
    _SEC_NUM_RE = re.compile(r"^\d+[-\d.a-zA-Z]+$")
    sec_num = None
    secdiv = content_div.find(id="secdiv")
    if secdiv:
        for b_tag in secdiv.find_all("b"):
            raw_num = _clean(b_tag.get_text()).rstrip(".")
            if _SEC_NUM_RE.match(raw_num):
                sec_num = raw_num
                break

    node_text = NodeText()
    history_text = ""

    for elem in content_div.find_all(recursive=False):
        tag = elem.name or ""
        elem_id = elem.get("id", "")

        # Skip navigation elements
        if elem_id in ("breadcrumb", "topnavtbl", "parenttbl"):
            continue
        if tag == "ul" and elem_id == "breadcrumb":
            continue
        if tag in ("h3", "table") and elem_id in ("topnavtbl", "parenttbl"):
            continue
        if tag == "hr":
            continue

        text = _clean(elem.get_text(separator=" "))
        if not text:
            continue

        if any(kw in text.lower() for kw in ("amended by", "enacted by", "history:")):
            history_text += (" " if history_text else "") + text
        else:
            node_text.add_paragraph(text=text)

    addendum: Optional[Addendum] = None
    if history_text:
        addendum = Addendum()
        addendum.history = AddendumType(type="history", text=history_text.strip())

    return (node_text if node_text.paragraphs else None), addendum, sec_num


# ---------------------------------------------------------------------------
# URL helpers
# ---------------------------------------------------------------------------


def _resolve_href(base_url: str, href: str) -> str:
    """Resolve an href relative to base_url's directory.

    Examples:
        base = "https://le.utah.gov/xcode/Title3/3.html"
        href = "C3_1800010118000101.html"
        -> "https://le.utah.gov/xcode/Title3/C3_1800010118000101.html"

        base = "https://le.utah.gov/xcode/Title3/C3_VERSION.html"
        href = "../../Title3/Chapter1/3-1.html"
        -> "https://le.utah.gov/xcode/Title3/Chapter1/3-1.html"
    """
    if href.startswith("http"):
        return href
    return urljoin(base_url, href)


def _sec_num_from_url(url: str) -> Optional[str]:
    """Derive a fallback section number from the section wrapper URL.

    Section wrapper URLs use the -S prefix:
        .../Title3/Chapter1/3-1-S1.html    -> "3-1-1"
        .../Title3/Chapter1/3-1-S1.1.html  -> "3-1-1.1"

    Strips the -S and uses the remainder as the section number.
    """
    path = url.split("?")[0]
    # Match filename like 3-1-S1.html or 3-1-S1.1.html
    m = re.search(r"([\d]+(?:-[\w]+)*)-S([\w.]+)\.html$", path, re.IGNORECASE)
    if m:
        return f"{m.group(1)}-{m.group(2)}"
    return None


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


def _check_reserved(text: str) -> Optional[str]:
    upper = text.upper()
    for kw in RESERVED_KEYWORDS:
        if kw.upper() in upper:
            return "reserved"
    return None


def _clean(raw: str) -> str:
    text = raw.replace("\xa0", " ").replace(" ", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


if __name__ == "__main__":
    main()
