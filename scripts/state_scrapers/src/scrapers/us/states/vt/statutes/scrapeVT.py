"""Vermont Statutes Annotated (V.S.A.) scraper.

Hierarchy scraped:
    TOC (legislature.vermont.gov/statutes/)
      -> Title page  (/statutes/title/NN)
           -> Chapter list  (/statutes/chapter/NN/MM)
                -> Section page  (/statutes/section/NN/MM/SSSSS)

Node types:
    title, chapter  -> node_type="structure"
    section         -> node_type="content"

Citation format: "<title_num> V.S.A. § <section_num>"

REAL HTML landmarks (verified 2026-05-11 against live site via Webshare proxy):
    TOC:
        <ul class="item-list statutes-list">
            <li><a href="statutes/title/01">Title 1: General Provisions</a></li>
        NOTE: first two links lack leading slash; the rest have "/statutes/title/NN"
        The scraper normalises both by joining with BASE_URL only when needed.

    Title page (e.g. /statutes/title/01):
        Links to chapters: href="/statutes/chapter/01/001"
        Pattern: /statutes/chapter/{title_padded}/{chapter_padded}

    Chapter page (e.g. /statutes/chapter/01/001):
        Links to sections: href="/statutes/section/01/001/00051"
        Pattern: /statutes/section/{title_padded}/{chapter_padded}/{section_padded}
        Text: " § 51.  Vermont Statutes Annotated defined"

    Section page (e.g. /statutes/section/01/001/00051):
        <ul class="item-list statutes-detail">
            <li>
                <p></p>
                <p style="..."><b>§ 51. Vermont Statutes Annotated defined</b></p>
                <p style="text-indent:...">Body text here. (Added 1959, No. 262...)</p>
            </li>
        </ul>
        History/addendum: the last paragraph often contains "(Added..." or "(Amended..."
        inline with the body. Some sections have it as a standalone final paragraph.

OLD BUGS (why 0 chunks were emitted):
    1. Chapter link regex matched "/statutes/title/NN/chapter/MM" but real URLs are
       "/statutes/chapter/NN/MM" -- completely different path structure.
    2. Section link regex similarly expected /title/.../chapter/.../section/ but real
       path is /statutes/section/NN/MM/SSSSS.
    3. The "-body" suffix appended to section URLs does not exist on this site.
    4. TOC links like "statutes/title/01" (no leading slash) were passed unchanged
       to BASE_URL + href, producing "https://legislature.vermont.govstatutes/title/01".

No Selenium. Pure HTTP + BeautifulSoup via get_url_as_soup (UA rotation + proxy).
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Optional, Tuple

# ---------------------------------------------------------------------------
# Path bootstrap (matches ME/DE pattern)
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

from src.utils.pydanticModels import Addendum, AddendumType, Node, NodeID, NodeText
from src.utils.scrapingHelpers import (
    get_url_as_soup,
    insert_jurisdiction_and_corpus_node,
    insert_node,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

COUNTRY = "us"
JURISDICTION = "vt"
CORPUS = "statutes"
TABLE_NAME = f"{COUNTRY}_{JURISDICTION}_{CORPUS}"

BASE_URL = "https://legislature.vermont.gov"
TOC_URL = "https://legislature.vermont.gov/statutes/"

RESERVED_KEYWORDS = ["(repealed)", "(expired)", "(reserved)", "(renumbered)"]
# Bracketed status markers used by VT, e.g. "[Repealed.]", "[Reserved.]"
_BRACKET_STATUS_RE = re.compile(r"[\[\(](repealed|expired|reserved|renumbered)[\.\]\)]", re.IGNORECASE)

# Addendum detection: paragraphs (or paragraph suffixes) that begin with
# "(Added" or "(Amended".
_ADDENDUM_RE = re.compile(r"^\((?:Added|Amended)", re.IGNORECASE)


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
    try:
        from vaquill_pipeline.config import SETTINGS
        return SETTINGS.chunks_dir / "state_vt_titles_done.txt"
    except Exception:
        # Fallback to a path next to this file when running standalone.
        return Path(__file__).parent / "state_vt_titles_done.txt"


def _load_titles_done() -> set:
    path = _titles_done_path()
    if not path.exists():
        return set()
    return {line.strip() for line in path.read_text().splitlines() if line.strip()}


def _mark_title_done(number: str) -> None:
    path = _titles_done_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a") as fh:
        fh.write(f"{number}\n")
        fh.flush()


def _scrape_all_titles(corpus_node: Node) -> None:
    """Fetch the root TOC and iterate over title links.

    Titles are independent units; we hand them to a ThreadPoolExecutor
    (size from ``VAQUILL_TITLE_WORKERS``, default 8). Completed titles are
    persisted in ``state_vt_titles_done.txt`` for resume. Set
    ``VAQUILL_FORCE_RESCRAPE=1`` to override.
    """
    import os
    from concurrent.futures import ThreadPoolExecutor, as_completed

    soup = get_url_as_soup(TOC_URL)
    toc_ul = soup.find("ul", class_="statutes-list")
    if toc_ul is None:
        toc_ul = soup

    titles_done = set() if os.environ.get("VAQUILL_FORCE_RESCRAPE") else _load_titles_done()
    if titles_done:
        print(f"[scrapeVT] resume: {len(titles_done)} titles already done", flush=True)

    work: list = []
    for a_tag in toc_ul.find_all("a", href=True):
        href: str = a_tag["href"].strip()

        m = re.search(r"statutes/title/([\w.\-]+)$", href)
        if not m:
            continue

        raw_number = m.group(1)
        title_number = _normalise_number(raw_number)
        node_name = _clean_text(a_tag.get_text())
        link = _make_absolute(href)
        node_id = f"{corpus_node.node_id}/title={title_number}"
        status = _check_reserved(node_name)

        title_node = Node(
            id=node_id,
            link=link,
            top_level_title=title_number,
            node_type="structure",
            level_classifier="title",
            number=title_number,
            node_name=node_name,
            parent=corpus_node.node_id,
            status=status,
        )
        insert_node(title_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)

        if status:
            continue
        if title_number in titles_done:
            continue
        work.append(title_node)

    def _do_title(node: Node):
        try:
            _scrape_title(node)
            _mark_title_done(str(node.number))
            return (str(node.number), "ok", None)
        except Exception as e:
            return (str(node.number), "fail", str(e)[:200])

    workers = int(os.environ.get("VAQUILL_TITLE_WORKERS", "8"))
    print(f"[scrapeVT] running {len(work)} titles with {workers} parallel workers", flush=True)
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for fut in as_completed(ex.submit(_do_title, n) for n in work):
            num, status, err = fut.result()
            if status == "fail":
                print(f"[scrapeVT] title {num}: {status}: {err}", flush=True)
            else:
                print(f"[scrapeVT] title {num}: {status}", flush=True)


# ---------------------------------------------------------------------------
# Chapter-level scraping
# ---------------------------------------------------------------------------


def _scrape_title(title_node: Node) -> None:
    """Fetch a title page and iterate over its chapter links.

    Real chapter links look like: /statutes/chapter/01/001
    """
    soup = get_url_as_soup(str(title_node.link))

    for a_tag in soup.find_all("a", href=True):
        href: str = a_tag["href"].strip()

        # Pattern: /statutes/chapter/{title}/{chapter}
        # Slugs may include hyphens or dots (e.g. "001A", "12-1", "5.1").
        ch_m = re.search(r"/statutes/chapter/([\w.\-]+)/([\w.\-]+)$", href)
        if not ch_m:
            continue

        raw_ch = ch_m.group(2)
        ch_number = _normalise_number(raw_ch)
        node_name = _clean_text(a_tag.get_text())
        if not node_name:
            continue

        link = _make_absolute(href)
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


def _scrape_chapter(chapter_node: Node) -> None:
    """Fetch a chapter page; emit subchapter structure nodes if present,
    otherwise route flat section links directly under the chapter.

    Subchapter URLs look like ``/statutes/subchapter/{t}/{c}/{sub}``.
    """
    soup = get_url_as_soup(str(chapter_node.link))

    subchapter_links: list[tuple[str, str]] = []  # (href, text)
    section_links: list[tuple[str, str]] = []
    for a_tag in soup.find_all("a", href=True):
        href: str = a_tag["href"].strip()
        if re.search(r"/statutes/(?:subchapter|article)/([\w.\-]+)/([\w.\-]+)/([\w.\-]+)$", href):
            subchapter_links.append((href, _clean_text(a_tag.get_text())))
        elif re.search(r"/statutes/section/([\w.\-]+)/([\w.\-]+)/([\w.\-]+)$", href):
            section_links.append((href, _clean_text(a_tag.get_text())))

    if subchapter_links:
        for href, text in subchapter_links:
            sub_m = re.search(r"/statutes/(subchapter|article)/([\w.\-]+)/([\w.\-]+)/([\w.\-]+)$", href)
            if not sub_m:
                continue
            level = sub_m.group(1)  # "subchapter" or "article"
            raw_sub = sub_m.group(4)
            sub_number = _normalise_number(raw_sub)
            if not text:
                continue
            link = _make_absolute(href)
            node_id = f"{chapter_node.node_id}/{level}={sub_number}"
            status = _check_reserved(text)
            sub_node = Node(
                id=node_id,
                link=link,
                top_level_title=chapter_node.top_level_title,
                node_type="structure",
                level_classifier=level,
                number=sub_number,
                node_name=text,
                parent=chapter_node.node_id,
                status=status,
            )
            insert_node(sub_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)
            if not status:
                _scrape_subchapter(sub_node)
        return

    _emit_sections(chapter_node, section_links)


def _scrape_subchapter(sub_node: Node) -> None:
    """Fetch a subchapter (or article) page and emit its section links."""
    soup = get_url_as_soup(str(sub_node.link))
    section_links: list[tuple[str, str]] = []
    for a_tag in soup.find_all("a", href=True):
        href: str = a_tag["href"].strip()
        if re.search(r"/statutes/section/([\w.\-]+)/([\w.\-]+)/([\w.\-]+)$", href):
            section_links.append((href, _clean_text(a_tag.get_text())))
    _emit_sections(sub_node, section_links)


def _emit_sections(parent_node: Node, section_links: list) -> None:
    """Emit section nodes (content) under any parent (chapter or subchapter)."""
    title_num = parent_node.top_level_title
    citation_prefix = f"{title_num} V.S.A. §" if not _is_pure_alpha(str(title_num)) else f"V.S.A. tit. {title_num} §"

    for href, node_name in section_links:
        sec_m = re.search(r"/statutes/section/([\w.\-]+)/([\w.\-]+)/([\w.\-]+)$", href)
        if not sec_m:
            continue
        raw_sec = sec_m.group(3)
        sec_number = _normalise_number(raw_sec)
        if not node_name:
            continue

        link = _make_absolute(href)
        node_id = f"{parent_node.node_id}/section={sec_number}"
        status = _check_reserved(node_name)
        citation = f"{citation_prefix} {sec_number}"

        if status:
            section_node = Node(
                id=node_id,
                link=link,
                top_level_title=parent_node.top_level_title,
                node_type="content",
                level_classifier="section",
                number=sec_number,
                node_name=node_name,
                parent=parent_node.node_id,
                citation=citation,
                status=status,
            )
            insert_node(section_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)
            continue

        node_text, addendum = _fetch_section_content(link)

        section_node = Node(
            id=node_id,
            link=link,
            top_level_title=parent_node.top_level_title,
            node_type="content",
            level_classifier="section",
            number=sec_number,
            node_name=node_name,
            parent=parent_node.node_id,
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

    VT section pages use:
        <ul class="item-list statutes-detail">
            <li>
                <p></p>                                         <- empty, skip
                <p ...><b>§ N. Section name</b></p>            <- heading, skip
                <p style="text-indent:...">Body text here.     <- content
                    (Added 1959, No. 262, ...)                  <- may be appended
                </p>
            </li>
        </ul>

    History lines may appear as:
        (a) The last sentence(s) of a body paragraph starting with "(Added"/"(Amended"
        (b) A separate trailing <p> starting with "(Added"/"(Amended"
    Both cases are split into the addendum.
    """
    soup = get_url_as_soup(url)

    container = soup.find("ul", class_="statutes-detail")
    if container is None:
        container = soup.find("div", id="main-content") or soup

    node_text = NodeText()
    history_parts: list[str] = []

    for p in container.find_all("p"):
        raw = p.get_text(separator=" ")
        text = _clean_text(raw)
        if not text:
            continue

        # Skip the section heading paragraph (contains bold "§ N. Title" and nothing else)
        b_tag = p.find("b")
        if b_tag:
            b_text = _clean_text(b_tag.get_text())
            rest = text.replace(b_text, "").strip()
            if not rest:
                continue

        # Check if the entire paragraph is an addendum line.
        if _ADDENDUM_RE.match(text):
            history_parts.append(text)
            continue

        # Some paragraphs have body text followed by "(Added ..." in the same <p>.
        # Split on the addendum pattern.
        addendum_split = re.split(r"\s+(?=\((?:Added|Amended)\b)", text, maxsplit=1, flags=re.IGNORECASE)
        if len(addendum_split) == 2:
            body_part = addendum_split[0].strip()
            hist_part = addendum_split[1].strip()
            if body_part:
                node_text.add_paragraph(text=body_part)
            if hist_part:
                history_parts.append(hist_part)
        else:
            node_text.add_paragraph(text=text)

    addendum: Optional[Addendum] = None
    if history_parts:
        addendum = Addendum()
        addendum.history = AddendumType(type="history", text=" ".join(history_parts))

    if not node_text.paragraphs:
        node_text = None  # type: ignore[assignment]

    return node_text, addendum


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_absolute(href: str) -> str:
    """Turn a relative or root-relative href into a full URL."""
    if href.startswith("http"):
        return href
    if href.startswith("/"):
        return BASE_URL + href
    # Relative without leading slash: e.g. "statutes/title/01"
    return BASE_URL + "/" + href


def _normalise_number(raw: str) -> str:
    """Strip leading zeros from numeric prefix while preserving alpha suffixes.

    Pure-alpha tokens (e.g. ``APPENDIX``) are returned unchanged so that
    Title 12 Appendix doesn't end up concatenated with a stray digit.

    Examples:
        "001"      -> "1"
        "01"       -> "1"
        "9A"       -> "9A"
        "09A"      -> "9A"
        "00134a"   -> "134a"
        "APPENDIX" -> "APPENDIX"
        "03APPENDIX" -> "3APPENDIX"
    """
    if not raw or not any(c.isdigit() for c in raw):
        return raw
    m = re.match(r"^0*(\d+)(.*)", raw)
    if m:
        return m.group(1) + m.group(2)
    return raw


def _is_pure_alpha(s: str) -> bool:
    return bool(s) and not any(c.isdigit() for c in s)


def _check_reserved(text: str) -> Optional[str]:
    """Return 'reserved' if the text contains a reserved keyword, else None.

    Matches both parenthesised lowercase markers like ``(repealed)`` and
    VT's bracketed form ``[Repealed.]`` (with any of the four keywords).
    """
    lower = text.lower()
    for kw in RESERVED_KEYWORDS:
        if kw in lower:
            return "reserved"
    if _BRACKET_STATUS_RE.search(text):
        return "reserved"
    return None


def _clean_text(raw: str) -> str:
    """Normalise whitespace and strip non-breaking spaces."""
    text = raw.replace("\xa0", " ").replace(" ", " ").replace("’", "'")
    text = re.sub(r"\s+", " ", text).strip()
    return text


# ---------------------------------------------------------------------------

if __name__ == "__main__":
    main()
