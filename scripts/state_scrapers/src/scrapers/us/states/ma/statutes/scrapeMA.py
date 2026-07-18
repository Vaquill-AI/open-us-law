"""Massachusetts General Laws (MGL) scraper.

Source: https://malegislature.gov/Laws/GeneralLaws (official MA General Court).

Previous source (law.onecle.com/massachusetts/) now returns HTTP 403 for all
chapter index pages, so it has been retired. malegislature.gov is the official
publisher; it geo-blocks non-US IPs at the network firewall (TCP timeout on
:443), so the hostname is registered in
``vaquill_pipeline.http_client._HARD_SITE_HINTS`` to force ZenRows / US
residential proxy routing on the scraper VM.

Hierarchy scraped (MGL): Part -> Title -> Chapter -> Section.

DOM walk (verified live 2026-05-12 via fetch_html with US proxy):
    1. /Laws/GeneralLaws
         -> 5 Part links only (no chapters here).
    2. /Laws/GeneralLaws/Part{R}
         -> renders an accordion with one panel per Title. Title metadata
            sits in accordion toggle anchors:
              <a href="#title{CODE}"
                 onclick="accordionAjaxLoad(partId, titleId, 'CODE')">
                 Title I
              </a>
            The chapter UL inside each panel is JS-loaded (empty in raw HTML).
            We extract (partId, titleId, code, label) from the onclick.
    3. /Laws/GeneralLaws/GetChaptersForTitle?partId=X&titleId=Y&code=Z
         -> server-rendered <ul class="generalLawsList"> with <a> per chapter:
              /Laws/GeneralLaws/PartI/TitleI/ChapterN
    4. /Laws/GeneralLaws/Part.../Title.../Chapter{N}
         -> section anchors inline (no AJAX): /.../Chapter{N}/Section{S}
    5. /.../Section{S}
         -> body lives under <div class="content"> as <p> paragraphs.
Earlier rewrites assumed Title + Chapter were normal anchor links on the
Part page; that returns 0 because both are JS-rendered. The walk below
combines the Part-page accordion (titles) with GetChaptersForTitle
(chapters) to recover the full TOC without Selenium.

URL pattern:
    https://malegislature.gov/Laws/GeneralLaws/Part{I-V}
    https://malegislature.gov/Laws/GeneralLaws/Part{P}/Title{R}
    https://malegislature.gov/Laws/GeneralLaws/Part{P}/Title{R}/Chapter{N}
    https://malegislature.gov/Laws/GeneralLaws/Part{P}/Title{R}/Chapter{N}/Section{S}

Where Part is roman I-V, Title is roman (I, II, IIA, ...), Chapter is alnum
(1, 6A, 183B), Section is alnum (1, 7A, 10B).

Node IDs:
    us/ma/statutes/part=I/title=I/chapter=1/section=1

Citation format:
    "Mass. Gen. Laws ch. <C>, sec. <S>"

Vaquill integration:
* vaquill_pipeline.patch.install() for r2_sync + JsonlSink in insert_node.
* Title-level resume via state_ma_units_done.txt
  (VAQUILL_FORCE_RESCRAPE=1 overrides).
* Title-level ThreadPoolExecutor (VAQUILL_TITLE_WORKERS, default 8).

No Selenium. Pure HTTP + BeautifulSoup via get_url_as_soup (proxy + UA rotation).
"""

from __future__ import annotations

import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Optional, Tuple
from urllib.parse import urlparse

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

# Vaquill pipeline integration: install patches for r2_sync + JsonlSink + state lock.
try:
    from vaquill_pipeline import patch as _vaquill_patch
    _vaquill_patch.install()
except Exception as _e:  # noqa: BLE001
    print(f"[warn] vaquill_pipeline.patch.install() skipped: {_e}", flush=True)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

COUNTRY = "us"
JURISDICTION = "ma"
CORPUS = "statutes"
TABLE_NAME = f"{COUNTRY}_{JURISDICTION}_{CORPUS}"

BASE_URL = "https://malegislature.gov"
TOC_URL = f"{BASE_URL}/Laws/GeneralLaws"

# MA has exactly five Parts. Descriptions are stable.
PARTS: List[Tuple[str, str]] = [
    ("I",   "Administration of the Government"),
    ("II",  "Real and Personal Property and Domestic Relations"),
    ("III", "Courts, Judicial Officers and Proceedings in Civil Cases"),
    ("IV",  "Crimes, Punishments and Proceedings in Criminal Cases"),
    ("V",   "The General Laws, and Express Repeal Of Certain Acts and Resolves"),
]

RESERVED_KEYWORDS = [
    "(repealed)",
    "(reserved)",
    "(expired)",
    "(renumbered)",
    "(deleted)",
    "reserved.",
    "repealed.",
]

# malegislature.gov path components
# Part link: /Laws/GeneralLaws/PartI
# Title link: /Laws/GeneralLaws/PartI/TitleI[A]
# Chapter link: /Laws/GeneralLaws/PartI/TitleI/Chapter1[A]
# Section link: /Laws/GeneralLaws/PartI/TitleI/Chapter1/Section1[A]
_PART_HREF_RE = re.compile(
    r"^/Laws/GeneralLaws/Part([IVXLCDM]+)/?$", re.IGNORECASE
)
_TITLE_HREF_RE = re.compile(
    r"^/Laws/GeneralLaws/Part[IVXLCDM]+/Title([IVXLCDM]+[A-Z]?)/?$",
    re.IGNORECASE,
)
_CHAPTER_HREF_RE = re.compile(
    r"^/Laws/GeneralLaws/Part[IVXLCDM]+/Title[IVXLCDM]+[A-Z]?/Chapter([0-9]+[A-Za-z]?)/?$",
    re.IGNORECASE,
)

# Part-page accordion toggle: anchor href="#titleI" plus onclick
# accordionAjaxLoad('<partId>', '<titleId>', '<CODE>').
_TITLE_TOGGLE_HREF_RE = re.compile(r"^#title([A-Z]+)$", re.IGNORECASE)
_ACCORDION_AJAX_RE = re.compile(
    r"""accordionAjaxLoad\(\s*['"](?P<partId>\d+)['"]\s*,
        \s*['"](?P<titleId>\d+)['"]\s*,
        \s*['"](?P<code>[A-Za-z0-9]+)['"]\s*\)""",
    re.IGNORECASE | re.VERBOSE,
)
_SECTION_HREF_RE = re.compile(
    r"^/Laws/GeneralLaws/Part[IVXLCDM]+/Title[IVXLCDM]+[A-Z]?/Chapter[0-9]+[A-Za-z]?/Section([0-9]+[A-Za-z0-9]*)/?$",
    re.IGNORECASE,
)

# Addendum/history paragraphs: "(Added by St.<year>, c.<n>...)" etc.
_ADDENDUM_RE = re.compile(
    r"^\s*\(?(?:Added|Amended|Repealed|St\.|P\.L\.|L\.|Acts|R\.L\.)", re.IGNORECASE
)

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    corpus_node: Node = insert_jurisdiction_and_corpus_node(COUNTRY, JURISDICTION, CORPUS)
    _scrape_all_parts(corpus_node)


# ---------------------------------------------------------------------------
# Resume helpers (part+title level)
# ---------------------------------------------------------------------------


def _units_done_path():
    from vaquill_pipeline.config import SETTINGS
    return SETTINGS.chunks_dir / "state_ma_units_done.txt"


def _load_units_done() -> set:
    try:
        path = _units_done_path()
    except Exception:
        return set()
    if not path.exists():
        return set()
    return {ln.strip() for ln in path.read_text().splitlines() if ln.strip()}


def _mark_unit_done(key: str) -> None:
    try:
        path = _units_done_path()
    except Exception:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a") as fh:
        fh.write(f"{key}\n")
        fh.flush()


# ---------------------------------------------------------------------------
# Part / Title / Chapter discovery
# ---------------------------------------------------------------------------


def _scrape_all_parts(corpus_node: Node) -> None:
    """Walk Parts -> fetch each Part TOC -> enumerate Titles, then schedule
    one parallel worker per Title that fetches the Title TOC, enumerates
    Chapters, and scrapes each Chapter's sections.

    LIVE DOM (verified 2026-05):
    * /Laws/GeneralLaws            -> 5 Part links only (no Chapters here)
    * /Laws/GeneralLaws/PartI      -> Title links only
    * /Laws/GeneralLaws/PartI/TitleI -> Chapter links
    * /Laws/GeneralLaws/PartI/TitleI/Chapter1 -> Section links

    Hierarchy: Part > Title > Chapter > Section.

    Title is the natural parallelism unit (VAQUILL_TITLE_WORKERS, default 8).
    """
    units_done = (
        set() if os.environ.get("VAQUILL_FORCE_RESCRAPE") else _load_units_done()
    )
    if units_done:
        print(
            f"[scrapeMA] resume: {len(units_done)} title units already done",
            flush=True,
        )

    # Collect (title_node, part_id_int, title_id_int, code, unit_key).
    # partId/titleId/code feed GetChaptersForTitle in the worker; chapter
    # discovery is deferred so we don't serialize on per-Title fetches.
    work: List[Tuple[Node, str, str, str, str]] = []

    for part_roman, part_description in PARTS:
        part_path = f"/Laws/GeneralLaws/Part{part_roman}"
        part_url = f"{BASE_URL}{part_path}"
        part_node = Node(
            id=f"{corpus_node.node_id}/part={part_roman}",
            link=part_url,
            top_level_title=part_roman,
            node_type="structure",
            level_classifier="part",
            number=part_roman,
            node_name=f"Part {part_roman} - {part_description}",
            parent=corpus_node.node_id,
        )
        insert_node(part_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)

        titles = _extract_titles_for_part(part_node)
        print(
            f"[scrapeMA] Part {part_roman}: discovered {len(titles)} title(s)",
            flush=True,
        )
        for title_node, part_id, title_id, code in titles:
            insert_node(title_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)
            unit_key = f"{part_node.number}|{title_node.number}"
            if unit_key in units_done:
                continue
            work.append((title_node, part_id, title_id, code, unit_key))

    workers = int(os.environ.get("VAQUILL_TITLE_WORKERS", "8"))
    print(
        f"[scrapeMA] running {len(work)} title units with {workers} parallel workers",
        flush=True,
    )

    def _do_unit(item):
        title_node, part_id, title_id, code, unit_key = item
        try:
            chapters = _extract_chapters_for_title(part_id, title_id, code)
            for ch_path, ch_number, ch_name in chapters:
                _scrape_chapter_link(title_node, ch_path, ch_number, ch_name)
            _mark_unit_done(unit_key)
            return (unit_key, "ok", len(chapters), None)
        except Exception as e:  # noqa: BLE001
            return (unit_key, "fail", 0, str(e)[:200])

    with ThreadPoolExecutor(max_workers=workers) as ex:
        for fut in as_completed(ex.submit(_do_unit, item) for item in work):
            key, status, n_ch, err = fut.result()
            if status == "fail":
                print(f"[scrapeMA] {key}: {status}: {err}", flush=True)
            else:
                print(f"[scrapeMA] {key}: {status} ({n_ch} chapters)", flush=True)


def _extract_titles_for_part(
    part_node: Node,
) -> List[Tuple[Node, str, str, str]]:
    """Fetch a Part TOC page and return [(title_node, partId, titleId, code)].

    The Part page renders Title metadata as accordion toggles:
      <a href="#titleI"
         onclick="accordionAjaxLoad('1', '1', 'I')">Title I</a>
      <a href="#titleI"
         onclick="accordionAjaxLoad('1', '1', 'I')">JURISDICTION ...</a>
    Two anchors per title (label "Title I" + descriptive name). We pair
    them up by the shared href / titleId. Chapter contents are NOT in the
    page (the <ul class="generalLawsList"> is JS-populated); they are
    fetched later via GetChaptersForTitle.
    """
    soup = get_url_as_soup(str(part_node.link))

    # Map titleId -> { code, roman, label_short, label_long }
    discovered: dict[str, dict] = {}

    for a in soup.find_all("a", href=True):
        raw_href = (a.get("href") or "").strip()
        m_href = _TITLE_TOGGLE_HREF_RE.match(raw_href)
        if not m_href:
            continue
        onclick = a.get("onclick") or ""
        m_ajax = _ACCORDION_AJAX_RE.search(onclick)
        if not m_ajax:
            continue
        part_id = m_ajax.group("partId")
        title_id = m_ajax.group("titleId")
        code = m_ajax.group("code").upper()
        text = _clean_text(a.get_text())
        if not text:
            continue
        entry = discovered.setdefault(
            title_id,
            {
                "part_id": part_id,
                "title_id": title_id,
                "code": code,
                "labels": [],
            },
        )
        # Skip the "Chapters X - Y" range label.
        if re.match(r"^Chapters?\b", text, flags=re.IGNORECASE):
            continue
        entry["labels"].append(text)

    out: List[Tuple[Node, str, str, str]] = []
    for entry in discovered.values():
        code = entry["code"]
        labels = entry["labels"]
        # Pick the descriptive (longer) label that ISN'T literally "Title X".
        short_label_re = re.compile(
            rf"^Title\s+{re.escape(code)}\s*$", re.IGNORECASE
        )
        descriptive = next(
            (lbl for lbl in labels if not short_label_re.match(lbl)),
            "",
        )
        node_name = f"Title {code}" + (f" - {descriptive}" if descriptive else "")
        title_node = Node(
            id=f"{part_node.node_id}/title={code}",
            link=f"{BASE_URL}/Laws/GeneralLaws/Part{part_node.number}/Title{code}",
            top_level_title=part_node.top_level_title,
            node_type="structure",
            level_classifier="title",
            number=code,
            node_name=node_name,
            parent=part_node.node_id,
        )
        out.append((title_node, entry["part_id"], entry["title_id"], code))

    return out


def _extract_chapters_for_title(
    part_id: str, title_id: str, code: str
) -> List[Tuple[str, str, str]]:
    """Fetch chapter list via the GetChaptersForTitle AJAX endpoint.

    Endpoint:
      /Laws/GeneralLaws/GetChaptersForTitle?partId=<int>&titleId=<int>&code=<R>
    Returns server-rendered fragment:
      <ul class="generalLawsList">
        <li><a href="/Laws/GeneralLaws/PartI/TitleI/Chapter1">
          <span class="chapter">Chapter 1</span>
          <span class="chapterTitle">JURISDICTION OF...</span>
        </a></li>
        ...
      </ul>
    """
    url = (
        f"{BASE_URL}/Laws/GeneralLaws/GetChaptersForTitle"
        f"?partId={part_id}&titleId={title_id}&code={code}"
    )
    soup = get_url_as_soup(url)
    out: List[Tuple[str, str, str]] = []
    seen: set = set()
    for a in soup.find_all("a", href=True):
        raw_href = (a.get("href") or "").strip()
        if not raw_href:
            continue
        href_path = urlparse(raw_href).path or raw_href
        m_ch = _CHAPTER_HREF_RE.match(href_path)
        if not m_ch:
            continue
        ch_number = m_ch.group(1).upper()
        if ch_number in seen:
            continue
        seen.add(ch_number)
        # Prefer span.chapterTitle text (the descriptive name) over the
        # composite "Chapter N <name>" anchor text.
        title_span = a.find("span", class_="chapterTitle")
        if title_span is not None:
            ch_name = _clean_text(title_span.get_text(" "))
        else:
            ch_name = _strip_label(
                _clean_text(a.get_text(" ")), "Chapter", ch_number
            )
        if not ch_name:
            ch_name = f"Chapter {ch_number}"
        out.append((href_path, ch_number, ch_name))
    return out


# ---------------------------------------------------------------------------
# Chapter / Section
# ---------------------------------------------------------------------------


def _scrape_chapter_link(
    title_node: Node,
    ch_path: str,
    ch_number: str,
    ch_name: str,
) -> None:
    """Insert chapter node parented under title, then scrape its sections."""
    status = _check_reserved(ch_name)
    ch_url = f"{BASE_URL}{ch_path}"
    chapter_node = Node(
        id=f"{title_node.node_id}/chapter={ch_number}",
        link=ch_url,
        top_level_title=title_node.top_level_title,
        node_type="structure",
        level_classifier="chapter",
        number=ch_number,
        node_name=ch_name,
        parent=title_node.node_id,
        status=status,
    )
    insert_node(chapter_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)
    if not status:
        _scrape_chapter(chapter_node)


def _scrape_chapter(chapter_node: Node) -> None:
    """Fetch a chapter page and iterate over section links."""
    soup = get_url_as_soup(str(chapter_node.link))
    ch_number = chapter_node.number

    seen_sections: set = set()
    for a_tag in soup.find_all("a", href=True):
        raw_href: str = (a_tag.get("href") or "").strip()
        if not raw_href:
            continue
        href_path = urlparse(raw_href).path or raw_href
        m = _SECTION_HREF_RE.match(href_path)
        if not m:
            continue
        sec_number = m.group(1).upper()
        if sec_number in seen_sections:
            continue
        seen_sections.add(sec_number)

        link_text = _clean_text(a_tag.get_text())
        node_name = _strip_label(link_text, "Section", sec_number) or f"Section {sec_number}"

        sec_url = f"{BASE_URL}{href_path}"
        node_id = f"{chapter_node.node_id}/section={sec_number}"
        citation = f"Mass. Gen. Laws ch. {ch_number}, sec. {sec_number}"
        status = _check_reserved(link_text)

        if status:
            section_node = Node(
                id=node_id,
                link=sec_url,
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

        node_text, addendum, body_status = _fetch_section_content(sec_url)
        section_node = Node(
            id=node_id,
            link=sec_url,
            top_level_title=chapter_node.top_level_title,
            node_type="content",
            level_classifier="section",
            number=sec_number,
            node_name=node_name,
            parent=chapter_node.node_id,
            citation=citation,
            node_text=node_text,
            addendum=addendum,
            status=body_status,
        )
        insert_node(section_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)


# ---------------------------------------------------------------------------
# Section body fetching
# ---------------------------------------------------------------------------


def _fetch_section_content(
    url: str,
) -> Tuple[Optional[NodeText], Optional[Addendum], Optional[str]]:
    """Fetch a section page; return (NodeText, Addendum, status).

    malegislature.gov section pages wrap the statute body in a container
    such as ``<div class="col-xs-12">`` containing ``<p>`` paragraphs, with
    history lines appearing at the tail prefixed with "(Added", "(Amended"
    or session-law citations ("St.YYYY, c.NN").

    We are defensive: we accept ANY ``<p>`` under the main content region
    (#content / .modulecontent / main / body fallback). Boilerplate is
    suppressed by ``_is_navigation_paragraph``.
    """
    soup = get_url_as_soup(url)

    # Prefer the document content container if present. On malegislature.gov
    # section pages the statute body sits inside <div class="content"> (the
    # generic id="content" container does not exist).
    body = (
        soup.find(class_="content")
        or soup.find(id="content")
        or soup.find(class_="modulecontent")
        or soup.find("main")
        or soup
    )

    paragraphs = body.find_all("p")
    node_text = NodeText()
    history_parts: list[str] = []
    body_status: Optional[str] = None

    for p in paragraphs:
        text = _clean_text(p.get_text(separator=" "))
        if not text:
            continue
        if _is_navigation_paragraph(text):
            continue

        # [Repealed YYYY...] markers anywhere in body promote status.
        if re.search(r"\[\s*Repealed\b", text, flags=re.IGNORECASE):
            body_status = "reserved"

        if _ADDENDUM_RE.match(text):
            history_parts.append(text)
            continue

        # Trailing history split: "<body> (Added by St.1999, c.127.)"
        split = re.split(
            r"\s+(?=\((?:Added|Amended|Repealed|St\.|P\.L\.|L\.|Acts|R\.L\.)\b)",
            text,
            maxsplit=1,
            flags=re.IGNORECASE,
        )
        if len(split) == 2:
            body_part = split[0].strip()
            hist_part = split[1].strip()
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

    out_text: Optional[NodeText] = node_text if node_text.paragraphs else None
    return out_text, addendum, body_status


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _strip_label(text: str, label: str, number: str) -> str:
    """Strip a leading "<Label> <Number>" (and trailing dash/colon) from text.

    Useful when malegislature.gov renders link text as e.g.
    "Title I  Administration of the Government" or
    "Chapter 1  Jurisdiction and Emblems of the Commonwealth".
    """
    if not text:
        return ""
    pattern = (
        r"^\s*" + re.escape(label) + r"\s+" + re.escape(number) + r"\b[\s\.\-:]*"
    )
    return re.sub(pattern, "", text, flags=re.IGNORECASE).strip()


def _is_navigation_paragraph(text: str) -> bool:
    lower = text.lower()
    if "general court of" in lower and "massachusetts" in lower:
        return True
    if "skip to content" in lower or "skip to main" in lower:
        return True
    if "terms of use" in lower or "privacy policy" in lower:
        return True
    if lower.startswith("print page") or lower.startswith("show recent"):
        return True
    if lower.startswith("use mylegislature"):
        return True
    if "not registered" in lower or "learn more here" in lower:
        return True
    if lower == "link to an external site":
        return True
    return False


def _check_reserved(text: str) -> Optional[str]:
    lower = text.lower()
    for kw in RESERVED_KEYWORDS:
        if kw in lower:
            return "reserved"
    return None


def _clean_text(raw: str) -> str:
    """Normalise whitespace and strip non-breaking spaces / smart quotes."""
    if not raw:
        return ""
    text = (
        raw.replace("\xa0", " ")
           .replace(" ", " ")
           .replace(" ", " ")
           .replace("’", "'")
           .replace("‘", "'")
           .replace("“", '"')
           .replace("”", '"')
    )
    text = re.sub(r"\s+", " ", text).strip()
    return text


# ---------------------------------------------------------------------------

if __name__ == "__main__":
    main()
