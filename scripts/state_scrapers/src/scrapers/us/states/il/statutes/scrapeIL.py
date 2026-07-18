"""Illinois Compiled Statutes (ILCS) scraper.

Source: https://www.ilga.gov/legislation/ilcs/ilcs.asp
Hierarchy: us/il/statutes/chapter=N/act=A/section=S
Citation: "<ChapterNumber> ILCS <ActNumber>/<SectionNumber>"
  e.g. "5 ILCS 70/0.01"

Structure discovered by probing ilga.gov (2026-05-11):
- TOC page lists ~68 chapters via
    /Legislation/ILCS/Acts?ChapterID=N&ChapterNumber=M&Chapter=...
- Each chapter's Acts page lists acts via
    /Legislation/ILCS/Articles?ActID=A&ChapterID=N&...
  Act link text: "<ChapterNum> ILCS <ActNum>/  Act Name."
- Each act's FullText page at
    /legislation/ILCS/details?...&ActID=A&ChapterID=N&SeqStart=&&ChapAct=FullText
  renders all sections inline.
- Section blocks live in <table align="center"><tr><td>
    <div align="justify"><code>...</code>...</div></td></tr></table>
- First <code> text in a section block matches r'\\(\\d+ ILCS \\d+/[^)]+\\)'
- "Art. N heading" blocks are article separators (no section text).
- Source notes match r'^\\(Source:'.
"""
from __future__ import annotations

import os
import re
import sys
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

from bs4 import BeautifulSoup, Tag

# ---------------------------------------------------------------------------
# Project-root bootstrap (mirrors ME/CT pattern)
# ---------------------------------------------------------------------------
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

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
COUNTRY = "us"
JURISDICTION = "il"
CORPUS = "statutes"
TABLE_NAME = f"{COUNTRY}_{JURISDICTION}_{CORPUS}"

BASE = "https://www.ilga.gov"
TOC_URL = f"{BASE}/legislation/ilcs/ilcs.asp"

RESERVED_KEYWORDS = (
    "(repealed",
    "(expired",
    "(reserved",
    "(renumbered",
    "(transferred",
)

# Matches the first <code> in a section block, e.g.:
#   "(5 ILCS 70/0.01)"  or  "(5 ILCS 100/Art. 5 heading)"
# Act number can contain a trailing letter on rare acts; allow digits + optional
# alpha. Note: do NOT anchor on $ alone (some blocks have trailing whitespace
# already stripped by _clean, but defensive non-anchored match avoids drops).
_SEC_CITE_RE = re.compile(r"^\((\d+)\s+ILCS\s+(\d+[A-Za-z]?)/([^)]+)\)\s*$")

# Section number from "Sec. 0.01." style. Section numbers can include digits,
# dots, hyphens, and trailing alpha (e.g. "11-1.20", "0.01", "2a"). Avoid
# greedy match of body text after the period.
_SEC_NUM_RE = re.compile(r"^Sec\.\s+([0-9][\w\.\-]*?)\.\s")

# Source note
_SOURCE_RE = re.compile(r"^\(Source:", re.IGNORECASE)

# Version marker, e.g. "(Text of Section before amendment by P.A. 103-...)",
# "(Text of Section after amendment by P.A. 103-...)", "(Text of Section from
# P.A. 103-... )". One section block can contain multiple version markers,
# each followed by its own "Sec. N." body and "(Source: ...)" note.
_VERSION_RE = re.compile(r"^\(Text of Section\b[^)]*\)\s*$", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    corpus_node: Node = insert_jurisdiction_and_corpus_node(COUNTRY, JURISDICTION, CORPUS)
    _scrape_toc(corpus_node)


# ---------------------------------------------------------------------------
# TOC -> Chapters
# ---------------------------------------------------------------------------


def _chapters_done_path() -> Path:
    """Where we persist the set of chapters already fully scraped for resume."""
    from vaquill_pipeline.config import SETTINGS
    return SETTINGS.chunks_dir / "state_il_chapters_done.txt"


def _load_chapters_done() -> set[str]:
    path = _chapters_done_path()
    if not path.exists():
        return set()
    return {ln.strip() for ln in path.read_text().splitlines() if ln.strip()}


def _mark_chapter_done(number: str) -> None:
    path = _chapters_done_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a") as fh:
        fh.write(f"{number}\n")
        fh.flush()


def _scrape_toc(corpus_node: Node) -> None:
    soup = get_url_as_soup(TOC_URL)
    seen_chapters: set[str] = set()

    chapters_done = (
        set() if os.environ.get("VAQUILL_FORCE_RESCRAPE") else _load_chapters_done()
    )
    if chapters_done:
        print(
            f"[scrapeIL] resume: {len(chapters_done)} chapters already done",
            flush=True,
        )

    # Collect chapter work items first (sequential, cheap parse).
    work: list[tuple[Node, str]] = []
    for link_tag in soup.find_all("a", href=True):
        href: str = link_tag["href"]
        # Chapter links contain Acts?ChapterID= and ChapterNumber=
        if "Acts?" not in href or "ChapterID=" not in href or "ChapterNumber=" not in href:
            continue

        params = dict(urllib.parse.parse_qsl(urllib.parse.urlparse(href).query))
        chapter_num = params.get("ChapterNumber", "").strip()
        chapter_id = params.get("ChapterID", "").strip()
        if not chapter_num or chapter_num in seen_chapters:
            continue
        seen_chapters.add(chapter_num)

        raw_name = _clean(link_tag.get_text())
        node_name = re.sub(r"\s+", " ", raw_name).strip()
        if not node_name:
            continue

        status = _check_reserved(node_name)
        node_id = f"{corpus_node.node_id}/chapter={chapter_num}"
        acts_url = f"{BASE}{href}" if href.startswith("/") else href

        chapter_node = Node(
            id=node_id,
            link=acts_url,
            top_level_title=chapter_num,
            node_type="structure",
            level_classifier="chapter",
            number=chapter_num,
            node_name=node_name,
            parent=corpus_node.node_id,
            status=status,
        )
        insert_node(chapter_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)

        if status:
            continue
        if chapter_num in chapters_done:
            continue
        work.append((chapter_node, chapter_id))

    def _do_chapter(item: tuple[Node, str]) -> tuple[str, str, Optional[str]]:
        chapter_node, chapter_id = item
        try:
            _scrape_acts(chapter_node, chapter_id)
            _mark_chapter_done(chapter_node.number)
            return (chapter_node.number, "ok", None)
        except Exception as exc:
            return (chapter_node.number, "fail", str(exc)[:200])

    workers = int(os.environ.get("VAQUILL_TITLE_WORKERS", "8"))
    print(
        f"[scrapeIL] running {len(work)} chapters with {workers} parallel workers",
        flush=True,
    )
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for fut in as_completed(ex.submit(_do_chapter, item) for item in work):
            num, status, err = fut.result()
            if status == "fail":
                print(f"[scrapeIL] chapter {num}: {status}: {err}", flush=True)
            else:
                print(f"[scrapeIL] chapter {num}: {status}", flush=True)


# ---------------------------------------------------------------------------
# Chapter -> Acts
# ---------------------------------------------------------------------------


def _scrape_acts(chapter_node: Node, chapter_id: str) -> None:
    soup = get_url_as_soup(str(chapter_node.link))
    seen_acts: set[str] = set()

    for link_tag in soup.find_all("a", href=True):
        href: str = link_tag["href"]
        if "Articles?" not in href or "ActID=" not in href:
            continue

        params = dict(urllib.parse.parse_qsl(urllib.parse.urlparse(href).query))
        act_id = params.get("ActID", "").strip()
        if not act_id or act_id in seen_acts:
            continue
        seen_acts.add(act_id)

        raw_text = _clean(link_tag.get_text())
        # e.g. "5 ILCS 70/       Statute on Statutes."
        # or   "5 ILCS 100/       Illinois Administrative Procedure Act."
        # Extract act number from the ILCS citation part
        m = re.match(r"(\d+)\s+ILCS\s+([\d]+)/", raw_text)
        if not m:
            continue
        chapter_num_from_text = m.group(1)   # "5"
        act_num = m.group(2)                 # "70" or "100"

        node_name = raw_text
        status = _check_reserved(node_name)

        node_id = f"{chapter_node.node_id}/act={act_num}"
        articles_url = f"{BASE}{href}" if href.startswith("/") else href

        # Build the FullText URL for this act
        # /legislation/ILCS/details?...&ActID=N&ChapterID=N&SeqStart=&&ChapAct=FullText
        full_text_url = (
            f"{BASE}/legislation/ILCS/details"
            f"?ChapAct={urllib.parse.quote(f'{chapter_num_from_text}+ILCS+{act_num}%2F')}"
            f"&ActID={act_id}"
            f"&ChapterID={chapter_id}"
            f"&SeqStart=&"
            f"&ChapAct=FullText"
        )

        act_node = Node(
            id=node_id,
            link=articles_url,
            top_level_title=chapter_node.top_level_title,
            node_type="structure",
            level_classifier="act",
            number=act_num,
            node_name=node_name,
            parent=chapter_node.node_id,
            status=status,
        )
        insert_node(act_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)

        if not status:
            _scrape_sections(act_node, act_id, chapter_id, chapter_num_from_text, act_num)


# ---------------------------------------------------------------------------
# Act -> Sections (via FullText)
# ---------------------------------------------------------------------------


def _scrape_sections(
    act_node: Node,
    act_id: str,
    chapter_id: str,
    chapter_num: str,
    act_num: str,
) -> None:
    """
    Fetch the FullText page for an act and parse each section block.

    Section blocks are wrapped in <table align="center"><tr><td>
      <div align="justify"> containing <code> elements.
    Article-heading blocks (containing "Art. N heading") are skipped.
    """
    full_text_url = (
        f"{BASE}/legislation/ILCS/details"
        f"?ActID={act_id}"
        f"&ChapterID={chapter_id}"
        f"&SeqStart="
        f"&ChapAct=FullText"
    )

    try:
        soup = get_url_as_soup(full_text_url)
    except Exception as exc:
        print(
            f"[il] could not fetch FullText for ActID={act_id}: {exc}",
            flush=True,
        )
        return

    scale = soup.find("div", class_="billtext-scale")
    if scale is None:
        # Fallback: some acts load directly in billtext-host without scale
        scale = soup.find("div", class_="billtext-host")
    if scale is None:
        return

    tables = scale.find_all("table", attrs={"align": "center"})
    if not tables:
        # Single-section acts use billtext-scale > div[align=justify] directly
        inner = scale.find("div", attrs={"align": "justify"})
        if inner:
            tables = [inner]

    for table in tables:
        _parse_section_block(table, act_node, chapter_num, act_num, full_text_url)


def _parse_section_block(
    block: Tag,
    act_node: Node,
    chapter_num: str,
    act_num: str,
    base_url: str,
) -> None:
    """
    Parse one section table block and insert a Node if it is a proper section
    (not an article heading).

    Layout inside each block:
      code[0]  : "(5 ILCS 70/0.01)"  -- citation
      code[1]  : "(from Ch. 1, par. 1000)"  -- optional historical ref
      code[N]  : "Sec. 0.01."   -- section number line
      code[N+1]: section name / body paragraphs
      code[-1] : "(Source: P.A. ...)"  -- source note
    """
    # Get the inner div[align=justify] if block is a <table>
    if block.name == "table":
        td = block.find("td")
        inner = td.find("div", attrs={"align": "justify"}) if td else None
        if inner is None:
            return
    else:
        inner = block

    codes = [c.get_text() for c in inner.find_all("code")]
    texts = [_clean(c) for c in codes if _clean(c)]

    if not texts:
        return

    first = texts[0]

    # Match citation in first code block
    m_cite = _SEC_CITE_RE.match(first)
    if not m_cite:
        return

    sec_path = m_cite.group(3).strip()  # "0.01" or "1-1" or "Art. 5 heading"

    # Skip article headings
    if "heading" in sec_path.lower() or "art." in sec_path.lower():
        return

    # The section number from the path (best-guess from citation).
    sec_number_default = sec_path

    # Determine status from section name or body
    full_text = " ".join(texts)
    status = _check_reserved(full_text)

    sec_url = f"{base_url}#{urllib.parse.quote(first)}"

    if status:
        node_id = f"{act_node.node_id}/section={sec_number_default}"
        citation = f"{chapter_num} ILCS {act_num}/{sec_number_default}"
        section_node = Node(
            id=node_id,
            link=sec_url,
            top_level_title=act_node.top_level_title,
            node_type="content",
            level_classifier="section",
            number=sec_number_default,
            node_name=citation,
            parent=act_node.node_id,
            citation=citation,
            status=status,
        )
        insert_node(section_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)
        return

    # ------------------------------------------------------------------
    # Version splitting.
    # A section with multiple effective-dated bodies has the structure:
    #   (N ILCS X/Y)              <- citation (texts[0])
    #   (from Ch. ..., par. ...)  <- optional legacy ref
    #   (Text of Section before amendment by P.A. 103-XYZ)
    #   Sec. Y. body text...
    #   (Source: P.A. ...)
    #   (Text of Section after amendment by P.A. 103-XYZ)
    #   Sec. Y. body text...
    #   (Source: P.A. ...)
    # When 0 version markers: single version (the whole tail).
    # When >=1 markers: each marker starts a new version slice.
    # Each version becomes its own node with id `.../section=Y::v1` etc.,
    # with the version label preserved in node_text as the leading paragraph.
    # ------------------------------------------------------------------
    version_indices = [i for i, t in enumerate(texts) if _VERSION_RE.match(t)]

    if not version_indices:
        slices = [(None, texts)]
    else:
        slices = []
        for n, start in enumerate(version_indices):
            end = version_indices[n + 1] if n + 1 < len(version_indices) else len(texts)
            label = texts[start]
            # carry citation/legacy ref + the slice body (start..end)
            head = [texts[0]]
            # include legacy ref token if present (texts[1] starts with "(from Ch.")
            if len(texts) > 1 and texts[1].startswith("(from Ch."):
                head.append(texts[1])
            slices.append((label, head + texts[start:end]))

    for idx, (version_label, slice_texts) in enumerate(slices, start=1):
        # Resolve section number from the slice's own "Sec. N." line.
        sec_number = sec_number_default
        for t in slice_texts[1:]:
            m_sec = _SEC_NUM_RE.match(t)
            if m_sec:
                sec_number = m_sec.group(1)
                break

        citation = f"{chapter_num} ILCS {act_num}/{sec_number}"
        node_id = f"{act_node.node_id}/section={sec_number}"
        if version_label is not None:
            node_id = f"{node_id}::v{idx}"

        node_text, addendum = _build_node_text(slice_texts, version_label=version_label)

        section_node = Node(
            id=node_id,
            link=sec_url,
            top_level_title=act_node.top_level_title,
            node_type="content",
            level_classifier="section",
            number=sec_number,
            node_name=citation,
            parent=act_node.node_id,
            citation=citation,
            node_text=node_text,
            addendum=addendum,
        )
        insert_node(section_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)


# ---------------------------------------------------------------------------
# Text extraction helpers
# ---------------------------------------------------------------------------


def _build_node_text(
    texts: list[str],
    version_label: Optional[str] = None,
) -> tuple[Optional[NodeText], Optional[Addendum]]:
    """
    Convert a list of cleaned code-block strings from a section block into
    NodeText + optional Addendum.

    Skips:
      - The first token (ILCS citation).
      - "(from Ch. N, par. N)" legacy reference tokens.
      - "(Text of Section ...)" version markers (caller emits them via
        ``version_label`` as the leading paragraph instead, so the version
        header is preserved per-node rather than dropped).
      - "Sec. N." section number tokens.
    Splits:
      - Source notes (starting with "(Source:") -> addendum.source.
    """
    node_text = NodeText()
    source_parts: list[str] = []
    in_source = False

    if version_label:
        # Preserve the version banner as the first paragraph so downstream
        # readers see which effective-dated body this node represents.
        node_text.add_paragraph(text=version_label)

    # Skip tokens that are metadata (citation/legacy ref/version marker/Sec line).
    skip_re = re.compile(
        r"^(\(\d+\s+ILCS|\(from\s+Ch\.|\(Text of Section\b|Sec\.\s+[0-9][\w\.\-]*\.)"
    )

    for i, t in enumerate(texts):
        if i == 0:
            # citation token
            continue
        if skip_re.match(t):
            continue
        if _SOURCE_RE.match(t):
            in_source = True
        if in_source:
            source_parts.append(t)
        else:
            node_text.add_paragraph(text=t)

    addendum: Optional[Addendum] = None
    if source_parts:
        addendum = Addendum()
        addendum.source = AddendumType(
            type="source", text=" ".join(source_parts)
        )

    if not node_text.paragraphs:
        node_text = None  # type: ignore[assignment]

    return node_text, addendum


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------


def _check_reserved(text: str) -> Optional[str]:
    lower = text.lower()
    for kw in RESERVED_KEYWORDS:
        if kw in lower:
            return "reserved"
    return None


def _clean(raw: str) -> str:
    text = raw.replace("\xa0", " ").replace("​", "")
    text = re.sub(r"\s+", " ", text).strip()
    return text


# ---------------------------------------------------------------------------
# CLI entry
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    main()
