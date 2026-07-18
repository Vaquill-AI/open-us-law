import json
import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Optional, Tuple

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
JURISDICTION = "nj"
CORPUS = "statutes"
TABLE_NAME = f"{COUNTRY}_{JURISDICTION}_{CORPUS}"

BASE_URL = "https://codes.findlaw.com"
TOC_URL = f"{BASE_URL}/nj/"

RESERVED_KEYWORDS = [
    "(repealed)",
    "(expired)",
    "(reserved)",
    "(renumbered)",
    "(deleted)",
    "(vacant)",
]

# Boilerplate div classes to skip when extracting paragraph text
_SKIP_CLASSES = {"codes-controls", "cite-this-article", "wasThisHelpful"}


# ---------------------------------------------------------------------------
# Resume helpers (title-level)
# ---------------------------------------------------------------------------


def _titles_done_path():
    from vaquill_pipeline.config import SETTINGS
    return SETTINGS.chunks_dir / "state_nj_titles_done.txt"


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


def main() -> None:
    # Install vaquill_pipeline patches (JsonlSink, r2_sync, http_client routing
    # through get_url_as_soup) so this scraper emits chunks + R2 mirrors the
    # same way scrapeDE / scrapeFL do.
    try:
        from vaquill_pipeline import patch
        patch.install()
    except Exception as e:
        print(f"[scrapeNJ] patch.install skipped: {e}", flush=True)

    corpus_node: Node = insert_jurisdiction_and_corpus_node(COUNTRY, JURISDICTION, CORPUS)
    scrape_all_titles(corpus_node)


# ---------------------------------------------------------------------------
# Top-level: title list (parallel)
# ---------------------------------------------------------------------------


def scrape_all_titles(corpus_node: Node) -> None:
    """Walk the NJ TOC page and run titles in parallel with resume.

    Each title is independent. Concurrency via env VAQUILL_TITLE_WORKERS
    (default 8). Title-level resume via state_nj_titles_done.txt; set env
    VAQUILL_FORCE_RESCRAPE=1 to ignore prior completion markers.
    """
    from vaquill_pipeline.http_client import fetch_html

    titles_done = set() if os.environ.get("VAQUILL_FORCE_RESCRAPE") else _load_titles_done()
    if titles_done:
        print(
            f"[scrapeNJ] resume: {len(titles_done)} titles already done: "
            f"{sorted(titles_done)}",
            flush=True,
        )

    html = fetch_html(TOC_URL)

    # Title links look like: https://codes.findlaw.com/nj/title-1-acts-laws-and-statutes/
    title_urls = list(
        dict.fromkeys(
            re.findall(r"https://codes\.findlaw\.com/nj/title-[^\"\s<>]+/", html)
        )
    )

    work: List[Tuple[Node, str]] = []
    for title_url in title_urls:
        title_number = _title_number_from_url(title_url)
        if title_number is None:
            continue

        slug = _title_slug(title_url)
        title_name = _slug_to_title_name(slug, title_number)

        node_id = f"{corpus_node.node_id}/title={title_number}"
        status = _check_reserved(title_name)

        title_node = Node(
            id=node_id,
            link=title_url,
            top_level_title=title_number,
            node_type="structure",
            level_classifier="title",
            number=title_number,
            node_name=title_name,
            parent=corpus_node.node_id,
            status=status,
        )
        insert_node(title_node, TABLE_NAME, ignore_duplicate=True, debug_mode=False)

        if status or title_number in titles_done:
            continue
        work.append((title_node, title_number))

    def _do_title(item):
        title_node, number = item
        try:
            scrape_title(title_node)
            _mark_title_done(number)
            return (number, "ok", None)
        except Exception as e:
            return (number, "fail", f"{type(e).__name__}: {e}"[:200])

    workers = int(os.environ.get("VAQUILL_TITLE_WORKERS", "8"))
    print(
        f"[scrapeNJ] running {len(work)} titles with {workers} parallel workers",
        flush=True,
    )
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for fut in as_completed(ex.submit(_do_title, item) for item in work):
            num, status, err = fut.result()
            if status == "fail":
                print(f"[scrapeNJ] title {num}: {status}: {err}", flush=True)
            else:
                print(f"[scrapeNJ] title {num}: {status}", flush=True)


# ---------------------------------------------------------------------------
# Title: extract subtitle / chapter / article tree from embedded JS
# ---------------------------------------------------------------------------


def scrape_title(title_node: Node) -> None:
    """Fetch title page, extract its full classifier tree, then walk sections.

    NJ Revised Statutes use Title > Subtitle > Chapter > Article > Section.
    FindLaw embeds the structure as a flat list in `categoriesContent`, in
    document order, so a Subtitle row precedes its child Chapter rows, and
    an Article row (where present) precedes its child Section rows. We
    rebuild the hierarchy by tracking the current subtitle (and, within a
    chapter, the current article) as we scan the list.
    """
    from vaquill_pipeline.http_client import fetch_html

    title_url = str(title_node.link)
    html = fetch_html(title_url)
    entries = _extract_categories(html)

    if not entries:
        # No category list at all; try to walk sections directly from a guessed first URL.
        first_url = _build_section_url(title_url, title_node.number, "1", "1")
        _walk_sections(title_node, title_node.number, first_url)
        return

    current_subtitle: Optional[Node] = None
    current_chapter: Optional[Node] = None
    current_article: Optional[Node] = None

    for entry in entries:
        kind, raw_number = _classify_entry(entry)
        if kind is None or raw_number is None:
            continue

        status = _check_reserved(entry)

        if kind == "subtitle":
            parent = title_node
            node_id = f"{parent.node_id}/subtitle={raw_number}"
            current_subtitle = Node(
                id=node_id,
                link=title_url,
                top_level_title=title_node.top_level_title,
                node_type="structure",
                level_classifier="subtitle",
                number=raw_number,
                node_name=entry,
                parent=parent.node_id,
                status=status,
            )
            insert_node(current_subtitle, TABLE_NAME, ignore_duplicate=True, debug_mode=False)
            # Reset article context when we cross subtitle boundary.
            current_chapter = None
            current_article = None
            continue

        if kind == "chapter":
            parent = current_subtitle if current_subtitle is not None else title_node
            node_id = f"{parent.node_id}/chapter={raw_number}"
            current_chapter = Node(
                id=node_id,
                link=title_url,
                top_level_title=title_node.top_level_title,
                node_type="structure",
                level_classifier="chapter",
                number=raw_number,
                node_name=entry,
                parent=parent.node_id,
                status=status,
            )
            insert_node(current_chapter, TABLE_NAME, ignore_duplicate=True, debug_mode=False)
            current_article = None
            if not status:
                first_url = _build_section_url(
                    title_url, title_node.number, raw_number, "1"
                )
                _walk_sections(current_chapter, title_node.number, first_url)
            continue

        if kind == "article":
            # Article must hang off the most recent chapter; if none exists,
            # surface it under the subtitle/title rather than dropping it.
            parent = current_chapter or current_subtitle or title_node
            node_id = f"{parent.node_id}/article={raw_number}"
            current_article = Node(
                id=node_id,
                link=title_url,
                top_level_title=title_node.top_level_title,
                node_type="structure",
                level_classifier="article",
                number=raw_number,
                node_name=entry,
                parent=parent.node_id,
                status=status,
            )
            insert_node(current_article, TABLE_NAME, ignore_duplicate=True, debug_mode=False)
            # Articles in NJ usually share section URLs with their chapter,
            # so we do not walk separately; sections were already enqueued
            # via the chapter. If chapter is None we walk from article-1.
            if current_chapter is None and not status:
                first_url = _build_section_url(
                    title_url, title_node.number, raw_number, "1"
                )
                _walk_sections(current_article, title_node.number, first_url)
            continue

        if kind == "subchapter":
            parent = current_chapter or current_subtitle or title_node
            node_id = f"{parent.node_id}/subchapter={raw_number}"
            sc_node = Node(
                id=node_id,
                link=title_url,
                top_level_title=title_node.top_level_title,
                node_type="structure",
                level_classifier="subchapter",
                number=raw_number,
                node_name=entry,
                parent=parent.node_id,
                status=status,
            )
            insert_node(sc_node, TABLE_NAME, ignore_duplicate=True, debug_mode=False)
            continue


# ---------------------------------------------------------------------------
# Section walker: follow next-part links within a chapter
# ---------------------------------------------------------------------------


def _walk_sections(
    parent_node: Node,
    title_number: str,
    start_url: str,
) -> None:
    """Walk next-part links starting from start_url, stopping when we leave the title."""
    title_slug = title_number.lower()
    title_path_fragment = f"/nj/title-{title_slug}-"

    current_url: Optional[str] = _normalise_url(start_url)
    visited: set[str] = set()

    while current_url:
        norm = _normalise_url(current_url)

        if norm in visited:
            break
        visited.add(norm)

        if title_path_fragment not in norm and f"/nj/title-{title_slug}/" not in norm:
            break

        try:
            soup = get_url_as_soup(norm)
        except Exception as exc:
            print(f"[SKIP] {norm}: {exc}", flush=True)
            break

        main_el = soup.find("main") if soup else None
        if main_el is None:
            break

        h1 = main_el.find("h1")
        if h1 and "404" in h1.get_text():
            break

        sec_number = _section_number_from_url(norm, title_number)
        if sec_number is None:
            break

        citation = f"N.J. Stat. § {title_number.upper()}:{sec_number}"

        node_name, node_text, addendum = _parse_section(main_el, title_number, sec_number)
        status = _check_reserved(node_name or "")

        node_id = f"{parent_node.node_id}/section={sec_number}"

        section_node = Node(
            id=node_id,
            link=norm,
            top_level_title=parent_node.top_level_title,
            node_type="content",
            level_classifier="section",
            number=sec_number,
            node_name=node_name or f"§ {title_number.upper()}:{sec_number}",
            parent=parent_node.node_id,
            citation=citation,
            node_text=node_text if not status else None,
            addendum=addendum if not status else None,
            status=status,
        )
        insert_node(section_node, TABLE_NAME, ignore_duplicate=True, debug_mode=False)

        current_url = _get_next_url(main_el)


# ---------------------------------------------------------------------------
# Content parser
# ---------------------------------------------------------------------------


def _parse_section(
    main_el: BeautifulSoup,
    title_number: str,
    sec_number: str,
) -> tuple[Optional[str], Optional[NodeText], Optional[Addendum]]:
    """Extract node_name, NodeText, and Addendum from a FindLaw section page."""
    h1 = main_el.find("h1")
    node_name = _clean_text(h1.get_text()) if h1 else None
    if node_name:
        # Strip prefix: "New Jersey Statutes Title X. <Name> § N-N"
        node_name = re.sub(
            r"^New Jersey Statutes\s+.*?§\s*[\w.:\-]+\s*",
            "",
            node_name,
        ).strip()
        if not node_name:
            node_name = f"§ {title_number.upper()}:{sec_number}"

    codes_div = main_el.find("div", class_="codes-content")
    if codes_div is None:
        return node_name, None, None

    node_text = NodeText()
    history_parts: list[str] = []

    for p in codes_div.find_all("p", recursive=True):
        if _should_skip_paragraph(p):
            continue
        raw = p.get_text(separator=" ")
        text = _clean_text(raw)
        if not text:
            continue
        if text.startswith("FindLaw Codes may not reflect"):
            continue
        if _looks_like_history(text):
            history_parts.append(text)
        else:
            node_text.add_paragraph(text=text)

    final_text: Optional[NodeText] = node_text if node_text.paragraphs else None

    addendum: Optional[Addendum] = None
    if history_parts:
        addendum = Addendum()
        addendum.history = AddendumType(type="history", text=" ".join(history_parts))

    return node_name, final_text, addendum


def _should_skip_paragraph(p: BeautifulSoup) -> bool:
    """Return True if this paragraph lives inside a boilerplate container."""
    return any(
        set(ancestor.get("class", [])) & _SKIP_CLASSES
        for ancestor in p.parents
        if hasattr(ancestor, "get")
    )


def _get_next_url(main_el: BeautifulSoup) -> Optional[str]:
    for a in main_el.find_all("a", href=True):
        txt = a.get_text().strip()
        href = a["href"].strip()
        if "Next" in txt and "findlaw.com" in href and "/nj/" in href:
            return _normalise_url(href)
    return None


# ---------------------------------------------------------------------------
# URL / number helpers
# ---------------------------------------------------------------------------


def _build_section_url(title_url: str, title_number: str, chapter: str, section: str) -> str:
    title_slug = title_number.lower()
    chapter_slug = chapter.lower()
    section_slug = section.lower()
    slug = f"nj-st-sect-{title_slug}-{chapter_slug}-{section_slug}"
    base = title_url.rstrip("/")
    return f"{base}/{slug}/"


def _section_number_from_url(url: str, title_number: str) -> Optional[str]:
    m = re.search(r"nj-st-sect-([\da-zA-Z][-\da-zA-Z.]+?)(?:/|\.html|$)", url)
    if not m:
        return None

    raw = m.group(1)
    title_prefix = title_number.lower()

    if not raw.lower().startswith(title_prefix + "-"):
        return None

    remainder = raw[len(title_prefix) + 1:]
    parts = remainder.split("-")

    if len(parts) == 3:
        return f"{parts[0]}-{parts[1]}.{parts[2]}"
    if len(parts) == 2:
        return f"{parts[0]}-{parts[1]}"
    if len(parts) == 1:
        return parts[0]
    return "-".join(parts)


def _title_number_from_url(url: str) -> Optional[str]:
    # Allow trailing dash OR trailing slash (some title slugs are just `title-N/`).
    m = re.search(r"/nj/title-([\da-zA-Z]+)(?:[-/])", url)
    if m:
        return m.group(1).upper()
    return None


def _title_slug(url: str) -> str:
    m = re.search(r"/nj/(title-[^/]+)/?", url)
    return m.group(1) if m else ""


def _slug_to_title_name(slug: str, title_number: str) -> str:
    name = re.sub(r"^title-[\da-zA-Z]+-?", "", slug)
    name = name.replace("-", " ").title()
    return f"Title {title_number}. {name}" if name else f"Title {title_number}"


# ---------------------------------------------------------------------------
# Category entry classification
# ---------------------------------------------------------------------------

_CLASSIFIER_PATTERNS = [
    ("subtitle", re.compile(r"^Subtitle\s+([\dA-Za-z]+)[.\s]", re.IGNORECASE)),
    ("subchapter", re.compile(r"^Subchapter\s+([\dA-Za-z]+)[.\s]", re.IGNORECASE)),
    ("chapter", re.compile(r"^Chapter\s+([\dA-Za-z]+)[.\s]", re.IGNORECASE)),
    ("article", re.compile(r"^Article\s+([\dA-Za-z]+)[.\s]", re.IGNORECASE)),
]


def _classify_entry(entry: str) -> Tuple[Optional[str], Optional[str]]:
    """Return (level_classifier, number) for a categoriesContent title string.

    Examples:
      "Subtitle 1. The Courts" => ("subtitle", "1")
      "Chapter 12A. Law Revision" => ("chapter", "12A")
      "Article 1. General Provisions" => ("article", "1")
    """
    for kind, pat in _CLASSIFIER_PATTERNS:
        m = pat.match(entry)
        if m:
            return kind, m.group(1).upper()
    return None, None


def _extract_categories(html: str) -> list[str]:
    """Return entry title strings from the embedded categoriesContent JS variable.

    The previous implementation used a non-greedy `\\[.*?\\]` which truncated
    on the first `]` and corrupted nested-array payloads. We instead locate
    the assignment and bracket-match the surrounding JSON array.
    """
    anchor = re.search(r"let\s+categoriesContent\s*=\s*\[", html)
    if not anchor:
        return []
    start = anchor.end() - 1  # index of the opening '['
    depth = 0
    in_str = False
    esc = False
    str_ch = ""
    end = -1
    for i in range(start, len(html)):
        ch = html[i]
        if in_str:
            if esc:
                esc = False
            elif ch == "\\":
                esc = True
            elif ch == str_ch:
                in_str = False
            continue
        if ch in ('"', "'"):
            in_str = True
            str_ch = ch
            continue
        if ch == "[":
            depth += 1
        elif ch == "]":
            depth -= 1
            if depth == 0:
                end = i + 1
                break
    if end == -1:
        return []
    blob = html[start:end]
    try:
        data = json.loads(blob)
    except json.JSONDecodeError:
        # Fall back to a tolerant title-only regex if the JSON has trailing commas etc.
        return [m.group(1) for m in re.finditer(r'"title"\s*:\s*"([^"]+)"', blob)]
    out: list[str] = []
    for item in data:
        if isinstance(item, dict) and "title" in item:
            out.append(item["title"])
    return out


def _normalise_url(url: str) -> str:
    url = url.rstrip("/")
    if not url.endswith("/") and ".html" not in url:
        url += "/"
    return url


# ---------------------------------------------------------------------------
# Text utilities
# ---------------------------------------------------------------------------


def _looks_like_history(text: str) -> bool:
    stripped = text.strip()
    patterns = [
        r"^\d{4}\s+(L\.|c\.|Laws|Comp\.|Amendment|Act)",
        r"^(Amended|Repealed|Added|L\.\s*\d{4})",
        r"^History:\s*",
        r"^Source:\s*",
        r"^\(\s*(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\.?\s+\d",
    ]
    for pat in patterns:
        if re.match(pat, stripped, re.IGNORECASE):
            return True
    return False


def _check_reserved(text: str) -> Optional[str]:
    lower = text.lower()
    for kw in RESERVED_KEYWORDS:
        if kw in lower:
            return "reserved"
    return None


def _clean_text(raw: str) -> str:
    text = raw.replace("\xa0", " ").replace(" ", " ").replace("’", "'")
    text = re.sub(r"\s+", " ", text).strip()
    return text


if __name__ == "__main__":
    main()
