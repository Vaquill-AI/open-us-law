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
JURISDICTION = "nm"
CORPUS = "statutes"
TABLE_NAME = f"{COUNTRY}_{JURISDICTION}_{CORPUS}"

BASE_URL = "https://codes.findlaw.com"
TOC_URL = f"{BASE_URL}/nm/"

RESERVED_KEYWORDS = [
    "(repealed)",
    "(expired)",
    "(reserved)",
    "(renumbered)",
    "(deleted)",
]


def main() -> None:
    # NOTE: run_state.py already calls patch.install(state_code="nm") and opens
    # the JsonlSink at state_nm_statutes.jsonl. Calling install() again here
    # with a different state_code (previously "nm_statutes") clobbers the
    # active sink and routes chunks to state_nm_statutes_statutes.jsonl, so
    # the user-visible output file looks empty. Do not re-install here.
    corpus_node: Node = insert_jurisdiction_and_corpus_node(COUNTRY, JURISDICTION, CORPUS)
    scrape_all_chapters(corpus_node)


# ---------------------------------------------------------------------------
# Chapter-level resume (persist completed chapter numbers)
# ---------------------------------------------------------------------------


def _chapters_done_path() -> Path:
    from vaquill_pipeline.config import SETTINGS
    return SETTINGS.chunks_dir / "state_nm_chapters_done.txt"


def _load_chapters_done() -> set:
    path = _chapters_done_path()
    if not path.exists():
        return set()
    return {line.strip() for line in path.read_text().splitlines() if line.strip()}


def _mark_chapter_done(number: str) -> None:
    path = _chapters_done_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a") as fh:
        fh.write(f"{number}\n")
        fh.flush()


# ---------------------------------------------------------------------------
# Top-level: chapter list + parallel scrape
# ---------------------------------------------------------------------------


def scrape_all_chapters(corpus_node: Node) -> None:
    chapters_done = (
        set() if os.environ.get("VAQUILL_FORCE_RESCRAPE") else _load_chapters_done()
    )
    if chapters_done:
        print(
            f"[scrapeNM] resume: {len(chapters_done)} chapters already done",
            flush=True,
        )

    soup = get_url_as_soup(TOC_URL)
    work: List[Tuple[Node, str]] = []
    seen: set = set()
    for link_tag in soup.find_all("a", href=True):
        href = link_tag["href"].strip()
        if not re.match(r"https://codes\.findlaw\.com/nm/chapter-([\dA-Za-z]+)-", href):
            continue
        if href in seen:
            continue
        seen.add(href)

        node_name = _clean_text(link_tag.get_text())
        if not node_name:
            continue

        ch_number = _chapter_number_from_url(href)
        if ch_number is None:
            continue

        node_id = f"{corpus_node.node_id}/chapter={ch_number}"
        status = _check_reserved(node_name)

        chapter_node = Node(
            id=node_id,
            link=href,
            top_level_title=ch_number,
            node_type="structure",
            level_classifier="chapter",
            number=ch_number,
            node_name=node_name,
            parent=corpus_node.node_id,
            status=status,
        )
        insert_node(chapter_node, TABLE_NAME, ignore_duplicate=True, debug_mode=False)

        if status or ch_number in chapters_done:
            continue
        work.append((chapter_node, href))

    workers = int(os.environ.get("VAQUILL_TITLE_WORKERS", "8"))
    print(
        f"[scrapeNM] running {len(work)} chapters with {workers} parallel workers",
        flush=True,
    )

    def _do_chapter(item: Tuple[Node, str]):
        chapter_node, chapter_url = item
        try:
            scrape_chapter(chapter_node, chapter_url)
            _mark_chapter_done(chapter_node.number)
            return (chapter_node.number, "ok", None)
        except Exception as e:
            return (chapter_node.number, "fail", str(e)[:200])

    with ThreadPoolExecutor(max_workers=workers) as ex:
        for fut in as_completed(ex.submit(_do_chapter, item) for item in work):
            num, status, err = fut.result()
            if status == "fail":
                print(f"[scrapeNM] chapter {num}: {status}: {err}", flush=True)
            else:
                print(f"[scrapeNM] chapter {num}: {status}", flush=True)


# ---------------------------------------------------------------------------
# Chapter: extract article list, then scrape each article's sections
# ---------------------------------------------------------------------------


def scrape_chapter(chapter_node: Node, chapter_url: str) -> None:
    soup = get_url_as_soup(chapter_url)
    articles = _extract_categories(soup)

    if not articles:
        _scrape_article_sections(chapter_node, chapter_node.number, "1", chapter_url)
        return

    for art_title in articles:
        art_number = _article_number_from_title(art_title)
        if art_number is None:
            continue

        art_url_slug = art_number.lower()
        node_id = f"{chapter_node.node_id}/article={art_number}"
        status = _check_reserved(art_title)

        first_sec_url = (
            f"{BASE_URL}/nm/{_chapter_url_slug(chapter_url)}/"
            f"nm-st-sect-{chapter_node.number.lower()}-{art_url_slug}-1.html"
        )

        article_node = Node(
            id=node_id,
            link=first_sec_url,
            top_level_title=chapter_node.top_level_title,
            node_type="structure",
            level_classifier="article",
            number=art_number,
            node_name=art_title,
            parent=chapter_node.node_id,
            status=status,
        )
        insert_node(article_node, TABLE_NAME, ignore_duplicate=True, debug_mode=False)

        if not status:
            _scrape_article_sections(
                article_node,
                chapter_node.number,
                art_url_slug,
                chapter_url,
            )


# ---------------------------------------------------------------------------
# Walk sections within an article. CRITICAL: respect article boundary.
# ---------------------------------------------------------------------------


def _scrape_article_sections(
    parent_node: Node,
    chapter_number: str,
    article_slug: str,
    chapter_url: str,
) -> None:
    chapter_slug = _chapter_url_slug(chapter_url)
    first_url = (
        f"{BASE_URL}/nm/{chapter_slug}/"
        f"nm-st-sect-{chapter_number.lower()}-{article_slug}-1.html"
    )

    current_url: Optional[str] = first_url
    visited: set = set()

    # Track the article token from URL parts. Sections live at
    # nm-st-sect-{ch}-{art}-{sec}[-{sub}]. We bail if {art} no longer
    # matches article_slug. This is the article-boundary fix.
    ch_token = chapter_number.lower()
    art_token = article_slug.lower()

    while current_url:
        norm = _normalise_url(current_url)
        if norm in visited:
            break
        visited.add(norm)

        # Hard stop: left the chapter slug entirely.
        if f"/nm/{chapter_slug}/" not in norm:
            break

        # Article-boundary check from URL BEFORE fetching.
        url_parts = _url_section_parts(norm)
        if url_parts is None:
            break
        url_ch, url_art = url_parts[0], url_parts[1]
        if url_ch != ch_token:
            break
        if url_art != art_token:
            # We've walked into the next article. Stop. The next article
            # will be handled by its own scrape_chapter iteration.
            break

        try:
            soup = get_url_as_soup(norm)
        except Exception as exc:
            print(f"[SKIP] {norm}: {exc}", flush=True)
            break

        main_el = soup.find("main")
        if main_el is None:
            break

        sec_number = _section_number_from_url(norm)
        if sec_number is None:
            break

        node_name, node_text, addendum = _parse_section_content(main_el, sec_number)
        status = _check_reserved(node_name) if node_name else None
        citation = f"N.M. Stat. § {sec_number}"

        node_id = f"{parent_node.node_id}/section={sec_number}"

        section_node = Node(
            id=node_id,
            link=norm,
            top_level_title=parent_node.top_level_title,
            node_type="content",
            level_classifier="section",
            number=sec_number,
            node_name=node_name or f"§ {sec_number}",
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


def _parse_section_content(
    main_el: BeautifulSoup,
    sec_number: str,
) -> Tuple[Optional[str], Optional[NodeText], Optional[Addendum]]:
    h1 = main_el.find("h1")
    node_name = _clean_text(h1.get_text()) if h1 else None
    if node_name:
        node_name = re.sub(
            r"^New Mexico Statutes.*?§\s*[\d\-A-Za-z\.]+\.\s*", "", node_name
        ).strip()
        node_name = f"§ {sec_number}. {node_name}" if node_name else f"§ {sec_number}"

    codes_div = main_el.find("div", class_="codes-content")
    if codes_div is None:
        return node_name, None, None

    node_text = NodeText()
    history_parts: List[str] = []

    _SKIP_CLASSES = {"codes-controls", "cite-this-article", "wasThisHelpful"}
    for p in codes_div.find_all("p", recursive=True):
        skip = any(
            set(ancestor.get("class", [])) & _SKIP_CLASSES
            for ancestor in p.parents
            if hasattr(ancestor, "get")
        )
        if skip:
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

    if not node_text.paragraphs:
        node_text = None

    addendum = None
    if history_parts:
        addendum = Addendum()
        addendum.history = AddendumType(type="history", text=" ".join(history_parts))

    return node_name, node_text, addendum


def _get_next_url(main_el: BeautifulSoup) -> Optional[str]:
    for a in main_el.find_all("a", href=True):
        txt = a.get_text().strip()
        href = a["href"].strip()
        if "Next" in txt and "findlaw.com" in href and "/nm/" in href:
            return _normalise_url(href)
    return None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _extract_categories(soup: BeautifulSoup) -> List[str]:
    import json

    for script in soup.find_all("script"):
        txt = script.string or ""
        m = re.search(r"let categoriesContent\s*=\s*(\[.*?\]);", txt, re.DOTALL)
        if m:
            try:
                data = json.loads(m.group(1))
                return [item["title"] for item in data if "title" in item]
            except (json.JSONDecodeError, KeyError, TypeError):
                pass
    return []


def _chapter_number_from_url(url: str) -> Optional[str]:
    m = re.search(r"/nm/chapter-([\dA-Za-z]+)-", url)
    return m.group(1).upper() if m else None


def _chapter_url_slug(chapter_url: str) -> str:
    m = re.search(r"/nm/(chapter-[^/]+?)/?(?:$|[/?#])", chapter_url)
    return m.group(1) if m else ""


def _article_number_from_title(title: str) -> Optional[str]:
    # Fix: previous regex required a literal `.` or whitespace AFTER the
    # number, so titles like "Article 5" with no trailing space or with
    # different punctuation would fail. Use word-boundary anchor.
    m = re.match(r"\s*Article\s+([\dA-Za-z]+)\b", title)
    return m.group(1).upper() if m else None


def _url_section_parts(url: str) -> Optional[List[str]]:
    """Return the raw lowercase parts after `nm-st-sect-`. Used for
    article-boundary detection.

    nm-st-sect-1-2-3.html       -> ['1','2','3']
    nm-st-sect-30-1-1-1.html    -> ['30','1','1','1']
    nm-st-sect-1-3a-2.html      -> ['1','3a','2']
    """
    m = re.search(r"nm-st-sect-([\da-z][-\da-z]+?)(?:\.html|/?$)", url, re.IGNORECASE)
    if not m:
        return None
    return [p.lower() for p in m.group(1).split("-")]


def _section_number_from_url(url: str) -> Optional[str]:
    parts = _url_section_parts(url)
    if not parts:
        return None
    parts = [
        p.upper() if re.match(r"^\d+[a-z]+$|^[a-z]+\d*$", p, re.IGNORECASE) else p
        for p in parts
    ]
    if len(parts) == 4:
        return f"{parts[0]}-{parts[1]}-{parts[2]}.{parts[3]}"
    return "-".join(parts)


def _normalise_url(url: str) -> str:
    url = url.split("#")[0].split("?")[0].rstrip("/")
    if not url.endswith(".html"):
        url += ".html"
    return url


def _looks_like_history(text: str) -> bool:
    stripped = text.strip()
    if re.match(r"^\d{4}\s+(Comp\.|Laws|N\.?M\.?S\.?A?|Amendment|Act)", stripped):
        return True
    if re.match(
        r"^\(\s*(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\.?\s+\d", stripped
    ):
        return True
    if stripped.startswith("History:") or stripped.startswith("Cross References"):
        return True
    return False


def _check_reserved(text: Optional[str]) -> Optional[str]:
    if not text:
        return None
    lower = text.lower()
    for kw in RESERVED_KEYWORDS:
        if kw in lower:
            return "reserved"
    return None


def _clean_text(raw: str) -> str:
    text = raw.replace("\xa0", " ").replace(" ", " ").replace(" ", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


if __name__ == "__main__":
    main()
