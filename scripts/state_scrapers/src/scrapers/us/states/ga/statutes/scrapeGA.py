"""Georgia Official Code Annotated (O.C.G.A.) scraper.

Source: https://codes.findlaw.com/ga/  (Thomson Reuters Findlaw mirror)

Why Findlaw rather than Onecle/Justia: Onecle (law.onecle.com) and Justia
(law.justia.com) both return HTTP 403 from our IPs/residential proxy as of
2026-05. Findlaw is reachable directly through vaquill_pipeline.http_client
and publishes a complete sitemap of every section URL.

Why sitemap-driven rather than crawling the TOC: the Findlaw title / chapter
pages are rendered client-side (the section links are injected by JS and are
not present in the raw HTML). The section pages themselves are server-rendered
and contain the full statute text. Two sitemap groups list every section URL:

    https://codes.findlaw.com/sitemapcodes/v2/ga/sitemap{1..N}.xml
    https://codes.findlaw.com/sitemapcodes/v3/ga/sitemap{1..N}.xml

URL patterns:
    /ga/title-{N}-{slug}/ga-code-sect-{N}-{M}-{S}/
    /ga/title-{N}-{slug}/ga-code-{N}-{M}-{S}/                 (older form)
    /ga/constitution-of-the-state-of-georgia/ga-const-art-{X}-sect-{Y}/

Hierarchy reconstructed from each section URL:
    Title  -> Chapter -> Section
where Title number = first dotted component, Chapter number = second dotted
component. Findlaw does not surface Article / Part labels server-side, so we
collapse to Title>Chapter>Section. Citations remain the canonical
"O.C.G.A. § {N}-{M}-{S}" form (which is the source of truth for OCGA).

Node ID path:
    us/ga/statutes/title={N}/chapter={M}/section={N}-{M}-{S}

Vaquill integration:
    * vaquill_pipeline.patch.install() for r2_sync + JsonlSink in insert_node.
    * Title-level ThreadPoolExecutor (VAQUILL_TITLE_WORKERS, default 8).
    * Title-level resume via state_ga_titles_done.txt
      (VAQUILL_FORCE_RESCRAPE=1 overrides).
    * Section fetch parallelism per title via VAQUILL_SECTION_WORKERS (default 4).

No Selenium required. Pure HTTP + BeautifulSoup via fetch_html / get_url_as_soup.
"""

from __future__ import annotations

import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional, Tuple

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

try:
    from vaquill_pipeline import patch as _vaquill_patch
    _vaquill_patch.install()
except Exception as _e:  # noqa: BLE001
    print(f"[warn] vaquill_pipeline.patch.install() skipped: {_e}", flush=True)

# Findlaw sitemaps return raw XML which is text/plain enough for fetch_html.
from vaquill_pipeline.http_client import fetch_html

COUNTRY = "us"
JURISDICTION = "ga"
CORPUS = "statutes"
TABLE_NAME = f"{COUNTRY}_{JURISDICTION}_{CORPUS}"

BASE = "https://codes.findlaw.com"
SITEMAP_VARIANTS = ("v2", "v3")
SITEMAP_MAX_PAGES = 30  # safety cap; current count is ~6 per variant

RESERVED_KEYWORDS = [
    "(reserved)",
    "(repealed)",
    "(expired)",
    "(renumbered)",
    "(deleted)",
    "reserved.",
]

# Regex for section URLs. Examples:
#   https://codes.findlaw.com/ga/title-16-crimes-and-offenses/ga-code-sect-16-10-53/
#   https://codes.findlaw.com/ga/title-33-insurance/ga-code-33-20d-1/
RE_SECTION_URL = re.compile(
    r"^https://codes\.findlaw\.com/ga/title-(\d+[a-z]?)-([\w\-]+)/"
    r"ga-code(?:-sect)?-([\w\-]+)/?$",
    re.IGNORECASE,
)

# Section number must be dotted-style, at least N-M-S where each component is
# digits or digits+letter (e.g. 10A, 16, 20D). Reject anything that does not
# fit so we ignore stray sitemap entries.
RE_SECTION_NUMBER = re.compile(r"^\d+[a-z]?-\w+(?:-[\w\.]+)+$", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    corpus_node: Node = insert_jurisdiction_and_corpus_node(COUNTRY, JURISDICTION, CORPUS)
    _scrape_all_titles(corpus_node)


# ---------------------------------------------------------------------------
# Sitemap discovery
# ---------------------------------------------------------------------------


def _enumerate_section_urls() -> List[str]:
    """Walk the Findlaw GA sitemaps and return all unique section URLs."""
    urls: set[str] = set()
    for variant in SITEMAP_VARIANTS:
        for page in range(1, SITEMAP_MAX_PAGES + 1):
            sm_url = f"{BASE}/sitemapcodes/{variant}/ga/sitemap{page}.xml"
            try:
                xml = fetch_html(sm_url, max_retries=3, timeout=30)
            except Exception as e:  # noqa: BLE001
                print(f"[scrapeGA] sitemap {variant}/{page} fetch failed: {e!s}", flush=True)
                break
            locs = re.findall(r"<loc>([^<]+)</loc>", xml)
            if not locs:
                # empty urlset -> end of pagination for this variant
                break
            urls.update(locs)
    # Keep only statute section URLs. Drop the constitution (separate corpus)
    # and any non-section pages.
    return sorted(u for u in urls if RE_SECTION_URL.match(u))


def _group_by_title(urls: List[str]) -> Dict[str, Dict[str, str]]:
    """Return {title_number: {"slug": title_slug, "name": title_node_name}}."""
    titles: Dict[str, Dict[str, str]] = {}
    for url in urls:
        m = RE_SECTION_URL.match(url)
        if not m:
            continue
        title_number = m.group(1).upper()
        title_slug = m.group(2)
        if title_number not in titles:
            titles[title_number] = {
                "slug": title_slug,
                "name": _title_name_from_slug(title_number, title_slug),
            }
    return titles


def _title_name_from_slug(number: str, slug: str) -> str:
    """Turn 'crimes-and-offenses' + '16' into 'Title 16. Crimes And Offenses'."""
    pretty = slug.replace("-", " ").strip().title().replace(" And ", " and ").replace(" Of ", " of ")
    return f"Title {number}. {pretty}"


# ---------------------------------------------------------------------------
# Resume bookkeeping (title-level)
# ---------------------------------------------------------------------------


def _titles_done_path():
    from vaquill_pipeline.config import SETTINGS
    return SETTINGS.chunks_dir / "state_ga_titles_done.txt"


def _load_titles_done() -> set:
    try:
        path = _titles_done_path()
    except Exception:
        return set()
    if not path.exists():
        return set()
    return {ln.strip() for ln in path.read_text().splitlines() if ln.strip()}


def _mark_title_done(number: str) -> None:
    try:
        path = _titles_done_path()
    except Exception:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a") as fh:
        fh.write(f"{number}\n")
        fh.flush()


# ---------------------------------------------------------------------------
# Top-level driver
# ---------------------------------------------------------------------------


def _scrape_all_titles(corpus_node: Node) -> None:
    print("[scrapeGA] enumerating sitemap section URLs", flush=True)
    section_urls = _enumerate_section_urls()
    print(f"[scrapeGA] {len(section_urls)} candidate section URLs", flush=True)
    if not section_urls:
        raise RuntimeError("Findlaw GA sitemaps returned zero section URLs")

    titles = _group_by_title(section_urls)

    # Bucket sections per title for downstream workers.
    sections_by_title: Dict[str, List[str]] = {}
    for url in section_urls:
        m = RE_SECTION_URL.match(url)
        if not m:
            continue
        sections_by_title.setdefault(m.group(1).upper(), []).append(url)

    titles_done = (
        set() if os.environ.get("VAQUILL_FORCE_RESCRAPE") else _load_titles_done()
    )
    if titles_done:
        print(f"[scrapeGA] resume: {len(titles_done)} titles already done", flush=True)

    # Insert Title structure nodes (idempotent) before parallel section work
    # so child nodes always have an existing parent row.
    title_nodes: Dict[str, Node] = {}
    for number, meta in sorted(titles.items(), key=lambda kv: _sort_key(kv[0])):
        node_id = f"{corpus_node.node_id}/title={number}"
        title_node = Node(
            id=node_id,
            link=f"{BASE}/ga/title-{number.lower()}-{meta['slug']}/",
            top_level_title=number,
            node_type="structure",
            level_classifier="title",
            number=number,
            node_name=meta["name"],
            parent=corpus_node.node_id,
        )
        insert_node(title_node, TABLE_NAME, ignore_duplicate=True, debug_mode=False)
        title_nodes[number] = title_node

    work = [
        (num, title_nodes[num], sections_by_title.get(num, []))
        for num in title_nodes
        if num not in titles_done
    ]

    workers = int(os.environ.get("VAQUILL_TITLE_WORKERS", "8"))
    print(
        f"[scrapeGA] running {len(work)} titles with {workers} parallel workers",
        flush=True,
    )

    def _do_title(item: Tuple[str, Node, List[str]]):
        num, tnode, urls = item
        try:
            _scrape_title(tnode, urls)
            _mark_title_done(num)
            return (num, "ok", len(urls), None)
        except Exception as e:  # noqa: BLE001
            return (num, "fail", len(urls), str(e)[:200])

    with ThreadPoolExecutor(max_workers=workers) as ex:
        for fut in as_completed(ex.submit(_do_title, w) for w in work):
            num, status, count, err = fut.result()
            if status == "fail":
                print(f"[scrapeGA] title {num} ({count} secs): {status}: {err}", flush=True)
            else:
                print(f"[scrapeGA] title {num} ({count} secs): {status}", flush=True)


def _sort_key(title_number: str):
    """Sort '1','2',...'10','10A','11',... naturally."""
    m = re.match(r"^(\d+)([A-Z]?)$", title_number.upper())
    if not m:
        return (9_999, title_number)
    return (int(m.group(1)), m.group(2))


# ---------------------------------------------------------------------------
# Per-title scrape: insert Chapter nodes + each Section
# ---------------------------------------------------------------------------


def _scrape_title(title_node: Node, section_urls: List[str]) -> None:
    # First pass: bucket section URLs by chapter (chapter = 2nd dotted token of
    # the section number). Examples:
    #   16-10-53     -> chapter 10
    #   33-20D-3     -> chapter 20D
    #   1-1-3.1      -> chapter 1
    chapters: Dict[str, List[Tuple[str, str]]] = {}
    for url in section_urls:
        m = RE_SECTION_URL.match(url)
        if not m:
            continue
        sec_number = m.group(3)
        if not RE_SECTION_NUMBER.match(sec_number):
            continue
        parts = sec_number.split("-")
        if len(parts) < 3:
            continue
        # parts[0] == title number (may include letter, e.g. 10A). Ensure it
        # matches this title; otherwise skip (sitemap stragglers).
        if parts[0].upper() != title_node.number.upper():
            continue
        chapter_number = parts[1].upper()
        chapters.setdefault(chapter_number, []).append((sec_number, url))

    section_workers = int(os.environ.get("VAQUILL_SECTION_WORKERS", "4"))

    for chapter_number in sorted(chapters, key=_chapter_sort_key):
        ch_node_id = f"{title_node.node_id}/chapter={chapter_number}"
        chapter_node = Node(
            id=ch_node_id,
            link=title_node.link,
            top_level_title=title_node.top_level_title,
            node_type="structure",
            level_classifier="chapter",
            number=chapter_number,
            node_name=f"Chapter {chapter_number}",
            parent=title_node.node_id,
        )
        insert_node(chapter_node, TABLE_NAME, ignore_duplicate=True, debug_mode=False)

        # Section fetches: small thread pool per chapter.
        tasks = chapters[chapter_number]
        with ThreadPoolExecutor(max_workers=section_workers) as ex:
            futs = [
                ex.submit(_scrape_section, chapter_node, sec_number, url)
                for sec_number, url in tasks
            ]
            for fut in as_completed(futs):
                # Errors inside _scrape_section are swallowed so one bad
                # section doesn't poison the whole chapter.
                try:
                    fut.result()
                except Exception as e:  # noqa: BLE001
                    print(f"[scrapeGA] section worker error: {e!s}", flush=True)


def _chapter_sort_key(ch: str):
    m = re.match(r"^(\d+)([A-Z]?)$", ch.upper())
    if not m:
        return (9_999, ch)
    return (int(m.group(1)), m.group(2))


# ---------------------------------------------------------------------------
# Section page parsing
# ---------------------------------------------------------------------------


def _scrape_section(chapter_node: Node, sec_number: str, url: str) -> None:
    node_id = f"{chapter_node.node_id}/section={sec_number}"
    citation = f"O.C.G.A. § {sec_number}"

    node_name, node_text, addendum = _fetch_section_content(url)

    status = _check_reserved(node_name or "") or _check_reserved(
        (" ".join(node_text.to_list_text()) if node_text else "")
    )

    section_node = Node(
        id=node_id,
        link=url,
        top_level_title=chapter_node.top_level_title,
        node_type="content",
        level_classifier="section",
        number=sec_number,
        node_name=node_name or f"§ {sec_number}",
        parent=chapter_node.node_id,
        citation=citation,
        node_text=node_text if node_text and node_text.paragraphs else None,
        addendum=addendum,
        status=status,
    )
    insert_node(section_node, TABLE_NAME, ignore_duplicate=True, debug_mode=False)


def _fetch_section_content(
    url: str,
) -> Tuple[Optional[str], Optional[NodeText], Optional[Addendum]]:
    """Parse a Findlaw GA section page.

    Returns (node_name, NodeText | None, Addendum | None). node_name comes from
    the H1, which has the form: "Georgia Code Title X. Foo § N-M-S".
    """
    try:
        soup = get_url_as_soup(url)
    except Exception:
        return None, None, None

    # 1) H1 -> node name (we keep the full "Title X. Foo § N-M-S" string).
    h1 = soup.find("h1")
    raw_name = _clean_text(h1.get_text()) if h1 else ""
    node_name = raw_name

    # 2) Statute body. Findlaw wraps the content in a div whose class contains
    #    "codes-content" / "codes-section-content" depending on layout version.
    body = (
        soup.select_one("div.codes-content")
        or soup.select_one("div.codes-section-content")
        or soup.select_one("div.codes")
        or soup.select_one("main")
    )
    node_text = NodeText()
    history_text = ""
    if body is not None:
        for el in body.find_all(["p", "div"], recursive=True):
            classes = " ".join(el.get("class") or [])
            text = _clean_text(el.get_text(separator=" "))
            if not text:
                continue
            # Findlaw appends a "Cite this article" / "FindLaw.com" footer; drop.
            if text.startswith("Cite this article"):
                break
            if text.startswith("FindLaw Codes may not reflect"):
                break
            if "credit" in classes.lower() or "history" in classes.lower():
                history_text = text
                continue
            # Skip pure navigation lines.
            if text in ("Previous Part of Code", "Next Part of Code", "Back to Chapter List", "Copy"):
                continue
            node_text.add_paragraph(text=text)

    addendum: Optional[Addendum] = None
    if history_text:
        addendum = Addendum()
        addendum.history = AddendumType(type="history", text=history_text)

    return node_name or None, (node_text if node_text.paragraphs else None), addendum


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


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
