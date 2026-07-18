"""Pennsylvania statutes scraper.

Audit context
-------------
PA was flagged CRITICAL by the US ingestion audit because the previous scraper
targeted onecle.com (now 403 across the board). A subsequent migration pointed
at palegis.us, but a live-fire check on 2026-05-12 confirmed that the entire
www.palegis.us / www.legis.state.pa.us / palrb.us host family is unreachable
from our infrastructure (direct connect timeouts, residential-proxy returns
404). With no working official source we fall back to the Thomson Reuters
Findlaw mirror (codes.findlaw.com/pa/), which is the same mechanism already
used in production for GA, AL, MS, NC, WY.

Source: https://codes.findlaw.com/pa/
        sitemap groups: /sitemapcodes/v2/pa/sitemap{1..N}.xml
                        /sitemapcodes/v3/pa/sitemap{1..N}.xml

Findlaw's title / chapter pages are rendered client-side (chapter links are
injected by JS and absent from the raw HTML). Section pages are server-rendered
and contain the full statute text. We therefore enumerate every section URL
from the sitemap and reconstruct Title -> Chapter -> Section structure from
the URL itself.

URL patterns
------------
Consolidated Statutes (Pa.C.S.A. -- Title 1-78 Pa.C.S.):
    /pa/title-{N}-pacsa-{slug}/pa-csa-sect-{N}-{S}/
    -> citation: "{N} Pa.C.S. § {S}"

Constitution (optional, controlled by VAQUILL_PA_INCLUDE_CONSTITUTION=1, on by default):
    /pa/constitution-of-the-commonwealth-of-pennsylvania/pa-const-art-{X}-(sect|preamble)-{Y}/

Unconsolidated Statutes (P.S. session laws, optional, off by default --
VAQUILL_PA_INCLUDE_UNCONSOLIDATED=1):
    /pa/title-{N}-ps-{slug}/pa-st-sect-{N}-{S}/

Vaquill integration mirrors GA / AL: vaquill_pipeline.patch.install() routes
insert_node through r2_sync + JsonlSink. Title-level ThreadPoolExecutor with
VAQUILL_TITLE_WORKERS (default 8), section-level VAQUILL_SECTION_WORKERS
(default 4). Title-level resume via state_pa_titles_done.txt;
VAQUILL_FORCE_RESCRAPE=1 overrides.
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

from vaquill_pipeline.http_client import fetch_html

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
COUNTRY = "us"
JURISDICTION = "pa"
CORPUS = "statutes"
TABLE_NAME = f"{COUNTRY}_{JURISDICTION}_{CORPUS}"

BASE = "https://codes.findlaw.com"
SITEMAP_VARIANTS = ("v2", "v3")
SITEMAP_MAX_PAGES = 30  # safety cap; live count is ~5 per variant

RESERVED_KEYWORDS = [
    "(reserved)",
    "(repealed)",
    "(expired)",
    "(renumbered)",
    "(deleted)",
    "reserved.",
]

# Feature gates for optional corpus pieces.
INCLUDE_UNCONSOLIDATED = os.environ.get("VAQUILL_PA_INCLUDE_UNCONSOLIDATED") == "1"
INCLUDE_CONSTITUTION = os.environ.get("VAQUILL_PA_INCLUDE_CONSTITUTION", "1") == "1"

# Section URL regex. Examples:
#   https://codes.findlaw.com/pa/title-18-pacsa-crimes-and-offenses/pa-csa-sect-18-2701/
#   https://codes.findlaw.com/pa/title-1-pacsa-general-provisions/pa-csa-sect-1-1101/
RE_PACSA_URL = re.compile(
    r"^https://codes\.findlaw\.com/pa/title-(\d+)-pacsa-([\w\-]+)/"
    r"pa-csa-sect-(\d+)-([\w\.\-]+)/?$",
    re.IGNORECASE,
)

#   https://codes.findlaw.com/pa/title-10-ps-charities-and-welfare/pa-st-sect-10-1/
RE_PS_URL = re.compile(
    r"^https://codes\.findlaw\.com/pa/title-(\d+)-ps-([\w\-]+)/"
    r"pa-st-sect-(\d+)-([\w\.\-]+)/?$",
    re.IGNORECASE,
)

#   https://codes.findlaw.com/pa/constitution-of-the-commonwealth-of-pennsylvania/pa-const-art-1-sect-1/
#   https://codes.findlaw.com/pa/constitution-of-the-commonwealth-of-pennsylvania/pa-const-art-1-preamble/
RE_CONST_URL = re.compile(
    r"^https://codes\.findlaw\.com/pa/constitution-of-the-commonwealth-of-pennsylvania/"
    r"pa-const-art-([\w\-]+?)-(?:sect-([\w\.\-]+)|preamble)/?$",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main() -> None:
    corpus_node: Node = insert_jurisdiction_and_corpus_node(COUNTRY, JURISDICTION, CORPUS)
    print("[scrapePA] source: codes.findlaw.com (sitemap-driven)", flush=True)

    print("[scrapePA] enumerating sitemap section URLs", flush=True)
    all_locs = _enumerate_section_urls()
    pacsa = [u for u in all_locs if RE_PACSA_URL.match(u)]
    ps = [u for u in all_locs if RE_PS_URL.match(u)]
    const = [u for u in all_locs if RE_CONST_URL.match(u)]
    print(
        f"[scrapePA] sitemap totals: pacsa={len(pacsa)} ps={len(ps)} const={len(const)}",
        flush=True,
    )
    if not pacsa:
        raise RuntimeError("Findlaw PA sitemaps returned zero Pa.C.S.A. section URLs")

    # Consolidated Statutes -- the legally required piece.
    _scrape_pacsa(corpus_node, pacsa)

    # Optional: Constitution.
    if INCLUDE_CONSTITUTION and const:
        _scrape_constitution(corpus_node, const)
    elif not INCLUDE_CONSTITUTION:
        print("[scrapePA] Constitution skipped (VAQUILL_PA_INCLUDE_CONSTITUTION=0)", flush=True)

    # Optional: Unconsolidated (P.S.).
    if INCLUDE_UNCONSOLIDATED and ps:
        _scrape_unconsolidated(corpus_node, ps)
    else:
        print(
            "[scrapePA] WARNING: Unconsolidated Statutes (P.S.) SKIPPED. Set "
            "VAQUILL_PA_INCLUDE_UNCONSOLIDATED=1 to include them. Corpus is "
            "INCOMPLETE without it.",
            flush=True,
        )


# ---------------------------------------------------------------------------
# Sitemap discovery
# ---------------------------------------------------------------------------
def _enumerate_section_urls() -> List[str]:
    urls: set = set()
    for variant in SITEMAP_VARIANTS:
        for page in range(1, SITEMAP_MAX_PAGES + 1):
            sm_url = f"{BASE}/sitemapcodes/{variant}/pa/sitemap{page}.xml"
            try:
                xml = fetch_html(sm_url, max_retries=3, timeout=30)
            except Exception as e:  # noqa: BLE001
                print(f"[scrapePA] sitemap {variant}/{page} fetch failed: {e!s}", flush=True)
                break
            locs = re.findall(r"<loc>([^<]+)</loc>", xml)
            if not locs:
                break
            urls.update(locs)
    return sorted(urls)


# ---------------------------------------------------------------------------
# Resume bookkeeping
# ---------------------------------------------------------------------------
def _titles_done_path() -> Path:
    try:
        from vaquill_pipeline.config import SETTINGS  # type: ignore
        return SETTINGS.chunks_dir / "state_pa_titles_done.txt"
    except Exception:
        return Path(__file__).parent / "state_pa_titles_done.txt"


def _load_titles_done() -> set:
    path = _titles_done_path()
    if not path.exists():
        return set()
    return {ln.strip() for ln in path.read_text().splitlines() if ln.strip()}


def _mark_title_done(key: str) -> None:
    path = _titles_done_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a") as fh:
        fh.write(f"{key}\n")
        fh.flush()


# ---------------------------------------------------------------------------
# Pa.C.S.A. (Consolidated)
# ---------------------------------------------------------------------------
def _scrape_pacsa(corpus_node: Node, section_urls: List[str]) -> None:
    # Bucket by title number; capture slug for title node.
    titles: Dict[str, str] = {}
    sections_by_title: Dict[str, List[Tuple[str, str]]] = {}
    for url in section_urls:
        m = RE_PACSA_URL.match(url)
        if not m:
            continue
        tnum = m.group(1).lstrip("0") or "0"
        slug = m.group(2)
        sec_num = m.group(4)
        # Section number on Findlaw uses {title}-{section}; canonical PA cite
        # only uses the section component.
        titles.setdefault(tnum, slug)
        sections_by_title.setdefault(tnum, []).append((sec_num, url))

    titles_done = (
        set() if os.environ.get("VAQUILL_FORCE_RESCRAPE") else _load_titles_done()
    )
    if titles_done:
        print(f"[scrapePA] resume: {len(titles_done)} titles already done", flush=True)

    # Insert title structure nodes idempotently.
    title_nodes: Dict[str, Node] = {}
    for tnum in sorted(titles, key=_title_sort_key):
        slug = titles[tnum]
        node_id = f"{corpus_node.node_id}/title={tnum}"
        title_node = Node(
            id=node_id,
            link=f"{BASE}/pa/title-{tnum}-pacsa-{slug}/",
            top_level_title=tnum,
            node_type="structure",
            level_classifier="title",
            number=tnum,
            node_name=f"Title {tnum} Pa.C.S.A. {_pretty_slug(slug)}",
            parent=corpus_node.node_id,
        )
        insert_node(title_node, TABLE_NAME, ignore_duplicate=True, debug_mode=False)
        title_nodes[tnum] = title_node

    work = [
        (tnum, title_nodes[tnum], sections_by_title.get(tnum, []))
        for tnum in title_nodes
        if f"pacsa:{tnum}" not in titles_done
    ]

    workers = int(os.environ.get("VAQUILL_TITLE_WORKERS", "8"))
    print(
        f"[scrapePA] pacsa: running {len(work)} titles with {workers} workers",
        flush=True,
    )

    def _do_title(item):
        tnum, tnode, urls = item
        try:
            _scrape_pacsa_title(tnode, urls)
            _mark_title_done(f"pacsa:{tnum}")
            return (tnum, "ok", len(urls), None)
        except Exception as e:  # noqa: BLE001
            return (tnum, "fail", len(urls), str(e)[:200])

    with ThreadPoolExecutor(max_workers=workers) as ex:
        for fut in as_completed(ex.submit(_do_title, w) for w in work):
            num, status, count, err = fut.result()
            if status == "fail":
                print(f"[scrapePA] title {num} ({count} secs): fail: {err}", flush=True)
            else:
                print(f"[scrapePA] title {num} ({count} secs): ok", flush=True)


def _scrape_pacsa_title(title_node: Node, section_tuples: List[Tuple[str, str]]) -> None:
    """Insert chapters (derived from section number prefix) + each section.

    For PA, Findlaw section numbers are dotted like "1-101", "18-2701",
    "75-1543.1". The first dotted token is the title number; the second token
    starts with the chapter (e.g. section 2701 belongs to chapter 27). We
    derive chapter = leading digits of the section component (before the
    trailing two-digit section number). When that heuristic is unsafe (very
    short numbers like "101", "5"), bucket under chapter "0" so no content is
    dropped.
    """
    chapters: Dict[str, List[Tuple[str, str]]] = {}
    for sec_num, url in section_tuples:
        chapter_number = _derive_chapter_from_section(sec_num)
        chapters.setdefault(chapter_number, []).append((sec_num, url))

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

        with ThreadPoolExecutor(max_workers=section_workers) as ex:
            futs = [
                ex.submit(_scrape_section, chapter_node, sec_num, url, "pacsa")
                for sec_num, url in chapters[chapter_number]
            ]
            for fut in as_completed(futs):
                try:
                    fut.result()
                except Exception as e:  # noqa: BLE001
                    print(f"[scrapePA] section worker error: {e!s}", flush=True)


def _derive_chapter_from_section(sec_num: str) -> str:
    """Heuristic: PA section numbers are usually ChapterNN (e.g. 2701 -> ch 27,
    1543 -> ch 15, 101 -> ch 1). Take all leading digits and drop the trailing
    two, with a floor of one digit. Falls back to "0" if format is unexpected.
    """
    m = re.match(r"^(\d+)", sec_num)
    if not m:
        return "0"
    digits = m.group(1)
    if len(digits) <= 2:
        # very short section numbers (e.g. "1", "5", "25") -- treat the whole
        # thing as chapter for grouping.
        return digits
    return digits[:-2]


# ---------------------------------------------------------------------------
# Constitution
# ---------------------------------------------------------------------------
def _scrape_constitution(corpus_node: Node, const_urls: List[str]) -> None:
    const_node_id = f"{corpus_node.node_id}/title=Constitution"
    const_node = Node(
        id=const_node_id,
        link=f"{BASE}/pa/constitution-of-the-commonwealth-of-pennsylvania/",
        top_level_title="Constitution",
        node_type="structure",
        level_classifier="title",
        number="Constitution",
        node_name="The Pennsylvania Constitution",
        parent=corpus_node.node_id,
    )
    insert_node(const_node, TABLE_NAME, ignore_duplicate=True, debug_mode=False)

    # Bucket by article.
    by_article: Dict[str, List[Tuple[str, str]]] = {}
    for url in const_urls:
        m = RE_CONST_URL.match(url)
        if not m:
            continue
        article = m.group(1)
        sec = m.group(2) or "preamble"
        by_article.setdefault(article, []).append((sec, url))

    section_workers = int(os.environ.get("VAQUILL_SECTION_WORKERS", "4"))
    for article in sorted(by_article, key=_chapter_sort_key):
        art_id = f"{const_node_id}/article={article}"
        art_node = Node(
            id=art_id,
            link=const_node.link,
            top_level_title="Constitution",
            node_type="structure",
            level_classifier="article",
            number=article,
            node_name=f"Article {article}",
            parent=const_node_id,
        )
        insert_node(art_node, TABLE_NAME, ignore_duplicate=True, debug_mode=False)

        with ThreadPoolExecutor(max_workers=section_workers) as ex:
            futs = [
                ex.submit(_scrape_section, art_node, sec, url, "const")
                for sec, url in by_article[article]
            ]
            for fut in as_completed(futs):
                try:
                    fut.result()
                except Exception as e:  # noqa: BLE001
                    print(f"[scrapePA] const section error: {e!s}", flush=True)


# ---------------------------------------------------------------------------
# Unconsolidated (P.S.)
# ---------------------------------------------------------------------------
def _scrape_unconsolidated(corpus_node: Node, ps_urls: List[str]) -> None:
    titles: Dict[str, str] = {}
    sections_by_title: Dict[str, List[Tuple[str, str]]] = {}
    for url in ps_urls:
        m = RE_PS_URL.match(url)
        if not m:
            continue
        tnum = m.group(1).lstrip("0") or "0"
        slug = m.group(2)
        sec_num = m.group(4)
        # Disambiguate P.S. titles from Pa.C.S.A. titles in the node tree.
        key = f"PS-{tnum}"
        titles.setdefault(key, (tnum, slug))  # type: ignore[arg-type]
        sections_by_title.setdefault(key, []).append((sec_num, url))

    title_nodes: Dict[str, Node] = {}
    for key in sorted(titles, key=lambda k: _title_sort_key(k.split("-")[1])):
        tnum, slug = titles[key]  # type: ignore[misc]
        node_id = f"{corpus_node.node_id}/title=PS-{tnum}"
        title_node = Node(
            id=node_id,
            link=f"{BASE}/pa/title-{tnum}-ps-{slug}/",
            top_level_title=f"PS-{tnum}",
            node_type="structure",
            level_classifier="title",
            number=f"PS-{tnum}",
            node_name=f"Title {tnum} P.S. {_pretty_slug(slug)}",
            parent=corpus_node.node_id,
        )
        insert_node(title_node, TABLE_NAME, ignore_duplicate=True, debug_mode=False)
        title_nodes[key] = title_node

    workers = int(os.environ.get("VAQUILL_TITLE_WORKERS", "8"))
    work = [
        (key, title_nodes[key], sections_by_title.get(key, []))
        for key in title_nodes
    ]
    print(
        f"[scrapePA] ps: running {len(work)} titles with {workers} workers",
        flush=True,
    )

    def _do_title(item):
        key, tnode, urls = item
        try:
            _scrape_ps_title(tnode, urls)
            _mark_title_done(key)
            return (key, "ok", len(urls), None)
        except Exception as e:  # noqa: BLE001
            return (key, "fail", len(urls), str(e)[:200])

    with ThreadPoolExecutor(max_workers=workers) as ex:
        for fut in as_completed(ex.submit(_do_title, w) for w in work):
            key, status, count, err = fut.result()
            if status == "fail":
                print(f"[scrapePA] {key} ({count} secs): fail: {err}", flush=True)
            else:
                print(f"[scrapePA] {key} ({count} secs): ok", flush=True)


def _scrape_ps_title(title_node: Node, section_tuples: List[Tuple[str, str]]) -> None:
    chapters: Dict[str, List[Tuple[str, str]]] = {}
    for sec_num, url in section_tuples:
        chapters.setdefault(_derive_chapter_from_section(sec_num), []).append((sec_num, url))

    section_workers = int(os.environ.get("VAQUILL_SECTION_WORKERS", "4"))
    for chapter_number in sorted(chapters, key=_chapter_sort_key):
        ch_id = f"{title_node.node_id}/chapter={chapter_number}"
        chapter_node = Node(
            id=ch_id,
            link=title_node.link,
            top_level_title=title_node.top_level_title,
            node_type="structure",
            level_classifier="chapter",
            number=chapter_number,
            node_name=f"Chapter {chapter_number}",
            parent=title_node.node_id,
        )
        insert_node(chapter_node, TABLE_NAME, ignore_duplicate=True, debug_mode=False)

        with ThreadPoolExecutor(max_workers=section_workers) as ex:
            futs = [
                ex.submit(_scrape_section, chapter_node, sec_num, url, "ps")
                for sec_num, url in chapters[chapter_number]
            ]
            for fut in as_completed(futs):
                try:
                    fut.result()
                except Exception as e:  # noqa: BLE001
                    print(f"[scrapePA] ps section error: {e!s}", flush=True)


# ---------------------------------------------------------------------------
# Section page fetch + parse (shared)
# ---------------------------------------------------------------------------
def _scrape_section(parent_node: Node, sec_number: str, url: str, kind: str) -> None:
    node_id = f"{parent_node.node_id}/section={sec_number}"
    if kind == "pacsa":
        # parent is a Chapter; title number is parent_node.top_level_title.
        citation = f"{parent_node.top_level_title} Pa.C.S. § {sec_number}"
    elif kind == "ps":
        tnum = parent_node.top_level_title.replace("PS-", "")
        citation = f"{tnum} P.S. § {sec_number}"
    else:  # const
        citation = (
            f"Pa. Const. art. {parent_node.number}, § {sec_number}"
            if sec_number != "preamble"
            else f"Pa. Const. art. {parent_node.number}, preamble"
        )

    node_name, node_text, addendum = _fetch_section_content(url)
    status = _check_reserved(node_name or "") or _check_reserved(
        " ".join(node_text.to_list_text()) if node_text else ""
    )

    section_node = Node(
        id=node_id,
        link=url,
        top_level_title=parent_node.top_level_title,
        node_type="content",
        level_classifier="section",
        number=sec_number,
        node_name=node_name or f"§ {sec_number}",
        parent=parent_node.node_id,
        citation=citation,
        node_text=node_text if node_text and node_text.paragraphs else None,
        addendum=addendum,
        status=status,
    )
    insert_node(section_node, TABLE_NAME, ignore_duplicate=True, debug_mode=False)


def _fetch_section_content(
    url: str,
) -> Tuple[Optional[str], Optional[NodeText], Optional[Addendum]]:
    try:
        soup = get_url_as_soup(url)
    except Exception:
        return None, None, None

    h1 = soup.find("h1")
    node_name = _clean_text(h1.get_text()) if h1 else ""

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
            if text.startswith("Cite this article"):
                break
            if text.startswith("FindLaw Codes may not reflect"):
                break
            if "credit" in classes.lower() or "history" in classes.lower():
                history_text = text
                continue
            if text in (
                "Previous Part of Code",
                "Next Part of Code",
                "Back to Chapter List",
                "Copy",
            ):
                continue
            node_text.add_paragraph(text=text)

    addendum: Optional[Addendum] = None
    if history_text:
        addendum = Addendum()
        addendum.history = AddendumType(type="history", text=history_text)

    return (
        node_name or None,
        (node_text if node_text.paragraphs else None),
        addendum,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _title_sort_key(t: str):
    m = re.match(r"^(\d+)([A-Z]?)$", t.upper())
    if not m:
        return (9_999, t)
    return (int(m.group(1)), m.group(2))


def _chapter_sort_key(ch: str):
    m = re.match(r"^(\d+)([A-Z]?)$", ch.upper())
    if not m:
        return (9_999, ch)
    return (int(m.group(1)), m.group(2))


def _pretty_slug(slug: str) -> str:
    pretty = slug.replace("-", " ").strip().title()
    return pretty.replace(" And ", " and ").replace(" Of ", " of ")


def _check_reserved(text: str) -> Optional[str]:
    lower = text.lower()
    for kw in RESERVED_KEYWORDS:
        if kw in lower:
            return "reserved"
    return None


def _clean_text(raw: str) -> str:
    if not raw:
        return ""
    text = raw.replace("\xa0", " ").replace(" ", " ").replace("’", "'")
    text = re.sub(r"\s+", " ", text).strip()
    return text


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    main()
