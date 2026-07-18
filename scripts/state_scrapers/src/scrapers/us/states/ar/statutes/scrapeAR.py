"""Arkansas Code Annotated scraper.

Source: Wayback Machine snapshots of `law.justia.com/codes/arkansas/2023/`
(via `https://web.archive.org/web/<ts>id_/<original>`).

Why Wayback Justia
------------------
The previous source `law.onecle.com/arkansas/` returns 403 across all section
URLs (Onecle has been hardening against scraping since 2024). The natural
fallbacks were each evaluated and rejected:

* `arkleg.state.ar.us` / `www.arkleg.state.ar.us`  -- TCP connect timeouts from
  all tested egress (the Arkansas General Assembly host appears to geo-fence or
  drop non-residential ASNs). Verified via `vaquill_pipeline.http_client.fetch_html`
  on 2026-05-11.
* `law.justia.com/codes/arkansas/...` direct       -- HTTP 403 (Cloudflare bot
  challenge) on every section URL, even with browser User-Agent + Referer headers.
* `casetext.com/statute/arkansas-code-of-1987`      -- HTTP 410 Gone (Thomson
  Reuters retired the free statute viewer after acquiring Casetext).
* `codes.findlaw.com/ar/...`                        -- TOC and section HTML
  return 200 but the substantive section text is rendered client-side by Next.js;
  only the WordPress shell ships in HTML and the `/codes-api/wp-json/` route
  only exposes `alabama` as a public custom post type (the AR data is fetched
  through a private/internal endpoint).
* `sos.arkansas.gov`                                -- the Secretary of State
  site does not publish the AR Code; it only hosts session/election material.
* `arkansascode.lexisnexis.com` / `advance.lexis.com` -- behind paywalled JS
  shell; the public landing page returns ~3KB of bootstrap with no code body.

The Wayback Machine has full-page snapshots of Justia AR Code captured by
Common Crawl. Using the `id_` flag (raw original) we get unmodified HTML with
real section text in the page body. Coverage is title-/section-level and is
NOT 100% (CDX shows ~370 saved URLs under `title-1/*` but only ~20 under
`title-11/*` as of 2026-05-11). When a section snapshot is missing we record
a structure node only and continue. This is good enough for the v1 corpus and
much fresher than the frozen 2016 Onecle mirror.

Hierarchy preserved: title -> [subtitle ->] chapter -> [subchapter ->] section.
Citation: ``Ark. Code Ann. § <NUMBER>``.

Resume + parallelism mirrors scrapeDE.py / scrapeAK.py: title-level done file
under SETTINGS.chunks_dir, ThreadPoolExecutor sized by VAQUILL_TITLE_WORKERS
(default 8). HTTP-pool reuse comes for free through
`vaquill_pipeline.http_client` (which underpins `get_url_as_soup`).
"""

from __future__ import annotations

import json
import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Project-root bootstrap (mirrors DE/AK)
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
JURISDICTION = "ar"
CORPUS = "statutes"
TABLE_NAME = f"{COUNTRY}_{JURISDICTION}_{CORPUS}"

# Year of the Justia Arkansas Code edition mirrored on Wayback. 2023 has the
# densest CDX coverage; 2024 has only a handful of captures as of 2026-05.
JUSTIA_YEAR = "2023"
JUSTIA_BASE = f"https://law.justia.com/codes/arkansas/{JUSTIA_YEAR}"

WAYBACK_PREFIX = "https://web.archive.org/web/2025id_/"  # raw snapshot, 2025-ish
WAYBACK_CDX = "https://web.archive.org/cdx/search/cdx"

# AR Code titles are numbered 1..28 (no Title 13 gaps after the 1987 codification
# reshuffle; reserved titles still render as structure-only).
ALL_TITLE_NUMBERS: list[int] = list(range(1, 29))

RESERVED_KEYWORDS = ["repealed", "reserved", "expired", "renumbered"]

# Section path regex against the original Justia URL (post-CDX), e.g.:
#   /codes/arkansas/2023/title-11/chapter-10/subchapter-2/section-11-10-210/
# Subtitle and subchapter levels are OPTIONAL.
_SECTION_PATH_RE = re.compile(
    r"^/codes/arkansas/" + re.escape(JUSTIA_YEAR) + r"/"
    r"title-(?P<title>[\w\-]+)/"
    r"(?:subtitle-(?P<subtitle>[\w\-]+)/)?"
    r"chapter-(?P<chapter>[\w\-]+)/"
    r"(?:subchapter-(?P<subchapter>[\w\-]+)/)?"
    r"section-(?P<section>[\w\-]+)/?$"
)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    corpus_node: Node = insert_jurisdiction_and_corpus_node(COUNTRY, JURISDICTION, CORPUS)
    _scrape_all_titles(corpus_node)


# ---------------------------------------------------------------------------
# Title-level resume bookkeeping (mirrors DE/AK)
# ---------------------------------------------------------------------------

def _titles_done_path() -> Path:
    try:
        from vaquill_pipeline.config import SETTINGS  # type: ignore
        return SETTINGS.chunks_dir / "state_ar_titles_done.txt"
    except Exception:
        return Path(__file__).parent / "state_ar_titles_done.txt"


def _load_titles_done() -> set[str]:
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


# ---------------------------------------------------------------------------
# Title discovery + dispatch
# ---------------------------------------------------------------------------

def _scrape_all_titles(corpus_node: Node) -> None:
    titles_done = (
        set() if os.environ.get("VAQUILL_FORCE_RESCRAPE") else _load_titles_done()
    )
    if titles_done:
        print(
            f"[scrapeAR] resume: {len(titles_done)} titles already done: "
            f"{sorted(titles_done, key=lambda x: int(x) if x.isdigit() else 999)}",
            flush=True,
        )

    work: list[tuple[Node, int]] = []
    for title_int in ALL_TITLE_NUMBERS:
        title_num = str(title_int)

        title_node = Node(
            id=f"{corpus_node.node_id}/title={title_num}",
            link=f"{JUSTIA_BASE}/title-{title_num}/",
            top_level_title=title_num,
            node_type="structure",
            level_classifier="title",
            number=title_num,
            node_name=f"Title {title_num}",
            parent=corpus_node.node_id,
        )
        insert_node(title_node, TABLE_NAME, ignore_duplicate=True, debug_mode=False)

        if title_num in titles_done:
            continue
        work.append((title_node, title_int))

    def _do_title(item: tuple[Node, int]) -> tuple[str, str, Optional[str]]:
        title_node, title_int = item
        try:
            _scrape_title(title_node, title_int)
            _mark_title_done(title_node.number)
            return (title_node.number, "ok", None)
        except Exception as exc:
            return (title_node.number, "fail", str(exc)[:200])

    workers = int(os.environ.get("VAQUILL_TITLE_WORKERS", "8"))
    print(
        f"[scrapeAR] running {len(work)} titles with {workers} parallel workers",
        flush=True,
    )
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for fut in as_completed(ex.submit(_do_title, item) for item in work):
            num, status, err = fut.result()
            tag = "ok" if status == "ok" else f"fail: {err}"
            print(f"[scrapeAR] title {num}: {tag}", flush=True)


# ---------------------------------------------------------------------------
# Per-title scrape: pull section URL list from CDX, then walk each.
# ---------------------------------------------------------------------------

def _scrape_title(title_node: Node, title_int: int) -> None:
    section_urls = _list_section_urls_for_title(title_int)
    if not section_urls:
        print(f"[scrapeAR] title {title_int}: no CDX captures found", flush=True)
        return

    # Track structural nodes already inserted so we don't re-insert per section.
    inserted: set[str] = set()

    for orig_url in section_urls:
        path = orig_url[len("https://law.justia.com") :] if orig_url.startswith("https") else orig_url
        m = _SECTION_PATH_RE.match(path)
        if not m:
            continue

        title = m.group("title")
        subtitle = m.group("subtitle")
        chapter = m.group("chapter")
        subchapter = m.group("subchapter")
        section_num = m.group("section")

        # Build parent chain, inserting any new structural ancestors first.
        parent_node = title_node

        if subtitle:
            st_id = f"{parent_node.node_id}/subtitle={subtitle}"
            if st_id not in inserted:
                st_node = Node(
                    id=st_id,
                    link=f"{JUSTIA_BASE}/title-{title}/subtitle-{subtitle}/",
                    top_level_title=title_node.top_level_title,
                    node_type="structure",
                    level_classifier="subtitle",
                    number=subtitle,
                    node_name=f"Subtitle {subtitle}",
                    parent=parent_node.node_id,
                )
                insert_node(st_node, TABLE_NAME, ignore_duplicate=True, debug_mode=False)
                inserted.add(st_id)
                parent_node = st_node
            else:
                parent_node = _shell_node(st_id, title_node.top_level_title)

        ch_id = f"{parent_node.node_id}/chapter={chapter}"
        if ch_id not in inserted:
            ch_node = Node(
                id=ch_id,
                link=_chapter_url(title, subtitle, chapter),
                top_level_title=title_node.top_level_title,
                node_type="structure",
                level_classifier="chapter",
                number=chapter,
                node_name=f"Chapter {chapter}",
                parent=parent_node.node_id,
            )
            insert_node(ch_node, TABLE_NAME, ignore_duplicate=True, debug_mode=False)
            inserted.add(ch_id)
            parent_node = ch_node
        else:
            parent_node = _shell_node(ch_id, title_node.top_level_title)

        if subchapter:
            sch_id = f"{parent_node.node_id}/subchapter={subchapter}"
            if sch_id not in inserted:
                sch_node = Node(
                    id=sch_id,
                    link=_subchapter_url(title, subtitle, chapter, subchapter),
                    top_level_title=title_node.top_level_title,
                    node_type="structure",
                    level_classifier="subchapter",
                    number=subchapter,
                    node_name=f"Subchapter {subchapter}",
                    parent=parent_node.node_id,
                )
                insert_node(sch_node, TABLE_NAME, ignore_duplicate=True, debug_mode=False)
                inserted.add(sch_id)
                parent_node = sch_node
            else:
                parent_node = _shell_node(sch_id, title_node.top_level_title)

        _scrape_section(
            parent_node=parent_node,
            title_top=title_node.top_level_title,
            section_num=section_num,
            orig_url=orig_url,
        )


def _list_section_urls_for_title(title_int: int) -> list[str]:
    """Return the de-duplicated list of Justia section URLs for a title.

    Uses the Wayback CDX API with a wildcard scoped to the title prefix and
    collapses on urlkey so we get one row per distinct section URL.

    NOTE: Wayback's CDX endpoint returns 503 when called with the full
    browser-impersonation header set used by `vaquill_pipeline.http_client`
    (Sec-Ch-Ua / Sec-Fetch-* tokens look bot-like to their WAF on the API
    endpoint, even though they're fine on `/web/`). We use a plain requests
    call with a minimal curl-style UA here, with manual backoff.
    """
    import time as _time
    import requests as _requests

    prefix = f"law.justia.com/codes/arkansas/{JUSTIA_YEAR}/title-{title_int}/"
    query = (
        f"{WAYBACK_CDX}?url={prefix}*"
        "&filter=statuscode:200&filter=mimetype:text/html"
        "&collapse=urlkey&output=json&fl=original"
    )
    rows: list[list[str]] = []
    last_err: Optional[str] = None
    for attempt in range(1, 6):
        try:
            resp = _requests.get(
                query,
                headers={"User-Agent": "curl/8.4.0", "Accept": "*/*"},
                timeout=60,
            )
            if resp.status_code == 200 and resp.text.strip():
                rows = json.loads(resp.text)
                break
            last_err = f"HTTP {resp.status_code}"
        except Exception as exc:  # noqa: BLE001
            last_err = str(exc)[:160]
        _time.sleep(min(2 ** attempt, 30))
    if not rows:
        print(
            f"[scrapeAR] CDX fetch failed for title {title_int}: {last_err}",
            flush=True,
        )
        return []

    seen: set[str] = set()
    out: list[str] = []
    for row in rows[1:]:  # skip header row
        url = row[0]
        # Only keep URLs that match this exact title (CDX prefix is substring-y).
        if not re.search(
            rf"/codes/arkansas/{JUSTIA_YEAR}/title-{title_int}(/|$)", url
        ):
            continue
        # Drop trailing query strings (`?current=1` etc).
        url = url.split("?", 1)[0]
        if not url.endswith("/"):
            url = url + "/"
        if "/section-" not in url:
            continue
        if url in seen:
            continue
        seen.add(url)
        out.append(url)
    return out


def _chapter_url(title: str, subtitle: Optional[str], chapter: str) -> str:
    base = f"{JUSTIA_BASE}/title-{title}/"
    if subtitle:
        base += f"subtitle-{subtitle}/"
    return base + f"chapter-{chapter}/"


def _subchapter_url(
    title: str, subtitle: Optional[str], chapter: str, subchapter: str
) -> str:
    return _chapter_url(title, subtitle, chapter) + f"subchapter-{subchapter}/"


def _shell_node(node_id: str, top_level_title: str) -> Node:
    """Lightweight reference to an already-inserted ancestor.

    insert_node has already persisted the real Node; we only need its node_id
    as a parent reference downstream. Constructing a synthetic Node lets the
    descendant node carry the proper `parent=` linkage without a DB read.
    """
    return Node(
        id=node_id,
        top_level_title=top_level_title,
        node_type="structure",
        level_classifier="placeholder",
        number="",
        node_name="",
        parent=None,
    )


# ---------------------------------------------------------------------------
# Section-level scrape
# ---------------------------------------------------------------------------

def _scrape_section(
    parent_node: Node,
    title_top: str,
    section_num: str,
    orig_url: str,
) -> None:
    node_id = f"{parent_node.node_id}/section={section_num}"
    citation = f"Ark. Code Ann. § {section_num}"
    wb_url = f"{WAYBACK_PREFIX}{orig_url}"

    node_text, addendum, status, node_name = _fetch_section_content(
        wb_url, section_num
    )

    section_node = Node(
        id=node_id,
        link=orig_url,
        top_level_title=title_top,
        node_type="content",
        level_classifier="section",
        number=section_num,
        node_name=node_name or f"§ {section_num}",
        parent=parent_node.node_id,
        citation=citation,
        node_text=node_text,
        addendum=addendum,
        status=status,
    )
    insert_node(section_node, TABLE_NAME, ignore_duplicate=True, debug_mode=False)


def _fetch_section_content(
    wb_url: str, section_num: str
) -> tuple[Optional[NodeText], Optional[Addendum], Optional[str], Optional[str]]:
    """Fetch a Justia section page via Wayback and parse body + history.

    Justia section layout (stable across the 2023 capture set):

        <h1>Section N-N-NNN - Name</h1>
        <div class="codes-listing"> ... body paragraphs ... </div>
        <p><em>... Disclaimers/links ...</em></p>

    History/source notes are not consistently exposed by Justia (they cite
    "Universal Citation: AR Code § ..." but the legislative history is
    omitted from the free tier). We surface what's there as an addendum when
    present and otherwise leave it empty.
    """
    try:
        soup = get_url_as_soup(wb_url)
    except Exception as exc:
        print(f"[scrapeAR] section fetch failed {section_num}: {exc}", flush=True)
        return None, None, None, None

    node_name: Optional[str] = None
    h1 = soup.find("h1")
    if h1 is not None:
        raw_h1 = _clean_text(h1.get_text(separator=" "))
        # Strip the "Section N-N-NNN - " prefix; what remains is the name.
        m = re.match(rf"^Section\s+{re.escape(section_num)}\s*[-–—]\s*(.+)$", raw_h1)
        if m:
            node_name = m.group(1).strip()
        else:
            node_name = raw_h1

    status: Optional[str] = _check_reserved(node_name or "")

    # The substantive code text lives inside the main column. Justia wraps it
    # in `<div class="codes-listing">` historically; on newer captures it sits
    # under a `<div class="-pad-l">`-style container right after the citation
    # block. Strategy: grab everything after the `<h1>` until the first
    # disclaimer-marker `<p>` ("Disclaimer:" or "Make your practice...").
    node_text = NodeText()
    if h1 is not None:
        body_paragraphs: list[str] = []
        for sib in h1.next_elements:
            tag = getattr(sib, "name", None)
            if tag is None:
                continue
            if tag == "p":
                text = _clean_text(sib.get_text(separator=" "))
                low = text.lower()
                if low.startswith("disclaimer") or "make your practice" in low:
                    break
                if not text:
                    continue
                # Skip Justia chrome ("Previous Next", breadcrumbs, etc.)
                if text in {"Previous Next", "Previous", "Next"}:
                    continue
                if text.startswith("Universal Citation:"):
                    continue
                body_paragraphs.append(text)
            elif tag in {"h2", "h3"}:
                # Subsection heading inside a section
                text = _clean_text(sib.get_text(separator=" "))
                if text:
                    body_paragraphs.append(text)
        # De-dup adjacent paragraphs (Justia sometimes duplicates breadcrumbs)
        last = None
        for p in body_paragraphs:
            if p == last:
                continue
            node_text.add_paragraph(text=p)
            last = p

    # Detect repealed bodies even when h1 didn't reveal it.
    # NodeText.paragraphs is Dict[str, Paragraph]; iterate values.
    flat = " ".join(p.text for p in node_text.paragraphs.values()).lower()
    if not status and re.search(r"\[\s*repealed\b", flat):
        status = "reserved"

    addendum: Optional[Addendum] = None
    # Try to find the "Acts ..." / "History" trailer paragraph if present.
    for p in node_text.paragraphs.values():
        if re.match(r"^(Acts|History|Source)\b", p.text):
            addendum = Addendum()
            addendum.history = AddendumType(type="history", text=p.text)
            break

    if not node_text.paragraphs:
        return None, addendum, status, node_name

    return node_text, addendum, status, node_name


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
    if not raw:
        return ""
    text = raw.replace("\xa0", " ").replace("​", "")
    text = re.sub(r"\s+", " ", text).strip()
    return text


# ---------------------------------------------------------------------------
# CLI entry
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    main()
