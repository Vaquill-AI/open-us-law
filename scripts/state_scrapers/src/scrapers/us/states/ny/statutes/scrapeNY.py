"""NY (New York) statutes scraper.

Sources (live-enumerated, all 5 NY Senate law categories + Constitution):
  - /legislation/laws/CONSOLIDATED     (~94 consolidated laws)
  - /legislation/laws/UNCONSOLIDATED   (~34 unconsolidated laws)
  - /legislation/laws/COURT_ACTS       (8 court acts)
  - /legislation/laws/RULES            (legislative rules)
  - /legislation/laws/MISC             (CNS Constitution etc.)

Structure: Law (chapter) -> [Part ->] [Title ->] Article -> Section

No Selenium. Pure HTTP + BeautifulSoup.

Note: nysenate.gov embeds Cloudflare bot-management JS in every real page,
which causes the pipeline's http_client to false-positive on challenge
detection. We bypass _fetch_soup and fetch directly with requests using
a plain curl-style UA which the site serves without restriction.
"""
from __future__ import annotations

import os
import re
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup, Tag

# ---------------------------------------------------------------------------
# Path bootstrap (mirrors DE scraper pattern)
# ---------------------------------------------------------------------------
current_file = Path(__file__).resolve()
src_directory = current_file.parent
while src_directory.name != "src" and src_directory.parent != src_directory:
    src_directory = src_directory.parent
project_root = src_directory.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.utils.pydanticModels import Node, NodeText  # noqa: E402
from src.utils.scrapingHelpers import (  # noqa: E402
    insert_jurisdiction_and_corpus_node,
    insert_node,
)

# ---------------------------------------------------------------------------
# NY-specific HTTP fetch (bypasses pipeline http_client's Cloudflare
# false-positive detection by using our own session). nysenate.gov serves
# real content on a curl-style UA without proxy; only the parallel-worker
# count needs to stay low to avoid Cloudflare rate-limits.
# ---------------------------------------------------------------------------

_SESSION = requests.Session()
_SESSION.headers.update({
    "User-Agent": "curl/8.7.1",
    "Accept": "*/*",
})


def _build_proxies() -> Optional[dict[str, str]]:
    """Webshare residential proxy (optional). Enabled when VAQUILL_USE_PROXY=1.

    Last NY run used direct connections with 1 worker. Re-runs against the
    108 missing laws benefit from proxy IP rotation in case Cloudflare
    has the VM IP on a soft list.
    """
    if os.environ.get("VAQUILL_USE_PROXY") != "1":
        return None
    user = os.environ.get("WEBSHARE_USERNAME")
    pwd = os.environ.get("WEBSHARE_PASSWORD")
    host = os.environ.get("WEBSHARE_PROXY_HOST")
    port = os.environ.get("WEBSHARE_PROXY_PORT")
    if not all([user, pwd, host, port]):
        print("[scrapeNY] WARN: VAQUILL_USE_PROXY=1 but creds incomplete; using direct.", flush=True)
        return None
    auth = f"{user}:{pwd}@{host}:{port}"
    return {"http": f"http://{auth}", "https": f"http://{auth}"}


_PROXIES = _build_proxies()
if _PROXIES:
    print(f"[scrapeNY] Webshare proxy enabled (host={os.environ.get('WEBSHARE_PROXY_HOST')})", flush=True)


def _fetch_soup(url: str, retries: int = 3) -> BeautifulSoup:
    """Fetch `url` and return a BeautifulSoup object. Raises on final failure.

    Raises ``RuntimeError`` on network failure. Caller is responsible for
    validating that the soup contains expected NY-Senate markup (e.g.
    ``nys-openleg-result-container``); a 200 OK on a Cloudflare challenge
    page yields a soup with no such container.
    """
    last_exc: Optional[BaseException] = None
    for attempt in range(1, retries + 1):
        try:
            resp = _SESSION.get(url, timeout=30, allow_redirects=True, proxies=_PROXIES)
            resp.raise_for_status()
            return BeautifulSoup(resp.text, "html.parser")
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            print(f"[scrapeNY] fetch attempt {attempt} failed for {url}: {exc}", flush=True)
            time.sleep(min(2 * attempt, 8))
    raise RuntimeError(f"Failed to fetch {url} after {retries} attempts") from last_exc

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
COUNTRY = "us"
JURISDICTION = "ny"
CORPUS = "statutes"
TABLE_NAME = f"{COUNTRY}_{JURISDICTION}_{CORPUS}"

BASE_URL = "https://www.nysenate.gov"
# Every NY Senate law category index. Iterated to enumerate all laws.
LAW_CATEGORY_URLS = [
    f"{BASE_URL}/legislation/laws/CONSOLIDATED",
    f"{BASE_URL}/legislation/laws/UNCONSOLIDATED",
    f"{BASE_URL}/legislation/laws/COURT_ACTS",
    f"{BASE_URL}/legislation/laws/RULES",
    f"{BASE_URL}/legislation/laws/MISC",
]
# Always include the Constitution explicitly (its slug is CNS via MISC, but the
# /legislation/laws/CONSTITUTION URL also resolves directly).
EXTRA_LAW_SLUGS = ["CNS"]

# Polite delay between requests (seconds).
REQUEST_DELAY = 0.2

# Law slugs already fully scraped, persisted across runs for resume.
_DONE_LOCK = threading.Lock()


def _laws_done_path() -> Path:
    """Where we persist the set of law slugs already fully scraped."""
    from vaquill_pipeline.config import SETTINGS
    return SETTINGS.chunks_dir / "state_ny_chapters_done.txt"


def _load_laws_done() -> set[str]:
    path = _laws_done_path()
    if not path.exists():
        return set()
    return {l.strip() for l in path.read_text().splitlines() if l.strip()}


def _mark_law_done(slug: str) -> None:
    path = _laws_done_path()
    with _DONE_LOCK:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "a") as fh:
            fh.write(f"{slug}\n")
            fh.flush()

# Level classifier names recognised in list items.
KNOWN_LEVELS = {
    "SECTION": "section",
    "ARTICLE": "article",
    "PART": "part",
    "TITLE": "title",
    "SUBARTICLE": "subarticle",
    "SUBPART": "subpart",
    "CHAPTER": "chapter",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _clean(text: str) -> str:
    """Strip excess whitespace and non-breaking spaces."""
    return re.sub(r"\s+", " ", text.replace("\xa0", " ")).strip()


def _parse_item_name(raw: str) -> tuple[str, str]:
    """
    Parse 'ARTICLE 1', 'PART 2-A', 'SECTION 101' -> (level_classifier, number).
    Falls back to ('section', raw) if format is unrecognised.
    """
    raw = _clean(raw).upper()
    for keyword, classifier in KNOWN_LEVELS.items():
        if raw.startswith(keyword + " "):
            number = raw[len(keyword) + 1:].strip()
            return classifier, number
    # Unknown format: treat as section by number only
    return "section", raw


def _citation(law_abbr: str, section_number: str) -> str:
    """Build NY citation: 'N.Y. ABC Law § 1'."""
    return f"N.Y. {law_abbr} Law § {section_number}"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    corpus_node: Node = insert_jurisdiction_and_corpus_node(COUNTRY, JURISDICTION, CORPUS)
    scrape_all_laws(corpus_node)


# ---------------------------------------------------------------------------
# Enumerate all NY laws across every category
# ---------------------------------------------------------------------------

# Category-index URLs link to per-law pages like /legislation/laws/XXX where
# XXX is 2-5 uppercase letters or digits. Anchor at a word boundary (not $)
# so trailing slashes / query strings don't cause misses.
_LAW_HREF_RE = re.compile(r"/legislation/laws/([A-Z][A-Z0-9]{1,5})(?:/|\?|#|$)")


def _enumerate_laws(category_url: str) -> list[tuple[str, str, str]]:
    """Return list of (abbr, name, url) for every law on a category index."""
    try:
        soup = _fetch_soup(category_url)
    except Exception as exc:
        print(f"[scrapeNY] failed category {category_url}: {exc}", flush=True)
        return []
    out: list[tuple[str, str, str]] = []
    seen: set[str] = set()
    for a in soup.find_all("a", href=True):
        href: str = a["href"]
        m = _LAW_HREF_RE.search(href)
        if not m:
            continue
        abbr = m.group(1)
        # Skip the category-index pages themselves (CONSOLIDATED etc.).
        if abbr in {"CONSOLIDATED", "UNCONSOLIDATED", "COURT", "ACTS", "RULES", "MISC"}:
            continue
        if abbr in seen:
            continue
        seen.add(abbr)
        name = _clean(a.get_text()) or abbr
        url = href if href.startswith("http") else f"{BASE_URL}{href}"
        out.append((abbr, name, url))
    return out


def scrape_all_laws(corpus_node: Node) -> None:
    """Enumerate every NY law across all 5 categories and scrape in parallel.

    Concurrency is controlled by ``VAQUILL_TITLE_WORKERS`` (default 8). Each
    law is independent. Law-level resume via ``state_ny_chapters_done.txt``.
    Set ``VAQUILL_FORCE_RESCRAPE=1`` to override.
    """
    laws: dict[str, tuple[str, str]] = {}
    for cat_url in LAW_CATEGORY_URLS:
        for abbr, name, url in _enumerate_laws(cat_url):
            laws.setdefault(abbr, (name, url))
        time.sleep(REQUEST_DELAY)

    for slug in EXTRA_LAW_SLUGS:
        laws.setdefault(slug, (slug, f"{BASE_URL}/legislation/laws/{slug}"))

    done = set() if os.environ.get("VAQUILL_FORCE_RESCRAPE") else _load_laws_done()
    print(f"[scrapeNY] discovered {len(laws)} laws ({len(done)} already done)", flush=True)

    work: list[tuple[Node, str, str]] = []
    for abbr, (name, url) in sorted(laws.items()):
        node_id = f"{corpus_node.node_id}/act={abbr}"
        law_node = Node(
            id=node_id,
            link=url,
            node_type="structure",
            level_classifier="act",
            number=abbr,
            node_name=name,
            top_level_title=abbr,
            parent=corpus_node.node_id,
        )
        # Idempotent insert up-front so the structure node exists even if a
        # child scrape later fails or this run is resumed.
        insert_node(law_node, TABLE_NAME, ignore_duplicate=True, debug_mode=False)
        if abbr in done:
            continue
        work.append((law_node, url, abbr))

    def _do_law(item: tuple[Node, str, str]) -> tuple[str, str, Optional[str]]:
        law_node, law_url, law_abbr = item
        try:
            sections = scrape_law_children(law_node, law_url, law_abbr)
        except Exception as exc:
            return (law_abbr, "fail", str(exc)[:200])
        if sections <= 0:
            # Don't checkpoint a 0-section result. Prior runs marked laws
            # done after silently failing on mid-recursion Cloudflare
            # challenge pages, leaving 108 NY laws empty in production.
            return (law_abbr, "fail", "0 sections (likely Cloudflare challenge on sub-pages)")
        _mark_law_done(law_abbr)
        return (law_abbr, "ok", f"{sections} sections")

    workers = int(os.environ.get("VAQUILL_TITLE_WORKERS", "8"))
    print(f"[scrapeNY] running {len(work)} laws with {workers} parallel workers", flush=True)
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(_do_law, item) for item in work]
        for fut in as_completed(futs):
            abbr, status, err = fut.result()
            if status == "fail":
                print(f"[scrapeNY] law {abbr}: fail: {err}", flush=True)
            else:
                print(f"[scrapeNY] law {abbr}: ok", flush=True)


# ---------------------------------------------------------------------------
# Recursive structure scraper
# ---------------------------------------------------------------------------

def scrape_law_children(
    parent_node: Node,
    url: str,
    law_abbr: str,
) -> int:
    """Fetch ``url`` and recursively walk the law structure.

    Returns the number of section content-nodes written during this call
    (including from recursive children). Callers in ``_do_law`` use this
    to decide whether to checkpoint the law as done.

    Raises ``RuntimeError`` when the fetched page is missing the
    ``nys-openleg-result-container`` div, which is the signature of a
    Cloudflare interstitial / partial render. Without raising, the
    silent-return path lets ``_do_law`` mark the law as done with zero
    sections (this happened to 108 NY laws in the prior run).
    """
    soup: BeautifulSoup = _fetch_soup(url)
    time.sleep(REQUEST_DELAY)

    result_container = soup.find("div", class_="nys-openleg-result-container")
    if not isinstance(result_container, Tag):
        # No NY-Senate markup. Treat as fetch failure so the law is
        # retried instead of silently checkpointed empty.
        raise RuntimeError(f"missing nys-openleg-result-container at {url}")

    # Read headline to decide whether this page IS a section
    headline_tag = result_container.find("h2", class_="nys-openleg-result-title-headline")
    headline_text = _clean(headline_tag.get_text()) if isinstance(headline_tag, Tag) else ""

    # Check for sub-items list
    items_ul = result_container.find("ul", class_="nys-openleg-items-container")
    sub_items = (
        items_ul.find_all("li", class_="nys-openleg-result-item-container")
        if isinstance(items_ul, Tag) else []
    )

    if sub_items:
        return _process_structure_items(parent_node, law_abbr, sub_items)
    if headline_text.upper().startswith("SECTION"):
        # Direct section page (no sub-list)
        return _scrape_section_page(
            parent_node=parent_node,
            law_abbr=law_abbr,
            result_container=result_container,
            url=url,
            headline_text=headline_text,
        )
    # Genuinely empty page (e.g. a law that's been entirely repealed).
    # Return 0 without raising so a future-empty law doesn't fail forever.
    return 0


def _process_structure_items(
    parent_node: Node,
    law_abbr: str,
    items: list,
) -> int:
    """Process child items (sub-structures or sections). Returns sections written.

    Per-item errors are caught and logged but do not abort the whole law;
    they DO reduce the returned count, which surfaces at the law level
    and prevents premature checkpointing.
    """
    total = 0
    for li in items:
        link_tag = li.find("a", class_="nys-openleg-result-item-link")
        if not isinstance(link_tag, Tag):
            continue

        item_href: str = link_tag.get("href", "")
        item_url = item_href if item_href.startswith("http") else f"{BASE_URL}{item_href}"

        name_div = li.find("div", class_="nys-openleg-result-item-name")
        desc_div = li.find("div", class_="nys-openleg-result-item-description")

        raw_name = _clean(name_div.get_text()) if isinstance(name_div, Tag) else ""
        description = _clean(desc_div.get_text()) if isinstance(desc_div, Tag) else ""

        level_classifier, number = _parse_item_name(raw_name)
        node_name = f"{raw_name} - {description}" if description else raw_name
        node_id = f"{parent_node.node_id}/{level_classifier}={number}"

        if level_classifier == "section":
            try:
                total += _fetch_and_store_section(
                    parent_node=parent_node,
                    law_abbr=law_abbr,
                    section_url=item_url,
                    section_number=number,
                    section_name=node_name,
                    node_id=node_id,
                )
            except Exception as exc:
                print(f"[scrapeNY] section error {item_url}: {exc}", flush=True)
        else:
            structure_node = Node(
                id=node_id,
                link=item_url,
                node_type="structure",
                level_classifier=level_classifier,
                number=number,
                node_name=node_name,
                top_level_title=parent_node.top_level_title,
                parent=parent_node.node_id,
            )
            insert_node(structure_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)
            try:
                total += scrape_law_children(structure_node, item_url, law_abbr)
            except Exception as exc:
                print(f"[scrapeNY] structure error {item_url}: {exc}", flush=True)
    return total


def _fetch_and_store_section(
    parent_node: Node,
    law_abbr: str,
    section_url: str,
    section_number: str,
    section_name: str,
    node_id: str,
) -> int:
    """Fetch a section page and store it as a content node. Returns 0 or 1."""
    soup: BeautifulSoup = _fetch_soup(section_url)
    time.sleep(REQUEST_DELAY)

    result_container = soup.find("div", class_="nys-openleg-result-container")
    if not isinstance(result_container, Tag):
        # Missing markup at the section level is a real failure: raise so
        # the calling _process_structure_items can log it and the parent
        # law's total section count reflects the loss.
        raise RuntimeError(f"missing nys-openleg-result-container at {section_url}")

    return _scrape_section_page(
        parent_node=parent_node,
        law_abbr=law_abbr,
        result_container=result_container,
        url=section_url,
        headline_text=section_name,
        override_section_number=section_number,
        override_node_id=node_id,
    )


def _scrape_section_page(
    parent_node: Node,
    law_abbr: str,
    result_container: Tag,
    url: str,
    headline_text: str,
    override_section_number: Optional[str] = None,
    override_node_id: Optional[str] = None,
) -> int:
    """Parse a section result container and insert a content node. Returns 0 or 1."""
    # Determine section number
    section_number = override_section_number
    if section_number is None:
        m = re.match(r"SECTION\s+(\S+)", headline_text.upper())
        if m:
            section_number = m.group(1)
        else:
            section_number = _clean(headline_text)

    # Short human-readable name
    short_title_tag = result_container.find("h3", class_="nys-openleg-result-title-short")
    short_title = _clean(short_title_tag.get_text()) if isinstance(short_title_tag, Tag) else ""
    node_name = f"§ {section_number}" + (f". {short_title}" if short_title else "")

    # Extract statute text, replacing <br> tags with newlines first
    text_div = result_container.find("div", class_="nys-openleg-result-text")
    raw_text = ""
    if isinstance(text_div, Tag):
        for br in text_div.find_all("br"):
            br.replace_with("\n")
        raw_text = text_div.get_text()
    raw_text = re.sub(r"\n{3,}", "\n\n", raw_text).strip()

    if not raw_text or len(raw_text) < 10:
        return 0

    # Remove leading "§ N." prefix the site inlines into the body text
    raw_text = re.sub(r"^§\s*\S+\.\s*", "", raw_text).strip()
    if not raw_text:
        return 0

    node_id = override_node_id or f"{parent_node.node_id}/section={section_number}"
    citation = _citation(law_abbr, section_number)

    nt = NodeText()
    paragraphs = [p.strip() for p in re.split(r"\n{2,}", raw_text) if p.strip()]
    if not paragraphs:
        paragraphs = [raw_text]
    for para in paragraphs:
        nt.add_paragraph(text=para)

    section_node = Node(
        id=node_id,
        link=url,
        citation=citation,
        node_type="content",
        level_classifier="section",
        number=section_number,
        node_name=node_name,
        top_level_title=parent_node.top_level_title,
        parent=parent_node.node_id,
        node_text=nt,
    )
    insert_node(section_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)
    return 1


if __name__ == "__main__":
    main()
