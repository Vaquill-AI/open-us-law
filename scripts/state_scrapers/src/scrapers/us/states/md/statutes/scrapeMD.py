import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Optional, Tuple

current_file = Path(__file__).resolve()
src_directory = current_file.parent
while src_directory.name != "src" and src_directory.parent != src_directory:
    src_directory = src_directory.parent
project_root = src_directory.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from src.utils.pydanticModels import Node, NodeText
from src.utils.scrapingHelpers import (
    insert_jurisdiction_and_corpus_node,
    insert_node,
)

# vaquill_pipeline: shared HTTP client (pooled keep-alive, proxy support,
# retries, mojibake handling) and patch hooks (JsonlSink, R2 sync, etc.).
from vaquill_pipeline import patch as vaquill_patch  # noqa: F401
from vaquill_pipeline.http_client import fetch_html

# Install the pipeline patches so insert_node fans out to JsonlSink/R2 just
# like every other state scraper does. Safe to call multiple times.
try:
    vaquill_patch.install()
except Exception:
    # Already installed or running outside the pipeline harness.
    pass

COUNTRY = "us"
JURISDICTION = "md"
CORPUS = "statutes"
TABLE_NAME = f"{COUNTRY}_{JURISDICTION}_{CORPUS}"

BASE_URL = "https://mgaleg.maryland.gov/mgawebsite"
TOC_URL = f"{BASE_URL}/Laws/Statutes"
SECTION_URL = f"{BASE_URL}/Laws/StatuteText"
NEXT_API_URL = f"{BASE_URL}/api/Laws/GetNext"
PREV_API_URL = f"{BASE_URL}/api/Laws/GetPrevious"

# The /api/Laws/GetNext and /api/Laws/GetPrevious endpoints content-negotiate
# based on the Accept header. fetch_html sends Accept: text/html, so the
# server returns a .NET XML envelope like
#   <string xmlns="http://schemas.microsoft.com/2003/10/Serialization/">1-101</string>
# rather than a JSON string. This regex extracts the inner value regardless
# of envelope (it also handles the legacy "..." quoted JSON form).
_API_STRING_RE = re.compile(
    r'<string[^>]*>([^<]*)</string>',
    re.IGNORECASE,
)


def _parse_api_section(body: Optional[str]) -> Optional[str]:
    if not body:
        return None
    text = body.strip()
    m = _API_STRING_RE.search(text)
    if m is not None:
        value = m.group(1).strip()
    else:
        # Fall back to JSON-quoted form ("1-101") if the server ever returns
        # application/json (e.g. behind a different Accept header).
        value = text.strip('"').strip()
    if not value or value.lower() == "null":
        return None
    return value
# The Articles dropdown is server-rendered into the Statutes TOC page as a
# <select id="Articles"> element. There is no /api/Laws/GetArticles endpoint
# (that URL 404s). We parse the option list out of the TOC HTML instead.
ARTICLES_SOURCE_URL = TOC_URL

RESERVED_KEYWORDS = ["REPEALED", "EXPIRED", "RESERVED", "RENUMBERED", "TRANSFERRED"]

# Skip Maryland Constitution rows returned by GetArticles (values c0, c1...);
# only the statute Articles (lowercase 3-letter codes starting with 'g') are
# in scope for this corpus.
_STATUTE_CODE_RE = re.compile(r"^g[a-z]{2,3}$")


def main() -> None:
    corpus_node: Node = insert_jurisdiction_and_corpus_node(COUNTRY, JURISDICTION, CORPUS)
    scrape_all_articles(corpus_node)


# ---------------------------------------------------------------------------
# Article enumeration + resume
# ---------------------------------------------------------------------------

def _articles_done_path():
    from vaquill_pipeline.config import SETTINGS
    return SETTINGS.chunks_dir / "state_md_articles_done.txt"


def _load_articles_done() -> set:
    if os.environ.get("VAQUILL_FORCE_RESCRAPE"):
        return set()
    path = _articles_done_path()
    if not path.exists():
        return set()
    return {ln.strip() for ln in path.read_text().splitlines() if ln.strip()}


def _mark_article_done(code: str) -> None:
    path = _articles_done_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a") as fh:
        fh.write(f"{code}\n")
        fh.flush()


_ARTICLES_SELECT_RE = re.compile(
    r'<select[^>]*id="Articles"[^>]*>(.*?)</select>',
    re.DOTALL | re.IGNORECASE,
)
_OPTION_RE = re.compile(
    r'<option[^>]+value="([^"]+)"[^>]*>([^<]+)</option>',
    re.IGNORECASE,
)


def _list_articles() -> List[Tuple[str, str]]:
    """Return [(article_code, article_name), ...] parsed from the TOC page.

    The Articles dropdown is server-rendered into the Statutes TOC page HTML
    as a <select id="Articles"> element. Each <option> has a value (e.g.
    "gag") and a display string (e.g. "Agriculture - (gag)"). The list
    contains both statute Articles (lowercase 3-letter codes starting with
    'g') and Constitution articles (codes like 'c0', 'c1', 'c11a'); we
    filter to statute Articles only.

    There is no /api/Laws/GetArticles endpoint -- that URL returns 404. The
    only live API endpoints under /api/Laws/ are GetNext and GetPrevious,
    which walk section codes within an Article.
    """
    body = fetch_html(ARTICLES_SOURCE_URL, timeout=20, max_retries=3)
    if not body:
        raise RuntimeError(f"Empty response from {ARTICLES_SOURCE_URL}")

    select_match = _ARTICLES_SELECT_RE.search(body)
    if not select_match:
        raise RuntimeError(
            f"Could not locate <select id='Articles'> in {ARTICLES_SOURCE_URL}"
        )

    out: List[Tuple[str, str]] = []
    for code, display in _OPTION_RE.findall(select_match.group(1)):
        code = code.strip()
        display = display.strip()
        if not code or not display:
            continue
        if not _STATUTE_CODE_RE.match(code):
            # Skip constitution rows ('c0', 'c1', 'c11a', ...).
            continue
        # "Agriculture - (gag)" -> "Agriculture"
        name = display.split(" - (")[0].strip() or display
        out.append((code, name))
    if not out:
        raise RuntimeError(
            f"Parsed zero statute Articles from {ARTICLES_SOURCE_URL}"
        )
    return out


def scrape_all_articles(corpus_node: Node) -> None:
    articles = _list_articles()
    done = _load_articles_done()
    if done:
        print(
            f"[scrapeMD] resume: {len(done)} articles already done: {sorted(done)}",
            flush=True,
        )

    # Insert all Article structure nodes up front (cheap, idempotent), then
    # hand the actual section walks to a thread pool. Each Article walk is
    # independent — no cross-article shared state.
    work: List[Tuple[Node, str, str]] = []
    for code, name in articles:
        node_id = f"{corpus_node.node_id}/article={code}"
        article_node = Node(
            id=node_id,
            link=f"{SECTION_URL}?article={code}&section=&enactments=false",
            top_level_title=code,
            node_type="structure",
            level_classifier="article",
            number=code,
            node_name=name,
            parent=corpus_node.node_id,
        )
        insert_node(article_node, TABLE_NAME, ignore_duplicate=True, debug_mode=False)
        if code in done:
            continue
        work.append((article_node, code, name))

    # MD's unit is the article, so VAQUILL_ARTICLE_WORKERS is the specific knob,
    # but fall back to the fleet-wide VAQUILL_TITLE_WORKERS the refresh tasks
    # actually set. Without the fallback, setting TITLE_WORKERS across the fleet
    # silently does nothing here.
    workers = int(
        os.environ.get("VAQUILL_ARTICLE_WORKERS")
        or os.environ.get("VAQUILL_TITLE_WORKERS", "8")
    )
    print(
        f"[scrapeMD] running {len(work)} articles with {workers} parallel workers",
        flush=True,
    )

    def _do(item):
        article_node, code, name = item
        try:
            scrape_article(article_node, code, name)
            _mark_article_done(code)
            return (code, "ok", None)
        except Exception as e:  # noqa: BLE001
            return (code, "fail", str(e)[:200])

    with ThreadPoolExecutor(max_workers=workers) as ex:
        for fut in as_completed(ex.submit(_do, item) for item in work):
            code, status, err = fut.result()
            if status == "fail":
                print(f"[scrapeMD] article {code}: {status}: {err}", flush=True)
            else:
                print(f"[scrapeMD] article {code}: {status}", flush=True)


# ---------------------------------------------------------------------------
# Per-article walk
# ---------------------------------------------------------------------------

def scrape_article(article_node: Node, article_code: str, article_name: str) -> None:
    first_section = _get_first_section(article_code)
    if first_section is None:
        print(f"  [warn] No first section found for article {article_code}, skipping.")
        return

    inserted_titles: dict[str, Node] = {}
    inserted_subtitles: dict[str, Node] = {}

    current_section_code: Optional[str] = first_section
    seen: set = set()
    while current_section_code:
        if current_section_code in seen:
            # Defensive: API has been observed to cycle in rare cases.
            break
        seen.add(current_section_code)

        title_num, subtitle_num, _section_part = _parse_section_code(current_section_code)

        title_key = f"{article_code}/{title_num}"
        if title_key not in inserted_titles:
            title_node = Node(
                id=f"{article_node.node_id}/title={title_num}",
                link=article_node.link,
                top_level_title=article_code,
                node_type="structure",
                level_classifier="title",
                number=title_num,
                node_name=f"Title {title_num}",
                parent=article_node.node_id,
            )
            insert_node(title_node, TABLE_NAME, ignore_duplicate=True, debug_mode=False)
            inserted_titles[title_key] = title_node
        title_node = inserted_titles[title_key]

        subtitle_key = f"{article_code}/{title_num}/{subtitle_num}"
        if subtitle_key not in inserted_subtitles:
            subtitle_node = Node(
                id=f"{title_node.node_id}/subtitle={subtitle_num}",
                link=article_node.link,
                top_level_title=article_code,
                node_type="structure",
                level_classifier="subtitle",
                number=subtitle_num,
                node_name=f"Subtitle {subtitle_num}",
                parent=title_node.node_id,
            )
            insert_node(subtitle_node, TABLE_NAME, ignore_duplicate=True, debug_mode=False)
            inserted_subtitles[subtitle_key] = subtitle_node
        subtitle_node = inserted_subtitles[subtitle_key]

        section_url = (
            f"{SECTION_URL}?article={article_code}"
            f"&section={current_section_code}&enactments=false"
        )
        node_text, status, section_heading = _fetch_section_content(section_url)
        citation = f"Md. Code, {article_name} § {current_section_code}"
        node_name = section_heading or f"§ {current_section_code}"

        section_node = Node(
            id=f"{subtitle_node.node_id}/section={current_section_code}",
            link=section_url,
            top_level_title=article_code,
            node_type="content",
            level_classifier="section",
            number=current_section_code,
            node_name=node_name,
            parent=subtitle_node.node_id,
            citation=citation,
            node_text=node_text,
            status=status,
        )
        insert_node(section_node, TABLE_NAME, ignore_duplicate=True, debug_mode=False)

        current_section_code = _get_next_section(article_code, current_section_code)


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

def _api_call(api_url: str, article_code: str, section_code: str) -> Optional[str]:
    from urllib.parse import urlencode
    qs = urlencode({
        "articleCode": article_code,
        "sectionCode": section_code,
        "enactments": "False",
    })
    body = fetch_html(f"{api_url}?{qs}", timeout=15, max_retries=3)
    return _parse_api_section(body)


# Common seed codes used to enter the section ring for an Article. GetNext
# with empty sectionCode returns empty on this API, so we seed with a
# plausible first section then walk GetPrevious until we hit the true first
# (where GetPrevious returns empty). Different Articles use different first
# sections (e.g. some start at 1-101, some at 1-01, some at 0-101).
_FIRST_SECTION_SEEDS = (
    "1-101", "1-01", "01-101", "1-001", "1-1-01", "0-101",
    "2-101", "1A-01", "1-100",
)


def _get_first_section(article_code: str) -> Optional[str]:
    seed: Optional[str] = None
    for candidate in _FIRST_SECTION_SEEDS:
        # GetNext from a real section returns the *next* one, so any non-empty
        # response means our candidate exists (or a neighbor does). We then
        # walk Previous from that anchor.
        nxt = _api_call(NEXT_API_URL, article_code, candidate)
        if nxt:
            seed = candidate
            break
        # Fallback: maybe the candidate itself doesn't exist but Previous works.
        prev = _api_call(PREV_API_URL, article_code, candidate)
        if prev:
            seed = prev
            break
    if seed is None:
        return None

    # Walk Previous until we hit the first section.
    current = seed
    guard = 0
    while guard < 5000:
        prev = _api_call(PREV_API_URL, article_code, current)
        if not prev:
            return current
        current = prev
        guard += 1
    return current


def _get_next_section(article_code: str, current_section_code: str) -> Optional[str]:
    return _api_call(NEXT_API_URL, article_code, current_section_code)


def _fetch_section_content(url: str):
    from bs4 import BeautifulSoup

    body = fetch_html(url, timeout=20, max_retries=3)
    if not body:
        return None, None, None
    soup = BeautifulSoup(body, "html.parser")

    stat_div = soup.find(id="StatuteText")
    if stat_div is None:
        return None, None, None

    raw_text = stat_div.get_text(separator="\n")
    if "File Not Found" in raw_text:
        return None, "reserved", None

    upper = raw_text.upper()
    status: Optional[str] = None
    for kw in RESERVED_KEYWORDS:
        if kw in upper:
            status = "reserved"
            break

    for tag in stat_div.find_all("div", class_="row"):
        tag.decompose()
    for tag in stat_div.find_all("div", style=re.compile(r"text-align\s*:\s*center")):
        tag.decompose()

    heading: Optional[str] = None
    heading_match = re.search(r"§\s*[\w–\-]+\.", raw_text)
    if heading_match is not None:
        heading = heading_match.group(0).strip()

    raw_inner = stat_div.get_text(separator="\n")
    paragraphs = [_clean_text(p) for p in raw_inner.split("\n") if _clean_text(p)]
    if heading:
        h = heading.strip(".").strip()
        paragraphs = [p for p in paragraphs if p.strip(".").strip() != h]

    if not paragraphs:
        return None, status, heading

    node_text = NodeText()
    for para in paragraphs:
        node_text.add_paragraph(text=para)

    return node_text, status, heading


def _parse_section_code(code: str):
    parts = code.split("-")
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    if len(parts) == 2:
        title = parts[0]
        rest = parts[1]
        m = re.match(r"^([0-9]+[A-Z]?)([0-9]{2}(?:\.[0-9]+)?)$", rest)
        if m is not None:
            return title, m.group(1), m.group(2)
        return title, rest, rest
    return code, "0", code


def _clean_text(raw: str) -> str:
    text = raw.replace("\xa0", " ").replace(" ", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


if __name__ == "__main__":
    main()
