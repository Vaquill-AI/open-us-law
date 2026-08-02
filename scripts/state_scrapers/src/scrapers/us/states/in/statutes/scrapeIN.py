"""Indiana Code (IC) scraper.

Source: IGA REST API  https://api.iga.in.gov/2024/ic/*
Requires:  IGA_API_KEY  environment variable (UUID format).
           Obtain a free key by emailing  apitoken.request@iga.in.gov  with your name,
           organisation (optional), address, phone and email.

Hierarchy: us/in/statutes/title=N/article=A/chapter=C/section=S
Citation:  "Ind. Code § T-A-C-S"   (e.g. "Ind. Code § 1-1-1-1")

API endpoints used (all return JSON when authenticated):
    GET /2024/ic/titles                          -> [{name, titleNumber, ...}, ...]
    GET /2024/ic/titles/{T}                      -> {articles: [{articleNumber,...},...]}
    GET /2024/ic/titles/{T}/articles/{A}         -> {chapters: [{chapterNumber,...},...]}
    GET /2024/ic/titles/{T}/articles/{A}/chapters/{C} -> {sections: [{sectionNumber,...},...]}
    GET /2024/ic/titles/{T}-{A}-{C}-{S}         -> section text object

Notes:
  - No Selenium. Pure HTTP + JSON via requests.
  - Sections rendered on chapter endpoint inline; individual section fetches
    fill node_text.
  - The IGA API uses AWS API Gateway with UUID-format x-api-key header auth.
  - RESERVED_KEYWORDS cause a node to be flagged status="reserved" and skipped
    for content fetching.
"""

from __future__ import annotations

import os
import re
import sys
import time
import urllib.parse
from pathlib import Path
from typing import Optional

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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
from src.utils.scrapingHelpers import insert_jurisdiction_and_corpus_node, insert_node

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
COUNTRY = "us"
JURISDICTION = "in"
CORPUS = "statutes"
TABLE_NAME = f"{COUNTRY}_{JURISDICTION}_{CORPUS}"

YEAR = "2024"
API_BASE = "https://api.iga.in.gov"
SITE_BASE = "https://iga.in.gov"

RESERVED_KEYWORDS = (
    "repealed",
    "expired",
    "reserved",
    "renumbered",
    "transferred",
)

_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------


def _api_key() -> str:
    """Return the IGA API key from the environment.

    Raises RuntimeError if IGA_API_KEY is not set, providing instructions to
    obtain a free key.
    """
    key = os.environ.get("IGA_API_KEY", "").strip()
    if not key:
        raise RuntimeError(
            "IGA_API_KEY environment variable is not set.\n"
            "To obtain a free key, email apitoken.request@iga.in.gov with your name,\n"
            "organisation (optional), address, phone number, and email address.\n"
            "The key is a UUID (e.g. xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx).\n"
            "Once received, add it to your .env file:  IGA_API_KEY=<your-key>"
        )
    return key


def _proxies() -> Optional[dict]:
    user = os.environ.get("WEBSHARE_USERNAME", "")
    pwd = os.environ.get("WEBSHARE_PASSWORD", "")
    if not user or not pwd:
        return None
    host = os.environ.get("WEBSHARE_PROXY_HOST", "p.webshare.io")
    port = os.environ.get("WEBSHARE_PROXY_PORT", "80")
    proxy_user = f"{user}-US-rotate"
    proxy_url = (
        f"http://{urllib.parse.quote(proxy_user)}:{urllib.parse.quote(pwd)}@{host}:{port}"
    )
    return {"http": proxy_url, "https": proxy_url}


_PROXIES = None
_PROXIES_LOADED = False


def _get_proxies() -> Optional[dict]:
    global _PROXIES, _PROXIES_LOADED
    if not _PROXIES_LOADED:
        _PROXIES = _proxies()
        _PROXIES_LOADED = True
    return _PROXIES


def _api_get(path: str, max_retries: int = 4) -> dict:
    """Make an authenticated GET to api.iga.in.gov and return parsed JSON."""
    url = f"{API_BASE}{path}"
    key = _api_key()
    headers = {
        "x-api-key": key,
        "Accept": "application/json",
        "User-Agent": _UA,
    }
    last_exc: Optional[BaseException] = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(
                url,
                headers=headers,
                proxies=_get_proxies(),
                timeout=45,
                verify=False,
            )
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 404:
                return {}
            if resp.status_code == 429:
                wait = min(5 * attempt, 30)
                print(
                    f"[in] rate-limited on {url}, sleeping {wait}s (attempt {attempt})",
                    flush=True,
                )
                time.sleep(wait)
                continue
            if resp.status_code == 403:
                data = resp.json()
                msg = data.get("message", "")
                if "Invalid API key" in msg or "Unauthorized" in msg:
                    raise RuntimeError(
                        f"IGA API key is invalid. Verify IGA_API_KEY is correct. "
                        f"URL: {url}, Response: {data}"
                    )
            resp.raise_for_status()
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
            wait = min(3 * attempt, 15)
            print(
                f"[in] network error attempt {attempt}/{max_retries} for {url}: "
                f"{type(exc).__name__}, retrying in {wait}s",
                flush=True,
            )
            last_exc = exc
            time.sleep(wait)
            continue
        except RuntimeError:
            raise
        except Exception as exc:
            last_exc = exc
            time.sleep(min(2 * attempt, 10))
            continue

    raise requests.exceptions.RetryError(
        f"all {max_retries} attempts failed for {url}"
    ) from last_exc


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


def _check_reserved(text: str) -> Optional[str]:
    lower = text.lower()
    for kw in RESERVED_KEYWORDS:
        if kw in lower:
            return "reserved"
    return None


def _clean(raw: str) -> str:
    text = raw.replace("\xa0", " ").replace(" ", " ")
    return re.sub(r"\s+", " ", text).strip()


def _section_url(title: str, article: str, chapter: str, section: str) -> str:
    """Return a canonical public URL for a section."""
    return (
        f"{SITE_BASE}/laws/{YEAR}/ic"
        f"/titles/{title}/articles/{article}/chapters/{chapter}"
    )


# ---------------------------------------------------------------------------
# Response shape helpers
# ---------------------------------------------------------------------------


def _iter_titles(data: dict) -> list:
    """Extract title list from /2024/ic/titles response."""
    if isinstance(data, list):
        return data
    for key in ("titles", "items", "data", "results"):
        if key in data and isinstance(data[key], list):
            return data[key]
    return []


def _get_number(obj: dict, *keys: str) -> Optional[str]:
    """Extract a number field from a dict, trying multiple key names."""
    for k in keys:
        v = obj.get(k)
        if v is not None:
            return str(v).strip()
    return None


def _iter_list(data: dict, *list_keys: str) -> list:
    """Extract a list from a dict, trying multiple possible keys."""
    if isinstance(data, list):
        return data
    for k in list_keys:
        v = data.get(k)
        if isinstance(v, list):
            return v
    return []


def _section_node_text(section_data: dict) -> tuple[Optional[NodeText], Optional[Addendum]]:
    """Parse a section API response into (NodeText, Addendum)."""
    if not section_data:
        return None, None

    # Common field names for section body text
    body_fields = (
        "sectionBody", "body", "text", "content",
        "sectionText", "rawText", "sectionContent",
    )
    history_fields = ("history", "amendments", "sourceNote", "amendment")

    node_text = NodeText()
    history_parts: list[str] = []

    for field in body_fields:
        raw = section_data.get(field)
        if raw and isinstance(raw, str):
            text = _clean(raw)
            if text:
                # Split on newlines to create separate paragraphs
                for para in re.split(r"\n{2,}", text):
                    para = _clean(para)
                    if para:
                        node_text.add_paragraph(text=para)
            break
        elif raw and isinstance(raw, list):
            for item in raw:
                if isinstance(item, str):
                    t = _clean(item)
                    if t:
                        node_text.add_paragraph(text=t)
                elif isinstance(item, dict):
                    t = _clean(item.get("text", item.get("content", "")))
                    if t:
                        node_text.add_paragraph(text=t)
            break

    for field in history_fields:
        raw = section_data.get(field)
        if raw and isinstance(raw, str):
            history_parts.append(_clean(raw))
        elif raw and isinstance(raw, list):
            for item in raw:
                if isinstance(item, str):
                    history_parts.append(_clean(item))

    addendum: Optional[Addendum] = None
    if history_parts:
        addendum = Addendum()
        addendum.history = AddendumType(
            type="history", text=" ".join(history_parts)
        )

    if not node_text.paragraphs:
        node_text = None  # type: ignore[assignment]

    return node_text, addendum


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    corpus_node: Node = insert_jurisdiction_and_corpus_node(COUNTRY, JURISDICTION, CORPUS)
    _scrape_all_titles(corpus_node)


# ---------------------------------------------------------------------------
# Title scraping
# ---------------------------------------------------------------------------


def _scrape_all_titles(corpus_node: Node) -> None:
    data = _api_get(f"/{YEAR}/ic/titles")
    titles = _iter_titles(data)

    if not titles:
        print(f"[in] WARNING: no titles returned from /ic/titles; raw keys: {list(data.keys())}", flush=True)
        # Fall back: try numbers 1-36 directly
        titles = [{"titleNumber": str(n)} for n in range(1, 37)]

    for title_obj in titles:
        t_num = _get_number(title_obj, "titleNumber", "number", "title", "id")
        if not t_num:
            continue

        t_name = _clean(str(
            title_obj.get("title", title_obj.get("name", title_obj.get("titleName", f"Title {t_num}")))
        ))
        node_name = f"Title {t_num} {t_name}" if not t_name.upper().startswith("TITLE") else t_name

        link = f"{SITE_BASE}/laws/{YEAR}/ic/titles/{t_num}"
        node_id = f"{corpus_node.node_id}/title={t_num}"
        status = _check_reserved(node_name)

        title_node = Node(
            id=node_id,
            link=link,
            top_level_title=t_num,
            node_type="structure",
            level_classifier="title",
            number=t_num,
            node_name=node_name,
            parent=corpus_node.node_id,
            status=status,
        )
        insert_node(title_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)

        if not status:
            _scrape_title(title_node)


# ---------------------------------------------------------------------------
# Article scraping
# ---------------------------------------------------------------------------


def _scrape_title(title_node: Node) -> None:
    t = title_node.number
    data = _api_get(f"/{YEAR}/ic/titles/{t}")
    if not data:
        return

    articles = _iter_list(data, "articles", "items", "data")
    if not articles:
        # Maybe the title response itself IS the list
        if isinstance(data, list):
            articles = data
        else:
            print(f"[in] no articles for title {t}", flush=True)
            return

    for article_obj in articles:
        a_num = _get_number(article_obj, "articleNumber", "number", "article", "id")
        if not a_num:
            continue

        a_name = _clean(str(
            article_obj.get("article", article_obj.get("name", article_obj.get("articleName", f"Article {a_num}")))
        ))
        node_name = (
            f"Article {a_num} {a_name}" if not a_name.upper().startswith("ARTICLE") else a_name
        )

        link = f"{SITE_BASE}/laws/{YEAR}/ic/titles/{t}/articles/{a_num}"
        node_id = f"{title_node.node_id}/article={a_num}"
        status = _check_reserved(node_name)

        article_node = Node(
            id=node_id,
            link=link,
            top_level_title=title_node.top_level_title,
            node_type="structure",
            level_classifier="article",
            number=a_num,
            node_name=node_name,
            parent=title_node.node_id,
            status=status,
        )
        insert_node(article_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)

        if not status:
            _scrape_article(article_node, t)


# ---------------------------------------------------------------------------
# Chapter scraping
# ---------------------------------------------------------------------------


def _scrape_article(article_node: Node, t_num: str) -> None:
    a = article_node.number
    data = _api_get(f"/{YEAR}/ic/titles/{t_num}/articles/{a}")
    if not data:
        return

    chapters = _iter_list(data, "chapters", "items", "data")
    if isinstance(data, list):
        chapters = data
    if not chapters:
        print(f"[in] no chapters for title {t_num} article {a}", flush=True)
        return

    for chapter_obj in chapters:
        c_num = _get_number(chapter_obj, "chapterNumber", "number", "chapter", "id")
        if not c_num:
            continue

        c_name = _clean(str(
            chapter_obj.get("chapter", chapter_obj.get("name", chapter_obj.get("chapterName", f"Chapter {c_num}")))
        ))
        node_name = (
            f"Chapter {c_num} {c_name}" if not c_name.upper().startswith("CHAPTER") else c_name
        )

        link = f"{SITE_BASE}/laws/{YEAR}/ic/titles/{t_num}/articles/{a}/chapters/{c_num}"
        node_id = f"{article_node.node_id}/chapter={c_num}"
        status = _check_reserved(node_name)

        chapter_node = Node(
            id=node_id,
            link=link,
            top_level_title=article_node.top_level_title,
            node_type="structure",
            level_classifier="chapter",
            number=c_num,
            node_name=node_name,
            parent=article_node.node_id,
            status=status,
        )
        insert_node(chapter_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)

        if not status:
            _scrape_chapter(chapter_node, t_num, a)


# ---------------------------------------------------------------------------
# Section scraping
# ---------------------------------------------------------------------------


def _scrape_chapter(chapter_node: Node, t_num: str, a_num: str) -> None:
    c = chapter_node.number
    data = _api_get(f"/{YEAR}/ic/titles/{t_num}/articles/{a_num}/chapters/{c}")
    if not data:
        return

    sections = _iter_list(data, "sections", "items", "data")
    if isinstance(data, list):
        sections = data
    if not sections:
        print(
            f"[in] no sections for title {t_num} article {a_num} chapter {c}",
            flush=True,
        )
        return

    for section_obj in sections:
        s_num = _get_number(
            section_obj,
            "sectionNumber", "number", "section", "id",
            "fullSectionNumber", "sectionCode",
        )
        if not s_num:
            continue

        # Normalise: strip leading/trailing whitespace and dots
        s_num = s_num.strip().rstrip(".")

        s_name = _clean(str(
            section_obj.get(
                "section",
                section_obj.get("name", section_obj.get("sectionName", s_num)),
            )
        ))
        node_name = (
            f"Ind. Code § {s_num} {s_name}"
            if not s_name.startswith("Ind.")
            else s_name
        )

        # IN section numbers follow T-A-C-S pattern.
        # Normalise to ensure no leading zeros: 01-01-01-01 -> 1-1-1-1.
        s_num_norm = _normalise_section(s_num, t_num, a_num, c)

        citation = f"Ind. Code § {s_num_norm}"
        link = _section_url(t_num, a_num, c, s_num_norm)
        node_id = f"{chapter_node.node_id}/section={s_num_norm}"
        status = _check_reserved(node_name)

        if status:
            section_node = Node(
                id=node_id,
                link=link,
                top_level_title=chapter_node.top_level_title,
                node_type="content",
                level_classifier="section",
                number=s_num_norm,
                node_name=node_name,
                parent=chapter_node.node_id,
                citation=citation,
                status=status,
            )
            insert_node(section_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)
            continue

        # Try to extract section text from the chapter response first
        node_text, addendum = _section_node_text(section_obj)

        # If no text in the chapter listing, fetch the individual section endpoint
        if node_text is None:
            sec_data = _api_get(f"/{YEAR}/ic/titles/{s_num_norm}")
            if sec_data:
                node_text, addendum = _section_node_text(sec_data)

        section_node = Node(
            id=node_id,
            link=link,
            top_level_title=chapter_node.top_level_title,
            node_type="content",
            level_classifier="section",
            number=s_num_norm,
            node_name=node_name,
            parent=chapter_node.node_id,
            citation=citation,
            node_text=node_text,
            addendum=addendum,
        )
        insert_node(section_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _normalise_section(raw: str, t_num: str, a_num: str, c_num: str) -> str:
    """Return a canonical T-A-C-S string, stripping leading zeros in each part."""
    # If the raw number already looks like T-A-C-S, use it
    parts = re.split(r"[-.]", raw)
    if len(parts) == 4:
        return "-".join(str(int(p)) if p.isdigit() else p for p in parts)
    # If it's a bare section number (just S), build from context
    bare = raw.strip()
    t = str(int(t_num)) if t_num.isdigit() else t_num
    a = str(int(a_num)) if a_num.isdigit() else a_num
    c = str(int(c_num)) if c_num.isdigit() else c_num
    s = str(int(bare)) if bare.isdigit() else bare
    return f"{t}-{a}-{c}-{s}"


if __name__ == "__main__":
    main()
