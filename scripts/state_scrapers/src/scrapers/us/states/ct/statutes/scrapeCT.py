"""Connecticut General Statutes scraper.

Source: https://www.cga.ct.gov/current/pub/titles.htm
Hierarchy: us/ct/statutes/title=N/chapter=M/section=S
Citation: Conn. Gen. Stat. § <SECTION>  (e.g., "Conn. Gen. Stat. § 1-1")

Notes:
- cga.ct.gov uses a TLS certificate that fails proxy MITM verification.
  All requests go through the Webshare US-rotate proxy with verify=False
  (InsecureRequestWarning suppressed via urllib3).
- All sections on a chapter page are inline anchors, not separate URLs.
  We walk siblings from each anchor's parent <p> until the next section
  anchor or the nav_tbl table.
- Reserved/repealed sections are bold inside the toc_catchln cell.
"""
from __future__ import annotations

import os
import re
import sys
import urllib.parse
import warnings
from pathlib import Path
from typing import Optional

import requests
import urllib3
from bs4 import BeautifulSoup, NavigableString, Tag

# Suppress the InsecureRequestWarning from urllib3 for the MITM proxy.
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

current_file = Path(__file__).resolve()
src_directory = current_file.parent
while src_directory.name != "src" and src_directory.parent != src_directory:
    src_directory = src_directory.parent
project_root = src_directory.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

from src.utils.pydanticModels import Addendum, AddendumType, Node, NodeText
from src.utils.scrapingHelpers import insert_jurisdiction_and_corpus_node, insert_node

COUNTRY = "us"
JURISDICTION = "ct"
CORPUS = "statutes"
TABLE_NAME = f"{COUNTRY}_{JURISDICTION}_{CORPUS}"

BASE_PUB = "https://www.cga.ct.gov/current/pub"
TOC_URL = f"{BASE_PUB}/titles.htm"

RESERVED_KEYWORDS = ("(REPEALED)", "(EXPIRED)", "(RESERVED)", "(RENUMBERED)", "(TRANSFERRED)")


# ---------------------------------------------------------------------------
# HTTP helpers (verify=False required for Webshare MITM + cga.ct.gov cert)
# ---------------------------------------------------------------------------

_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)


def _proxies() -> Optional[dict[str, str]]:
    user = os.environ.get("WEBSHARE_USERNAME")
    pwd = os.environ.get("WEBSHARE_PASSWORD")
    if not user or not pwd:
        return None
    host = os.environ.get("WEBSHARE_PROXY_HOST", "p.webshare.io")
    port = os.environ.get("WEBSHARE_PROXY_PORT", "80")
    proxy_user = f"{user}-US-rotate"
    url = f"http://{urllib.parse.quote(proxy_user)}:{urllib.parse.quote(pwd)}@{host}:{port}"
    return {"http": url, "https": url}


def _get_soup(url: str, max_retries: int = 4) -> BeautifulSoup:
    """Fetch URL and return BeautifulSoup. Uses proxy with verify=False.

    Retries on transient network errors (ChunkedEncodingError, IncompleteRead,
    ConnectionError) which occasionally occur with the Webshare rotating proxy
    on large pages.
    """
    import time as _time

    proxies = _proxies()
    last_exc: BaseException | None = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(
                url,
                headers={"User-Agent": _UA, "Accept-Language": "en-US,en;q=0.9"},
                proxies=proxies,
                verify=False,
                timeout=45,
                allow_redirects=True,
                stream=False,
            )
            resp.raise_for_status()
            return BeautifulSoup(resp.content, "html.parser")
        except (
            requests.exceptions.ChunkedEncodingError,
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
        ) as exc:
            last_exc = exc
            wait = min(3 * attempt, 12)
            print(
                f"[ct] fetch attempt {attempt}/{max_retries} failed for {url}: "
                f"{type(exc).__name__}. retrying in {wait}s",
                flush=True,
            )
            _time.sleep(wait)
    raise requests.exceptions.RetryError(
        f"all {max_retries} attempts failed for {url}"
    ) from last_exc


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def main() -> None:
    corpus_node: Node = insert_jurisdiction_and_corpus_node(COUNTRY, JURISDICTION, CORPUS)
    scrape_all_titles(corpus_node)


# ---------------------------------------------------------------------------
# Resume helpers (title-level persistence, mirrors DE pattern)
# ---------------------------------------------------------------------------


def _titles_done_path():
    """Persist completed titles for resumable runs."""
    from vaquill_pipeline.config import SETTINGS
    return SETTINGS.chunks_dir / "state_ct_titles_done.txt"


def _load_titles_done() -> set:
    try:
        path = _titles_done_path()
    except Exception:
        return set()
    if not path.exists():
        return set()
    return {l.strip() for l in path.read_text().splitlines() if l.strip()}


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
# Title scraping
# ---------------------------------------------------------------------------


def scrape_all_titles(corpus_node: Node) -> None:
    from concurrent.futures import ThreadPoolExecutor, as_completed

    soup = _get_soup(TOC_URL)

    titles_done = set() if os.environ.get("VAQUILL_FORCE_RESCRAPE") else _load_titles_done()
    if titles_done:
        print(
            f"[scrapeCT] resume: {len(titles_done)} titles already done: "
            f"{sorted(titles_done)}",
            flush=True,
        )

    work: list[Node] = []

    for td in soup.find_all("td", class_="left_38pct"):
        link_tag = td.find("a")
        desig_span = td.find("span", class_="toc_ttl_desig")
        if desig_span is None:
            continue

        title_label = desig_span.get_text(strip=True)  # e.g., "Title 1" or "Title 2a"
        parts = title_label.split(None, 1)
        if len(parts) < 2:
            continue
        number = parts[1]  # "1", "2a", "51a", etc.

        # Name from the sibling cell (right_62pct)
        name_td = td.find_next_sibling("td")
        if name_td:
            name_a = name_td.find("a")
            title_name = name_a.get_text(strip=True) if name_a else name_td.get_text(strip=True)
        else:
            title_name = title_label

        node_name = f"{title_label} {title_name}"
        status = _check_reserved(node_name)

        if link_tag and link_tag.get("href"):
            href = link_tag["href"].strip()
            link = f"{BASE_PUB}/{href}"
        else:
            link = TOC_URL
            status = status or "reserved"

        node_id = f"{corpus_node.node_id}/title={number}"

        title_node = Node(
            id=node_id,
            link=link,
            top_level_title=number,
            node_type="structure",
            level_classifier="title",
            number=number,
            node_name=node_name,
            parent=corpus_node.node_id,
            status=status,
        )
        insert_node(title_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)

        if status:
            continue
        if number in titles_done:
            continue
        work.append(title_node)

    def _do_title(t_node: Node):
        try:
            scrape_chapters(t_node)
            _mark_title_done(t_node.number)
            return (t_node.number, "ok", None)
        except Exception as e:
            return (t_node.number, "fail", str(e)[:200])

    workers = int(os.environ.get("VAQUILL_TITLE_WORKERS", "8"))
    print(
        f"[scrapeCT] running {len(work)} titles with {workers} parallel workers",
        flush=True,
    )
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for fut in as_completed(ex.submit(_do_title, t) for t in work):
            num, status_, err = fut.result()
            if status_ == "fail":
                print(f"[scrapeCT] title {num}: {status_}: {err}", flush=True)
            else:
                print(f"[scrapeCT] title {num}: {status_}", flush=True)


# ---------------------------------------------------------------------------
# Chapter scraping
# ---------------------------------------------------------------------------


def scrape_chapters(title_node: Node) -> None:
    soup = _get_soup(str(title_node.link))

    for td in soup.find_all("td", class_="left_40pct"):
        link_tag = td.find("a", class_="toc_ch_link")
        if link_tag is None:
            continue

        ch_label = link_tag.get_text(strip=True)  # "Chapter 1"
        parts = ch_label.split(None, 1)
        if len(parts) < 2:
            continue
        ch_number = parts[1]  # "1", "14A", "814", etc.

        href = link_tag.get("href", "").strip()
        ch_link = f"{BASE_PUB}/{href}"

        # Name from sibling cell
        name_td = td.find_next_sibling("td")
        if name_td:
            name_a = name_td.find("a")
            ch_name = name_a.get_text(strip=True) if name_a else name_td.get_text(strip=True)
        else:
            ch_name = ch_label

        node_name = f"{ch_label} {ch_name}"
        node_id = f"{title_node.node_id}/chapter={ch_number}"
        status = _check_reserved(node_name)

        chapter_node = Node(
            id=node_id,
            link=ch_link,
            top_level_title=title_node.top_level_title,
            node_type="structure",
            level_classifier="chapter",
            number=ch_number,
            node_name=node_name,
            parent=title_node.node_id,
            status=status,
        )
        insert_node(chapter_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)

        if not status:
            scrape_sections(chapter_node)


# ---------------------------------------------------------------------------
# Section scraping
# ---------------------------------------------------------------------------


def scrape_sections(chapter_node: Node) -> None:
    chapter_url = str(chapter_node.link).strip()
    soup = _get_soup(chapter_url)

    for p_tag in soup.find_all("p", class_="toc_catchln"):
        link_tag = p_tag.find("a")
        if link_tag is None:
            continue

        href = link_tag.get("href", "").strip()
        if not href.startswith("#"):
            continue

        anchor_id = href.lstrip("#")
        sec_number = _anchor_to_section_number(anchor_id)
        if not sec_number:
            continue

        node_name = link_tag.get_text(strip=True)
        status = _check_reserved(node_name)
        # Bold text inside the toc_catchln paragraph also indicates reserved
        if p_tag.find("b") is not None:
            status = status or "reserved"

        node_id = f"{chapter_node.node_id}/section={sec_number}"
        section_url = f"{chapter_url}#{anchor_id}"
        citation = f"Conn. Gen. Stat. § {sec_number}"

        if status:
            section_node = Node(
                id=node_id,
                link=section_url,
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

        node_text, addendum = _extract_section_content(soup, anchor_id)

        section_node = Node(
            id=node_id,
            link=section_url,
            top_level_title=chapter_node.top_level_title,
            node_type="content",
            level_classifier="section",
            number=sec_number,
            node_name=node_name,
            parent=chapter_node.node_id,
            citation=citation,
            node_text=node_text,
            addendum=addendum,
        )
        insert_node(section_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)


# ---------------------------------------------------------------------------
# Section content extraction
# ---------------------------------------------------------------------------


def _extract_section_content(
    soup: BeautifulSoup, anchor_id: str
) -> tuple[NodeText | None, Addendum | None]:
    """Walk siblings from the section anchor, collecting paragraphs and history."""
    anchor = soup.find(id=anchor_id)
    if anchor is None:
        return None, None

    node_text = NodeText()
    history_parts: list[str] = []

    # The anchor span is inside a <p>; that <p> is the heading, skip it.
    start_tag = anchor.parent
    if start_tag is None:
        return None, None
    it: Tag | NavigableString | None = start_tag.next_sibling
    while it is not None:
        if isinstance(it, Tag):
            # Stop at the navigation table at page bottom
            if it.name == "table" and "nav_tbl" in (it.get("class") or []):
                break
            if it.name == "table":
                # Any other table is still a hard structural break
                break
            # Stop when the next section's heading paragraph appears.
            # Detect via either: a new sec_* anchor span, or a catchln span,
            # or another toc_catchln-style heading paragraph.
            if it.name == "p":
                cls_list = it.get("class") or []
                if "toc_catchln" in cls_list:
                    break
                if it.find("span", class_="catchln"):
                    break
                # New section anchor span (id starts with sec_) signals boundary
                next_anchor = it.find(id=re.compile(r"^sec_"))
                if next_anchor is not None and next_anchor.get("id") != anchor_id:
                    break
            elif it.find and it.find("span", class_="catchln"):
                break

            raw = it.get_text(separator=" ", strip=True)
            if not raw:
                it = it.next_sibling
                continue

            cls_list: list[str] = it.get("class") or []
            cls = cls_list[0] if cls_list else ""

            if cls in ("source-first", "history-first"):
                history_parts.append(_clean(raw))
            else:
                node_text.add_paragraph(text=_clean(raw))

        it = it.next_sibling

    addendum: Addendum | None = None
    if history_parts:
        addendum = Addendum()
        addendum.history = AddendumType(
            type="history", text=" ".join(history_parts)
        )

    if not node_text.paragraphs:
        node_text = None  # type: ignore[assignment]

    return node_text, addendum


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------


def _anchor_to_section_number(anchor_id: str) -> str | None:
    """Convert anchor id like 'sec_1-1' or 'sec_1-1a' to '1-1' or '1-1a'.

    Defensive: only accept the expected character class so a malformed anchor
    (e.g., 'sec_' with no body, or containing spaces) returns None instead of
    producing a junk section number that later crashes node id construction.
    """
    if not anchor_id or not isinstance(anchor_id, str):
        return None
    m = re.match(r"^sec_([A-Za-z0-9][A-Za-z0-9._\-]*)$", anchor_id)
    return m.group(1) if m else None


def _check_reserved(text: str) -> str | None:
    upper = text.upper()
    for kw in RESERVED_KEYWORDS:
        if kw in upper:
            return "reserved"
    return None


def _clean(raw: str) -> str:
    text = raw.replace("\xa0", " ").replace(" ", " ")
    return re.sub(r"\s+", " ", text).strip()


if __name__ == "__main__":
    main()
