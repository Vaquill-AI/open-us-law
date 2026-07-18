"""Alabama Statutes scraper.

Source: https://alison.legislature.state.al.us/graphql (Alabama Legislative
Services Agency, official ALISON Code of Alabama 1975, current through the
2025 Regular Session).

The previously used Onecle mirror (law.onecle.com/alabama) returns HTTP 403
from non-US residential IPs and is geofenced regardless. Justia is also 403.
FindLaw's `codes.findlaw.com/al` is being rebuilt and currently has empty
content (the title pages literally point users back to the ALISON site).

The official ALISON site is a JS SPA backed by a public GraphQL endpoint
(`/graphql`). Three queries reverse-engineered from the bundle do the job:

  1) `codeOfAlabamaTitles`            -> a single string that flat-encodes the
                                         full Title > Chapter > Section tree.
                                         Rows are delimited by U+222B (INTEGRAL),
                                         fields by U+2020 (DAGGER). The leading
                                         row is the field header
                                         (`codeId†title†sectionRange†effectiveDate`).
                                         Each row begins with the numeric
                                         `codeId`, followed by a label like
                                         `Title 1 General Provisions.`,
                                         `Chapter 1 Construction of Code...`,
                                         or `Section 1-1-1 Meaning of ...`.
  2) `codeOfAlabamaSection(displayId)` -> body HTML + history for a single
                                         section (displayId is e.g. "1-1-1").
  3) Reserved sections are detectable from the title label (e.g. "Repealed").

Resume + parallelism follow the DE/AK pattern: env VAQUILL_TITLE_WORKERS
(default 8), title-level resume via state_al_titles_done.txt. The single
hierarchy fetch is shared across all worker threads.

Citation: `Ala. Code § <displayId>` (e.g. Ala. Code § 1-1-1).
"""
from __future__ import annotations

import json
import os
import re
import sys
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Project-root bootstrap (mirrors the DE/AK pattern)
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
    insert_jurisdiction_and_corpus_node,
    insert_node,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
COUNTRY = "us"
JURISDICTION = "al"
CORPUS = "statutes"
TABLE_NAME = f"{COUNTRY}_{JURISDICTION}_{CORPUS}"

GRAPHQL_URL = "https://alison.legislature.state.al.us/graphql"
ORIGIN = "https://alison.legislature.state.al.us"

ROW_SEP = "∫"   # U+222B INTEGRAL: row separator in the flat hierarchy string
FIELD_SEP = "†"  # U+2020 DAGGER: field separator within a row

RESERVED_KEYWORDS = ("repealed", "reserved", "expired", "renumbered", "deleted")

# Section displayIds look like "1-1-1", "13A-5-40", "10A-1-2.01", "32-5A-191.3".
# Allow letters in the title segment plus optional decimal subsection.
_SECTION_LABEL_RE = re.compile(
    r"^Section\s+([0-9]+[A-Za-z]?-[0-9]+[A-Za-z]?-[0-9A-Za-z.]+)\s*(.*)$"
)
_CHAPTER_LABEL_RE = re.compile(r"^Chapter\s+([0-9]+[A-Za-z]?)\s*(.*)$")
_TITLE_LABEL_RE = re.compile(r"^Title\s+([0-9]+[A-Za-z]?)\s*(.*)$")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    corpus_node: Node = insert_jurisdiction_and_corpus_node(COUNTRY, JURISDICTION, CORPUS)
    _scrape_all_titles(corpus_node)


# ---------------------------------------------------------------------------
# Title-level resume bookkeeping (mirrors AK / DE)
# ---------------------------------------------------------------------------

def _titles_done_path() -> Path:
    try:
        from vaquill_pipeline.config import SETTINGS  # type: ignore
        return SETTINGS.chunks_dir / "state_al_titles_done.txt"
    except Exception:
        return Path(__file__).parent / "state_al_titles_done.txt"


def _load_titles_done() -> set[str]:
    path = _titles_done_path()
    if not path.exists():
        return set()
    return {ln.strip() for ln in path.read_text().splitlines() if ln.strip()}


def _mark_title_done(number: str) -> None:
    path = _titles_done_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a") as fh:
        fh.write(f"{number}\n")
        fh.flush()


# ---------------------------------------------------------------------------
# GraphQL transport
# ---------------------------------------------------------------------------

_TITLES_QUERY = "query codeOfAlabamaTitles { titles: codeOfAlabamaTitles }"
_SECTION_QUERY = (
    "query codeOfAlabamaSection($displayId: String!) {"
    "  codesOfAlabama(where: { type: { eq: Section }, displayId: { eq: $displayId } }, versions: true) {"
    "    data { codeId displayId title content history effectiveDate }"
    "  }"
    "}"
)


def _gql(query: str, variables: Optional[dict] = None, *, timeout: float = 30.0) -> dict:
    """POST a GraphQL query through the same Webshare US proxy as fetch_html.

    We don't go through fetch_html because that's a GET helper. Replicating
    the proxy/UA pattern here keeps us on the supported Webshare credentials.
    """
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Origin": ORIGIN,
        "Referer": ORIGIN + "/",
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
        ),
    }
    proxies = None
    user = os.environ.get("WEBSHARE_USERNAME")
    pwd = os.environ.get("WEBSHARE_PASSWORD")
    if user and pwd:
        host = os.environ.get("WEBSHARE_PROXY_HOST", "p.webshare.io")
        port = os.environ.get("WEBSHARE_PROXY_PORT", "80")
        pu = f"{user}-US-rotate"
        proxy_url = (
            f"http://{urllib.parse.quote(pu)}:{urllib.parse.quote(pwd)}@{host}:{port}"
        )
        proxies = {"http": proxy_url, "https": proxy_url}

    payload = {"query": query}
    if variables is not None:
        payload["variables"] = variables

    last_exc: Optional[BaseException] = None
    for attempt in range(1, 5):
        try:
            resp = requests.post(
                GRAPHQL_URL,
                json=payload,
                headers=headers,
                proxies=proxies,
                timeout=timeout,
            )
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            continue
        if resp.status_code != 200:
            last_exc = requests.HTTPError(f"HTTP {resp.status_code} for graphql")
            continue
        try:
            body = resp.json()
        except json.JSONDecodeError as exc:
            last_exc = exc
            continue
        if body.get("errors"):
            # Don't retry semantic errors; they won't fix themselves.
            raise RuntimeError(f"GraphQL errors: {body['errors']!r}")
        return body["data"]
    raise last_exc or RuntimeError("graphql exhausted retries")


# ---------------------------------------------------------------------------
# Hierarchy parsing
# ---------------------------------------------------------------------------

def _fetch_hierarchy() -> list[tuple[str, str]]:
    """Pull the flat hierarchy string and return ordered (codeId, label) pairs.

    The first row is the field header (`codeId†title†sectionRange†...`) which
    we drop. Each remaining row's first field is the numeric codeId; the
    second field is the human label (e.g. "Title 1 General Provisions.",
    "Chapter 1 ...", "Section 1-1-1 ...").
    """
    data = _gql(_TITLES_QUERY)
    raw: str = data["titles"]
    pairs: list[tuple[str, str]] = []
    # Some payloads start with a leading ROW_SEP; lstrip just that char.
    for row in raw.lstrip(ROW_SEP).split(ROW_SEP):
        if not row:
            continue
        fields = row.split(FIELD_SEP)
        if len(fields) < 2:
            continue
        code_id, label = fields[0].strip(), fields[1].strip()
        if not code_id or code_id == "codeId":
            # Header row.
            continue
        pairs.append((code_id, label))
    return pairs


def _check_reserved(text: str) -> Optional[str]:
    lower = text.lower()
    for kw in RESERVED_KEYWORDS:
        if kw in lower:
            return "reserved"
    return None


def _clean_text(raw: str) -> str:
    if not raw:
        return ""
    return re.sub(r"\s+", " ", raw.replace("\xa0", " ")).strip()


# ---------------------------------------------------------------------------
# Section content
# ---------------------------------------------------------------------------

def _fetch_section_content(
    display_id: str,
) -> tuple[Optional[NodeText], Optional[Addendum], Optional[str]]:
    """Return (node_text, addendum, status) for a section by displayId."""
    try:
        data = _gql(_SECTION_QUERY, {"displayId": display_id})
    except Exception as exc:
        print(f"[scrapeAL] section fetch failed for {display_id}: {exc}", flush=True)
        return None, None, None

    items = (data.get("codesOfAlabama") or {}).get("data") or []
    if not items:
        return None, None, None
    item = items[0]
    html_body = item.get("content") or ""
    history_raw = item.get("history") or ""

    body_status: Optional[str] = None
    if re.search(r"\brepealed\b", html_body, flags=re.IGNORECASE):
        body_status = "reserved"

    node_text: Optional[NodeText] = None
    if html_body:
        soup = BeautifulSoup(html_body, "html.parser")
        nt = NodeText()
        # ALISON section bodies are <p> blocks. Fall back to top-level text
        # nodes if the markup doesn't use <p>.
        paragraphs = soup.find_all("p")
        if paragraphs:
            for p in paragraphs:
                text = _clean_text(p.get_text(separator=" "))
                if text:
                    nt.add_paragraph(text=text)
        else:
            text = _clean_text(soup.get_text(separator=" "))
            if text:
                nt.add_paragraph(text=text)
        if nt.paragraphs:
            node_text = nt

    addendum: Optional[Addendum] = None
    if history_raw:
        history_text = _clean_text(
            BeautifulSoup(history_raw, "html.parser").get_text(separator=" ")
        )
        if history_text:
            addendum = Addendum()
            addendum.history = AddendumType(type="history", text=history_text)

    return node_text, addendum, body_status


# ---------------------------------------------------------------------------
# Walker: turn the flat hierarchy into title > chapter > section nodes
# ---------------------------------------------------------------------------

def _scrape_all_titles(corpus_node: Node) -> None:
    """Walk the flat hierarchy once, dispatch per-title work to a pool."""
    pairs = _fetch_hierarchy()
    print(f"[scrapeAL] hierarchy rows: {len(pairs)}", flush=True)

    # Group rows by title. Each group is the list of (code_id, label) rows
    # belonging to one title (the title row itself + all its chapters and
    # sections in document order, until the next title row).
    titles_groups: list[list[tuple[str, str]]] = []
    current: list[tuple[str, str]] = []
    for code_id, label in pairs:
        if label.startswith("Title "):
            if current:
                titles_groups.append(current)
            current = [(code_id, label)]
        else:
            if not current:
                # Shouldn't happen, but skip stray pre-title rows.
                continue
            current.append((code_id, label))
    if current:
        titles_groups.append(current)

    titles_done = (
        set() if os.environ.get("VAQUILL_FORCE_RESCRAPE") else _load_titles_done()
    )
    if titles_done:
        print(
            f"[scrapeAL] resume: {len(titles_done)} titles already done",
            flush=True,
        )

    # First pass (sequential): insert structural nodes (title + chapter) so
    # later resumes can rely on them existing.
    work: list[tuple[Node, list[tuple[str, str]]]] = []
    for group in titles_groups:
        title_code_id, title_label = group[0]
        m = _TITLE_LABEL_RE.match(title_label)
        if not m:
            print(f"[scrapeAL] skipping unparseable title row: {title_label!r}", flush=True)
            continue
        title_number = m.group(1)
        title_name = _clean_text(title_label.rstrip("."))
        title_node = Node(
            id=f"{corpus_node.node_id}/title={title_number}",
            link=f"{ORIGIN}/code-of-alabama/?title={title_number}",
            top_level_title=title_number,
            node_type="structure",
            level_classifier="title",
            number=title_number,
            node_name=title_name,
            parent=corpus_node.node_id,
            status=_check_reserved(title_label),
        )
        insert_node(title_node, TABLE_NAME, ignore_duplicate=True, debug_mode=False)

        if title_node.status:
            continue
        if title_number in titles_done:
            continue
        work.append((title_node, group[1:]))  # drop the title row itself

    def _do_title(item: tuple[Node, list[tuple[str, str]]]) -> tuple[str, str, Optional[str]]:
        title_node, rows = item
        try:
            _scrape_title_rows(title_node, rows)
            _mark_title_done(title_node.number)
            return (title_node.number, "ok", None)
        except Exception as exc:  # noqa: BLE001
            return (title_node.number, "fail", str(exc)[:200])

    workers = int(os.environ.get("VAQUILL_TITLE_WORKERS", "8"))
    print(
        f"[scrapeAL] running {len(work)} titles with {workers} parallel workers",
        flush=True,
    )
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for fut in as_completed(ex.submit(_do_title, item) for item in work):
            num, status, err = fut.result()
            if status == "fail":
                print(f"[scrapeAL] title {num}: {status}: {err}", flush=True)
            else:
                print(f"[scrapeAL] title {num}: {status}", flush=True)


def _scrape_title_rows(title_node: Node, rows: list[tuple[str, str]]) -> None:
    """Process the rows belonging to one title.

    Rows arrive in document order. Chapter rows start with "Chapter ", section
    rows with "Section ". We track the current chapter to scope sections.
    """
    current_chapter: Optional[Node] = None
    for code_id, label in rows:
        if label.startswith("Chapter "):
            m = _CHAPTER_LABEL_RE.match(label)
            if not m:
                current_chapter = None
                continue
            ch_number = m.group(1)
            ch_name = _clean_text(label.rstrip("."))
            chapter_node = Node(
                id=f"{title_node.node_id}/chapter={ch_number}",
                link=f"{ORIGIN}/code-of-alabama/?title={title_node.number}&chapter={ch_number}",
                top_level_title=title_node.top_level_title,
                node_type="structure",
                level_classifier="chapter",
                number=ch_number,
                node_name=ch_name,
                parent=title_node.node_id,
                status=_check_reserved(label),
            )
            insert_node(chapter_node, TABLE_NAME, ignore_duplicate=True, debug_mode=False)
            current_chapter = chapter_node if not chapter_node.status else None
            continue

        if label.startswith("Section "):
            if current_chapter is None:
                # Section under a reserved/unparsed chapter; skip.
                continue
            m = _SECTION_LABEL_RE.match(label)
            if not m:
                continue
            display_id = m.group(1)
            sec_name = _clean_text(label.rstrip("."))
            link = f"{ORIGIN}/code-of-alabama/?title={title_node.number}&section={display_id}"
            citation = f"Ala. Code § {display_id}"
            status = _check_reserved(label)
            node_id = f"{current_chapter.node_id}/section={display_id}"

            if status:
                section_node = Node(
                    id=node_id,
                    link=link,
                    top_level_title=title_node.top_level_title,
                    node_type="content",
                    level_classifier="section",
                    number=display_id,
                    node_name=sec_name,
                    parent=current_chapter.node_id,
                    citation=citation,
                    status=status,
                )
                insert_node(section_node, TABLE_NAME, ignore_duplicate=True, debug_mode=False)
                continue

            node_text, addendum, body_status = _fetch_section_content(display_id)
            section_node = Node(
                id=node_id,
                link=link,
                top_level_title=title_node.top_level_title,
                node_type="content",
                level_classifier="section",
                number=display_id,
                node_name=sec_name,
                parent=current_chapter.node_id,
                citation=citation,
                node_text=node_text,
                addendum=addendum,
                status=body_status,
            )
            insert_node(section_node, TABLE_NAME, ignore_duplicate=True, debug_mode=False)
            continue

        # Anything else (e.g. division/subdivision labels) is ignored for now;
        # the official UI navigates only by title/chapter/section anyway.


# ---------------------------------------------------------------------------
# CLI entry
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    main()
