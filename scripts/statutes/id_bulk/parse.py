"""HTML parsing for the Idaho statutes site.

The parsing mirrors the proven scraper (state_scrapers/.../id/statutes/scrapeID.py)
so enumeration reaches the same sections and the extracted text matches:

  - Every listing page (TOC / title / chapter) renders the data as an HTML table
    inside the SECOND ``vc-column-inner-wrapper`` div (the Idaho template ships
    the class name with a historic three-n typo, ``vc-column-innner-wrapper``;
    both spellings are tolerated). Each ``<tr>`` has >= 3 ``<td>``: td[0] carries
    the "TITLE N" / "CHAPTER N" / section-number label plus the anchor, td[2] the
    description. Rows with no anchor are reserved / repealed.
  - A section page renders the body in a ``.pgbrk`` div whose first four
    top-level divs are breadcrumb headers (title name, title desc, chapter name,
    chapter desc) and are skipped; the rest are the statute paragraphs, ending in
    an optional ``History:`` credit run.
"""

from __future__ import annotations

import re

from bs4 import BeautifulSoup, Tag

BASE_URL = "https://legislature.idaho.gov"

RESERVED_KEYWORDS = ("[repealed]", "[expired]", "[reserved]", "redesignated")

# Number of leading divs inside .pgbrk that are breadcrumb headers to skip.
_HEADER_DIV_COUNT = 4

# WPBakery / Visual Composer wrapper class; canonical spelling first, then the
# template's historic three-n typo.
_WRAPPER_CLASSES = ("vc-column-inner-wrapper", "vc-column-innner-wrapper")

# A chapter can nest its sections one level deeper under a sub-chapter (``SCH``)
# or a part (``PT{n}``, used by Title 15 Uniform Probate Code). Both are crawled
# and their sections flatten into the parent chapter (the section URL itself uses
# the parent chapter path, e.g. .../T15CH1/SECT15-1-101), so the act_id stays
# uniform (STATE_ID_T15_C1_S15-1-101). Section links ("SECT") are matched first,
# so this never captures a section by mistake.
_SUBCONTAINER_RE = re.compile(r"(?:PT\d|SCH)", re.IGNORECASE)


def clean(raw: str) -> str:
    """Normalise whitespace and strip non-breaking spaces / smart quotes."""
    text = raw.replace("\xa0", " ").replace("’", "'").replace("‘", "'")
    return re.sub(r"\s+", " ", text).strip()


def is_reserved(text: str) -> bool:
    lower = text.lower()
    return any(kw in lower for kw in RESERVED_KEYWORDS)


def main_container(soup: BeautifulSoup) -> Tag | None:
    """Return the second wrapper div (the data container), tolerant of spelling."""
    for cls in _WRAPPER_CLASSES:
        containers = soup.find_all("div", class_=cls)
        if len(containers) >= 2:
            return containers[1]
    return None


def _abs(href: str) -> str:
    return BASE_URL + href.rstrip("/") + "/"


def title_rows(html: str) -> list[tuple[str, str, str]]:
    """Parse the TOC -> [(title_number, title_name, title_url)] for linked titles.

    Reserved titles (no anchor, or a reserved keyword in the name) are skipped:
    they contain no sections.
    """
    soup = BeautifulSoup(html, "html.parser")
    container = main_container(soup)
    if container is None:
        return []
    out: list[tuple[str, str, str]] = []
    for row in container.find_all("tr"):
        tds = row.find_all("td")
        if len(tds) < 3:
            continue
        a = tds[0].find("a", href=True)
        if a is None:
            continue
        label = clean(tds[0].get_text())  # "TITLE 18"
        words = label.split()
        if len(words) < 2:
            continue
        number = words[1]
        name = f"{label} {clean(tds[2].get_text())}"
        if is_reserved(name):
            continue
        out.append((number, name, _abs(a["href"])))
    return out


def chapter_rows(html: str) -> list[tuple[str, str]]:
    """Parse a title page -> [(chapter_number, chapter_url)] for linked chapters.

    Reserved / repealed chapters (no anchor) are skipped.
    """
    soup = BeautifulSoup(html, "html.parser")
    container = main_container(soup)
    if container is None:
        return []
    out: list[tuple[str, str]] = []
    for row in container.find_all("tr"):
        tds = row.find_all("td")
        if len(tds) < 3:
            continue
        a = tds[0].find("a", href=True)
        if a is None:
            continue
        label = clean(tds[0].get_text())  # "CHAPTER 40"
        words = label.split()
        if len(words) < 2:
            continue
        name = f"{label} {clean(tds[2].get_text())}"
        if is_reserved(name):
            continue
        out.append((words[1], _abs(a["href"])))
    return out


def section_rows(html: str) -> tuple[list[tuple[str, str, str]], list[str]]:
    """Parse a chapter / sub-chapter / part page.

    Returns ``(sections, subcontainer_urls)`` where each section is
    ``(section_number, section_desc, section_url)``. A row whose anchor points at
    a deeper container (a sub-chapter ``SCH`` or a part ``PT{n}``, never a
    section) is returned as a URL to crawl; its sections belong to the SAME parent
    chapter (the section URL uses the parent chapter path). Reserved rows (no
    anchor, or a reserved keyword) are dropped: they carry no body text.
    """
    soup = BeautifulSoup(html, "html.parser")
    container = main_container(soup)
    if container is None:
        return [], []
    sections: list[tuple[str, str, str]] = []
    subcontainers: list[str] = []
    for row in container.find_all("tr"):
        tds = row.find_all("td")
        if len(tds) < 3:
            continue
        label = clean(tds[0].get_text())
        if not label:
            continue
        a = tds[0].find("a", href=True)
        if a is None:
            continue
        href = a["href"]
        if "SECT" in href.upper():
            desc = clean(tds[2].get_text())
            if is_reserved(f"{label} {desc}"):
                continue
            sections.append((label, desc, _abs(href)))
        elif _SUBCONTAINER_RE.search(href):
            subcontainers.append(_abs(href))
    return sections, subcontainers


def section_paragraphs(html: str) -> list[str]:
    """Extract a section's body as an ordered list of paragraph strings.

    The first ``_HEADER_DIV_COUNT`` divs inside ``.pgbrk`` are breadcrumb headers
    (title / chapter names) and are skipped. Remaining divs are statute
    paragraphs; the trailing ``History:`` credit run is kept (appended as its own
    paragraphs) so amendment-year enrichment and currency signals survive.
    Deterministic: identical page -> identical paragraphs -> identical
    content-addressed point_id across runs.
    """
    soup = BeautifulSoup(html, "html.parser")
    container = soup.find(class_="pgbrk")
    if container is None:
        return []
    divs = container.find_all("div", recursive=False)[_HEADER_DIV_COUNT:]
    body: list[str] = []
    history: list[str] = []
    in_history = False
    for div in divs:
        text = clean(div.get_text())
        if not text:
            continue
        if text.startswith("History:") or in_history:
            in_history = True
            history.append(text)
        else:
            body.append(text)
    return body + history
