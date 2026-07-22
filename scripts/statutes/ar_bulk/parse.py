"""Parse Exa-rendered Justia pages: section heading/body + TOC child links.

Exa returns each page as markdown-ish text plus a flat ``extras.links`` list. A
Justia section page carries the statute body between the "official citation."
universal-citation blurb and the "Disclaimer:" footer, with the subsection
structure preserved as ``* **(a)**...`` bullets and the amendment history as
trailing ``*Amended by ...*`` italics. TOC pages (title / subtitle / chapter /
subchapter) carry no body, only the child links used to walk the tree.

Deterministic by construction: identical page -> identical paragraphs ->
identical content-addressed point_id across runs.
"""

from __future__ import annotations

import re

# First line of a section page:
#   "Arkansas Code §5-10-102 (2024) - Murder in the first degree :: 2024 ..."
_HEADING_RE = re.compile(
    r"Arkansas Code\s*§\s*(?P<num>[0-9A-Za-z][0-9A-Za-z.\-]*)\s*"
    r"\((?P<year>\d{4})\)\s*[-‐‑‒–—―]\s*(?P<title>.*?)\s*::",
    re.IGNORECASE,
)

_RESERVED_WORDS = ("repealed", "reserved", "expired", "renumbered", "transferred")

_YEAR_SEG_RE = re.compile(r"/codes/arkansas/(?:19|20)\d{2}(?:/|$)")
_MD_LINK_RE = re.compile(r"\[([^\]]*)\]\((?:[^)]*)\)")  # [text](url) -> text
_MD_IMG_RE = re.compile(r"!\[[^\]]*\]\([^)]*\)")  # ![alt](url) -> ""
_BULLET_RE = re.compile(r"^[*\-]\s+")  # leading markdown list marker
_LABEL_SPACE_RE = re.compile(r"^(\([0-9A-Za-z]+\))(?=\S)")  # "(a)text" -> "(a) text"
_WS_RE = re.compile(r"[ \t ]+")


def parse_heading(text: str) -> tuple[str, str, str] | None:
    """Return (section_number, edition_year, heading_title) from a section page.

    None if the page is not a section page (no "Arkansas Code § ... - ... ::").
    """
    m = _HEADING_RE.search(text or "")
    if not m:
        return None
    return m.group("num"), m.group("year"), m.group("title").strip()


def _strip_md(line: str) -> str:
    line = _MD_IMG_RE.sub("", line)
    line = _MD_LINK_RE.sub(r"\1", line)
    line = line.replace("**", "").replace("&amp;", "&")
    return _WS_RE.sub(" ", line).strip()


def extract_body(text: str) -> list[str]:
    """The statute body as ordered paragraphs (subsections + history lines).

    Isolates the region between the universal-citation blurb and the Disclaimer
    footer, drops Previous/Next navigation and images, and normalizes each
    ``* **(a)**text`` bullet into a ``(a) text`` paragraph so the downstream
    chunker can break on subsection boundaries.
    """
    if not text:
        return []
    region = text

    # 1) Cut the footer at the Justia disclaimer (present on every code page).
    for marker in ("Disclaimer:", "Disclaimer\n", "\nDisclaimer"):
        idx = region.find(marker)
        if idx >= 0:
            region = region[:idx]
            break

    # 2) Cut the header: prefer the end of the universal-citation blurb, then
    #    the "AR Code § ... (year)" line, then the section heading "... ::".
    cut = -1
    for marker in ("official citation.", "official citation"):
        k = region.find(marker)
        if k >= 0:
            cut = k + len(marker)
            break
    if cut < 0:
        m = re.search(r"AR Code\s*§[^\n]*\(\d{4}\)", region)
        if m:
            cut = m.end()
    if cut < 0:
        m = _HEADING_RE.search(region)
        if m:
            cut = m.end()
    if cut >= 0:
        region = region[cut:]

    # 3) Line-level clean: drop nav, images, empties.
    out: list[str] = []
    for raw in region.split("\n"):
        if "[Previous]" in raw or "[Next]" in raw:
            continue
        line = _strip_md(raw)
        line = _BULLET_RE.sub("", line)  # drop leading "* " / "- " list marker
        line = line.strip("*").strip()  # drop *italic* wrappers (amendment lines)
        if not line:
            continue
        line = _LABEL_SPACE_RE.sub(r"\1 ", line)  # "(a)text" -> "(a) text"
        out.append(line)
    return out


def extract_body_html(html: str, section_number: str) -> tuple[str, list[str]]:
    """Parse a raw Justia section HTML page (ScrapFly / Wayback): (heading, paras).

    Justia layout is stable: an ``<h1>`` whose text is the full breadcrumb ending
    in ``Section N-N-NNN - Name``, followed by the body ``<p>`` blocks up to the
    ``Disclaimer:`` footer. Same HTML whether the page came from ScrapFly (live,
    current) or a Wayback snapshot (archived).
    """
    from bs4 import BeautifulSoup  # local import: only the HTML path needs bs4

    soup = BeautifulSoup(html, "html.parser")
    h1 = soup.find("h1")
    heading = ""
    if h1 is not None:
        raw = _WS_RE.sub(" ", h1.get_text(" ")).strip()
        # The h1 carries the whole breadcrumb; the section name is the segment
        # after the LAST "Section {num} -", so search (not match-from-start).
        m = re.search(rf"Section\s+{re.escape(section_number)}\s*[-‐‑‒–—―]\s*(.+?)\s*$", raw)
        heading = m.group(1).strip() if m else raw

    paras: list[str] = []
    if h1 is not None:
        last = None
        for sib in h1.next_elements:
            tag = getattr(sib, "name", None)
            if tag == "p" or tag in {"h2", "h3", "li"}:
                text = _WS_RE.sub(" ", sib.get_text(" ")).strip()
                low = text.lower()
                # Stop at the footer / third-party chrome. The newsletter signup
                # block ("You're all set! ... Justia Opinion Summary Newsletters")
                # sits in the content column before the Disclaimer; a statute body
                # never contains "justia", so it is a safe hard stop that also
                # keeps any third-party name out of the corpus text.
                if (
                    low.startswith("disclaimer")
                    or "make your practice" in low
                    or "justia" in low
                    or low.startswith("you're all set")
                ):
                    break
                if not text or text in {"Previous Next", "Previous", "Next"}:
                    continue
                if text.startswith("Universal Citation:"):
                    continue
                if text == last:
                    continue
                paras.append(text)
                last = text
    return heading, paras


def section_status(heading: str, paragraphs: list[str]) -> str | None:
    """ "repealed" when the section is a repealed/reserved stub, else None."""
    head = (heading or "").lower()
    if any(w in head for w in _RESERVED_WORDS):
        return "repealed"
    body = " ".join(paragraphs[:2]).lower()
    if body and len(body) < 120 and any(w in body for w in _RESERVED_WORDS):
        return "repealed"
    return None


def _norm_url(u: str) -> str:
    u = u.split("#", 1)[0].split("?", 1)[0]
    if not u.endswith("/"):
        u += "/"
    return u


def links_from_html(html: str) -> list[str]:
    """Absolute law.justia.com hrefs from a raw HTML page (ScrapFly path).

    ScrapFly returns Justia's real HTML, so enumeration parses ``<a href>``
    directly (relative links are absolutized) instead of using Exa's link list.
    """
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")
    out: list[str] = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("/"):
            href = "https://law.justia.com" + href
        if "/codes/arkansas/" in href:
            out.append(href)
    return out


def child_links(current_url: str, links: list[str]) -> list[str]:
    """Child TOC/section URLs one path segment below ``current_url``.

    Keeps only current-edition (no year prefix) Arkansas links that extend the
    current node's path by exactly one segment, so cross-references (other
    titles), sibling nav, and old-edition trees are never followed.
    """
    cur = _norm_url(current_url)
    seen: set[str] = set()
    out: list[str] = []
    for link in links:
        if "/codes/arkansas/" not in link:
            continue
        if _YEAR_SEG_RE.search(link):
            continue
        ln = _norm_url(link)
        if not ln.startswith(cur):
            continue
        rest = ln[len(cur) :].strip("/")
        if not rest or "/" in rest:
            continue
        if ln in seen:
            continue
        seen.add(ln)
        out.append(ln)
    return out
