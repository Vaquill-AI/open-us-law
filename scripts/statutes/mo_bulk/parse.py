"""HTML parsers for the revisor.mo.gov pages.

Three parse steps mirror the three page types:
  - ``chapter_numbers``  : Home.aspx -> every distinct chapter number.
  - ``chapter_sections`` : OneChapter.aspx -> (section number, title) for a
    chapter, collected across ALL tables (the old scraper read only the first
    table, which is why it was thin).
  - ``section_content``  : OneSection.aspx -> ordered body paragraphs + the
    history/source note. This reuses the exact DOM path the old scraper used
    (which extracted text correctly); only enumeration was broken.
"""

from __future__ import annotations

import re

from bs4 import BeautifulSoup

# Home.aspx chapter links: /main/OneChapter.aspx?chapter=565
_CHAPTER_RE = re.compile(r"OneChapter\.aspx\?chapter=([0-9][\w.]*)")

# Section links on a chapter page point at PageSelect.aspx?section=565.001&bid=..
# (the section number is the query param, canonical and uniform across rows).
_SECTION_RE = re.compile(r"[?&]section=([0-9][0-9.]*[A-Za-z]?)")

# Trailing effective-date suffix the site appends to some section titles.
_EFF_DATE_RE = re.compile(r"\s*\(\d{1,2}/\d{1,2}/\d{4}\)\s*$")


def _clean_text(raw: str) -> str:
    """Normalize whitespace and drop non-breaking / soft-hyphen artifacts."""
    text = raw.replace("\xa0", " ").replace(" ", " ").replace(" ", " ")
    text = text.replace("\xad", "")  # soft hyphen (the site pads history notes)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def chapter_numbers(home_html: str) -> list[str]:
    """Every distinct chapter number linked from the TOC, numerically sorted.

    The TOC lists each chapter twice (grouped by title and alphabetically), so
    the raw link set is deduped. RSMo chapters are all integers.
    """
    seen: set[str] = set()
    for m in _CHAPTER_RE.finditer(home_html):
        seen.add(m.group(1).strip())

    def _key(c: str):
        try:
            return (0, int(c), c)
        except ValueError:
            return (1, 0, c)

    return sorted(seen, key=_key)


def chapter_sections(chapter_html: str, chapter_number: str) -> list[tuple[str, str]]:
    """Return ``(section_number, section_title)`` for one chapter.

    Iterates every row of every table (the fix for the first-table-only bug).
    A section is kept only when its number's integer prefix equals this chapter,
    which drops the occasional cross-reference link to a section in another
    chapter (e.g. a note citing 3.090 on the chapter 565 page) so each section is
    attributed to its own chapter exactly as the citation scheme requires.
    Deduped by section number, preserving first-seen order.
    """
    soup = BeautifulSoup(chapter_html, "html.parser")
    out: list[tuple[str, str]] = []
    seen: set[str] = set()

    for row in soup.find_all("tr"):
        tds = row.find_all("td")
        if not tds:
            continue
        link = tds[0].find("a", href=True)
        if link is None:
            continue
        m = _SECTION_RE.search(link["href"])
        if not m:
            # Fall back to the visible link text (also the section number).
            txt = _clean_text(link.get_text())
            if not re.fullmatch(r"[0-9][0-9.]*[A-Za-z]?", txt):
                continue
            secnum = txt
        else:
            secnum = m.group(1).strip()

        if secnum in seen:
            continue
        if secnum.split(".")[0] != str(chapter_number):
            continue  # cross-reference to another chapter's section
        seen.add(secnum)

        title = _clean_text(tds[1].get_text()) if len(tds) > 1 else ""
        title = _EFF_DATE_RE.sub("", title).strip()
        out.append((secnum, title))

    return out


def section_content(section_html: str) -> tuple[list[str], str]:
    """Extract ``(body_paragraphs, history_note)`` from a section page.

    Layout under #BOTTOM's previous sibling::

        div (outer)
          div (first child: nav + norm)
            div.norm
              p.norm ...   (section body paragraphs)
              div.foot     (history / source note)

    Only the ``norm`` div's direct children are read (``recursive=False``), so the
    history text nested inside ``div.foot`` is captured once as the note and not
    also duplicated as a body paragraph.
    """
    soup = BeautifulSoup(section_html, "html.parser")
    bottom = soup.find(id="BOTTOM")
    if bottom is None:
        return [], ""
    outer = bottom.find_previous_sibling()
    if outer is None:
        return [], ""
    first_child = outer.find(recursive=False)
    if first_child is None:
        return [], ""
    norm_div = first_child.find("div", class_="norm")
    if norm_div is None:
        return [], ""

    paras: list[str] = []
    history = ""
    for element in norm_div.find_all(recursive=False):
        cls_list = element.get("class", []) or []
        if element.name == "div" and "foot" in cls_list:
            raw = _clean_text(element.get_text(separator=" "))
            history = re.sub(r"^[\-\xad\s]+", "", raw).strip()
            continue
        if element.name == "p":
            text = _clean_text(element.get_text(separator=" "))
            if text:
                paras.append(text)

    return paras, history
