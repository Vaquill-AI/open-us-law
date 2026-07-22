"""CAML (California Automated Markup Language) -> plain text.

Section bodies in the CA bulk export are stored one-per-file as CAML XML. The
common shape is simple:

    <caml:Content xmlns:caml="...">
      <p>(a)<span class="EnSpace"/>This section applies to security ...</p>
      <p>(b)<span class="EnSpace"/>As used in this section ...</p>
    </caml:Content>

but a meaningful minority of sections carry richer markup that an early
p-only extractor got WRONG, and both failures were found by the ingest's
unknown-tag report rather than by eye:

  - <table> holds substantive statutory content (rate schedules, percentage
    tables). Tables are SIBLINGS of <p>, not children, so pulling only <p>
    text dropped the entire table -- e.g. an Insurance Code table
    "Single-family residence, owner occupied 80%" vanished completely.
  - <caml:Fraction><caml:Numerator>4</caml:Numerator>
    <caml:Denominator>5</caml:Denominator></caml:Fraction> is "4/5". Naive
    get_text() concatenated it to "45", turning "four-fifths (4/5)" into
    "four-fifths (45)".

So this walks the whole tree in document order instead of cherry-picking <p>,
and handles the structural tags explicitly. Anything still unrecognized keeps
its inner text (never dropped) and is reported to the caller.

Determinism matters as much as fidelity: chunk point_ids are content-addressed
as md5(act_id::chunk_index::sha1(text)), so any wobble re-embeds the affected
sections. The walk is deterministic and the output is normalized so upstream
line wrapping can never change the hash.
"""
from __future__ import annotations

import re

from bs4 import BeautifulSoup
from bs4.element import NavigableString, Tag

# Tags handled explicitly by the walker; anything else is reported as unknown
# (its text is still kept -- unknown means "review the rendering", not "drop").
_KNOWN = {
    "[document]", "caml:content", "content",
    "p", "h1", "br", "span",
    "table", "thead", "tbody", "tr", "td", "th", "col", "colgroup",
    "caml:fraction", "caml:numerator", "caml:denominator",
    "b", "i", "u", "sub", "sup", "em", "strong",
    "caml:tipin", "caml:labelledfield",
}

# Block-level tags start a new line/paragraph; inline tags flow inline.
_PARA = {"p", "h1", "table"}   # separated from neighbours by a blank line
_ROW = {"tr"}                  # one table row per line
_CELL = {"td", "th"}           # cells within a row, space-separated

_WS = re.compile(r"\s+")            # any whitespace run, inside a text node
_BLANKS = re.compile(r"\n{3,}")     # collapse 3+ newlines to a paragraph gap


def _walk(node) -> str:
    if isinstance(node, NavigableString):
        # Whitespace INSIDE a text node (including newlines) is source line
        # wrapping, i.e. formatting, so collapse it to single spaces. The only
        # structural newlines are the ones the walker injects below for
        # paragraphs and table rows; keeping raw newlines here would turn an
        # upstream line wrap into a spurious paragraph break and change the hash.
        return _WS.sub(" ", str(node))
    if not isinstance(node, Tag):
        return ""

    name = node.name.lower()

    if name == "br":
        return "\n"

    if name == "span":
        # The only span seen is the EnSpace subdivision spacer, which must
        # become a real space so "(a)<span/>This" is "(a) This", not "(a)This".
        cls = node.get("class") or []
        if any("enspace" in c.lower() for c in cls) or not node.get_text(strip=True):
            return " "
        return _children(node)

    if name == "caml:fraction":
        num = node.find("caml:numerator")
        den = node.find("caml:denominator")
        if num and den:
            frac = f"{num.get_text(strip=True)}/{den.get_text(strip=True)}"
            # Mixed number: a whole number sits in the text immediately before
            # the fraction ("1<Fraction>1/4</Fraction> days" = one and a
            # quarter). Without a space that renders "11/4" (eleven-fourths).
            # Add one only when the preceding char is a digit, so a parenthetical
            # fraction "(4/5)" is left tight.
            prev = node.previous_sibling
            if isinstance(prev, NavigableString) and prev.rstrip()[-1:].isdigit():
                frac = " " + frac
            return frac
        return _children(node)

    inner = _children(node)

    if name in _CELL:
        return inner.strip() + " "
    if name in _ROW:
        return "\n" + inner.strip()
    if name in _PARA:
        return "\n\n" + inner + "\n\n"
    # thead/tbody/colgroup/inline formatting: pass inner text through.
    return inner


def _children(node: Tag) -> str:
    return "".join(_walk(c) for c in node.children)


def _unknown_tags(soup: BeautifulSoup) -> set[str]:
    return {
        t.name.lower()
        for t in soup.find_all(True)
        if t.name.lower() not in _KNOWN
    }


def caml_to_text(xml: str) -> tuple[str, set[str]]:
    """Return ``(plain_text, unknown_tags)`` for one CAML section body.

    Paragraphs and tables are separated by blank lines; table rows by single
    newlines; cells by spaces. Nothing is dropped: any unrecognized element's
    inner text is preserved and the element name reported for review.
    """
    if not xml or not xml.strip():
        return "", set()

    soup = BeautifulSoup(xml, "html.parser")
    unknown = _unknown_tags(soup)

    text = _walk(soup)

    # Text-node whitespace is already collapsed in _walk; here just trim each
    # structural line and collapse blank-line runs to a single paragraph gap.
    lines = [ln.strip() for ln in text.split("\n")]
    text = "\n".join(lines)
    text = _BLANKS.sub("\n\n", text).strip()
    return text, unknown
