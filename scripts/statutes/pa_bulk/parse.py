"""Parse the official Pennsylvania per-title consolidated PDF into sections.

Why the PDF and not the HTML viewer: the ``view-statute?txtType=PDF&ttl=NN``
export is the full title in one file, so the whole ingest is ~79 proxied
requests instead of tens of thousands of per-section fetches. The extracted text
is clean single-column prose (no page-header/footer noise interrupting bodies),
so a text parse is reliable.

The two parsing problems and how they are solved:

  1. TOC vs body. A PA title PDF opens with a title-level table of contents AND a
     per-chapter table of contents, so every section number appears several
     times before its actual body. We take the LAST alone-on-a-line ``§ N.``
     occurrence of each number as its body header (all the TOCs precede the
     body). Validated on Titles 1/18/23/42/75: distinct numbers == body sections
     with no duplicates.

  2. Cross-references. Inline citations like ``44 Pa.C.S. § 2316.2 (relating
     to ...)`` must not be mistaken for section headers. A real header is the
     ONLY thing on its line and ends with a period (``§ 2502.``); an inline
     cross-reference never is (the number is mid-line, preceded by ``Pa.C.S. §``
     and followed by text). The alone-on-line rule excludes them.

Section numbers are emitted in canonical DOT form exactly as the code prints
them (``2502``, ``3132.1``, ``9799.11``) -> citation ``18 Pa.C.S. § 2502``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

# A section header is the ONLY content on its line: "§ 2502." (optionally spaced,
# allowing NBSP). The number is digits with optional decimal / trailing letter
# (e.g. 2502, 3132.1, 9799.11). Anchored to a full line so inline cross-refs are
# excluded.
_HDR_RE = re.compile(r"(?m)^[ \t ]*§[ \t ]*([0-9][0-9A-Za-z.]*?)\.[ \t ]*$")

# Structural markers that end a section body (next chapter / subchapter / part /
# article header, or a within-body mini table of contents "Sec.").
_STRUCT_RE = re.compile(
    r"(?m)^[ \t ]*(CHAPTER\b|Chapter [0-9A-Z]|SUBCHAPTER\b|Subchapter [A-Z]"
    r"|PART\b|Part [0-9A-Z]|ARTICLE\b|Article [0-9A-Z]|Sec\.[ \t ]*$"
    r"|TABLE OF CONTENTS)"
)

# A line that is nothing but a page number.
_PAGENUM_RE = re.compile(r"(?m)^[ \t ]*\d{1,4}[ \t ]*$")

# Index-page title links: ...ttl=NN...>NAME</a>
_INDEX_TITLE_RE = re.compile(r"ttl=(\d{1,2})[^>]*>(.*?)</a>", re.S)
_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")

_RESERVED_KEYWORDS = ("(reserved)", "(repealed)", "(expired)", "(renumbered)", "(deleted)")

# New-paragraph line starts: subsection/paragraph markers like (a) (1) (i) (a.1)
# (1.1), or a defined term in quotes like "Fireman."
_NEWPARA_RE = re.compile(
    r'^(\((?:[0-9]+|[0-9]+\.[0-9]+|[a-z]|[a-z]\.[0-9]+|[A-Z]|[ivxlcdm]+)\)|"[^"]+\.")'
)


@dataclass(frozen=True)
class ParsedSection:
    number: str  # canonical dot form, e.g. "2502", "3132.1"
    heading: str
    paragraphs: tuple[str, ...]
    status: str | None  # "reserved" / "repealed" / None


# ---------------------------------------------------------------------------
# Index / title universe + currency
# ---------------------------------------------------------------------------


def consolidated_titles(index_html: str) -> list[tuple[str, str]]:
    """All (title_number, title_name) pairs linked from the consolidated index.

    Returns every title slot (1..79); whether a title is actually consolidated
    (has content) or "Reserved" is decided from the title's own PDF (a reserved
    title parses to zero sections), never guessed here.
    """
    seen: set[str] = set()
    out: list[tuple[str, str]] = []
    for m in _INDEX_TITLE_RE.finditer(index_html):
        ttl = m.group(1).lstrip("0") or "0"
        if ttl in seen or ttl == "0":  # ttl=0 is the Constitution, not a statute
            continue
        name = _WS_RE.sub(" ", _TAG_RE.sub(" ", m.group(2))).strip()
        if not name:
            continue
        seen.add(ttl)
        out.append((ttl, name))
    out.sort(key=lambda p: int(p[0]))
    return out


def current_through(index_html: str) -> str | None:
    """Best-effort 'current through Act N of YYYY' currency string from the index."""
    for pat in (
        r"current(?:ly)?\s+through[^.<]{0,80}",
        r"through\s+Act\s+\d+\s+of\s+\d{4}",
        r"Act\s+\d+\s+of\s+\d{4}[^<]{0,40}",
    ):
        m = re.search(pat, index_html, re.I)
        if m:
            return _WS_RE.sub(" ", _TAG_RE.sub(" ", m.group(0))).strip()
    return None


# ---------------------------------------------------------------------------
# Section splitting + text
# ---------------------------------------------------------------------------


def _status_for(heading: str, body: str) -> str | None:
    hay = f"{heading} {body[:120]}".lower()
    for kw in _RESERVED_KEYWORDS:
        if kw in hay:
            return "repealed" if "repeal" in kw else "reserved"
    return None


def dewrap(text: str) -> list[str]:
    """Join hard-wrapped lines into paragraphs.

    The PDF hard-wraps every ~65 characters at word boundaries (never mid-word),
    so joining wrapped lines with a space restores readable prose. A new
    paragraph starts on a blank line or a subsection/definition marker.
    """
    paras: list[str] = []
    buf = ""
    for raw in text.split("\n"):
        s = raw.strip()
        if not s:
            if buf:
                paras.append(buf)
                buf = ""
            continue
        if _NEWPARA_RE.match(s):
            if buf:
                paras.append(buf)
            buf = s
        else:
            buf = f"{buf} {s}".strip() if buf else s
    if buf:
        paras.append(buf)
    return paras


def _split_heading_body(seg: str) -> tuple[str, str]:
    """Split a section's text (after its ``§ N.`` header) into (heading, body).

    A PA section heading is a short title phrase that TERMINATES with a period
    and may wrap across a few lines; the statutory text then follows, usually
    opening with a subsection marker ``(a)`` but sometimes with prose. So the
    heading is the run of lines up to and including the first line that ends with
    a period, or up to (not including) the first subsection/definition marker --
    whichever comes first. Decimal sections (908.1, 1102.1) are handled the same
    way; they are parsed from the body directly, not the TOC, because the TOC
    does not list them as their own ``§ N.`` header line.
    """
    lines = seg.split("\n")
    idx = 0
    while idx < len(lines) and not lines[idx].strip():
        idx += 1
    hlines: list[str] = []
    while idx < len(lines):
        s = lines[idx].strip()
        if not s:
            idx += 1
            continue
        if _NEWPARA_RE.match(s):  # body starts (subsection / defined term)
            break
        hlines.append(s)
        idx += 1
        if s.endswith("."):  # heading phrase terminates
            break
        if len(hlines) >= 8:  # runaway guard
            break
    heading = _WS_RE.sub(" ", " ".join(hlines)).strip()
    body = "\n".join(lines[idx:])
    return heading, body


def parse_title_pdf(pdf_bytes: bytes) -> list[ParsedSection]:
    """Parse a per-title consolidated PDF into ordered ParsedSection rows."""
    import fitz  # PyMuPDF

    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    full = "\n".join(doc[i].get_text() for i in range(doc.page_count))
    doc.close()

    heads = list(_HDR_RE.finditer(full))
    if not heads:
        return []

    # Last alone-on-a-line occurrence per number = its body header (all the
    # front tables of contents precede the body).
    last: dict[str, re.Match] = {}
    for m in heads:
        last[m.group(1)] = m
    body = sorted(last.values(), key=lambda m: m.start())
    starts = [m.start() for m in body]

    out: list[ParsedSection] = []
    for i, m in enumerate(body):
        num = m.group(1)
        seg_end = starts[i + 1] if i + 1 < len(starts) else len(full)
        seg = full[m.end() : seg_end]

        # Cut at the first structural marker (next chapter/subchapter/mini-TOC).
        sm = _STRUCT_RE.search(seg)
        if sm:
            seg = seg[: sm.start()]
        seg = _PAGENUM_RE.sub("", seg)

        heading, bodytext = _split_heading_body(seg)
        paras = dewrap(bodytext)
        status = _status_for(heading, " ".join(paras))
        out.append(
            ParsedSection(
                number=num,
                heading=heading,
                paragraphs=tuple(paras),
                status=status,
            )
        )
    return out
