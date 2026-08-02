"""Parser for the SDCL whole-title HTML (``/api/Statutes/{title}.html?all=true``).

One document per title carries the chapter table of contents PLUS every section's
heading, body, and Source history. Section boundaries are the heading paragraphs:
a paragraph whose CSS class ends in ``Normal`` and whose SENU span holds the
canonical citation (e.g. ``22-16-4``). Body paragraphs that follow, sharing the
heading's CSS hash prefix, belong to that section until the next heading; a
``Source:`` line is diverted to the section's amendment history.

Chapter is derived deterministically from the section number's middle segment
(``22-16-4`` -> chapter ``16``), so the Title -> Chapter -> Section hierarchy, and
therefore the act_id, reproduce the existing scraper's node path exactly. The
output is deterministic: identical HTML -> identical paragraphs -> identical
content-addressed point_id across runs.
"""

from __future__ import annotations

import re
import urllib.parse

from bs4 import BeautifulSoup

from .walk import SECTION_NUM_RE, SDSection, chapter_from_section, title_from_section

_WS = re.compile(r"\s+")

# Section-heading paragraphs have a class ending exactly in "Normal" (no suffix);
# body-text classes end with suffixes like "Normal-000000", "Statute", "NoIndent".
_CLASS_HEADING_RE = re.compile(r"Normal$")
# TOC entries (the chapter/section listing) carry a class ending in "B".
_CLASS_TOC_RE = re.compile(r"B$")
_SOURCE_RE = re.compile(r"^Source:", re.IGNORECASE)
_HASH_RE = re.compile(r"^(s[0-9a-f]+)")

RESERVED_KEYWORDS = ("repealed", "transferred", "expired", "reserved", "superseded", "omitted")
_RESERVED_RE = re.compile(
    r"\b(?:" + "|".join(RESERVED_KEYWORDS) + r")\b(?=$|\s*[\.\,\;\:\)\]\}]|\s*$)",
    re.IGNORECASE,
)


def _clean(raw: str) -> str:
    text = raw.replace("\xa0", " ").replace("\r", " ").replace("\n", " ")
    return _WS.sub(" ", text).strip()


def _hash_of(cls: str) -> str | None:
    m = _HASH_RE.match(cls)
    return m.group(1) if m else None


def _status_of(name: str) -> str | None:
    if not name:
        return None
    if not _RESERVED_RE.search(name):
        return None
    low = name.lower()
    if "repealed" in low:
        return "repealed"
    if "transferred" in low:
        return "transferred"
    return "reserved"


def _section_num_from_heading(p) -> str | None:
    """Canonical section number from a heading paragraph.

    Tries the SENU span text first, then a ``?Statute=`` href, then a regex on the
    plain text; each candidate is validated against the section-number grammar.
    """
    for span in p.find_all("span"):
        if "SENU" in " ".join(span.get("class", [])):
            num = span.get_text(strip=True)
            m = SECTION_NUM_RE.match(num)
            if m:
                return m.group(1)
    for a in p.find_all("a", href=True):
        qs = urllib.parse.parse_qs(urllib.parse.urlparse(a["href"]).query)
        num = (qs.get("Statute") or [""])[0]
        m = SECTION_NUM_RE.match(num)
        if m:
            return m.group(1)
    # Deliberately NO plain-text regex fallback: every genuine SDCL heading
    # carries a SENU span (or ?Statute= anchor), whereas a body paragraph that
    # merely starts with a triple-dashed token (e.g. the phone number
    # "605-773-6811", 605 being SD's area code) would be mis-read as a heading.
    return None


def _name_from_heading(plain: str, sec_num: str) -> str:
    """Strip the leading citation prefix off a heading's plain text to get the headnote."""
    tail = re.sub(r"^[\d\.\-A-Za-z]*?" + re.escape(sec_num) + r"\.?\s*", "", plain, count=1)
    if tail == plain:  # prefix not found at head; fall back to a generic strip
        tail = re.sub(r"^[\d\.\-]+\.\s*", "", plain)
    return tail.rstrip(".").strip()


def parse_title_sections(
    html: str, title_label: str | int
) -> list[tuple[SDSection, list[str], str, str | None]]:
    """Parse a whole title's HTML into ordered (section, paragraphs, source, status).

    ``source`` is the raw ``Source:`` history line (may be empty); ``status`` is
    "repealed"/"reserved"/"transferred" when the headnote says so, else None.
    Sections are yielded in document order; repealed/reserved stubs typically have
    no body and are dropped downstream by node_to_chunks (text < 20 chars).
    """
    soup = BeautifulSoup(html, "html.parser")
    out: list[tuple[SDSection, list[str], str, str | None]] = []

    cur_hash: str | None = None
    cur_num: str | None = None
    cur_name: str | None = None
    cur_body: list[str] = []
    cur_source: str = ""

    def _flush() -> None:
        if cur_num is None:
            return
        chapter = chapter_from_section(cur_num)
        sec_title = title_from_section(cur_num)
        if chapter is None or sec_title is None:
            return
        section = SDSection(
            title_number=sec_title,
            chapter=chapter,
            section_number=cur_num,
            section_title=cur_name or "",
        )
        out.append((section, list(cur_body), cur_source.strip(), _status_of(cur_name or "")))

    for p in soup.find_all("p"):
        cls_list = p.get("class", [])
        if not cls_list:
            continue
        cls = cls_list[0]
        if _CLASS_TOC_RE.search(cls):
            continue  # chapter/section table-of-contents row
        cls_hash = _hash_of(cls)
        if cls_hash is None:
            continue

        if _CLASS_HEADING_RE.search(cls):
            plain = _clean(p.get_text(strip=True))
            # A Source line also carries the "Normal" class and often embeds a
            # self-referential ?Statute= link / SENU span, so it must be routed to
            # history BEFORE heading detection or it is mis-read as a new section.
            if _SOURCE_RE.match(plain):
                if cls_hash == cur_hash and cur_num is not None:
                    cur_source += _clean(p.get_text(separator=" ")) + " "
                continue
            sec_num = _section_num_from_heading(p)
            # A genuine heading's visible text STARTS with its section number AND
            # the section belongs to the title being parsed (its number is
            # title-first, e.g. every section in title 22 is "22-..."). Both
            # guards together reject any residual link-bearing or numeric-looking
            # body paragraph.
            if (
                sec_num is not None
                and SECTION_NUM_RE.match(plain)
                and title_from_section(sec_num) == str(title_label)
            ):
                _flush()
                cur_hash = cls_hash
                cur_num = sec_num
                cur_name = _name_from_heading(plain, sec_num)
                cur_body = []
                cur_source = ""
            continue

        # Body paragraph: belongs to the open section only if the hash matches.
        if cls_hash != cur_hash or cur_num is None:
            continue
        txt = _clean(p.get_text(separator=" "))
        if not txt:
            continue
        if _SOURCE_RE.match(txt):
            cur_source += txt + " "
        else:
            cur_body.append(txt)

    _flush()
    return out
