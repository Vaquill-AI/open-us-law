"""Parse an official Indiana Code title HTML file into section records.

The bulk "ZIP (HTML only)" export holds one WordPerfect-generated HTML file per
title (``2026_Indiana_Code_HTML/{n}.html``). Inside, title / article / chapter /
section headings are flat sibling divs:

    <div class="title"   id="35">   <span id="ic_number">IC 35</span>
                                     <span id="shortdescription">TITLE 35 ...</span></div>
    <div class="article" id="35-42"> ... </div>
    <div class="chapter" id="35-42-1"> ... </div>
    <div class="section" id="35-42-1-1"><span id="ic_number">IC 35-42-1-1</span>
                                        <span id="shortdescription">Murder</span></div>
    <p>     Sec. 1. A person who: ...</p>
    <p>(1) knowingly or intentionally kills another human being; ...</p>
    ...

A section's body is every ``<p>`` between its heading div and the next structural
div (the next section, or the chapter / article heading that starts the next
container). The structure is flat, so bodies are sliced by div position rather
than parsed into a tree; each paragraph's inner tags (cross-reference anchors,
styling spans) are stripped to text. Whitespace is collapsed so identical text
yields identical content-addressed point_ids across re-ingests.
"""

from __future__ import annotations

import html as _html
import re

# All structural headings, captured with their class so section bodies can be
# bounded by "the next structural div of any level".
_STRUCT_DIV = re.compile(r'<div class="(title|article|chapter|section)" id="([^"]+)"')
_SHORTDESC = re.compile(r'<span id="shortdescription"[^>]*>(.*?)</span>', re.DOTALL)
_P_BLOCK = re.compile(r"<p\b[^>]*>(.*?)</p>", re.DOTALL)
_TAG = re.compile(r"<[^>]+>")
_WS = re.compile(r"\s+")

_RESERVED = ("repealed", "expired", "renumbered", "transferred", "reserved", "vacated")


def _text(fragment: str) -> str:
    """Strip inner tags, unescape entities, normalize whitespace."""
    t = _TAG.sub("", fragment)
    t = _html.unescape(t)
    t = t.replace("\xa0", " ").replace(" ", " ").replace(" ", " ")
    return _WS.sub(" ", t).strip()


def title_number(html: str) -> str | None:
    m = re.search(r'<div class="title" id="([^"]+)"', html)
    return m.group(1).strip() if m else None


def _status_for(section_title: str, first_para: str) -> str | None:
    blob = f"{section_title} {first_para}".lower()
    for kw in _RESERVED:
        if kw in blob:
            return "repealed" if kw in ("repealed", "expired", "vacated") else "reserved"
    return None


def iter_section_blocks(html: str):
    """Yield (section_id, section_title, [paragraph, ...], status) for each
    section in a title HTML file, in document order.

    Sections with no body paragraphs (pure structural stubs) are still yielded
    with an empty list; the orchestrator drops them (node_to_chunks also returns
    [] for empty text), so no empty points are created.
    """
    matches = list(_STRUCT_DIV.finditer(html))
    n = len(matches)
    for i, m in enumerate(matches):
        cls, node_id = m.group(1), m.group(2)
        if cls != "section":
            continue
        end = matches[i + 1].start() if i + 1 < n else len(html)
        block = html[m.start():end]

        sd = _SHORTDESC.search(block)
        section_title = _text(sd.group(1)) if sd else ""

        paras: list[str] = []
        for pm in _P_BLOCK.finditer(block):
            p = _text(pm.group(1))
            if p:
                paras.append(p)

        status = _status_for(section_title, paras[0] if paras else "")
        yield node_id, section_title, paras, status
