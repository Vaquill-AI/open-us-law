"""Parse the CA bulk export's relational tables and rebuild the code tree.

The export is a real database dump, not scraped HTML. Four tables matter:

    CODES_TBL.dat             30 codes: `LAB` -> `Labor Code - LAB`
    LAW_TOC_TBL.dat           24,360 hierarchy nodes (heading, depth, tree path)
    LAW_TOC_SECTIONS_TBL.dat  162,302 rows: section -> TOC node id
    LAW_SECTION_TBL.dat       162,302 rows: section -> .lob text + metadata

Rebuilding the tree from LAW_TOC_TBL is what lets us emit an act_id in the same
component ORDER the HTML scraper produced. The order is not derivable from the
section row alone: LAW_SECTION_TBL stores hierarchy in fixed positional columns
(division, title, part, chapter, article), but each CA code nests them
differently (Civil = Division>Part>Title>Chapter, Civil Procedure =
Part>Title>Chapter, Business & Professions = Division>Chapter>Article). Walking
the real tree removes the guesswork.

Rows are tab-separated; values are wrapped in backticks, with a literal NULL for
absent fields.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

# --- LAW_SECTION_TBL columns (verified against CIV 1950.5 and LAB 7856) ------
S_ID, S_CODE, S_SECT = 0, 1, 2
S_EFFECTIVE = 6
S_HISTORY = 13
S_LOB = 14
S_TIMESTAMP = 17

# --- LAW_TOC_TBL columns ----------------------------------------------------
T_CODE = 0
T_HEADING = 6
T_NODE_ID = 10
T_DEPTH = 11
T_PATH = 13

# --- LAW_TOC_SECTIONS_TBL columns -------------------------------------------
# TS_TOC_PATH joins to LAW_TOC_TBL's PATH column (T_PATH), not its node-id
# column: the values here are tree addresses ("1", "2.10", "3.18.1.1"), which
# is also why node_id is unusable as a join key (it is only unique per code).
TS_CODE = 1
TS_TOC_PATH = 2
TS_SECT = 3

# Heading prefixes -> the level_classifier the HTML scraper used. The act_id
# uses the first letter of the classifier (see node_to_payload._act_id).
_LEVEL_RE = re.compile(
    r"^\s*(DIVISION|SUBDIVISION|PART|TITLE|CHAPTER|ARTICLE|SUBCHAPTER|SUBARTICLE)\s+"
    r"([0-9][0-9A-Za-z.\-]*)",
    re.IGNORECASE,
)


def unquote(v: str) -> str | None:
    """Backtick-wrapped value -> str, or None for NULL/empty."""
    v = v.strip()
    if v.startswith("`") and v.endswith("`") and len(v) >= 2:
        v = v[1:-1]
    return None if v in ("", "NULL") else v


def rows(text: str) -> list[list[str]]:
    return [ln.split("\t") for ln in text.splitlines() if ln.strip()]


@dataclass
class TocNode:
    node_id: str
    code: str
    heading: str
    depth: int
    path: str
    level: str | None = None   # "division" | "chapter" | ...
    number: str | None = None  # "3" | "1.5" | ...
    parent_path: str | None = None
    children: list[str] = field(default_factory=list)


def parse_codes(text: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for r in rows(text):
        if len(r) >= 2:
            code, name = unquote(r[0]), unquote(r[1])
            if code and name:
                out[code.upper()] = name
    return out


def parse_toc(text: str) -> dict[tuple[str, str], TocNode]:
    """(code, node_id) -> TocNode, with level/number parsed out of the heading.

    Keyed by (code, node_id) because node_id is only unique WITHIN a code: id
    97 is a CIV chapter and also an FGC chapter. Keying on node_id alone
    collapses 24,360 rows to ~2,900 by last-write-wins and then joins sections
    to another code's tree entirely.
    """
    nodes: dict[tuple[str, str], TocNode] = {}
    for r in rows(text):
        if len(r) <= T_PATH:
            continue
        node_id = (r[T_NODE_ID] or "").strip()
        path = unquote(r[T_PATH]) or ""
        code = unquote(r[T_CODE]) or ""
        heading = unquote(r[T_HEADING]) or ""
        try:
            depth = int((r[T_DEPTH] or "0").strip())
        except ValueError:
            depth = 0
        if not node_id or not code:
            continue
        n = TocNode(node_id=node_id, code=code.upper(), heading=heading, depth=depth, path=path)
        m = _LEVEL_RE.match(heading)
        if m:
            n.level = m.group(1).lower()
            n.number = m.group(2).rstrip(".")
        # The path is a positional tree address ("5", "5.1", "4.26"), so the
        # parent is simply the path minus its last segment.
        if "." in path:
            n.parent_path = path.rsplit(".", 1)[0]
        nodes[(n.code, node_id)] = n
    return nodes


def index_by_path(nodes: dict[tuple[str, str], TocNode]) -> dict[tuple[str, str], TocNode]:
    """(code, path) -> node, so we can walk parents by trimming the path."""
    return {(n.code, n.path): n for n in nodes.values()}


def ancestors(node: TocNode, by_path: dict[tuple[str, str], TocNode]) -> list[TocNode]:
    """Root-first chain of nodes from the code root down to ``node``."""
    chain: list[TocNode] = []
    cur: TocNode | None = node
    seen: set[str] = set()
    while cur is not None:
        if cur.path in seen:  # defensive: malformed path cycle
            break
        seen.add(cur.path)
        chain.append(cur)
        if not cur.parent_path:
            break
        cur = by_path.get((cur.code, cur.parent_path))
    return list(reversed(chain))


def act_id_for(code: str, section: str, chain: list[TocNode]) -> str:
    """Rebuild the scraper's act_id: STATE_CA_C<code>_<L><num>..._S<section>.

    Mirrors vaquill_pipeline.node_to_payload._act_id, which emits
    f"{level[0].upper()}{number}" for each hierarchy component in tree order.
    Nodes whose heading has no parseable level/number (unnamed wrappers) are
    skipped, exactly as they contribute no component in the scraped tree.
    """
    parts = [f"STATE_CA_C{code.lower()}"]
    for n in chain:
        if n.level and n.number:
            parts.append(f"{n.level[0].upper()}{n.number}")
    parts.append(f"S{section}")
    return "_".join(parts)
