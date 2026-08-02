#!/usr/bin/env python3
"""Walk an Open Legislation law tree into flat Section records.

The API `result` for a law is a recursive node:
    { lawId, locationId, title, docType, docLevelId, text, documents:{items:[...]} }
docType is one of CHAPTER / ARTICLE / TITLE / SUBTITLE / PART / SECTION / ...
Only SECTION (and RULE, for the RULES volumes) nodes carry real statutory text;
the rest are structural containers we keep only for the breadcrumb / act_id path.
"""
from __future__ import annotations

from dataclasses import dataclass, field

# docTypes that are leaf content (carry section text), vs structural containers.
_LEAF_TYPES = {"SECTION", "RULE"}
# classifier name per structural docType, for the node_id path (-> act_id).
_CLS = {
    "ARTICLE": "article",
    "TITLE": "title",
    "SUBTITLE": "subtitle",
    "PART": "part",
    "SUBPART": "subpart",
}


@dataclass(frozen=True)
class Section:
    law_id: str                       # "EDN"
    law_name: str                     # "Education"
    location_id: str                  # unique doc id within the law, e.g. "318"
    doc_level_id: str                 # the section number for the citation, e.g. "318"
    title: str                        # catchline
    text: str
    ancestors: tuple = field(default_factory=tuple)  # ((classifier, docLevelId), ...)

    def citation(self) -> str:
        return f"N.Y. {self.law_id} Law § {self.doc_level_id}"


def iter_sections(result: dict):
    """Yield Section for every leaf (SECTION/RULE) node with text."""
    info = result.get("info") or {}
    law_id = info.get("lawId") or result.get("documents", {}).get("lawId") or ""
    law_name = info.get("name") or ""
    root = result.get("documents")
    if not root:
        return

    def _walk(node: dict, anc: tuple):
        doc_type = node.get("docType") or ""
        if node.get("repealed"):
            return
        if doc_type in _LEAF_TYPES:
            text = (node.get("text") or "").strip()
            if text:
                yield Section(
                    law_id=law_id,
                    law_name=law_name,
                    location_id=node.get("locationId") or node.get("docLevelId") or "",
                    doc_level_id=node.get("docLevelId") or node.get("locationId") or "",
                    title=(node.get("title") or "").strip(),
                    text=text,
                    ancestors=anc,
                )
            return
        items = (node.get("documents") or {}).get("items") or []
        # Single-blob law: a structural node (typically the root CHAPTER) that
        # carries the whole act's text directly and has NO child documents. Some
        # unconsolidated NY acts (e.g. LEH "Local Emergency Housing Rent Control
        # Act", NNY "New, New York Bond Act") are published this way, with 54 KB
        # of statutory text on the CHAPTER node and zero SECTION children. Without
        # this branch the leaf-only walk yields nothing and the entire act is
        # dropped. Emit the node itself as one section so its text is captured.
        # Only fires when there are no children, so structural containers with
        # sections are never double-emitted.
        if not items:
            text = (node.get("text") or "").strip()
            if text:
                yield Section(
                    law_id=law_id,
                    law_name=law_name,
                    location_id=node.get("locationId") or node.get("docLevelId") or "",
                    doc_level_id=node.get("docLevelId") or node.get("locationId") or "",
                    title=(node.get("title") or "").strip(),
                    text=text,
                    ancestors=anc,
                )
            return
        # structural container: extend the breadcrumb (skip the root CHAPTER == the law itself)
        cls = _CLS.get(doc_type)
        child_anc = anc + ((cls, node.get("docLevelId") or ""),) if cls else anc
        for child in items:
            yield from _walk(child, child_anc)

    yield from _walk(root, ())
