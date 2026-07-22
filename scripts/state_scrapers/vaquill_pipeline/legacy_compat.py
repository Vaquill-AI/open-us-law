"""Pre-Pydantic compatibility for ~12 upstream scrapers.

A handful of state scrapers (ME, MD, MN, MT, NE, NH, NY, OR, SC, VT, WI, WV)
were written against the *old* upstream API where ``insert_node`` accepted a
21-position tuple instead of a Pydantic ``Node``. They also reference
``insert_node`` / ``insert_node_ignore_duplicate`` as bare globals — no
import. They never worked after the upstream Pydantic migration.

This shim:
    1. Defines ``insert_node`` / ``insert_node_ignore_duplicate`` /
       ``insert_node_allow_duplicate`` that accept either a Node or the
       legacy 21-tuple, and route both through the monkey-patched
       ``utilityFunctions.pydantic_insert``.
    2. Provides ``inject_into(module)`` that pokes those functions into
       the scraper module's globals, so the bare call sites resolve.

Legacy 21-tuple shape (inferred from grep):
    ( node_id, top_level_title, node_type, level_classifier, node_text,
      _, citation, link, addendum, node_name, _, _, _, _, _,
      parent, _, _, _, _, _ )

Slots we keep:        0, 1, 2, 3, 4,    6, 7, 8, 9,            15.
Everything else is best-effort metadata the old Postgres schema carried.
"""
from __future__ import annotations

from typing import Any, Sequence, Union

from src.utils import utilityFunctions as util
from src.utils.pydanticModels import Node, NodeText


def _ensure_node_text(value: Any) -> Any:
    """Coerce a legacy ``node_text`` value into ``NodeText``.

    Old scrapers pass either ``None`` (structure node), a single string, or a
    list of paragraph strings.
    """
    if value is None or value == "" or value == []:
        return None
    if isinstance(value, NodeText):
        return value
    nt = NodeText()
    if isinstance(value, str):
        nt.add_paragraph(text=value)
        return nt
    if isinstance(value, (list, tuple)):
        for s in value:
            if s:
                nt.add_paragraph(text=str(s))
        return nt
    nt.add_paragraph(text=str(value))
    return nt


def _tuple_to_node(t: Sequence[Any]) -> Node:
    # Pad to 21 to be defensive against shorter tuples.
    padded = list(t) + [None] * (21 - len(t))
    node_id, top_level_title, node_type, level_classifier, node_text, _slot5, \
        citation, link, addendum, node_name, *_rest = padded[:10]
    parent = padded[15] if len(padded) > 15 else None

    if not node_type:
        node_type = "content" if node_text else "structure"
    if not level_classifier:
        level_classifier = "section" if node_type == "content" else "structure"

    kwargs = {
        "id": node_id,
        "node_type": node_type,
        "level_classifier": level_classifier,
    }
    if top_level_title is not None:
        kwargs["top_level_title"] = str(top_level_title)
    if citation:
        kwargs["citation"] = citation
    if link:
        kwargs["link"] = link
    if node_name:
        kwargs["node_name"] = node_name
    if parent:
        kwargs["parent"] = parent
    nt = _ensure_node_text(node_text)
    if nt is not None:
        kwargs["node_text"] = nt
    return Node(**kwargs)


def _coerce_to_node(data: Union[Node, Sequence[Any]]) -> Node:
    if isinstance(data, Node):
        return data
    if isinstance(data, (list, tuple)):
        return _tuple_to_node(data)
    raise TypeError(f"legacy_compat: cannot coerce {type(data).__name__} to Node")


def insert_node(data: Union[Node, Sequence[Any]], *_a, **_kw) -> Node:
    """Legacy-shaped ``insert_node`` that accepts a tuple or a Node."""
    node = _coerce_to_node(data)
    util.pydantic_insert("legacy_compat", [node])
    return node


def insert_node_ignore_duplicate(data: Union[Node, Sequence[Any]], *_a, **_kw) -> Node:
    return insert_node(data)


def insert_node_allow_duplicate(data: Union[Node, Sequence[Any]], *_a, **_kw) -> Node:
    return insert_node(data)


def insert_node_skip_duplicate(data: Union[Node, Sequence[Any]], *_a, **_kw) -> Node:
    return insert_node(data)


def inject_into(module) -> None:
    """Poke the compat helpers into a scraper module's globals."""
    module.insert_node = insert_node
    module.insert_node_ignore_duplicate = insert_node_ignore_duplicate
    module.insert_node_allow_duplicate = insert_node_allow_duplicate
    module.insert_node_skip_duplicate = insert_node_skip_duplicate
