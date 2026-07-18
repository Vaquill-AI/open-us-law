"""JSONL sink for the open state-statute scrapers.

The scrapers build ``Node`` pydantic models and call ``insert_node(...)`` (in
``scrapingHelpers``), which routes here to ``pydantic_insert``. In this open
edition, ``pydantic_insert`` appends each node as one JSON line to
``$OUT_DIR/<table_name>.jsonl`` (default ``./data``). No database, no cloud
storage, and no credentials are required - you run a scraper, you get a JSONL
file of normalized statutory nodes.

    OUT_DIR=./data python -m src.scrapers.us.states.ut.statutes.scrapeUT
    # -> ./data/us_ut_statutes.jsonl  (one JSON object per statutory node)
"""
from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from typing import Any, Dict, List

_OUT_DIR = Path(os.environ.get("OUT_DIR", "./data"))
_LOCK = threading.Lock()  # scrapers may insert from worker threads


def _to_record(model: Any) -> Dict[str, Any]:
    """Serialize a pydantic Node (v2 or v1) — or a plain object — to a dict."""
    if hasattr(model, "model_dump"):  # pydantic v2
        return model.model_dump(mode="json")
    if hasattr(model, "dict"):  # pydantic v1
        return model.dict()
    return dict(model)


def _append_jsonl(table_name: str, records: List[Dict[str, Any]]) -> None:
    _OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = _OUT_DIR / f"{table_name}.jsonl"
    with _LOCK, out_path.open("a", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")


def pydantic_insert(table_name: str, models: List[Any]) -> None:
    """Append each Node model to ``$OUT_DIR/<table_name>.jsonl``."""
    _append_jsonl(table_name, [_to_record(m) for m in models])


def regular_insert(table_name: str, dicts: List[Dict[str, Any]]) -> None:
    """Append plain dicts to the same JSONL sink."""
    _append_jsonl(table_name, list(dicts))


def db_connect(*_args, **_kwargs):  # pragma: no cover
    raise RuntimeError("This open edition writes JSONL to $OUT_DIR, not a database.")


def create_embedding(_text: str):  # pragma: no cover
    raise RuntimeError("Embedding is out of scope for the open scrapers.")


def create_chat_completion(*_a, **_kw):  # pragma: no cover
    raise RuntimeError("LLM calls are not used by the scrapers.")
