"""Monkeypatch the upstream Postgres sink so unchanged scrapers emit JSONL.

Wiring:
    install(state_code) returns a JsonlSink AND opens an ErrorReport. Both
    files live under data/state_chunks/ so a single ``ls`` shows the run's
    output (chunks + per-section failures) in one place.

    Every Node insertion is logged at debug level with seen / emitted /
    elapsed counters; every conversion failure becomes a structured row in
    state_<st>_errors.jsonl with the node_id and traceback tail.
"""
from __future__ import annotations

import datetime as _dt
import threading
import time
from pathlib import Path
from typing import Any, List, Optional

from .config import SETTINGS
from .jsonl_sink import JsonlSink
from .log import ErrorReport, get_logger, new_run_id
from .node_to_payload import node_to_chunks
from .selenium_shim import install_selenium_shim

_state_code: str = "us"
_sink: Optional[JsonlSink] = None
_report: Optional[ErrorReport] = None
_report_ctx = None  # type: ignore[var-annotated]
_seen = 0
_emitted = 0
_content_seen = 0
_max_content = 0  # 0 = unlimited
_run_id = ""
_t0 = 0.0
_log = get_logger()
_year = _dt.date.today().year

# Resume support: point_ids already in the JSONL when the run starts.
# If the patched pydantic_insert sees a chunk whose point_id is here, skip the
# JSONL write (the chunk text fetch already happened; we'd just write a dup).
# Content-addressed point_id formula means idempotent re-runs are safe.
_resume_skipset: set = set()

# Live-tail progress log: append-only, one human-readable line per content
# node. Path: data/state_chunks/state_<st>_progress.log.
# User can `tail -f` this during a long scrape.
_progress_fh = None

# Single lock protecting the global counters and the progress-log write so
# parallel scraper threads can call _pydantic_insert concurrently without
# racing on _seen / _emitted / progress lines. JsonlSink
# have their own internal locks.
_state_lock = threading.Lock()


class _StopAfterMax(Exception):
    """Raised by the patched pydantic_insert after _max_content content nodes."""
    pass


def install(state_code: str, output_path: Optional[Path] = None, max_content_nodes: int = 0) -> JsonlSink:
    """Install JSONL routing for the given state. Returns the live sink.

    ``max_content_nodes`` (0 = unlimited): stop the scraper after this many
    sections have been seen. Used by smoke runs to validate the path without
    a full multi-hour scrape.
    """
    global _state_code, _sink, _report, _report_ctx, _seen, _emitted, _content_seen, _max_content, _run_id, _t0, _log
    _state_code = state_code
    _seen = 0
    _emitted = 0
    _content_seen = 0
    _max_content = max_content_nodes
    _run_id = new_run_id(state_code)
    _t0 = time.time()
    _log = get_logger(state=state_code, run_id=_run_id)

    if output_path is None:
        output_path = SETTINGS.chunks_dir / f"state_{state_code.lower()}_statutes.jsonl"

    import src.utils.utilityFunctions as util

    _sink = JsonlSink(output_path)
    _report_ctx = ErrorReport.open(state_code, run_id=_run_id)
    _report = _report_ctx.__enter__()
    install_selenium_shim()

    # Build resume set from any existing JSONL chunks (deterministic point_id
    # means we can safely re-run; we just don't want to double-write).
    global _resume_skipset
    _resume_skipset = set()
    if output_path.exists():
        try:
            import json
            with open(output_path, encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        _resume_skipset.add(json.loads(line)["point_id"])
                    except Exception:
                        continue
        except Exception:
            _resume_skipset = set()

    # Live-tail progress log
    global _progress_fh
    progress_path = output_path.with_name(f"state_{state_code.lower()}_progress.log")
    _progress_fh = open(progress_path, "a", encoding="utf-8")
    _progress_fh.write(
        f"[{_dt.datetime.now().isoformat(timespec='seconds')}] "
        f"=== run start state={state_code} run_id={_run_id} resume_count={len(_resume_skipset)} ===\n"
    )
    _progress_fh.flush()

    _log.info(
        "install",
        chunks_path=str(output_path),
        errors_path=str(_report.path),
        progress_path=str(progress_path),
        resume_count=len(_resume_skipset),
    )

    def _pydantic_insert(table_name: str, models: List[Any]) -> None:
        global _seen, _emitted, _content_seen
        # Convert + chunk first (outside the lock — these are pure CPU ops).
        # JSONL write happens once per call inside JsonlSink's own lock.
        all_chunks: List[Dict[str, Any]] = []
        content_count_local = 0
        for node in models:
            is_content = getattr(node, "node_type", None) == "content"
            try:
                chunks = node_to_chunks(node, _state_code, _year)
            except Exception as e:  # noqa: BLE001
                node_id = getattr(node, "node_id", None) or "?"
                _log.error("node_to_chunks_failed", node_id=node_id, error=e)
                if _report is not None:
                    _report.record(stage="node_to_chunks", node_id=node_id, error=e)
                continue
            if _resume_skipset and chunks:
                chunks = [c for c in chunks if c["point_id"] not in _resume_skipset]
            if chunks:
                all_chunks.extend(chunks)
            if is_content:
                content_count_local += 1

        if all_chunks:
            _sink.write(all_chunks)

        # Update counters + progress log under the global state lock so
        # concurrent threads see consistent numbers.
        with _state_lock:
            _seen += len(models)
            _emitted += len(all_chunks)
            _content_seen += content_count_local
            if _progress_fh is not None and all_chunks:
                # one summary line per call (parallel batches collapse cleanly)
                elapsed = time.time() - _t0
                rate = _emitted / max(elapsed, 0.001)
                last_cite = all_chunks[-1]["metadata"].get("citation", "?")
                _progress_fh.write(
                    f"[{_dt.datetime.now().isoformat(timespec='seconds')}] "
                    f"emitted={_emitted:>6} content={_content_seen:>5} "
                    f"rate={rate:>5.1f}/s  {last_cite}\n"
                )
                _progress_fh.flush()
            stop_flag = bool(_max_content and _content_seen >= _max_content)
            if _seen % 500 == 0:
                rate = _seen / max(time.time() - _t0, 0.001)
                _log.info("progress", seen=_seen, emitted=_emitted, rate_nodes_per_sec=round(rate, 1))

        if stop_flag:
            raise _StopAfterMax(
                f"reached max_content_nodes={_max_content} for state={_state_code}"
            )

    def _regular_insert(table_name: str, dicts):  # noqa: ARG001
        return None

    def _db_connect(*_a, **_kw):  # pragma: no cover
        raise RuntimeError(
            "vaquill_pipeline.patch: Postgres backend is disabled. "
            "Chunks are routed to JSONL — run embed_and_upsert.py separately."
        )

    util.pydantic_insert = _pydantic_insert
    util.regular_insert = _regular_insert
    util.db_connect = _db_connect
    return _sink


def shutdown() -> None:
    global _sink, _report, _report_ctx, _progress_fh
    if _sink is not None:
        sink_path = _sink.path
        _sink.close()
        elapsed = time.time() - _t0
        _log.info(
            "shutdown",
            nodes_seen=_seen,
            chunks_emitted=_emitted,
            errors=(_report.count if _report else 0),
            elapsed_sec=round(elapsed, 1),
            rate_nodes_per_sec=round(_seen / max(elapsed, 0.001), 1),
            chunks_path=str(sink_path),
        )
        _sink = None
    if _progress_fh is not None:
        try:
            _progress_fh.write(
                f"[{_dt.datetime.now().isoformat(timespec='seconds')}] "
                f"=== run end emitted={_emitted} elapsed={time.time() - _t0:.1f}s ===\n"
            )
            _progress_fh.flush()
        finally:
            _progress_fh.close()
            _progress_fh = None
    if _report_ctx is not None:
        try:
            _report_ctx.__exit__(None, None, None)
        finally:
            _report_ctx = None
            _report = None
