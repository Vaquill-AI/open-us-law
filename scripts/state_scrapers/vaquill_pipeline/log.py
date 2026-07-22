"""Structured logging for the Vaquill ingestion pipeline.

Every event is a single JSON line on stdout (Dokploy-friendly), plus an
``errors.jsonl`` file under ``data/state_chunks/`` so future selector breaks
can be triaged offline. Stdout uses structlog if available; fallback to
stdlib json so the pipeline runs even in minimal containers.

Usage:

    from vaquill_pipeline.log import get_logger, ErrorReport
    log = get_logger(state="de", run_id="20260511-de-01")
    log.info("title_listed", n_titles=31, url="...")
    log.error("selector_drift", url=u, selector=".title-links", retry=2)

    with ErrorReport.open("de") as report:
        report.record(stage="scrape", url=u, error=exc)
"""
from __future__ import annotations

import datetime as _dt
import json
import os
import sys
import threading
import traceback
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterator, Mapping, Optional

try:
    import structlog
    _HAS_STRUCTLOG = True
except ImportError:  # pragma: no cover
    structlog = None  # type: ignore[assignment]
    _HAS_STRUCTLOG = False


_LEVEL_NUM = {"debug": 10, "info": 20, "warning": 30, "error": 40, "critical": 50}
_MIN_LEVEL = _LEVEL_NUM.get(os.environ.get("VAQUILL_LOG_LEVEL", "info").lower(), 20)


def _utcnow_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _short_tb(exc: BaseException, n_lines: int = 6) -> str:
    tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    lines = tb.splitlines()
    return "\n".join(lines[-n_lines:])


# ---------------------------------------------------------------------------
# Plain-Python fallback logger (used when structlog isn't installed)
# ---------------------------------------------------------------------------


class _StdJsonLogger:
    def __init__(self, **context: Any) -> None:
        self._ctx: Dict[str, Any] = {k: v for k, v in context.items() if v is not None}

    def bind(self, **kw: Any) -> "_StdJsonLogger":
        new = _StdJsonLogger(**self._ctx)
        new._ctx.update({k: v for k, v in kw.items() if v is not None})
        return new

    def _emit(self, level: str, event: str, **fields: Any) -> None:
        if _LEVEL_NUM[level] < _MIN_LEVEL:
            return
        rec = {"ts": _utcnow_iso(), "level": level, "event": event}
        rec.update(self._ctx)
        for k, v in fields.items():
            if isinstance(v, BaseException):
                rec["exc_type"] = type(v).__name__
                rec["exc_msg"] = str(v)
                rec["exc_tail"] = _short_tb(v)
            else:
                rec[k] = v
        sys.stdout.write(json.dumps(rec, default=str, ensure_ascii=False) + "\n")
        sys.stdout.flush()

    def debug(self, event: str, **fields: Any) -> None: self._emit("debug", event, **fields)
    def info(self, event: str, **fields: Any) -> None: self._emit("info", event, **fields)
    def warning(self, event: str, **fields: Any) -> None: self._emit("warning", event, **fields)
    def error(self, event: str, **fields: Any) -> None: self._emit("error", event, **fields)
    def critical(self, event: str, **fields: Any) -> None: self._emit("critical", event, **fields)


# ---------------------------------------------------------------------------
# Public factory
# ---------------------------------------------------------------------------


_configured = False
_configure_lock = threading.Lock()


def _configure_structlog() -> None:
    global _configured
    with _configure_lock:
        if _configured or not _HAS_STRUCTLOG:
            return
        structlog.configure(
            processors=[
                structlog.contextvars.merge_contextvars,
                structlog.processors.add_log_level,
                structlog.processors.TimeStamper(fmt="iso", utc=True),
                structlog.processors.StackInfoRenderer(),
                structlog.processors.format_exc_info,
                structlog.processors.JSONRenderer(),
            ],
            wrapper_class=structlog.make_filtering_bound_logger(_MIN_LEVEL),
            cache_logger_on_first_use=True,
        )
        _configured = True


def get_logger(**context: Any):
    """Return a JSON logger with the given persistent context fields."""
    if _HAS_STRUCTLOG:
        _configure_structlog()
        return structlog.get_logger(**{k: v for k, v in context.items() if v is not None})
    return _StdJsonLogger(**context)


# ---------------------------------------------------------------------------
# Per-state error report writer (kept separate from the chunk JSONL).
# ---------------------------------------------------------------------------


class ErrorReport:
    """Append-only NDJSON of structured scrape failures, one file per state.

    Lives next to the chunk JSONL so ops can diff between runs and see which
    sections newly broke. Open with ``ErrorReport.open("de")`` as a context
    manager; ``record(...)`` is concurrency-safe within a single process.
    """

    def __init__(self, path: Path, state: str, run_id: str) -> None:
        self.path = path
        self.state = state
        self.run_id = run_id
        self._fh = None
        self._lock = threading.Lock()
        self.count = 0

    @classmethod
    @contextmanager
    def open(
        cls,
        state: str,
        base_dir: Optional[Path] = None,
        run_id: Optional[str] = None,
    ) -> Iterator["ErrorReport"]:
        base = base_dir or Path(
            os.environ.get(
                "VAQUILL_CHUNKS_DIR",
                str(Path(__file__).resolve().parent.parent / "data" / "state_chunks"),
            )
        )
        base.mkdir(parents=True, exist_ok=True)
        path = base / f"state_{state.lower()}_errors.jsonl"
        rid = run_id or _dt.datetime.now().strftime("%Y%m%dT%H%M%S")
        rep = cls(path, state, rid)
        rep._fh = open(path, "a", encoding="utf-8")
        try:
            yield rep
        finally:
            try:
                rep._fh.flush()
            finally:
                rep._fh.close()

    def record(
        self,
        stage: str,
        url: Optional[str] = None,
        selector: Optional[str] = None,
        node_id: Optional[str] = None,
        error: Optional[BaseException] = None,
        **extras: Any,
    ) -> None:
        rec: Dict[str, Any] = {
            "ts": _utcnow_iso(),
            "run_id": self.run_id,
            "state": self.state,
            "stage": stage,
        }
        if url is not None:
            rec["url"] = url
        if selector is not None:
            rec["selector"] = selector
        if node_id is not None:
            rec["node_id"] = node_id
        if error is not None:
            rec["exc_type"] = type(error).__name__
            rec["exc_msg"] = str(error)
            rec["exc_tail"] = _short_tb(error)
        rec.update(extras)
        line = json.dumps(rec, default=str, ensure_ascii=False) + "\n"
        with self._lock:
            self._fh.write(line)
            self._fh.flush()
            self.count += 1


def new_run_id(state: str) -> str:
    """Stable-ish run ID for log correlation. Date + state + short uuid."""
    return f"{_dt.datetime.now().strftime('%Y%m%dT%H%M%S')}-{state.lower()}-{uuid.uuid4().hex[:6]}"
