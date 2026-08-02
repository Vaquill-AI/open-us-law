"""Append-only NDJSON writer for state chunk records. Thread-safe."""
from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any, Dict, Iterable, Optional


class JsonlSink:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = open(path, "a", encoding="utf-8")
        self._lock = threading.Lock()
        self.written = 0

    def write(self, records: Iterable[Dict[str, Any]]) -> None:
        # Serialize records OUTSIDE the lock so concurrent producers don't
        # block on each other's JSON encoding (cheap CPU work). Then take the
        # lock only for the file write so concurrent threads can't interleave.
        lines = [
            json.dumps(rec, ensure_ascii=False, default=str) + "\n"
            for rec in records
        ]
        if not lines:
            return
        blob = "".join(lines)
        with self._lock:
            self._fh.write(blob)
            self._fh.flush()
            self.written += len(lines)

    def close(self) -> None:
        with self._lock:
            try:
                self._fh.flush()
            finally:
                self._fh.close()
