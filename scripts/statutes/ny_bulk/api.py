#!/usr/bin/env python3
"""Open Legislation (NY Senate) Laws API client.

Docs: https://legislation.nysenate.gov/static/docs/html/laws.html
Requires a free API key (email-validated signup at legislation.nysenate.gov),
passed as the `key` query param. Set it in the environment as OPENLEG_API_KEY
(falls back to NYSENATE_API_KEY).

Endpoints used:
    GET /api/3/laws?limit=1000        -> all law volumes (lawId, name, lawType)
    GET /api/3/laws/{lawId}?full=true -> full recursive document tree incl. text
"""
from __future__ import annotations

import json
import os
import time
import urllib.parse as up
import urllib.request as ur

BASE = "https://legislation.nysenate.gov/api/3"

# Prefer the scraper's proxy-aware HTTP client on the box (nysenate.gov may
# geo-restrict from non-US egress); fall back to urllib for local/dev use.
try:
    from vaquill_pipeline.http_client import fetch_html as _fetch_html
except Exception:  # noqa: BLE001
    _fetch_html = None


def _key() -> str:
    k = os.environ.get("OPENLEG_API_KEY") or os.environ.get("NYSENATE_API_KEY")
    if not k:
        raise RuntimeError(
            "Set OPENLEG_API_KEY (free key from legislation.nysenate.gov) to use the NY Laws API"
        )
    return k


def _get(path: str, params: dict | None = None, retries: int = 5) -> dict:
    q = dict(params or {})
    q["key"] = _key()
    url = f"{BASE}{path}?{up.urlencode(q)}"
    last = None
    for attempt in range(retries):
        try:
            if _fetch_html is not None:
                # Big law trees (?full=true) can be several MB and slow over the
                # US proxy; give them a generous read timeout.
                data = json.loads(_fetch_html(url, timeout=120))
            else:
                req = ur.Request(url, headers={"Accept": "application/json", "User-Agent": "vaquill-corpus/1.0"})
                with ur.urlopen(req, timeout=120) as resp:
                    data = json.loads(resp.read().decode("utf-8"))
            if not data.get("success", False):
                raise RuntimeError(f"API error for {path}: {data.get('message')} ({data.get('errorCode')})")
            return data
        except Exception as exc:  # noqa: BLE001 - retry any transient failure
            last = exc
            time.sleep(min(2 ** attempt, 30))
    raise RuntimeError(f"GET {path} failed after {retries} tries: {last}")


def list_laws() -> list[dict]:
    """All NYS law volumes: [{lawId, name, lawType, chapter}, ...]."""
    data = _get("/laws", {"limit": 1000})
    return data["result"]["items"]


def get_law_tree(law_id: str) -> dict:
    """Full recursive document tree for one law, including section text.

    Returns the `result` object: {lawVersion, info, documents{...recursive...}}.
    """
    data = _get(f"/laws/{law_id}", {"full": "true"})
    return data["result"]
