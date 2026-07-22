"""Proxied client for the official Pennsylvania General Assembly consolidated
statutes surface at ``https://www.palegis.us/statutes/consolidated``.

Two operations we use:

    GET /statutes/consolidated                          -> index HTML (title list)
    GET /statutes/consolidated/view-statute?txtType=PDF&ttl=NN
                                                        -> the FULL per-title PDF

The per-title PDF is the primary bulk unit: a single request returns the entire
title's text (Title 18 is ~3 MB / 574 pages, Title 42 ~4.4 MB / 845 pages). This
is deliberately preferred over crawling the per-section HTML viewer: it is ONE
proxied request per title (~79 total) instead of tens of thousands of per-section
fetches, so it barely touches the shared ~20-worker proxy ceiling (which the
weekly bulk refresh and any concurrent state ingest contend for).

Geo-fence: www.palegis.us is unreachable from the scraper box directly (direct
connects time out) and only answers US exits, so every request MUST egress
through the Webshare US-rotate residential proxy. We reuse the proven
``vaquill_pipeline.http_client._proxy_for("us")`` (which builds the
``-US-rotate`` username form, NOT the bare global-rotate one) rather than
hand-rolling the proxy URL. A geo-fence can also answer HTTP 200 with a decoy
body, so the PDF fetch validates the ``%PDF`` magic bytes and retries on a
non-PDF response, each retry rotating the exit.

Unlike ``fetch_html``, this client does NOT upload responses to R2 (the PDF is
the bulk source, not a per-section document; cleaned section text is uploaded to
R2 by ``node_to_chunks`` instead).
"""

from __future__ import annotations

import os
import time
import urllib.parse as up

import requests
from vaquill_pipeline import http_client

BASE = "https://www.palegis.us/statutes/consolidated"
INDEX_URL = BASE
REFERER = "https://www.palegis.us/statutes"

_SESSION: requests.Session | None = None


def _session() -> requests.Session:
    global _SESSION
    if _SESSION is None:
        s = requests.Session()
        pool = max(int(os.environ.get("VAQUILL_HTTP_POOL", "24")), 24)
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=pool, pool_maxsize=pool, max_retries=0
        )
        s.mount("http://", adapter)
        s.mount("https://", adapter)
        _SESSION = s
    return _SESSION


def _proxies() -> dict | None:
    """US residential proxy when VAQUILL_USE_PROXY=1 (required on the box)."""
    if os.environ.get("VAQUILL_USE_PROXY") != "1":
        return None
    return http_client._proxy_for("us")


def _headers(accept: str) -> dict:
    profile = http_client._random_profile()
    return {
        "User-Agent": profile["ua"],
        "Accept": accept,
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": REFERER,
        "Connection": "keep-alive",
    }


def get_index(*, retries: int = 6, timeout: float = 45.0) -> str:
    """Fetch the consolidated-statutes index HTML (the title list)."""
    last = "no attempt"
    for attempt in range(1, retries + 1):
        try:
            resp = _session().get(
                INDEX_URL,
                headers=_headers("text/html,application/xhtml+xml,*/*;q=0.8"),
                proxies=_proxies(),
                timeout=timeout,
                allow_redirects=True,
            )
            if resp.status_code == 200 and resp.text.strip().startswith("<"):
                return resp.text
            last = f"HTTP {resp.status_code}"
        except Exception as exc:
            last = f"{type(exc).__name__}: {str(exc)[:120]}"
        time.sleep(min(1.5 * attempt, 8.0))
    raise RuntimeError(f"get_index failed for {INDEX_URL}: {last}")


def title_pdf(ttl: str, *, retries: int = 8, timeout: float = 120.0) -> bytes:
    """Fetch the full per-title consolidated PDF for ``ttl`` (e.g. '18' or '01').

    Validates the ``%PDF`` magic so a geo-fence decoy body (HTTP 200 + HTML
    shell) is rejected and retried on a fresh exit. Raises after ``retries``.
    """
    ttl2 = str(ttl).zfill(2)
    q = up.urlencode({"txtType": "PDF", "ttl": ttl2})
    url = f"{BASE}/view-statute?{q}"
    last = "no attempt"
    for attempt in range(1, retries + 1):
        try:
            resp = _session().get(
                url,
                headers=_headers("application/pdf,*/*"),
                proxies=_proxies(),
                timeout=timeout,
                allow_redirects=True,
            )
            if resp.status_code == 200 and resp.content[:4] == b"%PDF":
                return resp.content
            if resp.status_code == 200:
                last = f"decoy body (not %PDF): {resp.content[:16]!r}"
            else:
                last = f"HTTP {resp.status_code}"
        except Exception as exc:
            last = f"{type(exc).__name__}: {str(exc)[:120]}"
        time.sleep(min(2.0 * attempt, 12.0))
    raise RuntimeError(f"title_pdf failed for ttl={ttl2}: {last}")


def title_html_url(ttl: str) -> str:
    """Human-facing per-title HTML page (used as a section's source link)."""
    ttl2 = str(ttl).zfill(2)
    return f"{BASE}/view-statute?txtType=HTM&ttl={ttl2}"
