"""Thin HTML client for the revisor.mo.gov Revised Statutes of Missouri site.

Three server-rendered pages, all under ``/main/``:

    GET /main/Home.aspx                       -> title groups + chapter links
    GET /main/OneChapter.aspx?chapter={N}      -> a chapter's section rows
    GET /main/OneSection.aspx?section={S}      -> one section's body + history

Section text is present in the raw HTML (no JavaScript rendering needed), so a
plain GET + BeautifulSoup is sufficient. revisor.mo.gov geo-blocks the scraper
box, so the run must egress through the US residential proxy
(``VAQUILL_USE_PROXY=1``); we reuse the proven proxy and UA-rotation config from
``vaquill_pipeline.http_client``. Like the VA client, this does NOT upload the
fetched HTML to R2 (the cleaned section text is uploaded to R2 by
``node_to_chunks`` instead, keyed by act_id).
"""

from __future__ import annotations

import os
import time

import requests
from vaquill_pipeline import http_client

BASE = "https://revisor.mo.gov/main"
REFERER = "https://revisor.mo.gov/main/Home.aspx"

# One shared keep-alive session (pool sized to the ingest worker count).
_SESSION: requests.Session | None = None


def _session() -> requests.Session:
    global _SESSION
    if _SESSION is None:
        s = requests.Session()
        pool = max(int(os.environ.get("VAQUILL_HTTP_POOL", "40")), 40)
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


def get_html(path: str, *, retries: int = 6, timeout: float = 45.0) -> str:
    """GET a page under /main/ and return its HTML.

    ``path`` is the page + query relative to BASE, e.g.
    ``OneChapter.aspx?chapter=565`` or ``OneSection.aspx?section=565.020``.
    Retries on empty body / non-200 / network error with backoff, because the
    residential proxy occasionally drops a connection.
    """
    url = f"{BASE}/{path}"
    use_proxy = os.environ.get("VAQUILL_USE_PROXY") == "1"

    last = "no attempt"
    for attempt in range(1, retries + 1):
        try:
            profile = http_client._random_profile()
            headers = {
                "User-Agent": profile["ua"],
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": REFERER,
                "Connection": "keep-alive",
            }
            proxies = _proxies() if use_proxy else None
            resp = _session().get(
                url, headers=headers, proxies=proxies, timeout=timeout, allow_redirects=True
            )
            if resp.status_code == 200 and resp.text.strip():
                return resp.text
            last = f"HTTP {resp.status_code}" if resp.status_code != 200 else "empty body"
        except Exception as exc:
            last = f"{type(exc).__name__}: {str(exc)[:140]}"
        time.sleep(min(1.5 * attempt, 8.0))
    raise RuntimeError(f"get_html failed for {url}: {last}")


# ---------------------------------------------------------------------------
# Operations
# ---------------------------------------------------------------------------


def home() -> str:
    return get_html("Home.aspx")


def chapter(chapter_number: str) -> str:
    return get_html(f"OneChapter.aspx?chapter={chapter_number}")


def section(section_number: str) -> str:
    return get_html(f"OneSection.aspx?section={section_number}")
