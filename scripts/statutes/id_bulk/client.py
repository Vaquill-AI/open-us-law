"""Thin HTML client for the legislature.idaho.gov statutes site.

Idaho publishes no bulk file or JSON API, so section enumeration and section
bodies both come from the server-rendered HTML pages:

    /statutesrules/idstat/                      -> title table of contents
    /statutesrules/idstat/Title{N}/             -> chapter list for a title
    /statutesrules/idstat/Title{N}/T{N}CH{C}/   -> section list for a chapter
    /statutesrules/idstat/Title{N}/T{N}CH{C}/SECT{sec}/  -> one section body

The Idaho gov site geo-blocks the scraper box, so the run must egress through the
US residential proxy (``VAQUILL_USE_PROXY=1``); we reuse the proven proxy and
UA-rotation config from ``vaquill_pipeline.http_client``. This client does NOT
upload responses to R2 (the source HTML is uploaded to R2 by ``node_to_chunks``
 when a section's cleaned text is persisted).
"""

from __future__ import annotations

import os
import time

import requests
from vaquill_pipeline import http_client

SITE = "https://legislature.idaho.gov"
REFERER = "https://legislature.idaho.gov/statutesrules/idstat/"

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


def get_html(site_path_or_url: str, *, retries: int = 5, timeout: float = 45.0) -> str:
    """GET an HTML page from the idstat site.

    Accepts either a site-relative path ('/statutesrules/...') or an absolute URL
    on the idstat domain (as produced by the parser's row anchors). Retries on
    empty body / non-200 / network error with backoff, because the residential
    proxy occasionally drops a connection. A trailing slash is added so the
    WordPress permalink router serves the page instead of redirecting.
    """
    if site_path_or_url.startswith("http://") or site_path_or_url.startswith("https://"):
        url = site_path_or_url
    else:
        url = f"{SITE}{site_path_or_url}"
    if not url.endswith("/"):
        url += "/"
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
