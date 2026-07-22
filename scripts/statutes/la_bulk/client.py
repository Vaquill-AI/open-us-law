"""HTTP client for the legis.la.gov Louisiana Laws site.

Two GET surfaces, both plain HTML (the site is ASP.NET WebForms, but the TOC
leaves and the law text render as ordinary anchors/blocks, so no __VIEWSTATE
postback round-trips are needed):

    GET /legis/Laws_Toc.aspx?folder=<id>   -> a body/title TOC page
    GET /legis/Law.aspx?d=<docid>          -> one section/article page

legis.la.gov does NOT geo-block the scraper box (verified: direct 200s from
Hetzner fsn1), so this client egresses directly by default and only routes
through the US residential proxy when ``VAQUILL_USE_PROXY=1``. Direct is
preferred for a ~52k-page run because the shared proxy is a contended
bottleneck; the proxy remains available as a fallback if the site starts
throttling a single IP. Unlike ``fetch_html``, this client does NOT upload to R2
(the aspx URLs are not source documents; section text is uploaded to R2 by
``node_to_chunks``).
"""

from __future__ import annotations

import os
import time

import requests
from vaquill_pipeline import http_client

SITE = "https://legis.la.gov"
BASE = f"{SITE}/legis"
REFERER = f"{BASE}/LawSearch.aspx"

_SESSION: requests.Session | None = None


def _session() -> requests.Session:
    global _SESSION
    if _SESSION is None:
        s = requests.Session()
        pool = max(int(os.environ.get("VAQUILL_HTTP_POOL", "48")), 48)
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=pool, pool_maxsize=pool, max_retries=0
        )
        s.mount("http://", adapter)
        s.mount("https://", adapter)
        _SESSION = s
    return _SESSION


def _proxies() -> dict | None:
    if os.environ.get("VAQUILL_USE_PROXY") != "1":
        return None
    return http_client._proxy_for("us")


def _get(url: str, *, retries: int = 6, timeout: float = 45.0) -> str:
    """GET a legis.la.gov page and return HTML, retrying with backoff.

    Retries on empty body / non-200 / network error. The residential proxy (when
    enabled) occasionally drops a connection, and the site itself can 500 under
    load, so a bounded backoff keeps a big parallel run from losing pages.
    """
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
    raise RuntimeError(f"GET failed for {url}: {last}")


def toc(folder: int) -> str:
    """The TOC HTML for a folder id (a body or a Revised-Statutes title)."""
    return _get(f"{BASE}/Laws_Toc.aspx?folder={folder}")


def law(docid: str) -> str:
    """The HTML for one section/article document."""
    return _get(f"{BASE}/Law.aspx?d={docid}")


def law_url(docid: str) -> str:
    """The human-facing page a citation link should open."""
    return f"{BASE}/Law.aspx?d={docid}"
