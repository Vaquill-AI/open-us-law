"""Thin JSON client for the law.lis.virginia.gov Code of Virginia web service.

The Commonwealth exposes a WCF/ASP.NET REST service at ``/api/``. The four
operations we use:

    GET /api/CoVTitlesGetListOfJson/                          -> [Title, ...]
    GET /api/CoVChaptersGetListOfJson/{title}/                -> {ChapterList: [...]}
    GET /api/CoVSectionsGetListOfJson/{title}/{chapter}/      -> {ArticleList: [...]}
    GET /api/CoVSectionsGetSectionDetailsJson/{section}/      -> {ChapterList:[{Body}]}

Two IIS quirks the client works around:
  - The service 301-redirects the no-trailing-slash form and then serves its
    HTML help page for the slashed-but-parameterless base, so we always append
    a trailing slash to the FULL path (operation + params) and follow redirects.
  - CORS is locked to the site's own origin, so we send a matching Referer.

The VA gov site geo-blocks the scraper box, so the run must egress through the
US residential proxy (``VAQUILL_USE_PROXY=1``); we reuse the proven proxy and
UA-rotation config from ``vaquill_pipeline.http_client``. Unlike ``fetch_html``,
this client does NOT upload responses to R2 (the JSON API URLs are not source
documents; section text is uploaded to R2 by ``node_to_chunks`` instead).
"""

from __future__ import annotations

import json
import os
import time
import urllib.parse as up
from typing import Any

import requests
from vaquill_pipeline import http_client

BASE = "https://law.lis.virginia.gov/api"
SITE = "https://law.lis.virginia.gov"
REFERER = "https://law.lis.virginia.gov/vacode/"

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


def get_json(op_path: str, *, retries: int = 6, timeout: float = 45.0) -> Any:
    """GET an operation path (e.g. 'CoVChaptersGetListOfJson/18.2') and parse JSON.

    Segments are URL-encoded (title/section numbers carry dots and dashes, which
    are unreserved, plus the occasional letter suffix like '8.1A'). A trailing
    slash is appended so IIS routes the parameter instead of 301-ing to the help
    page. Retries on empty body / non-200 / network error with backoff, because
    the residential proxy occasionally drops a connection.
    """
    segs = [s for s in op_path.split("/") if s != ""]
    encoded = "/".join(up.quote(s, safe="") for s in segs)
    url = f"{BASE}/{encoded}/"
    use_proxy = os.environ.get("VAQUILL_USE_PROXY") == "1"

    last = "no attempt"
    for attempt in range(1, retries + 1):
        try:
            profile = http_client._random_profile()
            headers = {
                "User-Agent": profile["ua"],
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": REFERER,
                "X-Requested-With": "XMLHttpRequest",
                "Connection": "keep-alive",
            }
            proxies = _proxies() if use_proxy else None
            resp = _session().get(
                url, headers=headers, proxies=proxies, timeout=timeout, allow_redirects=True
            )
            if resp.status_code == 200:
                body = resp.text.strip()
                if not body:
                    last = "empty body"
                elif body.lstrip().startswith("<"):
                    # Got the HTML help page instead of JSON (routing miss).
                    last = "html help page"
                else:
                    return json.loads(body)
            else:
                last = f"HTTP {resp.status_code}"
        except Exception as exc:
            last = f"{type(exc).__name__}: {str(exc)[:140]}"
        time.sleep(min(1.5 * attempt, 8.0))
    raise RuntimeError(f"get_json failed for {url}: {last}")


def get_html(site_path: str, *, retries: int = 5, timeout: float = 45.0) -> str:
    """GET an HTML page from the vacode site (e.g. '/vacode/title18.2/').

    The JSON chapters / section-list endpoints silently omit decimal chapters
    (e.g. 3.1) and mis-group some sections, so section enumeration is done from
    the server-rendered vacode HTML pages instead, which are complete. Section
    hierarchy + body still come from the JSON section-detail endpoint.
    """
    url = f"{SITE}{site_path}"
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


def titles() -> list[dict]:
    return get_json("CoVTitlesGetListOfJson")


def chapters(title_number: str) -> dict:
    return get_json(f"CoVChaptersGetListOfJson/{title_number}")


def sections(title_number: str, chapter_number: str) -> dict:
    return get_json(f"CoVSectionsGetListOfJson/{title_number}/{chapter_number}")


def section_detail(section_number: str) -> dict:
    return get_json(f"CoVSectionsGetSectionDetailsJson/{section_number}")
