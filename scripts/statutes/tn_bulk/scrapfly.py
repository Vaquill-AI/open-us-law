"""ScrapFly fetch for Justia with per-worker sticky sessions.

This is the TN-local ScrapFly client. It reuses the approach proven in
``ar_bulk.client.scrapfly_html`` (``asp=true`` datacenter bypass of Cloudflare,
1 credit/page, ``is_rendered`` = "Disclaimer" footer present, honor 429
Retry-After, running credit total), and adds two things that matter at 46k-page
scale:

  * ``session`` -- a STICKY ScrapFly session per worker thread. Cloudflare's
    anti-bot clearance (the expensive part of ``asp``) is solved once per session
    and reused for every subsequent page on that session, instead of re-solved
    per page. With 5 worker threads that is 5 long-lived lanes. This is the
    2-4x throughput lever (and it lowers credits on any page that would have
    needed a re-solve).
  * ``cost_budget`` -- a hard per-page credit cap (default 2) so ScrapFly can
    NEVER escalate to the 25/40/80-credit residential+browser tiers. Justia
    renders at the 1-credit datacenter tier, so this only forbids runaway spend.

``cache=true`` (7-day TTL) makes any re-fetch (resume, overlap with a prior run)
cost 0 credits. Sessions are assigned per thread from a small counter, so a
``ThreadPoolExecutor(max_workers=5)`` naturally uses 5 stable session lanes.
"""

from __future__ import annotations

import itertools
import os
import threading
import time

import requests

_SCRAPFLY_URL = "https://api.scrapfly.io/scrape"

_cost_total = 0
_cost_lock = threading.Lock()
_tls = threading.local()
_counter = itertools.count()
_counter_lock = threading.Lock()


def _api_key() -> str:
    key = os.environ.get("SCRAPFLY_API_KEY") or os.environ.get("SCRAPER_SERVICE_API_KEY")
    if not key:
        raise RuntimeError("SCRAPFLY_API_KEY is not set")
    return key


def _session_name(prefix: str) -> str:
    """A stable session name for the calling worker thread (one lane per thread)."""
    sid = getattr(_tls, "sid", None)
    if sid is None:
        with _counter_lock:
            sid = next(_counter)
        _tls.sid = sid
    return f"{prefix}{sid}"


def cost_spent() -> int:
    return _cost_total


def is_rendered(html: str | None) -> bool:
    """True for a fully-rendered Justia code/TOC page (has the Disclaimer footer)."""
    return bool(html) and "Disclaimer" in html


def _add_cost(resp: requests.Response) -> None:
    global _cost_total
    try:
        c = int(resp.headers.get("X-Scrapfly-Api-Cost", "0"))
    except (TypeError, ValueError):
        c = 0
    if c:
        with _cost_lock:
            _cost_total += c


def fetch_html(
    url: str,
    *,
    cost_budget: int = 2,
    session: bool = True,
    session_prefix: str = "tn",
    require_render: bool = True,
    retries: int = 4,
    timeout: float = 150.0,
    cache: bool = True,
    cache_ttl: int = 604800,
) -> str | None:
    """Real Justia HTML via ScrapFly, or None. ``require_render`` gates on the
    Disclaimer footer (off for non-code pages like a sitemap).

    A 429 (account concurrency/throttle) honors Retry-After and does NOT consume
    a render attempt. A non-rendered 200 is retried at the SAME cheap tier (a
    transient Cloudflare blip), never escalated, so worst-case stays at
    ``cost_budget`` credits.
    """
    key = _api_key()
    params = {
        "key": key,
        "url": url,
        "asp": "true",
        "country": "us",
        "render_js": "false",
        "cost_budget": str(cost_budget),
    }
    # ScrapFly rejects session + cache together (CONFIG_ERROR). Session (solve
    # Cloudflare once per lane) is the throughput lever for a one-shot full crawl,
    # so it wins; cache (free re-fetch) is used only when session is off. Resume
    # safety without cache comes from the ingest skipping already-written sections.
    if session:
        params["session"] = _session_name(session_prefix)
        params["session_sticky_proxy"] = "true"
    elif cache:
        params["cache"] = "true"
        params["cache_ttl"] = str(cache_ttl)

    attempt = 0
    throttles = 0
    while attempt < retries:
        try:
            resp = requests.get(_SCRAPFLY_URL, params=params, timeout=timeout)
            _add_cost(resp)
            if resp.status_code == 429:
                throttles += 1
                if throttles > 8:
                    return None
                try:
                    wait = int(resp.headers.get("Retry-After", "30"))
                except (TypeError, ValueError):
                    wait = 30
                time.sleep(min(wait + 1, 90))
                continue
            if resp.status_code == 200:
                res = (resp.json().get("result") or {})
                if res.get("status_code") == 404:
                    return None  # dead Justia URL: do not retry
                content = res.get("content") or ""
                if content and (not require_render or is_rendered(content)):
                    return content
        except Exception:  # noqa: BLE001 - transient network/JSON, retry
            pass
        attempt += 1
        time.sleep(min(1.5 * attempt, 8.0))
    return None
