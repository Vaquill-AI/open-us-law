"""Exa Contents API client for reading Justia's Arkansas Code.

Justia (law.justia.com/codes/arkansas/) publishes the full, current Arkansas
Code of 1987 by Title -> [Subtitle ->] Chapter -> [Subchapter ->] Section, but
its pages are behind Cloudflare and 403 both the scraper box IP and the Webshare
US residential proxy. Exa's ``/contents`` endpoint renders the page for us
(defeating Cloudflare) and returns, in a single call:

  * ``text``  -- the page content (used for section body text), and
  * ``extras.links`` -- every link on the page (used to WALK the tree: a TOC
    page lists its child subtitle/chapter/subchapter/section URLs).

Pricing is $1 per 1,000 pages ($0.001/page, confirmed via ``costDollars``). A
full AR crawl (~25-30k sections plus a few thousand TOC pages) is on the order
of $30. ``maxAgeHours`` is omitted by default so Exa serves cached content when
it has it and livecrawls only when it does not -- cheap and fresh enough for an
annually-revised code; a weekly refresh mostly hits cache.

No Webshare proxy is used here (Exa is a separate API), so this ingest does not
contend for the shared ~20-worker proxy ceiling that the per-section HTML states
share during the weekly refresh.
"""

from __future__ import annotations

import json
import os
import time
import urllib.parse

import requests

EXA_CONTENTS_URL = "https://api.exa.ai/contents"

# Wayback fallback. Exa renders the vast majority of Justia sections, but a few
# exceptionally long pages (e.g. Ark. Code Ann. § 5-10-101, capital murder) get a
# Cloudflare challenge that Exa returns as a body-less shell / empty livecrawl.
# The Internet Archive is not geo-blocked and holds raw Justia captures, so we
# backfill those sections from the newest available Wayback snapshot of the same
# Justia URL. See parse.extract_body_html for the HTML-side body parse.
WAYBACK_CDX = "https://web.archive.org/cdx/search/cdx"
WAYBACK_RAW = "https://web.archive.org/web/{ts}id_/{url}"


class ExaError(RuntimeError):
    pass


def _api_key() -> str:
    key = os.environ.get("EXA_API_KEY")
    if not key:
        raise ExaError("EXA_API_KEY is not set")
    return key


def node(
    url: str,
    *,
    links: int = 400,
    max_chars: int = 20000,
    max_age_hours: int | None = None,
    livecrawl_timeout_ms: int = 25000,
    retries: int = 5,
    timeout: float = 90.0,
) -> tuple[str, list[str]]:
    """Fetch one Justia page via Exa. Return (text, child_link_urls).

    Retries with backoff on network error / non-200 / Exa error status. Raises
    ExaError after ``retries`` exhausted so the caller can count it as a failure
    (a section that never resolves is dropped, not silently blanked).
    """
    body: dict = {
        "urls": [url],
        "text": {"maxCharacters": max_chars},
        "extras": {"links": links},
        "livecrawlTimeout": livecrawl_timeout_ms,
    }
    if max_age_hours is not None:
        body["maxAgeHours"] = max_age_hours
    headers = {"x-api-key": _api_key(), "Content-Type": "application/json"}

    last = "no attempt"
    for attempt in range(1, retries + 1):
        try:
            resp = requests.post(EXA_CONTENTS_URL, headers=headers, json=body, timeout=timeout)
            if resp.status_code == 200:
                payload = resp.json()
                results = payload.get("results") or []
                if results:
                    res = results[0]
                    text = res.get("text") or ""
                    raw_links = (res.get("extras") or {}).get("links") or res.get("links") or []
                    link_urls = [str(x) for x in raw_links if isinstance(x, str)]
                    return text, link_urls
                # 200 but no result. In cache-only mode (max_age_hours == -1) this
                # is a DEFINITIVE miss -- Exa has nothing cached and will not
                # livecrawl -- so retrying just wastes ~4s of backoff per page.
                # Return an empty (unrendered) result immediately.
                if max_age_hours == -1:
                    return "", []
                statuses = payload.get("statuses") or []
                last = f"no result ({statuses[:1]})"
            elif resp.status_code == 429:
                last = "HTTP 429 (rate limit)"
                time.sleep(min(2.0 * attempt, 12.0))
                continue
            else:
                last = f"HTTP {resp.status_code}: {resp.text[:160]}"
        except Exception as exc:
            last = f"{type(exc).__name__}: {str(exc)[:160]}"
        time.sleep(min(1.5 * attempt, 8.0))
    raise ExaError(f"Exa contents failed for {url}: {last}")


def wayback_section_index(timeout: float = 120.0, retries: int = 8) -> list[tuple[str, str]]:
    """Every captured Justia AR section URL with its newest snapshot timestamp.

    Used to ENUMERATE the section universe when Justia's own TOC cannot be walked
    (Cloudflare blocks the live crawl), and to backfill section text directly
    from the snapshot without a per-URL CDX round-trip. Returns (original_url,
    timestamp) pairs; the caller normalizes with walk.to_current_url and dedupes
    by section number. ``collapse=urlkey`` keeps the most recent capture per URL.
    """
    q = (
        f"{WAYBACK_CDX}?url=law.justia.com/codes/arkansas*"
        "&output=json&fl=original,timestamp&collapse=urlkey"
        "&filter=statuscode:200&filter=mimetype:text/html&limit=400000"
    )
    last = "no attempt"
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(q, headers={"User-Agent": "Mozilla/5.0"}, timeout=timeout)
            if resp.status_code == 200 and resp.text.strip():
                rows = json.loads(resp.text)
                out = []
                for r in rows[1:]:
                    if isinstance(r, list) and len(r) >= 2 and "section-" in r[0]:
                        out.append((r[0], r[1]))
                return out
            last = f"HTTP {resp.status_code}"
        except Exception as exc:
            last = f"{type(exc).__name__}: {str(exc)[:120]}"
        time.sleep(min(3.0 * attempt, 15.0))
    raise ExaError(f"Wayback CDX index failed: {last}")


def wayback_raw_fetch(
    original: str, ts: str, *, timeout: float = 40.0, retries: int = 3
) -> str | None:
    """Raw (id_) HTML of a known Wayback snapshot, or None. No CDX round-trip."""
    raw_url = WAYBACK_RAW.format(ts=ts, url=original)
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(raw_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=timeout)
            if r.status_code == 200 and r.text.strip():
                return r.text
        except Exception:
            pass
        time.sleep(min(1.5 * attempt, 6.0))
    return None


SCRAPFLY_URL = "https://api.scrapfly.io/scrape"


def _scrapfly_key() -> str:
    key = os.environ.get("SCRAPFLY_API_KEY") or os.environ.get("SCRAPER_SERVICE_API_KEY")
    if not key:
        raise ExaError("SCRAPFLY_API_KEY is not set")
    return key


_SCRAPFLY_COST_SPENT = 0  # process-wide running total of X-Scrapfly-Api-Cost


def scrapfly_cost_spent() -> int:
    return _SCRAPFLY_COST_SPENT


def scrapfly_html(
    url: str,
    *,
    render_js: bool = False,
    retries: int = 4,
    timeout: float = 150.0,
    cache: bool = True,
    cache_ttl: int = 604800,
    cost_budget: int = 45,
) -> str | None:
    """Fetch a Justia page's real HTML through ScrapFly's anti-bot bypass.

    ``asp=true`` defeats Cloudflare (measured: 1 credit/page on Justia AR with the
    datacenter proxy, no browser). ``render_js`` stays off by default (Justia is
    server-rendered once past Cloudflare) and is escalated on retry if a page does
    not render. Returns the HTML string, or None after retries. Only counts a
    result rendered when the section/TOC page actually loaded (``Disclaimer:``
    footer present), so a Cloudflare interstitial is retried rather than parsed.
    """
    global _SCRAPFLY_COST_SPENT
    key = _scrapfly_key()
    js = render_js
    attempt = 0
    throttles = 0
    while attempt < retries:
        # Cost controls: datacenter proxy (1 credit) is the default; asp upgrades
        # to residential (25) only when Cloudflare blocks it. render_js stays off
        # (+5 avoided). cache=true makes any re-fetch within TTL cost 0 credits.
        # cost_budget caps a single page so a hard shield can't spike to 80+.
        params = {
            "key": key,
            "url": url,
            "asp": "true",
            "country": "us",
            "render_js": "true" if js else "false",
            "cost_budget": str(cost_budget),
        }
        if cache:
            params["cache"] = "true"
            params["cache_ttl"] = str(cache_ttl)
        try:
            resp = requests.get(SCRAPFLY_URL, params=params, timeout=timeout)
            try:
                _SCRAPFLY_COST_SPENT += int(resp.headers.get("X-Scrapfly-Api-Cost", "0"))
            except (TypeError, ValueError):
                pass
            if resp.status_code == 429:
                # Account-level concurrency/throttle: honor Retry-After and retry
                # WITHOUT consuming a render attempt (it is not a page failure).
                wait = 60
                try:
                    wait = int(resp.headers.get("Retry-After", "60"))
                except (TypeError, ValueError):
                    wait = 60
                throttles += 1
                if throttles > 8:
                    return None
                time.sleep(min(wait + 1, 90))
                continue
            if resp.status_code == 200:
                res = resp.json().get("result") or {}
                content = res.get("content") or ""
                inner = res.get("status_code")
                if inner == 404:
                    return None  # dead Justia URL: do not retry
                if content and is_rendered(content):
                    return content
            # Not rendered. Justia renders at the datacenter tier (1 credit), so a
            # non-rendered response is almost always a transient Cloudflare blip:
            # retry at the SAME cheap tier rather than escalating to a browser
            # render (~5 credits). Only the final attempt escalates to JS, and
            # only if the caller opted into it, to bound worst-case credit cost.
            if render_js and attempt == retries - 1:
                js = True
        except Exception:
            pass
        attempt += 1
        time.sleep(min(1.5 * attempt, 8.0))
    return None


def is_rendered(text: str) -> bool:
    """True when Exa returned a fully-rendered Justia code page.

    Every real section/TOC page ends with the "Disclaimer:" footer; a body-less
    Cloudflare-challenge shell does not. This is the signal used to decide when a
    fresh livecrawl retry (or a Wayback fallback) is needed.
    """
    return bool(text) and "Disclaimer" in text


def wayback_html(justia_url: str, *, timeout: float = 40.0, retries: int = 3) -> str | None:
    """Newest raw Wayback snapshot HTML of a Justia URL, or None.

    Tries the current-edition URL first, then edition-year variants, so a section
    Exa cannot render is backfilled from whatever the Internet Archive captured.
    """
    candidates = [justia_url]
    # e.g. .../codes/arkansas/title-5/... -> .../codes/arkansas/2023/title-5/...
    for year in ("2024", "2023", "2020", "2019", "2018"):
        candidates.append(justia_url.replace("/codes/arkansas/", f"/codes/arkansas/{year}/", 1))

    for cand in candidates:
        try:
            q = (
                f"{WAYBACK_CDX}?url={urllib.parse.quote(cand, safe='')}"
                "&output=json&fl=timestamp,original&filter=statuscode:200"
                "&filter=mimetype:text/html&collapse=digest&limit=-1"
            )
            resp = requests.get(q, headers={"User-Agent": "Mozilla/5.0"}, timeout=timeout)
            if resp.status_code != 200 or not resp.text.strip():
                continue
            rows = json.loads(resp.text)
            if len(rows) < 2:
                continue
            ts, original = rows[-1][0], rows[-1][1]
        except Exception:
            continue
        raw_url = WAYBACK_RAW.format(ts=ts, url=original)
        for attempt in range(1, retries + 1):
            try:
                r = requests.get(raw_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=timeout)
                if r.status_code == 200 and r.text.strip():
                    return r.text
            except Exception:
                pass
            time.sleep(min(1.5 * attempt, 6.0))
    return None
