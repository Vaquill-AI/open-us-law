"""Thin HTTP client for the official legis.iowa.gov Iowa Code surfaces.

Two surfaces, each used for what it does reliably:
  - Enumeration: the server-rendered chapter-listing HTML
    (``/law/iowaCode/chapters?title={roman}&year={year}``) maps each Title to its
    Chapter numbers (and flags RESERVED chapters). The Title is not present in the
    per-chapter XML, so this is where the Title -> Chapter mapping comes from.
  - Body: the per-chapter ``slim`` XML
    (``/docs/publications/ICC/{year}/attachments/{chapter}_slim.xml``) carries the
    chapter's section list, per-section headnote, nested body paragraphs, and
    amendment history for every section in one structured document.

Iowa gov geo-blocks the scraper box, so the run must egress through the US
residential proxy (``VAQUILL_USE_PROXY=1``); we reuse the proven proxy and
UA-rotation config from ``vaquill_pipeline.http_client``. Unlike ``fetch_bytes``,
this client does NOT upload responses to R2 (the chapter XML is an intermediate,
not a per-section source document; section text is uploaded to R2 by
``node_to_chunks`` instead, keyed by act_id).
"""

from __future__ import annotations

import os
import time

import requests
from vaquill_pipeline import http_client

SITE = "https://www.legis.iowa.gov"
REFERER = "https://www.legis.iowa.gov/law/iowaCode"

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


def _get(url: str, *, accept: str, retries: int, timeout: float) -> bytes:
    use_proxy = os.environ.get("VAQUILL_USE_PROXY") == "1"
    last = "no attempt"
    for attempt in range(1, retries + 1):
        try:
            profile = http_client._random_profile()
            headers = {
                "User-Agent": profile["ua"],
                "Accept": accept,
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": REFERER,
                "Connection": "keep-alive",
            }
            proxies = _proxies() if use_proxy else None
            resp = _session().get(
                url, headers=headers, proxies=proxies, timeout=timeout, allow_redirects=True
            )
            if resp.status_code == 200 and resp.content.strip():
                return resp.content
            last = f"HTTP {resp.status_code}" if resp.status_code != 200 else "empty body"
        except Exception as exc:
            last = f"{type(exc).__name__}: {str(exc)[:140]}"
        time.sleep(min(1.5 * attempt, 8.0))
    raise RuntimeError(f"GET failed for {url}: {last}")


def chapter_listing_html(
    title_roman: str, year: int, *, retries: int = 5, timeout: float = 45.0
) -> str:
    """HTML chapter-listing page for a Title (maps Title -> Chapter numbers)."""
    url = f"{SITE}/law/iowaCode/chapters?title={title_roman}&year={year}"
    return _get(
        url,
        accept="text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        retries=retries,
        timeout=timeout,
    ).decode("utf-8", "replace")


def title_listing_html(year: int, *, retries: int = 5, timeout: float = 45.0) -> str:
    """HTML Iowa Code root page (the list of 16 Titles)."""
    url = f"{SITE}/law/iowaCode?year={year}"
    return _get(
        url,
        accept="text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        retries=retries,
        timeout=timeout,
    ).decode("utf-8", "replace")


def chapter_xml(chapter: str, year: int, *, retries: int = 6, timeout: float = 60.0) -> bytes:
    """Per-chapter slim XML (section list + headnotes + body + history)."""
    url = f"{SITE}/docs/publications/ICC/{year}/attachments/{chapter}_slim.xml"
    return _get(url, accept="application/xml,text/xml,*/*;q=0.8", retries=retries, timeout=timeout)


def section_rtf_url(section_number: str, year: int) -> str:
    """Canonical per-section source link (what a citation should open).

    Matches the URL the scraper stored as each section's ``link``, so after the
    cutover every Iowa section carries a consistent per-section source_url.
    """
    return f"{SITE}/docs/code/{year}/{section_number}.rtf"
