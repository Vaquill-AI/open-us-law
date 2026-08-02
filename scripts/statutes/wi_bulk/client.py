"""Fetch client for docs.legis.wisconsin.gov statute pages.

Thin wrappers over ``vaquill_pipeline.http_client`` so proxy egress, UA
rotation, retries, and (when ``VAQUILL_R2_UPLOAD=1``) R2 source upload all match
the rest of the fleet. Wisconsin gov geo-blocks the scraper box, so the run must
set ``VAQUILL_USE_PROXY=1``; ``fetch_html`` falls back to the US residential
proxy on a geo-block automatically.

Endpoints:
    TOC:      /statutes/statutes/            -> chapter list
    Chapter:  /document/statutes/<N>         -> chapter TOC (windowed) + some bodies
    Section:  /document/statutes/<N.MM>      -> centered window of rendered sections
    PDF:      /document/statutes/<N>.pdf     -> complete chapter (completeness check)
"""

from __future__ import annotations

from vaquill_pipeline.http_client import fetch_bytes, fetch_html

BASE = "https://docs.legis.wisconsin.gov"
TOC_URL = f"{BASE}/statutes/statutes/"
REFERER = f"{BASE}/statutes/statutes/"


def toc_html(timeout: float = 60.0) -> str:
    return fetch_html(TOC_URL, country_code="us", use_proxy=True, timeout=timeout, referer=REFERER)


def chapter_html(chapter: str, timeout: float = 60.0) -> str:
    url = f"{BASE}/document/statutes/{chapter}"
    return fetch_html(url, country_code="us", use_proxy=True, timeout=timeout, referer=REFERER)


def section_html(section_number: str, timeout: float = 45.0) -> str:
    # max_retries=2 (direct then proxy) so a phantom section number seeded from a
    # PDF TOC that 404s on the viewer fails fast instead of burning 4 retries with
    # backoff (~15s each). The crawl retries genuine transient failures itself
    # (_MAX_SECTION_ATTEMPTS), and a "404" in the raised error is treated as a
    # permanent drop, so real transient errors still get several tries overall.
    url = f"{BASE}/document/statutes/{section_number}"
    return fetch_html(
        url, country_code="us", use_proxy=True, timeout=timeout, max_retries=2, referer=REFERER
    )


def section_url(section_number: str) -> str:
    """Canonical human-facing link for a section (what a citation should open)."""
    return f"{BASE}/document/statutes/{section_number}"


def chapter_pdf_bytes(chapter: str, timeout: float = 90.0) -> bytes:
    url = f"{BASE}/document/statutes/{chapter}.pdf"
    body, _ct = fetch_bytes(
        url, country_code="us", use_proxy=True, timeout=timeout, referer=REFERER
    )
    return body
