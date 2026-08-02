"""HTTP client for the leg.state.fl.us (Online Sunshine) Florida Statutes site.

Online Sunshine is the state's own ColdFusion statutes app. It publishes the SAME
2025 statute text as flsenate.gov with the SAME HTML classes (``div.Section`` /
``SectionNumber`` / ``CatchlineText`` / ``SectionBody`` / ``History`` / ``div.Part``
/ ``PartNumber``), so the parser is shared. We use Online Sunshine rather than
flsenate.gov because flsenate.gov blocks the scraper box (direct egress times out;
the Webshare US proxy exits are 502'd) while leg.state.fl.us serves the box
DIRECTLY (verified: HTTP 200 for Chapter 782/627/440/1002 from Hetzner fsn1). It
also blocks aggressively on burst, so keep concurrency modest.

URL surfaces:
    TOC:     index.cfm?Mode=View Statutes...                      -> 49 Title links
    Title:   index.cfm?App_mode=Display_Index&Title_Request=<rom> -> that title's
             chapter list (ChapterTOC)
    Chapter: index.cfm?App_mode=Display_Statute&URL=<band>/<pad>/<pad>.html
             -> the COMPLETE chapter (all sections + parts) in one fetch.

The chapter file path is fully derivable from the chapter number:
``band = floor(ch/100)*100`` as ``NNNN-NNNN`` and ``pad = ch`` zero-padded to 4
(verified across ch 1 / 440 / 627 / 782 / 1002). This client egresses directly by
default and only routes through the US residential proxy when
``VAQUILL_USE_PROXY=1``; it does NOT upload fetched HTML to R2 (the chapter page is
a whole-chapter document; per-section text is uploaded by ``node_to_chunks``).
"""

from __future__ import annotations

import os
import time

import requests
from vaquill_pipeline import http_client

SITE = "http://www.leg.state.fl.us/statutes"
INDEX = f"{SITE}/index.cfm"
_UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"

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


def band_for(chapter: str) -> str:
    """``782`` -> ``0700-0799`` (the Online Sunshine directory band)."""
    lo = (int(chapter) // 100) * 100
    return f"{lo:04d}-{lo + 99:04d}"


def padded(chapter: str) -> str:
    """``782`` -> ``0782`` (the Online Sunshine 4-digit chapter token)."""
    return f"{int(chapter):04d}"


def _get(url: str, *, retries: int = 6, timeout: float = 90.0, min_len: int = 400) -> str:
    """GET an Online Sunshine page, retrying with bounded backoff.

    Retries on non-200 / short body / network error. A large chapter page is
    multi-MB (Chapter 627 ~3 MB, Chapter 1002 ~1.2 MB), so the timeout is
    generous; ``min_len`` rejects a truncated/redirect body.
    """
    last = ""
    for attempt in range(1, retries + 1):
        try:
            resp = _session().get(
                url,
                headers={"User-Agent": _UA},
                proxies=_proxies(),
                timeout=timeout,
                allow_redirects=True,
            )
            if resp.status_code == 200 and len(resp.text) >= min_len:
                return resp.text
            last = f"HTTP {resp.status_code} len={len(resp.text)}"
        except Exception as exc:
            last = f"{type(exc).__name__}: {str(exc)[:120]}"
        if attempt < retries:
            time.sleep(min(2.0 * attempt, 15.0))
    raise RuntimeError(f"GET failed after {retries} tries ({last}): {url}")


def toc_html() -> str:
    return _get(f"{INDEX}?Mode=View%20Statutes&Submenu=1&Tab=statutes")


def title_index_html(roman: str) -> str:
    return _get(f"{INDEX}?App_mode=Display_Index&Title_Request={roman}")


def chapter_html(chapter: str) -> str:
    pad = padded(chapter)
    url = f"{INDEX}?App_mode=Display_Statute&URL={band_for(chapter)}/{pad}/{pad}.html"
    # Chapter pages are large; require a bigger min body to reject a stub/redirect.
    return _get(url, timeout=120.0, min_len=800)


def section_url(chapter: str, section_number: str) -> str:
    """Canonical human-facing link for a section within its chapter page."""
    pad = padded(chapter)
    return (
        f"{INDEX}?App_mode=Display_Statute&Search_String=&URL="
        f"{band_for(chapter)}/{pad}/Sections/{section_number}.html"
    )
