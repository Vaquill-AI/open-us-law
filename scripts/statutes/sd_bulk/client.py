"""Thin HTTP client for the official sdlegislature.gov SDCL API.

South Dakota's public site is a Vue SPA, but every statute is also served through
a legacy SDLRC HTML surface that predates the SPA (no browser needed):

  - Title (all=true): ``/api/Statutes/{title}.html?all=true`` returns the ENTIRE
    title in one document, the per-chapter table of contents PLUS every section's
    heading, body, and Source history. One request per title (1-62) yields the
    whole Code, so we never crawl per-chapter.

The response is UTF-16 LE without a BOM (raw bytes start ``3C 00`` = ``<`` in
UTF-16); individual endpoints occasionally fall back to UTF-8, so we sniff.

The SDLRC surface is served out of the Azure US Gov cloud and geo-fences non-US
egress, so on the Hetzner box the run must egress through the US residential
proxy (``VAQUILL_USE_PROXY=1``); we reuse the proven proxy + UA-rotation config
from ``vaquill_pipeline.http_client``. Unlike ``fetch_bytes``, this client does
NOT upload responses to R2 (the title HTML is an intermediate, not a per-section
source document; section text is uploaded to R2 by ``node_to_chunks`` instead,
keyed by act_id).
"""

from __future__ import annotations

import os
import time

import requests
from vaquill_pipeline import http_client

SITE = "https://sdlegislature.gov"
BASE_API = f"{SITE}/api/Statutes"
REFERER = f"{SITE}/Statutes"

# SDCL titles run 1-62 numerically, PLUS a handful of alpha-suffixed titles that
# replaced a repealed numeric title or sit alongside it (23A Criminal Procedure,
# 27A/27B, 29A Uniform Probate Code, 33A, 34A Environmental, 51A Banking, 57A
# Uniform Commercial Code). The old numeric-only scraper (range(1,63)) missed
# every one of these, dropping ~8,400 sections incl. all of Criminal Procedure,
# the UCC, and the Probate Code. The endpoint 404s (or returns a body with no
# sections) for non-existent labels, so we enumerate the deterministic label
# space and keep whatever actually parses to sections. Suffixes A-C cover the
# real set (max observed is B); C is probed for future-proofing.
TITLE_RANGE = range(1, 63)
_TITLE_SUFFIXES = ("A", "B", "C")


def candidate_title_labels() -> list[str]:
    """Every title label to probe: numeric 1-62 plus {N}{A,B,C} alpha variants.

    Non-existent labels 404 / parse to zero sections and are dropped by the
    caller, so this over-generates deliberately rather than hard-coding the alpha
    set (which changes across sessions when a numeric title is recodified).
    """
    labels: list[str] = [str(n) for n in TITLE_RANGE]
    labels += [f"{n}{s}" for n in TITLE_RANGE for s in _TITLE_SUFFIXES]
    return labels


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


def _decode(raw: bytes) -> str:
    """Decode SDLRC bytes: UTF-16 LE (no BOM) when the 2nd byte is 0x00, else UTF-8."""
    if not raw:
        return ""
    if len(raw) >= 2 and raw[1] == 0x00:
        try:
            return raw.decode("utf-16-le")
        except UnicodeDecodeError:
            pass
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return raw.decode("latin-1", "replace")


def _get(url: str, *, retries: int, timeout: float) -> bytes | None:
    """GET raw bytes. Returns None on a genuine 404 (title does not exist)."""
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
            if resp.status_code == 404:
                return None
            if resp.status_code == 200 and resp.content.strip():
                return resp.content
            last = f"HTTP {resp.status_code}" if resp.status_code != 200 else "empty body"
        except Exception as exc:
            last = f"{type(exc).__name__}: {str(exc)[:140]}"
        time.sleep(min(1.5 * attempt, 8.0))
    raise RuntimeError(f"GET failed for {url}: {last}")


def title_html(title_label: str | int, *, retries: int = 6, timeout: float = 90.0) -> str | None:
    """Whole-title SDCL HTML (chapter TOC + every section body + history).

    ``title_label`` is a numeric title (``22``) or an alpha-suffixed one
    (``57A``). Returns the decoded HTML, or None when the title does not exist
    (404).
    """
    url = f"{BASE_API}/{title_label}.html?all=true"
    raw = _get(url, retries=retries, timeout=timeout)
    if raw is None:
        return None
    return _decode(raw)


def title_url(title_label: str | int) -> str:
    return f"{BASE_API}/{title_label}.html?all=true"


def chapter_url(title_number: str, chapter: str) -> str:
    """Canonical per-chapter source link (what a section citation should open)."""
    return f"{BASE_API}/{title_number}-{chapter}.html?all=true"
