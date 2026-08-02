"""Thin XML client for the legislature.mi.gov MCL document tree.

Michigan publishes the whole Compiled Laws as one clean UTF-16 XML file per
chapter under ``/documents/mcl/`` (an IIS autoindex), each holding every act and
section of that chapter with body text inline. This is far lighter than the
per-section HTML crawl the old scraper used: the entire Code is ~241 chapter
files, one request each, no per-section fetch.

Two site quirks force a dedicated client rather than the shared
``vaquill_pipeline.http_client``:

  1. **Geo-block.** legislature.mi.gov TCP-times-out from non-US IPs (it is in
     http_client's hard-site list), so every request must egress through the US
     residential proxy (``VAQUILL_USE_PROXY=1``). We reuse
     ``http_client._proxy_for("us")`` so the proxy username is the ``-US-rotate``
     form that geo-locks exits to the US.
  2. **Incomplete TLS chain.** The server presents its leaf certificate without
     the intermediate CA, so standard verification fails with
     "unable to get local issuer certificate". This is a server-side
     misconfiguration on Michigan's end, not a MITM: the content is public
     statute text served over a residential CONNECT tunnel, and every section's
     text is content-hashed into its point_id downstream, so we disable
     certificate verification for this host only. urllib3's InsecureRequest
     warning is silenced to keep the ingest log readable.

This client does NOT upload responses to R2 (the cleaned section text is uploaded
to R2 by ``node_to_chunks``  in the chunk phase); keeping fetch
free of R2 writes is also what lets the ingest phase-separate crawl from chunk
and dodge a fetch/chunk thread-safety race.
"""

from __future__ import annotations

import os
import re
import time

import requests
import urllib3
from vaquill_pipeline import http_client

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

SITE = "https://www.legislature.mi.gov"
MCL_DIR = f"{SITE}/documents/mcl/"

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


def _get_bytes(url: str, *, retries: int = 5, timeout: float = 90.0) -> bytes:
    """GET raw bytes from the MCL site, retrying on empty/non-200/network error.

    Certificate verification is disabled (see module docstring: MI serves an
    incomplete cert chain). The residential proxy occasionally drops a
    connection, so we retry with backoff.
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
                "Connection": "keep-alive",
            }
            proxies = _proxies() if use_proxy else None
            resp = _session().get(
                url,
                headers=headers,
                proxies=proxies,
                timeout=timeout,
                allow_redirects=True,
                verify=False,
            )
            if resp.status_code == 200 and resp.content.strip():
                return resp.content
            last = f"HTTP {resp.status_code}" if resp.status_code != 200 else "empty body"
        except Exception as exc:
            last = f"{type(exc).__name__}: {str(exc)[:140]}"
        time.sleep(min(1.5 * attempt, 8.0))
    raise RuntimeError(f"fetch failed for {url}: {last}")


def _decode(data: bytes) -> str:
    """Decode an MCL document. Files are UTF-16 with a BOM; fall back to UTF-8."""
    if data[:2] in (b"\xff\xfe", b"\xfe\xff"):
        return data.decode("utf-16")
    try:
        return data.decode("utf-8-sig")
    except UnicodeDecodeError:
        return data.decode("utf-16", errors="replace")


# The autoindex lists chapter files as ``Chapter%20{name}.xml``.
_CHAPTER_HREF_RE = re.compile(r"Chapter%20([\w.]+)\.xml", re.IGNORECASE)


def list_chapters(*, retries: int = 5) -> list[str]:
    """Fetch the MCL directory autoindex and return the chapter names present.

    Deduped and sorted numerically where possible so the run is deterministic.
    """
    html = _decode(_get_bytes(MCL_DIR, retries=retries))
    names = {m.group(1) for m in _CHAPTER_HREF_RE.finditer(html)}

    def _key(n: str):
        try:
            return (0, float(n))
        except ValueError:
            return (1, n)

    return sorted(names, key=_key)


def get_chapter_xml(chapter_name: str, *, retries: int = 5) -> str:
    """Fetch and decode one chapter's MCL XML document."""
    url = f"{MCL_DIR}Chapter%20{chapter_name}.xml"
    return _decode(_get_bytes(url, retries=retries))
