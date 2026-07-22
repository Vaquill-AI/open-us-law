"""Python port of vaquill-all/news/scripts/job-scrapper/http-client.ts.

Adopted instead of reinvented:
    - DataImpulse residential proxy with country geo-targeting (sticky sessions
      via sid). Set DATAIMPULSE_USERNAME / DATAIMPULSE_PASSWORD.
    - ZenRows / ScraperAPI fallback for sites known to challenge bots
      (Cloudflare, Akamai). Set SCRAPER_SERVICE_API_KEY.
    - 4-profile User-Agent rotation matching the news repo (Chrome 130/131
      on Windows/macOS/Linux with Sec-Ch-Ua hints).
    - Cloudflare challenge detection + retry with a fresh session.
    - Custom retry/backoff (no separate tenacity layer needed).

This single ``fetch_html`` is used by ``scrapingHelpers.get_url_as_soup``
so every upstream state scraper transparently gets proxy + UA rotation +
Cloudflare bypass without code changes.
"""
from __future__ import annotations

import os
import random
import time
import urllib.parse
from pathlib import Path
from typing import Any, Dict, Optional

import requests

from .log import get_logger

_log = get_logger(component="http_client")


# Persistent HTTP session: keep-alive connection pool gives 3-4x speedup over
# fresh sockets per request, with negligible memory cost. One Session shared
# across the process is the canonical pattern for requests.
_SESSION: Optional[requests.Session] = None


def _get_session() -> requests.Session:
    global _SESSION
    if _SESSION is None:
        _SESSION = requests.Session()
        # Pool sizing is PER HOST, and a state scraper points all of its workers
        # at one host. If pool_maxsize < worker count, urllib3 discards the
        # surplus connections and re-opens a socket per request, silently losing
        # the keep-alive speedup this session exists for. Keep the pool at least
        # as large as the scraper's thread count.
        pool = max(
            int(os.environ.get("VAQUILL_HTTP_POOL", "20")),
            int(os.environ.get("VAQUILL_TITLE_WORKERS", "8")),
        )
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=pool,
            pool_maxsize=pool,
            max_retries=0,  # we do our own retries
        )
        _SESSION.mount("http://", adapter)
        _SESSION.mount("https://", adapter)
        # Trust store = certifi + intermediates that some state sites fail to send
        # (see ca_bundle). Set on the session so every scraper inherits it without
        # having to remember a per-call verify=.
        bundle = ca_bundle()
        if bundle:
            _SESSION.verify = bundle
    return _SESSION


# ---------------------------------------------------------------------------
# Browser profiles (port of news repo BROWSER_PROFILES)
# ---------------------------------------------------------------------------

_BROWSER_PROFILES = [
    {
        "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "sec_ch_ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        "platform": '"Windows"',
    },
    {
        "ua": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "sec_ch_ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        "platform": '"macOS"',
    },
    {
        "ua": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "sec_ch_ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        "platform": '"Linux"',
    },
    {
        "ua": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
        "sec_ch_ua": '"Chromium";v="130", "Google Chrome";v="130", "Not?A_Brand";v="99"',
        "platform": '"Windows"',
    },
]


def _random_profile() -> Dict[str, str]:
    return random.choice(_BROWSER_PROFILES)


# ---------------------------------------------------------------------------
# Residential proxy. Webshare is primary (creds verified, US rotating works);
# DataImpulse remains as fallback (its current creds return 407 from the gateway).
# ---------------------------------------------------------------------------

_CA_BUNDLE_CACHE: Optional[str] = None


def ca_bundle() -> str:
    """Path to a CA bundle = certifi plus intermediates some state sites omit.

    www.legislature.mi.gov serves its leaf certificate without the DigiCert
    intermediate that signed it, so strict verification fails with "unable to get
    local issuer certificate". Browsers paper over this by fetching the missing
    cert from the leaf's AIA extension; requests/urllib do not.

    Completing the chain keeps verification ON. The alternative people reach for -
    verify=False - would disable certificate checking for a source we ingest as
    primary law, which is not a trade worth making to save one file.

    Built once into a temp file and reused. Falls back to plain certifi if the
    vendored PEMs are missing, so a partial deploy degrades to today's behaviour
    rather than breaking every fetch.
    """
    global _CA_BUNDLE_CACHE
    if _CA_BUNDLE_CACHE is not None:
        return _CA_BUNDLE_CACHE

    import tempfile

    try:
        import certifi

        base = Path(certifi.where()).read_text(encoding="utf-8")
    except Exception:  # pragma: no cover - certifi is a hard dep of requests
        _CA_BUNDLE_CACHE = ""
        return _CA_BUNDLE_CACHE

    extra = []
    certs_dir = Path(__file__).resolve().parent / "certs"
    if certs_dir.is_dir():
        for pem in sorted(certs_dir.glob("*.pem")):
            try:
                extra.append(pem.read_text(encoding="utf-8"))
            except OSError:
                continue
    if not extra:
        _CA_BUNDLE_CACHE = certifi.where()
        return _CA_BUNDLE_CACHE

    fh = tempfile.NamedTemporaryFile("w", suffix="-vaquill-ca.pem", delete=False)
    fh.write(base)
    for pem in extra:
        fh.write("\n")
        fh.write(pem)
    fh.close()
    _CA_BUNDLE_CACHE = fh.name
    _log.info("ca_bundle_built", path=fh.name, extra_intermediates=len(extra))
    return _CA_BUNDLE_CACHE


_DC_LIST_CACHE: Optional[list] = None
_DC_STICKY: Optional[tuple] = None


def _webshare_datacenter_endpoints() -> list:
    """Load (and cache) the Webshare datacenter proxy list.

    One IP:port per line, ``ip:port:user:pass``. Downloaded from the Webshare
    dashboard; the path is gitignored because the lines carry credentials.
    """
    global _DC_LIST_CACHE
    if _DC_LIST_CACHE is not None:
        return _DC_LIST_CACHE

    entries: list = []
    rel = os.environ.get("WEBSHARE_DC_LIST", "data/proxies/webshare_datacenter.txt")
    path = Path(rel)
    if not path.is_absolute():
        # scripts/us_corpus/state_scrapers/vaquill_pipeline/ -> project root
        path = Path(__file__).resolve().parents[4] / rel
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            parts = line.strip().split(":")
            if len(parts) == 4:
                entries.append(tuple(parts))  # (ip, port, user, pass)
    except OSError:
        entries = []

    if not entries:
        # Single endpoint fallback when the list file is not deployed.
        ep = os.environ.get("WEBSHARE_DC_ENDPOINT", "")
        u = os.environ.get("WEBSHARE_DC_USERNAME", "")
        p = os.environ.get("WEBSHARE_DC_PASSWORD", "")
        if ep and u and p and ":" in ep:
            ip, _, port = ep.partition(":")
            entries = [(ip, port, u, p)]

    _DC_LIST_CACHE = entries
    return entries


def _webshare_datacenter_proxies(country_code: str) -> Optional[Dict[str, str]]:
    """Preferred proxy tier: Webshare DATACENTER (100 US IPs / 250 GB, ~$3/mo).

    Chosen over rotating residential because it is both cheaper and far larger:
    the rotating plan bills per GB and hit its cap in July 2026, after which every
    request returned "402 Bandwidth limit reached" and HTTPS CONNECT was refused
    outright, taking down all ~63 monthly refresh tasks at once.

    Measured the same day against the sources that reject the box directly: 8 of 9
    became reachable through a US datacenter exit (Chicago), 0 of 9 direct. These
    are country filters, not bot-walls, so a datacenter IP in the US satisfies them.
    The genuine bot-walls (Justia, NV, NH, LA, OK) are handled by the
    scraping-service path below, not by any proxy.

    Only honours country_code="us"; the plan is US-only.
    """
    if country_code.lower() != "us":
        return None
    entries = _webshare_datacenter_endpoints()
    if not entries:
        return None

    # STICKY per process. Picking a random endpoint per request looks like better
    # rotation but silently destroys connection reuse: requests pools per
    # (proxy, host), so a new proxy each call means a fresh TCP + TLS handshake
    # every time. Measured against ilga.gov: 0.71 req/s rotating vs 2.56 req/s
    # sticky, which turned a 10-hour Illinois crawl into a 76-hour one.
    #
    # One endpoint per PROCESS still spreads load across the pool, because each
    # scraper/worker picks its own, and the choice is seeded by pid so concurrent
    # processes do not collide on the same exit.
    global _DC_STICKY
    if _DC_STICKY is None:
        _DC_STICKY = entries[random.Random(os.getpid()).randrange(len(entries))]
    ip, port, user, pwd = _DC_STICKY
    url = f"http://{urllib.parse.quote(user)}:{urllib.parse.quote(pwd)}@{ip}:{port}"
    return {"http": url, "https": url}


def rotate_datacenter_endpoint() -> None:
    """Drop the sticky endpoint so the next call picks a different exit.

    For callers that hit a per-IP block or rate limit and want to move on without
    restarting the process.
    """
    global _DC_STICKY
    _DC_STICKY = None


def _webshare_proxies(country_code: str) -> Optional[Dict[str, str]]:
    user = os.environ.get("WEBSHARE_USERNAME")
    pwd = os.environ.get("WEBSHARE_PASSWORD")
    if not user or not pwd:
        return None
    # Webshare residential format: {user}-{COUNTRY}-rotate:{pass} for rotating
    # US exit. Country code must be uppercase. Verified 2026-05-11.
    proxy_user = f"{user}-{country_code.upper()}-rotate"
    host = os.environ.get("WEBSHARE_PROXY_HOST", "p.webshare.io")
    port = os.environ.get("WEBSHARE_PROXY_PORT", "80")
    proxy_url = f"http://{urllib.parse.quote(proxy_user)}:{urllib.parse.quote(pwd)}@{host}:{port}"
    return {"http": proxy_url, "https": proxy_url}


def _dataimpulse_proxies(country_code: str) -> Optional[Dict[str, str]]:
    user = os.environ.get("DATAIMPULSE_USERNAME")
    pwd = os.environ.get("DATAIMPULSE_PASSWORD")
    if not user or not pwd:
        return None
    sid = f"{random.randint(0, 1 << 30):08x}"
    proxy_user = f"{user}__cr.{country_code}__sid.{sid}"
    proxy_url = f"http://{urllib.parse.quote(proxy_user)}:{urllib.parse.quote(pwd)}@gw.dataimpulse.com:823"
    return {"http": proxy_url, "https": proxy_url}


def _proxy_for(country_code: str = "us", session_id: Optional[str] = None) -> Optional[Dict[str, str]]:
    """Return a requests-style proxies dict, cheapest workable tier first.

    1. Webshare DATACENTER - 100 US IPs / 250 GB for ~$3/mo. Clears 8 of the 9
       sources that refuse the box directly, because those are country filters.
    2. Webshare ROTATING RESIDENTIAL - billed per GB and ~9x the price for 1/25th
       the bandwidth, so it is reserved for sites that actually refuse datacenter
       IPs. Kept wired because the plan's quota resets monthly.
    3. DataImpulse - last resort.
    """
    return (
        _webshare_datacenter_proxies(country_code)
        or _webshare_proxies(country_code)
        or _dataimpulse_proxies(country_code)
    )


# ---------------------------------------------------------------------------
# Sites that need the scraping-service fallback (ZenRows / ScraperAPI)
# ---------------------------------------------------------------------------

_HARD_SITE_HINTS = (
    # Justia frequently 403s direct hits from non-US residential IPs.
    "law.justia.com",
    "justia.com",
    # IL legislative site sometimes returns Cloudflare challenge.
    "ilga.gov",
    # eCFR backed by Cloudflare in some regions.
    "ecfr.gov",
    # WV legislature code site blocks non-US IPs at the IIS level.
    "code.wvlegislature.gov",
    "wvlegislature.gov",
    # NH RSA is behind FortiWeb Cloud WAF (Azure); drops all non-browser HTTP
    # connections from non-US IPs. Needs ZenRows or a US exit proxy.
    "gencourt.state.nh.us",
    # NE Legislature TCP-times out from non-US IPs. Needs US residential proxy.
    "nebraskalegislature.gov",
    # VT legislature site TCP-times-out from non-US IPs (WAF geo-block).
    # Must be fetched through a US-routed host or residential proxy.
    "legislature.vermont.gov",
    # WI Legislature docs site fails DNS resolution from non-US environments
    # and may geo-restrict direct connections. Route through US residential proxy.
    "docs.legis.wisconsin.gov",
    # UT Legislature site TCP-times-out from non-US IPs (geo-block). The
    # versioned static HTML content files are accessible once routing is US-based.
    "le.utah.gov",
    # RI Legislature webserver TCP-times-out from non-US IPs (IP firewall at
    # the state network level). Route through a US residential proxy or ZenRows.
    "webserver.rilin.state.ri.us",
    "rilegislature.gov",
    "www.rilegislature.gov",
    # MO Revisor of Statutes TCP-times-out from non-US IPs (state firewall
    # geo-block). HTTPS 443 drops all packets; HTTP 80 returns 403. Must be
    # fetched through ZenRows (SCRAPER_SERVICE_API_KEY) or a US residential proxy.
    "revisor.mo.gov",
    "www.revisor.mo.gov",
    # MI Legislature TCP-times-out from non-US IPs (state network geo-block).
    # The ASP.NET objectname-based URLs are session-free once proxied through
    # ZenRows (SCRAPER_SERVICE_API_KEY) or a US residential proxy.
    "legislature.mi.gov",
    "www.legislature.mi.gov",
    # MA General Court (malegislature.gov) TCP-times-out from non-US IPs
    # (Massachusetts state firewall geo-block). HTTPS 443 drops all packets
    # to international egress. Must be fetched through ZenRows
    # (SCRAPER_SERVICE_API_KEY) or a US residential proxy.
    "malegislature.gov",
    "www.malegislature.gov",
    # OH Revised Code TCP-times-out from non-US IPs (state firewall geo-block).
    # HTTPS 443 drops packets to international egress. Must be fetched through
    # a US residential proxy (Webshare) or ZenRows. Verified 2026-05-12.
    "codes.ohio.gov",
)


def _is_hard_site(url) -> bool:
    u = str(url)
    return any(h in u for h in _HARD_SITE_HINTS)


def _cloudflare_blocked(body: str) -> bool:
    return (
        "Just a moment" in body
        or "challenge-platform" in body
        or "Verify you are human" in body
        or "cf-mitigated" in body
    )


# ---------------------------------------------------------------------------
# Core fetch
# ---------------------------------------------------------------------------


def fetch_html(
    url: str,
    *,
    country_code: str = "us",
    use_proxy: bool = True,
    timeout: float = 30.0,
    max_retries: int = 4,
    extra_headers: Optional[Dict[str, str]] = None,
    referer: Optional[str] = None,
) -> str:
    """Fetch a URL and return decoded HTML/text.

    Strategy: **prefer direct fetch**, fall back to proxy only on
    geo-block / connection failure. Webshare bandwidth is metered, so
    we don't want to route every fetch through it when direct works.

    Each attempt:
      1) direct (no proxy) — for most sites this succeeds
      2) on TCP/connection failure: retry with residential proxy
      3) on persistent failure: try ZenRows scraper-service if configured

    Raises ``requests.HTTPError`` if all retries fail.
    """
    scraper_key = os.environ.get("SCRAPER_SERVICE_API_KEY")
    last_exc: Optional[BaseException] = None

    for attempt in range(1, max_retries + 1):
        # 1) PREFER DIRECT on the first attempt — most state-gov sites work
        #    direct and Webshare bandwidth is metered. Only fall back to
        #    proxy on attempt >= 2 (after a connection failure).
        is_hard = _is_hard_site(url)
        use_proxy_this_attempt = (
            use_proxy
            and (attempt > 1 or is_hard)  # 1st attempt: direct unless known-hard site
        )

        # 1a) Hard sites: route through ZenRows if a key is configured
        if scraper_key and is_hard and attempt >= 2:
            bypass = (
                "https://api.zenrows.com/v1/?apikey=" + urllib.parse.quote(scraper_key)
                + "&url=" + urllib.parse.quote(url, safe="")
                + "&premium_proxy=true"
                + f"&proxy_country={country_code}"
            )
            _log.info("fetch_via_scraper_service", url=url, attempt=attempt)
            try:
                resp = requests.get(bypass, timeout=60)
                if resp.status_code == 200:
                    return resp.text
                _log.warning("scraper_service_status", url=url, status=resp.status_code, body_head=resp.text[:200])
            except Exception as e:  # noqa: BLE001
                _log.warning("scraper_service_error", url=url, error=str(e))
                last_exc = e

        # 2) Standard request with rotated UA + (optional) residential proxy.
        profile = _random_profile()
        proxies = _proxy_for(country_code) if use_proxy_this_attempt else None
        headers = {
            "User-Agent": profile["ua"],
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,"
                      "image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Sec-Ch-Ua": profile["sec_ch_ua"],
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": profile["platform"],
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Connection": "keep-alive",
        }
        if referer:
            headers["Referer"] = referer
        if extra_headers:
            headers.update(extra_headers)

        try:
            resp = _get_session().get(url, headers=headers, proxies=proxies, timeout=timeout, allow_redirects=True)
        except requests.exceptions.ProxyError as e:
            # Transient proxy hiccup — retry the next attempt with the proxy
            # again. Webshare rotating exits occasionally drop a connection;
            # permanently disabling the proxy for the whole run causes every
            # subsequent fetch to hit the direct path and stall on geo-blocked
            # hosts (e.g. codes.ohio.gov).
            _log.warning("proxy_error_retry", url=url, attempt=attempt, error=str(e)[:160])
            last_exc = e
            time.sleep(min(2 * attempt, 6))
            continue
        except Exception as e:  # noqa: BLE001
            _log.warning("fetch_exception", url=url, attempt=attempt, error=str(e))
            last_exc = e
            time.sleep(min(2 * attempt, 8))
            continue

        if resp.status_code == 200:
            if _cloudflare_blocked(resp.text):
                _log.warning("cloudflare_challenge", url=url, attempt=attempt)
                last_exc = requests.HTTPError("Cloudflare challenge")
                time.sleep(min(2 * attempt, 8))
                continue
            return resp.text

        if resp.status_code in (403, 429):
            _log.warning("http_blocked", url=url, attempt=attempt, status=resp.status_code)
            last_exc = requests.HTTPError(f"HTTP {resp.status_code} for {url}")
            time.sleep(min(3 * attempt, 12))
            continue

        _log.warning("http_nonok", url=url, attempt=attempt, status=resp.status_code)
        last_exc = requests.HTTPError(f"HTTP {resp.status_code} for {url}")
        time.sleep(min(2 * attempt, 8))

    if last_exc:
        raise last_exc
    raise requests.HTTPError(f"failed to fetch {url} after {max_retries} attempts")


def fetch_soup(url: str, **kw: Any):
    """Convenience: return BeautifulSoup from fetch_html."""
    from bs4 import BeautifulSoup
    return BeautifulSoup(fetch_html(url, **kw), "html.parser")


def fetch_bytes(
    url: str,
    *,
    country_code: str = "us",
    use_proxy: bool = True,
    timeout: float = 60.0,
    max_retries: int = 3,
    extra_headers: Optional[Dict[str, str]] = None,
    referer: Optional[str] = None,
) -> tuple[bytes, str]:
    """Fetch a non-HTML resource (PDF / XML / JSON / DOCX), return ``(body, content_type)``.

    Uses the same UA rotation + proxy logic as ``fetch_html`` and also pushes
    the bytes to R2 so chunks can reference the source file. Use this from
    scrapers instead of ``requests.get(...)`` for any non-HTML fetch.

    Like ``fetch_html``, this prefers a DIRECT fetch and only falls back to the
    residential proxy on attempt >= 2 (or immediately for a known-hard/
    geo-blocked host). That matters more here than for HTML: these are the
    PDF/DOCX/XML payloads, i.e. the largest bodies we pull, and Webshare
    bandwidth is metered. Previously every byte of every PDF was proxied.

    Raises ``requests.HTTPError`` if all retries fail.
    """
    last_exc: Optional[BaseException] = None
    for attempt in range(1, max_retries + 1):
        profile = _random_profile()
        is_hard = _is_hard_site(url)
        proxies = (
            _proxy_for(country_code)
            if (use_proxy and (attempt > 1 or is_hard))
            else None
        )
        headers = {
            "User-Agent": profile["ua"],
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
        }
        if referer:
            headers["Referer"] = referer
        if extra_headers:
            headers.update(extra_headers)

        try:
            resp = _get_session().get(url, headers=headers, proxies=proxies, timeout=timeout, allow_redirects=True)
        except requests.exceptions.ProxyError as e:
            # Transient proxy hiccup — retry the next attempt with the proxy
            # again, exactly as fetch_html does. This used to set a module-wide
            # _PROXY_DISABLED flag on the FIRST such error, which permanently
            # disabled proxying for the rest of the process. Because the flag
            # was also read by fetch_html, one dropped Webshare exit during a
            # single PDF fetch silently forced every later HTML fetch onto the
            # direct path, where geo-blocked hosts (codes.ohio.gov et al) just
            # hang until timeout. It also never reset and was mutated from
            # worker threads without a lock.
            _log.warning("proxy_error_retry", url=url, attempt=attempt, error=str(e)[:160])
            last_exc = e
            time.sleep(min(2 * attempt, 6))
            continue
        except Exception as e:  # noqa: BLE001
            _log.warning("fetch_bytes_exception", url=url, attempt=attempt, error=str(e))
            last_exc = e
            time.sleep(min(2 * attempt, 8))
            continue

        if resp.status_code == 200:
            content_type = resp.headers.get("Content-Type", "")
            return resp.content, content_type

        _log.warning("fetch_bytes_nonok", url=url, attempt=attempt, status=resp.status_code)
        last_exc = requests.HTTPError(f"HTTP {resp.status_code} for {url}")
        time.sleep(min(2 * attempt, 8))

    if last_exc:
        raise last_exc
    raise requests.HTTPError(f"failed to fetch_bytes {url} after {max_retries} attempts")
