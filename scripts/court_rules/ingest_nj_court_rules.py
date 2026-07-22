#!/usr/bin/env python3
"""Ingest New Jersey Rules of Court into statutes_us.

corpus_type='state_rules', document_type='court_rule', act_id prefix
'SRULES_NJ_'. Same record shape as the other court-rules ingests; reconcile
stays act_id-scoped and never touches NJ statutes/constitution (state='nj').

Source + the hard part
----------------------
The NJ Judiciary (njcourts.gov) publishes each Rule chapter as a text-layered
PDF at a stable path:

    https://www.njcourts.gov/sites/default/files/r{part}-{chapter}.pdf
    (fallback root) https://www.njcourts.gov/attorneys/assets/rules/r{part}-{chapter}.pdf

BUT the entire site (HTML pages AND the PDFs) sits behind an Imperva/Incapsula
WAF with a JavaScript challenge: a plain requests/curl fetch returns a ~940-byte
403 Incapsula block page, not the PDF. A realistic User-Agent alone does NOT
clear it (it is a JS/cookie challenge, not UA filtering, and not geographic).

Empirical status (2026-07-19, box + Webshare US residential proxy):
  - curl_cffi chrome impersonation -> blocked (returns the ~940-byte block page).
  - `--use-browser` (Playwright on a sticky US IP) DOES clear the Incapsula
    challenge on the njcourts.gov HOMEPAGE (renders fine), but the PDF file
    paths (/sites/default/files/r*.pdf, /attorneys/assets/rules/r*.pdf) return a
    SEPARATE, persistent ~1050-byte Incapsula block (`<META ROBOTS NOINDEX>`)
    that does NOT self-solve via in-page fetch(), top-level navigation, download
    capture, or patient re-fetch (8 rounds/35s all blocked). So the residential
    proxy IP is trusted for the homepage but flagged for file downloads.
  - `--use-exa` (Exa /contents, api.exa.ai) also FAILS at scale: Exa's live
    crawler hits the same Incapsula wall and returns `CRAWL_LIVECRAWL_TIMEOUT`
    (504) for every uncached PDF. Only PDFs Exa had already indexed return text
    (of the probed set, just r4-86.pdf was cached). So Exa is not a general NJ
    fetcher either -- it can only serve its pre-existing cache.
  - Conclusion: every no-solver path is blocked (direct, curl_cffi, warmed
    Playwright, Exa live-crawl). NJ needs a commercial Incapsula-solving fetch
    (ScrapFly/ZenRows / a SCRAPER_SERVICE_API_KEY, the same gap as the AR/TN
    statute cutovers), or a genuinely clean residential IP pool. The `--use-exa`
    and `--use-browser` paths below are working scaffolds (Exa parses the one
    cached PDF cleanly); point `_exa_contents` at a solver-backed endpoint once a
    key exists, and the parse/record/embed pipeline is ready to go.

Structure: 8 Parts (I General, II Appellate, III Criminal, IV Civil, V Family,
VI Tax, VII Municipal, VIII Special Civil). Each chapter PDF holds multiple
numbered sections. Citation form: R. {part}:{chapter}-{section} (e.g. R. 4:5-1 =
Part IV, chapter 5, section 1). The PDF filename encodes only part+chapter.

The NJ Rules of Evidence (N.J.R.E.) are a distinct body (cited N.J.R.E. 401),
not part of R. Parts I-VIII; ingest them separately if wanted, not here.

Dependencies to run: pymupdf/fitz (parse; already on the image) + an
Incapsula-clearing fetch (curl_cffi, or Playwright + US proxy on the image).

Output: state_nj_court_rules.jsonl (embed with lib/embed_and_upsert.py).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID

sys.stdout.reconfigure(line_buffering=True)  # type: ignore[attr-defined]

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = Path(os.environ.get("OUT_DIR", "./data"))
OUT = DATA_DIR / "state_nj_court_rules.jsonl"

NJ_PDF_ROOTS = [
    "https://www.njcourts.gov/sites/default/files/r{part}-{chapter}.pdf",
    "https://www.njcourts.gov/attorneys/assets/rules/r{part}-{chapter}.pdf",
]
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
)

# Part number -> name. Chapters per part are discovered by probing r{p}-{c}.pdf.
PART_NAMES = {
    "1": "Rules of General Application",
    "2": "Rules Governing Appellate Practice",
    "3": "Rules Governing Criminal Practice",
    "4": "Rules Governing Civil Practice in the Superior Court, Tax Court and Surrogate's Courts",
    "5": "Rules Governing Practice in the Chancery Division, Family Part",
    "6": "Rules Governing Practice in the Tax Court",
    "7": "Rules Governing Practice in the Municipal Courts",
    "8": "Rules Governing Practice in the Special Civil Part",
}
MAX_CHAPTER = 100  # probe r{part}-1.pdf .. r{part}-100.pdf, skip misses


def _load_env() -> None:
    env_path = _PROJECT_ROOT / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


_load_env()


def _proxies() -> dict[str, str] | None:
    user = os.environ.get("WEBSHARE_USERNAME", "")
    pwd = os.environ.get("WEBSHARE_PASSWORD", "")
    if not user or not pwd:
        return None
    proxy_user = f"{user}-US-rotate"
    host = os.environ.get("WEBSHARE_PROXY_HOST", "p.webshare.io")
    port = os.environ.get("WEBSHARE_PROXY_PORT", "80")
    url = f"http://{urllib.parse.quote(proxy_user)}:{urllib.parse.quote(pwd)}@{host}:{port}"
    return {"http": url, "https": url}


def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def _point_id(act_id: str, chunk_idx: int, text: str) -> str:
    seed = f"{act_id}::{chunk_idx}::{_sha1(text)[:12]}"
    return str(UUID(hashlib.md5(seed.encode()).hexdigest()))


def _safe(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "_", s).strip("_")


# ---------------------------------------------------------------------------
# Incapsula-aware fetch. curl_cffi (chrome JA3) does NOT clear njcourts.gov's
# Incapsula deployment, so the working path is a warmed Playwright browser on a
# STICKY US proxy IP (Incapsula cookies are IP-bound): open the site once to pass
# the JS challenge, then pull every PDF via context.request (shares cookies +
# proxy). Enable with --use-browser (sets NJ_USE_BROWSER=1).
# ---------------------------------------------------------------------------
def _is_pdf(b: bytes | None) -> bool:
    return bool(b) and b[:4] == b"%PDF"


_INCAP_MARKERS = ("request unsuccessful", "incapsula incident", "_incapsula_resource")

# Sticky Playwright browser context, warmed once against the Incapsula challenge.
_PW: dict = {}


def _sticky_proxy_pw() -> dict | None:
    user = os.environ.get("WEBSHARE_USERNAME", "")
    pwd = os.environ.get("WEBSHARE_PASSWORD", "")
    if not user or not pwd:
        return None
    slot = os.environ.get("NJ_PROXY_SLOT", "1")
    host = os.environ.get("WEBSHARE_PROXY_HOST", "p.webshare.io")
    port = os.environ.get("WEBSHARE_PROXY_PORT", "80")
    return {
        "server": f"http://{host}:{port}",
        "username": f"{user}-US-{slot}",
        "password": pwd,
    }


def _browser_ctx():
    if _PW.get("ctx") is not None:
        return _PW["ctx"]
    from playwright.sync_api import sync_playwright

    p = sync_playwright().start()
    browser = p.chromium.launch(
        headless=True,
        proxy=_sticky_proxy_pw(),
        args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
    )
    ctx = browser.new_context(
        user_agent=UA, locale="en-US", timezone_id="America/New_York",
        viewport={"width": 1280, "height": 900},
    )
    page = ctx.new_page()
    # Warm the Incapsula clearance cookie on the homepage.
    for attempt in range(6):
        try:
            page.goto("https://www.njcourts.gov/", wait_until="domcontentloaded", timeout=60000)
        except Exception:
            page.wait_for_timeout(3000)
            continue
        page.wait_for_timeout(4000)
        body = (page.evaluate("document.body ? document.body.innerText : ''") or "").lower()
        if len(body) > 800 and not any(m in body for m in _INCAP_MARKERS):
            print(f"  [NJ] Incapsula cleared (warm attempt {attempt + 1})", flush=True)
            break
        page.wait_for_timeout(3000)
    else:
        print("  [NJ] WARNING: Incapsula not visibly cleared; fetches may 403", flush=True)
    _PW.update(p=p, browser=browser, ctx=ctx, page=page)
    return ctx


# Fetch inside the warmed page via same-origin fetch(): this carries the
# Incapsula clearance cookie and JS context, so the PDF endpoint is not
# re-challenged (context.request.get, which bypasses the page, IS blocked).
_IN_PAGE_FETCH = """
async (u) => {
  try {
    const r = await fetch(u, {credentials: 'include'});
    if (!r.ok) return {status: r.status, b64: null};
    const buf = await r.arrayBuffer();
    const bytes = new Uint8Array(buf);
    let bin = '';
    const chunk = 0x8000;
    for (let i = 0; i < bytes.length; i += chunk) {
      bin += String.fromCharCode.apply(null, bytes.subarray(i, i + chunk));
    }
    return {status: r.status, b64: btoa(bin)};
  } catch (e) { return {status: -1, b64: null}; }
}
"""


def _browser_fetch(url: str) -> bytes | None:
    import base64

    _browser_ctx()
    page = _PW["page"]
    for attempt in range(3):
        try:
            res = page.evaluate(_IN_PAGE_FETCH, url)
            if res and res.get("status") == 404:
                return None
            if res and res.get("b64"):
                body = base64.b64decode(res["b64"])
                if _is_pdf(body):
                    return body
        except Exception:
            pass
        page.wait_for_timeout(1200)
    return None


# ScrapFly ASP (Anti-Scraping-Protection) solves Incapsula and returns the PDF
# bytes, which the existing pymupdf parser handles. This is the intended path for
# NJ once SCRAPFLY_API_KEY is set (Exa/browser/curl_cffi are all Incapsula-blocked
# on the njcourts PDF paths). Cost note: ASP requests are billed per request, so
# the direct-mode loop below uses a consecutive-miss cutoff to bound the count.
def _scrapfly_bytes(url: str) -> bytes | None:
    import base64

    import requests as _rq

    key = os.environ.get("SCRAPFLY_API_KEY", "")
    if not key:
        return None
    params = {
        "key": key, "url": url, "asp": "true", "render_js": "false",
        "country": "us", "retry": "true",
    }
    try:
        r = _rq.get("https://api.scrapfly.io/scrape", params=params, timeout=180)
        if r.status_code != 200:
            return None
        res = (r.json() or {}).get("result", {})
        content = res.get("content")
        if content is None:
            return None
        if isinstance(content, str):
            if res.get("content_encoding") == "base64":
                try:
                    content = base64.b64decode(content)
                except Exception:
                    return None
            else:
                content = content.encode("latin-1", "ignore")
        return content if content[:4] == b"%PDF" else None
    except Exception:
        return None


def _fetch_bytes(url: str) -> bytes | None:
    if os.environ.get("NJ_USE_SCRAPFLY"):
        return _scrapfly_bytes(url)
    if os.environ.get("NJ_USE_BROWSER"):
        return _browser_fetch(url)
    # Fallback: curl_cffi (works only if Incapsula is not enforced for the IP).
    cookie = os.environ.get("NJ_INCAP_COOKIE", "")
    headers = {"User-Agent": UA}
    if cookie:
        headers["Cookie"] = cookie
    try:
        from curl_cffi import requests as cf_requests  # type: ignore

        for attempt in range(4):
            r = cf_requests.get(url, impersonate="chrome", proxies=_proxies(),
                                headers=headers if cookie else None,
                                timeout=60, allow_redirects=True)
            if r.status_code == 200 and _is_pdf(r.content):
                return r.content
            if r.status_code == 404:
                return None
            time.sleep(2 + attempt * 3)
        return None
    except Exception:
        return None


# ---------------------------------------------------------------------------
# PDF parse -> sections (split on {part}:{chapter}-{section} headers)
# ---------------------------------------------------------------------------
@dataclass
class Rule:
    part: str
    chapter: str
    section: str        # e.g. "1", "5", "1A"
    part_name: str
    section_title: str
    raw_text: str
    source_url: str


def _pdf_text(pdf_bytes: bytes) -> str:
    import fitz  # PyMuPDF (leak-free; pdfplumber leaks across recurring refreshes)

    parts: list[str] = []
    with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
        for page in doc:
            t = page.get_text("text") or ""
            if t.strip():
                parts.append(t)
    return re.sub(r"[ \t]+", " ", "\n".join(parts))


def _parse_text(part: str, chapter: str, text: str, source_url: str) -> list[Rule]:
    # Section headers within a chapter file: "4:5-1." possibly with a trailing
    # title, e.g. "4:5-1. General Requirements for Pleadings".
    hdr = re.compile(
        rf"(?m)^\s*{re.escape(part)}:{re.escape(chapter)}-(\d+[A-Za-z]?)\.?\s*(.*?)\s*$"
    )
    matches = list(hdr.finditer(text))
    if not matches:
        return []
    rules: list[Rule] = []
    for i, m in enumerate(matches):
        sec = m.group(1)
        title = m.group(2).strip().rstrip(".")
        start = m.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        body = text[start:end].strip()
        if len(body) < 60:
            continue
        rules.append(Rule(
            part=part, chapter=chapter, section=sec,
            part_name=PART_NAMES.get(part, f"Part {part}"),
            section_title=title or f"Rule {part}:{chapter}-{sec}",
            raw_text=(f"Rule {part}:{chapter}-{sec}. {title}\n\n{body}" if title else body),
            source_url=source_url,
        ))
    # dedupe by section keeping longest body (drops any ToC echo)
    best: dict[str, Rule] = {}
    for r in rules:
        cur = best.get(r.section)
        if cur is None or len(r.raw_text) > len(cur.raw_text):
            best[r.section] = r
    return list(best.values())


def parse_chapter_pdf(part: str, chapter: str, pdf_bytes: bytes, source_url: str) -> list[Rule]:
    return _parse_text(part, chapter, _pdf_text(pdf_bytes), source_url)


def _chapter_pdf(part: str, chapter: str) -> tuple[bytes, str] | None:
    for tmpl in NJ_PDF_ROOTS:
        url = tmpl.format(part=part, chapter=chapter)
        b = _fetch_bytes(url)
        if _is_pdf(b):
            return b, url  # type: ignore[return-value]
    return None


# ---------------------------------------------------------------------------
# Exa mode: api.exa.ai/contents returns extracted PDF TEXT and clears the
# Incapsula wall that blocks direct/curl/browser fetches. This is the working
# path for NJ. Enable with --use-exa (needs EXA_API_KEY).
# ---------------------------------------------------------------------------
_EXA_ENDPOINT = "https://api.exa.ai/contents"


def _exa_contents(urls: list[str], batch: int = 20) -> dict[str, str]:
    """Batch-fetch URL text via Exa. Returns url -> text for URLs that returned
    real content (short/empty responses = missing chapter or block, dropped)."""
    import requests as _rq

    key = os.environ.get("EXA_API_KEY", "")
    if not key:
        print("  [NJ] EXA_API_KEY missing; --use-exa cannot run", flush=True)
        return {}
    out: dict[str, str] = {}
    hdr = {"x-api-key": key, "Content-Type": "application/json"}
    for i in range(0, len(urls), batch):
        chunk = urls[i : i + batch]
        try:
            r = _rq.post(_EXA_ENDPOINT, headers=hdr,
                         json={"urls": chunk, "text": True}, timeout=180)
            if r.status_code != 200:
                print(f"  [NJ] exa status {r.status_code} on batch {i // batch}", flush=True)
                continue
            for res in r.json().get("results", []):
                u = res.get("url") or ""
                t = res.get("text") or ""
                # Match Exa's returned url back to a requested url (Exa may
                # normalise trailing bits); fall back to substring match.
                key_u = u if u in chunk else next((c for c in chunk if c.split("/")[-1] in u), u)
                if len(t) > 200:
                    out[key_u] = t
        except Exception as e:
            print(f"  [NJ] exa batch {i // batch} failed: {e}", flush=True)
    return out


# ---------------------------------------------------------------------------
# Record shape (mirrors the other court-rules ingests)
# ---------------------------------------------------------------------------
def _to_chunk_record(rule: Rule) -> dict:
    act_id = f"SRULES_NJ_P{rule.part}_C{_safe(rule.chapter)}_S{_safe(rule.section)}"
    title_label = "New Jersey Rules of Court"
    citation = f"R. {rule.part}:{rule.chapter}-{rule.section}"
    text_for_embedding = (
        f"{title_label} | {rule.part_name} | {citation}\n"
        f"{rule.section_title}\n\n{rule.raw_text}"
    )
    md = {
        "act_id": act_id,
        "corpus_type": "state_rules",
        "category": "state_rules",
        "document_type": "court_rule",
        "jurisdiction": "US",
        "country_code": "US",
        "state": "nj",
        "title_name": title_label,
        "title": title_label,
        "title_code": "nj_court_rules",
        "top_level_title": "rules-nj",
        "level_classifier": "rule",
        "chapter": f"{rule.part}:{rule.chapter}",
        "chapter_name": f"Part {rule.part}. {rule.part_name}",
        "subchapter": None,
        "subchapter_name": rule.part_name,
        "section_number": f"{rule.part}:{rule.chapter}-{rule.section}",
        "section_title": f"{citation} {rule.section_title}",
        "citation": citation,
        "citation_short": citation,
        "display_label": citation,
        "display_title": rule.section_title,
        "display_path": f"{title_label} / Part {rule.part} / {citation}",
        "breadcrumb": [title_label, f"Part {rule.part}. {rule.part_name}", citation],
        "sort_key": act_id,
        "act_status": "in_force",
        "renumbered_to": "",
        "transferred_to": "",
        "year": None,
        "word_count": len(rule.raw_text.split()),
        "subsection_count": 0,
        "subsection_letters": [],
        "numbered_paragraph_count": 0,
        "amendments_count": 0,
        "amendment_years": [],
        "last_amended_year": None,
        "cross_references_count": 0,
        "cross_references_usc": [],
        "cross_references_cfr": [],
        "public_laws_count": 0,
        "public_laws_referenced": [],
        "source_url": rule.source_url,
        "parent_id": None,
        "raw_node_id": act_id,
        "full_text_sha1": _sha1(rule.raw_text),
    }
    return {
        "point_id": _point_id(act_id, 0, rule.raw_text),
        "text_for_embedding": text_for_embedding,
        "raw_text": rule.raw_text,
        "metadata": md,
    }


def _write_jsonl(path: Path, rules: list[Rule]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    records: dict[str, dict] = {}
    for r in rules:
        rec = _to_chunk_record(r)
        records[rec["metadata"]["act_id"]] = rec
    with open(path, "w", encoding="utf-8") as fh:
        for rec in records.values():
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
    return len(records)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--parts", default="1,2,3,4,5,6,7,8",
                    help="Comma-separated NJ Part numbers to ingest.")
    ap.add_argument("--max-chapter", type=int, default=MAX_CHAPTER)
    ap.add_argument("--use-browser", action="store_true",
                    help="Fetch via a warmed Playwright browser (blocked on PDF paths).")
    ap.add_argument("--use-exa", action="store_true",
                    help="Fetch chapter text via Exa (only serves Exa's cache; live-crawl is Incapsula-blocked).")
    ap.add_argument("--use-scrapfly", action="store_true",
                    help="Fetch PDF bytes via ScrapFly ASP (solves Incapsula; needs SCRAPFLY_API_KEY). RECOMMENDED for NJ.")
    ap.add_argument("--miss-cutoff", type=int, default=12,
                    help="Direct/ScrapFly mode: stop probing a Part after this many consecutive missing chapters.")
    ap.add_argument("--out", type=Path, default=OUT)
    args = ap.parse_args()

    if args.use_browser:
        os.environ["NJ_USE_BROWSER"] = "1"
    if args.use_scrapfly:
        os.environ["NJ_USE_SCRAPFLY"] = "1"
    parts = [p.strip() for p in args.parts.split(",") if p.strip()]
    mode = ("exa" if args.use_exa else "scrapfly" if args.use_scrapfly
            else "browser" if args.use_browser else "direct")
    print(f"=== New Jersey court-rules ingest: parts {parts} ({mode}) ===", flush=True)
    all_rules: list[Rule] = []

    def _handle(part: str, chapter: str, rules: list[Rule]) -> None:
        all_rules.extend(rules)

    if args.use_exa:
        cand: dict[str, tuple[str, str]] = {}
        for part in parts:
            for c in range(1, args.max_chapter + 1):
                cand[NJ_PDF_ROOTS[0].format(part=part, chapter=c)] = (part, str(c))
        print(f"[NJ] Exa-fetching up to {len(cand)} candidate chapter URLs...", flush=True)
        texts = _exa_contents(list(cand.keys()))
        print(f"[NJ] Exa returned usable content for {len(texts)} URLs", flush=True)
        per_part: dict[str, int] = {}
        for url, text in texts.items():
            part, chapter = cand.get(url, (None, None))  # type: ignore[assignment]
            if part is None:
                continue
            rules = _parse_text(part, chapter, text, url)
            if rules:
                per_part[part] = per_part.get(part, 0) + 1
                _handle(part, chapter, rules)
        for part in parts:
            print(f"  [NJ Part {part}] {per_part.get(part, 0)} chapters, "
                  f"{len([r for r in all_rules if r.part == part])} sections", flush=True)
    else:
        for part in parts:
            found_chapters = 0
            misses = 0
            for c in range(1, args.max_chapter + 1):
                got = _chapter_pdf(part, str(c))
                if not got:
                    misses += 1
                    if misses >= args.miss_cutoff and found_chapters:
                        break  # bound per-request billing once a Part's chapters run out
                    continue
                misses = 0
                pdf, url = got
                found_chapters += 1
                _handle(part, str(c), parse_chapter_pdf(part, str(c), pdf, url))
            print(f"  [NJ Part {part}] {found_chapters} chapters, "
                  f"{len([r for r in all_rules if r.part == part])} sections", flush=True)

    n = _write_jsonl(args.out, all_rules)
    print(f"\n[NJ] done: {len(all_rules)} sections, {n} unique act_ids\n=> {args.out}", flush=True)
    if not all_rules:
        print("[NJ] NOTE: 0 sections. Direct/browser paths are Incapsula-blocked on "
              "the PDF file paths; use --use-exa with EXA_API_KEY set.", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
