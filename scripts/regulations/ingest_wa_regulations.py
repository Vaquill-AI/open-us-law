#!/usr/bin/env python3
"""Ingest the Washington Administrative Code (WAC) — Washington's state regs.

OFFICIAL SOURCE ONLY: the Code Reviser's own static document file server at
https://lawfilesext.leg.wa.gov/law/wac/ . No aggregators (no Justia/ZenRows).

The Code Reviser's "bulk download / web services" landing page
(leg.wa.gov/code-reviser/bulk-download-and-web-services) now 404s, but the
underlying bulk file server it pointed at is alive and authoritative. It serves
the *entire* WAC as a browsable IIS directory tree of static .htm files — one
file per section — which is effectively a bulk download (fast static host, no
dynamic rate-limiting like app.leg.wa.gov). XML files do not exist (.xml 404s);
the .htm files ARE the structured source, each delimited by HTML comment field
markers (`<!-- field: Citations -->`, `CaptionsTitles`, `Text`, `History`).

    /law/wac/                                  → 228 "WAC <n>  TITLE" dirs
    /law/wac/WAC <n>  TITLE/                   → chapter dirs + a title-index dir
    /law/wac/WAC <n>  TITLE/WAC <n>   TITLE/   → title-index .htm (agency name)
    /law/wac/WAC <n>  TITLE/WAC <n> -<c>  CHAPTER/
                                               → CHAPTER.htm (chapter caption) +
                                                 one .htm per live section
    .../WAC <n> -<c> -<sec>.htm                → a single section, e.g.
                                                 "WAC 246-100-006".

Repealed/recodified/decodified sections are listed only in the chapter's
disposition table; they get no standalone .htm in the directory, so crawling
the listing yields live (in-force) sections only — same behavior as the OAC
ingester.

A section's History field carries everything WA exposes:
    "Statutory Authority: RCW 43.20.050. WSR 91-02-051 (Order 124B),
     recodified as § 246-100-006, filed 12/27/90, effective 1/31/91; ..."
We parse it into structured fields: statutory_authority (RCW cites),
effective_date (latest "effective <date>"), prior_effective_dates, the WSR
filing numbers, and order numbers. Nothing is discarded.

WAC structure: Title -> Chapter -> Section. Bluebook cite form is
"WAC <title>-<chapter>-<section>". corpus_type='state_regulation'.

The host is geo-restricted to the US; Webshare US proxy + Mozilla UA + polite
pacing.
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
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

sys.stdout.reconfigure(line_buffering=True)  # type: ignore[attr-defined]

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = Path(os.environ.get("OUT_DIR", "./data"))
OUT = DATA_DIR / "state_wa_regulations.jsonl"

WA_HOST = "https://lawfilesext.leg.wa.gov"
WA_ROOT = f"{WA_HOST}/law/wac/"
# Per-section human-readable / PDF citation host (used only for source_url).
WA_CITE = "https://app.leg.wa.gov/WAC/default.aspx?cite="

_MOZ_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
)


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


def _us_proxies() -> Optional[dict]:
    user = os.environ.get("WEBSHARE_USERNAME", "")
    pwd = os.environ.get("WEBSHARE_PASSWORD", "")
    if not user or not pwd:
        return None
    proxy_user = f"{user}-US-rotate"
    host = os.environ.get("WEBSHARE_PROXY_HOST", "p.webshare.io")
    port = os.environ.get("WEBSHARE_PROXY_PORT", "80")
    url = f"http://{urllib.parse.quote(proxy_user)}:{urllib.parse.quote(pwd)}@{host}:{port}"
    return {"http": url, "https": url}


SESSION = requests.Session()
SESSION.headers.update({"User-Agent": _MOZ_UA})


def fetch(url: str, retries: int = 5) -> Optional[str]:
    proxies = _us_proxies()
    for attempt in range(retries):
        try:
            r = SESSION.get(url, timeout=60, proxies=proxies, allow_redirects=True)
            if r.status_code == 200:
                # The .htm files are UTF-8 with a BOM; requests guesses latin-1
                # from the (absent) charset header, which mangles non-ASCII.
                r.encoding = "utf-8-sig"
                return r.text
            if r.status_code == 429:
                time.sleep(2 ** attempt)
                continue
            if r.status_code in (502, 503, 504):
                time.sleep(2)
                continue
            return None
        except Exception:
            time.sleep(2)
    return None


# ---------------------------------------------------------------------------
# Discovery: root -> titles -> chapters -> section files
# ---------------------------------------------------------------------------

# IIS listing entries are absolute hrefs under /law/wac/. We classify by suffix.
_TITLE_DIR_RE = re.compile(r"/law/wac/WAC%20\s*[^/]*TITLE/?$", re.IGNORECASE)
# Title code sits between "WAC " and " TITLE", e.g. "246", "132Z", "1".
_TITLE_CODE_RE = re.compile(r"WAC\s+([0-9A-Za-z]+)\s+TITLE", re.IGNORECASE)
# Chapter dir: ".../WAC <title> -<chapter>  CHAPTER/"
_CHAPTER_CODE_RE = re.compile(
    r"WAC\s+([0-9A-Za-z]+)\s*-\s*([0-9A-Za-z]+)\s+CHAPTER", re.IGNORECASE
)
# Section file: ".../WAC <title> -<chapter> -<section>.htm"
_SECTION_FILE_RE = re.compile(
    r"WAC\s+([0-9A-Za-z]+)\s*-\s*([0-9A-Za-z]+)\s*-\s*([0-9A-Za-z]+)\.htm$",
    re.IGNORECASE,
)


def _listing_hrefs(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    out: list[str] = []
    for a in soup.find_all("a", href=True):
        if a.get_text(strip=True) == "[To Parent Directory]":
            continue
        out.append(a["href"])
    return out


def _abs(href: str) -> str:
    return href if href.startswith("http") else f"{WA_HOST}{href}"


def list_titles() -> list[tuple[str, str]]:
    """Return [(title_code, title_dir_url)] for every WAC title."""
    html = fetch(WA_ROOT)
    if not html:
        raise RuntimeError("could not fetch WAC root listing")
    out: list[tuple[str, str]] = []
    seen: set[str] = set()
    for href in _listing_hrefs(html):
        if not _TITLE_DIR_RE.search(href):
            continue
        m = _TITLE_CODE_RE.search(urllib.parse.unquote(href))
        if not m:
            continue
        code = m.group(1).upper()
        if code in seen:
            continue
        seen.add(code)
        out.append((code, _abs(href)))
    out.sort(key=_title_sort_key)
    return out


def _title_sort_key(item: tuple[str, str]) -> tuple[int, str]:
    code = item[0] if isinstance(item, tuple) else item
    m = re.match(r"(\d+)", code)
    return (int(m.group(1)) if m else 9999, code)


def list_chapters_in_title(title_url: str) -> list[tuple[str, str, str]]:
    """Return [(title_code, chapter_code, chapter_dir_url)] for a title."""
    html = fetch(title_url)
    if not html:
        return []
    out: list[tuple[str, str, str]] = []
    seen: set[str] = set()
    for href in _listing_hrefs(html):
        m = _CHAPTER_CODE_RE.search(urllib.parse.unquote(href))
        if not m:
            continue
        title_code, chap_code = m.group(1).upper(), m.group(2)
        key = f"{title_code}-{chap_code}"
        if key in seen:
            continue
        seen.add(key)
        out.append((title_code, chap_code, _abs(href)))
    return out


def list_sections_in_chapter(chapter_url: str) -> list[tuple[str, str, str, str]]:
    """Return [(title, chapter, section, section_file_url)] for a chapter.

    The chapter's own ".../CHAPTER.htm" index file is excluded because it does
    not match the three-part section pattern.
    """
    html = fetch(chapter_url)
    if not html:
        return []
    out: list[tuple[str, str, str, str]] = []
    seen: set[str] = set()
    for href in _listing_hrefs(html):
        leaf = urllib.parse.unquote(href.rstrip("/").split("/")[-1])
        m = _SECTION_FILE_RE.search(leaf)
        if not m:
            continue
        t, c, s = m.group(1).upper(), m.group(2), m.group(3)
        key = f"{t}-{c}-{s}"
        if key in seen:
            continue
        seen.add(key)
        out.append((t, c, s, _abs(href)))
    return out


# ---------------------------------------------------------------------------
# Title-name / chapter-name resolution (the agency captions)
# ---------------------------------------------------------------------------

def title_name(title_code: str, title_url: str) -> str:
    """Resolve a title's agency caption from its title-index .htm.

    Layout: .../WAC <t>  TITLE/WAC <t>   TITLE/WAC <t>   TITLE.htm
    whose CaptionsTitles field holds e.g. "HEALTH, DEPARTMENT OF".
    """
    html = fetch(title_url)
    if not html:
        return ""
    for href in _listing_hrefs(html):
        if re.search(r"WAC\s+[^/]*TITLE/?$", urllib.parse.unquote(href), re.IGNORECASE):
            idx_html = fetch(_abs(href.rstrip("/") + "/"))
            if not idx_html:
                continue
            for f2 in _listing_hrefs(idx_html):
                if f2.lower().endswith(".htm"):
                    sub = fetch(_abs(f2))
                    if sub:
                        cap = _field(sub, "CaptionsTitles")
                        if cap:
                            return cap
    return ""


def chapter_name(chapter_url: str) -> str:
    """Resolve a chapter caption from the chapter's index .htm."""
    html = fetch(chapter_url)
    if not html:
        return ""
    for href in _listing_hrefs(html):
        if re.search(r"CHAPTER\.htm$", urllib.parse.unquote(href), re.IGNORECASE):
            sub = fetch(_abs(href))
            if sub:
                return _field(sub, "CaptionsTitles")
    return ""


# ---------------------------------------------------------------------------
# Section parsing
# ---------------------------------------------------------------------------

def _field(html: str, name: str) -> str:
    """Extract the text of one ``<!-- field: NAME -->...<!-- field: -->`` block."""
    m = re.search(
        rf"<!--\s*field:\s*{re.escape(name)}\s*-->(.*?)<!--\s*field:",
        html,
        re.S | re.IGNORECASE,
    )
    if not m:
        return ""
    return BeautifulSoup(m.group(1), "html.parser").get_text(" ", strip=True)


def _body_text(html: str) -> str:
    """Extract the section body, preserving paragraph breaks between <div>s."""
    m = re.search(
        r"<!--\s*field:\s*Text\s*-->(.*?)<!--\s*TextEnd\s*-->",
        html,
        re.S | re.IGNORECASE,
    )
    region = m.group(1) if m else ""
    if not region:
        return ""
    soup = BeautifulSoup(region, "html.parser")
    lines: list[str] = []
    for div in soup.find_all("div"):
        t = div.get_text(" ", strip=True)
        if t:
            lines.append(t)
    if not lines:
        t = soup.get_text(" ", strip=True)
        if t:
            lines.append(t)
    text = "\n".join(lines)
    return re.sub(r"[ \t]+", " ", text).strip()


_RESERVED_PAT = re.compile(
    r"\b(repealed|reserved|recodified|decodified|expired)\b", re.IGNORECASE
)
_RCW_RE = re.compile(r"RCW\s+([\d]+[\dA-Za-z.]*)")
_WSR_RE = re.compile(r"WSR\s+([\d]{2}-[\d]{2}-[\d]{3,})")
_ORDER_RE = re.compile(r"\(Order\s+([^)]+)\)")
# "effective 1/31/91" or "effective 1-31-91"
_EFF_RE = re.compile(r"effective\s+([\d]{1,2}[/-][\d]{1,2}[/-][\d]{2,4})", re.IGNORECASE)


def _dedupe(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for x in items:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def _parse_history(hist: str) -> dict:
    """Split a WAC History string into structured fields.

    Example History:
      "Statutory Authority: RCW 43.20.050. WSR 91-02-051 (Order 124B),
       recodified as § 246-100-006, filed 12/27/90, effective 1/31/91;
       WSR 87-11-047 (Order 302), § 248-100-006, filed 5/19/87."
    """
    rcws = _dedupe(_RCW_RE.findall(hist))
    wsrs = _dedupe(_WSR_RE.findall(hist))
    orders = _dedupe(o.strip() for o in _ORDER_RE.findall(hist))
    effs = _dedupe(_EFF_RE.findall(hist))
    # "Statutory Authority:" preamble (everything up to the first "WSR ").
    stat_auth = ""
    m = re.search(r"Statutory Authority:\s*(.*?)(?:\bWSR\b|$)", hist, re.S | re.IGNORECASE)
    if m:
        stat_auth = re.sub(r"\s+", " ", m.group(1)).strip().rstrip(".").strip()
    return {
        "statutory_authority": stat_auth or ("RCW " + ", ".join(rcws) if rcws else ""),
        "rcw_citations": [f"RCW {c}" for c in rcws],
        "wsr_filings": [f"WSR {w}" for w in wsrs],
        "order_numbers": orders,
        "effective_date": effs[0] if effs else "",
        "prior_effective_dates": "; ".join(effs[1:]) if len(effs) > 1 else "",
    }


@dataclass
class Section:
    title_code: str
    chapter_code: str
    section_code: str
    chapter_name: str
    title_name: str
    caption: str
    raw_text: str
    history: str
    source_url: str
    statutory_authority: str = ""
    rcw_citations: list[str] = field(default_factory=list)
    wsr_filings: list[str] = field(default_factory=list)
    order_numbers: list[str] = field(default_factory=list)
    effective_date: str = ""
    prior_effective_dates: str = ""

    @property
    def cite_num(self) -> str:
        return f"{self.title_code}-{self.chapter_code}-{self.section_code}"

    @property
    def chapter_id(self) -> str:
        return f"{self.title_code}-{self.chapter_code}"


def parse_section(html: str, title_code: str, chapter_code: str,
                  section_code: str, chapter_nm: str, title_nm: str,
                  source_url: str) -> Optional[Section]:
    caption = _field(html, "CaptionsTitles").rstrip(".")
    body = _body_text(html)
    history = _field(html, "History")
    if _RESERVED_PAT.search(caption) and len(body) < 30:
        return None
    if len(body) < 30 and not history:
        return None
    h = _parse_history(history)
    return Section(
        title_code=title_code,
        chapter_code=chapter_code,
        section_code=section_code,
        chapter_name=chapter_nm,
        title_name=title_nm,
        caption=caption,
        raw_text=body,
        history=history,
        source_url=source_url,
        statutory_authority=h["statutory_authority"],
        rcw_citations=h["rcw_citations"],
        wsr_filings=h["wsr_filings"],
        order_numbers=h["order_numbers"],
        effective_date=h["effective_date"],
        prior_effective_dates=h["prior_effective_dates"],
    )


# ---------------------------------------------------------------------------
# Chunk emission
# ---------------------------------------------------------------------------

def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def _point_id(act_id: str, idx: int, text: str) -> str:
    seed = f"{act_id}::{idx}::{_sha1(text)[:12]}"
    return str(uuid.UUID(hashlib.md5(seed.encode()).hexdigest()))


def to_chunk_record(s: Section) -> dict:
    safe = re.sub(r"[^0-9A-Za-z]+", "_", s.cite_num)
    act_id = f"STATE_WA_ADC_{safe}"
    citation = f"WAC {s.cite_num}"
    text = s.raw_text
    section_title = f"WAC {s.cite_num}. {s.caption}" if s.caption else f"WAC {s.cite_num}"

    # Rich, searchable embed header: surface the regulation-specific metadata
    # (effective date, enabling RCW authority) so it is retrievable, not just
    # stored.
    meta_lines: list[str] = []
    if s.effective_date:
        meta_lines.append(f"Effective: {s.effective_date}")
    if s.statutory_authority:
        meta_lines.append(f"Statutory Authority: {s.statutory_authority}")
    if s.wsr_filings:
        meta_lines.append(f"Filings: {', '.join(s.wsr_filings)}")
    meta_header = ("\n" + "\n".join(meta_lines)) if meta_lines else ""
    title_caption = f": {s.title_name}" if s.title_name else ""
    chap_caption = f": {s.chapter_name}" if s.chapter_name else ""
    text_for_embedding = (
        f"Regulation: Washington Administrative Code | US | Washington | In Force\n"
        f"Title {s.title_code}{title_caption} / "
        f"Chapter {s.chapter_id}{chap_caption}\n"
        f"{section_title}{meta_header}\n\n{text}"
    )

    md = {
        "act_id": act_id,
        "corpus_type": "state_regulation",
        "category": "state_regulation",
        "document_type": "regulation",
        "jurisdiction": "US",
        "country_code": "US",
        "state": "wa",
        "title_number": s.title_code,
        "title_name": s.title_name or f"WAC Title {s.title_code}",
        "title": "Washington Administrative Code",
        "title_code": f"wac_{s.title_code.lower()}",
        "top_level_title": s.title_code,
        "chapter": s.chapter_id,
        "chapter_name": s.chapter_name,
        "section_number": s.cite_num,
        "section_title": section_title,
        "year": 2026,
        "act_status": "in_force",
        "renumbered_to": "",
        "transferred_to": "",
        "level_classifier": "section",
        # --- regulation-specific rich metadata (captured, not discarded) ---
        "effective_date": s.effective_date or None,
        "statutory_authority": s.statutory_authority or None,
        "rule_amplifies": None,
        "prior_effective_dates": s.prior_effective_dates or None,
        "history": s.history or None,
        "wsr_filings": s.wsr_filings,
        "rcw_citations": s.rcw_citations,
        "rule_order_numbers": s.order_numbers,
        "issuing_agency": s.title_name or None,
        "issuing_agency_code": s.title_code,
        "citation": citation,
        "citation_short": citation,
        "display_label": citation,
        "display_title": section_title,
        "display_path": (
            f"Washington Administrative Code / Title {s.title_code} / "
            f"Chapter {s.chapter_id} / Section {s.cite_num}"
        ),
        "breadcrumb": [
            {"type": "title", "num": s.title_code,
             "label": f"Title {s.title_code}", "name": s.title_name},
            {"type": "chapter", "num": s.chapter_id,
             "label": f"Chapter {s.chapter_id}", "name": s.chapter_name},
            {"type": "section", "num": s.cite_num,
             "label": f"WAC {s.cite_num}", "name": s.caption},
        ],
        "sort_key": act_id,
        "word_count": len(text.split()),
        "subsection_count": 0,
        "subsection_letters": [],
        "numbered_paragraph_count": 0,
        "cross_references_count": len(s.rcw_citations),
        "cross_references_usc": [],
        "cross_references_cfr": [],
        "amendment_years": [],
        "amendments_count": len(s.wsr_filings),
        "last_amended_year": None,
        "public_laws_referenced": [],
        "public_laws_count": 0,
        "source_url": s.source_url,
        "parent_id": f"us/wa/regulations/title={s.title_code}/chapter={s.chapter_id}",
        "raw_node_id": (
            f"us/wa/regulations/title={s.title_code}/chapter={s.chapter_id}"
            f"/section={s.cite_num}"
        ),
        "parent_chunk_id": _point_id(act_id, -1, text),
        "full_text_sha1": _sha1(text),
    }
    return {
        "point_id": _point_id(act_id, 0, text),
        "text_for_embedding": text_for_embedding,
        "raw_text": text,
        "metadata": md,
    }


# ---------------------------------------------------------------------------
# Crawl
# ---------------------------------------------------------------------------

def process_chapter(title_code: str, chapter_code: str, chapter_url: str,
                    title_nm: str) -> list[Section]:
    chap_nm = chapter_name(chapter_url)
    secs_meta = list_sections_in_chapter(chapter_url)
    out: list[Section] = []
    for (t, c, snum, surl) in secs_meta:
        html = fetch(surl)
        if not html:
            continue
        cite = f"{t}-{c}-{snum}"
        sec = parse_section(
            html, t, c, snum, chap_nm, title_nm,
            f"{WA_CITE}{cite}",
        )
        if not sec:
            continue
        out.append(sec)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--titles", default="",
                    help="Comma-separated title codes (e.g. '246,132Z'). Default: all.")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--delay", type=float, default=0.3)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    print(f"[WAC] discovering titles from {WA_ROOT}", flush=True)
    titles = list_titles()
    if args.titles:
        wanted = {t.strip().upper() for t in args.titles.split(",") if t.strip()}
        titles = [(c, u) for (c, u) in titles if c in wanted]
    print(f"[WAC] {len(titles)} titles", flush=True)

    # Phase 1: gather all chapters (+ resolve title agency names once per title).
    all_chapters: list[tuple[str, str, str, str]] = []  # (title, chap, url, title_name)
    for (title_code, title_url) in titles:
        title_nm = title_name(title_code, title_url)
        chaps = list_chapters_in_title(title_url)
        for (t, c, curl) in chaps:
            all_chapters.append((t, c, curl, title_nm))
        print(f"  [title {title_code}] {len(chaps)} chapters"
              f"  ({title_nm[:48]})", flush=True)
        time.sleep(args.delay)
    print(f"\n[WAC] {len(all_chapters)} chapters to fetch", flush=True)
    if args.dry_run:
        return 0

    # Phase 2: crawl chapters (each chapter pulls its own sections).
    chunks: list[dict] = []
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {
            ex.submit(process_chapter, t, c, curl, tnm): (t, c)
            for (t, c, curl, tnm) in all_chapters
        }
        done = 0
        for fut in as_completed(futures):
            done += 1
            try:
                secs = fut.result()
                chunks.extend(to_chunk_record(x) for x in secs)
            except Exception as e:
                print(f"  ! chapter failed: {e}", flush=True)
            if done % 50 == 0 or done == len(all_chapters):
                rate = len(chunks) / max(time.time() - t0, 0.1)
                print(f"  ... {done:>5}/{len(all_chapters)} chapters, "
                      f"{len(chunks):>6} sections, {rate:.1f}/s", flush=True)
            time.sleep(args.delay / max(args.workers, 1))

    # Dedup + write.
    seen: set[str] = set()
    if OUT.exists():
        with open(OUT) as fh:
            for line in fh:
                try:
                    seen.add(json.loads(line)["point_id"])
                except Exception:
                    pass
    written = 0
    with open(OUT, "a") as fh:
        for c in chunks:
            if c["point_id"] in seen:
                continue
            fh.write(json.dumps(c, ensure_ascii=False) + "\n")
            seen.add(c["point_id"])
            written += 1

    print(f"\n=== Done: parsed={len(chunks):,}, new={written:,}, "
          f"elapsed={time.time()-t0:.1f}s ===", flush=True)
    print(f"JSONL: {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
