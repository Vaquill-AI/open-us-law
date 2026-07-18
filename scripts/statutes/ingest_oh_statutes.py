#!/usr/bin/env python3
"""Ingest Ohio Revised Code into the state-statutes pipeline.

codes.ohio.gov was redesigned to render full section text inline on the
chapter page (the legacy "sections table" no longer exists). One fetch per
chapter pulls every section in that chapter (some chapters have 90+ sections,
~200KB body each).

URL layout:
    /ohio-revised-code              → TOC, 33 title links
    /ohio-revised-code/title-N      → list of chapter relative URLs
    /ohio-revised-code/chapter-NNN  → all sections inline, header
                                       'Section NNN.NN | <heading>' per section

Section regex anchor: "Section NNN.NN" appearing on its own line. We slice
each section from its anchor until the next anchor; metadata trailers
("Effective:", "Latest Legislation:", "PDF: Download Authenticated PDF")
are stripped to keep just the substantive rule text.

Geo-restricted; uses Webshare US proxy + Mozilla UA. Rate-limited politely
(2 workers, 1.5s pause per chapter) — the site 429s at >4 req/s.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

sys.stdout.reconfigure(line_buffering=True)  # type: ignore[attr-defined]

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = Path(os.environ.get("OUT_DIR", "./data"))
OUT = DATA_DIR / "state_oh_statutes.jsonl"

OH_BASE = "https://codes.ohio.gov"
OH_TOC = f"{OH_BASE}/ohio-revised-code"


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

_MOZ_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
)


def _us_proxies() -> Optional[dict]:
    user = os.environ.get("WEBSHARE_USERNAME", "")
    pwd = os.environ.get("WEBSHARE_PASSWORD", "")
    if not user or not pwd:
        return None
    import urllib.parse
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
            r = SESSION.get(url, timeout=45, proxies=proxies, allow_redirects=True)
            if r.status_code == 200:
                return r.text
            if r.status_code == 429:
                # Rate limited — back off exponentially
                time.sleep(2 ** attempt)
                continue
            if r.status_code in (502, 503, 504):
                time.sleep(2)
                continue
            # Other status — give up
            return None
        except Exception:
            time.sleep(2)
    return None


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------

_TITLE_HREF_RE = re.compile(r"(?:^|/)title-(\d+)(?:\?.*)?$")
_CHAPTER_HREF_RE = re.compile(r"(?:^|/)chapter-([0-9A-Za-z\-]+)(?:\?.*)?$")


def list_titles() -> list[tuple[str, str]]:
    """Return [(title_num, full_url), ...] for all 33 OH titles."""
    html = fetch(OH_TOC)
    if not html:
        raise RuntimeError("could not fetch OH TOC")
    soup = BeautifulSoup(html, "html.parser")
    seen: dict[str, str] = {}
    for a in soup.find_all("a", href=True):
        href = a["href"]
        m = _TITLE_HREF_RE.search(href)
        if not m:
            continue
        title_num = m.group(1)
        # The TOC at /ohio-revised-code lists hrefs like "ohio-revised-code/title-1".
        # Those resolve relative to the *parent* dir (root), so the canonical
        # absolute URL is OH_BASE/<href>.
        if href.startswith("http"):
            full = href
        elif href.startswith("/"):
            full = OH_BASE + href
        else:
            full = f"{OH_BASE}/{href}"
        seen[title_num] = full
    out = sorted(seen.items(), key=lambda kv: int(kv[0]) if kv[0].isdigit() else 9999)
    return [(t, u) for t, u in out]


def list_chapters_in_title(title_num: str, title_url: str) -> list[tuple[str, str]]:
    """Return [(chapter_num, chapter_full_url), ...] for one title."""
    html = fetch(title_url)
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    seen: dict[str, str] = {}
    for a in soup.find_all("a", href=True):
        href = a["href"]
        m = _CHAPTER_HREF_RE.search(href)
        if not m:
            continue
        chap_num = m.group(1)
        # title page hrefs like "chapter-101" resolve to /ohio-revised-code/chapter-101
        if href.startswith("http"):
            full = href
        elif href.startswith("/"):
            full = OH_BASE + href
        else:
            full = f"{OH_BASE}/ohio-revised-code/{href}"
        seen[chap_num] = full
    return sorted(seen.items(), key=lambda kv: (
        int(kv[0]) if kv[0].isdigit() else 9999, kv[0]
    ))


# ---------------------------------------------------------------------------
# Section parsing
# ---------------------------------------------------------------------------

# Match a "Section NNN.NN" header on its own line followed by the heading.
# Sections in OH look like 101.01, 101.011, 1109.51, 3964.123, etc.
_SECTION_RE = re.compile(
    r"\nSection\s+(\d+(?:\.\d+(?:\.\d+)?)?[A-Za-z]?)\s*\|\s*([^\n]+?)\n",
    re.MULTILINE,
)

_RESERVED_PAT = re.compile(r"\[(repealed|expired|reserved|renumbered|amended)\b", re.IGNORECASE)


def _strip_metadata(body: str) -> str:
    """Drop the per-section metadata block that precedes the rule text."""
    body = re.sub(
        r"Effective:[^\n]*\nLatest Legislation:[^\n]*\nPDF:[^\n]*\nDownload Authenticated PDF\n",
        "", body, flags=re.IGNORECASE,
    )
    body = re.sub(
        r"Effective:[^\n]*\n", "", body, flags=re.IGNORECASE,
    )
    body = re.sub(
        r"Latest Legislation:[^\n]*\n", "", body, flags=re.IGNORECASE,
    )
    body = re.sub(
        r"PDF:\s*Download Authenticated PDF\s*", "", body, flags=re.IGNORECASE,
    )
    body = re.sub(r"\s+", " ", body).strip()
    return body


@dataclass
class Section:
    title_num: str
    chapter_num: str
    chapter_name: str
    section_num: str
    section_title: str
    raw_text: str
    source_url: str


def parse_chapter(html: str, title_num: str, chapter_num: str,
                  chapter_url: str) -> tuple[str, list[Section]]:
    """Return (chapter_name, [Section, ...]) parsed from one chapter page."""
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=True)

    # Chapter heading: "Chapter <N> | <Name>"
    chap_name = ""
    m_cname = re.search(rf"\nChapter\s+{re.escape(chapter_num)}\s*\|\s*([^\n]+?)\n", text)
    if m_cname:
        chap_name = m_cname.group(1).strip()

    matches = list(_SECTION_RE.finditer(text))
    sections: list[Section] = []
    for i, m in enumerate(matches):
        sec_num = m.group(1).strip()
        sec_head = m.group(2).strip().rstrip(".")
        if _RESERVED_PAT.search(sec_head):
            continue
        start = m.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        body_raw = text[start:end]
        body_clean = _strip_metadata(body_raw)
        # Trim trailing breadcrumb / amendment-history block. OH section bodies
        # end with the substantive law text; the next "View Latest Amended
        # Version" / "Last updated" markers are not part of it.
        for trail in ("\nView ", "\nLast updated"):
            idx = body_clean.find(trail)
            if idx > 0:
                body_clean = body_clean[:idx].strip()
                break
        if len(body_clean) < 30:
            continue
        sections.append(Section(
            title_num=title_num,
            chapter_num=chapter_num,
            chapter_name=chap_name,
            section_num=sec_num,
            section_title=f"§ {sec_num}. {sec_head}",
            raw_text=body_clean,
            source_url=f"{OH_BASE}/ohio-revised-code/section-{sec_num}",
        ))
    return chap_name, sections


# ---------------------------------------------------------------------------
# Chunk emission
# ---------------------------------------------------------------------------

def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def _point_id(act_id: str, chunk_idx: int, text: str) -> str:
    seed = f"{act_id}::{chunk_idx}::{_sha1(text)[:12]}"
    return str(uuid.UUID(hashlib.md5(seed.encode()).hexdigest()))


def to_chunk_record(sec: Section) -> dict:
    act_id = f"STATE_OH_T{sec.title_num}_C{sec.chapter_num}_S{sec.section_num}"
    text = sec.raw_text
    citation = f"Ohio Rev. Code § {sec.section_num}"
    text_for_embedding = (
        f"Statute: Ohio Revised Code | US | Ohio | In Force\n"
        f"Title {sec.title_num} / Chapter {sec.chapter_num}: {sec.chapter_name}\n"
        f"{sec.section_title}\n\n{text}"
    )
    md = {
        "act_id": act_id,
        "corpus_type": "state",
        "category": "state_statute",
        "document_type": "statute",
        "jurisdiction": "US",
        "country_code": "US",
        "state": "oh",
        "title_number": int(sec.title_num) if sec.title_num.isdigit() else None,
        "title_name": f"Title {sec.title_num}",
        "title": "Ohio Revised Code",
        "title_code": None,
        "top_level_title": sec.title_num,
        "chapter": sec.chapter_num,
        "chapter_name": sec.chapter_name,
        "section_number": sec.section_num,
        "section_title": sec.section_title,
        "year": 2026,
        "act_status": "in_force",
        "renumbered_to": "",
        "transferred_to": "",
        "level_classifier": "section",
        "citation": citation,
        "citation_short": citation,
        "display_label": citation,
        "display_title": sec.section_title,
        "display_path": (
            f"Ohio Revised Code / Title {sec.title_num} / "
            f"Chapter {sec.chapter_num} / § {sec.section_num}"
        ),
        "breadcrumb": [
            {"type": "title", "num": sec.title_num,
             "label": f"Title {sec.title_num}", "name": f"Title {sec.title_num}"},
            {"type": "chapter", "num": sec.chapter_num,
             "label": f"Chapter {sec.chapter_num}", "name": sec.chapter_name},
            {"type": "section", "num": sec.section_num,
             "label": f"§ {sec.section_num}", "name": ""},
        ],
        "sort_key": act_id,
        "word_count": len(text.split()),
        "subsection_count": 0,
        "subsection_letters": [],
        "numbered_paragraph_count": 0,
        "cross_references_count": 0,
        "cross_references_usc": [],
        "cross_references_cfr": [],
        "amendment_years": [],
        "amendments_count": 0,
        "last_amended_year": None,
        "public_laws_referenced": [],
        "public_laws_count": 0,
        "source_url": sec.source_url,
        "parent_id": f"us/oh/statutes/title={sec.title_num}/chapter={sec.chapter_num}",
        "raw_node_id": (
            f"us/oh/statutes/title={sec.title_num}/chapter={sec.chapter_num}/"
            f"section={sec.section_num}"
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

def process_chapter(title_num: str, chapter_num: str,
                    chapter_url: str) -> list[Section]:
    html = fetch(chapter_url)
    if not html:
        return []
    chap_name, sections = parse_chapter(html, title_num, chapter_num, chapter_url)
    if not sections:
        return []
    return sections


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--titles", default="",
                    help="Comma-separated title numbers (e.g. '1,2'). Default: all.")
    ap.add_argument("--workers", type=int, default=2,
                    help="Parallel chapter fetches. Keep low (1-3) to dodge 429s.")
    ap.add_argument("--delay", type=float, default=1.2,
                    help="Per-chapter sleep between fetches (seconds).")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    print(f"[OH] discovering titles from {OH_TOC}", flush=True)
    titles = list_titles()
    if args.titles:
        wanted = {t.strip() for t in args.titles.split(",") if t.strip()}
        titles = [(t, u) for t, u in titles if t in wanted]
    print(f"[OH] {len(titles)} titles: {[t for t, _ in titles[:10]]}{'...' if len(titles) > 10 else ''}", flush=True)

    # Phase 1: gather chapter URLs across all selected titles
    all_chapters: list[tuple[str, str, str]] = []
    for title_num, title_url in titles:
        chaps = list_chapters_in_title(title_num, title_url)
        print(f"  [T{title_num}] {len(chaps)} chapters", flush=True)
        for chap_num, chap_url in chaps:
            all_chapters.append((title_num, chap_num, chap_url))
        time.sleep(args.delay)

    print(f"\n[OH] {len(all_chapters)} chapters to fetch", flush=True)
    if args.dry_run:
        return 0

    # Phase 2: crawl chapters (limited parallelism + per-chapter delay)
    chunks: list[dict] = []
    t0 = time.time()
    if args.workers <= 1:
        for i, (t, c, u) in enumerate(all_chapters, 1):
            secs = process_chapter(t, c, u)
            chunks.extend(to_chunk_record(s) for s in secs)
            if i % 25 == 0 or i == len(all_chapters):
                rate = len(chunks) / max(time.time() - t0, 0.1)
                print(f"  ... {i:>4}/{len(all_chapters)} chapters, {len(chunks):>5} sections, "
                      f"{rate:.1f}/s", flush=True)
            time.sleep(args.delay)
    else:
        # Bounded parallelism
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            futures = {
                ex.submit(process_chapter, t, c, u): (t, c)
                for t, c, u in all_chapters
            }
            done = 0
            for fut in as_completed(futures):
                done += 1
                try:
                    secs = fut.result()
                    chunks.extend(to_chunk_record(s) for s in secs)
                except Exception as e:
                    print(f"  ! chapter failed: {e}", flush=True)
                if done % 25 == 0 or done == len(all_chapters):
                    rate = len(chunks) / max(time.time() - t0, 0.1)
                    print(f"  ... {done:>4}/{len(all_chapters)} chapters, "
                          f"{len(chunks):>5} sections, {rate:.1f}/s", flush=True)
                time.sleep(args.delay / args.workers)

    # Dedup + write JSONL
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

    elapsed = time.time() - t0
    print(
        f"\n=== Done: parsed={len(chunks):,}, new={written:,}, "
        f"dupes={len(chunks) - written:,}, elapsed={elapsed:.1f}s ===",
        flush=True,
    )
    print(f"JSONL: {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
