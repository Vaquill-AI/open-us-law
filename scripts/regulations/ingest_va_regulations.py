#!/usr/bin/env python3
"""Ingest the Virginia Administrative Code (VAC) — Virginia's state regulations.

OFFICIAL SOURCE ONLY: law.lis.virginia.gov. Virginia's Division of Legislative
Automated Systems (DLAS) exposes a public RESTful JSON web service (linked from
https://law.lis.virginia.gov/developers via /jsonapi/), so this scraper hits the
structured API directly instead of parsing HTML - far faster and lossless.

API base: https://law.lis.virginia.gov/api
    AdministrativeCodeGetTitleListOfJson
        -> list of {TitleNumber, TitleName}
    AdministrativeCodeGetAgencyListOfJson/{title}
        -> {TitleNumber, TitleName, AgencyList:[{AgencyNumber, AgencyName}]}
    AdministrativeCodeChapterListOfJson/{title}/{agency}
        -> nested ...AgencyList[].ChapterList:[{ChapterNumber, ChapterName}]
    AdministrativeCodeGetSectionListOfJson/{title}/{agency}/{chapter}
        -> nested ...Sections:[{SectionNumber, SectionTitle, ...}]
    AdministrativeCodeGetSectionDetailsJson/{title}/{agency}/{chapter}/{section}/{point}/{colon}
        -> nested ...Sections:[{Body, Authority, HistoricalNote, Part*, Article*}]
        (pass 0 for {point} and {colon} when the section number has neither)

VAC hierarchy: Title -> Agency -> Chapter -> Section. Citation form like
"23VAC10-20-30" = Title 23, Agency 10, Chapter 20, Section 30. Official short
form is the compact "23VAC10-20-30"; Bluebook form is
"23 Va. Admin. Code § 10-20-30". corpus_type='state_regulation'.

Each section detail carries rich metadata captured into structured fields:
    Authority      -> statutory_authority (enabling Code of Virginia sections)
    HistoricalNote -> history; parsed into effective_date + prior_effective_dates
                      ("Derived from VR..., eff. <date>; amended, Virginia
                      Register Volume NN, Issue NN, eff. <date>")

Geo-restricted; Webshare US proxy + Mozilla UA + polite pacing.
"""

from __future__ import annotations

import argparse
import hashlib
import html as _html
import json
import os
import re
import sys
import time
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
OUT = DATA_DIR / "state_va_regulations.jsonl"

VA_BASE = "https://law.lis.virginia.gov"
VA_API = f"{VA_BASE}/api"

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
    import urllib.parse

    proxy_user = f"{user}-US-rotate"
    host = os.environ.get("WEBSHARE_PROXY_HOST", "p.webshare.io")
    port = os.environ.get("WEBSHARE_PROXY_PORT", "80")
    url = f"http://{urllib.parse.quote(proxy_user)}:{urllib.parse.quote(pwd)}@{host}:{port}"
    return {"http": url, "https": url}


SESSION = requests.Session()
SESSION.headers.update({"User-Agent": _MOZ_UA})


def fetch(url: str, retries: int = 6) -> Optional[str]:
    """Fetch raw text. Retries 429 with exponential backoff and tolerates the
    transient SSL/connection errors that rotating proxies occasionally raise."""
    proxies = _us_proxies()
    for attempt in range(retries):
        try:
            r = SESSION.get(url, timeout=60, proxies=proxies, allow_redirects=True)
            if r.status_code == 200:
                return r.text
            if r.status_code == 429:
                time.sleep(2**attempt)
                continue
            if r.status_code in (502, 503, 504):
                time.sleep(2)
                continue
            return None
        except Exception:
            time.sleep(1.5)
    return None


def fetch_json(url: str, retries: int = 6):
    txt = fetch(url, retries=retries)
    if txt is None:
        return None
    try:
        return json.loads(txt)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Discovery: titles -> agencies -> chapters -> section stubs
# ---------------------------------------------------------------------------


def _find_key(obj, key: str):
    """Depth-first search for the first non-null value under `key` in the
    nested {AgencyList:[{ChapterList:[{Sections:[...]}]}]} responses."""
    if isinstance(obj, dict):
        if obj.get(key):
            return obj[key]
        for v in obj.values():
            r = _find_key(v, key)
            if r:
                return r
    elif isinstance(obj, list):
        for v in obj:
            r = _find_key(v, key)
            if r:
                return r
    return None


def list_titles() -> list[tuple[str, str]]:
    data = fetch_json(f"{VA_API}/AdministrativeCodeGetTitleListOfJson")
    if not isinstance(data, list):
        raise RuntimeError("could not fetch VAC title list")
    out: list[tuple[str, str]] = []
    for t in data:
        num = str(t.get("TitleNumber", "")).strip()
        name = str(t.get("TitleName", "")).strip()
        if num:
            out.append((num, name))
    return sorted(out, key=lambda x: int(x[0]))


def list_agencies(title: str) -> list[tuple[str, str]]:
    data = fetch_json(f"{VA_API}/AdministrativeCodeGetAgencyListOfJson/{title}")
    agencies = _find_key(data, "AgencyList") if data else None
    out: list[tuple[str, str]] = []
    for a in agencies or []:
        num = str(a.get("AgencyNumber", "")).strip()
        name = str(a.get("AgencyName", "")).strip()
        if num:
            out.append((num, name))
    return out


def list_chapters(title: str, agency: str) -> list[tuple[str, str]]:
    data = fetch_json(f"{VA_API}/AdministrativeCodeChapterListOfJson/{title}/{agency}")
    chapters = _find_key(data, "ChapterList") if data else None
    out: list[tuple[str, str]] = []
    for c in chapters or []:
        num = str(c.get("ChapterNumber", "")).strip()
        name = str(c.get("ChapterName", "")).strip()
        # The "Preface" pseudo-chapter is an agency summary, not a regulation.
        if num and num.lower() != "preface":
            out.append((num, name))
    return out


def list_section_stubs(title: str, agency: str, chapter: str) -> list[dict]:
    data = fetch_json(f"{VA_API}/AdministrativeCodeGetSectionListOfJson/{title}/{agency}/{chapter}")
    sections = _find_key(data, "Sections") if data else None
    return list(sections or [])


# ---------------------------------------------------------------------------
# Section detail + parsing
# ---------------------------------------------------------------------------

_RESERVED_PAT = re.compile(r"\[(repealed|reserved|renumbered|expired)\b", re.IGNORECASE)
# A VAC section number may carry a decimal point and/or a colon division suffix,
# e.g. "30.1" or "40:1". The detail endpoint takes them as separate path parts.
_SEC_PARTS_RE = re.compile(r"^(\d+)(?:\.(\d+))?(?::(\d+))?$")

_MONTHS = "January|February|March|April|May|June|July|August|September|October|November|December"
# "eff. January 1, 1985" or "eff. March 4, 2009"
_EFF_RE = re.compile(rf"eff\.\s+((?:{_MONTHS})\s+\d{{1,2}},\s+\d{{4}})", re.IGNORECASE)
# "Virginia Register Volume 25, Issue 11"
_REGISTER_RE = re.compile(r"Volume\s+\d+,\s+Issue\s+\d+", re.IGNORECASE)
# "Derived from VR630-1-8" or "Derived from VR123-45-67"
_DERIVED_RE = re.compile(r"Derived from\s+([A-Za-z0-9.\-]+)", re.IGNORECASE)


def _html_to_text(raw: str) -> str:
    """Convert a section Body's HTML to clean plain text, preserving paragraph
    breaks (the API wraps each numbered/lettered paragraph in its own <p>)."""
    if not raw:
        return ""
    soup = BeautifulSoup(raw, "html.parser")
    parts: list[str] = []
    blocks = soup.find_all(["p", "div", "li", "tr"])
    if blocks:
        for b in blocks:
            t = b.get_text(" ", strip=True)
            if t:
                parts.append(t)
        text = "\n".join(parts)
    else:
        text = soup.get_text(" ", strip=True)
    text = _html.unescape(text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _strip_tags(raw: str) -> str:
    """Flatten an HTML fragment (Authority/HistoricalNote) to plain text."""
    if not raw:
        return ""
    txt = BeautifulSoup(raw, "html.parser").get_text(" ", strip=True)
    txt = _html.unescape(txt)
    return re.sub(r"\s+", " ", txt).strip()


def _parse_history(hist: str) -> dict:
    """Pull structured fields out of a VAC HistoricalNote, e.g.:
    'Derived from VR630-1-8, eff. January 1, 1985 ...; amended, Virginia
     Register Volume 25, Issue 11, eff. March 4, 2009.'"""
    effs: list[str] = []
    seen_eff: set[str] = set()
    for m in _EFF_RE.findall(hist):
        k = m.strip()
        if k and k not in seen_eff:
            seen_eff.add(k)
            effs.append(k)
    registers: list[str] = []
    seen_reg: set[str] = set()
    for m in _REGISTER_RE.findall(hist):
        k = re.sub(r"\s+", " ", m).strip()
        if k and k not in seen_reg:
            seen_reg.add(k)
            registers.append(k)
    derived = _DERIVED_RE.search(hist)
    return {
        # Latest effective date is the section's current effective date.
        "effective_date": effs[-1] if effs else "",
        "prior_effective_dates": "; ".join(effs[:-1]) if len(effs) > 1 else "",
        "register_citations": registers,
        "derived_from": derived.group(1).rstrip(".,") if derived else "",
    }


@dataclass
class Section:
    title_num: str
    title_name: str
    agency_num: str
    agency_name: str
    chapter_num: str
    chapter_name: str
    section_num: str  # canonical, e.g. "30", "30.1", "40:1"
    section_point: str  # decimal-point part for the detail endpoint
    section_colon: str  # colon-division part for the detail endpoint
    section_title: str
    raw_text: str  # cleaned body text
    source_url: str
    part_number: str = ""
    part_name: str = ""
    article_number: str = ""
    article_name: str = ""
    statutory_authority: str = ""  # enabling Code of Virginia section(s)
    history: str = ""  # full HistoricalNote text
    effective_date: str = ""  # latest "eff." date
    prior_effective_dates: str = ""  # earlier "eff." dates
    register_citations: list[str] = field(default_factory=list)
    derived_from: str = ""  # original VR-number this rule derives from


def _vac_section_label(s: Section) -> str:
    """Compact official VAC section number, e.g. '20-30', '20-30.1', '20-40:1'."""
    num = s.section_num
    return f"{s.chapter_num}-{num}"


def _detail_url(title: str, agency: str, chapter: str, section: str, point: str, colon: str) -> str:
    p = point or "0"
    c = colon or "0"
    return (
        f"{VA_API}/AdministrativeCodeGetSectionDetailsJson/"
        f"{title}/{agency}/{chapter}/{section}/{p}/{c}"
    )


def fetch_section(
    title_num: str,
    title_name: str,
    agency_num: str,
    agency_name: str,
    chapter_num: str,
    chapter_name: str,
    stub: dict,
) -> Optional[Section]:
    raw_num = str(stub.get("SectionNumber", "")).strip()
    stub_title = str(stub.get("SectionTitle", "")).strip()
    if not raw_num:
        return None
    if _RESERVED_PAT.search(stub_title):
        return None
    m = _SEC_PARTS_RE.match(raw_num)
    base = m.group(1) if m else raw_num
    point = (m.group(2) if m else "") or ""
    colon = (m.group(3) if m else "") or ""

    url = _detail_url(title_num, agency_num, chapter_num, base, point, colon)
    data = fetch_json(url)
    sections = _find_key(data, "Sections") if data else None
    detail = sections[0] if sections else {}

    body_html = detail.get("Body") or ""
    body = _html_to_text(body_html)
    title_txt = str(detail.get("SectionTitle") or stub_title).strip().rstrip(".")
    authority = _strip_tags(detail.get("Authority") or "")
    history = _strip_tags(detail.get("HistoricalNote") or "")
    parsed = _parse_history(history)

    if _RESERVED_PAT.search(title_txt) or len(body) < 30:
        return None

    canonical = raw_num
    section_url = (
        f"{VA_BASE}/admincode/title{title_num}/agency{agency_num}/"
        f"chapter{chapter_num}/section{canonical}/"
    )
    return Section(
        title_num=title_num,
        title_name=title_name,
        agency_num=agency_num,
        agency_name=agency_name,
        chapter_num=chapter_num,
        chapter_name=chapter_name,
        section_num=canonical,
        section_point=point,
        section_colon=colon,
        section_title=title_txt,
        raw_text=body,
        source_url=section_url,
        part_number=str(detail.get("PartNumber") or "").strip(),
        part_name=str(detail.get("PartName") or "").strip(),
        article_number=str(detail.get("ArticleNumber") or "").strip(),
        article_name=str(detail.get("ArticleName") or "").strip(),
        statutory_authority=authority,
        history=history,
        effective_date=parsed["effective_date"],
        prior_effective_dates=parsed["prior_effective_dates"],
        register_citations=parsed["register_citations"],
        derived_from=parsed["derived_from"],
    )


# ---------------------------------------------------------------------------
# Chunk emission
# ---------------------------------------------------------------------------


def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def _point_id(act_id: str, idx: int, text: str) -> str:
    seed = f"{act_id}::{idx}::{_sha1(text)[:12]}"
    return str(uuid.UUID(hashlib.md5(seed.encode()).hexdigest()))


def _safe(s: str) -> str:
    return re.sub(r"[^0-9A-Za-z]+", "_", s).strip("_")


def to_chunk_record(s: Section) -> dict:
    sec_label = _vac_section_label(s)  # e.g. "20-30"
    # Official compact VAC citation, e.g. "23VAC10-20-30".
    citation_short = f"{s.title_num}VAC{s.agency_num}-{sec_label}"
    # Bluebook form, e.g. "23 Va. Admin. Code § 10-20-30".
    citation = f"{s.title_num} Va. Admin. Code § {s.agency_num}-{sec_label}"
    act_id = (
        f"STATE_VA_ADC_{_safe(s.title_num)}_{_safe(s.agency_num)}_"
        f"{_safe(s.chapter_num)}_{_safe(s.section_num)}"
    )
    text = s.raw_text

    # Rich, searchable embed header: surface the regulation-specific metadata
    # (effective date, enabling statute, history) so it is retrievable.
    meta_lines = []
    if s.effective_date:
        meta_lines.append(f"Effective: {s.effective_date}")
    if s.statutory_authority:
        meta_lines.append(f"Statutory Authority: {s.statutory_authority}")
    if s.history:
        meta_lines.append(f"History: {s.history}")
    meta_header = ("\n" + "\n".join(meta_lines)) if meta_lines else ""
    part_line = ""
    if s.part_name or s.article_name:
        bits = []
        if s.part_name:
            bits.append(f"Part {s.part_number}: {s.part_name}".strip())
        if s.article_name:
            bits.append(f"Article {s.article_number}: {s.article_name}".strip())
        part_line = "\n" + " / ".join(bits)
    text_for_embedding = (
        f"Regulation: Virginia Administrative Code | US | Virginia | In Force\n"
        f"Title {s.title_num} ({s.title_name}) / Agency {s.agency_num}: "
        f"{s.agency_name} / Chapter {s.chapter_num}: {s.chapter_name}{part_line}\n"
        f"{citation_short}. {s.section_title}{meta_header}\n\n{text}"
    )

    display_title = f"{citation_short}. {s.section_title}"
    breadcrumb = [
        {
            "type": "title",
            "num": s.title_num,
            "label": f"Title {s.title_num}",
            "name": s.title_name,
        },
        {
            "type": "agency",
            "num": s.agency_num,
            "label": f"Agency {s.agency_num}",
            "name": s.agency_name,
        },
        {
            "type": "chapter",
            "num": s.chapter_num,
            "label": f"Chapter {s.chapter_num}",
            "name": s.chapter_name,
        },
        {"type": "section", "num": s.section_num, "label": citation_short, "name": s.section_title},
    ]

    md = {
        "act_id": act_id,
        "corpus_type": "state_regulation",
        "category": "state_regulation",
        "document_type": "regulation",
        "jurisdiction": "US",
        "country_code": "US",
        "state": "va",
        "title_number": s.title_num,
        "title_name": f"Title {s.title_num} — {s.title_name}",
        "title": "Virginia Administrative Code",
        "title_code": f"vac_title_{s.title_num}",
        "top_level_title": s.title_num,
        "chapter": s.chapter_num,
        "chapter_name": s.chapter_name,
        "section_number": citation_short,
        "section_title": display_title,
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
        "register_citations": s.register_citations,
        "derived_from": s.derived_from or None,
        "issuing_agency": s.agency_name,
        "issuing_agency_code": s.agency_num,
        "part_number": s.part_number or None,
        "part_name": s.part_name or None,
        "article_number": s.article_number or None,
        "article_name": s.article_name or None,
        "citation": citation,
        "citation_short": citation_short,
        "display_label": citation_short,
        "display_title": display_title,
        "display_path": (
            f"Virginia Administrative Code / Title {s.title_num} {s.title_name} / "
            f"Agency {s.agency_num} {s.agency_name} / Chapter {s.chapter_num} / "
            f"{citation_short}"
        ),
        "breadcrumb": breadcrumb,
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
        "source_url": s.source_url,
        "parent_id": (
            f"us/va/regulations/title={s.title_num}/agency={s.agency_num}/chapter={s.chapter_num}"
        ),
        "raw_node_id": (
            f"us/va/regulations/title={s.title_num}/agency={s.agency_num}/"
            f"chapter={s.chapter_num}/section={s.section_num}"
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


@dataclass
class ChapterRef:
    title_num: str
    title_name: str
    agency_num: str
    agency_name: str
    chapter_num: str
    chapter_name: str


def process_chapter(ref: ChapterRef, delay: float) -> list[Section]:
    stubs = list_section_stubs(ref.title_num, ref.agency_num, ref.chapter_num)
    if not stubs:
        return []
    out: list[Section] = []
    for stub in stubs:
        sec = fetch_section(
            ref.title_num,
            ref.title_name,
            ref.agency_num,
            ref.agency_name,
            ref.chapter_num,
            ref.chapter_name,
            stub,
        )
        if sec is None:
            continue
        out.append(sec)
        if delay:
            time.sleep(delay)
    return out


def discover_chapters(titles: list[str], delay: float) -> list[ChapterRef]:
    all_titles = list_titles()
    if titles:
        wanted = {t.strip() for t in titles if t.strip()}
        all_titles = [(n, nm) for (n, nm) in all_titles if n in wanted]
    print(f"[VAC] {len(all_titles)} titles", flush=True)

    refs: list[ChapterRef] = []
    for tnum, tname in all_titles:
        agencies = list_agencies(tnum)
        for anum, aname in agencies:
            chapters = list_chapters(tnum, anum)
            for cnum, cname in chapters:
                refs.append(ChapterRef(tnum, tname, anum, aname, cnum, cname))
            time.sleep(delay)
        print(
            f"  [title {tnum} {tname}] {len(agencies)} agencies, running chapter total {len(refs)}",
            flush=True,
        )
    return refs


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--titles", default="", help="Comma-separated title numbers (e.g. '23,18'). Default: all."
    )
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument(
        "--delay", type=float, default=0.3, help="Per-section delay inside a chapter worker."
    )
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    print(f"[VAC] discovering structure from {VA_API}", flush=True)
    titles = [t for t in args.titles.split(",")] if args.titles else []
    refs = discover_chapters(titles, args.delay)
    print(f"\n[VAC] {len(refs)} chapters to fetch", flush=True)
    if args.dry_run:
        return 0

    chunks: list[dict] = []
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {ex.submit(process_chapter, ref, args.delay): ref for ref in refs}
        done = 0
        for fut in as_completed(futures):
            done += 1
            try:
                secs = fut.result()
                chunks.extend(to_chunk_record(x) for x in secs)
            except Exception as e:
                print(f"  ! chapter failed: {e}", flush=True)
            if done % 25 == 0 or done == len(refs):
                rate = len(chunks) / max(time.time() - t0, 0.1)
                print(
                    f"  ... {done:>5}/{len(refs)} chapters, "
                    f"{len(chunks):>6} sections, {rate:.1f}/s",
                    flush=True,
                )

    # Dedup + write
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

    print(
        f"\n=== Done: parsed={len(chunks):,}, new={written:,}, elapsed={time.time() - t0:.1f}s ===",
        flush=True,
    )
    print(f"JSONL: {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
