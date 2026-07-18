#!/usr/bin/env python3
"""Ingest the New Mexico Administrative Code (NMAC) — NM's state regulations.

OFFICIAL SOURCE ONLY: the New Mexico State Records & Archives Commission's
NMAC home at https://www.srca.nm.gov/nmac-home/ . No aggregators (no Justia /
Casetext / ZenRows / etc.).

The SRCA web app exposes the NMAC as a hierarchy of Title -> Chapter -> Part ->
Section, with the citable unit being the *Section* (cite form: "T.C.P.S NMAC",
e.g. "20.11.42.1 NMAC"). The chapter and Part landing pages on srca.nm.gov are
informational only — they list reserved/vacant slots but DO NOT enumerate live
Parts. The actual rule text lives as one PDF per Part at the deterministic URL

    https://www.srca.nm.gov/parts/title{TT}/{TT}.{CCC}.{PPPP}.pdf

(title zero-padded to 2, chapter to 3, part to 4 digits). The 404 page is a
~120 KB HTML body; real PDFs return Content-Type application/pdf.

Discovery strategy (since SRCA never enumerates live Parts in HTML):
    1. Pull the *Yoast* sitemap and gather all NMAC Title page URLs.
    2. Pull every Cumulative Index / annual History-of-NMAC-Updates PDF
       linked from https://www.srca.nm.gov/nmac/history-of-nmac-updates and
       https://www.srca.nm.gov/nmac-home/new-mexico-register/cumulative-index/
       (yearly indexes 2001-current). The indexes list every Part that has
       had an action (new / amendment / repeal) in that year as a literal
       "T.C.P NMAC" reference; their union is the master list of every
       Part that has ever existed in the code.
    3. (Optional) Brute-probe a bounded keyspace via 4-byte Range requests
       to catch any never-amended Part not in the indexes.

Per-Part parsing follows the NMAC "Anatomy of a Rule" spec
(https://www.srca.nm.gov/nmac-home/anatomy-of-a-rule/):

    TITLE T NAME
    CHAPTER C NAME
    PART P NAME
    T.C.P.1 ISSUING AGENCY: ...
    [T.C.P.1 NMAC - N, mm/dd/yyyy]      <- per-section history note
    T.C.P.2 SCOPE: ...
    ...
    T.C.P.S <ALL CAPS TITLE>: <body>
    [T.C.P.S NMAC - <action>, <previous>, mm/dd/yyyy]
    ...
    HISTORY OF T.C.P NMAC:
        <pre-NMAC history>
        History of Repealed Material: ...
        Other History: ...

Section 3 ("STATUTORY AUTHORITY:") names the enabling Section(s) of NMSA;
this becomes statutory_authority for every section in the Part. The
bracketed history note after each section is parsed into effective_date
(last mm/dd/yyyy in the bracket) plus the action code (N = new, A = amend,
Rp = repealed-and-replaced, Rn = renumbered). The PDF tail block "HISTORY
OF <part> NMAC:" feeds prior_effective_dates.

corpus_type='state_regulation', state='nm'. One chunk per Section.

Geo-restricted; Webshare US proxy + Mozilla UA + polite pacing.
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
from dataclasses import dataclass, field
from pathlib import Path

import requests

sys.stdout.reconfigure(line_buffering=True)  # type: ignore[attr-defined]

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = Path(os.environ.get("OUT_DIR", "./data"))
OUT = DATA_DIR / "state_nm_regulations.jsonl"

NM_BASE = "https://www.srca.nm.gov"
NM_HOME = f"{NM_BASE}/nmac-home/"
NM_TITLES = f"{NM_BASE}/nmac-home/nmac-titles/"
NM_HISTORY = f"{NM_BASE}/nmac/history-of-nmac-updates"
NM_CUM_INDEX = f"{NM_BASE}/nmac-home/new-mexico-register/cumulative-index/"
NM_SITEMAPS = [
    f"{NM_BASE}/page-sitemap.xml",
    f"{NM_BASE}/page-sitemap2.xml",
]

_MOZ_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
)


def _nm_amended_year(effective_date: str | None) -> int | None:
    """Year from a "MM/DD/YYYY" effective date, clamped to a plausible window.

    Malformed or forward-dated effective dates would otherwise produce a
    garbage or future "last amended" year.
    """
    if not effective_date:
        return None
    tail = effective_date.split("/")[-1].strip()
    if not tail.isdigit():
        return None
    year = int(tail)
    return year if 1789 <= year <= 2026 else None


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


def _us_proxies() -> dict | None:
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


def fetch(url: str, retries: int = 5) -> str | None:
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
            time.sleep(2)
    return None


def fetch_bytes(url: str, retries: int = 5) -> bytes | None:
    proxies = _us_proxies()
    for attempt in range(retries):
        try:
            r = SESSION.get(url, timeout=120, proxies=proxies, allow_redirects=True)
            if r.status_code == 200:
                # On 404 the server returns a 120 KB HTML body, so verify the
                # response is actually a PDF (real Parts are application/pdf).
                ct = (r.headers.get("Content-Type") or "").lower()
                if "pdf" in ct or r.content[:4] == b"%PDF":
                    return r.content
                return None
            if r.status_code == 429:
                time.sleep(2**attempt)
                continue
            if r.status_code in (502, 503, 504):
                time.sleep(2)
                continue
            return None
        except Exception:
            time.sleep(2)
    return None


def probe_exists(url: str) -> bool:
    """Cheap existence check via a 4-byte Range request.

    The SRCA server honors Range, so real Parts return 206 + 4 bytes of PDF
    magic ("%PDF"). Non-existent Parts return the ~120 KB HTML 404 page with
    status 200 (broken server config). We verify both content-type and the
    PDF magic to avoid false positives.
    """
    proxies = _us_proxies()
    try:
        r = SESSION.get(
            url,
            timeout=30,
            proxies=proxies,
            headers={"Range": "bytes=0-3", "User-Agent": _MOZ_UA},
            allow_redirects=True,
        )
        if r.status_code not in (200, 206):
            return False
        ct = (r.headers.get("Content-Type") or "").lower()
        if "pdf" in ct:
            return True
        return r.content[:4] == b"%PDF"
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Discovery: union of all "T.C.P NMAC" references across NMAC year/cumulative
# Index PDFs gives the master list of live Parts.
# ---------------------------------------------------------------------------

# Title page slug pattern in sitemap: /nmac-titles/title-{N}-{slug}/
_TITLE_RE = re.compile(r"/nmac-titles/title-(\d+)-([^/]+)/$")
# Part triple inside Index PDFs (and elsewhere). Note: title 1-24, chapter
# 1-999, part 1-9999 in practice.
_TCP_RE = re.compile(r"\b(\d{1,2})\.(\d{1,3})\.(\d{1,4})\s+NMAC\b")
# Per-section header at start of line inside a Part PDF.
_SEC_RE = re.compile(
    r"(?m)^(\d{1,2})\.(\d{1,3})\.(\d{1,4})\.(\d{1,4})\s+"
    r"([A-Z][A-Z &/\-\(\)0-9.,'’\"]+?):\s",
)
# Bracketed history note that follows each section. Last mm/dd/yyyy wins.
_HIST_RE = re.compile(r"\[([^\[\]]*?NMAC[^\[\]]*?)\]")
_DATE_RE = re.compile(r"(\d{1,2})/(\d{1,2})/(\d{2,4})")
# Tail-of-PDF "HISTORY OF T.C.P NMAC:" block.
_TAIL_HIST_RE = re.compile(
    r"HISTORY\s+OF\s+\d{1,2}\.\d{1,3}\.\d{1,4}\s+NMAC\s*:(.*)$",
    re.IGNORECASE | re.DOTALL,
)


def _collect_titles_from_sitemap() -> list[tuple[str, str, str]]:
    """Return [(title_num, slug, title_url), ...] for all NMAC Titles."""
    titles: list[tuple[str, str, str]] = []
    seen: set[str] = set()
    for sm in NM_SITEMAPS:
        body = fetch(sm) or ""
        for loc in re.findall(r"<loc>([^<]+)</loc>", body):
            m = _TITLE_RE.search(loc)
            if not m:
                continue
            tnum, slug = m.group(1), m.group(2)
            if tnum in seen:
                continue
            seen.add(tnum)
            titles.append((tnum, slug, loc))
    titles.sort(key=lambda x: int(x[0]))
    return titles


def _title_name_from_slug(slug: str) -> str:
    # "general-government-administration" -> "General Government Administration"
    return " ".join(p.capitalize() for p in slug.split("-"))


def _collect_year_index_pdfs() -> list[str]:
    """Gather every NMAC year-index / cumulative-index PDF URL from the two
    official index pages, plus the per-year history-of-nmac-updates child
    pages they link to (those carry older monthly Update PDFs).
    """
    urls: set[str] = set()
    pages: list[str] = [NM_HISTORY, NM_CUM_INDEX]
    # The yearly history-of-nmac-updates-YYYY pages are linked from NM_HISTORY.
    hist_html = fetch(NM_HISTORY) or ""
    for href in re.findall(r'href="([^"]+)"', hist_html):
        if "/history-of-nmac-updates-" in href:
            full = href if href.startswith("http") else NM_BASE + href
            pages.append(full)

    for page in pages:
        html = fetch(page) or ""
        for href in re.findall(r'href="([^"]+)"', html):
            if not href.lower().endswith(".pdf"):
                continue
            # Anchor to NMAC index/update PDFs only.
            hl = href.lower()
            if not any(k in hl for k in ("index", "update", "supp", "cumulative")):
                continue
            full = href if href.startswith("http") else NM_BASE + href
            urls.add(full)
    return sorted(urls)


def _pdf_text(pdf_bytes: bytes) -> str:
    # pymupdf (fitz) — 5-10x faster + doesn't leak page trees like pdfplumber
    # at scale (the pdfplumber leak took RSS to ~5.8 GB on this scraper).
    import fitz  # pymupdf

    parts: list[str] = []
    with fitz.open(stream=pdf_bytes, filetype="pdf") as pdf:
        for page in pdf:
            t = page.get_text() or ""
            if t.strip():
                parts.append(t)
    return "\n".join(parts)


def discover_part_triples_from_indexes() -> set[tuple[int, int, int]]:
    """Return the union set of (title, chapter, part) found in every NMAC
    Index PDF. Each Index PDF lists every Part touched in that year — across
    2001-current that union is effectively the master list of live Parts.
    """
    triples: set[tuple[int, int, int]] = set()
    idx_urls = _collect_year_index_pdfs()
    print(f"[NMAC] {len(idx_urls)} index PDFs to scan", flush=True)
    for u in idx_urls:
        body = fetch_bytes(u)
        if not body:
            print(f"  ! index fetch failed: {u}", flush=True)
            continue
        try:
            text = _pdf_text(body)
        except Exception as e:
            print(f"  ! index pdf parse err {u}: {e}", flush=True)
            continue
        new = 0
        for m in _TCP_RE.finditer(text):
            t, c, p = int(m.group(1)), int(m.group(2)), int(m.group(3))
            if 1 <= t <= 24 and 1 <= c <= 999 and 1 <= p <= 9999:
                if (t, c, p) not in triples:
                    triples.add((t, c, p))
                    new += 1
        print(
            f"  ... +{new:5d} new (running {len(triples):5d}) from {u.rsplit('/', 1)[-1]}",
            flush=True,
        )
    return triples


# ---------------------------------------------------------------------------
# Part PDF -> per-Section parsing
# ---------------------------------------------------------------------------


@dataclass
class PartInfo:
    title_num: int
    chapter_num: int
    part_num: int
    title_name: str  # "PUBLIC FINANCE"
    chapter_name: str  # "AUDITS OF GOVERNMENTAL ENTITIES"
    part_name: str  # "BUDGET CERTIFICATION OF LOCAL PUBLIC BODIES"
    issuing_agency: str  # from Section .1 ISSUING AGENCY body
    statutory_authority: str  # from Section .3 STATUTORY AUTHORITY body
    tail_history: str  # raw "HISTORY OF T.C.P NMAC:" block
    pdf_url: str


@dataclass
class SectionRec:
    title_num: int
    chapter_num: int
    part_num: int
    section_num: int
    section_title: str  # "ISSUING AGENCY"
    body: str
    history_note: str  # raw bracketed text
    action_code: str  # N / A / Rp / Rn / "" if unknown
    effective_date: str  # mm/dd/yyyy from last date in the history note
    prior_effective_dates: list[str] = field(default_factory=list)


def _clean_pdf_text(text: str) -> str:
    # Strip running headers/footers conservatively: lines that are just
    # "T.C.P NMAC <page>" or "<page> T.C.P NMAC".
    out: list[str] = []
    for ln in text.splitlines():
        s = ln.strip()
        if re.fullmatch(r"\d{1,2}\.\d{1,3}\.\d{1,4}\s+NMAC\s+\d+", s):
            continue
        if re.fullmatch(r"\d+\s+\d{1,2}\.\d{1,3}\.\d{1,4}\s+NMAC", s):
            continue
        out.append(ln)
    return "\n".join(out)


def _normalise(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def _extract_part_header(text: str) -> tuple[str, str, str]:
    """Return (title_name, chapter_name, part_name) from the PDF header block.

    Format:
        TITLE T <NAME>
        CHAPTER C <NAME>
        PART P <NAME>
    """
    tname = cname = pname = ""
    m_t = re.search(r"(?m)^TITLE\s+\d+\s+(.+?)$", text)
    if m_t:
        tname = m_t.group(1).strip()
    m_c = re.search(r"(?m)^CHAPTER\s+\d+\s+(.+?)$", text)
    if m_c:
        cname = m_c.group(1).strip()
    m_p = re.search(r"(?m)^PART\s+\d+\s+(.+?)$", text)
    if m_p:
        pname = m_p.group(1).strip()
    return tname, cname, pname


def _extract_tail_history(text: str) -> str:
    m = _TAIL_HIST_RE.search(text)
    if not m:
        return ""
    block = _normalise(m.group(1))
    # Trim a trailing pagination artifact (e.g. "2.2.3 NMAC 3").
    block = re.sub(r"\s+\d{1,2}\.\d{1,3}\.\d{1,4}\s+NMAC\s+\d+\s*$", "", block)
    return block[:4000]


def parse_part_pdf(
    text: str, title_num: int, chapter_num: int, part_num: int, pdf_url: str
) -> tuple[PartInfo | None, list[SectionRec]]:
    text = _clean_pdf_text(text)
    tname, cname, pname = _extract_part_header(text)

    # Find every section header inside this Part.
    matches = [
        m
        for m in _SEC_RE.finditer(text)
        if int(m.group(1)) == title_num
        and int(m.group(2)) == chapter_num
        and int(m.group(3)) == part_num
    ]
    if not matches:
        return None, []

    # Strip the "HISTORY OF T.C.P NMAC:" tail before per-section slicing so
    # we don't leak it into the final section's body.
    tail_history = _extract_tail_history(text)
    tail_idx = text.rfind("HISTORY OF")
    body_text = text[:tail_idx] if tail_idx > 0 else text

    matches = [m for m in matches if m.start() < (tail_idx if tail_idx > 0 else len(text))]

    sections: list[SectionRec] = []
    section_bodies: dict[int, str] = {}
    section_titles: dict[int, str] = {}
    section_histories: dict[int, str] = {}

    for i, m in enumerate(matches):
        sec_num = int(m.group(4))
        sec_title = _normalise(m.group(5)).rstrip(".:")
        start = m.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(body_text)
        region = body_text[start:end]

        # Pull the bracketed history note(s) from the end of the region.
        hist_match = list(_HIST_RE.finditer(region))
        history_raw = ""
        body_raw = region
        if hist_match:
            last = hist_match[-1]
            history_raw = last.group(1).strip()
            body_raw = region[: last.start()].rstrip()

        body_clean = _normalise(body_raw)
        # Skip sections whose body is empty or trivially short (e.g. a
        # placeholder "[RESERVED]" or a header that bleeds into the part's
        # tail-history block). The downstream embedder rejects empty raw_text.
        if len(body_clean) < 5:
            continue
        sections.append(
            SectionRec(
                title_num=title_num,
                chapter_num=chapter_num,
                part_num=part_num,
                section_num=sec_num,
                section_title=sec_title,
                body=body_clean,
                history_note=history_raw,
                action_code=_history_action(history_raw),
                effective_date=_history_last_date(history_raw),
                prior_effective_dates=_history_all_dates(history_raw)[:-1] if history_raw else [],
            )
        )
        section_bodies[sec_num] = body_clean
        section_titles[sec_num] = sec_title
        section_histories[sec_num] = history_raw

    # Section .1 = ISSUING AGENCY ; Section .3 = STATUTORY AUTHORITY (per NMAC
    # "Anatomy of a Rule" — the seven mandatory sections start every Part).
    issuing_agency = section_bodies.get(1, "")[:1500]
    statutory_authority = section_bodies.get(3, "")[:2000]

    part = PartInfo(
        title_num=title_num,
        chapter_num=chapter_num,
        part_num=part_num,
        title_name=tname,
        chapter_name=cname,
        part_name=pname,
        issuing_agency=issuing_agency,
        statutory_authority=statutory_authority,
        tail_history=tail_history,
        pdf_url=pdf_url,
    )
    return part, sections


def _history_last_date(hist: str) -> str:
    if not hist:
        return ""
    dates = _DATE_RE.findall(hist)
    if not dates:
        return ""
    mm, dd, yy = dates[-1]
    if len(yy) == 2:
        yy = ("20" if int(yy) < 50 else "19") + yy
    return f"{int(mm):02d}/{int(dd):02d}/{yy}"


def _history_all_dates(hist: str) -> list[str]:
    out: list[str] = []
    for mm, dd, yy in _DATE_RE.findall(hist):
        if len(yy) == 2:
            yy = ("20" if int(yy) < 50 else "19") + yy
        out.append(f"{int(mm):02d}/{int(dd):02d}/{yy}")
    return out


_ACTION_RE = re.compile(
    r"NMAC\s*-\s*(N(?:/E)?|A(?:/E)?|Rp|Rn|R|REC|RC)\b",
    re.IGNORECASE,
)


def _history_action(hist: str) -> str:
    if not hist:
        return ""
    m = _ACTION_RE.search(hist)
    if not m:
        return ""
    return m.group(1).upper()


# ---------------------------------------------------------------------------
# Chunk emission
# ---------------------------------------------------------------------------


def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def _point_id(act_id: str, idx: int, text: str) -> str:
    seed = f"{act_id}::{idx}::{_sha1(text)[:12]}"
    return str(uuid.UUID(hashlib.md5(seed.encode()).hexdigest()))


def _act_id(s: SectionRec) -> str:
    return f"SREGS_NM_T{s.title_num}_C{s.chapter_num}_P{s.part_num}_S{s.section_num}"


def _pdf_path(t: int, c: int, p: int) -> str:
    return f"parts/title{t:02d}/{t:02d}.{c:03d}.{p:04d}.pdf"


def pdf_url_for(t: int, c: int, p: int) -> str:
    return f"{NM_BASE}/{_pdf_path(t, c, p)}"


def to_chunk_record(part: PartInfo, s: SectionRec) -> dict:
    act_id = _act_id(s)
    cite_full = f"{s.title_num}.{s.chapter_num}.{s.part_num}.{s.section_num} NMAC"
    cite_part = f"{s.title_num}.{s.chapter_num}.{s.part_num} NMAC"
    section_label = f"Section {s.section_num}. {s.section_title.title()}"
    text = s.body

    meta_lines: list[str] = []
    if s.effective_date:
        meta_lines.append(f"Effective: {s.effective_date}")
    if part.statutory_authority:
        meta_lines.append(f"Statutory Authority: {part.statutory_authority}")
    if part.issuing_agency:
        meta_lines.append(f"Issuing Agency: {part.issuing_agency}")
    if s.history_note:
        meta_lines.append(f"History Note: [{s.history_note}]")
    meta_header = ("\n" + "\n".join(meta_lines)) if meta_lines else ""

    text_for_embedding = (
        f"Regulation: New Mexico Administrative Code (NMAC) | US | New Mexico | In Force\n"
        f"Title {s.title_num} ({part.title_name}) / Chapter {s.chapter_num} "
        f"({part.chapter_name}) / Part {s.part_num} ({part.part_name})\n"
        f"{cite_full} — {section_label}{meta_header}\n\n{text}"
    )

    md = {
        "act_id": act_id,
        "corpus_type": "state_regulation",
        "category": "state_regulation",
        "document_type": "regulation",
        "jurisdiction": "US",
        "country_code": "US",
        "state": "nm",
        "title_number": s.title_num,
        "title_name": f"NMAC Title {s.title_num} — {part.title_name}",
        "title": "New Mexico Administrative Code",
        "title_code": "regs_nm",
        "top_level_title": "regs-nm",
        "chapter": f"{s.title_num}.{s.chapter_num}",
        "chapter_name": part.chapter_name,
        "part_number": f"{s.title_num}.{s.chapter_num}.{s.part_num}",
        "part_name": part.part_name,
        "section_number": cite_full,
        "section_title": section_label,
        "year": int(s.effective_date.split("/")[-1]) if s.effective_date else 2026,
        "act_status": "in_force",
        "renumbered_to": "",
        "transferred_to": "",
        "level_classifier": "section",
        # --- regulation-specific rich metadata (captured, not discarded) ---
        "effective_date": s.effective_date or None,
        "promulgated_under": None,
        "statutory_authority": part.statutory_authority or None,
        "rule_amplifies": part.statutory_authority or None,
        "prior_effective_dates": (
            ", ".join(s.prior_effective_dates) if s.prior_effective_dates else None
        ),
        "review_date": None,
        "issuing_agency": part.issuing_agency or None,
        "issuing_agency_code": None,
        "history_note_raw": f"[{s.history_note}]" if s.history_note else None,
        "history_action": s.action_code or None,
        "part_history": part.tail_history or None,
        "last_amended_date": s.effective_date or None,
        "last_amended_year": _nm_amended_year(s.effective_date),
        "citation": cite_full,
        "citation_short": cite_full,
        "citation_part": cite_part,
        "display_label": cite_full,
        "display_title": section_label,
        "display_path": (
            f"NMAC / Title {s.title_num} ({part.title_name}) / "
            f"Chapter {s.chapter_num} ({part.chapter_name}) / "
            f"Part {s.part_num} ({part.part_name}) / Section {s.section_num}"
        ),
        "breadcrumb": [
            {
                "type": "title",
                "num": str(s.title_num),
                "label": f"Title {s.title_num}",
                "name": part.title_name,
            },
            {
                "type": "chapter",
                "num": str(s.chapter_num),
                "label": f"Chapter {s.chapter_num}",
                "name": part.chapter_name,
            },
            {
                "type": "part",
                "num": str(s.part_num),
                "label": f"Part {s.part_num}",
                "name": part.part_name,
            },
            {
                "type": "section",
                "num": str(s.section_num),
                "label": f"Section {s.section_num}",
                "name": s.section_title.title(),
            },
        ],
        "sort_key": (f"{s.title_num:02d}.{s.chapter_num:03d}.{s.part_num:04d}.{s.section_num:04d}"),
        "word_count": len(text.split()),
        "subsection_count": 0,
        "subsection_letters": [],
        "numbered_paragraph_count": 0,
        "cross_references_count": 0,
        "cross_references_usc": [],
        "cross_references_cfr": [],
        "amendment_years": [],
        "amendments_count": 0,
        "public_laws_referenced": [],
        "public_laws_count": 0,
        "source_url": part.pdf_url,
        "parent_id": (
            f"us/nm/regulations/title={s.title_num}/chapter={s.chapter_num}/part={s.part_num}"
        ),
        "raw_node_id": (
            f"us/nm/regulations/title={s.title_num}/chapter={s.chapter_num}/"
            f"part={s.part_num}/section={s.section_num}"
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


def process_part(triple: tuple[int, int, int]) -> tuple[PartInfo | None, list[SectionRec]]:
    t, c, p = triple
    url = pdf_url_for(t, c, p)
    pdf_bytes = fetch_bytes(url)
    if not pdf_bytes:
        return None, []

    try:
        text = _pdf_text(pdf_bytes)
    except Exception as e:
        print(f"  ! pdf parse err {t}.{c}.{p}: {e}", flush=True)
        return None, []

    part, sections = parse_part_pdf(text, t, c, p, url)
    if not part:
        return None, []

    return part, sections


def _brute_extend_triples(
    seed: set[tuple[int, int, int]], delay: float, workers: int
) -> set[tuple[int, int, int]]:
    """Optional: probe Part numbers adjacent to seeded ones in case a Part has
    never appeared in any annual Index (the very small population of rules
    that were filed before 2001 and never since amended). Bounded so we stay
    polite: for each (T,C) seen in the seed, probe parts 1..max(seen_p)+5.
    """
    by_tc: dict[tuple[int, int], int] = {}
    for t, c, p in seed:
        by_tc[(t, c)] = max(by_tc.get((t, c), 0), p)
    cands: list[tuple[int, int, int]] = []
    for (t, c), maxp in by_tc.items():
        for p in range(1, maxp + 6):
            if (t, c, p) in seed:
                continue
            cands.append((t, c, p))
    print(
        f"[NMAC] brute-extend probing {len(cands)} candidate parts "
        f"(adjacent to {len(seed)} seeded)",
        flush=True,
    )
    found: set[tuple[int, int, int]] = set()
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(probe_exists, pdf_url_for(*t)): t for t in cands}
        done = 0
        for fut in as_completed(futures):
            tcp = futures[fut]
            done += 1  # noqa: SIM113 (counter over as_completed, not a positional index)
            try:
                if fut.result():
                    found.add(tcp)
            except Exception:
                pass
            if done % 250 == 0:
                print(f"  ... probed {done}/{len(cands)}, new {len(found)}", flush=True)
            time.sleep(delay / max(workers, 1))
    return found


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--titles",
        default="",
        help="Comma-separated NMAC title numbers (e.g. '1,20'). Default: all.",
    )
    ap.add_argument(
        "--parts",
        default="",
        help=(
            "Comma-separated triples T.C.P (e.g. '20.11.42,2.2.3'). Overrides "
            "all discovery and processes only these."
        ),
    )
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--delay", type=float, default=0.6)
    ap.add_argument(
        "--brute-extend",
        action="store_true",
        help="After index discovery, probe a small adjacent keyspace for any "
        "never-amended Parts not captured by any Index PDF.",
    )
    ap.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Cap number of Parts processed (for testing).",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Discover Parts only; do not fetch PDFs or write JSONL.",
    )
    args = ap.parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if args.parts:
        triples: set[tuple[int, int, int]] = set()
        for s in args.parts.split(","):
            t, c, p = s.strip().split(".")
            triples.add((int(t), int(c), int(p)))
        print(f"[NMAC] explicit parts override: {len(triples)} parts", flush=True)
    else:
        print(
            f"[NMAC] discovering Part triples from NMAC Index PDFs at {NM_HISTORY}",
            flush=True,
        )
        triples = discover_part_triples_from_indexes()
        print(f"[NMAC] {len(triples)} unique Part triples from indexes", flush=True)
        if args.brute_extend:
            extra = _brute_extend_triples(triples, args.delay, args.workers)
            print(f"[NMAC] +{len(extra)} from brute-extend probe", flush=True)
            triples |= extra
        if args.titles:
            wanted = {int(x.strip()) for x in args.titles.split(",") if x.strip()}
            triples = {t for t in triples if t[0] in wanted}
            print(f"[NMAC] filtered to titles {sorted(wanted)}: {len(triples)} parts", flush=True)

    triples_list = sorted(triples)
    if args.limit:
        triples_list = triples_list[: args.limit]
    print(f"[NMAC] {len(triples_list)} parts to fetch", flush=True)

    if args.dry_run:
        for t in triples_list[:30]:
            print(f"  {t[0]}.{t[1]}.{t[2]}  {pdf_url_for(*t)}")
        if len(triples_list) > 30:
            print(f"  ... and {len(triples_list) - 30} more")
        return 0

    # Stream-write per section so the process doesn't buffer all chunks in
    # memory (the earlier accumulate pattern grew unbounded).
    seen: set[str] = set()
    if OUT.exists():
        with open(OUT) as fh:
            for line in fh:
                try:
                    seen.add(json.loads(line)["point_id"])
                except Exception:
                    pass
    parsed = 0
    written = 0
    missing = 0
    t0 = time.time()
    with open(OUT, "a") as out_fh, ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {ex.submit(process_part, t): t for t in triples_list}
        done = 0
        for fut in as_completed(futures):
            tcp = futures[fut]
            done += 1  # noqa: SIM113 (counter over as_completed, not a positional index)
            try:
                part, secs = fut.result()
                if part is None:
                    missing += 1
                else:
                    for s in secs:
                        c = to_chunk_record(part, s)
                        parsed += 1
                        if c["point_id"] not in seen:
                            out_fh.write(json.dumps(c, ensure_ascii=False) + "\n")
                            out_fh.flush()
                            seen.add(c["point_id"])
                            written += 1
            except Exception as e:
                print(f"  ! part failed {tcp}: {e}", flush=True)
            if done % 25 == 0 or done == len(triples_list):
                rate = parsed / max(time.time() - t0, 0.1)
                print(
                    f"  ... {done:>5}/{len(triples_list)} parts, "
                    f"missing={missing}, {parsed:>6} sections, "
                    f"{written} written, {rate:.1f}/s",
                    flush=True,
                )
            time.sleep(args.delay / max(args.workers, 1))

    print(
        f"\n=== Done: parts={len(triples_list)} (missing={missing}), "
        f"sections={parsed:,}, new={written:,}, "
        f"elapsed={time.time() - t0:.1f}s ===",
        flush=True,
    )
    print(f"JSONL: {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
