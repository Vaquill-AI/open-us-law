#!/usr/bin/env python3
"""Ingest all Social Security Administration Rulings (SSR + AR) into the
Vaquill corpus.

SSRs are precedential guidance the agency uses to explain how it interprets
and applies Social Security statutes/regulations to specific programs. AR
(Acquiescence Rulings) instruct SSA how to apply a federal circuit court
holding that departs from SSA's national interpretation. Both are legally
binding on SSA adjudicators and are core sources for Social Security law
practitioners.

Structure (all under https://www.ssa.gov/OP_Home/rulings/):
    rulings-toc.html   -> 4 program TOCs:
        oasi-toc.html  -> 29 OASI subject-indexes (oasi/01/SSR-OASI01toc.html ...)
        di-toc.html    -> 10 DI subject-indexes   (di/01/SSR-DI01toc.html ...)
        ssi-toc.html   -> 7  SSI subject-indexes  (ssi/01/SSR-SSI01toc.html ...)
        ar-toc.html    -> 11 AR circuit-indexes   (ar/01/AR01toc.html ...)
    Each subject/circuit index lists individual ruling HTML pages, e.g.
        oasi/01/SSR64-33-oasi-01.html
        di/04/SSR2013-03-di-04.html
        ar/02/AR86-02-ar-02.html
    Individual pages carry the canonical ruling number in <title> and <h1>
    (e.g. "SSR 13-3p", "SSR 64-33c", "AR 86-2R(2)").

Rich metadata captured per ruling (never stripped):
    ruling_number, program, subject/topic (from parent subject-index h2),
    doc_type (Ruling / Policy Interpretation Ruling / Acquiescence Ruling),
    effective_date, publication_date, published_in_fed_reg (Vol/No/page),
    superseded_by (arrays), rescinds, supersedes,
    rulings_referenced (other SSR/AR numbers cited),
    regulations_referenced (CFR citations),
    statutes_referenced (42 U.S.C. citations).

Output JSONL:
    <OUT_DIR>/state_ssa_rulings.jsonl
    (override the output directory with $OUT_DIR)

Usage:
    # Full scrape (default):
    python scripts/federal/ingest_ssa_rulings.py

    # Sample run for development:
    python scripts/federal/ingest_ssa_rulings.py --programs DI --limit-per-program 3

    # Discover only (dry run):
    python scripts/federal/ingest_ssa_rulings.py --dry-run

Notes:
    * ssa.gov requires a full browser header set (User-Agent, Accept,
      Accept-Language, Sec-Fetch-*) and a same-origin Referer for deep
      pages, otherwise it returns 403 via Akamai.
    * The WEBSHARE US proxy IPs are blocked by ssa.gov, so direct fetches
      are used. Any US-egress or US-adjacent host works with the headers.
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
from bs4 import BeautifulSoup

sys.stdout.reconfigure(line_buffering=True)  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# Paths / constants
# ---------------------------------------------------------------------------

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = Path(os.environ.get("OUT_DIR", "./data"))
OUT_JSONL = DATA_DIR / "state_ssa_rulings.jsonl"

BASE = "https://www.ssa.gov/OP_Home/rulings"
LANDING = f"{BASE}/"
TOC_URL = f"{BASE}/rulings-toc.html"

# Full desktop-Chrome UA. Bare "Mozilla/5.0" is 403'd by Akamai on ssa.gov.
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
)

# Programs and their master-TOC URLs. Program name -> (toc_url, canonical_name,
# canonical_prefix used in ruling numbers on this program's index pages).
PROGRAMS: dict[str, dict] = {
    "OASI": {
        "toc": f"{BASE}/oasi-toc.html",
        "label": "Old-Age and Survivors Insurance",
        "citation_prefix": "SSR",
    },
    "DI": {
        "toc": f"{BASE}/di-toc.html",
        "label": "Disability Insurance",
        "citation_prefix": "SSR",
    },
    "SSI": {
        "toc": f"{BASE}/ssi-toc.html",
        "label": "Supplemental Security Income",
        "citation_prefix": "SSR",
    },
    "AR": {
        "toc": f"{BASE}/ar-toc.html",
        "label": "Acquiescence Rulings",
        "citation_prefix": "AR",
    },
}


# ---------------------------------------------------------------------------
# Env + HTTP
# ---------------------------------------------------------------------------


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


_SESSION = requests.Session()


def _base_headers(referer: str | None = None) -> dict[str, str]:
    h = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        # NOTE: intentionally omit "br". requests only auto-decodes gzip
        # + deflate; brotli-encoded responses come back as raw bytes and
        # break every downstream parser.
        "Accept-Encoding": "gzip, deflate",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin" if referer else "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "Connection": "keep-alive",
    }
    if referer:
        h["Referer"] = referer
    return h


def fetch(
    url: str, referer: str | None = None, retries: int = 6, base_sleep: float = 0.6
) -> str | None:
    """Fetch a URL with retries + exponential backoff.

    ssa.gov (Akamai) returns 403 aggressively for missing headers or bursty
    traffic. Empirically a same-origin Referer + a modest pause between
    requests works reliably; when we do trip a 403 the site clears the
    block after a few seconds so backoff-and-retry works.
    """
    last_err: str | None = None
    for attempt in range(retries):
        try:
            r = _SESSION.get(url, headers=_base_headers(referer), timeout=45, allow_redirects=True)
            if r.status_code == 200:
                time.sleep(base_sleep)
                return r.text
            if r.status_code in (429, 502, 503, 504):
                time.sleep(2 + attempt * (attempt + 1))
                continue
            if r.status_code == 403:
                # Akamai transient block; back off.
                time.sleep(3 + attempt * (attempt + 1))
                continue
            last_err = f"HTTP {r.status_code}"
            return None
        except Exception as exc:  # network flake
            last_err = str(exc)
            time.sleep(2 + attempt)
    print(f"  ! fetch failed for {url}: {last_err}", flush=True)
    return None


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------


_SUBJECT_INDEX_RE = re.compile(
    r"^(?:https?://[^/]+)?/?(?:OP_Home/rulings/)?"
    r"(?P<prog>oasi|di|ssi|ar)/(?P<num>\d+)/"
    r"(?:SSR-(?:OASI|DI|SSI)|AR)\d+toc\.html$",
    re.IGNORECASE,
)


def _extract_subject_indexes(prog: str, toc_html: str) -> list[str]:
    """Return absolute URLs of subject/circuit TOCs for a program."""
    soup = BeautifulSoup(toc_html, "html.parser")
    urls: list[str] = []
    seen: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not _SUBJECT_INDEX_RE.match(href):
            continue
        # Only keep this program's TOCs (some pages cross-link).
        p_low = prog.lower()
        if not href.lower().startswith(f"{p_low}/"):
            continue
        absurl = href if href.startswith("http") else f"{BASE}/{href}"
        if absurl in seen:
            continue
        seen.add(absurl)
        urls.append(absurl)
    return urls


@dataclass
class SubjectIndex:
    program: str
    url: str
    subject_label: str  # e.g. "Old-Age and Survivors Insurance Benefit Payments"
    subject_num: str  # e.g. "01"
    entries: list[tuple[str, str]] = field(default_factory=list)  # (ruling_url, headline)


def _parse_subject_index(program: str, url: str, html: str) -> SubjectIndex:
    soup = BeautifulSoup(html, "html.parser")
    # Pick the specific topic <h2> for this subject. Subject-index pages
    # often have two <h2>s: a generic banner ("Social Security and
    # Acquiescence Rulings") and the specific subject
    # ("Disability, Period of Disability"). We keep the last non-generic,
    # non-TOC one — that's consistently the specific subject.
    _GENERIC = re.compile(
        r"social\s+security\s+(and\s+acquiescence\s+)?rulings?",
        re.IGNORECASE,
    )
    subject_label = ""
    for h2 in soup.find_all("h2"):
        t = " ".join(h2.get_text(" ", strip=True).split())
        if not t or "table of contents" in t.lower():
            continue
        if _GENERIC.search(t):
            continue
        subject_label = t
    if not subject_label:
        # Fall back to any non-TOC h2, then to h1.
        for h2 in soup.find_all("h2"):
            t = " ".join(h2.get_text(" ", strip=True).split())
            if t and "table of contents" not in t.lower():
                subject_label = t
                break
    if not subject_label:
        h1 = soup.find("h1")
        subject_label = " ".join(h1.get_text(" ", strip=True).split()) if h1 else ""

    m = re.search(r"/(\d+)/[^/]+toc\.html", url)
    subject_num = m.group(1) if m else ""

    ruling_href_re = re.compile(
        r"^(?:SSR|AR)[0-9A-Za-z\-]*(?:-(?:oasi|di|ssi|ar)-\d+)?\.html$",
        re.IGNORECASE,
    )
    entries: list[tuple[str, str]] = []
    seen: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.startswith("http") or href.startswith("#"):
            continue
        if "toc" in href.lower():
            continue
        base = href.rsplit("/", 1)[-1]
        if not ruling_href_re.match(base):
            continue
        # Absolute URL sits in the same directory as the subject index.
        absurl = url.rsplit("/", 1)[0] + "/" + base
        if absurl in seen:
            continue
        seen.add(absurl)
        headline = " ".join(a.get_text(" ", strip=True).split())
        entries.append((absurl, headline))

    return SubjectIndex(
        program=program,
        url=url,
        subject_label=subject_label,
        subject_num=subject_num,
        entries=entries,
    )


# ---------------------------------------------------------------------------
# Ruling parsing
# ---------------------------------------------------------------------------


@dataclass
class Ruling:
    ruling_number: str
    program: str
    subject_label: str
    subject_num: str
    doc_type: str
    headline: str
    section_title: str  # "SSR NN-Np: <headline>"
    body_text: str
    effective_date: str = ""
    publication_date: str = ""
    published_in_fed_reg: str = ""
    superseded_by: list[str] = field(default_factory=list)
    supersedes: list[str] = field(default_factory=list)
    rescinds: list[str] = field(default_factory=list)
    rulings_referenced: list[str] = field(default_factory=list)
    regulations_referenced: list[str] = field(default_factory=list)
    statutes_referenced: list[str] = field(default_factory=list)
    source_url: str = ""
    year: int | None = None
    raw_html: str = ""


_SSR_NUM_RE = re.compile(
    r"\b(SSR\s+(?:19|20)?\d{2}[-–]\d{1,3}[a-z]?p?)\b",
    re.IGNORECASE,
)
_AR_NUM_RE = re.compile(
    # NOTE: no trailing \b: after "(1)" we go non-word -> non-word so \b
    # never matches, and we would silently drop the circuit-suffix.
    r"\b(AR\s+(?:19|20)?\d{2}\s*[-–]\s*\d{1,3}[A-Za-z]?(?:\s*\(\d+\))?)(?!\w)",
    re.IGNORECASE,
)
_CFR_RE = re.compile(
    r"\b(\d{1,3})\s+CFR\s+((?:\d+(?:\.\d+)?(?:\([a-z0-9]+\))*"
    r"(?:\([a-z0-9]+\))*))\b"
)
_USC_RE = re.compile(r"\b(\d+)\s+U\.?\s?S\.?\s?C\.?\s*§?\s*(\d+[a-z]?(?:[-–]\d+)?)")
_FR_RE = re.compile(
    r"Federal\s+Register\s+Vol\.\s*(\d+),\s*No\.\s*(\d+),\s*page\s+(\d+)",
    re.IGNORECASE,
)
_EFF_RE = re.compile(r"Effective\s+Date\s*:\s*([A-Za-z]+\s+\d{1,2},\s+\d{4})", re.IGNORECASE)
_PUB_RE = re.compile(r"Publication\s+Date\s*:\s*([A-Za-z]+\s+\d{1,2},\s+\d{4})", re.IGNORECASE)


def _normalize_ssr(s: str) -> str:
    s = s.replace("–", "-")
    s = re.sub(r"\s+", " ", s).strip()
    # "SSR  96-8p" -> "SSR 96-8p"
    return re.sub(r"(?i)^ssr\s+", "SSR ", s)


def _normalize_ar(s: str) -> str:
    s = s.replace("–", "-")
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"(?i)^ar\s+", "AR ", s)
    # SSA canonical form has no space before the circuit suffix: "AR 88-4(1)".
    s = re.sub(r"\s+\(", "(", s)
    # And no spaces around the hyphen inside the number.
    s = re.sub(r"(?<=\d)\s*-\s*(?=\d)", "-", s)
    return s


def _extract_ruling_number(soup: BeautifulSoup, program: str) -> str:
    # Prefer <title>; falls back to <h1>. Both consistently start with the
    # canonical number.
    for tag in (soup.title, soup.find("h1")):
        if tag is None:
            continue
        t = tag.get_text(" ", strip=True)
        if not t:
            continue
        # Try SSR then AR patterns.
        m = _SSR_NUM_RE.search(t) or _AR_NUM_RE.search(t)
        if m:
            val = m.group(1)
            return _normalize_ar(val) if val.upper().startswith("AR") else _normalize_ssr(val)
    return ""


def _guess_doc_type(body_text: str, program: str) -> str:
    low = body_text.lower()[:2000]
    if program == "AR":
        return "Acquiescence Ruling"
    if "policy interpretation ruling" in low:
        return "Policy Interpretation Ruling"
    if "policy statement" in low:
        return "Policy Statement"
    return "Ruling"


def _extract_body(soup: BeautifulSoup) -> tuple[str, str]:
    """Return (headline_from_h1, cleaned_body_text)."""
    main = soup.find("div", id="content") or soup.find("main") or soup
    h1 = main.find("h1")
    headline = h1.get_text(" ", strip=True) if h1 else ""

    paragraphs: list[str] = []
    for el in main.find_all(["p", "h2", "h3", "h4", "h5", "ul", "ol"]):
        # Stop at "Back to Table of Contents".
        txt = el.get_text(" ", strip=True)
        if not txt:
            continue
        if txt.lower().startswith("back to table of contents"):
            break
        if el.name in ("ul", "ol"):
            for li in el.find_all("li", recursive=False):
                lt = li.get_text(" ", strip=True)
                if lt:
                    paragraphs.append("- " + lt)
            continue
        paragraphs.append(txt)

    text = "\n\n".join(paragraphs)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return headline, text


def _extract_supersession(body_text: str) -> tuple[list[str], list[str], list[str]]:
    """Best-effort extraction of supersedes / superseded_by / rescinds.

    SSA writes these as phrases like:
        "This ruling supersedes SSR 82-58."
        "SSR 82-58 is rescinded."
        "This ruling is superseded by SSR 96-3p."
    """
    supersedes: list[str] = []
    superseded_by: list[str] = []
    rescinds: list[str] = []

    lowered = body_text

    # Windowed matches: find each verb and pull SSR/AR numbers in the same
    # sentence.
    def _pull(nums_in: str) -> list[str]:
        found: list[str] = []
        for m in _SSR_NUM_RE.finditer(nums_in):
            found.append(_normalize_ssr(m.group(1)))
        for m in _AR_NUM_RE.finditer(nums_in):
            found.append(_normalize_ar(m.group(1)))
        # Dedup preserving order.
        seen: set[str] = set()
        out: list[str] = []
        for v in found:
            if v not in seen:
                seen.add(v)
                out.append(v)
        return out

    # Split on sentence-ish boundaries.
    sentences = re.split(r"(?<=[.\n])\s+", lowered)
    for sent in sentences:
        s_low = sent.lower()
        if "superseded by" in s_low:
            superseded_by.extend(_pull(sent))
        elif "supersedes" in s_low:
            supersedes.extend(_pull(sent))
        if "rescind" in s_low:
            rescinds.extend(_pull(sent))

    # Dedup + drop self-references handled by caller.
    def _dedup(xs: list[str]) -> list[str]:
        seen: set[str] = set()
        out: list[str] = []
        for x in xs:
            if x not in seen:
                seen.add(x)
                out.append(x)
        return out

    return _dedup(supersedes), _dedup(superseded_by), _dedup(rescinds)


def _extract_cross_references(
    body_text: str, self_num: str
) -> tuple[list[str], list[str], list[str]]:
    """Return (rulings_referenced, regulations_referenced, statutes_referenced)."""
    rulings: list[str] = []
    for m in _SSR_NUM_RE.finditer(body_text):
        v = _normalize_ssr(m.group(1))
        if v != self_num:
            rulings.append(v)
    for m in _AR_NUM_RE.finditer(body_text):
        v = _normalize_ar(m.group(1))
        if v != self_num:
            rulings.append(v)

    regs: list[str] = []
    for m in _CFR_RE.finditer(body_text):
        regs.append(f"{m.group(1)} CFR {m.group(2)}")

    statutes: list[str] = []
    for m in _USC_RE.finditer(body_text):
        statutes.append(f"{m.group(1)} U.S.C. {m.group(2)}")

    def _dedup(xs: list[str]) -> list[str]:
        seen: set[str] = set()
        out: list[str] = []
        for x in xs:
            if x not in seen:
                seen.add(x)
                out.append(x)
        return out

    return _dedup(rulings), _dedup(regs), _dedup(statutes)


def parse_ruling(
    html: str,
    program: str,
    subject_label: str,
    subject_num: str,
    source_url: str,
    headline_hint: str,
) -> Ruling | None:
    soup = BeautifulSoup(html, "html.parser")

    ruling_number = _extract_ruling_number(soup, program)
    if not ruling_number:
        # Fall back to the ruling number in the subject-index link text.
        m = _SSR_NUM_RE.search(headline_hint) or _AR_NUM_RE.search(headline_hint)
        if m:
            ruling_number = (
                _normalize_ssr(m.group(1))
                if m.group(1).upper().startswith("SSR")
                else _normalize_ar(m.group(1))
            )
    if not ruling_number:
        return None

    headline, body_text = _extract_body(soup)
    if len(body_text) < 40:
        return None

    doc_type = _guess_doc_type(body_text, program)

    eff = _EFF_RE.search(body_text)
    pub = _PUB_RE.search(body_text)
    fr = _FR_RE.search(body_text)
    published_in_fed_reg = ""
    if fr:
        published_in_fed_reg = f"{fr.group(1)} Fed. Reg. {fr.group(3)}"

    year = None
    year_match = re.search(r"(19|20)\d{2}", ruling_number)
    if year_match:
        y = int(year_match.group(0))
        if 1936 <= y <= 2100:
            year = y
    if year is None and eff:
        y2 = re.search(r"(19|20)\d{2}", eff.group(1))
        if y2:
            year = int(y2.group(0))

    supersedes, superseded_by, rescinds = _extract_supersession(body_text)
    # Drop any self-reference safety net (already usually excluded).
    supersedes = [x for x in supersedes if x != ruling_number]
    superseded_by = [x for x in superseded_by if x != ruling_number]
    rescinds = [x for x in rescinds if x != ruling_number]

    rulings_ref, regs_ref, statutes_ref = _extract_cross_references(body_text, ruling_number)

    # Build a clean section title. If the h1 already carries the ruling
    # number ("SSR 23-1p: TITLES II ..."), keep it as-is; otherwise
    # prepend the ruling number.
    _clean_head = headline.strip() if headline else ""
    if _clean_head:
        _clean_head = re.sub(r"\s+", " ", _clean_head)
        if _clean_head.upper().startswith(ruling_number.upper()):
            section_title = _clean_head
        else:
            section_title = f"{ruling_number}: {_clean_head}"
    else:
        section_title = f"{ruling_number}: {headline_hint}"

    return Ruling(
        ruling_number=ruling_number,
        program=program,
        subject_label=subject_label,
        subject_num=subject_num,
        doc_type=doc_type,
        headline=headline,
        section_title=section_title,
        body_text=body_text,
        effective_date=eff.group(1) if eff else "",
        publication_date=pub.group(1) if pub else "",
        published_in_fed_reg=published_in_fed_reg,
        superseded_by=superseded_by,
        supersedes=supersedes,
        rescinds=rescinds,
        rulings_referenced=rulings_ref,
        regulations_referenced=regs_ref,
        statutes_referenced=statutes_ref,
        source_url=source_url,
        year=year,
        raw_html=html,
    )


# ---------------------------------------------------------------------------
# Chunk emission
# ---------------------------------------------------------------------------


def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def _act_id_safe(ruling_number: str, program: str) -> str:
    # "SSR 96-8p" -> "SSR_96_8p"; "AR 86-2R(2)" -> "AR_86_2R_2_"
    slug = re.sub(r"[^A-Za-z0-9]+", "_", ruling_number).strip("_")
    return f"SSA_SSR_{program.upper()}_{slug}"


def _point_id(act_id: str, idx: int, text: str) -> str:
    seed = f"{act_id}::{idx}::{_sha1(text)[:12]}"
    return str(uuid.UUID(hashlib.md5(seed.encode()).hexdigest()))


def to_chunk_record(r: Ruling) -> dict:
    act_id = _act_id_safe(r.ruling_number, r.program)
    citation = r.ruling_number

    header_lines = [
        "Agency Guidance: Social Security Rulings | US | Federal",
        f"{r.program} — {r.subject_label}",
        f"{r.ruling_number}: {r.headline or r.section_title}",
    ]
    if r.effective_date:
        header_lines.append(f"Effective: {r.effective_date}")
    if r.published_in_fed_reg:
        header_lines.append(f"Fed. Reg.: {r.published_in_fed_reg}")
    text_for_embedding = "\n".join(header_lines) + "\n\n" + r.body_text

    display_path = f"Social Security Rulings / {r.program} / {r.subject_label} / {r.ruling_number}"

    breadcrumb = [
        {"type": "corpus", "num": "ssr", "label": "SSR"},
        {"type": "program", "num": r.program, "label": PROGRAMS[r.program]["label"]},
        {"type": "subject", "num": r.subject_num, "label": r.subject_label},
        {"type": "ruling", "num": r.ruling_number, "label": r.ruling_number},
    ]

    md = {
        "act_id": act_id,
        "corpus_type": "agency_guidance",
        "category": "ssa_ruling",
        "document_type": "ruling",
        "jurisdiction": "US",
        "country_code": "US",
        "state": "federal",
        "level_classifier": "ruling",
        "act_status": "in_force",
        # Title identity (synthetic slot 9200 for SSA agency guidance).
        "title": "Social Security Rulings",
        "title_name": "Social Security Rulings",
        "title_code": "ssr",
        "title_number": 9200,
        "top_level_title": f"ssr-{r.program.lower()}",
        # Program / subject
        "issuing_agency": "Social Security Administration",
        "issuing_agency_code": "SSA",
        "program": r.program,
        "program_label": PROGRAMS[r.program]["label"],
        "chapter": r.program,
        "chapter_name": PROGRAMS[r.program]["label"],
        "subject_number": r.subject_num,
        "subject_label": r.subject_label,
        "topic": r.subject_label,
        # Ruling identity
        "ruling_number": r.ruling_number,
        "doc_type": r.doc_type,
        "section_number": r.ruling_number,
        "section_title": r.section_title,
        "headline": r.headline,
        # Dates
        "year": r.year,
        "effective_date": r.effective_date or None,
        "publication_date": r.publication_date or None,
        "published_in_fed_reg": r.published_in_fed_reg or None,
        "last_amended_year": r.year,
        # Supersession
        "superseded_by": r.superseded_by,
        "supersedes": r.supersedes,
        "rescinds": r.rescinds,
        # Cross-references
        "rulings_referenced": r.rulings_referenced,
        "regulations_referenced": r.regulations_referenced,
        "cross_references_cfr": r.regulations_referenced,
        "statutes_referenced": r.statutes_referenced,
        "cross_references_usc": r.statutes_referenced,
        "cross_references_count": (
            len(r.rulings_referenced) + len(r.regulations_referenced) + len(r.statutes_referenced)
        ),
        # Citation + display
        "citation": citation,
        "citation_short": citation,
        "display_label": citation,
        "display_title": r.section_title,
        "display_path": display_path,
        "breadcrumb": breadcrumb,
        "sort_key": act_id,
        # Bookkeeping (shared corpus schema)
        "chunk_index": 0,
        "total_chunks": 1,
        "word_count": len(r.body_text.split()),
        "subsection_count": 0,
        "subsection_letters": [],
        "numbered_paragraph_count": 0,
        "amendment_years": [],
        "amendments_count": 0,
        "public_laws_referenced": [],
        "public_laws_count": 0,
        "renumbered_to": "",
        "transferred_to": "",
        # Source
        "source_url": r.source_url,
        "full_text_sha1": _sha1(r.body_text),
        # Compat fields other pipelines may look at.
        "text": r.body_text,
        "parent_id": f"us/federal/ssa/rulings/program={r.program}/subject={r.subject_num}",
        "raw_node_id": (
            f"us/federal/ssa/rulings/program={r.program}"
            f"/subject={r.subject_num}/ruling={r.ruling_number}"
        ),
    }

    return {
        "point_id": _point_id(act_id, 0, r.body_text),
        "text_for_embedding": text_for_embedding.strip(),
        "raw_text": r.body_text,
        "metadata": md,
    }


# ---------------------------------------------------------------------------
# Crawl
# ---------------------------------------------------------------------------


def _discover(programs: list[str]) -> list[SubjectIndex]:
    """Fetch program TOCs and subject indexes for the requested programs.

    Returns a flat list of SubjectIndex records, each with its ruling
    entries already parsed.
    """
    subject_indexes: list[SubjectIndex] = []

    # Fetch master TOC just to prove access; not strictly required for
    # discovery because we already know each program's TOC URL.
    _ = fetch(TOC_URL, referer=LANDING)

    for prog in programs:
        cfg = PROGRAMS[prog]
        print(f"[{prog}] discovering subject indexes from {cfg['toc']}", flush=True)
        toc_html = fetch(cfg["toc"], referer=TOC_URL)
        if not toc_html:
            print(f"[{prog}] ! could not fetch TOC", flush=True)
            continue
        idx_urls = _extract_subject_indexes(prog, toc_html)
        print(f"[{prog}] {len(idx_urls)} subject indexes", flush=True)
        for idx_url in idx_urls:
            idx_html = fetch(idx_url, referer=cfg["toc"])
            if not idx_html:
                print(f"  [{prog}] ! subject index failed: {idx_url}", flush=True)
                continue
            si = _parse_subject_index(prog, idx_url, idx_html)
            print(
                f"  [{prog}/{si.subject_num}] {si.subject_label!r}: {len(si.entries)} rulings",
                flush=True,
            )
            subject_indexes.append(si)

    return subject_indexes


def _process_ruling(
    prog: str,
    subj_label: str,
    subj_num: str,
    ruling_url: str,
    headline_hint: str,
    referer: str,
) -> dict | None:
    html = fetch(ruling_url, referer=referer)
    if not html:
        return None
    parsed = parse_ruling(
        html,
        prog,
        subj_label,
        subj_num,
        ruling_url,
        headline_hint,
    )
    if not parsed:
        return None

    return to_chunk_record(parsed)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--programs",
        default="OASI,DI,SSI,AR",
        help="Comma-separated programs to scrape (OASI,DI,SSI,AR).",
    )
    ap.add_argument(
        "--limit-per-program",
        type=int,
        default=0,
        help="If >0, stop after N rulings per program (dev-only).",
    )
    ap.add_argument(
        "--workers",
        type=int,
        default=3,
        help="Concurrency for ruling fetches. Keep low; SSA rate limits.",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Discover subject indexes only, do not fetch rulings.",
    )
    ap.add_argument("--out", default=str(OUT_JSONL))
    args = ap.parse_args()

    programs = [p.strip().upper() for p in args.programs.split(",") if p.strip()]
    for p in programs:
        if p not in PROGRAMS:
            print(f"unknown program: {p}", file=sys.stderr)
            return 2

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    t0 = time.time()
    subject_indexes = _discover(programs)
    if args.dry_run:
        total = sum(len(s.entries) for s in subject_indexes)
        print(
            f"\n[dry-run] {len(subject_indexes)} subject indexes, {total} rulings discovered",
            flush=True,
        )
        return 0

    # Build the crawl queue, respecting --limit-per-program.
    queue: list[tuple[str, SubjectIndex, str, str]] = []
    per_prog: dict[str, int] = dict.fromkeys(programs, 0)
    for si in subject_indexes:
        for ruling_url, headline in si.entries:
            if args.limit_per_program and per_prog[si.program] >= args.limit_per_program:
                break
            queue.append((si.program, si, ruling_url, headline))
            per_prog[si.program] += 1

    print(f"\n[crawl] {len(queue)} rulings queued across {len(programs)} programs", flush=True)

    # Load existing point_ids to skip already-ingested rulings.
    seen: set[str] = set()
    if out_path.exists():
        with open(out_path, encoding="utf-8") as fh:
            for line in fh:
                try:
                    seen.add(json.loads(line)["point_id"])
                except Exception:
                    pass
    print(f"[crawl] {len(seen)} existing point_ids will be skipped", flush=True)

    written = 0
    parsed_ok = 0
    with (
        open(out_path, "a", encoding="utf-8") as fh,
        ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex,
    ):
        futures = {}
        for prog, si, ruling_url, headline in queue:
            fut = ex.submit(
                _process_ruling,
                prog,
                si.subject_label,
                si.subject_num,
                ruling_url,
                headline,
                si.url,
            )
            futures[fut] = ruling_url

        done = 0
        for fut in as_completed(futures):
            done += 1
            url = futures[fut]
            try:
                rec = fut.result()
            except Exception as exc:
                print(f"  ! ruling failed {url}: {exc}", flush=True)
                continue
            if rec is None:
                continue
            parsed_ok += 1
            if rec["point_id"] in seen:
                continue
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
            fh.flush()
            seen.add(rec["point_id"])
            written += 1
            if done % 25 == 0 or done == len(queue):
                rate = done / max(time.time() - t0, 0.1)
                print(
                    f"  ... {done:>5}/{len(queue)} rulings, "
                    f"parsed={parsed_ok}, new={written}, {rate:.1f}/s",
                    flush=True,
                )

    print(
        f"\n=== Done: queued={len(queue)}, parsed={parsed_ok}, "
        f"new={written}, elapsed={time.time() - t0:.1f}s ===",
        flush=True,
    )
    print(f"JSONL: {out_path}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
