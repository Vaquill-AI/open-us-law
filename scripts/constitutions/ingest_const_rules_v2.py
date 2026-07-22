#!/usr/bin/env python3
"""Clean, parallel ingest of US Constitution + Federal Court Rules.

Parallelism: ThreadPoolExecutor over (corpus_set, page_url) work items.
Multi-format R2: every chunk gets html/pdf/docx/txt URLs in metadata
(only formats actually mirrored show up in r2_formats_available).

Output:
  data/state_chunks/constitution_chunks.jsonl
  data/state_chunks/federal_rules_chunks.jsonl

Then run:
  cat data/state_chunks/constitution_chunks.jsonl \
      data/state_chunks/federal_rules_chunks.jsonl \
    > /tmp/const_rules.jsonl
  python scripts/us_corpus/embed_and_upsert.py --input /tmp/const_rules.jsonl
  python scripts/us_corpus/sync_constitution_and_rules_to_supabase.py
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import hashlib
import json
import os
import re
import sys
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

sys.stdout.reconfigure(line_buffering=True)  # type: ignore[attr-defined]

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = Path(os.environ.get("OUT_DIR", "./data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
CONST_OUT = DATA_DIR / "constitution_chunks.jsonl"
RULES_OUT = DATA_DIR / "federal_rules_chunks.jsonl"

UA = "Mozilla/5.0 (Vaquill ingestion bot; +https://vaquill.ai)"

# Roman numerals
_ROMAN = ["", "I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", "X",
          "XI", "XII", "XIII", "XIV", "XV", "XVI", "XVII", "XVIII", "XIX",
          "XX", "XXI", "XXII", "XXIII", "XXIV", "XXV", "XXVI", "XXVII"]


# ---------------------------------------------------------------------------
# Env + R2
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


# R2 mirroring is disabled in the open release. These no-op stubs let the
# per-source scrapers run unchanged and simply skip the source-file mirror;
# the JSONL output is unaffected.
def put_if_changed(*_args, **_kwargs) -> bool:
    return False


def _put_if_changed(*_args, **_kwargs) -> bool:
    return False


def public_url(*_args, **_kwargs) -> str:
    return ""



# ---------------------------------------------------------------------------
# HTTP — session reuse + parallel
# ---------------------------------------------------------------------------


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": UA})
    adapter = requests.adapters.HTTPAdapter(pool_connections=32, pool_maxsize=32, max_retries=0)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


SESSION = _session()
POLITE_DELAY = 0.0  # set >0 in polite mode for an extra sleep per fetch


def fetch_text(url: str, retries: int = 4) -> str:
    last = None
    for attempt in range(retries):
        try:
            r = SESSION.get(url, timeout=45, allow_redirects=True)
            r.raise_for_status()
            if POLITE_DELAY > 0:
                time.sleep(POLITE_DELAY)
            return r.text
        except Exception as e:  # noqa
            last = e
            # Exponential backoff plus longer floor if we suspect WAF block
            wait = max(1.0, 0.5 * (2 ** attempt))
            time.sleep(wait)
    raise RuntimeError(f"fetch failed {url}: {last}")


def fetch_bytes(url: str, retries: int = 4) -> bytes:
    last = None
    for attempt in range(retries):
        try:
            r = SESSION.get(url, timeout=120, allow_redirects=True)
            r.raise_for_status()
            return r.content
        except Exception as e:
            last = e
            time.sleep(0.5 * (2 ** attempt))
    raise RuntimeError(f"fetch_bytes failed {url}: {last}")


# ---------------------------------------------------------------------------
# Chunk record
# ---------------------------------------------------------------------------


@dataclass
class Section:
    act_id: str
    citation: str
    title_name: str
    section_number: str
    section_title: str
    raw_text: str
    source_url: str
    breadcrumb: list = field(default_factory=list)
    # R2 mirror URLs (filled in by mirror() pass)
    r2_html_url: Optional[str] = None
    r2_pdf_url: Optional[str] = None
    r2_docx_url: Optional[str] = None
    r2_xml_url: Optional[str] = None
    # Constitution: corpus_type='constitution', state='federal'
    # Rules: corpus_type='federal_rules', state='federal'
    corpus_type: str = "constitution"
    top_level_title: str = "constitution"
    level_classifier: str = "section"


def sha1_hex(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def point_id_for(act_id: str, chunk_idx: int, text: str) -> str:
    """Content-addressed UUID — same formula as state-statute pipeline."""
    h = hashlib.md5(f"{act_id}::{chunk_idx}::{sha1_hex(text)[:12]}".encode()).hexdigest()
    return str(uuid.UUID(h))


def to_chunk_record(sec: Section) -> dict:
    text = sec.raw_text.strip()
    if not text:
        text = sec.section_title
    text_for_embedding = (
        f"{sec.title_name} | {sec.citation}\n"
        f"{sec.section_title}\n\n{text}"
    )
    formats = ["html"]
    if sec.r2_pdf_url:
        formats.append("pdf")
    if sec.r2_docx_url:
        formats.append("docx")
    if sec.r2_xml_url:
        formats.append("xml")
    formats.append("txt")

    md = {
        "act_id": sec.act_id,
        "corpus_type": sec.corpus_type,
        "category": "federal_authority",
        "document_type": (
            "constitution" if sec.corpus_type == "constitution"
            else "court_rule"
        ),
        "jurisdiction": "US",
        "country_code": "US",
        "state": "federal",
        "title_name": sec.title_name,
        "title": sec.title_name,
        "top_level_title": sec.top_level_title,
        "level_classifier": sec.level_classifier,
        "chapter": None,
        "section_number": sec.section_number,
        "section_title": sec.section_title,
        "citation": sec.citation,
        "citation_short": sec.citation,
        "display_label": sec.citation,
        "display_title": sec.section_title,
        "display_path": " / ".join(sec.breadcrumb) if sec.breadcrumb else sec.section_title,
        "breadcrumb": sec.breadcrumb,
        "sort_key": sec.act_id,
        "act_status": "in_force",
        "renumbered_to": "",
        "transferred_to": "",
        "year": None,
        "word_count": len(text.split()),
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
        "source_url": sec.source_url,
        "parent_id": None,
        "raw_node_id": sec.act_id,
        "full_text_sha1": sha1_hex(text),
    }
    return {
        "point_id": point_id_for(sec.act_id, 0, text),
        "text_for_embedding": text_for_embedding,
        "raw_text": text,
        "metadata": md,
    }


# ---------------------------------------------------------------------------
# US Constitution scraper (Cornell LII)
# ---------------------------------------------------------------------------

CONST_BASE = "https://www.law.cornell.edu/constitution"


_BOR_WORDS = ["first", "second", "third", "fourth", "fifth", "sixth",
              "seventh", "eighth", "ninth", "tenth"]


def _const_pages() -> list[tuple[str, str]]:
    """Return [(slug, url), ...] of constitution pages.

    Cornell URL patterns:
      preamble       -> /preamble
      articles I-VII -> /articlei, /articleii, ..., /articlevii
      amendments 1-10 (Bill of Rights) -> /first_amendment ... /tenth_amendment
      amendments 11-27 -> /amendmentxi, /amendmentxii, ...
    """
    out: list[tuple[str, str]] = [("preamble", f"{CONST_BASE}/preamble")]
    for n in range(1, 8):
        out.append((f"article{_ROMAN[n].lower()}", f"{CONST_BASE}/article{_ROMAN[n].lower()}"))
    # Bill of Rights uses word-form URLs
    for n in range(1, 11):
        word = _BOR_WORDS[n - 1]
        out.append((f"{word}_amendment", f"{CONST_BASE}/{word}_amendment"))
    # Amendments 11+ use roman-form URLs
    for n in range(11, 28):
        out.append((f"amendment{_ROMAN[n].lower()}", f"{CONST_BASE}/amendment{_ROMAN[n].lower()}"))
    return out


def _clean_text(soup_or_str) -> str:
    if hasattr(soup_or_str, "get_text"):
        s = soup_or_str.get_text(" ", strip=True)
    else:
        s = str(soup_or_str)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def parse_const_page(slug: str, url: str, html: str) -> list[Section]:
    """Parse one Cornell Constitution page into Section records.

    Cornell layout: each article/amendment page has the full text as a list of
    sections inside <article> or .field-name-body.
    """
    soup = BeautifulSoup(html, "html.parser")
    body = soup.find("div", class_="field-name-body") or soup.find("article") or soup
    out: list[Section] = []

    if slug == "preamble":
        text = _clean_text(body)
        out.append(Section(
            act_id="CONST_US_PREAMBLE",
            citation="U.S. Const. pmbl.",
            title_name="US Constitution",
            section_number="pmbl",
            section_title="Preamble",
            raw_text=text,
            source_url=url,
            breadcrumb=["US Constitution", "Preamble"],
        ))
        return out

    if slug.startswith("article"):
        roman = slug.replace("article", "").upper()
        # find section headings like "Section 1." or "Section 1"
        sections = re.split(r"\n*\bSection\s+(\d+)\.\s*", body.get_text("\n", strip=True))
        if len(sections) > 1:
            # sections[0] = preface before Section 1 (often empty), then alternating num, text
            for i in range(1, len(sections), 2):
                sec_num = sections[i].strip()
                sec_text = sections[i + 1].strip() if i + 1 < len(sections) else ""
                if not sec_text:
                    continue
                out.append(Section(
                    act_id=f"CONST_US_A{roman}_S{sec_num}",
                    citation=f"U.S. Const. art. {roman}, § {sec_num}",
                    title_name="US Constitution",
                    section_number=sec_num,
                    section_title=f"Article {roman}, Section {sec_num}",
                    raw_text=re.sub(r"\s+", " ", sec_text).strip(),
                    source_url=url,
                    breadcrumb=["US Constitution", f"Article {roman}", f"Section {sec_num}"],
                ))
        else:
            # no Section split — emit as single article
            text = _clean_text(body)
            out.append(Section(
                act_id=f"CONST_US_A{roman}",
                citation=f"U.S. Const. art. {roman}",
                title_name="US Constitution",
                section_number="",
                section_title=f"Article {roman}",
                raw_text=text,
                source_url=url,
                breadcrumb=["US Constitution", f"Article {roman}"],
            ))
        return out

    # Match both BoR slugs ("first_amendment") and roman slugs ("amendmentxi")
    bor_match = re.match(r"^(\w+)_amendment$", slug)
    if bor_match:
        word = bor_match.group(1)
        if word in _BOR_WORDS:
            n = _BOR_WORDS.index(word) + 1
            roman = _ROMAN[n]
            return _parse_amendment_body(slug, url, body, n, roman)
    if slug.startswith("amendment"):
        roman = slug.replace("amendment", "").upper()
        n = _ROMAN.index(roman) if roman in _ROMAN else 0
        return _parse_amendment_body(slug, url, body, n, roman)
    return out


def _parse_amendment_body(slug, url, body, n: int, roman: str) -> list[Section]:
    out: list[Section] = []
    if True:
        # fall through legacy code path below preserved for reference; we
        # consolidate amendment parsing here.
        amend_text = body.get_text("\n", strip=True)
        sections = re.split(r"\n*\bSection\s+(\d+)\.\s*", amend_text)
        if len(sections) > 1:
            for i in range(1, len(sections), 2):
                sec_num = sections[i].strip()
                sec_text = sections[i + 1].strip() if i + 1 < len(sections) else ""
                if not sec_text:
                    continue
                out.append(Section(
                    act_id=f"CONST_US_AM{n}_S{sec_num}",
                    citation=f"U.S. Const. amend. {roman}, § {sec_num}",
                    title_name="US Constitution",
                    section_number=sec_num,
                    section_title=f"Amendment {roman}, Section {sec_num}",
                    raw_text=re.sub(r"\s+", " ", sec_text).strip(),
                    source_url=url,
                    breadcrumb=["US Constitution", f"Amendment {roman}", f"Section {sec_num}"],
                ))
        else:
            text = _clean_text(body)
            out.append(Section(
                act_id=f"CONST_US_AM{n}",
                citation=f"U.S. Const. amend. {roman}",
                title_name="US Constitution",
                section_number="",
                section_title=f"Amendment {roman}",
                raw_text=text,
                source_url=url,
                breadcrumb=["US Constitution", f"Amendment {roman}"],
            ))
        return out
    return out


# ---------------------------------------------------------------------------
# Federal Rules scraper (Cornell LII)
# ---------------------------------------------------------------------------

RULE_SETS = [
    # (key, label, index_url, citation_template, title_name)
    ("frcp",  "FRCP",  "https://www.law.cornell.edu/rules/frcp",
     "Fed. R. Civ. P. {rule}",  "Federal Rules of Civil Procedure"),
    ("frcrp", "FRCrP", "https://www.law.cornell.edu/rules/frcrmp",
     "Fed. R. Crim. P. {rule}", "Federal Rules of Criminal Procedure"),
    ("fre",   "FRE",   "https://www.law.cornell.edu/rules/fre",
     "Fed. R. Evid. {rule}",     "Federal Rules of Evidence"),
    ("frap",  "FRAP",  "https://www.law.cornell.edu/rules/frap",
     "Fed. R. App. P. {rule}",   "Federal Rules of Appellate Procedure"),
    ("frbp",  "FRBP",  "https://www.law.cornell.edu/rules/frbp",
     "Fed. R. Bankr. P. {rule}", "Federal Rules of Bankruptcy Procedure"),
    ("ustc",  "USTC",  "https://www.law.cornell.edu/rules/uscourts/tax",
     "U.S. Tax Ct. R. {rule}",   "US Tax Court Rules of Practice and Procedure"),
]


def parse_rule_index(html: str, base_url: str) -> list[tuple[str, str]]:
    """Return [(rule_number, rule_page_url), ...] from a rule-set index page."""
    soup = BeautifulSoup(html, "html.parser")
    out: list[tuple[str, str]] = []
    seen: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        # Cornell rule URLs look like /rules/frcp/rule_1 or /rules/frcp/rule_4.1
        m = re.search(r"/rules/[\w/]+/rule_([\w\.]+)$", href)
        if not m:
            continue
        rule_num = m.group(1)
        if rule_num in seen:
            continue
        seen.add(rule_num)
        if href.startswith("/"):
            url = "https://www.law.cornell.edu" + href
        else:
            url = href
        out.append((rule_num, url))
    return out


def parse_rule_page(rule_num: str, url: str, html: str,
                    key: str, label: str, citation_tmpl: str,
                    title_name: str) -> Section:
    soup = BeautifulSoup(html, "html.parser")
    body = soup.find("div", class_="field-name-body") or soup.find("article") or soup
    h1 = soup.find("h1")
    h_title = h1.get_text(strip=True) if h1 else f"Rule {rule_num}"
    text = _clean_text(body)
    act_id = f"FRULES_{label.upper()}_R{rule_num.upper().replace('.', '_')}"
    citation = citation_tmpl.format(rule=rule_num)
    return Section(
        act_id=act_id,
        citation=citation,
        title_name=title_name,
        section_number=rule_num,
        section_title=h_title,
        raw_text=text,
        source_url=url,
        breadcrumb=[title_name, f"Rule {rule_num}"],
        corpus_type="federal_rules",
        top_level_title=key,
        level_classifier="rule",
    )


# ---------------------------------------------------------------------------
# Binary mirrors (PDF + DOCX) — one-per-corpus uploads, parallel
# ---------------------------------------------------------------------------

BINARY_SOURCES = [
    # (corpus_key, format, r2_key, source_url)
    ("constitution", "pdf",
     "constitution/pdf/us_constitution.pdf",
     "https://constitutioncenter.org/media/files/constitution.pdf"),
    ("frcp", "docx",
     "federal_rules/frcp/docx/federal_rules_civil_procedure.docx",
     "https://www.uscourts.gov/file/document/federal-rules-civil-procedure"),
    ("frcrp", "docx",
     "federal_rules/frcrp/docx/federal_rules_criminal_procedure.docx",
     "https://www.uscourts.gov/file/document/federal-rules-criminal-procedure"),
    ("fre", "pdf",
     "federal_rules/fre/pdf/federal_rules_evidence.pdf",
     "https://www.uscourts.gov/file/document/federal-rules-evidence"),
    ("frap", "docx",
     "federal_rules/frap/docx/federal_rules_appellate_procedure.docx",
     "https://www.uscourts.gov/file/document/federal-rules-appellate-procedure"),
    ("frbp", "docx",
     "federal_rules/frbp/docx/federal_rules_bankruptcy_procedure.docx",
     "https://www.uscourts.gov/file/document/federal-rules-bankruptcy-procedure"),
    ("sct", "pdf",
     "federal_rules/sct/pdf/supreme_court_rules.pdf",
     "https://www.supremecourt.gov/ctrules/2019RulesoftheCourt.pdf"),
    # USTC no binary mirror discovered
]


def mirror_binary(r2, corpus_key: str, fmt: str, r2_key: str, src_url: str) -> tuple[str, Optional[str]]:
    """Download src_url, validate magic bytes, upload to R2 (idempotent)."""
    try:
        body = fetch_bytes(src_url)
        magic = body[:4]
        if fmt == "pdf" and magic != b"%PDF":
            print(f"  [{corpus_key}/{fmt}] bad magic {magic!r}; skipping")
            return (corpus_key, None)
        if fmt == "docx" and magic != b"PK\x03\x04":
            print(f"  [{corpus_key}/{fmt}] bad magic {magic!r}; skipping")
            return (corpus_key, None)
        ct = "application/pdf" if fmt == "pdf" else \
             "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        changed = _put_if_changed(r2, r2_key, body, ct)
        url = public_url(r2_key)
        print(f"  [{corpus_key}/{fmt}] {len(body):,} bytes {'uploaded' if changed else '(unchanged)'} -> {r2_key}")
        return (corpus_key, url)
    except Exception as e:
        print(f"  [{corpus_key}/{fmt}] FAIL: {e}")
        return (corpus_key, None)


def upload_binaries_parallel(r2) -> dict[str, dict[str, Optional[str]]]:
    if r2 is None:
        return {}
    out: dict[str, dict[str, Optional[str]]] = {}
    with cf.ThreadPoolExecutor(max_workers=4) as ex:
        futures = []
        for ck, fmt, r2_key, src in BINARY_SOURCES:
            futures.append((ck, fmt, ex.submit(mirror_binary, r2, ck, fmt, r2_key, src)))
        for ck, fmt, fut in futures:
            _, url = fut.result()
            out.setdefault(ck, {})[fmt] = url
    return out


# ---------------------------------------------------------------------------
# Per-section text + html mirror (parallel)
# ---------------------------------------------------------------------------


def mirror_section_assets(r2, sec: Section, html_text: str,
                          r2_html_key: str, r2_txt_key: str) -> None:
    """Upload section HTML + canonical TXT to R2. Mutates sec.r2_html_url/r2_txt_url."""
    try:
        _put_if_changed(r2, r2_html_key, html_text.encode("utf-8"), "text/html; charset=utf-8")
        sec.r2_html_url = public_url(r2_html_key)
    except Exception as e:
        print(f"  [{sec.act_id}] html upload FAIL: {e}")
    try:
        _put_if_changed(r2, r2_txt_key, sec.raw_text.encode("utf-8"), "text/plain; charset=utf-8")
        sec.r2_txt_url = public_url(r2_txt_key)
    except Exception as e:
        print(f"  [{sec.act_id}] txt upload FAIL: {e}")


# ---------------------------------------------------------------------------
# Workflow
# ---------------------------------------------------------------------------


def run_constitution(r2, bin_urls: dict[str, dict[str, Optional[str]]],
                     workers: int) -> list[Section]:
    pages = _const_pages()  # ~35 pages
    print(f"\n=== Constitution: fetching {len(pages)} pages with {workers} workers ===")
    results: list[Section] = []
    page_html: dict[str, str] = {}

    # Parallel fetch
    with cf.ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(fetch_text, url): (slug, url) for slug, url in pages}
        for fut in cf.as_completed(futs):
            slug, url = futs[fut]
            try:
                html = fut.result()
                page_html[slug] = html
            except Exception as e:
                print(f"  [{slug}] fetch fail: {e}")

    # Parse + emit Section objects
    for slug, url in pages:
        if slug not in page_html:
            continue
        recs = parse_const_page(slug, url, page_html[slug])
        results.extend(recs)
    print(f"  parsed {len(results)} sections")

    # Mirror HTML + TXT per section, parallel
    const_pdf_url = bin_urls.get("constitution", {}).get("pdf")
    with cf.ThreadPoolExecutor(max_workers=workers) as ex:
        futs = []
        for sec in results:
            r2_html = f"constitution/source/{sec.act_id}.html"
            r2_txt = f"constitution/sections/{sec.act_id}.txt"
            html = page_html.get(_slug_for(sec), "")
            sec.r2_pdf_url = const_pdf_url
            futs.append(ex.submit(mirror_section_assets, r2, sec, html, r2_html, r2_txt))
        for _ in cf.as_completed(futs):
            pass
    print(f"  mirrored {len(results)} sections to R2 (html + txt)")
    return results


def _slug_for(sec: Section) -> str:
    """Return the const page slug a section came from (for HTML mirror)."""
    a = sec.act_id
    if a == "CONST_US_PREAMBLE":
        return "preamble"
    if a.startswith("CONST_US_A") and not a.startswith("CONST_US_AM"):
        # Article: act_id like CONST_US_AI_S1 or CONST_US_AIV
        rest = a.split("_")[2]
        roman = rest.lstrip("A").lower() if rest.startswith("A") else rest.lower()
        return f"article{roman}"
    if a.startswith("CONST_US_AM"):
        rest = a.split("_")[2]  # e.g. "AM14" or "AM1"
        n = int(re.findall(r"\d+", rest)[0])
        if 1 <= n <= 10:
            return f"{_BOR_WORDS[n - 1]}_amendment"
        return f"amendment{_ROMAN[n].lower()}"
    return ""


def run_rules(r2, bin_urls: dict[str, dict[str, Optional[str]]],
              workers: int, only_sets: list[str] | None = None) -> list[Section]:
    sets = RULE_SETS if not only_sets else [r for r in RULE_SETS if r[0] in only_sets]
    print(f"\n=== Federal Rules: {len(sets)} sets with {workers} workers per set ===")
    all_secs: list[Section] = []

    for key, label, index_url, citation_tmpl, title_name in sets:
        try:
            idx_html = fetch_text(index_url)
        except Exception as e:
            print(f"  [{label}] index fetch FAIL: {e}")
            continue
        rules = parse_rule_index(idx_html, index_url)
        print(f"  [{label}] {len(rules)} rules from {index_url}")

        # Parallel fetch each rule
        pages: dict[str, str] = {}
        with cf.ThreadPoolExecutor(max_workers=workers) as ex:
            futs = {ex.submit(fetch_text, url): (rn, url) for rn, url in rules}
            for fut in cf.as_completed(futs):
                rn, url = futs[fut]
                try:
                    pages[rn] = fut.result()
                except Exception as e:
                    print(f"    [{label} rule {rn}] fetch fail: {e}")
        print(f"    fetched {len(pages)}/{len(rules)} rule pages")

        # Parse to sections + mirror
        secs: list[Section] = []
        pdf_url = bin_urls.get(key, {}).get("pdf")
        docx_url = bin_urls.get(key, {}).get("docx")
        with cf.ThreadPoolExecutor(max_workers=workers) as ex:
            mirror_futs = []
            for rn, html in pages.items():
                url = next(u for r, u in rules if r == rn)
                sec = parse_rule_page(rn, url, html, key, label, citation_tmpl, title_name)
                sec.r2_pdf_url = pdf_url
                sec.r2_docx_url = docx_url
                secs.append(sec)
                r2_html = f"federal_rules/{key}/source/rule_{rn}.html"
                r2_txt = f"federal_rules/{key}/sections/{sec.act_id}.txt"
                mirror_futs.append(ex.submit(mirror_section_assets, r2, sec, html, r2_html, r2_txt))
            for _ in cf.as_completed(mirror_futs):
                pass
        print(f"    [{label}] mirrored {len(secs)} sections")
        all_secs.extend(secs)

    return all_secs


def write_jsonl(path: Path, secs: list[Section]) -> int:
    """Merge with existing JSONL — never destructive.

    Reads existing chunks indexed by act_id. New secs replace same-act_id
    entries; existing entries we didn't re-scrape are preserved. This makes
    partial runs (e.g. Cornell IP-blocks mid-scrape) safe to retry.
    """
    existing: dict[str, dict] = {}
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                aid = rec.get("metadata", {}).get("act_id")
                if aid:
                    existing[aid] = rec
            except json.JSONDecodeError:
                continue

    # Overlay new sections
    n_new = 0
    n_updated = 0
    for sec in secs:
        rec = to_chunk_record(sec)
        aid = sec.act_id
        if aid in existing:
            n_updated += 1
        else:
            n_new += 1
        existing[aid] = rec

    # Write merged set
    with open(path, "w", encoding="utf-8") as fh:
        for rec in existing.values():
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")

    print(f"  merge: {n_new} new, {n_updated} updated, {len(existing)} total")
    return len(existing)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--corpus", choices=["constitution", "rules", "all"], default="all")
    ap.add_argument("--workers", type=int, default=16)
    ap.add_argument("--polite", action="store_true",
                    help="One worker per set, 1.0s delay per fetch (avoids Cornell WAF)")
    ap.add_argument("--only-sets", default="",
                    help="Comma-separated subset of rule keys (frcp,frcrp,fre,frap,frbp,sct,ustc)")
    args = ap.parse_args()
    if args.polite:
        args.workers = 1
        global POLITE_DELAY
        POLITE_DELAY = 1.0
        print("=== POLITE MODE: 1 worker, 1.0s delay per fetch ===")

    _load_env()

    print("=== Step 1: parallel-upload canonical PDFs/DOCX ===")
    r2 = None  # R2 mirror disabled in the open release
    bin_urls = upload_binaries_parallel(r2)
    print()

    if args.corpus in ("constitution", "all"):
        secs = run_constitution(r2, bin_urls, args.workers)
        n = write_jsonl(CONST_OUT, secs)
        print(f"\n-> wrote {n} chunks to {CONST_OUT}")

    if args.corpus in ("rules", "all"):
        only = [s.strip() for s in args.only_sets.split(",") if s.strip()] if args.only_sets else None
        secs = run_rules(r2, bin_urls, args.workers, only_sets=only)
        n = write_jsonl(RULES_OUT, secs)
        print(f"\n-> wrote {n} chunks to {RULES_OUT}")

    print("\nDONE")
    return 0


if __name__ == "__main__":
    sys.exit(main())
