#!/usr/bin/env python3
"""Ingest 50 state constitutions into the existing pipeline.

corpus_type='state_constitution', act_id prefix 'SCONST_<ST>_'

Architecture: per-state config object. Each state has a discovery function
that returns (article_id, html_text, url) tuples, then a single uniform
parser splits articles into sections.

Run:
    python scripts/us_corpus/ingest_state_constitutions.py --states ca
    python scripts/us_corpus/ingest_state_constitutions.py --states ca,tx,ny --workers 8
    python scripts/us_corpus/ingest_state_constitutions.py --all

Mirrors HTML + canonical TXT to R2 at state_constitutions/<st>/source/...
and state_constitutions/<st>/sections/...

After scrape, run:
    python scripts/us_corpus/embed_and_upsert.py --input data/state_chunks/state_constitutions_chunks.jsonl
    python scripts/us_corpus/sync_constitution_and_rules_to_supabase.py \\
        --input data/state_chunks/state_constitutions_chunks.jsonl
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
from typing import Callable, Optional

import requests
from bs4 import BeautifulSoup

sys.stdout.reconfigure(line_buffering=True)  # type: ignore[attr-defined]

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
# Default to the vendored state-scrapers chunks dir so this writes alongside
# every other state ingestion output. STATE_CHUNKS_DIR_OVERRIDE env var lets
# callers point at an alternate location (used by the VM-host pipeline).
DATA_DIR = Path(os.environ.get("OUT_DIR", "./data"))
OUT = DATA_DIR / "state_constitutions_chunks.jsonl"

UA = "Mozilla/5.0 (Vaquill ingestion bot; +https://vaquill.ai)"


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
# HTTP session
# ---------------------------------------------------------------------------


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": UA})
    a = requests.adapters.HTTPAdapter(pool_connections=32, pool_maxsize=32, max_retries=0)
    s.mount("http://", a)
    s.mount("https://", a)
    return s


SESSION = _session()


def _us_proxies() -> Optional[dict]:
    """Webshare US-rotating proxy dict for state .gov sources that geo-block.
    Returns None if WEBSHARE_USERNAME / WEBSHARE_PASSWORD aren't set."""
    _load_env()
    user = os.environ.get("WEBSHARE_USERNAME", "")
    pwd = os.environ.get("WEBSHARE_PASSWORD", "")
    if not user or not pwd:
        return None
    host = os.environ.get("WEBSHARE_PROXY_HOST", "p.webshare.io")
    port = os.environ.get("WEBSHARE_PROXY_PORT", "80")
    import urllib.parse
    proxy_user = f"{user}-US-rotate"
    url = f"http://{urllib.parse.quote(proxy_user)}:{urllib.parse.quote(pwd)}@{host}:{port}"
    return {"http": url, "https": url}


_MOZ_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
)


def fetch_text(url: str, retries: int = 4, use_us_proxy: bool = False) -> str:
    proxies = _us_proxies() if use_us_proxy else None
    # Many state .gov sites block obvious-bot UA strings; spoof Chrome for
    # proxied fetches.
    headers = {"User-Agent": _MOZ_UA} if use_us_proxy else None
    last = None
    for attempt in range(retries):
        try:
            r = SESSION.get(url, timeout=45, allow_redirects=True,
                            proxies=proxies, headers=headers)
            r.raise_for_status()
            return r.text
        except Exception as e:
            last = e
            time.sleep(max(1.0, 0.5 * (2 ** attempt)))
    raise RuntimeError(f"fetch failed {url}: {last}")


# ---------------------------------------------------------------------------
# Record schema (matches embed_and_upsert chunk shape)
# ---------------------------------------------------------------------------


_FULL_STATE_NAME = {
    "al": "Alabama", "ak": "Alaska", "az": "Arizona", "ar": "Arkansas",
    "ca": "California", "co": "Colorado", "ct": "Connecticut", "de": "Delaware",
    "fl": "Florida", "ga": "Georgia", "hi": "Hawaii", "id": "Idaho",
    "il": "Illinois", "in": "Indiana", "ia": "Iowa", "ks": "Kansas",
    "ky": "Kentucky", "la": "Louisiana", "me": "Maine", "md": "Maryland",
    "ma": "Massachusetts", "mi": "Michigan", "mn": "Minnesota", "ms": "Mississippi",
    "mo": "Missouri", "mt": "Montana", "ne": "Nebraska", "nv": "Nevada",
    "nh": "New Hampshire", "nj": "New Jersey", "nm": "New Mexico", "ny": "New York",
    "nc": "North Carolina", "nd": "North Dakota", "oh": "Ohio", "ok": "Oklahoma",
    "or": "Oregon", "pa": "Pennsylvania", "ri": "Rhode Island", "sc": "South Carolina",
    "sd": "South Dakota", "tn": "Tennessee", "tx": "Texas", "ut": "Utah",
    "vt": "Vermont", "va": "Virginia", "wa": "Washington", "wv": "West Virginia",
    "wi": "Wisconsin", "wy": "Wyoming",
}

# Citation patterns per state (Bluebook short form)
_CITATION_TMPL = {
    "ca": "Cal. Const. art. {art}, § {sec}",
    "tx": "Tex. Const. art. {art}, § {sec}",
    "ny": "N.Y. Const. art. {art}, § {sec}",
    "fl": "Fla. Const. art. {art}, § {sec}",
    "il": "Ill. Const. art. {art}, § {sec}",
    "pa": "Pa. Const. art. {art}, § {sec}",
    # default below if state not in map
}


@dataclass
class Section:
    state: str
    article_id: str
    section_number: str
    section_title: str
    raw_text: str
    source_url: str
    article_title: str = ""
    r2_html_url: Optional[str] = None
    r2_pdf_url: Optional[str] = None


def sha1_hex(s: str) -> str:
    return hashlib.sha1(s.encode()).hexdigest()


def point_id_for(act_id: str, chunk_idx: int, text: str) -> str:
    h = hashlib.md5(f"{act_id}::{chunk_idx}::{sha1_hex(text)[:12]}".encode()).hexdigest()
    return str(uuid.UUID(h))


def to_chunk_record(sec: Section) -> dict:
    text = sec.raw_text.strip()
    state_full = _FULL_STATE_NAME.get(sec.state, sec.state.upper())
    title_name = f"{state_full} Constitution"
    citation_tmpl = _CITATION_TMPL.get(sec.state)
    if not citation_tmpl:
        # generic fallback
        state_abbrev = sec.state.title()
        citation_tmpl = f"{state_abbrev}. Const. art. {{art}}, § {{sec}}"
    citation = citation_tmpl.format(art=sec.article_id, sec=sec.section_number)
    act_id = f"SCONST_{sec.state.upper()}_A{sec.article_id}_S{sec.section_number}"

    text_for_embedding = (
        f"{title_name} | {citation}\n"
        f"{sec.section_title or 'Section ' + sec.section_number}\n\n{text}"
    )
    formats = ["html"]
    if sec.r2_pdf_url:
        formats.append("pdf")
    formats.append("txt")

    md = {
        "act_id": act_id,
        "corpus_type": "state_constitution",
        "category": "state_constitution",
        "document_type": "constitution",
        "jurisdiction": "US",
        "country_code": "US",
        "state": sec.state,
        "title_name": title_name,
        "title": title_name,
        "top_level_title": f"constitution-{sec.state}",
        "title_code": f"const_{sec.state}",
        "level_classifier": "section",
        "chapter": None,
        "section_number": sec.section_number,
        "section_title": sec.section_title,
        "citation": citation,
        "citation_short": citation,
        "display_label": citation,
        "display_title": sec.section_title or f"Section {sec.section_number}",
        "display_path": f"Article {sec.article_id} / Section {sec.section_number}",
        "breadcrumb": [title_name, f"Article {sec.article_id}", f"Section {sec.section_number}"],
        "sort_key": act_id,
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
        "raw_node_id": act_id,
        "full_text_sha1": sha1_hex(text),
    }
    return {
        "point_id": point_id_for(act_id, 0, text),
        "text_for_embedding": text_for_embedding,
        "raw_text": text,
        "metadata": md,
    }


# ---------------------------------------------------------------------------
# California
# ---------------------------------------------------------------------------

CA_BASE = "https://leginfo.legislature.ca.gov/faces/codes_displayText.xhtml"
CA_ROMAN = ["I", "II", "III", "IIIB", "IV", "V", "VI", "VII", "VIII", "IX", "X", "XA",
            "XB", "XBA", "XI", "XII", "XIII", "XIIIA", "XIIIB", "XIIIC", "XIIID", "XIV",
            "XV", "XVI", "XVII", "XVIII", "XIX", "XIXA", "XIXB", "XIXC", "XIXD", "XX",
            "XXI", "XXII", "XXXIV", "XXXV"]


def scrape_ca(r2) -> list[Section]:
    """California Constitution from leginfo.legislature.ca.gov (article-by-article)."""
    out: list[Section] = []
    print(f"\n[CA] {len(CA_ROMAN)} candidate articles")
    for art in CA_ROMAN:
        url = f"{CA_BASE}?lawCode=CONS&article={art}"
        try:
            html = fetch_text(url)
        except Exception as e:
            print(f"  [CA art {art}] fetch FAIL: {e}")
            continue
        soup = BeautifulSoup(html, "html.parser")
        # The full code lives in a <div id="manylawsections"> with each section
        # as <span> or <h6> tags. The page is JSF-rendered, look for actual
        # statute markers.
        # On leginfo, the body content is under <div id="manylawsections">
        container = soup.find(id="manylawsections")
        if not container:
            # Fallback: extract all text and parse SECTION markers
            container = soup.find("body") or soup
        body_text = container.get_text("\n", strip=True)
        # Split on SECTION N. or SEC. N. markers
        parts = re.split(r"\n(?:SECTION|SEC\.)\s+(\d+(?:\.\d+)?[A-Z]?)\.\s*", body_text)
        if len(parts) <= 1:
            print(f"  [CA art {art}] no SECTION markers found, skip")
            continue
        # Drop the preamble that came before the first SECTION marker
        # (article header / navigation cruft)
        section_pairs = [(parts[i], parts[i + 1]) for i in range(1, len(parts) - 1, 2)]
        # Article title (first text block before SECTION 1)
        article_title = ""
        first_part = parts[0]
        m = re.search(r"ARTICLE\s+[\w\.]+\s+([A-Z][A-Z, ]+?)(?:\n|\[|\s+\(|$)", first_part[:500])
        if m:
            article_title = m.group(1).title().strip()

        # Upload article HTML once
        r2_html_key = f"state_constitutions/ca/source/article_{art}.html"
        put_if_changed(r2, r2_html_key, html.encode("utf-8"), "text/html; charset=utf-8")
        r2_html_url = public_url(r2_html_key)

        for sec_num, sec_text in section_pairs:
            text = re.sub(r"\s+", " ", sec_text).strip()
            if not text or len(text) < 5:
                continue
            sec = Section(
                state="ca",
                article_id=art,
                section_number=sec_num,
                section_title=f"California Constitution Article {art}, Section {sec_num}",
                article_title=article_title,
                raw_text=text,
                source_url=url,
                r2_html_url=r2_html_url,
            )
            # Upload per-section TXT
            r2_txt_key = f"state_constitutions/ca/sections/SCONST_CA_A{art}_S{sec_num}.txt"
            put_if_changed(r2, r2_txt_key, sec.raw_text.encode("utf-8"), "text/plain; charset=utf-8")
            sec.r2_txt_url = public_url(r2_txt_key)
            out.append(sec)
        print(f"  [CA art {art}] {len(section_pairs)} sections")
    print(f"[CA] done: {len(out)} sections total")
    return out


# ---------------------------------------------------------------------------
# Texas
# ---------------------------------------------------------------------------

TX_BASE = "https://tcss.legis.texas.gov/resources/CN"


def scrape_tx(r2) -> list[Section]:
    """Texas Constitution from tcss.legis.texas.gov (htm files per article).

    Previously used statutes.capitol.texas.gov which as of 2026 returns a 250 KB
    Angular SPA shell instead of the htm content: the section-split regex yields
    zero matches and the loop `continue`s silently, so no rows land. The
    tcss.legis.texas.gov mirror still serves the original htm output.
    """
    out: list[Section] = []
    # TX Constitution: 17 articles (I-XVII)
    # URL pattern: /Docs/CN/htm/CN.{N}.htm  (where N is 1-17, sometimes with letters)
    for n in range(1, 18):
        url = f"{TX_BASE}/htm/CN.{n}.htm"
        try:
            html = fetch_text(url)
        except Exception as e:
            print(f"  [TX art {n}] fetch FAIL: {e}")
            continue
        soup = BeautifulSoup(html, "html.parser")
        body_text = soup.get_text("\n", strip=True)
        # TX uses Sec. N.NN format
        parts = re.split(r"\n(?:Sec\.|SECTION|SEC\.)\s+(\d+(?:[a-z]?(?:-\d+)?))\.\s*", body_text)
        if len(parts) <= 1:
            continue
        section_pairs = [(parts[i], parts[i + 1]) for i in range(1, len(parts) - 1, 2)]

        r2_html_key = f"state_constitutions/tx/source/article_{n}.html"
        put_if_changed(r2, r2_html_key, html.encode("utf-8"), "text/html; charset=utf-8")
        r2_html_url = public_url(r2_html_key)

        for sec_num, sec_text in section_pairs:
            text = re.sub(r"\s+", " ", sec_text).strip()
            if not text or len(text) < 5:
                continue
            sec = Section(
                state="tx",
                article_id=str(n),
                section_number=sec_num,
                section_title=f"Texas Constitution Article {n}, Section {sec_num}",
                raw_text=text,
                source_url=url,
                r2_html_url=r2_html_url,
            )
            r2_txt_key = f"state_constitutions/tx/sections/SCONST_TX_A{n}_S{sec_num}.txt"
            put_if_changed(r2, r2_txt_key, sec.raw_text.encode("utf-8"), "text/plain; charset=utf-8")
            sec.r2_txt_url = public_url(r2_txt_key)
            out.append(sec)
        print(f"  [TX art {n}] {len(section_pairs)} sections")
    print(f"[TX] done: {len(out)} sections total")
    return out


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Wikisource generic scraper — inline-Section-marker style
# (Works for: VA, NC, WI; the others on Wikisource are stub/article-list pages.)
# ---------------------------------------------------------------------------

# Wikisource state constitution slugs.
# Verified live 2026-05-27 by probing https://en.wikisource.org/wiki/<slug>
# and confirming non-trivial page bodies (>5 KB). Wikisource hosts a remarkably
# complete set: 37 of 49 jurisdictions (50 states + DC, excluding ones with
# bespoke scrapers below). Missing on Wikisource as of probe date:
#   al, ar, ia, in, ky, me, mi, nd, pa, ri, ut, dc
# These need bespoke scrapers against state .gov sites (TODO).
_WS_INLINE_STATES = {
    "ak": "Constitution_of_the_State_of_Alaska",
    "az": "Constitution_of_Arizona",
    # Disambig fixes (probed 2026-05-27 — main "Constitution_of_X" was a
    # versions-list disambig page, picked most-recent year-stamped child).
    "co": "Constitution_of_the_State_of_Colorado_(2020)",
    "ct": "Constitution_of_Connecticut",
    "de": "Constitution_of_Delaware_(2023)",
    "fl": "Constitution_of_the_State_of_Florida_(1968)",
    "ga": "Constitution_of_Georgia_(2018)",
    "hi": "1950_Constitution_of_the_State_of_Hawaii",
    "id": "Constitution_of_the_State_of_Idaho_(2017)",
    "il": "Illinois_Constitution_of_1970",
    "ks": "Kansas_Constitution",
    "la": "Louisiana_State_Constitution_(1974)",
    "ma": "Constitution_of_the_Commonwealth_of_Massachusetts_(1853)",
    "md": "Constitution_of_Maryland",
    "mn": "Constitution_of_the_State_of_Minnesota_(2016)",
    "mo": "Constitution_of_the_State_of_Missouri_(1945)",
    "ms": "Mississippi_Constitution_(1890)",
    "mt": "Montana_Constitution",
    "nc": "North_Carolina_Constitution",
    "ne": "Nebraska_Constitution",
    "nh": "New_Hampshire_Constitution_(2019)",
    "nj": "New_Jersey_Constitution_of_1947",
    "nm": "Constitution_of_New_Mexico",
    "nv": "Constitution_of_Nevada",
    "ny": "New_York_Constitution_as_of_2004",
    "oh": "Ohio_Constitution_of_1912",
    "ok": "Constitution_of_Oklahoma",
    "or": "Oregon_Constitution",
    "sc": "Constitution_of_South_Carolina",
    "sd": "Constitution_of_South_Dakota",
    "tn": "Constitution_of_the_State_of_Tennessee_(2011)",
    "va": "Constitution_of_Virginia",
    "vt": "Constitution_of_the_State_of_Vermont",
    "wa": "Washington_State_Constitution",
    "wi": "Constitution_of_Wisconsin",
    "wv": "West_Virginia_State_Constitution",
    "wy": "Wyoming_Constitution",
}


def scrape_wikisource_inline(state: str, slug: str, r2) -> list[Section]:
    """Scrape a Wikisource state constitution page that has inline Section markers."""
    url = f"https://en.wikisource.org/wiki/{slug}"
    out: list[Section] = []
    try:
        html = fetch_text(url)
    except Exception as e:
        print(f"  [{state}] fetch FAIL: {e}")
        return out

    # Mirror the source HTML once
    r2_html_key = f"state_constitutions/{state}/source/wikisource_main.html"
    put_if_changed(r2, r2_html_key, html.encode("utf-8"), "text/html; charset=utf-8")
    r2_html_url = public_url(r2_html_key)

    soup = BeautifulSoup(html, "html.parser")
    # Wikisource pages vary on which div holds the actual constitution text:
    #   - Most state pages: content in `div.mw-parser-output`.
    #   - Some (FL 1968, OR, WY): `mw-parser-output` is a thin disambig
    #     wrapper but the body lives in `div.mw-content-ltr` (Wikisource's
    #     newer template structure).
    # Pick whichever yields more body text.
    candidates = [soup.find("div", class_="mw-parser-output"),
                  soup.find("div", class_="mw-content-ltr"),
                  soup.find("div", class_="mw-content-text")]
    candidates = [c for c in candidates if c is not None]
    if candidates:
        body = max(candidates, key=lambda d: len(d.get_text("\n", strip=True)))
    else:
        body = soup
    body_text = body.get_text("\n", strip=True)

    # Walk Article + Section markers. Wikisource uses many heading styles:
    #   "Article I" / "ARTICLE I" alone on a line (the easy case)
    #   "ARTICLE I BILL OF RIGHTS" (article number + title on same line — NY, OH)
    #   "Article I. Bill of Rights" (article number with dot + title)
    #   "Article 1: Executive" (arabic numeral + colon — KS)
    #   "Article I: DECLARATION OF RIGHTS" (colon + all-caps title — FL)
    # Strategy: split on lines that START with ARTICLE/Article and a Roman or
    # arabic numeral, capturing only the numeral (everything after — name,
    # period, colon, etc. — is consumed up to end-of-line).
    art_parts = re.split(
        r"\n(?:ARTICLE|Article)\s+([IVXLC\d]+(?:[\.\-][IVXLC\d]+)?(?:[A-Z])?)[\.\:]?(?:[ \t][^\n]*)?\n",
        body_text,
    )
    if len(art_parts) <= 1:
        # No Article splits — treat whole body as single article
        art_parts = ["", "I", body_text]
    art_iter = [(art_parts[i], art_parts[i + 1]) for i in range(1, len(art_parts) - 1, 2)]

    for art_id, art_body in art_iter:
        # Split on Section markers within this article
        sec_parts = re.split(r"\n(?:SECTION|Section|Sec\.)\s+(\d+(?:\.\d+)?[A-Za-z]?)\.?\s*", art_body)
        if len(sec_parts) <= 1:
            # Fall back: emit article as a single record.
            # (Previously capped at 5000 chars — that produced audit-flagged
            # mid-word truncations on WI/NJ/KS/WV/SD/AK/MD articles. Voyage-4
            # accepts ~32k tokens per input, so passing the full body is safe.
            # If an article exceeds ~20k chars we log a warning so we can add
            # proper multi-chunk emission later.)
            text = re.sub(r"\s+", " ", art_body).strip()
            if len(text) > 20000:
                print(f"  [warn] {state}/A{art_id}: fallback article is {len(text):,} chars; "
                      "consider chunking (currently embedded as a single record)")
            if text:
                sec = Section(state=state, article_id=art_id, section_number="0",
                              section_title=f"{state.upper()} Const., Article {art_id}",
                              raw_text=text, source_url=url, r2_html_url=r2_html_url)
                out.append(sec)
            continue
        sec_iter = [(sec_parts[i], sec_parts[i + 1]) for i in range(1, len(sec_parts) - 1, 2)]
        for sec_num, sec_text in sec_iter:
            text = re.sub(r"\s+", " ", sec_text).strip()
            if not text or len(text) < 5:
                continue
            sec = Section(state=state, article_id=art_id, section_number=sec_num,
                          section_title=f"{state.upper()} Const., Article {art_id}, Section {sec_num}",
                          raw_text=text, source_url=url, r2_html_url=r2_html_url)
            r2_txt_key = f"state_constitutions/{state}/sections/SCONST_{state.upper()}_A{art_id}_S{sec_num}.txt"
            put_if_changed(r2, r2_txt_key, sec.raw_text.encode("utf-8"), "text/plain; charset=utf-8")
            sec.r2_txt_url = public_url(r2_txt_key)
            out.append(sec)
    print(f"  [{state}] wikisource inline: {len(out)} sections from {url}")
    return out


def scrape_wikisource_factory(state: str, slug: str):
    """Return a scraper closure for a Wikisource-inline state."""
    return lambda r2: scrape_wikisource_inline(state, slug, r2)


# ---------------------------------------------------------------------------
# Pennsylvania — legis.state.pa.us serves each Article as its own URL at
# /WU01/LI/LI/CT/HTM/00/00.{N:03d}..HTM  (note the literal double-dot before
# .HTM). 11 articles (I-XI). Each article's body contains all its §-numbered
# sections inline. Geo-restricted; needs US proxy.
# ---------------------------------------------------------------------------

PA_ARTICLE_URL_TMPL = "https://www.legis.state.pa.us/WU01/LI/LI/CT/HTM/00/00.{n:03d}..HTM"

# Roman numeral mapping for article IDs 1..11
_PA_ROMAN = ["I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", "X", "XI"]

# Section-body marker: "§ N.  Section heading.\n  body text..."
# We use a non-greedy capture of everything until the NEXT § marker (or
# end-of-text). Internal `00c1NNs` anchor markers and amendment notes
# (lines starting with "(Date,") are scrubbed downstream.
_PA_SECTION_RE = re.compile(
    r"§\s*(\d+(?:\.\d+)?[A-Za-z]?)\.\s+([^\n]+?)\n([\s\S]*?)(?=\n§\s*\d|\Z)",
    re.MULTILINE,
)


def _clean_pa_section_body(raw: str) -> str:
    """Strip internal anchor markers like '00c103s', amendment lineage notes,
    and excessive whitespace from a PA section body.
    """
    # Drop anchor lines like 00c103s, 00c106v
    raw = re.sub(r"\n\s*00[a-zA-Z0-9]+s?\s*\n", "\n", raw)
    raw = re.sub(r"\n\s*00[a-zA-Z0-9]+s?\s*$", "", raw)
    # Drop lines that are just bare amendment-date parentheticals
    raw = re.sub(r"\n\s*\([A-Z][a-z]+\.\s+\d+,\s+\d{4}[^\n]*\)\s*", " ", raw)
    raw = re.sub(r"\s+", " ", raw).strip()
    return raw


def scrape_pa(r2) -> list[Section]:
    out: list[Section] = []
    for n, art_id in enumerate(_PA_ROMAN, start=1):
        url = PA_ARTICLE_URL_TMPL.format(n=n)
        try:
            html = fetch_text(url, use_us_proxy=True)
        except Exception as e:
            print(f"  [PA art {art_id}] fetch FAIL: {e}")
            continue

        r2_html_key = f"state_constitutions/pa/source/article_{art_id}.html"
        put_if_changed(r2, r2_html_key, html.encode("utf-8"), "text/html; charset=utf-8")
        r2_html_url = public_url(r2_html_key)

        soup = BeautifulSoup(html, "html.parser")
        body_text = soup.get_text("\n", strip=True)

        # Discover the article title (e.g. "DECLARATION OF RIGHTS" for Art I).
        # It appears as a line right after the line "ARTICLE I" / "ARTICLE II".
        art_title = ""
        m_at = re.search(rf"\nARTICLE\s+{re.escape(art_id)}\s*\n([^\n]+?)\n", body_text)
        if m_at:
            art_title = m_at.group(1).strip().title()

        sec_iter = list(_PA_SECTION_RE.finditer(body_text))
        count_this_art = 0
        for sm in sec_iter:
            sec_num = sm.group(1).strip()
            sec_head = re.sub(r"\s+", " ", sm.group(2)).strip().rstrip(".")
            sec_body = _clean_pa_section_body(sm.group(3))
            if len(sec_body) < 20:
                continue
            sec = Section(
                state="pa",
                article_id=art_id,
                section_number=sec_num,
                section_title=f"Pa. Const. art. {art_id}, § {sec_num}. {sec_head}",
                article_title=art_title,
                raw_text=sec_body,
                source_url=url,
                r2_html_url=r2_html_url,
            )
            r2_txt_key = f"state_constitutions/pa/sections/SCONST_PA_A{art_id}_S{sec_num}.txt"
            put_if_changed(r2, r2_txt_key, sec.raw_text.encode("utf-8"), "text/plain; charset=utf-8")
            sec.r2_txt_url = public_url(r2_txt_key)
            out.append(sec)
            count_this_art += 1
        print(f"  [PA art {art_id}] {count_this_art} sections")
    print(f"[PA] done: {len(out)} sections total")
    return out


# ---------------------------------------------------------------------------
# Kentucky — apps.legislature.ky.gov hosts the constitution as a TOC plus
# per-section sub-pages identified by ?rsn=N. KY's constitution is mostly
# flat (Bill of Rights then numbered sections 1-263+; no formal Articles).
# We assign all sections to Article "I" and use the actual section number.
# ---------------------------------------------------------------------------

KY_CONST_TOC = "https://apps.legislature.ky.gov/law/constitution"
KY_BASE = "https://apps.legislature.ky.gov"


def scrape_ky(r2) -> list[Section]:
    out: list[Section] = []
    try:
        toc_html = fetch_text(KY_CONST_TOC, use_us_proxy=True)
    except Exception as e:
        print(f"  [KY] TOC fetch FAIL: {e}")
        return out

    soup = BeautifulSoup(toc_html, "html.parser")
    # Mirror TOC HTML
    r2_html_key = "state_constitutions/ky/source/toc.html"
    put_if_changed(r2, r2_html_key, toc_html.encode("utf-8"), "text/html; charset=utf-8")
    toc_html_url = public_url(r2_html_key)

    section_specs: list[tuple[str, str, str]] = []  # (sec_id, sec_title, url)
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/Law/Constitution/Constitution/ViewConstitution" not in href:
            continue
        text = a.get_text(strip=True)
        m = re.match(r"Section\s+(\d+[A-Za-z]?)\s*[.\-…]\s*(.*)", text)
        if not m:
            continue
        sec_num = m.group(1)
        sec_title = m.group(2).strip().rstrip(".")
        full_url = href if href.startswith("http") else f"{KY_BASE}{href}"
        section_specs.append((sec_num, sec_title, full_url))

    print(f"  [KY] discovered {len(section_specs)} sections")
    if not section_specs:
        return out

    # Crawl section pages in parallel (8 workers — KY site is slow with proxy)
    def _fetch_one(spec):
        sec_num, sec_title, url = spec
        try:
            html = fetch_text(url, use_us_proxy=True)
        except Exception as e:
            return None
        body = BeautifulSoup(html, "html.parser")
        # The section text lives in the main content panel. Heuristic: find
        # all <div>/<p> elements with substantial text inside the body. The
        # actual section text appears after the heading "Section N - Title".
        main = (body.find("main") or body.find("div", id="MainContent")
                or body.find("div", class_=re.compile("content", re.I)) or body)
        text = main.get_text("\n", strip=True)
        # Strip the long site-wide nav prefix that appears before the actual
        # section header. Find "Section N" anchor and slice from there.
        anchor = re.search(rf"Section\s+{re.escape(sec_num)}\s*[.\-…]", text)
        if anchor:
            text = text[anchor.start():]
        # Trim footer (everything after "Text as Ratified" if present, or
        # cut at "Source:" or "© ")
        for trail in ["\n© ", "\nPrint this page", "\nReturn to top"]:
            idx = text.find(trail)
            if idx > 0:
                text = text[:idx]
                break
        text = re.sub(r"\s+", " ", text).strip()
        return (sec_num, sec_title, url, text, html)

    with cf.ThreadPoolExecutor(max_workers=8) as ex:
        results = list(ex.map(_fetch_one, section_specs))

    fetched = sum(1 for r in results if r is not None)
    print(f"  [KY] fetched {fetched} / {len(section_specs)} section pages")

    for result in results:
        if result is None:
            continue
        sec_num, sec_title, url, text, raw_html = result
        if len(text) < 30:
            continue
        sec = Section(
            state="ky",
            article_id="I",
            section_number=sec_num,
            section_title=f"Ky. Const. § {sec_num}",
            article_title=sec_title,
            raw_text=text,
            source_url=url,
            r2_html_url=toc_html_url,
        )
        r2_txt_key = f"state_constitutions/ky/sections/SCONST_KY_A_I_S{sec_num}.txt"
        put_if_changed(r2, r2_txt_key, sec.raw_text.encode("utf-8"), "text/plain; charset=utf-8")
        sec.r2_txt_url = public_url(r2_txt_key)
        out.append(sec)
    print(f"[KY] done: {len(out)} sections")
    return out


# ---------------------------------------------------------------------------
# Michigan — legislature.mi.gov serves the full constitution as a 2.1 MB PDF
# at /documents/publications/constitution.pdf. We download it, extract text
# with pdfplumber, and split by "ARTICLE I", "ARTICLE II", ... headers and
# nested "§ N" markers.
# Geo-restricted; needs US proxy.
# ---------------------------------------------------------------------------

MI_CONST_PDF = "https://www.legislature.mi.gov/documents/publications/constitution.pdf"

_MI_ARTICLE_RE = re.compile(
    r"\n\s*ARTICLE\s+([IVXL]+)\s*\n([^\n]+?)\n",
    re.IGNORECASE,
)
_MI_SECTION_RE = re.compile(
    r"§\s*(\d+[A-Za-z]?)\.?\s+(.*?)(?=\n§\s*\d|\Z)",
    re.MULTILINE | re.DOTALL,
)


def _fetch_bytes_proxy(url: str, retries: int = 3) -> Optional[bytes]:
    proxies = _us_proxies()
    headers = {"User-Agent": _MOZ_UA}
    for attempt in range(retries):
        try:
            r = SESSION.get(url, timeout=120, allow_redirects=True,
                            proxies=proxies, headers=headers)
            r.raise_for_status()
            return r.content
        except Exception:
            if attempt == retries - 1:
                return None
            time.sleep(2.0)
    return None


def _pdf_to_text(pdf_bytes: bytes) -> str:
    try:
        import pdfplumber
        import io
        parts: list[str] = []
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                t = page.extract_text() or ""
                if t.strip():
                    parts.append(t)
        return "\n\n".join(parts)
    except Exception as e:
        print(f"  ! pdf extract failed: {e}")
        return ""


def scrape_mi(r2) -> list[Section]:
    out: list[Section] = []
    pdf_bytes = _fetch_bytes_proxy(MI_CONST_PDF)
    if not pdf_bytes:
        print("  [MI] PDF fetch FAIL")
        return out
    # R2-mirror the PDF
    r2_pdf_key = "state_constitutions/mi/source/mi_constitution.pdf"
    put_if_changed(r2, r2_pdf_key, pdf_bytes, "application/pdf")
    r2_pdf_url = public_url(r2_pdf_key)

    text = _pdf_to_text(pdf_bytes)
    if not text:
        return out

    # Find article boundaries
    art_matches = list(_MI_ARTICLE_RE.finditer(text))
    if not art_matches:
        print(f"  [MI] no ARTICLE markers found in {len(text)} chars; abort")
        return out

    for i, m in enumerate(art_matches):
        art_id = m.group(1).strip()
        art_title = re.sub(r"\s+", " ", m.group(2)).strip()
        start = m.end()
        end = art_matches[i + 1].start() if i + 1 < len(art_matches) else len(text)
        art_body = text[start:end]

        sec_iter = list(_MI_SECTION_RE.finditer(art_body))
        if not sec_iter:
            # Whole article as one chunk
            body_clean = re.sub(r"\s+", " ", art_body).strip()
            if len(body_clean) >= 100:
                sec = Section(
                    state="mi",
                    article_id=art_id,
                    section_number="0",
                    section_title=f"Mich. Const. art. {art_id}",
                    article_title=art_title,
                    raw_text=body_clean,
                    source_url=MI_CONST_PDF,
                    r2_pdf_url=r2_pdf_url,
                )
                out.append(sec)
            continue

        for sm in sec_iter:
            sec_num = sm.group(1).strip()
            sec_body = re.sub(r"\s+", " ", sm.group(2)).strip()
            if len(sec_body) < 30:
                continue
            sec = Section(
                state="mi",
                article_id=art_id,
                section_number=sec_num,
                section_title=f"Mich. Const. art. {art_id}, § {sec_num}",
                article_title=art_title,
                raw_text=sec_body,
                source_url=MI_CONST_PDF,
                r2_pdf_url=r2_pdf_url,
            )
            r2_txt_key = f"state_constitutions/mi/sections/SCONST_MI_A{art_id}_S{sec_num}.txt"
            put_if_changed(r2, r2_txt_key, sec.raw_text.encode("utf-8"), "text/plain; charset=utf-8")
            sec.r2_txt_url = public_url(r2_txt_key)
            out.append(sec)
    print(f"[MI] done: {len(out)} sections from PDF ({len(text)} chars extracted)")
    return out


STATE_SCRAPERS: dict[str, Callable] = {
    "ca": scrape_ca,
    "tx": scrape_tx,
    "pa": scrape_pa,
    "ky": scrape_ky,
    "mi": scrape_mi,
}
# Auto-register Wikisource-inline states
for _st, _slug in _WS_INLINE_STATES.items():
    STATE_SCRAPERS[_st] = scrape_wikisource_factory(_st, _slug)


def merge_jsonl(path: Path, new_secs: list[Section]) -> int:
    """Append-safe merge by act_id."""
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
    for sec in new_secs:
        rec = to_chunk_record(sec)
        existing[rec["metadata"]["act_id"]] = rec
    with open(path, "w", encoding="utf-8") as fh:
        for rec in existing.values():
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
    return len(existing)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--states", default="",
                    help="Comma-separated state codes (ca,tx,ny,fl,...).")
    ap.add_argument("--all", action="store_true",
                    help="Run all configured state scrapers.")
    ap.add_argument("--workers", type=int, default=8,
                    help="Number of parallel state scrapers (default: 8).")
    args = ap.parse_args()

    _load_env()
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if args.all:
        state_codes = list(STATE_SCRAPERS.keys())
    elif args.states:
        state_codes = [s.strip().lower() for s in args.states.split(",") if s.strip()]
    else:
        ap.error("specify --states ca,tx or --all")

    workers = max(1, args.workers)
    print(f"=== State Constitutions: {len(state_codes)} states (workers={workers}) ===")
    all_secs: list[Section] = []
    r2 = None  # R2 mirror disabled in the open release

    def _run_one(st: str) -> tuple[str, list[Section] | None, str | None]:
        if st not in STATE_SCRAPERS:
            return (st, None, "no scraper configured")
        try:
            return (st, STATE_SCRAPERS[st](r2), None)
        except Exception as e:  # noqa: BLE001
            return (st, None, str(e)[:200])

    if workers > 1 and len(state_codes) > 1:
        from concurrent.futures import ThreadPoolExecutor, as_completed
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures = [ex.submit(_run_one, st) for st in state_codes]
            for fut in as_completed(futures):
                st, secs, err = fut.result()
                if err:
                    print(f"  [{st}] FAIL: {err}")
                elif secs is not None:
                    all_secs.extend(secs)
    else:
        for st in state_codes:
            _, secs, err = _run_one(st)
            if err:
                print(f"  [{st}] FAIL: {err}")
            elif secs is not None:
                all_secs.extend(secs)

    total = merge_jsonl(OUT, all_secs)
    print(f"\n=== JSONL has {total} state-constitution chunks at {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
