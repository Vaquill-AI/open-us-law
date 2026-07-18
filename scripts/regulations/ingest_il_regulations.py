#!/usr/bin/env python3
"""Ingest the Illinois Administrative Code (IAC) — Illinois' state regulations.

OFFICIAL SOURCE ONLY: Illinois General Assembly, Joint Committee on Administrative
Rules (JCAR). The JCAR maintains the IAC database and publishes it weekly on
ilga.gov. The public HTML landing at https://ilga.gov/agencies/JCAR/AdminCode
serves a server-rendered Title/Part/Section catalogue, but its Part/Section
detail endpoints currently return a soft-404 error template.

Fortunately, JCAR also exposes the SAME data as a flat HTML mirror on its FTP
tree under https://ilga.gov/ftp/JCAR/AdminCode/. Every section is its own
plain HTML file generated from the official Word source. We harvest that tree
directly - no aggregators, no Justia/ZenRows/etc. (DOCX mirrors of all parts
sit alongside under /ftp/JCAR/AdminCodeDoc/<title>/, but we don't need them:
the HTML is lossless.)

URL pattern (case-sensitive):

    /ftp/JCAR/AdminCode/                              → 33 title directories
    /ftp/JCAR/AdminCode/<title>/                      → list of section files
    /ftp/JCAR/AdminCode/<title>/<TTTPPPPP[L]SSSSSR.html>
        TTT     zero-padded title number (e.g. 008)
        PPPPP   zero-padded part number (e.g. 00001 → Part 1)
        L       optional subpart letter (A, B, C, ...) for parts that subdivide
        SSSSS   zero-padded section number with implicit 2 decimals
                (e.g. 00100 → Section 1.10, 00150 → Section 1.15, 02050 →
                Section 20.50). For titles like 068, the section already encodes
                its dotted form: 00000050 → 50.
        R       file-type suffix; only R (Rule/section) is published as HTML.
                P (Part heading), A/E/G/I/K/L/M (other metadata blocks) are
                only available as .docx under /ftp/JCAR/AdminCodeDoc/.

IAC hierarchy: Title (e.g. "TITLE 8: AGRICULTURE AND ANIMALS") → Chapter
(e.g. "CHAPTER I: DEPARTMENT OF AGRICULTURE") → Subchapter
(e.g. "SUBCHAPTER a: GENERAL RULES") → Part (e.g. "PART 1") → optional Subpart
(e.g. "SUBPART B") → Section (e.g. "Section 1.75"). Sections are the citable
unit. Official cite form: "<title> Ill. Adm. Code <part>.<section>" (e.g.
"8 Ill. Adm. Code 1.75"). NOTE: this is the ADMIN code; IL Supreme Court rules
live in a separate corpus and are handled by ingest_state_court_rules.py.

Each section HTML embeds the full breadcrumb in a header div and a
<meta name="sectionname"> tag, plus a "(Source:  Amended at <volume> Ill. Reg.
<page>, effective <date>)" footer that carries the effective date AND the
Illinois Register citation that adopted/amended the rule. Rich metadata
captured into structured fields (NEVER discarded):

    Source footer       → effective_date, register_citations
                          (e.g. "16 Ill. Reg. 15850"),
                          source_history (the raw "(Source: ...)" string)
    sectionname meta    → display_title (canonical mixed-case form)
    Breadcrumb          → issuing_agency (the CHAPTER line, which is the
                          state agency), subchapter_name, part_name

corpus_type='state_regulation'. act_id='STATE_IL_IAC_T<title>_P<part>_S<section>'.

Geo-restricted; Webshare US proxy + Mozilla UA + polite pacing. The FTP tree
is unauthenticated HTTP-on-HTTPS and tolerates several concurrent connections,
but we keep workers low to be neighborly.
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

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = Path(os.environ.get("OUT_DIR", "./data"))
OUT = DATA_DIR / "state_il_regulations.jsonl"

IL_BASE = "https://ilga.gov"
IL_ROOT = f"{IL_BASE}/ftp/JCAR/AdminCode/"

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


def fetch(url: str, retries: int = 6) -> str | None:
    """Fetch raw text. Retries 429 with exponential backoff and tolerates the
    transient SSL/connection errors that rotating proxies occasionally raise."""
    proxies = _us_proxies()
    for attempt in range(retries):
        try:
            r = SESSION.get(url, timeout=90, proxies=proxies, allow_redirects=True)
            if r.status_code == 200:
                # Honor the page's declared encoding (IL pages are windows-1252).
                if r.encoding is None or r.encoding.lower() == "iso-8859-1":
                    r.encoding = "windows-1252"
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


# ---------------------------------------------------------------------------
# Discovery: title directories -> section file URLs
# ---------------------------------------------------------------------------

# Title directories are 3-digit zero-padded numbers (001..095).
_TITLE_DIR_RE = re.compile(r"^/ftp/JCAR/AdminCode/(\d{3})/?$", re.IGNORECASE)
# Section file: TTTPPPPP[L]SSSSSR.html  (subpart letter optional)
_SECTION_FILE_RE = re.compile(r"^(\d{3})(\d{5})([A-Za-z])?(\d{4,6})R\.html$")


def list_titles() -> list[str]:
    """Return sorted 3-digit title numbers (e.g. ['001', '002', ..., '095'])."""
    html = fetch(IL_ROOT)
    if not html:
        raise RuntimeError("could not fetch IL AdminCode root")
    soup = BeautifulSoup(html, "html.parser")
    seen: set[str] = set()
    for a in soup.find_all("a", href=True):
        m = _TITLE_DIR_RE.match(a["href"].strip())
        if m:
            seen.add(m.group(1))
    return sorted(seen)


def list_sections_in_title(title: str) -> list[tuple[str, str]]:
    """Return list of (section_url, filename) tuples for a title directory.

    Only includes "R" files (Rule/section content). P/A/E/G/I/K/L/M files
    are not published as HTML - they are .docx-only and live under
    /ftp/JCAR/AdminCodeDoc/<title>/.
    """
    url = f"{IL_ROOT}{title}/"
    html = fetch(url)
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    out: list[tuple[str, str]] = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href.endswith("R.html"):
            continue
        fn = href.rsplit("/", 1)[-1]
        if not _SECTION_FILE_RE.match(fn):
            continue
        full = href if href.startswith("http") else f"{IL_BASE}{href}"
        out.append((full, fn))
    return out


# ---------------------------------------------------------------------------
# Section parsing
# ---------------------------------------------------------------------------

# (Source:  Amended at 16 Ill. Reg. 15850, effective October 5, 1992)
# (Source:  Added at 47 Ill. Reg. 4444, effective April 1, 2023)
# (Source:  Old Section ... repealed ... New Section ... effective ...)
_SOURCE_RE = re.compile(r"\(Source:\s*(.+?)\)\s*$", re.DOTALL)
_ILL_REG_RE = re.compile(r"\b(\d{1,3})\s+Ill\.\s*Reg\.\s+(\d{1,6})\b", re.IGNORECASE)
_MONTHS = "January|February|March|April|May|June|July|August|September|October|November|December"
_EFFECTIVE_RE = re.compile(
    rf"effective\s+((?:{_MONTHS})\s+\d{{1,2}},\s+\d{{4}})",
    re.IGNORECASE,
)
_RESERVED_PAT = re.compile(r"\[(repealed|reserved|renumbered)\b", re.IGNORECASE)


def _dedupe(xs: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for x in xs:
        k = re.sub(r"\s+", " ", x).strip()
        if k and k not in seen:
            seen.add(k)
            out.append(k)
    return out


def _parse_filename(fn: str) -> tuple[str, str, str, str] | None:
    """Return (title, part, subpart_letter, raw_section_digits) from filename.

    Subpart letter is '' if the file lacks one (e.g. titles that don't use
    Word subpart bookmarks, like Title 68 Professions).
    """
    m = _SECTION_FILE_RE.match(fn)
    if not m:
        return None
    title, part, subpart, sec_digits = m.group(1), m.group(2), m.group(3) or "", m.group(4)
    return title, part, subpart, sec_digits


def _section_number_from_meta(sectionname: str) -> str | None:
    """Extract the dotted section number from the <meta name=sectionname> value
    (e.g. "Section 1.10  Definitions" -> "1.10")."""
    m = re.match(r"^\s*Section\s+([\w.\-]+)\b", sectionname, re.IGNORECASE)
    return m.group(1) if m else None


@dataclass
class Section:
    title_num: str  # zero-padded, e.g. "008"
    title_name: str  # e.g. "AGRICULTURE AND ANIMALS"
    chapter_label: str  # e.g. "CHAPTER I: DEPARTMENT OF AGRICULTURE"
    issuing_agency: str  # e.g. "DEPARTMENT OF AGRICULTURE"
    subchapter_label: str  # e.g. "SUBCHAPTER a: GENERAL RULES"
    part_num: str  # part as integer-string, e.g. "1"
    part_name: str  # e.g. "ADMINISTRATIVE RULES (...)"
    subpart_letter: str  # e.g. "B" or ""
    subpart_name: str  # if exposed in breadcrumb, else ""
    section_num: str  # e.g. "1.10"
    section_title: str  # e.g. "Definitions"
    raw_text: str
    source_url: str
    effective_date: str = ""  # latest effective date from "(Source: ...)"
    register_citations: list[str] = field(default_factory=list)  # Ill. Reg. cites
    source_history: str = ""  # raw "(Source: ...)" string


def _clean_breadcrumb_line(line: str) -> str:
    return re.sub(r"\s+", " ", line).strip().strip(":").strip()


def _parse_breadcrumb(div) -> dict:
    """Extract TITLE / CHAPTER / SUBCHAPTER / PART / SUBPART lines from the
    centered heading div at the top of every IAC section page."""
    # IL renders breadcrumb as text separated by <br> inside the heading div.
    raw = div.get_text("\n", strip=True) if div else ""
    out = {
        "title_name": "",
        "chapter_label": "",
        "issuing_agency": "",
        "subchapter_label": "",
        "part_num": "",
        "part_name": "",
        "subpart_letter": "",
        "subpart_name": "",
    }
    if not raw:
        return out
    for line in raw.split("\n"):
        line = line.strip()
        if not line:
            continue
        upper = line.upper()
        if upper.startswith("TITLE "):
            # "TITLE 8: AGRICULTURE AND ANIMALS"
            m = re.match(r"TITLE\s+\d+\s*:?\s*(.+)$", line, re.IGNORECASE)
            if m:
                out["title_name"] = _clean_breadcrumb_line(m.group(1))
        elif upper.startswith("CHAPTER "):
            out["chapter_label"] = _clean_breadcrumb_line(line)
            m = re.match(r"CHAPTER\s+[IVXLCDM\d]+\s*:?\s*(.+)$", line, re.IGNORECASE)
            if m:
                out["issuing_agency"] = _clean_breadcrumb_line(m.group(1))
        elif upper.startswith("SUBCHAPTER "):
            out["subchapter_label"] = _clean_breadcrumb_line(line)
        elif upper.startswith("PART "):
            # IL sometimes squashes spaces: "PART 1ADMINISTRATIVE RULES(...)".
            m = re.match(r"PART\s+(\w+)\s*(.*)$", line, re.IGNORECASE)
            if m:
                out["part_num"] = m.group(1).strip()
                out["part_name"] = _clean_breadcrumb_line(m.group(2))
        elif upper.startswith("SUBPART "):
            m = re.match(r"SUBPART\s+([A-Z])\s*:?\s*(.*)$", line, re.IGNORECASE)
            if m:
                out["subpart_letter"] = m.group(1).upper()
                out["subpart_name"] = _clean_breadcrumb_line(m.group(2))
    return out


def _strip_source_block(text: str) -> tuple[str, str, list[str], str]:
    """Extract the "(Source: ...)" tail from the section body. Returns
    (body_without_source, effective_date, register_citations, raw_source)."""
    m = _SOURCE_RE.search(text)
    if not m:
        return text.strip(), "", [], ""
    raw = m.group(1).strip()
    # last "effective <date>" wins (Source blocks sometimes list multiple)
    effective = ""
    for em in _EFFECTIVE_RE.finditer(raw):
        effective = em.group(1).strip()
    cites = _dedupe([f"{a} Ill. Reg. {b}" for a, b in _ILL_REG_RE.findall(raw)])
    body = text[: m.start()].rstrip()
    return body, effective, cites, raw


def parse_section(html: str, fn: str, url: str) -> Section | None:
    soup = BeautifulSoup(html, "html.parser")
    section_meta = ""
    mtag = soup.find("meta", attrs={"name": "sectionname"})
    if mtag and mtag.get("content"):
        section_meta = mtag["content"].strip()
    # The centered heading div carries the breadcrumb.
    heading_div = soup.find("div", attrs={"align": "center", "class": "heading"})
    if heading_div is None:
        # Fallback: very rare but try first div
        heading_div = soup.find("div")
    bc = _parse_breadcrumb(heading_div)

    # Strip non-content elements before harvesting body text.
    for tag in soup(["style", "script", "meta", "link"]):
        tag.decompose()

    # Drop the heading div so it doesn't double up in body text.
    if heading_div is not None:
        heading_div.decompose()

    body_full = soup.get_text("\n", strip=True)
    # Skip placeholder / repealed / reserved pages - they carry no rule text.
    if _RESERVED_PAT.search(body_full[:300]):
        return None

    # The body usually starts with the bolded "Section X.YZ  Title" line.
    # Pull the section title out of either the bolded line or the meta tag.
    section_num_meta = _section_number_from_meta(section_meta) if section_meta else None
    section_title = ""
    if section_meta:
        # "Section 1.10  Definitions" -> "Definitions"
        m_t = re.match(r"^\s*Section\s+[\w.\-]+\s+(.+)$", section_meta, re.IGNORECASE)
        if m_t:
            section_title = m_t.group(1).strip()

    # Filename-derived fallbacks for section number when meta is absent.
    parsed_fn = _parse_filename(fn)
    if parsed_fn is None:
        return None
    title_num, part_pad, subpart_letter_fn, sec_digits = parsed_fn
    if not section_num_meta:
        # Best-effort dotted form from filename digits. IL uses 5 digits for
        # most titles with implicit 2 decimals (00100 -> 1.10) and 6-8 digits
        # for titles like 068 where the leading zeros encode whole numbers
        # (00000050 -> 0.50, 00000100 -> 1.00). Prefer body-text scan.
        m_body = re.search(r"\bSection\s+([\d.]+)\b", body_full)
        section_num_meta = (
            m_body.group(1) if m_body else f"{int(sec_digits):d}"
        )

    body_clean, effective, cites, raw_source = _strip_source_block(body_full)

    # Final cosmetic cleanup: collapse runs of whitespace but preserve paragraph
    # boundaries (double newlines).
    body_clean = re.sub(r"[ \t]+", " ", body_clean)
    body_clean = re.sub(r"\n[ \t]+", "\n", body_clean)
    body_clean = re.sub(r"\n{3,}", "\n\n", body_clean).strip()
    if len(body_clean) < 30:
        return None

    # Fill any blanks in breadcrumb from filename when source omitted them.
    if not bc["part_num"]:
        bc["part_num"] = str(int(part_pad))
    if not bc["subpart_letter"]:
        bc["subpart_letter"] = subpart_letter_fn

    return Section(
        title_num=title_num,
        title_name=bc["title_name"],
        chapter_label=bc["chapter_label"],
        issuing_agency=bc["issuing_agency"],
        subchapter_label=bc["subchapter_label"],
        part_num=bc["part_num"],
        part_name=bc["part_name"],
        subpart_letter=bc["subpart_letter"],
        subpart_name=bc["subpart_name"],
        section_num=section_num_meta,
        section_title=section_title,
        raw_text=body_clean,
        source_url=url,
        effective_date=effective,
        register_citations=cites,
        source_history=raw_source,
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
    title_int = int(s.title_num)
    part_safe = _safe(s.part_num)
    sec_safe = _safe(s.section_num)
    act_id = f"STATE_IL_IAC_T{title_int}_P{part_safe}_S{sec_safe}"
    citation = f"{title_int} Ill. Adm. Code {s.part_num}.{s.section_num}"
    text = s.raw_text
    # Rich, searchable embed header: include the regulation-specific metadata
    # (effective date, Ill. Reg. citations) so it is retrievable, not just
    # stored. The Source line tells you which adopting/amending rulemaking
    # gave this text its current form.
    meta_lines = []
    if s.effective_date:
        meta_lines.append(f"Effective: {s.effective_date}")
    if s.register_citations:
        meta_lines.append(f"Illinois Register: {'; '.join(s.register_citations)}")
    if s.source_history:
        meta_lines.append(f"Source: {s.source_history}")
    meta_header = ("\n" + "\n".join(meta_lines)) if meta_lines else ""
    section_title_disp = f"Section {s.section_num}" + (
        f"  {s.section_title}" if s.section_title else ""
    )
    text_for_embedding = (
        f"Regulation: Illinois Administrative Code | US | Illinois | In Force\n"
        f"Title {title_int}: {s.title_name} / {s.chapter_label} / Part {s.part_num} {s.part_name}\n"
        f"{section_title_disp}{meta_header}\n\n{text}"
    )
    breadcrumb = [
        {
            "type": "title",
            "num": str(title_int),
            "label": f"Title {title_int}",
            "name": s.title_name,
        },
        {
            "type": "chapter",
            "num": s.chapter_label.split(":", 1)[0].replace("CHAPTER", "").strip(),
            "label": s.chapter_label,
            "name": s.issuing_agency,
        },
    ]
    if s.subchapter_label:
        breadcrumb.append(
            {
                "type": "subchapter",
                "num": s.subchapter_label.split(":", 1)[0].replace("SUBCHAPTER", "").strip(),
                "label": s.subchapter_label,
                "name": "",
            }
        )
    breadcrumb.append(
        {
            "type": "part",
            "num": s.part_num,
            "label": f"Part {s.part_num}",
            "name": s.part_name,
        }
    )
    if s.subpart_letter:
        breadcrumb.append(
            {
                "type": "subpart",
                "num": s.subpart_letter,
                "label": f"Subpart {s.subpart_letter}",
                "name": s.subpart_name,
            }
        )
    breadcrumb.append(
        {
            "type": "section",
            "num": s.section_num,
            "label": f"Section {s.section_num}",
            "name": s.section_title,
        }
    )
    # Per-amendment effective date list: each Ill. Reg. cite implies one
    # rulemaking. We don't have per-cite dates (only the latest), but we keep
    # the cites in register_citations for downstream history reconstruction.
    prior_effective_dates = ""

    md = {
        "act_id": act_id,
        "corpus_type": "state_regulation",
        "category": "state_regulation",
        "document_type": "regulation",
        "jurisdiction": "US",
        "country_code": "US",
        "state": "il",
        "title_number": title_int,
        "title_name": f"Illinois Administrative Code — Title {title_int}: {s.title_name}".rstrip(
            " :—"
        ),
        "title": "Illinois Administrative Code",
        "title_code": "regs_il",
        "top_level_title": "regs-il",
        "chapter": f"Title {title_int} Part {s.part_num}",
        "chapter_name": (s.part_name or "").strip() or s.title_name,
        "section_number": f"{s.part_num}.{s.section_num}",
        "section_title": section_title_disp,
        "year": 2026,
        "act_status": "in_force",
        "renumbered_to": "",
        "transferred_to": "",
        "level_classifier": "section",
        # --- regulation-specific rich metadata (captured, not discarded) ---
        "effective_date": s.effective_date or None,
        "last_amended_date": s.effective_date or None,
        "statutory_authority": None,  # not exposed in section HTML (lives in P-file .docx)
        "rule_amplifies": None,
        "promulgated_under": None,
        "prior_effective_dates": prior_effective_dates or None,
        "history_note": s.source_history or None,
        "review_date": None,
        "register_citations": s.register_citations,
        "register_publication": (s.register_citations[-1] if s.register_citations else None),
        "session_law_citations": [],
        "issuing_agency": s.issuing_agency or None,
        "issuing_agency_code": s.chapter_label or None,
        "subchapter": s.subchapter_label or None,
        "subpart_letter": s.subpart_letter or None,
        "subpart_name": s.subpart_name or None,
        "part_number": s.part_num,
        "part_name": s.part_name or None,
        "citation": citation,
        "citation_short": citation,
        "display_label": citation,
        "display_title": section_title_disp,
        "display_path": (
            f"Illinois Administrative Code / Title {title_int} {s.title_name} / "
            f"{s.chapter_label} / Part {s.part_num} {s.part_name} / "
            f"Section {s.section_num} {s.section_title}".rstrip(" /")
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
        "amendments_count": len(s.register_citations),
        "last_amended_year": None,
        "public_laws_referenced": [],
        "public_laws_count": 0,
        "source_url": s.source_url,
        "parent_id": f"us/il/regulations/title={title_int}/part={s.part_num}",
        "raw_node_id": (
            f"us/il/regulations/title={title_int}/part={s.part_num}/section={s.section_num}"
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


def process_section(url: str, fn: str, title: str) -> Section | None:
    html = fetch(url)
    if not html:
        return None
    sec = parse_section(html, fn, url)
    if sec is None:
        return None
    return sec


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--titles",
        default="",
        help="Comma-separated title numbers, zero-padded (e.g. '008,068'). Default: all.",
    )
    ap.add_argument("--workers", type=int, default=3)
    ap.add_argument("--delay", type=float, default=0.4)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    print(f"[IAC] discovering titles from {IL_ROOT}", flush=True)
    titles = list_titles()
    if args.titles:
        wanted = {t.strip().zfill(3) for t in args.titles.split(",") if t.strip()}
        titles = [t for t in titles if t in wanted]
    print(f"[IAC] {len(titles)} titles", flush=True)

    # Phase 1: gather all section URLs across requested titles.
    all_sections: list[tuple[str, str, str]] = []
    for t in titles:
        items = list_sections_in_title(t)
        all_sections.extend((u, fn, t) for u, fn in items)
        print(f"  [title {t}] {len(items)} sections", flush=True)
        time.sleep(args.delay)
    print(f"\n[IAC] {len(all_sections):,} sections to fetch", flush=True)
    if args.dry_run:
        return 0

    # Phase 2: parallel fetch + parse.
    chunks: list[dict] = []
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {
            ex.submit(process_section, u, fn, t): (u, fn) for (u, fn, t) in all_sections
        }
        done = 0
        for fut in as_completed(futures):
            done += 1
            try:
                sec = fut.result()
                if sec is not None:
                    chunks.append(to_chunk_record(sec))
            except Exception as e:
                print(f"  ! section failed: {e}", flush=True)
            if done % 200 == 0 or done == len(all_sections):
                rate = len(chunks) / max(time.time() - t0, 0.1)
                print(
                    f"  ... {done:>6}/{len(all_sections):,} sections fetched, "
                    f"{len(chunks):>6,} parsed, {rate:.1f}/s",
                    flush=True,
                )
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

    print(
        f"\n=== Done: parsed={len(chunks):,}, new={written:,}, "
        f"elapsed={time.time() - t0:.1f}s ===",
        flush=True,
    )
    print(f"JSONL: {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
