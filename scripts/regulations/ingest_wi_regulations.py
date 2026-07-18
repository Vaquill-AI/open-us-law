#!/usr/bin/env python3
"""Ingest the Wisconsin Administrative Code (Wis. Admin. Code) — WI state regs.

OFFICIAL SOURCE ONLY: https://docs.legis.wisconsin.gov/code/admin_code
(the Wisconsin State Legislature's own document portal). No aggregators.

The portal does NOT expose per-chapter or bulk XML/JSON (Tax 11.xml, ch.
Tax 11.xml etc. all 404). It is a server-rendered HTML application, so we
scrape HTML — but the markup is highly structured, which makes per-section
extraction reliable:

    /code/admin_code                          → 87 agency links
                                                (/document/administrativecode/<Agency>
                                                 redirects to /code/admin_code/<agency>)
    /code/admin_code/<agency>                 → chapter links "ch. <Agency> <n>"
    /code/admin_code/<agency>/<n>             → section list "<Agency> <n>.<sec>"
    /document/administrativecode/<Agency>%20<n>.<sec>
                                              → a "window" page whose
                                                #contentFrame renders the target
                                                section in full (plus neighbor
                                                fragments). Each text block carries
                                                a `data-cites` path like
                                                administrativecode/Tax 11.04(1m)(a),
                                                so we filter to exactly the target
                                                section's blocks.

Each section ends with a `qsnote_history` block ("Tax 11.04 History | History:
Cr. Register, January, 1979, No. 277, eff. 2-1-79; CR 09-090: ... Register May
2010 No. 653, eff. 6-1-10; ...") plus `qsnote_note` blocks ("Note: Section Tax
11.04 interprets s. 77.54 ..."). We parse those into structured metadata:
effective_date (latest eff.), prior_effective_dates, register_citations,
order_numbers (CR/EmR), statutory_authority ("adopted pursuant to ss. ..."),
rule_amplifies ("interprets s. ...").

corpus_type='state_regulation'. Bluebook cite form: "Wis. Admin. Code <Agency>
§ <ch>.<sec>".

Geo-restricted; Webshare US proxy + Mozilla UA + polite pacing (the site 429s
under heavy concurrency).
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
OUT = DATA_DIR / "state_wi_regulations.jsonl"

WI_BASE = "https://docs.legis.wisconsin.gov"
WI_TOC = f"{WI_BASE}/code/admin_code"

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
    url = (
        f"http://{urllib.parse.quote(proxy_user)}:"
        f"{urllib.parse.quote(pwd)}@{host}:{port}"
    )
    return {"http": url, "https": url}


SESSION = requests.Session()
SESSION.headers.update({"User-Agent": _MOZ_UA})


def fetch(url: str, retries: int = 5) -> Optional[str]:
    proxies = _us_proxies()
    for attempt in range(retries):
        try:
            r = SESSION.get(url, timeout=60, proxies=proxies, allow_redirects=True)
            if r.status_code == 200:
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
# Discovery: TOC -> agencies -> chapters -> sections
# ---------------------------------------------------------------------------

# Agency links on the TOC: /document/administrativecode/<Agency> (no chapter num),
# where <Agency> is the original-cased code (Tax, NR, ATCP, Gen Couns, ...).
_AGENCY_HREF_RE = re.compile(
    r"^/document/administrativecode/([^/?]+)$"
)
# Chapter links on an agency page: /document/administrativecode/ch.%20<Agency>%20<n>
_CHAPTER_HREF_RE = re.compile(
    r"^/document/administrativecode/(ch\.%20[^/?]+?)(?:\.pdf)?$"
)
# Large agencies group chapters under subchapter-group pages, e.g.
# /document/administrativecode/Chs.%20ATCP%201-9;%20General — these must be
# followed one level deeper to reach the individual "ch. ATCP N" links.
_CHAPTER_GROUP_HREF_RE = re.compile(
    r"^/document/administrativecode/(Chs\.%20[^/?]+?)(?:\.pdf)?$"
)
# Section links on a chapter page: /document/administrativecode/<Agency>%20<ch>.<sec>
# Exclude "ch. ..."/"Chs. ..." container links and any ".pdf" exports (the
# chapter PDF href "ch.%20ATCP%20134.pdf" otherwise looks like a section).
_SECTION_HREF_RE = re.compile(
    r"^/document/administrativecode/((?!ch\.%20|Chs\.%20)[^/?]+%20\d+\.[0-9A-Za-z]+)$"
)


def list_agencies() -> list[tuple[str, str]]:
    """Return [(agency_code, agency_name)] from the master TOC."""
    html = fetch(WI_TOC)
    if not html:
        raise RuntimeError("could not fetch WI admin code TOC")
    soup = BeautifulSoup(html, "html.parser")
    seen: dict[str, str] = {}
    for a in soup.find_all("a", href=True):
        m = _AGENCY_HREF_RE.match(a["href"])
        if not m:
            continue
        code = urllib.parse.unquote(m.group(1)).strip()
        # Skip portal pages (published, using, differences, emr_notification, ...).
        if code != code.upper() and code.lower() == code:
            # all-lowercase slugs are portal helper pages, real codes are mixed/UPPER
            continue
        if "/" in code or " " in code and code.islower():
            continue
        name = a.get_text(strip=True)
        if not name or "Published under" in name:
            continue
        seen[code] = name
    return sorted(seen.items(), key=lambda kv: kv[0].lower())


def _chapters_from_page(html: str) -> tuple[list[str], list[str]]:
    """Return (chapter_labels, chapter_group_labels) found on a page."""
    soup = BeautifulSoup(html, "html.parser")
    chapters: list[str] = []
    groups: list[str] = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        m = _CHAPTER_HREF_RE.match(href)
        if m:
            label = urllib.parse.unquote(m.group(1)).strip()  # "ch. Tax 1"
            if label not in chapters:
                chapters.append(label)
            continue
        g = _CHAPTER_GROUP_HREF_RE.match(href)
        if g:
            label = urllib.parse.unquote(g.group(1)).strip()  # "Chs. ATCP 1-9; ..."
            if label not in groups:
                groups.append(label)
    return chapters, groups


def list_chapters_in_agency(agency: str) -> list[str]:
    """Return chapter labels ('ch. Tax 1') for an agency code, following
    subchapter-group pages ('Chs. ATCP 1-9; General') one level deeper when a
    large agency partitions its chapters into groups.
    """
    url = f"{WI_BASE}/document/administrativecode/{urllib.parse.quote(agency)}"
    html = fetch(url)
    if not html:
        return []
    chapters, groups = _chapters_from_page(html)
    for grp in groups:
        g_url = f"{WI_BASE}/document/administrativecode/{urllib.parse.quote(grp)}"
        g_html = fetch(g_url)
        if not g_html:
            continue
        g_chapters, _ = _chapters_from_page(g_html)
        for ch in g_chapters:
            if ch not in chapters:
                chapters.append(ch)
    return chapters


def list_sections_in_chapter(chapter_label: str) -> tuple[str, str, list[str]]:
    """For a chapter label ('ch. Tax 1') return (chapter_num, chapter_name,
    [section numbers like 'Tax 1.01']).
    """
    url = f"{WI_BASE}/document/administrativecode/{urllib.parse.quote(chapter_label)}"
    html = fetch(url)
    if not html:
        return ("", "", [])
    soup = BeautifulSoup(html, "html.parser")
    cf = soup.find(id="contentFrame")
    chap_name = ""
    if cf:
        # The chapter title is rendered in div.qstitle_chap (e.g. "SALES AND
        # USE TAX"). Title-case it for display.
        title_el = cf.find("div", class_="qstitle_chap")
        if title_el:
            raw_title = title_el.get_text(" ", strip=True)
            chap_name = raw_title.title() if raw_title.isupper() else raw_title
    secs: list[str] = []
    for a in soup.find_all("a", href=True):
        m = _SECTION_HREF_RE.match(a["href"])
        if not m:
            continue
        sec = urllib.parse.unquote(m.group(1)).strip()  # "Tax 1.01"
        if sec not in secs:
            secs.append(sec)
    chap_num = chapter_label.replace("ch. ", "").strip()  # "Tax 1"
    return (chap_num, chap_name, secs)


# ---------------------------------------------------------------------------
# Section parsing
# ---------------------------------------------------------------------------

# A section number like "Tax 11.04" or "NR 102.04" or "ATCP 10.01" or "PI 8.001".
_SEC_RE = re.compile(r"^(.+?)\s+(\d+)\.([0-9A-Za-z]+)$")
# data-cites entry: administrativecode/Tax 11.04(1m)(a)
_CITE_RE = re.compile(r"administrativecode/(.+?\s+\d+\.[0-9A-Za-z]+)")
_RESERVED_PAT = re.compile(
    r"\b(rescinded|repealed|reserved|renumbered)\b", re.IGNORECASE
)


@dataclass
class Section:
    agency: str  # original-cased, e.g. "Tax", "NR", "ATCP"
    chapter_num: str  # e.g. "Tax 11"
    chapter_name: str
    section_number: str  # e.g. "Tax 11.04"
    section_title: str  # e.g. "Constructing buildings for exempt entities."
    raw_text: str
    source_url: str
    effective_date: str = ""  # latest "eff." date, e.g. "8-1-21"
    prior_effective_dates: str = ""  # earlier eff. dates
    history_text: str = ""  # full raw History block
    register_citations: list[str] = field(default_factory=list)
    order_numbers: list[str] = field(default_factory=list)  # CR / EmR numbers
    statutory_authority: str = ""  # "adopted pursuant to ss. ..."
    rule_amplifies: str = ""  # "interprets s. ..."
    note_text: str = ""  # combined Note: lines


def _section_of_block(block) -> Optional[str]:
    """Identify which section a content block belongs to."""
    cites = block.get("data-cites")
    if cites:
        try:
            arr = json.loads(cites)
        except Exception:
            arr = []
        for c in arr:
            m = _CITE_RE.match(c)
            if m:
                return m.group(1).strip()
    # Notes / history carry the section in a text prefix: "Tax 11.04 Note" /
    # "Tax 11.04 History".
    t = block.get_text(" ", strip=True)
    m = re.match(r"(.+?\s+\d+\.[0-9A-Za-z]+)\s+(?:Note|History)\b", t)
    if m:
        return m.group(1).strip()
    return None


def _clean_block_text(cls: str, raw: str, sec_num: str) -> str:
    """Strip the leading machine-citation token that WI repeats at the start of
    every block (e.g. "Tax 11.04(1m)(a) (a) ..."), keeping the human numbering.
    """
    # Remove a leading "<sec_num>(...)" or "<sec_num> Note/History" token.
    out = re.sub(
        rf"^{re.escape(sec_num)}(?:\([^)]*\))*[0-9A-Za-z.]*\s*", "", raw
    )
    out = re.sub(rf"^{re.escape(sec_num)}\s+(?:Note|History)\s*", "", out)
    out = re.sub(r"\s+", " ", out).strip()
    return out


def _strip_label(text: str, label: str) -> str:
    """Drop the doubled leading label WI renders (e.g. "History History:" or
    "Note Note:") at the start of note/history blocks.
    """
    return re.sub(rf"^(?:{label}\s+)?{label}:\s*", "", text, flags=re.IGNORECASE).strip()


_MONTHS = (
    "January|February|March|April|May|June|July|August|September|"
    "October|November|December"
)
# "eff. 8-1-21" or "eff. 10-1-91"
_EFF_RE = re.compile(r"eff\.\s*(\d{1,2}-\d{1,2}-\d{2,4})", re.IGNORECASE)
# "Register July 2021 No. 787" / "Register, November, 1977, No. 263"
_REGISTER_RE = re.compile(
    rf"Register,?\s+(?:{_MONTHS}),?\s+\d{{4}},?\s+No\.\s*\d+", re.IGNORECASE
)
# Rule-making order numbers: "CR 09-090", "CR12-014", "EmR0924"
_ORDER_RE = re.compile(r"\b(?:CR|EmR)\s*\d{2,4}-?\d{0,3}", re.IGNORECASE)


def _parse_history(hist: str) -> dict:
    """Pull structured fields out of a WI History block."""
    effs = _EFF_RE.findall(hist)
    registers = _REGISTER_RE.findall(hist)
    orders = [re.sub(r"\s+", " ", o).strip() for o in _ORDER_RE.findall(hist)]
    # Dedupe while preserving order.
    def _dedupe(xs: list[str]) -> list[str]:
        seen: set[str] = set()
        out: list[str] = []
        for x in xs:
            k = x.strip()
            if k and k not in seen:
                seen.add(k)
                out.append(k)
        return out

    effs = _dedupe(effs)
    return {
        "effective_date": effs[-1] if effs else "",
        "prior_effective_dates": "; ".join(effs[:-1]) if len(effs) > 1 else "",
        "register_citations": _dedupe(registers),
        "order_numbers": _dedupe(orders),
    }


def parse_section_page(
    html: str, agency: str, chapter_num: str, chapter_name: str,
    section_number: str, source_url: str,
) -> Optional[Section]:
    soup = BeautifulSoup(html, "html.parser")
    cf = soup.find(id="contentFrame")
    if not cf:
        return None

    title = ""
    body_parts: list[str] = []
    note_parts: list[str] = []
    history_text = ""

    for d in cf.find_all("div", class_=re.compile(r"^qs(atxt|note)")):
        if _section_of_block(d) != section_number:
            continue
        cls = (d.get("class") or [""])[0]
        raw = d.get_text(" ", strip=True)
        if cls == "qsatxt_1sect":
            # "Tax 11.04 Tax 11.04 Constructing buildings for exempt entities."
            m = re.match(
                rf"{re.escape(section_number)}\s+{re.escape(section_number)}\s+(.+)",
                raw,
            )
            heading = (m.group(1) if m else raw).strip()
            # Some short sections inline the body in this block. Use the leading
            # sentence as the title; keep the full heading as the section's body.
            first = re.split(r"(?<=\.)\s", heading, maxsplit=1)
            title = first[0].strip().rstrip(".")
            if len(first) > 1 and first[1].strip():
                body_parts.append(heading)
            continue
        if cls == "qsnote_history":
            history_text = _strip_label(
                _clean_block_text(cls, raw, section_number), "History"
            )
            continue
        if cls.startswith("qsnote"):
            note_parts.append(
                _strip_label(
                    _clean_block_text(cls, raw, section_number), "Note"
                )
            )
            continue
        body_parts.append(_clean_block_text(cls, raw, section_number))

    note_text = " ".join(p for p in note_parts if p)
    body = "\n".join(p for p in body_parts if p).strip()
    if not body and not title:
        return None
    if _RESERVED_PAT.search(title) and len(body) < 30:
        return None

    full_text = (title + ".\n" + body).strip() if title else body

    hist = _parse_history(history_text)

    # Statutory authority: "adopted pursuant to ss. 227.11 (2), 440.09, ...,
    # Stats." Capture the statute list up to and including the "Stats." marker
    # (section numbers contain internal periods, so we anchor on "Stats.").
    stat_auth = ""
    m_auth = re.search(
        r"adopted pursuant to (ss?\..*?Stats\.)", full_text + " " + note_text,
        re.IGNORECASE | re.DOTALL,
    )
    if m_auth:
        stat_auth = re.sub(r"\s+", " ", m_auth.group(1)).strip().rstrip(",")

    # Rule amplifies / implements: "interprets s. 77.54, ..., Stats."
    amplifies = ""
    m_amp = re.search(
        r"interprets?\s+(ss?\..*?Stats\.)", note_text + " " + full_text,
        re.IGNORECASE | re.DOTALL,
    )
    if m_amp:
        amplifies = re.sub(r"\s+", " ", m_amp.group(1)).strip().rstrip(",")

    return Section(
        agency=agency,
        chapter_num=chapter_num,
        chapter_name=chapter_name,
        section_number=section_number,
        section_title=title,
        raw_text=full_text,
        source_url=source_url,
        effective_date=hist["effective_date"],
        prior_effective_dates=hist["prior_effective_dates"],
        history_text=history_text,
        register_citations=hist["register_citations"],
        order_numbers=hist["order_numbers"],
        statutory_authority=stat_auth,
        rule_amplifies=amplifies,
        note_text=note_text,
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


def _act_id(agency: str, section_number: str, chapter_num: str) -> str:
    """STATE_WI_ADC_<Agency>_<chapter>_<section>, e.g. STATE_WI_ADC_Tax_11_04.

    section_number is like "Tax 11.04"; strip the duplicated agency prefix so
    the id is not "...Tax_Tax_11_04".
    """
    m = _SEC_RE.match(section_number)
    chap_only = m.group(2) if m else chapter_num.split()[-1]
    sec_only = m.group(3) if m else _safe(section_number)
    return f"STATE_WI_ADC_{_safe(agency)}_{chap_only}_{sec_only}"


def to_chunk_record(s: Section) -> dict:
    # section_number like "Tax 11.04" -> chapter "11", section "04".
    m = _SEC_RE.match(s.section_number)
    chap_only = m.group(2) if m else s.chapter_num.split()[-1]
    sec_only = m.group(3) if m else ""
    act_id = _act_id(s.agency, s.section_number, s.chapter_num)
    # Bluebook: "Wis. Admin. Code <Agency> § <ch>.<sec>".
    citation = f"Wis. Admin. Code {s.agency} § {chap_only}.{sec_only}"
    citation_short = f"Wis. Admin. Code {s.section_number}"
    text = s.raw_text

    meta_lines = []
    if s.effective_date:
        meta_lines.append(f"Effective: {s.effective_date}")
    if s.statutory_authority:
        meta_lines.append(f"Statutory Authority: {s.statutory_authority}")
    if s.rule_amplifies:
        meta_lines.append(f"Interprets: {s.rule_amplifies}")
    meta_header = ("\n" + "\n".join(meta_lines)) if meta_lines else ""
    text_for_embedding = (
        f"Regulation: Wisconsin Administrative Code | US | Wisconsin | In Force\n"
        f"{s.agency} / Chapter {s.chapter_num}: {s.chapter_name}\n"
        f"§ {s.section_number} {s.section_title}{meta_header}\n\n{text}"
    )

    md = {
        "act_id": act_id,
        "corpus_type": "state_regulation",
        "category": "state_regulation",
        "document_type": "regulation",
        "jurisdiction": "US",
        "country_code": "US",
        "state": "wi",
        "title_number": None,
        "title_name": f"Wisconsin Administrative Code — {s.agency}",
        "title": "Wisconsin Administrative Code",
        "title_code": f"wac_{_safe(s.agency).lower()}",
        "top_level_title": s.agency,
        "chapter": s.chapter_num,
        "chapter_name": s.chapter_name,
        "section_number": s.section_number,
        "section_title": s.section_title,
        "year": 2026,
        "act_status": "in_force",
        "renumbered_to": "",
        "transferred_to": "",
        "level_classifier": "section",
        # --- regulation-specific rich metadata (captured, not discarded) ---
        "effective_date": s.effective_date or None,
        "prior_effective_dates": s.prior_effective_dates or None,
        "statutory_authority": s.statutory_authority or None,
        "rule_amplifies": s.rule_amplifies or None,
        "issuing_agency": s.agency,
        "issuing_agency_code": s.agency,
        "register_citations": s.register_citations,
        "rule_order_numbers": s.order_numbers,
        "history_note": s.history_text or None,
        "section_note": s.note_text or None,
        "register_publication": (
            s.register_citations[-1] if s.register_citations else None
        ),
        "citation": citation,
        "citation_short": citation_short,
        "display_label": citation,
        "display_title": f"§ {s.section_number} {s.section_title}".strip(),
        "display_path": (
            f"Wisconsin Administrative Code / {s.agency} / "
            f"Chapter {s.chapter_num} / § {s.section_number}"
        ),
        "breadcrumb": [
            {"type": "agency", "num": s.agency,
             "label": s.agency, "name": ""},
            {"type": "chapter", "num": s.chapter_num,
             "label": f"Chapter {s.chapter_num}", "name": s.chapter_name},
            {"type": "section", "num": s.section_number,
             "label": f"§ {s.section_number}", "name": s.section_title},
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
        "amendments_count": len(s.order_numbers),
        "last_amended_year": None,
        "public_laws_referenced": [],
        "public_laws_count": 0,
        "source_url": s.source_url,
        "parent_id": (
            f"us/wi/regulations/agency={_safe(s.agency)}/"
            f"chapter={_safe(s.chapter_num)}"
        ),
        "raw_node_id": (
            f"us/wi/regulations/agency={_safe(s.agency)}/"
            f"chapter={_safe(s.chapter_num)}/section={_safe(s.section_number)}"
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

def process_section(
    agency: str, chapter_num: str, chapter_name: str, section_number: str,
) -> Optional[Section]:
    url = f"{WI_BASE}/document/administrativecode/{urllib.parse.quote(section_number)}"
    html = fetch(url)
    if not html:
        return None
    sec = parse_section_page(
        html, agency, chapter_num, chapter_name, section_number, url
    )
    if sec is None:
        return None
    return sec


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--agencies", default="",
        help="Comma-separated agency codes (e.g. 'Tax,NR'). Default: all.",
    )
    ap.add_argument("--workers", type=int, default=3)
    ap.add_argument("--delay", type=float, default=0.6)
    ap.add_argument("--dry-run", action="store_true",
                    help="Discover agencies/chapters/sections, count only.")
    args = ap.parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    print(f"[WAC] discovering agencies from {WI_TOC}", flush=True)
    agencies = list_agencies()
    if args.agencies:
        wanted = {a.strip() for a in args.agencies.split(",") if a.strip()}
        agencies = [(c, n) for c, n in agencies if c in wanted]
    print(f"[WAC] {len(agencies)} agencies", flush=True)

    # Phase 1: agency -> chapters -> sections
    targets: list[tuple[str, str, str, str]] = []  # (agency, chap_num, chap_name, sec)
    for code, _name in agencies:
        chapters = list_chapters_in_agency(code)
        time.sleep(args.delay)
        for chap_label in chapters:
            chap_num, chap_name, secs = list_sections_in_chapter(chap_label)
            for sec in secs:
                targets.append((code, chap_num or chap_label.replace("ch. ", ""),
                                chap_name, sec))
            time.sleep(args.delay)
        print(f"  [agency {code}] {len(chapters)} chapters", flush=True)
    print(f"\n[WAC] {len(targets)} sections to fetch", flush=True)
    if args.dry_run:
        return 0

    # Phase 2: crawl sections
    chunks: list[dict] = []
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {
            ex.submit(process_section, a, cn, cname, sec): sec
            for (a, cn, cname, sec) in targets
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
            if done % 100 == 0 or done == len(targets):
                rate = len(chunks) / max(time.time() - t0, 0.1)
                print(f"  ... {done:>6}/{len(targets)} sections, "
                      f"{len(chunks):>6} parsed, {rate:.1f}/s", flush=True)
            time.sleep(args.delay / max(args.workers, 1))

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

    print(f"\n=== Done: parsed={len(chunks):,}, new={written:,}, "
          f"elapsed={time.time()-t0:.1f}s ===", flush=True)
    print(f"JSONL: {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
