#!/usr/bin/env python3
"""Ingest the Code of Colorado Regulations (CCR) — Colorado's state regulations.

OFFICIAL SOURCE ONLY: https://www.sos.state.co.us/CCR/ (Colorado Secretary of
State). No aggregators (no Justia/ZenRows/etc.).

Bulk XML / API: NONE. The SOS portal is a server-rendered Java/Phoenix
application that serves rule bodies as authenticated PDFs (and Word). Discovery
is HTML; bodies are PDF. The PDFs are born-digital and parse losslessly with
pdfplumber. Three discovery layers:

    /CCR/NumericalDeptList.do
        → 25 departments. Each department has agencies; the page renders a
          flat list of agency links. Department block headers carry the
          dept name + numeric prefix (e.g. "900 Department of Law").
        → href shape: /CCR/NumericalCCRDocList.do?deptID=N&deptName=...
                       &agencyID=N&agencyName=NNN <Agency name>
    /CCR/NumericalCCRDocList.do?deptID=&agencyID=
        → All rule sets under that agency, each row a
          <a href="/CCR/DisplayRule.do?action=ruleinfo&ruleId=N
                    &deptID=&agencyID=&deptName=&agencyName=
                    &seriesNum=<N CCR S-N (Part NN?)>">
          The seriesNum IS the CCR citation form, e.g. "4 CCR 904-1" or
          "6 CCR 1007-1 Part 02".
    /CCR/DisplayRule.do?action=ruleinfo&ruleId=N&...
        → Rule landing page. Title in <p class="pagehead5">
          "<cite>   <TITLE>". "Current version" row links the active version's
          PDF via inline JS:
              OpenRuleWindow(ruleVersionId, fileName)  →
              /CCR/GenerateRulePdf.do?ruleVersionId=NNNN&fileName=<cite>
          The link text reads "<cite> effective MM/DD/YYYY (PDF)".

The PDF is the substantive rule. Each PDF begins with a header (department,
agency, rule title, citation) followed by the substantive numbered rule body
and ends with an "Editor's Notes / History" trailer that records the
authoritative effective date(s). corpus_type='state_regulation'.

CCR hierarchy: Department (numeric prefix, e.g. 900) → Agency (numeric, e.g.
904 Attorney General-Consumer Protection Section) → Rule (citation form
"<N> CCR <agency>-<rule>", e.g. "4 CCR 904-1"). The rule (one PDF) is the
citable unit; act_id='STATE_CO_CCR_<cite sanitized>'.

Rich metadata captured into structured fields (NEVER discarded):
    Current version effective date  → effective_date
    Editor's Notes / History block  → history_note (raw),
                                       prior_effective_dates (parsed dates)
    Department / Agency names       → issuing_agency, chapter_name
    seriesNum                       → citation / citation_short
    rule title (pagehead5)          → section_title

Geo-restricted; Webshare US proxy + Mozilla UA + polite pacing.
"""

from __future__ import annotations

import argparse
import hashlib
import html as html_lib
import json
import os
import re
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import quote, unquote

import requests

sys.stdout.reconfigure(line_buffering=True)  # type: ignore[attr-defined]

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = Path(os.environ.get("OUT_DIR", "./data"))
OUT = DATA_DIR / "state_co_regulations.jsonl"

CO_BASE = "https://www.sos.state.co.us"
CO_TOC = f"{CO_BASE}/CCR/NumericalDeptList.do"

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


def fetch(url: str, retries: int = 6, as_bytes: bool = False):
    """Fetch URL. Returns text by default, bytes if as_bytes=True. Retries 429
    with exponential backoff and tolerates the transient SSL/connection errors
    that rotating proxies occasionally raise (WRONG_VERSION_NUMBER etc.)."""
    proxies = _us_proxies()
    for attempt in range(retries):
        try:
            r = SESSION.get(url, timeout=120, proxies=proxies, allow_redirects=True)
            if r.status_code == 200:
                return r.content if as_bytes else r.text
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
# Discovery: dept list -> per-agency rule lists -> per-rule landing pages
# ---------------------------------------------------------------------------

_AGENCY_HREF_RE = re.compile(r"/CCR/NumericalCCRDocList\.do\?deptID=(\d+)[^\"']*?agencyID=(\d+)")
_RULE_HREF_RE = re.compile(
    r"/CCR/DisplayRule\.do\?action=ruleinfo&ruleId=(\d+)[^\"']*?seriesNum=([^\"'&]+)"
)
# OpenRuleWindow('<ruleVersionId>', '<cite>') - the active version's PDF id.
_VERSION_RE = re.compile(r"OpenRuleWindow\(\s*'(\d+)'\s*,\s*'([^']+)'\s*\)")
# "<cite> effective MM/DD/YYYY (PDF)" - the active version's effective date.
_EFF_DATE_RE = re.compile(
    r"effective\s+(\d{1,2}/\d{1,2}/\d{4})\s*\(PDF\)",
    re.IGNORECASE,
)
# Title row: "<cite>&nbsp;&nbsp; <TITLE>". &nbsp; survives html.unescape as \xa0.
_TITLE_ROW_RE = re.compile(r'<p class="pagehead5"[^>]*>([^<]+)</p>', re.IGNORECASE)


@dataclass
class AgencyRef:
    dept_id: str
    agency_id: str
    dept_name: str  # e.g. "900 Department of Law"
    agency_name: str  # e.g. "904 Attorney General-Consumer Protection Section"


@dataclass
class RuleRef:
    rule_id: str  # ruleinfo ID
    citation: str  # e.g. "4 CCR 904-1" or "6 CCR 1007-1 Part 02"
    agency: AgencyRef


def list_departments_and_agencies() -> list[AgencyRef]:
    """Walk the numerical Department List, returning every agency under every
    department with the long human-readable dept/agency names attached."""
    html = fetch(CO_TOC)
    if not html:
        raise RuntimeError("could not fetch CCR Department List")
    seen: dict[tuple[str, str], AgencyRef] = {}
    for a in re.finditer(r'href="(/CCR/NumericalCCRDocList\.do\?[^"]+)"', html):
        href = html_lib.unescape(a.group(1))
        m = _AGENCY_HREF_RE.search(href)
        if not m:
            continue
        # Pull dept/agency names from the same href's query string. They are
        # URL-encoded; unescape the entities, then percent-decode.
        dept_name = ""
        agency_name = ""
        m2 = re.search(r"deptName=([^&]*)", href)
        if m2:
            dept_name = unquote(m2.group(1)).strip()
        m3 = re.search(r"agencyName=([^&]*)", href)
        if m3:
            agency_name = unquote(m3.group(1)).strip()
        key = (m.group(1), m.group(2))
        seen.setdefault(
            key,
            AgencyRef(
                dept_id=m.group(1),
                agency_id=m.group(2),
                dept_name=dept_name,
                agency_name=agency_name,
            ),
        )
    # Sort by integer dept then agency id for stable output.
    return sorted(
        seen.values(),
        key=lambda x: (int(x.dept_id), int(x.agency_id)),
    )


def list_rules_in_agency(agency: AgencyRef) -> list[RuleRef]:
    """Return every rule (CCR citation) under an agency. Each row is a
    DisplayRule.do?action=ruleinfo link carrying ruleId + seriesNum."""
    url = (
        f"{CO_BASE}/CCR/NumericalCCRDocList.do?deptID={agency.dept_id}&agencyID={agency.agency_id}"
    )
    html = fetch(url)
    if not html:
        return []
    seen: dict[str, RuleRef] = {}
    for href in re.findall(r'href="(/CCR/DisplayRule\.do\?action=ruleinfo[^"]+)"', html):
        href = html_lib.unescape(href)
        m = _RULE_HREF_RE.search(href)
        if not m:
            continue
        rule_id = m.group(1)
        cite = unquote(m.group(2)).strip()
        if not cite or cite in seen:
            continue
        seen[cite] = RuleRef(rule_id=rule_id, citation=cite, agency=agency)
    # Stable sort by citation.
    return sorted(seen.values(), key=lambda r: r.citation)


# ---------------------------------------------------------------------------
# Rule landing page parse -> ruleVersionId + effective date + title
# ---------------------------------------------------------------------------

_MONTHS = "January|February|March|April|May|June|July|August|September|October|November|December"
# "Entire rule eff. 10/30/2007." / "eff. 10/01/2024" / etc.
_HISTORY_DATE_RE = re.compile(r"\b(\d{1,2}/\d{1,2}/\d{2,4})\b")


@dataclass
class RuleVersion:
    rule_ref: RuleRef
    rule_title: str  # e.g. "REPOSSESSOR BONDS"
    rule_version_id: str  # active version id used to fetch PDF
    effective_date: str  # MM/DD/YYYY


def parse_rule_info(rule: RuleRef, html: str) -> RuleVersion | None:
    """Pull the active-version id + effective date + display title from a rule
    landing page. The "Current version" PDF link is the first OpenRuleWindow
    call on the page (archived versions follow, in a separate table)."""
    # The title row carries entities (e.g. "4 CCR 904-1&nbsp;&nbsp;REPOSSESSOR BONDS").
    title = ""
    m_title = _TITLE_ROW_RE.search(html)
    if m_title:
        raw = html_lib.unescape(m_title.group(1))
        # raw is "<cite>   <TITLE>" - strip the citation prefix.
        raw = raw.replace("\xa0", " ").strip()
        # Drop a leading copy of the citation (matched loosely so "Part NN"
        # variants are handled).
        prefix = rule.citation.replace("\xa0", " ").strip()
        if raw.startswith(prefix):
            raw = raw[len(prefix) :].strip()
        title = re.sub(r"\s+", " ", raw).strip(" -:")

    # Find the FIRST OpenRuleWindow call (current version). Archived rows
    # render their own OpenRuleWindow calls AFTER the current row.
    m_v = _VERSION_RE.search(html)
    if not m_v:
        return None
    version_id = m_v.group(1)
    # Effective date sits in the same anchor text as the current-version PDF
    # link; find the first one.
    eff = ""
    m_eff = _EFF_DATE_RE.search(html)
    if m_eff:
        eff = m_eff.group(1).strip()
    return RuleVersion(
        rule_ref=rule,
        rule_title=title or rule.citation,
        rule_version_id=version_id,
        effective_date=eff,
    )


# ---------------------------------------------------------------------------
# PDF parsing -> body text + editor's notes / history
# ---------------------------------------------------------------------------

# Common page footer that pdfplumber surfaces at the bottom of every page:
#     "Code of Colorado Regulations N"
_FOOTER_RE = re.compile(r"(?m)^\s*Code of Colorado Regulations\s+\d+\s*$")
# Editor's Notes trailer marker. Some PDFs use "Editor's Notes" / "Editor Notes",
# always preceded/followed by an underline made of underscores.
_EDITORS_NOTES_RE = re.compile(
    r"_{5,}\s*\n\s*Editor[’']?s\s+Notes",
    re.IGNORECASE,
)


def _pdf_text(pdf_bytes: bytes) -> str:
    # pymupdf (fitz) - 5-10x faster than pdfplumber and doesn't leak page
    # trees the way pdfplumber does at scale. Confirmed OK on this scraper's
    # PDFs after the earlier out-of-memory incident with pdfplumber.
    import fitz  # pymupdf

    parts: list[str] = []
    with fitz.open(stream=pdf_bytes, filetype="pdf") as pdf:
        for page in pdf:
            t = page.get_text() or ""
            if t.strip():
                parts.append(t)
    return "\n".join(parts)


def _strip_footers(text: str) -> str:
    return _FOOTER_RE.sub("", text)


def _split_body_and_notes(text: str) -> tuple[str, str]:
    """Split the PDF text into (body, editor's_notes_block). If no notes
    section exists, the second element is ''."""
    m = _EDITORS_NOTES_RE.search(text)
    if not m:
        return text.strip(), ""
    return text[: m.start()].rstrip(), text[m.start() :].strip()


def _parse_history(notes_block: str) -> dict:
    """Pull structured fields from the Editor's Notes / History trailer."""
    history_note = ""
    prior_effective_dates: list[str] = []
    if not notes_block:
        return {"history_note": history_note, "prior_effective_dates": []}
    # The trailer usually reads:
    #     ______________________
    #     Editor's Notes
    #     History
    #     Entire rule eff. 10/30/2007.
    #     Rules 1, 2 amended eff. ...
    # Strip the leading divider and labels, keep the substantive lines.
    body = re.split(r"History\s*\n", notes_block, maxsplit=1)
    history_body = body[1].strip() if len(body) == 2 else notes_block
    # Cap to the next divider if more sections follow.
    history_body = re.split(r"\n_{5,}", history_body, maxsplit=1)[0]
    history_body = re.sub(r"\s+", " ", history_body).strip()
    history_note = history_body
    for m in _HISTORY_DATE_RE.finditer(history_body):
        d = m.group(1)
        if d not in prior_effective_dates:
            prior_effective_dates.append(d)
    return {
        "history_note": history_note,
        "prior_effective_dates": prior_effective_dates,
    }


def _strip_pdf_header(
    body: str, dept_name: str, agency_name: str, citation: str, title: str
) -> str:
    """The PDF leads with department / agency / title / citation lines and a
    bracketed editor's-notes pointer. Drop them so the substantive text leads
    the chunk."""
    # Remove the leading 4-line header by skipping until we hit a line that
    # looks like substantive content (a label, a numbered paragraph, or a
    # capitalized heading other than the agency/title/citation).
    lines = [ln.rstrip() for ln in body.splitlines()]
    # Drop leading blank lines.
    while lines and not lines[0].strip():
        lines.pop(0)
    skip_terms = {
        dept_name.upper().strip(),
        agency_name.upper().strip(),
        citation.upper().strip(),
        title.upper().strip(),
    }
    # Strip the agency-name leading number prefix from skip terms (e.g.
    # "900 DEPARTMENT OF LAW" → "DEPARTMENT OF LAW").
    skip_terms |= {re.sub(r"^\d+\s+", "", t) for t in skip_terms}
    # Skip the first ~8 lines if they match header content.
    cut = 0
    for i, ln in enumerate(lines[:10]):
        s = ln.strip().upper()
        if not s:
            continue
        if s in skip_terms or s.startswith("[EDITOR"):
            cut = i + 1
            continue
        if any(s.startswith(t.split()[0]) for t in skip_terms if t):
            # don't be too aggressive - only skip if short
            if len(s) < 80:
                cut = i + 1
                continue
        break
    return "\n".join(lines[cut:]).strip()


@dataclass
class Rule:
    rule_ref: RuleRef
    rule_title: str
    citation: str
    rule_version_id: str
    effective_date: str  # MM/DD/YYYY
    raw_text: str  # body only, header + editor's notes stripped
    history_note: str
    prior_effective_dates: list[str] = field(default_factory=list)
    source_url: str = ""  # ruleinfo page (HTML landing)
    pdf_url: str = ""  # GenerateRulePdf.do? URL


# ---------------------------------------------------------------------------
# Chunk emission
# ---------------------------------------------------------------------------


def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def _point_id(act_id: str, idx: int, text: str) -> str:
    seed = f"{act_id}::{idx}::{_sha1(text)[:12]}"
    return str(uuid.UUID(hashlib.md5(seed.encode()).hexdigest()))


def _safe(citation: str) -> str:
    return re.sub(r"[^0-9A-Za-z]+", "_", citation).strip("_")


def _dept_prefix(dept_name: str) -> str:
    """Pull the leading numeric prefix from a department / agency name (e.g.
    '900 Department of Law' → '900'). Returns '' if the prefix is absent."""
    m = re.match(r"^\s*([\d,]+)\s+", dept_name)
    return m.group(1).strip() if m else ""


def to_chunk_record(r: Rule) -> dict:
    safe = _safe(r.citation)
    act_id = f"STATE_CO_CCR_{safe}"
    citation = r.citation
    text = r.raw_text
    dept_prefix = _dept_prefix(r.rule_ref.agency.dept_name)
    agency_prefix = _dept_prefix(r.rule_ref.agency.agency_name)
    # Rich, searchable embed header: include the regulation-specific metadata
    # (effective date, issuing agency) so it is retrievable, not just stored.
    meta_lines = []
    if r.effective_date:
        meta_lines.append(f"Effective: {r.effective_date}")
    if r.history_note:
        meta_lines.append(f"History: {r.history_note}")
    meta_header = ("\n" + "\n".join(meta_lines)) if meta_lines else ""
    text_for_embedding = (
        f"Regulation: Code of Colorado Regulations | US | Colorado | In Force\n"
        f"{r.rule_ref.agency.dept_name} / {r.rule_ref.agency.agency_name}\n"
        f"{citation} {r.rule_title}{meta_header}\n\n{text}"
    )
    md = {
        "act_id": act_id,
        "corpus_type": "state_regulation",
        "category": "state_regulation",
        "document_type": "regulation",
        "jurisdiction": "US",
        "country_code": "US",
        "state": "co",
        "title_number": None,
        "title_name": (
            f"Code of Colorado Regulations — {r.rule_ref.agency.dept_name}".rstrip(" —")
        ),
        "title": "Code of Colorado Regulations",
        "title_code": "regs_co",
        "top_level_title": "regs-co",
        "chapter": (
            f"{dept_prefix} / {agency_prefix}"
            if dept_prefix and agency_prefix
            else (dept_prefix or agency_prefix or r.rule_ref.agency.dept_id)
        ),
        "chapter_name": (f"{r.rule_ref.agency.dept_name} / {r.rule_ref.agency.agency_name}"),
        "section_number": citation,
        "section_title": r.rule_title,
        "year": 2026,
        "act_status": "in_force",
        "renumbered_to": "",
        "transferred_to": "",
        "level_classifier": "section",
        # --- regulation-specific rich metadata (captured, not discarded) ---
        "effective_date": r.effective_date or None,
        "published_date": None,
        "statutory_authority": None,
        "promulgated_under": None,
        "rule_amplifies": None,
        "prior_effective_dates": (
            ", ".join(r.prior_effective_dates) if r.prior_effective_dates else None
        ),
        "review_date": None,
        "history_note": r.history_note or None,
        "last_amended_date": (r.prior_effective_dates[-1] if r.prior_effective_dates else None),
        "issuing_agency": r.rule_ref.agency.agency_name or None,
        "issuing_agency_code": agency_prefix or r.rule_ref.agency.agency_id,
        "issuing_department": r.rule_ref.agency.dept_name or None,
        "issuing_department_code": dept_prefix or r.rule_ref.agency.dept_id,
        "rule_version_id": r.rule_version_id or None,
        "rule_id": r.rule_ref.rule_id,
        "citation": citation,
        "citation_short": citation,
        "display_label": citation,
        "display_title": f"{citation} {r.rule_title}".strip(),
        "display_path": (
            f"Code of Colorado Regulations / "
            f"{r.rule_ref.agency.dept_name} / "
            f"{r.rule_ref.agency.agency_name} / {citation}"
        ),
        "breadcrumb": [
            {
                "type": "department",
                "num": dept_prefix or r.rule_ref.agency.dept_id,
                "label": r.rule_ref.agency.dept_name,
                "name": r.rule_ref.agency.dept_name,
            },
            {
                "type": "agency",
                "num": agency_prefix or r.rule_ref.agency.agency_id,
                "label": r.rule_ref.agency.agency_name,
                "name": r.rule_ref.agency.agency_name,
            },
            {
                "type": "rule",
                "num": citation,
                "label": citation,
                "name": r.rule_title,
            },
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
        "amendments_count": len(r.prior_effective_dates),
        "last_amended_year": None,
        "public_laws_referenced": [],
        "public_laws_count": 0,
        "source_url": r.source_url,
        "pdf_url": r.pdf_url,
        "parent_id": (
            f"us/co/regulations/dept={r.rule_ref.agency.dept_id}"
            f"/agency={r.rule_ref.agency.agency_id}"
        ),
        "raw_node_id": (
            f"us/co/regulations/dept={r.rule_ref.agency.dept_id}"
            f"/agency={r.rule_ref.agency.agency_id}/rule={_safe(citation)}"
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


def process_rule(rule_ref: RuleRef) -> Rule | None:
    """Fetch ruleinfo page → resolve current ruleVersionId + effective date →
    download PDF → extract substantive text + History. Returns None if any step
    yields no usable content."""
    info_url = (
        f"{CO_BASE}/CCR/DisplayRule.do?action=ruleinfo"
        f"&ruleId={rule_ref.rule_id}"
        f"&deptID={rule_ref.agency.dept_id}"
        f"&agencyID={rule_ref.agency.agency_id}"
        f"&deptName={quote(rule_ref.agency.dept_name)}"
        f"&agencyName={quote(rule_ref.agency.agency_name)}"
        f"&seriesNum={quote(rule_ref.citation)}"
    )
    info_html = fetch(info_url)
    if not info_html:
        return None
    rv = parse_rule_info(rule_ref, info_html)
    if not rv:
        return None

    pdf_url = (
        f"{CO_BASE}/CCR/GenerateRulePdf.do"
        f"?ruleVersionId={rv.rule_version_id}"
        f"&fileName={quote(rule_ref.citation)}"
    )
    pdf_bytes = fetch(pdf_url, as_bytes=True)
    if not pdf_bytes or not isinstance(pdf_bytes, (bytes, bytearray)):
        return None
    if not pdf_bytes.startswith(b"%PDF"):
        return None
    try:
        raw = _pdf_text(pdf_bytes)
    except Exception:
        return None
    if not raw or len(raw) < 50:
        return None

    cleaned = _strip_footers(raw)
    body_block, notes_block = _split_body_and_notes(cleaned)
    history = _parse_history(notes_block)
    body = _strip_pdf_header(
        body_block,
        rule_ref.agency.dept_name,
        rule_ref.agency.agency_name,
        rule_ref.citation,
        rv.rule_title,
    )
    body = re.sub(r"[ \t]+\n", "\n", body)
    body = re.sub(r"\n{3,}", "\n\n", body).strip()
    if len(body) < 50:
        return None

    return Rule(
        rule_ref=rule_ref,
        rule_title=rv.rule_title,
        citation=rule_ref.citation,
        rule_version_id=rv.rule_version_id,
        effective_date=rv.effective_date,
        raw_text=body,
        history_note=history["history_note"],
        prior_effective_dates=history["prior_effective_dates"],
        source_url=info_url,
        pdf_url=pdf_url,
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--departments",
        default="",
        help="Comma-separated department IDs (e.g. '1,11'). Default: all.",
    )
    ap.add_argument(
        "--agencies",
        default="",
        help="Comma-separated agency IDs (e.g. '11,82'). Default: all.",
    )
    ap.add_argument("--workers", type=int, default=3)
    ap.add_argument("--delay", type=float, default=0.8)
    ap.add_argument(
        "--max-rules",
        type=int,
        default=0,
        help="If >0, stop after fetching this many rules (smoke test).",
    )
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    print(f"[CCR] discovering departments from {CO_TOC}", flush=True)
    agencies = list_departments_and_agencies()
    if args.departments:
        want = {d.strip() for d in args.departments.split(",") if d.strip()}
        agencies = [a for a in agencies if a.dept_id in want]
    if args.agencies:
        want = {a.strip() for a in args.agencies.split(",") if a.strip()}
        agencies = [a for a in agencies if a.agency_id in want]
    print(f"[CCR] {len(agencies)} agencies", flush=True)

    # Phase 1: gather all rule refs
    all_rules: list[RuleRef] = []
    for ag in agencies:
        rules = list_rules_in_agency(ag)
        all_rules.extend(rules)
        print(
            f"  [dept {ag.dept_id} agency {ag.agency_id}] {len(rules)} rules ({ag.agency_name})",
            flush=True,
        )
        time.sleep(args.delay)
    print(f"\n[CCR] {len(all_rules)} rules to fetch", flush=True)
    if args.max_rules > 0:
        all_rules = all_rules[: args.max_rules]
        print(f"[CCR] capped at --max-rules={args.max_rules}", flush=True)
    if args.dry_run:
        return 0

    # Phase 2: crawl rules (each = 1 PDF). Stream-write to JSONL per rule so
    # the process doesn't buffer chunks in memory (earlier pattern hit 5.8 GB
    # RSS before the final flush).
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
    failed = 0
    t0 = time.time()
    with open(OUT, "a") as out_fh, ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {ex.submit(process_rule, rr): rr for rr in all_rules}
        done = 0
        for fut in as_completed(futures):
            done += 1  # noqa: SIM113 (counter over as_completed, not a positional index)
            try:
                rule = fut.result()
                if rule is None:
                    failed += 1
                else:
                    c = to_chunk_record(rule)
                    parsed += 1
                    if c["point_id"] not in seen:
                        out_fh.write(json.dumps(c, ensure_ascii=False) + "\n")
                        out_fh.flush()
                        seen.add(c["point_id"])
                        written += 1
            except Exception as e:
                failed += 1
                print(f"  ! rule failed: {e}", flush=True)
            if done % 25 == 0 or done == len(all_rules):
                rate = parsed / max(time.time() - t0, 0.1)
                print(
                    f"  ... {done:>5}/{len(all_rules)} rules, "
                    f"{parsed:>6} parsed, {written} written, {failed} failed, "
                    f"{rate:.1f}/s",
                    flush=True,
                )
            time.sleep(args.delay / max(args.workers, 1))

    print(
        f"\n=== Done: parsed={parsed:,}, new={written:,}, "
        f"failed={failed}, elapsed={time.time() - t0:.1f}s ===",
        flush=True,
    )
    print(f"JSONL: {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
