#!/usr/bin/env python3
"""Ingest IDAPA — the Idaho Administrative Code — Idaho's state regulations.

OFFICIAL SOURCE ONLY: the Idaho Office of Administrative Rules Coordinator at
https://adminrules.idaho.gov/rules/current/ (no aggregators).

The site is a WordPress front-end backed by an Azure Cognitive Search index.
The "current rules" listing is delivered by a public REST endpoint that the
page's own JS posts to:

    POST /wp-json/dfm-document-display/fetch-documents
        headers: X-WP-Nonce: <nonce scraped from the page>
        body:    {"azurePayload": {"documentType": "currentRules"},
                  "updateAgency": true}

    -> {"data": [ {documentNumber, documentName, agency, idapa, file, ...}, ... ]}

Each `data` item is one IDAPA *chapter docket* (~381 total). `documentNumber`
is the docket itself: ``<agency>.<chapter>.<subchapter>`` (e.g. ``02.08.01``).
`file` is a PDF on Azure blob storage - the rules are PDF-ONLY (no HTML or
bulk XML/zip export exists; checked: every one of the 381 docs is `.pdf`).

So discovery is structured JSON; the bodies are PDFs parsed with pdfplumber
(same approach as ingest_pr_codes.py). Each chapter PDF carries many numbered
*sections* delimited by ``NNN. TITLE.`` headers (zero-padded 3-digit number,
ALL-CAPS title). Per IDAPA convention each provision ends with one or more
``(M-D-YY)`` parenthetical date codes - the last is the effective date, the
full set is the amendment history. We emit one chunk per section.

IDAPA hierarchy / cite form:
    IDAPA <agency>.<chapter>.<section>   e.g. IDAPA 16.03.01.000
    act_id: STATE_ID_IDAPA_<agency>_<chapter>_<section>  (sanitized)

Rich metadata captured (never stripped): effective_date, statutory_authority
(from the ``000. LEGAL AUTHORITY.`` section + the chapter preamble), the
issuing agency, and prior_effective_dates/history. corpus_type='state_regulation',
state='id'.

Geo-restricted; Webshare US proxy + Mozilla UA + polite pacing.
"""

from __future__ import annotations

import argparse
import hashlib
import io
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

sys.stdout.reconfigure(line_buffering=True)  # type: ignore[attr-defined]

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = Path(os.environ.get("OUT_DIR", "./data"))
OUT = DATA_DIR / "state_id_regulations.jsonl"

ID_BASE = "https://adminrules.idaho.gov"
ID_CURRENT = f"{ID_BASE}/rules/current/"
ID_FETCH = f"{ID_BASE}/wp-json/dfm-document-display/fetch-documents"

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


def fetch(url: str, retries: int = 5) -> Optional[str]:
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


def fetch_bytes(url: str, retries: int = 5) -> Optional[bytes]:
    proxies = _us_proxies()
    for attempt in range(retries):
        try:
            r = SESSION.get(url, timeout=120, proxies=proxies, allow_redirects=True)
            if r.status_code == 200:
                return r.content
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
# Discovery: scrape the nonce, POST fetch-documents -> chapter docket list
# ---------------------------------------------------------------------------

_CFG_RE = re.compile(r"var\s+dfmFetchDocuments\s*=\s*(\{.*?\})\s*;", re.DOTALL)


def _discover_config() -> tuple[str, str, str]:
    """Return (rest_base, nonce, document_type) scraped from the live page."""
    html = fetch(ID_CURRENT)
    if not html:
        raise RuntimeError("could not fetch IDAPA current-rules page")
    m = _CFG_RE.search(html)
    if not m:
        raise RuntimeError("could not locate dfmFetchDocuments config (nonce)")
    cfg = json.loads(m.group(1))
    base = cfg.get("rest_base", f"{ID_BASE}/wp-json/dfm-document-display")
    return base, cfg["nonce"], cfg.get("document_type", "currentRules")


@dataclass
class ChapterDoc:
    docket: str  # e.g. "02.08.01" -> agency.chapter.subchapter
    agency_num: str  # zero-padded agency, e.g. "02"
    chapter_id: str  # the docket itself, e.g. "02.08.01"
    chapter_name: str  # e.g. "Sheep and Goat Rules of the ..."
    agency_name: str  # e.g. "Agriculture, Department of"
    year: int
    pdf_url: str


def list_chapter_docs() -> list[ChapterDoc]:
    base, nonce, dtype = _discover_config()
    payload = {"azurePayload": {"documentType": dtype}, "updateAgency": True}
    headers = {
        "X-WP-Nonce": nonce,
        "Content-Type": "application/json",
        "Referer": ID_CURRENT,
        "User-Agent": _MOZ_UA,
    }
    proxies = _us_proxies()
    last_err = None
    for attempt in range(5):
        try:
            r = SESSION.post(
                base + "/fetch-documents",
                headers=headers,
                data=json.dumps(payload),
                timeout=120,
                proxies=proxies,
            )
            if r.status_code == 200:
                data = r.json().get("data", [])
                break
            if r.status_code == 429:
                time.sleep(2**attempt)
                continue
            last_err = f"status {r.status_code}"
            time.sleep(2)
        except Exception as e:
            last_err = str(e)
            time.sleep(2)
    else:
        raise RuntimeError(f"fetch-documents failed: {last_err}")

    out: list[ChapterDoc] = []
    for d in data:
        docket = str(d.get("documentNumber", "")).strip()
        pdf_url = str(d.get("file", "")).strip()
        if not docket or not pdf_url.lower().endswith(".pdf"):
            continue
        agency_num = docket.split(".")[0]
        name = str(d.get("documentName", "")).strip()
        # documentName is usually "02.08.01 - <chapter name>"; strip the docket prefix
        chap_name = re.sub(rf"^{re.escape(docket)}\s*[-–—]\s*", "", name).strip()
        try:
            year = int(d.get("year") or 0)
        except (TypeError, ValueError):
            year = 0
        out.append(
            ChapterDoc(
                docket=docket,
                agency_num=agency_num,
                chapter_id=docket,
                chapter_name=chap_name or name,
                agency_name=str(d.get("agency", "")).strip(),
                year=year or 2026,
                pdf_url=pdf_url,
            )
        )
    out.sort(key=lambda c: c.docket)
    return out


# ---------------------------------------------------------------------------
# PDF parse: chapter PDF -> per-section chunks
# ---------------------------------------------------------------------------

# Section header at start of line: "NNN. TITLE." (3-digit zero-padded number).
_SECTION_RE = re.compile(r"(?m)^(\d{3})\.\s+(.+?)$")
# Trailing parenthetical date codes, e.g. (7-1-25), (3-23-23)
_DATE_RE = re.compile(r"\((\d{1,2}-\d{1,2}-\d{2,4})\)")
# Footer/running-header blocks injected on every PDF page.
_FOOTER_PAGE_RE = re.compile(r"(?m)^Section\s+\d+\s+Page\s+\d+\s*$")
_HEADER_CODE_RE = re.compile(r"(?m)^IDAHO ADMINISTRATIVE CODE\s+IDAPA[^\n]*$")
_RESERVED_RE = re.compile(r"\(RESERVED\)", re.IGNORECASE)


def _pdf_text(pdf_bytes: bytes) -> str:
    import pdfplumber

    parts: list[str] = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            t = page.extract_text() or ""
            if t.strip():
                parts.append(t)
    return "\n".join(parts)


def _is_section_header(title: str) -> bool:
    """Real IDAPA section titles are ALL-CAPS and end with a period.

    This distinguishes them from Table-of-Contents lines (which carry dotted
    leaders and page numbers, or wrap in title-case) and from in-body numbered
    subsections (which are title-case, e.g. "01. Authorized Federal Inspector.").
    """
    t = title.strip()
    if "...." in t:  # TOC dotted leader
        return False
    if re.search(r"\.\s*\d+\s*$", t):  # TOC "... page N"
        return False
    if _RESERVED_RE.search(t):
        return False
    core = re.sub(r"\([^)]*\)|[^A-Za-z ]", " ", t)  # drop "(A THROUGH L)", punct
    letters = [c for c in core if c.isalpha()]
    if not letters:
        return False
    upper = sum(1 for c in letters if c.isupper())
    return (upper / len(letters)) > 0.85 and t.endswith(".")


def _strip_footers(text: str) -> str:
    """Remove the repeated 3-line page footer/running-header blocks."""
    out_lines: list[str] = []
    lines = text.split("\n")
    i = 0
    while i < len(lines):
        if _FOOTER_PAGE_RE.match(lines[i]):
            # skip "Section N Page N" + the two running-header lines that follow
            i += 1
            if i < len(lines) and _HEADER_CODE_RE.match(lines[i]):
                i += 1
                # the agency/division running-head line typically ends in "Rules"
                if i < len(lines) and lines[i].strip():
                    i += 1
            continue
        if _HEADER_CODE_RE.match(lines[i]):
            i += 1
            if i < len(lines) and lines[i].strip().endswith("Rules"):
                i += 1
            continue
        out_lines.append(lines[i])
        i += 1
    return "\n".join(out_lines)


def _clean_body(raw: str) -> str:
    body = _strip_footers(raw)
    body = re.sub(r"\s+", " ", body).strip()
    return body


def _extract_preamble_authority(text: str) -> str:
    """The chapter preamble Q&A includes a 'legal authority' block listing the
    enabling statutes. Used as a fallback for statutory_authority.
    """
    m = re.search(
        r"What is the legal authority[^\n]*\n(.*?)"
        r"(?:Who do I contact|Where can I find|Table of Contents)",
        text,
        re.IGNORECASE | re.DOTALL,
    )
    if not m:
        return ""
    block = re.sub(r"\s+", " ", m.group(1)).strip()
    return block[:1200]


@dataclass
class Section:
    docket: str
    agency_num: str
    chapter_id: str
    chapter_name: str
    agency_name: str
    section_num: str  # "000", "010", "100" ...
    section_title: str  # cleaned, title-case
    raw_text: str
    source_url: str
    pdf_url: str
    year: int
    effective_date: str = ""  # last (M-D-YY) in the section
    statutory_authority: str = ""  # from 000 LEGAL AUTHORITY / preamble
    rule_amplifies: str = ""  # statutes the rule implements (preamble)
    prior_effective_dates: list[str] = field(default_factory=list)  # all date codes
    issuing_agency: str = ""


def _title_case(caps_title: str) -> str:
    """Convert an ALL-CAPS section title to a readable title, preserving the
    parenthetical qualifiers like (A THROUGH L)."""
    t = caps_title.strip().rstrip(".")
    # leave acronyms be by only lowering long runs; simple title-casing is fine
    return t.title().replace("’S", "’s").replace("'S", "'s")


def parse_chapter_pdf(text: str, doc: ChapterDoc) -> list[Section]:
    text = text.replace("\r\n", "\n")
    matches = list(_SECTION_RE.finditer(text))
    headers = [m for m in matches if _is_section_header(m.group(2))]
    if not headers:
        return []

    preamble_auth = _extract_preamble_authority(text)
    chapter_legal_auth = ""

    out: list[Section] = []
    for i, m in enumerate(headers):
        sec_num = m.group(1)
        title_caps = m.group(2).strip()
        start = m.end()
        end = headers[i + 1].start() if i + 1 < len(headers) else len(text)
        body = _clean_body(text[start:end])
        if len(body) < 20:
            continue
        dates = _DATE_RE.findall(body)
        effective = dates[-1] if dates else ""
        # The 000. LEGAL AUTHORITY. section body names the enabling statutes.
        if sec_num == "000" and ("Idaho Code" in body or "Section" in body):
            chapter_legal_auth = re.split(r"\(\d{1,2}-\d{1,2}-\d{2,4}\)", body)[0].strip()

        out.append(
            Section(
                docket=doc.docket,
                agency_num=doc.agency_num,
                chapter_id=doc.chapter_id,
                chapter_name=doc.chapter_name,
                agency_name=doc.agency_name,
                section_num=sec_num,
                section_title=_title_case(title_caps),
                raw_text=body,
                source_url=ID_CURRENT,
                pdf_url=doc.pdf_url,
                year=doc.year,
                effective_date=effective,
                prior_effective_dates=sorted(set(dates)),
                issuing_agency=doc.agency_name,
            )
        )

    # Backfill statutory authority across the chapter (000 section preferred,
    # else the preamble Q&A block). Never strip - capture into structured field.
    authority = chapter_legal_auth or preamble_auth
    for s in out:
        s.statutory_authority = authority
        s.rule_amplifies = preamble_auth
    return out


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


def _act_id(s: Section) -> str:
    # The docket (e.g. "02.08.01") already encodes agency.chapter.subchapter;
    # the section number is the final level. So the full identity is
    # STATE_ID_IDAPA_<docket>_<section> -> STATE_ID_IDAPA_02_08_01_010.
    return f"STATE_ID_IDAPA_{_safe(s.docket)}_{_safe(s.section_num)}"


def to_chunk_record(s: Section) -> dict:
    act_id = _act_id(s)
    # IDAPA cite form: IDAPA <agency>.<chapter>.<section>
    citation = f"IDAPA {s.chapter_id}.{s.section_num}"
    section_label = f"Section {s.section_num}. {s.section_title}"
    text = s.raw_text

    # Rich, searchable embed header: surface the regulation-specific metadata
    # (effective date, enabling statute) so it is retrievable, not just stored.
    meta_lines = []
    if s.effective_date:
        meta_lines.append(f"Effective: {s.effective_date}")
    if s.statutory_authority:
        meta_lines.append(f"Statutory Authority: {s.statutory_authority}")
    meta_header = ("\n" + "\n".join(meta_lines)) if meta_lines else ""
    text_for_embedding = (
        f"Regulation: Idaho Administrative Code (IDAPA) | US | Idaho | In Force\n"
        f"Agency {s.agency_num} ({s.agency_name}) / Chapter {s.chapter_id}: "
        f"{s.chapter_name}\n"
        f"{citation} — {section_label}{meta_header}\n\n{text}"
    )
    md = {
        "act_id": act_id,
        "corpus_type": "state_regulation",
        "category": "state_regulation",
        "document_type": "regulation",
        "jurisdiction": "US",
        "country_code": "US",
        "state": "id",
        "title_number": None,
        "title_name": f"Idaho Administrative Code — IDAPA {s.agency_num} ({s.agency_name})",
        "title": "Idaho Administrative Code",
        "title_code": f"idapa_{s.agency_num}",
        "top_level_title": s.agency_num,
        "chapter": s.chapter_id,
        "chapter_name": s.chapter_name,
        "section_number": s.section_num,
        "section_title": section_label,
        "year": s.year,
        "act_status": "in_force",
        "renumbered_to": "",
        "transferred_to": "",
        "level_classifier": "rule",
        # --- regulation-specific rich metadata (captured, not discarded) ---
        "effective_date": s.effective_date or None,
        "promulgated_under": None,
        "statutory_authority": s.statutory_authority or None,
        "rule_amplifies": s.rule_amplifies or None,
        "prior_effective_dates": (
            ", ".join(s.prior_effective_dates) if s.prior_effective_dates else None
        ),
        "review_date": None,
        "issuing_agency": s.issuing_agency or None,
        "issuing_agency_code": s.agency_num,
        "citation": citation,
        "citation_short": citation,
        "display_label": citation,
        "display_title": section_label,
        "display_path": (
            f"Idaho Administrative Code / IDAPA {s.agency_num} ({s.agency_name}) / "
            f"Chapter {s.chapter_id} / Section {s.section_num}"
        ),
        "breadcrumb": [
            {
                "type": "agency",
                "num": s.agency_num,
                "label": f"IDAPA {s.agency_num}",
                "name": s.agency_name,
            },
            {
                "type": "chapter",
                "num": s.chapter_id,
                "label": f"Chapter {s.chapter_id}",
                "name": s.chapter_name,
            },
            {
                "type": "section",
                "num": s.section_num,
                "label": f"Section {s.section_num}",
                "name": s.section_title,
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
        "amendments_count": 0,
        "last_amended_year": None,
        "public_laws_referenced": [],
        "public_laws_count": 0,
        "source_url": s.source_url,
        "pdf_url": s.pdf_url,
        "parent_id": f"us/id/regulations/agency={s.agency_num}/chapter={s.chapter_id}",
        "raw_node_id": (
            f"us/id/regulations/agency={s.agency_num}/chapter={s.chapter_id}/"
            f"section={s.section_num}"
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


def process_chapter(doc: ChapterDoc) -> list[Section]:
    pdf_bytes = fetch_bytes(doc.pdf_url)
    if not pdf_bytes:
        print(f"  ! fetch failed: {doc.docket} {doc.pdf_url}", flush=True)
        return []
    try:
        text = _pdf_text(pdf_bytes)
    except Exception as e:
        print(f"  ! pdf parse err {doc.docket}: {e}", flush=True)
        return []
    sections = parse_chapter_pdf(text, doc)
    return sections


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--agencies",
        default="",
        help="Comma-separated IDAPA agency numbers (e.g. '02,16'). Default: all.",
    )
    ap.add_argument(
        "--dockets",
        default="",
        help="Comma-separated full dockets (e.g. '16.03.01'). Overrides --agencies.",
    )
    ap.add_argument("--workers", type=int, default=3)
    ap.add_argument("--delay", type=float, default=0.8)
    ap.add_argument("--limit", type=int, default=0, help="Cap chapters (testing).")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    print(f"[IDAPA] discovering chapter dockets from {ID_CURRENT}", flush=True)
    docs = list_chapter_docs()
    print(f"[IDAPA] {len(docs)} chapter dockets discovered", flush=True)

    if args.dockets:
        wanted = {d.strip() for d in args.dockets.split(",") if d.strip()}
        docs = [d for d in docs if d.docket in wanted]
    elif args.agencies:
        wanted = {a.strip().zfill(2) for a in args.agencies.split(",") if a.strip()}
        docs = [d for d in docs if d.agency_num in wanted]
    if args.limit:
        docs = docs[: args.limit]

    print(f"[IDAPA] {len(docs)} chapters to fetch", flush=True)
    if args.dry_run:
        for d in docs[:20]:
            print(f"  {d.docket}  {d.agency_name[:30]:30}  {d.pdf_url}")
        if len(docs) > 20:
            print(f"  ... and {len(docs) - 20} more")
        return 0

    chunks: list[dict] = []
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {ex.submit(process_chapter, d): d for d in docs}
        done = 0
        for fut in as_completed(futures):
            done += 1
            try:
                sections = fut.result()
                chunks.extend(to_chunk_record(x) for x in sections)
            except Exception as e:
                print(f"  ! chapter failed: {e}", flush=True)
            if done % 25 == 0 or done == len(docs):
                rate = len(chunks) / max(time.time() - t0, 0.1)
                print(
                    f"  ... {done:>4}/{len(docs)} chapters, "
                    f"{len(chunks):>6} sections, {rate:.1f}/s",
                    flush=True,
                )
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

    print(
        f"\n=== Done: parsed={len(chunks):,}, new={written:,}, elapsed={time.time() - t0:.1f}s ===",
        flush=True,
    )
    print(f"JSONL: {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
