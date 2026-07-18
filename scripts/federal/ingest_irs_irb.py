#!/usr/bin/env python3
"""Ingest IRS Internal Revenue Bulletin (IRB) agency guidance into ``statutes_us``.

OFFICIAL SOURCE ONLY: the ``irs.gov`` "drop" folder at
``https://www.irs.gov/pub/irs-drop/`` (no aggregators, no third-party mirrors).
That folder is the same authoritative store the IRS uses to publish every
Revenue Procedure, Revenue Ruling, Notice, and Announcement one PDF at a time
before those documents are compiled into the weekly Internal Revenue Bulletin.

Why /pub/irs-drop and not the IRB weekly landing pages: the weekly HTML pages
at ``/irb/<year>-<week>_IRB`` mention each document by *label* only ("Rev.
Proc. 2024-9") -- they do not link to the drop PDFs, so we cannot enumerate
from them. The drop directory itself lists only the ~46 most recent PDFs and
otherwise returns nothing (Apache autoindex is capped), so a direct listing
does not cover the 10-year backfill. But the drop filenames are deterministic:

    <prefix>-<YY>-<NN>.pdf  (2-digit year, 2-digit zero-padded index)

    prefix -> doc_type
      rp   -> Revenue Procedure   (``rev_proc``)
      rr   -> Revenue Ruling      (``rev_rul``)
      n    -> Notice              (``notice``)
      a    -> Announcement        (``announcement``)

So the discovery layer is a gap-tolerant HEAD-probe of ``NN=1..MAX`` per
(year, prefix). Confirmed by probing 2015..2026 for every prefix -- all four
document types resolve at this URL scheme, with occasional gaps (some
sequence numbers are never issued or are pulled from public distribution).
The probe stops after ``--stop-after-misses`` consecutive 404s beyond the
last hit, so the wasted request count is bounded.

Rich per-document metadata is captured (never stripped): doc_number, year,
doc_index, subject/topic, effective/applicable date, cross-references to
prior guidance ("modifies", "supersedes", "amplifies", "clarifies",
"obsoletes"), IRC section citations (26 U.S.C. §), 26 CFR citations, and
IRB weekly-bulletin references.

Emitted schema is the standard chunk envelope shared with USC/CFR/state
scrapers, with:

    corpus_type     = "agency_guidance"     (NEW namespace)
    category        = "irs_rev_proc" | "irs_rev_rul" | "irs_notice" | "irs_announcement"
    document_type   = same as category
    state           = "federal"
    jurisdiction    = "US"
    level_classifier = "guidance"
    title_code      = "irb"
    top_level_title = "irb-<year>"
    title_number    = 9300   (synthetic; free slot -- no collision with USC/CFR)
    title           = "Internal Revenue Bulletin"

Geo-restricted; Webshare US proxy + Mozilla UA + polite pacing.

Usage:

    # dry-run one year, print a sample per doc_type
    PYTHONPATH=. python -m scripts.federal.ingest_irs_irb \\
        --years 2024 --limit-per-type 3 --dry-run

    # full backfill 2015..2025, 5 workers
    OUT_DIR=/path/to/data \\
    PYTHONPATH=. python -m scripts.federal.ingest_irs_irb \\
        --years 2015-2025 --workers 5
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

import requests

sys.stdout.reconfigure(line_buffering=True)  # type: ignore[attr-defined]

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = Path(os.environ.get("OUT_DIR", "./data"))
OUT = DATA_DIR / "state_irs_irb.jsonl"

DROP_BASE = "https://www.irs.gov/pub/irs-drop"

# Synthetic title_number for the IRB. USC uses 1..54; CFR uses 1..50. 9300
# is safely in a free block, matching the pattern other agency-guidance
# ingesters have adopted (executive actions live at 9200).
IRB_TITLE_NUMBER = 9300

_MOZ_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
)

# ---------------------------------------------------------------------------
# doc_type <-> URL prefix map
# ---------------------------------------------------------------------------

# The filename prefix is the URL-side scheme (rp/rr/n/a). The other columns
# drive the emitted metadata: `category` / `document_type` payloads, the
# short "IRS_XX" component of act_id, the human-facing label ("Rev. Proc.")
# and the plain-English breadcrumb name.
DOC_TYPES: dict[str, dict[str, str]] = {
    "rp": {
        "doc_type": "rev_proc",
        "category": "irs_rev_proc",
        "act_key": "RP",
        "label": "Rev. Proc.",
        "long_label": "Revenue Procedure",
    },
    "rr": {
        "doc_type": "rev_rul",
        "category": "irs_rev_rul",
        "act_key": "RR",
        "label": "Rev. Rul.",
        "long_label": "Revenue Ruling",
    },
    "n": {
        "doc_type": "notice",
        "category": "irs_notice",
        "act_key": "NOTICE",
        "label": "Notice",
        "long_label": "Notice",
    },
    "a": {
        "doc_type": "announcement",
        "category": "irs_announcement",
        "act_key": "ANN",
        "label": "Announcement",
        "long_label": "Announcement",
    },
}


# ---------------------------------------------------------------------------
# Env / proxy (same pattern as sibling scrapers so behavior is uniform)
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


def _us_proxies() -> dict | None:
    user = os.environ.get("WEBSHARE_USERNAME", "")
    pwd = os.environ.get("WEBSHARE_PASSWORD", "")
    if not user or not pwd:
        return None
    proxy_user = f"{user}-US-rotate"
    host = os.environ.get("WEBSHARE_PROXY_HOST", "p.webshare.io")
    port = os.environ.get("WEBSHARE_PROXY_PORT", "80")
    url = f"http://{urllib.parse.quote(proxy_user)}:{urllib.parse.quote(pwd)}@{host}:{port}"
    return {"http": url, "https": url}


SESSION = requests.Session()
SESSION.headers.update({"User-Agent": _MOZ_UA})


def _head_ok(url: str, retries: int = 3) -> bool:
    """Cheap probe: is this PDF fetchable? True iff 200 + pdf Content-Type."""
    proxies = _us_proxies()
    for attempt in range(retries):
        try:
            r = SESSION.head(url, timeout=30, proxies=proxies, allow_redirects=True)
            if r.status_code == 200:
                ct = r.headers.get("Content-Type", "")
                return "pdf" in ct.lower()
            if r.status_code == 404:
                return False
            if r.status_code == 429:
                time.sleep(2**attempt)
                continue
            if r.status_code in (502, 503, 504):
                time.sleep(1)
                continue
            return False
        except Exception:
            time.sleep(1)
    return False


def _fetch_bytes(url: str, retries: int = 5) -> bytes | None:
    proxies = _us_proxies()
    for attempt in range(retries):
        try:
            r = SESSION.get(url, timeout=120, proxies=proxies, allow_redirects=True)
            if r.status_code == 200 and "pdf" in r.headers.get("Content-Type", "").lower():
                return r.content
            if r.status_code == 404:
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


# ---------------------------------------------------------------------------
# Discovery: gap-tolerant HEAD-probe per (year, prefix)
# ---------------------------------------------------------------------------


def _drop_url(prefix: str, year: int, idx: int) -> str:
    """Filename scheme: ``<prefix>-<YY>-<NN>.pdf`` (2-digit year, 2-digit
    zero-padded index). Confirmed by probing 2015..2026 -- unpadded indices
    (``rp-24-8.pdf``) 404. Four-digit year (``rp-2024-08.pdf``) also 404s.
    """
    yy = f"{year % 100:02d}"
    nn = f"{idx:02d}"
    return f"{DROP_BASE}/{prefix}-{yy}-{nn}.pdf"


def enumerate_docs(
    prefix: str,
    year: int,
    max_probe: int,
    stop_after_misses: int,
    workers: int = 4,
) -> list[tuple[int, str]]:
    """Probe ``NN = 1..max_probe`` and return the OK ones as ``(idx, url)``.

    Stops early once we have seen ``stop_after_misses`` consecutive misses
    beyond the last hit. Uses a small worker pool because HEAD requests
    dominate: 200 sequential requests through the US proxy is ~20 s but
    4-way parallel drops that to ~5 s.
    """
    hits: list[tuple[int, str]] = []
    last_hit_idx = 0
    seen_hits = 0

    # Probe in batches so we can bail out early after N consec misses.
    batch = max(workers * 4, 16)
    idx = 1
    while idx <= max_probe:
        end = min(idx + batch - 1, max_probe)
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures = {
                ex.submit(_head_ok, _drop_url(prefix, year, i)): i for i in range(idx, end + 1)
            }
            batch_results: dict[int, bool] = {}
            for fut in as_completed(futures):
                i = futures[fut]
                try:
                    batch_results[i] = fut.result()
                except Exception:
                    batch_results[i] = False
        for i in range(idx, end + 1):
            if batch_results.get(i):
                hits.append((i, _drop_url(prefix, year, i)))
                last_hit_idx = i
                seen_hits += 1
        idx = end + 1
        # early bail: if we already have hits AND we are more than
        # `stop_after_misses` past the last hit, we are past the sequence tail
        if seen_hits and (idx - 1 - last_hit_idx) >= stop_after_misses:
            break
    return sorted(hits, key=lambda t: t[0])


# ---------------------------------------------------------------------------
# PDF text extraction + rich metadata parse
# ---------------------------------------------------------------------------


def _pdf_text(pdf_bytes: bytes) -> str:
    """PyMuPDF extraction. Faster than pdfplumber and preserves line breaks
    IRS PDFs need for header vs. body separation."""
    import fitz  # PyMuPDF

    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    try:
        return "".join(page.get_text() for page in doc)
    finally:
        doc.close()


_DOC_NUMBER_RE = re.compile(
    r"(?:Revenue\s+Procedure|Rev\.\s*Proc\.|Revenue\s+Ruling|"
    r"Rev\.\s*Rul\.|Notice|Announcement)\s+(\d{4}-\d+)",
    re.IGNORECASE,
)

# In the header block that appears before the doc-number label, the
# subject/topic is one or two short lines. Capture the last non-empty
# lines above the doc-number marker.
_BLANKS_RE = re.compile(r"\n\s*\n")

# "SECTION 1. PURPOSE" (or "PURPOSE AND SCOPE") first paragraph is the
# authoritative subject if the header did not carry one. IRS uses spaced
# periods on some documents, so tolerate the variants.
_PURPOSE_RE = re.compile(
    r"SECTION\s+1\.\s*(?:PURPOSE\s+AND\s+SCOPE|PURPOSE|BACKGROUND\s+AND\s+PURPOSE)\s*\n(.+?)"
    r"(?=\n\s*SECTION\s+2\.|\n\s*\.0\d\.|\Z)",
    re.IGNORECASE | re.DOTALL,
)

_EFFECTIVE_DATE_RE = re.compile(
    r"(?i)(?:effective\s+date\s+is|(?:this\s+(?:notice|revenue\s+procedure|"
    r"revenue\s+ruling|announcement)\s+is\s+)?effective(?:\s+on)?)\s+"
    r"([A-Z][a-z]+\s+\d{1,2},\s+\d{4})"
)
_APPLICABLE_DATE_RE = re.compile(
    r"(?i)applicable\s+(?:on|date\s+is)\s+([A-Z][a-z]+\s+\d{1,2},\s+\d{4})"
)
_ISSUED_DATE_RE = re.compile(
    r"(?i)(?:issued|dated|published)\s+(?:on\s+)?"
    r"([A-Z][a-z]+\s+\d{1,2},\s+\d{4})"
)

_IRB_REF_RE = re.compile(r"(\d{4}-\d+\s+I\.R\.B\.\s*\d*)")
# Doc-label matcher used to walk the weekly IRB HTML index and build a
# {doc_number_label -> "YYYY-WW I.R.B."} lookup for the ``irb_reference``
# field. The drop-folder PDFs do NOT self-cite the weekly bulletin they
# appear in (the weekly compilation is published later), so this lookup
# is the only reliable way to populate the field.
_WEEKLY_DOC_LABEL_RE = re.compile(
    r"(Rev\.\s*Proc\.\s*\d{4}-\d+|Rev\.\s*Rul\.\s*\d{4}-\d+|"
    r"Notice\s+\d{4}-\d+|Announcement\s+\d{4}-\d+)"
)

# Cross-reference verbs: each one gets its own field so downstream services
# can render a "See also: modifies Rev. Proc. 2020-3" style badge without
# re-parsing the text.
_CROSSREF_VERBS = ("modifies", "supersedes", "amplifies", "clarifies", "obsoletes")
_CROSSREF_RE_TPL = (
    r"(?i)\b{verb}\b(?:[\s,]+(?:in\s+part|and\s+supersedes|and\s+amplifies))?[\s,]+"
    r"(Rev\.\s*Proc\.\s*\d{{2,4}}-\d+|Rev\.\s*Rul\.\s*\d{{2,4}}-\d+|"
    r"Notice\s+\d{{2,4}}-\d+|Announcement\s+\d{{2,4}}-\d+)"
)

_IRC_RE = re.compile(
    r"(?:26\s+U\.?S\.?C\.?\s+)?"
    r"(?:§|[Ss]ection|[Ss]ec\.)\s*"
    r"(\d+[A-Z]?(?:\([a-z0-9]+\))*)"
)
_CFR_RE = re.compile(r"26\s+CFR\s+(?:§\s*)?(\d+[A-Za-z0-9.\-]+)")

_PRIOR_GUIDANCE_RE = re.compile(
    r"(Rev\.\s*Proc\.\s*\d{2,4}-\d+|Rev\.\s*Rul\.\s*\d{2,4}-\d+|"
    r"Notice\s+\d{2,4}-\d+|Announcement\s+\d{2,4}-\d+)"
)


def _clean_ws(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def _find_doc_number(text: str) -> re.Match | None:
    """Return the FIRST match, which is the doc's own number (not a cite).

    IRS body text often mentions many other Rev Proc / Rev Rul / Notice
    numbers; the doc's own number is the first labeled line at the top of
    the document, immediately after the subject header.
    """
    return _DOC_NUMBER_RE.search(text)


def _extract_subject(text: str, doc_number_pos: int) -> str:
    """Subject/topic is the header text just before the doc-number label.

    Fallback: the first paragraph of SECTION 1. PURPOSE. Both are cleaned to
    a single-line string suitable for a display badge and for the embedding
    header line, capped at a reasonable length.
    """
    header_block = text[:doc_number_pos]
    # keep only the last 1-3 non-empty text lines above the doc number
    lines = [line.strip() for line in header_block.splitlines()]
    non_empty = [line for line in lines if line and not line.isspace()]
    if non_empty:
        # take the last 1..3 lines that look like plain prose (not "Part III"
        # or numbered section pointers), joined with a space
        candidates = [line for line in non_empty[-4:] if not re.match(r"^(Part\s+\w+|\d+)$", line)]
        if candidates:
            subj = _clean_ws(" ".join(candidates[-2:]))
            if 3 < len(subj) < 400:
                return subj.rstrip(".")

    # fallback: SECTION 1. PURPOSE first line/sentence
    m = _PURPOSE_RE.search(text)
    if m:
        body = _clean_ws(m.group(1))
        # first sentence, capped
        first_sent = re.split(r"(?<=[.!?])\s+", body)[0]
        if 5 < len(first_sent) < 400:
            return first_sent.rstrip(".")
        return body[:300].rstrip(".")
    return ""


def _first_present(*matches: re.Match | None) -> str:
    for m in matches:
        if m:
            return m.group(1)
    return ""


@dataclass
class IrbDoc:
    prefix: str  # "rp" | "rr" | "n" | "a"
    year: int
    doc_index: int
    source_url: str
    pdf_bytes: bytes
    raw_text: str
    # extracted
    doc_number_label: str = ""  # e.g. "Rev. Proc. 2024-8"
    doc_number_long: str = ""  # e.g. "Revenue Procedure 2024-8"
    subject: str = ""
    # Own weekly IRB compilation citation (e.g. "2024-05 I.R.B."). Populated
    # from the weekly-IRB HTML lookup, NOT from the drop PDF itself (which
    # never self-cites). Blank when the lookup is unavailable or when the
    # doc has not yet been rolled into a compiled weekly bulletin.
    irb_reference: str = ""
    # All IRB citations found in the body text -- useful for citation graph
    # ("this Rev. Proc. references 2014-2 I.R.B. 295") but distinct from
    # the doc's own IRB week.
    irb_references_in_body: list[str] = field(default_factory=list)
    effective_date: str = ""
    applicable_date: str = ""
    issued_date: str = ""
    modifies: list[str] = field(default_factory=list)
    supersedes: list[str] = field(default_factory=list)
    amplifies: list[str] = field(default_factory=list)
    clarifies: list[str] = field(default_factory=list)
    obsoletes: list[str] = field(default_factory=list)
    code_sections_referenced: list[str] = field(default_factory=list)
    regs_referenced: list[str] = field(default_factory=list)
    prior_guidance_cited: list[str] = field(default_factory=list)
    taxonomy_keywords: list[str] = field(default_factory=list)


def parse_irb_pdf(prefix: str, year: int, idx: int, url: str, pdf_bytes: bytes) -> IrbDoc | None:
    try:
        text = _pdf_text(pdf_bytes)
    except Exception as e:
        print(f"  ! pymupdf parse failed for {url}: {e}", flush=True)
        return None
    if not text or len(text) < 100:
        return None

    doc = IrbDoc(
        prefix=prefix,
        year=year,
        doc_index=idx,
        source_url=url,
        pdf_bytes=pdf_bytes,
        raw_text=text,
    )

    dm = _find_doc_number(text)
    if dm:
        doc.doc_number_long = _clean_ws(dm.group(0))
        # canonicalize into "Rev. Proc. YYYY-N" short form using DOC_TYPES.label
        label = DOC_TYPES[prefix]["label"]
        doc.doc_number_label = f"{label} {dm.group(1)}"
        doc.subject = _extract_subject(text, dm.start())
    else:
        # fall back to constructed label from filename -- always present
        label = DOC_TYPES[prefix]["label"]
        doc.doc_number_label = f"{label} {year}-{idx}"
        doc.doc_number_long = doc.doc_number_label

    # Body IRB cites (dedup, cap at 20). The doc's OWN weekly IRB is set
    # separately from the weekly-IRB HTML lookup.
    body_irb = sorted({_clean_ws(m.group(1)) for m in _IRB_REF_RE.finditer(text)})
    doc.irb_references_in_body = body_irb[:20]

    doc.effective_date = _first_present(_EFFECTIVE_DATE_RE.search(text))
    doc.applicable_date = _first_present(_APPLICABLE_DATE_RE.search(text))
    doc.issued_date = _first_present(_ISSUED_DATE_RE.search(text))

    # cross-refs: dedupe + skip self-references (a document sometimes cites
    # itself in the drafting information block)
    self_cite = doc.doc_number_label.replace(" ", "").lower()

    def _clean_refs(refs: list[str]) -> list[str]:
        out: list[str] = []
        seen: set[str] = set()
        for ref in refs:
            r = _clean_ws(ref)
            key = r.replace(" ", "").lower()
            if key == self_cite or key in seen:
                continue
            seen.add(key)
            out.append(r)
        return out

    for verb in _CROSSREF_VERBS:
        pattern = re.compile(_CROSSREF_RE_TPL.format(verb=verb))
        refs = pattern.findall(text)
        setattr(doc, verb, _clean_refs(refs))

    doc.code_sections_referenced = sorted({m.group(1) for m in _IRC_RE.finditer(text)})[:80]
    doc.regs_referenced = sorted({m.group(1) for m in _CFR_RE.finditer(text)})[:40]
    doc.prior_guidance_cited = _clean_refs(_PRIOR_GUIDANCE_RE.findall(text))[:60]

    return doc


# ---------------------------------------------------------------------------
# Chunk emission
# ---------------------------------------------------------------------------


def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def _point_id(act_id: str, idx: int, text: str) -> str:
    seed = f"{act_id}::{idx}::{_sha1(text)[:12]}"
    return str(uuid.UUID(hashlib.md5(seed.encode()).hexdigest()))


def _act_id(doc: IrbDoc) -> str:
    key = DOC_TYPES[doc.prefix]["act_key"]
    return f"IRS_{key}_{doc.year}_{doc.doc_index}"


def _text_for_embedding(doc: IrbDoc) -> str:
    meta = DOC_TYPES[doc.prefix]
    header = f"Agency Guidance: IRS {meta['long_label']} | US | Federal"
    subj = doc.subject or "(untitled)"
    line2 = f"{doc.year} IRB — {subj}"
    line3 = doc.doc_number_label
    tags: list[str] = []
    if doc.issued_date:
        tags.append(f"[Issued: {doc.issued_date}]")
    if doc.effective_date:
        tags.append(f"[Effective: {doc.effective_date}]")
    if doc.applicable_date:
        tags.append(f"[Applicable: {doc.applicable_date}]")
    tagline = " ".join(tags)
    return f"{header}\n{line2}\n{line3}\n{tagline}\n\n{doc.raw_text}".rstrip()


def to_chunk_record(doc: IrbDoc) -> dict:
    meta = DOC_TYPES[doc.prefix]
    act_id = _act_id(doc)
    citation = doc.doc_number_label
    subject_line = doc.subject or citation
    display_title = f"{citation} — {subject_line}" if doc.subject else citation

    text = doc.raw_text
    text_for_embedding = _text_for_embedding(doc)

    breadcrumb = [
        {"type": "corpus", "label": "IRS IRB"},
        {"type": "doc_type", "label": meta["long_label"]},
        {"type": "year", "num": str(doc.year), "label": str(doc.year)},
        {"type": "doc", "num": str(doc.doc_index), "label": citation},
    ]

    payload = {
        # core identity
        "act_id": act_id,
        "corpus_type": "agency_guidance",
        "category": meta["category"],
        "document_type": meta["category"],
        "jurisdiction": "US",
        "country_code": "US",
        "state": "federal",
        "act_status": "in_force",
        "level_classifier": "guidance",
        # title hierarchy: title_number stays synthetic so the IRB does not
        # collide with a real USC / CFR title. top_level_title carries the
        # human-usable "irb-<year>" grouping.
        "title_number": IRB_TITLE_NUMBER,
        "title_code": "irb",
        "title": "Internal Revenue Bulletin",
        "title_name": "Internal Revenue Bulletin",
        "top_level_title": f"irb-{doc.year}",
        "chapter": str(doc.year),
        "chapter_name": f"IRB {doc.year}",
        "section_number": str(doc.doc_index),
        "section_title": subject_line,
        "year": doc.year,
        # doc-level metadata (rich, never stripped)
        "doc_type": meta["doc_type"],
        "doc_number": citation,
        "doc_index": doc.doc_index,
        "irb_reference": doc.irb_reference or None,
        "irb_references_in_body": doc.irb_references_in_body,
        "issued_date": doc.issued_date or None,
        "effective_date": doc.effective_date or None,
        "applicable_date": doc.applicable_date or None,
        "subject": doc.subject or None,
        "topic": doc.subject or None,
        "taxonomy_keywords": doc.taxonomy_keywords,
        "modifies": doc.modifies,
        "supersedes": doc.supersedes,
        "amplifies": doc.amplifies,
        "clarifies": doc.clarifies,
        "obsoletes": doc.obsoletes,
        "superseded_by": [],
        "code_sections_referenced": doc.code_sections_referenced,
        "regs_referenced": doc.regs_referenced,
        "prior_rev_procs_cited": doc.prior_guidance_cited,
        # display helpers used by the frontend citation renderer
        "citation": citation,
        "citation_short": citation,
        "display_label": citation,
        "display_title": display_title,
        "display_path": (
            f"Internal Revenue Bulletin / IRB {doc.year} / {meta['long_label']} / {citation}"
        ),
        "breadcrumb": breadcrumb,
        # chunk shape
        "chunk_index": 0,
        "total_chunks": 1,
        "sort_key": act_id,
        "word_count": len(text.split()),
        "full_text_sha1": _sha1(text),
        # source
        "source_url": doc.source_url,
        "authority": "Internal Revenue Service, US Department of the Treasury",
        "parent_id": f"us/federal/irs/irb/{doc.year}/{meta['doc_type']}",
        "raw_node_id": (f"us/federal/irs/irb/{doc.year}/{meta['doc_type']}/{doc.doc_index}"),
    }
    return {
        "point_id": _point_id(act_id, 0, text),
        "text_for_embedding": text_for_embedding,
        "raw_text": text,
        "metadata": payload,
    }


# ---------------------------------------------------------------------------
# Fetch + parse orchestration
# ---------------------------------------------------------------------------


def process_one(
    prefix: str,
    year: int,
    idx: int,
    url: str,
    irb_lookup: dict[str, str] | None = None,
) -> IrbDoc | None:
    pdf_bytes = _fetch_bytes(url)
    if pdf_bytes is None:
        return None
    doc = parse_irb_pdf(prefix, year, idx, url, pdf_bytes)
    if doc is None:
        return None
    if irb_lookup:
        # Populate the doc's own weekly-IRB citation from the lookup. Try
        # the short form ("Rev. Proc. 2024-8") first, then the canonical
        # short label as computed from the file name in case the PDF's
        # doc-number regex missed a variant.
        keys = [doc.doc_number_label]
        alt = f"{DOC_TYPES[prefix]['label']} {year}-{idx}"
        if alt not in keys:
            keys.append(alt)
        for k in keys:
            if k in irb_lookup:
                doc.irb_reference = irb_lookup[k]
                break
    return doc


# ---------------------------------------------------------------------------
# Weekly-IRB HTML lookup: {doc_label -> "YYYY-WW I.R.B."}
# ---------------------------------------------------------------------------


def _weekly_url(year: int, week: int) -> str:
    return f"https://www.irs.gov/irb/{year}-{week:02d}_IRB"


def _fetch_html(url: str, retries: int = 3) -> str | None:
    proxies = _us_proxies()
    for attempt in range(retries):
        try:
            r = SESSION.get(url, timeout=45, proxies=proxies, allow_redirects=True)
            if r.status_code == 200 and "html" in r.headers.get("Content-Type", "").lower():
                return r.text
            if r.status_code == 404:
                return None
            if r.status_code == 429:
                time.sleep(2**attempt)
                continue
            if r.status_code in (502, 503, 504):
                time.sleep(1)
                continue
            return None
        except Exception:
            time.sleep(1)
    return None


def build_irb_lookup(
    years: list[int], workers: int = 4, cache_path: Path | None = None
) -> dict[str, str]:
    """Walk ``/irb/YYYY-WW_IRB`` for each year, extract doc labels, and build
    a ``{"Rev. Proc. YYYY-N": "YYYY-WW I.R.B."}`` lookup.

    IRB weekly pages exist for WW=1..52 (occasionally 53). We probe each
    week, stop after ``MAX_MISS`` consecutive misses beyond the last hit,
    and cache the result to ``cache_path`` if provided so re-runs are cheap.
    Failures are non-fatal: missing weeks just leave the corresponding docs
    without an ``irb_reference`` and downstream still works.
    """
    if cache_path and cache_path.exists():
        try:
            cached = json.loads(cache_path.read_text())
            print(f"[IRB] loaded weekly-IRB lookup from cache: {len(cached)} entries", flush=True)
            return cached
        except Exception:
            pass

    lookup: dict[str, str] = {}
    MAX_MISS = 4
    for year in years:
        last_hit = 0
        miss_streak = 0
        for wk in range(1, 55):
            url = _weekly_url(year, wk)
            html = _fetch_html(url)
            if html is None:
                miss_streak += 1
                if last_hit and miss_streak >= MAX_MISS:
                    break
                continue
            miss_streak = 0
            last_hit = wk
            week_cite = f"{year}-{wk:02d} I.R.B."
            for label in _WEEKLY_DOC_LABEL_RE.findall(html):
                key = _clean_ws(label)
                # canonicalize "Rev. Proc." spacing
                key = re.sub(r"Rev\.\s*Proc\.", "Rev. Proc.", key)
                key = re.sub(r"Rev\.\s*Rul\.", "Rev. Rul.", key)
                if key not in lookup:
                    lookup[key] = week_cite
        print(
            f"[IRB]   weekly lookup {year}: {last_hit} weeks scanned, cumulative entries={len(lookup)}",
            flush=True,
        )

    if cache_path:
        try:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_text(json.dumps(lookup, indent=2, sort_keys=True))
            print(f"[IRB] wrote lookup cache: {cache_path} ({len(lookup)} entries)", flush=True)
        except Exception as e:
            print(f"  ! failed to write lookup cache: {e}", flush=True)
    return lookup


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_years(spec: str) -> list[int]:
    """Accept ``2024`` or ``2015-2025`` or ``2015,2018,2020``."""
    if "-" in spec and "," not in spec:
        lo, hi = spec.split("-", 1)
        return list(range(int(lo), int(hi) + 1))
    if "," in spec:
        return [int(x.strip()) for x in spec.split(",") if x.strip()]
    return [int(spec)]


def _parse_doc_types(spec: str) -> list[str]:
    """Accept ``rev_proc,notice`` or short prefixes ``rp,n`` or ``all``."""
    if not spec or spec == "all":
        return list(DOC_TYPES.keys())
    prefixes: list[str] = []
    for t in spec.split(","):
        t = t.strip().lower()
        if not t:
            continue
        if t in DOC_TYPES:
            prefixes.append(t)
            continue
        # long form -> short prefix
        for pfx, meta in DOC_TYPES.items():
            if meta["doc_type"] == t or meta["category"] == t:
                prefixes.append(pfx)
                break
        else:
            raise SystemExit(f"unknown doc_type: {t}")
    return prefixes


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Ingest IRS IRB agency guidance from irs.gov/pub/irs-drop."
    )
    ap.add_argument(
        "--years", default="2015-2025", help="Year range: '2024' | '2015-2025' | '2015,2020,2024'."
    )
    ap.add_argument(
        "--doc-types",
        default="all",
        help="Which types to ingest: 'all' | 'rev_proc,notice' | 'rp,rr,n,a'.",
    )
    ap.add_argument(
        "--max-probe", type=int, default=200, help="Highest NN to probe per prefix per year."
    )
    ap.add_argument(
        "--stop-after-misses",
        type=int,
        default=25,
        help="Stop probing after N consecutive misses beyond the last hit.",
    )
    ap.add_argument("--workers", type=int, default=4, help="Concurrent HEAD/GET workers.")
    ap.add_argument("--delay", type=float, default=0.1, help="Sleep between GETs to be polite.")
    ap.add_argument(
        "--limit-per-year", type=int, default=0, help="Cap docs per (year, doc_type). 0 = no cap."
    )
    ap.add_argument(
        "--limit-per-type",
        type=int,
        default=0,
        help="Cap docs per doc_type across all years. 0 = no cap.",
    )
    ap.add_argument(
        "--dry-run", action="store_true", help="Do not write JSONL."
    )
    ap.add_argument(
        "--enumerate-only", action="store_true", help="Just print discovered URLs and exit."
    )
    ap.add_argument(
        "--skip-irb-lookup",
        action="store_true",
        help="Skip building the weekly-IRB HTML lookup (irb_reference will be null).",
    )
    ap.add_argument(
        "--irb-lookup-cache",
        default=str(DATA_DIR / "irb_weekly_lookup.json"),
        help="Path to cache the weekly-IRB lookup JSON (persistent across runs).",
    )
    args = ap.parse_args()

    years = _parse_years(args.years)
    prefixes = _parse_doc_types(args.doc_types)
    print(
        f"[IRB] years={years}, doc_types={prefixes}, "
        f"max_probe={args.max_probe}, stop_after_misses={args.stop_after_misses}",
        flush=True,
    )

    # 1) Discover URLs
    all_urls: list[tuple[str, int, int, str]] = []
    for pfx in prefixes:
        per_type_count = 0
        for year in years:
            hits = enumerate_docs(
                pfx,
                year,
                max_probe=args.max_probe,
                stop_after_misses=args.stop_after_misses,
                workers=args.workers,
            )
            if args.limit_per_year:
                hits = hits[: args.limit_per_year]
            print(f"[IRB]   {pfx} {year}: {len(hits)} docs", flush=True)
            for idx, url in hits:
                if args.limit_per_type and per_type_count >= args.limit_per_type:
                    break
                all_urls.append((pfx, year, idx, url))
                per_type_count += 1

    print(f"[IRB] total to fetch: {len(all_urls)}", flush=True)

    if args.enumerate_only:
        for pfx, year, idx, url in all_urls:
            print(url)
        return 0

    if not all_urls:
        return 0

    # 1b) Weekly-IRB HTML lookup (populates irb_reference). Cheap enough to
    # always run; the cache makes re-runs free.
    irb_lookup: dict[str, str] = {}
    if not args.skip_irb_lookup:
        cache_path = Path(args.irb_lookup_cache)
        irb_lookup = build_irb_lookup(years, workers=args.workers, cache_path=cache_path)

    # 2) Fetch + parse
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    chunks: list[dict] = []
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {
            ex.submit(process_one, pfx, year, idx, url, irb_lookup): (pfx, year, idx, url)
            for (pfx, year, idx, url) in all_urls
        }
        done = 0
        for fut in as_completed(futures):
            done += 1
            pfx, year, idx, url = futures[fut]
            try:
                doc = fut.result()
            except Exception as e:
                print(f"  ! failed {url}: {e}", flush=True)
                doc = None
            if doc is None:
                continue
            record = to_chunk_record(doc)
            chunks.append(record)
            if args.dry_run and done <= 8:
                # print a compact preview of the first few parsed records
                md = record["metadata"]
                print(
                    f"\n---- SAMPLE {done}: {md['citation']} ({md['category']}) ----",
                    flush=True,
                )
                print(f"  subject        : {md['subject']}", flush=True)
                print(f"  irb_reference  : {md['irb_reference']}", flush=True)
                print(f"  effective_date : {md['effective_date']}", flush=True)
                print(f"  supersedes     : {md['supersedes'][:3]}", flush=True)
                print(f"  modifies       : {md['modifies'][:3]}", flush=True)
                print(f"  code_sections  : {md['code_sections_referenced'][:5]}", flush=True)
                print(f"  regs_referenced: {md['regs_referenced'][:5]}", flush=True)
                print(f"  prior cites    : {md['prior_rev_procs_cited'][:5]}", flush=True)
                print(f"  word_count     : {md['word_count']}", flush=True)
                print(f"  source_url     : {md['source_url']}", flush=True)
            if done % 25 == 0 or done == len(all_urls):
                rate = done / max(time.time() - t0, 0.1)
                print(
                    f"  ... {done:>4}/{len(all_urls)}  parsed={len(chunks)}  {rate:.1f}/s",
                    flush=True,
                )
            time.sleep(args.delay / max(args.workers, 1))

    if args.dry_run:
        print(
            f"\n=== DRY RUN: parsed={len(chunks):,}, elapsed={time.time() - t0:.1f}s ===",
            flush=True,
        )
        return 0

    # 3) Write JSONL (dedupe on point_id + tolerate re-runs)
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
        for rec in chunks:
            if rec["point_id"] in seen:
                continue
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
            seen.add(rec["point_id"])
            written += 1

    print(
        f"\n=== Done: parsed={len(chunks):,}, new={written:,}, elapsed={time.time() - t0:.1f}s ===",
        flush=True,
    )
    print(f"JSONL: {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
