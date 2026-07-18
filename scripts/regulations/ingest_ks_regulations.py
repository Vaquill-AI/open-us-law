#!/usr/bin/env python3
"""Ingest the Kansas Administrative Regulations (KAR) — Kansas's state regs.

OFFICIAL SOURCE ONLY: the Kansas Secretary of State publishes the canonical
bound edition of K.A.R. as a five-volume + index PDF set at
``www.sos.ks.gov/publications/agency-regulation-resources.html``. The newer
``rules.ks.gov`` SPA (Esper platform) is the official online viewer but its
API is auth-gated and not scrapable; the SOS PDF set is the same content
"Approved for Printing by the State Rules and Regulations Board" under
K.S.A. 77-415 et seq. (the K.A.R. enabling statute). No aggregators.

Source layout:
    https://www.sos.ks.gov/publications/KAR/2022/2022_KAR_Volumes_Book_1.pdf
        Agencies 1 through 27  (~1054 pages, ~3500+ rules)
    https://www.sos.ks.gov/publications/KAR/2022/2022_KAR_Volumes_Book_2.pdf
        Agency 28
    https://www.sos.ks.gov/publications/KAR/2022/2022_KAR_Volumes_Book_3.pdf
        Agencies 30 through 70
    https://www.sos.ks.gov/publications/KAR/2022/2022_KAR_Volumes_Book_4.pdf
        Agencies 71 through 100
    https://www.sos.ks.gov/publications/KAR/2022/2022_KAR_Volumes_Book_5.pdf
        Agencies 101 through 133
    https://www.sos.ks.gov/publications/KAR/2022/2022_KAR_Volumes_Index.pdf

KAR hierarchy: Agency -> Article -> Regulation (citation form
``K.A.R. <agency>-<article>-<reg>`` e.g. ``K.A.R. 1-1-1``). Pages are typeset
in two columns with running heads; each rule looks like::

    1-5-26. Stand-by compensation. (a) Any ap-
    pointing authority may require ... <body> ...
    (Authorized by K.S.A. 1995 Supp. 75-3747;
    implementing K.S.A. 75-3746; effective May 1,
    1979; amended May 1, 1985; amended, T-86-17,
    June 17, 1985; amended May 1, 1986; amended
    May 31, 1996.)

The trailing parenthetical is the *history block* and carries the rich
metadata we never strip: ``Authorized by`` -> statutory_authority,
``implementing`` -> rule_amplifies, ``effective <date>`` + ``amended <date>``
chain -> effective_date / prior_effective_dates / amendment_years.

Agency boundaries are detected by an ``Agency N\\n<Agency Name>`` block on a
fresh page; article boundaries by ``Article N.--<ARTICLE NAME>`` (em-dash
variants). Reserved/revoked rules (only a history block, no body) are kept
flagged ``act_status='revoked'`` but skipped from the in-force ingest unless
``--all-statuses`` is set.

corpus_type='state_regulation', state='ks'. Geo-restricted (SOS); Webshare
US proxy + Chrome UA + polite pacing.
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
OUT = DATA_DIR / "state_ks_regulations.jsonl"

KS_BASE = "https://www.sos.ks.gov"
KS_KAR_YEAR = "2022"
KS_KAR_DIR = f"{KS_BASE}/publications/KAR/{KS_KAR_YEAR}"
KS_BOOKS: dict[str, tuple[str, tuple[int, int]]] = {
    # book label -> (PDF filename, (agency_min, agency_max))  inclusive
    "book1": (f"{KS_KAR_YEAR}_KAR_Volumes_Book_1.pdf", (1, 27)),
    "book2": (f"{KS_KAR_YEAR}_KAR_Volumes_Book_2.pdf", (28, 28)),
    "book3": (f"{KS_KAR_YEAR}_KAR_Volumes_Book_3.pdf", (30, 70)),
    "book4": (f"{KS_KAR_YEAR}_KAR_Volumes_Book_4.pdf", (71, 100)),
    "book5": (f"{KS_KAR_YEAR}_KAR_Volumes_Book_5.pdf", (101, 133)),
}

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


def fetch_bytes(url: str, retries: int = 5) -> Optional[bytes]:
    proxies = _us_proxies()
    for attempt in range(retries):
        try:
            r = SESSION.get(url, timeout=300, proxies=proxies, allow_redirects=True)
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
# PDF parsing: column-aware text extraction and rule splitting
# ---------------------------------------------------------------------------

# Rule header at start-of-line: "<agency>-<article>-<reg>." optionally with a
# trailing letter ("1-5-19a"). The article and reg parts can themselves carry a
# trailing lowercase letter for sub-numbering.
_RULE_HEADER_RE = re.compile(
    r"(?m)^(\d{1,3}-\d{1,3}[a-z]?-\d{1,3}[a-zA-Z]?)\.\s+"
)

# Agency boundary: "Agency N\n<Name>" near top of a page (the volumes use one
# fresh page per new agency, with the name as the first non-trivial line).
_AGENCY_HEADER_RE = re.compile(r"^Agency\s+(\d+)\s*$", re.MULTILINE)

# Article boundary: "Article N.--<NAME>" with various dash variants. The PDF
# sometimes wraps the article header across columns; we tolerate that by
# matching either em-dash, en-dash, or double-hyphen.
_ARTICLE_HEADER_RE = re.compile(
    r"Article\s+(\d+[A-Za-z]?)\s*[.–—\-]+\s*([A-Z][A-Z0-9 ,'/\-&]+?)(?=\n|$)"
)

# Trailing history block: "(Authorized by ... effective <date>; amended <date>; ...)"
# The block may span line breaks; we capture greedily up to the closing paren.
_HISTORY_BLOCK_RE = re.compile(
    r"\((Authorized by[^)]*?(?:effective|revoked|amended|implementing)[^)]*?)\)",
    re.IGNORECASE | re.DOTALL,
)

# Individual date in a history block: "May 1, 1979", "Jan. 6, 1992", "Dec. 20, 2002"
_MONTHS = (
    "January|February|March|April|May|June|July|August|September|"
    "October|November|December|Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sept|Sep|Oct|Nov|Dec"
)
_DATE_IN_HIST_RE = re.compile(
    rf"({_MONTHS})\.?\s+(\d{{1,2}}),\s+(\d{{4}})",
    re.IGNORECASE,
)

# Statutory authority + amplifies extraction
_AUTH_RE = re.compile(
    r"Authorized by\s+(.+?)(?=;\s*implementing|;\s*effective|;\s*revoked|;\s*amended|\)|$)",
    re.IGNORECASE | re.DOTALL,
)
_IMPL_RE = re.compile(
    r"implementing\s+(.+?)(?=;\s*effective|;\s*revoked|;\s*amended|\)|$)",
    re.IGNORECASE | re.DOTALL,
)


def _extract_columns_text(page) -> str:
    """Extract page text respecting the two-column layout used in KAR volumes.

    KAR is typeset roughly 50/50 in two columns. Splitting on width/2 yields
    correctly ordered reading flow (left top-to-bottom, then right
    top-to-bottom) instead of pdfplumber's default left-to-right-row-wise
    extraction that interleaves columns.
    """
    width = page.width
    height = page.height
    mid = width / 2.0
    # Tiny gutter buffer to avoid clipping characters that straddle the midpoint
    left = page.crop((0, 0, mid + 4, height)).extract_text() or ""
    right = page.crop((mid - 4, 0, width, height)).extract_text() or ""
    return left + "\n" + right


def _pdf_pages(pdf_bytes: bytes):
    """Yield (page_index, page_object) for a PDF, lazily."""
    import pdfplumber

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for i, page in enumerate(pdf.pages):
            yield i, page


def _book_text_per_page(pdf_bytes: bytes) -> list[str]:
    """Return one combined-column text string per page of the book PDF."""
    out: list[str] = []
    for _i, page in _pdf_pages(pdf_bytes):
        out.append(_extract_columns_text(page))
    return out


# ---------------------------------------------------------------------------
# Agency / Article state-machine over the per-page extracts
# ---------------------------------------------------------------------------


@dataclass
class _AgencyCtx:
    number: str = ""
    name: str = ""


@dataclass
class _ArticleCtx:
    number: str = ""
    name: str = ""


def _detect_agency(page_text: str) -> Optional[tuple[str, str]]:
    """If this page begins a new Agency block, return (agency_number, name).

    Agency pages in the KAR set look like::

        Agency 1
        Department of Administration
        Articles
        1-1. Purpose, ...

    We detect by an isolated ``Agency N`` line followed by a name line that
    contains letters and no digit prefix that looks like a rule number.
    """
    lines = [ln.strip() for ln in page_text.splitlines() if ln.strip()]
    for idx, ln in enumerate(lines[:6]):
        m = re.match(r"^Agency\s+(\d+)$", ln)
        if not m:
            continue
        num = m.group(1)
        # next non-trivial line is the agency name
        for nxt in lines[idx + 1 : idx + 5]:
            if not nxt:
                continue
            if re.match(r"^Articles?$", nxt, re.IGNORECASE):
                continue
            if re.match(r"^\d", nxt):  # looks like rule number
                continue
            return num, nxt
        return num, ""
    return None


def _detect_articles(page_text: str) -> list[tuple[str, str]]:
    """Return ALL "Article N.--<NAME>" headers on the page in order."""
    found: list[tuple[str, str]] = []
    for m in _ARTICLE_HEADER_RE.finditer(page_text):
        num = m.group(1)
        name = re.sub(r"\s+", " ", m.group(2)).strip().rstrip(".")
        # Filter out the TOC entries that look like "Article 5. KANSAS\n5-1. ..."
        if name and not re.match(r"^[a-z]", name):
            found.append((num, name))
    return found


# ---------------------------------------------------------------------------
# Rule extraction
# ---------------------------------------------------------------------------


@dataclass
class _RuleSpan:
    rule_num: str  # e.g. "1-5-26"
    text: str  # raw concatenated text including header + body + history


def _split_rules(joined_text: str) -> list[_RuleSpan]:
    """Split a contiguous text region (multiple pages of one agency) into
    rules, keyed on the start-of-line ``N-N-N.`` header pattern.
    """
    matches = list(_RULE_HEADER_RE.finditer(joined_text))
    out: list[_RuleSpan] = []
    for i, m in enumerate(matches):
        rule_num = m.group(1)
        start = m.start()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(joined_text)
        out.append(_RuleSpan(rule_num=rule_num, text=joined_text[start:end].strip()))
    return out


@dataclass
class Rule:
    agency: str
    agency_name: str
    article: str
    article_name: str
    rule_num: str  # e.g. "1-5-26"
    rule_title: str  # e.g. "Stand-by compensation"
    raw_text: str  # body text, history block stripped
    history_raw: str  # the (Authorized by ...) block, intact
    status: str = "in_force"  # "in_force" | "revoked"
    effective_date: str = ""  # first effective date (initial promulgation)
    last_amended_date: str = ""  # most recent amended date
    statutory_authority: str = ""  # K.S.A. citation(s) authorizing the rule
    rule_amplifies: str = ""  # implementing K.S.A. citation(s)
    prior_effective_dates: list[str] = field(default_factory=list)
    amendment_years: list[int] = field(default_factory=list)
    source_url: str = ""  # book PDF URL
    source_book: str = ""  # "book1" etc.


def _parse_title_and_body(rule_text: str) -> tuple[str, str, str]:
    """From the raw rule span (header included), return (title, body, history).

    Title is whatever follows ``N-N-N.`` up to the first period that ends a
    sentence-like fragment; body is the substantive text; history is the
    trailing ``(...)`` block. Reserved/revoked rules consist of *only* a
    history block immediately after the header.
    """
    # Strip the "N-N-N." prefix
    m = _RULE_HEADER_RE.match(rule_text)
    after_header = rule_text[m.end() :] if m else rule_text

    # History block at the end
    history = ""
    hist_match = None
    for hm in _HISTORY_BLOCK_RE.finditer(after_header):
        hist_match = hm  # take the LAST one (in case body quotes a paren)
    if hist_match:
        history = hist_match.group(1).strip()
        body_region = after_header[: hist_match.start()].rstrip()
    else:
        body_region = after_header

    # Title = first sentence-ending fragment, then body is the rest.
    # KAR title typically ends with ". " followed by "(a)" or a capital letter.
    title = ""
    body = body_region
    m_title = re.match(r"\s*([^.]{1,200}?)\.\s+(.*)", body_region, re.DOTALL)
    if m_title:
        title = m_title.group(1).strip()
        body = m_title.group(2).strip()
    else:
        # Pure-revoked rule: no title, only history
        title = ""
        body = body_region.strip()

    # Collapse whitespace in body
    body = re.sub(r"\s+", " ", body).strip()
    # Drop column running-header noise that bled in: e.g. "Stand-by 1-5-28"
    body = re.sub(r"\s+\d+-\d+[a-z]?-\d+[a-zA-Z]?\b", "", body)
    return title, body, history


def _parse_history(history: str) -> dict:
    """Decompose the (Authorized by ...; implementing ...; effective ...; amended ...)
    block into the structured fields we keep as rich metadata."""
    out: dict = {
        "statutory_authority": "",
        "rule_amplifies": "",
        "effective_date": "",
        "last_amended_date": "",
        "prior_effective_dates": [],
        "amendment_years": [],
        "status": "in_force",
    }
    if not history:
        return out
    flat = re.sub(r"\s+", " ", history).strip()

    m_auth = _AUTH_RE.search(flat)
    if m_auth:
        out["statutory_authority"] = m_auth.group(1).strip().rstrip(";,. ")

    m_impl = _IMPL_RE.search(flat)
    if m_impl:
        out["rule_amplifies"] = m_impl.group(1).strip().rstrip(";,. ")

    # All dates in order
    dates: list[str] = []
    for dm in _DATE_IN_HIST_RE.finditer(flat):
        mon = dm.group(1)
        day = dm.group(2)
        yr = dm.group(3)
        dates.append(f"{mon} {day}, {yr}")

    # Walk the history sentence to label each date by its verb (effective /
    # amended / revoked). Each date is preceded by exactly one of those verbs
    # somewhere in the surrounding clause.
    effective_dates: list[str] = []
    amended_dates: list[str] = []
    revoked = False
    # Split on semicolons to get clauses
    for clause in re.split(r";", flat):
        cl = clause.strip()
        for dm in _DATE_IN_HIST_RE.finditer(cl):
            ds = f"{dm.group(1)} {dm.group(2)}, {dm.group(3)}"
            cl_low = cl.lower()
            if "revoked" in cl_low:
                revoked = True
            if "amended" in cl_low:
                amended_dates.append(ds)
            elif "effective" in cl_low:
                effective_dates.append(ds)

    if effective_dates:
        out["effective_date"] = effective_dates[0]
        out["prior_effective_dates"] = effective_dates[1:] + amended_dates
    if amended_dates:
        out["last_amended_date"] = amended_dates[-1]
    if revoked and not effective_dates and not amended_dates:
        out["status"] = "revoked"
    elif revoked:
        out["status"] = "revoked"

    # Amendment years (unique, sorted)
    years: set[int] = set()
    for d in effective_dates + amended_dates:
        ym = re.search(r"(\d{4})$", d)
        if ym:
            years.add(int(ym.group(1)))
    out["amendment_years"] = sorted(years)

    return out


def parse_book(
    pdf_bytes: bytes,
    book_label: str,
    source_url: str,
    agency_range: tuple[int, int],
) -> list[Rule]:
    """Parse one of the five KAR volume PDFs into a flat list of Rule objects."""
    pages = _book_text_per_page(pdf_bytes)

    # State that walks across pages
    cur_agency = _AgencyCtx()
    cur_article = _ArticleCtx()
    # Accumulate text per (agency, article) so a rule that wraps page boundaries
    # is still parsed as one span. Resetting on article-change avoids
    # cross-article rule-number collisions.
    bucket_key: tuple[str, str] = ("", "")
    bucket_text: list[str] = []

    rules: list[Rule] = []

    def _flush_bucket(
        agency: _AgencyCtx,
        article: _ArticleCtx,
    ) -> None:
        if not bucket_text:
            return
        joined = "\n".join(bucket_text)
        for span in _split_rules(joined):
            title, body, history = _parse_title_and_body(span.text)
            if not title and not body:
                continue  # noise
            hist_meta = _parse_history(history)
            # Skip rules with no substantive body (pure revoked/reserved)
            # unless --all-statuses is later requested by caller.
            r = Rule(
                agency=agency.number,
                agency_name=agency.name,
                article=article.number,
                article_name=article.name,
                rule_num=span.rule_num,
                rule_title=title,
                raw_text=body,
                history_raw=history,
                status=hist_meta["status"],
                effective_date=hist_meta["effective_date"],
                last_amended_date=hist_meta["last_amended_date"],
                statutory_authority=hist_meta["statutory_authority"],
                rule_amplifies=hist_meta["rule_amplifies"],
                prior_effective_dates=hist_meta["prior_effective_dates"],
                amendment_years=hist_meta["amendment_years"],
                source_url=source_url,
                source_book=book_label,
            )
            rules.append(r)

    for _i, page_text in enumerate(pages):
        # 1. Agency boundary?
        agency_hit = _detect_agency(page_text)
        if agency_hit is not None:
            num, name = agency_hit
            agency_min, agency_max = agency_range
            try:
                num_int = int(num)
            except ValueError:
                num_int = -1
            if agency_min <= num_int <= agency_max:
                _flush_bucket(cur_agency, cur_article)
                bucket_text = []
                cur_agency = _AgencyCtx(number=num, name=name)
                cur_article = _ArticleCtx()
                bucket_key = (cur_agency.number, "")

        # 2. Article boundary(s) on this page - there may be more than one.
        article_hits = _detect_articles(page_text)
        if article_hits:
            # If a new article header appears, flush prior bucket then continue.
            # All article headers on this page are after the article boundary,
            # so we treat the page text as belonging to the LAST article on it.
            # First flush current.
            _flush_bucket(cur_agency, cur_article)
            bucket_text = []
            last_num, last_name = article_hits[-1]
            cur_article = _ArticleCtx(number=last_num, name=last_name)
            bucket_key = (cur_agency.number, cur_article.number)

        bucket_text.append(page_text)

    # Flush the tail
    _flush_bucket(cur_agency, cur_article)

    # Drop rules whose agency wasn't established (preface pages / TOC)
    rules = [r for r in rules if r.agency]
    return rules


# ---------------------------------------------------------------------------
# Chunk emission
# ---------------------------------------------------------------------------


def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def _point_id(act_id: str, idx: int, text: str) -> str:
    seed = f"{act_id}::{idx}::{_sha1(text)[:12]}"
    return str(uuid.UUID(hashlib.md5(seed.encode()).hexdigest()))


def _safe(s: str) -> str:
    return re.sub(r"[^0-9A-Za-z]", "_", s)


def _act_id(r: Rule) -> str:
    parts = r.rule_num.split("-")
    if len(parts) == 3:
        ag, art, rg = parts
    else:
        ag, art, rg = r.agency, r.article or "0", "_".join(parts)
    return f"STATE_KS_KAR_{_safe(ag)}_{_safe(art)}_{_safe(rg)}"


def to_chunk_record(r: Rule) -> dict:
    act_id = _act_id(r)
    citation = f"K.A.R. {r.rule_num}"
    section_title = (f"{citation}. {r.rule_title}".rstrip(".") if r.rule_title else citation)
    text = r.raw_text

    # Rich, searchable embed header - surfaces the rich metadata so it is
    # retrievable, not just stored (per ingestion spec).
    meta_lines: list[str] = []
    if r.effective_date:
        meta_lines.append(f"Effective: {r.effective_date}")
    if r.last_amended_date and r.last_amended_date != r.effective_date:
        meta_lines.append(f"Last Amended: {r.last_amended_date}")
    if r.statutory_authority:
        meta_lines.append(f"Statutory Authority: {r.statutory_authority}")
    if r.rule_amplifies:
        meta_lines.append(f"Implementing: {r.rule_amplifies}")
    meta_header = ("\n" + "\n".join(meta_lines)) if meta_lines else ""

    chapter_label = f"Agency {r.agency} / Article {r.article}" if r.article else f"Agency {r.agency}"
    chapter_name = " / ".join(
        x for x in (r.agency_name, r.article_name) if x
    )

    text_for_embedding = (
        f"Regulation: Kansas Administrative Regulations | US | Kansas | "
        f"{'In Force' if r.status == 'in_force' else r.status.title()}\n"
        f"{chapter_label}: {chapter_name}\n"
        f"{citation}. {r.rule_title}{meta_header}\n\n{text}"
    )

    md = {
        "act_id": act_id,
        "corpus_type": "state_regulation",
        "category": "state_regulation",
        "document_type": "regulation",
        "jurisdiction": "US",
        "country_code": "US",
        "state": "ks",
        "title_number": None,
        "title_name": (
            f"Kansas Administrative Regulations — Agency {r.agency} "
            f"({r.agency_name})" if r.agency_name else
            f"Kansas Administrative Regulations — Agency {r.agency}"
        ),
        "title": "Kansas Administrative Regulations",
        "title_code": "regs_ks",
        "top_level_title": "regs-ks",
        "chapter": f"{r.agency}-{r.article}" if r.article else r.agency,
        "chapter_name": chapter_name,
        "section_number": r.rule_num,
        "section_title": section_title,
        "year": int(KS_KAR_YEAR),
        "act_status": "in_force" if r.status == "in_force" else r.status,
        "renumbered_to": "",
        "transferred_to": "",
        "level_classifier": "section",
        # --- regulation-specific rich metadata (captured, never stripped) ---
        "effective_date": r.effective_date or None,
        "last_amended_date": r.last_amended_date or None,
        "statutory_authority": r.statutory_authority or None,
        "rule_amplifies": r.rule_amplifies or None,
        "promulgated_under": "K.S.A. 77-415 et seq.",
        "prior_effective_dates": r.prior_effective_dates,
        "review_date": None,
        "history": r.history_raw or None,
        "issuing_agency": r.agency_name or None,
        "issuing_agency_code": r.agency,
        "article_number": r.article or None,
        "article_name": r.article_name or None,
        "citation": citation,
        "citation_short": citation,
        "display_label": citation,
        "display_title": section_title,
        "display_path": (
            f"Kansas Administrative Regulations / Agency {r.agency} "
            f"({r.agency_name}) / Article {r.article} ({r.article_name}) / "
            f"{citation}"
        ),
        "breadcrumb": [
            {
                "type": "agency",
                "num": r.agency,
                "label": f"Agency {r.agency}",
                "name": r.agency_name,
            },
            {
                "type": "article",
                "num": r.article,
                "label": f"Article {r.article}",
                "name": r.article_name,
            },
            {
                "type": "regulation",
                "num": r.rule_num,
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
        "amendment_years": r.amendment_years,
        "amendments_count": len(r.amendment_years),
        "last_amended_year": (r.amendment_years[-1] if r.amendment_years else None),
        "public_laws_referenced": [],
        "public_laws_count": 0,
        "source_url": r.source_url,
        "source_book": r.source_book,
        "parent_id": f"us/ks/regulations/agency={r.agency}/article={r.article}",
        "raw_node_id": (
            f"us/ks/regulations/agency={r.agency}/article={r.article}/"
            f"reg={r.rule_num}"
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
# Crawl driver
# ---------------------------------------------------------------------------


def process_book(book_label: str, all_statuses: bool) -> list[Rule]:
    """Download one KAR volume PDF, parse it, then return the Rule objects
    ready for chunking."""
    filename, agency_range = KS_BOOKS[book_label]
    url = f"{KS_KAR_DIR}/{filename}"
    print(f"[KAR] fetching {url} ...", flush=True)
    pdf_bytes = fetch_bytes(url)
    if not pdf_bytes:
        print(f"  ! could not fetch {url}", flush=True)
        return []

    rules = parse_book(pdf_bytes, book_label, url, agency_range)
    kept: list[Rule] = []
    for rule in rules:
        if rule.status != "in_force" and not all_statuses:
            continue
        kept.append(rule)
    print(
        f"  [{book_label}] parsed={len(rules):,} kept={len(kept):,} "
        f"(agencies {agency_range[0]}-{agency_range[1]})",
        flush=True,
    )
    return kept


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--books",
        default="",
        help="Comma-separated books to ingest (e.g. 'book1,book3'). Default: all five.",
    )
    ap.add_argument("--workers", type=int, default=2,
                    help="Parallel book downloads. Each book is large; 2 is plenty.")
    ap.add_argument(
        "--all-statuses",
        action="store_true",
        help="Include revoked/reserved rules (default: in-force only).",
    )
    ap.add_argument("--dry-run", action="store_true",
                    help="Discover only; do not write JSONL.")
    args = ap.parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    books = list(KS_BOOKS.keys())
    if args.books:
        wanted = {b.strip() for b in args.books.split(",") if b.strip()}
        books = [b for b in books if b in wanted]
    print(f"[KAR] processing {len(books)} books: {books}", flush=True)

    if args.dry_run:
        return 0

    t0 = time.time()
    all_rules: list[Rule] = []
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
        futures = {
            ex.submit(process_book, b, args.all_statuses): b for b in books
        }
        for fut in as_completed(futures):
            book = futures[fut]
            try:
                all_rules.extend(fut.result())
            except Exception as e:
                print(f"  ! book {book} failed: {e}", flush=True)

    chunks = [to_chunk_record(r) for r in all_rules]

    # Dedup against existing JSONL by point_id (idempotent re-runs)
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
        f"\n=== Done: rules={len(all_rules):,}, chunks={len(chunks):,}, "
        f"new={written:,}, elapsed={time.time() - t0:.1f}s ===",
        flush=True,
    )
    print(f"JSONL: {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
