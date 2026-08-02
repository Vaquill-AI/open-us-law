#!/usr/bin/env python3
"""Federal Register RULE + PRORULE ingest via GovInfo bulk XML.

Design (why bulk XML and not per-doc API calls):

  The FR API's per-document detail endpoint requires 1 HTTP call per doc.
  With ~3,200 rules per year and IP-based rate limiting, a full-year run
  takes ~2.5 hours; a 32-year backfill would take ~80 hours. GovInfo
  publishes the same content as a single XML per publication day, served
  via CloudFlare-cached static files at
      https://www.govinfo.gov/bulkdata/FR/{YYYY}/{MM}/FR-{YYYY}-{MM}-{DD}.xml

  Each daily XML is ~5-10 MB and inlines every RULE / PRORULE / NOTICE /
  PRESDOCU published that day, with full body text. ~250 business days per
  year -> ~250 CloudFlare requests per year, no rate-limit risk.

  However, the FR API's structured metadata (agency slugs, RIN arrays,
  CFR references as {title, part} objects, OIRA `significant` flag,
  topics, docket_ids) is RICHER than what we can regex out of the XML.
  So this ingest uses BOTH sources:

    Phase 1: FR API `/documents.json` list endpoint (paginated per month
             to stay under the 2000-record pagination cap). We ask for
             all metadata fields we need via `fields[]`. ~12 calls/year.
    Phase 2: GovInfo bulk XML per unique publication date -> full body
             text keyed by <FRDOC> number. ~250 calls/year, all cached.
    Phase 3: Join by document_number and emit a chunk record in the
             standard schema.

Approx throughput on ordinary broadband:
    ~4-6 concurrent async workers per source. ~5-10 min per year.
    Multi-year parallelisation (4 years concurrent) -> full 1994-2025
    backfill in ~30-45 min.

XML parsing notes (from GPO's FR-XML_User-Guide):
  - <FEDREG> root wraps all daily content.
  - Doc containers: <RULE>, <PRORULE>, <NOTICE>, <PRESDOCU>.
  - <PREAMB> has metadata (<FRDOC>, <SUBJECT>, <AGENCY>, <CFR>, <RIN>,
    <EFFDATE>, <ACT>, <SUM>).
  - <SUPLINF> holds the body.
  - <PRTPAGE P=".."/> markers are page-boundary artefacts and must be
    stripped from extracted text.
  - <E T=".."> emphasis nodes wrap inline styled text; use itertext()
    to include their content, not `.text` alone.

Usage:

    # Single-year, single-type
    PYTHONPATH=. python -m scripts.federal.ingest_federal_register_bulk \\
        --year 2024 --doc-type RULE

    # Full backfill with year parallelism
    PYTHONPATH=. python -m scripts.federal.ingest_federal_register_bulk \\
        --year-range 1994-2025 --doc-type RULE --year-workers 4
"""
from __future__ import annotations

import argparse
import asyncio
import gzip
import hashlib
import json
import os
import re
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import httpx
from lxml import etree

sys.stdout.reconfigure(line_buffering=True)  # type: ignore[attr-defined]
sys.stderr.reconfigure(line_buffering=True)  # type: ignore[attr-defined]

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

# ---------------------------------------------------------------------------
# Env
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

# ---------------------------------------------------------------------------
# Constants + config
# ---------------------------------------------------------------------------

FR_API = "https://www.federalregister.gov/api/v1"
GOVINFO_BULK = "https://www.govinfo.gov/bulkdata/FR"
UA = "Vaquill-Legal-Ingest/2.0 (contact: priyansh@vaquill.ai)"

DATA_DIR = Path(os.environ.get("OUT_DIR", "./data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Fields we ask the FR API to inline on every list result - matches every
# key our old per-doc script populated, so downstream sync / embed is
# unchanged.
FR_LIST_FIELDS = [
    "document_number", "title", "abstract", "type", "subtype",
    "publication_date", "volume", "start_page", "end_page", "page_length",
    "citation", "action", "dates", "effective_on", "comments_close_on",
    "significant", "agencies", "topics", "cfr_references", "docket_ids",
    "regulation_id_numbers", "correction_of", "corrections",
    "regulations_dot_gov_url", "html_url", "pdf_url", "raw_text_url",
    "body_html_url", "full_text_xml_url", "president",
]

# Doc-type <-> (API type-token, XML container tag, act_id prefix, category)
DOC_TYPE_MAP: dict[str, dict] = {
    "RULE": {
        "api_type": "RULE",
        "xml_tag": "RULE",
        "act_prefix": "FR_RULE_",
        "category": "fr_rule",
        "jsonl": "state_fr_rules.jsonl",
    },
    "PRORULE": {
        "api_type": "PRORULE",
        "xml_tag": "PRORULE",
        "act_prefix": "FR_PRORULE_",
        "category": "fr_prorule",
        "jsonl": "state_fr_prorules.jsonl",
    },
}

# ---------------------------------------------------------------------------
# XML parsing
# ---------------------------------------------------------------------------

# "[FR Doc. 2024-01234 Filed 1-25-24; 8:45 am]"  ->  "2024-01234"
_FRDOC_RE = re.compile(r"FR Doc\.\s+([\w\-]+)")


def _strip_prtpage(root: etree._Element) -> None:
    """Remove <PRTPAGE> markers in-place so itertext() doesn't yield page nums."""
    for prt in root.iter("PRTPAGE"):
        parent = prt.getparent()
        if parent is not None:
            parent.remove(prt)


def _clean_text_from(elem: etree._Element) -> str:
    """Extract clean joined text from an XML element (after stripping PRTPAGE).

    Uses itertext() so nested emphasis (<E T="03">...</E>) is preserved.
    Collapses whitespace but keeps paragraph boundaries by joining runs with
    a single space and inserting a newline at block-level element boundaries.
    """
    parts: list[str] = []
    _BLOCK = {"P", "HD", "SECTION", "SUBSECT", "TITLE", "PART", "AGY", "ACT",
              "SUM", "EFFDATE", "FURINF", "DATES", "SUPLINF", "LSTSUB",
              "PREAMB", "AMDPAR", "STARS", "GID"}
    for evt, node in etree.iterwalk(elem, events=("start", "end")):
        if evt == "start":
            if node.text and node.text.strip():
                parts.append(node.text.strip())
        else:  # end
            if node.tag in _BLOCK:
                parts.append("\n")
            if node.tail and node.tail.strip():
                parts.append(node.tail.strip())
    out = " ".join(p for p in parts if p)
    out = re.sub(r"[ \t]+", " ", out)
    out = re.sub(r"\n{3,}", "\n\n", out)
    return out.strip()


def parse_fr_xml(xml_bytes: bytes, target_tag: str) -> dict[str, str]:
    """Return {frdoc_number: cleaned_body_text} for every <target_tag> in the file.

    target_tag is 'RULE' or 'PRORULE' (or 'NOTICE' / 'PRESDOCU' later).
    """
    try:
        root = etree.fromstring(xml_bytes)
    except etree.XMLSyntaxError as exc:
        print(f"    [xml] parse error: {exc}", flush=True)
        return {}

    _strip_prtpage(root)

    result: dict[str, str] = {}
    for doc in root.iter(target_tag):
        # Find FRDOC element (search inside PREAMB or LSTSUB)
        frdoc_text = None
        for frdoc_el in doc.iter("FRDOC"):
            if frdoc_el.text:
                frdoc_text = frdoc_el.text
                break
        if not frdoc_text:
            continue
        m = _FRDOC_RE.search(frdoc_text)
        if not m:
            continue
        docnum = m.group(1)

        body_text = _clean_text_from(doc)
        if body_text and len(body_text) >= 100:
            result[docnum] = body_text
    return result


# ---------------------------------------------------------------------------
# FR API list enumeration (paginated by month to stay under 2000-cap)
# ---------------------------------------------------------------------------


async def _fetch_list_page(
    client: httpx.AsyncClient, year: int, month: int, api_type: str, page: int
) -> dict:
    # Assemble query params (FR uses [] repeat notation)
    params = [
        ("per_page", "1000"),
        ("conditions[type][]", api_type),
        ("conditions[publication_date][gte]", f"{year}-{month:02d}-01"),
        ("conditions[publication_date][lte]",
         f"{year}-{month:02d}-{_last_day_of_month(year, month):02d}"),
        ("page", str(page)),
    ]
    for f in FR_LIST_FIELDS:
        params.append(("fields[]", f))
    for attempt in range(5):
        try:
            r = await client.get(f"{FR_API}/documents.json", params=params, timeout=60)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429, 500, 502, 503, 504):
                await asyncio.sleep(2 * (2 ** attempt))
                continue
            print(f"    [api] {year}-{month:02d} p{page} HTTP {r.status_code}", flush=True)
            return {}
        except Exception as exc:
            print(f"    [api] {year}-{month:02d} p{page} exc {type(exc).__name__}", flush=True)
            await asyncio.sleep(2 * (2 ** attempt))
    return {}


def _last_day_of_month(year: int, month: int) -> int:
    if month == 12:
        d = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        d = date(year, month + 1, 1) - timedelta(days=1)
    return d.day


async def fetch_metadata_for_year(
    client: httpx.AsyncClient, year: int, api_type: str
) -> dict[str, dict]:
    """Return {document_number: full_metadata_dict} for every doc in year+type."""
    docs: dict[str, dict] = {}
    for month in range(1, 13):
        page = 1
        while True:
            data = await _fetch_list_page(client, year, month, api_type, page)
            results = data.get("results") or []
            if not results:
                break
            for r in results:
                dn = r.get("document_number")
                if dn:
                    docs[dn] = r
            total_pages = int(data.get("total_pages") or 1)
            if page >= total_pages:
                break
            page += 1
    return docs


# ---------------------------------------------------------------------------
# Bulk XML fetching + parsing per publication date
# ---------------------------------------------------------------------------


async def fetch_and_parse_xml(
    client: httpx.AsyncClient, pub_date: str, target_tag: str
) -> dict[str, str]:
    """Return {frdoc: body_text} for every target_tag doc in the daily XML."""
    y, m, d = pub_date.split("-")
    url = f"{GOVINFO_BULK}/{y}/{m}/FR-{y}-{m}-{d}.xml"
    for attempt in range(4):
        try:
            r = await client.get(url, timeout=60)
            if r.status_code == 200:
                return parse_fr_xml(r.content, target_tag)
            if r.status_code == 404:
                # Weekend / holiday / non-publication day
                return {}
            if r.status_code in (429, 500, 502, 503, 504):
                await asyncio.sleep(2 * (2 ** attempt))
                continue
            return {}
        except Exception:
            await asyncio.sleep(2 * (2 ** attempt))
    return {}


# ---------------------------------------------------------------------------
# Record builder (matches schema of the old ingest -> sync + embed unchanged)
# ---------------------------------------------------------------------------


def _sha1_hex(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def _point_id_for(act_id: str, chunk_idx: int, text: str) -> str:
    seed = f"{act_id}::{chunk_idx}::{_sha1_hex(text)[:12]}"
    return str(uuid.UUID(hashlib.md5(seed.encode()).hexdigest()))


def _agency_slugs(agencies: list[dict]) -> list[str]:
    return sorted({a.get("slug") for a in (agencies or []) if a.get("slug")})


def _cfr_refs_normalized(refs: list[dict]) -> list[dict]:
    out = []
    for r in refs or []:
        t = r.get("title")
        p = r.get("part")
        if t is not None and p is not None:
            out.append({"title": t, "part": str(p)})
    return out


def build_record(api_meta: dict, body_text: str, dt_conf: dict) -> dict:
    docnum = api_meta.get("document_number", "")
    act_id = f"{dt_conf['act_prefix']}{docnum}"
    year_int: int | None = None
    pub_date = api_meta.get("publication_date") or ""
    if pub_date and len(pub_date) >= 4:
        try:
            year_int = int(pub_date[:4])
        except ValueError:
            year_int = None

    citation = api_meta.get("citation") or ""
    title = api_meta.get("title") or ""
    action = api_meta.get("action") or ""
    agencies = api_meta.get("agencies") or []
    topics = api_meta.get("topics") or []

    text_for_embedding = (
        f"Federal Register | {citation} | {api_meta.get('type', '')}\n"
        f"{title}\n"
        f"Action: {action}\n"
        f"Agencies: {', '.join(a.get('name','') for a in agencies)}\n"
        f"Topics: {', '.join(topics)}\n"
        f"{api_meta.get('abstract', '')}\n\n"
        f"{body_text}"
    )

    metadata: dict[str, Any] = {
        # Identity
        "act_id": act_id,
        "corpus_type": "agency_action",
        "category": dt_conf["category"],
        "document_type": "regulation",
        "jurisdiction": "federal",
        "country_code": "US",
        "state": "federal",
        "act_status": "in_force",
        "level_classifier": "regulation",

        # Retrieval body
        "text": body_text,
        "full_text_sha1": _sha1_hex(body_text),
        "word_count": len(body_text.split()),
        "chunk_index": 0,
        "total_chunks": 1,

        # Title / citation
        "title": "Federal Register",
        "title_code": "fr",
        "title_number": 9400,
        "top_level_title": f"fr-vol-{api_meta.get('volume')}" if api_meta.get("volume") else "fr",
        "section_number": docnum,
        "section_title": title,
        "display_title": title,
        "display_label": f"{citation} - {title}"[:200] if citation else title,
        "display_path": f"Federal Register / Vol. {api_meta.get('volume')} / {citation}",
        "breadcrumb": [
            "Federal Register",
            f"Vol. {api_meta.get('volume')}" if api_meta.get('volume') else "",
            "Rules" if dt_conf["category"] == "fr_rule" else "Proposed Rules",
            citation,
        ],
        "citation": citation,
        "citation_short": citation,
        "year": year_int,
        "sort_key": f"{pub_date}::{docnum}",

        # URLs
        "source_url": api_meta.get("html_url", ""),

        # Cross-references
        "cross_references_cfr": _cfr_refs_normalized(api_meta.get("cfr_references") or []),
        "cross_references_count": len(api_meta.get("cfr_references") or []),
        "cross_references_usc": [],
        "public_laws_count": 0,
        "public_laws_referenced": [],

        # FR-specific rich metadata
        "fr_type": api_meta.get("type"),
        "fr_subtype": api_meta.get("subtype"),
        "fr_publication_date": pub_date,
        "fr_effective_on": api_meta.get("effective_on"),
        "fr_comments_close_on": api_meta.get("comments_close_on"),
        "fr_volume": api_meta.get("volume"),
        "fr_start_page": api_meta.get("start_page"),
        "fr_end_page": api_meta.get("end_page"),
        "fr_page_length": api_meta.get("page_length"),
        "fr_action": action,
        "fr_dates_text": api_meta.get("dates") or "",
        "fr_significant": api_meta.get("significant"),
        "fr_correction_of": api_meta.get("correction_of"),
        "fr_corrections": api_meta.get("corrections") or [],
        "fr_docket_ids": api_meta.get("docket_ids") or [],
        "fr_regulation_id_numbers": api_meta.get("regulation_id_numbers") or [],
        "fr_regulations_dot_gov_url": api_meta.get("regulations_dot_gov_url"),
        "fr_agencies": agencies,
        "fr_agency_slugs": _agency_slugs(agencies),
        "fr_topics": topics,
        "fr_president": api_meta.get("president"),
        "abstract": api_meta.get("abstract") or "",

        # Standard schema keepalive
        "raw_node_id": act_id,
        "parent_id": None,
        "renumbered_to": "",
        "transferred_to": "",
        "amendments_count": 0,
        "amendment_years": [],
        "last_amended_year": None,
        "subsection_count": 0,
        "subsection_letters": [],
        "numbered_paragraph_count": 0,
    }
    return {
        "point_id": _point_id_for(act_id, 0, body_text),
        "text_for_embedding": text_for_embedding,
        "raw_text": body_text,
        "metadata": metadata,
    }


# ---------------------------------------------------------------------------
# JSONL merge
# ---------------------------------------------------------------------------


def merge_jsonl(path: Path, new_records: list[dict]) -> int:
    existing: dict[str, dict] = {}
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                aid = (rec.get("metadata") or {}).get("act_id")
                if aid:
                    existing[aid] = rec
            except json.JSONDecodeError:
                continue
    for rec in new_records:
        aid = rec["metadata"]["act_id"]
        existing[aid] = rec
    with open(path, "w", encoding="utf-8") as fh:
        for rec in existing.values():
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
    return len(existing)


# ---------------------------------------------------------------------------
# Per-year pipeline
# ---------------------------------------------------------------------------


def _load_existing_act_ids(path: Path) -> set[str]:
    """Return the set of act_ids already present in the year's JSONL."""
    if not path.exists():
        return set()
    seen: set[str] = set()
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                aid = (rec.get("metadata") or {}).get("act_id")
                if aid:
                    seen.add(aid)
            except json.JSONDecodeError:
                continue
    return seen


async def ingest_year(
    year: int, doc_type: str, xml_workers: int, out_path: Path, api_client: httpx.AsyncClient
) -> tuple[int, int]:
    """Ingest one year of a single doc_type. Returns (ok_count, fail_count).

    Resumable: reads existing JSONL, skips docs whose act_id is already
    present. Flushes records to JSONL every 25 completed dates so a crash
    only loses the last in-flight batch.
    """
    dt_conf = DOC_TYPE_MAP[doc_type]
    t0 = time.time()
    already_have = _load_existing_act_ids(out_path)
    print(f"[{year} {doc_type}] fetching API metadata (12 monthly calls)... "
          f"resume={len(already_have):,} already in JSONL", flush=True)

    api_meta = await fetch_metadata_for_year(api_client, year, dt_conf["api_type"])
    print(f"[{year} {doc_type}] API returned {len(api_meta):,} docs", flush=True)
    if not api_meta:
        return 0, 0

    # Filter out docs already ingested (resume)
    if already_have:
        act_prefix = dt_conf["act_prefix"]
        before = len(api_meta)
        api_meta = {dn: m for dn, m in api_meta.items()
                    if f"{act_prefix}{dn}" not in already_have}
        print(f"[{year} {doc_type}] resume skip {before - len(api_meta):,}, "
              f"{len(api_meta):,} still todo", flush=True)
    if not api_meta:
        print(f"[{year} {doc_type}] nothing to do; DONE", flush=True)
        return 0, 0

    # Group by publication date
    from collections import defaultdict
    by_date: dict[str, list[dict]] = defaultdict(list)
    for meta in api_meta.values():
        pd = meta.get("publication_date")
        if pd:
            by_date[pd].append(meta)
    print(f"[{year} {doc_type}] {len(by_date)} unique publication dates", flush=True)

    # Fetch + parse XMLs concurrently
    sem = asyncio.Semaphore(xml_workers)
    matched = unmatched = 0
    FLUSH_EVERY = 25  # flush after this many completed dates
    flush_buffer: list[dict] = []

    async with httpx.AsyncClient(
        headers={"User-Agent": UA, "Accept-Encoding": "gzip"},
        timeout=httpx.Timeout(60.0),
        http2=False,
    ) as xml_client:

        async def process_date(pub_date: str, docs_for_date: list[dict]) -> list[dict]:
            nonlocal matched, unmatched
            async with sem:
                bodies = await fetch_and_parse_xml(xml_client, pub_date, dt_conf["xml_tag"])
            out: list[dict] = []
            for meta in docs_for_date:
                dn = meta.get("document_number")
                body = bodies.get(dn, "")
                if not body:
                    unmatched += 1
                    continue
                rec = build_record(meta, body, dt_conf)
                out.append(rec)
                matched += 1
            return out

        # asyncio.as_completed lets us flush partial results incrementally
        # so a crash mid-year only loses the last unflushed batch.
        tasks = [process_date(d, docs) for d, docs in by_date.items()]
        completed = 0
        for coro in asyncio.as_completed(tasks):
            chunk = await coro
            completed += 1
            flush_buffer.extend(chunk)
            if len(flush_buffer) >= 200 or completed % FLUSH_EVERY == 0:
                if flush_buffer:
                    merged = merge_jsonl(out_path, flush_buffer)
                    print(f"[{year} {doc_type}] flush at date {completed}/{len(tasks)}  "
                          f"merged_total={merged:,}", flush=True)
                    flush_buffer = []

    if flush_buffer:
        merged = merge_jsonl(out_path, flush_buffer)
        print(f"[{year} {doc_type}] final flush  merged_total={merged:,}", flush=True)

    elapsed = time.time() - t0
    print(f"[{year} {doc_type}] DONE  matched={matched:,}  unmatched={unmatched:,}  "
          f"elapsed={elapsed:.1f}s ({matched/max(elapsed,1):.1f}/s)", flush=True)
    return matched, unmatched


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def _parse_year_range(s: str) -> list[int]:
    if "-" in s:
        a, b = s.split("-", 1)
        return list(range(int(a), int(b) + 1))
    return [int(s)]


def _year_out_path(base_stem: str, year: int) -> Path:
    """Per-year JSONL so parallel year workers don't fight over one file."""
    return DATA_DIR / f"{base_stem}_{year}.jsonl"


async def _run(args) -> None:
    dt_conf = DOC_TYPE_MAP[args.doc_type]
    # Base stem: strip trailing '.jsonl' if user provided --out
    if args.out:
        stem = args.out.stem
    else:
        stem = dt_conf["jsonl"].replace(".jsonl", "")

    years = _parse_year_range(args.year_range) if args.year_range else [args.year]
    print(f"Federal Register bulk ingest: doc-type={args.doc_type} years={years}")
    print(f"  xml_workers/year={args.xml_workers}  year_workers={args.year_workers}")
    print(f"  out_pattern={DATA_DIR / (stem + '_{year}.jsonl')}")

    year_sem = asyncio.Semaphore(args.year_workers)

    async with httpx.AsyncClient(
        headers={"User-Agent": UA, "Accept-Encoding": "gzip"},
        timeout=httpx.Timeout(60.0),
        http2=False,
    ) as api_client:

        async def one_year(y: int):
            out_path = _year_out_path(stem, y)
            async with year_sem:
                return await ingest_year(
                    y, args.doc_type, args.xml_workers, out_path, api_client
                )

        results = await asyncio.gather(*[one_year(y) for y in years])
    tot_ok = sum(r[0] for r in results)
    tot_miss = sum(r[1] for r in results)
    print()
    print(f"=== Overall: doc-type={args.doc_type} years={years} ===")
    print(f"  matched={tot_ok:,}  unmatched={tot_miss:,}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, help="Single year (e.g. 2024)")
    ap.add_argument("--year-range", type=str, help="Range like 1994-2025")
    ap.add_argument("--doc-type", default="RULE", choices=["RULE", "PRORULE"])
    ap.add_argument("--xml-workers", type=int, default=8,
                    help="Concurrent bulk-XML downloads per year")
    ap.add_argument("--year-workers", type=int, default=4,
                    help="Concurrent years")
    ap.add_argument("--out", type=Path, default=None)
    args = ap.parse_args()
    if not args.year and not args.year_range:
        ap.error("must pass --year or --year-range")

    asyncio.run(_run(args))


if __name__ == "__main__":
    main()
