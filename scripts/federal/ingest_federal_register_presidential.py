#!/usr/bin/env python3
"""Ingest presidential documents (Executive Orders + Proclamations + Memos)
from federalregister.gov API into the existing pipeline.

corpus_type='executive_action', act_id prefix 'EXEC_<DOCNUM>'

API: https://www.federalregister.gov/api/v1/documents
  - public, no key needed
  - paginated 100/page, 50 pages max but can filter by year for full coverage
  - returns metadata + raw_text_url + body_html_url + pdf_url + document_number

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
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import requests

sys.stdout.reconfigure(line_buffering=True)  # type: ignore[attr-defined]

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = Path(os.environ.get("OUT_DIR", "./data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
OUT = DATA_DIR / "executive_actions_chunks.jsonl"

UA = "Mozilla/5.0 (open-us-law ingestion bot; +https://github.com/Vaquill-AI/open-us-law)"
FR_BASE = "https://www.federalregister.gov/api/v1/documents"


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


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": UA})
    a = requests.adapters.HTTPAdapter(pool_connections=32, pool_maxsize=32, max_retries=0)
    s.mount("http://", a)
    s.mount("https://", a)
    return s


SESSION = _session()


def fetch_json(url: str) -> dict:
    for attempt in range(4):
        try:
            r = SESSION.get(url, timeout=30, allow_redirects=True)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt == 3:
                raise
            time.sleep(1.0 * (attempt + 1))
    return {}


def fetch_text(url: str) -> str:
    for attempt in range(4):
        try:
            r = SESSION.get(url, timeout=60, allow_redirects=True)
            r.raise_for_status()
            return r.text
        except Exception as e:
            if attempt == 3:
                raise
            time.sleep(1.0 * (attempt + 1))
    return ""


def fetch_bytes(url: str) -> bytes:
    for attempt in range(4):
        try:
            r = SESSION.get(url, timeout=120, allow_redirects=True)
            r.raise_for_status()
            return r.content
        except Exception as e:
            if attempt == 3:
                raise
            time.sleep(1.0 * (attempt + 1))
    return b""


# ---------------------------------------------------------------------------
# Chunk schema
# ---------------------------------------------------------------------------


@dataclass
class Doc:
    doc_num: str
    title: str
    type_: str
    subtype: str
    president: str
    pub_date: str
    sign_date: str
    citation: str
    raw_text: str
    pdf_url_source: Optional[str]
    html_url_source: Optional[str]
    raw_text_url_source: Optional[str]


def sha1_hex(s: str) -> str:
    return hashlib.sha1(s.encode()).hexdigest()


def point_id_for(act_id: str, chunk_idx: int, text: str) -> str:
    h = hashlib.md5(f"{act_id}::{chunk_idx}::{sha1_hex(text)[:12]}".encode()).hexdigest()
    return str(uuid.UUID(h))


def to_chunk_record(d: Doc) -> dict:
    text = d.raw_text.strip()
    act_id = f"EXEC_{d.doc_num}"
    title_name = "Presidential Documents"
    subtype = (d.subtype or "").lower()
    if "executive" in subtype:
        doctype = "executive_order"
    elif "proclamation" in subtype:
        doctype = "proclamation"
    elif "memorandum" in subtype:
        doctype = "memorandum"
    else:
        doctype = "presidential_document"

    citation = d.citation or f"FR Doc. {d.doc_num}"

    text_for_embedding = (
        f"{title_name} | {d.type_} | {doctype}\n"
        f"{d.title}\n"
        f"President: {d.president or 'Unknown'} | Signed: {d.sign_date} | Published: {d.pub_date}\n\n"
        f"{text}"
    )

    md = {
        "act_id": act_id,
        "corpus_type": "executive_action",
        "category": "executive_action",
        "document_type": doctype,
        "jurisdiction": "US",
        "country_code": "US",
        "state": "federal",
        "title_name": title_name,
        "title": title_name,
        "top_level_title": "presidential_documents",
        "title_code": "PRESDOCU",
        "level_classifier": doctype,
        "chapter": None,
        "section_number": d.doc_num,
        "section_title": d.title,
        "citation": citation,
        "citation_short": citation,
        "display_label": citation,
        "display_title": d.title,
        "display_path": f"Presidential Documents / {doctype.replace('_', ' ').title()} / {d.doc_num}",
        "breadcrumb": ["Presidential Documents", doctype.replace("_", " ").title(), d.doc_num],
        "sort_key": act_id,
        "act_status": "in_force",
        "renumbered_to": "",
        "transferred_to": "",
        "year": int(d.pub_date.split("-")[0]) if d.pub_date else None,
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
        "source_url": d.html_url_source or d.raw_text_url_source or d.pdf_url_source,
        "parent_id": None,
        "raw_node_id": act_id,
        "full_text_sha1": sha1_hex(text) if text else None,
        # extra president-specific fields
        "president": d.president,
        "signing_date": d.sign_date,
    }
    return {
        "point_id": point_id_for(act_id, 0, text or d.title),
        "text_for_embedding": text_for_embedding,
        "raw_text": text,
        "metadata": md,
    }


# ---------------------------------------------------------------------------
# Federal Register API
# ---------------------------------------------------------------------------


_FIELDS = [
    "document_number", "title", "publication_date", "signing_date", "type",
    "subtype", "president", "raw_text_url", "body_html_url", "pdf_url",
    "citation", "html_url",
]


def list_presidential_docs(year: int, per_page: int = 100) -> list[dict]:
    """List presidential documents for one year. Returns flattened metadata."""
    out = []
    page = 1
    fields = "&fields[]=" + "&fields[]=".join(_FIELDS)
    while True:
        url = (
            f"{FR_BASE}?conditions%5Btype%5D=PRESDOCU"
            f"&conditions%5Bpublication_date%5D%5Byear%5D={year}"
            f"&per_page={per_page}&page={page}"
            f"{fields}"
        )
        d = fetch_json(url)
        results = d.get("results", []) or []
        if not results:
            break
        out.extend(results)
        if len(results) < per_page:
            break
        page += 1
        time.sleep(0.3)
    return out


def fetch_doc_text(meta: dict) -> str:
    """Get the raw text body of a doc. Prefer raw_text_url, fallback body_html_url."""
    raw_url = meta.get("raw_text_url")
    if raw_url:
        try:
            return fetch_text(raw_url)
        except Exception:
            pass
    html_url = meta.get("body_html_url")
    if html_url:
        try:
            html = fetch_text(html_url)
            # strip tags
            from bs4 import BeautifulSoup
            return BeautifulSoup(html, "html.parser").get_text(" ", strip=True)
        except Exception:
            pass
    return ""


def ingest_doc(meta: dict) -> Optional[Doc]:
    doc_num = meta.get("document_number") or ""
    if not doc_num:
        return None
    text = fetch_doc_text(meta)
    if not text or len(text.strip()) < 30:
        return None

    pres = meta.get("president") or {}
    pres_name = pres.get("name") if isinstance(pres, dict) else (pres or "")

    d = Doc(
        doc_num=doc_num,
        title=meta.get("title") or "",
        type_=meta.get("type") or "Presidential Document",
        subtype=meta.get("subtype") or "",
        president=pres_name,
        pub_date=meta.get("publication_date") or "",
        sign_date=meta.get("signing_date") or "",
        citation=meta.get("citation") or "",
        raw_text=text,
        pdf_url_source=meta.get("pdf_url"),
        html_url_source=meta.get("body_html_url") or meta.get("html_url"),
        raw_text_url_source=meta.get("raw_text_url"),
    )

    return d


# ---------------------------------------------------------------------------
# Merge-safe JSONL writer
# ---------------------------------------------------------------------------


def merge_jsonl(path: Path, new_docs: list[Doc]) -> int:
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
    for d in new_docs:
        rec = to_chunk_record(d)
        existing[rec["metadata"]["act_id"]] = rec
    with open(path, "w", encoding="utf-8") as fh:
        for rec in existing.values():
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
    return len(existing)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--years", default="",
                    help="Comma-separated years (e.g. '2024,2025'). Default: last 5 years")
    ap.add_argument("--from-year", type=int, default=None)
    ap.add_argument("--to-year", type=int, default=None)
    ap.add_argument("--workers", type=int, default=8)
    args = ap.parse_args()

    if args.years:
        years = [int(y) for y in args.years.split(",") if y.strip()]
    elif args.from_year and args.to_year:
        years = list(range(args.from_year, args.to_year + 1))
    else:
        from datetime import datetime
        ny = datetime.utcnow().year
        years = list(range(ny - 4, ny + 1))

    _load_env()

    print(f"=== Federal Register Presidential Docs: years {years[0]}..{years[-1]} ===")
    all_docs: list[Doc] = []
    for y in years:
        metas = list_presidential_docs(y)
        print(f"  {y}: {len(metas)} docs in API")
        # Parallel fetch text
        with cf.ThreadPoolExecutor(max_workers=args.workers) as ex:
            futs = [ex.submit(ingest_doc, m) for m in metas]
            year_docs = []
            for fut in cf.as_completed(futs):
                d = fut.result()
                if d:
                    year_docs.append(d)
        print(f"  {y}: ingested {len(year_docs)} with text")
        all_docs.extend(year_docs)

    n = merge_jsonl(OUT, all_docs)
    print(f"\n=> JSONL has {n} executive-action chunks at {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
