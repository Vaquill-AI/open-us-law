#!/usr/bin/env python3
"""Ingest Illinois statutes (ILCS) from the official ILGA file tree.

The IL scraper gave us 22,930 of the 72,163 sections Illinois publishes (32%):
19 of 68 chapters, missing ~49,500 sections across 49 chapters. This replaces
it with the ILGA static file tree at https://www.ilga.gov/ftp/ILCS/ , driven by
the official `Section Sequence.txt` manifest (every section listed up front, so
there is no discovery crawl and no way to miss a chapter).

Pipeline (mirrors ingest_ca_bulk.py):
    manifest -> for each K entry: derive URL -> fetch section HTML -> text ->
    synthetic Node -> node_to_payload.node_to_chunks -> JSONL

Reusing node_to_chunks means act_id / point_id / citation / chunking / R2 upload
match the scraper path exactly. act_id reproduction verified at 98.72% by
ilga_bulk/verify_act_ids.py (the 293 misses are the scraper's own ::vN
multi-version suffixes; the base act_ids all match).

Run on the scraper box (ilga.gov needs US egress via the proxy):
    docker exec -d -e VAQUILL_USE_PROXY=1 -e VAQUILL_R2_UPLOAD=1 -e VAQUILL_R2_POOL=64 \
      -w /app vaquill-scraper-worker \
      python -u scripts/us_corpus/ingest_ilga_bulk.py --workers 32 \
        > /app/il_ingest.log 2>&1
    then: embed_and_upsert.py --input .../state_il_statutes.jsonl --reconcile
          sync_states_to_supabase.py --states il

Cutover note: bulk text differs from scraped text, so the 22,637 sections we
already hold get new point_ids once; --reconcile deletes the superseded ones.
Because the mapping matches at 98.72%, reconcile can see those old points.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.parse as up
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_SCRAPERS = _HERE.parent / "state_scrapers"
sys.path.insert(0, str(_HERE))
sys.path.insert(0, str(_SCRAPERS))

from ilga_bulk import manifest as M
from ilga_bulk.parse import FTP_BASE, html_to_text, section_url

STATE = "il"
CORPUS = "statutes"
COUNTRY = "us"
MANIFEST_URL = f"{FTP_BASE}/aReadMe/{up.quote('Section Sequence.txt')}"

# Human-facing source_url for a section (the ILGA public site view).
SECTION_VIEW = "https://www.ilga.gov/legislation/ilcs/ilcs.asp"


def _default_out() -> Path:
    override = os.environ.get("STATE_CHUNKS_DIR_OVERRIDE")
    base = Path(override) if override else (_SCRAPERS / "data" / "state_chunks")
    return base / f"state_{STATE}_statutes.jsonl"


def _node_id(chapter: str, act: str, section: str) -> str:
    """Path-style node id the pipeline turns into STATE_IL_C{ch}_A{act}_S{sec}."""
    return f"{COUNTRY}/{STATE}/{CORPUS}/chapter={chapter}/act={act}/section={section}"


def build_records(session_workers: int, limit: int = 0) -> tuple[list[dict], dict]:
    from src.utils.pydanticModels import Node, NodeText
    from vaquill_pipeline.http_client import fetch_html
    from vaquill_pipeline.node_to_payload import node_to_chunks

    print(f"fetching manifest: {MANIFEST_URL}", flush=True)
    entries = M.sections(M.parse_manifest(fetch_html(MANIFEST_URL)))
    if limit:
        entries = entries[:limit]
    year = time.gmtime().tm_year
    print(f"  {len(entries):,} sections to ingest", flush=True)

    stats = {"sections": 0, "chunks": 0, "no_text": 0, "fetch_err": 0}
    out: list[dict] = []

    def _one(e: M.ManifestEntry):
        # All work per section runs in the worker: fetch, parse, chunk (which
        # uploads section text to R2). node_to_chunks is safe concurrently.
        try:
            html = fetch_html(section_url(e.raw))
        except Exception as exc:
            return ("fetch_err", f"{e.act_id()}: {str(exc)[:120]}", None)
        text = html_to_text(html)
        if not text or len(text) < 20:
            return ("no_text", None, None)
        node_id = _node_id(e.chapter, e.act, e.section)
        node = Node(
            id=node_id,
            link=SECTION_VIEW,
            node_type="content",
            level_classifier="section",
            number=e.section,
            node_name=f"Section {e.section}",
            node_text=NodeText(),
            citation=e.citation(),
            top_level_title=e.chapter,
            parent="/".join(node_id.split("/")[:-1]),
            status=None,
        )
        node.node_text.add_paragraph(text=text)
        try:
            chunks = node_to_chunks(node, STATE, year)
        except Exception as exc:
            return ("fetch_err", f"{e.act_id()}: node_to_chunks: {str(exc)[:120]}", None)
        return ("ok", None, chunks)

    print(f"fetching + parsing sections with {session_workers} workers...", flush=True)
    t0 = time.time()
    done = 0
    with ThreadPoolExecutor(max_workers=session_workers) as ex:
        for fut in as_completed(ex.submit(_one, e) for e in entries):
            kind, msg, chunks = fut.result()
            done += 1
            if done % 2000 == 0:
                rate = done / max(time.time() - t0, 0.001)
                print(f"  {done:,}/{len(entries):,}  {rate:.1f}/s  chunks={stats['chunks']:,}", flush=True)
            if kind == "ok":
                stats["sections"] += 1
                stats["chunks"] += len(chunks)
                out.extend(chunks)
            else:
                stats[kind] = stats.get(kind, 0) + 1
                if kind == "fetch_err" and stats[kind] <= 5:
                    print(f"  {msg}", flush=True)
    return out, stats


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", type=Path, default=None)
    ap.add_argument("--limit", type=int, default=0, help="only N sections (smoke)")
    ap.add_argument("--workers", type=int, default=32)
    args = ap.parse_args()

    out_path = args.out or _default_out()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    records, stats = build_records(args.workers, limit=args.limit)

    # Fresh file per run so --reconcile sees exactly this run's corpus.
    with open(out_path, "w", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")

    print("\n=== done ===")
    print(f"  sections   : {stats['sections']:,}")
    print(f"  chunks     : {stats['chunks']:,}")
    print(f"  no_text    : {stats.get('no_text', 0):,}")
    print(f"  fetch_err  : {stats.get('fetch_err', 0):,}")
    print(f"  wrote      : {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
