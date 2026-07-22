#!/usr/bin/env python3
"""Ingest the full Pennsylvania Consolidated Statutes from the official
palegis.us per-title PDF export.

The previous PA scraper reached only the Thomson Reuters Findlaw mirror
(codes.findlaw.com/pa) after the official host was thought unreachable, and it
left the corpus thin: 12,510 distinct Pa.C.S. sections, section-shallow inside
big titles (Title 42 Judiciary 1,137 of ~2,500, Title 18 Crimes 676 of ~740,
Title 75 Vehicles 1,000 of ~1,150). This replaces it with the Commonwealth's own
per-title PDF so every consolidated title is ingested completely.

Source (one proxied request per title, ~79 total -- tiny proxy footprint):
    GET /statutes/consolidated                          -> title universe
    GET /statutes/consolidated/view-statute?txtType=PDF&ttl=NN  -> full title PDF

Pipeline (mirrors ingest_va_bulk.py):
    index -> consolidated titles
    per title: PDF -> parse.parse_title_pdf -> ParsedSection rows
    per section: synthetic Node -> node_to_payload.node_to_chunks -> JSONL

Reusing node_to_chunks means act_id / point_id / citation / chunking / R2 upload
match the canonical pipeline exactly. Section numbers are canonical dot form
(``2502``, ``3132.1``), so citations read ``18 Pa.C.S. § 2502``. The bulk act_ids
therefore differ from the legacy Findlaw hyphen-form ones only by the decimal
separator, so a state-scoped reconcile (document_type=statute-scoped, which
preserves PA's constitution + court rules) replaces the old points -- see
verify_act_ids.py for the reproduction / coverage measurement.

Phases are separated (fetch+parse ALL titles, THEN chunk ALL sections) so the
r2_sync crawl/chunk race never triggers and the proxy phase is bounded.

Run on the scraper box (palegis geo-blocks the box; US proxy egress required):
    docker exec -d -e VAQUILL_USE_PROXY=1 -e VAQUILL_R2_UPLOAD=1 -e VAQUILL_R2_POOL=64 \
      -w /app vaquill-scraper-worker \
      python -u scripts/us_corpus/statutes/ingest_pa_bulk.py --workers 12 \
        > /app/pa_ingest.log 2>&1
    then: embed_and_upsert.py --input .../state_pa_statutes.jsonl              (additive)
          embed_and_upsert.py --input .../state_pa_statutes.jsonl --reconcile-state pa \
            --min-run-points N --dry-run     (verify kept/stale, constitution preserved)
          embed_and_upsert.py --input .../state_pa_statutes.jsonl --reconcile-state pa \
            --min-run-points N               (real)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_SCRAPERS = _HERE.parent / "state_scrapers"
sys.path.insert(0, str(_HERE))
sys.path.insert(0, str(_SCRAPERS))

from pa_bulk import client as C
from pa_bulk import parse as P
from pa_bulk.walk import PASection

STATE = "pa"


def _default_out() -> Path:
    override = os.environ.get("STATE_CHUNKS_DIR_OVERRIDE")
    base = Path(override) if override else (_SCRAPERS / "data" / "state_chunks")
    return base / f"state_{STATE}_statutes.jsonl"


def fetch_and_parse(
    titles: list[tuple[str, str]], workers: int
) -> tuple[dict[str, tuple[str, list[P.ParsedSection]]], dict]:
    """Phase 1: fetch every title PDF and parse it (proxy phase, phase-separated).

    Returns {title_number: (title_name, sections)} and a stats dict. Titles that
    parse to zero sections are reserved / not-yet-consolidated and are reported
    but carried with an empty list.
    """
    print(f"fetching + parsing {len(titles)} titles with {workers} workers...", flush=True)
    results: dict[str, tuple[str, list[P.ParsedSection]]] = {}
    stats = {"titles": 0, "reserved": 0, "sections": 0, "fetch_err": 0}

    def _one(item: tuple[str, str]):
        ttl, name = item
        try:
            pdf = C.title_pdf(ttl)
        except Exception as exc:
            return (ttl, name, None, str(exc)[:160])
        secs = P.parse_title_pdf(pdf)
        return (ttl, name, secs, None)

    with ThreadPoolExecutor(max_workers=workers) as ex:
        for fut in as_completed(ex.submit(_one, it) for it in titles):
            ttl, name, secs, err = fut.result()
            if err is not None:
                stats["fetch_err"] += 1
                print(f"  [fetch] title {ttl} ({name}): ERROR {err}", flush=True)
                results[ttl] = (name, [])
                continue
            results[ttl] = (name, secs)
            stats["titles"] += 1
            if not secs:
                stats["reserved"] += 1
                print(f"  title {ttl:<3} {name[:40]:<40} RESERVED (0 sections)", flush=True)
            else:
                stats["sections"] += len(secs)
                print(f"  title {ttl:<3} {name[:40]:<40} {len(secs):,} sections", flush=True)
    return results, stats


def build_records(
    parsed: dict[str, tuple[str, list[P.ParsedSection]]], chunk_workers: int
) -> tuple[list[dict], dict]:
    """Phase 2: turn parsed sections into chunk rows (R2 phase, no proxy)."""
    from src.utils.pydanticModels import Node, NodeText
    from vaquill_pipeline.node_to_payload import node_to_chunks

    year = time.gmtime().tm_year
    work: list[tuple[str, str, P.ParsedSection]] = []
    for ttl, (name, secs) in parsed.items():
        for sec in secs:
            work.append((ttl, name, sec))
    print(f"\nchunking {len(work):,} sections with {chunk_workers} workers...", flush=True)

    stats = {"sections": 0, "chunks": 0, "no_text": 0, "err": 0, "reserved": 0}
    out: list[dict] = []

    def _one(item: tuple[str, str, P.ParsedSection]):
        ttl, name, ps = item
        if not ps.paragraphs:
            return ("no_text", None)
        sec = PASection(
            title_number=ttl,
            title_name=name,
            section_number=ps.number,
            section_title=ps.heading,
            status=ps.status,
        )
        node_id = sec.node_id()
        node = Node(
            id=node_id,
            link=C.title_html_url(ttl),
            node_type="content",
            level_classifier="section",
            number=sec.section_number,
            node_name=sec.section_title or f"Section {sec.section_number}",
            node_text=NodeText(),
            citation=sec.citation(),
            top_level_title=sec.title_number,
            parent="/".join(node_id.split("/")[:-1]),
            status=sec.status,
        )
        for para in ps.paragraphs:
            node.node_text.add_paragraph(text=para)
        try:
            chunks = node_to_chunks(node, STATE, year)
        except Exception as exc:
            return ("err", f"{sec.citation()}: {str(exc)[:120]}")
        return ("ok", chunks, ps.status)

    t0 = time.time()
    done = 0
    with ThreadPoolExecutor(max_workers=chunk_workers) as ex:
        for fut in as_completed(ex.submit(_one, it) for it in work):
            res = fut.result()
            kind = res[0]
            done += 1
            if done % 2000 == 0:
                rate = done / max(time.time() - t0, 0.001)
                eta = (len(work) - done) / rate / 60 if rate else 0
                print(
                    f"  {done:,}/{len(work):,}  {rate:.0f}/s  chunks={stats['chunks']:,}  "
                    f"ETA={eta:.1f}m",
                    flush=True,
                )
            if kind == "ok":
                chunks = res[1]
                stats["sections"] += 1
                stats["chunks"] += len(chunks)
                if res[2]:
                    stats["reserved"] += 1
                out.extend(chunks)
            elif kind == "no_text":
                stats["no_text"] += 1
            else:
                stats["err"] += 1
                if stats["err"] <= 8:
                    print(f"  {res[1]}", flush=True)
    return out, stats


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", type=Path, default=None)
    ap.add_argument("--workers", type=int, default=12, help="per-title PDF fetch workers")
    ap.add_argument("--chunk-workers", type=int, default=16, help="section chunk/R2 workers")
    ap.add_argument(
        "--titles", type=str, default="", help="comma-separated title numbers (smoke subset)"
    )
    args = ap.parse_args()

    out_path = args.out or _default_out()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    print("fetching consolidated index...", flush=True)
    index_html = C.get_index()
    titles = P.consolidated_titles(index_html)
    currency = P.current_through(index_html)
    print(f"  {len(titles)} title slots; currency: {currency!r}", flush=True)

    if args.titles:
        want = {t.strip().lstrip("0") or "0" for t in args.titles.split(",") if t.strip()}
        titles = [(t, n) for t, n in titles if t in want]
        print(f"  restricted to {len(titles)} titles: {[t for t, _ in titles]}", flush=True)

    parsed, fstats = fetch_and_parse(titles, args.workers)
    records, cstats = build_records(parsed, args.chunk_workers)

    # Fresh file per run so --reconcile-state sees exactly this run's corpus.
    with open(out_path, "w", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")

    consolidated = sorted((int(t) for t, (_n, s) in parsed.items() if s), key=lambda x: x)
    reserved = sorted((int(t) for t, (_n, s) in parsed.items() if not s), key=lambda x: x)

    print("\n=== done ===")
    print(f"  currency          : {currency!r}")
    print(f"  titles fetched    : {fstats['titles']:,}  (fetch_err {fstats['fetch_err']})")
    print(f"  consolidated ({len(consolidated)}): {consolidated}")
    print(f"  reserved     ({len(reserved)}): {reserved}")
    print(f"  sections parsed   : {fstats['sections']:,}")
    print(f"  sections chunked  : {cstats['sections']:,}  (reserved {cstats['reserved']})")
    print(f"  no_text/err       : {cstats['no_text']:,} / {cstats['err']:,}")
    print(f"  chunks            : {cstats['chunks']:,}")
    print(f"  wrote             : {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
