#!/usr/bin/env python3
"""Ingest the full Idaho Code from the official legislature.idaho.gov site.

The Idaho scraper (state_scrapers/.../id/statutes/scrapeID.py) reached only ~17
of the 74 titles Idaho publishes (titles 1-14, 16, 17, 18), leaving whole codes
(incl. Title 19 Criminal Procedure, 32 Domestic Relations, 48 Consumer
Protection, 55 Property, 63 Revenue and Taxation) missing. This replaces it with
a complete crawl of Idaho's own statutes site so the entire Code is ingested.

Idaho publishes no bulk zip / JSON API (unlike NJ / NY / VA); the authoritative
current source is the server-rendered HTML at
legislature.idaho.gov/statutesrules/idstat/, a clean Title -> Chapter -> Section
tree (with occasional sub-chapters that flatten into their parent chapter). It is
the same source that produced the existing Idaho act_ids.

Pipeline (mirrors ingest_va_bulk.py / ingest_ilga_bulk.py):
    TOC (titles) -> title pages (chapters) -> chapter pages (section numbers,
    sub-chapters) -> section pages (body) -> synthetic Node
    -> node_to_payload.node_to_chunks -> JSONL

Reusing node_to_chunks means act_id / point_id / citation / chunking / R2 upload
match the scraper path exactly. The Title/Chapter/Section hierarchy reproduces
the existing act_id scheme (e.g. 18-4003 -> STATE_ID_T18_C40_S18-4003); verified
against Qdrant by id_bulk/verify_act_ids.py before any full run.

Run on the scraper box (Idaho gov geo-blocks the box; proxy egress required):
    docker exec -d -e VAQUILL_USE_PROXY=1 -e VAQUILL_R2_UPLOAD=1 -e VAQUILL_R2_POOL=64 \
      -w /app vaquill-scraper-worker \
      python -u scripts/us_corpus/ingest_id_bulk.py --workers 24 \
        > /app/id_ingest.log 2>&1
    then: embed_and_upsert.py --input .../state_id_statutes.jsonl            (additive)
          embed_and_upsert.py --input .../state_id_statutes.jsonl --reconcile

Cutover note: bulk text differs from scraped text, so every section gets a fresh
content-addressed point_id; act_id-scoped --reconcile deletes the superseded ones
within the act_ids this run touched. state=id ALSO holds Idaho's IDAPA
regulations and constitution under the same state tag, so --reconcile-state id is
NEVER used here; act_id-scoped reconcile is naturally safe (regulation /
constitution act_ids are never in the run).
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

from id_bulk import client as C
from id_bulk import parse as P
from id_bulk.walk import IDSection

STATE = "id"


def _default_out() -> Path:
    override = os.environ.get("STATE_CHUNKS_DIR_OVERRIDE")
    base = Path(override) if override else (_SCRAPERS / "data" / "state_chunks")
    return base / f"state_{STATE}_statutes.jsonl"


def enumerate_sections(index_workers: int) -> list[IDSection]:
    """Crawl TOC -> titles -> chapters -> sections (+ sub-chapters).

    Each level's pages are fetched in parallel. Sections are deduped by node_id
    (a section can be listed only once, but a defensive dedupe guards against a
    sub-chapter row that also appears on the parent chapter page).
    """
    print("fetching title list...", flush=True)
    titles = P.title_rows(C.get_html("/statutesrules/idstat/"))
    print(f"  {len(titles)} titles", flush=True)

    # Phase 1: title pages -> (title_number, chapter_number, chapter_url)
    chapters: list[tuple[str, str, str]] = []

    def _title(item):
        number, _name, url = item
        try:
            html = C.get_html(url)
        except Exception as exc:
            return (number, None, str(exc)[:120])
        rows = P.chapter_rows(html)
        return (number, [(number, ch, chu) for ch, chu in rows], None)

    errs = 0
    with ThreadPoolExecutor(max_workers=index_workers) as ex:
        for fut in as_completed(ex.submit(_title, t) for t in titles):
            tn, rows, err = fut.result()
            if err:
                errs += 1
                if errs <= 10:
                    print(f"  [title {tn}] {err}", flush=True)
                continue
            chapters.extend(rows)
    print(f"  {len(chapters):,} chapters across {len(titles)} titles", flush=True)

    # Phase 2: chapter pages, then BFS over any sub-chapters / parts they
    # contain, until no deeper containers remain. Each frontier level is fetched
    # in parallel; sections at every level flatten into their parent chapter
    # (carried as (title, chapter) through the frontier). visited guards cycles.
    sections: dict[str, IDSection] = {}
    frontier: list[tuple[str, str, str]] = list(chapters)  # (title, chapter, url)
    visited: set[str] = {url for _t, _c, url in frontier}
    errs = 0
    level = 0

    def _fetch(item):
        title_number, chapter_number, url = item
        try:
            html = C.get_html(url)
        except Exception as exc:
            return (title_number, chapter_number, url, None, None, str(exc)[:120])
        secs, subs = P.section_rows(html)
        return (title_number, chapter_number, url, secs, subs, None)

    while frontier:
        level += 1
        next_frontier: list[tuple[str, str, str]] = []
        with ThreadPoolExecutor(max_workers=index_workers) as ex:
            for fut in as_completed(ex.submit(_fetch, it) for it in frontier):
                tn, ch, _url, secs, subs, err = fut.result()
                if err:
                    errs += 1
                    if errs <= 10:
                        print(f"  [container {tn}-{ch}] {err}", flush=True)
                    continue
                for label, desc, sec_url in secs:
                    s = IDSection(tn, ch, label, desc, sec_url)
                    sections[s.node_id()] = s
                for sub_url in subs:
                    if sub_url not in visited:
                        visited.add(sub_url)
                        next_frontier.append((tn, ch, sub_url))
        if next_frontier:
            print(
                f"  level {level}: found {len(next_frontier):,} sub-chapters/parts to crawl",
                flush=True,
            )
        frontier = next_frontier

    out = sorted(sections.values(), key=lambda s: s.node_id())
    print(f"  enumerated {len(out):,} distinct sections ({errs} container fetch errors)", flush=True)
    return out


def build_records(index_workers: int, body_workers: int, limit: int = 0) -> tuple[list[dict], dict]:
    from src.utils.pydanticModels import Node, NodeText
    from vaquill_pipeline.node_to_payload import node_to_chunks

    sections = enumerate_sections(index_workers)
    if limit:
        sections = sections[:limit]
    year = time.gmtime().tm_year
    print(f"fetching {len(sections):,} section bodies with {body_workers} workers...", flush=True)

    stats = {"sections": 0, "chunks": 0, "no_text": 0, "fetch_err": 0}
    out: list[dict] = []

    def _one(sec: IDSection):
        try:
            html = C.get_html(sec.url)
        except Exception as exc:
            return ("fetch_err", f"{sec.section_number}: {str(exc)[:120]}", None)
        paras = P.section_paragraphs(html)
        if not paras:
            return ("no_text", None, None)

        node_id = sec.node_id()
        node = Node(
            id=node_id,
            link=sec.url,
            node_type="content",
            level_classifier="section",
            number=sec.section_number,
            node_name=sec.node_name(),
            node_text=NodeText(),
            citation=sec.citation(),
            top_level_title=sec.title_number,
            parent=sec.parent_id(),
        )
        for para in paras:
            node.node_text.add_paragraph(text=para)
        try:
            chunks = node_to_chunks(node, STATE, year)
        except Exception as exc:
            return ("fetch_err", f"{sec.section_number}: node_to_chunks: {str(exc)[:120]}", None)
        if not chunks:
            return ("no_text", None, None)
        return ("ok", None, chunks)

    t0 = time.time()
    done = 0
    with ThreadPoolExecutor(max_workers=body_workers) as ex:
        for fut in as_completed(ex.submit(_one, s) for s in sections):
            kind, tag, chunks = fut.result()
            done += 1
            if done % 1000 == 0:
                rate = done / max(time.time() - t0, 0.001)
                eta = (len(sections) - done) / rate / 60 if rate else 0
                print(
                    f"  {done:,}/{len(sections):,}  {rate:.1f}/s  chunks={stats['chunks']:,}  "
                    f"ETA={eta:.1f}m",
                    flush=True,
                )
            if kind == "ok":
                stats["sections"] += 1
                stats["chunks"] += len(chunks)
                out.extend(chunks)
            else:
                stats[kind] = stats.get(kind, 0) + 1
                if kind == "fetch_err" and stats[kind] <= 8:
                    print(f"  {tag}", flush=True)
    return out, stats


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", type=Path, default=None)
    ap.add_argument("--limit", type=int, default=0, help="only first N sections (smoke test)")
    ap.add_argument("--workers", type=int, default=24, help="section-body fetch workers")
    ap.add_argument("--index-workers", type=int, default=16, help="HTML enumeration workers")
    args = ap.parse_args()

    out_path = args.out or _default_out()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    records, stats = build_records(args.index_workers, args.workers, limit=args.limit)

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
