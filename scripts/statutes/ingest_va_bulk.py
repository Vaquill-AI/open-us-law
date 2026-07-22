#!/usr/bin/env python3
"""Ingest the full Code of Virginia from the official law.lis.virginia.gov site.

The VA scraper (state_scrapers/.../va/statutes/scrapeVA.py) reached only 27 of
the ~76 titles Virginia publishes, leaving whole codes (incl. Title 18.2 Crimes,
40.1 Labor, 58.1 Taxation, 19.2 Criminal Procedure) partly or wholly missing.
This replaces it with the Commonwealth's own data surfaces so the entire Code is
ingested completely.

Two-surface source (each used for what it does reliably):
  - Enumeration: the server-rendered vacode HTML (title page -> chapter pages ->
    section row checkboxes). The JSON chapters / section-list endpoints silently
    drop decimal chapters (e.g. 3.1 -> sections 1-300..1-313) and mis-group some
    sections, so they are NOT trusted for the section list.
  - Hierarchy + body: the JSON section-detail endpoint, which returns the full
    Title -> Subtitle -> Part -> Chapter -> SubPart -> Article ancestry plus the
    body HTML for every section number, including the ones the list endpoints
    miss.

Pipeline (mirrors ingest_ilga_bulk.py / ingest_ny_bulk.py):
    titles -> title HTML (chapters) -> chapter HTML (section numbers)
    -> section-detail JSON (hierarchy + Body) -> synthetic Node
    -> node_to_payload.node_to_chunks -> JSONL

Reusing node_to_chunks means act_id / point_id / citation / chunking / R2 upload
match the scraper path exactly. The detail hierarchy reproduces the existing
act_id scheme (e.g. 18.2-32 -> STATE_VA_T18.2_C4_A1_S18.2-32); verified against
Qdrant by va_bulk/verify_act_ids.py before any full run.

Run on the scraper box (VA gov sites geo-block the box; proxy egress required):
    docker exec -d -e VAQUILL_USE_PROXY=1 -e VAQUILL_R2_UPLOAD=1 -e VAQUILL_R2_POOL=64 \
      -w /app vaquill-scraper-worker \
      python -u scripts/us_corpus/ingest_va_bulk.py --workers 24 \
        > /app/va_ingest.log 2>&1
    then: embed_and_upsert.py --input .../state_va_statutes.jsonl        (additive)
          embed_and_upsert.py --input .../state_va_statutes.jsonl --reconcile

Cutover note: bulk text differs from scraped text, so every section gets a fresh
content-addressed point_id; --reconcile deletes the superseded ones within the
act_ids this run touched (act_id reproduction is high because the detail endpoint
exposes the same subtitle/part/chapter/article structure the scraper used).
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

from va_bulk import client as C
from va_bulk import parse as P
from va_bulk import walk as W

STATE = "va"


def _default_out() -> Path:
    override = os.environ.get("STATE_CHUNKS_DIR_OVERRIDE")
    base = Path(override) if override else (_SCRAPERS / "data" / "state_chunks")
    return base / f"state_{STATE}_statutes.jsonl"


def enumerate_section_numbers(index_workers: int) -> list[str]:
    """Breadth-first crawl of the vacode HTML for every section number.

    Titles are organized by chapter, by part (UCC titles 8.x), or with nested
    subtitle/part/article levels, so we crawl any container link rather than
    assuming ``chapter``. Each level's pages are fetched in parallel; section
    anchors are collected and any deeper container links are queued for the next
    level, deduped by a visited set.
    """
    print("fetching title list...", flush=True)
    titles = P.dedupe_titles(C.titles())
    print(f"  {len(titles)} distinct titles", flush=True)

    # (title_number, path) so container_links can be title-scoped per page.
    frontier: list[tuple[str, str]] = [
        (t["TitleNumber"].strip(), f"/vacode/title{t['TitleNumber'].strip()}/") for t in titles
    ]
    visited: set[str] = {p for _tn, p in frontier}
    secnums: set[str] = set()
    errs = 0
    level = 0

    def _fetch(item: tuple[str, str]):
        tn, path = item
        try:
            html = C.get_html(path)
        except Exception as exc:
            return (tn, path, None, str(exc)[:120])
        return (tn, path, html, None)

    while frontier:
        level += 1
        pages = len(frontier)
        next_frontier: list[tuple[str, str]] = []
        with ThreadPoolExecutor(max_workers=index_workers) as ex:
            for fut in as_completed(ex.submit(_fetch, it) for it in frontier):
                tn, path, html, err = fut.result()
                if err:
                    errs += 1
                    if errs <= 10:
                        print(f"  [crawl] {path}: {err}", flush=True)
                    continue
                secnums.update(P.section_numbers(html))
                for child in P.container_links(html, tn):
                    if child not in visited:
                        visited.add(child)
                        next_frontier.append((tn, child))
        print(
            f"  level {level}: {pages:,} pages -> sections={len(secnums):,}  "
            f"next={len(next_frontier):,}",
            flush=True,
        )
        frontier = next_frontier

    print(f"  enumerated {len(secnums):,} distinct sections ({errs} fetch errors)", flush=True)
    return sorted(secnums)


def build_records(index_workers: int, body_workers: int, limit: int = 0) -> tuple[list[dict], dict]:
    from src.utils.pydanticModels import Node, NodeText
    from vaquill_pipeline.node_to_payload import node_to_chunks

    secnums = enumerate_section_numbers(index_workers)
    if limit:
        secnums = secnums[:limit]
    year = time.gmtime().tm_year
    print(f"fetching {len(secnums):,} section details with {body_workers} workers...", flush=True)

    stats = {"sections": 0, "chunks": 0, "no_text": 0, "fetch_err": 0, "repealed": 0, "gone": 0}
    out: list[dict] = []

    def _one(secnum: str):
        try:
            detail = C.section_detail(secnum)
        except Exception as exc:
            return ("fetch_err", f"{secnum}: {str(exc)[:120]}", None)
        sec, body_html = W.section_from_detail(detail)
        if sec is None:
            return ("gone", None, None)
        paras = P.body_to_paragraphs(body_html)
        if not paras:
            return ("no_text", None, None)

        status = None
        if "repealed" in sec.section_title.lower() or "[repealed]" in body_html.lower():
            status = "repealed"

        node_id = sec.node_id()
        node = Node(
            id=node_id,
            link=P.vacode_url(sec.section_number),
            node_type="content",
            level_classifier="section",
            number=sec.section_number,
            node_name=sec.section_title or f"Section {sec.section_number}",
            node_text=NodeText(),
            citation=sec.citation(),
            top_level_title=sec.title_number,
            parent="/".join(node_id.split("/")[:-1]),
            status=status,
        )
        for para in paras:
            node.node_text.add_paragraph(text=para)
        try:
            chunks = node_to_chunks(node, STATE, year)
        except Exception as exc:
            return ("fetch_err", f"{secnum}: node_to_chunks: {str(exc)[:120]}", None)
        return ("ok", ("repealed" if status else None), chunks)

    t0 = time.time()
    done = 0
    with ThreadPoolExecutor(max_workers=body_workers) as ex:
        for fut in as_completed(ex.submit(_one, s) for s in secnums):
            kind, tag, chunks = fut.result()
            done += 1
            if done % 1000 == 0:
                rate = done / max(time.time() - t0, 0.001)
                eta = (len(secnums) - done) / rate / 60 if rate else 0
                print(
                    f"  {done:,}/{len(secnums):,}  {rate:.1f}/s  chunks={stats['chunks']:,}  "
                    f"ETA={eta:.1f}m",
                    flush=True,
                )
            if kind == "ok":
                stats["sections"] += 1
                stats["chunks"] += len(chunks)
                if tag == "repealed":
                    stats["repealed"] += 1
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
    ap.add_argument("--workers", type=int, default=24, help="section-detail fetch workers")
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
    print(f"  repealed   : {stats.get('repealed', 0):,}")
    print(f"  chunks     : {stats['chunks']:,}")
    print(f"  no_text    : {stats.get('no_text', 0):,}")
    print(f"  gone       : {stats.get('gone', 0):,}")
    print(f"  fetch_err  : {stats.get('fetch_err', 0):,}")
    print(f"  wrote      : {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
