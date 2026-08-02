#!/usr/bin/env python3
"""Ingest the full Iowa Code from the official legis.iowa.gov per-chapter XML.

The Iowa scraper (state_scrapers/.../ia/statutes/scrapeIA.py) fetched one RTF per
section over a latency-bound crawl and only ever completed 6 of Iowa's 16 titles
(I, IV, V, VI, VII, VIII), leaving whole codes missing, most importantly Title
XVI (Criminal Law and Procedure, e.g. chapter 707 murder) plus Business Entities,
Property, Judicial Procedures, Natural Resources, Elections, Local Government,
Financial Resources, and Commerce. This replaces it with the Legislature's own
structured bulk surface so the entire Code is ingested completely.

Two-surface source (each used for what it does reliably):
  - Enumeration: the chapter-listing HTML per Title
    (/law/iowaCode/chapters?title={roman}&year={year}) supplies the Title ->
    Chapter mapping (the Title is NOT in the per-chapter XML) and the RESERVED
    flag so empty chapters are skipped.
  - Hierarchy + body: the per-chapter slim XML
    (/docs/publications/ICC/{year}/attachments/{chapter}_slim.xml) carries the
    chapter's section list, per-section headnote, nested body paragraphs, and
    amendment history for every section in one structured document.

Pipeline (mirrors ingest_va_bulk.py / ingest_ilga_bulk.py):
    titles -> per-Title chapter HTML (chapters) -> per-chapter slim XML
    (sections + hierarchy + body) -> synthetic Node
    -> node_to_payload.node_to_chunks -> JSONL

Reusing node_to_chunks means act_id / point_id / citation / chunking / R2 upload
match the scraper path exactly. Iowa is a flat Title -> Chapter -> Section, so the
act_id scheme reproduces exactly (707.2 -> STATE_IA_TXVI_C707_S707.2); verified
against Qdrant by ia_bulk/verify_act_ids.py before any full run.

Run on the scraper box (Iowa gov geo-blocks the box; proxy egress required):
    docker exec -d -e VAQUILL_USE_PROXY=1 -e VAQUILL_R2_UPLOAD=1 -e VAQUILL_R2_POOL=64 \
      -w /app vaquill-scraper-worker \
      python -u scripts/us_corpus/ingest_ia_bulk.py --workers 24 \
        > /app/ia_ingest.log 2>&1
    then: embed_and_upsert.py --input .../state_ia_statutes.jsonl        (additive)
          embed_and_upsert.py --input .../state_ia_statutes.jsonl --reconcile

Cutover note: bulk text differs from scraped text, so every section gets a fresh
content-addressed point_id; --reconcile deletes the superseded ones within the
act_ids this run touched (act_id reproduction is ~100% because Iowa's hierarchy
is a flat Title/Chapter/Section that the XML + enumeration reproduce exactly).
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

from ia_bulk import client as C
from ia_bulk import parse as P

STATE = "ia"
DEFAULT_YEAR = 2026


def _default_out() -> Path:
    override = os.environ.get("STATE_CHUNKS_DIR_OVERRIDE")
    base = Path(override) if override else (_SCRAPERS / "data" / "state_chunks")
    return base / f"state_{STATE}_statutes.jsonl"


def enumerate_chapters(year: int, index_workers: int) -> list[tuple[str, str]]:
    """Every (title_roman, chapter) pair in the Code, reserved chapters excluded.

    One HTML fetch for the Title list, then one chapter-listing fetch per Title
    (~16 requests), fanned out over ``index_workers``.
    """
    print("fetching Iowa Code title list...", flush=True)
    titles = P.titles_from_toc(C.title_listing_html(year))
    print(f"  {len(titles)} titles: {titles}", flush=True)
    if not titles:
        raise RuntimeError("no titles enumerated from the Iowa Code TOC")

    pairs: list[tuple[str, str]] = []
    reserved = 0

    def _one(title_roman: str):
        html = C.chapter_listing_html(title_roman, year)
        return title_roman, P.chapters_from_listing(html)

    with ThreadPoolExecutor(max_workers=index_workers) as ex:
        for fut in as_completed(ex.submit(_one, t) for t in titles):
            title_roman, chapters = fut.result()
            for ch, is_reserved in chapters:
                if is_reserved:
                    reserved += 1
                    continue
                pairs.append((title_roman, ch))

    pairs.sort(key=lambda tc: (tc[0], tc[1]))
    print(
        f"  enumerated {len(pairs):,} live chapters ({reserved:,} reserved skipped)",
        flush=True,
    )
    return pairs


def build_records(year: int, index_workers: int, body_workers: int, limit: int = 0):
    from src.utils.pydanticModels import Node, NodeText
    from vaquill_pipeline.node_to_payload import node_to_chunks

    pairs = enumerate_chapters(year, index_workers)
    if limit:
        pairs = pairs[:limit]
    print(f"fetching {len(pairs):,} chapter XMLs with {body_workers} workers...", flush=True)

    stats = {
        "chapters": 0,
        "sections": 0,
        "chunks": 0,
        "no_text": 0,
        "reserved": 0,
        "repealed": 0,
        "fetch_err": 0,
        "empty_xml": 0,
    }
    out: list[dict] = []

    def _one(pair: tuple[str, str]):
        title_roman, chapter = pair
        try:
            xml_bytes = C.chapter_xml(chapter, year)
        except Exception as exc:
            return ("fetch_err", f"{title_roman}/{chapter}: {str(exc)[:120]}", None)
        try:
            sections = P.parse_chapter_sections(xml_bytes, title_roman, chapter)
        except Exception as exc:
            return ("fetch_err", f"{title_roman}/{chapter}: parse: {str(exc)[:120]}", None)
        if not sections:
            return ("empty_xml", None, None)

        chapter_chunks: list[dict] = []
        local = {"sections": 0, "chunks": 0, "no_text": 0, "reserved": 0, "repealed": 0}
        for sec, paras, status in sections:
            if status == "reserved":
                local["reserved"] += 1
            if not paras:
                local["no_text"] += 1
                continue
            node_id = sec.node_id()
            node = Node(
                id=node_id,
                link=C.section_rtf_url(sec.section_number, year),
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
                return (
                    "fetch_err",
                    f"{sec.section_number}: node_to_chunks: {str(exc)[:120]}",
                    None,
                )
            if not chunks:
                local["no_text"] += 1
                continue
            local["sections"] += 1
            local["chunks"] += len(chunks)
            if status == "repealed":
                local["repealed"] += 1
            chapter_chunks.extend(chunks)
        return ("ok", local, chapter_chunks)

    t0 = time.time()
    done = 0
    total = len(pairs)
    with ThreadPoolExecutor(max_workers=body_workers) as ex:
        for fut in as_completed(ex.submit(_one, p) for p in pairs):
            kind, payload, chunks = fut.result()
            done += 1
            if kind == "ok":
                stats["chapters"] += 1
                for k, v in payload.items():
                    stats[k] = stats.get(k, 0) + v
                out.extend(chunks)
            else:
                stats[kind] = stats.get(kind, 0) + 1
                if kind == "fetch_err" and stats[kind] <= 10:
                    print(f"  {payload}", flush=True)
            if done % 200 == 0:
                rate = done / max(time.time() - t0, 0.001)
                eta = (total - done) / rate / 60 if rate else 0
                print(
                    f"  {done:,}/{total:,} chapters  {rate:.1f}/s  "
                    f"sections={stats['sections']:,} chunks={stats['chunks']:,}  ETA={eta:.1f}m",
                    flush=True,
                )
    return out, stats


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", type=Path, default=None)
    ap.add_argument("--year", type=int, default=DEFAULT_YEAR)
    ap.add_argument("--limit", type=int, default=0, help="only first N chapters (smoke test)")
    ap.add_argument("--workers", type=int, default=24, help="chapter-XML fetch workers")
    ap.add_argument("--index-workers", type=int, default=8, help="HTML enumeration workers")
    args = ap.parse_args()

    out_path = args.out or _default_out()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    records, stats = build_records(args.year, args.index_workers, args.workers, limit=args.limit)

    # Fresh file per run so --reconcile sees exactly this run's corpus.
    with open(out_path, "w", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")

    print("\n=== done ===")
    print(f"  chapters   : {stats['chapters']:,}")
    print(f"  sections   : {stats['sections']:,}")
    print(f"  repealed   : {stats.get('repealed', 0):,}")
    print(f"  reserved   : {stats.get('reserved', 0):,}")
    print(f"  chunks     : {stats['chunks']:,}")
    print(f"  no_text    : {stats.get('no_text', 0):,}")
    print(f"  empty_xml  : {stats.get('empty_xml', 0):,}")
    print(f"  fetch_err  : {stats.get('fetch_err', 0):,}")
    print(f"  wrote      : {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
