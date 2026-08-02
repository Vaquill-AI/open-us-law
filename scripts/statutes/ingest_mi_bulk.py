#!/usr/bin/env python3
"""Ingest the full Michigan Compiled Laws from the official legislature.mi.gov XML.

The Michigan scraper (state_scrapers/.../mi/statutes/scrapeMI.py) crawled the
per-section HTML pages and reached only ~195 of the 241 chapters Michigan
publishes and only ~17,881 sections, leaving whole codes section-thin (Chapter
750, the Michigan Penal Code, held just 750.1-750.3 of its ~896 sections; first
degree murder, MCL 750.316, was entirely absent). This replaces it with a
complete pass over Michigan's own MCL XML tree so the entire Code is ingested.

Source: the authoritative current edition is one clean UTF-16 XML file per
chapter at ``legislature.mi.gov/documents/mcl/Chapter {N}.xml`` (an IIS
autoindex), each holding every act and section of that chapter with body text
inline. Unlike the HTML scraper, no per-section fetch is needed: the whole Code
is ~241 chapter files, one request each. See mi_bulk/client.py for the two site
quirks (US geo-block -> proxy required; incomplete TLS chain -> verification
disabled for this host).

Pipeline (mirrors ingest_id_bulk.py / ingest_va_bulk.py):
    chapter index -> chapter XMLs (fetched in parallel) -> parse acts/sections
    (divisions flattened) -> synthetic Node -> node_to_payload.node_to_chunks
    -> JSONL

Reusing node_to_chunks means act_id / point_id / citation / chunking / R2 upload
match the scraper path exactly. The Chapter/Act/Section triple reproduces the
existing act_id scheme (e.g. 750.316 -> STATE_MI_C750_AAct-328-of-1931_S750.316);
verified against Qdrant by mi_bulk/verify_act_ids.py before any full run.

Phase separation: ALL chapters are fetched + parsed first (client does no R2
write), THEN every section is chunked (node_to_chunks does the R2 upload). This
sidesteps the r2_sync crawl/chunk race and keeps the proxy busy on one phase at a
time.

Run on the scraper box (Michigan gov geo-blocks the box; proxy egress required):
    docker exec -d -e VAQUILL_USE_PROXY=1 -e VAQUILL_R2_UPLOAD=1 -e VAQUILL_R2_POOL=64 \
      -w /app vaquill-scraper-worker \
      python -u scripts/us_corpus/statutes/ingest_mi_bulk.py --workers 20 \
        > /app/mi_ingest.log 2>&1
    then: embed_and_upsert.py --input .../state_mi_statutes.jsonl            (additive)
          embed_and_upsert.py --input .../state_mi_statutes.jsonl --reconcile

Cutover note: bulk text differs from scraped text, so every section gets a fresh
content-addressed point_id; act_id-scoped --reconcile deletes the superseded ones
within the act_ids this run touched. state=mi ALSO holds Michigan's Constitution
(307 points, document_type=constitution) under the same state tag, so
--reconcile-state mi is NEVER used here; act_id-scoped reconcile is naturally safe
(constitution act_ids are never in the run).
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

from typing import TYPE_CHECKING

from mi_bulk import client as C
from mi_bulk import parse as P

if TYPE_CHECKING:
    from mi_bulk.walk import MISection

STATE = "mi"


def _default_out() -> Path:
    override = os.environ.get("STATE_CHUNKS_DIR_OVERRIDE")
    base = Path(override) if override else (_SCRAPERS / "data" / "state_chunks")
    return base / f"state_{STATE}_statutes.jsonl"


def enumerate_sections(index_workers: int, limit_chapters: int = 0) -> list[MISection]:
    """Fetch every chapter XML in parallel and parse out all sections.

    Sections are deduped by node_id (chapter/act/section); a defensive dedupe
    guards against the same section appearing under two division paths.
    """
    print("fetching chapter index...", flush=True)
    chapters = C.list_chapters()
    if limit_chapters:
        chapters = chapters[:limit_chapters]
    print(f"  {len(chapters)} chapters listed", flush=True)

    sections: dict[str, MISection] = {}
    errs = 0
    act_less_total = 0
    done = 0

    def _one(name: str):
        try:
            xml_text = C.get_chapter_xml(name)
        except Exception as exc:
            return (name, None, 0, str(exc)[:140])
        try:
            secs, act_less = P.parse_chapter(xml_text, name)
        except Exception as exc:
            return (name, None, 0, f"parse: {str(exc)[:140]}")
        return (name, secs, act_less, None)

    t0 = time.time()
    with ThreadPoolExecutor(max_workers=index_workers) as ex:
        for fut in as_completed(ex.submit(_one, c) for c in chapters):
            name, secs, act_less, err = fut.result()
            done += 1
            if err:
                errs += 1
                if errs <= 15:
                    print(f"  [chapter {name}] {err}", flush=True)
                continue
            act_less_total += act_less
            for s in secs:
                sections[s.node_id()] = s
            if done % 40 == 0:
                rate = done / max(time.time() - t0, 0.001)
                print(
                    f"  {done}/{len(chapters)} chapters  {rate:.1f}/s  sections={len(sections):,}",
                    flush=True,
                )

    out = sorted(sections.values(), key=lambda s: s.node_id())
    print(
        f"  enumerated {len(out):,} distinct sections across {len(chapters)} chapters "
        f"({errs} chapter errors, {act_less_total} act-less sections skipped)",
        flush=True,
    )
    return out


def build_records(
    index_workers: int, chunk_workers: int, limit_chapters: int = 0
) -> tuple[list[dict], dict]:
    from src.utils.pydanticModels import Node, NodeText
    from vaquill_pipeline.node_to_payload import node_to_chunks

    sections = enumerate_sections(index_workers, limit_chapters=limit_chapters)
    year = time.gmtime().tm_year
    print(f"chunking {len(sections):,} sections with {chunk_workers} workers...", flush=True)

    stats = {"sections": 0, "chunks": 0, "no_text": 0, "err": 0}
    out: list[dict] = []

    def _one(sec: MISection):
        node = Node(
            id=sec.node_id(),
            link=sec.url(),
            node_type="content",
            level_classifier="section",
            number=sec.section_number,
            node_name=sec.node_name(),
            node_text=NodeText(),
            citation=sec.citation(),
            top_level_title=sec.chapter_number,
            parent=sec.parent_id(),
            status=("repealed" if sec.repealed else None),
        )
        for para in sec.paragraphs:
            node.node_text.add_paragraph(text=para)
        try:
            chunks = node_to_chunks(node, STATE, year)
        except Exception as exc:
            return ("err", f"{sec.section_number}: node_to_chunks: {str(exc)[:120]}", None)
        if not chunks:
            return ("no_text", None, None)
        return ("ok", None, chunks)

    t0 = time.time()
    done = 0
    with ThreadPoolExecutor(max_workers=chunk_workers) as ex:
        for fut in as_completed(ex.submit(_one, s) for s in sections):
            kind, tag, chunks = fut.result()
            done += 1
            if done % 2000 == 0:
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
                if kind == "err" and stats[kind] <= 8:
                    print(f"  {tag}", flush=True)
    return out, stats


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", type=Path, default=None)
    ap.add_argument(
        "--limit-chapters", type=int, default=0, help="only first N chapters (smoke test)"
    )
    ap.add_argument(
        "--workers", type=int, default=16, help="chapter-XML fetch + section chunk workers"
    )
    ap.add_argument(
        "--index-workers", type=int, default=0, help="chapter fetch workers (default: --workers)"
    )
    args = ap.parse_args()

    out_path = args.out or _default_out()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    index_workers = args.index_workers or args.workers

    records, stats = build_records(index_workers, args.workers, limit_chapters=args.limit_chapters)

    # Fresh file per run so --reconcile sees exactly this run's corpus.
    with open(out_path, "w", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")

    print("\n=== done ===")
    print(f"  sections   : {stats['sections']:,}")
    print(f"  chunks     : {stats['chunks']:,}")
    print(f"  no_text    : {stats.get('no_text', 0):,}")
    print(f"  err        : {stats.get('err', 0):,}")
    print(f"  wrote      : {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
