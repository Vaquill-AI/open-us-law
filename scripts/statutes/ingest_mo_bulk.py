#!/usr/bin/env python3
"""Ingest the full Revised Statutes of Missouri from the official revisor.mo.gov site.

The MO scraper (state_scrapers/.../mo/statutes/scrapeMO.py) reached only ~385
statute sections. The cause was enumeration, not extraction: ``_scrape_chapter``
read only the FIRST ``<table>`` on each chapter page, while revisor.mo.gov groups
a chapter's sections across several tables (one per subchapter heading), so every
section past the first table was dropped. Section text extraction itself was
fine. This replaces that path with a complete walk of the same official pages.

Pipeline (mirrors ingest_va_bulk.py / ingest_ilga_bulk.py):
    Home.aspx (chapters)
      -> OneChapter.aspx?chapter=N  (ALL section rows across ALL tables)
      -> OneSection.aspx?section=S  (body paragraphs + history note)
      -> synthetic Node -> node_to_payload.node_to_chunks -> JSONL

Reusing node_to_chunks means act_id / point_id / citation / breadcrumb / chunking
/ R2 upload match the scraper path exactly. The node id path
``us/mo/statutes/chapter={chapter}/section={section}`` reproduces the existing
act_id scheme (565.020 -> STATE_MO_C565_S565.020); verified against Qdrant by
mo_bulk/verify_act_ids.py before any full run.

Run on the scraper box (revisor.mo.gov geo-blocks the box; proxy egress required):
    docker exec -d -e VAQUILL_USE_PROXY=1 -e VAQUILL_R2_UPLOAD=1 -e VAQUILL_R2_POOL=64 \
      -w /app vaquill-scraper-worker \
      python -u scripts/us_corpus/statutes/ingest_mo_bulk.py --workers 32 \
        > /app/mo_ingest.log 2>&1
    then: embed_and_upsert.py --input .../state_mo_statutes.jsonl            (additive)
          embed_and_upsert.py --input .../state_mo_statutes.jsonl --reconcile

Cutover note: bulk text differs from the old scraped text, so every section gets
a fresh content-addressed point_id; --reconcile deletes the superseded ones
within the act_ids this run touched. act_id reproduction is exact (chapter =
section-number prefix = the chapter page enumerated from), so an act_id-scoped
reconcile is safe and never touches the Missouri constitution stored under the
same state=mo tag.
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

from mo_bulk import client as C
from mo_bulk import parse as P
from mo_bulk.walk import MOSection

STATE = "mo"


def _default_out() -> Path:
    override = os.environ.get("STATE_CHUNKS_DIR_OVERRIDE")
    base = Path(override) if override else (_SCRAPERS / "data" / "state_chunks")
    return base / f"state_{STATE}_statutes.jsonl"


def enumerate_sections(index_workers: int) -> list[MOSection]:
    """Every (chapter, section, title) in RSMo, from Home.aspx + chapter pages.

    Chapters are fetched in parallel; each chapter contributes its full section
    list (all tables). Sections are deduped globally by number (a section can be
    attributed to exactly one chapter = its number prefix).
    """
    print("fetching TOC (Home.aspx)...", flush=True)
    home = C.home()
    chapters = P.chapter_numbers(home)
    print(f"  {len(chapters):,} distinct chapters", flush=True)

    sections: dict[str, MOSection] = {}
    errs = 0
    done = 0
    t0 = time.time()

    def _one(ch: str):
        try:
            html = C.chapter(ch)
        except Exception as exc:
            return (ch, None, str(exc)[:120])
        return (ch, P.chapter_sections(html, ch), None)

    with ThreadPoolExecutor(max_workers=index_workers) as ex:
        for fut in as_completed(ex.submit(_one, c) for c in chapters):
            ch, rows, err = fut.result()
            done += 1
            if err:
                errs += 1
                if errs <= 10:
                    print(f"  [chapter {ch}] {err}", flush=True)
                continue
            for secnum, title in rows:
                if secnum not in sections:
                    sections[secnum] = MOSection(
                        chapter=ch, section_number=secnum, section_title=title
                    )
            if done % 50 == 0:
                rate = done / max(time.time() - t0, 0.001)
                print(
                    f"  chapters {done:,}/{len(chapters):,}  sections={len(sections):,}  "
                    f"{rate:.1f}/s",
                    flush=True,
                )

    print(
        f"  enumerated {len(sections):,} distinct sections "
        f"across {len(chapters):,} chapters ({errs} chapter fetch errors)",
        flush=True,
    )
    return sorted(sections.values(), key=lambda s: (int(s.chapter), s.section_number))


def build_records(index_workers: int, body_workers: int, limit: int = 0) -> tuple[list[dict], dict]:
    from src.utils.pydanticModels import Node, NodeText
    from vaquill_pipeline.node_to_payload import node_to_chunks

    secs = enumerate_sections(index_workers)
    if limit:
        secs = secs[:limit]
    year = time.gmtime().tm_year
    print(f"fetching {len(secs):,} section bodies with {body_workers} workers...", flush=True)

    stats = {"sections": 0, "chunks": 0, "no_text": 0, "fetch_err": 0, "reserved": 0}
    out: list[dict] = []

    def _one(sec: MOSection):
        status = sec.status()
        if status:
            # Reserved/repealed headnote: no live body, no chunk. Count and skip
            # the network fetch (matches the scraper, which stored these as
            # status-only nodes with no text).
            return ("reserved", None)
        # The section page occasionally returns HTTP 200 with an incomplete body
        # (no norm div / no paragraphs) on a transient server or proxy hiccup.
        # get_html only retries non-200 / empty responses, so this content-level
        # miss slips through as a false "no_text" and silently drops a live
        # section. Re-fetch a few times; a genuinely repealed/empty section still
        # yields no paragraphs after the retries and is correctly counted.
        paras: list[str] = []
        history = ""
        last_exc = None
        for attempt in range(4):
            try:
                html = C.section(sec.section_number)
            except Exception as exc:
                last_exc = exc
                continue
            paras, history = P.section_content(html)
            if paras:
                break
            time.sleep(0.5 * (attempt + 1))
        if not paras:
            if last_exc is not None:
                return ("fetch_err", f"{sec.section_number}: {str(last_exc)[:120]}")
            return ("no_text", None)

        node_id = sec.node_id()
        node = Node(
            id=node_id,
            link=sec.source_url(),
            node_type="content",
            level_classifier="section",
            number=sec.section_number,
            node_name=sec.node_name() or f"Section {sec.section_number}",
            node_text=NodeText(),
            citation=sec.citation(),
            top_level_title=sec.chapter,
            parent="/".join(node_id.split("/")[:-1]),
        )
        for para in paras:
            node.node_text.add_paragraph(text=para)
        if history:
            node.node_text.add_paragraph(text=history)
        try:
            chunks = node_to_chunks(node, STATE, year)
        except Exception as exc:
            return ("fetch_err", f"{sec.section_number}: node_to_chunks: {str(exc)[:120]}")
        if not chunks:
            return ("no_text", None)
        return ("ok", chunks)

    t0 = time.time()
    done = 0
    with ThreadPoolExecutor(max_workers=body_workers) as ex:
        for fut in as_completed(ex.submit(_one, s) for s in secs):
            kind, payload = fut.result()
            done += 1
            if done % 1000 == 0:
                rate = done / max(time.time() - t0, 0.001)
                eta = (len(secs) - done) / rate / 60 if rate else 0
                print(
                    f"  {done:,}/{len(secs):,}  {rate:.1f}/s  chunks={stats['chunks']:,}  "
                    f"ETA={eta:.1f}m",
                    flush=True,
                )
            if kind == "ok":
                stats["sections"] += 1
                stats["chunks"] += len(payload)
                out.extend(payload)
            else:
                stats[kind] = stats.get(kind, 0) + 1
                if kind == "fetch_err" and stats[kind] <= 8:
                    print(f"  {payload}", flush=True)
    return out, stats


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", type=Path, default=None)
    ap.add_argument("--limit", type=int, default=0, help="only first N sections (smoke test)")
    ap.add_argument("--workers", type=int, default=32, help="section-body fetch workers")
    ap.add_argument("--index-workers", type=int, default=16, help="chapter enumeration workers")
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
    print(f"  reserved   : {stats.get('reserved', 0):,}")
    print(f"  chunks     : {stats['chunks']:,}")
    print(f"  no_text    : {stats.get('no_text', 0):,}")
    print(f"  fetch_err  : {stats.get('fetch_err', 0):,}")
    print(f"  wrote      : {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
