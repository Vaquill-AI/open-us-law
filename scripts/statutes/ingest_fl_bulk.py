#!/usr/bin/env python3
"""Ingest the 2025 Florida Statutes from leg.state.fl.us (Online Sunshine).

Coverage finding (2026-07-19): FL was NOT section-thin. A full crawl of all 638
chapters found 24,866 current sections; Qdrant already held 24,376 (~98%), with 0
sections missing that the live code still publishes. The real gap was 490 sections,
almost all in the K-20 Education Code (Title XLVIII, chapters 1001-1013) plus
substance-abuse ch 397 and family-law ch 88. (The 2026-07-19 completeness audit's
"~35-45k real sections / 54-70%" was an over-estimate; no state publishes an
official aggregate section count.) This ingest replaces the fragile per-section
Selenium scraper (scrapeFL.py) with a complete, full-text source.

Two modes:
  - default            : full re-ingest of all 24,866 sections (currency refresh;
                         pair with an act_id-scoped ``--reconcile`` on embed).
  - ``--only-new``     : emit only sections whose act_id is not already in Qdrant
                         (additive gap-fill, no reconcile). Used for the 2026-07-19
                         run: 490 net-new sections, embedded additively.

Source note: flsenate.gov publishes the same statutes but blocks the scraper box
(direct egress times out; the Webshare US proxy is 502'd). leg.state.fl.us serves
the box DIRECTLY and carries the SAME statute text with the SAME HTML classes, so
it is the source here.

Source model:
  - TOC (``Mode=View Statutes``) -> the 49 Titles (roman numerals).
  - Title index (``Display_Index&Title_Request=<roman>``) -> the chapters that
    belong to each Title (so the act_id Title level is taken from the source,
    never guessed).
  - Chapter page (``Display_Statute&URL=<band>/<pad>/<pad>.html``) -> the COMPLETE
    chapter in one fetch: ``div.Section`` blocks with number / catchline / body /
    history, grouped under ``div.Part`` for part-bearing chapters. Not windowed
    (Chapter 627 -> all 628 sections across 22 parts, matching flsenate.gov's
    independent count), so one fetch per chapter is authoritative.

Each section -> synthetic ``Node`` -> ``node_to_payload.node_to_chunks`` -> JSONL,
so act_id / point_id / citation / breadcrumb / chunking / R2 section-text upload
match the scraper path byte-for-byte. act_id reproduces the existing scheme
exactly, INCLUDING the Title and Part roman levels:
    STATE_FL_TXLVI_C782_S782.04              (Ch 782 homicide)
    STATE_FL_TXXXVII_C627_PII_S627.40952     (Ch 627 Part II insurance)
verified against Qdrant by fl_bulk/verify_act_ids.py before any full run. High
reproduction means an act_id-scoped ``--reconcile`` is surgical: it retires only
superseded points within the touched act_ids and never touches the 188 Florida
constitution points (document_type=constitution) that share the ``state=fl`` tag.
Do NOT use ``--reconcile-state fl`` (the flat scheme in the brief would not match
existing act_ids and would duplicate the state).

Run on the scraper box (leg.state.fl.us serves the box directly; flsenate.gov
blocks it. Keep concurrency modest -- the site throttles bursts):
    # additive gap-fill (what the 2026-07-19 run used):
    docker exec -d -e VAQUILL_R2_UPLOAD=1 -e VAQUILL_R2_POOL=64 \
      -w /app vaquill-scraper-worker \
      python -u scripts/us_corpus/statutes/ingest_fl_bulk.py --workers 12 --only-new \
        > /app/fl_ingest.log 2>&1
    then: lib/embed_and_upsert.py --input .../state_fl_statutes.jsonl            (additive)

    # full currency refresh (if ever wanted): drop --only-new, then embed with
    # --reconcile (act_id-scoped; never --reconcile-state fl -- see note above).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_SCRAPERS = _HERE.parent / "state_scrapers"
sys.path.insert(0, str(_HERE))
sys.path.insert(0, str(_SCRAPERS))

from fl_bulk import client as C
from fl_bulk import parse as P
from fl_bulk.walk import FLSection

STATE = "fl"


def _default_out() -> Path:
    override = os.environ.get("STATE_CHUNKS_DIR_OVERRIDE")
    base = Path(override) if override else (_SCRAPERS / "data" / "state_chunks")
    return base / f"state_{STATE}_statutes.jsonl"


def _seckey(sec: str):
    """Natural-ish ordering for FL section numbers ('782.4' < '782.04' handled by
    splitting into numeric/text runs)."""
    return [int(t) if t.isdigit() else t for t in re.split(r"(\d+)", sec) if t]


def enumerate_chapters(index_workers: int) -> list[tuple[str, str]]:
    """All (title_roman, chapter_number) pairs from the TOC + Title index pages.

    The main TOC gives the 49 Title romans; each Title index page lists the
    chapters that belong to that title. A chapter that is repealed/reserved is
    simply not listed, so nothing needs a reserved filter here.
    """
    print("fetching statutes TOC...", flush=True)
    romans = P.title_romans(C.toc_html())
    print(f"  {len(romans)} titles", flush=True)
    if not romans:
        raise RuntimeError("no titles parsed from TOC; source structure changed")

    out: list[tuple[str, str]] = []

    def _one(roman: str):
        try:
            html = C.title_index_html(roman)
        except Exception as exc:
            return (roman, None, str(exc)[:140])
        return (roman, P.title_chapters(html), None)

    with ThreadPoolExecutor(max_workers=index_workers) as ex:
        for fut in as_completed(ex.submit(_one, r) for r in romans):
            roman, chs, err = fut.result()
            if err:
                print(f"  title {roman}: enumerate error: {err}", flush=True)
                continue
            for ch in chs:
                out.append((roman, ch))

    # Dedupe (a chapter belongs to exactly one title) and order by chapter number
    # for stable logs.
    seen: set[str] = set()
    uniq: list[tuple[str, str]] = []
    for roman, ch in sorted(out, key=lambda x: _seckey(x[1])):
        if ch in seen:
            continue
        seen.add(ch)
        uniq.append((roman, ch))
    print(f"  enumerated {len(uniq)} chapters", flush=True)
    return uniq


def _act_id_for_record(rec: dict) -> str:
    """The act_id a record will chunk to (for --only-new filtering)."""
    from src.utils.pydanticModels import Node, NodeText
    from vaquill_pipeline.node_to_payload import _act_id

    obj = FLSection(
        title_roman=rec["title_roman"],
        chapter=rec["chapter"],
        section_number=rec["number"],
        part_roman=rec.get("part_roman"),
    )
    node_id = obj.node_id()
    node = Node(
        id=node_id,
        node_type="content",
        level_classifier="section",
        number=rec["number"],
        node_name=rec.get("catchline") or f"Section {rec['number']}",
        node_text=NodeText(),
        citation=obj.citation(),
        top_level_title=rec["chapter"],
        parent="/".join(node_id.split("/")[:-1]),
    )
    return _act_id(node, STATE)


def existing_statute_act_ids() -> set[str]:
    """All existing FL STATUTE act_ids from Qdrant (excludes constitution)."""
    from qdrant_client import QdrantClient
    from qdrant_client.models import FieldCondition, Filter, MatchValue

    qc = QdrantClient(
        url=os.environ["QDRANT_URL"], api_key=os.environ.get("QDRANT_API_KEY"), timeout=180
    )
    out: set[str] = set()
    off = None
    while True:
        pts, off = qc.scroll(
            collection_name="statutes_us",
            scroll_filter=Filter(
                must=[
                    FieldCondition(key="state", match=MatchValue(value=STATE)),
                    FieldCondition(key="document_type", match=MatchValue(value="statute")),
                ]
            ),
            limit=6000,
            with_payload=["act_id"],
            with_vectors=False,
            offset=off,
        )
        for p in pts:
            a = (p.payload or {}).get("act_id")
            if a:
                out.add(a)
        if off is None:
            break
    return out


def _build_chunks_for_section(rec: dict, year: int) -> list[dict]:
    from src.utils.pydanticModels import Node, NodeText
    from vaquill_pipeline.node_to_payload import node_to_chunks

    obj = FLSection(
        title_roman=rec["title_roman"],
        chapter=rec["chapter"],
        section_number=rec["number"],
        section_title=rec["catchline"],
        part_roman=rec.get("part_roman"),
        status=rec.get("status"),
    )
    node_id = obj.node_id()
    node = Node(
        id=node_id,
        link=C.section_url(rec["chapter"], rec["number"]),
        node_type="content",
        level_classifier="section",
        number=rec["number"],
        node_name=(f"{rec['number']} {rec['catchline']}".strip() or f"Section {rec['number']}"),
        node_text=NodeText(),
        citation=obj.citation(),
        top_level_title=rec["chapter"],
        parent="/".join(node_id.split("/")[:-1]),
        status=rec.get("status"),
    )
    paras = list(rec.get("paragraphs") or [])
    hist = (rec.get("history") or "").strip()
    if hist:
        paras.append(hist if hist.lower().startswith("history") else f"History: {hist}")
    for para in paras:
        node.node_text.add_paragraph(text=para)
    return node_to_chunks(node, STATE, year)


def build_records(
    index_workers: int, workers: int, limit: int = 0, only_new: bool = False
) -> tuple[list[dict], dict]:
    chapters = enumerate_chapters(index_workers)
    if limit:
        chapters = chapters[:limit]
        print(f"  smoke-test: limiting to first {len(chapters)} chapters", flush=True)

    have_act_ids: set[str] = set()
    if only_new:
        have_act_ids = existing_statute_act_ids()
        print(
            f"  --only-new: {len(have_act_ids):,} existing FL statute act_ids in Qdrant", flush=True
        )

    year = time.gmtime().tm_year
    stats = {
        "chapters": 0,
        "chapter_err": 0,
        "empty_chapters": 0,
        "already_present": 0,
        "sections": 0,
        "reserved": 0,
        "empty_sections": 0,
        "chunks": 0,
    }

    # PHASE 1 -- fetch + parse every chapter page (network-bound; no R2 writes
    # here, so _stem_index stays empty and the r2_sync race cannot occur). Section
    # records are collected in memory and chunked in phase 2.
    pending: list[dict] = []
    t0 = time.time()
    done = 0
    print("phase 1: fetching chapter pages...", flush=True)

    def _crawl(item: tuple[str, str]):
        roman, chapter = item
        try:
            html = C.chapter_html(chapter)
        except Exception as exc:
            return (chapter, None, str(exc)[:140])
        recs = P.parse_chapter_all(html, chapter)
        for r in recs:
            r["title_roman"] = roman
        return (chapter, recs, None)

    with ThreadPoolExecutor(max_workers=workers) as ex:
        for fut in as_completed(ex.submit(_crawl, it) for it in chapters):
            done += 1
            chapter, recs, err = fut.result()
            if err:
                stats["chapter_err"] += 1
                print(f"  chapter {chapter}: fetch error: {err}", flush=True)
                continue
            if not recs:
                stats["empty_chapters"] += 1
                continue
            stats["chapters"] += 1
            pending.extend(recs)
            if done % 50 == 0 or done == len(chapters):
                rate = done / max(time.time() - t0, 0.001)
                eta = (len(chapters) - done) / rate / 60 if rate else 0
                print(
                    f"  [{done}/{len(chapters)}] sections queued={len(pending):,} ETA={eta:.1f}m",
                    flush=True,
                )

    # --only-new: keep only sections whose act_id is NOT already in Qdrant, so the
    # additive embed adds exactly the net-new sections (and R2 uploads only run for
    # them in phase 2). Existing sections are left untouched (no reconcile needed).
    if only_new:
        before = len(pending)
        pending = [r for r in pending if _act_id_for_record(r) not in have_act_ids]
        stats["already_present"] = before - len(pending)
        print(
            f"  --only-new: {len(pending):,} net-new sections "
            f"({stats['already_present']:,} already present, skipped)",
            flush=True,
        )

    # PHASE 2 -- chunk every section (node_to_chunks uploads the section text to
    # R2). Reserved sections carry no body and drop out (chunks == []).
    print(f"phase 2: chunking {len(pending):,} sections...", flush=True)
    t1 = time.time()
    cdone = 0

    def _chunk(rec: dict) -> tuple[str | None, list[dict]]:
        if rec.get("status"):
            return ("reserved", [])
        try:
            return (None, _build_chunks_for_section(rec, year))
        except Exception as exc:
            print(f"  {rec.get('number')}: node_to_chunks error: {str(exc)[:140]}", flush=True)
            return ("err", [])

    out: list[dict] = []
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for tag, chunks in ex.map(_chunk, pending):
            cdone += 1
            if tag == "reserved":
                stats["reserved"] += 1
            elif not chunks:
                stats["empty_sections"] += 1
            else:
                stats["sections"] += 1
                stats["chunks"] += len(chunks)
                out.extend(chunks)
            if cdone % 5000 == 0:
                rate = cdone / max(time.time() - t1, 0.001)
                print(
                    f"  [{cdone:,}/{len(pending):,}] chunked | chunks={stats['chunks']:,} "
                    f"{rate:.0f}/s",
                    flush=True,
                )
    return out, stats


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", type=Path, default=None)
    ap.add_argument("--workers", type=int, default=20, help="chapter fetch + chunk workers")
    ap.add_argument("--index-workers", type=int, default=12, help="Title-page enumeration workers")
    ap.add_argument("--limit", type=int, default=0, help="only first N chapters (smoke test)")
    ap.add_argument(
        "--only-new",
        action="store_true",
        help="emit only sections whose act_id is not already in Qdrant (additive gap-fill)",
    )
    args = ap.parse_args()

    out_path = args.out or _default_out()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    records, stats = build_records(
        args.index_workers, args.workers, limit=args.limit, only_new=args.only_new
    )

    # Fresh file per run so --reconcile sees exactly this run's corpus.
    with open(out_path, "w", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")

    print("\n=== done ===")
    print(f"  chapters ok  : {stats['chapters']:,}")
    print(f"  chapter err  : {stats['chapter_err']:,}")
    print(f"  empty chaps  : {stats['empty_chapters']:,}")
    print(f"  already present: {stats.get('already_present', 0):,}")
    print(f"  sections     : {stats['sections']:,}")
    print(f"  reserved     : {stats['reserved']:,}")
    print(f"  empty secs   : {stats['empty_sections']:,}")
    print(f"  chunks       : {stats['chunks']:,}")
    print(f"  wrote        : {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
