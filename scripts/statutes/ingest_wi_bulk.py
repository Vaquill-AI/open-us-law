#!/usr/bin/env python3
"""Ingest the full Wisconsin Statutes from the official docs.legis.wisconsin.gov.

Wisconsin was near-empty in ``statutes_us`` (812 sections across 244 chapters).
Worse, the old per-chapter scraper read only the ``qsatxt_1sect`` wrapper div,
which for any section with subsections holds just the number + title -- so even
the 812 it captured were title-only for structured sections. This replaces it
with a complete, full-text ingest from the Commonwealth's own HTML viewer.

Source model (the docs.legis viewer is windowed, which drove the design):
  - The TOC page lists all ~470 chapters (``/document/statutes/<N>``).
  - A chapter/section page renders only a small CENTERED window of consecutive
    sections (fully, with subsection bodies) plus a wider ~45-entry table-of-
    contents anchor list. Neither a chapter page nor the JSON list gives the
    whole chapter, so completeness cannot come from one fetch.
  - A section's body is the FLAT run of ``div.qsatxt_*`` blocks sharing its
    ``data-section`` (wrapper + subsections + paragraphs + subdivisions); case
    annotations live in separate ``qsnote_*`` siblings and are dropped.

Per-chapter window-harvest crawl (self-enumerating + complete):
    seed from the chapter page (anchors + any rendered bodies), then repeatedly
    fetch the lowest section that still lacks a trusted body. Each fetch yields a
    centered window: every section it renders except the possibly-truncated
    forward-edge one is trusted (bounded by the next section's start); the
    requested anchor is always complete. Each page's TOC anchors grow the known
    set, so the frontier walks the whole chapter and terminates when every known
    section has a trusted body. Chapters run in parallel; the crawl within a
    chapter is sequential.

Each section -> synthetic ``Node`` -> ``node_to_payload.node_to_chunks`` -> JSONL,
so act_id / point_id / citation / breadcrumb / chunking / R2 upload match the
scraper path byte-for-byte. act_id is ``STATE_WI_C<chapter>_S<section>`` (100% of
the existing WI statute act_ids are in this form -> an act_id-scoped
``--reconcile`` is safe; it never touches the Wis. Admin. Code regulations or the
constitution that share the ``state=wi`` tag).

Run on the scraper box (WI gov geo-blocks the box; proxy egress required):
    docker exec -d -e VAQUILL_USE_PROXY=1 -e VAQUILL_R2_UPLOAD=1 -e VAQUILL_R2_POOL=64 \
      -w /app vaquill-scraper-worker \
      python -u scripts/us_corpus/statutes/ingest_wi_bulk.py --workers 24 \
        > /app/wi_ingest.log 2>&1
    then: lib/embed_and_upsert.py --input .../state_wi_statutes.jsonl            (additive)
          lib/embed_and_upsert.py --input .../state_wi_statutes.jsonl --reconcile
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

from wi_bulk import client as C
from wi_bulk import parse as P
from wi_bulk.walk import WISection

STATE = "wi"
BASE = "https://docs.legis.wisconsin.gov"
# Safety backstop: no real WI chapter needs anywhere near this many fetches.
_MAX_FETCHES_PER_CHAPTER = 800
# Retries for a section that keeps failing to fetch (transient proxy errors)
# before we give up on it. A 404 (renumbered/repealed) gives up on attempt 1.
_MAX_SECTION_ATTEMPTS = 3
_EMPTY_SECTION = {"title": "", "paragraphs": [], "history": "", "status": None, "_empty": True}


def _default_out() -> Path:
    override = os.environ.get("STATE_CHUNKS_DIR_OVERRIDE")
    base = Path(override) if override else (_SCRAPERS / "data" / "state_chunks")
    return base / f"state_{STATE}_statutes.jsonl"


def _seckey(sec: str):
    """Natural-ish ordering key for Wisconsin section numbers ('940.9' < '940.10')."""
    return [int(t) if t.isdigit() else t for t in re.split(r"(\d+)", sec) if t]


def enumerate_chapters() -> list[str]:
    """All chapter numbers from the statutes TOC (``/document/statutes/<N>``)."""
    html = C.toc_html()
    chapters: set[str] = set()
    for m in re.finditer(r"/document/statutes/(\d+)(?:[\"'/#?]|$)", html):
        chapters.add(m.group(1))
    return sorted(chapters, key=int)


def _absorb(ordered: list[str], secs: dict[str, dict], trusted: dict[str, dict], requested):
    """Trust the fully-rendered section bodies from one page.

    Every section on the page is bounded (hence complete) except the forward-edge
    last one, which may be truncated by the render window -- leave it pending
    unless it is the requested anchor (which the viewer always renders in full).
    """
    n = len(ordered)
    for i, sec in enumerate(ordered):
        if sec in trusted and not trusted[sec].get("_empty"):
            continue
        is_last = i == n - 1
        if is_last and sec != requested and n > 1:
            continue
        d = secs[sec]
        if not d["paragraphs"] and not d["title"]:
            continue
        trusted[sec] = d


def _pdf_toc_seed(chapter: str) -> set[str]:
    """Complete current-section list for a chapter from its official PDF front TOC.

    The HTML viewer's sliding anchor window (~7 entries) is too small to discover
    every section by crawling alone when a chapter has large sections with big
    numbering gaps (e.g. ch 20 appropriations: 20.575 -> 20.585 -> 20.625). The
    per-chapter PDF's front table of contents lists every current section, so it
    is the completeness backbone; the crawl then just fetches each section's clean
    HTML body. Returns an empty set on any PDF failure (falls back to anchors).
    """
    try:
        import fitz

        doc = fitz.open(stream=C.chapter_pdf_bytes(chapter), filetype="pdf")
        text = "\n".join(doc[i].get_text() for i in range(doc.page_count))
        return P.pdf_front_toc_sections(text, chapter)
    except Exception:
        return set()


def crawl_chapter(chapter: str) -> tuple[str, dict[str, dict], int]:
    """Window-harvest crawl of one chapter. Returns (chapter, sections, fetches)."""
    known: set[str] = set()
    trusted: dict[str, dict] = {}
    fetches = 0

    try:
        html = C.chapter_html(chapter)
        fetches += 1
    except Exception:
        return chapter, {}, fetches
    known |= P.section_anchors(html, chapter)
    ordered, secs = P.parse_page(html, chapter)
    _absorb(ordered, secs, trusted, requested=None)
    known |= set(ordered)

    # Seed the complete current-section list from the official chapter PDF's front
    # TOC so no section is missed on large-section chapters (the HTML anchor
    # window alone under-discovers them). HTML anchors above still union in, so a
    # section either source names is fetched.
    known |= _pdf_toc_seed(chapter)
    fetches += 1

    # Per-section failure counter. A section that 404s is a renumbered/repealed
    # number the viewer no longer serves (drop it immediately). A transient error
    # (proxy IncompleteRead / timeout) is retried up to _MAX_SECTION_ATTEMPTS so a
    # live section is never silently dropped on a hiccup.
    attempts: dict[str, int] = {}
    while fetches < _MAX_FETCHES_PER_CHAPTER:
        pending = sorted((s for s in known if s not in trusted), key=_seckey)
        if not pending:
            break
        s = pending[0]
        try:
            html = C.section_html(s)
            fetches += 1
        except Exception as exc:
            attempts[s] = attempts.get(s, 0) + 1
            if "404" in str(exc) or attempts[s] >= _MAX_SECTION_ATTEMPTS:
                # Permanently gone (renumbered) or repeatedly unreachable: mark
                # empty so the frontier advances; dropped from output below.
                trusted[s] = _EMPTY_SECTION
            continue
        known |= P.section_anchors(html, chapter)
        ordered, secs = P.parse_page(html, chapter)
        _absorb(ordered, secs, trusted, requested=s)
        known |= set(ordered)
        if s not in trusted:
            # The anchor itself did not render (viewer glitch / structure-only
            # heading). Mark empty to guarantee forward progress.
            trusted[s] = _EMPTY_SECTION

    return chapter, {s: d for s, d in trusted.items() if not d.get("_empty")}, fetches


def _build_chunks_for_section(sec: str, data: dict, year: int) -> list[dict]:
    from src.utils.pydanticModels import Node, NodeText
    from vaquill_pipeline.node_to_payload import node_to_chunks

    obj = WISection(section_number=sec, section_title=data["title"])
    node_id = obj.node_id()
    node = Node(
        id=node_id,
        link=C.section_url(sec),
        node_type="content",
        level_classifier="section",
        number=sec,
        node_name=data["title"] or f"Section {sec}",
        node_text=NodeText(),
        citation=obj.citation(),
        top_level_title=obj.chapter,
        parent="/".join(node_id.split("/")[:-1]),
        status=data["status"],
    )
    # Body paragraphs, then the section history as a trailing paragraph (matches
    # the VA/IL ingests, which embed the history/credit line as body text so it
    # is searchable; node_to_chunks reads node_text only).
    paras = list(data["paragraphs"])
    hist = data["history"].strip()
    if hist:
        paras.append(hist if hist.lower().startswith("history") else f"History: {hist}")
    for para in paras:
        node.node_text.add_paragraph(text=para)
    return node_to_chunks(node, STATE, year)


def build_records(workers: int, limit_chapters: int = 0) -> tuple[list[dict], dict]:
    chapters = enumerate_chapters()
    print(f"enumerated {len(chapters)} chapters", flush=True)
    if limit_chapters:
        chapters = chapters[:limit_chapters]
        print(f"  smoke-test: limiting to first {len(chapters)} chapters", flush=True)

    year = time.gmtime().tm_year
    stats = {
        "chapters": 0,
        "sections": 0,
        "chunks": 0,
        "empty_sections": 0,
        "fetches": 0,
        "chapter_err": 0,
    }
    out: list[dict] = []

    # PHASE 1 -- crawl every chapter (worker threads). Only fetch_html/upload_source
    # write r2_sync's shared _stem_index here; nothing iterates it, so the writes
    # are race-free. Chunking is deferred to phase 2 on purpose: node_to_chunks
    # calls r2_sync.lookup_all_formats, which ITERATES _stem_index -- doing that
    # while phase-1 workers are still writing it raises "dictionary changed size
    # during iteration" and drops the section's R2 links. Separating the phases
    # freezes _stem_index before any lookup runs.
    pending: list[tuple[str, dict]] = []
    t0 = time.time()
    done = 0
    print("phase 1: crawling chapters...", flush=True)
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(crawl_chapter, ch): ch for ch in chapters}
        for fut in as_completed(futures):
            ch = futures[fut]
            done += 1
            try:
                chapter, sections, fetches = fut.result()
            except Exception as exc:
                stats["chapter_err"] += 1
                print(f"  chapter {ch}: crawl error: {str(exc)[:160]}", flush=True)
                continue
            stats["fetches"] += fetches
            if not sections:
                stats["chapter_err"] += 1
                print(f"  chapter {ch}: 0 sections ({fetches} fetches)", flush=True)
                continue
            stats["chapters"] += 1
            for sec in sorted(sections, key=_seckey):
                pending.append((sec, sections[sec]))
            if done % 25 == 0 or done == len(chapters):
                rate = done / max(time.time() - t0, 0.001)
                eta = (len(chapters) - done) / rate / 60 if rate else 0
                print(
                    f"  [{done}/{len(chapters)}] crawled ch {chapter} | sections queued="
                    f"{len(pending):,} fetches={stats['fetches']:,} ETA={eta:.1f}m",
                    flush=True,
                )

    # PHASE 2 -- chunk every section (worker threads). _stem_index is now frozen
    # (no more source uploads), so concurrent lookup_all_formats reads are safe,
    # and upload_section_text is stateless. R2 links populate reliably.
    print(f"phase 2: chunking {len(pending):,} sections...", flush=True)
    t1 = time.time()
    cdone = 0

    def _chunk(item: tuple[str, dict]) -> list[dict]:
        sec, data = item
        try:
            return _build_chunks_for_section(sec, data, year)
        except Exception as exc:
            print(f"  {sec}: node_to_chunks error: {str(exc)[:140]}", flush=True)
            return []

    with ThreadPoolExecutor(max_workers=workers) as ex:
        for chunks in ex.map(_chunk, pending):
            cdone += 1
            if not chunks:
                stats["empty_sections"] += 1
            else:
                stats["sections"] += 1
                stats["chunks"] += len(chunks)
                out.extend(chunks)
            if cdone % 2000 == 0:
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
    ap.add_argument("--workers", type=int, default=24, help="parallel chapter crawlers")
    ap.add_argument("--limit", type=int, default=0, help="only first N chapters (smoke test)")
    args = ap.parse_args()

    out_path = args.out or _default_out()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    records, stats = build_records(args.workers, limit_chapters=args.limit)

    # Fresh file per run so --reconcile sees exactly this run's corpus.
    with open(out_path, "w", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")

    print("\n=== done ===")
    print(f"  chapters ok : {stats['chapters']:,}")
    print(f"  chapter err : {stats['chapter_err']:,}")
    print(f"  sections    : {stats['sections']:,}")
    print(f"  empty secs  : {stats['empty_sections']:,}")
    print(f"  chunks      : {stats['chunks']:,}")
    print(f"  page fetches: {stats['fetches']:,}")
    print(f"  wrote       : {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
