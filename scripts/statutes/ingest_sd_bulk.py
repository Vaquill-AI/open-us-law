#!/usr/bin/env python3
"""Ingest the full South Dakota Codified Laws (SDCL) from sdlegislature.gov.

The SD scraper (state_scrapers/.../sd/statutes/scrapeSD.py) hit the SAME official
SDLRC API this script uses, but never completed: the corpus holds only ~18 of the
62 numeric SDCL titles (max title present = 24), missing whole codes, most
importantly Motor Vehicles (32), Insurance (58), and Labor (60-62). It ALSO used a
numeric-only ``range(1,63)`` enumerator, so it silently dropped every
alpha-suffixed title, ~8,400 sections including all of Criminal Procedure (23A),
the Uniform Commercial Code (57A), the Uniform Probate Code (29A), Environmental
Protection (34A), Banking (51A), and Water Management (46A). This is a completeness
re-ingest, not a new source: it enumerates the full numeric + alpha title space
and rebuilds the entire Code (65 content titles, ~39.5k distinct sections).

Single-surface source (one request per title label, no per-chapter crawl):
  - ``/api/Statutes/{label}.html?all=true`` returns the ENTIRE title in one
    UTF-16 document, the per-chapter table of contents PLUS every section's
    heading, body, and Source history. ~65 title fetches yield the whole Code.

Pipeline (mirrors ingest_ia_bulk.py):
    numeric 1-62 + alpha {N}A/B/C labels -> per-title HTML (chapter TOC + all
    section bodies) -> parse -> synthetic Node -> node_to_payload.node_to_chunks
    -> JSONL

Reusing node_to_chunks means act_id / point_id / citation / chunking / R2 upload
match the scraper path exactly. SDCL is a flat Title -> Chapter -> Section and the
chapter is embedded in the section number, so the act_id reproduces exactly
(22-16-4 -> STATE_SD_T22_C16_S22-16-4); verified against Qdrant by
sd_bulk/verify_act_ids.py before any full run.

Phase-separated (crawl then chunk) so the r2_sync in-process url map is never
read+written concurrently: Phase 1 fetches + parses ALL titles (pure HTTP + parse,
no R2); Phase 2 runs node_to_chunks over every parsed section (the only R2 writer).

Run on the scraper box (SDLRC is served from the Azure US Gov cloud and geo-fences
non-US egress; proxy egress required from Hetzner):
    docker exec -d -e VAQUILL_USE_PROXY=1 -e VAQUILL_R2_UPLOAD=1 -e VAQUILL_R2_POOL=64 \
      -w /app vaquill-scraper-worker \
      python -u scripts/us_corpus/statutes/ingest_sd_bulk.py --workers 16 \
        > /app/sd_ingest.log 2>&1
    then: embed_and_upsert.py --input .../state_sd_statutes.jsonl        (additive)
          embed_and_upsert.py --input .../state_sd_statutes.jsonl --reconcile

Cutover note: bulk text differs from scraped text, so every section gets a fresh
content-addressed point_id; --reconcile deletes the superseded ones within the
act_ids this run touched (act_id reproduction is ~100% because SDCL's Title/Chapter/
Section hierarchy is embedded in the section number itself). ``state=sd`` also holds
the SD Constitution (document_type=constitution); its act_ids are never in this run
so an act_id-scoped reconcile never touches it.
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

from sd_bulk import client as C
from sd_bulk import parse as P
from sd_bulk.walk import SDSection

STATE = "sd"
DEFAULT_YEAR = 2025  # SDCL current through the 2025 legislative session.


def _default_out() -> Path:
    override = os.environ.get("STATE_CHUNKS_DIR_OVERRIDE")
    base = Path(override) if override else (_SCRAPERS / "data" / "state_chunks")
    return base / f"state_{STATE}_statutes.jsonl"


# ---------------------------------------------------------------------------
# Phase 1: crawl + parse (no R2)
# ---------------------------------------------------------------------------


def crawl_and_parse(year: int, fetch_workers: int, limit: int = 0):
    """Fetch every SDCL title 1-62 and parse it into section records.

    Returns (sections, stats) where each section is a
    (SDSection, body_paragraphs, source, status) tuple. Pure HTTP + parse, so
    the r2_sync url map is untouched here (Phase 2 is the only R2 writer).
    """
    titles = C.candidate_title_labels()
    if limit:
        titles = titles[:limit]
    print(
        f"Phase 1: probing + parsing {len(titles)} candidate SDCL title labels "
        f"(numeric 1-62 + alpha) with {fetch_workers} workers...",
        flush=True,
    )

    stats = {"titles_present": 0, "titles_missing": 0, "fetch_err": 0, "sections": 0, "repealed": 0}
    sections: list[tuple[SDSection, list[str], str, str | None]] = []
    present_labels: list[str] = []

    def _one(label: str):
        try:
            html = C.title_html(label)
        except Exception as exc:
            return (label, "fetch_err", str(exc)[:140], None)
        if html is None:
            return (label, "missing", None, None)
        try:
            parsed = P.parse_title_sections(html, label)
        except Exception as exc:
            return (label, "fetch_err", f"parse: {str(exc)[:140]}", None)
        return (label, "ok", None, parsed)

    t0 = time.time()
    done = 0
    with ThreadPoolExecutor(max_workers=fetch_workers) as ex:
        for fut in as_completed(ex.submit(_one, t) for t in titles):
            label, kind, err, parsed = fut.result()
            done += 1
            if kind == "ok" and parsed:
                # A label with at least one section is a real content title. A
                # 404 or a recodified-empty numeric shell (e.g. 57, replaced by
                # 57A) simply contributes nothing.
                stats["titles_present"] += 1
                present_labels.append(label)
                for rec in parsed:
                    sections.append(rec)
                    stats["sections"] += 1
                    if rec[3] in ("repealed", "reserved", "transferred"):
                        stats["repealed"] += 1
            elif kind in ("ok", "missing"):
                stats["titles_missing"] += 1
            else:
                stats["fetch_err"] += 1
                print(f"  title {label}: {err}", flush=True)
            if done % 30 == 0:
                rate = done / max(time.time() - t0, 0.001)
                print(
                    f"  {done}/{len(titles)} labels probed  {rate:.1f}/s  "
                    f"present={stats['titles_present']} sections={stats['sections']:,}",
                    flush=True,
                )

    def _tk(x):
        return (int("".join(ch for ch in x if ch.isdigit()) or 0), x)

    print(
        f"Phase 1 done: {stats['titles_present']} content titles "
        f"({sorted(present_labels, key=_tk)}), {stats['sections']:,} sections parsed.",
        flush=True,
    )
    return sections, stats


# ---------------------------------------------------------------------------
# Phase 2: chunk (R2 uploads happen here, in node_to_chunks)
# ---------------------------------------------------------------------------


def chunk_sections(sections, year: int, chunk_workers: int):
    from src.utils.pydanticModels import Node, NodeText
    from vaquill_pipeline.node_to_payload import node_to_chunks

    print(
        f"Phase 2: chunking {len(sections):,} sections with {chunk_workers} workers...", flush=True
    )
    stats = {"sections": 0, "chunks": 0, "no_text": 0, "chunk_err": 0}
    out: list[dict] = []

    def _one(rec):
        sec, body, source, status = rec
        if not body:
            return ("no_text", None)
        node_id = sec.node_id()
        node = Node(
            id=node_id,
            link=C.chapter_url(sec.title_number, sec.chapter),
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
        for para in body:
            node.node_text.add_paragraph(text=para)
        if source:
            # Append the Source/history line as a trailing paragraph (matches
            # ingest_ia_bulk.py), so amendment-year enrichment sees it.
            node.node_text.add_paragraph(text=source)
        try:
            chunks = node_to_chunks(node, STATE, year)
        except Exception as exc:
            return ("chunk_err", f"{sec.section_number}: {str(exc)[:120]}")
        if not chunks:
            return ("no_text", None)
        return ("ok", chunks)

    t0 = time.time()
    done = 0
    total = len(sections)
    with ThreadPoolExecutor(max_workers=chunk_workers) as ex:
        for fut in as_completed(ex.submit(_one, r) for r in sections):
            kind, payload = fut.result()
            done += 1
            if kind == "ok":
                stats["sections"] += 1
                stats["chunks"] += len(payload)
                out.extend(payload)
            elif kind == "no_text":
                stats["no_text"] += 1
            else:
                stats["chunk_err"] += 1
                if stats["chunk_err"] <= 10:
                    print(f"  {payload}", flush=True)
            if done % 2000 == 0:
                rate = done / max(time.time() - t0, 0.001)
                eta = (total - done) / rate / 60 if rate else 0
                print(
                    f"  {done:,}/{total:,} sections  {rate:.1f}/s  "
                    f"chunks={stats['chunks']:,}  ETA={eta:.1f}m",
                    flush=True,
                )
    return out, stats


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", type=Path, default=None)
    ap.add_argument("--year", type=int, default=DEFAULT_YEAR)
    ap.add_argument("--limit", type=int, default=0, help="only first N titles (smoke test)")
    ap.add_argument("--workers", type=int, default=16, help="Phase 2 chunk workers")
    ap.add_argument("--fetch-workers", type=int, default=12, help="Phase 1 title-fetch workers")
    args = ap.parse_args()

    out_path = args.out or _default_out()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    sections, p1 = crawl_and_parse(args.year, args.fetch_workers, limit=args.limit)
    records, p2 = chunk_sections(sections, args.year, args.workers)

    # Fresh file per run so --reconcile sees exactly this run's corpus.
    with open(out_path, "w", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")

    print("\n=== done ===")
    print(f"  titles present : {p1['titles_present']}")
    print(f"  titles missing : {p1['titles_missing']}")
    print(f"  sections parsed: {p1['sections']:,}")
    print(f"  repealed/rsrvd : {p1['repealed']:,}")
    print(f"  sections chunked: {p2['sections']:,}")
    print(f"  chunks         : {p2['chunks']:,}")
    print(f"  no_text        : {p2['no_text']:,}")
    print(f"  chunk_err      : {p2['chunk_err']:,}")
    print(f"  wrote          : {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
