#!/usr/bin/env python3
"""Ingest New Jersey statutes from the official njleg daily bulk export.

The NJ scraper walked njleg's Title -> Chapter -> Section tree and minted a
separate act_id per chapter path, so the same section (e.g. 40A:11-52) landed
under chapters C1, C2, C3, C6, C10A ... as five distinct act_ids. That inflated
the corpus to 144,307 act_ids / 158,903 points for only 19,068 real distinct
sections, AND still missed 28 whole titles (Criminal 2C, Education 18A, Title
2A, Tax 54, ...).

This replaces it with the official daily zip:
    https://pub.njleg.state.nj.us/Statutes/STATUTES-TEXT.zip  (-> STATUTES.RTF)
which is the complete New Jersey Permanent Statutes Database (68 titles, updated
through the latest public law). Parsing the RTF's style tags (nj_bulk.parse)
yields 55,910 clean distinct sections, one act_id per citation.

Verified (nj_bulk overlap probe) against the live corpus:
    bulk covers 19,044 / 19,068 existing sections (99.87%) after case norm,
    adds 36,866 new sections; only 24 existing (0.13%, a repealed 34:1b-* run)
    are not in the bulk.

Because the bulk cannot reproduce the scraper's structural chapter, act_ids do
NOT match the old ones. So this is a full cutover with a STATE-SCOPED reconcile
(delete every state=nj point not in this run), not the act_id-scoped reconcile
CA/IL used. The chapter in the bulk act_id is the section-number prefix
(40A:11-52 -> chapter 11), internal only; the user-facing citation is rendered
from the citation field and is unchanged.

Pipeline (mirrors ingest_ilga_bulk.py, but all text is in the RTF so there is
no per-section network fetch):
    download/parse RTF -> for each Section: synthetic Node ->
    node_to_payload.node_to_chunks -> JSONL

Run on the scraper box (the zip download needs US egress via the proxy; the
parse + embed do not):
    docker exec -d -e VAQUILL_USE_PROXY=1 -e VAQUILL_R2_UPLOAD=1 -e VAQUILL_R2_POOL=64 \
      -w /app vaquill-scraper-worker \
      python -u scripts/us_corpus/ingest_nj_bulk.py --workers 32 \
        > /app/nj_ingest.log 2>&1
    then: embed_and_upsert.py --input .../state_nj_statutes.jsonl        (additive)
          verify, then re-run embed_and_upsert.py ... --reconcile --state-scoped
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_SCRAPERS = _HERE.parent / "state_scrapers"
sys.path.insert(0, str(_HERE))
sys.path.insert(0, str(_SCRAPERS))
sys.path.insert(0, str(_ROOT / "scripts" / "us_corpus"))

from nj_bulk.parse import Section, iter_sections

STATE = "nj"
CORPUS = "statutes"
COUNTRY = "us"
ZIP_URL = "https://pub.njleg.state.nj.us/Statutes/STATUTES-TEXT.zip"
RTF_MEMBER = "STATUTES.RTF"
# Stable public viewer for a section (njleg has no per-section deep link).
SECTION_VIEW = "https://www.njleg.state.nj.us/legislative-activity/statutes"


def _default_out() -> Path:
    override = os.environ.get("STATE_CHUNKS_DIR_OVERRIDE")
    base = Path(override) if override else (_SCRAPERS / "data" / "state_chunks")
    return base / f"state_{STATE}_statutes.jsonl"


def _node_id(sec: Section) -> str:
    """Path the pipeline turns into STATE_NJ_T{title}_C{chapter}_S{section}."""
    return (
        f"{COUNTRY}/{STATE}/{CORPUS}"
        f"/title={sec.title}/chapter={sec.chapter}/section={sec.section}"
    )


def _load_rtf(zip_path: Path) -> str:
    if not zip_path.exists():
        from vaquill_pipeline.http_client import fetch_bytes

        print(f"downloading {ZIP_URL} ...", flush=True)
        data = fetch_bytes(ZIP_URL)
        zip_path.write_bytes(data)
        print(f"  saved {len(data):,} bytes -> {zip_path}", flush=True)
    with zipfile.ZipFile(zip_path) as z:
        return z.read(RTF_MEMBER).decode("cp1252", "replace")


def build_records(workers: int, zip_path: Path, limit: int = 0) -> tuple[list[dict], dict]:
    from src.utils.pydanticModels import Node, NodeText
    from vaquill_pipeline.node_to_payload import node_to_chunks

    rtf = _load_rtf(zip_path)
    print(f"RTF loaded ({len(rtf):,} chars); parsing sections...", flush=True)
    sections = list(iter_sections(rtf))
    if limit:
        sections = sections[:limit]
    year = time.gmtime().tm_year
    print(f"  {len(sections):,} sections parsed", flush=True)

    stats = {"sections": 0, "chunks": 0, "no_text": 0, "err": 0}
    out: list[dict] = []

    def _one(sec: Section):
        if not sec.body or len(sec.body) < 5:
            return ("no_text", None, None)
        node_id = _node_id(sec)
        node = Node(
            id=node_id,
            link=SECTION_VIEW,
            node_type="content",
            level_classifier="section",
            number=sec.section,
            node_name=sec.catchline or f"Section {sec.section}",
            node_text=NodeText(),
            citation=sec.citation(),
            top_level_title=sec.title,
            parent="/".join(node_id.split("/")[:-1]),
            status=None,
        )
        node.node_text.add_paragraph(text=sec.body)
        try:
            chunks = node_to_chunks(node, STATE, year)
        except Exception as exc:
            return ("err", f"{sec.citation()}: {str(exc)[:120]}", None)
        return ("ok", None, chunks)

    print(f"chunking with {workers} workers (R2 upload inline)...", flush=True)
    t0 = time.time()
    done = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for fut in as_completed(ex.submit(_one, s) for s in sections):
            kind, msg, chunks = fut.result()
            done += 1
            if done % 5000 == 0:
                rate = done / max(time.time() - t0, 0.001)
                print(f"  {done:,}/{len(sections):,}  {rate:.1f}/s  chunks={stats['chunks']:,}", flush=True)
            if kind == "ok":
                stats["sections"] += 1
                stats["chunks"] += len(chunks)
                out.extend(chunks)
            else:
                stats[kind] = stats.get(kind, 0) + 1
                if kind == "err" and stats[kind] <= 5:
                    print(f"  {msg}", flush=True)
    return out, stats


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", type=Path, default=None)
    ap.add_argument("--zip", type=Path, default=Path("/app/nj_statutes.zip"))
    ap.add_argument("--limit", type=int, default=0, help="only N sections (smoke)")
    ap.add_argument("--workers", type=int, default=32)
    args = ap.parse_args()

    out_path = args.out or _default_out()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    records, stats = build_records(args.workers, args.zip, limit=args.limit)

    # Fresh file per run so --reconcile sees exactly this run's corpus.
    with open(out_path, "w", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")

    print("\n=== done ===")
    print(f"  sections : {stats['sections']:,}")
    print(f"  chunks   : {stats['chunks']:,}")
    print(f"  no_text  : {stats.get('no_text', 0):,}")
    print(f"  err      : {stats.get('err', 0):,}")
    print(f"  wrote    : {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
