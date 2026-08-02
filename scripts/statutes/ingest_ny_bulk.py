#!/usr/bin/env python3
"""Ingest New York statutes from the official Open Legislation (NY Senate) API.

The NY scraper hit the JS-backed nysenate.gov laws pages with plain HTTP, so it
came out section-thin inside the big consolidated laws (e.g. Tax ~521 of ~1.2k).
This replaces it with the structured Open Legislation JSON API, which returns
the complete recursive document tree (every section's text) per law volume.

NY act_ids do not cleanly reproduce (law/article/title/part hierarchy), but the
citation does (N.Y. {lawId} Law § {section}), so this is a full cutover with a
STATE-SCOPED reconcile (like NJ): embed all API sections, then delete every
state=ny point not in this run. Gate on citation-set overlap before deleting.

Requires OPENLEG_API_KEY (free key from legislation.nysenate.gov).

Pipeline (mirrors ingest_nj_bulk.py):
    list_laws -> for each statute law: get full tree -> walk SECTION nodes ->
    synthetic Node -> node_to_payload.node_to_chunks -> JSONL

Run on the scraper box:
    docker exec -d -e OPENLEG_API_KEY=... -e VAQUILL_USE_PROXY=1 \
      -e VAQUILL_R2_UPLOAD=1 -e VAQUILL_R2_POOL=64 -w /app vaquill-scraper-worker \
      python -u scripts/us_corpus/statutes/ingest_ny_bulk.py --workers 8 \
        > /app/ny_ingest.log 2>&1
    then: lib/embed_and_upsert.py --input .../state_ny_statutes.jsonl        (additive)
          verify citation overlap, then --reconcile-state ny
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

from ny_bulk import api as A
from ny_bulk.walk import Section, iter_sections

STATE = "ny"
CORPUS = "statutes"
COUNTRY = "us"
# Statute-bearing volumes; MISC (holds the CNS Constitution) is excluded here
# because the constitution lives in the state_constitution corpus, not statutes.
STATUTE_LAW_TYPES = {"CONSOLIDATED", "UNCONSOLIDATED", "COURT_ACTS", "RULES"}


def _default_out() -> Path:
    override = os.environ.get("STATE_CHUNKS_DIR_OVERRIDE")
    base = Path(override) if override else (_SCRAPERS / "data" / "state_chunks")
    return base / f"state_{STATE}_statutes.jsonl"


def _node_id(sec: Section) -> str:
    # The law volume is the top level; "act" is the pipeline's classifier for it
    # (ALLOWED_LEVELS has no "law"), matching the existing NY act_id prefix A{lawId}.
    parts = [f"{COUNTRY}/{STATE}/{CORPUS}", f"act={sec.law_id}"]
    for cls, lvl in sec.ancestors:
        if cls and lvl:
            parts.append(f"{cls}={lvl}")
    parts.append(f"section={sec.location_id}")
    return "/".join(parts)


def _section_url(sec: Section) -> str:
    return f"https://www.nysenate.gov/legislation/laws/{sec.law_id}/{sec.location_id}"


def build_records(
    workers: int, limit_laws: int = 0, only_laws: set[str] | None = None
) -> tuple[list[dict], dict]:
    from src.utils.pydanticModels import Node, NodeText
    from vaquill_pipeline.node_to_payload import node_to_chunks

    laws = [law for law in A.list_laws() if law.get("lawType") in STATUTE_LAW_TYPES]
    if only_laws:
        want = {c.upper() for c in only_laws}
        laws = [law for law in laws if law.get("lawId", "").upper() in want]
    if limit_laws:
        laws = laws[:limit_laws]
    year = time.gmtime().tm_year
    print(f"{len(laws)} statute law volumes to ingest", flush=True)

    stats = {"laws": 0, "sections": 0, "chunks": 0, "no_text": 0, "law_err": 0}
    out: list[dict] = []

    def _one_law(law: dict):
        law_id = law["lawId"]
        try:
            tree = A.get_law_tree(law_id)
        except Exception as exc:
            return ("law_err", f"{law_id}: {str(exc)[:140]}", [])
        chunks: list[dict] = []
        secs = 0
        for sec in iter_sections(tree):
            try:
                node_id = _node_id(sec)
                node = Node(
                    id=node_id,
                    link=_section_url(sec),
                    node_type="content",
                    level_classifier="section",
                    number=sec.doc_level_id,
                    node_name=sec.title or f"Section {sec.doc_level_id}",
                    node_text=NodeText(),
                    citation=sec.citation(),
                    top_level_title=sec.law_id,
                    parent="/".join(node_id.split("/")[:-1]),
                    status=None,
                )
                node.node_text.add_paragraph(text=sec.text)
                chunks.extend(node_to_chunks(node, STATE, year))
                secs += 1
            except Exception:
                pass
        return ("ok", law_id, chunks, secs)

    print(f"fetching {len(laws)} law trees with {workers} workers...", flush=True)
    t0 = time.time()
    done = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for fut in as_completed(ex.submit(_one_law, law) for law in laws):
            res = fut.result()
            done += 1
            if res[0] == "ok":
                _, law_id, chunks, secs = res
                stats["laws"] += 1
                stats["sections"] += secs
                stats["chunks"] += len(chunks)
                out.extend(chunks)
                print(f"  [{done}/{len(laws)}] {law_id}: {secs:,} sections, {len(chunks):,} chunks", flush=True)
            else:
                stats["law_err"] += 1
                print(f"  [{done}/{len(laws)}] LAW_ERR {res[1]}", flush=True)
    print(f"fetched in {time.time()-t0:.0f}s", flush=True)
    return out, stats


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", type=Path, default=None)
    ap.add_argument("--limit-laws", type=int, default=0, help="only first N laws (smoke)")
    ap.add_argument(
        "--only-laws",
        nargs="+",
        default=None,
        metavar="LAWID",
        help="ingest only these lawIds (e.g. LEH NNY) for a targeted backfill",
    )
    ap.add_argument("--workers", type=int, default=8)
    args = ap.parse_args()

    out_path = args.out or _default_out()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    records, stats = build_records(
        args.workers,
        limit_laws=args.limit_laws,
        only_laws=set(args.only_laws) if args.only_laws else None,
    )

    with open(out_path, "w", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")

    print("\n=== done ===")
    print(f"  laws     : {stats['laws']:,}")
    print(f"  sections : {stats['sections']:,}")
    print(f"  chunks   : {stats['chunks']:,}")
    print(f"  law_err  : {stats['law_err']:,}")
    print(f"  wrote    : {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
