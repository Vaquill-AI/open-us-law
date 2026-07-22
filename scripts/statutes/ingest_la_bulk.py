#!/usr/bin/env python3
"""Ingest all six Louisiana statute bodies from the official legis.la.gov site.

The old LA scraper (state_scrapers/.../la/statutes/scrapeLA.py) pulled from the
FindLaw mirror and stalled at ~10,216 sections: Revised Statutes a quarter
present (6,077 of ~46,000) and the Code of Civil Procedure essentially missing
(21 of ~1,250 articles). This replaces it with the State Legislature's own Laws
site, which publishes every body completely.

Pipeline (mirrors ingest_va_bulk.py / ingest_nj_bulk.py):
    scan folder-id range -> keep TOC folders whose LabelHeader names a statute
    body -> collect every Law.aspx doc id -> fetch each Law page -> parse
    LabelName (citation identity) + LabelDocument (text) -> synthetic Node
    -> node_to_payload.node_to_chunks -> JSONL

Reusing node_to_chunks means act_id / point_id / citation / breadcrumb /
chunking / R2 upload match the scraper path exactly. The node id path
``code={slug}/(title=.../section=... | article=...)`` reproduces the existing LA
act_id scheme (e.g. STATE_LA_Crevised-statutes_T14_S30).

legis.la.gov does not geo-block the box, so this runs direct by default (set
VAQUILL_USE_PROXY=1 to route through the US residential proxy if the site starts
throttling a single IP). Typical launch on the scraper box:

    docker exec -d -e VAQUILL_R2_UPLOAD=1 -e VAQUILL_R2_POOL=64 \
      -w /app vaquill-scraper-worker \
      python -u scripts/us_corpus/statutes/ingest_la_bulk.py --workers 24 \
        > /app/la_ingest.log 2>&1
    then: embed_and_upsert.py --input .../state_la_statutes.jsonl              (additive)
          verify counts, then --reconcile-state la --min-run-points 30000      (cutover)

Cutover note: bulk text differs from scraped text, so every section gets a fresh
content-addressed point_id. The Revised-Statutes base act_ids reproduce the old
scrape, but the dotted RS sub-sections and the article codes' FindLaw roman
titles do not, so this cuts over state-scoped (document_type=statute, which
preserves the 51 La. constitution points under state=la) after la_bulk/
verify_act_ids.py confirms the bulk covers the existing citations.
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

from la_bulk import client as C
from la_bulk import parse as P
from la_bulk import walk as W

STATE = "la"


def _default_out() -> Path:
    override = os.environ.get("STATE_CHUNKS_DIR_OVERRIDE")
    base = Path(override) if override else (_SCRAPERS / "data" / "state_chunks")
    return base / f"state_{STATE}_statutes.jsonl"


def discover_docids(folder_min: int, folder_max: int, index_workers: int) -> tuple[list[str], dict]:
    """Scan the folder-id range; return every statute Law.aspx doc id.

    Each folder page is fetched once and yields both its LabelHeader (which body
    it belongs to) and its flat list of Law.aspx doc ids. Folders whose header is
    not one of the six statute bodies (Constitution, LAC regulations, House/
    Senate/Joint Rules, AG opinions, ...) are dropped, so no folder id is
    hard-coded and non-statute bodies are never ingested.
    """
    folders = list(range(folder_min, folder_max + 1))
    print(f"scanning folders {folder_min}..{folder_max} for statute bodies...", flush=True)

    def _fetch(fid: int):
        try:
            html = C.toc(fid)
        except Exception as exc:
            return (fid, None, [], str(exc)[:120])
        return (fid, P.folder_header(html), P.toc_docids(html), None)

    per_body: dict[str, int] = {}
    seen: set[str] = set()
    docids: list[str] = []
    kept_folders: dict[str, list[int]] = {}
    errs = 0
    with ThreadPoolExecutor(max_workers=index_workers) as ex:
        for fut in as_completed(ex.submit(_fetch, f) for f in folders):
            fid, header, ids, err = fut.result()
            if err:
                errs += 1
                continue
            body = W.HEADER_TO_PREFIX.get(header or "")
            if not body or not ids:
                continue
            kept_folders.setdefault(body, []).append(fid)
            new = [d for d in ids if d not in seen]
            seen.update(new)
            docids.extend(new)
            per_body[body] = per_body.get(body, 0) + len(ids)

    print(f"  fetch errors: {errs}", flush=True)
    for body in W.BODIES:
        fids = sorted(kept_folders.get(body, []))
        print(
            f"  {body:5} {W.BODIES[body]['header']:28} folders={len(fids):3}  "
            f"leaves={per_body.get(body, 0):,}",
            flush=True,
        )
    print(f"  total distinct doc ids: {len(docids):,}", flush=True)
    stats = {"folders_scanned": len(folders), "folder_errs": errs, "leaves_by_body": per_body}
    return docids, stats


def build_records(
    folder_min: int, folder_max: int, index_workers: int, body_workers: int, limit: int = 0
) -> tuple[list[dict], dict]:
    from src.utils.pydanticModels import Node, NodeText
    from vaquill_pipeline.node_to_payload import node_to_chunks

    docids, disc = discover_docids(folder_min, folder_max, index_workers)
    if limit:
        docids = docids[:limit]
    year = time.gmtime().tm_year
    print(f"fetching {len(docids):,} law pages with {body_workers} workers...", flush=True)

    stats = {
        "sections": 0,
        "chunks": 0,
        "no_text": 0,
        "not_section": 0,
        "fetch_err": 0,
        "repealed": 0,
        "by_body": {},
    }
    out: list[dict] = []

    def _one(docid: str):
        try:
            html = C.law(docid)
        except Exception as exc:
            return ("fetch_err", f"{docid}: {str(exc)[:120]}", None)
        parsed = P.parse_label(P.label_name(html))
        if parsed is None:
            return ("not_section", None, None)
        body, title, number = parsed
        heading, paras = P.heading_and_body(P.document_blocks(html))
        if not paras:
            return ("no_text", None, None)

        sec = W.LASection(body=body, title=title, number=number, heading=heading)
        status = None
        low_head = heading.lower()
        if "repealed" in low_head or "reserved" in low_head or "blank" in low_head:
            status = "repealed"

        node_id = sec.node_id()
        node = Node(
            id=node_id,
            link=C.law_url(docid),
            node_type="content",
            level_classifier="section" if body == "RS" else "article",
            number=sec.number,
            node_name=sec.heading or f"{W.BODIES[body]['header']} {sec.number}",
            node_text=NodeText(),
            citation=sec.citation(),
            top_level_title=sec.top_level_title(),
            parent="/".join(node_id.split("/")[:-1]),
            status=status,
        )
        for para in paras:
            node.node_text.add_paragraph(text=para)
        try:
            chunks = node_to_chunks(node, STATE, year)
        except Exception as exc:
            return ("fetch_err", f"{docid}: node_to_chunks: {str(exc)[:120]}", None)
        if not chunks:
            return ("no_text", None, None)
        return ("ok", (body, "repealed" if status else None), chunks)

    t0 = time.time()
    done = 0
    with ThreadPoolExecutor(max_workers=body_workers) as ex:
        for fut in as_completed(ex.submit(_one, d) for d in docids):
            kind, tag, chunks = fut.result()
            done += 1
            if done % 2000 == 0:
                rate = done / max(time.time() - t0, 0.001)
                eta = (len(docids) - done) / rate / 60 if rate else 0
                print(
                    f"  {done:,}/{len(docids):,}  {rate:.1f}/s  sections={stats['sections']:,}  "
                    f"chunks={stats['chunks']:,}  ETA={eta:.1f}m",
                    flush=True,
                )
            if kind == "ok":
                body, rep = tag
                stats["sections"] += 1
                stats["chunks"] += len(chunks)
                stats["by_body"][body] = stats["by_body"].get(body, 0) + 1
                if rep:
                    stats["repealed"] += 1
                out.extend(chunks)
            else:
                stats[kind] = stats.get(kind, 0) + 1
                if kind == "fetch_err" and stats[kind] <= 8:
                    print(f"  {tag}", flush=True)
    stats["discovery"] = disc
    return out, stats


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", type=Path, default=None)
    ap.add_argument("--limit", type=int, default=0, help="only first N doc ids (smoke test)")
    ap.add_argument("--workers", type=int, default=24, help="law-page fetch workers")
    ap.add_argument("--index-workers", type=int, default=24, help="folder-scan workers")
    ap.add_argument("--folder-min", type=int, default=66)
    ap.add_argument("--folder-max", type=int, default=220)
    args = ap.parse_args()

    out_path = args.out or _default_out()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    records, stats = build_records(
        args.folder_min, args.folder_max, args.index_workers, args.workers, limit=args.limit
    )

    # Fresh file per run so a state-scoped reconcile sees exactly this run's corpus.
    with open(out_path, "w", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")

    print("\n=== done ===")
    print(f"  sections   : {stats['sections']:,}")
    print(f"  by body    : {stats['by_body']}")
    print(f"  repealed   : {stats.get('repealed', 0):,}")
    print(f"  chunks     : {stats['chunks']:,}")
    print(f"  not_section: {stats.get('not_section', 0):,}  (title headings etc.)")
    print(f"  no_text    : {stats.get('no_text', 0):,}")
    print(f"  fetch_err  : {stats.get('fetch_err', 0):,}")
    print(f"  wrote      : {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
