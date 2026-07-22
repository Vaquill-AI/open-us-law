#!/usr/bin/env python3
"""Ingest California statutes from the state's OFFICIAL bulk export.

Why this exists: the CA scraper gave us 28,472 of the ~161,296 sections
California publishes (17.7%). Fifteen of thirty codes were missing outright,
including the Labor, Penal, Vehicle, Insurance and Probate Codes, so a query
like "employee minimum wage overtime" returned wage garnishment and Food and Ag
carnival-ride rules instead of Labor Code 1194. This replaces the scrape with
downloads.leginfo.legislature.ca.gov's pubinfo_<session>.zip, which is the
legislature's own database dump. It also settles the robots question rather
than routing around it: leginfo.legislature.ca.gov publishes
"User-agent: * / Disallow: /", robots is per-origin, and the downloads host
carries no robots.txt.

Output is a JSONL of chunk records in the SAME shape the state scrapers emit,
so the rest of the pipeline is unchanged:

    python scripts/us_corpus/ingest_ca_bulk.py --session 2025
    python scripts/us_corpus/embed_and_upsert.py \
        --input .../state_chunks/state_ca_statutes.jsonl --reconcile
    python scripts/us_corpus/sync_states_to_supabase.py --states ca

Chunk records are produced by vaquill_pipeline.node_to_payload.node_to_chunks,
the exact function the scrapers use, by handing it synthetic Node objects. That
is deliberate: act_id, the content-addressed point_id, citation, breadcrumb,
display_path, chunking and the R2 section-text upload then match the scraper
path bit for bit, instead of a parallel implementation drifting out of sync and
breaking /statutes/search.

Cutover note: CAML text is not byte-identical to the old scraped HTML text, so
the ~27k sections we already had get new point_ids once. That is a deliberate,
one-time re-embed, and --reconcile removes the superseded chunks. Because act_id
reconstruction matches at 99.28% (see ca_bulk/verify_act_ids.py), reconcile can
actually see those old points; without that it would silently keep both copies.
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

from ca_bulk import tables as T
from ca_bulk.caml import caml_to_text
from ca_bulk.zip_reader import (
    LocalZip,
    RemoteZip,
    ensure_local,
    session_zip_url,
)

STATE = "ca"
CORPUS = "statutes"
COUNTRY = "us"

# leginfo's public section viewer, kept as the human-facing source_url.
SECTION_URL = (
    "https://leginfo.legislature.ca.gov/faces/codes_displaySection.xhtml"
    "?lawCode={code}&sectionNum={section}"
)


def _default_out() -> Path:
    override = os.environ.get("STATE_CHUNKS_DIR_OVERRIDE")
    base = Path(override) if override else (_SCRAPERS / "data" / "state_chunks")
    return base / f"state_{STATE}_statutes.jsonl"


def _node_id(code: str, chain: list[T.TocNode], section: str) -> str:
    """Path-style node id the pipeline turns into act_id.

    node_to_payload._component_pairs drops the first three components
    (country/jurisdiction/corpus) and emits f"{classifier[0].upper()}{number}"
    for the rest, so
        us/ca/statutes/code=civ/division=3/part=4/title=5/chapter=2/section=1950.5
    becomes STATE_CA_Cciv_D3_P4_T5_C2_S1950.5 -- byte-identical to the scraper.
    """
    parts = [COUNTRY, STATE, CORPUS, f"code={code.lower()}"]
    for n in chain:
        if n.level and n.number:
            parts.append(f"{n.level}={n.number}")
    parts.append(f"section={section}")
    return "/".join(parts)


def build_records(z: RemoteZip, session: str, limit: int = 0, workers: int = 8) -> tuple[list[dict], dict]:
    """Return (chunk_records, stats). Reads tables, walks the tree, fetches .lobs."""
    from src.utils.pydanticModels import Node, NodeText
    from vaquill_pipeline.node_to_payload import node_to_chunks

    print("reading tables...", flush=True)
    code_names = T.parse_codes(z.read_text("CODES_TBL.dat"))
    toc_nodes = T.parse_toc(z.read_text("LAW_TOC_TBL.dat"))
    by_path = T.index_by_path(toc_nodes)

    # section -> its TOC chain
    chains: dict[tuple[str, str], list[T.TocNode]] = {}
    for r in T.rows(z.read_text("LAW_TOC_SECTIONS_TBL.dat")):
        if len(r) <= T.TS_SECT:
            continue
        code = (T.unquote(r[T.TS_CODE]) or "").upper()
        sect = T.unquote(r[T.TS_SECT])
        toc_path = T.unquote(r[T.TS_TOC_PATH]) or ""
        if not code or not sect:
            continue
        node = by_path.get((code, toc_path))
        chains[(code, sect.rstrip("."))] = T.ancestors(node, by_path) if node else []

    # CONS is the California Constitution, not a statute. It belongs to the
    # state_constitution corpus (already covered separately), and its bulk
    # section labels ("SEC. 3", "Section 1") repeat across articles with no
    # disambiguating hierarchy here, so ingesting it as statutes both mislabels
    # it and collapses 423 sections into 69 colliding act_ids. Exclude it.
    sec_rows = [
        r for r in T.rows(z.read_text("LAW_SECTION_TBL.dat"))
        if len(r) > T.S_TIMESTAMP and (T.unquote(r[T.S_CODE]) or "").upper() != "CONS"
    ]
    if limit:
        sec_rows = sec_rows[:limit]

    # Group rows by node_id (which maps 1:1 to act_id). California keeps
    # "double-jointed" sections: two coexisting enactment versions of one
    # section number at the same location (e.g. GOV 3556 "as amended" AND "as
    # added" by the same chapter). They share an act_id, so left as separate
    # records they collide on the R2 section-text key and produce duplicate
    # citations. Grouping by node_id merges ONLY true collisions (identical
    # code+hierarchy+section); a section number legitimately reused under a
    # different article has a different node_id and stays separate. Each group
    # becomes ONE section whose text carries every version, labelled by its
    # enactment history -- faithful to how leginfo itself presents them, loses
    # no enacted text, and keeps one act_id so reconcile stays clean.
    groups: dict[str, list[list[str]]] = {}
    no_chain_keys: set[str] = set()
    for r in sec_rows:
        code = (T.unquote(r[T.S_CODE]) or "").upper()
        sect = (T.unquote(r[T.S_SECT]) or "").rstrip(".")
        if not code or not sect:
            continue
        chain = chains.get((code, sect))
        if chain is None:
            no_chain_keys.add(f"{code} {sect}")
            chain = []
        groups.setdefault(_node_id(code, chain, sect), []).append(r)

    print(
        f"  codes={len(code_names)} toc_nodes={len(toc_nodes):,} "
        f"section_rows={len(sec_rows):,} act_id_groups={len(groups):,} "
        f"multi_version={sum(1 for v in groups.values() if len(v) > 1):,}",
        flush=True,
    )

    year = int(session) if session.isdigit() else time.gmtime().tm_year
    stats = {
        "sections": 0, "no_lob": 0, "empty_text": 0, "chunks": 0,
        "unknown_tags": set(), "no_chain": len(no_chain_keys), "multi_version": 0,
    }
    out: list[dict] = []
    members = z.members()

    def _one(item: tuple[str, list[list[str]]]):
        """Build ONE section (possibly combining versions) for a node_id group.

        All per-section work runs in the worker: zip reads, CAML conversion, and
        node_to_chunks -- the expensive call, since it uploads the section text
        to R2 (one network PUT). Keeping node_to_chunks in the main loop pinned
        the run to one thread at ~1/s; here the pool actually parallelizes it. It
        is safe concurrently: the scrapers already drive node_to_chunks from 8
        threads via vaquill_pipeline.patch, and r2_sync locks client creation.
        """
        node_id, rows = item
        code = (T.unquote(rows[0][T.S_CODE]) or "").upper()
        sect = (T.unquote(rows[0][T.S_SECT]) or "").rstrip(".")
        # Deterministic version order so the combined text (and thus the
        # content-addressed point_id) is stable across runs.
        rows = sorted(rows, key=lambda r: T.unquote(r[0]) or "")

        unknown: set[str] = set()
        parts: list[str] = []
        for r in rows:
            lob = T.unquote(r[T.S_LOB])
            if not lob or lob not in members:
                continue
            try:
                body = z.read(lob).decode("utf-8", "replace")
            except Exception as exc:
                return ("error", f"{code} {sect}: {exc}", unknown, False)
            text, unk = caml_to_text(body)
            unknown |= unk
            if not text:
                continue
            if len(rows) > 1:
                # Label each version by its enactment so the combined section
                # reads unambiguously instead of concatenating two "(a)" blocks.
                hist = T.unquote(r[T.S_HISTORY]) or ""
                header = f"[{hist}]" if hist else "[Alternate version]"
                parts.append(f"{header}\n{text}")
            else:
                parts.append(text)

        combined = "\n\n".join(parts).strip()
        if not combined or len(combined) < 20:
            return ("empty_text", None, unknown, False)

        node = Node(
            id=node_id,
            link=SECTION_URL.format(code=code, section=f"{sect}."),
            node_type="content",
            level_classifier="section",
            number=sect,
            node_name=f"Section {sect}",
            node_text=NodeText(),
            citation=f"Cal. {code} § {sect}",
            top_level_title=code,
            parent="/".join(node_id.split("/")[:-1]),
            status=None,
        )
        node.node_text.add_paragraph(text=combined)
        try:
            chunks = node_to_chunks(node, STATE, year)
        except Exception as exc:
            return ("error", f"{code} {sect}: node_to_chunks: {exc}", unknown, False)
        return ("ok", chunks, unknown, len(rows) > 1)

    print(f"building sections with {workers} workers...", flush=True)
    t0 = time.time()
    done = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for fut in as_completed(ex.submit(_one, it) for it in groups.items()):
            res = fut.result()
            done += 1
            if done % 2000 == 0:
                rate = done / max(time.time() - t0, 0.001)
                print(f"  {done:,}/{len(groups):,}  {rate:.1f}/s  chunks={stats['chunks']:,}", flush=True)
            if not res:
                continue
            kind, payload, unknown, multi = res
            if unknown:
                stats["unknown_tags"] |= unknown
            if kind != "ok":
                stats[kind] = stats.get(kind, 0) + 1
                if kind == "error" and stats[kind] <= 5:
                    print(f"  {payload}", flush=True)
                continue
            if multi:
                stats["multi_version"] += 1
            stats["sections"] += 1
            stats["chunks"] += len(payload)
            out.extend(payload)
    return out, stats


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--session", default="2025", help="CA legislative session, e.g. 2025")
    ap.add_argument("--out", type=Path, default=None)
    ap.add_argument("--limit", type=int, default=0, help="only N sections (smoke)")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument(
        "--cache",
        type=Path,
        default=None,
        help="download the zip here and read locally. Strongly recommended for a "
             "full run: 161k range requests is ~11h, one 1.11 GB download is minutes.",
    )
    args = ap.parse_args()

    out_path = args.out or _default_out()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    url = session_zip_url(args.session)
    if args.cache:
        print(f"source: {url} (local cache)")
        z = LocalZip(ensure_local(url, args.cache))
        size, etag, lastmod = z.head()
        print(f"  local {size:,} bytes")
    else:
        z = RemoteZip(url)
        size, etag, lastmod = z.head()
        print(f"source: {url}\n  {size:,} bytes  etag={etag}  last-modified={lastmod}")

    records, stats = build_records(z, args.session, limit=args.limit, workers=args.workers)

    # Fresh file per run: the JSONL must represent EXACTLY this run's corpus or
    # --reconcile cannot tell a superseded chunk from a live one.
    with open(out_path, "w", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")

    print("\n=== done ===")
    print(f"  sections     : {stats['sections']:,}")
    print(f"  chunks       : {stats['chunks']:,}")
    print(f"  multi_version: {stats.get('multi_version', 0):,} (versions combined into one section)")
    print(f"  no_lob       : {stats.get('no_lob', 0):,}")
    print(f"  empty_text   : {stats.get('empty_text', 0):,}")
    print(f"  no_chain     : {stats.get('no_chain', 0):,}")
    print(f"  errors       : {stats.get('error', 0):,}")
    if stats["unknown_tags"]:
        print(f"  UNKNOWN CAML TAGS (text may be dropped): {sorted(stats['unknown_tags'])}")
    print(f"  wrote       : {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
