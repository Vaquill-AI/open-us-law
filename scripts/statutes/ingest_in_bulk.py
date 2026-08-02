#!/usr/bin/env python3
"""Ingest the full Indiana Code from the state's official bulk HTML export.

Source: "Current Indiana Code (all titles): ZIP (HTML only)" from
https://iga.in.gov/ic/{year}/{year}-Indiana-Code-html.zip. The file is one HTML
file per title, every section carrying its full Title-Article-Chapter-Section id
on the div, so the hierarchy is read directly rather than scraped. iga.in.gov
GEO-FENCES this ZIP (a non-US client, including the box's direct egress, gets a
691-byte SPA shell; a US exit gets the real ~43 MB zip), so with no --src the
ingest auto-downloads it through the Webshare US-rotate proxy (see in_bulk/
download.py). The downloads page is a JS SPA that hides the file URL, which is
why the year-templated URL is hardcoded there rather than discovered.

Pipeline (mirrors ingest_va_bulk.py / ingest_ilga_bulk.py):
    [download + extract ZIP via US proxy, unless --src is given]
    title HTML files -> section blocks (id + heading + body paragraphs)
    -> synthetic Node -> node_to_payload.node_to_chunks -> JSONL

Reusing node_to_chunks means act_id / point_id / citation / chunking / R2 upload
match the scraper path exactly:
    35-42-1-1 -> STATE_IN_T35_A42_C1_S35-42-1-1 -> Ind. Code § 35-42-1-1

The point_id is content-addressed (act_id + chunk_index + text hash, not the
year), so a refresh re-embeds unchanged sections to the same id and an
act_id-scoped --reconcile deletes only superseded points. This is a bulk state
like CA/NJ, fully auto-refreshable.

Run inside the scraper container (auto-downloads via the US proxy):
    docker exec -d -e VAQUILL_USE_PROXY=1 -e VAQUILL_R2_UPLOAD=1 \
      -e VAQUILL_R2_POOL=64 -w /app vaquill-scraper-worker \
      python -u scripts/us_corpus/statutes/ingest_in_bulk.py \
        --workers 32 > /app/in_ingest.log 2>&1
    # or, from a local copy of the extracted export:
    #   ... ingest_in_bulk.py --src /app/in_code_html --workers 32
    then: embed_and_upsert.py --input .../state_in_statutes.jsonl [--reconcile]
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

from in_bulk import parse as P
from in_bulk import walk as W

STATE = "in"


def _default_out() -> Path:
    override = os.environ.get("STATE_CHUNKS_DIR_OVERRIDE")
    base = Path(override) if override else (_SCRAPERS / "data" / "state_chunks")
    return base / f"state_{STATE}_statutes.jsonl"


def _title_files(src: Path) -> list[Path]:
    """Every title HTML file, in numeric title order (7.1 sorts after 7).

    Accepts either a dir that directly holds the per-title *.html files or an
    export root with a ``{year}_Indiana_Code_HTML`` subdir (any edition year).
    """
    from in_bulk.download import _extract_root

    html_dir = src if any(src.glob("*.html")) else _extract_root(src)
    files = list(html_dir.glob("*.html"))
    if not files:
        raise SystemExit(f"no title HTML files under {html_dir}")

    def _key(p: Path) -> float:
        try:
            return float(p.stem)
        except ValueError:
            return 1e9

    return sorted(files, key=_key)


def build_records(src: Path, session: str, workers: int, limit: int = 0):
    from src.utils.pydanticModels import Node, NodeText
    from vaquill_pipeline.node_to_payload import node_to_chunks

    year = int(session) if session.isdigit() else time.gmtime().tm_year

    # Phase 1: parse every title file into flat section tasks (fast, local).
    tasks: list[tuple[W.INSection, list[str]]] = []
    files = _title_files(src)
    print(f"parsing {len(files)} title files...", flush=True)
    bad_ids = 0
    for fp in files:
        html = fp.read_text(encoding="utf-8", errors="replace")
        file_title = P.title_number(html)
        count = 0
        for section_id, section_title, paras, status in P.iter_section_blocks(html):
            if not paras:
                continue
            sec = W.parse_section_id(section_id)
            if sec is None:
                bad_ids += 1
                if bad_ids <= 10:
                    print(f"  [skip] unparseable section id: {section_id!r}", flush=True)
                continue
            tasks.append((sec.with_content(section_title, status), paras))
            count += 1
        print(f"  title {file_title:<5} ({fp.name}): {count:,} sections", flush=True)

    if limit:
        tasks = tasks[:limit]
    print(
        f"parsed {len(tasks):,} sections ({bad_ids} unparseable ids); "
        f"building chunks with {workers} workers...",
        flush=True,
    )

    stats = {"sections": 0, "chunks": 0, "repealed": 0, "no_text": 0, "err": 0}
    out: list[dict] = []

    def _one(task: tuple[W.INSection, list[str]]):
        sec, paras = task
        node_id = sec.node_id()
        node = Node(
            id=node_id,
            link=sec.public_url(session),
            node_type="content",
            level_classifier="section",
            number=sec.citation_number,
            node_name=sec.section_title or f"Section {sec.citation_number}",
            node_text=NodeText(),
            citation=sec.citation(),
            top_level_title=sec.title,
            parent="/".join(node_id.split("/")[:-1]),
            status=sec.status,
        )
        for para in paras:
            node.node_text.add_paragraph(text=para)
        try:
            chunks = node_to_chunks(node, STATE, year)
        except Exception as exc:
            return ("err", f"{sec.citation_number}: {str(exc)[:120]}", None)
        if not chunks:
            return ("no_text", None, None)
        return ("ok", ("repealed" if sec.status == "repealed" else None), chunks)

    t0 = time.time()
    done = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for fut in as_completed(ex.submit(_one, t) for t in tasks):
            kind, tag, chunks = fut.result()
            done += 1
            if done % 2000 == 0:
                rate = done / max(time.time() - t0, 0.001)
                eta = (len(tasks) - done) / rate / 60 if rate else 0
                print(
                    f"  {done:,}/{len(tasks):,}  {rate:.0f}/s  "
                    f"chunks={stats['chunks']:,}  ETA={eta:.1f}m",
                    flush=True,
                )
            if kind == "ok":
                stats["sections"] += 1
                stats["chunks"] += len(chunks)
                if tag == "repealed":
                    stats["repealed"] += 1
                out.extend(chunks)
            else:
                stats[kind] = stats.get(kind, 0) + 1
                if kind == "err" and stats["err"] <= 8:
                    print(f"  {tag}", flush=True)
    return out, stats


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--src",
        type=Path,
        default=None,
        help="dir holding the extracted export (or its {year}_Indiana_Code_HTML "
        "subdir). If omitted, the year's ZIP is downloaded via the US proxy.",
    )
    ap.add_argument(
        "--session",
        default=None,
        help="code edition/session year (default: current year, with prior-year "
        "fallback when auto-downloading)",
    )
    ap.add_argument(
        "--workdir",
        type=Path,
        default=None,
        help="scratch dir for the auto-downloaded + extracted ZIP",
    )
    ap.add_argument("--out", type=Path, default=None)
    ap.add_argument("--limit", type=int, default=0, help="only first N sections (smoke test)")
    ap.add_argument("--workers", type=int, default=32)
    args = ap.parse_args()

    session = args.session or str(time.gmtime().tm_year)
    if args.src:
        src = args.src
    else:
        from in_bulk import download as DL

        workdir = args.workdir or (_SCRAPERS / "data" / "in_download")
        print(f"no --src given: downloading Indiana Code ZIP (session={session})", flush=True)
        src, session = DL.resolve_and_download(session, workdir)
        print(f"using extracted export at {src} (edition {session})", flush=True)

    out_path = args.out or _default_out()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    records, stats = build_records(src, session, args.workers, limit=args.limit)

    # Fresh file per run so a later --reconcile sees exactly this run's corpus.
    with open(out_path, "w", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")

    print("\n=== done ===")
    print(f"  sections   : {stats['sections']:,}")
    print(f"  repealed   : {stats.get('repealed', 0):,}")
    print(f"  chunks     : {stats['chunks']:,}")
    print(f"  no_text    : {stats.get('no_text', 0):,}")
    print(f"  errors     : {stats.get('err', 0):,}")
    print(f"  wrote      : {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
