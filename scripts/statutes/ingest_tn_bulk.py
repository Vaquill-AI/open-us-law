#!/usr/bin/env python3
"""Ingest the full Tennessee Code Annotated from Justia, fetched via ScrapFly.

Why this shape (source-access assessment, 2026-07-19)
-----------------------------------------------------
TN was 11,199 sections (~23-28%), titles 1-40 only, titles 41-71 entirely
missing (Insurance 56, Motor Vehicles 55, Taxes 67, Property 66, Welfare 71, ...).
The TCA is published by LexisNexis; there is no official free zip/API. The only
complete, current free mirror is Justia (2024 edition, full body text), but
Justia Cloudflare-403s our own residential proxy on every request. Exa clears
Cloudflare only for pages ALREADY in its cache (a cache-reader, not a crawler:
obscure titles 41-71 shell), and Wayback holds only ~9.7k stale sections.

Resolution: fetch Justia through ScrapFly (`ar_bulk.client.scrapfly_html`,
`asp=true` datacenter proxy, ~1 credit/page, `cache=true` -> free re-fetch). It
returns Justia's REAL HTML past Cloudflare, so the TOC walk and every section
(including 41-71) render deterministically. `cost_budget=2` forbids ScrapFly
from escalating to the 25/40/80-credit residential+browser tiers, and the free
tier allows 5 concurrent connections, so ``--workers 5``.

Pipeline (mirrors ingest_wi_bulk.py, per-section-fetch class):
    enumerate section URLs (crawl.discover: ScrapFly TOC BFS over real HTML)
    -> for each: ScrapFly-fetch HTML -> ar_bulk.parse.extract_body_html
    -> synthetic Node -> node_to_payload.node_to_chunks -> JSONL

Reusing node_to_chunks makes act_id / point_id / citation / breadcrumb /
chunking / R2 text upload match the scraper path byte-for-byte. act_id is
``STATE_TN_T<title>_C<chapter>_S<section>`` (title+chapter from the URL path);
the existing titles 1-40 reproduce at 100%, so an act_id-scoped ``--reconcile``
is safe and never touches the 148 ``document_type=constitution`` points on
``state=tn``.

Requires SCRAPFLY_API_KEY in the environment (see .env.example). Run:
    SCRAPFLY_API_KEY=... python scripts/statutes/ingest_tn_bulk.py --workers 5
    then: lib/embed_and_upsert.py --input .../state_tn_statutes.jsonl            (additive)
          lib/embed_and_upsert.py --input .../state_tn_statutes.jsonl --reconcile (dry-run first)
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

from ar_bulk import client as arc
from ar_bulk import parse as arp
from tn_bulk import crawl, scrapfly
from tn_bulk.walk import TNSection

STATE = "tn"

# TCA titles 1-71. Title 14 (COVID-19) terminated and 19 is RESERVED; both are
# harmless to enumerate (they resolve to few/zero sections) and are kept so the
# set is the honest official universe.
ALL_TITLES = [str(i) for i in range(1, 72)]


def _default_out() -> Path:
    override = os.environ.get("STATE_CHUNKS_DIR_OVERRIDE")
    base = Path(override) if override else (_SCRAPERS / "data" / "state_chunks")
    return base / f"state_{STATE}_statutes.jsonl"


def _seckey(sec_num: str):
    """Sort key for a TCA section number, total-ordered across mixed segments.

    Justia sub-slugs mix numeric and alpha segments (``39-13-202`` vs
    ``39-13-204-d-1``), so a naive ``int(p) if p.isdigit() else p`` tuple raises
    ``TypeError: '<' not supported between 'str' and 'int'`` when two sections
    differ in segment type at the same position. Every element is normalized to a
    uniform ``(type_rank, int, str)`` triple so any two keys are comparable.
    """
    parts = sec_num.replace("-", ".").split(".")
    return tuple((0, int(p), "") if p.isdigit() else (1, 0, p) for p in parts)


def _titlekey(title: str):
    """Numeric-first title sort (TCA titles are 1-71; tolerate an alpha suffix)."""
    m = re.match(r"(\d+)([A-Za-z]*)$", title or "")
    return (int(m.group(1)), m.group(2)) if m else (9999, title or "")


def _sort_sections(secs) -> list[TNSection]:
    """Deduped (by URL) + ordered sections. Dedupe guards a resumed walk that
    re-appended a title, and cross-references reachable from two TOC paths."""
    uniq: dict[str, TNSection] = {}
    for s in secs:
        uniq.setdefault(s.url, s)
    return sorted(uniq.values(), key=lambda s: (_titlekey(s.title), _seckey(s.section_number)))


def _url_row(s: TNSection) -> str:
    return json.dumps(
        {"url": s.url, "title": s.title, "chapter": s.chapter, "section": s.section_number}
    )


def _load_urls(path: Path) -> list[TNSection]:
    out: list[TNSection] = []
    with open(path, encoding="utf-8") as fh:
        for ln in fh:
            try:
                d = json.loads(ln)
            except Exception:  # noqa: BLE001
                continue
            out.append(
                TNSection(
                    url=d["url"], title=d["title"], chapter=d["chapter"], section_number=d["section"]
                )
            )
    return out


def enumerate_sections(
    titles: list[str], workers: int, urls_out: Path | None, reuse: bool = True
) -> list[TNSection]:
    """Walk the TOC, CHECKPOINTING each title's sections to ``urls_out`` as it lands.

    The walk is the expensive, non-cacheable half (~4.5k ScrapFly credits, ~35 min
    for all 71 titles), so it is resumable at TITLE granularity:

      * each title's sections are appended + flushed the moment that title's walk
        finishes (never batched to the end, and never held only in memory),
      * on restart, titles already present in ``urls_out`` are skipped.

    So a crash costs at most the one title in flight. An earlier version wrote the
    list only after all 71 titles AND after a sort, and a sort crash discarded a
    fully completed walk.
    """
    existing: list[TNSection] = []
    done_titles: set[str] = set()
    if reuse and urls_out and urls_out.exists():
        existing = _load_urls(urls_out)
        done_titles = {s.title for s in existing}

    todo = [t for t in titles if t not in done_titles]
    if done_titles:
        print(
            f"phase 1: resuming, {len(done_titles)} titles already walked "
            f"({len(existing):,} sections); {len(todo)} titles to go",
            flush=True,
        )
    if not todo:
        print(f"phase 1: SKIPPED, all {len(titles)} titles already enumerated", flush=True)
        return _sort_sections(existing)

    print(f"phase 1: enumerating section URLs for {len(todo)} titles...", flush=True)
    t0 = time.time()
    found: list[TNSection] = list(existing)
    fh = None
    if urls_out:
        urls_out.parent.mkdir(parents=True, exist_ok=True)
        # Append when continuing a checkpointed walk; truncate on a forced fresh
        # walk so --reenumerate cannot double-write the old rows.
        fh = open(urls_out, "a" if done_titles else "w", encoding="utf-8")

    def _on_title(title: str, secs: list[TNSection], stats: dict) -> None:
        found.extend(secs)
        if fh is not None:
            for s in secs:
                fh.write(_url_row(s) + "\n")
            fh.flush()  # checkpoint: this title can never be lost now
        print(
            f"  title {title:>2}: {len(secs):>5} sections  ({stats.get('seconds', 0)}s, "
            f"{stats.get('index_fetches', 0)} index fetches, credits={scrapfly.cost_spent()})",
            flush=True,
        )

    try:
        crawl.discover(todo, workers=workers, on_title=_on_title)
    finally:
        if fh is not None:
            fh.close()

    print(
        f"  discovered {len(found):,} sections total in {time.time() - t0:.0f}s "
        f"(scrapfly_cost={scrapfly.cost_spent()})",
        flush=True,
    )
    return _sort_sections(found)


def build_records(sections: list[TNSection], workers: int, out_path: Path, append: bool = False) -> dict:
    """Fetch + chunk each section, STREAM-writing chunks to ``out_path``.

    Stream-write (not accumulate-then-write) is mandatory: a 46k-section run that
    only flushes at the end loses everything on a crash (the AR lesson). The
    ``as_completed`` loop runs on the main thread, so a single open handle needs
    no lock. ``append`` continues a resumed run (session mode has no ScrapFly
    cache, so resume-skip is how a restart avoids re-paying credits).
    """
    from src.utils.pydanticModels import Node, NodeText
    from vaquill_pipeline.node_to_payload import node_to_chunks

    year = time.gmtime().tm_year
    stats = {"sections": 0, "chunks": 0, "no_text": 0, "miss": 0, "wayback": 0, "fetch_err": 0}

    def _fetch_html(sec: TNSection) -> tuple[str, bool]:
        """(html, from_wayback). ScrapFly (sticky session) first; Wayback on non-render."""
        html = scrapfly.fetch_html(sec.url, cost_budget=2)
        if html and scrapfly.is_rendered(html):
            return html, False
        wb = arc.wayback_html(sec.url)
        if wb and scrapfly.is_rendered(wb):
            return wb, True
        return "", False

    def _one(sec: TNSection):
        try:
            html, from_wb = _fetch_html(sec)
        except Exception as exc:  # noqa: BLE001
            return ("fetch_err", f"{sec.section_number}: {str(exc)[:120]}", None, False)
        if not html:
            return ("miss", None, None, False)
        name, paras = arp.extract_body_html(html, sec.section_number)
        body = "\n".join(paras).strip()
        if not body or len(body) < 20:
            return ("no_text", None, None, from_wb)
        status = arp.section_status(name, paras)
        name = name or sec.section_name
        node_id = sec.node_id()
        node = Node(
            id=node_id,
            link=sec.url,
            node_type="content",
            level_classifier="section",
            number=sec.section_number,
            node_name=name or f"§ {sec.section_number}",
            node_text=NodeText(),
            citation=sec.citation(),
            top_level_title=sec.title,
            parent="/".join(node_id.split("/")[:-1]),
            status=status,
        )
        node.node_text.add_paragraph(text=body)
        try:
            chunks = node_to_chunks(node, STATE, year)
        except Exception as exc:  # noqa: BLE001
            return ("fetch_err", f"{sec.section_number}: node_to_chunks: {str(exc)[:120]}", None, from_wb)
        return ("ok", None, chunks, from_wb)

    print(f"phase 2: fetching + chunking {len(sections):,} sections, {workers} workers...", flush=True)
    t0 = time.time()
    done = 0
    # Fresh file per run so --reconcile sees exactly this run's corpus (unless
    # resuming, where we append to the partial file).
    with open(out_path, "a" if append else "w", encoding="utf-8") as fh:
        with ThreadPoolExecutor(max_workers=workers) as ex:
            for fut in as_completed(ex.submit(_one, s) for s in sections):
                kind, msg, chunks, from_wb = fut.result()
                done += 1
                if from_wb:
                    stats["wayback"] += 1
                if done % 500 == 0:
                    rate = done / max(time.time() - t0, 0.001)
                    print(f"  {done:,}/{len(sections):,}  {rate:.1f}/s  sections={stats['sections']:,} miss={stats['miss']} cost={scrapfly.cost_spent()}", flush=True)
                if kind == "ok":
                    stats["sections"] += 1
                    stats["chunks"] += len(chunks)
                    for rec in chunks:
                        fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
                    fh.flush()
                else:
                    stats[kind] = stats.get(kind, 0) + 1
                    if kind == "fetch_err" and stats[kind] <= 8:
                        print(f"  {msg}", flush=True)
    return stats


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", type=Path, default=None)
    ap.add_argument("--titles", nargs="*", default=None, help="subset of titles (default: all 1-71)")
    ap.add_argument("--limit", type=int, default=0, help="only first N sections (smoke)")
    ap.add_argument("--workers", type=int, default=5, help="ScrapFly free-tier concurrency is 5")
    ap.add_argument("--urls-out", type=Path, default=None, help="also write the enumerated section URL list")
    ap.add_argument("--resume", action="store_true", help="skip sections already in --out and append")
    ap.add_argument("--reenumerate", action="store_true", help="force a fresh TOC walk even if --urls-out exists")
    args = ap.parse_args()

    titles = args.titles or ALL_TITLES
    out_path = args.out or _default_out()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Enumeration resume is title-granular: titles already checkpointed into
    # --urls-out are skipped, so a restart re-walks only what is missing.
    sections = enumerate_sections(
        titles, args.workers, args.urls_out, reuse=not args.reenumerate
    )

    append = False
    if args.resume and out_path.exists():
        done: set[str] = set()
        with open(out_path, encoding="utf-8") as fh:
            for ln in fh:
                try:
                    sn = json.loads(ln).get("metadata", {}).get("section_number")
                except Exception:  # noqa: BLE001
                    sn = None
                if sn:
                    done.add(sn)
        before = len(sections)
        sections = [s for s in sections if s.section_number not in done]
        append = True
        print(f"  resume: {len(done):,} sections already done, {before - len(sections):,} skipped, {len(sections):,} remaining", flush=True)

    if args.limit:
        sections = sections[: args.limit]

    stats = build_records(sections, args.workers, out_path, append=append)

    titles_seen = sorted({s.title for s in sections}, key=int)
    print("\n=== done ===")
    print(f"  titles     : {len(titles_seen)} ({titles_seen[0]}..{titles_seen[-1]})")
    print(f"  sections   : {stats['sections']:,}")
    print(f"  chunks     : {stats['chunks']:,}")
    print(f"  via wayback: {stats.get('wayback', 0):,}  (ScrapFly non-render, backfilled from snapshot)")
    print(f"  miss       : {stats.get('miss', 0):,}  (neither ScrapFly nor Wayback rendered)")
    print(f"  no_text    : {stats.get('no_text', 0):,}")
    print(f"  fetch_err  : {stats.get('fetch_err', 0):,}")
    print(f"  scrapfly_credits: {scrapfly.cost_spent():,}")
    print(f"  wrote      : {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
