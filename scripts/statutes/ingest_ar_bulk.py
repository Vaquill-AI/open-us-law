#!/usr/bin/env python3
"""Ingest the full Arkansas Code Annotated from Justia's current edition via Exa.

The old AR scraper (state_scrapers/.../ar/statutes/scrapeAR.py) pulled from
Wayback Machine snapshots of Justia's 2023 edition and stalled at 46 distinct
sections: Wayback's CDX coverage of Justia AR is sparse (~9,800 sections across
mixed 2012-2023 editions, roughly a third of the code) and stale. This replaces
it with Justia's complete, current (2024) edition, read through the Exa
``/contents`` API.

Why Exa (the source decision)
-----------------------------
Every free path to a COMPLETE, CURRENT Arkansas Code was evaluated and rejected:

  * Justia direct / via Webshare US proxy -- HTTP 403 (Cloudflare) from both the
    box IP and the residential proxy pool; no ZenRows key on the box.
  * advance.lexis.com (the official Lexis publisher) -- Akamai JS shell, ~3.7 KB
    bootstrap with no code body.
  * codes.findlaw.com/ar -- section text is JS-rendered (Next.js); TOC pages ship
    no static section links (this is the failure class the old scrape hit).
  * arkleg.state.ar.us -- reachable via the residential proxy now, but serves only
    Acts/Bills navigation and links out to Lexis for the code text.
  * Wayback-Justia -- free and reachable but ~35% complete and edition-mixed.

Exa's ``/contents`` endpoint renders Justia for us (defeating Cloudflare) and
resolves the no-year URL to the current 2024 edition. One call per page returns
both the section body (``text``) and every child link (``extras.links``), at
$1 per 1,000 pages, so the whole code is ~$30. See ar_bulk/client.py.

Pipeline (mirrors ingest_la_bulk.py):
    Phase 1 (discover) -- BFS from /codes/arkansas/ down the Title -> [Subtitle]
      -> Chapter -> [Subchapter] -> Section tree via extras.links, collecting
      every section URL. Only current-edition (no year prefix) links one segment
      deep are followed, so cross-references and old editions are never walked.
    Phase 2 (chunk) -- fetch each section, parse heading + body, build a synthetic
      Node, node_to_payload.node_to_chunks -> JSONL.

Reusing node_to_chunks means act_id / point_id / citation / breadcrumb /
chunking / R2 upload match the scraper path. The node id path
``title={t}/chapter={c}/[subchapter={sc}/]section={full}`` yields act_id
``STATE_AR_T{t}_C{c}[_S{sc}]_S{full}`` and citation ``Ark. Code Ann. § {full}``.

Cutover: the existing 46 FindLaw-format stubs have inconsistent act_ids that do
NOT reproduce, so this cuts over STATE-SCOPED (document_type=statute), which
preserves the 236 Ark. constitution points under state=ar. Typical run on the
scraper box (Exa needs no Webshare proxy, so no --workers pressure on the shared
proxy ceiling; R2 upload of section text still wants the pool bump):

    docker exec -d -e VAQUILL_R2_UPLOAD=1 -e VAQUILL_R2_POOL=64 \
      -w /app vaquill-scraper-worker \
      python -u scripts/us_corpus/statutes/ingest_ar_bulk.py --workers 16 \
        > /app/ar_ingest.log 2>&1
    then: embed_and_upsert.py --input .../state_ar_statutes.jsonl              (additive)
          ar_bulk/verify_act_ids.py (coverage gate)
          --reconcile-state ar --min-run-points N                             (cutover)
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

from ar_bulk import client as C
from ar_bulk import parse as P
from ar_bulk import walk as W

STATE = "ar"
ROOT_URL = "https://law.justia.com/codes/arkansas/"


def _default_out() -> Path:
    override = os.environ.get("STATE_CHUNKS_DIR_OVERRIDE")
    base = Path(override) if override else (_SCRAPERS / "data" / "state_chunks")
    return base / f"state_{STATE}_statutes.jsonl"


def _toc_children(url: str, source: str, cache_only: bool) -> list[str]:
    """Child links of a TOC page, from the selected source's real page.

    scrapfly -> parse Justia's raw HTML (`<a href>`); exa -> Exa `extras.links`.
    Returns the raw arkansas link list; the caller filters to one-segment children.
    """
    if source == "scrapfly":
        html = C.scrapfly_html(url)
        return P.links_from_html(html) if html else []
    _, links = C.node(url, max_age_hours=(-1 if cache_only else None))
    return links


def discover_sections(
    workers: int,
    titles: list[str] | None,
    max_nodes: int,
    wayback_enum: bool = True,
    cache_only: bool = True,
    source: str = "exa",
    sections_allow: set[str] | None = None,
) -> tuple[list[str], dict]:
    """Enumerate the AR section universe as current-edition Justia URLs.

    BFS-walks Justia's tree via Exa (cache-only when ``cache_only``, so it never
    burns time on Cloudflare-blocked livecrawls), then unions the result with the
    Internet Archive's captured section URLs (``wayback_enum``), deduped by
    section number. A section page is collected; a TOC page is recursed. Runs one
    BFS level at a time so progress is legible and the visited set stays bounded.
    """
    walk_age = -1 if cache_only else None  # cache-only avoids futile Cloudflare livecrawls

    # Incremental refresh: an allowlist of section numbers (e.g. the sections a
    # legislative session amended) auto-scopes the walk to just the affected
    # titles, so a refresh crawls a handful of titles instead of all 28.
    if sections_allow and not titles:
        titles = sorted({s.split("-", 1)[0] for s in sections_allow if "-" in s})
        print(
            f"section allowlist: {len(sections_allow)} sections across titles {titles}", flush=True
        )

    # Seed frontier: either the whole code (root) or a subset of titles. The AR
    # code has 28 titles; seed them all explicitly and additionally try the root
    # index (in case a title was renumbered). Cloudflare often leaves the root a
    # cache shell, so relying on it alone yields an empty walk.
    if titles:
        frontier = [f"{ROOT_URL}title-{t}/" for t in titles]
    else:
        frontier = [f"{ROOT_URL}title-{t}/" for t in range(1, 29)]
        print("fetching root title index...", flush=True)
        try:
            frontier += P.child_links(ROOT_URL, _toc_children(ROOT_URL, source, cache_only))
        except Exception:
            pass
    frontier = sorted(set(frontier))
    print(f"seed TOC nodes: {len(frontier)}", flush=True)

    visited: set[str] = set()
    # section number -> current-edition URL (first writer wins; Exa-walk URLs are
    # authoritative current-structure URLs, so they seed before the Wayback merge).
    by_number: dict[str, str] = {}
    toc_fetched = 0
    errs = 0
    level = 0

    def _record_section(url: str) -> None:
        cur = W.to_current_url(url)
        skel = W.section_from_url(cur)
        if skel is None:
            return
        if sections_allow is not None and skel.number not in sections_allow:
            return  # incremental: only the amended sections
        by_number.setdefault(skel.number, cur)

    while frontier:
        level += 1
        next_frontier: set[str] = set()
        print(
            f"  level {level}: fetching {len(frontier)} TOC nodes "
            f"(sections so far: {len(by_number):,})",
            flush=True,
        )

        def _fetch(url: str):
            try:
                links = _toc_children(url, source, cache_only)
                return url, P.child_links(url, links), None
            except Exception as exc:
                return url, [], str(exc)[:140]

        with ThreadPoolExecutor(max_workers=workers) as ex:
            for fut in as_completed(ex.submit(_fetch, u) for u in frontier):
                url, kids, err = fut.result()
                visited.add(url)
                if err:
                    errs += 1
                    if errs <= 8:
                        print(f"    toc err {url}: {err}", flush=True)
                    continue
                toc_fetched += 1
                for k in kids:
                    if W.is_section_url(k):
                        _record_section(k)
                    elif k not in visited:
                        next_frontier.add(k)
        if len(visited) + len(by_number) > max_nodes:
            print(f"  WARNING: node cap {max_nodes:,} hit; stopping discovery early", flush=True)
            break
        frontier = sorted(next_frontier)

    exa_walk_count = len(by_number)
    print(
        f"  Exa-cache walk found {exa_walk_count:,} sections "
        f"({toc_fetched:,} TOC nodes rendered, {errs} unrenderable)",
        flush=True,
    )

    # Union with the Internet Archive's captured section universe. Cloudflare
    # blocks Justia's live TOC crawl, so the Exa walk only sees cached pages;
    # Wayback backfills the section universe (deduped by section number).
    wayback_added = 0
    wb_map: dict[str, tuple[str, str]] = {}  # section number -> (original_url, timestamp)
    if wayback_enum:
        print("fetching Wayback CDX section index...", flush=True)
        try:
            for original, ts in C.wayback_section_index():
                cur = W.to_current_url(original)
                skel = W.section_from_url(cur)
                if skel is None:
                    continue
                if sections_allow is not None and skel.number not in sections_allow:
                    continue  # incremental: only the amended sections
                # Keep the newest snapshot per section number for the text fallback.
                wb_map[skel.number] = (original, ts)
                if skel.number not in by_number:
                    by_number[skel.number] = cur
                    wayback_added += 1
            print(
                f"  Wayback added {wayback_added:,} sections not seen by the Exa walk "
                f"({len(wb_map):,} snapshots mapped)",
                flush=True,
            )
        except Exception as exc:
            print(f"  Wayback enumeration failed: {str(exc)[:140]}", flush=True)

    stats = {
        "toc_fetched": toc_fetched,
        "toc_errs": errs,
        "levels": level,
        "exa_walk_sections": exa_walk_count,
        "wayback_sections": wayback_added,
    }
    print(
        f"discovery done: {len(by_number):,} distinct sections "
        f"(exa-walk {exa_walk_count:,} + wayback {wayback_added:,}) "
        f"[scrapfly credits so far: {C.scrapfly_cost_spent():,}]",
        flush=True,
    )
    return sorted(by_number.values()), stats, wb_map


def build_records(
    out_path: Path,
    workers: int,
    titles: list[str] | None,
    limit: int,
    max_nodes: int,
    wayback_enum: bool = True,
    cache_only: bool = True,
    wayback_workers: int = 4,
    source: str = "exa",
    sections_allow: set[str] | None = None,
) -> dict:
    """Two-phase, streaming, crash-safe section fetch.

    Phase A (Exa, ``workers`` threads): pull every section's current-edition text
    from Exa's cache (fast, current 2024). Sections Exa cannot render (Cloudflare
    shell) are queued for Phase B if the Internet Archive has a snapshot.
    Phase B (Wayback, ``wayback_workers`` threads): backfill the Exa-misses from
    the mapped snapshot (raw id_ fetch, no per-URL CDX), gently so archive.org
    does not throttle. Chunks are written to ``out_path`` as they are produced,
    so an interrupted run still leaves a usable JSONL.
    """
    import threading

    from src.utils.pydanticModels import Node, NodeText
    from vaquill_pipeline.node_to_payload import node_to_chunks

    section_urls, disc, wb_map = discover_sections(
        workers,
        titles,
        max_nodes,
        wayback_enum=wayback_enum,
        cache_only=cache_only,
        source=source,
        sections_allow=sections_allow,
    )
    if limit:
        section_urls = section_urls[:limit]
    crawl_year = time.gmtime().tm_year

    stats = {
        "sections": 0,
        "chunks": 0,
        "no_text": 0,
        "not_section": 0,
        "fetch_err": 0,
        "repealed": 0,
        "from_exa": 0,
        "from_wayback": 0,
        "by_title": {},
    }
    fh = open(out_path, "w", encoding="utf-8")
    lock = threading.Lock()

    def _emit(title: str, status: str | None, source: str, chunks: list[dict]) -> None:
        with lock:
            for c in chunks:
                fh.write(json.dumps(c, ensure_ascii=False) + "\n")
            stats["sections"] += 1
            stats["chunks"] += len(chunks)
            stats["by_title"][title] = stats["by_title"].get(title, 0) + 1
            stats["from_exa" if source == "exa" else "from_wayback"] += 1
            if status:
                stats["repealed"] += 1

    def _build_chunks(skel: W.ARSection, heading: str, paras: list[str], url: str, year: int):
        sec = W.ARSection(
            title=skel.title,
            chapter=skel.chapter,
            subchapter=skel.subchapter,
            number=skel.number,
            heading=heading,
        )
        status = P.section_status(heading, paras)
        node_id = sec.node_id()
        # Node.link is a pydantic URL (rejects empty), so pass the fetch URL to
        # satisfy it, then STRIP source_url from the output metadata below so the
        # corpus carries NO third-party source link (source_url -> externalUrl in
        # the API). The reference is the citation + our own R2 section text.
        node = Node(
            id=node_id,
            link=url,
            node_type="content",
            level_classifier="section",
            number=sec.number,
            node_name=sec.heading or f"Ark. Code Ann. § {sec.number}",
            node_text=NodeText(),
            citation=sec.citation(),
            top_level_title=sec.top_level_title(),
            parent="/".join(node_id.split("/")[:-1]),
            status=status,
        )
        for para in paras:
            node.node_text.add_paragraph(text=para)
        chunks = node_to_chunks(node, STATE, year)
        for c in chunks:  # no third-party source link in the corpus
            md = c.get("metadata") or {}
            md["source_url"] = ""
            md["r2_source_url"] = ""
        return sec, status, chunks

    # ---- ScrapFly: single-phase complete current-edition crawl ----
    if source == "scrapfly":
        print(f"SCRAPFLY crawl: {len(section_urls):,} sections, {workers} workers...", flush=True)

        def _sf_one(url: str) -> str:
            skel = W.section_from_url(url)
            if skel is None:
                return "not_section"
            html = C.scrapfly_html(url)
            if not html:
                return "no_text"
            heading, paras = P.extract_body_html(html, skel.number)
            if not paras:
                return "no_text"
            try:
                sec, status, chunks = _build_chunks(skel, heading, paras, url, crawl_year)
            except Exception:
                return "fetch_err"
            if not chunks:
                return "no_text"
            _emit(sec.title, status, "exa", chunks)  # "exa" counter = current-edition
            return "ok"

        t0 = time.time()
        done = 0
        with ThreadPoolExecutor(max_workers=workers) as ex:
            for fut in as_completed(ex.submit(_sf_one, u) for u in section_urls):
                r = fut.result()
                done += 1
                if r in ("not_section", "no_text", "fetch_err"):
                    stats[r] = stats.get(r, 0) + 1
                if done % 500 == 0:
                    rate = done / max(time.time() - t0, 0.001)
                    eta = (len(section_urls) - done) / rate / 60 if rate else 0
                    credits = C.scrapfly_cost_spent()
                    print(
                        f"  {done:,}/{len(section_urls):,}  {rate:.1f}/s  "
                        f"ok={stats['sections']:,}  no_text={stats['no_text']:,}  "
                        f"credits={credits:,} ({credits / max(done, 1):.1f}/pg)  ETA={eta:.1f}m",
                        flush=True,
                    )
        fh.close()
        stats["discovery"] = disc
        return stats

    # ---- Phase A: Exa current-edition cache ----
    ages = (-1,) if cache_only else (None, 0, 0)
    wayback_todo: list[str] = []
    todo_lock = threading.Lock()

    def _exa_one(url: str) -> str:
        skel = W.section_from_url(url)
        if skel is None:
            return "not_section"
        text = ""
        for age in ages:
            try:
                text, _ = C.node(url, max_chars=150000, max_age_hours=age)
            except C.ExaError:
                text = ""
            if C.is_rendered(text):
                break
        if C.is_rendered(text):
            paras = P.extract_body(text)
            if paras:
                parsed = P.parse_heading(text)
                heading = parsed[2] if parsed else ""
                year = int(parsed[1]) if parsed else crawl_year
                try:
                    sec, status, chunks = _build_chunks(skel, heading, paras, url, year)
                except Exception:
                    return "fetch_err"
                if chunks:
                    _emit(sec.title, status, "exa", chunks)
                    return "exa"
        # Exa could not render it: queue for Wayback if a snapshot exists.
        if skel.number in wb_map:
            with todo_lock:
                wayback_todo.append(url)
            return "queued"
        return "no_text"

    print(f"PHASE A (Exa cache): {len(section_urls):,} sections, {workers} workers...", flush=True)
    t0 = time.time()
    done = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for fut in as_completed(ex.submit(_exa_one, u) for u in section_urls):
            r = fut.result()
            done += 1
            if r in ("not_section", "no_text", "fetch_err"):
                stats[r] = stats.get(r, 0) + 1
            if done % 1000 == 0:
                rate = done / max(time.time() - t0, 0.001)
                print(
                    f"  A {done:,}/{len(section_urls):,}  {rate:.1f}/s  "
                    f"exa={stats['from_exa']:,}  queued={len(wayback_todo):,}",
                    flush=True,
                )

    # ---- Phase B: Wayback backfill of Exa-misses (gentle on archive.org) ----
    print(
        f"PHASE B (Wayback): {len(wayback_todo):,} sections, {wayback_workers} workers...",
        flush=True,
    )

    def _wb_one(url: str) -> str:
        skel = W.section_from_url(url)
        if skel is None:
            return "not_section"
        wb = wb_map.get(skel.number)
        if wb is None:
            return "no_text"
        html = C.wayback_raw_fetch(wb[0], wb[1])
        if not html:
            return "no_text"
        heading, paras = P.extract_body_html(html, skel.number)
        if not paras:
            return "no_text"
        try:
            sec, status, chunks = _build_chunks(skel, heading, paras, url, crawl_year)
        except Exception:
            return "fetch_err"
        if not chunks:
            return "no_text"
        _emit(sec.title, status, "wayback", chunks)
        return "wayback"

    t1 = time.time()
    dn = 0
    with ThreadPoolExecutor(max_workers=wayback_workers) as ex:
        for fut in as_completed(ex.submit(_wb_one, u) for u in wayback_todo):
            r = fut.result()
            dn += 1
            if r in ("no_text", "fetch_err", "not_section"):
                stats[r] = stats.get(r, 0) + 1
            if dn % 500 == 0:
                rate = dn / max(time.time() - t1, 0.001)
                eta = (len(wayback_todo) - dn) / rate / 60 if rate else 0
                print(
                    f"  B {dn:,}/{len(wayback_todo):,}  {rate:.1f}/s  "
                    f"wayback={stats['from_wayback']:,}  ETA={eta:.1f}m",
                    flush=True,
                )

    fh.close()
    stats["discovery"] = disc
    return stats


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", type=Path, default=None)
    ap.add_argument("--limit", type=int, default=0, help="only first N section urls (smoke test)")
    ap.add_argument("--workers", type=int, default=16, help="Exa (Phase A) fetch workers")
    ap.add_argument(
        "--wayback-workers",
        type=int,
        default=4,
        help="Wayback (Phase B) workers; keep low so archive.org does not throttle",
    )
    ap.add_argument(
        "--titles", type=str, default="", help="comma list of titles to restrict to, e.g. 5,26"
    )
    ap.add_argument(
        "--max-nodes", type=int, default=400000, help="safety cap on total nodes discovered"
    )
    ap.add_argument(
        "--no-wayback",
        action="store_true",
        help="skip Wayback CDX enumeration + fallback (Exa-cache only)",
    )
    ap.add_argument(
        "--livecrawl",
        action="store_true",
        help="try Exa livecrawl too (only useful once a Cloudflare-solving key is configured)",
    )
    ap.add_argument(
        "--source",
        choices=["exa", "scrapfly"],
        default="exa",
        help="scrapfly = complete + current via ScrapFly asp (needs SCRAPFLY_API_KEY); "
        "exa = partial via Exa cache + Wayback",
    )
    ap.add_argument(
        "--sections",
        type=str,
        default="",
        help="INCREMENTAL refresh: comma list of section numbers to re-crawl "
        "(e.g. 5-10-102,26-51-101). Auto-scopes the walk to the affected titles; "
        "pair with embed_and_upsert.py --reconcile (act_id-scoped) to update only these.",
    )
    ap.add_argument(
        "--sections-file",
        type=str,
        default="",
        help="INCREMENTAL refresh: file with one section number per line (the amended list).",
    )
    args = ap.parse_args()

    titles = [t.strip() for t in args.titles.split(",") if t.strip()] or None
    sections_allow: set[str] | None = None
    allow = [s.strip() for s in args.sections.split(",") if s.strip()]
    if args.sections_file:
        with open(args.sections_file, encoding="utf-8") as fh:
            allow += [ln.strip() for ln in fh if ln.strip() and not ln.startswith("#")]
    if allow:
        sections_allow = set(allow)
    out_path = args.out or _default_out()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    # ScrapFly renders the whole tree, so Wayback is unnecessary in that mode.
    use_wayback = (not args.no_wayback) and args.source != "scrapfly"

    # Fresh file per run (streamed as records are produced) so a state-scoped
    # reconcile sees exactly this run's corpus and an interrupted run is usable.
    stats = build_records(
        out_path,
        args.workers,
        titles,
        args.limit,
        args.max_nodes,
        wayback_enum=use_wayback,
        cache_only=not args.livecrawl,
        wayback_workers=args.wayback_workers,
        source=args.source,
        sections_allow=sections_allow,
    )

    distinct_titles = len(stats["by_title"])
    print("\n=== done ===")
    print(f"  sections   : {stats['sections']:,}")
    print(f"  titles      : {distinct_titles} covered")
    print(
        f"  by title    : {dict(sorted(stats['by_title'].items(), key=lambda kv: int(kv[0][:2] if kv[0][:2].isdigit() else kv[0][:1])))}"
    )
    print(f"  from Exa    : {stats.get('from_exa', 0):,}  (current 2024 edition)")
    print(f"  from Wayback: {stats.get('from_wayback', 0):,}  (archived edition)")
    print(f"  repealed   : {stats.get('repealed', 0):,}")
    print(f"  chunks     : {stats['chunks']:,}")
    print(f"  not_section: {stats.get('not_section', 0):,}")
    print(
        f"  no_text    : {stats.get('no_text', 0):,}  (neither Exa cache nor Wayback rendered a body)"
    )
    print(f"  fetch_err  : {stats.get('fetch_err', 0):,}")
    if args.source == "scrapfly":
        print(f"  ScrapFly credits used: {C.scrapfly_cost_spent():,}")
    print(f"  wrote      : {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
