#!/usr/bin/env python3
"""Additive ingest of Puerto Rico codes from the official OGP Biblioteca Virtual.

This is the OGP-sourced expansion of the PR statute corpus. It complements (does not
replace) `ingest_pr_codes.py`: it reuses that module's proven PDF parse + record builder
so records stay schema-identical to the existing PR corpus, and adds OGP-specific TOC
dedup and inline-LPRA extraction (see pr_bulk/parse.py).

Source: bvirtualogp.pr.gov (official, free, public domain, amendment-consolidated,
current). Language is Spanish, the enacted authoritative language; we never ingest the
copyrighted LexisNexis English LPRA. Citation scheme stays code-article primary
(STATE_PR_{CODE}_{ART|SEC}{n} / "Cod. X P.R. art. N"); the LPRA cite is a SECONDARY
`citation_lpra` field. See FEASIBILITY.md and the statutes-corpus-ingest skill.

Each code in pr_bulk/catalog.py with no existing PR points (e.g. incentivos) is a purely
additive ingest: embed WITHOUT --reconcile. Re-ingesting an existing code later is an
act_id-scoped reconcile within that code's STATE_PR_{CODE}_* prefix, never a
--reconcile-state pr (which would also touch other PR codes).

Usage:
    python scripts/us_corpus/statutes/ingest_pr_bulk.py --codes incentivos
    python scripts/us_corpus/statutes/ingest_pr_bulk.py --dry-run          # parse only
    python scripts/statutes/ingest_pr_bulk.py --out /tmp/pr_bulk.jsonl

Then embed additively:
    python scripts/us_corpus/lib/embed_and_upsert.py --input <OUT>
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

_STATUTES_DIR = Path(__file__).resolve().parent
if str(_STATUTES_DIR) not in sys.path:
    sys.path.insert(0, str(_STATUTES_DIR))

import ingest_pr_codes as legacy
from pr_bulk.catalog import OGP_CODES
from pr_bulk.parse import parse_pdf

sys.stdout.reconfigure(line_buffering=True)  # type: ignore[attr-defined]

OUT_DEFAULT = legacy.DATA_DIR / "state_pr_bulk.jsonl"


def process_code(slug: str, meta: dict) -> list[legacy.Article]:
    """Fetch + parse every part of one OGP code; returns deduped Articles."""
    articles: list[legacy.Article] = []
    for part_label, pdf_url in meta["parts"]:
        pdf_bytes = legacy.fetch_bytes(pdf_url)
        if not pdf_bytes or pdf_bytes[:5] != b"%PDF-":
            got = "no bytes" if not pdf_bytes else f"non-PDF ({pdf_bytes[:5]!r})"
            print(f"  [{slug}] FAIL fetch {pdf_url}: {got}", flush=True)
            continue
        try:
            parts = parse_pdf(pdf_bytes, slug, meta, part_label, pdf_url)
        except Exception as exc:
            print(f"  [{slug}] parse error: {exc}", flush=True)
            continue
        print(f"  [{slug}] {part_label[:45]}: {len(parts)} sections", flush=True)
        articles.extend(parts)
    return articles


# Law titles are often long and WRAP across a line inside the quotes (e.g. "Ley de
# Ejecucion del Plan ... del Departamento de Desarrollo\nEconomico y Comercio de 2018"),
# so allow up to ~200 chars incl. embedded newlines; whitespace is collapsed by the caller.
_TITLE_RE = re.compile(r"[“\"]([^”\"]{6,200})[”\"]")
# Older laws / Plans de Reorganizacion print the title UNQUOTED as the first line(s) before
# the "Ley Num. N de <date>" / "Plan de Reorganizacion Num. N" enactment line.
_ENGLISH_TAG_RE = re.compile(r"^\s*[<«]\s*english\s*[>»]\s*", re.I)
_ENACT_RE = re.compile(
    r"\n\s*(?:Ley\s+N[úu]m\.|Plan\s+de\s+Reorganizaci[óo]n\s+N[úu]m\.|"
    r"Resoluci[óo]n\s+Conjunta\s+N[úu]m\.)"
)


def _extract_title(pdf_text: str) -> str:
    head = _ENGLISH_TAG_RE.sub("", pdf_text[:800])
    m = _TITLE_RE.search(head)
    if m:
        return re.sub(r"\s+", " ", m.group(1)).strip()
    m2 = _ENACT_RE.search(head)  # unquoted: title is everything before the enactment line
    if m2:
        title = re.sub(r"\s+", " ", head[: m2.start()]).strip()
        if 6 <= len(title) <= 200:
            return title
    return ""


# OGP is NOT geo-fenced, so fetch it DIRECT (no Webshare proxy) - the proxy adds the ~20
# ceiling + ~10x latency for nothing. Reused across the named-law sweep.
_DIRECT = None


def _direct_get(url: str) -> bytes | None:
    global _DIRECT
    if _DIRECT is None:
        import requests

        _DIRECT = requests.Session()
        _DIRECT.headers.update({"User-Agent": legacy._MOZ_UA})
    try:
        r = _DIRECT.get(url, timeout=90)
        if r.status_code == 200 and r.content[:5] == b"%PDF-":
            return r.content
    except Exception:
        return None
    return None


def _named_law_meta(stem: str, marker: str, pdf_text: str) -> dict:
    """Auto-derive a code meta dict for a long-tail named OGP law.

    Title is the quoted name on the PDF's first page ("Ley de X" / "Codigo X"); the primary
    citation is the standard "Ley N-Y" form (the real LPRA cite rides along as the secondary
    citation_lpra field). Used for the bulk named-law sweep, not the hand-curated codes.
    """
    num, _, year = stem.partition("-")
    name = _extract_title(pdf_text) or f"Ley {num}-{year} de Puerto Rico"
    return {
        "name": name,
        "name_en": name,
        "citation": f"Ley {num}-{year}",
        "year": int(year) if year.isdigit() else 0,
        "marker": marker,
    }


def process_named_law(stem: str, marker: str) -> list[legacy.Article]:
    """Fetch + parse one long-tail named law (single fetch), auto-deriving its metadata."""
    from pr_bulk.catalog import OGP_BASES

    slug = f"ley_{stem.replace('-', '_')}"
    pdf_bytes, url = None, ""
    for base in OGP_BASES:  # a stem lives in leyesreferencia OR LeyesOrganicas
        url = f"{base}/{stem}.pdf"
        b = _direct_get(url)  # OGP is not geo-fenced -> direct, no proxy
        if b:
            pdf_bytes = b
            break
    if not pdf_bytes:
        print(f"  [{slug}] FAIL fetch {stem} (tried both folders)", flush=True)
        return []
    meta = _named_law_meta(stem, marker, legacy._pdf_text(pdf_bytes))
    try:
        arts = parse_pdf(pdf_bytes, slug, meta, meta["name"][:40], url)
    except Exception as exc:
        print(f"  [{slug}] parse error: {exc}", flush=True)
        return []
    print(f"  [{slug}] {meta['name'][:40]}: {len(arts)} sections", flush=True)
    return arts


def to_record(a: legacy.Article) -> dict:
    """Build the chunk record via the legacy builder, then attach citation_lpra."""
    rec = legacy.to_chunk_record(a)
    rec["metadata"]["citation_lpra"] = getattr(a, "lpra_citation", "") or ""
    rec["metadata"]["source"] = "ogp_bvirtual"
    return rec


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--codes", default="", help="Comma-separated slugs. Default: all OGP.")
    ap.add_argument("--out", type=Path, default=OUT_DEFAULT, help="JSONL output path.")
    ap.add_argument("--dry-run", action="store_true", help="Parse + report counts, no write.")
    ap.add_argument(
        "--named-list",
        type=Path,
        default=None,
        help="JSON [[stem, marker], ...] of long-tail named OGP laws to bulk-ingest with "
        "auto-derived metadata (instead of the hand-curated OGP_CODES catalog).",
    )
    args = ap.parse_args()

    if args.named_list:
        laws = json.loads(args.named_list.read_text())
        print(f"[PR-OGP] named-law sweep: {len(laws)} laws", flush=True)
        t0 = time.time()
        all_articles: list[legacy.Article] = []
        # Parallelize across laws (direct fetch + parse are I/O/CPU light per law); each
        # law's own R2 uploads are already threaded, so keep this pool modest.
        with ThreadPoolExecutor(max_workers=8) as pool:
            for arts in pool.map(lambda lm: process_named_law(lm[0], lm[1]), laws):
                all_articles.extend(arts)
        if args.dry_run:
            print(f"\n=== Dry run: {len(all_articles):,} sections parsed ===")
            return 0
        args.out.parent.mkdir(parents=True, exist_ok=True)
        seen: set[str] = set()
        written = 0
        with open(args.out, "w") as fh:
            for a in all_articles:
                rec = to_record(a)
                if rec["point_id"] in seen:
                    continue
                fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
                seen.add(rec["point_id"])
                written += 1
        print(
            f"\n=== Done: parsed={len(all_articles):,}, written={written:,}, "
            f"elapsed={time.time() - t0:.1f}s ===",
            flush=True,
        )
        print(f"JSONL: {args.out}")
        return 0

    if args.codes:
        wanted = {c.strip() for c in args.codes.split(",") if c.strip()}
        codes = {k: v for k, v in OGP_CODES.items() if k in wanted}
        missing = wanted - set(codes)
        if missing:
            print(f"[PR-OGP] unknown code slugs: {sorted(missing)}", flush=True)
    else:
        # A no-args run ingests every non-pending code. Pending codes (unresolved parser
        # edge case, e.g. municipal) must be named explicitly so they never ship broken.
        codes = {k: v for k, v in OGP_CODES.items() if not v.get("pending")}
        skipped = [k for k, v in OGP_CODES.items() if v.get("pending")]
        if skipped:
            print(
                f"[PR-OGP] skipping pending codes (name explicitly to force): {skipped}", flush=True
            )

    print(f"[PR-OGP] codes: {list(codes.keys())}", flush=True)

    t0 = time.time()
    all_articles: list[legacy.Article] = []
    for slug, meta in codes.items():
        print(f"\n[PR-OGP] === {meta['name']} ===", flush=True)
        arts = process_code(slug, meta)
        all_articles.extend(arts)
        with_lpra = sum(1 for a in arts if getattr(a, "lpra_citation", ""))
        print(
            f"[PR-OGP] {slug}: {len(arts)} sections ({with_lpra} with inline L.P.R.A. cite)",
            flush=True,
        )

    if args.dry_run:
        print(f"\n=== Dry run: {len(all_articles):,} sections parsed, not written ===")
        return 0

    args.out.parent.mkdir(parents=True, exist_ok=True)
    seen: set[str] = set()
    written = 0
    with open(args.out, "w") as fh:  # fresh file per run (reconcile-safe)
        for a in all_articles:
            rec = to_record(a)
            if rec["point_id"] in seen:
                continue
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
            seen.add(rec["point_id"])
            written += 1

    print(
        f"\n=== Done: parsed={len(all_articles):,}, written={written:,}, "
        f"elapsed={time.time() - t0:.1f}s ===",
        flush=True,
    )
    print(f"JSONL: {args.out}")
    print(f"Next: python scripts/us_corpus/lib/embed_and_upsert.py --input {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
