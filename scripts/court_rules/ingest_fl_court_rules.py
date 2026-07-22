#!/usr/bin/env python3
"""Ingest Florida court rules into statutes_us.

corpus_type='state_rules', document_type='court_rule', act_id prefix
'SRULES_FL_'. Same record shape as the other court-rules ingests; reconcile
stays act_id-scoped and never touches FL statutes/constitution (state='fl').

Source
------
The Florida Bar publishes the official consolidated Rules of Court as ONE
text-layered PDF per chapter, indexed at https://www.floridabar.org/rules/ctproc/.
The index HTML is bot-walled (403 to a plain fetch; a real browser / curl_cffi
chrome impersonation clears it), but the chapter PDFs themselves are served
open (HTTP 200, no wall) from www-media.floridabar.org and the judiciary mirror
flcourts-media.flcourts.gov. So we resolve the current dated PDF URLs from the
index once (curl_cffi + US proxy), falling back to the pinned seed URLs below,
then download + parse each PDF directly.

The Florida Evidence Code is Chapter 90 of the Florida STATUTES, not a court
rule; it is ingested by the statutes pipeline, not here.

Numbering: every chapter uses {chapter}.{3-digit} (e.g. Civil 1.110, Appellate
9.110). Rule bodies are delimited in the PDF by "RULE {n.nnn}." headers; the ToC
front-matter repeats the numbers, so we start each chapter at the first RULE
*body* occurrence.

Dependencies (pymupdf is already on the scraper image):
  - pymupdf/fitz  (PDF text extraction; leak-free, unlike pdfplumber)
  - curl_cffi     (only to resolve fresh index URLs; seeds work without it)

Two phases: resolve+download all chapter PDFs, then parse+chunk all rules.
Output: state_fl_court_rules.jsonl (embed with lib/embed_and_upsert.py).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID

import requests

sys.stdout.reconfigure(line_buffering=True)  # type: ignore[attr-defined]

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = Path(os.environ.get("OUT_DIR", "./data"))
OUT = DATA_DIR / "state_fl_court_rules.jsonl"

FL_INDEX = "https://www.floridabar.org/rules/ctproc/"
UA = "Mozilla/5.0 (open-us-law ingestion bot; +https://github.com/Vaquill-AI/open-us-law)"


# Each chapter: slug -> {chapter number token, name, citation_prefix, seed PDF}.
# The seed PDF is a pinned recent URL used when index resolution is unavailable;
# the weekly refresh re-resolves the current dated URL from the index.
RULE_SETS: dict[str, dict] = {
    "civil": {
        "chapter": "1", "name": "Florida Rules of Civil Procedure",
        "citation_prefix": "Fla. R. Civ. P.",
        "seed": "https://www-media.floridabar.org/uploads/2026/04/Civil-Procedure-Rules-04-01-26.pdf",
    },
    "genprac": {
        "chapter": "2", "name": "Florida Rules of General Practice and Judicial Administration",
        "citation_prefix": "Fla. R. Gen. Prac. & Jud. Admin.",
        "seed": "",
    },
    "criminal": {
        "chapter": "3", "name": "Florida Rules of Criminal Procedure",
        "citation_prefix": "Fla. R. Crim. P.",
        "seed": "",
    },
    "svp": {
        "chapter": "4", "name": "Florida Rules of Civil Procedure for Involuntary Commitment of Sexually Violent Predators",
        "citation_prefix": "Fla. R. Civ. P. Involuntary Commitment",
        "seed": "",
    },
    "probate": {
        "chapter": "5", "name": "Florida Probate Rules",
        "citation_prefix": "Fla. Prob. R.", "seed": "",
    },
    "traffic": {
        "chapter": "6", "name": "Florida Traffic Court Rules",
        "citation_prefix": "Fla. R. Traf. Ct.", "seed": "",
    },
    "smallclaims": {
        "chapter": "7", "name": "Florida Small Claims Rules",
        "citation_prefix": "Fla. Sm. Cl. R.", "seed": "",
    },
    "juvenile": {
        "chapter": "8", "name": "Florida Rules of Juvenile Procedure",
        "citation_prefix": "Fla. R. Juv. P.",
        "seed": "https://flcourts-media.flcourts.gov/content/download/217911/file/Florida-Rules-of-Juvenile-Procedure.pdf",
    },
    "appellate": {
        "chapter": "9", "name": "Florida Rules of Appellate Procedure",
        "citation_prefix": "Fla. R. App. P.",
        "seed": "https://flcourts-media.flcourts.gov/content/download/219033/file/appellate-court-procedures.pdf",
    },
    "family": {
        "chapter": "12", "name": "Florida Family Law Rules of Procedure",
        "citation_prefix": "Fla. Fam. L. R. P.", "seed": "",
    },
    # ADR / court-officer rule sets ("Additional Resources" on the ctproc index).
    # Not chapter-numbered in the 1-12 scheme; they carry their own rule prefixes
    # (mediators 10.x, interpreters 14.x, parenting coordinators 15.x) and use the
    # same "RULE {n.nnn}" body headers, so the standard parser handles them.
    "mediators": {
        "chapter": "10", "name": "Florida Rules for Certified and Court-Appointed Mediators",
        "citation_prefix": "Fla. R. Med.",
        "seed": "https://flcourts-media.flcourts.gov/content/download/1998036/file/FRC&CAM_01.2025%20ADA.pdf",
    },
    "interpreters": {
        "chapter": "14", "name": "Florida Rules for Certification and Regulation of Spoken Language Court Interpreters",
        "citation_prefix": "Fla. R. Interp.",
        "seed": "https://flcourts-media.flcourts.gov/content/download/216676/file/FLORIDA-RULES-FOR-CERTIFICATION-AND-REGULATION-OF-INTERPRETERS.pdf",
    },
    "parenting": {
        "chapter": "15", "name": "Florida Rules for Qualified and Court-Appointed Parenting Coordinators",
        "citation_prefix": "Fla. R. Parent. Coord.",
        "seed": "https://flcourts-media.flcourts.gov/content/download/216760/file/rules-qualified-court-appointed-parenting-coordinators.pdf",
    },
}


def _load_env() -> None:
    env_path = _PROJECT_ROOT / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


_load_env()


def _proxies() -> dict[str, str] | None:
    user = os.environ.get("WEBSHARE_USERNAME", "")
    pwd = os.environ.get("WEBSHARE_PASSWORD", "")
    if not user or not pwd:
        return None
    proxy_user = f"{user}-US-rotate"
    host = os.environ.get("WEBSHARE_PROXY_HOST", "p.webshare.io")
    port = os.environ.get("WEBSHARE_PROXY_PORT", "80")
    url = f"http://{urllib.parse.quote(proxy_user)}:{urllib.parse.quote(pwd)}@{host}:{port}"
    return {"http": url, "https": url}


# ---------------------------------------------------------------------------
def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def _point_id(act_id: str, chunk_idx: int, text: str) -> str:
    seed = f"{act_id}::{chunk_idx}::{_sha1(text)[:12]}"
    return str(UUID(hashlib.md5(seed.encode()).hexdigest()))


def _safe(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "_", s).strip("_")


# ---------------------------------------------------------------------------
# Index resolution (curl_cffi clears the Bar bot-wall; seeds are the fallback)
# ---------------------------------------------------------------------------
def resolve_pdf_urls() -> dict[str, str]:
    """Return slug -> current PDF URL, resolved from the ctproc index when
    possible, else the pinned seed."""
    resolved: dict[str, str] = {s: m["seed"] for s, m in RULE_SETS.items() if m["seed"]}
    try:
        from curl_cffi import requests as cf_requests  # type: ignore
    except Exception:
        print("[FL] curl_cffi unavailable; using pinned seed URLs only", flush=True)
        return resolved
    try:
        r = cf_requests.get(FL_INDEX, impersonate="chrome", proxies=_proxies(),
                            timeout=45, allow_redirects=True)
        if r.status_code != 200:
            print(f"[FL] index status {r.status_code}; using seeds", flush=True)
            return resolved
        hrefs = re.findall(r'href="([^"]+\.pdf)"', r.text, re.I)
        # Map each chapter's PDF by matching the rule-set name keywords. The ADR
        # sets (mediators/interpreters/parenting) are not linked on the ctproc
        # index as chapter PDFs, so they keep their pinned flcourts-media seed.
        kw_map = {
            "civil": "civil", "genprac": "judicial-admin", "criminal": "criminal",
            "svp": "sexually-violent", "probate": "probate", "traffic": "traffic",
            "smallclaims": "small-claims", "juvenile": "juvenile",
            "appellate": "appellate", "family": "family",
        }
        for slug, meta in RULE_SETS.items():
            kw = kw_map.get(slug)
            if not kw:
                continue
            for h in hrefs:
                if kw.replace("-", "") in h.lower().replace("-", ""):
                    resolved[slug] = h if h.startswith("http") else urllib.parse.urljoin(FL_INDEX, h)
                    break
        print(f"[FL] resolved {len(resolved)} PDF URLs from index", flush=True)
    except Exception as e:
        print(f"[FL] index resolution failed ({e}); using seeds", flush=True)
    return resolved


def download_pdf(url: str) -> bytes | None:
    for attempt in range(4):
        try:
            r = requests.get(url, headers={"User-Agent": UA}, timeout=90,
                             proxies=_proxies(), allow_redirects=True)
            if r.status_code == 200 and r.content[:4] == b"%PDF":
                return r.content
            time.sleep(1.5 * (attempt + 1))
        except Exception:
            time.sleep(1.5 * (attempt + 1))
    return None


# ---------------------------------------------------------------------------
# PDF parse -> rules
# ---------------------------------------------------------------------------
@dataclass
class Rule:
    slug: str
    set_name: str
    citation_prefix: str
    rule_id: str        # e.g. "1.110"
    section_title: str  # e.g. "General Rules of Pleading"
    raw_text: str
    source_url: str


# Case-insensitive: the 1-12 chapters print "RULE 1.110" (caps) but the ADR sets
# print "Rule 15.000" (title case). Anchored + the 120-char body filter guard it.
_RULE_HDR_RE = re.compile(r"(?im)^\s*RULE\s+(\d+\.\d+)\.?\s*(.*?)\s*$")


def _pdf_text(pdf_bytes: bytes) -> str:
    import fitz  # PyMuPDF (leak-free; pdfplumber leaks across recurring refreshes)

    parts: list[str] = []
    with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
        for page in doc:
            t = page.get_text("text") or ""
            if t.strip():
                parts.append(t)
    text = "\n".join(parts)
    text = re.sub(r"[ \t]+", " ", text)
    return text


def parse_chapter(slug: str, meta: dict, pdf_bytes: bytes, source_url: str) -> list[Rule]:
    text = _pdf_text(pdf_bytes)
    matches = list(_RULE_HDR_RE.finditer(text))
    if not matches:
        print(f"  [FL {slug}] no RULE headers found", flush=True)
        return []
    # The ToC repeats rule headers before the bodies. Bodies start at the second
    # occurrence of the first rule id (first is the ToC line). Find the split by
    # locating the last occurrence of the first rule id's header cluster: simpler
    # and robust: drop headers whose captured "title" is empty AND whose slice to
    # the next header is trivially short (ToC lines).
    rules: list[Rule] = []
    for i, m in enumerate(matches):
        rid = m.group(1)
        title = m.group(2).strip().rstrip(".")
        start = m.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        body = text[start:end].strip()
        body = re.sub(r"\s+\n", "\n", body).strip()
        # ToC entries: very short slice (just a page number / dotted leader).
        if len(body) < 120:
            continue
        rules.append(Rule(
            slug=slug, set_name=meta["name"], citation_prefix=meta["citation_prefix"],
            rule_id=rid, section_title=title or f"Rule {rid}",
            raw_text=f"RULE {rid}. {title}\n\n{body}" if title else body,
            source_url=source_url,
        ))
    # Deduplicate by rule_id keeping the longest body (guards against a ToC
    # header slipping through with a moderate-length following block).
    best: dict[str, Rule] = {}
    for r in rules:
        cur = best.get(r.rule_id)
        if cur is None or len(r.raw_text) > len(cur.raw_text):
            best[r.rule_id] = r
    out = list(best.values())
    # Completeness check: every rule number that appears as a "RULE n.nnn"
    # header anywhere (ToC + body). Since we keep any occurrence with a >=120-char
    # body, a rule number that is still absent has NO operative body anywhere ==
    # a vacant (repealed/reserved) number, not a parser drop. Report those as
    # vacant so "100%" means 100% of operative, text-bearing rules.
    universe = set(re.findall(r"(?i)RULE\s+(\d+\.\d+)", text))
    emitted = {r.rule_id for r in out}
    vacant = sorted(universe - emitted)
    tag = f"; {len(vacant)} vacant (repealed/reserved, no text): {vacant[:10]}" if vacant else "; ToC-complete"
    print(f"  [FL {slug}] {len(out)} rules{tag}", flush=True)
    return out


# ---------------------------------------------------------------------------
# Record shape (mirrors the other court-rules ingests)
# ---------------------------------------------------------------------------
def _to_chunk_record(rule: Rule) -> dict:
    safe_rid = _safe(rule.rule_id)
    act_id = f"SRULES_FL_{rule.slug.upper()}_R{safe_rid}"
    title_label = "Florida Rules of Court"
    citation = f"{rule.citation_prefix} {rule.rule_id}"
    text_for_embedding = (
        f"{title_label} | {rule.set_name} | {citation}\n"
        f"Rule {rule.rule_id}. {rule.section_title}\n\n{rule.raw_text}"
    )
    md = {
        "act_id": act_id,
        "corpus_type": "state_rules",
        "category": "state_rules",
        "document_type": "court_rule",
        "jurisdiction": "US",
        "country_code": "US",
        "state": "fl",
        "title_name": rule.set_name,
        "title": title_label,
        "title_code": f"fl_{rule.slug}",
        "top_level_title": f"rules-fl-{rule.slug}",
        "level_classifier": "rule",
        "chapter": rule.slug,
        "chapter_name": rule.set_name,
        "subchapter": None,
        "subchapter_name": rule.set_name,
        "section_number": rule.rule_id,
        "section_title": f"Rule {rule.rule_id}. {rule.section_title}",
        "citation": citation,
        "citation_short": citation,
        "display_label": citation,
        "display_title": rule.section_title,
        "display_path": f"{rule.set_name} / Rule {rule.rule_id}",
        "breadcrumb": [title_label, rule.set_name, f"Rule {rule.rule_id}"],
        "sort_key": act_id,
        "act_status": "in_force",
        "renumbered_to": "",
        "transferred_to": "",
        "year": None,
        "word_count": len(rule.raw_text.split()),
        "subsection_count": 0,
        "subsection_letters": [],
        "numbered_paragraph_count": 0,
        "amendments_count": 0,
        "amendment_years": [],
        "last_amended_year": None,
        "cross_references_count": 0,
        "cross_references_usc": [],
        "cross_references_cfr": [],
        "public_laws_count": 0,
        "public_laws_referenced": [],
        "source_url": rule.source_url,
        "parent_id": None,
        "raw_node_id": act_id,
        "full_text_sha1": _sha1(rule.raw_text),
    }
    return {
        "point_id": _point_id(act_id, 0, rule.raw_text),
        "text_for_embedding": text_for_embedding,
        "raw_text": rule.raw_text,
        "metadata": md,
    }


def _write_jsonl(path: Path, rules: list[Rule]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    records: dict[str, dict] = {}
    for r in rules:
        rec = _to_chunk_record(r)
        records[rec["metadata"]["act_id"]] = rec
    with open(path, "w", encoding="utf-8") as fh:
        for rec in records.values():
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
    return len(records)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sets", default="", help="Comma-separated slugs (default: all).")
    ap.add_argument("--out", type=Path, default=OUT)
    args = ap.parse_args()

    slugs = (
        [s.strip() for s in args.sets.split(",") if s.strip()]
        if args.sets else list(RULE_SETS.keys())
    )
    print(f"=== Florida court-rules ingest: {slugs} ===", flush=True)
    urls = resolve_pdf_urls()
    all_rules: list[Rule] = []
    for slug in slugs:
        meta = RULE_SETS[slug]
        url = urls.get(slug)
        if not url:
            print(f"  [FL {slug}] no PDF URL (no seed, index unresolved); SKIP", flush=True)
            continue
        pdf = download_pdf(url)
        if not pdf:
            print(f"  [FL {slug}] PDF download failed: {url}", flush=True)
            continue
        rules = parse_chapter(slug, meta, pdf, url)
        all_rules.extend(rules)
    n = _write_jsonl(args.out, all_rules)
    print(f"\n[FL] done: {len(all_rules)} rules, {n} unique act_ids\n=> {args.out}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
