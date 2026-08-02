#!/usr/bin/env python3
"""Ingest Texas court rules into statutes_us.

corpus_type='state_rules', document_type='court_rule', act_id prefix
'SRULES_TX_'. Same record shape as the other court-rules ingests; reconcile
stays act_id-scoped and never touches TX statutes/constitution (state='tx').

Source
------
The Texas Judicial Branch publishes each statewide rule set as ONE text-layered
PDF, indexed at https://www.txcourts.gov/rules-forms/rules-standards/. txcourts.gov
is GEO-FENCED behind an Azure Front Door WAF (403 to non-US IPs, including the
Hetzner scraper box) but is otherwise open: from a US IP every request is a plain
PDF GET, no JS challenge. So ALL fetches here route through the Webshare US-rotate
residential proxy. Media-ID URLs (/media/<id>/...pdf) rotate on every amendment,
so we resolve the current URLs from the index each run, falling back to pinned
seeds.

Scope note: Texas has NO "Rules of Criminal Procedure" as a court-rules set;
criminal procedure is the Code of Criminal Procedure, a STATUTE (ingested by the
statutes pipeline). The court-rules corpus is Civil, Appellate, Evidence, plus
the administrative/ethics sets.

Numbering: TRCP uses sparse integers with lettered inserts (21, 21a, 108a); TRE
uses article-based 3-digit numbers (401, 803); TRAP uses integers + decimals
(9, 31.8). Rule bodies are delimited by "RULE {n}" / "Rule {n}" headers.

Dependencies (pymupdf is already on the scraper image):
  - pymupdf/fitz  (PDF text extraction; leak-free, unlike pdfplumber)
  - curl_cffi     (optional; only to resolve fresh index URLs. Seeds work without it)

Output: state_tx_court_rules.jsonl (embed with lib/embed_and_upsert.py).
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
OUT = DATA_DIR / "state_tx_court_rules.jsonl"

TX_INDEX = "https://www.txcourts.gov/rules-forms/rules-standards/"
UA = "Mozilla/5.0 (open-us-law ingestion bot; +https://github.com/Vaquill-AI/open-us-law)"


RULE_SETS: dict[str, dict] = {
    "civil": {
        "name": "Texas Rules of Civil Procedure",
        "citation_prefix": "Tex. R. Civ. P.",
        "seed": "https://www.txcourts.gov/media/1462348/texas-rules-of-civil-procedure-march-1-2026.pdf",
        "index_kw": "rules-of-civil-procedure",
    },
    "appellate": {
        "name": "Texas Rules of Appellate Procedure",
        "citation_prefix": "Tex. R. App. P.",
        "seed": "https://www.txcourts.gov/media/1457526/texas-rules-of-appellate-procedure.pdf",
        "index_kw": "rules-of-appellate-procedure",
    },
    "evidence": {
        "name": "Texas Rules of Evidence",
        "citation_prefix": "Tex. R. Evid.",
        "seed": "https://www.txcourts.gov/media/1456691/texas-rules-of-evidence-effective-912025.pdf",
        "index_kw": "rules-of-evidence",
    },
    "judadmin": {
        "name": "Texas Rules of Judicial Administration",
        "citation_prefix": "Tex. R. Jud. Admin.",
        "seed": "", "index_kw": "rules-of-judicial-administration",
    },
    "discconduct": {
        "name": "Texas Disciplinary Rules of Professional Conduct",
        "citation_prefix": "Tex. Disciplinary R. Prof. Conduct",
        "seed": "", "index_kw": "disciplinary-rules-of-professional-conduct",
    },
    "judconduct": {
        # Structured as Canons (Canon 1..8), not "RULE n" — parsed with _HDR_CANON.
        "name": "Texas Code of Judicial Conduct",
        "citation_prefix": "Tex. Code Jud. Conduct, Canon",
        "seed": "", "index_kw": "code-of-judicial-conduct",
    },
    "discproc": {
        # Texas Rules of Disciplinary Procedure — on txcourts.gov as trdp.pdf but
        # NOT linked from the rules-standards index, so it needs the pinned seed.
        # Bare "1.01." headers (no "RULE" prefix) -> parsed with _HDR_NUMBERED.
        "name": "Texas Rules of Disciplinary Procedure",
        "citation_prefix": "Tex. R. Disciplinary P.",
        "seed": "https://www.txcourts.gov/media/1457737/trdp.pdf",
        "index_kw": "texas-rules-of-disciplinary-procedure",
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


def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def _point_id(act_id: str, chunk_idx: int, text: str) -> str:
    seed = f"{act_id}::{chunk_idx}::{_sha1(text)[:12]}"
    return str(UUID(hashlib.md5(seed.encode()).hexdigest()))


def _safe(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "_", s).strip("_")


# ---------------------------------------------------------------------------
# Index resolution + PDF download (both proxied; txcourts.gov geo-fences non-US)
# ---------------------------------------------------------------------------
def resolve_pdf_urls() -> dict[str, str]:
    resolved: dict[str, str] = {s: m["seed"] for s, m in RULE_SETS.items() if m["seed"]}
    html = None
    try:
        from curl_cffi import requests as cf_requests  # type: ignore

        r = cf_requests.get(TX_INDEX, impersonate="chrome", proxies=_proxies(),
                            timeout=45, allow_redirects=True)
        if r.status_code == 200:
            html = r.text
    except Exception:
        pass
    if html is None:
        try:
            r = requests.get(TX_INDEX, headers={"User-Agent": UA}, proxies=_proxies(),
                             timeout=45, allow_redirects=True)
            if r.status_code == 200:
                html = r.text
        except Exception:
            pass
    if not html:
        print("[TX] index unresolved; using pinned seed URLs", flush=True)
        return resolved
    hrefs = re.findall(r'href="([^"]+\.pdf)"', html, re.I)
    for slug, meta in RULE_SETS.items():
        kw = meta["index_kw"].replace("-", "")
        for h in hrefs:
            if kw in h.lower().replace("-", ""):
                resolved[slug] = h if h.startswith("http") else urllib.parse.urljoin(TX_INDEX, h)
                break
    print(f"[TX] resolved {len(resolved)} PDF URLs", flush=True)
    return resolved


def download_pdf(url: str) -> bytes | None:
    for attempt in range(5):
        try:
            r = requests.get(url, headers={"User-Agent": UA}, timeout=90,
                             proxies=_proxies(), allow_redirects=True)
            if r.status_code == 200 and r.content[:4] == b"%PDF":
                return r.content
            time.sleep(2 * (attempt + 1))
        except Exception:
            time.sleep(2 * (attempt + 1))
    return None


# ---------------------------------------------------------------------------
# PDF parse -> rules
# ---------------------------------------------------------------------------
@dataclass
class Rule:
    slug: str
    set_name: str
    citation_prefix: str
    rule_id: str        # "21", "21a", "401", "9.1"
    section_title: str
    raw_text: str
    source_url: str


# TX rule headers vary by set:
#  - default ("RULE 21.", "RULE 21a.", "Rule 401.", "Rule 9.1.", "Rule 1.01.")
#    -> integer with optional decimal and/or a single trailing letter.
#  - Code of Judicial Conduct -> "Canon 1" .. "Canon 8".
#  - Rules of Disciplinary Procedure -> bare "1.01." (no RULE prefix), followed
#    by a Capitalized title. The >=120-char body filter drops stray line starts.
_HDR_RULE = re.compile(r"(?im)^\s*RULES?\s+(\d+(?:\.\d+)?[a-zA-Z]?)\.?\s*(.*?)\s*$")
_HDR_CANON = re.compile(r"(?im)^\s*CANON\s+(\d+)\b\.?\s*(.*?)\s*$")
_HDR_NUMBERED = re.compile(r"(?m)^\s*(\d{1,2}\.\d{1,2})\.\s+([A-Z][^\n]{0,140})$")
_SET_HDR = {"judconduct": _HDR_CANON, "discproc": _HDR_NUMBERED}
_SET_LABEL = {"judconduct": "Canon"}  # how a unit is named in title/body


def _pdf_text(pdf_bytes: bytes) -> str:
    import fitz  # PyMuPDF (leak-free; pdfplumber leaks across recurring refreshes)

    parts: list[str] = []
    with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
        for page in doc:
            t = page.get_text("text") or ""
            if t.strip():
                parts.append(t)
    return re.sub(r"[ \t]+", " ", "\n".join(parts))


def parse_set(slug: str, meta: dict, pdf_bytes: bytes, source_url: str) -> list[Rule]:
    text = _pdf_text(pdf_bytes)
    hdr_re = _SET_HDR.get(slug, _HDR_RULE)
    label = _SET_LABEL.get(slug, "RULE")
    matches = list(hdr_re.finditer(text))
    if not matches:
        print(f"  [TX {slug}] no rule headers found", flush=True)
        return []
    rules: list[Rule] = []
    for i, m in enumerate(matches):
        rid = m.group(1)
        title = m.group(2).strip().rstrip(".")
        start = m.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        body = text[start:end].strip()
        if len(body) < 120:  # ToC line, not a body
            continue
        head = f"{label} {rid}. {title}" if title else f"{label} {rid}"
        rules.append(Rule(
            slug=slug, set_name=meta["name"], citation_prefix=meta["citation_prefix"],
            rule_id=rid, section_title=title or f"{label} {rid}",
            raw_text=f"{head}\n\n{body}",
            source_url=source_url,
        ))
    best: dict[str, Rule] = {}
    for r in rules:
        cur = best.get(r.rule_id)
        if cur is None or len(r.raw_text) > len(cur.raw_text):
            best[r.rule_id] = r
    out = list(best.values())
    print(f"  [TX {slug}] {len(out)} {label.lower()}s", flush=True)
    return out


# ---------------------------------------------------------------------------
# Record shape (mirrors the other court-rules ingests)
# ---------------------------------------------------------------------------
def _to_chunk_record(rule: Rule) -> dict:
    safe_rid = _safe(rule.rule_id)
    act_id = f"SRULES_TX_{rule.slug.upper()}_R{safe_rid}"
    title_label = "Texas Rules of Court"
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
        "state": "tx",
        "title_name": rule.set_name,
        "title": title_label,
        "title_code": f"tx_{rule.slug}",
        "top_level_title": f"rules-tx-{rule.slug}",
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
    print(f"=== Texas court-rules ingest: {slugs} ===", flush=True)
    urls = resolve_pdf_urls()
    all_rules: list[Rule] = []
    for slug in slugs:
        meta = RULE_SETS[slug]
        url = urls.get(slug)
        if not url:
            print(f"  [TX {slug}] no PDF URL (no seed, index unresolved); SKIP", flush=True)
            continue
        pdf = download_pdf(url)
        if not pdf:
            print(f"  [TX {slug}] PDF download failed: {url}", flush=True)
            continue
        rules = parse_set(slug, meta, pdf, url)
        all_rules.extend(rules)
    n = _write_jsonl(args.out, all_rules)
    print(f"\n[TX] done: {len(all_rules)} rules, {n} unique act_ids\n=> {args.out}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
