#!/usr/bin/env python3
"""Ingest the Ohio Administrative Code (OAC) — Ohio's state regulations.

Same platform as the Ohio Revised Code (codes.ohio.gov), so this reuses the
inline-chapter-scrape approach from ingest_oh_statutes.py. The OAC adds one
extra level of hierarchy:

    /ohio-administrative-code              → 327 agency/division links
    /ohio-administrative-code/{agency}     → chapter links (chapter-{ag}-{ch})
    /ohio-administrative-code/chapter-{ag}-{ch}
                                           → all rules inline, each headed
                                             "Rule {ag}-{ch}-{rule} | <heading>"

Rule numbers look like 3745-1-01, 109:4-3-01 (agencies can carry a ':' division
suffix). corpus_type='state_regulation'.

Geo-restricted; Webshare US proxy + Mozilla UA + polite pacing (the site 429s
above ~4 req/s).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

sys.stdout.reconfigure(line_buffering=True)  # type: ignore[attr-defined]

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = Path(os.environ.get("OUT_DIR", "./data"))
OUT = DATA_DIR / "state_oh_regulations.jsonl"

OH_BASE = "https://codes.ohio.gov"
OH_TOC = f"{OH_BASE}/ohio-administrative-code"

_MOZ_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
)


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


def _us_proxies() -> Optional[dict]:
    user = os.environ.get("WEBSHARE_USERNAME", "")
    pwd = os.environ.get("WEBSHARE_PASSWORD", "")
    if not user or not pwd:
        return None
    import urllib.parse
    proxy_user = f"{user}-US-rotate"
    host = os.environ.get("WEBSHARE_PROXY_HOST", "p.webshare.io")
    port = os.environ.get("WEBSHARE_PROXY_PORT", "80")
    url = f"http://{urllib.parse.quote(proxy_user)}:{urllib.parse.quote(pwd)}@{host}:{port}"
    return {"http": url, "https": url}


SESSION = requests.Session()
SESSION.headers.update({"User-Agent": _MOZ_UA})


def fetch(url: str, retries: int = 5) -> Optional[str]:
    proxies = _us_proxies()
    for attempt in range(retries):
        try:
            r = SESSION.get(url, timeout=60, proxies=proxies, allow_redirects=True)
            if r.status_code == 200:
                return r.text
            if r.status_code == 429:
                time.sleep(2 ** attempt)
                continue
            if r.status_code in (502, 503, 504):
                time.sleep(2)
                continue
            return None
        except Exception:
            time.sleep(2)
    return None


# ---------------------------------------------------------------------------
# Discovery: TOC -> agencies -> chapters
# ---------------------------------------------------------------------------

# Agency code: digits with optional ':N' division suffix, e.g. 3745, 109:4
_AGENCY_HREF_RE = re.compile(r"^/?(?:ohio-administrative-code/)?(\d+(?::\d+)?)$")
# Chapter href: chapter-{agency}-{chapter}, agency may contain ':'
_CHAPTER_HREF_RE = re.compile(r"chapter-(\d+(?::\d+)?-[0-9A-Za-z]+)$")


def list_agencies() -> list[str]:
    html = fetch(OH_TOC)
    if not html:
        raise RuntimeError("could not fetch OAC TOC")
    soup = BeautifulSoup(html, "html.parser")
    seen: set[str] = set()
    for a in soup.find_all("a", href=True):
        m = _AGENCY_HREF_RE.match(a["href"])
        if m:
            seen.add(m.group(1))
    return sorted(seen, key=lambda s: (int(s.split(":")[0]), s))


def list_chapters_in_agency(agency: str) -> list[str]:
    """Return list of full chapter URLs for an agency."""
    url = f"{OH_BASE}/ohio-administrative-code/{agency}"
    html = fetch(url)
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    seen: dict[str, str] = {}
    for a in soup.find_all("a", href=True):
        m = _CHAPTER_HREF_RE.search(a["href"])
        if not m:
            continue
        chap_id = m.group(1)  # e.g. 3745-1 or 109:4-3
        full = f"{OH_BASE}/ohio-administrative-code/chapter-{chap_id}"
        seen[chap_id] = full
    return list(seen.values())


# ---------------------------------------------------------------------------
# Rule parsing
# ---------------------------------------------------------------------------

# Rule header: "Rule 3745-1-01 | Heading" or "Rule 109:4-3-01 | Heading"
_RULE_RE = re.compile(
    r"\nRule\s+(\d+(?::\d+)?-[0-9A-Za-z]+-[0-9A-Za-z.]+)\s*\|\s*([^\n]+?)\n",
    re.MULTILINE,
)
_RESERVED_PAT = re.compile(r"\[(rescinded|repealed|reserved|renumbered)\b", re.IGNORECASE)


_MONTHS = (
    "January|February|March|April|May|June|July|August|September|"
    "October|November|December"
)


def _strip_metadata(body: str) -> str:
    # OAC rule pages render a metadata block after the heading:
    #   Effective:\n<date>\nPromulgated Under: <ch>\nStatutory Authority: ...\n
    #   Rule Amplifies: ...\nPrior Effective Dates: <dates>\n  then the body.
    # The label lines and their (often next-line) values both need removing.
    body = re.sub(r"Effective:[^\n]*\n", "", body, flags=re.IGNORECASE)
    body = re.sub(r"(?:Five|Five-Year|Last) [^\n]*[Rr]eview[^\n]*\n", "", body)
    body = re.sub(r"Promulgated Under:[^\n]*\n", "", body, flags=re.IGNORECASE)
    body = re.sub(r"Statutory Authority:[^\n]*\n", "", body, flags=re.IGNORECASE)
    body = re.sub(r"Rule Amplifies:[^\n]*\n", "", body, flags=re.IGNORECASE)
    body = re.sub(r"Prior Effective Dates:[^\n]*\n", "", body, flags=re.IGNORECASE)
    body = re.sub(r"PDF:\s*Download Authenticated PDF\s*", "", body, flags=re.IGNORECASE)
    # Strip a leading run of orphaned metadata values (dates, "Ch 119.",
    # promulgation fragments) that precede the substantive text. Stop at the
    # first real paragraph marker — "(A)", a digit list, or a capitalized
    # sentence longer than a fragment.
    leading_noise = re.compile(
        rf"^\s*(?:(?:{_MONTHS})\s+\d{{1,2}},\s+\d{{4}}"
        r"|Ch\.?\s*\d+\.?"
        r"|\d{2}/\d{2}/\d{4}"
        r"|R\.?C\.?\s*\d[\d.]*"
        r"|[;,.]"
        r")\s*",
        re.IGNORECASE,
    )
    prev = None
    while prev != body:
        prev = body
        body = leading_noise.sub("", body, count=1)
    body = re.sub(r"\s+", " ", body).strip()
    return body


@dataclass
class Rule:
    agency: str
    chapter_id: str
    chapter_name: str
    rule_num: str
    rule_title: str
    raw_text: str
    source_url: str
    effective_date: str = ""          # e.g. "February 6, 2017"
    promulgated_under: str = ""       # rulemaking-procedure statute, e.g. "119.03"
    statutory_authority: str = ""     # enabling statute(s)
    rule_amplifies: str = ""          # statute(s) the rule implements
    prior_effective_dates: str = ""   # history
    review_date: str = ""             # five-year-review date


def _extract_rule_meta(region: str) -> dict:
    """Pull the labeled metadata fields that appear between a rule's heading
    and its substantive body on codes.ohio.gov. Values usually sit on the line
    after the label. Returns a dict of the fields we found.
    """
    out = {
        "effective_date": "",
        "promulgated_under": "",
        "statutory_authority": "",
        "rule_amplifies": "",
        "prior_effective_dates": "",
        "review_date": "",
    }
    patterns = {
        "effective_date": r"Effective:\s*\n?\s*([^\n]+)",
        "promulgated_under": r"Promulgated Under:\s*\n?\s*([^\n]+)",
        "statutory_authority": r"Statutory Authority:\s*\n?\s*([^\n]+)",
        "rule_amplifies": r"Rule Amplifies:\s*\n?\s*([^\n]+)",
        "prior_effective_dates": r"Prior Effective Dates:\s*\n?\s*([^\n]+)",
        "review_date": r"(?:Five Year Review|FYR)[^\n:]*:\s*\n?\s*([^\n]+)",
    }
    for key, pat in patterns.items():
        m = re.search(pat, region, re.IGNORECASE)
        if m:
            val = m.group(1).strip()
            # Guard: don't capture the next label as a value
            if val and not re.match(r"(Effective|Promulgated|Statutory|Rule Amplifies|Prior Effective|PDF|Five Year)", val, re.IGNORECASE):
                out[key] = val
    return out


def parse_chapter(html: str, chapter_id: str, chapter_url: str) -> list[Rule]:
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=True)
    chap_name = ""
    m_cn = re.search(rf"\nChapter\s+{re.escape(chapter_id)}\s*\|\s*([^\n]+?)\n", text)
    if m_cn:
        chap_name = m_cn.group(1).strip()
    agency = chapter_id.split("-")[0]

    matches = list(_RULE_RE.finditer(text))
    out: list[Rule] = []
    for i, m in enumerate(matches):
        rule_num = m.group(1).strip()
        rule_title = m.group(2).strip().rstrip(".")
        if _RESERVED_PAT.search(rule_title):
            continue
        start = m.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        region = text[start:end]
        # Capture the labeled metadata block (effective date, authority, etc.)
        # BEFORE cleaning, so nothing is lost — it becomes structured metadata.
        meta = _extract_rule_meta(region)
        body = _strip_metadata(region)
        for trail in ("\nView ", "\nLast updated"):
            idx = body.find(trail)
            if idx > 0:
                body = body[:idx].strip()
                break
        if len(body) < 30:
            continue
        out.append(Rule(
            agency=agency,
            chapter_id=chapter_id,
            chapter_name=chap_name,
            rule_num=rule_num,
            rule_title=f"Rule {rule_num}. {rule_title}",
            raw_text=body,
            source_url=f"{OH_BASE}/ohio-administrative-code/rule-{rule_num}",
            effective_date=meta["effective_date"],
            promulgated_under=meta["promulgated_under"],
            statutory_authority=meta["statutory_authority"],
            rule_amplifies=meta["rule_amplifies"],
            prior_effective_dates=meta["prior_effective_dates"],
            review_date=meta["review_date"],
        ))
    return out


# ---------------------------------------------------------------------------
# Chunk emission
# ---------------------------------------------------------------------------

def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def _point_id(act_id: str, idx: int, text: str) -> str:
    seed = f"{act_id}::{idx}::{_sha1(text)[:12]}"
    return str(uuid.UUID(hashlib.md5(seed.encode()).hexdigest()))


def to_chunk_record(r: Rule) -> dict:
    safe = r.rule_num.replace(":", "_").replace("-", "_").replace(".", "_")
    act_id = f"STATE_OH_ADC_{safe}"
    citation = f"Ohio Admin. Code {r.rule_num}"
    text = r.raw_text
    # Rich, searchable embed header: include the regulation-specific metadata
    # (effective date, enabling statute) so it is retrievable, not just stored.
    meta_lines = []
    if r.effective_date:
        meta_lines.append(f"Effective: {r.effective_date}")
    if r.statutory_authority:
        meta_lines.append(f"Statutory Authority: {r.statutory_authority}")
    if r.rule_amplifies:
        meta_lines.append(f"Amplifies: {r.rule_amplifies}")
    if r.promulgated_under:
        meta_lines.append(f"Promulgated Under: {r.promulgated_under}")
    meta_header = ("\n" + "\n".join(meta_lines)) if meta_lines else ""
    text_for_embedding = (
        f"Regulation: Ohio Administrative Code | US | Ohio | In Force\n"
        f"Agency {r.agency} / Chapter {r.chapter_id}: {r.chapter_name}\n"
        f"{r.rule_title}{meta_header}\n\n{text}"
    )
    md = {
        "act_id": act_id,
        "corpus_type": "state_regulation",
        "category": "state_regulation",
        "document_type": "regulation",
        "jurisdiction": "US",
        "country_code": "US",
        "state": "oh",
        "title_number": None,
        "title_name": f"Ohio Administrative Code — Agency {r.agency}",
        "title": "Ohio Administrative Code",
        "title_code": f"oac_{r.agency.replace(':', '_')}",
        "top_level_title": r.agency,
        "chapter": r.chapter_id,
        "chapter_name": r.chapter_name,
        "section_number": r.rule_num,
        "section_title": r.rule_title,
        "year": 2026,
        "act_status": "in_force",
        "renumbered_to": "",
        "transferred_to": "",
        "level_classifier": "rule",
        # --- regulation-specific rich metadata (captured, not discarded) ---
        "effective_date": r.effective_date or None,
        "promulgated_under": r.promulgated_under or None,
        "statutory_authority": r.statutory_authority or None,
        "rule_amplifies": r.rule_amplifies or None,
        "prior_effective_dates": r.prior_effective_dates or None,
        "review_date": r.review_date or None,
        "issuing_agency_code": r.agency,
        "citation": citation,
        "citation_short": citation,
        "display_label": citation,
        "display_title": r.rule_title,
        "display_path": (
            f"Ohio Administrative Code / Agency {r.agency} / "
            f"Chapter {r.chapter_id} / Rule {r.rule_num}"
        ),
        "breadcrumb": [
            {"type": "agency", "num": r.agency, "label": f"Agency {r.agency}", "name": ""},
            {"type": "chapter", "num": r.chapter_id,
             "label": f"Chapter {r.chapter_id}", "name": r.chapter_name},
            {"type": "rule", "num": r.rule_num, "label": f"Rule {r.rule_num}", "name": ""},
        ],
        "sort_key": act_id,
        "word_count": len(text.split()),
        "subsection_count": 0,
        "subsection_letters": [],
        "numbered_paragraph_count": 0,
        "cross_references_count": 0,
        "cross_references_usc": [],
        "cross_references_cfr": [],
        "amendment_years": [],
        "amendments_count": 0,
        "last_amended_year": None,
        "public_laws_referenced": [],
        "public_laws_count": 0,
        "source_url": r.source_url,
        "parent_id": f"us/oh/regulations/agency={r.agency}/chapter={r.chapter_id}",
        "raw_node_id": f"us/oh/regulations/agency={r.agency}/chapter={r.chapter_id}/rule={r.rule_num}",
        "parent_chunk_id": _point_id(act_id, -1, text),
        "full_text_sha1": _sha1(text),
    }
    return {
        "point_id": _point_id(act_id, 0, text),
        "text_for_embedding": text_for_embedding,
        "raw_text": text,
        "metadata": md,
    }


# ---------------------------------------------------------------------------
# Crawl
# ---------------------------------------------------------------------------

def process_chapter(chapter_url: str) -> list[Rule]:
    html = fetch(chapter_url)
    if not html:
        return []
    chapter_id = chapter_url.rsplit("chapter-", 1)[-1]
    rules = parse_chapter(html, chapter_id, chapter_url)
    return rules


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--agencies", default="",
                    help="Comma-separated agency codes (e.g. '3745,109:4'). Default: all.")
    ap.add_argument("--workers", type=int, default=3)
    ap.add_argument("--delay", type=float, default=0.8)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    print(f"[OAC] discovering agencies from {OH_TOC}", flush=True)
    agencies = list_agencies()
    if args.agencies:
        wanted = {a.strip() for a in args.agencies.split(",") if a.strip()}
        agencies = [a for a in agencies if a in wanted]
    print(f"[OAC] {len(agencies)} agencies", flush=True)

    # Phase 1: gather all chapter URLs
    all_chapters: list[str] = []
    for ag in agencies:
        chaps = list_chapters_in_agency(ag)
        all_chapters.extend(chaps)
        print(f"  [agency {ag}] {len(chaps)} chapters", flush=True)
        time.sleep(args.delay)
    print(f"\n[OAC] {len(all_chapters)} chapters to fetch", flush=True)
    if args.dry_run:
        return 0

    # Phase 2: crawl chapters
    chunks: list[dict] = []
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {ex.submit(process_chapter, u): u for u in all_chapters}
        done = 0
        for fut in as_completed(futures):
            done += 1
            try:
                rules = fut.result()
                chunks.extend(to_chunk_record(x) for x in rules)
            except Exception as e:
                print(f"  ! chapter failed: {e}", flush=True)
            if done % 50 == 0 or done == len(all_chapters):
                rate = len(chunks) / max(time.time() - t0, 0.1)
                print(f"  ... {done:>5}/{len(all_chapters)} chapters, "
                      f"{len(chunks):>6} rules, {rate:.1f}/s", flush=True)
            time.sleep(args.delay / max(args.workers, 1))

    # Dedup + write
    seen: set[str] = set()
    if OUT.exists():
        with open(OUT) as fh:
            for line in fh:
                try:
                    seen.add(json.loads(line)["point_id"])
                except Exception:
                    pass
    written = 0
    with open(OUT, "a") as fh:
        for c in chunks:
            if c["point_id"] in seen:
                continue
            fh.write(json.dumps(c, ensure_ascii=False) + "\n")
            seen.add(c["point_id"])
            written += 1

    print(f"\n=== Done: parsed={len(chunks):,}, new={written:,}, "
          f"elapsed={time.time()-t0:.1f}s ===", flush=True)
    print(f"JSONL: {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
