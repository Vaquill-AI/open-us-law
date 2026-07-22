#!/usr/bin/env python3
"""Ingest New York court rules (22 NYCRR, Title 22 - Judiciary) to JSONL.

Source decision
---------------
The authoritative NY judiciary site (www.nycourts.gov/rules/) is behind a
Cloudflare + Akamai challenge (403 to any plain fetch), and the official NY
Department of State NYCRR text on govt.westlaw.com is an even harder Thomson
Reuters JS bot-wall.

Cornell LII mirrors the full official NYCRR as clean, un-walled HTML, one page
per section, at a stable, enumerable URL scheme:

    index  : /regulations/new-york/title-22[/subtitle-X/chapter-Y/subchapter-Z/part-N]
    section: /regulations/new-york/22-NYCRR-<section-id>   (e.g. 22-NYCRR-130-1.1)

We crawl the whole of Title 22 (the Judiciary title: Chief Judge / Chief
Administrator / Uniform Trial Court rules / Court of Appeals / Appellate
Division / ancillary judicial agencies / forms) so coverage is complete rather
than container-thin.

CPLR is NOT ingested here: the Civil Practice Law and Rules is a statute
(Consolidated Laws chapter CVP), handled by the statutes pipeline, not a court
rule.

Scope note: LII refreshes NYCRR quarterly, so treat its text as current-as-of
its last quarterly update.

LII blocks datacenter/cloud IP ranges, so fetches from a cloud host need a US
residential proxy (WEBSHARE_* env vars, see .env.example) or run --no-proxy from
a residential connection.

Two phases (crawl then chunk):
  1. BFS every Title 22 index page -> list of (part, section-url) with names.
  2. Fetch each section page in a thread pool -> extract text -> JSONL rows.

Output: state_ny_court_rules.jsonl
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import hashlib
import json
import os
import re
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from uuid import UUID

import requests
from bs4 import BeautifulSoup

sys.stdout.reconfigure(line_buffering=True)  # type: ignore[attr-defined]

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = Path(os.environ.get("OUT_DIR", str(_PROJECT_ROOT / "data")))
OUT = DATA_DIR / "state_ny_court_rules.jsonl"

LII_BASE = "https://www.law.cornell.edu"
TITLE_ROOT = "/regulations/new-york/title-22"

UA = "Mozilla/5.0 (open-us-law corpus ingestion bot; +https://github.com/Vaquill-AI/open-us-law)"


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


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------
# Cornell LII refuses datacenter/cloud IP ranges outright (ECONNREFUSED, not a
# 429). Route through a US residential proxy so fetches egress from residential
# IPs. Rotation also spreads the crawl across many IPs, keeping the load on the
# LII mirror light per-IP. Configure via WEBSHARE_* (see .env.example); the
# `-US-rotate` suffix on the Webshare username locks exits to the US.
def _proxies() -> dict[str, str] | None:
    import urllib.parse

    user = os.environ.get("WEBSHARE_USERNAME", "")
    pwd = os.environ.get("WEBSHARE_PASSWORD", "")
    if not user or not pwd:
        return None
    proxy_user = f"{user}-US-rotate"
    host = os.environ.get("WEBSHARE_PROXY_HOST", "p.webshare.io")
    port = os.environ.get("WEBSHARE_PROXY_PORT", "80")
    url = f"http://{urllib.parse.quote(proxy_user)}:{urllib.parse.quote(pwd)}@{host}:{port}"
    return {"http": url, "https": url}


# Set by main(): the proxy dict (or None for a direct connection).
PROXIES: dict[str, str] | None = None


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": UA})
    a = requests.adapters.HTTPAdapter(pool_connections=64, pool_maxsize=64, max_retries=0)
    s.mount("http://", a)
    s.mount("https://", a)
    return s


SESSION = _session()


def fetch(url: str, retries: int = 4) -> str | None:
    for attempt in range(retries):
        try:
            r = SESSION.get(url, timeout=45, allow_redirects=True, proxies=PROXIES)
            if r.status_code == 404:
                return None
            r.raise_for_status()
            return r.text
        except Exception:
            if attempt == retries - 1:
                return None
            time.sleep(0.6 * (2**attempt))
    return None


def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def _point_id(act_id: str, chunk_idx: int, text: str) -> str:
    seed = f"{act_id}::{chunk_idx}::{_sha1(text)[:12]}"
    return str(UUID(hashlib.md5(seed.encode()).hexdigest()))


def _safe(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "_", s).strip("_")


# ---------------------------------------------------------------------------
# Model
# ---------------------------------------------------------------------------
@dataclass
class NyRule:
    part_num: str  # e.g. "202", "130", "1250"
    part_name: str  # e.g. "Uniform Civil Rules For The Supreme Court ..."
    category_name: str  # parent chapter/subchapter name (breadcrumb)
    section_id: str  # e.g. "202.5", "130-1.1", "202.5a"
    section_title: str  # "Costs; sanctions"
    raw_text: str
    source_url: str
    act_status: str = "in_force"


@dataclass
class PartSpec:
    part_num: str
    part_name: str
    category_name: str
    part_url: str
    section_urls: list[str] = field(default_factory=list)


_SECTION_HREF_RE = re.compile(r'href="(/regulations/new-york/22-NYCRR-[^"#?]+)"')
_PART_HREF_RE = re.compile(r'href="(/regulations/new-york/title-22[^"#?]*/part-[0-9A-Za-z.]+)"')
_INDEX_HREF_RE = re.compile(r'href="(/regulations/new-york/title-22[^"#?]*)"')


def _index_name(html: str) -> str:
    """Human name of an index page: the text after the last ' - ' in the H1."""
    soup = BeautifulSoup(html, "lxml")
    h1 = soup.find("h1")
    if not h1:
        return ""
    t = h1.get_text(" ", strip=True)
    # H1 like "N.Y. Comp. Codes R. & Regs. Tit. 22, ... Part 202 - Uniform ..."
    if " - " in t:
        return t.split(" - ", 1)[1].strip()
    return t.strip()


# ---------------------------------------------------------------------------
# Phase 1: BFS enumerate every Part under Title 22
# ---------------------------------------------------------------------------
def enumerate_parts() -> list[PartSpec]:
    seen: set[str] = set()
    queue: list[tuple[str, str]] = [(TITLE_ROOT, "New York Court Rules (22 NYCRR)")]
    part_urls: dict[str, str] = {}  # part_url -> discovering-index name
    while queue:
        url, parent_name = queue.pop()
        if url in seen:
            continue
        seen.add(url)
        html = fetch(LII_BASE + url)
        if not html:
            continue
        this_name = _index_name(html) or parent_name
        # Collect part links discovered on this page (parented by this page).
        for h in _PART_HREF_RE.findall(html):
            if h not in part_urls:
                part_urls[h] = this_name
        # Recurse into deeper index pages (subtitle/chapter/subchapter) that are
        # not themselves Part pages.
        for h in _INDEX_HREF_RE.findall(html):
            if "/part-" in h or h in seen:
                continue
            queue.append((h, this_name))
    print(f"[NY] discovered {len(part_urls)} Part index pages", flush=True)

    # Fetch each Part page: capture part name + its section links.
    specs: list[PartSpec] = []

    def _load_part(item: tuple[str, str]) -> PartSpec | None:
        purl, category = item
        m = re.search(r"/part-([0-9A-Za-z.]+)$", purl)
        part_num = m.group(1) if m else purl.rsplit("-", 1)[-1]
        # A Part may list its sections directly (e.g. Part 202) OR only link to
        # Subpart index pages (e.g. Part 130 -> subpart-130-1 / subpart-130-2)
        # whose section pages live one level deeper. BFS the whole subtree
        # rooted at this Part, collecting every 22-NYCRR-* section link. Without
        # this, subpart-structured Parts silently lose all their sections.
        sec_urls: list[str] = []
        part_name: str | None = None
        sub_seen: set[str] = set()
        sub_queue: list[str] = [purl]
        while sub_queue:
            u = sub_queue.pop()
            if u in sub_seen:
                continue
            sub_seen.add(u)
            html = fetch(LII_BASE + u)
            if not html:
                if u == purl:
                    print(f"  ! part fetch failed: {purl}", flush=True)
                continue
            if part_name is None:
                part_name = _index_name(html) or f"Part {part_num}"
            for s in _SECTION_HREF_RE.findall(html):
                if s not in sec_urls:
                    sec_urls.append(s)
            # Descend only into deeper index pages within THIS Part's subtree
            # (subparts / sub-subparts), never siblings or ancestors.
            for h in _INDEX_HREF_RE.findall(html):
                if h.startswith(purl + "/") and h not in sub_seen:
                    sub_queue.append(h)
        if part_name is None:
            return None
        return PartSpec(
            part_num=part_num,
            part_name=part_name,
            category_name=category,
            part_url=LII_BASE + purl,
            section_urls=sec_urls,
        )

    with cf.ThreadPoolExecutor(max_workers=12) as ex:
        for spec in ex.map(_load_part, list(part_urls.items())):
            if spec is not None:
                specs.append(spec)
    total_secs = sum(len(s.section_urls) for s in specs)
    print(f"[NY] {len(specs)} parts, {total_secs} section links to fetch", flush=True)
    return specs


# ---------------------------------------------------------------------------
# Phase 2: fetch + parse each section page
# ---------------------------------------------------------------------------
# H1 like "... §§ 130-1.1 - Costs; sanctions". Capture the trailing title.
_TITLE_H1_RE = re.compile(r"§§?\s*[^\s].*?\s+-\s+(.*)$")


def _parse_section(html: str, section_id_hint: str) -> tuple[str, str, str] | None:
    """Return (section_id, section_title, body_text) or None if not substantive."""
    soup = BeautifulSoup(html, "lxml")
    h1 = soup.find("h1")
    h1_text = h1.get_text(" ", strip=True) if h1 else ""
    section_id = section_id_hint
    section_title = ""
    m = _TITLE_H1_RE.search(h1_text)
    if m:
        section_title = m.group(1).strip()
    elif " - " in h1_text:
        section_title = h1_text.split(" - ", 1)[1].strip()

    container = soup.find("div", class_="statereg-text")
    if container is None:
        return None
    body = re.sub(r"\s+", " ", container.get_text(" ", strip=True)).strip()
    if len(body) < 25:
        return None
    return section_id, section_title, body


def _section_id_from_url(url: str) -> str:
    slug = url.rsplit("/22-NYCRR-", 1)[-1]
    return slug.strip()


def _act_status_for(title: str, body: str) -> str:
    head = f"{title} {body[:60]}".lower()
    if "[repealed" in head or "repealed]" in head:
        return "repealed"
    if "[reserved" in head or "reserved]" in head:
        return "reserved"
    return "in_force"


def scrape_sections(specs: list[PartSpec], workers: int) -> list[NyRule]:
    jobs: list[tuple[PartSpec, str]] = []
    for spec in specs:
        for surl in spec.section_urls:
            jobs.append((spec, surl))

    rules: list[NyRule] = []

    def _one(job: tuple[PartSpec, str]) -> NyRule | None:
        spec, surl = job
        full = LII_BASE + surl
        html = fetch(full)
        if not html:
            return None
        sid = _section_id_from_url(surl)
        parsed = _parse_section(html, sid)
        if parsed is None:
            return None
        section_id, section_title, body = parsed
        status = _act_status_for(section_title, body)
        return NyRule(
            part_num=spec.part_num,
            part_name=spec.part_name,
            category_name=spec.category_name,
            section_id=section_id,
            section_title=section_title or f"Section {section_id}",
            raw_text=body,
            source_url=full,
            act_status=status,
        )

    done = 0
    with cf.ThreadPoolExecutor(max_workers=workers) as ex:
        for res in ex.map(_one, jobs):
            done += 1
            if res is not None:
                rules.append(res)
            if done % 250 == 0 or done == len(jobs):
                print(f"  [NY] sections {done}/{len(jobs)} parsed={len(rules)}", flush=True)
    return rules


# ---------------------------------------------------------------------------
# Record shape (mirrors the other court-rules ingests exactly)
# ---------------------------------------------------------------------------
def _to_chunk_record(rule: NyRule) -> dict:
    safe_sid = _safe(rule.section_id)
    act_id = f"SRULES_NY_22NYCRR_P{_safe(rule.part_num)}_S{safe_sid}"
    title_label = "22 NYCRR (NY Court Rules)"
    citation = f"22 NYCRR § {rule.section_id}"
    text_for_embedding = (
        f"{title_label} | {rule.category_name} | {citation}\n"
        f"Part {rule.part_num}. {rule.part_name}\n"
        f"§ {rule.section_id} {rule.section_title}\n\n{rule.raw_text}"
    )
    md = {
        "act_id": act_id,
        "corpus_type": "state_rules",
        "category": "state_rules",
        "document_type": "court_rule",
        "jurisdiction": "US",
        "country_code": "US",
        "state": "ny",
        "title_name": "22 NYCRR",
        "title": title_label,
        "title_code": "22nycrr",
        "top_level_title": "22nycrr",
        "level_classifier": "section",
        "chapter": rule.part_num,
        "chapter_name": f"Part {rule.part_num}. {rule.part_name}",
        "subchapter": None,
        "subchapter_name": rule.category_name,
        "section_number": rule.section_id,
        "section_title": f"§ {rule.section_id} {rule.section_title}",
        "citation": citation,
        "citation_short": citation,
        "display_label": citation,
        "display_title": rule.section_title,
        "display_path": (
            f"22 NYCRR / {rule.category_name} / Part {rule.part_num} / § {rule.section_id}"
        ),
        "breadcrumb": [
            "22 NYCRR",
            rule.category_name,
            f"Part {rule.part_num}. {rule.part_name}",
            f"§ {rule.section_id}",
        ],
        "sort_key": act_id,
        "act_status": rule.act_status,
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
        "raw_node_id": act_id,
        "full_text_sha1": _sha1(rule.raw_text),
    }
    return {
        "point_id": _point_id(act_id, 0, rule.raw_text),
        "text_for_embedding": text_for_embedding,
        "raw_text": rule.raw_text,
        "metadata": md,
    }


def _write_jsonl(path: Path, rules: list[NyRule]) -> int:
    """Fresh JSONL per run, deduped by act_id."""
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
    ap.add_argument(
        "--workers",
        type=int,
        default=12,
        help="Concurrent section fetches (be polite to the LII mirror).",
    )
    ap.add_argument(
        "--limit-parts",
        type=int,
        default=0,
        help="Smoke mode: only process the first N discovered parts.",
    )
    ap.add_argument(
        "--part",
        action="append",
        default=[],
        help="Only process these part numbers (repeatable), e.g. --part 130 --part 202.",
    )
    ap.add_argument("--out", type=Path, default=OUT)
    ap.add_argument(
        "--no-proxy",
        action="store_true",
        help="Fetch LII directly (only works from a non-datacenter/residential IP).",
    )
    args = ap.parse_args()

    global PROXIES
    if not args.no_proxy:
        PROXIES = _proxies()
    print("=== NY 22 NYCRR court-rules ingest (Cornell LII) ===", flush=True)
    print(f"[NY] proxy: {'US-rotate residential' if PROXIES else 'DIRECT'}", flush=True)
    t0 = time.time()
    specs = enumerate_parts()
    if args.part:
        want = {p.strip() for p in args.part}
        specs = [s for s in specs if s.part_num in want]
        print(f"[NY] filtered to parts {sorted(want)}: {len(specs)} specs", flush=True)
    if args.limit_parts:
        specs = specs[: args.limit_parts]
        print(f"[NY] smoke mode: first {len(specs)} parts", flush=True)

    rules = scrape_sections(specs, args.workers)
    n = _write_jsonl(args.out, rules)
    print(
        f"\n[NY] done: {len(rules)} sections, {n} unique act_ids in "
        f"{time.time() - t0:.1f}s\n=> {args.out}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
