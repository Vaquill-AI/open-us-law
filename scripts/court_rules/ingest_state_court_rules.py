#!/usr/bin/env python3
"""Ingest state court rules into the existing pipeline.

corpus_type='state_rules', act_id prefix 'SRULES_<ST>_'

Currently configured states:
  - ca (California Rules of Court)  - courts.ca.gov/cms/rules/index/<title>
  - mt (Administrative Rules of Montana)  - rules.mt.gov (NOTE: this is admin
       rules, not court rules; renaming will happen later if needed)

Adding more states: provide a scrape_<st>() function + register in STATE_SCRAPERS.
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
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

import requests
from bs4 import BeautifulSoup

sys.stdout.reconfigure(line_buffering=True)  # type: ignore[attr-defined]

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = Path(os.environ.get("OUT_DIR", "./data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
OUT = DATA_DIR / "state_court_rules_chunks.jsonl"

UA = "Mozilla/5.0 (Vaquill ingestion bot; +https://vaquill.ai)"


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


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": UA})
    a = requests.adapters.HTTPAdapter(pool_connections=32, pool_maxsize=32, max_retries=0)
    s.mount("http://", a)
    s.mount("https://", a)
    return s


SESSION = _session()


def fetch(url: str, retries: int = 3) -> Optional[str]:
    for attempt in range(retries):
        try:
            r = SESSION.get(url, timeout=30, allow_redirects=True)
            if r.status_code == 404:
                return None
            r.raise_for_status()
            return r.text
        except Exception as e:
            if attempt == retries - 1:
                return None
            time.sleep(0.5 * (2 ** attempt))
    return None


_FULL_STATE_NAME = {
    "ca": "California", "mt": "Montana", "tx": "Texas", "ny": "New York",
    "fl": "Florida",
}


@dataclass
class Rule:
    state: str
    title_id: str  # e.g. "1" for CA Title 1
    title_name: str  # e.g. "Rules Applicable to All Courts"
    rule_id: str  # e.g. "1.1" or "1.100"
    section_title: str
    raw_text: str
    source_url: str


def sha1_hex(s: str) -> str:
    return hashlib.sha1(s.encode()).hexdigest()


def point_id_for(act_id: str, chunk_idx: int, text: str) -> str:
    h = hashlib.md5(f"{act_id}::{chunk_idx}::{sha1_hex(text)[:12]}".encode()).hexdigest()
    return str(uuid.UUID(h))


def to_chunk_record(r: Rule) -> dict:
    text = r.raw_text.strip()
    state_full = _FULL_STATE_NAME.get(r.state, r.state.upper())
    full_corpus_name = f"{state_full} Court Rules"
    act_id = f"SRULES_{r.state.upper()}_T{r.title_id}_R{r.rule_id.replace('.', '_')}"
    citation = f"{state_full[:4]}. R. Ct. {r.rule_id}"  # rough form

    text_for_embedding = (
        f"{full_corpus_name} | {citation}\n"
        f"{r.section_title}\n\n{text}"
    )
    md = {
        "act_id": act_id,
        "corpus_type": "state_rules",
        # Canonical value per CANONICAL_CATEGORIES in
        # app/services/us_statutes_taxonomy.py. Previously emitted as
        # 'state_court_rule', which the DQ audit found the retriever dispatch
        # doesn't recognise (silent USC-default fallback would fire). 8,467
        # rows were bulk-normalised to 'state_rules' on 2026-07-16; the
        # scraper now emits the canonical value directly to prevent drift.
        "category": "state_rules",
        "document_type": "court_rule",
        "jurisdiction": "US",
        "country_code": "US",
        "state": r.state,
        "title_name": full_corpus_name,
        "title": full_corpus_name,
        "top_level_title": f"rules-{r.state}",
        "title_code": f"rules_{r.state}",
        "level_classifier": "rule",
        "chapter": r.title_id,
        "chapter_name": r.title_name,
        "section_number": r.rule_id,
        "section_title": r.section_title,
        "citation": citation,
        "citation_short": citation,
        "display_label": citation,
        "display_title": r.section_title,
        "display_path": f"{full_corpus_name} / Title {r.title_id} / Rule {r.rule_id}",
        "breadcrumb": [full_corpus_name, f"Title {r.title_id}: {r.title_name}", f"Rule {r.rule_id}"],
        "sort_key": act_id,
        "act_status": "in_force",
        "renumbered_to": "",
        "transferred_to": "",
        "year": None,
        "word_count": len(text.split()),
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
        "source_url": r.source_url,
        "parent_id": None,
        "raw_node_id": act_id,
        "full_text_sha1": sha1_hex(text) if text else None,
    }
    return {
        "point_id": point_id_for(act_id, 0, text),
        "text_for_embedding": text_for_embedding,
        "raw_text": text,
        "metadata": md,
    }


# ---------------------------------------------------------------------------
# California Rules of Court (courts.ca.gov)
# ---------------------------------------------------------------------------

CA_TITLE_NAMES = {
    "one":      "Rules Applicable to All Courts",
    "two":      "Rules Relating to the Supreme Court and Courts of Appeal",
    "three":    "Civil Rules",
    "four":     "Criminal Rules",
    "five":     "Family and Juvenile Rules",
    "six":      "Trial Court Rules",
    "seven":    "Probate Rules",
    "eight":    "Appellate Rules",
    "nine":     "Rules on Professional Conduct of Lawyers",
    "ten":      "Judicial Administration Rules",
    "ethics":   "Code of Judicial Ethics",
    "standards":"Standards of Judicial Administration",
}
CA_TITLE_NUM = {
    "one": "1", "two": "2", "three": "3", "four": "4", "five": "5",
    "six": "6", "seven": "7", "eight": "8", "nine": "9", "ten": "10",
    "ethics": "ethics", "standards": "standards",
}


def _ca_rule_candidates(title_num: str) -> list[str]:
    """Enumerate possible rule IDs for a CA title.

    CA rules look like 1.1, 1.10, 1.100, 1.5.1, 2.30, etc. There's no
    deterministic count. We probe a wide range of (rule_int, sub_int) and
    skip 404s. Common max for big titles is ~500 rules.
    """
    out = []
    # primary range 1..500
    for n in range(1, 501):
        out.append(f"{title_num}.{n}")
    # sub-numbered (very common): 1.1.1, 1.1.10, 1.10.1, etc.
    for n in range(1, 51):
        for s in range(1, 30):
            out.append(f"{title_num}.{n}.{s}")
    return out


def scrape_ca() -> list[Rule]:
    out: list[Rule] = []
    for slug, title_num in CA_TITLE_NUM.items():
        if title_num in ("ethics", "standards"):
            continue  # different structure, skip for v1
        title_name = CA_TITLE_NAMES[slug]
        print(f"  [CA Title {title_num}: {title_name}]")
        candidates = _ca_rule_candidates(title_num)
        # Probe in parallel, give up on first 50 consecutive 404s for efficiency
        consec_404 = 0
        found_count = 0
        with cf.ThreadPoolExecutor(max_workers=12) as ex:
            # Submit in chunks of 50
            for batch_start in range(0, len(candidates), 50):
                batch = candidates[batch_start:batch_start + 50]
                urls = [(rid, f"https://courts.ca.gov/cms/rules/index/{slug}/rule{title_num}_{rid.split('.', 1)[1].replace('.', '_')}") for rid in batch]
                results = list(ex.map(lambda x: (x[0], x[1], fetch(x[1])), urls))
                got_any_in_batch = False
                for rid, url, html in results:
                    if html is None:
                        continue
                    got_any_in_batch = True
                    found_count += 1
                    soup = BeautifulSoup(html, "html.parser")
                    body = soup.find("div", class_="field-name-body") or soup.find("article") or soup.find("main") or soup
                    h1 = soup.find("h1")
                    sec_title = h1.get_text(strip=True) if h1 else f"Rule {rid}"
                    text = re.sub(r"\s+", " ", body.get_text(" ", strip=True)).strip()
                    if len(text) < 30:
                        continue
                    rule = Rule(state="ca", title_id=title_num, title_name=title_name,
                                rule_id=rid, section_title=sec_title, raw_text=text,
                                source_url=url)
                    out.append(rule)
                if not got_any_in_batch:
                    consec_404 += 50
                    if consec_404 >= 100:
                        break  # likely past the last rule in this title
                else:
                    consec_404 = 0
        print(f"    Title {title_num}: {found_count} rules")
    print(f"[CA] done: {len(out)} rules total")
    return out


# ---------------------------------------------------------------------------
# Pennsylvania Rules of Civil Procedure (231 Pa. Code)
# Source: pacodeandbulletin.gov (the actual content site; pacode.com is an SPA shell)
#
# Structure:
#   Title 231 -> Parts (I, II) -> Chapters (1, 100, 200, ...) -> Rules (s51.html, ...)
# Each rule lives at:
#   /secure/pacode/data/231/chapter{N}/s{rid}.html
# ---------------------------------------------------------------------------

PA_TITLES: dict[str, str] = {
    # title -> human name (parts/subparts discovered automatically from TOCs)
    "231": "Rules of Civil Procedure",
    "234": "Rules of Criminal Procedure",
    "246": "Minor Court Civil Rules",
    "210": "Appellate Procedure",
    "237": "Juvenile Court Rules",
    "207": "Judicial Conduct",
    "204": "Judicial System General Provisions",
    "225": "Rules of Evidence",
    "201": "Rules of Judicial Administration",
}

PA_BASE = "https://www.pacodeandbulletin.gov/secure/pacode/data"
PA_NAME = "Pennsylvania"


def _pa_walk_tocs(title_num: str) -> list[tuple[str, str]]:
    """Recursively discover all chapter TOC URLs under a PA title.

    Returns: list of (chapter_num, chapter_toc_absolute_url).
    Handles three TOC patterns:
      - title -> chapters (e.g. T234)
      - title -> parts -> chapters (e.g. T231)
      - title -> parts -> subparts -> chapters (e.g. T237)
    """
    base = f"{PA_BASE}/{title_num}"
    visited: set[str] = set()
    chapters: list[tuple[str, str]] = []
    queue = [f"{base}/{title_num}toc.html"]
    while queue:
        url = queue.pop()
        if url in visited:
            continue
        visited.add(url)
        html = fetch(url)
        if not html:
            continue
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            if not href or href.startswith("#") or href.startswith("/"):
                continue
            name = a.get_text(strip=True)
            low = name.lower()
            if "rescinded" in low or "repealed" in low or "[reserved]" in low:
                continue
            # Chapter TOC: chapterN/chapNtoc.html
            m = re.match(r"chapter(\d+[A-Za-z]?)/chap\d+[A-Za-z]?toc\.html$", href)
            if m:
                chap_num = m.group(1)
                full = f"{base}/{href}"
                chapters.append((chap_num, full))
                continue
            # Part or subpart TOC -> recurse
            if re.match(r"(sub)?part[a-zA-Z0-9]+toc\.html$", href):
                # Resolve relative to current url
                # url like /secure/pacode/data/237/partItoc.html → subpartIAtoc.html stays at title root
                parent = url.rsplit("/", 1)[0]
                queue.append(f"{parent}/{href}")
    # Dedup by chap_num (different parts can reference the same chapter file
    # in some titles, though rare)
    seen: set[str] = set()
    dedup: list[tuple[str, str]] = []
    for cn, u in chapters:
        if u in seen:
            continue
        seen.add(u)
        dedup.append((cn, u))
    return dedup


def _pa_extract_rule_links(chapter_html: str) -> list[tuple[str, str, str]]:
    """Return list of (rule_id, rule_title, s<rid>.html) from a chapter TOC.

    The chapter file has the TOC at the top then full rule text below;
    we use the TOC links to enumerate rule IDs, then fetch each rule's
    own page for clean section extraction.
    """
    soup = BeautifulSoup(chapter_html, "html.parser")
    out: list[tuple[str, str, str]] = []
    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        m = re.match(r"s(\d+[A-Za-z]?(?:\.\d+)*)\.html$", href)
        if not m:
            continue
        rid = m.group(1)
        title = a.get_text(strip=True)
        if "rescinded" in title.lower() or "reserved" in title.lower():
            continue
        out.append((rid, title, href))
    # Dedup by rid (file lists each link twice: TOC + anchor)
    seen: set[str] = set()
    deduped: list[tuple[str, str, str]] = []
    for rid, title, href in out:
        if rid in seen:
            continue
        seen.add(rid)
        deduped.append((rid, title, href))
    return deduped


def _pa_extract_rule_text(rule_html: str) -> tuple[str, str]:
    """Return (section_title, body_text) for a single PA rule page."""
    soup = BeautifulSoup(rule_html, "html.parser")
    blockquote = soup.find("blockquote") or soup
    # The blockquote contains nav image at top, then H4 with rule title,
    # then the rule body up to a final <hr> + copyright notice.
    # Strip the image map and nav table.
    for tag in blockquote.find_all(["map", "img", "area", "table"]):
        tag.decompose()
    # Extract H4 heading as section title (e.g. "Rule 51. Title and Citation...")
    h4 = blockquote.find("h4")
    section_title = h4.get_text(" ", strip=True) if h4 else ""
    # Drop everything from the last <hr> onward (copyright notice)
    hrs = blockquote.find_all("hr")
    if hrs:
        last_hr = hrs[-1]
        for sib in list(last_hr.find_all_next()):
            sib.decompose()
        last_hr.decompose()
    # Drop the h4 itself so we don't duplicate it inside the body
    if h4:
        h4.decompose()
    text = re.sub(r"\s+", " ", blockquote.get_text(" ", strip=True)).strip()
    return section_title, text


def scrape_pa() -> list[Rule]:
    """Scrape Pennsylvania court rules across configured titles."""
    out: list[Rule] = []
    for title_num, title_name in PA_TITLES.items():
        print(f"  [PA Title {title_num}: {title_name}]")
        chapter_specs = _pa_walk_tocs(title_num)
        print(f"    {len(chapter_specs)} chapters")

        # Step 1: collect all rule URLs across all chapters (in parallel)
        def _list_chapter_rules(spec):
            chap_num, chap_url = spec
            chap_html = fetch(chap_url)
            if not chap_html:
                return []
            rule_specs = _pa_extract_rule_links(chap_html)
            out_specs: list[tuple[str, str, str, str]] = []
            for rid, rule_title, rule_path in rule_specs:
                rule_url = f"{PA_BASE}/{title_num}/chapter{chap_num}/{rule_path}"
                out_specs.append((chap_num, rid, rule_title, rule_url))
            return out_specs

        all_rule_specs: list[tuple[str, str, str, str]] = []
        with cf.ThreadPoolExecutor(max_workers=16) as ex:
            for chapter_rules in ex.map(_list_chapter_rules, chapter_specs):
                all_rule_specs.extend(chapter_rules)
        print(f"    {len(all_rule_specs)} rules total to fetch")

        # Step 2: fetch all rule pages in parallel (much faster than per-chapter loops)
        def _crawl_one_rule(spec):
            chap_num, rid, rule_title, rule_url = spec
            rh = fetch(rule_url)
            if not rh:
                return None
            section_title, body = _pa_extract_rule_text(rh)
            if len(body) < 30:
                return None
            section_title = section_title or rule_title or f"Rule {rid}"
            rule = Rule(
                state="pa",
                title_id=title_num,
                title_name=title_name,
                rule_id=rid,
                section_title=section_title,
                raw_text=body,
                source_url=rule_url,
            )
            return rule

        with cf.ThreadPoolExecutor(max_workers=20) as ex:
            for rule in ex.map(_crawl_one_rule, all_rule_specs):
                if rule is not None:
                    out.append(rule)
        print(f"    Title {title_num}: {len([r for r in out if r.title_id == title_num])} rules captured")
    print(f"[PA] done: {len(out)} rules total")
    return out


# ---------------------------------------------------------------------------
# Illinois Supreme Court Rules (illinoiscourts.gov)
#
# Structure: one ASP page per Article (?a=i ... ?a=xii). Each page has a table
# of rules where each rule links to a PDF at /resources/{uuid}/file.
# Rules are organized by Article (e.g. Article I = "General Rules", Article II =
# "Rules on Civil Proceedings in the Trial Court", etc.).
# ---------------------------------------------------------------------------

IL_BASE = "https://www.illinoiscourts.gov"
IL_ARTICLES = {
    "i":    "General Rules",
    "ii":   "Rules on Civil Proceedings in the Trial Court",
    "iii":  "Civil Appeals Rules",
    "iv":   "Rules on Criminal Proceedings in the Trial Court",
    "v":    "Criminal Appeals Rules",
    "vi":   "Special Proceedings",
    "vii":  "Rules on Proceedings in the Trial Court",
    "viii": "Illinois Rules of Professional Conduct of 2010",
    "ix":   "Rules on Admission and Discipline of Attorneys",
    "x":    "Standards on Attorneys, Judges and Witnesses",
    "xi":   "Illinois Code of Judicial Conduct of 2023",
    "xii":  "Local Rules",
}


def _il_parse_rule_table(html: str) -> list[tuple[str, str, str]]:
    """Return list of (rule_num, title, pdf_url) from an Article page."""
    soup = BeautifulSoup(html, "html.parser")
    rules: list[tuple[str, str, str]] = []
    # The page has a GridView (`gvRules_*`) with rule rows.
    for span in soup.find_all("span", id=re.compile(r"gvRules_lblRuleNum_\d+")):
        # Walk up to the <tr>, then find the title <a> with PDF link.
        tr = span
        while tr and tr.name != "tr":
            tr = tr.parent
        if not tr:
            continue
        # The rule_num span is itself inside a <td>; the next sibling <td>
        # contains the title link.
        rule_num = span.get_text(strip=True).replace("Rule ", "").replace("Rule", "").strip()
        a = tr.find("a", href=re.compile(r"/resources/[0-9a-f-]+/file", re.I))
        if not a:
            continue
        title = a.get_text(" ", strip=True)
        pdf_url = a["href"]
        if pdf_url.startswith("/"):
            pdf_url = IL_BASE + pdf_url
        rules.append((rule_num, title, pdf_url))
    return rules


def _extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from a PDF using pdfplumber. Falls back to empty string."""
    try:
        import pdfplumber
        import io
        text_parts: list[str] = []
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                t = page.extract_text() or ""
                if t.strip():
                    text_parts.append(t)
        text = "\n\n".join(text_parts)
        # Normalize whitespace and strip excessive blank lines
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()
    except Exception as e:
        print(f"    ! pdf extract failed: {e}", flush=True)
        return ""


def _fetch_bytes(url: str, retries: int = 3) -> Optional[bytes]:
    for attempt in range(retries):
        try:
            r = SESSION.get(url, timeout=60, allow_redirects=True)
            if r.status_code == 404:
                return None
            r.raise_for_status()
            return r.content
        except Exception:
            if attempt == retries - 1:
                return None
            time.sleep(0.5 * (2 ** attempt))
    return None


def scrape_il() -> list[Rule]:
    out: list[Rule] = []
    for art_id, art_name in IL_ARTICLES.items():
        print(f"  [IL Article {art_id.upper()}: {art_name}]")
        page = fetch(f"{IL_BASE}/rules/supreme-court-rules?a={art_id}")
        if not page:
            print(f"    ! could not fetch article {art_id}")
            continue
        rule_specs = _il_parse_rule_table(page)
        print(f"    {len(rule_specs)} rule rows")

        def _crawl_rule(spec):
            rule_num, title, pdf_url = spec
            pdf = _fetch_bytes(pdf_url)
            if not pdf:
                return None
            text = _extract_pdf_text(pdf)
            if len(text) < 50:
                return None
            rule = Rule(
                state="il",
                title_id=art_id.upper(),
                title_name=f"Article {art_id.upper()}: {art_name}",
                rule_id=rule_num,
                section_title=f"Rule {rule_num}. {title}",
                raw_text=text,
                source_url=pdf_url,
            )
            return rule

        with cf.ThreadPoolExecutor(max_workers=8) as ex:
            for result in ex.map(_crawl_rule, rule_specs):
                if result is not None:
                    out.append(result)
        print(f"    Article {art_id}: {len([r for r in out if r.title_id == art_id.upper()])} rules")
    print(f"[IL] done: {len(out)} rules total")
    return out


STATE_SCRAPERS: dict[str, Callable] = {
    "ca": scrape_ca,
    "pa": scrape_pa,
    "il": scrape_il,
}


def merge_jsonl(path: Path, new_rules: list[Rule]) -> int:
    existing: dict[str, dict] = {}
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                aid = rec.get("metadata", {}).get("act_id")
                if aid:
                    existing[aid] = rec
            except json.JSONDecodeError:
                continue
    for r in new_rules:
        rec = to_chunk_record(r)
        existing[rec["metadata"]["act_id"]] = rec
    with open(path, "w", encoding="utf-8") as fh:
        for rec in existing.values():
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
    return len(existing)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--states", default="ca")
    args = ap.parse_args()
    _load_env()
    state_codes = [s.strip().lower() for s in args.states.split(",") if s.strip()]
    print(f"=== State Court Rules: {state_codes} ===")
    all_rules: list[Rule] = []
    for st in state_codes:
        if st not in STATE_SCRAPERS:
            print(f"  [{st}] no scraper, skipping")
            continue
        try:
            rules = STATE_SCRAPERS[st]()
            all_rules.extend(rules)
        except Exception as e:
            print(f"  [{st}] FAIL: {e}")
    n = merge_jsonl(OUT, all_rules)
    print(f"\n=> JSONL has {n} state-court-rule chunks at {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
