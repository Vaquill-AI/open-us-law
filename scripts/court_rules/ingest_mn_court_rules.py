#!/usr/bin/env python3
"""Ingest the Minnesota Court Rules into the Vaquill legal corpus.

OFFICIAL SOURCE ONLY: https://www.revisor.mn.gov/court_rules/ (the Minnesota
Office of the Revisor of Statutes). No aggregators (no Justia/Casetext/etc.).

This is the SAME revisor.mn.gov platform used by ingest_mn_regulations.py, so the
fetch/proxy/R2 helpers are shared. Bulk XML / JSON: NONE; every probe 404s. The
portal serves a server-rendered, highly structured HTML application, so this
scraper parses HTML losslessly.

------------------------------------------------------------------------------
STRUCTURE
------------------------------------------------------------------------------
The landing page lists 11 top-level rule sets, each a two-letter code:

    cr  Rules of Criminal Procedure          ev  Rules of Evidence
    sg  Sentencing Guidelines                ms  Miscellaneous Rules
    cp  Rules of Civil Procedure             ra  Rules on Record Access
    gp  General Rules of Practice            dc  District Court Special Rules
    ju  Juvenile Court Rules                 ap  Rules of Appellate Procedure
    pr  Professional Rules

Each rule set is rendered one of two ways, both of which resolve to a single
"rule page" per citable rule:

  1. PLAIN     /court_rules/<code>/id/<rule_id>/
               (cr, cp, ev, gp, ra, sg, parts of ap)
  2. SUBTYPE   /court_rules/<code>/subtype/<subtype>/id/<rule_id>/
               (ms, ju, dc, pr, parts of ap — these sets are split into named
                sub-collections, e.g. pr/subtype/admi = Rules for Admission to
                the Bar)

Discovery: BFS over each set's landing page + every linked
/court_rules/rule/<slug> "Table of Headnotes" page, harvesting every id/ link
(both URL forms). ~950-1000 rules total.

A rule page wraps the substantive rule in:
    <div class="court_rule" id="courtr.<group>-<rule_id>">
      <h3 class="courtr_no">Rule N.<span class="headnote">TITLE</span></h3>
      <div class="sub_rule" id="N.MM"> ... <p class="i1/i2"> body </p> ...
      <p>(Amended effective July 1, 2013; ... September 1, 2020.)</p>   <- history
      <div class="comment"><h3 class="header">Advisory Committee Comment - ...
    </div>
(The outer <div class="court_rule" id="xtend"> is page chrome, skipped.)

------------------------------------------------------------------------------
RICH METADATA (captured, NEVER stripped)
------------------------------------------------------------------------------
    effective_date        most-recent "effective <date>" in the history line
    prior_effective_dates earlier effective dates (the amendment trail)
    amendment_history     raw "(Amended effective ...)" paragraph(s)
    committee_comment      full Advisory Committee / Task Force / Committee
                           Comment block(s), joined
    rule_set / chapter     the set (Civil/Criminal/Evidence/...) is the "chapter"

corpus_type='state_rules'. act_id='SRULES_MN_<ruleset>_R<rule sanitized>'.

Geo-restricted; Webshare US proxy + Chrome UA + polite pacing (the site 429s
under heavy concurrency).
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
import urllib.parse
import uuid
from dataclasses import dataclass, field
from pathlib import Path

import requests
from bs4 import BeautifulSoup

sys.stdout.reconfigure(line_buffering=True)  # type: ignore[attr-defined]

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = Path(os.environ.get("OUT_DIR", "./data"))
OUT = DATA_DIR / "state_mn_court_rules.jsonl"

MN_BASE = "https://www.revisor.mn.gov"
MN_COURT_RULES = f"{MN_BASE}/court_rules/"

_MOZ_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
)

# ---------------------------------------------------------------------------
# Rule-set metadata: code -> (human name, official citation prefix)
# Citation forms follow the Minnesota Rules of Court / Bluebook conventions.
# ---------------------------------------------------------------------------
RULE_SETS: dict[str, tuple[str, str]] = {
    "cp": ("Rules of Civil Procedure", "Minn. R. Civ. P."),
    "cr": ("Rules of Criminal Procedure", "Minn. R. Crim. P."),
    "ev": ("Rules of Evidence", "Minn. R. Evid."),
    "ap": ("Rules of Civil Appellate Procedure", "Minn. R. Civ. App. P."),
    "gp": ("General Rules of Practice", "Minn. Gen. R. Prac."),
    "ju": ("Juvenile Court Rules", "Minn. R. Juv. P."),
    "pr": ("Professional Rules", "Minn. R. Prof."),
    "ra": ("Rules of Public Access to Records", "Minn. R. Pub. Access"),
    "sg": ("Sentencing Guidelines", "Minn. Sent. Guidelines"),
    "ms": ("Miscellaneous Rules", "Minn. Misc. R."),
    "dc": ("District Court Special Rules", "Minn. Dist. Ct. Spec. R."),
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


def _us_proxies() -> dict | None:
    user = os.environ.get("WEBSHARE_USERNAME", "")
    pwd = os.environ.get("WEBSHARE_PASSWORD", "")
    if not user or not pwd:
        return None
    proxy_user = f"{user}-US-rotate"
    host = os.environ.get("WEBSHARE_PROXY_HOST", "p.webshare.io")
    port = os.environ.get("WEBSHARE_PROXY_PORT", "80")
    url = f"http://{urllib.parse.quote(proxy_user)}:{urllib.parse.quote(pwd)}@{host}:{port}"
    return {"http": url, "https": url}


SESSION = requests.Session()
SESSION.headers.update({"User-Agent": _MOZ_UA})


def fetch(url: str, retries: int = 6) -> str | None:
    """Fetch raw HTML. Retries 429 with exponential backoff and tolerates the
    transient SSL/connection errors rotating proxies occasionally raise."""
    proxies = _us_proxies()
    for attempt in range(retries):
        try:
            r = SESSION.get(url, timeout=90, proxies=proxies, allow_redirects=True)
            if r.status_code == 200:
                return r.text
            if r.status_code == 404:
                return None
            if r.status_code == 429:
                time.sleep(2**attempt)
                continue
            if r.status_code in (502, 503, 504):
                time.sleep(2)
                continue
            return None
        except Exception:
            time.sleep(2)
    return None


# ---------------------------------------------------------------------------
# Discovery: BFS each rule set's landing + headnote pages -> rule page URLs
# ---------------------------------------------------------------------------

# Plain rule page: /court_rules/cp/id/3/
_ID_PLAIN_RE = re.compile(r"/court_rules/([a-z]+)/id/([\w.]+)/")
# Subtype rule page: /court_rules/pr/subtype/admi/id/4/
_ID_SUBTYPE_RE = re.compile(r"/court_rules/([a-z]+)/subtype/([a-z]+)/id/([\w.]+)/")
# Table-of-headnotes / sub-collection index pages: /court_rules/rule/<slug>
_RULE_INDEX_RE = re.compile(r"^/court_rules/rule/[\w-]+$")


@dataclass(frozen=True)
class RuleRef:
    """A discovered rule page, identified by its set code, optional subtype, and
    rule id. `group` is the container-id prefix the page uses (courtr.<group>-)."""

    code: str  # rule-set code, e.g. "cp", "pr"
    subtype: str  # "" for plain, e.g. "admi" for subtype pages
    rule_id: str  # e.g. "3", "5A", "702", "4"

    @property
    def url(self) -> str:
        if self.subtype:
            return f"{MN_BASE}/court_rules/{self.code}/subtype/{self.subtype}/id/{self.rule_id}/"
        return f"{MN_BASE}/court_rules/{self.code}/id/{self.rule_id}/"


def discover_rule_refs(code: str) -> list[RuleRef]:
    """BFS a rule set: walk its landing page + every linked headnote/index page,
    harvesting all rule page URLs (both plain and subtype forms).

    Cross-references in comment text occasionally link to OTHER sets' rules; we
    keep only refs whose set code matches `code` to avoid double-counting."""
    seen_pages: set[str] = set()
    refs: set[RuleRef] = set()
    queue: list[str] = [f"/court_rules/{code}/"]
    while queue:
        path = queue.pop()
        if path in seen_pages:
            continue
        seen_pages.add(path)
        html = fetch(MN_BASE + path)
        if not html:
            continue
        for c, st, rid in _ID_SUBTYPE_RE.findall(html):
            if c == code:
                refs.add(RuleRef(code=code, subtype=st, rule_id=rid))
        for c, rid in _ID_PLAIN_RE.findall(html):
            if c == code:
                refs.add(RuleRef(code=code, subtype="", rule_id=rid))
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"].split("#")[0].strip()
            if _RULE_INDEX_RE.match(href) and href not in seen_pages:
                queue.append(href)
    # A subtype rule shadows a plain ref with the same rule_id only when its set
    # genuinely has no plain pages; both can coexist (e.g. ap). Dedup by URL.
    return sorted(refs, key=lambda r: (r.subtype, _sort_key_id(r.rule_id)))


def _sort_key_id(rid: str) -> tuple:
    m = re.match(r"^(\d+)([A-Za-z]*)$", rid)
    if m:
        return (int(m.group(1)), m.group(2))
    return (10**9, rid)


# ---------------------------------------------------------------------------
# Rule page parsing
# ---------------------------------------------------------------------------

_MONTHS = "January|February|March|April|May|June|July|August|September|October|November|December"
_DATE_RE = re.compile(rf"(?:{_MONTHS})\s+\d{{1,2}},\s+\d{{4}}")
# An "(Adopted/Amended effective <date>; ...)" history parenthetical.
_HISTORY_PAREN_RE = re.compile(
    r"\(([^()]*\b(?:effective|adopted|amended|promulgated)\b[^()]*)\)", re.IGNORECASE
)


@dataclass
class Rule:
    code: str  # rule-set code, e.g. "cp"
    subtype: str  # "" or e.g. "admi"
    rule_set_name: str  # e.g. "Rules of Civil Procedure"
    citation_prefix: str  # e.g. "Minn. R. Civ. P."
    rule_id: str  # e.g. "3"
    rule_title: str  # e.g. "Commencement of the Action; ..."
    raw_text: str  # substantive body (comments stripped out)
    source_url: str
    effective_date: str = ""
    prior_effective_dates: list[str] = field(default_factory=list)
    amendment_history: str = ""  # raw "(Amended effective ...)" text
    committee_comment: str = ""  # joined Advisory/Task Force/Committee comments


def _clean(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _container(soup: BeautifulSoup):
    """Return the substantive rule container <div class="court_rule"
    id="courtr.<group>-<id>">, skipping the page-chrome wrapper (id="xtend")."""
    for d in soup.find_all("div", class_="court_rule"):
        if (d.get("id") or "").startswith("courtr."):
            return d
    return None


def _extract_comments(container) -> str:
    """Join every committee/task-force comment block on the page. These carry
    the official drafters' interpretive notes and MUST be preserved."""
    parts: list[str] = []
    for c in container.find_all("div", class_="comment"):
        parts.append(_clean(c.get_text(" ", strip=True)))
    return "\n\n".join(p for p in parts if p)


def _extract_history(container) -> tuple[str, list[str]]:
    """Return (raw_history_text, effective_dates_in_order). MN encodes the
    amendment trail in one or more "(Adopted/Amended effective <date>; ...)"
    paragraphs, NOT in a separate <div>."""
    raw_parts: list[str] = []
    dates: list[str] = []
    for pp in container.find_all("p"):
        t = pp.get_text(" ", strip=True)
        if len(t) > 500:
            continue
        m = _HISTORY_PAREN_RE.search(t)
        if not m:
            continue
        frag = m.group(1).strip()
        raw_parts.append(frag)
        dates.extend(_DATE_RE.findall(frag))
    # Dedupe dates preserving order.
    seen: set[str] = set()
    ordered: list[str] = []
    for d in dates:
        if d not in seen:
            seen.add(d)
            ordered.append(d)
    return ("; ".join(raw_parts), ordered)


def _body_text(container, rule_id: str, rule_title: str) -> str:
    """Substantive body, with committee comments and permalink chrome removed.
    Drops the leading "Rule N. Title" duplicate heading."""
    clone = BeautifulSoup(str(container), "html.parser")
    for el in clone.find_all("div", class_="comment"):
        el.decompose()
    for el in clone.find_all("a", class_="permalink"):
        el.decompose()
    body = _clean(clone.get_text(" ", strip=True))
    for lead in (
        f"Rule {rule_id}. {rule_title}",
        f"Rule {rule_id} {rule_title}",
        f"Rule {rule_id}.",
        f"Rule {rule_id}",
    ):
        if body.startswith(lead):
            body = body[len(lead) :].strip()
            break
    return body


def parse_rule(html: str, ref: RuleRef) -> Rule | None:
    soup = BeautifulSoup(html, "html.parser")
    container = _container(soup)
    if container is None:
        return None
    head = container.find(class_="courtr_no")
    if head is None:
        return None
    head_txt = _clean(head.get_text(" ", strip=True))
    m = re.match(r"^Rule\s+([\w.]+)\.?\s*(.*)$", head_txt)
    if m:
        rule_id = m.group(1).rstrip(".")
        rule_title = m.group(2).strip().rstrip(".")
    else:
        rule_id = ref.rule_id
        rule_title = head_txt
    if not rule_title:
        hn = head.find(class_="headnote")
        if hn:
            rule_title = hn.get_text(strip=True).rstrip(".")
    rule_title = rule_title or f"Rule {rule_id}"

    body = _body_text(container, rule_id, rule_title)
    if len(body) < 20:
        return None
    # Skip repealed/reserved shells.
    if re.search(r"^\s*\[(repealed|reserved|renumbered|abrogated)\b", body, re.IGNORECASE):
        return None

    amendment_history, dates = _extract_history(container)
    committee_comment = _extract_comments(container)
    effective_date = dates[-1] if dates else ""
    prior = dates[:-1] if len(dates) > 1 else []

    name, prefix = RULE_SETS.get(ref.code, (ref.code.upper(), f"Minn. R. {ref.code.upper()}"))
    return Rule(
        code=ref.code,
        subtype=ref.subtype,
        rule_set_name=name,
        citation_prefix=prefix,
        rule_id=rule_id,
        rule_title=rule_title,
        raw_text=body,
        source_url=ref.url,
        effective_date=effective_date,
        prior_effective_dates=prior,
        amendment_history=amendment_history,
        committee_comment=committee_comment,
    )


# ---------------------------------------------------------------------------
# Chunk emission (schema copied from ingest_state_court_rules.py)
# ---------------------------------------------------------------------------


def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def _point_id(act_id: str, idx: int, text: str) -> str:
    seed = f"{act_id}::{idx}::{_sha1(text)[:12]}"
    return str(uuid.UUID(hashlib.md5(seed.encode()).hexdigest()))


def _safe(s: str) -> str:
    return re.sub(r"[^0-9A-Za-z]+", "_", s).strip("_")


def to_chunk_record(r: Rule) -> dict:
    text = r.raw_text.strip()
    # ruleset key for act_id: include subtype so e.g. pr/admi != pr/cond.
    ruleset_key = f"{r.code}{('_' + r.subtype) if r.subtype else ''}".upper()
    act_id = f"SRULES_MN_{ruleset_key}_R{_safe(r.rule_id)}"
    citation = f"{r.citation_prefix} {r.rule_id}"
    full_corpus_name = "Minnesota Court Rules"
    chapter_name = f"{r.rule_set_name} ({r.subtype})" if r.subtype else r.rule_set_name

    meta_lines: list[str] = []
    if r.effective_date:
        meta_lines.append(f"Effective: {r.effective_date}")
    if r.amendment_history:
        meta_lines.append(f"History: {r.amendment_history}")
    meta_header = ("\n" + "\n".join(meta_lines)) if meta_lines else ""
    text_for_embedding = (
        f"Court Rule: {full_corpus_name} | US | Minnesota | In Force\n"
        f"{r.rule_set_name} | {citation}\n"
        f"Rule {r.rule_id}. {r.rule_title}{meta_header}\n\n{text}"
    )

    md = {
        "act_id": act_id,
        "corpus_type": "state_rules",
        # Canonical value per CANONICAL_CATEGORIES; see
        # app/services/us_statutes_taxonomy.py (was 'state_court_rule' —
        # 2026-07-16 audit fix).
        "category": "state_rules",
        "document_type": "court_rule",
        "jurisdiction": "US",
        "country_code": "US",
        "state": "mn",
        "title_number": None,
        "title_name": full_corpus_name,
        "title": full_corpus_name,
        "title_code": "rules_mn",
        "top_level_title": "rules-mn",
        "level_classifier": "rule",
        "chapter": r.code,
        "chapter_name": chapter_name,
        "section_number": r.rule_id,
        "section_title": r.rule_title,
        "year": 2026,
        "act_status": "in_force",
        "renumbered_to": "",
        "transferred_to": "",
        # --- court-rule-specific rich metadata (captured, not discarded) ---
        "rule_set": r.rule_set_name,
        "rule_set_code": r.code,
        "rule_subtype": r.subtype or None,
        "effective_date": r.effective_date or None,
        "prior_effective_dates": r.prior_effective_dates,
        "amendment_history": r.amendment_history or None,
        "committee_comment": r.committee_comment or None,
        "history_note": r.amendment_history or None,
        "citation": citation,
        "citation_short": citation,
        "display_label": citation,
        "display_title": f"Rule {r.rule_id}. {r.rule_title}".strip(),
        "display_path": (f"{full_corpus_name} / {r.rule_set_name} / Rule {r.rule_id}"),
        "breadcrumb": [
            {
                "type": "corpus",
                "num": "mn",
                "label": full_corpus_name,
                "name": full_corpus_name,
            },
            {
                "type": "chapter",
                "num": r.code,
                "label": r.rule_set_name,
                "name": chapter_name,
            },
            {
                "type": "rule",
                "num": r.rule_id,
                "label": f"Rule {r.rule_id}",
                "name": r.rule_title,
            },
        ],
        "sort_key": act_id,
        "word_count": len(text.split()),
        "subsection_count": 0,
        "subsection_letters": [],
        "numbered_paragraph_count": 0,
        "cross_references_count": 0,
        "cross_references_usc": [],
        "cross_references_cfr": [],
        "amendment_years": sorted(
            {
                int(m.group(0))
                for d in [r.effective_date, *r.prior_effective_dates]
                if (m := re.search(r"\d{4}", d))
            }
        ),
        "amendments_count": len(r.prior_effective_dates) + (1 if r.effective_date else 0),
        "last_amended_year": (
            int(m.group(0))
            if r.effective_date and (m := re.search(r"\d{4}", r.effective_date))
            else None
        ),
        "public_laws_referenced": [],
        "public_laws_count": 0,
        "source_url": r.source_url,
        "parent_id": f"us/mn/court_rules/set={r.code}"
        + (f"/subtype={r.subtype}" if r.subtype else ""),
        "raw_node_id": (
            f"us/mn/court_rules/set={r.code}"
            + (f"/subtype={r.subtype}" if r.subtype else "")
            + f"/rule={r.rule_id}"
        ),
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


def process_ref(ref: RuleRef) -> Rule | None:
    html = fetch(ref.url)
    if not html:
        return None
    rule = parse_rule(html, ref)
    if rule is None:
        return None
    return rule


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--sets",
        default="",
        help="Comma-separated rule-set codes (e.g. 'cp,cr,ev'). Default: all.",
    )
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--delay", type=float, default=0.5)
    ap.add_argument("--dry-run", action="store_true", help="Discover only; no fetch/write.")
    args = ap.parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    codes = list(RULE_SETS.keys())
    if args.sets:
        wanted = {c.strip().lower() for c in args.sets.split(",") if c.strip()}
        codes = [c for c in codes if c in wanted]

    print(f"[MN-CR] discovering rules for sets: {codes}", flush=True)
    all_refs: list[RuleRef] = []
    with cf.ThreadPoolExecutor(max_workers=min(len(codes), 6)) as ex:
        for code, refs in zip(codes, ex.map(discover_rule_refs, codes), strict=True):
            print(f"  {code}: {len(refs)} rules", flush=True)
            all_refs.extend(refs)
    print(f"[MN-CR] {len(all_refs)} rule pages to fetch", flush=True)
    if args.dry_run:
        return 0

    chunks: list[dict] = []
    t0 = time.time()
    with cf.ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {ex.submit(process_ref, ref): ref for ref in all_refs}
        for done, fut in enumerate(cf.as_completed(futures), start=1):
            try:
                rule = fut.result()
                if rule is not None:
                    chunks.append(to_chunk_record(rule))
            except Exception as e:
                print(f"  ! {futures[fut].url} failed: {e}", flush=True)
            if done % 50 == 0 or done == len(all_refs):
                rate = len(chunks) / max(time.time() - t0, 0.1)
                print(
                    f"  ... {done:>4}/{len(all_refs)} pages, {len(chunks):>5} rules, {rate:.1f}/s",
                    flush=True,
                )
            time.sleep(args.delay / max(args.workers, 1))

    # Dedup by point_id + append.
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

    print(
        f"\n=== Done: parsed={len(chunks):,}, new={written:,}, elapsed={time.time() - t0:.1f}s ===",
        flush=True,
    )
    print(f"JSONL: {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
