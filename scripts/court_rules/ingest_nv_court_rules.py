#!/usr/bin/env python3
"""Ingest Nevada Court Rules from the OFFICIAL Nevada Legislature source.

Source: https://www.leg.state.nv.us/CourtRules/
Index lists ~42 rule-set HTML pages (NRCP.html, NRCrP.html, NRAP.html,
SCR.html, DCR.html, FirstDCR.html .. EleventhDCR.html, JCRCP.html, etc.).
Each rule-set page is a single big HTML doc with every rule inline.

Rule-heading markup (uniform across rule sets):
    <p class="SectBody">
      <a name="{SLUG}Rule{N}"></a>
      <span class="Empty">Rule </span>
      <span class="Section">{N}</span>
      <span class="Leadline">.</span>
      <span class="Empty">  </span>
      <span class="Leadline">{rule heading}</span>
    </p>
Rule number "N" is the anchor suffix after "Rule" — periods are encoded as
underscores ("4.1" -> "Rule4_1"). Sub-rules like "3A" are written verbatim.

Amendment-history brackets are in <p class="SourceNote">:
    "[Added; effective March 1, 2019.]"
    "[Amended; effective Jan 1, 2020. As amended through 2024.]"
Each rule's body runs from its <p class="SectBody"> heading paragraph up to
the next rule heading, with one or more trailing <p class="SourceNote">
elements that we capture as `amendment_history`.

Output: state_nv_court_rules.jsonl (corpus_type='state_rules').
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
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup, Tag

sys.stdout.reconfigure(line_buffering=True)  # type: ignore[attr-defined]

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = Path(os.environ.get("OUT_DIR", "./data"))
OUT = DATA_DIR / "state_nv_court_rules.jsonl"

NV_BASE = "https://www.leg.state.nv.us/CourtRules"
NV_INDEX = f"{NV_BASE}/"

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
    proxy_user = f"{user}-US-rotate"
    host = os.environ.get("WEBSHARE_PROXY_HOST", "p.webshare.io")
    port = os.environ.get("WEBSHARE_PROXY_PORT", "80")
    url = (
        f"http://{urllib.parse.quote(proxy_user)}:{urllib.parse.quote(pwd)}@{host}:{port}"
    )
    return {"http": url, "https": url}


SESSION = requests.Session()
SESSION.headers.update({"User-Agent": _MOZ_UA})


def fetch(url: str, retries: int = 5) -> Optional[str]:
    """GET with Webshare US rotate + 429/5xx backoff. 502 commonly means the
    proxy IP is bad — retry yields a fresh IP."""
    proxies = _us_proxies()
    for attempt in range(retries):
        try:
            r = SESSION.get(url, timeout=60, proxies=proxies, allow_redirects=True)
            if r.status_code == 200:
                return r.text
            if r.status_code == 404:
                return None
            if r.status_code == 429:
                time.sleep(2 ** attempt)
                continue
            if r.status_code in (500, 502, 503, 504):
                time.sleep(1 + attempt)
                continue
            return None
        except Exception:
            time.sleep(1 + attempt)
    return None


# ---------------------------------------------------------------------------
# Rule-set registry
# ---------------------------------------------------------------------------
# Per rule-set: slug -> {name, citation_prefix, anchor_prefixes}.
# Slug is the .html filename without extension. `anchor_prefixes` lists the
# tokens that appear BEFORE the literal "Rule" in <a name="..."> for that
# page. Most rule sets use the slug itself (e.g. NRCP -> "NRCPRule1"), but a
# handful use legacy internal prefixes (e.g. Guardianship.html -> "GRRule1",
# JCR_LV.html -> "JCR_LVLRPRule1_1"). One rule set can host multiple groups
# (SCR_AudTranEquip splits IX-A and IX-B into four sub-prefixes).
RULE_SETS: dict[str, dict] = {
    "NRCP":  {"name": "Nevada Rules of Civil Procedure",                              "citation_prefix": "Nev. R. Civ. P.",                  "anchor_prefixes": ["NRCP"]},
    "NRCrP": {"name": "Nevada Rules of Criminal Practice",                            "citation_prefix": "Nev. R. Crim. P.",                 "anchor_prefixes": ["NRCrP"]},
    "NSTR":  {"name": "Nevada Short Trial Rules",                                     "citation_prefix": "Nev. Short Trial R.",              "anchor_prefixes": ["NSTR"]},
    "NATR":  {"name": "Nevada Alternate Trial Rules",                                 "citation_prefix": "Nev. Alt. Trial R.",               "anchor_prefixes": ["NATR"]},
    "RGADR": {"name": "Rules Governing Alternative Dispute Resolution",               "citation_prefix": "Nev. R. ADR",                      "anchor_prefixes": ["RGADR"]},
    "FMR":   {"name": "Nevada Foreclosure Mediation Rules",                           "citation_prefix": "Nev. Foreclosure Mediation R.",    "anchor_prefixes": ["FMR"]},
    "Guardianship": {"name": "Statewide Rules for Guardianship",                      "citation_prefix": "Nev. R. Guardianship",             "anchor_prefixes": ["GR"]},
    "NEFCR": {"name": "Nevada Electronic Filing and Conversion Rules",                "citation_prefix": "NEFCR",                            "anchor_prefixes": ["NEFCR"]},
    "DCR":   {"name": "Rules of the District Court of the State of Nevada",           "citation_prefix": "Nev. DCR",                         "anchor_prefixes": ["DCR"]},
    "FirstDCR":    {"name": "Rules of Practice for the First Judicial District Court",    "citation_prefix": "Nev. 1st Jud. DCR",            "anchor_prefixes": ["FirstDCR"]},
    "SecondDCR":   {"name": "Rules of Practice for the Second Judicial District Court",   "citation_prefix": "Nev. 2d Jud. DCR",             "anchor_prefixes": ["SecondDCR"]},
    "SecondDCR_Crim": {"name": "Criminal Rules of Practice for the Second Judicial District Court", "citation_prefix": "Nev. 2d Jud. DCR Crim.", "anchor_prefixes": ["SecondDCR_Crim"]},
    "ThirdDCR":    {"name": "Rules of Practice for the Third Judicial District Court",    "citation_prefix": "Nev. 3d Jud. DCR",             "anchor_prefixes": ["ThirdDCR"]},
    "FourthDCR":   {"name": "Rules of Practice for the Fourth Judicial District Court",   "citation_prefix": "Nev. 4th Jud. DCR",            "anchor_prefixes": ["FourthDCR"]},
    "SeventhDCR":  {"name": "Rules of Practice for the Seventh Judicial District Court",  "citation_prefix": "Nev. 7th Jud. DCR",            "anchor_prefixes": ["SeventhDCR"]},
    "EighthDCR":   {"name": "Rules of Practice for the Eighth Judicial District Court",   "citation_prefix": "Nev. 8th Jud. DCR",            "anchor_prefixes": ["EighthDCR"]},
    "NinthDCR":    {"name": "Rules of Practice for the Ninth Judicial District Court",    "citation_prefix": "Nev. 9th Jud. DCR",            "anchor_prefixes": ["NinthDCR"]},
    "TenthDCR":    {"name": "Rules of Practice for the Tenth Judicial District Court",    "citation_prefix": "Nev. 10th Jud. DCR",           "anchor_prefixes": ["TenthDCR"]},
    "EleventhDCR": {"name": "Rules of Practice for the Eleventh Judicial District Court", "citation_prefix": "Nev. 11th Jud. DCR",           "anchor_prefixes": ["EleventhDCR"]},
    "JCRCP": {"name": "Nevada Justice Court Rules of Civil Procedure",                "citation_prefix": "Nev. JCRCP",                       "anchor_prefixes": ["JCRCP"]},
    "Civil_Traffic_Infractions": {"name": "Justice and Municipal Court Rules for Civil Traffic Infractions", "citation_prefix": "Nev. Civ. Traffic Infraction R.", "anchor_prefixes": ["NRCTI"]},
    "JCR_Henderson":   {"name": "Local Rules of Practice for the Justice Court of Henderson Township",            "citation_prefix": "Henderson JCR", "anchor_prefixes": ["JCR_Hen"]},
    "JCR_LVTownship":  {"name": "Justice Court Rules of Las Vegas Township",                                       "citation_prefix": "LV Twp. JCR",   "anchor_prefixes": ["JCR_LV"]},
    "JCR_LV":          {"name": "Las Vegas Justice Court Local Rules of Practice",                                 "citation_prefix": "LVJCR",         "anchor_prefixes": ["JCR_LVLRP"]},
    "JCR_NLV":         {"name": "Local Rules of Practice for the Justice Court of North Las Vegas Township",       "citation_prefix": "NLVJCR",        "anchor_prefixes": ["JCR_NLV"]},
    "JCR_Pahrump":     {"name": "Local Rules of Practice for the Justice Court of Pahrump Township",               "citation_prefix": "Pahrump JCR",   "anchor_prefixes": ["JCR_PT"]},
    "JCR_Reno":        {"name": "Local Rules of Practice for the Justice Court of Reno Township",                  "citation_prefix": "Reno JCR",      "anchor_prefixes": ["JCR_Reno"]},
    "JCR_Rural":       {"name": "Local Rules of Practice for the Rural Justice Courts in the State of Nevada",     "citation_prefix": "Nev. Rural JCR","anchor_prefixes": ["JCR_Rural"]},
    "RPC":   {"name": "Nevada Rules of Professional Conduct",                         "citation_prefix": "Nev. RPC",                         "anchor_prefixes": ["RPC"]},
    "Conduct_CWC": {"name": "Nevada Rules of Conduct for Lawyers Representing Children in Child Welfare Cases", "citation_prefix": "Nev. CWC Conduct R.", "anchor_prefixes": ["CWC"]},
    "MRRS":  {"name": "Nevada Minimum Records of Retention Schedule",                 "citation_prefix": "Nev. MRRS",                        "anchor_prefixes": ["MRRS"]},
    "NRAP":  {"name": "Nevada Rules of Appellate Procedure",                          "citation_prefix": "Nev. R. App. P.",                  "anchor_prefixes": ["NRAP"]},
    "NRAD":  {"name": "Nevada Rules on the Administrative Docket",                    "citation_prefix": "Nev. R. Admin. Docket",            "anchor_prefixes": ["NRAD"]},
    "SCR":          {"name": "Nevada Supreme Court Rules (Parts I-V)",                "citation_prefix": "SCR",                              "anchor_prefixes": ["SCR"]},
    "SCR_CJC":      {"name": "Revised Nevada Code of Judicial Conduct (Part VI)",     "citation_prefix": "Nev. Code Jud. Conduct",           "anchor_prefixes": ["SCR_CJC"]},
    "SCR_RGSRCR":   {"name": "Rules Governing Sealing and Redacting Court Records (Part VII)", "citation_prefix": "Nev. R. Sealing & Redacting", "anchor_prefixes": ["SCR_SRCR"]},
    "SCR_RJE":      {"name": "Rules Governing the Standing Committee on Judicial Ethics (Part VIII)", "citation_prefix": "Nev. R. Jud. Ethics Comm.", "anchor_prefixes": ["SCR_RJE"]},
    "SCR_AudTranEquip": {"name": "Rules Governing Appearance by Audiovisual Transmission Equipment (Part IX-A & IX-B)", "citation_prefix": "Nev. R. Audiovisual",
                         "anchor_prefixes": ["SCR_ATE_IX_A_A_", "SCR_ATE_IX_A_B_", "SCR_ATE_IX_B_A_", "SCR_ATE_IX_B_B_"]},
    "SCR_Fees":     {"name": "Rules Governing the Collection of Fees and Charges (Part X)", "citation_prefix": "Nev. R. Fees",                 "anchor_prefixes": ["SCR_FEE"]},
    "SCR_REE":      {"name": "Rules Pertaining to Exhibits Marked and/or Admitted Into Evidence (Part XI)", "citation_prefix": "Nev. R. Exhibits", "anchor_prefixes": ["SCR_REE"]},
    "SCR_Addenda":  {"name": "Nevada Supreme Court Rules Addendum",                   "citation_prefix": "Nev. SCR Add.",                    "anchor_prefixes": ["SCR_Add"]},
    "PCD":   {"name": "Policy for Handling Filed, Lodged, and Presumptively Confidential Documents", "citation_prefix": "Nev. PCD", "anchor_prefixes": ["PCD"]},
}


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------
def discover_rule_sets() -> list[str]:
    """Scrape index for all linked .html files. Falls back to the static
    RULE_SETS keys if discovery fails."""
    html = fetch(NV_INDEX)
    if not html:
        return list(RULE_SETS.keys())
    soup = BeautifulSoup(html, "html.parser")
    found: list[str] = []
    seen: set[str] = set()
    for a in soup.find_all("a", href=True):
        h = a.get("href", "")
        if not h or "://" in h:
            continue
        # We expect relative refs like "NRCP.html"
        m = re.match(r"^([A-Za-z0-9_]+)\.html?$", h)
        if not m:
            continue
        slug = m.group(1)
        if slug in seen:
            continue
        seen.add(slug)
        found.append(slug)
    # Order: keep discovery order but ensure registered ones come first for stability
    return found or list(RULE_SETS.keys())


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------
_MONTH = (
    "January|February|March|April|May|June|July|August|September|"
    "October|November|December|Jan\\.|Feb\\.|Mar\\.|Apr\\.|Jun\\.|Jul\\.|"
    "Aug\\.|Sept\\.|Sep\\.|Oct\\.|Nov\\.|Dec\\."
)
_DATE_RE = re.compile(rf"\b({_MONTH})\s+\d{{1,2}},\s+\d{{4}}", re.IGNORECASE)

# Anchor-name -> (anchor_prefix, rule number). We accept any of the configured
# anchor_prefixes for the rule set, followed by literal "Rule", then digits +
# optional "_NN" (period-encoded sub-rules like 4_1) or letter suffix like
# "3A". Returns (matched_prefix, rule_num) or None.
def _anchor_to_rule_num(
    anchor_prefixes: list[str], anchor_name: str
) -> Optional[tuple[str, str]]:
    # Try longest prefix first so e.g. "SCR_SRCR" wins over "SCR".
    for prefix in sorted(anchor_prefixes, key=len, reverse=True):
        full = f"{prefix}Rule"
        if not anchor_name.startswith(full):
            continue
        raw = anchor_name[len(full):]
        if not raw:
            continue
        if not re.match(r"^[0-9A-Za-z][0-9A-Za-z_]*$", raw):
            continue
        return prefix, raw.replace("_", ".")
    return None


def _heading_text(p: Tag) -> str:
    """Reconstruct the rule heading from a <p class='SectBody'> with an
    anchor. Strips the anchor and returns 'Rule N. Heading text'."""
    # Drop the leading <a name=...> if present
    text = p.get_text(" ", strip=True)
    # Collapse whitespace + soft hyphens
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _is_rule_anchor_paragraph(
    p: Tag, anchor_prefixes: list[str]
) -> Optional[str]:
    """If this <p> contains the first rule-heading <a name="..."> matching
    one of the configured anchor prefixes, return the rule number; else
    None. This avoids matching non-rule anchors like 'SCRPartI'."""
    if not isinstance(p, Tag):
        return None
    if p.name != "p":
        return None
    a = p.find("a", attrs={"name": True})
    if not a or not isinstance(a, Tag):
        return None
    result = _anchor_to_rule_num(anchor_prefixes, str(a.get("name", "")))
    return result[1] if result else None


@dataclass
class Rule:
    slug: str                          # rule-set slug, e.g. "NRCP"
    set_name: str                      # full rule-set name
    citation_prefix: str               # e.g. "Nev. R. Civ. P."
    rule_num: str                      # e.g. "4.1" or "3A"
    section_title: str                 # raw heading text, e.g. "Rule 4. Summons and Service"
    raw_text: str                      # body without amendment-history brackets
    amendment_history: list[str] = field(default_factory=list)
    effective_date: str = ""
    prior_effective_dates: list[str] = field(default_factory=list)
    source_url: str = ""


def parse_rule_set(html: str, slug: str) -> list[Rule]:
    """Walk every <p> in document order. When we hit a rule-heading <p>, open
    a new rule. All subsequent siblings become the body until the next rule
    heading. Trailing <p class='SourceNote'> entries are split off as
    amendment_history."""
    soup = BeautifulSoup(html, "html.parser")
    meta = RULE_SETS.get(slug, {"name": slug, "citation_prefix": slug, "anchor_prefixes": [slug]})
    set_name = meta["name"]
    citation_prefix = meta["citation_prefix"]
    anchor_prefixes = meta.get("anchor_prefixes", [slug])

    # Linearize all <p> elements in document order so we don't depend on the
    # exact container hierarchy (some pages wrap groups in <div>, others
    # don't).
    paragraphs = list(soup.find_all("p"))

    rules: list[Rule] = []
    current: Optional[dict] = None

    def _flush(cur: dict) -> None:
        body_parts: list[str] = []
        history: list[str] = []
        for kind, text in cur["chunks"]:
            if kind == "history":
                history.append(text)
            else:
                if text:
                    body_parts.append(text)
        raw_text = re.sub(r"\s+", " ", " ".join(body_parts)).strip()
        if not raw_text:
            return  # heading-only stub; drop

        # Effective date = first date found in (a) the most recent
        # "[Added; effective ...]" then (b) any history bracket; fall back to
        # first date inside body.
        effective_date = ""
        prior_dates: list[str] = []
        for h in history:
            for m in _DATE_RE.finditer(h):
                d = m.group(0)
                if not effective_date and ("Added" in h or "effective" in h.lower()):
                    effective_date = d
                if d not in prior_dates:
                    prior_dates.append(d)
        if not effective_date and prior_dates:
            effective_date = prior_dates[0]

        rules.append(Rule(
            slug=slug,
            set_name=set_name,
            citation_prefix=citation_prefix,
            rule_num=cur["rule_num"],
            section_title=cur["heading"],
            raw_text=raw_text,
            amendment_history=history,
            effective_date=effective_date,
            prior_effective_dates=prior_dates,
            source_url=f"{NV_BASE}/{slug}.html#{slug}Rule{cur['rule_num'].replace('.', '_')}",
        ))

    for p in paragraphs:
        rule_num = _is_rule_anchor_paragraph(p, anchor_prefixes)
        if rule_num is not None:
            if current is not None:
                _flush(current)
            current = {
                "rule_num": rule_num,
                "heading": _heading_text(p),
                "chunks": [],
            }
            continue
        if current is None:
            continue
        # Skip non-paragraph junk
        classes = p.get("class") or []
        text = p.get_text(" ", strip=True)
        if not text:
            continue
        if "SourceNote" in classes or text.startswith("[") and text.endswith("]"):
            current["chunks"].append(("history", text))
        else:
            current["chunks"].append(("body", text))

    if current is not None:
        _flush(current)
    return rules


# ---------------------------------------------------------------------------
# Chunk emission
# ---------------------------------------------------------------------------
def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def _point_id(act_id: str, idx: int, text: str) -> str:
    seed = f"{act_id}::{idx}::{_sha1(text)[:12]}"
    return str(uuid.UUID(hashlib.md5(seed.encode()).hexdigest()))


def _safe(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9]", "_", s).strip("_")


def to_chunk_record(r: Rule) -> dict:
    text = r.raw_text
    act_id = f"SRULES_NV_{r.slug.upper()}_R{_safe(r.rule_num)}"
    citation = f"{r.citation_prefix} {r.rule_num}"

    meta_lines: list[str] = []
    if r.effective_date:
        meta_lines.append(f"Effective: {r.effective_date}")
    if r.amendment_history:
        meta_lines.append("History: " + " ".join(r.amendment_history))
    meta_header = ("\n" + "\n".join(meta_lines)) if meta_lines else ""

    text_for_embedding = (
        f"Nevada Court Rules | US | Nevada | In Force\n"
        f"{r.set_name} | {citation}\n"
        f"{r.section_title}{meta_header}\n\n{text}"
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
        "state": "nv",
        "title_number": None,
        "title_name": "Nevada Court Rules",
        "title": "Nevada Court Rules",
        "title_code": "rules_nv",
        "top_level_title": "rules-nv",
        "level_classifier": "rule",
        "chapter": r.slug,
        "chapter_name": r.set_name,
        "section_number": r.rule_num,
        "section_title": r.section_title,
        "year": 2026,
        "act_status": "in_force",
        "renumbered_to": "",
        "transferred_to": "",
        # --- rule-specific rich metadata ---
        "effective_date": r.effective_date or None,
        "amendment_history": r.amendment_history,
        "prior_effective_dates": r.prior_effective_dates,
        "citation": citation,
        "citation_short": citation,
        "display_label": citation,
        "display_title": r.section_title,
        "display_path": f"Nevada Court Rules / {r.set_name} / Rule {r.rule_num}",
        "breadcrumb": [
            {"type": "title", "num": "rules-nv", "label": "Nevada Court Rules", "name": "Nevada Court Rules"},
            {"type": "rule_set", "num": r.slug, "label": r.slug, "name": r.set_name},
            {"type": "rule", "num": r.rule_num, "label": f"Rule {r.rule_num}", "name": r.section_title},
        ],
        "sort_key": act_id,
        "word_count": len(text.split()),
        "subsection_count": 0,
        "subsection_letters": [],
        "numbered_paragraph_count": 0,
        "cross_references_count": 0,
        "cross_references_usc": [],
        "cross_references_cfr": [],
        "amendments_count": len(r.amendment_history),
        "amendment_years": sorted({
            int(m.group(0)) for h in r.amendment_history
            for m in re.finditer(r"\b(19|20)\d{2}\b", h)
        }),
        "last_amended_year": (
            max((int(m.group(0)) for h in r.amendment_history
                 for m in re.finditer(r"\b(19|20)\d{2}\b", h)), default=None)
        ),
        "public_laws_referenced": [],
        "public_laws_count": 0,
        "source_url": r.source_url,
        "parent_id": f"us/nv/court_rules/{r.slug}",
        "raw_node_id": f"us/nv/court_rules/{r.slug}/rule={r.rule_num}",
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
def process_rule_set(slug: str) -> list[Rule]:
    url = f"{NV_BASE}/{slug}.html"
    html = fetch(url)
    if not html:
        print(f"  [{slug}] failed to fetch", flush=True)
        return []
    rules = parse_rule_set(html, slug)
    print(f"  [{slug}] {len(rules)} rules", flush=True)
    return rules


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--rule-sets",
        default="",
        help="Comma-separated slugs (e.g. 'NRCP,NRAP'). Default: all discovered.",
    )
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--delay", type=float, default=0.5,
                    help="Per-task pacing to be polite to leg.state.nv.us.")
    ap.add_argument("--dry-run", action="store_true",
                    help="Just discover rule-set slugs and exit.")
    args = ap.parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    print(f"[NV] discovering rule sets from {NV_INDEX}", flush=True)
    discovered = discover_rule_sets()
    print(f"[NV] discovered {len(discovered)} rule sets", flush=True)

    if args.rule_sets:
        wanted = {s.strip() for s in args.rule_sets.split(",") if s.strip()}
        slugs = [s for s in discovered if s in wanted]
        # Allow caller to pass slugs not in discovery (e.g. for testing)
        for s in wanted:
            if s not in slugs:
                slugs.append(s)
    else:
        slugs = discovered

    print(f"[NV] processing {len(slugs)} rule sets", flush=True)
    if args.dry_run:
        for s in slugs:
            print(f"  - {s}: {RULE_SETS.get(s, {}).get('name', '(unregistered)')}")
        return 0

    chunks: list[dict] = []
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {ex.submit(process_rule_set, s): s for s in slugs}
        done = 0
        for fut in as_completed(futures):
            done += 1
            try:
                rules = fut.result()
                chunks.extend(to_chunk_record(r) for r in rules)
            except Exception as e:
                print(f"  ! rule-set failed: {e}", flush=True)
            if done % 5 == 0 or done == len(slugs):
                rate = len(chunks) / max(time.time() - t0, 0.1)
                print(
                    f"  ... {done:>3}/{len(slugs)} rule sets, "
                    f"{len(chunks):>5} rules, {rate:.1f}/s",
                    flush=True,
                )
            time.sleep(args.delay / max(args.workers, 1))

    # Dedup + append
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
        f"\n=== Done: parsed={len(chunks):,}, new={written:,}, "
        f"elapsed={time.time()-t0:.1f}s ===",
        flush=True,
    )
    print(f"JSONL: {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
