#!/usr/bin/env python3
"""Ingest Arizona Court Rules from the OFFICIAL public source.

Source: https://govt.westlaw.com/azrules/

This is the AZ Supreme Court's designated free public Weblinks instance (run by
Thomson Reuters for the AZ Judicial Branch, linked from azcourts.gov/Rules).
The Westlaw Weblinks platform serves a clean, unauthenticated HTML view of all
40+ AZ rule sets. NOT the paywalled Westlaw research product.

Topology:
    /azrules/Browse/Home/Arizona/ArizonaCourtRules
        |-- 40 top-level rule sets (each with guid=N...)
            |-- For larger sets: nested Browse pages (Parts I, II, III; Articles)
            |-- Leaf rules: /azrules/Document/<GUID>?viewType=FullText...

A BrowserHawk "click here to continue" gate sits in front of Browse pages,
bypassed cleanly by appending &bhcp=1 to every Browse URL. Cloudflare also
fronts the host: requests without a browser-grade TLS fingerprint get a
turnstile 403, so we use curl_cffi with chrome impersonation.

Rule document HTML uses these structural divs (no scraping ambiguity):
    div.co_title          -> "Rule 4. Summons"
    div.co_effectiveDate  -> "Effective: January 1, 2026"
    div.co_cites          -> "16 A.R.S. Rules of Civil Procedure, Rule 4"
    div.co_prelimHead     -> rule-set + Roman-numeral section breadcrumb
    div.co_body           -> rule text (paragraphs, subsections)
    div.co_printHeading > h2 "Credits"
        + following div.co_paragraph
                          -> amendment history
                             ("Added Sept. 2, 2016, effective Jan. 1, 2017.
                               Amended Aug. 31, 2017, effective Jan. 1, 2018;
                               Aug. 28, 2025, effective Jan. 1, 2026.")

Output: state_az_court_rules.jsonl (corpus_type='state_rules').
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

from bs4 import BeautifulSoup, Tag

# curl_cffi mimics real Chrome TLS/JA3 to bypass Cloudflare turnstile.
# Plain requests + Webshare proxy yields a hard 403 on this host.
from curl_cffi import requests as cf_requests  # type: ignore[import-not-found]

sys.stdout.reconfigure(line_buffering=True)  # type: ignore[attr-defined]

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = Path(os.environ.get("OUT_DIR", "./data"))
OUT = DATA_DIR / "state_az_court_rules.jsonl"

AZ_BASE = "https://govt.westlaw.com"
AZ_INDEX = f"{AZ_BASE}/azrules/Browse/Home/Arizona/ArizonaCourtRules"

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


def _us_proxies() -> dict | None:
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


_IMPERSONATE = os.environ.get("AZ_CURL_IMPERSONATE", "chrome")


def _with_bhcp(url: str) -> str:
    """Append &bhcp=1 to Browse URLs so the BrowserHawk gate is skipped.
    Document URLs are not gated, but adding the flag is harmless."""
    if "bhcp=" in url:
        return url
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}bhcp=1"


def fetch(url: str, retries: int = 8) -> str | None:
    """GET with browser TLS impersonation (curl_cffi) + Webshare US rotate +
    long Cloudflare-aware backoff. govt.westlaw.com sits behind Cloudflare
    turnstile, so a real-Chrome JA3 is required to avoid an immediate 403.

    Cloudflare also rate-limits this host's DC-proxy egress hard; once an IP
    bucket trips, it stays in a turnstile for ~30-90 s. We back off long
    (capped at 60 s per attempt) and rely on Webshare's rotate endpoint
    handing out a fresh egress IP on each retry."""
    proxies = _us_proxies()
    target = _with_bhcp(url)
    last_status = None
    for attempt in range(retries):
        try:
            # Note: do NOT pass custom Accept/Accept-Language headers — they
            # override the impersonation profile's fingerprint and Cloudflare
            # returns 403.
            r = cf_requests.get(
                target,
                impersonate=_IMPERSONATE,
                proxies=proxies,
                timeout=60,
                allow_redirects=True,
            )
            last_status = r.status_code
            if r.status_code == 200:
                # Some Browse pages still return the BrowserHawk noscript page;
                # detect and re-request the bhjs=0 fallback that ships with it.
                if "bhawkTest" in r.text and "Please click here to continue" in r.text:
                    alt = _with_bhcp(re.sub(r"&bhcp=1", "&bhjs=0", target))
                    r2 = cf_requests.get(
                        alt, impersonate=_IMPERSONATE, proxies=proxies,
                        timeout=60, allow_redirects=True,
                    )
                    if r2.status_code == 200 and "bhawkTest" not in r2.text:
                        return r2.text
                    time.sleep(3 + attempt)
                    continue
                return r.text
            if r.status_code == 404:
                return None
            if r.status_code in (403, 429):
                # Cloudflare bucket. Back off hard so the rotate endpoint
                # has time to issue a new exit IP.
                time.sleep(min(8 + attempt * 6, 60))
                continue
            if r.status_code in (500, 502, 503, 504):
                time.sleep(2 + attempt)
                continue
            return None
        except Exception:
            time.sleep(2 + attempt)
    if last_status is not None:
        print(f"  fetch giving up: {url} (last status={last_status})", flush=True)
    return None


# ---------------------------------------------------------------------------
# Rule-set registry
# ---------------------------------------------------------------------------
# Each entry: slug -> {guid, name, citation_prefix}.
# slug is a stable short token used for act_id, R2 keys, and folder paths.
# guid is the Westlaw category GUID exposed on /azrules/Browse/Home/Arizona/ArizonaCourtRules.
# citation_prefix follows the Arizona Citation Manual / Bluebook T1 abbreviations.
RULE_SETS: dict[str, dict] = {
    "ARCP":      {"guid": "N93E3A75086BD11E6B9D68CD8AD30786D", "name": "Rules of Civil Procedure for the Superior Courts of Arizona", "citation_prefix": "Ariz. R. Civ. P."},
    "ARCrP":     {"guid": "NCB1EB43070CB11DAA16E8D4AC7636430", "name": "Rules of Criminal Procedure",                                  "citation_prefix": "Ariz. R. Crim. P."},
    "ARE":       {"guid": "N89B7E4A0715511DAA16E8D4AC7636430", "name": "Rules of Evidence for Courts in the State of Arizona",         "citation_prefix": "Ariz. R. Evid."},
    "ARSC":      {"guid": "N96EE7620715511DAA16E8D4AC7636430", "name": "Rules of the Supreme Court of Arizona",                        "citation_prefix": "Ariz. R. Sup. Ct."},
    "ARCAP":     {"guid": "N0854C3F0715611DAA16E8D4AC7636430", "name": "Rules of Civil Appellate Procedure",                           "citation_prefix": "Ariz. R. Civ. App. P."},
    "ARSpecAct": {"guid": "N0E359650715611DAA16E8D4AC7636430", "name": "Rules of Procedure for Special Actions",                       "citation_prefix": "Ariz. R. P. Spec. Actions"},
    "ARCommReg": {"guid": "N10250A40715611DAA16E8D4AC7636430", "name": "Rules of Procedure for Direct Appeals from Decisions of the Corporation Commission to the Arizona Court of Appeals", "citation_prefix": "Ariz. R. P. Corp. Comm. App."},
    "ARPubPwr":  {"guid": "N11E530D0715611DAA16E8D4AC7636430", "name": "Rules of Procedure for Direct Appeals from Decisions of the Governing Bodies of Public Power Entities",            "citation_prefix": "Ariz. R. P. Pub. Power App."},
    "URP":       {"guid": "N13BBC590715611DAA16E8D4AC7636430", "name": "Uniform Rules of Practice of the Superior Court of Arizona [abrogated]",                                            "citation_prefix": "Ariz. Unif. R. P. Sup. Ct."},
    "ARFLP":     {"guid": "N1A651810715611DAA16E8D4AC7636430", "name": "Rules of Family Law Procedure",                                 "citation_prefix": "Ariz. R. Fam. L. P."},
    "ARPOP":     {"guid": "NB4DED0E0679D11DCA204A4EECBB71484", "name": "Arizona Rules of Protective Order Procedure",                  "citation_prefix": "Ariz. R. Protective Order P."},
    "ARProbP":   {"guid": "NEB9773C0971D11DD86F49F8874280CEA", "name": "Arizona Rules of Probate Procedure",                           "citation_prefix": "Ariz. R. Prob. P."},
    "AREvictP":  {"guid": "NCC4C6060DCE211DDB971F5C1341DE2D7", "name": "Rules of Procedure for Eviction Actions",                      "citation_prefix": "Ariz. R. P. Eviction Actions"},
    "ARJP":      {"guid": "NB9CC48B0715611DAA16E8D4AC7636430", "name": "Rules of Procedure for the Juvenile Court",                    "citation_prefix": "Ariz. R. P. Juv. Ct."},
    "ARTaxCt":   {"guid": "N316771C0715611DAA16E8D4AC7636430", "name": "Arizona Tax Court Rules of Practice",                          "citation_prefix": "Ariz. Tax Ct. R. P."},
    "LRPSupCt":  {"guid": "N3644B6D0715611DAA16E8D4AC7636430", "name": "Local Rules of Practice Superior Court",                       "citation_prefix": "Ariz. Sup. Ct. Loc. R."},
    "LRPima":    {"guid": "ND1435D30715611DAA16E8D4AC7636430", "name": "Local Rules of Procedure for the Pima County Juvenile Court [abrogated]",                                         "citation_prefix": "Pima Cty. Juv. Ct. Loc. R."},
    "URPArb":    {"guid": "NA70A57D0715611DAA16E8D4AC7636430", "name": "Uniform Rules of Procedure for Arbitration [abrogated]",       "citation_prefix": "Ariz. Unif. R. P. Arb."},
    "URPMedMal": {"guid": "NA90D53C0715611DAA16E8D4AC7636430", "name": "Uniform Rules of Practice for Medical Malpractice Cases [abrogated]",                                              "citation_prefix": "Ariz. Unif. R. P. Med. Mal."},
    "URPMedLR":  {"guid": "NAA44E960715611DAA16E8D4AC7636430", "name": "Uniform Rules of Procedure for Medical Liability Review Panels in the Superior Court [repealed]",                  "citation_prefix": "Ariz. Unif. R. P. Med. Liab."},
    "SCRAPCiv":  {"guid": "NAC1340C0715611DAA16E8D4AC7636430", "name": "Superior Court Rules of Appellate Procedure - Civil",          "citation_prefix": "Ariz. Sup. Ct. R. App. P. Civ."},
    "SCRAPCrim": {"guid": "NB12418F0715611DAA16E8D4AC7636430", "name": "Superior Court Rules of Appellate Procedure - Criminal",       "citation_prefix": "Ariz. Sup. Ct. R. App. P. Crim."},
    "ARTribJud": {"guid": "NB5F79A00715611DAA16E8D4AC7636430", "name": "Rules of Procedure for the Recognition of Tribal Court Civil Judgments",                                            "citation_prefix": "Ariz. R. P. Tribal Ct. Civ. J."},
    "ARTribICO": {"guid": "NB47E4070715611DAA16E8D4AC7636430", "name": "Rules of Procedure for Enforcement of Tribal Court Involuntary Commitment Orders",                                 "citation_prefix": "Ariz. R. P. Tribal Ct. Invol. Commit."},
    "ARAdmRev":  {"guid": "NB746AF40715611DAA16E8D4AC7636430", "name": "Rules of Procedure for Judicial Review of Administrative Decisions",                                                "citation_prefix": "Ariz. R. P. Jud. Rev. Admin. Decisions"},
    "ARTraffic": {"guid": "ND5AAFD10715611DAA16E8D4AC7636430", "name": "Rules of Procedure in Traffic Cases and Boating Cases [Abrogated]",                                                "citation_prefix": "Ariz. R. P. Traf."},
    "ARCivTraf": {"guid": "ND8C694F0715611DAA16E8D4AC7636430", "name": "Rules of Court Procedure for Civil Traffic, Boating, Marijuana, and Parking and Standing Violations",              "citation_prefix": "Ariz. R. Ct. P. Civ. Traf."},
    "JCRCP":     {"guid": "ND4E6D1300BBC11E2B693E1305F461EC5", "name": "Justice Court Rules of Civil Procedure",                       "citation_prefix": "Ariz. Just. Ct. R. Civ. P."},
    "ARSmCl":    {"guid": "N86E7BC50E52711E99877D89EFDB0D4E0", "name": "Rules of Small Claims Procedure",                              "citation_prefix": "Ariz. R. Sm. Cl. P."},
    "LRPhoenix": {"guid": "NE102E380715611DAA16E8D4AC7636430", "name": "Local Rules of Practice and Procedure - City Court - City of Phoenix",                                              "citation_prefix": "Phx. City Ct. Loc. R."},
    "LRTucson":  {"guid": "NE8920220715611DAA16E8D4AC7636430", "name": "Local Rules of Practice and Procedure in City Court Civil Proceedings City of Tucson",                              "citation_prefix": "Tucson City Ct. Loc. R."},
    "LRYuma":    {"guid": "NCE96610026D811E3920E99B24BCEE601", "name": "Local Rules of Practice and Procedure for the Yuma Municipal Court",                                                "citation_prefix": "Yuma Muni. Ct. Loc. R."},
    "LRPimaJP":  {"guid": "NECE44540715611DAA16E8D4AC7636430", "name": "Local Rules for Pima County Justice of the Peace Courts Providing for Pre-Trial Conferences in Criminal Cases",     "citation_prefix": "Pima Cty. JP Ct. Loc. R."},
    "FASTAR":    {"guid": "N6CD90570E98C11EFAD08E3C4D4471532", "name": "Rules for the Fast Trial and Alternative Resolution (Fastar) Program",                                              "citation_prefix": "Ariz. FASTAR R."},
    "CJC":       {"guid": "NEE660340715611DAA16E8D4AC7636430", "name": "Rules of the Commission on Judicial Conduct",                  "citation_prefix": "Ariz. R. Comm'n Jud. Conduct"},
    "JPRRules":  {"guid": "NF6717DD0715611DAA16E8D4AC7636430", "name": "Rules of Procedure for Judicial Performance Review in the State of Arizona",                                       "citation_prefix": "Ariz. R. P. Jud. Performance Rev."},
    "URCommApp": {"guid": "NF81B5D40715611DAA16E8D4AC7636430", "name": "Uniform Rules of Procedure for Commissions on Appellate and Trial Court Appointments",                              "citation_prefix": "Ariz. Unif. R. P. Comm'n App. Trial Ct."},
    "JNomComm":  {"guid": "NFA3D2BD0715611DAA16E8D4AC7636430", "name": "Rules of Procedure for Judicial Nominating Commissions [Deleted]",                                                  "citation_prefix": "Ariz. R. P. Jud. Nom. Comm."},
    "FCRB":      {"guid": "N0276E250715711DAA16E8D4AC7636430", "name": "Rules of Procedure for the Foster Care Review Boards",        "citation_prefix": "Ariz. R. P. Foster Care Rev. Bd."},
    "FCRBSupp":  {"guid": "N03FAEA40715711DAA16E8D4AC7636430", "name": "Supplemental Rules of the State Foster Care Review Board",    "citation_prefix": "Ariz. Supp. R. Foster Care Rev. Bd."},
}


def _browse_url(guid: str) -> str:
    return (
        f"{AZ_BASE}/azrules/Browse/Home/Arizona/ArizonaCourtRules/ArizonaStatutesCourtRules"
        f"?guid={guid}&transitionType=CategoryPageItem&contextData=(sc.Default)"
    )


def _doc_url(guid: str) -> str:
    return (
        f"{AZ_BASE}/azrules/Document/{guid}"
        f"?viewType=FullText&originationContext=documenttoc"
        f"&transitionType=CategoryPageItem&contextData=(sc.Default)"
    )


# ---------------------------------------------------------------------------
# Discovery: recursive Browse walker
# ---------------------------------------------------------------------------
@dataclass
class DocRef:
    """A leaf rule found in a rule-set Browse tree."""
    slug: str
    set_name: str
    citation_prefix: str
    doc_guid: str            # the Westlaw document GUID for this leaf
    section_path: list[str]  # breadcrumb of sub-category names traversed
    list_title: str          # link text shown in the TOC, e.g. "Rule 4. Summons"


_GUID_RE = re.compile(r"guid=([A-Za-z0-9]+)")
_DOC_RE = re.compile(r"^/azrules/Document/([A-Za-z0-9]+)")


def _walk_browse(slug: str, set_name: str, cit_prefix: str, guid: str,
                 section_path: list[str], visited: set[str], out: list[DocRef]) -> None:
    """Depth-first walk of one Browse page. Document links become DocRefs;
    nested Browse links recurse. visited prevents revisits of the same GUID."""
    if guid in visited:
        return
    visited.add(guid)
    html = fetch(_browse_url(guid))
    if not html:
        return
    soup = BeautifulSoup(html, "html.parser")
    container = soup.find("ul", class_="co_genericWhiteBox") or soup
    for a in container.find_all("a", href=True):
        if not isinstance(a, Tag):
            continue
        href = a.get("href", "")
        text = a.get_text(" ", strip=True)
        if not href or not text:
            continue

        dm = _DOC_RE.match(href)
        if dm:
            out.append(DocRef(
                slug=slug,
                set_name=set_name,
                citation_prefix=cit_prefix,
                doc_guid=dm.group(1),
                section_path=[_normalize(p) for p in section_path],
                list_title=_normalize(text),
            ))
            continue

        if "/azrules/Browse/" in href:
            gm = _GUID_RE.search(href)
            if not gm:
                continue
            child_guid = gm.group(1)
            if child_guid == guid or child_guid in visited:
                continue
            _walk_browse(
                slug, set_name, cit_prefix, child_guid,
                [*section_path, _normalize(text)], visited, out,
            )


def discover_rule_set_docs(slug: str) -> list[DocRef]:
    meta = RULE_SETS[slug]
    out: list[DocRef] = []
    _walk_browse(
        slug, meta["name"], meta["citation_prefix"], meta["guid"],
        [], set(), out,
    )
    # The TOC commonly emits "Refs & Annos" and "Disposition Table" leaves —
    # keep them but tag them so we can filter at chunk-emit time if they have
    # no useful body. (No filtering here; the document parser drops empties.)
    return out


# ---------------------------------------------------------------------------
# Document parsing
# ---------------------------------------------------------------------------
_MONTHS = (
    "January|February|March|April|May|June|July|August|September|"
    "October|November|December|Jan\\.|Feb\\.|Mar\\.|Apr\\.|May\\.|Jun\\.|Jul\\.|"
    "Aug\\.|Sept\\.|Sep\\.|Oct\\.|Nov\\.|Dec\\."
)
_DATE_RE = re.compile(rf"\b({_MONTHS})\s+\d{{1,2}},\s+\d{{4}}", re.IGNORECASE)

# Rule heading title pattern: "Rule 4. Summons", "Rule 4.1. Service ...",
# "ER 1.1. Competence", "Form 1. Civil Cover Sheet". The captured number
# allows dotted/letter suffixes like "4.1", "31A", "1.1".
_TITLE_RE = re.compile(
    r"^\s*(?:Rule|Rules?|ER|Form|R\.|Canon)\s+([0-9]+(?:\.[0-9A-Za-z]+)*[A-Za-z]?)"
    r"\.?\s*[\.\-:—]*\s*(.*)$"
)


@dataclass
class Rule:
    slug: str                          # rule-set slug, e.g. "ARCP"
    set_name: str                      # full rule-set name
    citation_prefix: str               # e.g. "Ariz. R. Civ. P."
    rule_num: str                      # e.g. "4.1" or "31A"; fallback empty
    section_title: str                 # raw heading text from co_title
    list_title: str                    # TOC link text
    breadcrumb_section: str            # prelim head Roman-numeral section
    section_path: list[str]            # breadcrumb of nested Browse sub-cats
    raw_text: str                      # body text (no amendment-history)
    amendment_history: list[str] = field(default_factory=list)
    effective_date: str = ""
    prior_effective_dates: list[str] = field(default_factory=list)
    citation_long: str = ""            # e.g. "16 A.R.S. Rules of Civil Procedure, Rule 4"
    doc_guid: str = ""
    source_url: str = ""


_NBSP_CLASS = "     "


def _normalize(s: str) -> str:
    """Collapse unicode thin/em spaces into ASCII spaces so downstream regex
    matches reliably. Westlaw heavily uses \\u2002 / \\u2003 inside titles."""
    if not s:
        return ""
    for ch in _NBSP_CLASS:
        s = s.replace(ch, " ")
    return re.sub(r"\s+", " ", s).strip()


def _text(el: Tag | None) -> str:
    if not el:
        return ""
    return _normalize(el.get_text(" ", strip=True))


def parse_document(html: str, ref: DocRef) -> Rule | None:
    """Parse one /azrules/Document/<GUID> page into a Rule. Returns None if
    the page has no usable body (e.g. a "Refs & Annos" stub)."""
    soup = BeautifulSoup(html, "html.parser")
    title = _text(soup.find("div", class_="co_title"))
    if not title:
        # Fall back to the H1 in the doc header
        h1 = soup.find("h1", id="co_docHeaderTitleLine")
        title = _text(h1)
    if not title:
        title = ref.list_title

    # Drop pure metadata pages (no rule body)
    body_el = soup.find("div", class_="co_body")
    body_text = _text(body_el)
    if not body_text:
        # "Refs & Annos" and "Disposition Table" pages — skip
        return None

    eff = _text(soup.find("div", class_="co_effectiveDate"))
    if eff.lower().startswith("effective:"):
        eff = eff.split(":", 1)[1].strip()
    cit_long = _text(soup.find("div", class_="co_cites"))

    # The Roman-numeral prelim ("II. Commencing an Action ...")
    prelim_section = ""
    for d in soup.find_all("div", class_="co_prelimHead"):
        t = _text(d)
        # The first co_prelimHead is the rule-set name itself; the deeper one
        # (#co_prelimGoldenLeaf) is the in-set section. Take whichever
        # explicitly starts with a Roman numeral.
        if re.match(r"^[IVX]+\.\s", t):
            prelim_section = t
            break

    # Amendment history: Credits section followed by co_paragraph divs.
    history: list[str] = []
    cred_heading = soup.find(id="co_anchor_Credits")
    if cred_heading is None:
        cred_heading = soup.find("h2", string=lambda s: bool(s and "Credit" in s))
    if cred_heading is not None:
        for sib in cred_heading.find_all_next():
            if not isinstance(sib, Tag):
                continue
            if sib.find_parent(id="co_endOfDocument") is not None:
                break
            classes = sib.get("class") or []
            if (
                "co_paragraph" in classes
                or "co_paragraphText" in classes
                or ("co_contentBlock" in classes and "co_includeCurrencyBlock" in classes)
            ):
                t = _text(sib)
                if t:
                    history.append(t)
            elif sib.name == "table" and sib.get("id") == "co_endOfDocument":
                break

    # Dedup history (Credits + a few stray co_paragraphText children may dup)
    deduped: list[str] = []
    seen_h: set[str] = set()
    for h in history:
        if h in seen_h:
            continue
        # Skip junk like "State Court Rules and the Code of Judicial..."
        if h.lower().startswith("state court rules"):
            continue
        seen_h.add(h)
        deduped.append(h)
    history = deduped

    # Rule number from title; fallback rule_num="" lets us still emit
    # comment/preface chunks, which we tag with a "P" sort suffix below.
    rule_num = ""
    section_label = title
    m = _TITLE_RE.match(title)
    if m:
        rule_num = m.group(1)
        if m.group(2):
            section_label = title  # keep "Rule 4. Summons" as section_title

    # Effective + prior dates:
    #   - effective_date = the explicit co_effectiveDate header value (latest
    #     in-force version, e.g. "January 1, 2026").
    #   - prior_effective_dates = every date mentioned in Credits, in source
    #     order. We keep duplicates out and the original date string.
    effective_date = eff
    prior_dates: list[str] = []
    for h in history:
        for m in _DATE_RE.finditer(h):
            d = m.group(0)
            if d not in prior_dates:
                prior_dates.append(d)

    return Rule(
        slug=ref.slug,
        set_name=ref.set_name,
        citation_prefix=ref.citation_prefix,
        rule_num=rule_num,
        section_title=section_label,
        list_title=ref.list_title,
        breadcrumb_section=prelim_section,
        section_path=list(ref.section_path),
        raw_text=body_text,
        amendment_history=history,
        effective_date=effective_date,
        prior_effective_dates=prior_dates,
        citation_long=cit_long,
        doc_guid=ref.doc_guid,
        source_url=_doc_url(ref.doc_guid),
    )


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
    # If rule_num parsing failed, use the doc GUID short tail so act_id stays
    # unique (these tend to be prefatory comments and committee tables).
    rule_token = _safe(r.rule_num) if r.rule_num else f"P{r.doc_guid[:8]}"
    act_id = f"SRULES_AZ_{r.slug.upper()}_R{rule_token}"

    citation = (
        f"{r.citation_prefix} {r.rule_num}"
        if r.rule_num else f"{r.citation_prefix} ({r.section_title[:60]})"
    )

    meta_lines: list[str] = []
    if r.effective_date:
        meta_lines.append(f"Effective: {r.effective_date}")
    if r.amendment_history:
        meta_lines.append("History: " + " ".join(r.amendment_history))
    meta_header = ("\n" + "\n".join(meta_lines)) if meta_lines else ""

    # Build a single, deduplicated breadcrumb tail. section_path already
    # carries the Roman-numeral parts (we descend into them while walking
    # Browse). breadcrumb_section comes from the document's own co_prelimHead
    # and is usually the same string, so only append it if it's unique.
    crumb_parts: list[str] = list(r.section_path)
    if r.breadcrumb_section and r.breadcrumb_section not in crumb_parts:
        crumb_parts.append(r.breadcrumb_section)
    breadcrumb_tail = " / ".join(crumb_parts).strip(" /")
    breadcrumb_line = f"{r.set_name}"
    if breadcrumb_tail:
        breadcrumb_line += f" | {breadcrumb_tail}"

    text_for_embedding = (
        f"Arizona Court Rules | US | Arizona | In Force\n"
        f"{breadcrumb_line} | {citation}\n"
        f"{r.section_title}{meta_header}\n\n{text}"
    )

    breadcrumb: list[dict] = [
        {"type": "title", "num": "rules-az", "label": "Arizona Court Rules",
         "name": "Arizona Court Rules"},
        {"type": "rule_set", "num": r.slug, "label": r.slug, "name": r.set_name},
    ]
    for sp in crumb_parts:
        # Roman-numeral parts (e.g. "II. Commencing an Action ...") become
        # type=section; anything below them is a subsection.
        kind = "section" if re.match(r"^[IVX]+\.\s", sp) else "subsection"
        num = sp.split(".", 1)[0] if kind == "section" else sp[:24]
        breadcrumb.append({"type": kind, "num": num, "label": sp, "name": sp})
    breadcrumb.append({
        "type": "rule", "num": r.rule_num or rule_token,
        "label": f"Rule {r.rule_num}" if r.rule_num else r.section_title,
        "name": r.section_title,
    })

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
        "state": "az",
        "title_number": None,
        "title_name": "Arizona Court Rules",
        "title": "Arizona Court Rules",
        "title_code": "rules_az",
        "top_level_title": "rules-az",
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
        "citation_long": r.citation_long,
        "display_label": citation,
        "display_title": r.section_title,
        "display_path": " / ".join([
            "Arizona Court Rules", r.set_name,
            *crumb_parts,
            (f"Rule {r.rule_num}" if r.rule_num else r.section_title),
        ]),
        "breadcrumb": breadcrumb,
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
            for m in re.finditer(r"\b(?:19|20)\d{2}\b", h)
        }),
        "last_amended_year": (
            max((int(m.group(0)) for h in r.amendment_history
                 for m in re.finditer(r"\b(?:19|20)\d{2}\b", h)), default=None)
        ),
        "public_laws_referenced": [],
        "public_laws_count": 0,
        "source_url": r.source_url,
        "parent_id": f"us/az/court_rules/{r.slug}",
        "raw_node_id": f"us/az/court_rules/{r.slug}/rule={r.rule_num or rule_token}",
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
def fetch_and_parse(ref: DocRef) -> Rule | None:
    html = fetch(_doc_url(ref.doc_guid))
    if not html:
        return None

    rule = parse_document(html, ref)
    if rule is None:
        return None
    return rule


def process_rule_set(slug: str, workers: int, delay: float) -> list[Rule]:
    print(f"[{slug}] discovering ...", flush=True)
    refs = discover_rule_set_docs(slug)
    print(f"[{slug}] found {len(refs)} document refs", flush=True)
    rules: list[Rule] = []
    if not refs:
        return rules
    # Per rule-set thread pool. Westlaw Weblinks tolerates moderate
    # concurrency; we still rate-limit through the proxy pool.
    with ThreadPoolExecutor(max_workers=max(workers, 1)) as ex:
        futures = {ex.submit(fetch_and_parse, ref): ref for ref in refs}
        for done, fut in enumerate(as_completed(futures), start=1):
            try:
                rule = fut.result()
                if rule is not None:
                    rules.append(rule)
            except Exception as e:
                print(f"  ! {slug} doc failed: {e}", flush=True)
            if done % 25 == 0 or done == len(refs):
                print(
                    f"  [{slug}] doc {done}/{len(refs)}, parsed={len(rules)}",
                    flush=True,
                )
            time.sleep(delay / max(workers, 1))
    print(f"[{slug}] done: {len(rules)} rules", flush=True)
    return rules


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--rule-sets",
        default="",
        help="Comma-separated slugs (e.g. 'ARCP,ARE'). Default: all 40.",
    )
    ap.add_argument("--workers", type=int, default=4,
                    help="Concurrent document fetches per rule set.")
    ap.add_argument("--delay", type=float, default=0.4,
                    help="Per-task pacing.")
    ap.add_argument("--dry-run", action="store_true",
                    help="Discover docs per rule set and print counts; no parsing.")
    args = ap.parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if args.rule_sets:
        wanted = {s.strip() for s in args.rule_sets.split(",") if s.strip()}
        slugs = [s for s in RULE_SETS if s in wanted]
        for s in wanted:
            if s not in slugs:
                slugs.append(s)
    else:
        slugs = list(RULE_SETS.keys())

    print(f"[AZ] processing {len(slugs)} rule sets", flush=True)
    if args.dry_run:
        for s in slugs:
            refs = discover_rule_set_docs(s)
            print(f"  - {s} ({RULE_SETS[s]['name']}): {len(refs)} docs")
        return 0

    chunks: list[dict] = []
    t0 = time.time()
    for slug in slugs:
        try:
            rules = process_rule_set(slug, args.workers, args.delay)
            chunks.extend(to_chunk_record(r) for r in rules)
        except Exception as e:
            print(f"! rule-set {slug} failed: {e}", flush=True)

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
