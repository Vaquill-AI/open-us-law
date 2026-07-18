#!/usr/bin/env python3
"""Ingest the Maryland Rules (court rules) from the OFFICIAL state-sponsored
portal at govt.westlaw.com/mdc, the Maryland Code and Court Rules site
maintained for the Maryland Thurgood Marshall State Law Library.

OFFICIAL SOURCE ONLY: the Maryland Judiciary's Standing Committee on Rules of
Practice and Procedure publishes the Maryland Rules through this portal (see
https://mdcourts.gov/rules, which states "The Maryland Rules are available
through Westlaw" and links directly to the URL we crawl below). Cornell
Legal Information Institute also points to the same URL as the
authoritative source. No aggregator scraping (no Justia/Casetext/etc).

Why we cannot use regs.maryland.gov: that platform hosts only COMAR and the
Maryland Register; its /us/md/court/rules subtree 404s (probed
2026-05-30). Why mdcourts.gov/rules is insufficient: that landing page only
links out to Westlaw plus rules-change notice PDFs (it has just eight
distinct /rules/* subpaths, none containing rule text).

Discovery: the portal serves three URL flavours:

  /mdc/Browse/Home/Maryland/MarylandCodeCourtRules?guid=<root>&bhcp=1
        -> 23 children: Preamble + Title 1..21 + Appendix.
  /mdc/Browse/Home/Maryland/MarylandCodeCourtRules?guid=<title>&bhcp=1
        -> Chapter list for one Title (e.g. Title 1 -> 6 chapters).
  /mdc/Browse/Home/Maryland/MarylandCodeCourtRules?guid=<chapter>&bhcp=1
        -> Inline rule list. Each rule is rendered as a
           <a href="/mdc/Document/<rule_guid>?viewType=FullText...">
           anchor (NOT a guid= query; the rule GUID is in the path).
  /mdc/Document/<rule_guid>?viewType=FullText&bhcp=1
        -> Full rule body, ~17-200KB. The page bears:
           - Title chain ("Title 1. General Provisions / Chapter 100 ...")
           - "RULE N-NNN. <TITLE>"
           - "Effective: <date>"
           - body paragraphs
           - "Source: ..." block
           - "Credits" block with the amendment-history bracket
           - "Editors' Notes" / "Committee note" (optional)

The `?bhcp=1` flag (BrowserHawk continue) bypasses the JS-detection
interstitial that would otherwise gate every initial GET. A clean
`requests.Session` per URL keeps Cloudflare turnstile from challenging a
"shared" cookie jar; CF only fires when many requests share a session
through different IPs (which is exactly the rotating-proxy footprint we
must avoid). Each request goes through a US Webshare rotating proxy with
the Mozilla UA the project standardises on.

Hierarchy: Maryland Rules > Title N (21 titles + Preamble + Appendix) >
Chapter NNN > Rule N-NNN (rule numbers are TITLE-NUMBER, e.g. "1-101",
"2-501", "16-1003"). The em-dash (U+2013) inside "Rule 1–101" is converted
to ASCII '-' for citation rendering. corpus_type='state_rules' (matches
NV/MN/NY court-rule chunks). Citation form: "Md. Rule N-NNN".
"""

from __future__ import annotations

import argparse
import datetime as _dt
import hashlib
import html as htmllib
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

import requests
from bs4 import BeautifulSoup

sys.stdout.reconfigure(line_buffering=True)  # type: ignore[attr-defined]

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = Path(os.environ.get("OUT_DIR", "./data"))
OUT = DATA_DIR / "state_md_court_rules.jsonl"

WL_HOST = "https://govt.westlaw.com"
MD_RULES_ROOT_GUID = "ND4CC33B09CCE11DB9BCF9DAC28345A2A"  # Maryland Rules root

_MOZ_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
)


def _load_env() -> None:
    # Look for `.env` in the repo root (Mac dev) AND `/root/vq/.env` (the
    # US-proxied scraping VM, which clones the repo under /root/vq).
    candidates = [_PROJECT_ROOT / ".env", Path("/root/vq/.env")]
    for env_path in candidates:
        if not env_path.exists():
            continue
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


_load_env()


def _us_proxies() -> dict | None:
    """Webshare US rotating proxy as a requests-style dict."""
    user = os.environ.get("WEBSHARE_USERNAME", "")
    pwd = os.environ.get("WEBSHARE_PASSWORD", "")
    if not user or not pwd:
        return None
    host = os.environ.get("WEBSHARE_PROXY_HOST", "p.webshare.io")
    port = os.environ.get("WEBSHARE_PROXY_PORT", "80")
    mode = os.environ.get("WEBSHARE_PROXY_MODE", "rotate")
    suffix = "US-sticky" if mode == "sticky" else "US-rotate"
    proxy_user = f"{user}-{suffix}"
    url = f"http://{urllib.parse.quote(proxy_user)}:{urllib.parse.quote(pwd)}@{host}:{port}"
    return {"http": url, "https": url}


def _with_bhcp(url: str) -> str:
    """Append `?bhcp=1` (BrowserHawk continue) so the portal skips its
    JS-detection interstitial. Without this, the response body is the 264-byte
    'browser requirements' stub. Idempotent."""
    if "bhcp=1" in url:
        return url
    sep = "&" if "?" in url else "?"
    return f"{url}{sep}bhcp=1"


def fetch(url: str, retries: int = 6) -> str | None:
    """Fetch HTML via Webshare proxy.

    Each call uses a FRESH `requests.Session`. Cloudflare's turnstile fires
    when many requests share a long-lived cookie jar across rotating IPs;
    short-lived sessions stay below that threshold while still benefiting
    from connection-level retries. We honor 429 / 5xx with exponential
    backoff and treat 200-but-tiny (the 264-byte BH interstitial) as a soft
    failure worth retrying; it usually means the proxy IP happened to land
    on a CF-challenged route."""
    proxies = _us_proxies()
    target = _with_bhcp(url)
    for attempt in range(retries):
        sess = requests.Session()
        sess.headers.update({"User-Agent": _MOZ_UA})
        try:
            r = sess.get(target, timeout=60, proxies=proxies, allow_redirects=True)
            if r.status_code == 200:
                r.encoding = "utf-8"
                # Detect the BrowserHawk interstitial or Cloudflare challenge
                # so we can rotate IP and try again.
                if len(r.text) < 1500 and "Please click here to continue" in r.text:
                    time.sleep(1 + attempt)
                    continue
                if (
                    len(r.text) < 4000
                    and ("Just a moment" in r.text or "challenges.cloudflare.com" in r.text)
                ):
                    time.sleep(1.5 + attempt)
                    continue
                return r.text
            if r.status_code == 404:
                return None
            if r.status_code == 429:
                time.sleep(2**attempt)
                continue
            if r.status_code in (500, 502, 503, 504):
                time.sleep(1.5 + attempt)
                continue
            if r.status_code == 403:
                # Cloudflare bot challenge fired on this proxy IP. Wait long
                # enough that the next request rotates to a different egress.
                time.sleep(3 + attempt * 2)
                continue
            return None
        except requests.RequestException:
            time.sleep(1 + attempt)
    return None


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------


_TITLE_LABEL_RE = re.compile(r"^Title\s+(\d+)[\. \s]+(.*)$")
_CHAPTER_LABEL_RE = re.compile(r"^Chapter\s+(\d+[A-Za-z]?)[\. \s]*(.*)$")
_RULE_LABEL_RE = re.compile(
    r"^Rule\s+(\d+[A-Z]?[–\-](\d+)(?:\.(\d+))?(?:[A-Za-z])?)[\. \s]*(.*)$"
)


def _norm_label(s: str) -> str:
    s = htmllib.unescape(s)
    s = s.replace(" ", " ").replace(" ", " ")
    return re.sub(r"\s+", " ", s).strip()


def _norm_rule_num(num: str) -> str:
    """Convert "1–101" or "1-101" to canonical "1-101"."""
    return num.replace("–", "-").replace("—", "-").strip()


@dataclass
class TitleRef:
    title_num: str
    title_name: str
    url: str


@dataclass
class ChapterRef:
    title_num: str
    title_name: str
    chapter_num: str
    chapter_name: str
    url: str


@dataclass
class RuleRef:
    title_num: str
    title_name: str
    chapter_num: str
    chapter_name: str
    rule_num: str  # e.g. "1-101", "1-101.1"
    rule_title: str
    url: str  # /mdc/Document/...
    rule_guid: str


def _browse_url(guid: str) -> str:
    return f"{WL_HOST}/mdc/Browse/Home/Maryland/MarylandCodeCourtRules?guid={guid}"


def _absolutize(href: str) -> str:
    href = htmllib.unescape(href)
    if href.startswith("http"):
        return href
    if href.startswith("/"):
        return WL_HOST + href
    return f"{WL_HOST}/{href}"


def parse_toc_anchors(html: str) -> list[tuple[str, str, str, str | None]]:
    """Return (label, href, browse_guid, document_guid) for every TOC anchor
    in a Browse page. browse_guid is set when href is a /Browse/...?guid=,
    document_guid is set when href is /mdc/Document/<guid>."""
    soup = BeautifulSoup(html, "html.parser")
    out: list[tuple[str, str, str, str | None]] = []
    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        label = _norm_label(a.get_text(" ", strip=True))
        if not label or not href:
            continue
        m_doc = re.match(r"/mdc/Document/([A-Z0-9]+)", href)
        if m_doc:
            out.append((label, _absolutize(href), "", m_doc.group(1)))
            continue
        m_browse = re.search(r"guid=([A-Z0-9]+)", href)
        if m_browse:
            out.append((label, _absolutize(href), m_browse.group(1), None))
    return out


def list_titles() -> list[TitleRef]:
    html = fetch(_browse_url(MD_RULES_ROOT_GUID))
    if not html:
        raise RuntimeError("could not fetch Maryland Rules root TOC")
    out: list[TitleRef] = []
    for label, href, browse_guid, _doc_guid in parse_toc_anchors(html):
        if browse_guid == MD_RULES_ROOT_GUID:
            continue
        m = _TITLE_LABEL_RE.match(label)
        if not m:
            continue
        out.append(TitleRef(
            title_num=m.group(1),
            title_name=m.group(2).strip().rstrip(".") or "",
            url=_absolutize(href),
        ))
    # Sort by numeric title.
    out.sort(key=lambda t: int(t.title_num))
    return out


def list_chapters(title: TitleRef) -> list[ChapterRef]:
    html = fetch(title.url)
    if not html:
        return []
    out: list[ChapterRef] = []
    for label, href, browse_guid, _doc_guid in parse_toc_anchors(html):
        if not browse_guid:
            continue
        m = _CHAPTER_LABEL_RE.match(label)
        if not m:
            continue
        out.append(ChapterRef(
            title_num=title.title_num,
            title_name=title.title_name,
            chapter_num=m.group(1),
            chapter_name=m.group(2).strip().rstrip(".") or "",
            url=_absolutize(href),
        ))
    return out


def list_rules(chapter: ChapterRef) -> list[RuleRef]:
    """Each chapter page lists its rules inline as /mdc/Document/<guid>
    anchors with text 'Rule N-NNN. <title>'."""
    html = fetch(chapter.url)
    if not html:
        return []
    out: list[RuleRef] = []
    seen: set[str] = set()
    for label, href, _browse_guid, doc_guid in parse_toc_anchors(html):
        if not doc_guid:
            continue
        m = _RULE_LABEL_RE.match(label)
        if not m:
            continue
        rule_num = _norm_rule_num(m.group(1))
        rule_title = m.group(4).strip().rstrip(".")
        if doc_guid in seen:
            continue
        seen.add(doc_guid)
        out.append(RuleRef(
            title_num=chapter.title_num,
            title_name=chapter.title_name,
            chapter_num=chapter.chapter_num,
            chapter_name=chapter.chapter_name,
            rule_num=rule_num,
            rule_title=rule_title,
            url=_absolutize(href),
            rule_guid=doc_guid,
        ))
    return out


# ---------------------------------------------------------------------------
# Rule body parsing
# ---------------------------------------------------------------------------


_MONTH = (
    "January|February|March|April|May|June|July|August|September|"
    "October|November|December|"
    r"Jan\.|Feb\.|Mar\.|Apr\.|Jun\.|Jul\.|Aug\.|Sept\.|Sep\.|Oct\.|Nov\.|Dec\."
)
_DATE_RE = re.compile(rf"\b({_MONTH})\s+\d{{1,2}},\s+\d{{4}}", re.IGNORECASE)
_EFF_RE = re.compile(rf"effective\s+((?:{_MONTH})\s+\d{{1,2}},\s+\d{{4}})", re.IGNORECASE)
_EFF_LINE_RE = re.compile(
    rf"^\s*Effective:\s*((?:{_MONTH})\s+\d{{1,2}},\s+\d{{4}})", re.IGNORECASE | re.MULTILINE
)


def _block_text(soup_block) -> str:
    """Flatten one block (`<p>`, `<div>`, etc.) into a single text line."""
    return re.sub(r"\s+", " ", soup_block.get_text(" ", strip=True)).strip()


@dataclass
class ParsedRule:
    body: str
    source_note: str
    credits: str
    committee_note: str
    cross_references: list[str]
    editors_notes: str
    effective_date: str


# Known subheading labels (case-insensitive). Anything else falls into body.
_SUBHEAD_LABELS = {
    "source": "source",
    "credits": "credits",
    "history": "credits",
    "committee note": "committee_note",
    "committee notes": "committee_note",
    "cross reference": "cross_reference",
    "cross references": "cross_reference",
    "editors' notes": "editors_notes",
    "editor's notes": "editors_notes",
    "notes of decisions": "skip",
    "library references": "skip",
    "research references": "skip",
    "currentness": "skip",
}


def parse_rule(html: str) -> ParsedRule:
    """Split a leaf rule's rendered HTML into body + structured sub-blocks.

    The Westlaw rendering uses stable CSS class hooks:
      - `div.co_effectiveDate`   -> "Effective: <date>"
      - `div.co_contentBlock.co_rule`  -> the rule's substantive body
            (contains `div.co_paragraph > div.co_paragraphText` per
             enumerated subsection)
      - `<h2>Credits</h2>` followed by `div.co_paragraph.co_paragraphText`
            with the bracketed amendment list
      - `<h2>Committee note</h2>` (or "Source", "Cross reference",
            "Editors' Notes") + sibling co_paragraph blocks
    A trailing "Current with amendments..." footer is ignored.
    """
    soup = BeautifulSoup(html, "html.parser")
    container = soup.find(id="co_document") or soup.find("main") or soup

    # Effective date is a labeled DIV inside the document head.
    effective = ""
    eff_el = container.find("div", class_="co_effectiveDate")
    if eff_el:
        m = re.search(r"Effective:\s*(.+)", eff_el.get_text(" ", strip=True))
        if m:
            effective = m.group(1).strip()

    # Body: every `co_paragraphText` inside the body wrapper that sits
    # BEFORE the Credits/Committee/etc heading. The co_rule wrapper contains
    # both the substantive subsections AND a trailing Credits bracket; we
    # split on the heading boundary so the body stops where the headings
    # begin.
    body_wrap = container.find("div", class_="co_rule") or container.find("div", class_="co_body")
    body_parts: list[str] = []
    source_note = ""
    if body_wrap:
        for pt in body_wrap.find_all("div", class_="co_paragraphText"):
            # Stop if we passed a Credits/etc heading.
            prev_h = pt.find_previous(["h2", "h3"])
            if prev_h is not None:
                hkey = prev_h.get_text(" ", strip=True).rstrip(":").strip().lower()
                if hkey in _SUBHEAD_LABELS and _SUBHEAD_LABELS[hkey] != "skip":
                    continue
            txt = re.sub(r"\s+", " ", pt.get_text(" ", strip=True)).strip()
            if not txt:
                continue
            # Pull "Source: ..." inline (it's emitted as a regular co_paragraph
            # without an h2 heading).
            sm = re.match(r"^Source:\s*(.+)$", txt)
            if sm:
                source_note = sm.group(1).strip()
                continue
            body_parts.append(txt)
    body = "\n".join(body_parts).strip()

    # Walk <h2> subheadings inside the document container to find Credits,
    # Committee note, Cross reference, Editors' Notes blocks.
    credits = ""
    committee = ""
    editors = ""
    cross_refs_raw: list[str] = []
    for h in container.find_all(["h2", "h3"]):
        head_txt = h.get_text(" ", strip=True).rstrip(":").strip().lower()
        if head_txt not in _SUBHEAD_LABELS:
            continue
        key = _SUBHEAD_LABELS[head_txt]
        if key == "skip":
            continue
        # Collect sibling co_paragraphText until next h2/h3.
        block_parts: list[str] = []
        node = h.parent  # the wrapper of the heading
        # iterate siblings of the heading's *wrapper* (the heading is usually
        # inside a `co_printHeading` div which sits beside content blocks)
        anchor = node if node and node.name == "div" else h
        for sib in anchor.next_siblings:
            if not hasattr(sib, "find_all"):
                continue
            if sib.find(["h2", "h3"]):
                # Another section header reached -> stop
                stops = sib.find_all(["h2", "h3"])
                if any(s.get_text(strip=True) for s in stops):
                    break
            for pt in sib.find_all("div", class_="co_paragraphText"):
                txt = re.sub(r"\s+", " ", pt.get_text(" ", strip=True)).strip()
                if txt:
                    block_parts.append(txt)
        joined = "\n".join(block_parts).strip()
        if key == "credits":
            credits = (credits + " " + joined).strip() if credits else joined
        elif key == "committee_note":
            committee = joined
        elif key == "editors_notes":
            editors = joined
        elif key == "cross_reference":
            for piece in re.split(r"[;\n]\s*", joined):
                piece = piece.strip()
                if piece:
                    cross_refs_raw.append(piece)
        elif key == "source":
            if not source_note:
                source_note = joined

    if not effective and credits:
        # Prefer the LAST "eff. <date>" mention in Credits (most recent
        # amendment); fall back to the LAST bare date in the block.
        eff_dates = _EFF_RE.findall(credits)
        if eff_dates:
            effective = eff_dates[-1]
        else:
            bare = _DATE_RE.findall(credits)
            if bare:
                # findall on a group regex returns the group; rebuild a real
                # date with a final pass.
                full = list(_DATE_RE.finditer(credits))
                if full:
                    effective = full[-1].group(0)

    return ParsedRule(
        body=body,
        source_note=source_note,
        credits=credits,
        committee_note=committee,
        cross_references=cross_refs_raw,
        editors_notes=editors,
        effective_date=effective,
    )


def _extract_amendment_history(credits: str) -> tuple[list[str], list[str]]:
    """Split the Credits block into amendment-history entries and a
    deduplicated list of effective dates.

    Westlaw renders Credits as a single bracketed amendment list:
    "[Adopted ..., eff. ... Amended ..., eff. ...; ...]".
    """
    if not credits:
        return [], []
    # Strip leading/trailing brackets that wrap the whole Credits block.
    text = credits.strip().lstrip("[").rstrip("]").strip()
    # Split on each "Adopted" / "Amended" verb so each historical event is
    # its own entry; recover the verb prefix for readability.
    fixed: list[str] = []
    cursor = 0
    for verb_match in re.finditer(r"(Adopted|Amended)\s+", text):
        if verb_match.start() > cursor and text[cursor:verb_match.start()].strip():
            fixed.append(text[cursor:verb_match.start()].strip().rstrip(";").rstrip("."))
        cursor = verb_match.start()
    if cursor < len(text):
        fixed.append(text[cursor:].strip().rstrip(";").rstrip("."))
    history = [h for h in fixed if h and not re.fullmatch(r"[\s\[\]]+", h)] or [text]
    dates: list[str] = []
    for h in history:
        for m in _DATE_RE.finditer(h):
            d = m.group(0)
            if d not in dates:
                dates.append(d)
    return history, dates


# ---------------------------------------------------------------------------
# Chunk emission
# ---------------------------------------------------------------------------


def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def _point_id(act_id: str, idx: int, text: str) -> str:
    seed = f"{act_id}::{idx}::{_sha1(text)[:12]}"
    return str(uuid.UUID(hashlib.md5(seed.encode()).hexdigest()))


def _safe(s: str) -> str:
    return re.sub(r"[^0-9A-Za-z]+", "_", s).strip("_")


@dataclass
class Rule:
    title_num: str
    title_name: str
    chapter_num: str
    chapter_name: str
    rule_num: str
    rule_title: str
    raw_text: str
    committee_note: str = ""
    source_note: str = ""
    cross_references: list[str] = field(default_factory=list)
    amendment_history: list[str] = field(default_factory=list)
    effective_date: str = ""
    prior_effective_dates: list[str] = field(default_factory=list)
    editors_notes: str = ""
    source_url: str = ""


def to_chunk_record(r: Rule) -> dict:
    act_id = f"SRULES_MD_T{_safe(r.title_num)}_R{_safe(r.rule_num)}"
    citation = f"Md. Rule {r.rule_num}"
    text = r.raw_text
    section_title = f"Rule {r.rule_num}. {r.rule_title}" if r.rule_title else f"Rule {r.rule_num}"

    meta_lines: list[str] = []
    if r.effective_date:
        meta_lines.append(f"Effective: {r.effective_date}")
    if r.source_note:
        meta_lines.append(f"Source: {r.source_note}")
    if r.committee_note:
        cn = r.committee_note
        meta_lines.append(f"Committee Note: {cn if len(cn) < 800 else cn[:800] + ' [...]'}")
    if r.cross_references:
        meta_lines.append("Cross References: " + "; ".join(r.cross_references[:6]))
    meta_header = ("\n" + "\n".join(meta_lines)) if meta_lines else ""

    chap_caption = f": {r.chapter_name}" if r.chapter_name else ""
    title_caption = f" ({r.title_name})" if r.title_name else ""
    text_for_embedding = (
        f"Court Rule: Maryland Rules | US | Maryland | In Force\n"
        f"Title {r.title_num}{title_caption} / "
        f"Chapter {r.chapter_num}{chap_caption}\n"
        f"{section_title}{meta_header}\n\n{text}"
    )

    breadcrumb = [
        {"type": "title", "num": "rules-md", "label": "Maryland Rules", "name": "Maryland Rules"},
        {"type": "title", "num": r.title_num, "label": f"Title {r.title_num}", "name": r.title_name},
        {"type": "chapter", "num": r.chapter_num, "label": f"Chapter {r.chapter_num}", "name": r.chapter_name},
        {"type": "rule", "num": r.rule_num, "label": citation, "name": r.rule_title},
    ]

    display_path = (
        f"Maryland Rules / Title {r.title_num} {r.title_name} / "
        f"Chapter {r.chapter_num} {r.chapter_name} / {citation}"
    )
    amendment_years = sorted({
        int(m.group(0)) for h in r.amendment_history
        for m in re.finditer(r"\b(19|20)\d{2}\b", h)
    })
    md = {
        "act_id": act_id,
        "corpus_type": "state_rules",
        # Canonical value per CANONICAL_CATEGORIES; see
        # app/services/us_statutes_taxonomy.py (was 'state_court_rule' -
        # 2026-07-16 audit fix).
        "category": "state_rules",
        "document_type": "court_rule",
        "jurisdiction": "US",
        "country_code": "US",
        "state": "md",
        "title_number": r.title_num,
        "title_name": (
            f"Title {r.title_num} - {r.title_name}" if r.title_name else f"Title {r.title_num}"
        ),
        "title": "Maryland Rules",
        "title_code": "rules_md",
        "top_level_title": "rules-md",
        "level_classifier": "rule",
        "chapter": r.chapter_num,
        "chapter_name": r.chapter_name,
        "section_number": r.rule_num,
        "section_title": section_title,
        "year": 2026,
        "act_status": "in_force",
        "renumbered_to": "",
        "transferred_to": "",
        # --- rich rule-specific metadata (captured, never discarded) ---
        "effective_date": r.effective_date or None,
        "committee_note": r.committee_note or None,
        "source_note": r.source_note or None,
        "cross_references": r.cross_references,
        "amendment_history": r.amendment_history,
        "prior_effective_dates": r.prior_effective_dates,
        "editors_notes": r.editors_notes or None,
        "citation": citation,
        "citation_short": citation,
        "display_label": citation,
        "display_title": section_title,
        "display_path": display_path,
        "breadcrumb": breadcrumb,
        "sort_key": act_id,
        "word_count": len(text.split()),
        "subsection_count": 0,
        "subsection_letters": [],
        "numbered_paragraph_count": 0,
        "cross_references_count": len(r.cross_references),
        "cross_references_usc": [],
        "cross_references_cfr": [],
        "amendment_years": amendment_years,
        "amendments_count": len(r.amendment_history),
        "last_amended_year": amendment_years[-1] if amendment_years else None,
        "public_laws_referenced": [],
        "public_laws_count": 0,
        "source_url": r.source_url,
        "parent_id": f"us/md/court_rules/title={r.title_num}/chapter={r.chapter_num}",
        "raw_node_id": (
            f"us/md/court_rules/title={r.title_num}/chapter={r.chapter_num}/"
            f"rule={r.rule_num}"
        ),
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


def process_rule(ref: RuleRef) -> Rule | None:
    html = fetch(ref.url)
    if not html:
        return None
    parsed = parse_rule(html)
    if len(parsed.body) < 30:
        # Reserved/empty rule; skip.
        return None
    amendment_history, dates = _extract_amendment_history(parsed.credits)
    effective = parsed.effective_date or (dates[0] if dates else "")
    prior = [d for d in dates if d != effective]

    return Rule(
        title_num=ref.title_num,
        title_name=ref.title_name,
        chapter_num=ref.chapter_num,
        chapter_name=ref.chapter_name,
        rule_num=ref.rule_num,
        rule_title=ref.rule_title,
        raw_text=parsed.body,
        committee_note=parsed.committee_note,
        source_note=parsed.source_note,
        cross_references=parsed.cross_references,
        amendment_history=amendment_history,
        effective_date=effective,
        prior_effective_dates=prior,
        editors_notes=parsed.editors_notes,
        source_url=ref.url,
    )


def main() -> int:
    ap = argparse.ArgumentParser(description="Ingest Maryland Rules from govt.westlaw.com/mdc")
    ap.add_argument("--titles", default="", help="Comma-separated title numbers (e.g. '1,2,16'). Default: all.")
    ap.add_argument("--workers", type=int, default=4, help="Concurrent rule fetches.")
    ap.add_argument("--limit", type=int, default=0, help="Cap rule count for debug.")
    ap.add_argument("--delay", type=float, default=0.3, help="Per-task pacing inside the pool.")
    ap.add_argument("--dry-run", action="store_true", help="Discover only; print first 30 leaves.")
    args = ap.parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    wanted = {t.strip() for t in args.titles.split(",") if t.strip()} if args.titles else None

    print(f"[MD] fetching Maryland Rules root TOC", flush=True)
    titles = list_titles()
    if wanted:
        titles = [t for t in titles if t.title_num in wanted]
    print(f"[MD] {len(titles)} titles to crawl", flush=True)

    rule_refs: list[RuleRef] = []
    for t in titles:
        chaps = list_chapters(t)
        print(f"  Title {t.title_num} ({t.title_name[:48]}): {len(chaps)} chapters", flush=True)
        for ch in chaps:
            rls = list_rules(ch)
            rule_refs.extend(rls)
        time.sleep(0.1)
    print(f"[MD] discovered {len(rule_refs)} rule leaves total", flush=True)

    if args.dry_run:
        for ref in rule_refs[:30]:
            print(f"  T{ref.title_num} Ch{ref.chapter_num} Rule {ref.rule_num}: {ref.rule_title[:60]}")
        return 0

    if args.limit:
        rule_refs = rule_refs[: args.limit]

    chunks: list[dict] = []
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {ex.submit(process_rule, ref): ref for ref in rule_refs}
        for done, fut in enumerate(as_completed(futures), start=1):
            try:
                rule = fut.result()
                if rule is not None:
                    chunks.append(to_chunk_record(rule))
            except Exception as e:
                print(f"  ! rule failed: {e}", flush=True)
            if done % 25 == 0 or done == len(rule_refs):
                rate = len(chunks) / max(time.time() - t0, 0.1)
                print(
                    f"  ... {done:>5}/{len(rule_refs)} rules, {len(chunks):>5} ok, "
                    f"{rate:.2f}/s",
                    flush=True,
                )
            time.sleep(args.delay / max(args.workers, 1))

    # Dedup + append.
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
