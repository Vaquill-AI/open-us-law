#!/usr/bin/env python3
"""Ingest the Code of Maryland Regulations (COMAR) — Maryland's state regs.

OFFICIAL SOURCE ONLY: the Maryland Division of State Documents (DSD) renders the
authoritative COMAR online at https://regs.maryland.gov (the DSD-hosted Library
of Maryland Regulations). dsd.maryland.gov/Pages/COMARSearch.aspx redirects here.
No aggregators (no Justia/ZenRows).

The DSD platform exposes a fully structured TOC as JSON, so discovery is lossless
and fast — there is no need to parse navigation HTML:

    /us/md/exec/comar/index.json
        -> the ENTIRE COMAR hierarchy tree in one ~575KB file. Each node carries
           "sc" (the dotted code), "t" (title), "et" (element type: container |
           section | para), "c" (children), and chapter nodes additionally carry
           "fh" (index.full.html) + "j" (index.json). The tree bottoms out at the
           chapter level (1,372 chapter containers); the regulations themselves
           live inside each chapter's documents.
    /us/md/exec/comar/<T.S.C>/index.json
        -> that chapter's section list: children with et=="section" give the
           clean "sc" code + title (e.g. "10.09.36.01" / ".01 Definitions.").
           (Its nested "para" nodes carry only 75-char text snippets, so they are
           NOT used for body text.)
    /us/md/exec/comar/<T.S.C>/index.full.html
        -> that chapter's FULL rendered text — the authoritative body source.
           Structure inside <main id="area__content">:
             <h1 class="h__toc">                  chapter title
             <section class="annotations">        "Administrative History" block:
                 effective dates + Maryland Register citations (chapter-scoped)
             <h2>Authority</h2> + sibling          enabling statute(s) (chapter-scoped)
             <h2 class="h__section">.NN Title.</h2> one per regulation, followed by
                 its body paragraphs and an optional <h3>Cross References</h3>.

There is no per-regulation document and no XML endpoint (both 404); the chapter
full.html is the atomic full-text unit, same shape as the OAC/WAC ingesters.

COMAR hierarchy: Title -> Subtitle -> Chapter -> Regulation. Citation form is
"COMAR <title>.<subtitle>.<chapter>.<regulation>" (e.g. COMAR 10.09.36.01).
Title codes can carry a letter suffix (13A, 13B, 19A). corpus_type='state_regulation'.

A chapter's Authority + Administrative History apply to all of the chapter's
regulations and are captured into structured fields (statutory_authority,
history, register_citations, effective_date, prior_effective_dates) — nothing
discarded.

The host is geo-restricted to the US; Webshare US proxy + Mozilla UA + polite
pacing.
"""

from __future__ import annotations

import argparse
import datetime as _dt
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

import requests
from bs4 import BeautifulSoup

sys.stdout.reconfigure(line_buffering=True)  # type: ignore[attr-defined]

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = Path(os.environ.get("OUT_DIR", "./data"))
OUT = DATA_DIR / "state_md_regulations.jsonl"

MD_HOST = "https://regs.maryland.gov"
MD_ROOT = f"{MD_HOST}/us/md/exec/comar"
MD_INDEX = f"{MD_ROOT}/index.json"

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
    url = f"http://{urllib.parse.quote(proxy_user)}:{urllib.parse.quote(pwd)}@{host}:{port}"
    return {"http": url, "https": url}


SESSION = requests.Session()
SESSION.headers.update({"User-Agent": _MOZ_UA})


def fetch(url: str, retries: int = 6) -> str | None:
    """Fetch raw text. Retries 429 with exponential backoff and tolerates the
    transient SSL/connection errors that rotating proxies occasionally raise."""
    proxies = _us_proxies()
    for attempt in range(retries):
        try:
            r = SESSION.get(url, timeout=60, proxies=proxies, allow_redirects=True)
            if r.status_code == 200:
                # DSD serves UTF-8 but omits the charset header on some paths;
                # force it so non-ASCII (§, curly quotes, em dashes) survive.
                r.encoding = "utf-8"
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
            time.sleep(1.5)
    return None


def fetch_json(url: str, retries: int = 6):
    txt = fetch(url, retries=retries)
    if txt is None:
        return None
    try:
        return json.loads(txt)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Discovery: one JSON tree -> every chapter
# ---------------------------------------------------------------------------


@dataclass
class ChapterRef:
    title_code: str  # "10", "13A"
    title_name: str  # "EXECUTIVE DEPARTMENT"
    subtitle_code: str  # "09"
    subtitle_name: str  # "MEDICAL CARE PROGRAMS"
    chapter_code: str  # "36"
    chapter_name: str  # "General Medical Assistance Provider Participation..."
    sc: str  # "10.09.36"
    full_html_path: str  # "/us/md/exec/comar/10.09.36/index.full.html"
    json_path: str  # "/us/md/exec/comar/10.09.36/index.json"


def _clean_container_name(et_title: str, code: str) -> str:
    """Strip the "Title 10 ", "Subtitle 09 ", "Chapter 36 " prefix from a node
    title, leaving just the human name (uppercase for title/subtitle)."""
    t = et_title.strip()
    t = re.sub(r"^(?:Title|Subtitle|Chapter)\s+[0-9A-Za-z\-]+\s*", "", t)
    return t.strip()


def list_chapters() -> list[ChapterRef]:
    """Walk the single root index.json tree and return every chapter container.

    A title node has a 1-part sc ("10"); a subtitle node 2-part ("10.09"); a
    chapter node 3-part ("10.09.36") and carries "fh" (the full-html path)."""
    root = fetch_json(MD_INDEX)
    if not isinstance(root, dict):
        raise RuntimeError("could not fetch COMAR root index.json")

    out: list[ChapterRef] = []

    def walk(node: dict, t_code: str, t_name: str, s_code: str, s_name: str) -> None:
        for ch in node.get("c", []):
            sc = str(ch.get("sc", "")).strip()
            et = ch.get("et")
            name = _clean_container_name(str(ch.get("t", "")), sc)
            parts = sc.split(".")
            if et == "container" and len(parts) == 1:
                walk(ch, parts[0], name, "", "")
            elif et == "container" and len(parts) == 2:
                walk(ch, t_code, t_name, parts[1], name)
            elif et == "container" and len(parts) == 3 and ch.get("fh"):
                out.append(
                    ChapterRef(
                        title_code=t_code,
                        title_name=t_name,
                        subtitle_code=s_code,
                        subtitle_name=s_name,
                        chapter_code=parts[2],
                        chapter_name=name,
                        sc=sc,
                        full_html_path=str(ch.get("fh")),
                        json_path=str(ch.get("j") or f"{MD_ROOT}/{sc}/index.json"),
                    )
                )
            elif et == "container":
                # Deeper/odd nesting: keep descending so nothing is missed.
                walk(ch, t_code, t_name, s_code, s_name)

    walk(root, "", "", "", "")
    out.sort(key=_chapter_sort_key)
    return out


def _chapter_sort_key(c: ChapterRef) -> tuple:
    def num(code: str) -> tuple[int, str]:
        m = re.match(r"(\d+)", code)
        return (int(m.group(1)) if m else 9999, code)

    return (num(c.title_code), num(c.subtitle_code), num(c.chapter_code))


# ---------------------------------------------------------------------------
# Section enumeration (clean codes/titles from the chapter index.json)
# ---------------------------------------------------------------------------


def list_section_stubs(chapter_json_path: str) -> list[tuple[str, str]]:
    """Return [(regulation_num, section_title)] for a chapter, e.g.
    [("01", ".01 Definitions."), ("03-1", ".03-1 Conditions ...")]."""
    data = fetch_json(f"{MD_HOST}{chapter_json_path}")
    if not isinstance(data, dict):
        return []
    out: list[tuple[str, str]] = []
    for ch in data.get("c", []):
        if ch.get("et") != "section":
            continue
        sc = str(ch.get("sc", "")).strip()  # e.g. "10.09.36.03-1"
        title = str(ch.get("t", "")).strip()
        # The regulation number is the last dotted part of the sc code.
        reg = sc.split(".")[-1]
        if reg:
            out.append((reg, title))
    return out


# ---------------------------------------------------------------------------
# Chapter HTML parsing (full text + chapter-level Authority/History)
# ---------------------------------------------------------------------------

_RESERVED_PAT = re.compile(r"\b(repealed|reserved|renumbered|rescinded)\b", re.IGNORECASE)
_MONTHS = "January|February|March|April|May|June|July|August|September|October|November|December"
# "effective July 1, 1990"
_EFF_RE = re.compile(rf"effective\s+((?:{_MONTHS})\s+\d{{1,2}},\s+\d{{4}})", re.IGNORECASE)
# Maryland Register citation: "17:15 Md. R. 1851" (volume:issue Md. R. page)
_MDR_RE = re.compile(r"\b(\d{1,3}:\d{1,3})\s+Md\.?\s*R\.?\s*(\d+)")
# Regulation header text: ".01 Definitions." or ".03-1 Conditions ..."
_REG_HEAD_RE = re.compile(r"^\.([0-9]+(?:-[0-9]+)?)\s*(.*)$")


def _norm_ws(s: str) -> str:
    # Collapse runs of whitespace (incl. NBSP) but keep single newlines.
    s = s.replace(" ", " ")
    s = re.sub(r"[ \t]+", " ", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()


# A standalone enumerator paragraph, e.g. "A.", "(1)", "(a)", "B-1." — when a
# paragraph collapses to just its label, the following paragraph holds its text.
_LONE_ENUM_RE = re.compile(r"^(?:\([0-9A-Za-z]+\)|[0-9A-Za-z]{1,4}[.\-]?)$")


def _join_enumerators(parts: list[str]) -> str:
    """Join paragraph fragments, gluing a lone enumerator label to the text that
    follows it so the body reads as flowing numbered paragraphs rather than a
    label on its own line. Defensive: most paragraphs already include their text."""
    cleaned = [_norm_ws(p) for p in parts]
    cleaned = [p for p in cleaned if p]
    merged: list[str] = []
    i = 0
    while i < len(cleaned):
        cur = cleaned[i]
        if _LONE_ENUM_RE.match(cur) and i + 1 < len(cleaned):
            merged.append(f"{cur} {cleaned[i + 1]}")
            i += 2
        else:
            merged.append(cur)
            i += 1
    return _norm_ws("\n".join(merged))


def _dedupe(items) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for x in items:
        if x and x not in seen:
            seen.add(x)
            out.append(x)
    return out


def _latest_eff(dates: list[str]) -> str:
    def pd(s: str):
        try:
            return _dt.datetime.strptime(s, "%B %d, %Y")
        except Exception:
            return _dt.datetime.min

    return max(dates, key=pd) if dates else ""


@dataclass
class ChapterMeta:
    statutory_authority: str = ""
    history: str = ""
    register_citations: list[str] = field(default_factory=list)
    effective_date: str = ""
    prior_effective_dates: str = ""


def _block_text_after_h2(main, label: str) -> str:
    """Text of the siblings that follow an <h2>label</h2> up to the next <h2>."""
    for h2 in main.find_all("h2"):
        if h2.get_text(strip=True).lower() == label.lower():
            parts: list[str] = []
            for sib in h2.next_siblings:
                if getattr(sib, "name", None) == "h2":
                    break
                t = sib.get_text(" ", strip=True) if hasattr(sib, "get_text") else str(sib).strip()
                if t:
                    parts.append(t)
            return _norm_ws("\n".join(parts))
    return ""


def _parse_chapter_meta(main) -> ChapterMeta:
    # Authority block.
    authority = _block_text_after_h2(main, "Authority")
    # Administrative History block lives in <section class="annotations">.
    ann = main.find("section", class_="annotations")
    history = ""
    if ann:
        history = _norm_ws(ann.get_text("\n", strip=True))
        # Drop the leading "Administrative History\nEffective date:" labels.
        history = re.sub(r"^Administrative History\s*", "", history, flags=re.IGNORECASE)
        history = re.sub(r"^Effective date:\s*", "", history, flags=re.IGNORECASE).strip()
    effs = _dedupe(_EFF_RE.findall(history))
    registers = _dedupe(f"{v} Md. R. {p}" for v, p in _MDR_RE.findall(history))
    latest = _latest_eff(effs)
    priors = [e for e in effs if e != latest]
    return ChapterMeta(
        statutory_authority=authority,
        history=history,
        register_citations=registers,
        effective_date=latest,
        prior_effective_dates="; ".join(priors) if priors else "",
    )


@dataclass
class Regulation:
    title_code: str
    title_name: str
    subtitle_code: str
    subtitle_name: str
    chapter_code: str
    chapter_name: str
    reg_num: str  # "01", "03-1"
    reg_title: str  # "Definitions" (header text minus the ".NN ")
    raw_text: str
    source_url: str
    # chapter-scoped metadata (applies to this regulation)
    statutory_authority: str = ""
    history: str = ""
    register_citations: list[str] = field(default_factory=list)
    effective_date: str = ""
    prior_effective_dates: str = ""

    @property
    def cite_num(self) -> str:
        return f"{self.title_code}.{self.subtitle_code}.{self.chapter_code}.{self.reg_num}"

    @property
    def chapter_id(self) -> str:
        return f"{self.title_code}.{self.subtitle_code}.{self.chapter_code}"


def parse_chapter(html: str, ref: ChapterRef, stub_titles: dict[str, str]) -> list[Regulation]:
    soup = BeautifulSoup(html, "html.parser")
    main = soup.find(id="area__content") or soup
    meta = _parse_chapter_meta(main)

    heads = main.find_all("h2", class_="h__section")
    out: list[Regulation] = []
    for h in heads:
        head_txt = h.get_text(" ", strip=True)
        m = _REG_HEAD_RE.match(head_txt)
        if not m:
            continue
        reg_num = m.group(1)
        reg_title = m.group(2).strip().rstrip(".")
        # Prefer the clean title from the JSON stub when available.
        stub_t = stub_titles.get(reg_num, "")
        if stub_t:
            sm = _REG_HEAD_RE.match(stub_t)
            if sm and sm.group(2).strip():
                reg_title = sm.group(2).strip().rstrip(".")

        # Body = every sibling between this h__section and the next one. Each
        # paragraph sibling is flattened with single spaces (the elaws markup
        # carries the enumerator "A.", "(1)" in an inline child of the same <p>
        # as its text, so a space keeps the line reading naturally), then joined
        # newline-per-paragraph. A <h3> "Cross References" subheading is kept as
        # a labeled line so the cross-reference list stays attributed.
        parts: list[str] = []
        for sib in h.next_siblings:
            name = getattr(sib, "name", None)
            if name == "h2" and "h__section" in (sib.get("class") or []):
                break
            if name == "h3":
                parts.append(f"{sib.get_text(' ', strip=True)}:")
                continue
            t = sib.get_text(" ", strip=True) if hasattr(sib, "get_text") else str(sib).strip()
            if t:
                parts.append(t)
        body = _join_enumerators(parts)

        if _RESERVED_PAT.search(reg_title) and len(body) < 30:
            continue
        if len(body) < 15:
            # Stub-only / placeholder regulation with no substantive text.
            continue

        out.append(
            Regulation(
                title_code=ref.title_code,
                title_name=ref.title_name,
                subtitle_code=ref.subtitle_code,
                subtitle_name=ref.subtitle_name,
                chapter_code=ref.chapter_code,
                chapter_name=ref.chapter_name,
                reg_num=reg_num,
                reg_title=reg_title,
                raw_text=body,
                source_url=f"{MD_ROOT}/{ref.sc}.{reg_num}",
                statutory_authority=meta.statutory_authority,
                history=meta.history,
                register_citations=meta.register_citations,
                effective_date=meta.effective_date,
                prior_effective_dates=meta.prior_effective_dates,
            )
        )
    return out


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


def to_chunk_record(r: Regulation) -> dict:
    cite_num = r.cite_num  # "10.09.36.01"
    act_id = (
        f"STATE_MD_COMAR_{_safe(r.title_code)}_{_safe(r.subtitle_code)}_"
        f"{_safe(r.chapter_code)}_{_safe(r.reg_num)}"
    )
    citation = f"COMAR {cite_num}"
    text = r.raw_text
    section_title = f"COMAR {cite_num}. {r.reg_title}" if r.reg_title else f"COMAR {cite_num}"

    # Rich, searchable embed header: surface the regulation-specific metadata
    # (effective date, enabling statutory authority) so it is retrievable, not
    # merely stored.
    meta_lines: list[str] = []
    if r.effective_date:
        meta_lines.append(f"Effective: {r.effective_date}")
    if r.statutory_authority:
        meta_lines.append(f"Statutory Authority: {r.statutory_authority}")
    if r.register_citations:
        meta_lines.append(f"Maryland Register: {', '.join(r.register_citations[:8])}")
    meta_header = ("\n" + "\n".join(meta_lines)) if meta_lines else ""
    sub_caption = f": {r.subtitle_name}" if r.subtitle_name else ""
    chap_caption = f": {r.chapter_name}" if r.chapter_name else ""
    title_caption = f" ({r.title_name})" if r.title_name else ""
    text_for_embedding = (
        f"Regulation: Code of Maryland Regulations | US | Maryland | In Force\n"
        f"Title {r.title_code}{title_caption} / "
        f"Subtitle {r.subtitle_code}{sub_caption} / "
        f"Chapter {r.chapter_code}{chap_caption}\n"
        f"{section_title}{meta_header}\n\n{text}"
    )

    display_title = section_title
    breadcrumb = [
        {
            "type": "title",
            "num": r.title_code,
            "label": f"Title {r.title_code}",
            "name": r.title_name,
        },
        {
            "type": "subtitle",
            "num": r.subtitle_code,
            "label": f"Subtitle {r.subtitle_code}",
            "name": r.subtitle_name,
        },
        {
            "type": "chapter",
            "num": r.chapter_code,
            "label": f"Chapter {r.chapter_code}",
            "name": r.chapter_name,
        },
        {
            "type": "regulation",
            "num": r.reg_num,
            "label": citation,
            "name": r.reg_title,
        },
    ]

    md = {
        "act_id": act_id,
        "corpus_type": "state_regulation",
        "category": "state_regulation",
        "document_type": "regulation",
        "jurisdiction": "US",
        "country_code": "US",
        "state": "md",
        "title_number": r.title_code,
        "title_name": f"Title {r.title_code} — {r.title_name}"
        if r.title_name
        else f"Title {r.title_code}",
        "title": "Code of Maryland Regulations",
        "title_code": f"comar_title_{r.title_code.lower()}",
        "top_level_title": r.title_code,
        "chapter": r.chapter_id,
        "chapter_name": r.chapter_name,
        "section_number": cite_num,
        "section_title": section_title,
        "year": 2026,
        "act_status": "in_force",
        "renumbered_to": "",
        "transferred_to": "",
        "level_classifier": "regulation",
        # --- regulation-specific rich metadata (captured, not discarded) ---
        "effective_date": r.effective_date or None,
        "statutory_authority": r.statutory_authority or None,
        "rule_amplifies": None,
        "prior_effective_dates": r.prior_effective_dates or None,
        "history": r.history or None,
        "register_citations": r.register_citations,
        "issuing_agency": r.title_name or None,
        "issuing_agency_code": r.title_code,
        "subtitle": r.subtitle_code,
        "subtitle_name": r.subtitle_name,
        "citation": citation,
        "citation_short": citation,
        "display_label": citation,
        "display_title": display_title,
        "display_path": (
            f"Code of Maryland Regulations / Title {r.title_code} {r.title_name} / "
            f"Subtitle {r.subtitle_code} {r.subtitle_name} / "
            f"Chapter {r.chapter_code} {r.chapter_name} / {citation}"
        ),
        "breadcrumb": breadcrumb,
        "sort_key": act_id,
        "word_count": len(text.split()),
        "subsection_count": 0,
        "subsection_letters": [],
        "numbered_paragraph_count": 0,
        "cross_references_count": 0,
        "cross_references_usc": [],
        "cross_references_cfr": [],
        "amendment_years": _amendment_years(r),
        "amendments_count": len(r.register_citations),
        "last_amended_year": _last_amended_year(r),
        "public_laws_referenced": [],
        "public_laws_count": 0,
        "source_url": r.source_url,
        "parent_id": (
            f"us/md/regulations/title={r.title_code}/subtitle={r.subtitle_code}/"
            f"chapter={r.chapter_code}"
        ),
        "raw_node_id": (
            f"us/md/regulations/title={r.title_code}/subtitle={r.subtitle_code}/"
            f"chapter={r.chapter_code}/regulation={r.reg_num}"
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


def _amendment_years(r: Regulation) -> list[int]:
    # Clamp to [1789, edition year]: the history text can carry forward-looking
    # or garbled years that would otherwise become a future "last amended".
    years = {int(m) for m in re.findall(r"\b((?:19|20)\d{2})\b", r.history)}
    return sorted(y for y in years if 1789 <= y <= 2026)


def _last_amended_year(r: Regulation) -> int | None:
    yrs = _amendment_years(r)
    return yrs[-1] if yrs else None


# ---------------------------------------------------------------------------
# Crawl
# ---------------------------------------------------------------------------


def process_chapter(ref: ChapterRef) -> list[Regulation]:
    html = fetch(f"{MD_HOST}{ref.full_html_path}")
    if not html:
        return []

    # Clean section codes/titles come from the chapter's index.json.
    stubs = list_section_stubs(ref.json_path)
    stub_titles = dict(stubs)

    return parse_chapter(html, ref, stub_titles)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--titles",
        default="",
        help="Comma-separated title codes (e.g. '10,13A'). Default: all.",
    )
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--delay", type=float, default=0.3)
    ap.add_argument("--limit", type=int, default=0, help="Cap chapters (debug).")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    print(f"[COMAR] discovering structure from {MD_INDEX}", flush=True)
    chapters = list_chapters()
    if args.titles:
        wanted = {t.strip().upper() for t in args.titles.split(",") if t.strip()}
        chapters = [c for c in chapters if c.title_code.upper() in wanted]
    if args.limit:
        chapters = chapters[: args.limit]
    n_titles = len({c.title_code for c in chapters})
    print(f"[COMAR] {n_titles} titles, {len(chapters)} chapters to fetch", flush=True)
    if args.dry_run:
        for c in chapters[:10]:
            print(
                f"  {c.sc}  T{c.title_code} S{c.subtitle_code} C{c.chapter_code}  {c.chapter_name[:48]}"
            )
        return 0

    chunks: list[dict] = []
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {ex.submit(process_chapter, c): c for c in chapters}
        for done, fut in enumerate(as_completed(futures), start=1):
            try:
                regs = fut.result()
                chunks.extend(to_chunk_record(x) for x in regs)
            except Exception as e:
                print(f"  ! chapter failed: {e}", flush=True)
            if done % 50 == 0 or done == len(chapters):
                rate = len(chunks) / max(time.time() - t0, 0.1)
                print(
                    f"  ... {done:>5}/{len(chapters)} chapters, "
                    f"{len(chunks):>6} regulations, {rate:.1f}/s",
                    flush=True,
                )
            time.sleep(args.delay / max(args.workers, 1))

    # Dedup + write.
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
