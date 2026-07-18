#!/usr/bin/env python3
"""Ingest the Minnesota Administrative Rules (Minn. R.) — MN's state regulations.

OFFICIAL SOURCE ONLY: https://www.revisor.mn.gov/rules/ (the Minnesota Office of
the Revisor of Statutes). No aggregators (no Justia/ZenRows/etc.).

Bulk XML / API: NONE. The Revisor portal serves the statutes/rules as a
server-rendered HTML application. Every XML/JSON probe 404s
(/rules/<ch>/full/xml, /rules/xml/<ch>, /rules/<part>/xml,
/static/data/rules/<ch>.xml). So this scraper parses HTML — but the markup is
highly structured, which makes extraction lossless and lets us pull a whole
chapter (all parts + metadata) in a SINGLE request via the "Full Chapter Text"
view:

    /rules/                         → "Table of Chapters" (each chapter = an
                                       agency's rules); chapters link to
                                       /rules/<chapter>/ and /rules/agency/<n>
    /rules/numerical/               → numeric Table of Chapters: ~832 links
                                       /rules/<chapter>/ (chapter = 3-4 digits,
                                       e.g. 1005, 4717)
    /rules/<chapter>/full           → "Full Chapter Text": EVERY part rendered
                                       inline, each as a <div class="part"
                                       id="rule.<part>"> with an
                                       <h1 class="headnote"> heading, body <p>/
                                       <div class="subp"> blocks, followed by
                                       sibling metadata divs:
                                         <div class="stat_auth">  Statutory Authority
                                         <div class="history">    History (SR + L cites)
                                         <div class="published">  Published Electronically
                                       Repealed/reserved parts render as
                                       <h1 class="headnote repealed"> + a single
                                       "[Repealed, 19 SR 1419; ...]" paragraph.

MN hierarchy: Chapter (the agency's rule chapter, e.g. 4717) → Part
(e.g. 4717.0150) → Subpart (Subp. N). Parts are the citable unit. Part numbers
look like NNNN.NNNN (chapter is the leading digits before the dot). Official
cite form: "Minn. R. <part>" (e.g. "Minn. R. 4717.0150").

Rich metadata captured into structured fields (NEVER discarded):
    Statutory Authority → statutory_authority (enabling Minn. Stat. sections)
    History             → history_note (raw), register_citations (SR cites),
                          session_law_citations (L <year> c <ch> s <sec>),
                          prior_effective_dates
    Published Elec.     → effective_date (date the part was last published as
                          in force) + published_date
    Chapter header      → issuing_agency (e.g. "DEPARTMENT OF HEALTH")

corpus_type='state_regulation'. act_id='STATE_MN_ADR_<part sanitized>'.

Geo-restricted; Webshare US proxy + Mozilla UA + polite pacing (the site 429s
under heavy concurrency).
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
from dataclasses import dataclass, field
from pathlib import Path

import requests
from bs4 import BeautifulSoup

sys.stdout.reconfigure(line_buffering=True)  # type: ignore[attr-defined]

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = Path(os.environ.get("OUT_DIR", "./data"))
OUT = DATA_DIR / "state_mn_regulations.jsonl"

MN_BASE = "https://www.revisor.mn.gov"
MN_TOC = f"{MN_BASE}/rules/numerical/"

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
    import urllib.parse

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
            r = SESSION.get(url, timeout=90, proxies=proxies, allow_redirects=True)
            if r.status_code == 200:
                return r.text
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
# Discovery: numeric Table of Chapters -> chapter numbers
# ---------------------------------------------------------------------------

# Chapter link: /rules/<chapter>/ where chapter is 3-4 digits (no dot).
_CHAPTER_HREF_RE = re.compile(r"^/rules/(\d{3,4})/?$")


def list_chapters() -> list[str]:
    """Return sorted chapter numbers (e.g. ['1005', '1100', ...]) from the
    numeric Table of Chapters."""
    html = fetch(MN_TOC)
    if not html:
        raise RuntimeError("could not fetch MN rules numeric TOC")
    soup = BeautifulSoup(html, "html.parser")
    seen: set[str] = set()
    for a in soup.find_all("a", href=True):
        m = _CHAPTER_HREF_RE.match(a["href"].strip())
        if m:
            seen.add(m.group(1))
    return sorted(seen, key=lambda s: int(s))


# ---------------------------------------------------------------------------
# Part / metadata parsing
# ---------------------------------------------------------------------------

# Part number, e.g. 4717.0150 (possibly with a trailing letter on rare parts).
_PART_NUM_RE = re.compile(r"^(\d{3,4})\.(\d{3,4}[A-Za-z]?)$")
_RESERVED_PAT = re.compile(r"\[(repealed|reserved|renumbered)\b", re.IGNORECASE)

_MONTHS = "January|February|March|April|May|June|July|August|September|October|November|December"
# "May 11, 2009"
_DATE_RE = re.compile(rf"(?:{_MONTHS})\s+\d{{1,2}},\s+\d{{4}}")
# State Register citation, e.g. "19 SR 1419", "46 SR 175"
_SR_RE = re.compile(r"\b\d+\s+SR\s+\d+\b")
# Session-law citation, e.g. "L 2008 c 328 s 13", "L 2000 c 469 s 7"
_SESSION_LAW_RE = re.compile(r"\bL\s+\d{4}\s+c\s+\d+(?:\s+s\s+[\d.,\s]+)?", re.IGNORECASE)


def _dedupe(xs: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for x in xs:
        k = re.sub(r"\s+", " ", x).strip()
        if k and k not in seen:
            seen.add(k)
            out.append(k)
    return out


def _strip_label(text: str, *labels: str) -> str:
    """Remove a leading label that MN renders inside the metadata div, e.g.
    "Statutory Authority: MS s 144.05" or "History: 19 SR 1419"."""
    out = text
    for label in labels:
        out = re.sub(rf"^\s*{re.escape(label)}\s*:?\s*", "", out, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", out).strip()


def _parse_history(hist: str) -> dict:
    """Pull structured fields out of an MN History block.

    MN history reads like "19 SR 1419; 19 SR 1637; L 2008 c 328 s 13". Unlike
    some states it does not inline explicit effective dates per amendment; the
    SR (State Register) and L (session-law) citations ARE the amendment trail.
    """
    registers = _SR_RE.findall(hist)
    session_laws = _SESSION_LAW_RE.findall(hist)
    return {
        "register_citations": _dedupe(registers),
        "session_law_citations": _dedupe(session_laws),
    }


@dataclass
class Part:
    chapter: str  # e.g. "4717"
    chapter_name: str  # e.g. "CHAPTER 4717, ENVIRONMENTAL HEALTH"
    agency: str  # e.g. "DEPARTMENT OF HEALTH"
    part_num: str  # e.g. "4717.0150"
    part_title: str  # e.g. "APPLICABILITY."
    raw_text: str
    source_url: str
    effective_date: str = ""  # date last published as in force (Published Elec.)
    published_date: str = ""  # raw "Published Electronically" date
    statutory_authority: str = ""  # enabling Minn. Stat. sections
    history_note: str = ""  # full raw History block
    register_citations: list[str] = field(default_factory=list)  # SR cites
    session_law_citations: list[str] = field(default_factory=list)  # L cites
    prior_effective_dates: str = ""  # earlier dates if exposed


def _footer_divs_after(pdiv) -> dict:
    """Collect the metadata footer divs that follow a part div in the full
    chapter view. In the "Full Chapter Text" markup these are SIBLINGS of the
    <div class="part">, not children, and (in that view) carry an empty part id,
    so association is purely positional: walk forward over div siblings as long
    as they are stat_auth/history/published.
    """
    out: dict[str, str] = {}
    sib = pdiv.find_next_sibling()
    while sib is not None and getattr(sib, "name", None) == "div":
        classes = sib.get("class") or []
        key = next((c for c in ("stat_auth", "history", "published") if c in classes), None)
        if not key:
            break
        out[key] = sib.get_text(" ", strip=True)
        sib = sib.find_next_sibling()
    return out


def _part_body_text(pdiv, part_num: str, part_title: str) -> str:
    """Extract the substantive body of a part. MN renders the part heading as an
    <h1 class="headnote"> and a "§" permalink anchor before every subpart; both
    are chrome, not content, so we drop them. Subparts keep their
    "Subp. N. <Headnote>" structure for readability."""
    # Re-parse the part's own HTML into a fresh tree so we don't mutate the
    # shared soup, then remove the heading + permalink glyphs that would
    # otherwise duplicate the title and litter the text with "§".
    clone = BeautifulSoup(str(pdiv), "html.parser")
    for el in clone.find_all("h1", class_="headnote"):
        el.decompose()
    for el in clone.find_all("a", class_="permalink"):
        el.decompose()
    text = clone.get_text(" ", strip=True)
    body = re.sub(r"\s+", " ", text).strip()
    # Defensive: if the bare part heading still leads (older markup), drop it.
    for lead in (f"{part_num} {part_title}.", f"{part_num} {part_title}", part_num):
        if body.startswith(lead):
            body = body[len(lead) :].strip()
            break
    return body


def parse_chapter(html: str, chapter: str) -> list[Part]:
    soup = BeautifulSoup(html, "html.parser")
    cn = soup.find("h2", class_="chapter_no")
    chapter_name = cn.get_text(" ", strip=True) if cn else f"CHAPTER {chapter}"
    ag = soup.find("h3", class_="agency")
    agency = ag.get_text(" ", strip=True) if ag else ""

    out: list[Part] = []
    for pdiv in soup.find_all("div", class_="part"):
        h = pdiv.find("h1", class_="headnote")
        if not h:
            continue
        head_text = re.sub(r"\s+", " ", h.get_text(" ", strip=True)).strip()
        # Headnote reads "<part> <TITLE>." (live) or just "<part>" (repealed).
        m = re.match(r"^(\d{3,4}\.\d{3,4}[A-Za-z]?)\b\s*(.*)$", head_text)
        if not m:
            continue
        part_num = m.group(1)
        part_title = m.group(2).strip().rstrip(".") or part_num
        if "repealed" in (h.get("class") or []):
            continue
        body_full = pdiv.get_text(" ", strip=True)
        if _RESERVED_PAT.search(body_full[:120]):
            continue

        footers = _footer_divs_after(pdiv)
        stat_auth = _strip_label(footers.get("stat_auth", ""), "Statutory Authority")
        hist = _strip_label(footers.get("history", ""), "History")
        published_raw = _strip_label(footers.get("published", ""), "Published Electronically")
        pub_m = _DATE_RE.search(published_raw)
        published_date = pub_m.group(0) if pub_m else published_raw.strip()
        parsed = _parse_history(hist)

        body = _part_body_text(pdiv, part_num, part_title)
        if len(body) < 20:
            continue

        out.append(
            Part(
                chapter=chapter,
                chapter_name=chapter_name,
                agency=agency,
                part_num=part_num,
                part_title=part_title,
                raw_text=body,
                source_url=f"{MN_BASE}/rules/{part_num}",
                effective_date=published_date,
                published_date=published_date,
                statutory_authority=stat_auth,
                history_note=hist,
                register_citations=parsed["register_citations"],
                session_law_citations=parsed["session_law_citations"],
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


def _safe(part_num: str) -> str:
    return re.sub(r"[^0-9A-Za-z]+", "_", part_num).strip("_")


def to_chunk_record(p: Part) -> dict:
    safe = _safe(p.part_num)
    act_id = f"STATE_MN_ADR_{safe}"
    citation = f"Minn. R. {p.part_num}"
    text = p.raw_text
    # Rich, searchable embed header: include the regulation-specific metadata
    # (effective date, enabling statute) so it is retrievable, not just stored.
    meta_lines = []
    if p.effective_date:
        meta_lines.append(f"Effective: {p.effective_date}")
    if p.statutory_authority:
        meta_lines.append(f"Statutory Authority: {p.statutory_authority}")
    if p.history_note:
        meta_lines.append(f"History: {p.history_note}")
    meta_header = ("\n" + "\n".join(meta_lines)) if meta_lines else ""
    text_for_embedding = (
        f"Regulation: Minnesota Administrative Rules | US | Minnesota | In Force\n"
        f"{p.agency} / Chapter {p.chapter}: {p.chapter_name}\n"
        f"Part {p.part_num} {p.part_title}{meta_header}\n\n{text}"
    )
    md = {
        "act_id": act_id,
        "corpus_type": "state_regulation",
        "category": "state_regulation",
        "document_type": "regulation",
        "jurisdiction": "US",
        "country_code": "US",
        "state": "mn",
        "title_number": None,
        "title_name": f"Minnesota Administrative Rules — {p.agency}".rstrip(" —"),
        "title": "Minnesota Administrative Rules",
        "title_code": f"mar_{p.chapter}",
        "top_level_title": p.chapter,
        "chapter": p.chapter,
        "chapter_name": p.chapter_name,
        "section_number": p.part_num,
        "section_title": p.part_title,
        "year": 2026,
        "act_status": "in_force",
        "renumbered_to": "",
        "transferred_to": "",
        "level_classifier": "part",
        # --- regulation-specific rich metadata (captured, not discarded) ---
        "effective_date": p.effective_date or None,
        "published_date": p.published_date or None,
        "statutory_authority": p.statutory_authority or None,
        "rule_amplifies": None,
        "prior_effective_dates": p.prior_effective_dates or None,
        "history_note": p.history_note or None,
        "register_citations": p.register_citations,
        "session_law_citations": p.session_law_citations,
        "register_publication": (p.register_citations[-1] if p.register_citations else None),
        "issuing_agency": p.agency or None,
        "issuing_agency_code": p.chapter,
        "citation": citation,
        "citation_short": citation,
        "display_label": citation,
        "display_title": f"Part {p.part_num} {p.part_title}".strip(),
        "display_path": (
            f"Minnesota Administrative Rules / {p.agency} / Chapter {p.chapter} / Part {p.part_num}"
        ),
        "breadcrumb": [
            {"type": "agency", "num": p.chapter, "label": p.agency, "name": p.agency},
            {
                "type": "chapter",
                "num": p.chapter,
                "label": f"Chapter {p.chapter}",
                "name": p.chapter_name,
            },
            {
                "type": "part",
                "num": p.part_num,
                "label": f"Part {p.part_num}",
                "name": p.part_title,
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
        "amendment_years": [],
        "amendments_count": len(p.register_citations) + len(p.session_law_citations),
        "last_amended_year": None,
        "public_laws_referenced": [],
        "public_laws_count": 0,
        "source_url": p.source_url,
        "parent_id": f"us/mn/regulations/chapter={p.chapter}",
        "raw_node_id": f"us/mn/regulations/chapter={p.chapter}/part={p.part_num}",
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


def process_chapter(chapter: str) -> list[Part]:
    url = f"{MN_BASE}/rules/{chapter}/full"
    html = fetch(url)
    if not html:
        return []
    parts = parse_chapter(html, chapter)
    return parts


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--chapters",
        default="",
        help="Comma-separated chapter numbers (e.g. '4717,1200'). Default: all.",
    )
    ap.add_argument("--workers", type=int, default=3)
    ap.add_argument("--delay", type=float, default=0.8)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    print(f"[MAR] discovering chapters from {MN_TOC}", flush=True)
    chapters = list_chapters()
    if args.chapters:
        wanted = {c.strip() for c in args.chapters.split(",") if c.strip()}
        chapters = [c for c in chapters if c in wanted]
    print(f"[MAR] {len(chapters)} chapters to fetch", flush=True)
    if args.dry_run:
        return 0

    chunks: list[dict] = []
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {ex.submit(process_chapter, c): c for c in chapters}
        done = 0
        for fut in as_completed(futures):
            done += 1
            try:
                parts = fut.result()
                chunks.extend(to_chunk_record(x) for x in parts)
            except Exception as e:
                print(f"  ! chapter failed: {e}", flush=True)
            if done % 25 == 0 or done == len(chapters):
                rate = len(chunks) / max(time.time() - t0, 0.1)
                print(
                    f"  ... {done:>4}/{len(chapters)} chapters, "
                    f"{len(chunks):>6} parts, {rate:.1f}/s",
                    flush=True,
                )
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

    print(
        f"\n=== Done: parsed={len(chunks):,}, new={written:,}, elapsed={time.time() - t0:.1f}s ===",
        flush=True,
    )
    print(f"JSONL: {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
