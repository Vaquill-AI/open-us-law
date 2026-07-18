#!/usr/bin/env python3
"""Ingest the Kentucky Administrative Regulations (KAR) — Kentucky's state regs.

OFFICIAL SOURCE ONLY: apps.legislature.ky.gov (the same domain the Kentucky
constitution scraper uses, so it is Webshare-US-proxy accessible). No Justia,
ZenRows, or other aggregators.

There is NO bulk/structured (XML) feed. KAR is served as HTML, with per-section
authenticated PDF (.pdf) and Word (.docx) downloads. We scrape the HTML, which
is cleanly class-tagged (div.regulation-content, div.metadata-item, etc.).

Hierarchy (TOC of titles -> chapters -> regulations):

    /law/kar/titles.htm                       -> 135 titles, each <a href="NNN">
                                                 "Title NNN - <agency> [status]"
    /law/kar/titles/{title}                    -> chapters, each <a href="NNN">
                                                 "Chapter NNN - <name> [status]"
    /law/kar/titles/{title}/{chapter}          -> regulations, each
                                                 <a href="/law/kar/titles/{t}/{c}/{r}/">
                                                 "Regulation NNN - <heading> [status]"
                                                 (a "/REG/" suffix variant is the
                                                 *proposed* version - skipped.)
    /law/kar/titles/{title}/{chapter}/{reg}/   -> the regulation page

NOTE on URL form: the title TOC lives at `titles.htm`, but the working title /
chapter / regulation pages live under `/law/kar/titles/...` (the `.htm` is a
virtual page; the real directory is `/law/kar/titles/`).

KAR cite form: "<title> KAR <chapter>:<reg>" e.g. "201 KAR 2:050". Each reg page
carries: the citation, RELATES TO (statutes the rule implements), STATUTORY
AUTHORITY ("KRS ..."), NECESSITY/FUNCTION/CONFORMITY notes, the section bodies,
and a HISTORY block of effective dates. The promulgating agency is the Title
name. corpus_type='state_regulation', state='ky'.

We ingest the in-force ("Current") regulations (the live body of law) and record
status. Geo-restricted; Webshare US proxy + Chrome UA + polite pacing.
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
OUT = DATA_DIR / "state_ky_regulations.jsonl"

KY_BASE = "https://apps.legislature.ky.gov"
KY_TITLES_TOC = f"{KY_BASE}/law/kar/titles.htm"
KY_TITLES_DIR = f"{KY_BASE}/law/kar/titles"

_MOZ_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
)

# Only these statuses represent the in-force body of law. Proposed / Withdrawn /
# Repealed / Inactive / Expired are recorded if encountered but not ingested,
# mirroring the OAC scraper's skip of rescinded/repealed/reserved rules.
_IN_FORCE_STATUSES = {"current"}


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


def fetch(url: str, retries: int = 5) -> str | None:
    proxies = _us_proxies()
    for attempt in range(retries):
        try:
            r = SESSION.get(url, timeout=60, proxies=proxies, allow_redirects=True)
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


def fetch_bytes(url: str, retries: int = 3) -> bytes | None:
    proxies = _us_proxies()
    for attempt in range(retries):
        try:
            r = SESSION.get(url, timeout=120, proxies=proxies, allow_redirects=True)
            if r.status_code == 200:
                return r.content
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
# Discovery: titles TOC -> titles -> chapters -> regulations
# ---------------------------------------------------------------------------

# A title/chapter href on a TOC page is a bare 3-digit (or numeric) relative
# segment, e.g. "001", "201". A regulation href is an absolute path ending in a
# trailing slash: /law/kar/titles/{title}/{chapter}/{reg}/  (a "/REG/" suffix
# variant is the *proposed* version of that reg and is skipped).
_SEGMENT_HREF_RE = re.compile(r"^(\d+)$")
_REG_HREF_RE = re.compile(r"^/law/kar/titles/(\d+)/(\d+)/([0-9A-Za-z]+)/$")


@dataclass
class TitleRef:
    title: str  # zero-padded title number, e.g. "001"
    agency: str  # title name = promulgating agency
    status: str  # title-level status label


@dataclass
class ChapterRef:
    title: str
    agency: str
    chapter: str  # zero-padded chapter number, e.g. "002"
    chapter_name: str
    status: str


@dataclass
class RegRef:
    title: str
    agency: str
    chapter: str
    chapter_name: str
    reg: str  # zero-padded reg number, e.g. "010"
    heading: str
    status: str
    url: str


def _status_from_text(t: str) -> str:
    """The status label is the trailing word of a TOC entry."""
    if not t:
        return ""
    tail = t.split()[-1].strip().lower()
    if tail in {
        "current",
        "proposed",
        "withdrawn",
        "repealed",
        "inactive",
        "expired",
        "reserved",
        "vacated",
    }:
        return tail
    return ""


def list_titles() -> list[TitleRef]:
    html = fetch(KY_TITLES_TOC)
    if not html:
        raise RuntimeError("could not fetch KAR titles TOC")
    soup = BeautifulSoup(html, "html.parser")
    out: list[TitleRef] = []
    seen: set[str] = set()
    for a in soup.find_all("a", href=True):
        m = _SEGMENT_HREF_RE.match(a["href"].strip())
        if not m:
            continue
        title_num = m.group(1)
        if title_num in seen:
            continue
        text = a.get_text(" ", strip=True)
        # "Title 001 - Legislative Research Commission Current"
        agency = ""
        m_name = re.search(r"Title\s+\d+\s*(?:[-—]|—)\s*(.+)", text)
        if m_name:
            agency = m_name.group(1).strip()
            agency = re.sub(
                r"\s+(Current|Proposed|Withdrawn|Repealed|Inactive|Expired|Reserved|Vacated)$",
                "",
                agency,
                flags=re.IGNORECASE,
            ).strip()
        seen.add(title_num)
        out.append(TitleRef(title=title_num, agency=agency, status=_status_from_text(text)))
    return sorted(out, key=lambda t: int(t.title))


def list_chapters(title: TitleRef) -> list[ChapterRef]:
    url = f"{KY_TITLES_DIR}/{title.title}"
    html = fetch(url)
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    out: list[ChapterRef] = []
    seen: set[str] = set()
    for a in soup.find_all("a", href=True):
        m = _SEGMENT_HREF_RE.match(a["href"].strip())
        if not m:
            continue
        chap_num = m.group(1)
        if chap_num in seen:
            continue
        text = a.get_text(" ", strip=True)
        if "Chapter" not in text:
            continue
        chap_name = ""
        m_name = re.search(r"Chapter\s+\d+\s*(?:[-—]|—)\s*(.+)", text)
        if m_name:
            chap_name = m_name.group(1).strip()
            chap_name = re.sub(
                r"\s+(Current|Proposed|Withdrawn|Repealed|Inactive|Expired|Reserved|Vacated)$",
                "",
                chap_name,
                flags=re.IGNORECASE,
            ).strip()
        seen.add(chap_num)
        out.append(
            ChapterRef(
                title=title.title,
                agency=title.agency,
                chapter=chap_num,
                chapter_name=chap_name,
                status=_status_from_text(text),
            )
        )
    return out


def list_regs(chapter: ChapterRef) -> list[RegRef]:
    url = f"{KY_TITLES_DIR}/{chapter.title}/{chapter.chapter}"
    html = fetch(url)
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    out: list[RegRef] = []
    seen: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        m = _REG_HREF_RE.match(href)
        if not m:
            continue  # skips "/REG/" proposed-version variants
        t_num, c_num, r_num = m.group(1), m.group(2), m.group(3)
        if t_num != chapter.title or c_num != chapter.chapter:
            continue
        if r_num in seen:
            continue
        text = a.get_text(" ", strip=True)
        heading = ""
        m_h = re.search(r"Regulation\s+\w+\s*(?:[-—]|—)\s*(.+)", text)
        if m_h:
            heading = m_h.group(1).strip()
            heading = re.sub(
                r"\s+(Current|Proposed|Withdrawn|Repealed|Inactive|Expired|Reserved|Vacated)$",
                "",
                heading,
                flags=re.IGNORECASE,
            ).strip()
        seen.add(r_num)
        out.append(
            RegRef(
                title=chapter.title,
                agency=chapter.agency,
                chapter=chapter.chapter,
                chapter_name=chapter.chapter_name,
                reg=r_num,
                heading=heading,
                status=_status_from_text(text),
                url=f"{KY_BASE}{href}",
            )
        )
    return out


# ---------------------------------------------------------------------------
# Regulation page parsing
# ---------------------------------------------------------------------------

_EFF_RE = re.compile(r"eff\.\s*([\d]{1,2}[-/]\d{1,2}[-/]\d{2,4})", re.IGNORECASE)


def _norm_date(d: str) -> str:
    d = d.replace("/", "-")
    parts = d.split("-")
    if len(parts) == 3:
        mm, dd, yy = parts
        if len(yy) == 2:
            yy = ("19" if int(yy) > 30 else "20") + yy
        return f"{int(mm):02d}-{int(dd):02d}-{yy}"
    return d


def _year_of(d: str) -> int | None:
    m = re.search(r"(\d{4})$", d)
    return int(m.group(1)) if m else None


@dataclass
class Reg:
    title: str
    agency: str
    chapter: str
    chapter_name: str
    reg: str
    cite_number: str  # e.g. "201 KAR 2:010"
    heading: str
    raw_text: str
    source_url: str
    relates_to: str = ""  # RELATES TO: statutes implemented
    statutory_authority: str = ""  # STATUTORY AUTHORITY: enabling KRS
    necessity_function_conformity: str = ""
    history_raw: str = ""
    effective_date: str = ""  # most-recent effective date
    prior_effective_dates: list[str] = field(default_factory=list)
    amendment_years: list[int] = field(default_factory=list)
    expiration_date: str = ""  # 7-year expiration from sidebar
    status: str = "current"


def parse_reg(html: str, ref: RegRef) -> Reg | None:
    soup = BeautifulSoup(html, "html.parser")
    rc = soup.find("div", class_="regulation-content")
    if rc is None:
        return None

    # --- citation: span.citation-header ("201 KAR 2:010.") + span.citation-text
    cite_el = rc.find(class_="citation")
    cite_number = ""
    heading = ref.heading
    if cite_el:
        ch = cite_el.find(class_="citation-header")
        ct = cite_el.find(class_="citation-text")
        if ch:
            cite_number = ch.get_text(strip=True).rstrip(".").strip()
        if ct:
            heading = ct.get_text(" ", strip=True).strip().rstrip(".")
    if not cite_number:
        cite_number = f"{int(ref.title)} KAR {int(ref.chapter)}:{ref.reg}"

    # --- labeled metadata items (RELATES TO / STATUTORY AUTHORITY / NFC) ---
    relates_to = ""
    statutory_authority = ""
    nfc = ""
    for mi in rc.find_all("div", class_="metadata-item"):
        hdr = mi.find(class_="metadata-item-header")
        val = mi.find(class_="metadata-item-text")
        if not hdr:
            continue
        label = hdr.get_text(strip=True).rstrip(":").upper()
        value = val.get_text(" ", strip=True) if val else ""
        if label == "RELATES TO":
            relates_to = value
        elif label == "STATUTORY AUTHORITY":
            statutory_authority = value
        elif label.startswith("NECESSITY"):
            nfc = value

    # --- HISTORY block (effective dates) - lives in a sibling div ---
    history_raw = ""
    hist = soup.find("div", class_="history-content")
    if hist:
        hb = hist.find(class_="history-body")
        history_raw = (hb or hist).get_text(" ", strip=True)
        history_raw = re.sub(r"^HISTORY:\s*", "", history_raw, flags=re.IGNORECASE).strip()
    eff_dates = [_norm_date(d) for d in _EFF_RE.findall(history_raw)]
    effective_date = eff_dates[-1] if eff_dates else ""
    prior = eff_dates[:-1] if len(eff_dates) > 1 else []
    # Clamp to a plausible window: malformed/reversed effective dates otherwise
    # yield garbage years (e.g. "5202") via _year_of's trailing-4-digit match.
    amendment_years = sorted(
        {
            y
            for d in eff_dates
            if (y := _year_of(d)) is not None and 1789 <= y <= 2026
        }
    )

    # --- 7-year expiration from the sidebar ---
    expiration_date = ""
    sb = soup.find("div", class_="regulation-sidebar")
    if sb:
        m = re.search(r"7-Year Expiration:\s*([\d/]+)", sb.get_text(" ", strip=True))
        if m:
            expiration_date = _norm_date(m.group(1))

    # --- body: assemble headers ("Section 1.", "(1)", "(a)") + section text in
    # document order. Strip the duplicate trailing copy by only reading the
    # first regulation-content card. ---
    parts: list[str] = []
    for el in rc.find_all(["h2", "span"]):
        cls = el.get("class") or []
        if "headers" in cls or "section-text" in cls:
            parts.append(el.get_text(" ", strip=True))
    body = re.sub(r"\s+", " ", " ".join(parts)).strip()
    if len(body) < 30:
        # Fallback: whole card text minus the metadata labels we already pulled.
        body = rc.get_text(" ", strip=True)
        for lbl in ("RELATES TO:", "STATUTORY AUTHORITY:", "NECESSITY, FUNCTION, AND CONFORMITY:"):
            body = body.replace(lbl, " ")
        body = re.sub(r"\s+", " ", body).strip()
        if len(body) < 30:
            return None

    return Reg(
        title=ref.title,
        agency=ref.agency,
        chapter=ref.chapter,
        chapter_name=ref.chapter_name,
        reg=ref.reg,
        cite_number=cite_number,
        heading=heading,
        raw_text=body,
        source_url=ref.url,
        relates_to=relates_to,
        statutory_authority=statutory_authority,
        necessity_function_conformity=nfc,
        history_raw=history_raw,
        effective_date=effective_date,
        prior_effective_dates=prior,
        amendment_years=amendment_years,
        expiration_date=expiration_date,
        status=ref.status or "current",
    )


# ---------------------------------------------------------------------------
# Chunk emission
# ---------------------------------------------------------------------------


def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def _point_id(act_id: str, idx: int, text: str) -> str:
    seed = f"{act_id}::{idx}::{_sha1(text)[:12]}"
    return str(uuid.UUID(hashlib.md5(seed.encode()).hexdigest()))


def _act_id(r: Reg) -> str:
    safe_t = re.sub(r"[^0-9A-Za-z]", "_", r.title)
    safe_c = re.sub(r"[^0-9A-Za-z]", "_", r.chapter)
    safe_r = re.sub(r"[^0-9A-Za-z]", "_", r.reg)
    return f"STATE_KY_KAR_T{safe_t}_C{safe_c}_R{safe_r}"


def to_chunk_record(r: Reg) -> dict:
    act_id = _act_id(r)
    citation = r.cite_number  # "201 KAR 2:010"
    section_title = f"{citation}. {r.heading}".strip().rstrip(".")
    text = r.raw_text

    # Rich, searchable embed header: include the regulation-specific metadata
    # (effective date, statutory authority) so it is retrievable, not just
    # stored, per the ingestion spec.
    meta_lines: list[str] = []
    if r.effective_date:
        meta_lines.append(f"Effective: {r.effective_date}")
    if r.statutory_authority:
        meta_lines.append(
            f"Statutory Authority: KRS {r.statutory_authority}"
            if not r.statutory_authority.upper().startswith("KRS")
            else f"Statutory Authority: {r.statutory_authority}"
        )
    if r.relates_to:
        meta_lines.append(f"Relates To: {r.relates_to}")
    meta_header = ("\n" + "\n".join(meta_lines)) if meta_lines else ""
    text_for_embedding = (
        f"Regulation: Kentucky Administrative Regulations | US | Kentucky | In Force\n"
        f"Title {r.title} ({r.agency}) / Chapter {r.chapter}: {r.chapter_name}\n"
        f"{citation}. {r.heading}{meta_header}\n\n{text}"
    )

    md = {
        "act_id": act_id,
        "corpus_type": "state_regulation",
        "category": "state_regulation",
        "document_type": "regulation",
        "jurisdiction": "US",
        "country_code": "US",
        "state": "ky",
        "title_number": int(r.title),
        "title_name": f"Kentucky Administrative Regulations — Title {r.title} ({r.agency})",
        "title": "Kentucky Administrative Regulations",
        "title_code": f"kar_{int(r.title)}",
        "top_level_title": r.title,
        "chapter": f"{int(r.title)}:{int(r.chapter)}",
        "chapter_name": r.chapter_name,
        "section_number": citation,
        "section_title": section_title,
        "year": 2026,
        "act_status": "in_force" if r.status in _IN_FORCE_STATUSES else r.status,
        "renumbered_to": "",
        "transferred_to": "",
        "level_classifier": "regulation",
        # --- regulation-specific rich metadata (captured, never stripped) ---
        "effective_date": r.effective_date or None,
        "statutory_authority": r.statutory_authority or None,
        "rule_amplifies": r.relates_to or None,  # RELATES TO
        "relates_to": r.relates_to or None,
        "necessity_function_conformity": r.necessity_function_conformity or None,
        "prior_effective_dates": r.prior_effective_dates,
        "history": r.history_raw or None,
        "expiration_date": r.expiration_date or None,
        "issuing_agency": r.agency or None,
        "issuing_agency_code": r.title,
        "citation": citation,
        "citation_short": citation,
        "display_label": citation,
        "display_title": section_title,
        "display_path": (
            f"Kentucky Administrative Regulations / Title {r.title} "
            f"({r.agency}) / Chapter {r.chapter} / {citation}"
        ),
        "breadcrumb": [
            {"type": "title", "num": r.title, "label": f"Title {r.title}", "name": r.agency},
            {
                "type": "chapter",
                "num": r.chapter,
                "label": f"Chapter {r.chapter}",
                "name": r.chapter_name,
            },
            {"type": "regulation", "num": citation, "label": citation, "name": r.heading},
        ],
        "sort_key": act_id,
        "word_count": len(text.split()),
        "subsection_count": 0,
        "subsection_letters": [],
        "numbered_paragraph_count": 0,
        "cross_references_count": 0,
        "cross_references_usc": [],
        "cross_references_cfr": [],
        "amendment_years": r.amendment_years,
        "amendments_count": len(r.amendment_years),
        "last_amended_year": (r.amendment_years[-1] if r.amendment_years else None),
        "public_laws_referenced": [],
        "public_laws_count": 0,
        "source_url": r.source_url,
        "parent_id": f"us/ky/regulations/title={r.title}/chapter={r.chapter}",
        "raw_node_id": (f"us/ky/regulations/title={r.title}/chapter={r.chapter}/reg={r.reg}"),
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


def process_reg(ref: RegRef) -> Reg | None:
    html = fetch(ref.url)
    if not html:
        return None
    reg = parse_reg(html, ref)
    if reg is None:
        return None
    return reg


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--titles", default="", help="Comma-separated title numbers (e.g. '201,902'). Default: all."
    )
    ap.add_argument("--workers", type=int, default=3)
    ap.add_argument("--delay", type=float, default=0.8)
    ap.add_argument(
        "--all-statuses",
        action="store_true",
        help="Ingest non-Current regs too (default: in-force only).",
    )
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    print(f"[KAR] discovering titles from {KY_TITLES_TOC}", flush=True)
    titles = list_titles()
    if args.titles:
        wanted = {t.strip().zfill(3) for t in args.titles.split(",") if t.strip()}
        titles = [
            t
            for t in titles
            if t.title in wanted or t.title.lstrip("0") in {w.lstrip("0") for w in wanted}
        ]
    print(f"[KAR] {len(titles)} titles", flush=True)

    # Phase 1: titles -> chapters -> regulation refs
    all_regs: list[RegRef] = []
    for t in titles:
        chapters = list_chapters(t)
        time.sleep(args.delay)
        n_before = len(all_regs)
        for ch in chapters:
            regs = list_regs(ch)
            for rg in regs:
                if not args.all_statuses and (rg.status or "current") not in _IN_FORCE_STATUSES:
                    continue
                all_regs.append(rg)
            time.sleep(args.delay)
        print(
            f"  [title {t.title} {t.agency[:32]}] "
            f"{len(chapters)} chapters, {len(all_regs) - n_before} regs",
            flush=True,
        )
    print(f"\n[KAR] {len(all_regs)} regulations to fetch", flush=True)
    if args.dry_run:
        return 0

    # Phase 2: crawl regulation pages
    chunks: list[dict] = []
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {ex.submit(process_reg, ref): ref for ref in all_regs}
        for done, fut in enumerate(as_completed(futures), start=1):
            try:
                reg = fut.result()
                if reg is not None:
                    chunks.append(to_chunk_record(reg))
            except Exception as e:
                print(f"  ! reg failed: {e}", flush=True)
            if done % 100 == 0 or done == len(all_regs):
                rate = len(chunks) / max(time.time() - t0, 0.1)
                print(
                    f"  ... {done:>6}/{len(all_regs)} regs, {len(chunks):>6} parsed, {rate:.1f}/s",
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
