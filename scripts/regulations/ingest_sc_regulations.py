#!/usr/bin/env python3
"""Ingest the South Carolina Code of Regulations -- SC's state regulations.

OFFICIAL SOURCE ONLY: scstatehouse.gov (the SC General Assembly's Legislative
Council). No Justia / ZenRows / aggregators. The Legislative Council publishes
the unannotated Code of Regulations as per-chapter Microsoft Word (.docx) files
linked from the regulation index. The HTML "view" pages (c001.php, six61.php)
are commented out / 404, so the .docx download is the machine-readable source.

    /coderegs/statmast.php
        -> 108 chapter rows, each "CHAPTER N - <AGENCY>", with a Word link
           /getfile.php?TYPE=CODEOFREGS&CHAPTER=N  (returns C0NN.docx)
        -> Chapter 61 (Dept. of Environmental Services) is too large to ship as
           one file, so its row links to /coderegs/Chapter61Word.php, which lists
           ~18 segment files:
           /getfile.php?TYPE=CODEOFREGS&CHAPTER=61&LETTER=<segment>

Inside each .docx the structure is:

    CHAPTER N
    Department of Labor, Licensing and Regulation-Board of Accountancy
    (Statutory Authority: 1976 Code Sections 40-1-70 and 40-2-70)
    1-01. General Requirements for Licensure as a CPA.
        A. Completed application ...
        B. ...
    HISTORY: Added by State Register Volume 31, Issue No. 5, eff May 25, 2007.
             Amended by SCSR 44-6 Doc. No. 4923, eff June 26, 2020; ...
    1-02. Examinations.
        ...

Regulation numbers are "<chapter>-<reg>" with an optional decimal sub-number,
e.g. 1-01, 61-79, 61-58.17, 61-79.260. Each regulation becomes one chunk.

SC cite form: "S.C. Code Regs. <chapter>-<regulation>" (e.g. S.C. Code Regs.
61-79). corpus_type='state_regulation', state='sc'.

Rich metadata is captured, never stripped: effective_date (latest "eff." date),
prior_effective_dates/history (full HISTORY note), statutory_authority (the
"(Statutory Authority: ...)" line), issuing_agency (the chapter's promulgating
department/board), register_citations (State Register / SCSR doc citations).

Geo-restricted; Webshare US proxy + Mozilla UA + polite pacing.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import re
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

sys.stdout.reconfigure(line_buffering=True)  # type: ignore[attr-defined]

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = Path(os.environ.get("OUT_DIR", "./data"))
OUT = DATA_DIR / "state_sc_regulations.jsonl"

SC_BASE = "https://www.scstatehouse.gov"
SC_TOC = f"{SC_BASE}/coderegs/statmast.php"

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


def fetch(url: str, retries: int = 5) -> str | None:
    """Fetch text. Retries 429 with exponential backoff; tolerates the transient
    SSL/connection errors rotating proxies occasionally raise."""
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


def fetch_bytes(url: str, retries: int = 5) -> bytes | None:
    """Fetch raw bytes (for .docx downloads). Same retry policy as fetch()."""
    proxies = _us_proxies()
    for attempt in range(retries):
        try:
            r = SESSION.get(url, timeout=120, proxies=proxies, allow_redirects=True)
            if r.status_code == 200:
                ct = r.headers.get("Content-Type", "")
                # getfile.php returns a tiny "File not found" HTML body on miss.
                if "text/html" in ct and len(r.content) < 64:
                    return None
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
# Discovery: statmast.php -> chapters -> .docx file URLs
# ---------------------------------------------------------------------------

# A chapter row's first cell: "CHAPTER 1 - DEPARTMENT OF ... - BOARD OF ..."
# The separator after the number can be a hyphen, em dash, or "--".
_CHAPTER_HDR_RE = re.compile(r"^\s*CHAPTER\s+(\d+)\s*(?:[-–—]+)?\s*(.*)$", re.IGNORECASE)
_GETFILE_RE = re.compile(r"getfile\.php\?TYPE=CODEOFREGS&CHAPTER=(\d+)", re.IGNORECASE)


@dataclass
class ChapterFile:
    """One downloadable .docx for a chapter (or one segment of chapter 61)."""

    chapter: str  # e.g. "1", "61"
    chapter_name: str  # agency/department name from the index row
    file_url: str  # absolute getfile.php URL
    segment: str = ""  # non-empty only for chapter-61 split segments


def _clean_agency(name: str) -> str:
    """Normalize the index-row agency name to title case-ish display form.
    The index ships all-caps with mixed '--' / em-dash separators."""
    name = re.sub(r"\s+", " ", name).strip().rstrip(".-–— ")
    # Collapse "--" / em dash separators into a single em dash for readability.
    name = re.sub(r"\s*-{2,}\s*", " — ", name)
    name = re.sub(r"\s*[–—]\s*", " — ", name)
    return name


def list_chapter_files() -> list[ChapterFile]:
    """Parse statmast.php into the list of .docx files to fetch.

    Normal chapters expose a direct getfile.php Word link. Chapter 61 instead
    links to Chapter61Word.php, which we follow to collect its segment files.
    """
    html = fetch(SC_TOC)
    if not html:
        raise RuntimeError("could not fetch SC Code of Regulations index")
    soup = BeautifulSoup(html, "html.parser")
    out: list[ChapterFile] = []
    seen_files: set[str] = set()

    for tr in soup.find_all("tr"):
        td0 = tr.find("td")
        if not td0:
            continue
        m = _CHAPTER_HDR_RE.match(td0.get_text(" ", strip=True))
        if not m:
            continue
        chapter = m.group(1)
        chapter_name = _clean_agency(m.group(2))

        gf = None
        word_php = None
        for a in tr.find_all("a", href=True):
            href = a["href"]
            if _GETFILE_RE.search(href):
                gf = urljoin(SC_BASE + "/", href)
            elif re.search(r"Chapter\d+Word\.php", href, re.IGNORECASE):
                word_php = urljoin(SC_BASE + "/coderegs/", href)

        if gf:
            if gf not in seen_files:
                seen_files.add(gf)
                out.append(ChapterFile(chapter=chapter, chapter_name=chapter_name, file_url=gf))
        elif word_php:
            for cf in _list_segment_files(chapter, chapter_name, word_php):
                if cf.file_url not in seen_files:
                    seen_files.add(cf.file_url)
                    out.append(cf)

    return out


def _list_segment_files(chapter: str, chapter_name: str, word_php_url: str) -> list[ChapterFile]:
    """Follow Chapter61Word.php to list its segment .docx files."""
    html = fetch(word_php_url)
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    out: list[ChapterFile] = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        mm = re.search(
            r"getfile\.php\?TYPE=CODEOFREGS&CHAPTER=\d+&LETTER=([^\"'&]+)", href, re.IGNORECASE
        )
        if not mm:
            continue
        out.append(
            ChapterFile(
                chapter=chapter,
                chapter_name=chapter_name,
                file_url=urljoin(SC_BASE + "/", href),
                segment=mm.group(1),
            )
        )
    return out


# ---------------------------------------------------------------------------
# .docx parsing -> regulations
# ---------------------------------------------------------------------------

# Regulation header. Two styles occur across chapters:
#   "1-01. General Requirements ..."  (period after the number)
#   "5-1 Definition of Terms."        (no period; older chapters)
# Number = <chapter>-<reg>[.<sub>], optional trailing letter. The trailing
# period is optional, but the title must then begin with a capital, quote, or
# parenthesis so inline statutory cross-references (e.g. "Section 46-17-190")
# split onto their own line never get mistaken for a regulation header.
_REG_HDR_RE = re.compile(r'^(\d+-\d+(?:\.\d+)?[A-Za-z]?)\.?\s+([A-Z“"(].*)$')
_RESERVED_PAT = re.compile(r"^(deleted|reserved|repealed|renumbered|transferred)\b", re.IGNORECASE)

_MONTHS = "January|February|March|April|May|June|July|August|September|October|November|December"
# "eff May 25, 2007" / "eff. June 26, 2020"
_EFF_RE = re.compile(rf"eff\.?\s+((?:{_MONTHS})\s+\d{{1,2}},\s+\d{{4}})", re.IGNORECASE)
# "State Register Volume 31, Issue No. 5"
_REG_STATE_RE = re.compile(r"State Register Volume\s+\d+,\s+Issue No\.\s+\d+", re.IGNORECASE)
# "SCSR 44-6 Doc. No. 4923"
_REG_SCSR_RE = re.compile(r"SCSR\s+[\d-]+\s+Doc\.\s+No\.\s+\d+", re.IGNORECASE)
# Chapter-level "(Statutory Authority: 1976 Code Sections 40-1-70 and 40-2-70)"
_STAT_AUTH_RE = re.compile(r"\(Statutory Authority:\s*(.+?)\)\s*$", re.IGNORECASE)


@dataclass
class Regulation:
    chapter: str
    chapter_name: str
    reg_num: str  # e.g. "1-01", "61-79", "61-58.17"
    reg_title: str  # heading text (without number)
    raw_text: str  # assembled body (indentation preserved)
    source_url: str
    statutory_authority: str = ""  # chapter/article "(Statutory Authority: ...)"
    issuing_agency: str = ""  # promulgating department/board (chapter name)
    history: str = ""  # full HISTORY note
    effective_date: str = ""  # latest "eff." date
    prior_effective_dates: str = ""  # earlier "eff." dates, "; "-joined
    register_citations: list[str] = field(default_factory=list)


def _docx_paragraphs(blob: bytes) -> list[str]:
    """Extract paragraph texts from a .docx blob, preserving leading-tab
    indentation (SC encodes subsection depth with tabs)."""
    import docx

    doc = docx.Document(io.BytesIO(blob))
    return [p.text for p in doc.paragraphs]


def _parse_history(hist: str) -> dict:
    """Pull structured fields from an SC HISTORY note. The latest 'eff.' date is
    the regulation's current effective date; earlier ones are prior dates."""
    effs: list[str] = []
    seen_eff: set[str] = set()
    for m in _EFF_RE.findall(hist):
        k = m.strip()
        if k and k not in seen_eff:
            seen_eff.add(k)
            effs.append(k)
    registers: list[str] = []
    seen_reg: set[str] = set()
    for pat in (_REG_STATE_RE, _REG_SCSR_RE):
        for m in pat.findall(hist):
            k = re.sub(r"\s+", " ", m).strip()
            if k and k not in seen_reg:
                seen_reg.add(k)
                registers.append(k)
    return {
        "effective_date": effs[-1] if effs else "",
        "prior_effective_dates": "; ".join(effs[:-1]) if len(effs) > 1 else "",
        "register_citations": registers,
    }


def parse_docx(blob: bytes, cf: ChapterFile) -> list[Regulation]:
    """Walk a chapter .docx and split it into regulations.

    State machine over paragraphs:
      - A line matching _REG_HDR_RE starts a new regulation (flush the previous).
      - A "HISTORY:" line closes the current regulation's body and supplies its
        history/effective-date/register metadata.
      - "(Statutory Authority: ...)" updates the running chapter/article authority
        that subsequent regulations inherit.
      - Everything else between a header and its HISTORY is body text.
    """
    paras = _docx_paragraphs(blob)
    chapter_authority = ""  # most-recent "(Statutory Authority: ...)" seen
    out: list[Regulation] = []

    cur: Regulation | None = None
    body_lines: list[str] = []

    def flush() -> None:
        nonlocal cur, body_lines
        if cur is None:
            return
        body = "\n".join(body_lines).strip()
        body = re.sub(r"\n{3,}", "\n\n", body)
        # Skip deleted/repealed/reserved stubs (no substantive text).
        if not _RESERVED_PAT.search(cur.reg_title) and len(body) >= 25:
            cur.raw_text = body
            out.append(cur)
        cur = None
        body_lines = []

    for raw in paras:
        line = raw.rstrip()
        stripped = line.strip()
        if not stripped:
            if cur is not None:
                body_lines.append("")
            continue

        # Chapter/article statutory authority -- inherited by following regs.
        ma = _STAT_AUTH_RE.match(stripped)
        if ma:
            chapter_authority = re.sub(r"\s+", " ", ma.group(1)).strip()
            continue

        # HISTORY closes the current regulation.
        if stripped.upper().startswith("HISTORY:"):
            if cur is not None:
                hist = re.sub(r"\s+", " ", stripped[len("HISTORY:") :]).strip()
                parsed = _parse_history(hist)
                cur.history = hist
                cur.effective_date = parsed["effective_date"]
                cur.prior_effective_dates = parsed["prior_effective_dates"]
                cur.register_citations = parsed["register_citations"]
                flush()
            continue

        mh = _REG_HDR_RE.match(stripped)
        if mh:
            # New regulation begins -- finalize the previous one first.
            flush()
            reg_num = mh.group(1)
            reg_title = mh.group(2).strip().rstrip(".")
            cur = Regulation(
                chapter=cf.chapter,
                chapter_name=cf.chapter_name,
                reg_num=reg_num,
                reg_title=reg_title,
                raw_text="",
                source_url=cf.file_url,
                statutory_authority=chapter_authority,
                issuing_agency=cf.chapter_name,
            )
            body_lines = []
            continue

        if cur is not None:
            # Preserve the original indentation (tabs -> spaces for readability).
            body_lines.append(line.replace("\t", "    "))

    flush()
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
    safe = _safe(r.reg_num)
    act_id = f"STATE_SC_CODEREGS_{safe}"
    # Official SC cite form, e.g. "S.C. Code Regs. 61-79".
    citation = f"S.C. Code Regs. {r.reg_num}"
    citation_short = citation
    text = r.raw_text

    display_title = f"S.C. Code Regs. {r.reg_num}. {r.reg_title}"

    # Rich, searchable embed header: surface the regulation-specific metadata
    # (effective date, enabling statute, history) so it is retrievable, not just
    # stored. Effective date + statutory authority go into the embed header.
    meta_lines = []
    if r.effective_date:
        meta_lines.append(f"Effective: {r.effective_date}")
    if r.statutory_authority:
        meta_lines.append(f"Statutory Authority: {r.statutory_authority}")
    if r.history:
        meta_lines.append(f"History: {r.history}")
    meta_header = ("\n" + "\n".join(meta_lines)) if meta_lines else ""
    text_for_embedding = (
        f"Regulation: South Carolina Code of Regulations | US | South Carolina | In Force\n"
        f"Chapter {r.chapter}: {r.chapter_name}\n"
        f"{citation}. {r.reg_title}{meta_header}\n\n{text}"
    )

    breadcrumb = [
        {
            "type": "chapter",
            "num": r.chapter,
            "label": f"Chapter {r.chapter}",
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
        "state": "sc",
        "title_number": None,
        "title_name": f"South Carolina Code of Regulations — Chapter {r.chapter}",
        "title": "South Carolina Code of Regulations",
        "title_code": f"sccr_chapter_{r.chapter}",
        "top_level_title": r.chapter,
        "chapter": r.chapter,
        "chapter_name": r.chapter_name,
        "section_number": r.reg_num,
        "section_title": display_title,
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
        "issuing_agency": r.issuing_agency or None,
        "issuing_agency_code": r.chapter,
        "citation": citation,
        "citation_short": citation_short,
        "display_label": citation,
        "display_title": display_title,
        "display_path": (
            f"South Carolina Code of Regulations / Chapter {r.chapter} "
            f"{r.chapter_name} / {citation}"
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
        "amendment_years": [],
        "amendments_count": 0,
        "last_amended_year": None,
        "public_laws_referenced": [],
        "public_laws_count": 0,
        "source_url": r.source_url,
        "parent_id": f"us/sc/regulations/chapter={r.chapter}",
        "raw_node_id": f"us/sc/regulations/chapter={r.chapter}/regulation={r.reg_num}",
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


def process_chapter_file(cf: ChapterFile) -> list[Regulation]:
    blob = fetch_bytes(cf.file_url)
    if not blob:
        return []
    seg = f"-{_safe(cf.segment)}" if cf.segment else ""
    try:
        regs = parse_docx(blob, cf)
    except Exception as e:
        print(f"  ! parse failed chapter {cf.chapter}{seg}: {e}", flush=True)
        return []
    return regs


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--chapters",
        default="",
        help="Comma-separated chapter numbers (e.g. '1,61'). Default: all.",
    )
    ap.add_argument("--workers", type=int, default=3)
    ap.add_argument("--delay", type=float, default=0.8)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    print(f"[SCCR] discovering chapters from {SC_TOC}", flush=True)
    files = list_chapter_files()
    if args.chapters:
        wanted = {c.strip() for c in args.chapters.split(",") if c.strip()}
        files = [f for f in files if f.chapter in wanted]
    n_chapters = len({f.chapter for f in files})
    print(f"[SCCR] {n_chapters} chapters, {len(files)} .docx files to fetch", flush=True)
    if args.dry_run:
        for f in files:
            seg = f" [seg {f.segment}]" if f.segment else ""
            print(f"  Chapter {f.chapter}{seg}: {f.chapter_name}\n     {f.file_url}", flush=True)
        return 0

    chunks: list[dict] = []
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {ex.submit(process_chapter_file, f): f for f in files}
        done = 0
        for fut in as_completed(futures):
            done += 1  # noqa: SIM113 (counter advances alongside sleep/error handling)
            try:
                regs = fut.result()
                chunks.extend(to_chunk_record(x) for x in regs)
            except Exception as e:
                print(f"  ! chapter file failed: {e}", flush=True)
            if done % 10 == 0 or done == len(files):
                rate = len(chunks) / max(time.time() - t0, 0.1)
                print(
                    f"  ... {done:>4}/{len(files)} files, "
                    f"{len(chunks):>6} regulations, {rate:.1f}/s",
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
