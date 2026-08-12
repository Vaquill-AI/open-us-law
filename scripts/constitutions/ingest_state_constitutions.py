#!/usr/bin/env python3
"""Ingest US state constitutions into the open corpus.

corpus_type='state_constitution', act_id prefix 'SCONST_<ST>_'

Architecture: per-state config object. Each state has a discovery function
that returns (article_id, html_text, url) tuples, then a single uniform
parser splits articles into sections.

Most states are scraped from their own official .gov publisher. A small
number fall back to an open mirror where no reproducible official source
exists; those are listed in `_WS_INLINE_STATES` with the reason inline.

Run:
    OUT_DIR=./data python scripts/constitutions/ingest_state_constitutions.py --states ca
    OUT_DIR=./data python scripts/constitutions/ingest_state_constitutions.py --states ca,tx,ny --workers 8
    OUT_DIR=./data python scripts/constitutions/ingest_state_constitutions.py --all

Writes newline-delimited JSON chunk records to
$OUT_DIR/state_constitutions_chunks.jsonl.
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import hashlib
import json
import os
import re
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import quote

import requests
from bs4 import BeautifulSoup, NavigableString, Tag

if TYPE_CHECKING:
    from collections.abc import Callable

sys.stdout.reconfigure(line_buffering=True)  # type: ignore[attr-defined]

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
# The repo root has to be importable for the `scripts.lib.*` and
# `scripts.statutes.*` imports below. This script is launched both as
# `python -m scripts...` (root already on the path) and by absolute path
# (sys.path[0] is this directory), and in the second case a missing bootstrap
# would fail every one of those imports.
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from scripts.lib.payload_builder import build_payload  # noqa: E402
from scripts.lib.pdf_extract import PdfExtractionUnavailable, pdf_to_text  # noqa: E402

DATA_DIR = Path(os.environ.get("OUT_DIR", "./data"))
OUT = DATA_DIR / "state_constitutions_chunks.jsonl"

UA = "Mozilla/5.0 (Vaquill ingestion bot; +https://vaquill.ai)"


# ---------------------------------------------------------------------------
# Env + R2
# ---------------------------------------------------------------------------


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


# R2 mirroring is disabled in the open release. These no-op stubs let the
# per-source scrapers run unchanged and simply skip the source-file mirror;
# the JSONL output is unaffected.
def _r2_client():
    return None


def put_if_changed(*_args, **_kwargs) -> bool:
    return False


def _put_if_changed(*_args, **_kwargs) -> bool:
    return False


def public_url(*_args, **_kwargs) -> str:
    return ""


# ---------------------------------------------------------------------------
# HTTP session
# ---------------------------------------------------------------------------


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": UA})
    a = requests.adapters.HTTPAdapter(pool_connections=32, pool_maxsize=32, max_retries=0)
    s.mount("http://", a)
    s.mount("https://", a)
    return s


SESSION = _session()


def _us_proxies() -> dict | None:
    """US-rotating proxy dict for state .gov sources that geo-block.
    Returns None if US_PROXY_USERNAME / US_PROXY_PASSWORD aren't set."""
    _load_env()
    user = os.environ.get("US_PROXY_USERNAME", "")
    pwd = os.environ.get("US_PROXY_PASSWORD", "")
    if not user or not pwd:
        return None
    host = os.environ.get("US_PROXY_HOST", "")
    port = os.environ.get("US_PROXY_PORT", "80")
    import urllib.parse

    proxy_user = f"{user}-US-rotate"
    url = f"http://{urllib.parse.quote(proxy_user)}:{urllib.parse.quote(pwd)}@{host}:{port}"
    return {"http": url, "https": url}


_MOZ_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
)


def fetch_text(url: str, retries: int = 4, use_us_proxy: bool = False) -> str:
    proxies = _us_proxies() if use_us_proxy else None
    # Many state .gov sites block obvious-bot UA strings; spoof Chrome for
    # proxied fetches.
    headers = {"User-Agent": _MOZ_UA} if use_us_proxy else None
    last = None
    for attempt in range(retries):
        try:
            r = SESSION.get(url, timeout=45, allow_redirects=True, proxies=proxies, headers=headers)
            r.raise_for_status()
            return r.text
        except Exception as e:
            last = e
            time.sleep(max(1.0, 0.5 * (2**attempt)))
    raise RuntimeError(f"fetch failed {url}: {last}")


# ---------------------------------------------------------------------------
# Fail-closed drop accounting
# ---------------------------------------------------------------------------


class _DropTracker:
    """Thread-safe tally of dropped/failed units so a broken scrape exits non-zero.

    Scrapers run in a ThreadPoolExecutor, so every bump takes the lock. A run
    that silently sheds articles, sections or whole states used to still return
    0 (the documented TX/CA SPA-shell collapse: the section-split regex matches
    nothing and the loop just `continue`s). main() now returns non-zero whenever
    anything was dropped, so a broken scrape fails loudly instead of logging
    success over lost primary law. Per-fragment min-length drops inside an
    otherwise productive unit stay on the happy path and are not counted; only a
    whole unit (article/state) yielding nothing, a fetch that failed, or a
    payload missing a REQUIRED field is a hard drop.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.fetch_failures = 0  # a unit/page we could not fetch (record(s) lost)
        self.empty_units = 0  # a fetched unit that parsed to zero sections
        self.state_failures = 0  # a whole state scraper raised / was unconfigured
        self.payload_defects = 0  # a record missing a REQUIRED payload field

    def fetch_failed(self, ctx: str, err: object = "", count: int = 1) -> None:
        with self._lock:
            self.fetch_failures += count
        print(f"  [{ctx}] fetch FAIL (record(s) dropped): {err}")

    def unit_empty(self, ctx: str) -> None:
        with self._lock:
            self.empty_units += 1
        print(f"  [{ctx}] produced 0 sections after fetch (parse drop)")

    def state_failed(self, ctx: str, err: object = "") -> None:
        with self._lock:
            self.state_failures += 1
        print(f"  [{ctx}] state FAIL: {err}")

    def payload_defect(self, ctx: str, missing: tuple[str, ...]) -> None:
        with self._lock:
            self.payload_defects += 1
        print(f"  PAYLOAD DEFECT {ctx}: missing REQUIRED {list(missing)}")

    @property
    def hard_drops(self) -> int:
        return self.fetch_failures + self.empty_units + self.state_failures + self.payload_defects

    def summary(self) -> str:
        return (
            f"fetch_failures={self.fetch_failures} empty_units={self.empty_units} "
            f"state_failures={self.state_failures} payload_defects={self.payload_defects}"
        )


_DROPS = _DropTracker()


# ---------------------------------------------------------------------------
# Record schema (matches embed_and_upsert chunk shape)
# ---------------------------------------------------------------------------


_FULL_STATE_NAME = {
    "al": "Alabama",
    "ak": "Alaska",
    "az": "Arizona",
    "ar": "Arkansas",
    "ca": "California",
    "co": "Colorado",
    "ct": "Connecticut",
    "de": "Delaware",
    "fl": "Florida",
    "ga": "Georgia",
    "hi": "Hawaii",
    "id": "Idaho",
    "il": "Illinois",
    "in": "Indiana",
    "ia": "Iowa",
    "ks": "Kansas",
    "ky": "Kentucky",
    "la": "Louisiana",
    "me": "Maine",
    "md": "Maryland",
    "ma": "Massachusetts",
    "mi": "Michigan",
    "mn": "Minnesota",
    "ms": "Mississippi",
    "mo": "Missouri",
    "mt": "Montana",
    "ne": "Nebraska",
    "nv": "Nevada",
    "nh": "New Hampshire",
    "nj": "New Jersey",
    "nm": "New Mexico",
    "ny": "New York",
    "nc": "North Carolina",
    "nd": "North Dakota",
    "oh": "Ohio",
    "ok": "Oklahoma",
    "or": "Oregon",
    "pa": "Pennsylvania",
    "ri": "Rhode Island",
    "sc": "South Carolina",
    "sd": "South Dakota",
    "tn": "Tennessee",
    "tx": "Texas",
    "ut": "Utah",
    "vt": "Vermont",
    "va": "Virginia",
    "wa": "Washington",
    "wv": "West Virginia",
    "wi": "Wisconsin",
    "wy": "Wyoming",
}

# Citation patterns per state (Bluebook short form)
_CITATION_TMPL = {
    "ca": "Cal. Const. art. {art}, § {sec}",
    "tx": "Tex. Const. art. {art}, § {sec}",
    "ny": "N.Y. Const. art. {art}, § {sec}",
    "fl": "Fla. Const. art. {art}, § {sec}",
    "il": "Ill. Const. art. {art}, § {sec}",
    "pa": "Pa. Const. art. {art}, § {sec}",
    "az": "Ariz. Const. art. {art}, § {sec}",
    "al": "Ala. Const. art. {art}, § {sec}",
    "md": "Md. Const. art. {art}, § {sec}",
    "mn": "Minn. Const. art. {art}, § {sec}",
    # default below if state not in map
}


@dataclass
class Section:
    state: str
    article_id: str
    section_number: str
    section_title: str
    raw_text: str
    source_url: str
    article_title: str = ""
    r2_html_url: str | None = None
    r2_pdf_url: str | None = None
    r2_docx_url: str | None = None
    r2_txt_url: str | None = None


def sha1_hex(s: str) -> str:
    return hashlib.sha1(s.encode()).hexdigest()


def point_id_for(act_id: str, chunk_idx: int, text: str) -> str:
    h = hashlib.md5(f"{act_id}::{chunk_idx}::{sha1_hex(text)[:12]}".encode()).hexdigest()
    return str(uuid.UUID(h))


def title_name_for(state: str) -> str:
    return f"{_FULL_STATE_NAME.get(state, state.upper())} Constitution"


def citation_for(sec: Section) -> str:
    citation_tmpl = _CITATION_TMPL.get(sec.state)
    if not citation_tmpl:
        # generic fallback
        state_abbrev = sec.state.title()
        citation_tmpl = f"{state_abbrev}. Const. art. {{art}}, § {{sec}}"
    return citation_tmpl.format(art=sec.article_id, sec=sec.section_number)


def act_id_for(sec: Section) -> str:
    """The section's stable act_id. Identical on every chunk of that section."""
    return f"SCONST_{sec.state.upper()}_A{sec.article_id}_S{sec.section_number}"


def chunk_body(text: str) -> list[str]:
    """One chunk per section: this repo does not ship the canonical chunker.

    The pipeline's canonical chunker is not published here, so every section
    is emitted whole. That is safe for the large majority of constitution
    sections (a paragraph or two, well under any chunk ceiling) and only
    matters for the handful of long article-length sections, which land in a
    single oversized record instead of several.

    Deliberately NOT wired to the one other chunker in this repo,
    `state_scrapers/vaquill_pipeline/node_to_payload.py::chunk_text`. Its
    header claims parity with the canonical implementation, but it runs on
    different knobs (1000-token target and a 100-token floor, against the
    canonical 512/50) and folds an undersized trailing fragment back into the
    previous chunk, so it splits the same text at different offsets. Chunk
    boundaries feed `point_id_for`, so adopting a chunker that disagrees with
    the one a corpus was built with does not re-chunk that corpus, it
    duplicates it under new ids. A wrong chunker is worse than one chunk.
    """
    if not text:
        return [""]
    return [text]


def chunk_header(
    *,
    title_name: str,
    citation: str,
    section_number: str,
    section_title: str,
    chunk_index: int,
    total_chunks: int,
) -> str:
    """Context prefix prepended to a chunk's embedding text.

    Exported so an OFFLINE re-chunk of already-ingested points (see
    rechunk_state_constitutions.py) produces byte-identical embedding text to a
    fresh ingest. If these two ever diverge, the same section re-chunked by
    different routes gets different point_ids and silently duplicates.

    For a single-chunk section the output is byte-for-byte what the pre-chunking
    ingester emitted, so those sections keep their existing point_id AND their
    existing vector; ``embed_and_upsert --resume`` skips them for free. Most
    constitution sections are a paragraph or two and land here, which is exactly
    why the two lines below must not be restyled.
    """
    part = f" | part {chunk_index + 1} of {total_chunks}" if total_chunks > 1 else ""
    label = section_title or f"Section {section_number}"
    if chunk_index == 0:
        return f"{title_name} | {citation}{part}\n{label}\n\n"
    # Continuations carry a one-line anchor instead of the full descriptor: a
    # chunk pulled from the middle of a 172 KB article is meaningless without
    # knowing which article it is, but repeating the whole descriptor would
    # crowd out the actual content of a 2 KB chunk and make siblings look alike
    # to the ranker.
    return f"{title_name} | {citation}{part} | {label[:110]}\n\n"


# ---------------------------------------------------------------------------
# Amendment/ratification years -- data already sitting in raw_text, unparsed
# ---------------------------------------------------------------------------
# Every Wikisource-sourced state (scrape_wikisource_inline, 37 states) has its
# own official amendment-history annotation transcribed inline: it sits in its
# own paragraph right after the affected section's substantive text and
# before the NEXT "Section N." marker (verified 2026-08-05 against live
# rendered HTML for VA: the note is a <small> tag positioned exactly there),
# so the existing article/section split in scrape_wikisource_inline already
# attributes it to the correct section's raw_text -- it is just never parsed
# out into amendments_count/amendment_years/last_amended_year.
#
# The format is NOT uniform across states. Sampled 37 of 37 Wikisource states
# live 2026-08-05 across three passes (see docs/us-corpus/CORPUS_RICHNESS_TODO.md
# item 1.9 for the full survey): VA ("The amendment ratified November 5, 1996
# and effective January 1, 1997-...") and MD ("(added by Chapter 422, Acts of
# 2006, ratified Nov. 7, 2006; amended by ..., ratified Nov. 2, 2010; ...)")
# both use the literal word "ratified" immediately before each date, verified
# consistent enough to share one pattern. WI ("[As amended Nov. 1982 and
# April 1986]"), AK ("[Amended 1972]"), and MN ("[Amended, November 8,
# 1988]") are each their own bracketed date format. TN ("[As amended: Adopted
# in Convention ..., Approved at general election ..., Proclaimed by
# Governor, ....]", sometimes chaining several such events with "; As
# Amended:") is its own longer bracket format. NC ("(2013-300, s. 1.)")
# embeds the year directly in a trailing session-law citation. CT ("(Sec. 8
# amended in 1982. See Art. XVII of Amendments to the Constitution ...)")
# points to a separate amendments article rather than revising the text in
# place, but the "amended in YEAR" phrase itself is clean and consistent. WY
# ("This section was amended by a resolution adopted by the 1980
# legislature, ratified by a vote of the people at the general election held
# on November 4, 1980, and proclaimed in effect on November 14, 1980.") is
# its own prose sentence, distinct from VA/MD's shorter "ratified DATE"
# phrase. LA ("Amended by Acts 1989, No. 840, S1, approved Oct. 7, 1989, eff.
# Nov. 7, 1989; Acts 2003, No. 1295, ...") chains session-law citations like
# NC/MD. NH ("<dd>Amended 1974 adding sentence to prohibit
# discrimination.</dd>", one `<dd>` per amendment event, several can stack
# under one section) is its own short-form annotation. Two of the five
# bespoke (non-Wikisource) scrapers were sampled too, since they already have
# a working scraper and a real source page: CA ("(Sec. 2 amended June 3,
# 1980, by Prop. 5. Res.Ch. 77, 1978.)", leginfo.legislature.ca.gov) carries
# both the voter-ratification date and the legislature's own resolution
# year. TX ("(Feb. 15, 1876. Amended Nov. 5, 1918.)", tcss.legis.texas.gov)
# prints the ORIGINAL 1876 enactment date bare, with no "added"/"amended"
# keyword, right alongside a real "Amended DATE"/"Added DATE" citation in the
# same parenthetical -- the extractor keys on the keyword specifically so
# 1876 (an enactment date) is excluded, the same way the federal
# Constitution fix excludes 1788 from `amendment_years`.
#
# 26 of the 37 Wikisource states were sampled and found NOT safely extractable, each for a
# specific, checked reason, not left unchecked: KS, DE, HI, IL, MS, NV have
# no per-section annotation of any kind. ID, OH, MT, WA, NM mix several
# notation styles within the same document, most sections undated. AZ, GA,
# SC, WV have only generic prose mentions of "amendment"/"ratified" unrelated
# to tracking any specific section. NJ, OK, NY's apparent hits were Wikisource
# TOC/navigation link text, not real annotations. MO has an "(as amended in
# YEAR, YEAR, ...)" note, but it is a "Source:" line describing the
# PREDECESSOR 1875 constitution's own amendment history before this text was
# carried into the 1945 constitution -- a different fact than "when was the
# CURRENT text last amended," so extracting it under `amendment_years` would
# be reporting the wrong document's history under the right document's
# label; not attempted. OR's actual article text lives on separate per-article
# Wikisource subpages (`Oregon_Constitution/Article_N`), not the main page
# `scrape_wikisource_inline` fetches, so no per-section format could be
# sampled at all from the page this ingester actually scrapes -- flagged
# separately as a possible scraper-coverage gap, not an amendment-format
# question. VT only has a whole-DOCUMENT currency line ("AS ESTABLISHED JULY
# 9, 1793, AND AMENDED THROUGH NOVEMBER 5, 2002"), not per-section dates.
#
# A state absent from _AMENDMENT_YEAR_EXTRACTORS below is left at
# amendments_count=0 deliberately, not attempted -- guessing at a format
# never actually verified would risk reporting confidently wrong amendment
# history, which this corpus's own convention (see the federal Constitution
# fix, item 1.5/1.9) treats as worse than an honest gap. All 37 Wikisource
# states have now been checked at least once, and 2 of the 5 bespoke-scraper
# states (CA/TX) besides. The remaining 3 bespoke states (PA, KY, MI) and the
# 8 states with no registered scraper at all in this file (AL/AR/IA/IN/ME/ND/
# RI/UT -- confirmed against STATE_SCRAPERS below, not assumed from the
# `_WS_INLINE_STATES` comment, which is stale on this point: it still lists
# PA/KY/MI as "missing on Wikisource... TODO" even though all three already
# have their own working bespoke scraper further down this file) are
# genuinely unsampled for this. PA's source is geo-fenced and unreachable
# from environments without the scraper-box proxy; KY/MI were not attempted
# this pass. A genuinely new state here needs its own from-scratch source
# investigation, not an extension of any pattern below -- none of the
# bespoke scrapers share Wikisource's transcription conventions, and CA and
# TX did not even share each other's.

_VA_MD_RATIFIED_RE = re.compile(r"ratified\s+[A-Za-z]+\.?\s+\d{1,2},?\s+((?:19|20)\d{2})")
_WI_BRACKET_RE = re.compile(r"\[As (?:amended|created)[^\]]{0,200}\]")
_AK_BRACKET_RE = re.compile(r"\[Amended (?:19|20)\d{2}\]")
_MN_BRACKET_RE = re.compile(r"\[Amended,? [^\]]{0,60}\]")
_TN_BRACKET_RE = re.compile(r"\[As [Aa]mended:[^\]]{0,800}\]")
_NC_TRAILING_CITE_RE = re.compile(r"\((?:19|20)\d{2}[^()]{0,80}\)\s*$")
_CT_AMENDED_IN_RE = re.compile(r"amended in ((?:19|20)\d{2})", re.IGNORECASE)
_WY_NOTE_RE = re.compile(r"This section was (?:amended|repealed)(?: again)?.{0,400}")
_LA_AMENDED_BY_ACTS_RE = re.compile(r"Amended by Acts.{0,600}")
_NH_AMENDED_RE = re.compile(r"Amended (?:19|20)\d{2}.{0,150}")
_CA_SEC_NOTE_RE = re.compile(r"\(Sec\.\s*[\w.]+\s+(?:added|amended)[^)]{0,150}\)")
# TX prints the ORIGINAL 1876 adoption date bare, with no "added"/"amended"
# keyword, right alongside a real amendment date in the same parenthetical:
# "(Feb. 15, 1876. Amended Nov. 5, 1918.)". Anchoring on the keyword excludes
# 1876 (an enactment date, not an amendment) the same way the federal
# Constitution fix (1.5/1.9) excludes 1788 from amendment_years.
_TX_ADDED_AMENDED_RE = re.compile(
    r"(?i)\b(?:added|amended)\s+[A-Za-z]+\.?\s+\d{1,2},?\s+((?:19|20)\d{2})"
)
# NE trails every article with an "Adopted in YEAR[. Last amended in YEAR.]"
# note (sometimes "Amended in YEAR" without "Last" for a single-amendment
# section). Anchored on the "amended" keyword so the bare "Adopted in 1875"
# enactment date is excluded, same convention as TX/federal above. Verified
# live 2026-08-07 against en.wikisource.org/wiki/Nebraska_Constitution
# (sampled across the ~238 articles this cluster's NE parser produces).
#
# The bespoke scrape_ne (2026-08-08, official nebraskalegislature.gov source
# that replaced this Wikisource text) carries the SAME "amended" keyword but
# drops the word "in" in its own Source-line format ("Amended 1988,
# Initiative Measure No. 403." vs. Wikisource's "Amended in 1988") -- "in"
# made optional so this one extractor covers both sources' phrasing rather
# than forking a second regex for the same state.
_NE_AMENDED_IN_RE = re.compile(r"(?i)\b(?:last\s+)?amended(?:\s+in)?\s+((?:19|20)\d{2})")
# NJ marks each amended provision with "... amended effective MONTH DAY,
# YEAR." (e.g. "Article II, Section I, paragraph 1 amended effective January
# 17, 2006."). Verified live 2026-08-07 against
# en.wikisource.org/wiki/New_Jersey_Constitution_of_1947.
_NJ_AMENDED_EFFECTIVE_RE = re.compile(
    r"(?i)amended effective [A-Za-z]+\.?\s+\d{1,2},?\s+((?:19|20)\d{2})"
)
_YEAR_TOKEN_RE = re.compile(r"\b((?:19|20)\d{2})\b")


def _va_md_amendment_years(raw_text: str) -> list[int]:
    return [int(y) for y in _VA_MD_RATIFIED_RE.findall(raw_text)]


def _years_in_matched_spans(raw_text: str, span_re: re.Pattern[str]) -> list[int]:
    """Every 4-digit year inside each span the given pattern isolates.

    Shared by WI/AK/MN/TN/WY/LA/NH: each state's regex isolates its own
    annotation shape first (a literal `[...]` bracket for some states, a
    bounded prose window like "This section was amended...400 chars" for
    others), so a stray year elsewhere in the section's prose is never swept
    in, and every year token inside that isolated span is a real
    amendment/ratification/effective date in every sample checked for these
    seven states.
    """
    years: list[int] = []
    for span in span_re.findall(raw_text):
        years.extend(int(y) for y in _YEAR_TOKEN_RE.findall(span))
    return years


def _nc_amendment_years(raw_text: str) -> list[int]:
    m = _NC_TRAILING_CITE_RE.search(raw_text)
    if not m:
        return []
    return [int(y) for y in _YEAR_TOKEN_RE.findall(m.group(0))]


def _ct_amendment_years(raw_text: str) -> list[int]:
    return [int(y) for y in _CT_AMENDED_IN_RE.findall(raw_text)]


def _tx_amendment_years(raw_text: str) -> list[int]:
    return [int(y) for y in _TX_ADDED_AMENDED_RE.findall(raw_text)]


def _ne_amendment_years(raw_text: str) -> list[int]:
    return [int(y) for y in _NE_AMENDED_IN_RE.findall(raw_text)]


def _nj_amendment_years(raw_text: str) -> list[int]:
    return [int(y) for y in _NJ_AMENDED_EFFECTIVE_RE.findall(raw_text)]


# PA (bespoke scrape_pa, not Wikisource): every amended section ends with a
# session-law citation chain naming its own amending Joint Resolutions, e.g.
# "(Nov. 6, 1984, P.L.1306, J.R.2; Nov. 7, 1995, 1st Sp.Sess., P.L.1151,
# J.R.1; Nov. 4, 2003, P.L.459, J.R.1)" -- verified live 2026-08-07 against
# legis.state.pa.us Article I. `_clean_pa_section_body` used to delete this
# parenthetical outright (see its docstring); it is now kept inline like
# VA/MD/NC's own amendment notes, so this extractor can isolate it via its
# distinctive "J.R." (Joint Resolution) marker -- unique to PA amendment
# citations, so it will not accidentally sweep in an unrelated year elsewhere
# in the section body.
_PA_AMEND_CITE_RE = re.compile(r"\([^()]*?J\.R\.[^()]*\)", re.DOTALL)


def _pa_amendment_years(raw_text: str) -> list[int]:
    return _years_in_matched_spans(raw_text, _PA_AMEND_CITE_RE)


_AMENDMENT_YEAR_EXTRACTORS = {
    "va": _va_md_amendment_years,
    "md": _va_md_amendment_years,
    "wi": lambda t: _years_in_matched_spans(t, _WI_BRACKET_RE),
    "ak": lambda t: _years_in_matched_spans(t, _AK_BRACKET_RE),
    "mn": lambda t: _years_in_matched_spans(t, _MN_BRACKET_RE),
    "tn": lambda t: _years_in_matched_spans(t, _TN_BRACKET_RE),
    "wy": lambda t: _years_in_matched_spans(t, _WY_NOTE_RE),
    "la": lambda t: _years_in_matched_spans(t, _LA_AMENDED_BY_ACTS_RE),
    "nh": lambda t: _years_in_matched_spans(t, _NH_AMENDED_RE),
    "ca": lambda t: _years_in_matched_spans(t, _CA_SEC_NOTE_RE),
    "nc": _nc_amendment_years,
    "ct": _ct_amendment_years,
    "tx": _tx_amendment_years,
    "ne": _ne_amendment_years,
    "nj": _nj_amendment_years,
    "pa": _pa_amendment_years,
}
# "nv" registered further down, right after _nv_amendment_years is defined
# (its bespoke scraper and extractor live near the other bespoke scrapers,
# well after this dict -- see the assignment next to scrape_nv).


def amendment_years_for(sec: Section) -> tuple[list[int], list[int], int | None]:
    """(raw years found, deduped sorted years, most recent year) for a section.

    `raw` (with duplicates, in source order) is the count of amendment EVENTS
    found -- two ratification notes landing in the same year both count, so
    `amendments_count` does not silently undercount that real edge case. The
    deduped/sorted list is what `amendment_years` (a faceting field) wants.
    """
    extractor = _AMENDMENT_YEAR_EXTRACTORS.get(sec.state)
    if extractor is None:
        return [], [], None
    raw = extractor(sec.raw_text)
    if not raw:
        return [], [], None
    return raw, sorted(set(raw)), max(raw)


def to_chunk_records(sec: Section) -> list[dict]:
    """Build one embeddable record per chunk of a section.

    Everything that describes the SECTION rather than the chunk (act_id,
    citation, act_status, breadcrumb, display_path, sort_key, title identity,
    every r2_* pointer, full_text_sha1) is identical on every chunk and
    unchanged from the pre-chunking record.
    """
    body = sec.raw_text.strip()
    chunks = chunk_body(body)
    total = len(chunks)
    return [_build_one(sec, chunk, idx, total) for idx, chunk in enumerate(chunks)]


def _build_one(sec: Section, chunk: str, chunk_index: int, total_chunks: int) -> dict:
    # `text` is THIS chunk; `full_text` is the whole section, which is what the
    # section-level fields below (word counts, sha1) must keep describing so
    # they stay identical across a section's chunks.
    text = chunk
    full_text = sec.raw_text.strip()
    title_name = title_name_for(sec.state)
    citation = citation_for(sec)
    act_id = act_id_for(sec)
    amend_events, amend_years, last_amended = amendment_years_for(sec)

    text_for_embedding = (
        chunk_header(
            title_name=title_name,
            citation=citation,
            section_number=sec.section_number,
            section_title=sec.section_title,
            chunk_index=chunk_index,
            total_chunks=total_chunks,
        )
        + text
    )
    # Formats available AT SECTION granularity. "html" is NOT one of them: the
    # only HTML we mirror is the whole article/TOC page, which is provenance,
    # not a per-section artifact (see r2_source_url below).
    formats = []
    if sec.r2_pdf_url:
        formats.append("pdf")
    if sec.r2_docx_url:
        formats.append("docx")
    formats.append("txt")

    fields = {
        # Retrieval body. `text` is THIS chunk only. embed_and_upsert falls back
        # to the record's top-level `raw_text` when `metadata.text` is missing,
        # so the live payloads have always carried it; stating it here changes
        # no stored value and makes the record self-describing, which is what
        # lets the offline re-chunk rebuild a Section from a payload alone.
        "text": text,
        "act_id": act_id,
        "corpus_type": "state_constitution",
        "category": "state_constitution",
        "document_type": "constitution",
        "jurisdiction": "US",
        "country_code": "US",
        "state": sec.state,
        "title_name": title_name,
        "title": title_name,
        "top_level_title": f"constitution-{sec.state}",
        "title_code": f"const_{sec.state}",
        "level_classifier": "section",
        "chapter": None,
        "section_number": sec.section_number,
        "section_title": sec.section_title,
        "citation": citation,
        "citation_short": citation,
        "display_label": citation,
        "display_title": sec.section_title or f"Section {sec.section_number}",
        "display_path": f"Article {sec.article_id} / Section {sec.section_number}",
        "breadcrumb": [title_name, f"Article {sec.article_id}", f"Section {sec.section_number}"],
        "sort_key": act_id,
        "act_status": "in_force",
        "renumbered_to": "",
        "transferred_to": "",
        "year": None,
        "chunk_index": chunk_index,
        "total_chunks": total_chunks,
        # `word_count` describes THIS chunk (it is what `text` holds);
        # `document_word_count` describes the whole section so consumers can
        # still reason about it, and R2 holds the unsplit original at
        # r2_txt_url / r2_section_text_url.
        "word_count": len(text.split()),
        "document_word_count": len(full_text.split()),
        "subsection_count": 0,
        "subsection_letters": [],
        "numbered_paragraph_count": 0,
        "amendments_count": len(amend_events),
        "amendment_years": amend_years,
        "last_amended_year": last_amended,
        "cross_references_count": 0,
        "cross_references_usc": [],
        "cross_references_cfr": [],
        "public_laws_count": 0,
        "public_laws_referenced": [],
        # r2_html_url is deliberately NOT emitted. Every scraper here sets
        # sec.r2_html_url to a WHOLE-DOCUMENT page (one wikisource_main.html
        # shared by 37 states, the TOC page for KY, the whole article page for
        # CA/TX/PA), and r2_html_url is FIRST in the body read chain
        # (statutes_us_text._FORMAT_PREFERENCE), so every per-section request
        # resolved to that document. Recorded as provenance instead; the read
        # chain now falls through to the genuinely per-section
        # r2_section_text_url below. Fixing it here covers all six scrapers.
        "r2_source_url": sec.r2_html_url,
        "r2_pdf_url": sec.r2_pdf_url,
        "r2_docx_url": sec.r2_docx_url,
        "r2_xml_url": None,
        "r2_txt_url": sec.r2_txt_url,
        "r2_section_text_url": sec.r2_txt_url,
        "r2_formats_available": formats,
        "source_url": sec.source_url,
        "parent_id": None,
        "raw_node_id": act_id,
        # SECTION-level, so it is seeded from the whole section and stays
        # identical on every chunk (and unchanged for the sections that still
        # produce exactly one chunk).
        "full_text_sha1": sha1_hex(full_text),
    }
    if sec.article_title:
        # Recover the parsed-but-previously-dropped article title (CA/PA/MI
        # "DECLARATION OF RIGHTS", KY per-section heading). Every scraper set
        # Section.article_title but _build_one never shipped it; route it to the
        # canonical `article_name` key instead of discarding it.
        fields["article_name"] = sec.article_title
    # Canonical payload via the shared builder. `r2_txt_url` folds to
    # `r2_section_text_url` (payload_schema.ALIASES); `r2_source_url` is stored
    # as-is (a first-class field, not aliased to `state_html_url`), and the
    # public API's `stateHtmlUrl` response field falls back to it at READ time
    # (StatuteSection.from_payload,
    # statutes_us_source._normalize_row), so the provenance link still
    # surfaces there without this write path needing to rename it.
    # `corpus_type` is retained (the body-read chain keys the
    # whole-document-HTML skip on it for constitutions).
    md, audit = build_payload(fields)
    if audit.missing_required:
        _DROPS.payload_defect(act_id, audit.missing_required)
    return {
        "point_id": point_id_for(act_id, chunk_index, text),
        "text_for_embedding": text_for_embedding,
        "raw_text": text,
        "metadata": md,
    }


# ---------------------------------------------------------------------------
# California
# ---------------------------------------------------------------------------

CA_BASE = "https://leginfo.legislature.ca.gov/faces/codes_displayText.xhtml"
CA_ROMAN = [
    "I",
    "II",
    "III",
    "IIIB",
    "IV",
    "V",
    "VI",
    "VII",
    "VIII",
    "IX",
    "X",
    "XA",
    "XB",
    "XBA",
    "XI",
    "XII",
    "XIII",
    "XIIIA",
    "XIIIB",
    "XIIIC",
    "XIIID",
    "XIV",
    "XV",
    "XVI",
    "XVII",
    "XVIII",
    "XIX",
    "XIXA",
    "XIXB",
    "XIXC",
    "XIXD",
    "XX",
    "XXI",
    "XXII",
    "XXXIV",
    "XXXV",
]


# leginfo requires a SPACE between a lettered article's Roman-numeral prefix
# and its letter suffix (Prop 13's "Article XIII A", not "XIIIA") -- without
# it the endpoint silently serves its Angular SPA shell instead of
# server-rendered content, which the "no SECTION markers" fallback
# (correctly) drops as empty rather than surfacing as a fetch bug. A regex
# split is ambiguous here (a trailing "I" in "VIII"/"XVII" looks like a
# one-letter suffix of a shorter valid Roman numeral, e.g. "VII"+"I"), so
# this is an explicit map of the CA_ROMAN entries that are genuinely
# lettered, built from CA_ROMAN itself. Verified live 2026-08-07: this
# recovered Articles XIII A-D (Prop 13 tax limitation, among the most
# frequently cited provisions in CA constitutional law), X A, X B, and
# XIX A-D, previously silently unreachable. Plain (unlettered) article
# numbers are untouched.
_CA_SPACED_ARTICLE_QUERIES = {
    "IIIB": "III B",
    "XA": "X A",
    "XB": "X B",
    "XBA": "X B A",
    "XIIIA": "XIII A",
    "XIIIB": "XIII B",
    "XIIIC": "XIII C",
    "XIIID": "XIII D",
    "XIXA": "XIX A",
    "XIXB": "XIX B",
    "XIXC": "XIX C",
    "XIXD": "XIX D",
}


def _ca_article_query(art: str) -> str:
    return _CA_SPACED_ARTICLE_QUERIES.get(art, art)


def scrape_ca(r2) -> list[Section]:
    """California Constitution from leginfo.legislature.ca.gov (article-by-article)."""
    out: list[Section] = []
    print(f"\n[CA] {len(CA_ROMAN)} candidate articles")
    for art in CA_ROMAN:
        query_art = quote(_ca_article_query(art))
        url = f"{CA_BASE}?lawCode=CONS&article={query_art}"
        try:
            html = fetch_text(url)
        except Exception as e:
            _DROPS.fetch_failed(f"CA art {art}", e)
            continue
        soup = BeautifulSoup(html, "html.parser")
        # The full code lives in a <div id="manylawsections"> with each section
        # as <span> or <h6> tags. The page is JSF-rendered, look for actual
        # statute markers.
        # On leginfo, the body content is under <div id="manylawsections">
        container = soup.find(id="manylawsections")
        if not container:
            # Fallback: extract all text and parse SECTION markers
            container = soup.find("body") or soup
        body_text = container.get_text("\n", strip=True)
        # Split on SECTION N. or SEC. N. markers
        parts = re.split(r"\n(?:SECTION|SEC\.)\s+(\d+(?:\.\d+)?[A-Z]?)\.\s*", body_text)
        if len(parts) <= 1:
            _DROPS.unit_empty(f"CA art {art} (no SECTION markers)")
            continue
        # Drop the preamble that came before the first SECTION marker
        # (article header / navigation cruft)
        section_pairs = [(parts[i], parts[i + 1]) for i in range(1, len(parts) - 1, 2)]
        # Article title (first text block before SECTION 1)
        article_title = ""
        first_part = parts[0]
        m = re.search(r"ARTICLE\s+[\w\.]+\s+([A-Z][A-Z, ]+?)(?:\n|\[|\s+\(|$)", first_part[:500])
        if m:
            article_title = m.group(1).title().strip()

        # Upload article HTML once
        r2_html_key = f"state_constitutions/ca/source/article_{art}.html"
        put_if_changed(r2, r2_html_key, html.encode("utf-8"), "text/html; charset=utf-8")
        r2_html_url = public_url(r2_html_key)

        for sec_num, sec_text in section_pairs:
            text = re.sub(r"\s+", " ", sec_text).strip()
            if not text or len(text) < 5:
                continue
            sec = Section(
                state="ca",
                article_id=art,
                section_number=sec_num,
                section_title=f"California Constitution Article {art}, Section {sec_num}",
                article_title=article_title,
                raw_text=text,
                source_url=url,
                r2_html_url=r2_html_url,
            )
            # Upload per-section TXT
            r2_txt_key = f"state_constitutions/ca/sections/SCONST_CA_A{art}_S{sec_num}.txt"
            put_if_changed(
                r2, r2_txt_key, sec.raw_text.encode("utf-8"), "text/plain; charset=utf-8"
            )
            sec.r2_txt_url = public_url(r2_txt_key)
            out.append(sec)
        print(f"  [CA art {art}] {len(section_pairs)} sections")
    print(f"[CA] done: {len(out)} sections total")
    return out


# ---------------------------------------------------------------------------
# Texas
# ---------------------------------------------------------------------------

TX_BASE = "https://tcss.legis.texas.gov/resources/CN"


def scrape_tx(r2) -> list[Section]:
    """Texas Constitution from tcss.legis.texas.gov (htm files per article).

    Previously used statutes.capitol.texas.gov which as of 2026 returns a 250 KB
    Angular SPA shell instead of the htm content: the section-split regex yields
    zero matches and the loop `continue`s silently, so no rows land. The
    tcss.legis.texas.gov mirror still serves the original htm output.
    """
    out: list[Section] = []
    # TX Constitution: 17 articles (I-XVII)
    # URL pattern: /Docs/CN/htm/CN.{N}.htm  (where N is 1-17, sometimes with letters)
    for n in range(1, 18):
        url = f"{TX_BASE}/htm/CN.{n}.htm"
        try:
            html = fetch_text(url)
        except Exception as e:
            _DROPS.fetch_failed(f"TX art {n}", e)
            continue
        soup = BeautifulSoup(html, "html.parser")
        body_text = soup.get_text("\n", strip=True)
        # TX uses Sec. N.NN format
        parts = re.split(r"\n(?:Sec\.|SECTION|SEC\.)\s+(\d+(?:[a-z]?(?:-\d+)?))\.\s*", body_text)
        if len(parts) <= 1:
            # The documented capitol.texas.gov SPA-shell failure: the article was
            # fetched but the section-split matched nothing, so it used to drop
            # the whole article silently. Count it so the run fails loudly.
            _DROPS.unit_empty(f"TX art {n} (no SECTION markers)")
            continue
        section_pairs = [(parts[i], parts[i + 1]) for i in range(1, len(parts) - 1, 2)]

        r2_html_key = f"state_constitutions/tx/source/article_{n}.html"
        put_if_changed(r2, r2_html_key, html.encode("utf-8"), "text/html; charset=utf-8")
        r2_html_url = public_url(r2_html_key)

        for sec_num, sec_text in section_pairs:
            text = re.sub(r"\s+", " ", sec_text).strip()
            if not text or len(text) < 5:
                continue
            sec = Section(
                state="tx",
                article_id=str(n),
                section_number=sec_num,
                section_title=f"Texas Constitution Article {n}, Section {sec_num}",
                raw_text=text,
                source_url=url,
                r2_html_url=r2_html_url,
            )
            r2_txt_key = f"state_constitutions/tx/sections/SCONST_TX_A{n}_S{sec_num}.txt"
            put_if_changed(
                r2, r2_txt_key, sec.raw_text.encode("utf-8"), "text/plain; charset=utf-8"
            )
            sec.r2_txt_url = public_url(r2_txt_key)
            out.append(sec)
        print(f"  [TX art {n}] {len(section_pairs)} sections")
    print(f"[TX] done: {len(out)} sections total")
    return out


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Wikisource generic scraper — inline-Section-marker style
# (Works for: VA, NC, WI; the others on Wikisource are stub/article-list pages.)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Section-split bug fix (docs/us-corpus/handoffs/constitutions/HANDOFF_C00_index.md)
# ---------------------------------------------------------------------------
# The generic `\n(?:SECTION|Section|Sec\.)\s+(\d+...)` split only matches a
# section header that literally starts a line with one of those three words.
# Several states use a different convention entirely, so the regex never
# matches and the whole article silently falls back to one section_number="0"
# blob. Same shared-function-plus-override-dict shape as
# `_AMENDMENT_YEAR_EXTRACTORS` above: the generic path is untouched for every
# state not listed here.
#
# WV: sections are numbered "{article}-{section}." (e.g. "1-1.", "1-2."), no
# SECTION/Section/Sec. keyword at all. Verified live 2026-08-07 against the
# real page -- confirmed live 2026-08-07 the WV Wikisource transcription only
# covers Articles I-V (the page ends mid-document after Article V, Section 1,
# with no subpages linked); this regex fixes the split for what IS
# transcribed, it does not create the missing Articles VI-XIV.
_WV_SECTION_RE = re.compile(r"\n\d+-(\d+[A-Za-z]?)\.\s*")
# WI: headers are "<title phrase>. SECTION N.\n[edit]\n<body>" -- the keyword
# is mid-line (preceded by the section's own title phrase on the same line),
# not at the start of a line like the generic pattern requires. Verified live
# 2026-08-07: 172 of 172 "SECTION N" occurrences in the real page are
# followed immediately by a newline (the [edit] boilerplate), with zero
# false-positive collisions against inline body-text references.
_WI_SECTION_RE = re.compile(r"SECTION\s+(\d+(?:\.\d+)?[A-Za-z]?)\.\s*\n")
# SD: sections use "§N." not SECTION/Section/Sec. Verified live 2026-08-07.
_SD_SECTION_RE = re.compile(r"\n§\s*(\d+(?:\.\d+)?[A-Za-z]?)\.\s*")
# VT: has no "SECTION" markers at all -- Chapter I's numbered items are
# labelled "Article N." and Chapter II's are labelled "§N.". Both map onto
# this corpus's "section" concept (VT's numbered items are the leaf unit,
# same role SECTION plays elsewhere); one pattern covers both stylings.
_VT_SECTION_RE = re.compile(r"\n(?:Article|§)\s*(\d+[A-Za-z]?)\.\s*")
# KS: sections are "§ N: Title.\nBody..." -- a colon after the number, not a
# period, and the keyword is "§" not SECTION/Section/Sec. Verified live
# 2026-08-07 against en.wikisource.org/wiki/Kansas_Constitution: 100% of KS's
# 66 live points were section_number="0" blobs before this fix; the real page
# splits cleanly into 15 articles / real sections once the colon form is
# recognized.
_KS_SECTION_RE = re.compile(r"\n§\s*(\d+(?:\.\d+)?[A-Za-z]?):\s*")
# MD: two conventions on the SAME page. Most articles use "SECTION 1." for
# their first section then "SEC. 2.", "SEC. 3.", ... (all-caps abbreviation,
# period) for the rest -- the generic pattern's "Sec\." alternative is title
# case only and never matches "SEC.", so every article after its first
# section fell back to a single merged/blank blob. Separately, Maryland's
# Declaration of Rights is not "Article I" at all -- it is captured by the
# article-level split as a bare arabic "1" and its 46 items are labelled
# "Art. 1.", "Art. 2.", ... (a third convention). One case-insensitive
# pattern covers all three ("SECTION", "SEC", "Sec", "Art", with or without a
# trailing period). Verified live 2026-08-07: 21 of 23 articles split
# cleanly under this pattern (340 real sections, up from 160); the remaining
# two are Article X ("Vacant (repealed...)", a genuine one-line repeal
# notice, not a bug) and Article XI-I (a two-section supplementary Baltimore
# article using bare "1." / "2." markers with no keyword at all, left as a
# documented residual gap rather than risking a bare-number pattern that
# could collide with numbered-list body text elsewhere on the page).
_MD_SECTION_RE = re.compile(
    r"\n(?:SECTION|SEC|Sec|Art)\.?\s+(\d+(?:\.\d+)?[A-Za-z]?)\.?\s*", re.IGNORECASE
)

# GA: sections are "SECTION <roman>." (not an arabic number) one level below
# Article, with a further Paragraph level nested inside that this corpus's
# flat Article/Section model does not represent separately -- Paragraphs stay
# folded into the Section body, same as every other state's sub-section
# structure (subsection letters, etc.) that isn't split out on its own.
# Verified live 2026-08-07 against the corrected slug (see the _WS_INLINE_STATES
# comment above `"ga"`): 58 real sections across 11 articles.
_GA_SECTION_RE = re.compile(r"\nSECTION\s+([IVXLC]+)\.\s*")

# OK: most articles use the generic plain "Section N." form (238 of 346 live
# points before this fix already matched it correctly -- confirmed live
# 2026-08-07), but a minority repeat the ARTICLE's own Roman numeral inside
# every section header instead: Article X ("Section X-1: Fiscal year.",
# "Section X-2: ...") and Article XIV ("SECTION XIV-1\nBanking department.").
# The generic pattern's `\d+` never matches past the leading "X-"/"XIV-", so
# those two articles fell straight to the section_number="0" fallback (3 of
# OK's 8 live S0 blobs). Rather than a second override entry, one pattern
# handles both conventions: the "[IVXLC]+-" prefix is OPTIONAL, so it is a
# strict superset of the plain form and does not regress the 238 already-
# working sections. Verified live 2026-08-07 against
# en.wikisource.org/wiki/Constitution_of_Oklahoma. Two more OK S0 blobs are
# genuinely unsectioned in this transcription, not a format the split regex
# could recognize -- Article XXIV ("Constitutional Amendments") is continuous
# prose with no "Section" label anywhere, an incomplete transcription (the
# official Constitution's Article 24 has numbered Sections 2-9 that never
# made it into this page at all); Article XXVII's body is the one-line
# repeal notice "Sections 1-11 repealed by State Question No. 563...". Left
# as legitimate lone S0 records, re-verified live 2026-08-07. Article XXIX
# was ALSO previously assumed genuinely unsectioned here -- that was wrong:
# its live S0 point actually stored Wikisource's own trailing
# "Schedule"/"Sources" citation-and-license boilerplate, not article text,
# because the article split had no heading after XXIX to stop at. Fixed via
# _ok_trim_footer in _BODY_PREP_OVERRIDES (see its docstring), not this
# regex -- once the footer is trimmed before the split runs, XXIX is
# confirmed to have zero actual content transcribed under it (a real but
# stub-empty article on this page), so it correctly still lands at S0, just
# without the boilerplate contamination.
_OK_SECTION_RE = re.compile(
    r"\n(?:SECTION|Section)\s+(?:[IVXLC]+-)?(\d+(?:\.\d+)?[A-Za-z]?)\.?:?\s*"
)

# NY: only the FIRST section of every article uses "Section 1." (matches the
# generic pattern); every subsequent section uses "§N." with no "SECTION"/
# "Sec." keyword at all -- the generic pattern never matches these, so the
# rest of every article (Sections 2+) was swallowed into Section 1's body as
# one giant blob (up to 64 embed chunks for Article VI). Verified live
# 2026-08-07 against en.wikisource.org/wiki/New_York_Constitution_as_of_2004:
# this one pattern (matching "Section N." OR "§N.", single capture group)
# correctly splits all 20 articles (201 sections total), including
# hyphenated-letter section numbers ("§5-a.", "§36-a.", "§36-c.", "§2-a.",
# "§10-a."). Known residual: Article XX's last split absorbs the page's
# trailing Wikisource public-domain-notice boilerplate ("§ 313.6(C)(2)...")
# as a bogus final "section" -- same harmless footer-boilerplate class
# already accepted for OK's Article XXIV/XXIX (see _OK_SECTION_RE), not
# distinguishable from a real marker by this pattern.
_NY_SECTION_RE = re.compile(r"\n(?:Section\s+|§\s*)(\d+(?:\.\d+)?(?:-[a-zA-Z])?)\.?\s*")

# DE captions every section "§ N. Caption text." (the section symbol, not the
# words SECTION/Section/Sec.), and for most sections this caption line is
# immediately followed by a redundant spelled-out "Section N. <body>"
# restatement, which is why DE was never registered here: the generic
# SECTION/Section/Sec. split already happened to catch the restatement. Two
# of DE's 17 articles have exactly one section each with NO such restatement
# -- Article XII's sole section was wholly repealed by session law (no
# substantive text to restate) and Article XIV's sole section (the officers'
# oath) is stated directly with no "Section 1." lead-in -- so for those two
# the "§ N." caption is the ONLY marker that exists, and the generic split
# (which does not recognize "§" at all) falls straight to the whole-article
# section_number="0" fallback. Verified live 2026-08-07 against
# en.wikisource.org/wiki/Constitution_of_Delaware_(2023): splitting on "§ N."
# directly (ignoring the "Section N." restatement entirely, since it only
# ever repeats the SAME section rather than introducing a new one) is a
# strict, lossless superset of the generic split -- every one of DE's 17
# articles produces the same or MORE real sections (194 -> 206 statewide),
# with Article XII and XIV specifically going from 0 to their correct single
# "§ 1." each. The `(?:\A|\n)` alternation (rather than a bare leading `\n`)
# is required because `art_split_re`'s own trailing `\n` is consumed as the
# article-header delimiter, so a section marker that is the very FIRST thing
# in an article's body -- exactly XII's and XIV's situation, each having
# only one section -- would otherwise never have a preceding newline to
# match against. The trailing `\.` is optional because 2 of DE's 206 "§ N"
# captions (§8, §18 in Article II) are missing the period Wikisource uses
# everywhere else.
_DE_SECTION_RE = re.compile(r"(?:\A|\n)§\s*(\d+(?:\.\d+)?[A-Za-z]?)\.?\s*")

_SECTION_SPLIT_OVERRIDES: dict[str, re.Pattern[str]] = {
    # "wv" removed 2026-08-07: WV no longer runs scrape_wikisource_inline
    # (see the _WS_INLINE_STATES comment). _WV_SECTION_RE is now used
    # directly by scrape_wv below, on the official home.wvlegislature.gov
    # source, not through this override dict.
    "wi": _WI_SECTION_RE,
    "sd": _SD_SECTION_RE,
    "vt": _VT_SECTION_RE,
    # "ks" removed 2026-08-08: KS no longer runs scrape_wikisource_inline
    # (see the _WS_INLINE_STATES comment above). _KS_SECTION_RE encoded the
    # Wikisource-era colon form "§ N:"; scrape_ks below uses its own
    # official-source pattern ("§ N." with a period) directly, not through
    # this override dict. _KS_SECTION_RE is now orphaned, kept for its
    # comment's documentary value only.
    # "md" removed 2026-08-08: MD no longer runs scrape_wikisource_inline
    # (see the _WS_INLINE_STATES comment). scrape_md below (the official
    # mgaleg.maryland.gov GetNext/GetPrevious walk API) doesn't need a
    # section-split regex at all -- the site already returns one section per
    # walk step. _MD_SECTION_RE is now orphaned (kept, unused, for its
    # comment's documentary value only).
    # AK shares SD's "§N." convention exactly (no SECTION/Section/Sec.
    # keyword). Verified live 2026-08-07: 47 of 77 live AK points were
    # section_number="0" blobs before this fix. One residual S0 point is
    # legitimate, not a miss: Article 14 ("Apportionment Schedule
    # (repealed)") -- its entire body is the single line "Repealed by 1998
    # Ballot Measure No. 3.", no "§" marker anywhere because no substantive
    # text survives to subdivide. Verified live 2026-08-07.
    "ak": _SD_SECTION_RE,
    # CT's main (spelled-ordinal) articles use "SEC.N." with NO space between
    # the abbreviation and the number -- the generic pattern's "Sec\." branch
    # requires `\s+` right after, so it never matches. A minority of the
    # plain-Roman-numeral AMENDMENT articles (added 1992-2018) instead number
    # their added/replacement text "Sec. N." -- title case, WITH a period and
    # a space, e.g. "Sec. 18. The amount of general budget..." (AMENDXXVIII),
    # "Sec. 1. Section 25 of article fourth...\nSec. 2. Subsection a..."
    # (AMENDXXX, two real sections), "Sec. 19. ..." (AMENDXXXII, AMENDXXXIII).
    # This form was missing from this override (the file-level generic
    # default DOES include it as "Sec\.", but this CT-specific override,
    # introduced to handle "SEC.N.", never carried it over) -- so these 4
    # amendment articles fell straight to the whole-article section_number="0"
    # fallback despite containing real, explicitly-numbered "Sec. N." markers.
    # Verified live 2026-08-07 against en.wikisource.org/wiki/Constitution_of_Connecticut:
    # adding this one alternative recovers AMENDXXX's 2 sections and gives
    # AMENDXXVIII/AMENDXXXII/AMENDXXXIII their correct single section numbers
    # instead of 0; re-running the split across all 47 of CT's articles (14
    # main + 33 amendments) with this pattern changes none of the other 43.
    # See `_ct_split_articles` below for why CT also needs an article-level
    # override.
    "ct": re.compile(r"\n(?:SEC\.|SECTION|Section|Sec\.)\s*(\d+(?:\.\d+)?[A-Za-z]?)\.?\s*"),
    "ga": _GA_SECTION_RE,
    "ok": _OK_SECTION_RE,
    "ny": _NY_SECTION_RE,
    "de": _DE_SECTION_RE,
}

# VT is also structured as CHAPTER (top level) > Article/§ (leaf), not a flat
# ARTICLE list like most states -- the generic art-level split (which only
# recognizes ARTICLE/Article) happens to already match VT's Chapter-I-nested
# "Article N." headers directly, but that misses Chapter II entirely (its
# leaf items are "§N.", not "Article N.") and produces a flat, wrong
# article_id space. Override the article-level split to key on CHAPTER
# instead, verified live 2026-08-07: VT has exactly two chapters.
_VT_ARTICLE_RE = re.compile(r"\nCHAPTER\s+([IVXLC]+)[\.\:]?(?:[ \t][^\n]*)?\n")

# MD: the generic article-split's hyphen-suffix group only accepts a
# Roman-numeral letter (IVXLC) after the hyphen, so Maryland's lettered
# Article XI-A..XI-I sub-articles (only XI-C and XI-I coincidentally match,
# since C and I are themselves Roman letters) silently merge into
# neighboring articles' bodies -- verified live 2026-08-07: "Article XI"
# ends up 24,078 chars and contains three separate SECTION-1..SEC-N runs
# (real Art. XI, then XI-A, then XI-B) that collide on the same act_id, an
# active data-loss bug, not merely "unsplit". MD's only lettered articles
# are the XI-series, so accepting any single uppercase letter after the
# hyphen (not just IVXLC) is safe here; this is an MD-only override, the
# shared generic pattern used by every other Wikisource state is untouched.
_MD_ARTICLE_RE = re.compile(
    r"\n(?:ARTICLE|Article)\s+([IVXLC\d]+(?:-[A-Z])?)[\.\:]?(?:[ \t][^\n]*)?\n"
)

_ART_SPLIT_OVERRIDES: dict[str, re.Pattern[str]] = {
    "vt": _VT_ARTICLE_RE,
    # "md" removed 2026-08-08: see the _SECTION_SPLIT_OVERRIDES "md" comment
    # above -- same reason, _MD_ARTICLE_RE is now orphaned.
}

# A state's PRIMARY section-split convention (its _SECTION_SPLIT_OVERRIDES
# entry if any, else the generic SECTION/Section/Sec. pattern) sometimes
# finds nothing in exactly one or two articles that use a wholly different,
# rare convention not worth promoting to that state's primary pattern (since
# doing so risks colliding with body text elsewhere in the document -- see
# the MD comment below). This fallback is tried ONLY when the primary split
# already found zero matches in a given article, so it can never regress an
# already-correctly-splitting article; see the call site in
# scrape_wikisource_inline.
#
# MD Article XI-I ("City of Baltimore - Industrial Financing Loans"), and --
# once the article-level fix above stops merging them away -- XI-G and XI-H
# too, use bare "1." / "2." paragraph markers with NO SECTION/SEC/Sec/Art
# keyword at all. Not folded into _MD_SECTION_RE itself: that regex's own
# comment already flagged the collision risk against MD's other working
# articles' lettered sub-item prose ("(a)... (b)... (c)..."), so a bare
# numeral pattern belongs here, gated to fire only as a last resort.
# Verified live 2026-08-07: of MD's 30 real articles (post article-level
# fix), only Article X (genuinely no markers, correctly stays S0) and
# XI-G/XI-H/XI-I ever reach the zero-match branch, so this cannot regress
# any already-working article.
_MD_BARE_NUMERAL_FALLBACK_RE = re.compile(r"\n(\d+[A-Za-z]?)\.\s+")

# SD Article XXII ("Compact with the United States") and WA Article XXVI
# ("Compact With The United States") are each that state's federally-
# mandated Enabling Act ordinance -- unlike every other article in either
# document, it carries NO "SECTION"/"Section"/"Sec."/"§" keyword anywhere.
# Its four clauses are introduced by the spelled ordinals "First.",
# "Second.", "Third.", "Fourth." instead, so the state's own normal
# section-split regex (SD's §-pattern, WA's generic SECTION/Section/Sec.
# pattern) matches nothing inside this one article and the whole thing
# falls to the section_number="0" whole-article fallback. Verified live
# 2026-08-07 against en.wikisource.org/wiki/Constitution_of_South_Dakota
# (Article XXII, 4 of 4 clauses) and en.wikisource.org/wiki/Washington_State_Constitution
# (Article XXVI, 4 of 4 clauses). Gating this as a fallback (not unioned
# directly into either state's primary pattern) matters: SD's Article XXVI
# (a DIFFERENT article from the compact) separately contains its own
# embedded "First./Second./Third." enumeration INSIDE an already
# §-numbered section -- unioning this pattern into _SD_SECTION_RE directly
# was tried and reverted, since it wrongly re-split that already-correct
# section (23 -> 27 "sections", confirmed live 2026-08-07).
_COMPACT_ORDINANCE_RE = re.compile(r"\n(First|Second|Third|Fourth)\.\s*")

_SECTION_SPLIT_FALLBACK_OVERRIDES: dict[str, re.Pattern[str]] = {
    # "md" removed 2026-08-08: see the _SECTION_SPLIT_OVERRIDES "md" comment
    # above -- same reason, _MD_BARE_NUMERAL_FALLBACK_RE is now orphaned.
    "sd": _COMPACT_ORDINANCE_RE,
    # "wa" removed 2026-08-07: WA no longer runs scrape_wikisource_inline
    # (see the _WS_INLINE_STATES comment). _COMPACT_ORDINANCE_RE is now
    # used directly inside scrape_wa below for the same Article XXVI.
}

# CT's 14 substantive articles are headed "ARTICLE FIRST:", "ARTICLE SECOND.",
# ... (spelled-out ordinals, not Roman/arabic numerals), so the generic
# art-level regex (`[IVXLC\d]+`) never matches them at all. What it DOES
# match instead is the 33 numbered amendment articles appended after Article
# Fourteenth ("ARTICLE I.", "ARTICLE II.", ... using plain Roman numerals) --
# so the live pre-fix data is not just collapsed, it is ENTIRELY the
# amendments, with all 14 real articles silently discarded into the
# unsplit preamble text. Verified live 2026-08-07 against
# en.wikisource.org/wiki/Constitution_of_Connecticut. Fix: a dedicated
# two-pass split -- spelled ordinals first (mapped to Roman numerals I-XIV
# for act_id continuity with every other state), then the remaining tail
# split again on the plain-Roman amendment headings (article_id prefixed
# "AMEND" so it can never collide with a main article's numeral).
_CT_ORDINALS = {
    "FIRST": "I",
    "SECOND": "II",
    "THIRD": "III",
    "FOURTH": "IV",
    "FIFTH": "V",
    "SIXTH": "VI",
    "SEVENTH": "VII",
    "EIGHTH": "VIII",
    "NINTH": "IX",
    "TENTH": "X",
    "ELEVENTH": "XI",
    "TWELFTH": "XII",
    "THIRTEENTH": "XIII",
    "FOURTEENTH": "XIV",
}
_CT_MAIN_ARTICLE_RE = re.compile(r"\nARTICLE\s+(" + "|".join(_CT_ORDINALS) + r")\.?\*?\s*[^\n]*\n")
_CT_AMEND_ARTICLE_RE = re.compile(r"\nARTICLE\s+([IVXLC]+)\.\s*\n")


def _ct_split_articles(body_text: str) -> list[tuple[str, str]]:
    main_parts = _CT_MAIN_ARTICLE_RE.split(body_text)
    if len(main_parts) <= 1:
        return []
    main_iter = [
        [_CT_ORDINALS[main_parts[i]], main_parts[i + 1]] for i in range(1, len(main_parts) - 1, 2)
    ]
    # main_parts[-1] is "rest of the last main article" AND the whole
    # amendments block undivided (the amendment headings never matched
    # _CT_MAIN_ARTICLE_RE), so split it again and re-trim the last main
    # article's body to end where the amendments actually begin.
    tail = main_parts[-1]
    amend_parts = _CT_AMEND_ARTICLE_RE.split(tail)
    main_iter[-1][1] = amend_parts[0]
    amend_iter = [
        (f"AMEND{amend_parts[i]}", amend_parts[i + 1]) for i in range(1, len(amend_parts) - 1, 2)
    ]
    return [(a, b) for a, b in main_iter] + amend_iter


_ART_SPLIT_FN_OVERRIDES: dict[str, Callable[[str], list[tuple[str, str]]]] = {
    "ct": _ct_split_articles,
}


def _sd_skip_toc(body_text: str) -> str:
    """SD's Wikisource page front-loads a ~107K-char table of contents (with
    dotted page-number leaders) plus a schedule/index appendix, both of which
    repeat the same "Article N" heading text the real body uses. The real
    content begins at the SECOND occurrence of "Article I" (the TOC lists it
    once, the real article follows later); everything before that is
    front-matter, not constitutional text. Verified live 2026-08-07: exactly
    two "Article I" occurrences, the first in the TOC, the second real.
    """
    first = body_text.find("\nArticle I\n")
    if first == -1:
        return body_text
    second = body_text.find("\nArticle I\n", first + 1)
    return body_text[second:] if second != -1 else body_text


_OK_FOOTER_RE = re.compile(r"\n(?:Schedule|Sources)\n")


def _ok_trim_footer(body_text: str) -> str:
    """OK's page's last three headings are "Article XXIX - Ethics
    Commission" (zero content transcribed), "Schedule" (zero content), then
    "Sources" (an OSCN link plus the page's own {{PD-EdictGov}} public-domain
    notice -- Wikisource's own citation/license section, not constitutional
    text). Neither "Schedule" nor "Sources" matches the ARTICLE/Article
    split, so with no terminator after Article XXIX the article split
    previously ran to end-of-document, capturing this boilerplate as Article
    XXIX's body (the live SCONST_OK_AXXIX_S0 contamination -- corrects this
    module's earlier claim that Article XXIX was "genuinely unsectioned"
    without checking what text the live S0 point actually stored). Cut the
    page at the Schedule/Sources tail before the article split runs, so the
    page's own citation/license section can never be captured as a fake
    final article's body. Verified live 2026-08-07 against
    en.wikisource.org/wiki/Constitution_of_Oklahoma raw wikitext. No-op if
    the marker is absent.
    """
    m = _OK_FOOTER_RE.search(body_text)
    return body_text[: m.start()] if m else body_text


_BODY_PREP_OVERRIDES: dict[str, Callable[[str], str]] = {
    "sd": _sd_skip_toc,
    "ok": _ok_trim_footer,
}


# ---------------------------------------------------------------------------
# Full custom parsers -- states whose document nesting the generic 2-level
# (Article > Section) walk cannot represent at all, even with an
# article/section regex swap. Cluster 3 handoff
# (docs/us-corpus/handoffs/constitutions/HANDOFF_C03_cluster3_ma_nj.md),
# verified live against en.wikisource.org 2026-08-07. Each function fully
# replaces the generic Article+Section walk for its one state; every state
# not in `_CUSTOM_PARSERS` below is unaffected.
# ---------------------------------------------------------------------------


def _emit_section(
    state: str,
    r2,
    r2_html_url: str | None,
    url: str,
    article_id: str,
    section_number: str,
    raw_text: str,
    section_title: str | None = None,
    r2_pdf_url: str | None = None,
    r2_docx_url: str | None = None,
    article_title: str = "",
) -> Section | None:
    """Shared per-section finalize step (whitespace normalize, min-length
    filter, R2 txt mirror) for every constitution scraper -- HTML or PDF
    sourced, generic-path or custom-parser. Every scraper should route its
    section construction through this function rather than building a
    `Section` inline, so the min-length filter, R2 mirror key convention, and
    default title format can never drift between states.

    `r2_pdf_url` (new): pass this instead of `r2_html_url` for PDF-sourced
    states (NM/WA and most of the Tier-2/PDF-format states in C07) -- exactly
    one of the two should be set, matching whichever `Section.r2_*_url` field
    the source document actually mirrors to.

    `r2_docx_url` (new, CO): same idea for DOCX-sourced states -- mirrors the
    `r2_pdf_url` handling exactly (a whole-document provenance pointer, same
    per-section-repeated-URL precedent already accepted for PDF sources).

    `article_title` (new): pass the article's own name/heading text when the
    source provides one (e.g. "Bill of Rights", "Declaration of Rights") --
    it flows through to the `article_name` payload field in `_build_one`.
    Several already-shipped scrapers (CA/PA/MI/KY) already set this; WV/NV/
    NM/WA currently don't capture it (their ARTICLE_RE regexes don't retain
    the title text in a group) -- worth adding when convenient, not a
    required retrofit. New C07 states should capture it from the start where
    the source's ARTICLE heading includes a name, since it is materially
    richer metadata than a bare roman numeral and costs nothing once the
    regex already has to match past that text anyway.
    """
    text = re.sub(r"\s+", " ", raw_text).strip()
    if not text or len(text) < 5:
        return None
    sec = Section(
        state=state,
        article_id=article_id,
        section_number=section_number,
        section_title=section_title
        or f"{state.upper()} Const., Article {article_id}, Section {section_number}",
        raw_text=text,
        source_url=url,
        r2_html_url=r2_html_url,
        r2_pdf_url=r2_pdf_url,
        r2_docx_url=r2_docx_url,
        article_title=article_title,
    )
    r2_txt_key = f"state_constitutions/{state}/sections/SCONST_{state.upper()}_A{article_id}_S{section_number}.txt"
    put_if_changed(r2, r2_txt_key, sec.raw_text.encode("utf-8"), "text/plain; charset=utf-8")
    sec.r2_txt_url = public_url(r2_txt_key)
    return sec


def _emit_sections_from_articles(
    state: str,
    r2,
    url: str,
    art_iter: list[tuple[str, str]],
    section_re: re.Pattern,
    r2_html_url: str | None = None,
    r2_pdf_url: str | None = None,
    r2_docx_url: str | None = None,
    fallback_section_res: tuple[re.Pattern, ...] = (),
    article_titles: dict[str, str] | None = None,
) -> list[Section]:
    """Shared driver for the common two-level (Article > Section) shape used
    by CA/TX/PA/KY/MI/WV/NV/NM/WA and expected to cover most of the 41 C07
    states. Splits each article's body on `section_re`, emits one `Section`
    per match via `_emit_section`, and falls back to a single whole-article
    Section (section_number="0") when no section markers are found -- same
    behavior that was previously duplicated near-verbatim inside
    scrape_wv/scrape_nv/scrape_nm/scrape_wa (right down to the R2 key
    convention and the "no sections found" fallback), which is exactly the
    kind of copy-pasted boilerplate that let the same "article-split eats its
    own trailing newline" bug get independently rediscovered three times
    (WV, NM, WA) instead of fixed once.

    `fallback_section_res`: tried in order if `section_re` finds nothing in
    an article's body, before giving up to the whole-article fallback (e.g.
    WA's `_COMPACT_ORDINANCE_RE` for its one article with no "SECTION"
    keyword at all).

    `article_titles`: optional {article_id: title} map for states whose
    article-split regex captures a title (see `_emit_section`'s
    `article_title` docstring) -- pass a dict comprehension built alongside
    `art_iter`, not a change to `art_iter`'s own tuple shape.

    A repeated section number within one article (NV: a currently-effective
    and an already-adopted-but-not-yet-effective version printed side by
    side under the same number) gets a "-v2"/"-v3" suffix rather than
    silently overwriting the first -- both are real, separately citable
    text, not a scrape artifact.

    States with genuinely deeper nesting than Article > Section (NJ's
    Article > Subpart > Paragraph, MA's Part > Chapter > Section > Article,
    GA's expected Article > Section > Paragraph) should NOT force their
    shape through this function -- write a custom loop calling
    `_emit_section` directly at the leaf, the same way `_parse_nj`/
    `_parse_ma` already do. This function is for the shape that fits, not a
    dumping ground for every shape with a workaround bolted on.
    """
    out: list[Section] = []
    article_titles = article_titles or {}
    for art_id, art_body in art_iter:
        sec_parts = section_re.split("\n" + art_body)
        for fallback_re in fallback_section_res:
            if len(sec_parts) > 1:
                break
            sec_parts = fallback_re.split("\n" + art_body)
        if len(sec_parts) <= 1:
            sec = _emit_section(
                state,
                r2,
                r2_html_url,
                url,
                art_id,
                "0",
                art_body,
                section_title=f"{state.upper()} Const., Article {art_id}",
                r2_pdf_url=r2_pdf_url,
                r2_docx_url=r2_docx_url,
                article_title=article_titles.get(art_id, ""),
            )
            if sec:
                out.append(sec)
            continue

        sec_iter = [(sec_parts[k], sec_parts[k + 1]) for k in range(1, len(sec_parts) - 1, 2)]
        seen_nums: dict[str, int] = {}
        for sec_num_raw, sec_text_raw in sec_iter:
            seen_nums[sec_num_raw] = seen_nums.get(sec_num_raw, 0) + 1
            occurrence = seen_nums[sec_num_raw]
            sec_num = sec_num_raw if occurrence == 1 else f"{sec_num_raw}-v{occurrence}"
            sec = _emit_section(
                state,
                r2,
                r2_html_url,
                url,
                art_id,
                sec_num,
                sec_text_raw,
                r2_pdf_url=r2_pdf_url,
                r2_docx_url=r2_docx_url,
                article_title=article_titles.get(art_id, ""),
            )
            if sec:
                out.append(sec)

    if not out:
        _DROPS.unit_empty(f"{state.upper()} (0 sections after parse)")
    print(f"[{state.upper()}] done: {len(out)} sections across {len(art_iter)} articles")
    return out


# NE: every unit is a single "Article I-1" / "Article XVII-7" heading with NO
# further subdivision -- the hyphenated suffix IS the section number, printed
# in the SAME heading as the article. The generic split's
# `(?:[\.\-][IVXLC\d]+)?` group already captures the whole "I-1" as one
# article_id (matches the audit's SCONST_NE_AXI-2_S0-style act_ids), then
# finds no further Section marker inside and falls to the S0 fallback -- not
# a content-loss bug (each unit really is one atomic section, fully
# preserved), but a citation-correctness bug: "Neb. Const. art. I, § 1" reads
# as art_id="I-1", section="0" instead of art_id="I", section="1". Verified
# live 2026-08-07: 238 of 238 top-level headings use this exact
# "Article <roman>-<digit><letter?>" shape, zero exceptions.
_NE_ART_SEC_RE = re.compile(r"\n(?:ARTICLE|Article)\s+([IVXLC]+)-(\d+[A-Za-z]?)\n")


def _parse_ne(state: str, body_text: str, url: str, r2_html_url: str, r2) -> list[Section]:
    matches = list(_NE_ART_SEC_RE.finditer(body_text))
    out: list[Section] = []
    for i, m in enumerate(matches):
        art_id, sec_num = m.group(1), m.group(2)
        start = m.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(body_text)
        sec = _emit_section(state, r2, r2_html_url, url, art_id, sec_num, body_text[start:end])
        if sec:
            out.append(sec)
    return out


# NJ: 11 top-level ARTICLEs split fine with the generic regex, but NJ has no
# "SECTION"/"Sec." keyword anywhere -- paragraphs are bare "1.", "2.", "3."
# at the start of a line (matches the audit: 106/106 points S0). A handful of
# articles (confirmed live 2026-08-07: Article II) additionally nest a
# "SECTION <roman>" sub-level between the Article and the numbered
# paragraphs, restarting paragraph numbering at 1 per sub-section -- so a
# flat bare-number split alone would collide act_ids across those
# sub-sections (Article II § I ¶1 and § II ¶1 would both mint
# SCONST_NJ_AII_S1). Sub-sections are folded into a composite article_id
# ("II.I") when present; articles without them keep the plain article_id.
_NJ_SUBSECTION_RE = re.compile(r"\n(?:SECTION|Section)\s+([IVXLC]+)\s*\n")
_NJ_PARA_RE = re.compile(r"\n(\d+(?:\.\d+)?[A-Za-z]?)\.\s+")


def _parse_nj(state: str, body_text: str, url: str, r2_html_url: str, r2) -> list[Section]:
    art_split_re = re.compile(
        r"\n(?:ARTICLE|Article)\s+([IVXLC\d]+(?:[\.\-][IVXLC\d]+)?(?:[A-Z])?)[\.\:]?(?:[ \t][^\n]*)?\n"
    )
    art_parts = art_split_re.split(body_text)
    if len(art_parts) <= 1:
        return []
    art_pairs = [(art_parts[i], art_parts[i + 1]) for i in range(1, len(art_parts) - 1, 2)]
    out: list[Section] = []
    for art_id, art_body in art_pairs:
        # The official njleg.state.nj.us source (unlike the retired Wikisource
        # page) prints each article's own title as a short caption line
        # immediately after the "Article N" heading, before the first
        # numbered paragraph -- e.g. "Article I\nRights and Privileges\n1. ...".
        # Verified live 2026-08-08 across all 11 articles. This is exactly
        # the text `_NJ_PARA_RE.split` below already discards as "content
        # before the first match" -- capturing it here is not a behavior
        # change, just keeping what was already being thrown away.
        article_title = ""
        title_m = re.match(r"[^\S\n]*([^\n]{1,90})\n", art_body)
        if title_m and not title_m.group(1).strip()[:1].isdigit():
            article_title = title_m.group(1).strip()
        sub_parts = _NJ_SUBSECTION_RE.split(art_body)
        groups = (
            [
                (f"{art_id}.{sub_parts[i]}", sub_parts[i + 1])
                for i in range(1, len(sub_parts) - 1, 2)
            ]
            if len(sub_parts) > 1
            else [(art_id, art_body)]
        )
        for composite_art, group_body in groups:
            # `_NJ_SUBSECTION_RE.split` above consumes its own trailing "\n"
            # as the "SECTION <roman>" delimiter, so a SECTION-nested
            # composite article's paragraph "1." starts with nothing before
            # it to match `_NJ_PARA_RE`'s required leading "\n" -- the same
            # "article-split eats its own trailing newline" bug class
            # documented in HANDOFF_C07 for WV/NM/WA, just one level deeper.
            # Confirmed live 2026-08-08: every SECTION-nested composite
            # article (II.*, IV.*, V.*, VI.*, VII.*, VIII.*, XI.*) was
            # silently dropping its own paragraph 1 into the discarded
            # pre-match prefix before this fix -- for a composite with only
            # one paragraph (e.g. VI.I), that meant the whole subsection
            # vanished. Prepending "\n" is a no-op for the non-nested branch
            # (`group_body is art_body`, already preceded by the article's
            # own title-caption line, see above), so this is a strict
            # superset fix, not NJ-subsection-specific.
            pp = _NJ_PARA_RE.split("\n" + group_body)
            para_pairs = [(pp[i], pp[i + 1]) for i in range(1, len(pp) - 1, 2)]
            for para_num, para_body in para_pairs:
                sec = _emit_section(
                    state,
                    r2,
                    r2_html_url,
                    url,
                    composite_art,
                    para_num,
                    para_body,
                    article_title=article_title,
                )
                if sec:
                    out.append(sec)
    return out


# NH: no "SECTION" level at all -- the document is Part First / Part Second,
# each directly containing numbered "Article N." / "[Art.] N." / "[Art.] N-a."
# units (NH's "Article" plays the role this corpus's schema calls "section").
# Verified live 2026-08-07: Part First -> 43 articles, Part Second -> 108
# articles, matching NH's real Bill of Rights (~43 incl. lettered amendments)
# and Frame of Government sizes. article_id is set to "1"/"2" (Part First /
# Part Second); section_number is the article number as printed.
_NH_PART_RE = re.compile(r"\n(Part First|Part Second)\s*(?:—|-|:)?[^\n]*\n")
_NH_ART_RE = re.compile(r"\n(?:\[Art\.\]|Article)\s+(\d+(?:-[a-z])?)\.\s*")
_NH_PART_LABEL = {"Part First": "1", "Part Second": "2"}


def _parse_nh(state: str, body_text: str, url: str, r2_html_url: str, r2) -> list[Section]:
    parts = _NH_PART_RE.split(body_text)
    if len(parts) <= 1:
        return []
    part_pairs = [(parts[i], parts[i + 1]) for i in range(1, len(parts) - 1, 2)]
    out: list[Section] = []
    for part_name, part_body in part_pairs:
        part_id = _NH_PART_LABEL.get(part_name, part_name)
        ap = _NH_ART_RE.split(part_body)
        art_pairs = [(ap[i], ap[i + 1]) for i in range(1, len(ap) - 1, 2)]
        for art_num, art_body in art_pairs:
            sec = _emit_section(state, r2, r2_html_url, url, part_id, art_num, art_body)
            if sec:
                out.append(sec)
    return out


# MA: the fetched page repeats "Part the First"/"Part the Second" TWICE --
# once as a TOC heading (an unnumbered one-line summary per article, no real
# text) and once for the real content; only the SECOND occurrence starts real
# text (verified live 2026-08-07). Part the First (Declaration of Rights) is
# a flat list of 30 Articles ("Art. I." then bare "II.", "III.", ...); Part
# the Second (Frame of Government) nests Chapter > Section > Article three
# levels deep, with several chapters (III, IV, VI) having no Section level at
# all, and Chapter V printing its first Section with no "Section I" marker
# (only "Section II" onward is explicit). Wikisource also re-prints "Chapter
# N" as a running header before EVERY nested Section, not once per chapter,
# so a naive split on Chapter markers alone fragments one chapter into
# several pieces (fixed below by tracking current chapter/section state
# across a single combined scan instead of a nested two-pass split).
#
# Immediately after Chapter VI, the page continues straight into "Articles
# of Amendment" with NO further Chapter/Section marker to close Chapter VI
# off -- verified live 2026-08-07 that an earlier version of this parser
# swallowed the entire 22.6 KB Amendments text into Chapter VI's last
# article as a result. The amendments themselves use their OWN numbering
# (Art. I. for the first, then "Art. <arabic>." for the rest, not the bare
# continuation style Part the First uses) and are NOT parsed by this pass --
# Part the Second's body is truncated at the "Articles of Amendment" marker
# so Chapter VI's real content stays clean; capturing the ~120 amendments as
# their own records is flagged as follow-up work, not attempted here.
_MA_PART_RE = re.compile(r"\n(Part the First|Part the Second)\s*\n")
_MA_PART1_ART_RE = re.compile(r"\n(?:Art\.\s+)?([IVXL]+)\.\s+")
_MA_CHAPTER_RE = re.compile(r"\nChapter\s+([IVXL]+)\s*\n")
_MA_SECTION_RE = re.compile(r"\nSection\s+([IVXL]+)\s*\n")
_MA_AMENDMENTS_MARKER_RE = re.compile(r"\nArticles of Amendment\s*\n")
_MA_PART_LABEL = {"Part the First": "1", "Part the Second": "2"}


def _parse_ma_part2(state: str, part_body: str, url: str, r2_html_url: str, r2) -> list[Section]:
    """Chapter > Section > Article, tracked via one combined scan (see the
    module note above for why a nested split doesn't work here)."""
    amend_m = _MA_AMENDMENTS_MARKER_RE.search(part_body)
    if amend_m:
        part_body = part_body[: amend_m.start()]

    marks: list[tuple[int, int, str, str]] = []
    for m in _MA_CHAPTER_RE.finditer(part_body):
        marks.append((m.start(), m.end(), "chap", m.group(1)))
    for m in _MA_SECTION_RE.finditer(part_body):
        marks.append((m.start(), m.end(), "sec", m.group(1)))
    for m in _MA_PART1_ART_RE.finditer(part_body):
        marks.append((m.start(), m.end(), "art", m.group(1)))
    marks.sort(key=lambda t: t[0])

    # (chapter, section, article) -> list of body fragments. A regex false-
    # split mid-article (e.g. an inline side-note that happens to look like a
    # marker) can produce more than one fragment for the same key; merge them
    # instead of letting a later fragment silently overwrite an earlier one.
    grouped: dict[tuple[str, str, str], list[str]] = {}
    order: list[tuple[str, str, str]] = []
    cur_chap: str | None = None
    cur_sec = "I"  # Chapter V's own first Section has no explicit marker.
    for i, (_start, end, kind, val) in enumerate(marks):
        if kind == "chap":
            cur_chap = val
            cur_sec = "I"
            continue
        if kind == "sec":
            cur_sec = val
            continue
        if cur_chap is None:
            continue
        body_start = end
        body_end = marks[i + 1][0] if i + 1 < len(marks) else len(part_body)
        frag = part_body[body_start:body_end]
        key = (cur_chap, cur_sec, val)
        if key not in grouped:
            grouped[key] = []
            order.append(key)
        grouped[key].append(frag)

    out: list[Section] = []
    for chap_id, sec_id, art_num in order:
        merged_body = "\n".join(grouped[(chap_id, sec_id, art_num)])
        sec = _emit_section(
            state,
            r2,
            r2_html_url,
            url,
            f"2.{chap_id}.{sec_id}",
            art_num,
            merged_body,
            section_title=f"Mass. Const. Pt. 2, Ch. {chap_id}, Sec. {sec_id}, Art. {art_num}",
        )
        if sec:
            out.append(sec)
    return out


def _parse_ma(state: str, body_text: str, url: str, r2_html_url: str, r2) -> list[Section]:
    first_idxs = [m.start() for m in re.finditer(r"Part the First", body_text)]
    if len(first_idxs) < 2:
        return []
    real_text = "\n" + body_text[first_idxs[1] :]
    parts = _MA_PART_RE.split(real_text)
    if len(parts) <= 1:
        return []
    part_pairs = [(parts[i], parts[i + 1]) for i in range(1, len(parts) - 1, 2)]
    out: list[Section] = []
    for part_name, part_body in part_pairs:
        part_id = _MA_PART_LABEL.get(part_name, part_name)
        if part_name == "Part the First":
            ap = _MA_PART1_ART_RE.split(part_body)
            art_pairs = [(ap[i], ap[i + 1]) for i in range(1, len(ap) - 1, 2)]
            for art_num, art_body in art_pairs:
                sec = _emit_section(state, r2, r2_html_url, url, part_id, art_num, art_body)
                if sec:
                    out.append(sec)
            continue
        out.extend(_parse_ma_part2(state, part_body, url, r2_html_url, r2))
    return out


# ---------------------------------------------------------------------------
# Oregon Wikisource override — per-state override dispatched below, mirrors
# the amendment_years_for(sec) precedent (one shared function for the
# majority, a small per-state override for a confirmed-different minority).
#
# Confirmed live 2026-08-07 (fetched en.wikisource.org/wiki/Oregon_Constitution
# directly): unlike every other _WS_INLINE_STATES member, Oregon's Wikisource
# page is NOT a single page with inline Article + Section text. It is a
# ~3.2 KB portal/index page (its `mw-content-ltr` body, the candidate
# scrape_wikisource_inline's own "pick the longer candidate" logic already
# selects) whose actual content is a preamble plus a list of links, one per
# article, to SEPARATE Wikisource subpages
# (`/wiki/Oregon_Constitution/Article_I`, `.../Article_VII_(Amended)`,
# `.../Article_XI-F(1)`, etc.). That is why OR collapsed to a single
# SCONST_OR_AI_S0 blob: scrape_wikisource_inline's Article-split regex had
# nothing to split (the real text lives on 36 other pages it never fetches),
# so it fell through to "no Article splits -> whole body as one article",
# then to the Section-split fallback, emitting the ~3.2 KB index page itself
# (Wikisource boilerplate and all) as one contaminated "section".
#
# This is a fetch-strategy problem, not a regex problem, so the fix is a
# dedicated function rather than an entry in a section-split regex dict: walk
# the index page for article links, fetch every subpage, then split each
# subpage's body on the SAME "Section N" convention scrape_wikisource_inline
# already uses (confirmed live: OR's subpages do use plain "Section 1",
# "Section 2", ... headers, no period). Dispatched for "or" alone via the
# STATE_SCRAPERS override right after the auto-register loop below;
# scrape_wikisource_inline itself is untouched, so every other
# _WS_INLINE_STATES member keeps its existing behavior unchanged.
# ---------------------------------------------------------------------------

_OR_ARTICLE_LINK_RE = re.compile(r"^/wiki/Oregon_Constitution/Article_(.+)$")

# Some short/repealed OR articles share ONE Wikisource subpage (fetching
# Article_XI-B and Article_XI-C both land on the same "Articles XI-B and
# XI-C" page). Without isolating, both articles' points get the FULL
# combined text (XI-B's point would also contain XI-C's, and vice versa).
# Confirmed live 2026-08-07: the combined page's body has both prev/next nav
# headers ("Article XI-A", "Article XI-D") and real content headers
# ("Article XI-B", "Article XI-C") in the same "Article <id>\n" form; slicing
# from the target article's own content header to the next header after it
# (or EOF) isolates just that article's text. Only fires when the page
# actually contains 2+ such headers; single-article pages (verified against
# Article_I and Article_XVIII, whose only headers are prev/next nav to a
# DIFFERENT article than themselves) fall through unchanged.
_OR_ARTICLE_HEADER_RE = re.compile(r"\nArticle\s+([IVXLC]+(?:-[A-Z])?)\n")


def _or_isolate_article_segment(body_text: str, art_id: str) -> str:
    matches = list(_OR_ARTICLE_HEADER_RE.finditer("\n" + body_text))
    if len(matches) < 2:
        return body_text
    for i, m in enumerate(matches):
        if m.group(1) != art_id:
            continue
        start = m.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(body_text) + 1
        return ("\n" + body_text)[start:end].strip()
    return body_text


def _or_article_id_from_href(href: str) -> str | None:
    m = _OR_ARTICLE_LINK_RE.match(href)
    if not m:
        return None
    import urllib.parse

    tail = urllib.parse.unquote(m.group(1))
    return tail.replace("_(", "-").replace("(", "-").replace(")", "")


# ---------------------------------------------------------------------------
# Louisiana Wikisource override -- same family of bug as Oregon (content
# split across separate Wikisource pages) but the main page is not a pure
# link index: it carries real inline text for Articles I-III itself, plus a
# "Versions"-style paginated nav table (pipe-separated "Article IV - Article
# V | Article VI | ..." links) that repeats atop the main page AND every
# Part_N subpage. Confirmed live 2026-08-07
# (en.wikisource.org/wiki/Louisiana_State_Constitution_(1974)): that nav
# table's short link fragments ("Article IV - Article V", "Article VI", ...)
# themselves match the generic ARTICLE-split regex, minting spurious
# near-empty article_ids (IV/VI/VII/VIII/X, 1 char of body each) ahead of the
# real Articles I-III -- this is the source of LA's live SCONST_LA_AIV_S0
# contamination (an "IV" blob whose "text" is the nav table's boilerplate,
# not constitutional text). The real Article IV-XIV text lives on
# Part_2..Part_7 (one subpage covers 1-3 articles each; article boundaries
# never split across a Part_N page, verified live). Fix: trim each fetched
# page (main + every Part_N) down to where its REAL first ARTICLE heading
# (or, on the main page only, the "PREAMBLE" line) starts, discarding
# whatever nav/TOC boilerplate precedes it, then run the SAME
# article/section split scrape_wikisource_inline already uses on each
# trimmed page independently. Dispatched for "la" alone via the
# STATE_SCRAPERS override below; scrape_wikisource_inline itself is
# untouched, so no other _WS_INLINE_STATES member is affected.
# ---------------------------------------------------------------------------

_LA_MAIN_SLUG = "Louisiana_State_Constitution_(1974)"
_LA_PART_SLUGS = [f"{_LA_MAIN_SLUG}/Part_{n}" for n in range(2, 8)]
_LA_REAL_ARTICLE_HEAD_RE = re.compile(r"\nARTICLE\s+[IVXLC]+\.\s+[A-Z]")
_LA_PREAMBLE_RE = re.compile(r"\nPREAMBLE\n")


def _la_trim_nav(body_text: str) -> str:
    """Cut everything before the real content start (see module note above).

    On the main page the real content begins at "PREAMBLE"; on Part_N pages
    (no preamble) it begins at the first genuine "ARTICLE N. TITLE" heading.
    Take whichever of the two is found and starts earliest, so a page with
    both (only the main page does) is not over-trimmed.
    """
    starts = []
    m = _LA_PREAMBLE_RE.search(body_text)
    if m:
        starts.append(m.start())
    m = _LA_REAL_ARTICLE_HEAD_RE.search(body_text)
    if m:
        starts.append(m.start())
    if not starts:
        return body_text
    return body_text[min(starts) :]


# ---------------------------------------------------------------------------
# Pennsylvania — legis.state.pa.us serves each Article as its own URL at
# /WU01/LI/LI/CT/HTM/00/00.{N:03d}..HTM  (note the literal double-dot before
# .HTM). 11 articles (I-XI). Each article's body contains all its §-numbered
# sections inline. Geo-restricted; needs US proxy.
# ---------------------------------------------------------------------------

PA_ARTICLE_URL_TMPL = "https://www.legis.state.pa.us/WU01/LI/LI/CT/HTM/00/00.{n:03d}..HTM"

# Roman numeral mapping for article IDs 1..11
_PA_ROMAN = ["I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", "X", "XI"]

# Section-body marker: "§ N.  Section heading.\n  body text..."
# We use a non-greedy capture of everything until the NEXT § marker (or
# end-of-text). Internal `00c1NNs` anchor markers and amendment notes
# (lines starting with "(Date,") are scrubbed downstream.
_PA_SECTION_RE = re.compile(
    r"§\s*(\d+(?:\.\d+)?[A-Za-z]?)\.\s+([^\n]+?)\n([\s\S]*?)(?=\n§\s*\d|\Z)",
    re.MULTILINE,
)


def _clean_pa_section_body(raw: str) -> str:
    """Strip internal anchor markers like '00c103s' and excessive whitespace
    from a PA section body.

    The amendment-lineage parenthetical this used to strip here (e.g.
    "(Nov. 6, 1984, P.L.1306, J.R.2; Nov. 7, 1995, ... J.R.1)") is now KEPT
    inline, same convention VA/MD/NC/LA already use for their own amendment
    notes -- see `_pa_amendment_years` below, which extracts real years back
    out of exactly this text via `amendment_years_for`. Discarding it here
    would silently re-empty that extractor (the "extracted-then-discarded"
    pattern the corpus ingest skill documents); this note is genuinely a per-
    section amendment record, not noise.
    """
    # Drop anchor lines like 00c103s, 00c106v
    raw = re.sub(r"\n\s*00[a-zA-Z0-9]+s?\s*\n", "\n", raw)
    raw = re.sub(r"\n\s*00[a-zA-Z0-9]+s?\s*$", "", raw)
    raw = re.sub(r"\s+", " ", raw).strip()
    return raw


def scrape_pa(r2) -> list[Section]:
    out: list[Section] = []
    for n, art_id in enumerate(_PA_ROMAN, start=1):
        url = PA_ARTICLE_URL_TMPL.format(n=n)
        try:
            html = fetch_text(url, use_us_proxy=True)
        except Exception as e:
            _DROPS.fetch_failed(f"PA art {art_id}", e)
            continue

        r2_html_key = f"state_constitutions/pa/source/article_{art_id}.html"
        put_if_changed(r2, r2_html_key, html.encode("utf-8"), "text/html; charset=utf-8")
        r2_html_url = public_url(r2_html_key)

        soup = BeautifulSoup(html, "html.parser")
        body_text = soup.get_text("\n", strip=True)

        # Discover the article title (e.g. "DECLARATION OF RIGHTS" for Art I).
        # It appears as a line right after the line "ARTICLE I" / "ARTICLE II".
        art_title = ""
        m_at = re.search(rf"\nARTICLE\s+{re.escape(art_id)}\s*\n([^\n]+?)\n", body_text)
        if m_at:
            art_title = m_at.group(1).strip().title()

        sec_iter = list(_PA_SECTION_RE.finditer(body_text))
        count_this_art = 0
        for sm in sec_iter:
            sec_num = sm.group(1).strip()
            sec_head = re.sub(r"\s+", " ", sm.group(2)).strip().rstrip(".")
            sec_body = _clean_pa_section_body(sm.group(3))
            if len(sec_body) < 20:
                continue
            sec = Section(
                state="pa",
                article_id=art_id,
                section_number=sec_num,
                section_title=f"Pa. Const. art. {art_id}, § {sec_num}. {sec_head}",
                article_title=art_title,
                raw_text=sec_body,
                source_url=url,
                r2_html_url=r2_html_url,
            )
            r2_txt_key = f"state_constitutions/pa/sections/SCONST_PA_A{art_id}_S{sec_num}.txt"
            put_if_changed(
                r2, r2_txt_key, sec.raw_text.encode("utf-8"), "text/plain; charset=utf-8"
            )
            sec.r2_txt_url = public_url(r2_txt_key)
            out.append(sec)
            count_this_art += 1
        if count_this_art == 0:
            _DROPS.unit_empty(f"PA art {art_id} (no sections parsed)")
        print(f"  [PA art {art_id}] {count_this_art} sections")
    print(f"[PA] done: {len(out)} sections total")
    return out


# ---------------------------------------------------------------------------
# Kentucky — apps.legislature.ky.gov hosts the constitution as a TOC plus
# per-section sub-pages identified by ?rsn=N. KY's constitution is mostly
# flat (Bill of Rights then numbered sections 1-263+; no formal Articles).
# We assign all sections to Article "I" and use the actual section number.
# ---------------------------------------------------------------------------

KY_CONST_TOC = "https://apps.legislature.ky.gov/law/constitution"
KY_BASE = "https://apps.legislature.ky.gov"


def scrape_ky(r2) -> list[Section]:
    out: list[Section] = []
    try:
        toc_html = fetch_text(KY_CONST_TOC, use_us_proxy=True)
    except Exception as e:
        _DROPS.fetch_failed("KY TOC", e)
        return out

    soup = BeautifulSoup(toc_html, "html.parser")
    # Mirror TOC HTML
    r2_html_key = "state_constitutions/ky/source/toc.html"
    put_if_changed(r2, r2_html_key, toc_html.encode("utf-8"), "text/html; charset=utf-8")
    toc_html_url = public_url(r2_html_key)

    section_specs: list[tuple[str, str, str]] = []  # (sec_id, sec_title, url)
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/Law/Constitution/Constitution/ViewConstitution" not in href:
            continue
        text = a.get_text(strip=True)
        m = re.match(r"Section\s+(\d+[A-Za-z]?)\s*[.\-…]\s*(.*)", text)
        if not m:
            continue
        sec_num = m.group(1)
        sec_title = m.group(2).strip().rstrip(".")
        full_url = href if href.startswith("http") else f"{KY_BASE}{href}"
        section_specs.append((sec_num, sec_title, full_url))

    print(f"  [KY] discovered {len(section_specs)} sections")
    if not section_specs:
        _DROPS.unit_empty("KY (no section links discovered)")
        return out

    # Crawl section pages in parallel (8 workers — KY site is slow with proxy)
    def _fetch_one(spec):
        sec_num, sec_title, url = spec
        try:
            html = fetch_text(url, use_us_proxy=True)
        except Exception:
            return None
        body = BeautifulSoup(html, "html.parser")
        # The section text lives in the main content panel. Heuristic: find
        # all <div>/<p> elements with substantial text inside the body. The
        # actual section text appears after the heading "Section N - Title".
        main = (
            body.find("main")
            or body.find("div", id="MainContent")
            or body.find("div", class_=re.compile("content", re.I))
            or body
        )
        text = main.get_text("\n", strip=True)
        # Strip the long site-wide nav prefix that appears before the actual
        # section header. Find "Section N" anchor and slice from there.
        anchor = re.search(rf"Section\s+{re.escape(sec_num)}\s*[.\-…]", text)
        if anchor:
            text = text[anchor.start() :]
        # Trim footer (everything after "Text as Ratified" if present, or
        # cut at "Source:" or "© ")
        for trail in ["\n© ", "\nPrint this page", "\nReturn to top"]:
            idx = text.find(trail)
            if idx > 0:
                text = text[:idx]
                break
        text = re.sub(r"\s+", " ", text).strip()
        return (sec_num, sec_title, url, text, html)

    with cf.ThreadPoolExecutor(max_workers=8) as ex:
        results = list(ex.map(_fetch_one, section_specs))

    fetched = sum(1 for r in results if r is not None)
    n_failed = len(section_specs) - fetched
    if n_failed:
        # Each None is a section page whose fetch exhausted its retries: that
        # many sections were lost, not skipped.
        _DROPS.fetch_failed(
            f"KY ({n_failed} of {len(section_specs)} section pages)",
            "proxy/network",
            count=n_failed,
        )
    print(f"  [KY] fetched {fetched} / {len(section_specs)} section pages")

    for result in results:
        if result is None:
            continue
        sec_num, sec_title, url, text, _raw_html = result
        if len(text) < 30:
            continue
        sec = Section(
            state="ky",
            article_id="I",
            section_number=sec_num,
            section_title=f"Ky. Const. § {sec_num}",
            article_title=sec_title,
            raw_text=text,
            source_url=url,
            r2_html_url=toc_html_url,
        )
        r2_txt_key = f"state_constitutions/ky/sections/SCONST_KY_A_I_S{sec_num}.txt"
        put_if_changed(r2, r2_txt_key, sec.raw_text.encode("utf-8"), "text/plain; charset=utf-8")
        sec.r2_txt_url = public_url(r2_txt_key)
        out.append(sec)
    if not out:
        _DROPS.unit_empty("KY (0 sections after fetch)")
    print(f"[KY] done: {len(out)} sections")
    return out


# ---------------------------------------------------------------------------
# Michigan — legislature.mi.gov serves the full constitution as a 2.1 MB PDF
# at /documents/publications/constitution.pdf. We download it, extract text
# with PyMuPDF (see _pdf_to_text), and split by "ARTICLE I", "ARTICLE II",
# ... headers and nested "§ N" markers.
# Geo-restricted; needs US proxy.
# ---------------------------------------------------------------------------

MI_CONST_PDF = "https://www.legislature.mi.gov/documents/publications/constitution.pdf"

_MI_ARTICLE_RE = re.compile(
    r"\n\s*ARTICLE\s+([IVXL]+)\s*\n([^\n]+?)\n",
    re.IGNORECASE,
)
_MI_SECTION_RE = re.compile(
    r"§\s*(\d+[A-Za-z]?)\.?\s+(.*?)(?=\n§\s*\d|\Z)",
    re.MULTILINE | re.DOTALL,
)


def _fetch_bytes_proxy(url: str, retries: int = 3) -> bytes | None:
    proxies = _us_proxies()
    headers = {"User-Agent": _MOZ_UA}
    for attempt in range(retries):
        try:
            r = SESSION.get(
                url, timeout=120, allow_redirects=True, proxies=proxies, headers=headers
            )
            r.raise_for_status()
            return r.content
        except Exception:
            if attempt == retries - 1:
                return None
            time.sleep(2.0)
    return None


def _fetch_pdf_bytes_resilient(url: str) -> bytes | None:
    """Like `_fetch_bytes_proxy`, but treats a 200 response that is NOT
    actually a PDF (checked via the `%PDF` magic bytes) as a failure rather
    than a silent success.

    Internally this escalated to a commercial scraping service on failure.
    That tier is not part of this repo (it needs a paid API key and adds
    nothing reproducible), so the magic-byte check is the whole difference
    from `_fetch_bytes_proxy` here. A soft block therefore surfaces as a
    `_DROPS.fetch_failed` for that state rather than as zero silently
    ingested sections, which is the outcome the check exists for.

    Added for C07 batch 6: `_fetch_bytes_proxy` alone previously counted a
    200 response as success unconditionally, but iga.in.gov (IN) started
    returning its own React app shell (200, `text/html`, ~700 bytes) for
    this exact PDF path partway through this batch's own local testing on
    2026-08-08 -- a soft block that looks nothing like the 403/timeout the
    plain proxy path already handles, so it would otherwise have silently
    ingested zero real content while still reporting success up the call
    chain. The scraping service's residential pool did not clear it either in the same
    session; if this still fails when the batch is actually run on the
    scraper box, that is a genuine fetch failure worth flagging, not a bug
    in this helper.
    """
    b = _fetch_bytes_proxy(url)
    if b and b[:4] == b"%PDF":
        return b
    if b:
        print(f"  ! {url}: 200 but not a PDF ({len(b)} bytes), treating as a block")
    return None


def _pdf_to_text(pdf_bytes: bytes) -> str:
    """Extract a PDF's text layer, page by page, newlines preserved.

    Thin wrapper around the shared scripts.lib.pdf_extract.pdf_to_text
    (PyMuPDF-based) that fits this file's existing print-and-empty-string
    failure convention -- every call site here already treats an empty
    return as the failure signal via _DROPS.unit_empty, so a malformed or
    scanned PDF is reported as a dropped unit for THAT state instead of
    aborting the whole multi-state run.

    A missing PyMuPDF install is deliberately re-raised rather than folded
    into that convention: it is not a per-document problem, it is a broken
    environment that would otherwise silently empty out every PDF-sourced
    state (MI, NM, WA, WI, IN, LA, AR, TN, WY) while the run still reported
    success.
    """
    try:
        return pdf_to_text(pdf_bytes)
    except PdfExtractionUnavailable:
        raise
    except Exception as e:
        print(f"  ! pdf extract failed: {e}")
        return ""


def scrape_mi(r2) -> list[Section]:
    out: list[Section] = []
    pdf_bytes = _fetch_bytes_proxy(MI_CONST_PDF)
    if not pdf_bytes:
        _DROPS.fetch_failed("MI constitution PDF", "download failed")
        return out
    # R2-mirror the PDF
    r2_pdf_key = "state_constitutions/mi/source/mi_constitution.pdf"
    put_if_changed(r2, r2_pdf_key, pdf_bytes, "application/pdf")
    r2_pdf_url = public_url(r2_pdf_key)

    text = _pdf_to_text(pdf_bytes)
    if not text:
        _DROPS.unit_empty("MI (PDF text extraction empty)")
        return out

    # Find article boundaries
    art_matches = list(_MI_ARTICLE_RE.finditer(text))
    if not art_matches:
        _DROPS.unit_empty(f"MI (no ARTICLE markers in {len(text)} chars)")
        return out

    # The document opens with a full table of contents listing all 12
    # articles (each as its own "ARTICLE N" heading, title only, no section
    # bodies) before the real 12 articles begin -- verified live 2026-08-07:
    # 24 total "ARTICLE N" matches, exactly the 12 real ids twice each. Keep
    # only the longest body per article_id (the TOC copy's body is short --
    # just the next heading immediately follows -- the real article's body
    # is the whole article), the same duplicate-heading resolution already
    # used for the article-level split in scrape_wikisource_inline.
    raw_articles: dict[str, tuple[str, str]] = {}
    for i, m in enumerate(art_matches):
        art_id = m.group(1).strip()
        art_title = re.sub(r"\s+", " ", m.group(2)).strip()
        start = m.end()
        end = art_matches[i + 1].start() if i + 1 < len(art_matches) else len(text)
        art_body = text[start:end]
        if art_id not in raw_articles or len(art_body) > len(raw_articles[art_id][1]):
            raw_articles[art_id] = (art_title, art_body)

    for art_id, (art_title, art_body) in raw_articles.items():
        sec_iter = list(_MI_SECTION_RE.finditer(art_body))
        if not sec_iter:
            # Whole article as one chunk
            body_clean = re.sub(r"\s+", " ", art_body).strip()
            if len(body_clean) >= 100:
                sec = Section(
                    state="mi",
                    article_id=art_id,
                    section_number="0",
                    section_title=f"Mich. Const. art. {art_id}",
                    article_title=art_title,
                    raw_text=body_clean,
                    source_url=MI_CONST_PDF,
                    r2_pdf_url=r2_pdf_url,
                )
                out.append(sec)
            continue

        # Each article opens with a table-of-contents-style preview listing
        # every section's title only ("§2. Equal protection; discrimination.
        # §3. ...") immediately before the real numbered sections -- MI's TOC
        # uses the SAME "§ N" marker style as the real headers (unlike NM/NV,
        # there is no distinct keyword to exclude it by), so every TOC entry
        # and its real counterpart both match _MI_SECTION_RE under the same
        # section_number. Verified live 2026-08-07 against
        # legislature.mi.gov's constitution PDF: the TOC entry's "body" is
        # just the title (the next match immediately follows), while the
        # real section's body is the substantive text -- keep only the
        # longest body per (article, section_number), the same
        # duplicate-heading resolution already used for the article-level
        # split elsewhere in this file.
        best_by_num: dict[str, tuple[str, str]] = {}
        for sm in sec_iter:
            sec_num = sm.group(1).strip()
            sec_body = re.sub(r"\s+", " ", sm.group(2)).strip()
            if len(sec_body) < 30:
                continue
            if sec_num not in best_by_num or len(sec_body) > len(best_by_num[sec_num][1]):
                best_by_num[sec_num] = (sec_num, sec_body)

        for sec_num, sec_body in best_by_num.values():
            sec = Section(
                state="mi",
                article_id=art_id,
                section_number=sec_num,
                section_title=f"Mich. Const. art. {art_id}, § {sec_num}",
                article_title=art_title,
                raw_text=sec_body,
                source_url=MI_CONST_PDF,
                r2_pdf_url=r2_pdf_url,
            )
            r2_txt_key = f"state_constitutions/mi/sections/SCONST_MI_A{art_id}_S{sec_num}.txt"
            put_if_changed(
                r2, r2_txt_key, sec.raw_text.encode("utf-8"), "text/plain; charset=utf-8"
            )
            sec.r2_txt_url = public_url(r2_txt_key)
            out.append(sec)
    if not out:
        _DROPS.unit_empty("MI (0 sections after PDF parse)")
    print(f"[MI] done: {len(out)} sections from PDF ({len(text)} chars extracted)")
    return out


# ---------------------------------------------------------------------------
# West Virginia -- replaces the Wikisource source (see _WS_INLINE_STATES'
# former "wv" entry and _WV_SECTION_RE above), which only ever transcribed
# Articles I-V (48 sections) before this fix; the page ended mid-document
# with no subpages linked, so no regex fix could recover the missing
# Articles VI-XIV. home.wvlegislature.gov/constitution-of-west-virginia/ is
# the WV Legislature's own official page and carries the complete document
# (all 14 real articles, confirmed live 2026-08-07), using the IDENTICAL
# "{article}-{section}." numbering convention already handled by
# _WV_SECTION_RE, so the existing section-split pattern is reused directly
# (see the leading-newline note at its call site below). Needs the US proxy
# (same geo-fencing class as PA/MI).
# ---------------------------------------------------------------------------

WV_CONST_URL = "https://home.wvlegislature.gov/constitution-of-west-virginia/"
_WV_ARTICLE_RE = re.compile(r"\nARTICLE\s+([IVXLC]+)\n")


def scrape_wv(r2) -> list[Section]:
    out: list[Section] = []
    try:
        html = fetch_text(WV_CONST_URL, use_us_proxy=True)
    except Exception as e:
        _DROPS.fetch_failed("WV constitution", e)
        return out

    r2_html_key = "state_constitutions/wv/source/wv_constitution.html"
    put_if_changed(r2, r2_html_key, html.encode("utf-8"), "text/html; charset=utf-8")
    r2_html_url = public_url(r2_html_key)

    soup = BeautifulSoup(html, "html.parser")
    main = soup.find("main")
    body_text = (main or soup).get_text("\n", strip=True)

    art_matches = list(_WV_ARTICLE_RE.finditer(body_text))
    if not art_matches:
        _DROPS.unit_empty(f"WV (no ARTICLE markers in {len(body_text)} chars)")
        return out

    art_iter: list[tuple[str, str]] = []
    for i, m in enumerate(art_matches):
        art_id = m.group(1)
        start = m.end()
        end = art_matches[i + 1].start() if i + 1 < len(art_matches) else len(body_text)
        # _WV_SECTION_RE requires a leading "\n" before "N-N.", but the
        # article-split regex above consumes ITS OWN trailing "\n" as the
        # article-header delimiter, so art_body starts immediately at the
        # first section's "N-1." with nothing before it to match against --
        # silently dropping every article's first section (confirmed live
        # 2026-08-07: WV Articles I/II/IV each came up exactly one section
        # short, always missing section "1" specifically, until this fix).
        # _emit_sections_from_articles re-prepends "\n" before splitting, so
        # this is preserved through the shared helper, not worked around here.
        art_iter.append((art_id, body_text[start:end]))

    return _emit_sections_from_articles(
        "wv", r2, WV_CONST_URL, art_iter, _WV_SECTION_RE, r2_html_url=r2_html_url
    )


# ---------------------------------------------------------------------------
# Nevada -- replaces the Wikisource source (see _WS_INLINE_STATES' former
# "nv" entry), which the page itself flagged as incomplete transcription
# ("This work is incomplete... source document not known") and only ever
# carried the Preamble + Article I (2 live points before this fix).
# leg.state.nv.us/const/nvconst.html is the Nevada Legislature's own official,
# currently-maintained page (carries its own "[Rev. <date>]" revision stamp)
# and has the complete document, all 19 articles including Article XVIII
# (repealed 1992, kept for historical reference). Confirmed live 2026-08-07.
# Needs the US proxy (Cloudflare-blocks unproxied fetches).
#
# Each article opens with a short table-of-contents-style preview (bare
# "N.  Title" lines, tab-aligned with runs of non-breaking spaces) before the
# real numbered sections start; the section-split regex below only matches
# the REAL "Section." / "Sec:" markers (never the bare TOC numbers, which
# carry no keyword), so the whole TOC preview lands in the discarded
# "text before the first match" span for free -- except the TOC block's own
# one-time "Sec." column-header token, which DOES collide with the keyword
# pattern; the negative lookahead for trailing non-breaking-space padding
# excludes that one false match without needing separate TOC-detection code.
#
# A handful of sections legitimately appear TWICE under the same number: NV
# prints both a currently-effective version and an already-adopted-but-not-
# yet-effective future version side by side (e.g. Sec. 7 of Article 6, one
# "[Effective through November 27, 2028...]" and one "[Effective November 28,
# 2028, if... Assembly Joint Resolution No. 8 (2025)...]"). Both are real,
# separately citable text, not a scrape bug -- the second (and any further)
# occurrence of a section number within one article gets a "-vN" suffix
# rather than silently overwriting the first.
# ---------------------------------------------------------------------------

NV_CONST_URL = "https://www.leg.state.nv.us/const/nvconst.html"
_NV_ARTICLE_RE = re.compile(r"\nARTICLE\.?\s+([IVXLC]+|\d+)\.?\s*-?\s*[^\n]*\n")
_NV_SECTION_RE = re.compile(r"\n(?:Section|Sec)[.:]\s*(\d+[A-Za-z]?)\.?(?!\s{0,3}\xa0)\s*")
# Amendment/effective-date/repeal history is consistently bracketed, e.g.
# "[Amended in 1996. See: Statutes of Nevada 1993, p. 2938...]",
# "[Repealed in 1992.]", "[Effective through November 27, 2028...]".
# Verified live 2026-08-07: sweeps in the real citation years cleanly; a few
# non-amendment brackets (e.g. a redundant "[Right of Suffrage.]" title, or
# the page's own top-of-document "[Rev. <date>]" stamp, which never falls
# inside any single section's raw_text anyway) carry no year token, so they
# are harmless no-ops for this extractor.
_NV_BRACKET_RE = re.compile(r"\[[^\[\]]{0,600}\]")


def _nv_amendment_years(raw_text: str) -> list[int]:
    return _years_in_matched_spans(raw_text, _NV_BRACKET_RE)


_AMENDMENT_YEAR_EXTRACTORS["nv"] = _nv_amendment_years


def scrape_nv(r2) -> list[Section]:
    out: list[Section] = []
    try:
        html = fetch_text(NV_CONST_URL, use_us_proxy=True)
    except Exception as e:
        _DROPS.fetch_failed("NV constitution", e)
        return out

    r2_html_key = "state_constitutions/nv/source/nv_constitution.html"
    put_if_changed(r2, r2_html_key, html.encode("utf-8"), "text/html; charset=utf-8")
    r2_html_url = public_url(r2_html_key)

    soup = BeautifulSoup(html, "html.parser")
    body_text = soup.get_text("\n", strip=True)

    art_parts = _NV_ARTICLE_RE.split(body_text)
    if len(art_parts) <= 1:
        _DROPS.unit_empty(f"NV (no ARTICLE markers in {len(body_text)} chars)")
        return out
    art_iter = [(art_parts[i], art_parts[i + 1]) for i in range(1, len(art_parts) - 1, 2)]

    # NV's -vN duplicate-section-number handling (a currently-effective and
    # an already-adopted-but-not-yet-effective version printed side by side
    # under the same number, see the module comment above) is now the
    # shared helper's default behavior, not NV-specific code.
    return _emit_sections_from_articles(
        "nv", r2, NV_CONST_URL, art_iter, _NV_SECTION_RE, r2_html_url=r2_html_url
    )


# ---------------------------------------------------------------------------
# New Mexico -- replaces the Wikisource source (see _WS_INLINE_STATES'
# former "nm" entry), which only had 3 of the real 23 articles. NM has no
# usable HTML source: nmonesource.com (the state's official Compilation
# Commission research tool) is a Lexum-powered single-page app whose real
# content loads via JS/API calls, not present in the static HTML at all.
# Instead we use the New Mexico Secretary of State's own official PDF
# (sos.nm.gov), current through the 2024 general election, verified live
# 2026-08-07 against its own printed table of contents (every article's
# section range matches, once genuinely-repealed/skipped section numbers
# in the current numbering are accounted for -- e.g. Article XI's TOC
# itself only lists sections 1, 2, 13, 14, 18, 19, 20, the rest having been
# repealed over the years; this is NOT a scrape gap).
# ---------------------------------------------------------------------------

NM_CONST_PDF = "https://www.sos.nm.gov/wp-content/uploads/2025/01/NM_Constitution_-2025-for-SOS.pdf"
# Every page repeats a copyright/filename footer, corrupted by a PDF
# text-layer artifact that doubles each character in it (e.g.
# "NNMM__CCoonnssttiittuuttiioonn" for "NM_Constitution") -- confirmed live
# 2026-08-07 this garbling is confined to this one repeating footer block
# and never appears in the real constitutional text, so the footer is
# stripped outright rather than worked around.
_NM_FOOTER_RE = re.compile(r"\n?\d*\s*©\s*2025 State of New Mexico\..*?AAMM?\n*", re.DOTALL)
# Running page header, repeated on every page WITHIN a multi-page article:
# "Article N – Title" (title case, en-dash, all on ONE line) -- lands
# mid-section-body wherever a page breaks, so it is stripped as noise before
# section-splitting rather than left to leak into whichever section happens
# to straddle that page boundary (confirmed live 2026-08-07: Article II
# Section 1's body otherwise ends with a trailing "Article II - Bill of
# Right[s]" fragment). Distinct enough from the real all-caps "ARTICLE N"
# heading (title case here vs all-caps there) that this never eats a real
# heading.
_NM_RUNNING_HEADER_RE = re.compile(r"\nArticle\s+[IVXLC]+\s*[–-]\s*[^\n]*\n")
# All-caps "ARTICLE N" alone on its own line is the real heading.
_NM_ARTICLE_RE = re.compile(r"\nARTICLE\s+([IVXLC]+)\n[^\n]*\n")
# Real section markers are "Section N. [Catchline.]" or "Sec. N. [Catchline.]"
# with the number on the SAME line -- required to exclude the TOC preview's
# own "Sec.\n" column-header token (see below), and "Section" must NOT be
# required to have its own period (unlike "Sec.", it never takes one:
# "Section 1." not "Section. 1."). The whitespace between the keyword and
# the number must exclude "\n" (that is the TOC-exclusion) but MUST include
# more than plain " "/"\t": PyMuPDF (unlike the pdfplumber extraction this
# was first written against) preserves the PDF's actual embedded space
# character, and this PDF mixes ordinary " " ("Section 1. ...") with U+00A0
# non-breaking space ("Sec.\xa02. ...") for different sections on the same
# page. `[^\S\n]` (whitespace, any kind, except newline) covers both without
# reopening the TOC false-match. Verified live 2026-08-07: reproduces the
# same 265-section count PyMuPDF and pdfplumber both give for this PDF.
_NM_SECTION_RE = re.compile(r"\n(?:Section[^\S\n]+|Sec\.[^\S\n]+)(\d+[A-Za-z]?)\.[^\S\n]*")
# PDF text extraction preserves the source's justified-text line-wrap
# hyphens verbatim ("insepara-\nble", "consti-\ntution"), which would
# otherwise break full-text/embedding search for the whole word. Joining
# "word-\nword" back together is a standard PDF-cleanup step; the rare
# genuine hyphenated compound that happens to wrap at a hyphen (e.g.
# "twenty-\ntwo") gets silently rejoined into "twentytwo" instead of
# "twenty-two", accepted as a minor, uncommon cosmetic cost against the
# much larger and more common benefit of not silently splitting ordinary
# words in half throughout the whole document.
_NM_HYPHEN_WRAP_RE = re.compile(r"(\w)-\n(\w)")


def _nm_clean_body(text: str) -> str:
    text = _NM_FOOTER_RE.sub("\n", text)
    text = _NM_RUNNING_HEADER_RE.sub("\n", text)
    return _NM_HYPHEN_WRAP_RE.sub(r"\1\2", text)


def scrape_nm(r2) -> list[Section]:
    out: list[Section] = []
    pdf_bytes = _fetch_bytes_proxy(NM_CONST_PDF)
    if not pdf_bytes:
        _DROPS.fetch_failed("NM constitution PDF", "download failed")
        return out

    r2_pdf_key = "state_constitutions/nm/source/nm_constitution.pdf"
    put_if_changed(r2, r2_pdf_key, pdf_bytes, "application/pdf")
    r2_pdf_url = public_url(r2_pdf_key)

    text = _pdf_to_text(pdf_bytes)
    if not text:
        _DROPS.unit_empty("NM (PDF text extraction empty)")
        return out
    body_text = _nm_clean_body(text)

    art_parts = _NM_ARTICLE_RE.split("\n" + body_text)
    if len(art_parts) <= 1:
        _DROPS.unit_empty(f"NM (no ARTICLE markers in {len(body_text)} chars)")
        return out
    art_iter = [(art_parts[i], art_parts[i + 1]) for i in range(1, len(art_parts) - 1, 2)]

    return _emit_sections_from_articles(
        "nm", r2, NM_CONST_PDF, art_iter, _NM_SECTION_RE, r2_pdf_url=r2_pdf_url
    )


# ---------------------------------------------------------------------------
# Washington -- replaces the Wikisource source (see _WS_INLINE_STATES'
# former "wa" entry). Wikisource's main page turned out to itself carry an
# incomplete transcription: 12 of WA's 32 real articles (IV Judiciary, V
# Impeachment, VI Elections, VII Revenue/Taxation, VIII State Debt, IX
# Corporations, XI, XII, XIV-XVII) existed there only as bare heading stubs
# with zero body text -- confirmed live 2026-08-07, not a parser bug (see
# the removed _WS_INLINE_STATES comment for the investigation). The WA
# Legislature's own official PDF (leg.wa.gov) has the complete document,
# "in its currently amended form" per its own front matter (i.e. each
# section already reflects the latest amendment, not the 1889 original).
#
# The PDF has three parts: (A) the current constitution, (B) amendments in
# order of adoption (full historical text of every amendment resolution,
# duplicating (A)'s content in an older form), (C) an index. Only (A) is
# wanted -- (B) restates several of the same ARTICLE headings (e.g.
# "AMENDMENT 49...ARTICLE XXIX" narrating what that amendment created),
# which would otherwise register as duplicate/garbage article bodies
# (confirmed live 2026-08-07: naively parsing past the (A)/(B) boundary
# produced an 308,700-char "Article XXXII" that was actually most of parts
# B and C run together). (B) begins right at the document's own "AMENDMENT
# 1" heading, so the whole document is truncated there before any
# article/section split runs.
# ---------------------------------------------------------------------------

WA_CONST_PDF = "https://leg.wa.gov/media/o3fg0ey1/washington-state-constitution.pdf"
# Every page repeats a "<date> <time> [ <page> ] <last item on page>" footer
# (the trailing part varies -- an article/section cite, an amendment
# number, or just "WA Constitution" -- so the whole line is dropped).
_WA_FOOTER_RE = re.compile(
    r"\n?\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}\s+[AP]M\s*\[\s*\d+\s*\][^\n]*\n?"
)
# Same PDF line-wrap hyphenation artifact as NM (see _NM_HYPHEN_WRAP_RE).
_WA_HYPHEN_WRAP_RE = re.compile(r"(\w)-\n(\w)")
_WA_PART_B_MARKER = "\nAMENDMENT 1\n"
_WA_ARTICLE_RE = re.compile(r"\nARTICLE\s+([IVXLC]+)\n[^\n]*\n")
_WA_SECTION_RE = re.compile(r"\nSECTION\s+(\d+[A-Za-z]?)\s+")


def scrape_wa(r2) -> list[Section]:
    out: list[Section] = []
    pdf_bytes = _fetch_bytes_proxy(WA_CONST_PDF)
    if not pdf_bytes:
        _DROPS.fetch_failed("WA constitution PDF", "download failed")
        return out

    r2_pdf_key = "state_constitutions/wa/source/wa_constitution.pdf"
    put_if_changed(r2, r2_pdf_key, pdf_bytes, "application/pdf")
    r2_pdf_url = public_url(r2_pdf_key)

    text = _pdf_to_text(pdf_bytes)
    if not text:
        _DROPS.unit_empty("WA (PDF text extraction empty)")
        return out
    clean = _WA_FOOTER_RE.sub("\n", text)
    clean = _WA_HYPHEN_WRAP_RE.sub(r"\1\2", clean)
    part_b_idx = clean.find(_WA_PART_B_MARKER)
    if part_b_idx != -1:
        clean = clean[:part_b_idx]

    art_parts = _WA_ARTICLE_RE.split("\n" + clean)
    if len(art_parts) <= 1:
        _DROPS.unit_empty(f"WA (no ARTICLE markers in {len(clean)} chars)")
        return out
    art_iter = [(art_parts[i], art_parts[i + 1]) for i in range(1, len(art_parts) - 1, 2)]

    # WA's Article XXVI ("Compact With The United States") is the same
    # congressionally-mandated Enabling Act ordinance already handled for
    # SD/WA under scrape_wikisource_inline -- see _COMPACT_ORDINANCE_RE. No
    # "SECTION" keyword anywhere; its four clauses are headed "First.",
    # "Second.", "Third.", "Fourth." -- tried as a fallback when the normal
    # section regex finds nothing in an article's body.
    return _emit_sections_from_articles(
        "wa",
        r2,
        WA_CONST_PDF,
        art_iter,
        _WA_SECTION_RE,
        r2_pdf_url=r2_pdf_url,
        fallback_section_res=(_COMPACT_ORDINANCE_RE,),
    )


# ---------------------------------------------------------------------------
# Minnesota -- replaces the Wikisource source (see _WS_INLINE_STATES' former
# "mn" entry). revisor.mn.gov/constitution/ serves the ENTIRE document on one
# page with genuinely semantic markup: div.article (id="article_N") wraps a
# bare "ARTICLE N" h2 plus an h2.header title, then one div.section per
# section (h3.section_no carrying "Section 1."/"Sec. 2." with a nested
# span.headnote catchline, followed by the body <p> tags and an optional
# div.note carrying the amendment history, e.g. "[Amended, November 8,
# 1988]"). Confirmed live 2026-08-08: 14 div.article elements, 136
# div.section elements, matching the state's own article headings exactly.
# Clean enough to walk the DOM directly rather than regex-split plain text --
# the article/section boundaries are real tags, not text markers to rediscover.
# The amendment bracket note format matches the extractor already registered
# for "mn" in _AMENDMENT_YEAR_EXTRACTORS (_MN_BRACKET_RE) from the Wikisource
# era, so no new extractor is needed here; it just needs the note text to
# stay inside raw_text, which it does (only the h3 heading is stripped below).
# ---------------------------------------------------------------------------

MN_CONST_URL = "https://www.revisor.mn.gov/constitution/"
_MN_ARTICLE_NUM_RE = re.compile(r"ARTICLE\s+([IVXLC]+)", re.IGNORECASE)
_MN_SECTION_NUM_RE = re.compile(r"(\d+[A-Za-z]?)")


def scrape_mn(r2) -> list[Section]:
    try:
        html = fetch_text(MN_CONST_URL)
    except Exception as e:
        _DROPS.fetch_failed("MN constitution", e)
        return []

    r2_html_key = "state_constitutions/mn/source/mn_constitution.html"
    put_if_changed(r2, r2_html_key, html.encode("utf-8"), "text/html; charset=utf-8")
    r2_html_url = public_url(r2_html_key)

    soup = BeautifulSoup(html, "html.parser")
    out: list[Section] = []

    preamble_h2 = soup.find("h2", string=re.compile(r"^\s*Preamble\s*$", re.IGNORECASE))
    if preamble_h2:
        parts = []
        for sib in preamble_h2.find_next_siblings():
            # The preamble body is one or more <p> siblings of the "Preamble"
            # h2, directly followed by the first div.article -- stop there,
            # not just at the next h2 (which is nested INSIDE div.article,
            # not a sibling of it, so a bare "next h2" check never fires and
            # silently swept the entire rest of the document into the
            # preamble on first attempt, confirmed live 2026-08-08).
            if getattr(sib, "name", None) in ("h2", "div"):
                break
            parts.append(sib.get_text(" ", strip=True))
        preamble_text = " ".join(p for p in parts if p)
        sec = _emit_section(
            "mn",
            r2,
            r2_html_url,
            MN_CONST_URL,
            "0",
            "0",
            preamble_text,
            section_title="Minn. Const. Preamble",
        )
        if sec:
            out.append(sec)

    art_divs = soup.select("div.article")
    for art_div in art_divs:
        headers = art_div.find_all("h2", recursive=False)
        if not headers:
            continue
        art_m = _MN_ARTICLE_NUM_RE.search(headers[0].get_text(strip=True))
        if not art_m:
            continue
        art_id = art_m.group(1)
        art_title = headers[1].get_text(strip=True) if len(headers) > 1 else ""

        count_this_art = 0
        for sec_div in art_div.find_all("div", class_="section", recursive=False):
            h3 = sec_div.find("h3", class_="section_no")
            if not h3:
                continue
            headnote = h3.find("span", class_="headnote")
            sec_title_text = headnote.get_text(strip=True) if headnote else ""
            num_text = h3.get_text(" ", strip=True)
            if sec_title_text:
                num_text = num_text.replace(sec_title_text, "")
            sec_m = _MN_SECTION_NUM_RE.search(num_text)
            if not sec_m:
                continue
            sec_num = sec_m.group(1)

            body_copy = BeautifulSoup(str(sec_div), "html.parser")
            h3_copy = body_copy.find("h3", class_="section_no")
            if h3_copy:
                h3_copy.decompose()
            body_text = body_copy.get_text(" ", strip=True)

            sec_title = f"Minn. Const. art. {art_id}, § {sec_num}"
            if sec_title_text:
                sec_title += f". {sec_title_text}"
            sec = _emit_section(
                "mn",
                r2,
                r2_html_url,
                MN_CONST_URL,
                art_id,
                sec_num,
                body_text,
                section_title=sec_title,
                article_title=art_title,
            )
            if sec:
                out.append(sec)
                count_this_art += 1
        if count_this_art == 0:
            _DROPS.unit_empty(f"MN art {art_id} (no sections parsed)")

    if not out:
        _DROPS.unit_empty("MN (0 sections after parse)")
    print(f"[MN] done: {len(out)} sections across {len(art_divs)} articles")
    return out


# ---------------------------------------------------------------------------
# Florida -- replaces the Wikisource source (see _WS_INLINE_STATES' former
# "fl" entry). flsenate.gov/Laws/Constitution serves the entire document on
# one page, and it happens to be the SAME site/markup convention as FL's own
# statute scraper (scrapeFL.py): div.Article > div.Section, with
# span.SectionNumber / span.CatchlineText / span.SectionBody / div.History
# (span.HistoryText) class names identical to the statute pages. Confirmed
# live 2026-08-08: 12 div.Article, 213 div.Section, zero missing bodies. Each
# article also carries a div.CatchlineIndex mini-TOC (div.IndexItem, not
# div.Section) right before the real sections -- irrelevant here since this
# walks div.Section by CSS class directly rather than regex-splitting text,
# so the TOC's different class name excludes it for free. This is a DOM walk,
# not a regex-split job -- call _emit_section per div.Section directly.
# ---------------------------------------------------------------------------

FL_CONST_URL = "https://www.flsenate.gov/Laws/Constitution"
_FL_ARTICLE_NUM_RE = re.compile(r"ARTICLE\s+([IVXLC]+)", re.IGNORECASE)
_FL_SECTION_NUM_RE = re.compile(r"(\d+[A-Za-z]?)")
# Every sampled History note (147 of 213 sections carry one) ends its chain of
# session-law/CRC-revision citations with the literal word "adopted" followed
# by the ratification year, even when several proposal/filing years appear
# earlier in the same note (e.g. "Am. proposed ... 1998, filed ... May 5,
# 1998; adopted 1998") -- anchoring on "adopted" (not just any 4-digit year)
# keeps this from also sweeping in a filing/proposal year that isn't itself
# an amendment date. Verified live 2026-08-08 against a random sample of 8.
_FL_ADOPTED_RE = re.compile(r"adopted\s+((?:19|20)\d{2})", re.IGNORECASE)


def _fl_amendment_years(raw_text: str) -> list[int]:
    return [int(y) for y in _FL_ADOPTED_RE.findall(raw_text)]


_AMENDMENT_YEAR_EXTRACTORS["fl"] = _fl_amendment_years


def scrape_fl(r2) -> list[Section]:
    try:
        html = fetch_text(FL_CONST_URL)
    except Exception as e:
        _DROPS.fetch_failed("FL constitution", e)
        return []

    r2_html_key = "state_constitutions/fl/source/fl_constitution.html"
    put_if_changed(r2, r2_html_key, html.encode("utf-8"), "text/html; charset=utf-8")
    r2_html_url = public_url(r2_html_key)

    soup = BeautifulSoup(html, "html.parser")
    out: list[Section] = []
    art_divs = soup.select("div.Article")
    for art_div in art_divs:
        num_div = art_div.select_one("div.ArticleNumber")
        name_div = art_div.select_one("div.ArticleName")
        if not num_div:
            continue
        art_m = _FL_ARTICLE_NUM_RE.search(num_div.get_text(strip=True))
        if not art_m:
            continue
        art_id = art_m.group(1)
        art_title = name_div.get_text(strip=True) if name_div else ""

        count_this_art = 0
        for sec_div in art_div.select("div.Section"):
            num_span = sec_div.select_one("span.SectionNumber")
            catch_span = sec_div.select_one("span.CatchlineText")
            body_span = sec_div.select_one("span.SectionBody")
            if not num_span or not body_span:
                continue
            sec_m = _FL_SECTION_NUM_RE.search(num_span.get_text(strip=True))
            if not sec_m:
                continue
            sec_num = sec_m.group(1)
            catchline = catch_span.get_text(strip=True) if catch_span else ""

            body_text = body_span.get_text(" ", strip=True)
            history_div = sec_div.select_one("div.History")
            if history_div:
                # Kept inline (like VA/MD/PA's own amendment notes) rather
                # than discarded, so _fl_amendment_years above can find it.
                body_text = f"{body_text} {history_div.get_text(' ', strip=True)}"

            sec_title = f"Fla. Const. art. {art_id}, § {sec_num}"
            if catchline:
                sec_title += f". {catchline}"
            sec = _emit_section(
                "fl",
                r2,
                r2_html_url,
                FL_CONST_URL,
                art_id,
                sec_num,
                body_text,
                section_title=sec_title,
                article_title=art_title,
            )
            if sec:
                out.append(sec)
                count_this_art += 1
        if count_this_art == 0:
            _DROPS.unit_empty(f"FL art {art_id} (no sections parsed)")

    if not out:
        _DROPS.unit_empty("FL (0 sections after parse)")
    print(f"[FL] done: {len(out)} sections across {len(art_divs)} articles")
    return out


# ---------------------------------------------------------------------------
# Illinois -- replaces the Wikisource source (see _WS_INLINE_STATES' former
# "il" entry). ilga.gov/commission/lrb/con{1-14}.htm serves one page per
# article (14 pages, matching the 14 real articles I-XIV; there is no
# con15.htm -- the 1970 constitution's original Transition Schedule was
# self-executing and has lapsed, so the current official printing simply
# does not carry it as a live article, confirmed live 2026-08-08 via 404s on
# con15/con16). Needs the US proxy (geo-fenced; confirmed live 2026-08-08:
# direct fetch from a non-US IP connection-refused). Each page is already
# exactly one article, so there is no article-split step at all -- just a
# clean "SECTION N. TITLE ... (Source: ...)" per-section pattern, the
# identical r'^\(Source:' footer convention scrapeIL.py's statute parser
# already keys on. 140 SECTION markers across the 14 pages, verified live
# (156 chunk records, slightly below Wikisource's 174 pts baseline --
# confirmed live 2026-08-08 this is NOT a scrape gap: Wikisource's own page
# carries a 15th "TRANSITION SCHEDULE" article with 6 more sections that the
# official ilga.gov site does not publish at all (no con15.htm), because the
# 1970 constitution's transition schedule was self-executing and has already
# been spent -- the same "historical TOC range vs current in-force count"
# class NM's Article XI hit, not missing content).
# A section number can carry a decimal insert (e.g. "SECTION 8.1. CRIME
# VICTIMS' RIGHTS.", inserted between original Sections 8 and 9) -- the
# section-number group must capture the whole "8.1", not just "8", or the
# ".1." spills into the catchline group and two sections collide under a
# single act_id ("8"), silently dropping one via the merge-by-act_id write
# path. Confirmed live 2026-08-08 in Article I.
# ---------------------------------------------------------------------------

IL_CONST_URL_TMPL = "https://www.ilga.gov/commission/lrb/con{n}.htm"
_IL_ARTICLE_HEAD_RE = re.compile(r"ARTICLE\s+([IVXLC]+)\n([^\n]+)")
_IL_SECTION_RE = re.compile(r"\nSECTION\s+(\d+(?:\.\d+)?[A-Za-z]?)\.?\s*([^\n]*)\n")
# Every amended section's trailing "(Source: ...)" note that isn't just the
# bare "(Source: Illinois Constitution.)" baseline uses the phrase "Amendment
# adopted at general election <date>, <year>." -- the date itself sometimes
# line-wraps mid-phrase ("November \n4, 2014."), so the year is matched
# within a bounded any-character window after the keyword phrase rather than
# anchored to a single line. Verified live 2026-08-08 across all 14 articles:
# every non-baseline Source note across a distinct-note sample matches this
# shape, zero exceptions found.
_IL_AMEND_RE = re.compile(
    r"Amendment adopted at general election[\s\S]{0,60}?((?:19|20)\d{2})\.?\)",
    re.IGNORECASE,
)


def _il_amendment_years(raw_text: str) -> list[int]:
    return [int(y) for y in _IL_AMEND_RE.findall(raw_text)]


_AMENDMENT_YEAR_EXTRACTORS["il"] = _il_amendment_years


def scrape_il(r2) -> list[Section]:
    out: list[Section] = []
    for n in range(1, 15):
        url = IL_CONST_URL_TMPL.format(n=n)
        try:
            html = fetch_text(url, use_us_proxy=True)
        except Exception as e:
            _DROPS.fetch_failed(f"IL art page {n}", e)
            continue

        r2_html_key = f"state_constitutions/il/source/con{n}.html"
        put_if_changed(r2, r2_html_key, html.encode("utf-8"), "text/html; charset=utf-8")
        r2_html_url = public_url(r2_html_key)

        body_text = BeautifulSoup(html, "html.parser").get_text("\n", strip=True)
        art_m = _IL_ARTICLE_HEAD_RE.search(body_text)
        if not art_m:
            _DROPS.unit_empty(f"IL con{n}.htm (no ARTICLE heading in {len(body_text)} chars)")
            continue
        art_id = art_m.group(1)
        art_title = art_m.group(2).strip()

        sec_matches = list(_IL_SECTION_RE.finditer(body_text))
        if not sec_matches:
            _DROPS.unit_empty(f"IL art {art_id} (no SECTION markers)")
            continue

        count_this_art = 0
        for i, sm in enumerate(sec_matches):
            sec_num = sm.group(1)
            sec_head = sm.group(2).strip()
            start = sm.end()
            end = sec_matches[i + 1].start() if i + 1 < len(sec_matches) else len(body_text)
            sec_body = body_text[start:end]

            sec_title = f"Ill. Const. art. {art_id}, § {sec_num}"
            if sec_head:
                sec_title += f". {sec_head}"
            sec = _emit_section(
                "il",
                r2,
                r2_html_url,
                url,
                art_id,
                sec_num,
                sec_body,
                section_title=sec_title,
                article_title=art_title,
            )
            if sec:
                out.append(sec)
                count_this_art += 1
        if count_this_art == 0:
            _DROPS.unit_empty(f"IL art {art_id} (0 sections parsed)")
        print(f"  [IL art {art_id}] {count_this_art} sections")

    if not out:
        _DROPS.unit_empty("IL (0 sections after parse)")
    print(f"[IL] done: {len(out)} sections total")
    return out


# ---------------------------------------------------------------------------
# Arizona -- replaces the Wikisource source (see _WS_INLINE_STATES' former
# "az" entry). azleg.gov already pre-splits the constitution into one page
# per section (`/const/{article}/{section}.htm`), discovered via the TOC at
# `/constitution/` (which links every article as `constitution?article=N`,
# confirmed live 2026-08-08: articles 1-22, 6.1, 25-30 -- no 23/24, not a
# scrape gap, the TOC itself never lists them) and each article's own index
# page (`constitution?article=N`, which links every section page directly).
# The per-section pages are bare 1990s-era static HTML (`<P>` tags, literal
# `<!Creation Date: ...>` typist comments, no CSS classes), unlike the
# statute scraper's modern divs -- but rather than reconstruct URLs from a
# filename/directory-encoding formula (a real quirk: decimal numbers become
# an underscore in a FILENAME segment but stay a literal dot in a DIRECTORY
# segment), this scraper reads the real target URL directly off each
# article-index page's own links, sidestepping the encoding question
# entirely -- the site already did the encoding, no need to reverse it.
#
# Article 4 (Legislative) is the one article split into "Part 1" (Initiative
# and Referendum) and "Part 2" (the Legislature itself), each with its OWN
# Section 1, Section 2, ... -- confirmed live 2026-08-08 via the article-index
# page's link text ("Part 1 - Section 1" / "Part 2 - Section 1"). Folded into
# article_id as "4.1"/"4.2" (dot convention, matching the site's own existing
# dotted article numbering for 6.1) to keep every (article_id, section_number)
# pair unique; section_title carries the proper "art. 4, pt. N, § M" reading
# for humans, so the fold is purely an internal identifier convenience, not a
# citation-correctness compromise.
#
# Each section page's <title> reliably carries "Article N Section M - " then
# the catchline (verified across both a decades-old typist page and a
# freshly-added 2024 amendment page, the two structurally different formats
# found live), so the catchline is read from there rather than parsed out of
# the malformed inline markup (an unclosed <u>/<font> pair spanning two <P>
# tags in the older pages) -- then the catchline's first occurrence and any
# leading bare "N." / "Section N." label are stripped from the flattened body
# text, which handles both formats without needing to fix the malformed HTML.
#
# No per-section amendment history is published on these pages at all (both
# a decades-old and a freshly-added section were checked live 2026-08-08,
# neither carries one) -- left out of _AMENDMENT_YEAR_EXTRACTORS
# deliberately, not attempted, same convention as the module-level survey
# above for states with no extractable per-section format.
# ---------------------------------------------------------------------------

AZ_TOC_URL = "https://www.azleg.gov/constitution/"
AZ_PREAMBLE_URL = "https://www.azleg.gov/const/preamble.htm"
_AZ_ARTICLE_LINK_RE = re.compile(r"constitution\?article=([\d.]+)")
_AZ_DOC_NAME_RE = re.compile(r"docName=(https?://\S+?\.htm)")
_AZ_PART_RE = re.compile(r"Part\s+(\d+)\s*-\s*Section\s+(\d+(?:\.\d+)?)", re.IGNORECASE)
_AZ_SECTION_RE = re.compile(r"Section\s+(\d+(?:\.\d+)?)", re.IGNORECASE)
_AZ_TITLE_CATCHLINE_RE = re.compile(r".*?-\s*(.+)$")
_AZ_LEADING_NUM_RE = re.compile(r"^\s*\d+(?:\.\d+)?[A-Za-z]?\.\s*")
_AZ_LEADING_SECTION_RE = re.compile(r"^\s*Section\s+\d+(?:\.\d+)?[A-Za-z]?\.\s*", re.IGNORECASE)


def _az_discover_articles() -> list[str]:
    html = fetch_text(AZ_TOC_URL)
    arts = sorted(
        set(_AZ_ARTICLE_LINK_RE.findall(html)),
        key=lambda a: [int(p) for p in a.split(".")],
    )
    return arts


def _az_section_links(art: str) -> list[tuple[str, str, str]]:
    """Return (section_number, part_or_'', target_url) for one article page."""
    html = fetch_text(f"https://www.azleg.gov/constitution?article={art}")
    soup = BeautifulSoup(html, "html.parser")
    out: list[tuple[str, str, str]] = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "docName=" not in href or "/const/" not in href:
            continue
        m = _AZ_DOC_NAME_RE.search(href)
        if not m:
            continue
        target = m.group(1)
        text = a.get_text(strip=True)
        part_m = _AZ_PART_RE.search(text)
        if part_m:
            out.append((part_m.group(2), part_m.group(1), target))
            continue
        sec_m = _AZ_SECTION_RE.search(text)
        if not sec_m:
            continue
        out.append((sec_m.group(1), "", target))
    return out


def _az_clean_body(html: str) -> tuple[str, str]:
    """Return (catchline, cleaned body text) for one AZ section page."""
    soup = BeautifulSoup(html, "html.parser")
    title = soup.find("title")
    title_text = title.get_text(strip=True) if title else ""
    cl_m = _AZ_TITLE_CATCHLINE_RE.match(title_text)
    catchline = cl_m.group(1).strip() if cl_m else ""

    body = soup.find("body")
    full_text = (body or soup).get_text(" ", strip=True)
    if catchline:
        idx = full_text.find(catchline)
        if idx != -1:
            full_text = full_text[:idx] + full_text[idx + len(catchline) :]
    full_text = _AZ_LEADING_NUM_RE.sub("", full_text)
    full_text = _AZ_LEADING_SECTION_RE.sub("", full_text)
    return catchline, re.sub(r"\s+", " ", full_text).strip()


def scrape_az(r2) -> list[Section]:
    out: list[Section] = []
    try:
        articles = _az_discover_articles()
    except Exception as e:
        _DROPS.fetch_failed("AZ TOC", e)
        return out
    print(f"  [AZ] discovered {len(articles)} articles: {articles}")

    try:
        preamble_html = fetch_text(AZ_PREAMBLE_URL)
    except Exception as e:
        _DROPS.fetch_failed("AZ preamble", e)
        preamble_html = ""
    if preamble_html:
        _catchline, body = _az_clean_body(preamble_html)
        r2_pre_key = "state_constitutions/az/source/preamble.html"
        put_if_changed(r2, r2_pre_key, preamble_html.encode("utf-8"), "text/html; charset=utf-8")
        sec = _emit_section(
            "az",
            r2,
            public_url(r2_pre_key),
            AZ_PREAMBLE_URL,
            "0",
            "0",
            body,
            section_title="Ariz. Const. Preamble",
        )
        if sec:
            out.append(sec)

    # (article_id_for_act, section_number, part, target_url, index_url)
    work: list[tuple[str, str, str, str, str]] = []
    seen_nums_by_art_id: dict[str, dict[str, int]] = {}
    for art in articles:
        index_url = f"https://www.azleg.gov/constitution?article={art}"
        try:
            links = _az_section_links(art)
        except Exception as e:
            _DROPS.fetch_failed(f"AZ art {art} index", e)
            continue
        if not links:
            _DROPS.unit_empty(f"AZ art {art} (no section links discovered)")
            continue
        # A handful of sections carry an explicit "Section N Version M" link
        # alongside the bare "Section N" one -- a currently-effective text and
        # an already-adopted historical/amended version printed side by side
        # (e.g. Art. 5 Sec. 1's 1992 term-limits version vs. the current text;
        # Art. 19 has the same shape at Section 0), confirmed live 2026-08-08
        # by fetching both pages for one pair. Both are real, separately
        # citable text, not a scrape artifact -- same "-vN" disambiguation
        # `_emit_sections_from_articles` already applies for NV's identical
        # pattern, applied by hand here since AZ builds its own work list
        # rather than going through that shared driver.
        for sec_num, part, target in links:
            art_id = f"{art}.{part}" if part else art
            seen_nums = seen_nums_by_art_id.setdefault(art_id, {})
            seen_nums[sec_num] = seen_nums.get(sec_num, 0) + 1
            occurrence = seen_nums[sec_num]
            uniq_sec_num = sec_num if occurrence == 1 else f"{sec_num}-v{occurrence}"
            work.append((art_id, uniq_sec_num, part, target, index_url))

    def _fetch_one(item):
        art_id, sec_num, part, target, index_url = item
        try:
            html = fetch_text(target)
        except Exception as e:
            return (art_id, sec_num, part, target, index_url, None, str(e))
        return (art_id, sec_num, part, target, index_url, html, None)

    with cf.ThreadPoolExecutor(max_workers=8) as ex:
        results = list(ex.map(_fetch_one, work))

    n_failed = sum(1 for r in results if r[5] is None)
    if n_failed:
        _DROPS.fetch_failed(
            f"AZ ({n_failed} of {len(work)} section pages)", "fetch error", count=n_failed
        )
    print(f"  [AZ] fetched {len(work) - n_failed} / {len(work)} section pages")

    per_article_counts: dict[str, int] = {}
    for art_id, sec_num, part, target, index_url, html, err in results:
        if html is None:
            continue
        catchline, body = _az_clean_body(html)
        art_label = art_id.split(".")[0] if "." in art_id and part else art_id
        sec_title = f"Ariz. Const. art. {art_label}"
        if part:
            sec_title += f", pt. {part}"
        sec_title += f", § {sec_num}"
        if catchline:
            sec_title += f". {catchline}"

        # Each section is genuinely its own page (no single whole-document
        # page exists for AZ, unlike every other state here) -- mirror its
        # own HTML individually rather than pointing every section at one
        # shared provenance page.
        r2_html_key = f"state_constitutions/az/source/{art_id.replace('.', '_')}_{sec_num.replace('.', '_')}.html"
        put_if_changed(r2, r2_html_key, html.encode("utf-8"), "text/html; charset=utf-8")
        r2_html_url = public_url(r2_html_key)

        sec = _emit_section(
            "az",
            r2,
            r2_html_url,
            target,
            art_id,
            sec_num,
            body,
            section_title=sec_title,
        )
        if sec:
            out.append(sec)
            per_article_counts[art_id] = per_article_counts.get(art_id, 0) + 1

    for art_id, count in sorted(per_article_counts.items()):
        print(f"  [AZ art {art_id}] {count} sections")
    if not out:
        _DROPS.unit_empty("AZ (0 sections after parse)")
    print(f"[AZ] done: {len(out)} sections across {len(per_article_counts)} article/part units")
    return out


# ---------------------------------------------------------------------------
# Maryland -- replaces the Wikisource source (see _WS_INLINE_STATES' former
# "md" entry). mgaleg.maryland.gov serves the constitution through the
# IDENTICAL ASP.NET walk API and page template MD's own statute scraper
# (scrapeMD.py) already uses: `div#StatuteText` at
# `/mgawebsite/Laws/StatuteText?article=<code>&section=<code>` for content,
# and `/api/Laws/GetNext` / `/api/Laws/GetPrevious` (articleCode/sectionCode
# query params, response wrapped in a .NET XML `<string>` envelope) to walk
# the section ring within one article -- scrapeMD.py's own `_list_articles`
# comment already notes the Articles dropdown lists BOTH the statute codes
# ('g*') it scrapes AND the constitution codes ('c0', 'c1', 'c11a', ...) it
# explicitly filters OUT; this scraper is that filtered-out half. Confirmed
# live 2026-08-08: the walk works identically for constitution codes (same
# GetNext/GetPrevious API, same seed "1" reaches every article's first
# section -- simpler than the statute walk's multi-seed table, since
# constitution section codes are plain "1", "2", "3"... not statute-style
# "1-101"). scrapeMD.py uses vaquill_pipeline's fetch_html (statute-scraper
# infra); this module has its own fetch_text/proxy stack, so the walk is
# reimplemented against that instead of importing across the two frameworks.
#
# c0 (Declaration of Rights) has no roman-numeral article number at all --
# its TOC label is bare "Declaration of Rights", and its walked units are
# themselves called "Article N" (not "Section N" nested under a numbered
# Article like every other MD constitution article). Modeled as article_id
# "DR" with the walked code as section_number, and a dedicated citation
# format ("Md. Const., Decl. of Rights art. N") rather than forcing it
# through the generic "art. {article}, § {sec}" template that fits every
# other MD article.
#
# No per-section amendment/ratification history is rendered in
# `div#StatuteText` at all -- checked live 2026-08-08 against several
# heavily-amended sections (Article II § 1 governor's term, Article I § 1
# elections) that the Wikisource transcription DID annotate inline; the
# official site's own page simply doesn't carry it (a different rendering
# choice by the source, not a scrape gap). `_va_md_amendment_years` is
# already registered for "md" from the Wikisource era and stays registered
# (harmless no-op against this source, ready if a future request adds
# amendment metadata from elsewhere), but produces amendments_count=0 across
# the board here, not attempted further.
#
# 303 sections / 351 chunk records, BELOW Wikisource's 557 pts baseline --
# investigated live 2026-08-08 rather than shipped on faith, per the batch
# file's own testing checklist: walked Article III (Legislative) and IV
# (Judiciary) section-by-section and confirmed the GetNext ring correctly
# includes lettered/decimal inserts (35A, 40A-C, 41A through 41-I, 14A/14B,
# 18B, 21A) and correctly skips genuinely-repealed numbers (III's 8, 37,
# 41-42, 47; IV's many gaps) rather than stopping early. All 29 real
# articles are present (confirmed against the Articles dropdown itself,
# which has no "c10" at all -- MD's old Article X was repealed, not a
# discovery gap). The batch file's own research already flagged MD's
# Wikisource baseline as BUG_PRESENT (not CLEAN like MN/AL/IL); a lower,
# verified-complete count from the authoritative walk API is the expected
# outcome of fixing that bug, not a red flag.
# ---------------------------------------------------------------------------

MD_TOC_URL = "https://mgaleg.maryland.gov/mgawebsite/Laws/Statutes"
MD_SECTION_URL = "https://mgaleg.maryland.gov/mgawebsite/Laws/StatuteText"
MD_NEXT_API_URL = "https://mgaleg.maryland.gov/mgawebsite/api/Laws/GetNext"
MD_PREV_API_URL = "https://mgaleg.maryland.gov/mgawebsite/api/Laws/GetPrevious"

_MD_ARTICLES_SELECT_RE = re.compile(
    r'<select[^>]*id="Articles"[^>]*>(.*?)</select>', re.DOTALL | re.IGNORECASE
)
_MD_OPTION_RE = re.compile(r'<option[^>]+value="([^"]+)"[^>]*>([^<]+)</option>', re.IGNORECASE)
_MD_CONST_CODE_RE = re.compile(r"^c[a-z0-9]*$")
_MD_API_STRING_RE = re.compile(r"<string[^>]*>([^<]*)</string>", re.IGNORECASE)
_MD_ARTICLE_LABEL_RE = re.compile(r"^([IVXLC]+(?:-[A-Z])?)\s*-\s*(.+)$")
_MD_RESERVED_KEYWORDS = ("REPEALED", "EXPIRED", "RESERVED", "RENUMBERED", "TRANSFERRED")


def _md_parse_api(body: str | None) -> str | None:
    if not body:
        return None
    text = body.strip()
    m = _MD_API_STRING_RE.search(text)
    value = m.group(1).strip() if m else text.strip('"').strip()
    if not value or value.lower() == "null":
        return None
    return value


def _md_api_call(api_url: str, article_code: str, section_code: str) -> str | None:
    from urllib.parse import urlencode

    qs = urlencode(
        {"articleCode": article_code, "sectionCode": section_code, "enactments": "False"}
    )
    body = fetch_text(f"{api_url}?{qs}")
    return _md_parse_api(body)


def _md_list_const_articles(html: str) -> list[tuple[str, str, str]]:
    """Return [(code, article_id, article_title), ...] for the 'c*' rows only."""
    m = _MD_ARTICLES_SELECT_RE.search(html)
    if not m:
        raise RuntimeError("Could not locate <select id='Articles'> in MD TOC")
    out: list[tuple[str, str, str]] = []
    for code, display in _MD_OPTION_RE.findall(m.group(1)):
        code = code.strip()
        display = display.strip()
        if not code or not display or not _MD_CONST_CODE_RE.match(code):
            continue
        name = display.split(" - (")[0].strip()
        label_m = _MD_ARTICLE_LABEL_RE.match(name)
        if label_m:
            art_id, art_title = label_m.group(1), label_m.group(2)
        else:
            # c0, "Declaration of Rights" -- no roman-numeral prefix at all.
            art_id, art_title = "DR", name
        out.append((code, art_id, art_title))
    return out


def _md_get_first_section(article_code: str) -> str | None:
    """Find the true first section code for one article's GetNext/GetPrevious
    ring.

    A single-section article (e.g. c19 "Video Lottery Terminals", c20
    "Cannabis" -- both real, recently added articles with exactly one
    section) has BOTH GetNext("1") and GetPrevious("1") return empty, which
    is indistinguishable from "1" not existing at all if existence is
    inferred from the walk API alone (confirmed live 2026-08-08: this
    silently produced 0 sections for both before the fix). Confirm
    existence by fetching the section page directly instead, then walk
    GetPrevious to confirm/find the true first (a no-op for the
    single-section case, where GetPrevious immediately returns empty).
    """
    seed = None
    for candidate in ("1", "101", "1A", "0"):
        if _md_fetch_section_text(article_code, candidate) is not None:
            seed = candidate
            break
    if seed is None:
        return None
    current = seed
    guard = 0
    while guard < 2000:
        prev = _md_api_call(MD_PREV_API_URL, article_code, current)
        if not prev:
            return current
        current = prev
        guard += 1
    return current


def _md_fetch_section_text(article_code: str, section_code: str) -> tuple[str, str] | None:
    """Return (heading, body_text) or None if the page is a dead/missing stub."""
    url = f"{MD_SECTION_URL}?article={article_code}&section={section_code}&enactments=false"
    try:
        html = fetch_text(url)
    except Exception:
        return None
    soup = BeautifulSoup(html, "html.parser")
    stat_div = soup.find(id="StatuteText")
    if stat_div is None:
        return None
    raw_text = stat_div.get_text("\n")
    if "File Not Found" in raw_text:
        return None
    upper = raw_text.upper()
    if any(kw in upper for kw in _MD_RESERVED_KEYWORDS) and len(raw_text.strip()) < 200:
        return None
    for tag in stat_div.find_all("div", class_="row"):
        tag.decompose()
    for tag in stat_div.find_all("div", style=re.compile(r"text-align\s*:\s*center")):
        tag.decompose()
    body_text = re.sub(r"\s+", " ", stat_div.get_text(" ")).strip()
    body_text = re.sub(r"^\s*§\s*[\w.\-]+\.\s*", "", body_text)
    return url, body_text


def scrape_md(r2) -> list[Section]:
    out: list[Section] = []
    try:
        toc_html = fetch_text(MD_TOC_URL)
        articles = _md_list_const_articles(toc_html)
    except Exception as e:
        _DROPS.fetch_failed("MD TOC", e)
        return out
    r2_toc_key = "state_constitutions/md/source/toc.html"
    put_if_changed(r2, r2_toc_key, toc_html.encode("utf-8"), "text/html; charset=utf-8")
    r2_toc_url = public_url(r2_toc_key)
    print(f"  [MD] discovered {len(articles)} constitution articles")

    # Each article's own section ring must be walked sequentially (GetNext
    # needs the previous result), but the ~29 articles are fully independent
    # of each other -- same ThreadPoolExecutor-across-articles shape
    # scrapeMD.py's own scrape_all_articles already uses for the same reason.
    def _walk_one_article(item: tuple[str, str, str]) -> tuple[str, list[Section]]:
        code, art_id, art_title = item
        secs: list[Section] = []
        first = _md_get_first_section(code)
        if first is None:
            return art_id, secs
        current: str | None = first
        seen: set[str] = set()
        while current and current not in seen and len(seen) < 2000:
            seen.add(current)
            result = _md_fetch_section_text(code, current)
            if result is None:
                _DROPS.fetch_failed(f"MD art {art_id} § {current}", "empty/dead page")
            else:
                url, body_text = result
                if art_id == "DR":
                    sec_title = f"Md. Const., Decl. of Rights art. {current}"
                else:
                    sec_title = f"Md. Const. art. {art_id}, § {current}"
                sec = _emit_section(
                    "md",
                    r2,
                    r2_toc_url,
                    url,
                    art_id,
                    current,
                    body_text,
                    section_title=sec_title,
                    article_title=art_title,
                )
                if sec:
                    secs.append(sec)
            current = _md_api_call(MD_NEXT_API_URL, code, current)
        return art_id, secs

    with cf.ThreadPoolExecutor(max_workers=8) as ex:
        results = list(ex.map(_walk_one_article, articles))

    for art_id, secs in results:
        if not secs:
            _DROPS.unit_empty(f"MD art {art_id} (0 sections parsed)")
        else:
            out.extend(secs)
        print(f"  [MD art {art_id}] {len(secs)} sections")

    if not out:
        _DROPS.unit_empty("MD (0 sections after parse)")
    print(f"[MD] done: {len(out)} sections across {len(articles)} articles")
    return out


# ---------------------------------------------------------------------------
# Alabama -- AL has no scraper in this file at all before this (see the
# module-level amendment-years survey's note that AL is one of 8 states with
# no registered scraper, not a Wikisource replacement). Source is the SAME
# `alison.legislature.state.al.us/graphql` endpoint AL's own statute scraper
# (scrapeAL.py) already POSTs named queries to -- reuse the pattern (POST
# through the US proxy is NOT needed here; confirmed live 2026-08-08
# this endpoint answers named GraphQL queries directly even though browser
# introspection is Cloudflare-blocked), not scrapeAL.py's code directly
# (this module has its own fetch stack, no cross-framework import).
#
# `constitutionTitles` returns the SAME flat ROW_SEP/FIELD_SEP
# ("∫"/"†") hierarchy string as the statute scraper's `codeOfAlabamaTitles`,
# but flatter: Article -> Section directly, no Chapter level. Confirmed live
# 2026-08-08: 18 real roman-numeral articles (I-XVIII), 382 sections total,
# matching the batch file's verified count exactly.
#
# CRITICAL scope boundary: the SAME flat list continues past Article XVIII
# straight into a much larger "Local Provisions" branch (132 county/
# municipality Titles, 862 Chapters) that reuses arabic "Article 1",
# "Article 2", ... numbering per county -- confirmed live this is
# immediately adjacent in the row order with no separator. That is not part
# of the state Constitution and must never be pulled in by accident. Since
# `[IVXLC]+` cannot match an arabic digit, the article-header regex already
# excludes local-provisions "Article 1" rows on its own; scanning stops
# outright (not just skips) at the first row that is neither a roman-numeral
# Article header nor a Section row under a currently-open real article, so a
# stray non-Article/non-Section row can never be silently absorbed into the
# preceding real article either.
#
# `constitutionItems(where: { codeId: { in: [...] } })` returns full
# `content` (clean `<p>` HTML) and `history` (an amendment/ratification
# citation, e.g. "(proposed by Act 98-409, ratified January 6, 1999, as
# amendment 622)") batched by codeId -- fetching by codeId directly (already
# known from the hierarchy walk) instead of a second per-displayId lookup,
# since displayId is NOT unique across the Local Provisions branch (many
# counties independently have their own "Section 1"). The `history` format's
# "ratified <date>, <year>" phrase is the IDENTICAL shape the Wikisource-era
# `_va_md_amendment_years` extractor already matches, so it is reused
# directly rather than writing an AL-specific regex.
# ---------------------------------------------------------------------------

AL_GRAPHQL_URL = "https://alison.legislature.state.al.us/graphql"
AL_ORIGIN = "https://alison.legislature.state.al.us"
_AL_ARTICLE_RE = re.compile(r"^Article\s+([IVXLC]+)\s+(.*)$")
_AL_SECTION_RE = re.compile(r"^Section\s+(\S+)\s+(.*)$")

_AMENDMENT_YEAR_EXTRACTORS["al"] = _va_md_amendment_years


def _al_gql(query: str, timeout: float = 30.0) -> dict:
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Origin": AL_ORIGIN,
        "Referer": AL_ORIGIN + "/",
        "User-Agent": _MOZ_UA,
    }
    last = None
    for attempt in range(4):
        try:
            r = SESSION.post(
                AL_GRAPHQL_URL, json={"query": query}, headers=headers, timeout=timeout
            )
            r.raise_for_status()
            body = r.json()
        except Exception as e:
            last = e
            time.sleep(max(1.0, 0.5 * (2**attempt)))
            continue
        if body.get("errors"):
            raise RuntimeError(f"AL GraphQL errors: {body['errors']!r}")
        return body["data"]
    raise RuntimeError(f"AL GraphQL exhausted retries: {last}")


def _al_fetch_hierarchy() -> list[tuple[str, str]]:
    data = _al_gql("query constitutionTitles { titles: constitutionTitles }")
    raw: str = data["titles"]
    pairs: list[tuple[str, str]] = []
    for row in raw.split("∫"):
        if not row:
            continue
        fields = row.split("†")
        if len(fields) < 2:
            continue
        code_id, label = fields[0].strip(), fields[1].strip()
        if not code_id or code_id == "codeId":
            continue
        pairs.append((code_id, label))
    return pairs


def _al_real_articles(pairs: list[tuple[str, str]]) -> list[dict]:
    groups: list[dict] = []
    cur: dict | None = None
    for code_id, label in pairs:
        am = _AL_ARTICLE_RE.match(label)
        if am:
            cur = {"roman": am.group(1), "title": am.group(2).strip(), "sections": []}
            groups.append(cur)
            continue
        sm = _AL_SECTION_RE.match(label)
        if sm and cur is not None:
            cur["sections"].append((code_id, sm.group(1), sm.group(2).strip()))
            continue
        # Neither a roman-numeral Article header nor a Section under one --
        # the Local Provisions branch (or any other unrelated row). Stop
        # scanning entirely rather than skipping, so nothing past this point
        # can be silently absorbed into the last real article.
        break
    return groups


def _al_fetch_contents(code_ids: list[str]) -> dict[str, tuple[str, str]]:
    """codeId -> (content_html, history_text), batched to limit round trips."""
    out: dict[str, tuple[str, str]] = {}
    batch_size = 40
    for i in range(0, len(code_ids), batch_size):
        batch = code_ids[i : i + batch_size]
        ids_json = json.dumps(batch)
        q = (
            f"query {{ constitutionItems(where: {{ codeId: {{ in: {ids_json} }} }}) "
            "{ data { codeId content history } } }"
        )
        data = _al_gql(q)
        items = ((data.get("constitutionItems") or {}).get("data")) or []
        for item in items:
            out[item["codeId"]] = (item.get("content") or "", item.get("history") or "")
    return out


def scrape_al(r2) -> list[Section]:
    out: list[Section] = []
    try:
        pairs = _al_fetch_hierarchy()
    except Exception as e:
        _DROPS.fetch_failed("AL constitution hierarchy", e)
        return out

    groups = _al_real_articles(pairs)
    print(f"  [AL] discovered {len(groups)} real articles")
    if not groups:
        _DROPS.unit_empty("AL (0 real articles found in hierarchy)")
        return out

    all_code_ids = [code_id for g in groups for code_id, _, _ in g["sections"]]
    try:
        contents = _al_fetch_contents(all_code_ids)
    except Exception as e:
        _DROPS.fetch_failed(f"AL section content ({len(all_code_ids)} sections)", e)
        return out

    for g in groups:
        art_id = g["roman"]
        art_title = g["title"]
        count_this_art = 0
        for code_id, sec_num, catchline in g["sections"]:
            html_content, history = contents.get(code_id, ("", ""))
            if not html_content:
                # A hierarchy row can legitimately carry no content at all --
                # e.g. Article III § 43.01's catchline is bare "Reserved."
                # (confirmed live 2026-08-08) -- same class the statute
                # scraper's RESERVED_KEYWORDS check already treats as normal,
                # not a failure. Only count it as a real drop when the
                # catchline gives no such signal.
                if not re.search(
                    r"reserved|repealed|renumbered|transferred", catchline, re.IGNORECASE
                ):
                    _DROPS.fetch_failed(
                        f"AL art {art_id} § {sec_num}", "no content in batch response"
                    )
                continue
            body = BeautifulSoup(html_content, "html.parser").get_text(" ", strip=True)
            if history:
                # Kept inline (VA/MD/PA convention) so _va_md_amendment_years
                # above can find the "ratified <date>" phrase.
                body = f"{body} {history.strip()}"

            sec_title = f"Ala. Const. art. {art_id}, § {sec_num}"
            if catchline:
                sec_title += f". {catchline}"
            url = f"{AL_ORIGIN}/constitution-of-alabama?article={art_id}&section={sec_num}"
            sec = _emit_section(
                "al",
                r2,
                None,
                url,
                art_id,
                sec_num,
                body,
                section_title=sec_title,
                article_title=art_title,
            )
            if sec:
                out.append(sec)
                count_this_art += 1
        if count_this_art == 0:
            _DROPS.unit_empty(f"AL art {art_id} (0 sections)")
        print(f"  [AL art {art_id}] {count_this_art} sections")

    if not out:
        _DROPS.unit_empty("AL (0 sections after parse)")
    print(f"[AL] done: {len(out)} sections across {len(groups)} articles")
    return out


# ---------------------------------------------------------------------------
# Montana -- replaces the Wikisource source (see _WS_INLINE_STATES' former
# "mt" entry). mca.legmt.gov hosts the constitution on the SAME Drupal-style
# templated platform as MT's own statute scraper (scrapeMT.py), one extra
# nesting level deep (title_0000 = the constitution "title", then ARTICLE
# instead of CHAPTER, then PART, then SECTION -- scrapeMT.py's own TOC walker
# already explicitly SKIPS the "THE CONSTITUTION OF THE STATE OF MONTANA" row
# (data-titlenumber=0) waiting for exactly this). No proxy needed (reachable
# direct). Confirmed live 2026-08-08: 14 real articles (I-XIV) under
# title_0000/chapters_index.html's "chapter-toc-content" container, each with
# its own parts_index.html (one "PART N" wrapper per article, sometimes more)
# -> sections_index.html ("section-toc-content", bare "N. Title" li.line
# entries, no citation span unlike statutes) -> one page per section
# ("section-content" div for the body, "history-content" div for the
# session-law/initiative amendment citation when present).
# Wikisource baseline: 194 pts / 14 articles, verdict CLEAN.
# ---------------------------------------------------------------------------

MT_CONST_TOC = "https://mca.legmt.gov/bills/mca/title_0000/chapters_index.html"
MT_BASE = "https://mca.legmt.gov/bills/mca"
# "approved MONTH DAY, YEAR" closes every History line this source carries
# (Const. Amendment / Const. Initiative citations), e.g. "En. Sec. 1, Const.
# Initiative No. 96, approved Nov. 2, 2004." Verified live 2026-08-08.
_MT_APPROVED_RE = re.compile(r"(?i)\bapproved\s+[A-Za-z]+\.?\s+\d{1,2},?\s+((?:19|20)\d{2})")


def _mt_amendment_years(raw_text: str) -> list[int]:
    return [int(y) for y in _MT_APPROVED_RE.findall(raw_text)]


_AMENDMENT_YEAR_EXTRACTORS["mt"] = _mt_amendment_years


def _mt_strip_leading_dot_slash(href: str) -> str:
    return href[2:] if href.startswith("./") else href


def _mt_fetch_section_body(url: str) -> str:
    html = fetch_text(url)
    soup = BeautifulSoup(html, "html.parser")
    text_div = soup.find(class_="section-content")
    if text_div is None:
        return ""
    body = text_div.get_text(" ", strip=True)
    history_div = soup.find(class_="history-content")
    if history_div is not None:
        history = history_div.get_text(" ", strip=True)
        if history:
            body = f"{body} History: {history}"
    return body


def scrape_mt(r2) -> list[Section]:
    try:
        toc_html = fetch_text(MT_CONST_TOC)
    except Exception as e:
        _DROPS.fetch_failed("MT constitution TOC", e)
        return []
    r2_html_key = "state_constitutions/mt/source/toc.html"
    put_if_changed(r2, r2_html_key, toc_html.encode("utf-8"), "text/html; charset=utf-8")
    r2_html_url = public_url(r2_html_key)

    soup = BeautifulSoup(toc_html, "html.parser")
    container = soup.find(class_="chapter-toc-content")
    if container is None:
        _DROPS.unit_empty("MT (no chapter-toc-content in TOC)")
        return []

    articles: list[tuple[str, str, str]] = []  # (art_id, art_title, parts_url)
    for li in container.find_all("li", class_="line"):
        a = li.find("a")
        if a is None or not a.get("href"):
            continue
        text = re.sub(r"\s+", " ", a.get_text(" ", strip=True)).strip()
        m = re.match(r"(?i)ARTICLE\s+([IVXLC]+)\.?\s*(.*)", text)
        if not m:
            continue  # skips PREAMBLE / TRANSITION SCHEDULE -- no article num
        art_id = m.group(1)
        art_title = m.group(2).strip().rstrip(".").title()
        parts_url = f"{MT_BASE}/title_0000/{_mt_strip_leading_dot_slash(a['href'])}"
        articles.append((art_id, art_title, parts_url))

    print(f"  [MT] discovered {len(articles)} articles")
    if not articles:
        _DROPS.unit_empty("MT (no ARTICLE rows in TOC)")
        return []

    out: list[Section] = []
    for art_id, art_title, parts_url in articles:
        try:
            parts_html = fetch_text(parts_url)
        except Exception as e:
            _DROPS.fetch_failed(f"MT art {art_id} parts index", e)
            continue
        parts_soup = BeautifulSoup(parts_html, "html.parser")
        parts_container = parts_soup.find(class_="part-toc-content")
        if parts_container is None:
            _DROPS.unit_empty(f"MT art {art_id} (no part-toc-content)")
            continue

        section_specs: list[tuple[str, str, str]] = []  # (sec_num, sec_title, url)
        for part_li in parts_container.find_all("li"):
            part_a = part_li.find("a")
            if part_a is None or not part_a.get("href"):
                continue
            sections_url = (
                parts_url.rsplit("/", 1)[0] + "/" + _mt_strip_leading_dot_slash(part_a["href"])
            )
            try:
                sections_html = fetch_text(sections_url)
            except Exception as e:
                _DROPS.fetch_failed(f"MT art {art_id} sections index", e)
                continue
            sections_soup = BeautifulSoup(sections_html, "html.parser")
            sec_container = sections_soup.find(class_="section-toc-content")
            if sec_container is None:
                continue
            for sec_li in sec_container.find_all("li", class_="line"):
                sec_a = sec_li.find("a")
                if sec_a is None or not sec_a.get("href"):
                    continue
                sec_text = re.sub(r"\s+", " ", sec_a.get_text(" ", strip=True)).strip()
                sm = re.match(r"(\d+[A-Za-z]?)\.\s*(.*)", sec_text)
                if not sm:
                    continue
                sec_num, sec_title = sm.group(1), sm.group(2).strip()
                sec_url = (
                    sections_url.rsplit("/", 1)[0]
                    + "/"
                    + _mt_strip_leading_dot_slash(sec_a["href"])
                )
                section_specs.append((sec_num, sec_title, sec_url))

        if not section_specs:
            _DROPS.unit_empty(f"MT art {art_id} (no sections discovered)")
            continue

        def _fetch_one(spec):
            sec_num, sec_title, url = spec
            try:
                body = _mt_fetch_section_body(url)
            except Exception:
                return (sec_num, sec_title, url, None)
            return (sec_num, sec_title, url, body)

        with cf.ThreadPoolExecutor(max_workers=8) as ex:
            results = list(ex.map(_fetch_one, section_specs))

        count_this_art = 0
        for sec_num, sec_title, url, body in results:
            if body is None:
                _DROPS.fetch_failed(f"MT art {art_id} sec {sec_num}", "fetch failed")
                continue
            sec = _emit_section(
                "mt",
                r2,
                r2_html_url,
                url,
                art_id,
                sec_num,
                body,
                section_title=(
                    f"Mont. Const. art. {art_id}, § {sec_num}. {sec_title}" if sec_title else None
                ),
                article_title=art_title,
            )
            if sec:
                out.append(sec)
                count_this_art += 1
        if count_this_art == 0:
            _DROPS.unit_empty(f"MT art {art_id} (0 sections after fetch)")
        print(f"  [MT art {art_id}] {count_this_art} sections")

    if not out:
        _DROPS.unit_empty("MT (0 sections after parse)")
    print(f"[MT] done: {len(out)} sections across {len(articles)} articles")
    return out


# ---------------------------------------------------------------------------
# Ohio -- replaces the Wikisource source (see _WS_INLINE_STATES' former "oh"
# entry). codes.ohio.gov/ohio-constitution is the SAME CMS platform as OH's
# own statute scraper (codes.ohio.gov/ohio-revised-code) -- one page per
# article (.../ohio-constitution/article-N), a table.laws-table with one tr
# per section, each holding a div.content-head > a (citation + title, piped
# together: "Article I, Section 1 | Inalienable Rights") and a
# div.content-body (div.laws-section-info's "Effective: <date>" module +
# div.laws-body's actual text). Confirmed live 2026-08-08: 19 articles +
# Preamble + 2 Schedules. Batch research expected no proxy (same as the
# statute scraper), but a real run on the scraper box (2026-08-08) direct-
# connect-timed-out to codes.ohio.gov, so this uses the US proxy the same
# as the geo-blocked states elsewhere in this file.
# `_oh_amendment_years` reads each section's own "Effective: <date>" line,
# excluding the original 1851 constitution's own adoption date (not an
# amendment) the same way TX/federal exclude their own founding dates
# elsewhere in this file.
# Wikisource baseline: 229 pts / 18 articles, verdict CLEAN.
# ---------------------------------------------------------------------------

OH_CONST_TOC = "https://codes.ohio.gov/ohio-constitution"
OH_ARTICLE_URL_TMPL = "https://codes.ohio.gov/ohio-constitution/article-{n}"
_OH_EFFECTIVE_RE = re.compile(
    r"(?i)Effective:\s*[A-Za-z]+\.?\s+\d{1,2},?\s+((?:19|20)\d{2})"
)


def _oh_amendment_years(raw_text: str) -> list[int]:
    return [int(y) for y in _OH_EFFECTIVE_RE.findall(raw_text) if int(y) != 1851]


_AMENDMENT_YEAR_EXTRACTORS["oh"] = _oh_amendment_years


def scrape_oh(r2) -> list[Section]:
    try:
        toc_html = fetch_text(OH_CONST_TOC, use_us_proxy=True)
    except Exception as e:
        _DROPS.fetch_failed("OH constitution TOC", e)
        return []
    soup = BeautifulSoup(toc_html, "html.parser")
    article_ids: list[str] = []
    for a in soup.find_all("a", href=True):
        m = re.match(r"^ohio-constitution/article-(\d+)$", a["href"].strip())
        if m and m.group(1) not in article_ids:
            article_ids.append(m.group(1))

    print(f"  [OH] discovered {len(article_ids)} articles")
    if not article_ids:
        _DROPS.unit_empty("OH (no article links in TOC)")
        return []

    out: list[Section] = []
    for n in article_ids:
        url = OH_ARTICLE_URL_TMPL.format(n=n)
        try:
            html = fetch_text(url, use_us_proxy=True)
        except Exception as e:
            _DROPS.fetch_failed(f"OH art {n}", e)
            continue
        r2_html_key = f"state_constitutions/oh/source/article_{n}.html"
        put_if_changed(r2, r2_html_key, html.encode("utf-8"), "text/html; charset=utf-8")
        r2_html_url = public_url(r2_html_key)

        art_soup = BeautifulSoup(html, "html.parser")
        h1 = art_soup.find("h1")
        art_id, art_title = n, ""
        if h1:
            h1_text = h1.get_text(" ", strip=True)
            m = re.match(r"(?i)article\s+([IVXLC]+)\s*\|\s*(.*)", h1_text)
            if m:
                art_id, art_title = m.group(1), m.group(2).strip()

        table = art_soup.find("table", class_="laws-table")
        count_this_art = 0
        if table:
            for row in table.find_all("tr"):
                head = row.find("span", class_="content-head")
                body_div = row.find("div", class_="laws-body")
                if head is None or body_div is None:
                    continue
                head_text = re.sub(r"\s+", " ", head.get_text(" ", strip=True)).strip()
                parts = [p.strip() for p in head_text.split("|")]
                cite_part = parts[0] if parts else ""
                sec_title = parts[1] if len(parts) > 1 else ""
                sm = re.search(r"(?i)section\s+(\d+[A-Za-z]?)", cite_part)
                if not sm:
                    continue
                sec_num = sm.group(1)
                body = body_div.get_text(" ", strip=True)
                info = row.find("div", class_="laws-section-info")
                if info is not None:
                    info_text = re.sub(r"\s+", " ", info.get_text(" ", strip=True)).strip()
                    if info_text:
                        body = f"{body} {info_text}"
                sec = _emit_section(
                    "oh",
                    r2,
                    r2_html_url,
                    url,
                    art_id,
                    sec_num,
                    body,
                    section_title=(
                        f"Ohio Const. art. {art_id}, § {sec_num}. {sec_title}"
                        if sec_title
                        else None
                    ),
                    article_title=art_title,
                )
                if sec:
                    out.append(sec)
                    count_this_art += 1
        if count_this_art == 0:
            _DROPS.unit_empty(f"OH art {n} (0 sections)")
        print(f"  [OH art {art_id}] {count_this_art} sections")

    if not out:
        _DROPS.unit_empty("OH (0 sections after parse)")
    print(f"[OH] done: {len(out)} sections across {len(article_ids)} articles")
    return out


# ---------------------------------------------------------------------------
# Virginia -- replaces the Wikisource source (see _WS_INLINE_STATES' former
# "va" entry). law.lis.virginia.gov/constitution/ is the SAME LIS platform
# VA's own statute scraper (scrapeVA.py) already reaches with a plain,
# no-proxy BeautifulSoup fetch -- one page PER SECTION already
# (/constitution/article{N}/section{M}/), no article/section regex-splitting
# needed. Each section page's <span id="va_constitution"> holds exactly two
# <h2> tags (article heading, then section heading) followed by
# <section class="body"> with the real paragraph text -- confirmed live
# 2026-08-08. Article 13 ("Schedule") has no roman numeral in its heading;
# article_id falls back to the URL slug's digits for that one case. Reuses
# the existing _va_md_amendment_years extractor (already registered in
# _AMENDMENT_YEAR_EXTRACTORS for "va") unchanged -- this official source's
# "ratified <date>" phrasing is identical to what that extractor was built
# against.
# Wikisource baseline: 149 pts / 12 articles, verdict CLEAN.
# ---------------------------------------------------------------------------

VA_CONST_BASE = "https://law.lis.virginia.gov/constitution"
VA_ORIGIN = "https://law.lis.virginia.gov"
_VA_ARTICLE_HEADING_RE = re.compile(r"(?i)article\s+([IVXLC]+)\.?\s*(.*)")
_VA_SECTION_HEADING_RE = re.compile(r"(?i)section\s+([\dA-Za-z\-]+)\.?\s*(.*)")


def scrape_va(r2) -> list[Section]:
    try:
        toc_html = fetch_text(f"{VA_CONST_BASE}/")
    except Exception as e:
        _DROPS.fetch_failed("VA constitution TOC", e)
        return []
    r2_html_key = "state_constitutions/va/source/toc.html"
    put_if_changed(r2, r2_html_key, toc_html.encode("utf-8"), "text/html; charset=utf-8")
    r2_html_url = public_url(r2_html_key)

    soup = BeautifulSoup(toc_html, "html.parser")
    article_urls: list[tuple[str, str]] = []
    for a in soup.find_all("a", href=True):
        m = re.match(r"^/constitution/(article\d+)/$", a["href"])
        if m:
            article_urls.append((m.group(1), f"{VA_ORIGIN}{a['href']}"))

    specs: list[tuple[str, str]] = []
    for art_slug, art_url in article_urls:
        try:
            art_html = fetch_text(art_url)
        except Exception as e:
            _DROPS.fetch_failed(f"VA {art_slug} listing", e)
            continue
        art_soup = BeautifulSoup(art_html, "html.parser")
        for a in art_soup.find_all("a", href=True):
            if re.match(rf"^/constitution/{art_slug}/section[\w\-]+/$", a["href"]):
                specs.append((art_slug, f"{VA_ORIGIN}{a['href']}"))

    print(f"  [VA] discovered {len(specs)} section pages across {len(article_urls)} articles")
    if not specs:
        _DROPS.unit_empty("VA (no section pages discovered)")
        return []

    def _fetch_one(spec):
        art_slug, url = spec
        try:
            html = fetch_text(url)
        except Exception:
            return (art_slug, url, None)
        return (art_slug, url, html)

    with cf.ThreadPoolExecutor(max_workers=8) as ex:
        results = list(ex.map(_fetch_one, specs))

    n_failed = sum(1 for _, _, html in results if html is None)
    if n_failed:
        _DROPS.fetch_failed(
            f"VA ({n_failed} of {len(specs)} section pages)", "fetch failed", count=n_failed
        )

    out: list[Section] = []
    for art_slug, url, html in results:
        if html is None:
            continue
        soup = BeautifulSoup(html, "html.parser")
        span = soup.find("span", id="va_constitution")
        if span is None:
            continue
        headings = span.find_all("h2")
        if len(headings) < 2:
            continue
        art_text = headings[0].get_text(" ", strip=True)
        sec_text = headings[1].get_text(" ", strip=True)
        am = _VA_ARTICLE_HEADING_RE.match(art_text)
        if am:
            art_id, art_title = am.group(1), am.group(2).strip()
        else:
            art_id = re.sub(r"\D", "", art_slug) or art_slug
            art_title = art_text
        sm = _VA_SECTION_HEADING_RE.match(sec_text)
        if not sm:
            continue
        sec_num, sec_title = sm.group(1), sm.group(2).strip()
        body_section = span.find("section", class_="body")
        body = body_section.get_text(" ", strip=True) if body_section else ""
        sec = _emit_section(
            "va",
            r2,
            r2_html_url,
            url,
            art_id,
            sec_num,
            body,
            section_title=(
                f"Va. Const. art. {art_id}, § {sec_num}. {sec_title}" if sec_title else None
            ),
            article_title=art_title,
        )
        if sec:
            out.append(sec)

    if not out:
        _DROPS.unit_empty("VA (0 sections after parse)")
    print(f"[VA] done: {len(out)} sections")
    return out


# ---------------------------------------------------------------------------
# Wisconsin -- replaces the Wikisource source (see _WS_INLINE_STATES' former
# "wi" entry). docs.legis.wisconsin.gov/constitution/wi_unannotated serves a
# single PDF for the whole document (NOT the HTML the original batch research
# assumed from a cached preview -- confirmed live 2026-08-08 the URL's own
# Content-Type is application/pdf; docs.legis.wisconsin.gov/constitution/wi
# is real HTML but only a section-link index with no body text, and each
# per-section HTML page, e.g. /document/wisconsinconstitution/I,9m, turned
# out to render a whole PRINT PAGE of the PDF -- several neighboring sections
# at once, not just the one requested -- so the PDF is the cleaner
# single-fetch source). The PDF is a genuine two-column layout; plain
# single-column extraction interleaves lines from both columns and garbles
# the text, so this uses `pdf_to_text(..., columns=2)`.
#
# Even with columns=2, the first ~1-2 pages physically interleave the tail of
# the document's own table of contents (which lists every section under
# every article, left column) with the start of the real body text (right
# column) -- a real per-page 2-column layout artifact, not a bug in the
# extraction. `_wi_strip_toc_noise` removes the resulting bare "N.\nTitle.\n"
# TOC-entry runs (2+ in a row) and the standalone "Section" column-header
# line; what remains is real prose everywhere, so an ARTICLE-header match is
# trusted as a real boundary only when genuine section content (matched by
# `_WI_PDF_SECTION_RE`) starts within a short window after it -- the leftover
# bare "ARTICLE N.\nTITLE\n" headers (with no body immediately following, TOC
# artifacts of the same page-interleaving) are filtered out by that check
# rather than by position/gap heuristics, which proved unreliable against
# this specific two-column pagination. Verified live 2026-08-08: 14 real
# articles recovered cleanly in order (I through XIV).
#
# The PDF hyphenates line-wrapped words (confirmed live: "SEC-\nTION 15" for
# Section 15's own keyword), so dehyphenation runs BEFORE any other cleanup,
# same as NM/WA's own hyphen-wrap fixes.
#
# Every amended section carries a "[As amended|created ...]" bracket
# immediately after its own "SECTION N." marker -- this state's existing
# _WI_BRACKET_RE / _AMENDMENT_YEAR_EXTRACTORS["wi"] entry (built for the old
# Wikisource text) already matches this official source's identical bracket
# format unchanged.
# Wikisource baseline: 188 pts / 14 articles, verdict CLEAN.
# ---------------------------------------------------------------------------

WI_CONST_PDF = "https://docs.legis.wisconsin.gov/constitution/wi_unannotated"
_WI_HYPHEN_WRAP_RE = re.compile(r"([A-Za-z])-\n([A-Za-z])")
_WI_TOC_RUN_RE = re.compile(r"\n(?:[ \t]*\d+[A-Za-z]{0,2}\.[ \t]*\n[ \t]*[A-Z][^\n]{1,110}\n){2,}")
_WI_TOC_SECTION_HEADER_RE = re.compile(r"\n[ \t]*Section[ \t]*\n")
_WI_ARTICLE_RE = re.compile(r"\nARTICLE\s+([IVXLC]+)\.\s*\n([^\n]*)\n")
# Title group excludes "[" / "]": without that, a non-greedy leftmost-match
# scan can start INSIDE the PRECEDING section's own trailing "[... J.R. ...,
# vote <date>]" amendment-citation bracket (real titles never contain a
# bracket; amendment citations always do), producing a garbled section_title
# that splices citation text onto the next section's real title. Confirmed
# live 2026-08-08 on Art. V Sec. 8 ("J.R. 32, 1979 J.R. 3, vote April 1979]
# Secretary of state, when governor" instead of just "Secretary of state,
# when governor") -- raw_text/body is unaffected either way since it is
# sliced from the match's END, not its start; only section_title garbled.
_WI_PDF_SECTION_RE = re.compile(r"([A-Z][^\[\]]{1,160}?)\.\s+SECTION\s+(\d+[A-Za-z]{0,2})\.\s+")
# Every page break repeats a running footer ("Wisconsin Constitution updated
# by the Legislative Reference Bureau. Published <date>. Click for the
# Coverage of Annotations..." + "Report errors at ... lrb.legal@
# legis.wisconsin.gov") immediately followed by a running header ("ART. <rom>,
# S<sec>, WIS. CONSTITUTION") for the next page -- confirmed live 2026-08-08
# this leaks into a section_title whenever a page break falls between one
# section's trailing text and the next section's own title (raw_text is
# unaffected, same as the bracket-crossing fix above; this is section_title
# quality only). The column-merged extraction truncates the footer text
# inconsistently (mid-word, differently on different pages), so this anchors
# on the few short phrases that survive truncation intact rather than the
# footer's own start, and on the header's own stable "WIS. CONSTITUTION"
# suffix as the end of the block to strip.
_WI_RUNNING_HEADER_RE = re.compile(
    r"\n\s*[A-Za-z]+ \d{1,2}, \d{4}\.\s*\n\s*ART\.[^\n]*WIS\.\s*CONSTITUTION\s*\n"
)
_WI_FOOTER_BLOCK_RE = re.compile(
    r"(?:Report errors at[^\n]*\n|lrb\.legal@legis\.wisconsin\.gov\.?\n|Click for the Coverage of[^\n]*\n)"
    r"(?:[^\n]*\n){0,6}?"
    r"ART\.\s+[IVXLCM]+,\s*§[\w.]+,\s*WIS\.\s*CONSTITUTION\s*\n?"
)


def _wi_strip_toc_noise(text: str) -> str:
    text = _WI_TOC_RUN_RE.sub("\n", text)
    text = _WI_TOC_SECTION_HEADER_RE.sub("\n", text)
    text = _WI_RUNNING_HEADER_RE.sub("\n", text)
    text = _WI_FOOTER_BLOCK_RE.sub("\n", text)
    return text


def scrape_wi(r2) -> list[Section]:
    pdf_bytes = _fetch_bytes_proxy(WI_CONST_PDF)
    if not pdf_bytes:
        _DROPS.fetch_failed("WI constitution PDF", "download failed")
        return []
    r2_pdf_key = "state_constitutions/wi/source/wi_constitution.pdf"
    put_if_changed(r2, r2_pdf_key, pdf_bytes, "application/pdf")
    r2_pdf_url = public_url(r2_pdf_key)

    # Not _pdf_to_text: the WI print edition is a genuine two-column layout,
    # and single-column extraction interleaves the two columns line by line.
    try:
        text = pdf_to_text(pdf_bytes, columns=2)
    except PdfExtractionUnavailable:
        raise
    except Exception as e:
        print(f"  ! WI pdf extract failed: {e}")
        text = ""
    if not text:
        _DROPS.unit_empty("WI (PDF text extraction empty)")
        return []

    text = _WI_HYPHEN_WRAP_RE.sub(r"\1\2", text)
    cleaned = "\n" + _wi_strip_toc_noise(text)

    art_matches = list(_WI_ARTICLE_RE.finditer(cleaned))
    real: list[re.Match] = []
    for m in art_matches:
        window = cleaned[m.end() : m.end() + 200]
        if _WI_PDF_SECTION_RE.match(window) or _WI_PDF_SECTION_RE.search(window):
            real.append(m)
    if not real:
        _DROPS.unit_empty(f"WI (no real ARTICLE markers found in {len(cleaned)} chars)")
        return []

    out: list[Section] = []
    for i, m in enumerate(real):
        art_id = m.group(1)
        art_title = m.group(2).strip().rstrip(".").title()
        start = m.end()
        end = real[i + 1].start() if i + 1 < len(real) else len(cleaned)
        art_body = "\n" + cleaned[start:end]

        sec_matches = list(_WI_PDF_SECTION_RE.finditer(art_body))
        if not sec_matches:
            sec = _emit_section(
                "wi",
                r2,
                None,
                WI_CONST_PDF,
                art_id,
                "0",
                art_body,
                section_title=f"Wis. Const. art. {art_id}",
                r2_pdf_url=r2_pdf_url,
                article_title=art_title,
            )
            if sec:
                out.append(sec)
            continue

        count_this_art = 0
        # A repealed section's number is sometimes reused by a later
        # amendment (Art. VII Sec. 5: "Repealed April 1977" printed
        # immediately before the "Court of appeals" text created that same
        # amendment now carries under the same Sec. 5) -- both are real, so a
        # repeat gets the same "-vN" suffix convention already used for NV/UT
        # rather than colliding on one act_id.
        seen_nums: dict[str, int] = {}
        for j, sm in enumerate(sec_matches):
            sec_title_text = re.sub(r"\s+", " ", sm.group(1)).strip()
            sec_num_raw = sm.group(2)
            start_j = sm.end()
            end_j = sec_matches[j + 1].start() if j + 1 < len(sec_matches) else len(art_body)
            sec_body = art_body[start_j:end_j]
            seen_nums[sec_num_raw] = seen_nums.get(sec_num_raw, 0) + 1
            occurrence = seen_nums[sec_num_raw]
            sec_num = sec_num_raw if occurrence == 1 else f"{sec_num_raw}-v{occurrence}"
            sec = _emit_section(
                "wi",
                r2,
                None,
                WI_CONST_PDF,
                art_id,
                sec_num,
                sec_body,
                section_title=(
                    f"Wis. Const. art. {art_id}, § {sec_num_raw}. {sec_title_text}"
                    if sec_title_text
                    else None
                ),
                r2_pdf_url=r2_pdf_url,
                article_title=art_title,
            )
            if sec:
                out.append(sec)
                count_this_art += 1
        if count_this_art == 0:
            _DROPS.unit_empty(f"WI art {art_id} (0 sections)")
        print(f"  [WI art {art_id}] {count_this_art} sections")

    if not out:
        _DROPS.unit_empty("WI (0 sections after parse)")
    print(f"[WI] done: {len(out)} sections across {len(real)} articles")
    return out


# ---------------------------------------------------------------------------
# Nebraska -- replaces the Wikisource source (see _WS_INLINE_STATES' former
# "ne" entry). nebraskalegislature.gov/laws/browse-constitution.php lists
# every clause as its own pre-split page (articles.php?article=I-1, I-2,
# ...) -- the server has ALREADY done the article/section split, so this
# scraper is pure URL enumeration + fetch, no regex splitting at all
# (confirmed live 2026-08-08: 239 distinct clause codes incl. Preamble,
# matching this file's own earlier "238 of 238... Article <roman>-<digit>
# <letter?>" survey done for the Wikisource text -- this official source uses
# the identical "I-1" id scheme, just now as real per-clause URLs instead of
# inline headings). The `&print=true` view strips nav chrome down to
# <strong> (id/title) + <p> (body) + div.source (amendment/session-law
# citation, KEPT inline so _ne_amendment_years -- already registered for
# "ne" -- can find it) + div.anno (case-law annotations, EXCLUDED, not part
# of the constitution's own text). No article_title available from this
# source (the clause listing carries no separate per-article heading). Batch
# research expected no proxy, but a real run on the scraper box (2026-08-08)
# direct-connect-timed-out to nebraskalegislature.gov, so this uses the US
# proxy the same as the geo-blocked states elsewhere in this file.
# Wikisource baseline: 318 pts / 20 articles, verdict CLEAN.
# ---------------------------------------------------------------------------

NE_CONST_TOC = "https://nebraskalegislature.gov/laws/browse-constitution.php"
NE_ARTICLE_URL_TMPL = "https://nebraskalegislature.gov/laws/articles.php?article={code}&print=true"
_NE_CLAUSE_CODE_RE = re.compile(r"^([IVXLC]+)-(\d+[A-Za-z]?)$")


def scrape_ne(r2) -> list[Section]:
    try:
        toc_html = fetch_text(NE_CONST_TOC, use_us_proxy=True)
    except Exception as e:
        _DROPS.fetch_failed("NE constitution TOC", e)
        return []
    r2_html_key = "state_constitutions/ne/source/toc.html"
    put_if_changed(r2, r2_html_key, toc_html.encode("utf-8"), "text/html; charset=utf-8")
    r2_html_url = public_url(r2_html_key)

    soup = BeautifulSoup(toc_html, "html.parser")
    codes: list[str] = []
    seen: set[str] = set()
    for a in soup.find_all("a", href=True):
        if "print" in a["href"]:
            continue
        m = re.search(r"[?&]article=([^&]+)", a["href"])
        if not m:
            continue
        code = m.group(1)
        if (code == "Preamble" or _NE_CLAUSE_CODE_RE.match(code)) and code not in seen:
            seen.add(code)
            codes.append(code)

    print(f"  [NE] discovered {len(codes)} clause codes")
    if not codes:
        _DROPS.unit_empty("NE (no clause codes discovered)")
        return []

    def _fetch_one(code):
        url = NE_ARTICLE_URL_TMPL.format(code=code)
        try:
            html = fetch_text(url, use_us_proxy=True)
        except Exception:
            return (code, url, None)
        return (code, url, html)

    # nebraskalegislature.gov degrades badly under the usual 8-worker proxy
    # concurrency this file uses elsewhere -- confirmed live 2026-08-08, an
    # 8-worker first pass lost ~50% of 239 pages to transient failures (not a
    # real per-page block, since a lower-concurrency retry pass recovers most
    # of them). Run the bulk fetch gently (3 workers), then retry whatever
    # failed with even less concurrency (2 workers) before giving up for
    # real, rather than burning the whole run on one aggressive attempt.
    with cf.ThreadPoolExecutor(max_workers=3) as ex:
        results = list(ex.map(_fetch_one, codes))

    # Load on the shared US-rotating proxy pool (other concurrent scrapers,
    # not just this one) means even a gentle first pass can still leave a
    # sizeable tail failed -- confirmed live 2026-08-08, a 2-worker retry
    # alone still lost ~30% of 239 pages. Keep retrying at successively lower
    # concurrency, down to fully sequential, until a pass recovers nothing
    # more or everything succeeds, rather than giving up after one retry.
    for workers in (2, 1):
        retry_codes = [code for code, _, html in results if html is None]
        if not retry_codes:
            break
        print(f"  [NE] retrying {len(retry_codes)} failed pages at {workers}-worker concurrency")
        with cf.ThreadPoolExecutor(max_workers=workers) as ex:
            retry_results = list(ex.map(_fetch_one, retry_codes))
        recovered = sum(1 for _, _, html in retry_results if html is not None)
        retry_by_code = {code: (code, url, html) for code, url, html in retry_results}
        results = [retry_by_code.get(code, (code, url, html)) for code, url, html in results]
        if recovered == 0:
            break

    n_failed = sum(1 for _, _, html in results if html is None)
    if n_failed:
        _DROPS.fetch_failed(
            f"NE ({n_failed} of {len(codes)} clause pages)", "fetch failed", count=n_failed
        )

    out: list[Section] = []
    for code, url, html in results:
        if html is None:
            continue
        m = _NE_CLAUSE_CODE_RE.match(code)
        art_id, sec_num = (m.group(1), m.group(2)) if m else ("0", "0")
        page_soup = BeautifulSoup(html, "html.parser")
        strong = page_soup.find("strong")
        title_text = (
            re.sub(r"\s+", " ", strong.get_text(" ", strip=True)).strip() if strong else ""
        )
        title_text = re.sub(rf"^{re.escape(code)}\.?\s*", "", title_text).strip()

        for anno in page_soup.find_all("div", class_="anno"):
            anno.decompose()
        body_parts = [p.get_text(" ", strip=True) for p in page_soup.find_all("p")]
        source_div = page_soup.find("div", class_="source")
        if source_div is not None:
            body_parts.append(source_div.get_text(" ", strip=True))
        body = " ".join(p for p in body_parts if p)

        sec = _emit_section(
            "ne",
            r2,
            r2_html_url,
            url,
            art_id,
            sec_num,
            body,
            section_title=(
                f"Neb. Const. art. {art_id}, § {sec_num}. {title_text}" if title_text else None
            ),
        )
        if sec:
            out.append(sec)

    if not out:
        _DROPS.unit_empty("NE (0 sections after parse)")
    print(f"[NE] done: {len(out)} sections")
    return out


# ---------------------------------------------------------------------------
# Utah -- UT was never in _WS_INLINE_STATES (one of the 8 states this file's
# own earlier survey comment flags as genuinely unsampled, not merely
# unmigrated), so this is a new scraper rather than a Wikisource cutover.
# le.utah.gov/xcode/ is the SAME versioned wrapper/content-file CMS
# UT's own statute scraper (scrapeUT.py) already reaches -- Title/Chapter/
# Section there, Article/Section here (no chapter level in the
# constitution). Each level's WRAPPER page (e.g. .../ArticleI/Article_I.html)
# embeds an inline `versionArr` JS array naming the real content file
# (`UC_AI_....html`, same directory); the PARENT level's own #childtbl row
# already carries that filename in its href's `?v=` query string, so a child
# level's content file can be fetched DIRECTLY (`{dir}/{query_value}.html`)
# without a second round-trip through its own wrapper page first -- confirmed
# live 2026-08-08 this shortcut returns the identical #content div a full
# wrapper-then-versioned-file fetch would. Needs the US proxy (le.utah.gov
# TCP-times-out from non-US IPs, same as the statute scraper). Confirmed
# live: Preamble + 22 real articles (I-XVIII, XX, XXII-XXIV; XIX/XXI absent)
# each with its own real title ("Article I" -> "Declaration of Rights")
# straight from the top-level #childtbl. `_ut_amendment_years` reads each
# section's own "Effective M/D/YYYY" line (div#secdiv's first <b><i>),
# excluding Utah's own 1/1/1896 statehood date the same way OH/TX exclude
# their own founding dates elsewhere in this file.
# Wikisource baseline: 220 pts / 24 articles, verdict BUG_PRESENT.
# ---------------------------------------------------------------------------

UT_ORIGIN = "https://le.utah.gov"
UT_BASE = f"{UT_ORIGIN}/xcode"
UT_CONST_WRAPPER = f"{UT_BASE}/constitution.html"
_UT_VERSION_ARR_RE = re.compile(r"""versionArr\s*=\s*\[\s*\[\s*['"]([^'"]+\.html)['"]""")
_UT_EFFECTIVE_RE = re.compile(r"(?i)Effective\s+(\d{1,2})/(\d{1,2})/((?:19|20)\d{2})")


def _ut_amendment_years(raw_text: str) -> list[int]:
    years = []
    for _mo, _day, y in _UT_EFFECTIVE_RE.findall(raw_text):
        year = int(y)
        if year != 1896:
            years.append(year)
    return years


_AMENDMENT_YEAR_EXTRACTORS["ut"] = _ut_amendment_years


def _ut_version_query(href: str) -> str:
    _, _, query = href.partition("?")
    return query[2:] if query.startswith("v=") else query.rpartition("v=")[-1]


def _ut_content_soup(dir_url: str, version_query: str):
    """Fetch a child's content file directly using the parent #childtbl row's
    own `?v=` query value -- skips the wrapper-page round trip (see module
    note above)."""
    content_url = f"{dir_url}/{version_query}.html"
    html = fetch_text(content_url, use_us_proxy=True)
    soup = BeautifulSoup(html, "html.parser")
    content = soup.find(id="content")
    return (content, content_url) if content is not None else (None, content_url)


def scrape_ut(r2) -> list[Section]:
    try:
        wrapper_html = fetch_text(UT_CONST_WRAPPER, use_us_proxy=True)
    except Exception as e:
        _DROPS.fetch_failed("UT constitution wrapper", e)
        return []
    m = _UT_VERSION_ARR_RE.search(wrapper_html)
    if not m:
        _DROPS.fetch_failed("UT constitution wrapper", "no versionArr found")
        return []
    try:
        top_html = fetch_text(f"{UT_BASE}/{m.group(1)}", use_us_proxy=True)
    except Exception as e:
        _DROPS.fetch_failed("UT constitution top content", e)
        return []
    r2_html_key = "state_constitutions/ut/source/constitution.html"
    put_if_changed(r2, r2_html_key, top_html.encode("utf-8"), "text/html; charset=utf-8")
    r2_html_url = public_url(r2_html_key)

    top_soup = BeautifulSoup(top_html, "html.parser")
    childtbl = top_soup.find(id="childtbl")
    if childtbl is None:
        _DROPS.unit_empty("UT (no #childtbl in top content)")
        return []

    articles: list[tuple[str, str, str, str]] = []  # (art_id, art_title, dir_url, version_query)
    for row in childtbl.find_all("tr"):
        a = row.find("a", href=True)
        if a is None:
            continue
        label = a.get_text(" ", strip=True)
        m = re.match(r"(?i)article\s+([IVXLC]+)", label)
        if not m:
            continue  # skips Preamble -- no numbered sections to attribute it to
        art_id = m.group(1)
        tds = row.find_all("td")
        art_title = tds[1].get_text(" ", strip=True) if len(tds) > 1 else ""
        href_path = a["href"].split("?")[0]
        art_dir = href_path.rsplit("/", 1)[0]
        dir_url = f"{UT_ORIGIN}{art_dir}"
        articles.append((art_id, art_title, dir_url, _ut_version_query(a["href"])))

    print(f"  [UT] discovered {len(articles)} articles")
    if not articles:
        _DROPS.unit_empty("UT (no Article rows in top content)")
        return []

    out: list[Section] = []
    for art_id, art_title, dir_url, art_version in articles:
        try:
            art_content, art_content_url = _ut_content_soup(dir_url, art_version)
        except Exception as e:
            _DROPS.fetch_failed(f"UT art {art_id} content", e)
            continue
        if art_content is None:
            _DROPS.fetch_failed(f"UT art {art_id} content", "no #content div")
            continue
        sec_childtbl = art_content.find(id="childtbl")
        if sec_childtbl is None:
            _DROPS.unit_empty(f"UT art {art_id} (no section #childtbl)")
            continue

        section_specs: list[tuple[str, str]] = []  # (sec_num, version_query)
        for row in sec_childtbl.find_all("tr"):
            a = row.find("a", href=True)
            if a is None:
                continue
            sm = re.match(r"(?i)section\s+(\d+[A-Za-z]?)", a.get_text(" ", strip=True))
            if not sm:
                continue
            section_specs.append((sm.group(1), _ut_version_query(a["href"])))

        if not section_specs:
            # A handful of articles (e.g. Article III "Ordinance") are a
            # single undivided leaf page -- #childtbl exists but is empty,
            # the article's own real text sits directly in #content instead
            # of behind per-section rows. Emit the whole article as one
            # section rather than dropping it. Confirmed live 2026-08-08.
            body_copy = BeautifulSoup(str(art_content), "html.parser")
            for junk_id in ("childtbl", "topnavtbl", "parenttbl", "breadcrumb"):
                tag = body_copy.find(id=junk_id)
                if tag:
                    tag.decompose()
            body = body_copy.get_text(" ", strip=True)
            sec = _emit_section(
                "ut",
                r2,
                r2_html_url,
                art_content_url,
                art_id,
                "0",
                body,
                section_title=f"Utah Const. art. {art_id}",
                article_title=art_title,
            )
            if sec:
                out.append(sec)
                count_this_art = 1
            else:
                count_this_art = 0
            if count_this_art == 0:
                _DROPS.unit_empty(f"UT art {art_id} (0 sections)")
            print(f"  [UT art {art_id}] {count_this_art} sections (whole-article fallback)")
            continue

        def _fetch_one(spec):
            sec_num, sec_version = spec
            try:
                sec_content, sec_url = _ut_content_soup(dir_url, sec_version)
            except Exception:
                return (sec_num, None, None)
            return (sec_num, sec_content, sec_url)

        with cf.ThreadPoolExecutor(max_workers=8) as ex:
            results = list(ex.map(_fetch_one, section_specs))

        count_this_art = 0
        # A section number can legitimately repeat within one article: an
        # amendment not yet in force prints BOTH the currently-effective and
        # the not-yet-effective text under the same number ("Effective until
        # 11/23/2026" alongside "Effective 11/23/2026" -- confirmed live
        # 2026-08-08 for Art. VI Sec. 1 and Art. XXIII Sec. 1). Both are real,
        # separately citable text; a repeat gets NV's same "-vN" suffix
        # rather than colliding on one act_id.
        seen_nums: dict[str, int] = {}
        for sec_num, sec_content, sec_url in results:
            if sec_content is None:
                _DROPS.fetch_failed(f"UT art {art_id} sec {sec_num}", "fetch failed")
                continue
            secdiv = sec_content.find(id="secdiv")
            if secdiv is None:
                continue
            body_copy = BeautifulSoup(str(secdiv), "html.parser")
            bolds = body_copy.find_all("b")
            sec_title = bolds[1].get_text(" ", strip=True).strip("[]").strip() if len(bolds) >= 2 else ""
            body = body_copy.get_text(" ", strip=True)
            seen_nums[sec_num] = seen_nums.get(sec_num, 0) + 1
            occurrence = seen_nums[sec_num]
            emit_num = sec_num if occurrence == 1 else f"{sec_num}-v{occurrence}"
            sec = _emit_section(
                "ut",
                r2,
                r2_html_url,
                sec_url or art_content_url,
                art_id,
                emit_num,
                body,
                section_title=(
                    f"Utah Const. art. {art_id}, § {sec_num}. {sec_title}" if sec_title else None
                ),
                article_title=art_title,
            )
            if sec:
                out.append(sec)
                count_this_art += 1
        if count_this_art == 0:
            _DROPS.unit_empty(f"UT art {art_id} (0 sections)")
        print(f"  [UT art {art_id}] {count_this_art} sections")

    if not out:
        _DROPS.unit_empty("UT (0 sections after parse)")
    print(f"[UT] done: {len(out)} sections across {len(articles)} articles")
    return out


# ---------------------------------------------------------------------------
# Hawaii -- replaces the Wikisource source (see _WS_INLINE_STATES' former "hi"
# entry, 186 pts / 18 articles, verdict BUG_PRESENT per the C07 batch 4
# research). capitol.hawaii.gov serves the constitution the same way it
# serves the HRS statutes -- NOT the single TOC-plus-body page the batch
# research assumed: a TOC page (CONST_.htm) links to the FIRST section, then
# one page per section, walked via sequential "Next" navigation -- see
# scrapeHI.py (the existing HI statute scraper) for the identical, already-
# proven pattern against this same domain. Confirmed live 2026-08-08.
#
# Each section page uses semantic CSS classes that make extraction exact
# rather than heuristic: <p class="RegularParagraphs"> holds the real
# article/title/catchline/body text; <p class="XNotesHeading">/<p
# class="XNotes"> hold "Law Journals and Reviews"/"Case Notes"/"Attorney
# General Opinions" annotations, excluded by construction (never selected).
# Within RegularParagraphs, the centered (align="center") paragraphs are
# headings (ARTICLE N, the article's own title, the section's catchline);
# the first non-centered paragraph is the real "Section N. <body text>" and
# any further non-centered paragraphs on the same page continue that same
# section's body. Confirmed live 2026-08-08 across Article I.
#
# The page declares charset=utf-8 in a <meta> tag but the server sends no
# Content-Type charset header, so plain `requests` defaults to Latin-1 and
# every multi-byte character comes back mojibake'd (confirmed: a plain
# non-breaking space is served, when decoded correctly, as the two
# characters "Â\xa0" -- i.e. the page is already double-UTF-8-encoded).
# Fixed the same way scrapeHI.py's own `_fix_encoding` does: decode as UTF-8
# once, then re-decode that string as Latin-1 bytes through UTF-8 again.
#
# Amendment history is inline and bracketed on every amended section, e.g.
# "[Am Const Con 1978 and election Nov 7, 1978]", "[L 1972, SB No 1408-72
# and election Nov 7, 1972; ren Const Con 1978 and election Nov 7, 1978]" --
# verified live 2026-08-08 across Article I. This supersedes the Wikisource-
# era note in the _AMENDMENT_YEAR_EXTRACTORS docstring above ("KS, DE, HI,
# IL, MS, NV have no per-section annotation of any kind") -- that assessment
# was of the OLD Wikisource page's format; the new official source has its
# own clean, consistent bracket format.
# ---------------------------------------------------------------------------

HI_CONST_TOC = "https://capitol.hawaii.gov/hrscurrent/Vol01_Ch0001-0042F/05-CONST/CONST_.htm"
_HI_ARTICLE_RE = re.compile(r"^ARTICLE\s+([IVXLC]+)$")
# The number is sometimes bracketed ("Section [24].", "Section [11]." -- both
# confirmed live 2026-08-08, no discernible pattern distinguishing which
# sections get brackets and which don't, so both forms are accepted rather
# than treated as two different things) and sometimes has a stray space
# before the period ("Section 6 ."). `\s*\.` (not a bare `\.`) covers the
# latter; `\[?...\]?` covers the former.
_HI_SECTION_RE = re.compile(r"Section\s*\[?(\d+(?:\.\d+)?[A-Za-z]?)\]?\s*\.\s*(.*)", re.DOTALL)
_HI_BRACKET_RE = re.compile(r"\[[^\[\]]{0,300}\]")
_HI_NEXT_RE = re.compile(r"^\s*next\s*$", re.IGNORECASE)


def _hi_amendment_years(raw_text: str) -> list[int]:
    return _years_in_matched_spans(raw_text, _HI_BRACKET_RE)


_AMENDMENT_YEAR_EXTRACTORS["hi"] = _hi_amendment_years


def _hi_fix_encoding(raw_bytes: bytes) -> str:
    html = raw_bytes.decode("utf-8", errors="ignore")
    return html.encode("latin-1", errors="ignore").decode("utf-8", errors="ignore")


def _hi_fetch(url: str, retries: int = 4) -> str | None:
    headers = {"User-Agent": _MOZ_UA}
    for attempt in range(retries):
        try:
            r = SESSION.get(url, timeout=30, headers=headers)
            if r.status_code == 200:
                return _hi_fix_encoding(r.content)
        except Exception:
            pass
        time.sleep(max(1.0, 0.5 * (2**attempt)))
    return None


def _hi_find_next(soup: BeautifulSoup, base_url: str) -> str | None:
    for a in soup.find_all("a", href=True):
        if _HI_NEXT_RE.match(a.get_text(strip=True)):
            return requests.compat.urljoin(base_url, a["href"])
    return None


def scrape_hi(r2) -> list[Section]:
    toc_html = _hi_fetch(HI_CONST_TOC)
    if toc_html is None:
        _DROPS.fetch_failed("HI TOC", "fetch failed")
        return []
    r2_html_key = "state_constitutions/hi/source/hi_constitution_toc.html"
    put_if_changed(r2, r2_html_key, toc_html.encode("utf-8"), "text/html; charset=utf-8")
    r2_html_url = public_url(r2_html_key)

    soup = BeautifulSoup(toc_html, "html.parser")
    url = _hi_find_next(soup, HI_CONST_TOC)
    if not url:
        _DROPS.unit_empty("HI (no first section link from TOC)")
        return []
    # Chapter-runaway guard, same fix already proven in scrapeHI.py's own
    # `_scrape_chapter_sections`: capitol.hawaii.gov's sequential "Next"
    # navigation does not stop at the end of the constitution -- it walks
    # straight into whatever HRS statute chapter the site's own page
    # ordering places immediately after it (confirmed live 2026-08-08: an
    # unguarded crawl ran past the real ~230 constitution pages into
    # HRS0121-0128A statute pages, thousands of pages deep, before being
    # caught). Every real constitution section URL lives under the SAME
    # `.../05-CONST/` directory as the first one; stop the instant a "Next"
    # link leaves that prefix rather than trusting the site to terminate the
    # chain itself.
    const_dir_prefix = url.rsplit("/", 1)[0] + "/"

    out: list[Section] = []
    current_article = ""
    current_article_title = ""
    seen_urls: set[str] = set()
    page_count = 0
    # A long section's body can itself span several physical pages, linked by
    # the SAME "Next" chain and carrying no "Section N." heading of their own
    # on the continuation pages (e.g. Article XVI Sec. 3's salary-commission
    # text runs to a "..._0016-0003_0005.htm" fifth part) -- confirmed live
    # 2026-08-08: an earlier version of this scraper treated every page as
    # independent, so `_HI_SECTION_RE` failing to match a continuation page
    # silently DISCARDED that page's real text instead of recognizing it as
    # more of the section already open. Pending state is buffered across
    # pages and only finalized (`_pending_flush`) when a NEW "Section N."
    # marker is found or the crawl ends, so a continuation page's text is
    # appended to the section it belongs to rather than lost.
    pending_article = ""
    pending_article_title = ""
    pending_num = ""
    pending_url = ""
    pending_parts: list[str] = []

    def _pending_flush() -> None:
        if not pending_num:
            return
        sec = _emit_section(
            "hi",
            r2,
            r2_html_url,
            pending_url,
            pending_article,
            pending_num,
            " ".join(pending_parts),
            article_title=pending_article_title,
        )
        if sec:
            out.append(sec)

    while url and url not in seen_urls and url.startswith(const_dir_prefix):
        seen_urls.add(url)
        page_count += 1
        # retries=10 (not the default 4) deliberately -- this crawl only
        # discovers page N+1's URL by successfully parsing page N's "Next"
        # link, so ANY single page's fetch failure here silently truncates
        # the WHOLE REST of the ~230-page chain, not just that one page.
        # Confirmed live 2026-08-08: two consecutive full runs each lost
        # >90% of the crawl (17 then 14 of ~170 real sections) to one
        # transient failure at a different, effectively random page each
        # time -- the site/proxy combination is flaky enough that even a
        # high per-page success rate compounds into near-certain data loss
        # across 230 sequential attempts at the default retry budget.
        html = _hi_fetch(url, retries=10)
        if html is None:
            _DROPS.fetch_failed(f"HI page {page_count} ({url})", "fetch failed")
            break
        psoup = BeautifulSoup(html, "html.parser")
        paras = psoup.find_all("p", class_="RegularParagraphs")

        heading_lines: list[str] = []
        body_paras: list[str] = []
        for p in paras:
            text = p.get_text(" ", strip=True).replace("\xa0", " ").strip()
            if not text:
                continue
            is_centered = p.get("align") == "center" or "center" in (p.get("style") or "")
            if is_centered and not body_paras:
                heading_lines.append(text)
            else:
                body_paras.append(text)

        for line in heading_lines:
            m = _HI_ARTICLE_RE.match(line)
            if m:
                current_article = m.group(1)
                current_article_title = ""
            elif current_article and not current_article_title and line != current_article:
                current_article_title = (
                    f"{current_article_title} {line}".strip() if current_article_title else line
                )

        body_text = re.sub(r"\s+", " ", " ".join(body_paras)).strip()
        sm = _HI_SECTION_RE.match(body_text)
        if sm and current_article:
            _pending_flush()
            pending_article = current_article
            pending_article_title = current_article_title.strip()
            pending_num = sm.group(1)
            pending_url = url
            pending_parts = [sm.group(2).strip()]
        elif pending_num and current_article == pending_article:
            # No "Section N." marker on this page -- a continuation of the
            # currently-open section, not a new, empty unit.
            if body_text:
                pending_parts.append(body_text)
        elif url.endswith("-.htm"):
            # A chapter/article INDEX page, not content -- the same "-.htm"
            # boundary convention scrapeHI.py's own statute crawler already
            # recognizes and never treats as a content drop. Confirmed live
            # 2026-08-08: Article XIV ("Code of Ethics") has genuinely zero
            # sections in force; CONST_0014-.htm is its own empty chapter
            # marker, not a lost section.
            pass
        else:
            _DROPS.unit_empty(f"HI page {page_count} ({url}, no Section marker)")

        url = _hi_find_next(psoup, url)

    _pending_flush()
    if url and not url.startswith(const_dir_prefix):
        print(f"  [HI] stopped: Next link left the constitution ({url})")
    if not out:
        _DROPS.unit_empty("HI (0 sections after crawl)")
    print(f"[HI] done: {len(out)} sections across {page_count} pages")
    return out


# ---------------------------------------------------------------------------
# Kansas -- replaces the Wikisource source (see _WS_INLINE_STATES' former
# "ks" entry / _SECTION_SPLIT_OVERRIDES' former "ks" entry -- Wikisource used
# a colon form "§ N:" that no longer applies to this source; 245 pts / 16
# articles, verdict BUG_PRESENT). sos.ks.gov (the Secretary of State's own
# publications site, NOT kslegislature.gov -- a different domain from KS's
# statute scraper, so this is new fetch code) serves the constitution as 18
# per-document pages: Ordinance and Preamble, Bill of Rights, Articles 1-15,
# and Schedule and Resolutions (confirmed live 2026-08-08 against the site's
# own kansas-constitution.html index).
#
# Every page shares one template: <div class="page-content"> holds an
# <h3 class="constitution-subheading"> naming the document/article, then a
# flat run of <p> tags -- most (not all -- see below) carry
# class="constitution-paragraph" for body text or class="constitution-history"
# for a trailing "History: Adopted by convention...; ratified by
# electors...; L. YEAR, ch. N...; <amendment election date>." line per
# section. The "constitution-paragraph" class is NOT reliable as a body-text
# filter by itself: KS Article 1 Sec. 6's own (b) and (c) subsection
# paragraphs carry NO class at all (verified live 2026-08-08 by diffing the
# section's real official text -- (a)-(d) -- against what a class-only
# selector would return -- (a) and (d) only, silently dropping the middle of
# the section). This walks every <p> in document order instead and tracks
# section boundaries by content ("§ N." at the start of the paragraph), not
# by CSS class -- classless paragraphs simply continue whatever section is
# currently open, which is exactly right for these orphaned subsections.
#
# The Ordinance and Preamble page bundles two logically separate documents:
# the 1859 Ordinance's own 8 numbered sections (a real "§ N." series, cited
# here as article_id="ORD"), preceded by its own unnumbered enacting-clause
# recital ("WHEREAS...Be it ordained by the people of Kansas:...", captured
# as ORD's own section "0" rather than discarded), and -- textually AFTER
# the Ordinance's Section 8, not before it -- the actual constitutional
# Preamble ("We, the people of Kansas, grateful to Almighty God...").
# Verified live 2026-08-08: the Preamble is the LAST paragraph on this page,
# with no marker of its own other than its own well-known opening words, so
# it is detected by that phrase and split off into its own article_id="0"
# record (the scrape_mn/scrape_az/scrape_nc preamble convention), not folded
# into the Ordinance's Section 8.
#
# Encoding: same class of bug as HI (server declares no Content-Type charset
# so plain `requests` defaults to Latin-1 against real UTF-8 content, e.g.
# "§" comes back as "Â§") -- fixed by decoding the raw response bytes as
# UTF-8 directly rather than trusting `requests`' auto-detected `.encoding`.
# Unlike HI this is a single mis-decode, not a double-encode, so one
# `.content.decode("utf-8")` is enough (no Latin-1-then-UTF-8 round trip).
#
# Amendment years: the History line's trailing "; <Month> <Day>, <Year>."
# (present only when a section was actually amended -- an unamended
# section's History ends at its original "L. 1861, p. NN." citation with no
# further trailing date) is the real amendment/ratification election date.
# The "L. 1861, p. NN." citation itself is excluded deliberately: it is the
# ORIGINAL 1861 session-laws publication citation, present on every section
# including ones never amended since, the same "bare enactment date, not an
# amendment" pitfall already handled for TX (1876)/NE ("Adopted in Y") above
# -- so this keys on the DATE at the very end of the History text, not on
# any "L. YYYY" citation appearing earlier in it. Verified live 2026-08-08
# against all 16 History lines on Article 1 (10 amended in 1972, 6
# unamended, zero false positives either direction).
# ---------------------------------------------------------------------------

_KS_ROMAN = [
    "I", "II", "III", "IV", "V", "VI", "VII", "VIII",
    "IX", "X", "XI", "XII", "XIII", "XIV", "XV",
]  # fmt: skip
_KS_SECTION_HEAD_RE = re.compile(r"^§\s*(\d+[A-Za-z]?)\.\s*(.*)", re.DOTALL)
_KS_PREAMBLE_TRIGGER_RE = re.compile(r"^we,\s+the\s+people\s+of\s+kansas", re.IGNORECASE)
_KS_HISTORY_TRAILING_DATE_RE = re.compile(r";\s*[A-Za-z]+\.?\s+\d{1,2},\s*(?:19|20)\d{2}\.\s*$")


def _ks_amendment_years(raw_text: str) -> list[int]:
    return _years_in_matched_spans(raw_text, _KS_HISTORY_TRAILING_DATE_RE)


_AMENDMENT_YEAR_EXTRACTORS["ks"] = _ks_amendment_years


def _ks_page_title(soup: BeautifulSoup) -> str:
    h3 = soup.find("h3", class_="constitution-subheading")
    if not h3:
        return ""
    text = h3.get_text(" ", strip=True)
    return text.split(" - ", 1)[1].strip() if " - " in text else text


def _ks_fetch_page(slug: str) -> tuple[str, BeautifulSoup] | None:
    url = f"https://sos.ks.gov/publications/kansas-constitution/{slug}"
    try:
        r = SESSION.get(url, timeout=30, headers={"User-Agent": _MOZ_UA})
        r.raise_for_status()
    except Exception:
        return None
    html = r.content.decode("utf-8", errors="ignore")
    return html, BeautifulSoup(html, "html.parser")


def scrape_ks(r2) -> list[Section]:
    pages: list[tuple[str, str]] = [
        ("kansas-constitution-ordinance-and-preamble.html", "ORD"),
        ("kansas-constitution-bill-of-rights.html", "BOR"),
        *((f"kansas-constitution-article-{n}.html", roman) for n, roman in enumerate(_KS_ROMAN, start=1)),
        ("kansas-constitution-schedule-and-resolutions.html", "SCHEDULE"),
    ]

    out: list[Section] = []
    for slug, art_id in pages:
        fetched = _ks_fetch_page(slug)
        if fetched is None:
            _DROPS.fetch_failed(f"KS {slug}", "fetch failed")
            continue
        html, soup = fetched
        r2_html_key = f"state_constitutions/ks/source/{slug}"
        put_if_changed(r2, r2_html_key, html.encode("utf-8"), "text/html; charset=utf-8")
        r2_html_url = public_url(r2_html_key)
        url = f"https://sos.ks.gov/publications/kansas-constitution/{slug}"

        container = soup.find("div", class_="page-content")
        if container is None:
            _DROPS.unit_empty(f"KS {slug} (no page-content container)")
            continue
        page_title = _ks_page_title(soup)

        cur_art = art_id
        cur_title = page_title
        cur_num: str | None = None
        cur_parts: list[str] = []
        count_this_page = 0

        def _flush():
            nonlocal count_this_page
            if cur_num is None:
                return
            body = " ".join(cur_parts).strip()
            sec = _emit_section(
                "ks", r2, r2_html_url, url, cur_art, cur_num, body, article_title=cur_title
            )
            if sec:
                out.append(sec)
                count_this_page += 1

        for p in container.find_all("p"):
            text = re.sub(r"\s+", " ", p.get_text(" ", strip=True)).strip()
            if not text:
                continue
            classes = p.get("class") or []
            if "constitution-history" in classes:
                cur_parts.append(text)
                continue
            m = _KS_SECTION_HEAD_RE.match(text)
            if m:
                _flush()
                cur_num = m.group(1)
                cur_parts = [m.group(2).strip()]
                continue
            if art_id == "ORD" and _KS_PREAMBLE_TRIGGER_RE.match(text):
                _flush()
                cur_art, cur_title, cur_num, cur_parts = "0", "", "0", [text]
                continue
            if cur_num is None:
                # Preface text before the first "§ N." marker -- only the
                # Ordinance page has this (its enacting-clause recital); real
                # constitutional text, kept as its own section rather than
                # silently discarded.
                cur_num, cur_parts = "0", [text]
                continue
            cur_parts.append(text)
        _flush()

        if count_this_page == 0:
            _DROPS.unit_empty(f"KS {slug} (0 sections parsed)")
        print(f"  [KS {slug}] {count_this_page} sections")

    if not out:
        _DROPS.unit_empty("KS (0 sections after parse)")
    print(f"[KS] done: {len(out)} sections across {len(pages)} pages")
    return out


# ---------------------------------------------------------------------------
# Missouri -- replaces the Wikisource source (see _WS_INLINE_STATES' former
# "mo" entry, 360 pts / 13 articles, verdict CLEAN -- CLEAN just means no
# section-split bug, not complete: Wikisource has no Article XIV, ratified
# 2022, at all). revisor.mo.gov (the SAME ASP.NET site MO's own statute
# scraper already uses, `constit=y` query-param convention) serves the
# constitution one section per OneSection.aspx page -- confirmed live
# 2026-08-08, reachable through the US proxy (direct fetch from a
# non-US IP times out/connection-refuses, matching the statute scraper's own
# proxy requirement for this domain).
#
# The batch handoff's own warning -- "old scraper read 1st of ~8
# tables/chapter" -- is why this does NOT walk the page's HTML tables at
# all. Instead: the Home.aspx?constit=y index page links every real section
# via two anchor families in document order -- `ViewChapter.aspx?chapter=N`
# (14 article headings, e.g. "Article I  BILL OF RIGHTS") and
# `PageSelect.aspx?section=<label>&bid=<id>` (402 section links, each
# redirecting to its own `OneSection.aspx?section=...&bid=...&constit=y`,
# fetched directly here to skip the redirect hop). Confirmed live
# 2026-08-08: 402 real sections across all 14 articles (I=39 ... XIV=2),
# zero MO-side gaps; Article XIV (Marijuana Use and Regulation, the 2022
# amendment) is the one article Wikisource never had at all. Section labels
# are not always a bare integer -- MO's amendment-heavy articles (III, IV,
# V) use parenthetical letter/digit suffixes for sections added alongside an
# existing one ("18(a)", "37(a)".."37(h)", even the doubly-nested "25(c)(1)")
# -- kept as literal section_number strings rather than forced into a purely
# numeric shape, since that IS the citation form MO itself uses.
#
# Each OneSection.aspx page's real content lives in one predictable DOM
# shape (confirmed live 2026-08-08, verified against both a 1-paragraph and
# a 9-paragraph section): a `<span id="effdt">` banner ("Effective -  27 Feb
# 1945 ,  see footnote") immediately precedes a `<div class="norm"
# style="background-color:#fffff7...">` container holding one or more direct
# `<p class="norm">` body paragraphs -- the first one opens with a nested
# `<span class="bold">` carrying "<Article> Section <N>.  <Catchline>. —"
# (decomposed out to isolate the catchline from the real body text that
# follows it as the same `<p>`'s trailing text). A separate `<div
# class="foot">` sibling (its OWN "Source: Const. of 1875, Art. II, § 1."
# citation, describing the PREDECESSOR 1875 document, not this text's own
# amendment history -- the same "wrong document's history" pitfall already
# flagged for MO in the _AMENDMENT_YEAR_EXTRACTORS docstring above) is a
# sibling of the content container, never a `<p class="norm">` DIRECT child
# of it, so walking `find_all("p", class_="norm", recursive=False)` on the
# content container excludes it by construction, not by text-pattern
# filtering.
#
# Amendment years: unlike the predecessor-document "Source:" footnote (still
# correctly unused, per the reasoning above), the `<span id="effdt">` banner
# is this exact section's OWN current effective date, and it genuinely moves
# when a section is later amended (verified live 2026-08-08: Art. I Sec. 5
# shows "Effective -  06 Sep 2012", matching its real 2012 religious-freedom
# amendment). 1945-02-27 is the whole document's original convention-adopted
# date and appears on every never-since-amended section, so it is excluded
# the same way TX's bare 1876/NE's bare "Adopted in Y" are excluded above --
# only a DIFFERENT year than 1945 is treated as a real amendment. The one
# known inexact case: Article XIV's own effdt (2022) is that article's
# original adoption, not an amendment to pre-existing text -- accepted as a
# minor, documented imprecision (a genuinely new article's own creation date
# is not a meaningfully different fact from "the current text took effect
# in Y" for a faceting field), not worth a special case for one article.
# ---------------------------------------------------------------------------

MO_CONST_INDEX = "https://revisor.mo.gov/main/Home.aspx?constit=y"
_MO_ARTICLE_LINK_RE = re.compile(r"ViewChapter\.aspx\?chapter=([^&]+)")
_MO_SECTION_LINK_RE = re.compile(r"PageSelect\.aspx\?section=([^&]+)&bid=(\d+)")
_MO_ARTICLE_TITLE_RE = re.compile(r"^Article\s+[IVXLC]+\s+(.+)$")
_MO_EFFDT_RE = re.compile(r"Effective\s*-\s*\d{1,2}\s+\w+\s+((?:19|20)\d{2})")
_MO_HEADING_CATCHLINE_RE = re.compile(r"^[IVXLC]+\s+Section\s+[\w().]+\.\s*(.*?)\s*—?\s*$")


def _mo_amendment_years(raw_text: str) -> list[int]:
    years = [int(y) for y in _MO_EFFDT_RE.findall(raw_text)]
    return [y for y in years if y != 1945]


_AMENDMENT_YEAR_EXTRACTORS["mo"] = _mo_amendment_years


def _mo_fetch(url: str, retries: int = 4) -> str | None:
    proxies = _us_proxies()
    headers = {"User-Agent": _MOZ_UA}
    for attempt in range(retries):
        try:
            r = SESSION.get(url, timeout=45, headers=headers, proxies=proxies)
            if r.status_code == 200:
                return r.text
        except Exception:
            pass
        time.sleep(max(1.0, 0.5 * (2**attempt)))
    return None


def _mo_discover_sections() -> tuple[list[tuple[str, str, str]], dict[str, str]]:
    """([(article_id, section_label, bid), ...], {article_id: title})."""
    html = _mo_fetch(MO_CONST_INDEX)
    if html is None:
        _DROPS.fetch_failed("MO constitution index", "fetch failed")
        return [], {}
    soup = BeautifulSoup(html, "html.parser")
    titles: dict[str, str] = {}
    specs: list[tuple[str, str, str]] = []
    current_art: str | None = None
    for a in soup.find_all("a", href=True):
        href = a["href"]
        m_art = _MO_ARTICLE_LINK_RE.search(href)
        if m_art:
            current_art = m_art.group(1)
            m_title = _MO_ARTICLE_TITLE_RE.match(a.get_text(" ", strip=True))
            if m_title:
                titles[current_art] = m_title.group(1).strip().title()
            continue
        m_sec = _MO_SECTION_LINK_RE.search(href)
        if m_sec and current_art:
            label = m_sec.group(1).strip()
            sec_num = label[len(current_art) :].strip() if label.startswith(current_art) else label
            specs.append((current_art, sec_num, m_sec.group(2)))
    return specs, titles


def _mo_fetch_section(spec: tuple[str, str, str]) -> tuple[str, str, str, str, str] | None:
    """(article_id, section_number, url, catchline, body_text) or None."""
    art_id, sec_num, bid = spec
    label = f"{art_id}    {sec_num}"
    url = f"https://revisor.mo.gov/main/OneSection.aspx?section={quote(label)}&bid={bid}&constit=y"
    html = _mo_fetch(url)
    if html is None:
        return None
    soup = BeautifulSoup(html, "html.parser")
    effdt = soup.find("span", id="effdt")
    effdt_text = effdt.get_text(" ", strip=True) if effdt else ""
    wrap = effdt
    while wrap is not None and wrap.name != "div":
        wrap = wrap.parent
    container = wrap.find_next_sibling("div", class_="norm") if wrap else None
    if container is None:
        return None

    catchline = ""
    body_parts: list[str] = []
    for i, p in enumerate(container.find_all("p", class_="norm", recursive=False)):
        bold = p.find("span", class_="bold")
        if bold is not None:
            heading_text = re.sub(r"\s+", " ", bold.get_text(" ", strip=True)).strip()
            m = _MO_HEADING_CATCHLINE_RE.match(heading_text)
            catchline = m.group(1).strip().rstrip(".") if m else ""
            bold.decompose()
        text = re.sub(r"\s+", " ", p.get_text(" ", strip=True)).strip()
        if text:
            body_parts.append(text)
    body_text = " ".join(body_parts).strip()
    if effdt_text:
        body_text = f"{body_text} [{effdt_text}]"
    if not body_text:
        return None
    # The TOC's ViewChapter grouping lumps the constitution's one surviving
    # transitional "Schedule" clause (Supersession of prior constitutional
    # provisions) in under Article XII's own link block, restarting at
    # "Section 1" -- a real, site-native label collision against Article
    # XII's own genuine Section 1 (verified live 2026-08-08: two distinct
    # bids, "XII    1", one catchline "Limitation on revision and amendment"
    # -- the real Art. XII Sec. 1 -- the other literally prefixed
    # "SCHEDULE—" in the source's own text). Detected by that literal
    # prefix and rerouted to its own article_id rather than silently
    # colliding on (XII, 1).
    if catchline.startswith("SCHEDULE"):
        art_id = "SCHEDULE"
        catchline = re.sub(r"^SCHEDULE\s*[—-]\s*", "", catchline).strip()
    return (art_id, sec_num, url, catchline, body_text)


def scrape_mo(r2) -> list[Section]:
    specs, article_titles = _mo_discover_sections()
    if not specs:
        _DROPS.unit_empty("MO (no section specs discovered from index)")
        return []
    print(f"  [MO] discovered {len(specs)} sections across {len(article_titles)} articles")

    with cf.ThreadPoolExecutor(max_workers=8) as ex:
        results = list(ex.map(_mo_fetch_section, specs))

    n_failed = sum(1 for r in results if r is None)
    if n_failed:
        _DROPS.fetch_failed(f"MO ({n_failed} of {len(specs)} section pages)", "fetch/parse", count=n_failed)
    print(f"  [MO] fetched {len(specs) - n_failed} / {len(specs)} section pages")

    out: list[Section] = []
    for result in results:
        if result is None:
            continue
        art_id, sec_num, url, catchline, body_text = result
        sec_title = f"Mo. Const. art. {art_id}, § {sec_num}" + (f". {catchline}" if catchline else "")
        sec = _emit_section(
            "mo",
            r2,
            None,
            url,
            art_id,
            sec_num,
            body_text,
            section_title=sec_title,
            article_title=article_titles.get(art_id, ""),
        )
        if sec:
            out.append(sec)

    if not out:
        _DROPS.unit_empty("MO (0 sections after fetch)")
    print(f"[MO] done: {len(out)} sections")
    return out


# ---------------------------------------------------------------------------
# Idaho -- replaces the Wikisource source (see _WS_INLINE_STATES' former "id"
# entry, 248 pts / 21 articles, verdict CLEAN -- CLEAN means no split bug,
# not that this is skippable; official provenance is still the point of this
# whole migration). legislature.idaho.gov/statutesrules/idconst/ -> 21
# article index pages -> one page per section, confirmed live 2026-08-08.
# `scrapeID.py` (the existing Idaho STATUTE scraper) already implements this
# exact title/chapter -> section, one-page-per-leaf crawl against a sibling
# URL branch (`/idstat/` vs `/idconst/`) on the SAME domain -- this reuses
# its two load-bearing findings directly rather than rediscovering them:
#
# 1. **Needs the US proxy.** A direct fetch from a non-US IP connection-
#    refuses (confirmed live 2026-08-08, same as the statute scraper).
# 2. **Section text lives in a `div.pgbrk`**, whose real content is legacy
#    Arbortext-to-HTML output pasted in near-verbatim (a stray nested
#    `<html><head>...` document-within-a-document, its own `<style>`/
#    `<script>` blocks, all inside one outer `<p>` -- BeautifulSoup's
#    html.parser tolerates the broken nesting and still finds the real
#    content). `scrapeID.py`'s statute pages skip 4 leading breadcrumb
#    `<div>`s inside `pgbrk` before the real content starts; the
#    constitution's `pgbrk` has no such breadcrumb divs at all -- confirmed
#    live 2026-08-08 across Article I Sec. 1, Article V Sec. 2, and Article
#    VIII Sec. 3 (the state debt-limitation section, one of the longest) --
#    `pgbrk` holds exactly one direct child div, the section's own content,
#    every time. The section's catchline is its own nested
#    `<span style="text-transform: uppercase">` -- decomposed out (DOM-based,
#    not regex-guessed, since the catchline's own text can itself contain a
#    period that would break a naive "split on first period" regex) to
#    isolate it from the body text that follows as the same span's plain
#    trailing text.
#
# No amendment-history note of any kind appears in-body on any of the three
# sampled sections above (long or short) -- consistent with the existing
# _AMENDMENT_YEAR_EXTRACTORS docstring's ID assessment ("mix several
# notation styles within the same document, most sections undated"); no
# extractor registered for "id", the same explicit non-attempt as before,
# now re-confirmed against the official source rather than assumed carried
# over from the Wikisource-era finding.
# ---------------------------------------------------------------------------

ID_CONST_INDEX = "https://legislature.idaho.gov/statutesrules/idconst/"
_ID_ARTICLE_LINK_RE = re.compile(r"/idconst/(Art[IVXLC]+)/?$", re.IGNORECASE)
_ID_SECTION_LINK_RE = re.compile(r"/idconst/(Art[IVXLC]+)/(Sect[\w.]+)/?$", re.IGNORECASE)
_ID_ARTICLE_TITLE_RE = re.compile(r"^ARTICLE\s+[IVXLC]+\s+(.+)$", re.IGNORECASE)
_ID_SECTION_PREFIX_RE = re.compile(r"^Section\s+[\w.]+\.\s*", re.IGNORECASE)
_ID_UPPERCASE_STYLE_RE = re.compile(r"text-transform:\s*uppercase")


def _id_fetch(url: str, retries: int = 4) -> str | None:
    proxies = _us_proxies()
    headers = {"User-Agent": _MOZ_UA}
    for attempt in range(retries):
        try:
            r = SESSION.get(url, timeout=45, headers=headers, proxies=proxies)
            if r.status_code == 200:
                return r.text
        except Exception:
            pass
        time.sleep(max(1.0, 0.5 * (2**attempt)))
    return None


def _id_discover_sections() -> list[tuple[str, str, str]]:
    """(article_roman, section_number, url) across all 21 article pages."""
    idx_html = _id_fetch(ID_CONST_INDEX)
    if idx_html is None:
        _DROPS.fetch_failed("ID constitution index", "fetch failed")
        return []
    idx_soup = BeautifulSoup(idx_html, "html.parser")
    art_urls = []
    for a in idx_soup.find_all("a", href=True):
        m = _ID_ARTICLE_LINK_RE.search(a["href"])
        if m:
            art_urls.append(requests.compat.urljoin(ID_CONST_INDEX, a["href"]))

    def _art_sections(art_url: str) -> list[tuple[str, str, str]]:
        html = _id_fetch(art_url)
        if html is None:
            _DROPS.fetch_failed(f"ID article index ({art_url})", "fetch failed")
            return []
        soup = BeautifulSoup(html, "html.parser")
        out = []
        for a in soup.find_all("a", href=True):
            m = _ID_SECTION_LINK_RE.search(a["href"])
            if m:
                art_roman = m.group(1)[3:]  # "ArtVIII" -> "VIII"
                sec_num = m.group(2)[4:]  # "Sect3" -> "3"
                out.append((art_roman, sec_num, requests.compat.urljoin(art_url, a["href"])))
        return out

    with cf.ThreadPoolExecutor(max_workers=8) as ex:
        results = list(ex.map(_art_sections, art_urls))
    specs = [spec for group in results for spec in group]
    print(f"  [ID] {len(art_urls)} articles -> {len(specs)} section URLs")
    return specs


def _id_fetch_section(spec: tuple[str, str, str]) -> tuple[str, str, str, str, str, str] | None:
    """(article_id, section_number, url, article_title, catchline, body_text) or None."""
    art_id, sec_num, url = spec
    html = _id_fetch(url)
    if html is None:
        return None
    soup = BeautifulSoup(html, "html.parser")
    container = soup.find(class_="pgbrk")
    if container is None:
        return None
    art_title = ""
    h3 = soup.find("h3", class_="lso-toc")
    if h3:
        m = _ID_ARTICLE_TITLE_RE.match(re.sub(r"\s+", " ", h3.get_text(" ", strip=True)).strip())
        if m:
            art_title = m.group(1).strip().title()

    divs = container.find_all("div", recursive=False)
    content = divs[-1] if divs else container
    copy = BeautifulSoup(str(content), "html.parser")
    catchline = ""
    span = copy.find("span", style=_ID_UPPERCASE_STYLE_RE)
    if span is not None:
        catchline = re.sub(r"\s+", " ", span.get_text(" ", strip=True)).strip().rstrip(".")
        span.decompose()
    text = re.sub(r"\s+", " ", copy.get_text(" ", strip=True)).strip()
    text = _ID_SECTION_PREFIX_RE.sub("", text).strip()
    if catchline and text.startswith(catchline):
        text = text[len(catchline) :].strip()
    if not text:
        return None
    return (art_id, sec_num, url, art_title, catchline, text)


def scrape_id(r2) -> list[Section]:
    specs = _id_discover_sections()
    if not specs:
        _DROPS.unit_empty("ID (no section URLs discovered)")
        return []

    with cf.ThreadPoolExecutor(max_workers=8) as ex:
        results = list(ex.map(_id_fetch_section, specs))

    n_failed = sum(1 for r in results if r is None)
    if n_failed:
        _DROPS.fetch_failed(f"ID ({n_failed} of {len(specs)} section pages)", "fetch/parse", count=n_failed)
    print(f"  [ID] fetched {len(specs) - n_failed} / {len(specs)} section pages")

    out: list[Section] = []
    for result in results:
        if result is None:
            continue
        art_id, sec_num, url, art_title, catchline, body_text = result
        sec_title = f"Idaho Const. art. {art_id}, § {sec_num}" + (f". {catchline}" if catchline else "")
        sec = _emit_section(
            "id", r2, None, url, art_id, sec_num, body_text,
            section_title=sec_title, article_title=art_title,
        )
        if sec:
            out.append(sec)

    if not out:
        _DROPS.unit_empty("ID (0 sections after fetch)")
    print(f"[ID] done: {len(out)} sections")
    return out

# ---------------------------------------------------------------------------
# New Jersey -- replaces the Wikisource source (see _WS_INLINE_STATES' former
# "nj" entry). njleg.state.nj.us/constitution is the NJ Legislature's own
# official page (unlike NJ's own COURT RULES, which sit behind a hard
# Incapsula wall -- a separate, much harder problem documented in the NJ
# court-rules ingester, not this page). Text updated through amendments
# adopted November 2021.
#
# Reachable directly from some vantage points (confirmed live 2026-08-08
# from a research sandbox) but NOT from the scraper box's own network
# (confirmed live 2026-08-08: a direct fetch from the box times out at
# connect, not a 403/bot-challenge -- real network-level unreachability, the
# same class of geo-fencing the fetch ladder's proxy step exists for). Uses
# the US proxy unconditionally so the one environment that actually runs the
# monthly refresh (the box) works, even though a non-proxied fetch happens
# to succeed from other locations.
#
# Reuses _parse_nj (Article > optional SECTION sub-level > numbered
# paragraph) unchanged from the Wikisource-era fix -- the body shape (bare
# "1.", "2." paragraph markers, no SECTION/Sec. keyword) is identical on the
# new source, confirmed live 2026-08-08. _parse_nj itself was extended to
# capture each article's title line while migrating this state (see its own
# module comment), and a real bug was found and fixed in its SECTION-nested
# paragraph split (see the comment at its `_NJ_PARA_RE.split` call site).
#
# This new source produces 205 sections, below the Wikisource baseline's 328
# -- investigated live 2026-08-08 rather than shipped uncounted, per the
# batch methodology's "lower count is a red flag" rule. Every one of the
# 205 top-level `\n\d+.` paragraph markers present anywhere in the fetched
# document is captured (verified by a raw regex count over the whole body
# matching the emitted count exactly, zero loss), and the emitted sections'
# total character count is 93% of the source document's total body length
# (the remaining 7% is the discarded preamble, article-title captions, and
# SECTION-heading tokens -- all expected, not a silent gap). The gap versus
# 328 is therefore a genuine COUNTING-CONVENTION difference, not missing
# content: Wikisource's transcription apparently split lettered sub-items
# ("2. a. ... b. ...", a sub-paragraph of one numbered provision) into their
# own separate points, while `_NJ_PARA_RE` -- by design, matching every
# other state's convention of keeping subsection letters inline -- only
# treats digit-led markers as new top-level sections. 328 is not being
# treated as ground truth here since it was never independently verified
# against the New Jersey Legislature's own citation scheme, only against
# Wikisource's own (third-party, since-retired) transcription.
# ---------------------------------------------------------------------------

NJ_CONST_URL = "https://njleg.state.nj.us/constitution"


def scrape_nj(r2) -> list[Section]:
    try:
        html = fetch_text(NJ_CONST_URL, use_us_proxy=True)
    except Exception as e:
        _DROPS.fetch_failed("NJ constitution", e)
        return []

    r2_html_key = "state_constitutions/nj/source/nj_constitution.html"
    put_if_changed(r2, r2_html_key, html.encode("utf-8"), "text/html; charset=utf-8")
    r2_html_url = public_url(r2_html_key)

    soup = BeautifulSoup(html, "html.parser")
    main = soup.find("main")
    body_text = (main or soup).get_text("\n", strip=True)

    out = _parse_nj("nj", body_text, NJ_CONST_URL, r2_html_url, r2)
    if not out:
        _DROPS.unit_empty("NJ (0 sections after parse)")
    print(f"[NJ] done: {len(out)} sections")
    return out


# ---------------------------------------------------------------------------
# Oregon -- replaces the Wikisource-subpages source
# (scrape_oregon_wikisource_subpages above; STATE_SCRAPERS["or"] no longer
# points at it, see the override removed below).
# oregonlegislature.gov/bills_laws/Pages/OrConst.aspx is the Oregon
# Legislature's own official page (SharePoint-hosted), confirmed live
# 2026-08-08, current through the Nov 2024 amendments. Needs the US proxy --
# a direct fetch from outside the US gets connection-refused at the TCP
# level (confirmed live), not merely geo-redirected.
#
# The real document text lives in TWO of the page's `.ms-rtestate-field`
# SharePoint content divs (a Preamble-through-Article-VIII chunk, then an
# Article-IX-through-XVIII chunk), with several small boilerplate divs (page
# title, side-nav buttons) interspersed -- confirmed live 2026-08-08 the page
# splits its one logical document across two separate web-part divs. Only
# the divs over 1,000 chars carry real constitutional text; concatenated in
# DOM order rather than hardcoding an index, since SharePoint's own div count
# for boilerplate is not something to depend on staying fixed.
#
# Each article opens with a "Sec.  N.  Title\n1a.  Title\n1b.  Title..."
# table-of-contents preview (bare numbers after the first entry, no keyword)
# before the real text starts -- same TOC-collision shape as NV. Unlike NV,
# OR's real section markers spell out the full word "Section" ("Section 1.
# Title.\nBody...") while the TOC's one keyword token is always the
# abbreviation "Sec." -- confirmed live 2026-08-08: 389 of 389 real headers
# use "Section", 33 of 33 "Sec." occurrences are TOC column headers, zero
# overlap -- so anchoring on the full word alone (no lookahead trick needed,
# unlike NV) cleanly separates the two.
#
# Oregon reuses an article label after fully repealing the article that used
# it before (Article XI-A: "Rural Credits", created 1916, repealed 1942; the
# same "XI-A" label was later reassigned to "Farm and Home Loans to
# Veterans") -- confirmed live 2026-08-08, the only such reuse in the
# document. This is real history, not a scrape bug, so both are kept: the
# second occurrence of an article_id gets a "-vN" suffix, the same
# convention `_emit_sections_from_articles` already uses for a repeated
# SECTION number within one article. Article VII also legitimately exists in
# two parallel versions under one number ("ARTICLE VII (Amended)" and
# "ARTICLE VII (Original)", both still printed in full by the official
# source) and several Article XI sub-articles carry a parenthetical part
# number ("ARTICLE XI-F(1)", "ARTICLE XI-F(2)") -- both shapes are folded
# into a clean article_id (e.g. "VII-Amended", "XI-F1") by
# `_or_clean_article_id` rather than left with raw spaces/parens; these two
# shapes never collide with each other so neither needs the -vN suffix.
# ---------------------------------------------------------------------------

OR_CONST_URL = "https://www.oregonlegislature.gov/bills_laws/Pages/OrConst.aspx"
_OR_ARTICLE_RE = re.compile(
    r"\nARTICLE\s+([IVXLC]+(?:-[A-Z])?(?:\(\d\))?(?:\s*\((?:Amended|Original)\))?)\s*\n"
)
_OR_SECTION_RE = re.compile(r"\nSection\s+(\d+[A-Za-z]?)\.\s*")
# "adopted by the people <date>" is the one phrase present in essentially
# every bracketed history note regardless of which of the note's several
# other forms ("Created through...", "Amendment proposed by...", "Repeal
# proposed by...") precedes it -- anchoring on it (rather than sweeping every
# year in the bracket) excludes the non-amendment "Constitution of 1859"
# origin-date brackets that also appear alongside it. Verified live
# 2026-08-08.
_OR_ADOPTED_RE = re.compile(r"adopted by the people\s+[A-Za-z]+\.?\s+\d{1,2},?\s+((?:19|20)\d{2})")


def _or_amendment_years(raw_text: str) -> list[int]:
    return [int(y) for y in _OR_ADOPTED_RE.findall(raw_text)]


_AMENDMENT_YEAR_EXTRACTORS["or"] = _or_amendment_years


def _or_clean_article_id(raw: str) -> str:
    m = re.match(r"^([IVXLC]+(?:-[A-Z])?)(?:\((\d)\))?(?:\s*\((Amended|Original)\))?$", raw.strip())
    if not m:
        return re.sub(r"[()\s]", "", raw.strip())
    base, paren_num, tag = m.groups()
    cleaned = base + (paren_num or "")
    if tag:
        cleaned += f"-{tag}"
    return cleaned


def scrape_or(r2) -> list[Section]:
    try:
        html = fetch_text(OR_CONST_URL, use_us_proxy=True)
    except Exception as e:
        _DROPS.fetch_failed("OR constitution", e)
        return []

    r2_html_key = "state_constitutions/or/source/or_constitution.html"
    put_if_changed(r2, r2_html_key, html.encode("utf-8"), "text/html; charset=utf-8")
    r2_html_url = public_url(r2_html_key)

    soup = BeautifulSoup(html, "html.parser")
    content_divs = [
        d for d in soup.select(".ms-rtestate-field") if len(d.get_text(strip=True)) > 1000
    ]
    if not content_divs:
        _DROPS.unit_empty("OR (no content divs found)")
        return []
    body_text = "\n".join(d.get_text("\n", strip=True) for d in content_divs)

    padded = "\n" + body_text
    art_matches = list(_OR_ARTICLE_RE.finditer(padded))
    if not art_matches:
        _DROPS.unit_empty(f"OR (no ARTICLE markers in {len(body_text)} chars)")
        return []

    seen_ids: dict[str, int] = {}
    article_titles: dict[str, str] = {}
    art_iter: list[tuple[str, str]] = []
    for i, m in enumerate(art_matches):
        art_id = _or_clean_article_id(m.group(1))
        seen_ids[art_id] = seen_ids.get(art_id, 0) + 1
        if seen_ids[art_id] > 1:
            art_id = f"{art_id}-v{seen_ids[art_id]}"
        start = m.end()
        end = art_matches[i + 1].start() if i + 1 < len(art_matches) else len(padded)
        art_body = padded[start:end]
        title_m = re.match(r"[^\S\n]*([^\n]{1,90})\n", art_body)
        if title_m:
            article_titles[art_id] = title_m.group(1).strip()
        art_iter.append((art_id, art_body))

    return _emit_sections_from_articles(
        "or",
        r2,
        OR_CONST_URL,
        art_iter,
        _OR_SECTION_RE,
        r2_html_url=r2_html_url,
        article_titles=article_titles,
    )


# ---------------------------------------------------------------------------
# Colorado -- replaces the Wikisource source (see _WS_INLINE_STATES' former
# "co" entry). Published as "Title 00" of the SAME Colorado Revised Statutes
# DOCX series the existing CO statute scraper (state_scrapers/.../co/statutes/
# scrapeCO.py) already pulls for titles 01-44, at the identical
# leg.colorado.gov/sites/default/files/images/olls/ path -- confirmed live
# 2026-08-08: the origin URL and content.leg.colorado.gov both 403 directly,
# but the Wayback Machine mirror returns 200 with no auth required (same
# fetch pattern scrapeCO.py already relies on, reused directly below).
#
# python-docx paragraph structure (verified live 2026-08-08): "ARTICLE
# <roman>" alone on its own paragraph, the article's own name on the
# IMMEDIATELY FOLLOWING paragraph (e.g. "Boundaries", "Bill of Rights"),
# then a run of body/editorial paragraphs, then repeating
# "Section N[letter]. Title. Body text..." headers, each followed by a
# trailing "Source: ..." (sometimes also "Cross references:"/"Editor's
# note:") history paragraph before the next header -- left inline in the
# section body rather than stripped (same precedent as NV's bracketed
# amendment notes: informative, and exactly what the amendment-year
# extractor below reads). Confirmed live: 29 ARTICLE headers (I-XXIX,
# matching the batch handoff), 395 "Section N[letter]." headers (11
# lettered: 16a/30a/30b/22a/22b/25a/1a/2a/9a/12a/12b), zero PART level, and
# the document ends cleanly at the 1876 convention signers' list with no
# bleed-in from adjacent content -- no boundary truncation needed (unlike
# WA's multi-part PDF).
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Rhode Island -- replaces the Wikisource source (removed from
# _WS_INLINE_STATES below). rilegislature.gov/riconstitution/Constitution/
# ConstFull.aspx is the RI Legislature's own official page, confirmed live
# 2026-08-08. Same root domain as RI's own statute scraper
# (webserver.rilegislature.gov/Statutes) but a different subdomain (www vs
# webserver) -- needs the US proxy, same as statutes.
#
# The page is Word-export HTML (mso- inline styles, MsoNormal paragraph
# classes) with real semantic ARTICLE/Section headings marked up as h2/h3 --
# but NOT consistently: Article IX's own "ARTICLE IX" heading and its first
# section ("Section 1. Power vested in governor.") are both printed as plain
# <p> tags instead of <h2>/<h3> like every other article (confirmed live
# 2026-08-08, along with two more mis-tagged section headings elsewhere --
# "Section 4. Repealed." and "Section 11. Vote required to pass local or
# private appropriations.", the only three of ~317 <p> elements whose text
# happens to match the Section pattern). A pure h2/h3-tag walk would
# silently mis-attribute Article IX's real content to the end of Article
# VIII, so this walks every h1/h2/h3/p element in document order and matches
# the ARTICLE/Section pattern against each element's OWN text regardless of
# its tag, rather than trusting the tag name -- a strict superset of a
# tag-only walk, so it cannot regress any of the correctly-tagged articles.
#
# A short ALL-CAPS line immediately after an ARTICLE marker (e.g. "OF
# SUFFRAGE", "OF THE SENATE") is that article's own title, present for about
# half the articles; captured as article_title when present. Article I has
# no title line at all (goes straight into prose), confirmed live
# 2026-08-08 -- the all-caps check is what tells the two cases apart.
#
# Word-splitting each word into its own inline `<span style='letter-spacing:
# ...'>` (a justified-text artifact of the Word export) makes a naive
# `get_text("\n", strip=True)` flatten produce one word per line throughout
# the document -- get_text(" ", strip=True) (space-joined) is used per
# element instead, sidestepping the artifact entirely rather than needing a
# rejoin-broken-lines cleanup pass.
#
# The batch research flagged mis-encoded curly quotes/dashes ("â€™" etc, a
# charset-detection artifact) as an expected problem here; not reproduced --
# confirmed live 2026-08-08 this fetch path's charset detection already
# decodes the page correctly (every character above U+2100 in the fetched
# text is a real curly quote/em-dash, not mojibake), despite the page's own
# (apparently wrong) <meta charset=windows-1252> declaration. No charset
# cleanup step is included since there is nothing live to clean.
#
# No amendment/ratification-year annotation of any kind was found in the
# text (checked live 2026-08-08: "amend"/"ratif"/"adopted" all appear only
# in generic prose, e.g. describing the amendment PROCESS itself, never
# attached to a specific date next to a specific section) -- no extractor
# registered, same as the several Wikisource states already confirmed to
# have no safely extractable format (see the _AMENDMENT_YEAR_EXTRACTORS
# module note above).
# ---------------------------------------------------------------------------

RI_CONST_URL = "https://www.rilegislature.gov/riconstitution/Constitution/ConstFull.aspx"
_RI_ARTICLE_RE = re.compile(r"^ARTICLE\s+([IVXLC]+)$")
_RI_SECTION_RE = re.compile(r"^Section\s+(\d+[A-Za-z]?)\.\s*(.*)$")


def scrape_ri(r2) -> list[Section]:
    try:
        html = fetch_text(RI_CONST_URL, use_us_proxy=True)
    except Exception as e:
        _DROPS.fetch_failed("RI constitution", e)
        return []

    r2_html_key = "state_constitutions/ri/source/ri_constitution.html"
    put_if_changed(r2, r2_html_key, html.encode("utf-8"), "text/html; charset=utf-8")
    r2_html_url = public_url(r2_html_key)

    soup = BeautifulSoup(html, "html.parser")
    body_el = soup.find("body") or soup
    elements = body_el.find_all(["h1", "h2", "h3", "p"])

    out: list[Section] = []
    cur_art: str | None = None
    cur_art_title = ""
    cur_sec: str | None = None
    cur_sec_title = ""
    pending: list[str] = []
    had_section = False
    awaiting_title = False

    def flush_section() -> None:
        nonlocal pending, cur_sec, cur_sec_title
        if cur_sec is not None:
            text = "\n".join(pending)
            sec = _emit_section(
                "ri",
                r2,
                r2_html_url,
                RI_CONST_URL,
                cur_art,
                cur_sec,
                text,
                section_title=cur_sec_title or None,
                article_title=cur_art_title,
            )
            if sec:
                out.append(sec)
        pending = []
        cur_sec = None
        cur_sec_title = ""

    def close_article() -> None:
        nonlocal pending
        if cur_art is None:
            return
        if cur_sec is not None:
            flush_section()
        elif not had_section and pending:
            text = "\n".join(pending)
            sec = _emit_section(
                "ri",
                r2,
                r2_html_url,
                RI_CONST_URL,
                cur_art,
                "0",
                text,
                section_title=f"RI Const., Article {cur_art}",
                article_title=cur_art_title,
            )
            if sec:
                out.append(sec)
        pending = []

    for el in elements:
        text = re.sub(r"\s+", " ", el.get_text(" ", strip=True)).strip()
        if not text:
            continue
        m_art = _RI_ARTICLE_RE.match(text)
        if m_art:
            close_article()
            cur_art = m_art.group(1)
            cur_art_title = ""
            had_section = False
            awaiting_title = True
            continue
        if awaiting_title:
            awaiting_title = False
            if text == text.upper() and len(text) <= 90:
                cur_art_title = text
                continue
        m_sec = _RI_SECTION_RE.match(text)
        if m_sec and cur_art is not None:
            if cur_sec is not None:
                flush_section()
            else:
                pending = []
            had_section = True
            cur_sec = m_sec.group(1)
            cur_sec_title = re.sub(r"\s+", " ", m_sec.group(2)).strip()
            continue
        if cur_art is not None:
            pending.append(text)
    close_article()

    if not out:
        _DROPS.unit_empty("RI (0 sections after parse)")
    print(f"[RI] done: {len(out)} sections")
    return out


# ---------------------------------------------------------------------------
# New York -- replaces the Wikisource source (see _WS_INLINE_STATES' former
# "ny" entry). Uses the official Open Legislation (OpenLeg) API
# (legislation.nysenate.gov/api/3), the SAME API NY's own statute corpus
# already uses (scripts/us_corpus/statutes/ingest_ny_bulk.py, via
# ny_bulk/api.py + ny_bulk/walk.py). `GET /laws/CNS` is the full NY
# Constitution -- ingest_ny_bulk.py already excludes law type "MISC" (CNS's
# type) from its own statute run for exactly this reason ("the constitution
# lives in the state_constitution corpus, not statutes").
#
# Confirmed live 2026-08-08 against the real API response: root CHAPTER "CNS"
# has 21 top-level children -- one PREAMBLE (no docLevelId, no children) plus
# 20 ARTICLE containers (I-XX), each holding SECTION children DIRECTLY (no
# TITLE/PART nesting) -- 202 total leaf units via `ny_bulk.walk.iter_sections`
# (201 real sections + the Preamble). `iter_sections` already skips any node
# flagged `repealed`, so no separate handling is needed here for that.
#
# `ny_bulk.walk.iter_sections` is reused directly (pure JSON-tree walking, no
# network dependency, no reason to duplicate it) -- but `ny_bulk.api.get_law_tree`
# is NOT: its client tries `vaquill_pipeline.http_client` (a proxy-aware
# fetcher) first and falls back to bare `urllib` otherwise, and confirmed
# live 2026-08-08 on the scraper box, `vaquill_pipeline` isn't an installed
# package there, so it always falls through to bare urllib -- which hangs on
# an SSL handshake timeout against legislation.nysenate.gov from the box's
# non-US egress (the same geo-fencing class this file already handles
# elsewhere via `fetch_text(..., use_us_proxy=True)`). Routing the SAME
# request through this file's own proxy-aware SESSION instead fixed it
# (confirmed live: 200 OK, ~525KB for the full CNS tree) without touching the
# shared statute-ingestion module every other NY law type's scraper depends
# on.
# ---------------------------------------------------------------------------

NY_CONST_LAW_URL = "https://legislation.nysenate.gov/laws/CNS"
_NY_OPENLEG_BASE = "https://legislation.nysenate.gov/api/3"


def _ny_openleg_key() -> str:
    _load_env()
    key = os.environ.get("OPENLEG_API_KEY") or os.environ.get("NYSENATE_API_KEY")
    if not key:
        raise RuntimeError("OPENLEG_API_KEY (or NYSENATE_API_KEY) not set")
    return key


def _fetch_ny_law_tree(law_id: str) -> dict | None:
    try:
        key = _ny_openleg_key()
    except RuntimeError as e:
        print(f"  ! NY OpenLeg fetch failed: {e}")
        return None
    url = f"{_NY_OPENLEG_BASE}/laws/{law_id}?full=true&key={key}"
    proxies = _us_proxies()
    for attempt in range(4):
        try:
            r = SESSION.get(url, timeout=90, proxies=proxies, headers={"User-Agent": _MOZ_UA})
            r.raise_for_status()
            data = r.json()
            if not data.get("success", False):
                raise RuntimeError(f"OpenLeg API error: {data.get('message')}")
            return data["result"]
        except Exception as e:
            if attempt == 3:
                print(f"  ! NY OpenLeg fetch failed: {e}")
                return None
            time.sleep(2.0 * (attempt + 1))
    return None


def _ny_article_titles(result: dict) -> dict[str, str]:
    items = (result.get("documents") or {}).get("documents", {}).get("items", [])
    return {
        it.get("docLevelId"): (it.get("title") or "").strip()
        for it in items
        if it.get("docType") == "ARTICLE" and it.get("docLevelId")
    }


def scrape_ny(r2) -> list[Section]:
    from scripts.statutes.ny_bulk.walk import iter_sections

    result = _fetch_ny_law_tree("CNS")
    if not result:
        _DROPS.fetch_failed("NY constitution (OpenLeg CNS)", "download failed")
        return []

    r2_json_key = "state_constitutions/ny/source/ny_constitution_cns.json"
    put_if_changed(r2, r2_json_key, json.dumps(result).encode("utf-8"), "application/json")
    r2_json_url = public_url(r2_json_key)

    article_titles = _ny_article_titles(result)
    out: list[Section] = []
    for leaf in iter_sections(result):
        # OpenLeg's stored text uses a LITERAL two-character "\n" as its own
        # internal line-break marker, not a real newline byte (confirmed live
        # 2026-08-08: `"\\n" in leaf.text` is True, `"\n" in leaf.text` is
        # False) -- _emit_section's whitespace-normalize only collapses real
        # `\s` characters, so left alone every section's raw_text would carry
        # literal "\n" substrings straight into the stored/embedded text.
        text = leaf.text.replace("\\n", " ")
        art_id = next((lvl for cls, lvl in leaf.ancestors if cls == "article"), None)
        if art_id is None:
            # The Preamble (no ARTICLE ancestor) carries no docLevelId of its
            # own -- iter_sections' single-blob fallback uses its raw OpenLeg
            # locationId instead (e.g. "AA1"), not a real citable section
            # number. Same article_id="0"/section_number="0" convention
            # scrape_mn already uses for its own Preamble.
            sec = _emit_section(
                "ny",
                r2,
                r2_json_url,
                NY_CONST_LAW_URL,
                "0",
                "0",
                text,
                section_title="N.Y. Const. Preamble",
            )
        else:
            sec = _emit_section(
                "ny",
                r2,
                r2_json_url,
                NY_CONST_LAW_URL,
                art_id,
                leaf.doc_level_id,
                text,
                section_title=leaf.title or None,
                article_title=article_titles.get(art_id, ""),
            )
        if sec:
            out.append(sec)

    if not out:
        _DROPS.unit_empty("NY (0 sections after OpenLeg parse)")
    print(f"[NY] done: {len(out)} sections across {len(article_titles)} articles")
    return out


# ---------------------------------------------------------------------------
# Vermont -- replaces the Wikisource source (removed from _WS_INLINE_STATES
# below). legislature.vermont.gov/statutes/constitution-of-the-state-of-
# vermont is the VT Legislature's own official page, confirmed live
# 2026-08-08, current through amendments adopted through March 2021. Reuses
# the existing _VT_ARTICLE_RE/_VT_SECTION_RE pair written for the
# Wikisource-era fix (see _ART_SPLIT_OVERRIDES/_SECTION_SPLIT_OVERRIDES
# above) unchanged -- the document's own Chapter I "Article N." / Chapter II
# "§N." shape is identical on the new source, confirmed live 2026-08-08.
#
# legislature.vermont.gov's TLS certificate chain fails verification (the
# server doesn't send its own intermediate CA cert) -- confirmed live
# 2026-08-08 this is reproducible both through the US proxy AND on a
# direct connection, so it is a real server-side misconfiguration, not a
# proxy-specific MITM artifact (an unverified fetch of the exact same URL
# returns the identical ~103KB of real constitutional text every time).
# Since this is a read-only fetch of public, non-sensitive government text
# (no credentials or user data ever touch this connection), and the failure
# mode is a well-understood, reproducible server misconfiguration rather
# than an unknown risk, this one fetch disables certificate verification
# via `_fetch_text_no_verify` rather than dropping the whole state -- scoped
# to this single call, not a change to `fetch_text`'s behavior for any other
# state.
#
# The fetched page repeats a site-nav footer ("The Vermont General
# Assembly\nMontpelier, Vermont\nStatutes\n...") after the real document
# ends (right after §76, the last real section) -- truncated before any
# split runs, same convention as every other state's footer-stripping regex.
# No amendment-year extractor: VT's source carries only a whole-DOCUMENT
# currency line ("AS ESTABLISHED JULY 9, 1793, AND AMENDED THROUGH MARCH 31,
# 2021"), not per-section dates -- confirmed live 2026-08-08, same finding
# as the Wikisource-era investigation recorded in the _AMENDMENT_YEAR_EXTRACTORS
# module note above.
# ---------------------------------------------------------------------------

VT_CONST_URL = "https://legislature.vermont.gov/statutes/constitution-of-the-state-of-vermont"
_VT_FOOTER_RE = re.compile(r"\nThe Vermont General Assembly\n.*", re.DOTALL)


def _fetch_text_no_verify(url: str, retries: int = 3) -> str:
    import warnings

    proxies = _us_proxies()
    headers = {"User-Agent": _MOZ_UA}
    last = None
    for attempt in range(retries):
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                r = SESSION.get(
                    url,
                    timeout=45,
                    allow_redirects=True,
                    proxies=proxies,
                    headers=headers,
                    verify=False,
                )
            r.raise_for_status()
            return r.text
        except Exception as e:
            last = e
            time.sleep(max(1.0, 0.5 * (2**attempt)))
    raise RuntimeError(f"fetch failed {url}: {last}")


def scrape_vt(r2) -> list[Section]:
    try:
        html = _fetch_text_no_verify(VT_CONST_URL)
    except Exception as e:
        _DROPS.fetch_failed("VT constitution", e)
        return []

    r2_html_key = "state_constitutions/vt/source/vt_constitution.html"
    put_if_changed(r2, r2_html_key, html.encode("utf-8"), "text/html; charset=utf-8")
    r2_html_url = public_url(r2_html_key)

    soup = BeautifulSoup(html, "html.parser")
    main = soup.find("main")
    body_text = (main or soup).get_text("\n", strip=True)
    body_text = _VT_FOOTER_RE.sub("", body_text)

    art_parts = _VT_ARTICLE_RE.split("\n" + body_text)
    if len(art_parts) <= 1:
        _DROPS.unit_empty(f"VT (no CHAPTER markers in {len(body_text)} chars)")
        return []
    art_iter = [(art_parts[i], art_parts[i + 1]) for i in range(1, len(art_parts) - 1, 2)]

    return _emit_sections_from_articles(
        "vt", r2, VT_CONST_URL, art_iter, _VT_SECTION_RE, r2_html_url=r2_html_url
    )


# ---------------------------------------------------------------------------
# Mississippi -- replaces the Wikisource source (see _WS_INLINE_STATES'
# former "ms" entry). Source is the Secretary of State's own page
# (sos.state.ms.us/ed_pubs/constitution/constitution.asp), an official state
# domain. The markup
# is Word-export span/font soup, but `BeautifulSoup.get_text()` handles that
# automatically; the real complexity is structural, not markup-cleanliness.
#
# The page is windows-1252 (confirmed via its own <meta charset>), not the
# ISO-8859-1 `requests` would otherwise guess purely from response headers --
# they agree on plain ASCII but diverge on the 0x80-0x9F range (curly
# quotes, em dashes), which this Word-export document uses throughout.
#
# Confirmed live 2026-08-08: the document opens with a GLOBAL table of
# contents ("ARTICLE 1. DISTRIBUTION OF POWERS 1 ARTICLE 2. ..." -- WITH a
# trailing period after the number, and a page number where body text would
# be) before the real "PREAMBLE" and 15 numbered articles begin. The real
# article headers never carry that trailing period ("ARTICLE 1 DISTRIBUTION
# OF POWERS SECTION 1. Powers of government. SECTION 2. ... SECTION 1. The
# powers of the government..."), so truncating at the first "PREAMBLE"
# occurrence (which the global TOC itself never contains) drops it cleanly
# without needing a period-vs-no-period regex distinction.
#
# EVERY real article then ALSO opens with its own mini table of contents --
# each real section number listed twice: once as a bare title-only preview
# ("SECTION 1. Powers of government.") and once with the real substantive
# body plus a trailing "SOURCES:" citation. Confirmed live: 595 total
# "SECTION N." matches across the document against the batch handoff's
# pre-verified 286 real sections -- almost exactly double. Same TOC-
# collision class and same fix as scrape_mi's own per-article preview: keep
# only the LONGEST body per section number within an article (the preview's
# body is always short -- the next heading immediately follows it).
# ---------------------------------------------------------------------------

MS_CONST_URL = "https://www.sos.state.ms.us/ed_pubs/constitution/constitution.asp"
_MS_PREAMBLE_RE = re.compile(r"\bPREAMBLE\b")
_MS_ARTICLE_RE = re.compile(r"ARTICLE\s+(\d{1,2})\s+")
_MS_SECTION_RE = re.compile(r"SECTION\s+(\d+(?:-[A-Z])?)\.\s*")


def _fetch_ms_html() -> str | None:
    for attempt in range(4):
        try:
            r = SESSION.get(MS_CONST_URL, timeout=60, headers={"User-Agent": _MOZ_UA})
            r.raise_for_status()
            return r.content.decode("windows-1252", errors="replace")
        except Exception as e:
            if attempt == 3:
                print(f"  ! MS constitution fetch failed: {e}")
                return None
            time.sleep(2.0 * (attempt + 1))
    return None


def _ms_amendment_years(raw_text: str) -> list[int]:
    # MS's own "SOURCES: 1817 art...; 1832 art...; 1869 art..." antecedent
    # citations (predecessor MS constitutions -- 1817/1832/1869/1890 are MS's
    # four historical constitutions) are all pre-1900, so a plain 19xx/20xx
    # sweep of the whole section naturally excludes them and leaves only
    # genuine post-1890 amendment/repeal citations ("Laws of 1990, Ch.
    # 692...", "ratification by the electorate on November 6, 1990...") --
    # confirmed live 2026-08-08 against a broad sample, no span-isolation
    # needed the way CO's page-number collision required.
    return [int(y) for y in _YEAR_TOKEN_RE.findall(raw_text)]


_AMENDMENT_YEAR_EXTRACTORS["ms"] = _ms_amendment_years


def scrape_ms(r2) -> list[Section]:
    html = _fetch_ms_html()
    if not html:
        _DROPS.fetch_failed("MS constitution", "download failed")
        return []
    r2_html_key = "state_constitutions/ms/source/ms_constitution.html"
    put_if_changed(r2, r2_html_key, html.encode("utf-8"), "text/html; charset=utf-8")
    r2_html_url = public_url(r2_html_key)

    soup = BeautifulSoup(html, "html.parser")
    body_text = re.sub(r"\s+", " ", soup.get_text(" ", strip=True)).strip()

    pre_m = _MS_PREAMBLE_RE.search(body_text)
    if not pre_m:
        _DROPS.unit_empty("MS (no PREAMBLE marker found -- TOC boundary undetected)")
        return []
    real_body = body_text[pre_m.end() :]

    art_matches = list(_MS_ARTICLE_RE.finditer(real_body))
    if not art_matches:
        _DROPS.unit_empty(f"MS (no ARTICLE markers in {len(real_body)} chars)")
        return []

    out: list[Section] = []
    for i, m in enumerate(art_matches):
        art_id = m.group(1)
        start = m.end()
        end = art_matches[i + 1].start() if i + 1 < len(art_matches) else len(real_body)
        art_body = real_body[start:end]

        sec_matches = list(_MS_SECTION_RE.finditer(art_body))
        if not sec_matches:
            sec = _emit_section(
                "ms",
                r2,
                r2_html_url,
                MS_CONST_URL,
                art_id,
                "0",
                art_body,
                section_title=f"MS Const., Article {art_id}",
            )
            if sec:
                out.append(sec)
            continue

        # The article's own name is the text between the "ARTICLE N" header
        # and the first "SECTION" keyword (the mini-TOC's own opening).
        # Articles 4 (Legislative) and 15 (Amendments) are further divided
        # into named PARTs ("IN GENERAL", "QUALIFICATIONS AND PRIVILEGES OF
        # LEGISLATORS", ...) and carry their OWN nested "Beginning Section"
        # sub-index ahead of the section-level mini-TOC -- confirmed live
        # 2026-08-08. It doesn't affect section-content extraction (the
        # per-section regex still matches correctly either way), only the
        # captured title text, so truncate there rather than parsing the
        # sub-index structurally.
        article_title = art_body[: sec_matches[0].start()].split("Beginning Section")[0].strip()

        best_by_num: dict[str, str] = {}
        for j, sm in enumerate(sec_matches):
            sec_num = sm.group(1)
            s_start = sm.end()
            s_end = sec_matches[j + 1].start() if j + 1 < len(sec_matches) else len(art_body)
            sec_body = art_body[s_start:s_end].strip()
            if sec_num not in best_by_num or len(sec_body) > len(best_by_num[sec_num]):
                best_by_num[sec_num] = sec_body

        for sec_num, sec_body in best_by_num.items():
            sec = _emit_section(
                "ms",
                r2,
                r2_html_url,
                MS_CONST_URL,
                art_id,
                sec_num,
                sec_body,
                article_title=article_title,
            )
            if sec:
                out.append(sec)

    if not out:
        _DROPS.unit_empty("MS (0 sections after parse)")
    print(f"[MS] done: {len(out)} sections across {len(art_matches)} articles")
    return out


# ---------------------------------------------------------------------------
# Massachusetts -- replaces the Wikisource source (removed from
# _WS_INLINE_STATES below). malegislature.gov/Laws/Constitution is the MA
# Legislature's own official page, confirmed live 2026-08-08. Same domain as
# MA's own statute site; the US proxy already wired for MA's known geo-block
# is reused directly.
#
# This reuses the SHAPE of the Wikisource-era _parse_ma/_parse_ma_part2
# (Part > Chapter > Section > Article, with Part the First flat and Part the
# Second three-level-nested) but not their regexes: the official source's
# HTML has real semantic heading tags (h2 for Part/Amendments boundaries, h3
# for Chapter+Section citations, h4 for every leaf "Article N."), so this
# walks those tags directly instead of regex-splitting a flattened,
# whitespace-mangled text blob -- the official page wraps almost every word
# of a heading in its own inline span (a justified-text artifact, same class
# of problem as RI's), which breaks a flat get_text("\n") regex approach the
# way it did for RI (see scrape_ri's module note). get_text(" ", strip=True)
# per block-level element, walked in document order, sidesteps it the same
# way. The old _parse_ma/_parse_ma_part2 functions are left in place, unused
# once "ma" is removed from _WS_INLINE_STATES below (same precedent as the
# orphaned _MD_SECTION_RE/_MD_ARTICLE_RE after MD's own migration).
#
# The page's own Table of Contents (an `<li>`-based list, confirmed live
# 2026-08-08) never uses h2/h3/h4 tags, so restricting the walk to
# h2/h3/h4/p elements skips it automatically -- no separate TOC-boundary
# detection needed, unlike the Wikisource-era fix or several other C07
# states.
#
# Chapter+Section citations are printed as one heading per h3 but with
# wildly inconsistent internal punctuation depending on whether the
# chapter's own title is repeated (e.g. "Chapter I, LEGISLATIVE POWER ,
# Section I" for a chapter's first section, "Chapter I, Section II" for
# later ones without the title, "Chapter III. JUDICIARY POWER." for a
# chapter with a title but NO Section level at all) -- confirmed live
# 2026-08-08 across all 6 real chapters. `_ma_parse_chapter_heading` handles
# all of these by finding the "Chapter <roman>" prefix and, separately, an
# optional "Section <roman>" token anywhere after it, rather than trying to
# match one rigid punctuation pattern. Unlike the retired Wikisource source,
# every chapter's first Section is explicitly marked here (no "Chapter V's
# first section has no marker" special case needed).
#
# Stops at the "ARTICLES OF AMENDMENT." h2 boundary, same scope as the old
# Wikisource-era fix -- capturing the ~120 amendment articles (which restart
# their own "Article I."... numbering, rendered as h3 rather than h4 on this
# source, and would collide with Part I/II's Article numbers) is still
# flagged as follow-up work, not attempted here. No amendment-year
# extractor either: MA's own in-body annotations ("[Annulled by Amendments,
# Art. CVI.]", "[See Amendments, Arts. XLVI and XLVIII.]") cross-reference an
# amendment ARTICLE NUMBER, not a date -- there is no year anywhere in these
# notes to extract, confirmed live 2026-08-08.
# ---------------------------------------------------------------------------

MA_CONST_URL = "https://malegislature.gov/Laws/Constitution"
_MA_CH_PREFIX_RE = re.compile(r"^Chapter\s+([IVXL]+)")
_MA_SEC_INLINE_RE = re.compile(r"Section\s+([IVXL]+)")
_MA_ARTICLE_H4_RE = re.compile(r"^Article\s+([IVXL]+)\.?$")


def _ma_parse_chapter_heading(text: str) -> tuple[str, str, str | None]:
    """Split an h3 "Chapter <roman>[, TITLE][, Section <roman>]" citation
    into (chapter_roman, chapter_title, section_roman_or_None). See the
    module note above scrape_ma for why this can't be one fixed-punctuation
    regex."""
    m = _MA_CH_PREFIX_RE.match(text)
    chap = m.group(1)
    rest = text[m.end() :]
    sec_m = _MA_SEC_INLINE_RE.search(rest)
    if sec_m:
        title = rest[: sec_m.start()]
        section = sec_m.group(1)
    else:
        title = rest
        section = None
    title = title.strip(" ,.;:")
    return chap, title, section


def _ma_walk_elements(node):
    """Yield (kind, text) for every h2/h3/h4/p in document order, PLUS any
    body text that sits as a bare NavigableString directly inside a wrapper
    div rather than wrapped in its own <p> -- confirmed live 2026-08-08 this
    happens for at least one real provision (Chapter II, Section II, Article
    III: the governor-succession clause), where the source's markup is
    "<h4>Article III.</h4>Whenever the chair of the governor...<a ...>LV</a>
    .]<p></p>" with the actual body text an unwrapped sibling of the h4, not
    a child of any <p>. A plain `find_all(["h2","h3","h4","p"])` silently
    skips text like that -- it only matches Tag elements. This recurses
    through wrapper elements (the divs the real content is nested inside)
    but treats h2/h3/h4/p as leaves (their own get_text already covers
    everything inside them, so this does not recurse into their children --
    doing so would double-count that text as loose body content too)."""
    for child in node.children:
        if isinstance(child, NavigableString):
            text = str(child).strip()
            if text:
                yield ("text", text)
        elif isinstance(child, Tag):
            if child.name in ("h2", "h3", "h4", "p"):
                yield (child.name, child.get_text(" ", strip=True))
            elif child.name in ("script", "style"):
                continue
            else:
                yield from _ma_walk_elements(child)


def scrape_ma(r2) -> list[Section]:
    try:
        html = fetch_text(MA_CONST_URL, use_us_proxy=True)
    except Exception as e:
        _DROPS.fetch_failed("MA constitution", e)
        return []

    r2_html_key = "state_constitutions/ma/source/ma_constitution.html"
    put_if_changed(r2, r2_html_key, html.encode("utf-8"), "text/html; charset=utf-8")
    r2_html_url = public_url(r2_html_key)

    soup = BeautifulSoup(html, "html.parser")
    content = soup.select_one(".content") or soup.find("main") or soup

    out: list[Section] = []
    part: int | None = None
    cur_chapter: str | None = None
    cur_chapter_title = ""
    cur_section: str | None = None
    cur_article: str | None = None
    pending: list[str] = []
    # Two of Part 2's chapter/section units have real body text but NO
    # further "Article N." leaf marker at all -- confirmed live 2026-08-08:
    # Chapter IV ("Delegates to Congress", no Section level either) and
    # Chapter V's own Section II (one single paragraph on "the encouragement
    # of literature", sitting directly under a Section heading with nothing
    # further to subdivide it). The per-article accumulation below only ever
    # fires once an h4 opens `cur_article`, so both would otherwise vanish
    # entirely. `chapter_pending`/`chapter_had_article` track this in
    # parallel, scoped to whatever (chapter, section) is current: every h3
    # match -- chapter-level OR section-level -- flushes and resets both, so
    # a chapter/section unit that closes having opened zero Articles gets
    # its accumulated body emitted as a single section_number="0" fallback,
    # the same whole-unit fallback convention `_emit_sections_from_articles`
    # already uses when a normal article has no Section markers.
    chapter_pending: list[str] = []
    chapter_had_article = False

    def flush_chapter() -> None:
        nonlocal chapter_pending, chapter_had_article
        if part == 2 and cur_chapter is not None and not chapter_had_article and chapter_pending:
            composite = f"2.{cur_chapter}.{cur_section}" if cur_section else f"2.{cur_chapter}"
            title = f"Mass. Const. Pt. 2, Ch. {cur_chapter}" + (
                f", Sec. {cur_section}" if cur_section else ""
            )
            text = "\n".join(chapter_pending)
            sec = _emit_section(
                "ma",
                r2,
                r2_html_url,
                MA_CONST_URL,
                composite,
                "0",
                text,
                section_title=title,
                article_title=cur_chapter_title,
            )
            if sec:
                out.append(sec)
        chapter_pending = []
        chapter_had_article = False

    def flush_article() -> None:
        nonlocal pending, cur_article
        if cur_article is not None and part is not None:
            if part == 1:
                art_id, sec_num = "1", cur_article
                title = f"Mass. Const. Pt. 1, Art. {cur_article}"
                art_title = ""
            else:
                chap = cur_chapter or "?"
                composite = f"2.{chap}.{cur_section}" if cur_section else f"2.{chap}"
                art_id, sec_num = composite, cur_article
                title = (
                    f"Mass. Const. Pt. 2, Ch. {chap}"
                    + (f", Sec. {cur_section}" if cur_section else "")
                    + f", Art. {cur_article}"
                )
                art_title = cur_chapter_title
            text = "\n".join(pending)
            sec = _emit_section(
                "ma",
                r2,
                r2_html_url,
                MA_CONST_URL,
                art_id,
                sec_num,
                text,
                section_title=title,
                article_title=art_title,
            )
            if sec:
                out.append(sec)
        pending = []
        cur_article = None

    for kind, raw_text in _ma_walk_elements(content):
        text = re.sub(r"\s+", " ", raw_text).strip()
        if not text:
            continue
        if kind == "h2":
            flush_article()
            flush_chapter()
            if text == "PART THE FIRST":
                part, cur_chapter, cur_section = 1, None, None
            elif text == "PART THE SECOND":
                part, cur_chapter, cur_section = 2, None, None
            elif text.upper().startswith("ARTICLES OF AMENDMENT"):
                break
            # else: TOC/Preamble/other h2 -- ignore, part unchanged.
            continue
        if kind == "h3":
            if part != 2 or not _MA_CH_PREFIX_RE.match(text):
                continue  # Part 1's own subtitle h3 -- not a chapter marker.
            flush_article()
            flush_chapter()
            cur_chapter, cur_chapter_title, cur_section = _ma_parse_chapter_heading(text)
            continue
        if kind == "h4":
            m = _MA_ARTICLE_H4_RE.match(text)
            if not m or part is None:
                continue
            flush_article()
            cur_article = m.group(1)
            chapter_had_article = True
            continue
        # kind in ("p", "text") -- body content.
        if cur_article is not None:
            pending.append(text)
        if part == 2 and cur_chapter is not None:
            chapter_pending.append(text)
    flush_article()
    flush_chapter()

    if not out:
        _DROPS.unit_empty("MA (0 sections after parse)")
    print(f"[MA] done: {len(out)} sections")
    return out


# ---------------------------------------------------------------------------
# C07 batch 6: IN, OK, LA, AR, TN, WY (docs/us-corpus/handoffs/constitutions/
# C07_06_pdf_b.md) -- replaces Wikisource for all six. IN/LA/AR/TN/WY are the
# shared PDF-via-pdf_extract recipe already proven for MI/WV/NV/NM/WA; OK is
# this migration's one RTF-sourced state (striprtf, not pdf_extract).
# ---------------------------------------------------------------------------

# Indiana -- iga.in.gov's own published PDF (same parent domain as IN's
# statute scraper, which already needs the US proxy against this
# host -- both the HTML landing page and the PDF itself defeated direct
# fetch in batch research). Verified live 2026-08-08: clean "ARTICLE N.\n
# Title.\nSection N. ... (History: As Amended <date>)." shape, no TOC
# collision (the article-level ALL-CAPS "ARTICLE N." heading never repeats
# elsewhere in the document), 16 real articles -- ahead of the Wikisource
# baseline's 11.
IN_CONST_PDF = (
    "https://iga.in.gov/publications/indiana_constitution/Constitution%20(as%20amended%202024).pdf"
)
_IN_ARTICLE_RE = re.compile(r"\nARTICLE\s+(\d+)\.\s*\n([^\n]+)\n")
_IN_SECTION_RE = re.compile(r"\nSection\s+(\d+[A-Za-z]?)\.\s*")
# Every amended section carries a trailing "(History: As Amended <date>)."
# note, the same bracket-annotation shape NV's amendment extractor already
# handles (see _NV_BRACKET_RE) -- 87 of these confirmed live 2026-08-08, only
# the wrapping punctuation differs (parens, not square brackets).
_IN_HISTORY_RE = re.compile(r"\(History:[^)]*\)")


def _in_amendment_years(raw_text: str) -> list[int]:
    return _years_in_matched_spans(raw_text, _IN_HISTORY_RE)


_AMENDMENT_YEAR_EXTRACTORS["in"] = _in_amendment_years


def scrape_in(r2) -> list[Section]:
    out: list[Section] = []
    pdf_bytes = _fetch_pdf_bytes_resilient(IN_CONST_PDF)
    if not pdf_bytes:
        _DROPS.fetch_failed("IN constitution PDF", "download failed")
        return out

    r2_pdf_key = "state_constitutions/in/source/in_constitution.pdf"
    put_if_changed(r2, r2_pdf_key, pdf_bytes, "application/pdf")
    r2_pdf_url = public_url(r2_pdf_key)

    text = _pdf_to_text(pdf_bytes)
    if not text:
        _DROPS.unit_empty("IN (PDF text extraction empty)")
        return out

    art_matches = list(_IN_ARTICLE_RE.finditer(text))
    if not art_matches:
        _DROPS.unit_empty(f"IN (no ARTICLE markers in {len(text)} chars)")
        return out
    art_iter: list[tuple[str, str]] = []
    art_titles: dict[str, str] = {}
    for i, m in enumerate(art_matches):
        art_id = m.group(1)
        art_titles[art_id] = re.sub(r"\s+", " ", m.group(2)).strip()
        start = m.end()
        end = art_matches[i + 1].start() if i + 1 < len(art_matches) else len(text)
        art_iter.append((art_id, text[start:end]))

    return _emit_sections_from_articles(
        "in", r2, IN_CONST_PDF, art_iter, _IN_SECTION_RE, r2_pdf_url=r2_pdf_url, article_titles=art_titles
    )


# ---------------------------------------------------------------------------
# Oklahoma -- the one RTF-sourced state in this migration. Same domain AND
# same base folder (OK_Statutes/CompleteTitles/) as OK's statute scraper
# (which already fetches os{N}.pdf from this exact path, no proxy needed).
# The whole-document RTF (AllOKConstitutionArticles.rtf, 2.3MB) sits right
# next to those PDF titles. Verified live 2026-08-08: direct fetch (no
# the US proxy) succeeds. striprtf, not pdf_extract -- this is the only
# non-PDF, non-HTML source in this migration.
#
# The document opens with a full table of contents that repeats the SAME
# "ARTICLE N - Title" heading text the real body uses (the MI/NM TOC-
# collision bug class) -- but here the whole front matter is bounded by a
# single repeated marker: "PREAMBLE" appears exactly twice, once as the
# TOC's own first line, once as the real preamble heading. Verified live
# 2026-08-08. Taking everything from the SECOND "PREAMBLE" onward is the
# same fix already used for SD's Wikisource TOC (_sd_skip_toc) applied to a
# different source.
#
# Sections repeat their own article's Roman numeral as a hyphenated prefix
# ("SECTION VII-1.", "SECTION XXIX-6.") uniformly across the WHOLE document
# in this official source (not just Article X/XIV the way the old Wikisource
# transcription had it -- see the now-orphaned _OK_SECTION_RE above). Seven
# of OK's 30 real articles are themselves lettered sub-articles (VII-A,
# VII-B, XII-A, XIII-A, XIII-B, XXV-A, XXVIII-A -- e.g. "ARTICLE VII-A -
# Court on the Judiciary"), so the article-id capture must keep an optional
# "-LETTER" suffix (no surrounding spaces) distinct from the title-separator
# hyphen (always space-padded); confirmed live 2026-08-08 this is a lossless
# superset -- 37 real article/sub-article units, 456 sections, well past the
# Wikisource baseline's 405 points / 16 articles.
OK_CONST_RTF = "https://www.oklegislature.gov/OK_Statutes/CompleteTitles/AllOKConstitutionArticles.rtf"
_OK_OFFICIAL_ARTICLE_RE = re.compile(r"\nARTICLE\s+([IVXLC]+(?:-[A-Z])?)\s*-\s*([^\n]+)\n")
_OK_OFFICIAL_SECTION_RE = re.compile(r"\nSECTION\s+[IVXLC]+(?:-[A-Z])?-(\d+[A-Za-z]?)\.\s*")
# Every amended/added/repealed section ends with an "Added/Amended/Repealed
# by State Question No. N, ... adopted at [an/a general] election held
# [on] <date>." citation line -- verified live 2026-08-08 against 259 of
# these across the document; each isolated line is passed through the same
# _years_in_matched_spans sweep already used for NV/WV/etc, so a proposing
# Legislature session-law year mentioned in the same line (e.g. "Laws 1988")
# counts as a real amendment-event year too, matching the existing
# convention (see NV's own docstring on this).
_OK_CITE_RE = re.compile(r"(?:Added|Amended|Repealed) by[^\n]*")


def _ok_amendment_years(raw_text: str) -> list[int]:
    return _years_in_matched_spans(raw_text, _OK_CITE_RE)


_AMENDMENT_YEAR_EXTRACTORS["ok"] = _ok_amendment_years


def scrape_ok(r2) -> list[Section]:
    out: list[Section] = []
    try:
        r = SESSION.get(OK_CONST_RTF, timeout=90, headers={"User-Agent": _MOZ_UA})
        r.raise_for_status()
        rtf_bytes = r.content
    except Exception as e:
        _DROPS.fetch_failed("OK constitution RTF", e)
        return out

    r2_rtf_key = "state_constitutions/ok/source/ok_constitution.rtf"
    put_if_changed(r2, r2_rtf_key, rtf_bytes, "application/rtf")
    # Neither Section.r2_html_url nor r2_pdf_url is a literal match for RTF;
    # r2_html_url is the field _build_one already treats as a generic
    # whole-document provenance pointer (r2_source_url), never as a claim
    # about actual format, so it is reused here rather than r2_pdf_url
    # (which DOES add "pdf" to the payload's r2_formats_available list --
    # setting it for an RTF source would be a real, not just cosmetic, lie).
    r2_rtf_url = public_url(r2_rtf_key)

    from striprtf.striprtf import rtf_to_text

    raw = rtf_bytes.decode("cp1252", errors="replace")
    text = rtf_to_text(raw)
    if not text:
        _DROPS.unit_empty("OK (RTF text extraction empty)")
        return out

    preamble_idxs = [m.start() for m in re.finditer(r"\bPREAMBLE\b", text)]
    if len(preamble_idxs) < 2:
        _DROPS.unit_empty(f"OK (expected 2 PREAMBLE markers, found {len(preamble_idxs)})")
        return out
    body_text = text[preamble_idxs[-1] :]

    art_matches = list(_OK_OFFICIAL_ARTICLE_RE.finditer(body_text))
    if not art_matches:
        _DROPS.unit_empty(f"OK (no ARTICLE markers in {len(body_text)} chars)")
        return out
    art_iter: list[tuple[str, str]] = []
    art_titles: dict[str, str] = {}
    for i, m in enumerate(art_matches):
        art_id = m.group(1)
        art_titles[art_id] = re.sub(r"\s+", " ", m.group(2)).strip()
        start = m.end()
        end = art_matches[i + 1].start() if i + 1 < len(art_matches) else len(body_text)
        art_iter.append((art_id, body_text[start:end]))

    return _emit_sections_from_articles(
        "ok",
        r2,
        OK_CONST_RTF,
        art_iter,
        _OK_OFFICIAL_SECTION_RE,
        r2_html_url=r2_rtf_url,
        article_titles=art_titles,
    )


# ---------------------------------------------------------------------------
# Louisiana -- replaces the Wikisource-multipart source (see
# scrape_louisiana_wikisource_multipart above, now removed from
# STATE_SCRAPERS). The LA Senate's own compiled PDF (senate.la.gov,
# "As amended through calendar year 2023") needs the US proxy -- every
# legislative-branch LA domain came back DNS-unreachable direct from
# research, consistent with a real geo-fence rather than a site outage.
#
# The document opens with a ~16K-char table of contents using mixed-case
# "Article I. Declaration of Rights" / "§N. Title" headings; the REAL
# body re-heads every article in ALL CAPS on its own line ("ARTICLE I.\n
# DECLARATION OF RIGHTS\n"), which the TOC never does -- the same
# case-based TOC/body discriminator already used for NM's running header
# (_NM_ARTICLE_RE). Verified live 2026-08-08: exactly 14 ALL-CAPS matches,
# matching the Wikisource baseline's own 14-article count with much more
# complete section coverage (327 plain + 21 decimal "N.1" sections).
# Real sections repeat their catchline as BOTH a "§N. Catchline" line
# and a "Section N. <body>" restatement immediately after (the same
# caption-then-restatement shape already seen in DE) -- splitting on the
# "Section N." keyword is simplest since it never collides with the TOC
# (the TOC only ever uses "§", never the word "Section").
LA_CONST_PDF = "https://senate.la.gov/Documents/LAConstitution.pdf"
# Every page repeats "Compiled from the La. Senate Statutory Database.\n(As
# amended through calendar year <year>)\n-<roman-or-digit>-\n", which lands
# mid-section wherever a page breaks (106 occurrences confirmed live
# 2026-08-08) -- stripped before any split runs, both to keep section bodies
# clean and because its own "<year>" would otherwise be swept into
# _LA_AMENDED_BY_ACTS_RE's amendment-year extraction as a false hit.
_LA_FOOTER_RE = re.compile(
    r"\nCompiled from the La\. Senate Statutory Database\.\n\(As amended through calendar year \d+\)\n-[ivxlc\d]+-\n*",
    re.IGNORECASE,
)
_LA_HYPHEN_WRAP_RE = re.compile(r"(\w)-\n(\w)")
_LA_ARTICLE_RE = re.compile(r"\nARTICLE\s+([IVXLC]+)\.\s*\n([A-Z][A-Z .,;'\-]+)\n")
_LA_SECTION_RE = re.compile(r"\nSection\s+(\d+(?:\.\d+)?[A-Za-z]?)\.\s*")


def _la_clean_body(text: str) -> str:
    text = _LA_FOOTER_RE.sub("\n", text)
    return _LA_HYPHEN_WRAP_RE.sub(r"\1\2", text)


def scrape_la(r2) -> list[Section]:
    out: list[Section] = []
    pdf_bytes = _fetch_pdf_bytes_resilient(LA_CONST_PDF)
    if not pdf_bytes:
        _DROPS.fetch_failed("LA constitution PDF", "download failed")
        return out

    r2_pdf_key = "state_constitutions/la/source/la_constitution.pdf"
    put_if_changed(r2, r2_pdf_key, pdf_bytes, "application/pdf")
    r2_pdf_url = public_url(r2_pdf_key)

    text = _pdf_to_text(pdf_bytes)
    if not text:
        _DROPS.unit_empty("LA (PDF text extraction empty)")
        return out
    body_text = _la_clean_body(text)

    art_matches = list(_LA_ARTICLE_RE.finditer(body_text))
    if not art_matches:
        _DROPS.unit_empty(f"LA (no ALL-CAPS ARTICLE markers in {len(body_text)} chars)")
        return out
    art_iter: list[tuple[str, str]] = []
    art_titles: dict[str, str] = {}
    for i, m in enumerate(art_matches):
        art_id = m.group(1)
        art_titles[art_id] = re.sub(r"\s+", " ", m.group(2)).strip()
        start = m.end()
        end = art_matches[i + 1].start() if i + 1 < len(art_matches) else len(body_text)
        art_iter.append((art_id, body_text[start:end]))

    return _emit_sections_from_articles(
        "la", r2, LA_CONST_PDF, art_iter, _LA_SECTION_RE, r2_pdf_url=r2_pdf_url, article_titles=art_titles
    )


# ---------------------------------------------------------------------------
# Arkansas -- arkleg.state.ar.us's own published PDF (per the batch research
# URL, assembly/Summary/ArkansasConstitution1874.pdf) returns a genuine HTTP
# 500 from the live server -- verified live 2026-08-08 through the US proxy AND through the scraping service's residential pool (both actually reach the
# host; the host itself errors on this exact path), so this is a broken
# link/moved file, not a geo-fence or bot-challenge block that a fetch
# strategy can route around. No working alternate path was found on
# arkleg.state.ar.us itself (its FTPDocument file-serving endpoint, which
# serves hundreds of other arkleg documents fine, 404s-as-a-blank-GIF for
# every path guess tried).
#
# Fallback source: jonesboroar.gov's DocumentCenter mirror of the IDENTICAL
# Bureau of Legislative Research-published "Constitution of the State of
# Arkansas of 1874" compiled document (same title page, same pagination,
# byte-identical body text to every other .gov mirror of this same BLR
# publication found in research, e.g. izardcountyar.org) -- a municipal .gov
# host mirroring the state's own official publication, not a third-party/
# volunteer transcription. FLAG FOR HUMAN FOLLOW-UP: this compiled edition
# is stamped "Updated as of October 5, 2015" and its last amendment is
# AMEND. 94 (2014) -- Arkansas has adopted further constitutional amendments
# since (e.g. 2016 ballot measures), so this source is genuine, official,
# and far more complete than Wikisource's 236-point baseline, but is NOT
# currently amended. Revisit if/when the real arkleg.state.ar.us link is
# restored.
#
# Structure: a large front-loaded table of contents (every amendment's own
# sub-sections listed too) ends at the single real "PREAMBLE" heading
# (verified live 2026-08-08: exactly one occurrence in the whole document).
# The real body then has THREE distinct unit shapes back to back: 20 plain
# "Article N \nTitle" articles, then a "SCHEDULE" transitional-provisions
# block, then ~93 "AMEND. N.\nTitle" amendment units (Arkansas amendments are
# separate numbered units, not folded into their target article the way
# NV's are) -- each unit's own sections use the same "§ N.  Title."
# marker throughout. Mirrors CT's main-articles-then-amendments split
# (_ct_split_articles) rather than the generic single-level helper.
AR_CONST_PDF = "https://www.jonesboroar.gov/DocumentCenter/View/290/Arkansas-Constitution-PDF"
_AR_ARTICLE_RE = re.compile(r"\nArticle\s+(\d+)\s*\n([^\n]+)\n")
_AR_SCHEDULE_RE = re.compile(r"\nSCHEDULE\s*\n")
_AR_PROCLAMATION_RE = re.compile(r"\nPROCLAMATION\s*\n")
_AR_AMEND_RE = re.compile(r"\nAMEND\.\s+(\d+)\.\s*\n([^\n]+)\n")
_AR_SECTION_RE = re.compile(r"\n§\s*(\d+[A-Za-z]?(?:\.\d+)?)\.\s*")
# Lone page-number lines ("\n\n\n65 \n \n") repeat throughout the real body
# (confirmed live 2026-08-08) -- cosmetic noise inside a section's raw_text,
# stripped for cleanliness the same way WA/NM strip their own footers.
_AR_PAGE_NUM_RE = re.compile(r"\n[ \t]*\d{1,3}[ \t]*\n(?=\s*\n)")
# No amendment_years extractor: unlike NV/IN/OK, AR's ~93 numbered
# amendments do not carry a consistent per-section ratification-date
# citation in the body text itself (the adoption year lives in the
# amendment's own external legislative history, not printed inline here) --
# verified live 2026-08-08 against a sample of amendment sections. Left
# unregistered rather than guessing at a pattern that would silently
# misfire on some amendments and not others.


def _ar_split_units(body_text: str) -> tuple[list[tuple[str, str]], dict[str, str]]:
    art_matches = list(_AR_ARTICLE_RE.finditer(body_text))
    art_iter: list[tuple[str, str]] = []
    art_titles: dict[str, str] = {}
    for i, m in enumerate(art_matches):
        art_id = m.group(1)
        art_titles[art_id] = re.sub(r"\s+", " ", m.group(2)).strip()
        start = m.end()
        end = art_matches[i + 1].start() if i + 1 < len(art_matches) else len(body_text)
        art_iter.append((art_id, body_text[start:end]))

    sched_m = _AR_SCHEDULE_RE.search(body_text)
    tail_start = len(body_text)
    if sched_m and art_iter:
        last_id, _ = art_iter[-1]
        last_start = art_matches[-1].end()
        art_iter[-1] = (last_id, body_text[last_start : sched_m.start()])
        procl_m = _AR_PROCLAMATION_RE.search(body_text, sched_m.end())
        sched_end = procl_m.start() if procl_m else len(body_text)
        art_iter.append(("SCHED", body_text[sched_m.end() : sched_end]))
        art_titles["SCHED"] = "Schedule"
        tail_start = procl_m.end() if procl_m else len(body_text)

    tail = body_text[tail_start:]
    amend_matches = list(_AR_AMEND_RE.finditer(tail))
    for i, m in enumerate(amend_matches):
        amend_id = f"AMEND{m.group(1)}"
        art_titles[amend_id] = re.sub(r"\s+", " ", m.group(2)).strip()
        start = m.end()
        end = amend_matches[i + 1].start() if i + 1 < len(amend_matches) else len(tail)
        art_iter.append((amend_id, tail[start:end]))

    return art_iter, art_titles


def scrape_ar(r2) -> list[Section]:
    out: list[Section] = []
    pdf_bytes = _fetch_pdf_bytes_resilient(AR_CONST_PDF)
    if not pdf_bytes:
        _DROPS.fetch_failed("AR constitution PDF", "download failed")
        return out

    r2_pdf_key = "state_constitutions/ar/source/ar_constitution.pdf"
    put_if_changed(r2, r2_pdf_key, pdf_bytes, "application/pdf")
    r2_pdf_url = public_url(r2_pdf_key)

    text = _pdf_to_text(pdf_bytes)
    if not text:
        _DROPS.unit_empty("AR (PDF text extraction empty)")
        return out

    preamble_m = re.search(r"\nPREAMBLE\s*\n", text)
    if not preamble_m:
        _DROPS.unit_empty(f"AR (no real PREAMBLE marker in {len(text)} chars)")
        return out
    body_text = _AR_PAGE_NUM_RE.sub("\n", text[preamble_m.start() :])

    art_iter, art_titles = _ar_split_units(body_text)
    if not art_iter:
        _DROPS.unit_empty(f"AR (no article/amendment units in {len(body_text)} chars)")
        return out

    return _emit_sections_from_articles(
        "ar", r2, AR_CONST_PDF, art_iter, _AR_SECTION_RE, r2_pdf_url=r2_pdf_url, article_titles=art_titles
    )


# ---------------------------------------------------------------------------
# Tennessee -- the TN Secretary of State's own published PDF
# (publications.tnsosfiles.com, 26pp, "2023 TN Constitution"). A direct
# WebFetch 403'd in batch research but plain curl got 200 (AWS/CloudFront-
# hosted, no hard geoblock apparent) -- fetched here via the same
# proxy-then-scraping-service resilient helper as the rest of this batch
# (_fetch_pdf_bytes_resilient), which already tries the plain proxy path
# first.
#
# Cleanest PDF found across the whole Tier-2 group: single-column real text
# layer, zero page-footer noise, only 3 line-wrap hyphenation artifacts in
# the whole document. Verified live 2026-08-08: 11 ARTICLE markers / 151
# Section markers, matching the batch research's own live count. This is
# the CURRENT, INTEGRATED text (each amendment is folded directly into the
# section it amended, not appended as a separate numbered unit the way
# AR/NV do it) -- no per-section amendment-citation pattern was found
# (confirmed live 2026-08-08: zero matches for the old Wikisource-era
# _TN_BRACKET_RE against this source), so amendment_years stays empty for
# every TN section here; _AMENDMENT_YEAR_EXTRACTORS["tn"] is left pointing
# at the now-inert Wikisource-era lambda rather than removed, matching this
# file's existing orphaned-extractor convention (see the MD/_MD_SECTION_RE
# comment above).
TN_CONST_PDF = "https://publications.tnsosfiles.com/pub/2023%20TN%20Constitution.pdf"
_TN_ARTICLE_RE = re.compile(r"\nARTICLE\s+([IVXLC]+)\.?\s*\n([^\n]+)\n")
_TN_SECTION_RE = re.compile(r"\nSection\s+(\d+[A-Za-z]?)\.\s*")
# TN's unlabeled transitional "Schedule" (no ARTICLE heading of its own)
# trails directly after Article XI's real Section 19 and restarts its own
# numbering at "Section 1." -- verified live 2026-08-08 (Article XI Sections
# 1-4 each match twice: the real Miscellaneous Provisions section, then the
# Schedule's own unrelated Section 1-4). The shared helper's existing "-v2"
# suffix (see _emit_sections_from_articles) already handles this as two
# separate, real, citable sections rather than a collision -- same
# mechanism NV uses for its dual-effective-date sections, not a new rule.


def scrape_tn(r2) -> list[Section]:
    out: list[Section] = []
    pdf_bytes = _fetch_pdf_bytes_resilient(TN_CONST_PDF)
    if not pdf_bytes:
        _DROPS.fetch_failed("TN constitution PDF", "download failed")
        return out

    r2_pdf_key = "state_constitutions/tn/source/tn_constitution.pdf"
    put_if_changed(r2, r2_pdf_key, pdf_bytes, "application/pdf")
    r2_pdf_url = public_url(r2_pdf_key)

    text = _pdf_to_text(pdf_bytes)
    if not text:
        _DROPS.unit_empty("TN (PDF text extraction empty)")
        return out

    art_matches = list(_TN_ARTICLE_RE.finditer(text))
    if not art_matches:
        _DROPS.unit_empty(f"TN (no ARTICLE markers in {len(text)} chars)")
        return out
    art_iter: list[tuple[str, str]] = []
    art_titles: dict[str, str] = {}
    for i, m in enumerate(art_matches):
        art_id = m.group(1)
        art_titles[art_id] = re.sub(r"\s+", " ", m.group(2)).strip()
        start = m.end()
        end = art_matches[i + 1].start() if i + 1 < len(art_matches) else len(text)
        art_iter.append((art_id, text[start:end]))

    return _emit_sections_from_articles(
        "tn", r2, TN_CONST_PDF, art_iter, _TN_SECTION_RE, r2_pdf_url=r2_pdf_url, article_titles=art_titles
    )


# ---------------------------------------------------------------------------
# Wyoming -- the Wyoming Legislature's own PDF (wyoleg.gov), which treats the
# constitution as codified "Title 97". Plain IIS hosting, no WAF fingerprint
# seen in batch research; fetched here with the same proxy-first convention
# as the rest of this batch (unlikely to be strictly necessary but cheap to
# keep consistent).
#
# Clean inline "ARTICLE N - TITLE" (all caps) article headings and
# "Article N, Section M  Title." section headings (the section marker
# redundantly repeats its own article's number, harmless since the section
# split runs inside that article's already-isolated body). Verified live
# 2026-08-08: 21 articles / 316 sections, matching the batch research count
# and the Wikisource baseline exactly on article count while recovering
# real section-level structure Wikisource's page also had. A handful of
# real amendment/repeal notes are bracketed ("[Superseded by Article 18,
# Section 3 as amended 1922.]", "[Repealed by Laws 1965.]") -- a different,
# smaller set than the old Wikisource-era "This section was amended..."
# phrasing _WY_NOTE_RE expected (zero matches against this source, verified
# live 2026-08-08), so _AMENDMENT_YEAR_EXTRACTORS["wy"] is reassigned here
# to the bracket-based extractor, the same pattern already used for NV/WV.
WY_CONST_PDF = "https://wyoleg.gov/statutes/compress/title97.pdf"
_WY_ARTICLE_RE = re.compile(r"\nARTICLE\s+(\d+)\s*-\s*([^\n]+)\n")
_WY_SECTION_RE = re.compile(r"\nArticle\s+\d+,\s*Section\s+(\d+[A-Za-z]?)[^\S\n]*")
_WY_BRACKET_RE = re.compile(r"\[[^\[\]]{0,300}\]")


def _wy_amendment_years(raw_text: str) -> list[int]:
    return _years_in_matched_spans(raw_text, _WY_BRACKET_RE)


_AMENDMENT_YEAR_EXTRACTORS["wy"] = _wy_amendment_years


def scrape_wy(r2) -> list[Section]:
    out: list[Section] = []
    pdf_bytes = _fetch_pdf_bytes_resilient(WY_CONST_PDF)
    if not pdf_bytes:
        _DROPS.fetch_failed("WY constitution PDF", "download failed")
        return out

    r2_pdf_key = "state_constitutions/wy/source/wy_constitution.pdf"
    put_if_changed(r2, r2_pdf_key, pdf_bytes, "application/pdf")
    r2_pdf_url = public_url(r2_pdf_key)

    text = _pdf_to_text(pdf_bytes)
    if not text:
        _DROPS.unit_empty("WY (PDF text extraction empty)")
        return out

    art_matches = list(_WY_ARTICLE_RE.finditer(text))
    if not art_matches:
        _DROPS.unit_empty(f"WY (no ARTICLE markers in {len(text)} chars)")
        return out
    art_iter: list[tuple[str, str]] = []
    art_titles: dict[str, str] = {}
    for i, m in enumerate(art_matches):
        art_id = m.group(1)
        art_titles[art_id] = re.sub(r"\s+", " ", m.group(2)).strip()
        start = m.end()
        end = art_matches[i + 1].start() if i + 1 < len(art_matches) else len(text)
        art_iter.append((art_id, text[start:end]))

    return _emit_sections_from_articles(
        "wy", r2, WY_CONST_PDF, art_iter, _WY_SECTION_RE, r2_pdf_url=r2_pdf_url, article_titles=art_titles
    )


STATE_SCRAPERS: dict[str, Callable] = {
    "ca": scrape_ca,
    "tx": scrape_tx,
    "pa": scrape_pa,
    "ky": scrape_ky,
    "mi": scrape_mi,
    "wv": scrape_wv,
    "nv": scrape_nv,
    "nm": scrape_nm,
    "wa": scrape_wa,
    "mn": scrape_mn,
    "fl": scrape_fl,
    "il": scrape_il,
    "az": scrape_az,
    "md": scrape_md,
    "al": scrape_al,
    "mt": scrape_mt,
    "oh": scrape_oh,
    "va": scrape_va,
    "wi": scrape_wi,
    "ne": scrape_ne,
    "ut": scrape_ut,
    "hi": scrape_hi,
    "ks": scrape_ks,
    "mo": scrape_mo,
    "id": scrape_id,
    "nj": scrape_nj,
    "or": scrape_or,
    "ri": scrape_ri,
    "vt": scrape_vt,
    "ma": scrape_ma,
    "in": scrape_in,
    "ok": scrape_ok,
    "la": scrape_la,
    "ar": scrape_ar,
    "tn": scrape_tn,
    "wy": scrape_wy,
    "ny": scrape_ny,
    "ms": scrape_ms,
}
# Louisiana's old Wikisource-multipart override (STATE_SCRAPERS["la"] =
# scrape_louisiana_wikisource_multipart) removed 2026-08-08: replaced by the
# official-source scrape_la above (senate.la.gov PDF, C07 batch 6). This line
# ran AFTER the dict literal above, so leaving it in would have silently
# overwritten "la"'s new entry back to the old multipart Wikisource scraper.
# scrape_louisiana_wikisource_multipart itself is left in the file unused
# rather than deleted, matching this file's convention for retired scrapers.


def merge_jsonl(path: Path, new_secs: list[Section]) -> int:
    """Append-safe merge by act_id.

    A section now maps to a LIST of chunk records, so the whole list is replaced
    for a re-scraped act_id. Keying on point_id instead would leave the stale
    tail chunks of a section that got shorter sitting in the file forever.
    """
    existing: dict[str, list[dict]] = {}
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                aid = rec.get("metadata", {}).get("act_id")
                if aid:
                    existing.setdefault(aid, []).append(rec)
            except json.JSONDecodeError:
                continue
    for sec in new_secs:
        recs = to_chunk_records(sec)
        existing[recs[0]["metadata"]["act_id"]] = recs
    total = 0
    with open(path, "w", encoding="utf-8") as fh:
        for recs in existing.values():
            for rec in recs:
                fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
                total += 1
    return total


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--states", default="", help="Comma-separated state codes (ca,tx,ny,fl,...).")
    ap.add_argument("--all", action="store_true", help="Run all configured state scrapers.")
    ap.add_argument(
        "--workers", type=int, default=8, help="Number of parallel state scrapers (default: 8)."
    )
    args = ap.parse_args()

    _load_env()
    r2 = _r2_client()
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if args.all:
        state_codes = list(STATE_SCRAPERS.keys())
    elif args.states:
        state_codes = [s.strip().lower() for s in args.states.split(",") if s.strip()]
    else:
        ap.error("specify --states ca,tx or --all")

    workers = max(1, args.workers)
    print(f"=== State Constitutions: {len(state_codes)} states (workers={workers}) ===")
    all_secs: list[Section] = []

    def _run_one(st: str) -> tuple[str, list[Section] | None, str | None]:
        if st not in STATE_SCRAPERS:
            return (st, None, "no scraper configured")
        try:
            return (st, STATE_SCRAPERS[st](r2), None)
        except Exception as e:
            return (st, None, str(e)[:200])

    if workers > 1 and len(state_codes) > 1:
        from concurrent.futures import ThreadPoolExecutor, as_completed

        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures = [ex.submit(_run_one, st) for st in state_codes]
            for fut in as_completed(futures):
                st, secs, err = fut.result()
                if err:
                    _DROPS.state_failed(st, err)
                elif secs is not None:
                    all_secs.extend(secs)
    else:
        for st in state_codes:
            _, secs, err = _run_one(st)
            if err:
                _DROPS.state_failed(st, err)
            elif secs is not None:
                all_secs.extend(secs)

    total = merge_jsonl(OUT, all_secs)
    print(f"\n=== JSONL has {total} state-constitution chunks at {OUT}")
    if _DROPS.hard_drops:
        # A unit was dropped (fetch/parse failure, a whole state that raised, or
        # a payload missing a REQUIRED field). The good rows were still written
        # (merge is non-destructive), but we exit non-zero so a lossy run is
        # never reported as a success.
        print(f"\nBROKEN RUN: {_DROPS.summary()} -> exiting non-zero.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
