#!/usr/bin/env python3
"""Ingest the Utah Code into the state-statutes pipeline.

le.utah.gov publishes each Title as a structured XML file at
    /xcode/Title{N}/C{N}_{version}.xml

The version token (e.g. C3_1800010118000101) is discovered once from the
TOC page (rendered via Playwright; the TOC itself is JS). After that,
each Title's XML is fetched directly via HTTPS — fast, no rate limit
issues, no Playwright needed for the actual data.

XML schema (verified 2026-05-28):
    <title number="N">
      <catchline>...title name...</catchline>
      <chapter number="N-M">
        <catchline>...chapter name...</catchline>
        <section number="N-M-K">
          <histories><history>...</history></histories>
          <catchline>...section heading...</catchline>
          (body text + optional <subsection number="...">...</subsection>)
        </section>
        ...
      </chapter>
    </title>

Output:
    JSONL at <OUT_DIR>/state_ut_statutes.jsonl
    Compatible with sync_states_to_supabase.py (corpus_type='state', state='ut').
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from xml.etree import ElementTree as ET

import requests

sys.stdout.reconfigure(line_buffering=True)  # type: ignore[attr-defined]

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = Path(os.environ.get("OUT_DIR", "./data"))
OUT = DATA_DIR / "state_ut_statutes.jsonl"
TOC_DISCOVERY_CACHE = Path("/tmp/ut_title_versions.json")

UT_BASE = "https://le.utah.gov"
UT_TOC = f"{UT_BASE}/xcode/code.html"

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
    import urllib.parse
    proxy_user = f"{user}-US-rotate"
    host = os.environ.get("WEBSHARE_PROXY_HOST", "p.webshare.io")
    port = os.environ.get("WEBSHARE_PROXY_PORT", "80")
    url = f"http://{urllib.parse.quote(proxy_user)}:{urllib.parse.quote(pwd)}@{host}:{port}"
    return {"http": url, "https": url}


SESSION = requests.Session()
SESSION.headers.update({"User-Agent": _MOZ_UA})


# ---------------------------------------------------------------------------
# TOC discovery (one-time, uses Playwright to render the JS-only TOC)
# ---------------------------------------------------------------------------

async def _discover_title_xml_urls() -> dict[str, str]:
    """Return {title_num: xml_url} for every UT Title on the TOC."""
    from playwright.async_api import async_playwright
    proxy = _us_proxies()
    if proxy is None:
        raise RuntimeError("Webshare proxy required for UT discovery")
    pw_proxy = {
        "server": "http://p.webshare.io:80",
        "username": proxy["http"].split("//", 1)[1].split(":", 1)[0],
        "password": proxy["http"].split(":", 2)[2].split("@", 1)[0],
    }
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            proxy=pw_proxy,
            args=["--disable-blink-features=AutomationControlled"],
        )
        ctx = await browser.new_context(
            user_agent=_MOZ_UA,
            viewport={"width": 1280, "height": 900},
            locale="en-US",
        )
        await ctx.add_init_script(
            "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"
        )
        page = await ctx.new_page()
        try:
            print(f"  [UT discovery] fetching TOC: {UT_TOC}", flush=True)
            await page.goto(UT_TOC, wait_until="domcontentloaded", timeout=60000)
            # Wait until at least one Title link is visible in DOM, or 30s timeout
            elapsed = 0
            title_anchors: list[str] = []
            while elapsed < 30000:
                await page.wait_for_timeout(2000)
                elapsed += 2000
                title_anchors = await page.evaluate(
                    "Array.from(document.querySelectorAll('a[href]')).map(a => a.href).filter(h => h.includes('/xcode/Title'))"
                )
                if title_anchors:
                    break
            print(f"  [UT discovery] {len(title_anchors)} Title anchors after {elapsed}ms", flush=True)
            if title_anchors:
                print(f"  [UT discovery] sample: {title_anchors[:3]}", flush=True)
        finally:
            await browser.close()

    out: dict[str, str] = {}
    for href in title_anchors:
        # Title page href: /xcode/Title3/3.html?v=C3_1800010118000101
        m = re.search(
            r"/xcode/Title([0-9A-Za-z]+)/[0-9A-Za-z]+\.html\?v=(C[0-9A-Za-z]+_[0-9A-Za-z]+)",
            href,
        )
        if not m:
            continue
        title_num = m.group(1)
        version = m.group(2)
        # XML url is /xcode/Title{N}/{version}.xml
        out[title_num] = f"{UT_BASE}/xcode/Title{title_num}/{version}.xml"
    return out


def get_title_xml_urls(force_rediscover: bool = False) -> dict[str, str]:
    if not force_rediscover and TOC_DISCOVERY_CACHE.exists():
        try:
            return json.loads(TOC_DISCOVERY_CACHE.read_text())
        except Exception:
            pass
    urls = asyncio.run(_discover_title_xml_urls())
    TOC_DISCOVERY_CACHE.parent.mkdir(parents=True, exist_ok=True)
    TOC_DISCOVERY_CACHE.write_text(json.dumps(urls, indent=2))
    return urls


# ---------------------------------------------------------------------------
# XML parsing
# ---------------------------------------------------------------------------

@dataclass
class Section:
    title_num: str
    title_name: str
    chapter_num: str
    chapter_name: str
    section_num: str
    section_heading: str
    raw_text: str
    source_url: str


def _elem_text(elem: ET.Element) -> str:
    """Collect all descendant text. <subsection> body becomes inline text."""
    parts: list[str] = []
    if elem.text:
        parts.append(elem.text)
    for child in elem:
        tag = child.tag
        if tag in ("histories", "history", "modyear", "modchap"):
            # Skip amendment history blocks; keep them out of the embed body
            if child.tail:
                parts.append(child.tail)
            continue
        if tag == "catchline":
            # Already captured as heading; skip
            if child.tail:
                parts.append(child.tail)
            continue
        if tag == "subsection":
            num = child.attrib.get("number", "")
            sub_text = _elem_text(child).strip()
            label = num.split("(")[-1].split(")")[0] if "(" in num else ""
            prefix = f"({label}) " if label else ""
            parts.append(f"\n{prefix}{sub_text}")
            if child.tail:
                parts.append(child.tail)
            continue
        # Generic recursion (tab, xref, etc.)
        parts.append(_elem_text(child))
        if child.tail:
            parts.append(child.tail)
    return "".join(parts)


def _harvest_sections(elem: ET.Element, title_num: str, title_name: str,
                       chap_num: str, chap_name: str,
                       part_num: str = "", part_name: str = "") -> list[Section]:
    """Recursively collect <section> children. UT XML nests like:

        title -> chapter -> section
        title -> chapter -> part -> section
        title -> chapter -> part -> subpart -> section  (rare)
    """
    out: list[Section] = []
    for child in elem:
        if child.tag == "section":
            sec_num = child.attrib.get("number", "")
            sec_heading = ""
            scatch = child.find("catchline")
            if scatch is not None:
                sec_heading = (scatch.text or "").strip()
            body = _elem_text(child).strip()
            body = re.sub(r"\s+", " ", body).strip()
            if len(body) < 20:
                continue
            out.append(Section(
                title_num=title_num,
                title_name=title_name,
                chapter_num=chap_num,
                chapter_name=chap_name,
                section_num=sec_num,
                section_heading=sec_heading,
                raw_text=body,
                source_url=f"{UT_BASE}/xcode/Title{title_num}/Chapter{chap_num.split('-', 1)[-1]}/{sec_num}.html",
            ))
        elif child.tag in ("part", "subpart", "subdivision", "article"):
            # Capture this nested container's number/name, then recurse
            sub_num = child.attrib.get("number", "")
            sub_name = ""
            sc = child.find("catchline")
            if sc is not None:
                sub_name = (sc.text or "").strip()
            # For deep nesting, keep top-most part name as part_name on sections
            new_part_num = part_num or sub_num
            new_part_name = part_name or sub_name
            out.extend(_harvest_sections(child, title_num, title_name,
                                         chap_num, chap_name,
                                         new_part_num, new_part_name))
    return out


def parse_title_xml(xml_bytes: bytes) -> tuple[str, str, list[Section]]:
    """Parse one Title's XML. Returns (title_num, title_name, [Section, ...])."""
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as e:
        print(f"  ! XML parse error: {e}", flush=True)
        return "", "", []
    if root.tag != "title":
        return "", "", []

    title_num = root.attrib.get("number", "")
    title_name = ""
    catch = root.find("catchline")
    if catch is not None:
        title_name = (catch.text or "").strip()

    out: list[Section] = []
    for chap in root.findall("chapter"):
        chap_num = chap.attrib.get("number", "")
        chap_name = ""
        ccatch = chap.find("catchline")
        if ccatch is not None:
            chap_name = (ccatch.text or "").strip()
        out.extend(_harvest_sections(chap, title_num, title_name, chap_num, chap_name))
    return title_num, title_name, out


# ---------------------------------------------------------------------------
# Chunk emission
# ---------------------------------------------------------------------------

def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def _point_id(act_id: str, chunk_idx: int, text: str) -> str:
    seed = f"{act_id}::{chunk_idx}::{_sha1(text)[:12]}"
    return str(uuid.UUID(hashlib.md5(seed.encode()).hexdigest()))


def to_chunk_record(sec: Section) -> dict:
    # act_id segments: STATE_UT_T<title>_C<chap>_S<sec>; sec_num like "3-1-1"
    # already includes title and chapter, so the section column is the full
    # canonical identifier.
    act_id = f"STATE_UT_T{sec.title_num}_S{sec.section_num.replace('-', '_')}"
    text = sec.raw_text
    citation = f"Utah Code § {sec.section_num}"
    title_label = f"Utah Code Title {sec.title_num}: {sec.title_name}"
    text_for_embedding = (
        f"Statute: Utah Code | US | Utah | In Force\n"
        f"{title_label}\n"
        f"Chapter {sec.chapter_num}: {sec.chapter_name}\n"
        f"§ {sec.section_num}. {sec.section_heading}\n\n{text}"
    )
    md = {
        "act_id": act_id,
        "corpus_type": "state",
        "category": "state_statute",
        "document_type": "statute",
        "jurisdiction": "US",
        "country_code": "US",
        "state": "ut",
        "title_number": int(sec.title_num) if sec.title_num.isdigit() else None,
        "title_name": title_label,
        "title": "Utah Code",
        "title_code": sec.title_num if not sec.title_num.isdigit() else None,
        "top_level_title": sec.title_num,
        "chapter": sec.chapter_num,
        "chapter_name": sec.chapter_name,
        "section_number": sec.section_num,
        "section_title": f"§ {sec.section_num}. {sec.section_heading}",
        "year": 2026,
        "act_status": "in_force",
        "renumbered_to": "",
        "transferred_to": "",
        "level_classifier": "section",
        "citation": citation,
        "citation_short": citation,
        "display_label": citation,
        "display_title": f"§ {sec.section_num}. {sec.section_heading}",
        "display_path": (
            f"Utah Code / Title {sec.title_num} / Chapter {sec.chapter_num} / "
            f"§ {sec.section_num}"
        ),
        "breadcrumb": [
            {"type": "title", "num": sec.title_num,
             "label": f"Title {sec.title_num}", "name": sec.title_name},
            {"type": "chapter", "num": sec.chapter_num,
             "label": f"Chapter {sec.chapter_num}", "name": sec.chapter_name},
            {"type": "section", "num": sec.section_num,
             "label": f"§ {sec.section_num}", "name": sec.section_heading},
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
        "amendments_count": 0,
        "last_amended_year": None,
        "public_laws_referenced": [],
        "public_laws_count": 0,
        "source_url": sec.source_url,
        "parent_id": f"us/ut/statutes/title={sec.title_num}/chapter={sec.chapter_num}",
        "raw_node_id": (
            f"us/ut/statutes/title={sec.title_num}/chapter={sec.chapter_num}/"
            f"section={sec.section_num}"
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
# Crawl: fetch each title's XML and parse
# ---------------------------------------------------------------------------

def fetch_title_xml(xml_url: str, retries: int = 5) -> Optional[bytes]:
    proxies = _us_proxies()
    for attempt in range(retries):
        try:
            r = SESSION.get(xml_url, timeout=60, proxies=proxies, allow_redirects=True)
            if r.status_code == 200:
                return r.content
            if r.status_code == 429:
                time.sleep(2 ** attempt)
                continue
            return None
        except Exception:
            time.sleep(2)
    return None


def process_title(title_num: str, xml_url: str) -> list[Section]:
    xml_bytes = fetch_title_xml(xml_url)
    if not xml_bytes:
        return []
    tnum, tname, sections = parse_title_xml(xml_bytes)
    return sections


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--titles", default="",
                    help="Comma-separated title numbers (e.g. '3,4'). Default: all.")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--rediscover", action="store_true",
                    help="Force re-fetch of the TOC (otherwise uses cached versions).")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    print("[UT] discovering title XML URLs...", flush=True)
    all_urls = get_title_xml_urls(force_rediscover=args.rediscover)
    print(f"[UT] {len(all_urls)} titles discovered", flush=True)

    if args.titles:
        wanted = {t.strip() for t in args.titles.split(",") if t.strip()}
        all_urls = {t: u for t, u in all_urls.items() if t in wanted}
        print(f"[UT] filtered to {len(all_urls)}: {sorted(all_urls.keys())}", flush=True)

    if args.dry_run:
        for t, u in sorted(all_urls.items()):
            print(f"  T{t}: {u}")
        return 0

    all_sections: list[Section] = []
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = {
            ex.submit(process_title, t, u): t
            for t, u in all_urls.items()
        }
        done = 0
        for fut in as_completed(futures):
            done += 1
            t = futures[fut]
            try:
                secs = fut.result()
                all_sections.extend(secs)
                print(f"  [T{t}] {len(secs)} sections", flush=True)
            except Exception as e:
                print(f"  [T{t}] FAIL: {e}", flush=True)

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
        for sec in all_sections:
            rec = to_chunk_record(sec)
            if rec["point_id"] in seen:
                continue
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
            seen.add(rec["point_id"])
            written += 1

    elapsed = time.time() - t0
    print(
        f"\n=== Done: parsed={len(all_sections):,}, new={written:,}, "
        f"elapsed={elapsed:.1f}s ===", flush=True
    )
    print(f"JSONL: {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
