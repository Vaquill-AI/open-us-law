"""Ingest 22 NYCRR (NY court rules) via Playwright + Webshare residential proxy.

The new nycourts.gov site is protected by Cloudflare + Akamai. Direct fetches
return 403. The proven workaround:

  1. Launch real Chromium through the Webshare residential proxy.
  2. Visit `/rules/` first to acquire the CF clearance cookie ("__cf_bm").
  3. Within the same browser context, navigate to each of the 5 category
     indexes (chief-judge, chief-admin, trial-courts, court-of-appeals,
     appellate-division), extract Part URLs.
  4. Visit each Part page (e.g. /rules/part-100-judicial-conduct); the full
     text of every Section in that Part renders inline. Split on the
     "Section X.Y Title" header pattern to emit one chunk per section.

Output: state_court_rules_chunks.jsonl (merged with the existing CA/IL/PA
rules JSONL via the same `to_chunk_record` shape).
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from uuid import UUID

from playwright.async_api import async_playwright

try:
    from playwright_stealth import Stealth  # type: ignore
    _STEALTH_AVAILABLE = True
except ImportError:
    _STEALTH_AVAILABLE = False

sys.stdout.reconfigure(line_buffering=True)  # type: ignore[attr-defined]

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = Path(os.environ.get("OUT_DIR", "./data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
OUT = DATA_DIR / "state_court_rules_chunks.jsonl"


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

import random
import string


def _proxy_config(variant: str = "us_rotate") -> dict:
    """Build Webshare proxy config.

    variants:
      "us_rotate"   : `{user}-US-rotate`  — US-only, rotating IP per req
      "us_slot_<N>" : `{user}-US-<N>`     — US-only, FIXED IP slot N (sticky)
      "bare"        : `{user}` — no suffix; default Webshare routing

    Note: `{user}-session-<id>` and `{user}-country-us-...` formats are
    rejected by this Webshare account (400 Bad Request). Only `-US-rotate`
    and `-US-<N>` are honored.
    """
    base_user = os.environ.get("WEBSHARE_USERNAME", "")
    if variant == "us_rotate":
        user = f"{base_user}-US-rotate"
    elif variant.startswith("us_slot_"):
        n = variant.removeprefix("us_slot_")
        user = f"{base_user}-US-{n}"
    else:
        user = base_user
    return {
        "server": f"http://{os.environ.get('WEBSHARE_PROXY_HOST', 'p.webshare.io')}:{os.environ.get('WEBSHARE_PROXY_PORT', '80')}",
        "username": user,
        "password": os.environ.get("WEBSHARE_PASSWORD", ""),
    }


PROXY = _proxy_config("us_rotate")  # diagnostics only

CATEGORY_INDEXES = [
    ("chief-judge", "Rules of the Chief Judge",
     "https://www.nycourts.gov/rules/chief-judge-1-81"),
    ("chief-admin", "Rules of the Chief Administrator",
     "https://www.nycourts.gov/rules/chief-admin-100-161"),
    ("trial-courts", "Uniform Rules for the Trial Courts",
     "https://www.nycourts.gov/rules/trial-courts-200-221"),
    ("court-of-appeals", "Rules of the Court of Appeals",
     "https://www.nycourts.gov/rules/court-of-appeals-500-540"),
    ("appellate-division", "Rules of the Appellate Division",
     "https://www.nycourts.gov/rules/appellate-division-600-1500"),
]


def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def _point_id(act_id: str, chunk_idx: int, text: str) -> str:
    seed = f"{act_id}::{chunk_idx}::{_sha1(text)[:12]}"
    return str(UUID(hashlib.md5(seed.encode()).hexdigest()))


@dataclass
class NySection:
    category_slug: str       # e.g. "chief-admin"
    category_name: str       # e.g. "Rules of the Chief Administrator"
    part_num: str            # e.g. "100"
    part_name: str           # e.g. "Judicial Conduct"
    part_url: str
    section_id: str          # e.g. "100.0", "202.3", "100.3(A)" — leaf identifier
    section_title: str       # "Section 100.0 Terminology"
    raw_text: str


# Match "Section X.Y" or "§ X.Y" headers anywhere (not just line-start).
# innerText may collapse formatting, so we allow flexible whitespace.
_SECTION_RE = re.compile(
    r"\b(?:Section|§|§)\s+(\d+\.\d+[A-Za-z0-9\-]*)\b[ \t ]*([^\n]{0,250})",
    re.MULTILINE,
)


def _strip_chrome(body: str) -> str:
    """Remove site nav header and footer from the rendered page body."""
    # Find the "Breadcrumb" marker; content of interest starts after it
    bc = body.find("Breadcrumb")
    if bc != -1:
        # Skip the breadcrumb line itself
        nl = body.find("\n", bc)
        if nl != -1:
            body = body[nl + 1:]
    # Trim footer (everything from "Footer languages" onward)
    foot = body.find("Footer languages")
    if foot != -1:
        body = body[:foot]
    return body.strip()


def _parse_part_body(body: str, part_num: str, part_name: str, part_url: str,
                     category_slug: str, category_name: str) -> list[NySection]:
    """Split a Part page's body into sections."""
    body = _strip_chrome(body)
    # Find all "Section X.Y Title" markers and slice the text between them.
    matches = list(_SECTION_RE.finditer(body))
    if not matches:
        # Some Parts have no formal Sections (e.g. a single block).
        # Emit one chunk for the whole Part.
        body_clean = re.sub(r"\s+", " ", body).strip()
        if len(body_clean) < 100:
            return []
        return [NySection(
            category_slug=category_slug, category_name=category_name,
            part_num=part_num, part_name=part_name, part_url=part_url,
            section_id=part_num, section_title=f"Part {part_num}. {part_name}",
            raw_text=body_clean,
        )]
    sections: list[NySection] = []
    for i, m in enumerate(matches):
        sec_id = m.group(1).strip()
        sec_title_inline = m.group(2).strip().rstrip(".")
        start = m.start()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(body)
        chunk = body[start:end]
        # Strip the heading we already captured from the chunk's body
        chunk = re.sub(_SECTION_RE, "", chunk, count=1).strip()
        chunk = re.sub(r"\s+", " ", chunk).strip()
        if len(chunk) < 30:
            continue
        sections.append(NySection(
            category_slug=category_slug, category_name=category_name,
            part_num=part_num, part_name=part_name, part_url=part_url,
            section_id=sec_id,
            section_title=f"§ {sec_id} {sec_title_inline}",
            raw_text=chunk,
        ))
    return sections


def _to_chunk_record(sec: NySection) -> dict:
    safe_sid = sec.section_id.replace(".", "_").replace("(", "_").replace(")", "")
    act_id = f"SRULES_NY_22NYCRR_P{sec.part_num}_S{safe_sid}"
    title_label = "22 NYCRR (NY Court Rules)"
    citation = f"22 NYCRR § {sec.section_id}"
    text_for_embedding = (
        f"{title_label} | {sec.category_name} | {citation}\n"
        f"Part {sec.part_num}. {sec.part_name}\n"
        f"{sec.section_title}\n\n{sec.raw_text}"
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
        "state": "ny",
        "title_name": "22 NYCRR",
        "title": title_label,
        "title_code": "22nycrr",
        "top_level_title": "22nycrr",
        "level_classifier": "section",
        "chapter": sec.part_num,
        "chapter_name": f"Part {sec.part_num}. {sec.part_name}",
        "subchapter": sec.category_slug,
        "subchapter_name": sec.category_name,
        "section_number": sec.section_id,
        "section_title": sec.section_title,
        "citation": citation,
        "citation_short": citation,
        "display_label": citation,
        "display_title": sec.section_title,
        "display_path": (
            f"22 NYCRR / {sec.category_name} / Part {sec.part_num} / "
            f"§ {sec.section_id}"
        ),
        "breadcrumb": [
            "22 NYCRR",
            sec.category_name,
            f"Part {sec.part_num}. {sec.part_name}",
            f"§ {sec.section_id}",
        ],
        "sort_key": act_id,
        "act_status": "in_force",
        "renumbered_to": "",
        "transferred_to": "",
        "year": None,
        "word_count": len(sec.raw_text.split()),
        "subsection_count": 0,
        "subsection_letters": [],
        "numbered_paragraph_count": 0,
        "amendments_count": 0,
        "amendment_years": [],
        "last_amended_year": None,
        "cross_references_count": 0,
        "cross_references_usc": [],
        "cross_references_cfr": [],
        "public_laws_count": 0,
        "public_laws_referenced": [],
        "source_url": sec.part_url,
        "parent_id": None,
        "raw_node_id": act_id,
        "full_text_sha1": _sha1(sec.raw_text),
    }
    return {
        "point_id": _point_id(act_id, 0, sec.raw_text),
        "text_for_embedding": text_for_embedding,
        "raw_text": sec.raw_text,
        "metadata": md,
    }


_CF_MARKERS = (
    "performing security verification",
    "checking your browser",
    "just a moment",
    "verifying you are human",
    "ray id:",
    "performance and security by cloudflare",
    "why is this verification taking longer",
    "wait briefly. refreshing the page",
    "if the verification still does not complete",
    "if you are still stuck on this page",
)


async def _wait_past_cf(page, max_ms: int = 30000) -> bool:
    """Poll until the CF interstitial is gone AND body has real content.

    Returns True only when body has > 600 chars AND none of the CF markers
    are visible. Returns False if max_ms elapses without satisfying both.
    """
    elapsed = 0
    step = 1500
    while elapsed < max_ms:
        body = await page.evaluate("document.body ? document.body.innerText : ''")
        low = body.lower()
        has_cf = any(m in low for m in _CF_MARKERS)
        if has_cf or len(body) < 600:
            await page.wait_for_timeout(step)
            elapsed += step
            continue
        return True
    return False


async def _new_warmed_context(p, max_attempts: int = 8):
    """Launch Chromium through a Webshare *sticky* session and warm the CF cookie.

    Sticky session keeps the same residential IP across all subsequent requests,
    so the CF clearance cookie acquired on /rules/ remains valid for the deep
    Part pages. If a given IP can't clear the challenge in 45s, we close the
    browser and try again with a new sticky session ID.
    """
    if not os.environ.get("WEBSHARE_USERNAME"):
        raise RuntimeError("WEBSHARE_USERNAME / WEBSHARE_PASSWORD must be set in .env")
    last_err = None
    for attempt in range(1, max_attempts + 1):
        # Use a FIXED-slot Webshare proxy (one IP for entire session).
        # This is critical: CF cookies are IP-bound, so rotating IPs voids
        # the cf_clearance acquired during warm-up.
        # We cycle slot numbers across attempts so a bad slot can be skipped.
        slot = attempt  # try US-1, US-2, US-3, ...
        proxy = _proxy_config(f"us_slot_{slot}")
        browser = await p.chromium.launch(
            headless=True,
            proxy=proxy,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-features=IsolateOrigins,site-per-process",
                "--no-sandbox",
            ],
        )
        try:
            ctx = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1280, "height": 900},
                locale="en-US",
                timezone_id="America/New_York",
                java_script_enabled=True,
            )
            # Apply playwright-stealth — patches navigator.webdriver, plugins,
            # languages, chrome.runtime, permissions, WebGL fingerprint, etc.
            if _STEALTH_AVAILABLE:
                await Stealth().apply_stealth_async(ctx)
            else:
                await ctx.add_init_script(
                    "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
                )
            page = await ctx.new_page()
            print(f"  warm-up attempt {attempt}: visit /rules/", flush=True)
            try:
                await page.goto("https://www.nycourts.gov/rules/",
                                wait_until="domcontentloaded", timeout=60000)
            except Exception as e:
                last_err = e
                print(f"    goto failed: {type(e).__name__}: {str(e)[:120]}", flush=True)
                await browser.close()
                continue
            ok = await _wait_past_cf(page, max_ms=90000)
            if ok:
                # Verify we actually got the rules page, not a still-challenged state
                body = await page.evaluate("document.body ? document.body.innerText : ''")
                if "Chief Judge" in body and "Trial Courts" in body:
                    print(f"    ✓ warm-up succeeded on attempt {attempt} (body={len(body)} chars)", flush=True)
                    return browser, ctx, page
                else:
                    print(f"    body cleared CF but lacks expected content ({len(body)} chars), retrying", flush=True)
            else:
                print(f"    CF interstitial persisted >45s, rotating proxy IP", flush=True)
        except Exception as e:
            last_err = e
        await browser.close()
    raise RuntimeError(f"warm-up failed after {max_attempts} attempts; last err: {last_err!r}")


async def _collect_category_parts(page, cat_slug: str, cat_url: str) -> list[tuple[str, str, str]]:
    """Navigate to a category index page and return [(part_num, part_name, part_url), ...]."""
    print(f"  category: {cat_slug} -> {cat_url}", flush=True)
    await page.goto(cat_url, wait_until="domcontentloaded", timeout=60000)
    await _wait_past_cf(page, max_ms=30000)
    anchors = await page.evaluate(
        "Array.from(document.querySelectorAll('a[href]')).map(a => ({href: a.href, text: a.innerText.trim()}))"
    )
    out: list[tuple[str, str, str]] = []
    seen: set[str] = set()
    for a in anchors:
        h = a["href"]
        if "/rules/part-" not in h:
            continue
        if h in seen:
            continue
        seen.add(h)
        # Extract part number from URL: /rules/part-100-judicial-conduct → "100"
        m = re.search(r"/rules/part-(\d+[A-Za-z]?)-", h)
        if not m:
            continue
        part_num = m.group(1)
        text = a["text"]
        # text is like "100 - Judicial Conduct" — split on " - " for name
        if " - " in text:
            _, _, name = text.partition(" - ")
            name = name.strip()
        else:
            name = text.strip()
        if not name or "repealed" in name.lower():
            continue
        out.append((part_num, name, h))
    return out


async def _scrape_part(page, cat_slug: str, cat_name: str,
                       part_num: str, part_name: str, part_url: str,
                       debug_dump_dir: Optional[Path] = None) -> list[NySection]:
    """Fetch a Part page and parse sections.

    Verifies the fetched body actually corresponds to the Part requested:
    looks for "Part {part_num}." marker. If not found, retry up to 3x.
    """
    body = ""
    part_marker_a = f"Part {part_num}."
    part_marker_b = f"Part {part_num} "
    for attempt in range(3):
        try:
            await page.goto(part_url, wait_until="domcontentloaded", timeout=60000)
        except Exception as e:
            print(f"      goto attempt {attempt+1} failed: {type(e).__name__}", flush=True)
            await asyncio.sleep(2)
            continue
        await _wait_past_cf(page, max_ms=90000)  # CF can take 60+s on first hit
        try:
            await page.wait_for_load_state("networkidle", timeout=8000)
        except Exception:
            pass
        body = await page.evaluate("document.body ? document.body.innerText : ''")
        if len(body) >= 500 and (part_marker_a in body or part_marker_b in body):
            # Page is real and matches the Part requested
            break
        # Wrong content (CF intermediate, blank, or wrong page) — retry
        print(
            f"      retry {attempt+1}: body={len(body)} chars, "
            f"part-marker={(part_marker_a in body or part_marker_b in body)}",
            flush=True,
        )
        # Pause between retries to avoid rate-limiting cascades
        await asyncio.sleep(5)
    if len(body) < 500 or (part_marker_a not in body and part_marker_b not in body):
        # Save the bad body for offline debugging
        if debug_dump_dir is not None:
            try:
                debug_dump_dir.mkdir(parents=True, exist_ok=True)
                (debug_dump_dir / f"part_{part_num}_bad.txt").write_text(body)
            except Exception:
                pass
        return []
    sections = _parse_part_body(
        body, part_num=part_num, part_name=part_name, part_url=part_url,
        category_slug=cat_slug, category_name=cat_name,
    )
    if not sections:
        return []
    return sections


async def scrape_ny_async() -> list[NySection]:
    """Single-browser scrape using a FIXED-IP Webshare slot.

    cf_clearance is IP-bound, so a rotating proxy invalidates the cookie
    on every request. By using `{user}-US-{N}` we get one fixed IP for the
    whole session, and the cookie acquired during warm-up keeps working.
    If the chosen slot's IP can't pass CF, _new_warmed_context cycles to
    the next slot.
    """
    debug_dir = Path("/tmp/ny_bad_parts")
    all_sections: list[NySection] = []
    async with async_playwright() as p:
        browser, ctx, page = await _new_warmed_context(p)
        try:
            for cat_slug, cat_name, cat_url in CATEGORY_INDEXES:
                parts = await _collect_category_parts(page, cat_slug, cat_url)
                print(f"    -> {len(parts)} Parts in {cat_slug}", flush=True)
                for i, (part_num, part_name, part_url) in enumerate(parts, 1):
                    t0 = time.time()
                    try:
                        sections = await _scrape_part(
                            page, cat_slug, cat_name, part_num, part_name, part_url,
                            debug_dump_dir=debug_dir,
                        )
                        all_sections.extend(sections)
                        dt = time.time() - t0
                        print(
                            f"    [{cat_slug} {i}/{len(parts)}] Part {part_num} "
                            f"({part_name[:40]}): {len(sections)} sections ({dt:.1f}s)",
                            flush=True,
                        )
                    except Exception as e:
                        print(f"    ! Part {part_num} failed: {e}", flush=True)
                    # Small inter-Part delay to ease CF rate limiting
                    await asyncio.sleep(1.5)
        finally:
            await browser.close()
    return all_sections


def _merge_jsonl(path: Path, new_sections: list[NySection]) -> int:
    existing: dict[str, dict] = {}
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                aid = rec.get("metadata", {}).get("act_id")
                if aid:
                    existing[aid] = rec
            except json.JSONDecodeError:
                continue
    for s in new_sections:
        rec = _to_chunk_record(s)
        existing[rec["metadata"]["act_id"]] = rec
    with open(path, "w", encoding="utf-8") as fh:
        for rec in existing.values():
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
    return len(existing)


def main() -> int:
    print(f"=== NY 22 NYCRR scraper (Playwright + Webshare) ===")
    t0 = time.time()
    sections = asyncio.run(scrape_ny_async())
    print(f"\n[NY] done: {len(sections)} sections in {time.time() - t0:.1f}s")
    n = _merge_jsonl(OUT, sections)
    print(f"=> JSONL now has {n} state-court-rule chunks at {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
