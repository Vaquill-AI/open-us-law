"""Enumerate every Tennessee Code section URL by walking Justia's TOC via ScrapFly.

ScrapFly (`ar_bulk.client.scrapfly_html`, `asp=true`, datacenter proxy, 1 credit/
page) returns Justia's REAL HTML past Cloudflare, so the table-of-contents walk is
reliable: each index page's `<a href>` anchors are parsed directly (no Exa link-
extraction flakiness). Justia has no per-code sitemap (verified: `/sitemap-codes.xml`
and `/codes/sitemap.xml` 404), so a BFS down title -> chapter -> [part] -> section
is the enumeration path.

The walk is edition-current only (year-prefixed `/codes/tennessee/2021/...` links are
dropped) and title-scoped (a section belongs to a title iff its slug starts with the
title number), so cross-references and old editions are never followed.

ScrapFly free-tier concurrency is 5, so both this walk and the body fetch cap total
in-flight ScrapFly calls at ``workers`` (default 5). ``cache=true`` (7-day TTL) makes
a re-walk/resume cost 0 credits.
"""

from __future__ import annotations

import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from . import scrapfly
from .walk import TNSection, section_from_url

_JUSTIA = "https://law.justia.com"
_YEAR_SEG_RE = re.compile(r"/codes/tennessee/(?:19|20)\d{2}(?:/|$)")


def title_index_url(title: str) -> str:
    return f"{_JUSTIA}/codes/tennessee/title-{title}/"


def _norm(u: str) -> str:
    u = u.split("#", 1)[0].split("?", 1)[0]
    if u.startswith("/"):
        u = _JUSTIA + u
    if not u.endswith("/"):
        u += "/"
    return u


def _tn_links(html: str) -> list[str]:
    """Absolute current-edition law.justia.com/codes/tennessee hrefs on a page."""
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")
    out: list[str] = []
    for a in soup.find_all("a", href=True):
        href = _norm(a["href"])
        if "/codes/tennessee/" in href and not _YEAR_SEG_RE.search(href):
            out.append(href)
    return out


def _child_links(current_url: str, links: list[str]) -> list[str]:
    """Links exactly one path segment below ``current_url`` (its direct children)."""
    cur = _norm(current_url)
    seen: set[str] = set()
    out: list[str] = []
    for link in links:
        ln = _norm(link)
        if not ln.startswith(cur) or ln == cur:
            continue
        rest = ln[len(cur):].strip("/")
        if not rest or "/" in rest:
            continue
        if ln not in seen:
            seen.add(ln)
            out.append(ln)
    return out


def _fetch_index(url: str) -> tuple[str, list[str]]:
    html = scrapfly.fetch_html(url, cost_budget=2)
    if not html:
        return url, []
    return url, _tn_links(html)


def discover_title(title: str, workers: int = 5) -> tuple[dict[str, TNSection], dict]:
    """BFS one title's TOC via ScrapFly; return ``{section_url: TNSection}``."""
    sections: dict[str, TNSection] = {}
    visited: set[str] = set()
    frontier = [title_index_url(title)]
    stats = {"index_fetches": 0, "empty_pages": 0, "levels": 0}

    while frontier:
        stats["levels"] += 1
        level = [u for u in frontier if u not in visited]
        visited.update(level)
        next_frontier: set[str] = set()
        with ThreadPoolExecutor(max_workers=workers) as ex:
            for fut in as_completed(ex.submit(_fetch_index, u) for u in level):
                page_url, links = fut.result()
                stats["index_fetches"] += 1
                if not links:
                    stats["empty_pages"] += 1
                for child in _child_links(page_url, links):
                    sec = section_from_url(child)
                    if sec is not None and sec.title == title:
                        sections.setdefault(sec.url, sec)
                    elif "/section-" not in child and child not in visited:
                        next_frontier.add(child)
        frontier = list(next_frontier)
        if stats["index_fetches"] > 8000:  # safety valve; a title never nears this
            break
    return sections, stats


def discover(
    titles: list[str], workers: int = 5, on_title=None
) -> tuple[dict[str, TNSection], dict]:
    """Walk every title's TOC (sequential across titles, parallel within a title).

    ``on_title(title, sections, stats)`` fires as soon as each title's walk
    finishes, so the caller can CHECKPOINT that title's sections to disk. The walk
    is the expensive non-cacheable half (~4.5k ScrapFly credits for all 71 titles),
    so it must never be an all-or-nothing batch: a crash at title 60 should cost
    one title, not the whole walk.
    """
    all_secs: dict[str, TNSection] = {}
    per_title: dict[str, dict] = {}
    for t in titles:
        t0 = time.time()
        secs, stats = discover_title(t, workers=workers)
        stats["seconds"] = round(time.time() - t0)
        per_title[t] = {"sections": len(secs), **stats}
        for url, sec in secs.items():
            all_secs.setdefault(url, sec)
        if on_title is not None:
            on_title(t, list(secs.values()), per_title[t])
    return all_secs, {"per_title": per_title, "total": len(all_secs)}
