#!/usr/bin/env python3
"""Shared parsing helpers for the Puerto Rico statute corpus.

This module no longer fetches anything. It holds the `Article` dataclass, the
PDF text extractor and the chunk-record builder, which `ingest_pr_bulk.py`
imports to build Puerto Rico from the official OGP portal
(bvirtualogp.pr.gov). Run that script, not this one.

The prior fetch registry in this file pulled per-title PDFs from a commercial
host and has been removed; the official portal is the supported source.
"""

from __future__ import annotations

import hashlib
import io
import os
import re
import sys
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import requests

sys.stdout.reconfigure(line_buffering=True)  # type: ignore[attr-defined]

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = Path(os.environ.get("OUT_DIR", "./data"))
OUT = DATA_DIR / "state_pr_statutes.jsonl"

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


def fetch_bytes(url: str, retries: int = 5) -> Optional[bytes]:
    proxies = _us_proxies()
    for attempt in range(retries):
        try:
            r = SESSION.get(url, timeout=90, proxies=proxies, allow_redirects=True)
            if r.status_code == 200:
                return r.content
            if r.status_code == 429:
                time.sleep(2 ** attempt)
                continue
            return None
        except Exception:
            time.sleep(2)
    return None


# ---------------------------------------------------------------------------
# Code catalog
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# PDF parse
# ---------------------------------------------------------------------------

# Article header: "Artículo 12.-Heading." or "Artículo 12.34.-Heading"
_ARTICLE_RE = re.compile(
    r"Art[íi]culo\s+(\d+(?:\.\d+)?[A-Za-z]?)\s*\.?\s*[-–—]\s*([^\n]*)",
)
# Section header (tax/incentive codes): "Sección 1010.01.-Heading"
_SECCION_RE = re.compile(
    r"Secci[óo]n\s+(\d+(?:\.\d+){0,2}[A-Za-z]?)\s*\.?\s*[-–—]\s*([^\n]*)",
)


def _marker_re(meta: dict):
    return _SECCION_RE if meta.get("marker") == "seccion" else _ARTICLE_RE


def _pdf_text(pdf_bytes: bytes) -> str:
    import pdfplumber
    parts: list[str] = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            t = page.extract_text() or ""
            if t.strip():
                parts.append(t)
    return "\n".join(parts)


@dataclass
class Article:
    code_slug: str
    code_name: str
    code_name_en: str
    citation_prefix: str
    year: int
    part_label: str
    article_num: str
    article_heading: str
    raw_text: str
    source_url: str
    unit: str = "Artículo"  # "Artículo" or "Sección"


def parse_code_part(text: str, code_slug: str, meta: dict, part_label: str,
                    source_url: str) -> list[Article]:
    matches = list(_marker_re(meta).finditer(text))
    out: list[Article] = []
    for i, m in enumerate(matches):
        art_num = m.group(1).strip()
        heading = re.sub(r"\s+", " ", m.group(2)).strip().rstrip(".")
        start = m.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        body = text[start:end]
        # Strip page-footer artifacts (page numbers, "Código Civil ..." running heads)
        body = re.sub(r"\n\s*\d+\s*\n", "\n", body)
        body = re.sub(r"\s+", " ", body).strip()
        if len(body) < 20:
            continue
        out.append(Article(
            code_slug=code_slug,
            code_name=meta["name"],
            code_name_en=meta["name_en"],
            citation_prefix=meta["citation"],
            year=meta["year"],
            part_label=part_label,
            article_num=art_num,
            article_heading=heading,
            raw_text=body,
            source_url=source_url,
            unit="Sección" if meta.get("marker") == "seccion" else "Artículo",
        ))
    return out


# ---------------------------------------------------------------------------
# Chunk emission
# ---------------------------------------------------------------------------

def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def _point_id(act_id: str, idx: int, text: str) -> str:
    seed = f"{act_id}::{idx}::{_sha1(text)[:12]}"
    return str(uuid.UUID(hashlib.md5(seed.encode()).hexdigest()))


def to_chunk_record(a: Article) -> dict:
    unit_abbr = "sec." if a.unit == "Sección" else "art."
    id_prefix = "SEC" if a.unit == "Sección" else "ART"
    act_id = f"STATE_PR_{a.code_slug.upper()}_{id_prefix}{a.article_num.replace('.', '_')}"
    citation = f"{a.citation_prefix} {unit_abbr} {a.article_num}"
    text = a.raw_text
    text_for_embedding = (
        f"Estatuto: {a.code_name} | Puerto Rico (US) | Vigente\n"
        f"{a.part_label}\n"
        f"{a.unit} {a.article_num}. {a.article_heading}\n\n{text}"
    )
    md = {
        "act_id": act_id,
        "corpus_type": "state",
        "category": "state_statute",
        "document_type": "statute",
        "jurisdiction": "US",
        "country_code": "US",
        "state": "pr",
        "language_code": "es",
        "title_number": None,
        "title_name": a.code_name,
        "title": a.code_name_en,
        "title_code": a.code_slug,
        "top_level_title": a.code_slug,
        "chapter": a.part_label,
        "chapter_name": a.part_label,
        "section_number": a.article_num,
        "section_title": f"{a.unit} {a.article_num}. {a.article_heading}",
        "year": a.year,
        "act_status": "in_force",
        "renumbered_to": "",
        "transferred_to": "",
        "level_classifier": "article",
        "citation": citation,
        "citation_short": citation,
        "display_label": citation,
        "display_title": f"{unit_abbr.capitalize()} {a.article_num}. {a.article_heading}",
        "display_path": f"{a.code_name} / {a.part_label} / {unit_abbr.capitalize()} {a.article_num}",
        "breadcrumb": [
            {"type": "code", "num": a.code_slug, "label": a.code_name, "name": a.code_name},
            {"type": "part", "num": "", "label": a.part_label, "name": a.part_label},
            {"type": "article", "num": a.article_num,
             "label": f"{unit_abbr.capitalize()} {a.article_num}", "name": a.article_heading},
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
        "source_url": a.source_url,
        "parent_id": f"us/pr/statutes/code={a.code_slug}",
        "raw_node_id": f"us/pr/statutes/code={a.code_slug}/article={a.article_num}",
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
# Main
# ---------------------------------------------------------------------------
