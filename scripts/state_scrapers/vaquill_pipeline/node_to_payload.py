"""Convert open-source-legislation Node objects into chunk records compatible
with the canonical statutes_us ingestion pipeline.

Output schema matches ``scripts/us_corpus/chunk_and_ingest.py::Chunk`` so the
existing ``scripts/us_corpus/embed_and_upsert.py`` consumes our JSONL with
zero changes:

    {
        "point_id":            <deterministic UUID>,
        "text_for_embedding":  <metadata-prefixed text>,
        "raw_text":            <chunk text only>,
        "metadata":            <payload dict for Qdrant>
    }

Payload mirrors the existing eCFR + USC chunks in ``statutes_us`` but uses
``category="state_statute"``, ``document_type="statute"``, and a 2-letter
``state`` code (e.g. ``"de"``). The shared retriever already filters on
``country_code="US"`` and ``jurisdiction="US"`` so state chunks coexist with
the federal ones.
"""
from __future__ import annotations

import hashlib
import re
import uuid
from typing import Any, Dict, List, Optional

from .config import SETTINGS

_APPROX_CHARS_PER_TOKEN = SETTINGS.approx_chars_per_token


# ---------------------------------------------------------------------------
# Chunker — same logic as scripts/us_corpus/chunk_and_ingest.py::chunk_text
# ---------------------------------------------------------------------------


def estimate_tokens(text: str) -> int:
    return len(text) // _APPROX_CHARS_PER_TOKEN


def chunk_text(
    text: str,
    max_tokens: int = SETTINGS.chunk_size_tokens,
    overlap_tokens: int = SETTINGS.chunk_overlap_tokens,
) -> List[str]:
    """Split text into chunks at natural boundaries, with NO data loss.

    Key invariants:
      - Every character of ``text`` ends up in at least one chunk.
      - Trailing tail smaller than ``min_chunk_size_tokens`` is APPENDED to
        the previous chunk (not dropped) so re-scrapes don't silently lose
        a section's final paragraph.
      - Overlap between consecutive chunks preserves context across the
        boundary so retrieval doesn't lose a sentence split mid-thought.
    """
    if estimate_tokens(text) <= max_tokens:
        return [text]

    max_chars = max_tokens * _APPROX_CHARS_PER_TOKEN
    overlap_chars = overlap_tokens * _APPROX_CHARS_PER_TOKEN

    chunks: List[str] = []
    start = 0
    text_len = len(text)

    while start < text_len:
        end = min(start + max_chars, text_len)

        if end < text_len:
            best_break = -1
            for pattern in [
                r"\n\s*\([a-z]\)",
                r"\n\s*\(\d+\)",
                r"\n\s*\([ivx]+\)",
                r"\n\n",
                r"\.\s+",
            ]:
                search_start = start + int(max_chars * 0.7)
                match = None
                for m in re.finditer(pattern, text[search_start:end]):
                    match = m
                if match:
                    best_break = search_start + match.start()
                    break
            if best_break > start:
                end = best_break

        chunk = text[start:end].strip()
        if chunk:
            # Tail-fold: if this is the LAST fragment and it's smaller than
            # min_chunk_size_tokens, merge it into the previous chunk instead
            # of dropping it.
            is_last = end >= text_len
            too_small = estimate_tokens(chunk) < SETTINGS.min_chunk_size_tokens
            if is_last and too_small and chunks:
                chunks[-1] = (chunks[-1] + "\n\n" + chunk).strip()
            else:
                chunks.append(chunk)

        start = end - overlap_chars if end < text_len else text_len

    return chunks if chunks else [text]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


# Full 50 states + DC + PR. A missing code falls back to code.upper() as the
# state_name in every payload (see _STATE_NAMES.get below), which silently ships
# "IA"/"MO"/"NJ" instead of the real name; keep this complete so each new bulk
# cutover does not have to remember to add its state.
_STATE_NAMES = {
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
    "dc": "District of Columbia",
    "pr": "Puerto Rico",
}


def _node_text_to_str(node) -> str:
    """Robustly pull text out of a Node regardless of NodeText shape."""
    nt = getattr(node, "node_text", None)
    if nt is None:
        return ""
    # Preferred: NodeText.to_list_text() joins paragraphs in index order.
    try:
        parts = nt.to_list_text()
        if parts:
            return "\n\n".join(p for p in parts if p)
    except AttributeError:
        pass
    # Fallback: dict-of-Paragraph
    try:
        paras = list(nt.paragraphs.values())
        paras.sort(key=lambda p: getattr(p, "index", 0))
        return "\n\n".join(p.text for p in paras if getattr(p, "text", None))
    except Exception:
        return ""


def _component_pairs(node) -> List[tuple[str, str]]:
    try:
        classifiers = list(node.id.component_classifiers)
        numbers = list(node.id.component_numbers)
    except AttributeError:
        return []
    # Skip the first three: country / jurisdiction / corpus.
    return list(zip(classifiers[3:], numbers[3:]))


def _act_id(node, state_code: str) -> str:
    """Stable, human-readable act_id. Example: STATE_DE_T6_C5_S101."""
    parts = [f"STATE_{state_code.upper()}"]
    pairs = _component_pairs(node)
    if not pairs:
        parts.append(hashlib.sha1((node.node_id or "").encode()).hexdigest()[:8])
        return "_".join(parts)
    for cls_, num in pairs:
        parts.append(f"{cls_[0].upper()}{num}")
    return "_".join(parts)


def _section_number(node) -> Optional[str]:
    try:
        cls_, num = node.id.current_level or (None, None)
        if cls_ == "section":
            return num
    except Exception:
        pass
    return getattr(node, "number", None) or None


def _title_number(node) -> Optional[int]:
    for cls_, num in _component_pairs(node):
        if cls_ == "title":
            try:
                return int(re.sub(r"[^0-9]", "", num))
            except ValueError:
                return None
    return None


def _build_breadcrumb(node) -> List[Dict[str, Any]]:
    crumbs: List[Dict[str, Any]] = []
    for cls_, num in _component_pairs(node):
        crumbs.append({"type": cls_, "num": num, "label": f"{cls_.title()} {num}", "name": ""})
    return crumbs


def _build_display_path(state_code: str, breadcrumb: List[Dict[str, Any]]) -> str:
    state_name = _STATE_NAMES.get(state_code.lower(), state_code.upper())
    parts = [f"{state_name} Code"]
    parts.extend(c["label"] for c in breadcrumb)
    return " / ".join(parts)


def _make_point_id(act_id: str, chunk_index: int, chunk_text: str = "") -> str:
    """Content-addressed deterministic UUID.

    Includes a short hash of the chunk text so:
      - identical re-scrape produces identical UUIDs (idempotent upsert)
      - changed text produces a NEW UUID (old point survives in Qdrant
        until an explicit cleanup pass deletes orphans — no silent data loss)

    Compared to the simpler ``uuid(md5(act_id::chunk_index))`` formula used
    by eCFR / USC: that one overwrites in place on re-scrape, so a section
    that changed from 3 chunks to 2 would orphan chunk #2 AND overwrite
    chunks #0 and #1 with potentially different content. Both are silent
    data-loss vectors. The content-addressed formula here makes re-scrapes
    additive and recoverable.
    """
    text_hash = hashlib.sha1(chunk_text.encode("utf-8")).hexdigest()[:12] if chunk_text else "nochunktext"
    raw = f"{act_id}::{chunk_index}::{text_hash}"
    return str(uuid.UUID(hashlib.md5(raw.encode()).hexdigest()))


def _build_state_prefix(state_code: str, title_number, chapter, section_number, section_title, year) -> str:
    state_name = _STATE_NAMES.get(state_code.lower(), state_code.upper())
    line1 = f"Statute: {state_name} Code ({year}) | US | {state_name} | In Force"
    line2_parts: List[str] = []
    if title_number is not None:
        line2_parts.append(f"Title {title_number}")
    if chapter:
        line2_parts.append(f"Chapter {chapter}")
    if section_number:
        if section_title:
            line2_parts.append(f"Section {section_number}: {section_title}")
        else:
            line2_parts.append(f"Section {section_number}")
    line2 = " | ".join(line2_parts) if line2_parts else state_name
    return f"{line1}\n{line2}\n\n"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def node_to_chunks(node, state_code: str, year: int) -> List[Dict[str, Any]]:
    """Yield chunk dicts (point_id, text_for_embedding, raw_text, metadata).

    Returns [] for structure-only nodes with no text. ``year`` is the snapshot
    year (e.g. 2026) attached to every chunk's metadata.
    """
    if getattr(node, "node_type", None) != "content":
        return []

    text = _node_text_to_str(node)
    if not text or len(text) < 20:
        return []

    section_num = _section_number(node)
    title_num = _title_number(node)
    section_title = node.node_name or ""
    chapter = None
    for cls_, num in _component_pairs(node):
        if cls_ == "chapter":
            chapter = num
            break

    act_id = _act_id(node, state_code)
    breadcrumb = _build_breadcrumb(node)
    display_path = _build_display_path(state_code, breadcrumb)
    state_name = _STATE_NAMES.get(state_code.lower(), state_code.upper())

    citation = node.citation
    if not citation and section_num and title_num is not None:
        citation = f"{title_num} {state_code.upper()}. Code § {section_num}"
    elif not citation and section_num:
        citation = f"{state_code.upper()}. Code § {section_num}"

    source_url = str(node.link) if getattr(node, "link", None) else None
    display_title = section_title or (citation or act_id)
    prefix = _build_state_prefix(state_code, title_num, chapter, section_num, section_title, year)

    text_chunks = chunk_text(text)
    total_chunks = len(text_chunks)

    # Content-addressed parent id — survives chunk-strategy changes so siblings
    # can find each other across chunker revisions.
    parent_chunk_id = _make_point_id(act_id, -1, text)

    out: List[Dict[str, Any]] = []
    for i, chunk in enumerate(text_chunks):
        point_id = _make_point_id(act_id, i, chunk)
        metadata = {
            # Core (mirrors scripts/us_corpus/chunk_and_ingest.py metadata)
            "text": chunk,
            "act_id": act_id,
            "category": "state_statute",
            "document_type": "statute",
            "jurisdiction": "US",
            "country_code": "US",
            "state": state_code.lower(),
            "act_status": (node.status or "in_force"),

            # Section identity
            "section_number": section_num or "",
            "section_title": section_title,
            "title": f"{state_name} Code",
            "title_name": f"{state_name} Code",
            "title_number": title_num,
            "chapter": chapter or "",
            "year": year,

            # Chunking
            "chunk_index": i,
            "total_chunks": total_chunks,

            # Display metadata
            "citation": citation or "",
            "citation_short": citation or "",
            "display_label": citation or display_title,
            "display_title": display_title,
            "display_path": display_path,
            "breadcrumb": breadcrumb,

            "source_url": source_url or "",

            # Initialized for parity with eCFR/USC payloads; the enrichment
            # pass below populates these from ``text``.
            "word_count": len(chunk.split()),
            "subsection_count": 0,
            "subsection_letters": [],
            "numbered_paragraph_count": 0,
            "cross_references_usc": [],
            "cross_references_cfr": [],
            "cross_references_count": 0,
            "amendment_years": [],
            "amendments_count": 0,
            "last_amended_year": None,
            "public_laws_referenced": [],
            "public_laws_count": 0,
            "renumbered_to": "",
            "transferred_to": "",

            # State-specific
            "level_classifier": node.level_classifier,
            "top_level_title": getattr(node, "top_level_title", None) or "",
            "parent_id": getattr(node, "parent", None) or "",
            "raw_node_id": node.node_id,

            # Sibling-finder so a chunk can fetch its parent section's other
            # chunks (used by retrieval to expand context). parent_chunk_id is
            # the SAME for every chunk of the same section text.
            "parent_chunk_id": parent_chunk_id,
            "full_text_sha1": hashlib.sha1(text.encode("utf-8")).hexdigest(),
        }

        # Enrich from the FULL section text (not the chunk slice) so cross-
        # references and amendment years aren't lost when a long section gets
        # split across chunks.
        try:
            from .enrichers import enrich_payload  # noqa: WPS433
            enrich_payload(metadata, text)
        except Exception:
            pass

        out.append({
            "point_id": point_id,
            "text_for_embedding": prefix + chunk,
            "raw_text": chunk,
            "metadata": metadata,
        })

    return out
