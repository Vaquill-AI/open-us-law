"""Ingest the Code of the District of Columbia (D.C. Code) into our state-statute pipeline.

DC publishes the entire code as structured XML on GitHub under the Open Law
Library schema. The DC Council's website (code.dccouncil.gov) explicitly
asks consumers to bulk-download instead of scraping — so this script does:

  1. Clone (or shallow-fetch) the DCCouncil/law-xml-codified repo's
     `us/dc/council/code` subtree once (the authoritative codified build,
     ~24,113 leaf section files; NOT the raw un-codified law-xml repo).
  2. Walk every `titles/<N>/sections/*.xml` file.
  3. Parse out the <num>, <heading>, <text>, and <annotations> for each.
  4. Emit one chunk per section in the same JSONL shape as other state
     scrapers, mirror each section's text to R2 (state/dc/sections/<act_id>.txt),
     and write the chapter-level XML to R2 for archival.

Output:
    JSONL at <CHUNKS_DIR>/state_dc_statutes_chunks.jsonl
    R2 .txt mirrors at state/dc/sections/<act_id>.txt
    Compatible with the standard embed_and_upsert + sync_states_to_supabase
    pipeline (corpus_type='state', state='dc', category='state_statute').

Run:
    python scripts/us_corpus/ingest_dc_code.py
    python scripts/us_corpus/ingest_dc_code.py --titles 1,2 --workers 8
    python scripts/us_corpus/ingest_dc_code.py --xml-dir /tmp/dc-law-xml
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID
from xml.etree import ElementTree as ET

sys.stdout.reconfigure(line_buffering=True)  # type: ignore[attr-defined]

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
DATA_DIR = Path(
    os.environ.get(
        "STATE_CHUNKS_DIR_OVERRIDE",
        str(_PROJECT_ROOT / "scripts/us_corpus/state_scrapers/data/state_chunks"),
    )
)
OUT = DATA_DIR / "state_dc_statutes_chunks.jsonl"

# The CODIFIED build is the authoritative D.C. Code (24,113 leaf sections incl.
# repealed), not the raw un-codified `law-xml` repo (~12K files) this script
# originally used and which left DC ~7k sections short. The codified repo's
# published HEAD lives on the `publication/2021-10-18` branch (a legacy branch
# NAME kept current: HEAD commit is dated 2026-05-19); `main` is a stale 2021
# snapshot, so we pin the publication branch explicitly.
XML_REPO = "https://github.com/DCCouncil/law-xml-codified.git"
XML_BRANCH = "publication/2021-10-18"
XML_SUBPATH = "us/dc/council/code"

XMLNS = {
    "oll": "https://code.dccouncil.us/schemas/dc-library",
    "xi": "http://www.w3.org/2001/XInclude",
}


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


# ---------------------------------------------------------------------------
# Repo fetch (shallow git clone or update existing)
# ---------------------------------------------------------------------------


def _ensure_xml_repo(target_dir: Path) -> Path:
    """Shallow-clone (or pull) the DCCouncil/law-xml-codified repo at XML_BRANCH
    and return the path to the D.C. Code subtree. ~24K section files.
    """
    if target_dir.exists() and (target_dir / ".git").exists():
        print(f"[dc] pulling latest from existing clone at {target_dir}", flush=True)
        subprocess.run(
            [
                "git",
                "-C",
                str(target_dir),
                "pull",
                "--depth",
                "1",
                "--ff-only",
                "origin",
                XML_BRANCH,
            ],
            check=False,
            capture_output=True,
        )
    else:
        print(
            f"[dc] shallow-cloning {XML_REPO} ({XML_BRANCH}) → {target_dir} (~130 MB)", flush=True
        )
        target_dir.parent.mkdir(parents=True, exist_ok=True)
        subprocess.run(
            ["git", "clone", "--depth", "1", "--branch", XML_BRANCH, XML_REPO, str(target_dir)],
            check=True,
        )
    code_dir = target_dir / XML_SUBPATH
    if not code_dir.exists():
        raise FileNotFoundError(f"Expected {code_dir} after clone")
    return code_dir


# ---------------------------------------------------------------------------
# R2 helpers
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# XML parsing
# ---------------------------------------------------------------------------


@dataclass
class Section:
    title_num: str
    title_name: str  # from titles/<N>/index.xml <heading>
    chapter_num: str
    chapter_name: str
    subchapter: str
    subchapter_name: str
    section_number: str  # e.g. "1-101"
    section_title: str  # cleaned <heading>
    raw_text: str  # body <text> (concatenated if multiple)
    history: str  # annotations of type=History
    source_url: str
    is_omitted: bool = False  # <reason>Omitted</reason> sentinel — legitimately no body
    omitted_reason: str = ""


def _strip_ns(tag: str) -> str:
    return tag.split("}", 1)[-1] if "}" in tag else tag


def _inline_text(elem: ET.Element) -> str:
    """Collect text from element + descendants as inline string (single line)."""
    parts: list[str] = []
    if elem.text:
        parts.append(elem.text)
    for child in elem:
        parts.append(_inline_text(child))
        if child.tail:
            parts.append(child.tail)
    return " ".join(p.strip() for p in parts if p and p.strip())


# DC sections nest like: section → text | para → (num, text, para, aftertext) → ...
# Render to a flat plain-text block while preserving paragraph numbering.
_CONTAINER_TAGS = {"para", "container"}
_LEAF_TEXT_TAGS = {"text", "aftertext"}


def _render_table(elem: ET.Element, indent: str) -> list[str]:
    lines: list[str] = []
    for row in elem.iter():
        if _strip_ns(row.tag) == "tr":
            cells = [_inline_text(td).strip() for td in row if _strip_ns(td.tag) == "td"]
            cells = [c for c in cells if c]
            if cells:
                lines.append(f"{indent}| " + " | ".join(cells) + " |")
    return lines


def _render_node(elem: ET.Element, depth: int) -> list[str]:
    """Render a single body node (text, para, container, aftertext, table) to lines."""
    tag = _strip_ns(elem.tag)
    indent = "  " * depth
    lines: list[str] = []

    if tag in _LEAF_TEXT_TAGS:
        t = _inline_text(elem).strip()
        if t:
            lines.append(f"{indent}{t}")
        return lines

    if tag == "table":
        return _render_table(elem, indent)

    if tag in _CONTAINER_TAGS:
        num_text = ""
        heading_text = ""
        content_children: list[ET.Element] = []
        for child in elem:
            ctag = _strip_ns(child.tag)
            if ctag == "num":
                num_text = _inline_text(child).strip()
            elif ctag == "heading":
                heading_text = _inline_text(child).strip()
            elif ctag in _LEAF_TEXT_TAGS or ctag in _CONTAINER_TAGS or ctag == "table":
                content_children.append(child)
            # Ignore <annotations>, <prefix>, <reason>, etc. at this level

        prefix_bits = [b for b in (num_text, heading_text) if b]
        prefix = " ".join(prefix_bits)

        # Render children. The first text child inherits the prefix inline.
        rendered_any = False
        for i, c in enumerate(content_children):
            ctag = _strip_ns(c.tag)
            child_lines = _render_node(c, depth + (1 if ctag in _CONTAINER_TAGS else 0))
            if not child_lines:
                continue
            if not rendered_any and prefix:
                first = child_lines[0].lstrip()
                child_lines[0] = f"{indent}{prefix} {first}"
                rendered_any = True
            lines.extend(child_lines)

        # Pure-prefix container (e.g. label with no body) still gets a line.
        if not rendered_any and prefix:
            lines.append(f"{indent}{prefix}")
        return lines

    return lines


def _extract_body_text(root: ET.Element) -> str:
    """Render section body to a plain-text string, preserving paragraph numbering.

    Patterns:
      A) <section><text>intro</text><para><num>(1)</num><text>...</text></para>...</section>
      B) <section><para>...</para><para>...</para></section>   (no intro text)
    """
    lines: list[str] = []
    for child in root:
        ctag = _strip_ns(child.tag)
        if ctag in _LEAF_TEXT_TAGS or ctag in _CONTAINER_TAGS or ctag == "table":
            lines.extend(_render_node(child, depth=0))
    return "\n".join(lines).strip()


def _parse_section_xml(
    section_path: Path,
    title_num: str,
    title_name: str,
    chapter_num: str,
    chapter_name: str,
    subchapter: str,
    subchapter_name: str,
) -> Section | None:
    try:
        tree = ET.parse(section_path)
    except ET.ParseError as e:
        print(f"  ! XML parse error in {section_path}: {e}", flush=True)
        return None
    root = tree.getroot()
    if _strip_ns(root.tag) != "section":
        return None

    num = ""
    heading = ""
    history_parts: list[str] = []
    is_omitted = False
    omitted_reason = ""

    for child in root:
        tag = _strip_ns(child.tag)
        if tag == "num":
            num = (child.text or "").strip()
        elif tag == "heading":
            heading = _inline_text(child).strip()
        elif tag == "reason":
            is_omitted = True
            omitted_reason = _inline_text(child).strip() or "Omitted"
        elif tag == "annotations":
            for ann in child:
                if _strip_ns(ann.tag) == "annotation" and ann.attrib.get("type") == "History":
                    h = _inline_text(ann).strip()
                    if h:
                        history_parts.append(h)

    if not num:
        return None

    body = _extract_body_text(root)

    # Omitted sections: synthesize a body so they're still indexable.
    if not body and is_omitted:
        # Pull the editor's note explaining why
        editor_note = ""
        for child in root:
            if _strip_ns(child.tag) == "annotations":
                for ann in child:
                    if (
                        _strip_ns(ann.tag) == "annotation"
                        and ann.attrib.get("type") == "Editor's Notes"
                    ):
                        editor_note = _inline_text(ann).strip()
                        break
        body = f"[Omitted] {heading}. {editor_note}".strip()
    elif not body:
        # Truly empty section (no text, no para, no reason) — skip
        return None

    raw_text = body
    section_title = f"§ {num}. {heading}".rstrip(".") + "."
    history = "; ".join(history_parts)

    return Section(
        title_num=title_num,
        title_name=title_name,
        chapter_num=chapter_num,
        chapter_name=chapter_name,
        subchapter=subchapter,
        subchapter_name=subchapter_name,
        section_number=num,
        section_title=section_title,
        raw_text=raw_text,
        history=history,
        source_url=f"https://code.dccouncil.gov/us/dc/council/code/sections/{num}",
        is_omitted=is_omitted,
        omitted_reason=omitted_reason,
    )


def _walk_title(title_dir: Path) -> tuple[str, str, list[tuple[Path, dict]]]:
    """Walk title <N>/index.xml + sections/ dir. Return (title_num, title_name,
    [(section_path, chapter_context), ...])."""
    title_num = title_dir.name
    title_index = title_dir / "index.xml"
    title_name = ""
    chapter_map: dict[
        str, dict
    ] = {}  # section_num -> {chapter_num, chapter_name, subchapter, subchapter_name}

    if title_index.exists():
        try:
            tree = ET.parse(title_index)
            root = tree.getroot()
            if _strip_ns(root.tag) == "container":
                for child in root:
                    if _strip_ns(child.tag) == "heading":
                        title_name = _inline_text(child).strip()

                # Walk recursively to find <xi:include href="./sections/<id>.xml"/>
                # and associate each with its enclosing container chain.
                def walk_container(
                    c: ET.Element, chapter_num="", chapter_name="", subch="", subch_name=""
                ):
                    if _strip_ns(c.tag) == "container":
                        cur_prefix = ""
                        cur_num = ""
                        cur_heading = ""
                        for ch in c:
                            tag = _strip_ns(ch.tag)
                            if tag == "prefix":
                                cur_prefix = (ch.text or "").strip()
                            elif tag == "num":
                                cur_num = (ch.text or "").strip()
                            elif tag == "heading":
                                cur_heading = _inline_text(ch).strip()
                        if cur_prefix.lower() == "chapter":
                            chapter_num = cur_num
                            chapter_name = f"Chapter {cur_num}. {cur_heading}".rstrip(".") + "."
                        elif cur_prefix.lower() == "subchapter":
                            subch = cur_num
                            subch_name = f"Subchapter {cur_num}. {cur_heading}".rstrip(".") + "."
                    for ch in c:
                        tag = _strip_ns(ch.tag)
                        if tag == "container":
                            walk_container(ch, chapter_num, chapter_name, subch, subch_name)
                        elif tag == "include":  # xi:include
                            href = ch.attrib.get("href", "")
                            m = re.search(r"/([^/]+)\.xml$", href)
                            if m:
                                sec_num = m.group(1)
                                chapter_map[sec_num] = {
                                    "chapter_num": chapter_num,
                                    "chapter_name": chapter_name,
                                    "subchapter": subch,
                                    "subchapter_name": subch_name,
                                }

                walk_container(root)
        except ET.ParseError:
            pass

    sections_dir = title_dir / "sections"
    section_files: list[tuple[Path, dict]] = []
    if sections_dir.exists():
        for sec_file in sorted(sections_dir.glob("*.xml")):
            sec_num = sec_file.stem
            ctx = chapter_map.get(
                sec_num,
                {
                    "chapter_num": "",
                    "chapter_name": "",
                    "subchapter": "",
                    "subchapter_name": "",
                },
            )
            section_files.append((sec_file, ctx))
    return title_num, title_name, section_files


# ---------------------------------------------------------------------------
# Chunk emission
# ---------------------------------------------------------------------------


def _make_point_id(act_id: str, chunk_index: int, text: str) -> str:
    h = hashlib.sha1(text.encode("utf-8")).hexdigest()
    seed = f"{act_id}::{chunk_index}::{h}".encode()
    return str(UUID(hashlib.md5(seed).hexdigest()))


# Section-level <reason> value -> normalized act_status. The codified repo marks
# dead sections with a direct-child <reason> (Repealed / Expired / Omitted /
# Transferred / Recodified / Renumbered / Reserved / Abolished / Not Funded /
# Not Applicable). Sections with no <reason> (or a future-effective "Applicable
# <date>" note) are current law. The good-law/currency gate reads act_status, so
# repealed sections are ingested (searchable) but honestly flagged.
def _act_status_from_reason(reason: str) -> str:
    r = (reason or "").strip().lower()
    if not r:
        return "in_force"
    if "repeal" in r or "abolish" in r:
        return "repealed"
    if "expired" in r:
        return "expired"
    if "omitted" in r:
        return "omitted"
    if "reserved" in r:
        return "reserved"
    if "renumber" in r:
        return "renumbered"
    if "transferred" in r:
        return "transferred"
    if "recodified" in r:
        return "recodified"
    if "not fund" in r:  # "Not Funded" and its typo variants "Not Fundeded"
        return "not_funded"
    if "not applicable" in r:
        return "not_applicable"
    return "in_force"  # future-effective / unrecognized -> still current


# Human label for the embedding header, per status.
_STATUS_LABEL = {
    "in_force": "In Force",
    "repealed": "Repealed",
    "expired": "Expired",
    "omitted": "Omitted",
    "reserved": "Reserved",
    "renumbered": "Renumbered",
    "transferred": "Transferred",
    "recodified": "Recodified",
    "not_funded": "Not Funded",
    "not_applicable": "Not Applicable",
}


def _section_to_chunk(sec: Section) -> dict:
    act_id = f"STATE_DC_T{sec.title_num}_C{sec.chapter_num}_S{sec.section_number}"
    chunk_index = 0
    text = sec.raw_text
    act_status = _act_status_from_reason(sec.omitted_reason if sec.is_omitted else "")
    status_label = _STATUS_LABEL.get(act_status, "In Force")
    embed_text = (
        f"Statute: DC Code (2026) | US | District of Columbia | {status_label}\n"
        f"Title {sec.title_num}: {sec.title_name}\n"
        f"Chapter {sec.chapter_num}: {sec.chapter_name}\n"
        f"{sec.section_title}\n\n{text}"
    )
    return {
        "point_id": _make_point_id(act_id, chunk_index, text),
        "text_for_embedding": embed_text,
        "raw_text": text,
        "metadata": {
            "text": text,
            "act_id": act_id,
            "category": "state_statute",
            "document_type": "statute",
            "corpus_type": "state",
            "jurisdiction": "US",
            "country_code": "US",
            "state": "dc",
            "act_status": act_status,
            "section_number": sec.section_number,
            "section_title": sec.section_title,
            "title": "Code of the District of Columbia",
            "title_name": "DC Code",
            # Keep alphanumeric titles (27A/28A/29A) as their string form rather
            # than None, matching how alphanumeric title_number (e.g. Alabama 13A)
            # is supported across us_statutes. Digit titles stay int for parity.
            "title_number": int(sec.title_num) if sec.title_num.isdigit() else sec.title_num,
            "chapter": sec.chapter_num,
            "chapter_name": sec.chapter_name,
            "subchapter": sec.subchapter,
            "subchapter_name": sec.subchapter_name,
            "year": 2026,
            "chunk_index": chunk_index,
            "total_chunks": 1,
            "citation": f"D.C. Code § {sec.section_number}",
            "citation_short": f"D.C. Code § {sec.section_number}",
            "display_label": f"D.C. Code § {sec.section_number}",
            "display_title": sec.section_title,
            "display_path": (
                f"DC Code / Title {sec.title_num} / Chapter {sec.chapter_num} / "
                f"§ {sec.section_number}"
            ),
            "breadcrumb": [
                {
                    "type": "title",
                    "num": sec.title_num,
                    "label": f"Title {sec.title_num}",
                    "name": sec.title_name,
                },
                {
                    "type": "chapter",
                    "num": sec.chapter_num,
                    "label": f"Chapter {sec.chapter_num}",
                    "name": sec.chapter_name,
                },
                {
                    "type": "section",
                    "num": sec.section_number,
                    "label": f"§ {sec.section_number}",
                    "name": "",
                },
            ],
            "source_url": sec.source_url,
            "word_count": len(text.split()),
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
            "level_classifier": "section",
            "top_level_title": sec.title_num,
            "parent_id": f"us/dc/statutes/title={sec.title_num}/chapter={sec.chapter_num}",
            "raw_node_id": f"us/dc/statutes/title={sec.title_num}/chapter={sec.chapter_num}/section={sec.section_number}",
            "parent_chunk_id": _make_point_id(act_id, -1, text),
            "full_text_sha1": hashlib.sha1(text.encode("utf-8")).hexdigest(),
        },
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def _process_section(args_tuple):
    sec_file, ctx, title_num, title_name = args_tuple
    sec = _parse_section_xml(
        sec_file,
        title_num=title_num,
        title_name=title_name,
        chapter_num=ctx["chapter_num"],
        chapter_name=ctx["chapter_name"],
        subchapter=ctx["subchapter"],
        subchapter_name=ctx["subchapter_name"],
    )
    if not sec:
        return None
    return _section_to_chunk(sec)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--titles", default="", help="Comma-separated title numbers (e.g. '1,2'). Default: all."
    )
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument(
        "--xml-dir", default=None, help="Existing law-xml clone path (skips clone if set)."
    )
    ap.add_argument(
        "--dry-run", action="store_true", help="Just count sections, don't upload or write JSONL."
    )
    args = ap.parse_args()

    _load_env()
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    repo_dir = Path(args.xml_dir) if args.xml_dir else Path(tempfile.gettempdir()) / "dc-law-xml"
    code_dir = _ensure_xml_repo(repo_dir)
    print(f"[dc] code dir: {code_dir}")

    titles_dir = code_dir / "titles"
    title_dirs = sorted(
        [d for d in titles_dir.iterdir() if d.is_dir()],
        key=lambda p: int(p.name) if p.name.isdigit() else 9999,
    )
    print(f"[dc] discovered {len(title_dirs)} titles")

    if args.titles:
        wanted = {t.strip() for t in args.titles.split(",") if t.strip()}
        title_dirs = [t for t in title_dirs if t.name in wanted]
        print(f"[dc] filtered to {len(title_dirs)} titles: {[t.name for t in title_dirs]}")

    # Collect all section files first
    work: list[tuple[Path, dict, str, str]] = []
    for td in title_dirs:
        title_num, title_name, sec_files = _walk_title(td)
        print(f"[dc] Title {title_num}: {title_name[:60]!r} → {len(sec_files)} sections")
        for sec_file, ctx in sec_files:
            work.append((sec_file, ctx, title_num, title_name))

    print(f"\n[dc] total sections to process: {len(work):,}")
    if args.dry_run:
        return 0

    chunks: list[dict] = []
    failed = 0
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = [
            ex.submit(_process_section, (sec_file, ctx, title_num, title_name))
            for sec_file, ctx, title_num, title_name in work
        ]
        for i, fut in enumerate(as_completed(futures), start=1):
            try:
                chunk = fut.result()
                if chunk:
                    chunks.append(chunk)
                else:
                    failed += 1
            except Exception as e:
                failed += 1
                if failed <= 5:
                    print(f"  ! worker error: {e}", flush=True)
            if i % 500 == 0:
                rate = len(chunks) / max(time.time() - t0, 0.1)
                print(
                    f"  ... {i:>6,}/{len(work):,}  rate={rate:.0f}/s  failed={failed}", flush=True
                )

    # Write JSONL with content-hashed dedup
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
        f"\n=== Done: {len(chunks):,} parsed, {written:,} new chunks, "
        f"{len(chunks) - written:,} dupes, {failed} failed ==="
    )
    print(f"JSONL: {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
