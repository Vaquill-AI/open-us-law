#!/usr/bin/env python3
"""
Extract all USC ZIPs into a flat filesystem structure for analysis + R2 upload.

Structure after extraction:
  data/us_corpus/usc/extracted/
  ├── html/2024/title-{N}/
  │   ├── USCODE-2024-title{N}-chap{C}-sec{S}.htm    # Section HTMLs
  │   ├── USCODE-2024-title{N}-chap{C}.htm           # Chapter HTMLs (for chapter view)
  │   └── ... (plus TOC and front matter)
  ├── pdf/2024/title-{N}/
  │   └── (same structure, PDF files)
  └── xml/2024/title-{N}/
      ├── dip.xml                                     # METS manifest
      ├── mods.xml                                    # MODS metadata
      └── usc{N}.xml                                  # Full title USLM XML

Usage:
    # Extract all 53 USC titles
    python scripts/us_corpus/extract_usc_zips.py

    # Extract specific title
    python scripts/us_corpus/extract_usc_zips.py --title 42

    # Just analyze without extracting
    python scripts/us_corpus/extract_usc_zips.py --analyze-only

    # Skip extraction if target dir already has files (resume)
    python scripts/us_corpus/extract_usc_zips.py --resume
"""

from __future__ import annotations

import argparse
import re
import shutil
import sys
import time
import zipfile
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

sys.stdout.reconfigure(line_buffering=True)  # type: ignore[attr-defined]

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

USC_RAW_DIR = _PROJECT_ROOT / "data" / "us_corpus" / "usc" / "raw"
USC_EXTRACTED_DIR = _PROJECT_ROOT / "data" / "us_corpus" / "usc" / "extracted"


@dataclass
class ExtractStats:
    titles_processed: int = 0
    html_files: int = 0
    pdf_files: int = 0
    xml_files: int = 0
    html_bytes: int = 0
    pdf_bytes: int = 0
    xml_bytes: int = 0
    sections: int = 0  # Files ending in -sec{N}
    chapters: int = 0
    tocs: int = 0  # Table of contents files
    fronts: int = 0  # Front matter files
    errors: list[str] = field(default_factory=list)
    start: float = field(default_factory=time.time)

    def elapsed(self) -> str:
        return f"{time.time() - self.start:.1f}s"


def classify_file(filename: str) -> str:
    """Classify a file within a USC ZIP by its name pattern.

    Returns one of: 'section', 'chapter', 'toc', 'front', 'title', 'other'
    """
    name = filename.split("/")[-1]

    if name.endswith("-toc.htm") or name.endswith("-toc.pdf"):
        return "toc"
    if name.endswith("-front.htm") or name.endswith("-front.pdf"):
        return "front"
    if re.search(r"-sec[\w\.-]+\.(htm|pdf)$", name):
        return "section"
    if re.search(r"-chap\w+\.(htm|pdf)$", name):
        # Skip if it was already matched as section/toc/front
        return "chapter"
    if re.match(r"USCODE-\d+-title\d+\.(htm|pdf)$", name):
        return "title"  # Whole-title render
    return "other"


def extract_one_zip(
    zip_path: Path,
    output_base: Path,
    resume: bool,
    stats: ExtractStats,
) -> None:
    """Extract one USC ZIP into the cleaned directory layout."""
    year = zip_path.parent.name  # e.g., "2024"
    title_match = re.match(r"title-(\d+)\.zip", zip_path.name)
    if not title_match:
        return
    title_num = int(title_match.group(1))

    # Output dirs
    html_dir = output_base / "html" / year / f"title-{title_num}"
    pdf_dir = output_base / "pdf" / year / f"title-{title_num}"
    xml_dir = output_base / "xml" / year / f"title-{title_num}"
    html_dir.mkdir(parents=True, exist_ok=True)
    pdf_dir.mkdir(parents=True, exist_ok=True)
    xml_dir.mkdir(parents=True, exist_ok=True)

    # File classification counts for this title
    title_classes = defaultdict(int)

    with zipfile.ZipFile(zip_path) as z:
        for info in z.infolist():
            fname = info.filename

            # Skip directories
            if fname.endswith("/") or info.is_dir():
                continue

            # Determine output path and type
            base_name = fname.split("/")[-1]
            ext = base_name.rsplit(".", 1)[-1].lower() if "." in base_name else ""

            if ext == "htm":
                out_path = html_dir / base_name
                stats.html_files += 1
                stats.html_bytes += info.file_size
                kind = "html"
            elif ext == "pdf":
                out_path = pdf_dir / base_name
                stats.pdf_files += 1
                stats.pdf_bytes += info.file_size
                kind = "pdf"
            elif ext == "xml":
                # Rename 'dip.xml' and 'mods.xml' to keep them distinct; usc{N}.xml too
                out_path = xml_dir / base_name
                stats.xml_files += 1
                stats.xml_bytes += info.file_size
                kind = "xml"
            else:
                continue  # Skip unknown formats

            # Classify for stats
            if kind != "xml":
                cls = classify_file(fname)
                title_classes[cls] += 1
                if cls == "section":
                    stats.sections += 1
                elif cls == "chapter":
                    stats.chapters += 1
                elif cls == "toc":
                    stats.tocs += 1
                elif cls == "front":
                    stats.fronts += 1

            # Skip if exists + resume
            if resume and out_path.exists() and out_path.stat().st_size == info.file_size:
                continue

            try:
                with z.open(info) as src, open(out_path, "wb") as dst:
                    shutil.copyfileobj(src, dst, length=1024 * 1024)
            except Exception as e:
                stats.errors.append(f"{fname}: {e}")

    stats.titles_processed += 1
    print(
        f"  Title {title_num:2d}: html={sum(1 for _ in html_dir.glob('*.htm'))} "
        f"pdf={sum(1 for _ in pdf_dir.glob('*.pdf'))} "
        f"xml={sum(1 for _ in xml_dir.glob('*.xml'))} "
        f"| sec={title_classes['section']} chap={title_classes['chapter']} "
        f"toc={title_classes['toc']} front={title_classes['front']} other={title_classes['other']}"
    )


def analyze_only(title_filter: int | None) -> None:
    """Just analyze ZIP contents without extracting."""
    zips = sorted(USC_RAW_DIR.rglob("title-*.zip"))
    if title_filter:
        zips = [z for z in zips if z.stem == f"title-{title_filter}"]

    total_by_class: dict[str, int] = defaultdict(int)
    total_by_ext: dict[str, int] = defaultdict(int)
    total_bytes_by_ext: dict[str, int] = defaultdict(int)

    print(f"Analyzing {len(zips)} USC ZIPs...")
    for zip_path in zips:
        title_num = int(zip_path.stem.replace("title-", ""))
        title_classes = defaultdict(int)
        title_bytes = defaultdict(int)

        with zipfile.ZipFile(zip_path) as z:
            for info in z.infolist():
                if info.is_dir():
                    continue
                fname = info.filename
                ext = fname.rsplit(".", 1)[-1].lower() if "." in fname else ""
                cls = classify_file(fname)

                total_by_class[cls] += 1
                total_by_ext[ext] += 1
                total_bytes_by_ext[ext] += info.file_size
                title_classes[cls] += 1
                title_bytes[ext] += info.file_size

        print(
            f"  Title {title_num:2d}: "
            f"sec={title_classes['section']} chap={title_classes['chapter']} "
            f"toc={title_classes['toc']} front={title_classes['front']} "
            f"title={title_classes['title']} other={title_classes['other']} "
            f"| html={title_bytes['htm']/1024/1024:.1f}MB pdf={title_bytes['pdf']/1024/1024:.1f}MB"
        )

    print(f"\n=== TOTALS ===")
    print(f"By classification:")
    for cls, count in sorted(total_by_class.items(), key=lambda x: -x[1]):
        print(f"  {cls:10s}: {count:,}")
    print(f"\nBy extension:")
    for ext, count in sorted(total_by_ext.items(), key=lambda x: -x[1]):
        mb = total_bytes_by_ext[ext] / 1024 / 1024
        print(f"  .{ext:4s}: {count:,} files, {mb:,.1f} MB")


def main():
    parser = argparse.ArgumentParser(description="Extract USC ZIPs to flat filesystem")
    parser.add_argument("--title", type=int, help="Extract only this title")
    parser.add_argument("--analyze-only", action="store_true", help="Don't extract, just analyze")
    parser.add_argument("--resume", action="store_true", help="Skip files that already exist")
    parser.add_argument(
        "--output",
        type=str,
        default=str(USC_EXTRACTED_DIR),
        help=f"Output directory (default: {USC_EXTRACTED_DIR})",
    )
    args = parser.parse_args()

    if args.analyze_only:
        analyze_only(args.title)
        return

    output_base = Path(args.output)
    output_base.mkdir(parents=True, exist_ok=True)

    zips = sorted(USC_RAW_DIR.rglob("title-*.zip"))
    if args.title:
        zips = [z for z in zips if z.stem == f"title-{args.title}"]

    if not zips:
        print("No USC ZIPs found")
        sys.exit(1)

    print(f"Extracting {len(zips)} USC ZIPs to {output_base}...")
    stats = ExtractStats()

    for i, zip_path in enumerate(zips, 1):
        zip_size_mb = zip_path.stat().st_size / 1024 / 1024
        print(f"\n[{i}/{len(zips)}] {zip_path.name} ({zip_size_mb:.1f} MB)...")
        extract_one_zip(zip_path, output_base, args.resume, stats)

    print(f"\n{'='*60}")
    print(f"Extract Complete ({stats.elapsed()})")
    print(f"  Titles:           {stats.titles_processed}")
    print(f"  HTML files:       {stats.html_files:,} ({stats.html_bytes/1024/1024/1024:.2f} GB)")
    print(f"  PDF files:        {stats.pdf_files:,} ({stats.pdf_bytes/1024/1024/1024:.2f} GB)")
    print(f"  XML files:        {stats.xml_files:,} ({stats.xml_bytes/1024/1024:.1f} MB)")
    print(f"  ")
    print(f"  Sections:         {stats.sections:,}")
    print(f"  Chapters:         {stats.chapters:,}")
    print(f"  TOCs (can drop):  {stats.tocs:,}")
    print(f"  Front matter:     {stats.fronts:,}")
    if stats.errors:
        print(f"  Errors:           {len(stats.errors)}")
        for err in stats.errors[:5]:
            print(f"    - {err}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
