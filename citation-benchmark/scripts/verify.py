#!/usr/bin/env python3
"""Verify this folder: recompute every SHA-256, check row counts, scan for leaks.

    python3 scripts/verify.py            # from the folder root, or anywhere
    python3 scripts/verify.py --quiet

Reads only files in this folder. Exit 0 when every check passes, 1 otherwise.
Needs only the Python 3 standard library.
"""
import argparse, gzip, hashlib, json, re, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
# Anything that looks like an internal identifier or a link. The published rows carry neither.
LEAK = re.compile(
    r"https?://|\bwww\.|us1:|\bvq_|\b(STATE|SRULES|SCONST|SREG|SREGS|SAL|SSL|USC|USCAPP|CFR|FRULES)_[A-Z0-9]{1,6}_[A-Z0-9_.\-]+\b"
)
REQUIRED = {"id", "area", "jurisdiction_label", "corpus", "scope_group", "citation", "form_family", "outcome", "target", "state", "attestation_kind"}


def sha256(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--quiet", action="store_true")
    a = ap.parse_args()
    bad: list[str] = []
    sums = {}
    for line in (ROOT / "SHA256SUMS").read_text().splitlines():
        digest, _, name = line.partition("  ")
        sums[name] = digest
    for name, want in sorted(sums.items()):
        p = ROOT / name
        if not p.is_file():
            bad.append(f"missing file: {name}")
        elif sha256(p) != want:
            bad.append(f"checksum mismatch: {name}")
    on_disk = {str(p.relative_to(ROOT)) for p in ROOT.rglob("*") if p.is_file() and p.name not in ("SHA256SUMS", ".DS_Store") and "__pycache__" not in p.parts}
    for extra in sorted(on_disk - set(sums)):
        bad.append(f"file not listed in SHA256SUMS: {extra}")
    manifest = json.loads((ROOT / "MANIFEST.json").read_text())
    total = 0
    ids: set[str] = set()
    for name, meta in manifest["files"].items():
        p = ROOT / name
        if not p.is_file():
            continue
        if p.stat().st_size != meta["bytes"]:
            bad.append(f"size differs from MANIFEST: {name}")
        if name.startswith("data/"):
            n = 0
            with gzip.open(p, "rt", encoding="utf-8") as f:
                for line in f:
                    r = json.loads(line)
                    n += 1
                    miss = REQUIRED - r.keys()
                    if miss:
                        bad.append(f"{name}:{n} missing {sorted(miss)}")
                    if r["id"] in ids:
                        bad.append(f"duplicate id {r['id']}")
                    ids.add(r["id"])
                    if LEAK.search(line):
                        bad.append(f"{name}:{n} looks like an internal id or a URL")
            if n != meta["rows"]:
                bad.append(f"row count differs from MANIFEST: {name} {n} != {meta['rows']}")
            total += n
    if total != manifest["total_rows"]:
        bad.append(f"total rows {total} != MANIFEST {manifest['total_rows']}")
    if not a.quiet or bad:
        print(f"files checked: {len(sums)}   data rows: {total}")
    for b in bad:
        print("FAIL", b)
    print("OK" if not bad else f"{len(bad)} problem(s)")
    return 1 if bad else 0


sys.exit(main())
