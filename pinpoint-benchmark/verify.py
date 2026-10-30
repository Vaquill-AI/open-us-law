#!/usr/bin/env python3
"""Verify the release: checksums, row counts, schema, offsets, no engine field. Standard library only.
Usage: python verify.py   (run from the release directory)"""
import glob, gzip, hashlib, json, os, re, subprocess, sys
ROOT = os.path.dirname(os.path.abspath(__file__))
EXPECT = json.load(open(os.path.join(ROOT, "COUNTS.json")))
fail = []
def bad(m): fail.append(m); print("FAIL", m)

sums = json.load(open(os.path.join(ROOT, "SHA256SUMS.json")))
for rel, want in sums.items():
    p = os.path.join(ROOT, rel)
    if not os.path.exists(p): bad(f"missing {rel}"); continue
    if hashlib.sha256(open(p, "rb").read()).hexdigest() != want: bad(f"checksum {rel}")
print(f"checksums: {len(sums)} files checked")

seen = set()
for variant in ("data", "data_outline_only"):
    for st, n in EXPECT["sections"].items():
        fn = os.path.join(ROOT, variant, f"pinpoint_{st}.jsonl.gz")
        rows = [json.loads(l) for l in gzip.open(fn, "rt", encoding="utf-8")]
        if len(rows) != n: bad(f"{variant}/{st} rows {len(rows)} != {n}")
        nodes = 0
        for r in rows:
            if "engine" in r or "quality" in r: bad(f"{r['id']} carries an engine-derived field")
            if variant == "data" and r["id"] in seen: bad(f"duplicate id {r['id']}")
            seen.add(r["id"])
            if (variant == "data") != ("text" in r): bad(f"{r['id']} text presence wrong for {variant}")
            nodes += len(r["outline"])
            last = -1
            for o in r["outline"]:
                s, e = o["start"], o["end"]
                if s is None: bad(f"{r['id']} {o['path']} no offsets"); continue
                if not (0 <= s <= e <= r["text_chars"]): bad(f"{r['id']} {o['path']} offsets {s},{e}")
                if s < last: bad(f"{r['id']} {o['path']} start goes backwards")
                last = s
                if variant == "data" and o["head"]:
                    seg = re.sub(r"\s+", " ", r["text"][s:e])
                    h = " ".join(re.sub(r"\s+", " ", o["head"]).strip().split(" ")[:3])  # heads are labeller-typed; the first 3 words must match
                    if h not in seg: bad(f"{r['id']} {o['path']} head not inside its span")
        if nodes != EXPECT["nodes"][st]: bad(f"{variant}/{st} nodes {nodes} != {EXPECT['nodes'][st]}")
print(f"rows/nodes/offsets checked for {len(EXPECT['sections'])} states, both variants")
for v in ("data", "data_outline_only"):
    rc = subprocess.run([sys.executable, os.path.join(ROOT, "score.py"), "--data", os.path.join(ROOT, v), "--self-test"],
                        capture_output=True, text=True)
    if rc.returncode: bad(f"scorer self-test on {v}")
print("scorer self-test done")
print("OK" if not fail else f"{len(fail)} failures")
sys.exit(1 if fail else 0)
