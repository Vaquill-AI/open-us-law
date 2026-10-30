#!/usr/bin/env python3
"""Score your subsection outliner against the benchmark. Standard library only.

Usage:
  python score.py --data data/ --pred predictions.json [--include-excluded] [--json out.json]
  python score.py --data data/ --self-test

Predictions: one JSON object {"<id>": [ ... ]} where <id> is the benchmark id (for example "MN:62Q.481")
and each list item is either a path string ("1/a/2") or an object with a "path" key.
Run your outliner on the "text" field of every record (or on your own copy of that section) and emit
the printed-label path of every subdivision node, in document order. Sections you leave out of the
file are scored as an empty outline (they are NOT skipped), so omissions are not free.
Paths: printed labels outermost to innermost joined by "/", brackets, periods and words such as
"Subd." stripped, case kept (a (1m) is "1m", Subd. 2 is "2", (a) under (1) is "1/a").

Metrics (computed per state, per stratum, and overall):
  exact           share of sections whose predicted path SEQUENCE equals the truth sequence exactly
                  (order matters, duplicates matter). An empty truth is matched only by an empty prediction.
  node_precision  micro over all sections: matched nodes / predicted nodes
  node_recall     micro over all sections: matched nodes / truth nodes
  A matched node is a path present in both, counted as a multiset (a path printed twice must be predicted twice).
  When there are no predicted (or no truth) nodes at all, the corresponding ratio is reported as null.
Sections flagged scoring_excluded in the data are left out unless --include-excluded is given.
"""
import argparse, collections, glob, gzip, json, sys

def load_truth(data_dir):
    rows = []
    for fn in sorted(glob.glob(data_dir.rstrip("/") + "/pinpoint_*.jsonl.gz")):
        with gzip.open(fn, "rt", encoding="utf-8") as f:
            for line in f:
                rows.append(json.loads(line))
    if not rows:
        sys.exit(f"no pinpoint_*.jsonl.gz files under {data_dir}")
    return rows

def paths(items):
    out = []
    for it in items:
        out.append(it["path"] if isinstance(it, dict) else str(it))
    return out

class Acc:
    def __init__(self): self.n = self.exact = self.tp = self.pred = self.truth = 0
    def add(self, t, p):
        self.n += 1; self.exact += (t == p)
        tc, pc = collections.Counter(t), collections.Counter(p)
        self.tp += sum((tc & pc).values()); self.pred += len(p); self.truth += len(t)
    def result(self):
        r = lambda a, b: round(a / b, 4) if b else None
        return {"sections": self.n, "exact": r(self.exact, self.n), "exact_count": self.exact,
                "node_precision": r(self.tp, self.pred), "node_recall": r(self.tp, self.truth),
                "truth_nodes": self.truth, "predicted_nodes": self.pred}

def score(rows, preds, include_excluded=False):
    groups = collections.defaultdict(Acc)
    missing = 0
    for r in rows:
        if r.get("scoring_excluded") and not include_excluded:
            continue
        t = paths(r["outline"])
        if r["id"] in preds: p = paths(preds[r["id"]])
        else: p = []; missing += 1
        for key in ("ALL", r["state"], f"{r['state']}/{r['stratum']}", f"*/{r['stratum']}"):
            groups[key].add(t, p)
    return {k: groups[k].result() for k in sorted(groups)}, missing

def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--data", required=True, help="directory holding pinpoint_*.jsonl.gz (full or outline-only)")
    ap.add_argument("--pred"); ap.add_argument("--json"); ap.add_argument("--self-test", action="store_true")
    ap.add_argument("--include-excluded", action="store_true")
    a = ap.parse_args()
    rows = load_truth(a.data)
    if a.self_test:
        preds = {r["id"]: paths(r["outline"]) for r in rows}
    elif a.pred:
        preds = json.load(open(a.pred, encoding="utf-8"))
        unknown = sorted(set(preds) - {r["id"] for r in rows})
        if unknown: print(f"warning: {len(unknown)} prediction ids not in the benchmark, ignored, e.g. {unknown[:3]}", file=sys.stderr)
    else:
        ap.error("give --pred or --self-test")
    res, missing = score(rows, preds, a.include_excluded)
    print(f"{'group':14} {'n':>4} {'exact':>7} {'prec':>7} {'recall':>7}")
    for k, v in res.items():
        f = lambda x: "   n/a" if x is None else f"{x:7.1%}"
        print(f"{k:14} {v['sections']:4d} {f(v['exact'])} {f(v['node_precision'])} {f(v['node_recall'])}")
    if missing: print(f"note: {missing} sections had no prediction and were scored as empty outlines")
    if a.json: json.dump({"results": res, "sections_without_prediction": missing}, open(a.json, "w"), indent=1)
    if a.self_test and res["ALL"]["exact"] != 1.0: sys.exit("self-test failed")

if __name__ == "__main__":
    main()
