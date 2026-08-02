"""Run --max-sections=N against every state with a scraper and tabulate results.

Each state is launched as a subprocess with a per-state timeout so a hung
scraper can't wedge the sweep. We capture: exit code, sections emitted,
errors, classifier of failure (geofence / selenium / selector / other).

Usage:
    python -m vaquill_pipeline.sweep_states                   # all states, 3 sections each
    python -m vaquill_pipeline.sweep_states --max-sections 5
    python -m vaquill_pipeline.sweep_states --concurrency 4   # parallel runs
    python -m vaquill_pipeline.sweep_states --states de fl ny  # subset

Output: data/state_chunks/sweep_<ts>.csv  + console summary.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import csv
import datetime as _dt
import json
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from vaquill_pipeline.run_state import WORKING_STATES  # noqa: E402


def _classify(stdout: str, stderr: str, returncode: int, sections: int) -> str:
    """Best-effort failure classifier for triage."""
    if sections > 0:
        return "ok"
    blob = (stdout + "\n" + stderr).lower()
    if returncode == 124 or "timeout" in blob and "connect" in blob:
        if "connection to" in blob and "timed out" in blob:
            return "geofenced_tcp_timeout"
        return "timeout"
    if "chromedriver" in blob or "webdriver" in blob or "selenium" in blob:
        return "selenium_crash"
    if "noneType" in blob or "attributeerror" in blob:
        return "selector_drift"
    if "valueerror" in blob and "level_classifier" in blob:
        return "schema_mismatch"
    if "name resolution" in blob or "nodename" in blob:
        return "dns_failure"
    if "403" in blob and "forbidden" in blob:
        return "http_403_blocked"
    if "no module named" in blob:
        return "import_error"
    if returncode == 0:
        return "no_content_nodes"
    return "other"


def _run_one(state: str, max_sections: int, per_state_timeout: int) -> Dict[str, object]:
    chunks_dir = ROOT / "data" / "state_chunks"
    chunks_path = chunks_dir / f"state_{state}_statutes.jsonl"
    errors_path = chunks_dir / f"state_{state}_errors.jsonl"
    pre_count = 0
    if chunks_path.exists():
        with open(chunks_path, encoding="utf-8") as fh:
            pre_count = sum(1 for _ in fh)
    # JsonlSink uses append mode and content-addressed point_ids, so re-runs
    # are idempotent. Never wipe the JSONL on start: a failed re-scrape would
    # destroy prior data with nothing to replace it (real incident 2026-05-12).
    # Errors file is wiped to capture this run's failures only.
    if errors_path.exists():
        errors_path.unlink()

    t0 = time.time()
    proc = subprocess.run(
        [sys.executable, "-m", "vaquill_pipeline.run_state", state, "--max-sections", str(max_sections)],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=per_state_timeout,
    )
    elapsed = time.time() - t0

    sections = 0
    if chunks_path.exists():
        with open(chunks_path, encoding="utf-8") as fh:
            sections = sum(1 for _ in fh)
    # Report DELTA (new chunks this run) not total — keeps the CSV honest when
    # we re-run for recovery on top of prior data.
    sections = max(0, sections - pre_count)

    errors = 0
    if errors_path.exists():
        with open(errors_path, encoding="utf-8") as fh:
            errors = sum(1 for _ in fh)

    classifier = _classify(proc.stdout, proc.stderr, proc.returncode, sections)

    # Extract one-line root cause for the table
    tail = (proc.stderr or proc.stdout).strip().splitlines()
    err_line = ""
    for line in reversed(tail[-30:]):
        if any(k in line for k in ("Error", "Traceback", "Exception", "timed out", "ConnectTime")):
            err_line = line[:140]
            break
    if not err_line and proc.stderr:
        err_line = proc.stderr.strip().splitlines()[-1][:140] if proc.stderr.strip() else ""

    return {
        "state": state,
        "elapsed_sec": round(elapsed, 1),
        "chunks": sections,
        "errors": errors,
        "exit": proc.returncode,
        "classifier": classifier,
        "err_line": err_line,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-sections", type=int, default=3)
    ap.add_argument("--per-state-timeout", type=int, default=90)
    ap.add_argument("--concurrency", type=int, default=3)
    ap.add_argument("--states", nargs="+", default=None,
                    help="Subset of state codes. Default: all states with a scraper.")
    args = ap.parse_args()

    states = sorted(args.states or list(WORKING_STATES))
    print(f"[sweep] {len(states)} states, max_sections={args.max_sections}, "
          f"timeout={args.per_state_timeout}s, concurrency={args.concurrency}")
    print()

    results: List[Dict[str, object]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as ex:
        futures = {
            ex.submit(_run_one, st, args.max_sections, args.per_state_timeout): st
            for st in states
        }
        for fut in concurrent.futures.as_completed(futures):
            st = futures[fut]
            try:
                r = fut.result()
            except subprocess.TimeoutExpired:
                r = {"state": st, "elapsed_sec": args.per_state_timeout, "chunks": 0,
                     "errors": 0, "exit": 124, "classifier": "timeout", "err_line": "subprocess timeout"}
            except Exception as e:  # noqa: BLE001
                r = {"state": st, "elapsed_sec": -1, "chunks": 0, "errors": 0,
                     "exit": -1, "classifier": "sweep_error", "err_line": str(e)[:140]}
            results.append(r)
            mark = "OK " if r["chunks"] > 0 else "FAIL"
            print(f"  [{mark}] {r['state']:3s}  chunks={r['chunks']:>3}  "
                  f"err={r['errors']:>2}  {r['elapsed_sec']:>5}s  "
                  f"{r['classifier']:<25} {r['err_line']}")

    results.sort(key=lambda r: (r["classifier"] != "ok", r["state"]))
    ts = _dt.datetime.now().strftime("%Y%m%dT%H%M%S")
    out_csv = ROOT / "data" / "state_chunks" / f"sweep_{ts}.csv"
    with open(out_csv, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(results[0].keys()))
        w.writeheader()
        w.writerows(results)

    n_ok = sum(1 for r in results if r["chunks"] > 0)
    total_chunks = sum(int(r["chunks"]) for r in results)
    print()
    print(f"[sweep] DONE. {n_ok}/{len(results)} states produced chunks. "
          f"Total chunks: {total_chunks}. CSV: {out_csv}")
    print()
    by_class: Dict[str, int] = {}
    for r in results:
        by_class[r["classifier"]] = by_class.get(r["classifier"], 0) + 1
    print("[sweep] By failure class:")
    for k, v in sorted(by_class.items(), key=lambda kv: -kv[1]):
        print(f"  {v:>3}  {k}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
