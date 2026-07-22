"""CLI entry point: run one state's scraper end-to-end into a JSONL chunk file.

Example:
    python -m vaquill_pipeline.run_state de                       # full scrape
    python -m vaquill_pipeline.run_state de --max-sections 5      # smoke: stop after 5 sections

Two-stage pipeline:
    1. THIS script runs on a US-reachable host. Produces
         data/state_chunks/state_<st>_statutes.jsonl
       (and ``state_<st>_errors.jsonl`` for per-section failures).
    2. scp the JSONL to the Qdrant VM and run, from vaquill-qdrant-python:
         python scripts/us_corpus/embed_and_upsert.py --input <path>
       That handles Voyage (voyage-4-large, 1024d), FastEmbed BM25 sparse,
       and resumable upserts into the statutes_us collection.
"""
from __future__ import annotations

import argparse
import importlib
import sys
import traceback
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


STATES_DIR = ROOT / "src" / "scrapers" / "us" / "states"


def all_states_with_scraper():
    """Discover every state code that has a scrapeXX.py file."""
    out = {}
    for st_dir in sorted(STATES_DIR.iterdir()):
        if not st_dir.is_dir():
            continue
        st = st_dir.name
        cand = st_dir / "statutes" / f"scrape{st.upper()}.py"
        if cand.exists():
            out[st] = f"src.scrapers.us.states.{st}.statutes.scrape{st.upper()}"
    return out


WORKING_STATES = all_states_with_scraper()


def main() -> int:
    parser = argparse.ArgumentParser(description="Run one state's statutes scraper into JSONL.")
    parser.add_argument("state", choices=sorted(WORKING_STATES))
    parser.add_argument("--output", type=Path, help="Override JSONL output path")
    parser.add_argument("--max-sections", type=int, default=0,
                        help="Stop after this many content nodes (0=full scrape)")
    args = parser.parse_args()

    from dotenv import load_dotenv
    load_dotenv()

    from vaquill_pipeline import patch

    sink = patch.install(state_code=args.state, output_path=args.output,
                         max_content_nodes=args.max_sections)
    print(f"[vaquill] starting state={args.state} max_sections={args.max_sections or 'unlimited'}")

    module_path = WORKING_STATES[args.state]
    try:
        mod = importlib.import_module(module_path)
        # Legacy upstream scrapers reference bare ``insert_node`` /
        # ``insert_node_ignore_duplicate`` etc. without importing them. Inject
        # the tuple-aware compat shim into their globals so the calls resolve.
        from vaquill_pipeline import legacy_compat
        legacy_compat.inject_into(mod)
        if hasattr(mod, "main"):
            mod.main()
        elif hasattr(mod, "scrape"):
            mod.scrape(None)
        else:
            print(f"[vaquill] {module_path} has neither main() nor scrape()", file=sys.stderr)
            return 2
    except patch._StopAfterMax as e:
        print(f"[vaquill] stopped early: {e}")
    except Exception as e:  # noqa: BLE001
        print(f"[vaquill] scraper raised: {type(e).__name__}: {e}", file=sys.stderr)
        traceback.print_exc()
        patch.shutdown()
        return 3

    patch.shutdown()
    print(f"[vaquill] output: {sink.path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
