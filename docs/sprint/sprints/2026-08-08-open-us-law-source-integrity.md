---
id: "2026-08-08-open-us-law-source-integrity"
status: planned
current_role: developer
branch: sprint/2026-08-08-open-us-law-source-integrity
locked_by: "codex:developer"
locked_at: "2026-08-08T19:19:13Z"
last_agent: "codex:planner"
last_updated: "2026-08-08T19:19:13Z"
lint: PASS
evaluator: unittest
evaluator_command: ".venv/bin/python -m unittest discover -s tests/source_integrity -t . -v"
total_items: 4
completed_items: 0
dev_complete_items: 0
qa_cycles: 0
prd_sections:
  - docs/sprint/sprints/2026-08-08-open-us-law-source-integrity-review.md
design_sections: []
---

# Source-integrity repair for LexGraph definition certification

This upstream sprint is coordinated with LexGraph sprint
`2026-08-04-defs-us-preamble`. The repositories retain separate contracts,
branches, locks, tests, Developers, and QA verdicts.

## Manager rulings

- The director authorized work in `Vaquill-AI/open-us-law` on 2026-08-08.
- Upstream source identity and release integrity belong here; LexGraph retains
  its downstream B1 recognition repair.
- The cross-repo boundary remains provisional until both planning reports land.
- No Developer starts until trusted source inputs and the publishing seam are
  identified and RED tests exist.

## Next Steps

Developer handoff is ready. The Planner-owned test/fixture estate is frozen;
run the contract lint and complete source-integrity suite before production
edits, and keep the release dry-run offline until trusted HI input and
publication credentials are supplied.

## Dev Complete

None.

## Completed

None.

## Evaluation Notes

Planner RED census (zero errors): `.venv/bin/python -m unittest discover -s
tests/source_integrity -t . -v` runs 16 tests: 14 failures, 1 preservation
green, 1 opt-in live GovInfo skip. Expected RED by file: `test_hi_identity.py`
has 3 parser/emitted-Node failures and 1 hyphenated-ID GREEN;
`test_release_contract.py` has 7 failures (6 API contracts plus the actual CLI
materializer/dry-run/collision integration path). The CLI integration opens the
emitted artifact with the Planner-pinned PyArrow verifier and requires the
published 24-column schema, two distinct corrected HI IDs, exact statutory
body text, deterministic bytes/hash, and no collision-body merge. It also
requires explicit test-only HF repository/revision inputs bound to the manifest
with `upload_performed: false`; `test_usc_source_segments.py`
has 4 failures (2 helper contracts and both existing `download_usc` and
`parse_usc_zip` call seams) plus the opt-in live source test. Missing planned
modules are converted into assertion failures which continue into behavior
assertions once implemented; no existence-only test is used.

## QA Notes

None.

## Context Dump

- Determine the exact Hawaii scraper identity defect and its safe repair.
- Locate or explicitly bound the unavailable post-scrape publishing pipeline.
- Separate upstream HI/FED data work from LexGraph AR/ID/TX B1 work.
- Preserve source bytes, ordering, identity lineage, and reproducible manifests.
- Escalate if a corrected release cannot be produced from repository-owned code.

## Planner evidence and verified boundary

- The authority consumed by LexGraph is Hugging Face dataset revision
  `301000fc3465374ee0f23c3c6953a8a861e95cad`, titled **Open US Law v2026.07**
  (2026-08-02). Its repository tree has only `.gitattributes`, `README.md`,
  `SHA256SUMS.json`, artwork, and 105 Parquet files. Commit history after the
  snapshot changes card text only. It contains no producer manifest, release
  job, credential reference, materializer, or code revision binding.
- Selective, checksum-verified inspection of
  `us_hi_statutes.parquet` (`169c2bf075fb8681b92ce4d950444536be3087bc0250e7d4edead0948341222c`,
  16,446 rows, 10,452,322 bytes) proves the published row
  `STATE_HI_D2_T24_C431_S431` has section number `431`, citation `Haw. Rev.
  Stat. § 431`, heading prefix `:15-304`, and the official HRS URL ending
  `HRS_0431-0015-0304.htm`. This directly matches the scraper's current
  `r"§\\s*([\\d][\\w\\-\\.]*)"` extraction: `:` is excluded, so all sections
  beginning `431:` normalize to the same raw identity before they reach the
  append-only JSONL sink. The published 2,404,155-byte row therefore requires a
  later aggregation/materialization process that is absent from this repo.
- The same artifact contains `STATE_HI_D2_T27_C490_S490` with a genuine
  `490:9-342` heading truncated to `:9-342`, corroborating that this is a
  systematic source-identity failure, not a downstream definition-recognition
  issue. The earliest repair seam is the HI heading parser; the safe second seam
  is the missing materializer, which must reject or quarantine duplicate
  normalized identities instead of merging bodies.
- USC is upstream: `download_usc.py` and `parse_usc_zip.py` presently call the
  generic `html_to_text`, which includes every visible GovInfo segment. A
  trusted GovInfo 2024 Title 1 section 1 document delimits law with
  `field-start:statute`/`field-end:statute` and editorial material with
  `field-start:notes`; source structure, rather than a prose heuristic, can
  faithfully exclude notes. This validates the FED 2 upstream classification.
- Provisional cross-repo split is verified: HI 16 and FED 2 are source-owned
  here; AR 1, ID 2, and TX 12 are LexGraph occurrence-local B1 work. LexGraph
  consumes the new release seam only: a versioned Parquet set accompanied by
  `producer-manifest.json`, `identity-lineage.jsonl`, and optional
  `quarantine.jsonl`. Final cross-repo adjudication remains manager/LexGraph
  Planner-owned.

## Developer items and acceptance criteria

1. **SI-1 — Hawaii canonical identities.** Update only the HI section-heading
   parsing path so colons are preserved consistently in number, heading strip,
   citation, and emitted node ID. Distinct `431:15-304` and `431:15-305` must
   remain distinct; existing hyphenated IDs must remain byte/output compatible.
2. **SI-2 — Deterministic release materializer.** Add a repository-owned,
   offline JSONL-row-to-Parquet release seam that sorts deterministically,
   validates source URL/heading/citation/body, fails closed by default on a
   normalized identity collision, and supports explicit quarantine without body
   merging. It must write a versioned manifest binding producer/code revision,
   input hashes, schema, output hashes/counts, collisions/quarantine, and
   old-to-new identity lineage. A well-formed row's statutory body bytes must
   pass through unchanged.
3. **SI-3 — Typed USC statutory body.** Add a source-segment extractor and
   route both USC paths through it. It must consume only GovInfo's typed statute
   segment, preserve that segment's text, reject a missing/malformed statutory
   segment, and never infer the boundary from editorial prose.
4. **SI-4 — Publishable dry-run, not an implied release.** Require explicit
   offline `--hf-repo-id` and `--hf-revision`/tag inputs, bind them into the
   deterministic manifest, and record `upload_performed: false` for `--dry-run`.
   A real upload requires an external `HF_TOKEN` with dataset-write scope and a
   trusted, complete HI input capture; it must neither overwrite `301000fc…`
   nor claim a corrected snapshot before checksums and manifest are built.

## Exact Developer write set, commands, and stop rules

Write only:

- `scripts/state_scrapers/src/scrapers/us/states/hi/statutes/scrapeHI.py`
- new `scripts/release/__init__.py` and `scripts/release/materialize_snapshot.py`
- new `scripts/federal/usc_source_segments.py`, plus the minimal call-site
  changes in `scripts/federal/download_usc.py` and
  `scripts/federal/parse_usc_zip.py`
- `requirements.txt` only if a release-only dependency is necessary
- `README.md` and/or `scripts/README.md` only for the release CLI/runbook and
  provenance-publication instructions required by the new production path.

The Planner-owned files under `tests/source_integrity/` (including fixtures) are
frozen: Developer must not edit them, including the test-only pinned
`tests/source_integrity/requirements.txt` Parquet verifier. If the production
materializer itself needs a runtime dependency, the Developer must declare it
independently in `requirements.txt`. Sprint contract bookkeeping follows the
assigned role workflow and is not part of the production write set.

Commands: run the contract lint; run the complete unittest command; then run
the materializer with a checked-in test fixture and `--dry-run`. Run the live
GovInfo path only as `LIVE_SOURCE_INTEGRITY=1 ... unittest ...`; do not run a
full state crawl or publish from a Developer workstation. Stop and escalate if
the available HI source is not byte-identifiable, any normalized collision is
not quarantined/fatal, output hashing is nondeterministic, source provenance is
missing, or publication would require unknown external logic.

## Publication ownership and current dependency

The old Parquet materializer/release job is demonstrably external/absent; its
identity and credentials are not recoverable from this GitHub checkout or the
HF dataset repository. A minimal deterministic materializer belongs here
because it is required to make the source contract auditable. Actual upload is
owned by the dataset maintainer with HF dataset-write credentials. A corrected
release remains blocked until that owner supplies (a) an authenticated or
archived, checksum-identified HI source capture sufficient to re-enumerate the
affected sections and (b) the publication credential/target decision.
