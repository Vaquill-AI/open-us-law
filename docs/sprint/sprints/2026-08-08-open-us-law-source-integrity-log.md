# 2026-08-08 open-us-law source-integrity sprint log

Append-only evidence and role roster for the cross-repo repair.

## Roster

- Canonical task: `/root/open_us_law_source_planner`; role: Planner; model:
  `gpt-5.6-terra`; effort: `high`.
- Haiku was considered and rejected: this sprint requires source-identity,
  release-architecture, cross-repository contract, and RED-test judgment; it
  is not bounded mechanical discovery or fixture repair.

## Planner evidence — 2026-08-08

- Checkout verified clean at required base `00ae746b2b650f39b93dfdfdef7a82a28b4f06c0`
  before Planner writes, on `sprint/2026-08-08-open-us-law-source-integrity`.
  `origin` is `Vaquill-AI/open-us-law`; only `fork` is the authorized push
  remote (`vicciz-ceo/open-us-law`).
- HF API/git inspection: pinned snapshot
  `301000fc3465374ee0f23c3c6953a8a861e95cad` is **Open US Law v2026.07**
  (2026-08-02); later HF commits are card-only. Its tree has no release code,
  workflow, producer manifest, source capture, or credential metadata.
- Selective HF artifact read: `us_hi_statutes.parquet`, SHA-256
  `169c2bf075fb8681b92ce4d950444536be3087bc0250e7d4edead0948341222c`,
  10,452,322 bytes / 16,446 rows. The malformed `..._C431_S431` row points at
  `HRS_0431-0015-0304.htm` but stores section `431` and a `:15-304` residual;
  `..._C490_S490` likewise stores a `:9-342` residual. Temporary inspection
  artifact was removed after SHA and row verification.
- Test roots discovered: no pre-existing roots or CI configuration; Planner
  added only `tests/source_integrity` and `scripts/sprint/contract_lint.py`.
  Stale-pin reconciliation over every discovered root found two intentional,
  matching references to `301000fc3465374ee0f23c3c6953a8a861e95cad` (fixture
  provenance and its explanatory comment), and zero stale/mismatched pins.
- RED baseline: `.venv/bin/python -m unittest discover -s tests/source_integrity
  -t . -v` => `Ran 13 tests`; `FAILED (failures=3, errors=8, skipped=1)`.
  The three failures prove HI truncation at parser and emitted-Node seams; the
  eight errors prove the absent release/typed-USC components; the hyphenated-ID
  preservation control is GREEN. `LIVE_SOURCE_INTEGRITY=1` is deliberately
  opt-in and targets the pinned public GovInfo section. Contract lint PASS.

## Planner RED-gate correction — 2026-08-08

- Replaced all new-module `ModuleNotFoundError` outcomes with `TestCase.fail`
  assertions that retain their behavioral assertions after the module exists.
  No collection or test errors remain.
- Added source-faithful `release_rows.jsonl` and an actual subprocess contract
  for `python -m scripts.release.materialize_snapshot`: it requires local
  Parquet/manifest/lineage/quarantine outputs, content hashes, deterministic
  rerun bytes, collision fatal/quarantine behavior, and `--dry-run` with an
  unreachable HF endpoint (therefore no upload may be attempted).
- Added direct existing-call-site tests for `download_usc.html_to_text` and
  `parse_usc_zip.html_to_text`; both now prove the present generic extractor
  captures `Editorial Notes`, so a helper-only repair cannot satisfy the gate.
- Final RED census: `Ran 16 tests`; `FAILED (failures=14, skipped=1)`, **zero
  errors**. File census: HI 3 RED/1 GREEN; release 7 RED; USC 4 RED/1 skipped
  live test. Test root/evaluator unchanged: `tests/source_integrity` / stdlib
  unittest. Contract lint PASS after the role handoff.
- Stale-pin sweep repeated across the only test root: two intentional matching
  snapshot references and zero stale/mismatched pins.

## Planner Parquet and dry-run target correction — 2026-08-08

- Added the Planner-owned test dependency
  `tests/source_integrity/requirements.txt` pinning `pyarrow==24.0.0`; repo
  profile setup now installs it after the existing production requirements.
  Production `requirements.txt` remains untouched.
- The existing CLI RED now opens the emitted file through PyArrow, requires the
  published 24-column Parquet schema, two distinct `431:15-*` rows, exact text
  round-trip, deterministic file bytes/hash, and an empty Parquet artifact for
  quarantined duplicate IDs. Arbitrary bytes with a `.parquet` suffix cannot
  satisfy it.
- The same CLI RED passes test-only `--hf-repo-id` and `--hf-revision` values;
  it requires the exact values and `upload_performed: false` in the manifest
  while `HF_ENDPOINT` is unreachable. Thus a dry run must not upload. The real
  HF target decision remains external.
- Count remains `Ran 16 tests`; `FAILED (failures=14, skipped=1)`, zero errors.
