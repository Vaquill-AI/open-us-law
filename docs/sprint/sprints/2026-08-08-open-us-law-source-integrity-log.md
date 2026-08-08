# 2026-08-08 open-us-law source-integrity sprint log

Append-only evidence and role roster for the cross-repo repair.

## QA cycle 2 — 2026-08-08

- `/root/open_us_law_qa2` — QA — `gpt-5.6-terra`, high; Haiku was considered
  and rejected because full artifact/identity regression adjudication and
  pinned-source compatibility require high QA judgment.
- Started clean at local/fork `7394ba5a1c06894aadde166fded55d2d148b891e` on
  the required sprint branch. Confirmed QA RED `6ed1900` and repair `351b824`
  in history; the repair is one production grammar-line change allowing dotted
  structural tokens before the terminal `_S{section}` capture.
- Baseline evaluator: 25 run, 24 pass, 1 opt-in live skip, no failures/errors.
  SI-2 pinned `STATE_AK_T11_C11.76_S11.76.115` / `11.76.115` passes. Numeric,
  hyphen, colon, and dotted compatibility; provenance; and duplicate/error and
  quarantine collision behavior remain green.
- Fresh offline dry-run under an unreachable HF endpoint produced real 2-row
  Parquet (`81062957e6accf6b0caefe6650ee4731c75fd1e4584bfe5ea899a147a8c969e5`),
  manifest, two-line lineage, and empty quarantine. Manifest records
  `upload_performed: false`; no credentials or upload were used.
- Added QA-only regression rejecting a mismatched terminal section despite the
  dotted Alaska structural prefix. Final evaluator: 26 run, 25 pass, 1 opt-in
  live skip, no failures/errors. Contract lint PASS. SI-2 is Completed;
  sprint state is `review` / `planner`, `qa_cycles: 2`; lock fields unchanged.

## QA cycle 1 — 2026-08-08

- `/root/open_us_law_final_qa` — QA — `gpt-5.6-terra`, high; Haiku rejected
  because full cross-repo source/provenance, artifact, live-path, and
  regression adjudication requires high QA judgment.
- Start verified clean on the required sprint branch at local/fork
  `efe1d68d08a8ecd7fcf805754b68f58fc3d9efbd`; required test root existed.
  Baseline authoritative evaluator: 21 run, 20 pass, 1 opt-in skip, 0 errors.
- SI-1 PASS: fixture emitted the full colon ID/citation/title/body/URL; direct
  HRS request was Cloudflare-403 and therefore not guessed. SI-3 PASS: both
  USC call sites trace to the typed extractor; live GovInfo check passed.
- SI-4 PASS: two fresh offline dry runs produced byte-identical four-file
  artifacts, exact 24-column Parquet, target/revision binding and
  `upload_performed: false`; materializer contains no HF client/credential or
  upload path and refuses non-dry-run. Production pin is `pyarrow==24.0.0`.
- SI-2 FAIL: selective ranged reads of pinned HF revision
  `301000fc3465374ee0f23c3c6953a8a861e95cad` found valid numeric PA,
  hyphenated ID, dotted AK, and fixture-backed colon HI identities. The
  committed QA RED proves the current grammar rejects actual AK
  `STATE_AK_T11_C11.76_S11.76.115` despite matching `11.76.115`; no artifact
  may claim full dataset compatibility until repaired. No HF upload occurred.

## Developer roster — 2026-08-08

- `/root/open_us_law_dev_hi` — Developer SI-1 — `gpt-5.6-terra`, low;
  Haiku-eligible but unavailable, so this is the cheapest available fit.
- `/root/open_us_law_dev_release` — Developer SI-2/SI-4 —
  `gpt-5.6-terra`, medium; Haiku rejected for artifact/schema/CLI safety work.
- `/root/open_us_law_dev_usc` — Developer SI-3 — `gpt-5.6-terra`, medium;
  Haiku rejected for source-structure parsing and two live call sites.
- `/root/open_us_law_lint_microfix` — Planner-role sprint-tooling fix —
  `gpt-5.6-terra`, low; Haiku-eligible but unavailable.
- `/root/open_us_law_qa1_ak_fix` — QA-fail Developer SI-2 —
  `gpt-5.6-terra`, low; Haiku-eligible but unavailable.

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

## Planner SI-2/SI-4 identity/reproducibility replan — 2026-08-08

- Resumed clean at manager-required/fork SHA
  `32810817e207c2a0b98a9a03b5f6e7918b30487e`; held release commit
  `1e6b07b` was inspected and exercised only in a disposable overlay, never
  merged. SI-1/SI-3 production/tests were frozen.
- Selectively downloaded pinned HF `us_pa_statutes.parquet` only: expected and
  observed SHA-256
  `4b78240c493ce6ddb458203a4276865b6da43c3e195dcbeb53a0519fdaaf29f2`,
  14,547 rows / 8,543,572 bytes. 13,060 published rows have matching plain
  numeric `_S…` and `section_number`; sample `STATE_PA_T3_C15_S1521` / `1521`
  / `3 Pa.C.S. § 1521` points to the official PA legislature. Temporary data
  was removed after verification.
- Replaced the invalid `_S431`-is-malformed assumption with a structured
  mismatch: act-id suffix `431:15-305` versus section/citation `431:15-304`.
  Added separate compatibility controls for plain numeric, hyphen, colon, and
  dotted legal identifiers. Repository mappings in `node_to_payload`, PA, ID,
  HI, and WI source paths ground those cases.
- Explicit ruling: the already-collapsed HI `_S431` row is internally
  indistinguishable from legitimate numeric syntax. It cannot be safely split
  or rejected without the trusted external HRS capture/heading evidence;
  corrected source acquisition remains mandatory.
- Held diff audit found floating `pyarrow>=24.0`, no materializer/runtime
  version in the manifest, and no binding for its otherwise explicit Parquet
  write controls. Tests now require `pyarrow==24.0.0` plus manifest-bound
  materializer name/version, exact Python/PyArrow versions, compression,
  dictionary/statistics flags, Parquet/data-page versions, and row-group size.
- Current branch: 21 run / 12 failures / 1 skipped / **0 errors**; the 8
  integrated SI-1/SI-3 non-live tests are GREEN. Held implementation overlay:
  21 run / 5 failures / 1 skipped / **0 errors**; 15 GREEN. The five held REDs
  are numeric compatibility, structured mismatch, API manifest runtime,
  production exact pin, and CLI manifest runtime binding.
- Stale-pin sweep over the sole test root: three intentional references to the
  same full HF snapshot (HI fixture/provenance plus PA evidence), zero stale or
  mismatched pins. Contract lint PASS; `dev_complete_items: 2`, `qa_cycles: 0`,
  and manager lock fields preserved.
