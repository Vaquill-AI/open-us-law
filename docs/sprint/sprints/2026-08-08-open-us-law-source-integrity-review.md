# Intake findings — source-integrity repair

These findings were produced during LexGraph definition-certification planning
and are inputs for verification, not a completed upstream diagnosis.

- LexGraph consumes the pinned Hugging Face snapshot `301000fc…`.
- Hawaii row `STATE_HI_D2_T24_C431_S431` contains 2,404,155 text bytes and
  multiple statutes; another collapsed `_S490` row contains the genuine
  `Confirmer` definition.
- The Hawaii scraper parses section numbers with
  `r"§\s*([\d][\w\-\.]*)"`, excluding colon-bearing identifiers such as
  `431:15-304`; the published row has truncated number `431`, residual title
  `:15-304…`, and an ID ending `_S431`.
- The inspected repository did not expose the Parquet materializer/release job
  that aggregated colliding source identities into the published row.
- Adjudicated LexGraph false captures split provisionally as: HI 16 and FED 2
  require upstream source/provenance repair; AR 1, ID 2, and TX 12 are
  downstream occurrence-local B1 recognition work.
- A corrected release must include old-to-new identity lineage, source URL and
  heading provenance, exact body bytes, stable ordering, and a producer
  manifest. Duplicate normalized IDs must fail closed rather than merge.
- The director authorized an upstream PR and coordinated cross-repo planning on
  2026-08-08. Actual publication remains unverified.
