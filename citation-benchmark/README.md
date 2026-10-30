# US Citation Resolution Benchmark

35,466 labelled legal citation strings, each with the outcome a citation resolver should produce.
A lawyer writes `N.Y. Penal Law § 125.25 (McKinney 2023)`, `42 USC 1983` or `Cal. Rules of Court, rule 8.204`.
The resolver must return the one right provision, or must refuse (or say it is answering a container or a range) when the citation does not name one section.

DRAFT. Dated 2026-10-30. Not yet published.

## Read this first: we tune against this set

Vaquill built this set to measure and improve its own citation resolver, and the resolver has been tuned against it for months.
Every failure the set exposed was a bug we then fixed.
A score on this set from our resolver is therefore NOT a held-out result and says little about performance on unseen citations.
For an unbiased number, use a resolver that has not seen these rows, or hold out an area, and report which.
We publish the set so that others can measure their resolvers on the same strings, not to claim a benchmark score.
Our own measurements of unseen citations are on a different, court-written set (see the `citation-census` folder).

## Layout

```
data/<area>.jsonl.gz      one JSON object per line, 7 files
samples/sample_200.jsonl  200 random rows (plain text), to look at before downloading
stats/                    counts by area, scope group, outcome, state and form family; summary.json
scripts/verify.py         recomputes every checksum and re-checks the rows (standard library only)
MANIFEST.json  SHA256SUMS every file with size, row count and SHA-256
```

Check a download with `python3 scripts/verify.py`.

## Rows

| field | meaning |
| --- | --- |
| `id` | row id, `<area>-<n>` |
| `area` | research pass that produced the row: `statutes_am`, `statutes_nw`, `constitutions`, `regulations`, `court_rules`, `federal_sessionlaws`, `synthetic_forms` |
| `jurisdiction_label` | free-text jurisdiction label from the research pass (a state name, `Federal`, or a descriptive group). Use `state` for filtering |
| `state` | two-letter code, `federal` for U.S.-level law, or null when the row names no single jurisdiction |
| `corpus` | `statute`, `regulation`, `constitution`, `court_rule`, `federal_rule`, `federal_code`, `federal_reg`, `session_law`, `admin_register`, `guidance`, `other` |
| `scope_group` | `statutes` (statute, federal_code, session_law), `constitutions`, `regulations` (regulation, federal_reg, admin_register), `rules` (court_rule, federal_rule), `other` (guidance, other) |
| `citation` | the citation string, exactly as a lawyer would write it. Publisher parentheticals such as `(LexisNexis 2023)` are part of the string on purpose |
| `form_family` | slug for the citation FORM the row exercises (`ny_consolidated_abbrev`, `usc_nodots_symbol`). Some slugs name a commercial citation style; they describe a citation format and link nowhere |
| `outcome` | `resolve`, `refuse`, `flag_or_refuse` or `session_law`. See the protocol |
| `target` | for `resolve` rows, the one correct provision: `state`, `corpus_type`, `section_number`, `section_title`, `canonical_citation` (how the provision is printed in our corpus) and `status` (`in_force`, `repealed`, ...). Null otherwise |
| `acceptable_alternatives` | other provisions that also count as correct (same shape as `target`): a twin in a second code, or a rule also enacted as a statute |
| `refusal_reason` | for `refuse` and `flag_or_refuse` rows: `not_held`, `ambiguous`, `out_of_scope`, `container_level`, `multi_section` |
| `match_level` | on a `resolve` row: the answer must also say it is answering a `container` (the FIRST section of the cited chapter, article or amendment) or `multiple` (the first of a range or list) |
| `repair_kind` | on a `resolve` row: `former_law`. The citation names a repealed number; the answer must be the successor AND say it was repaired |
| `expected_subsection` | expected pinpoint, dotted (`f.1`) |
| `expected_subsection_path` | expected pinpoint as printed labels, outermost first (`["f","1.5"]`). An empty list means the citation carries no pinpoint and the answer must carry none |
| `scope_state_required` | a state the caller must pass with the citation (a bare `CR 56` needs one) |
| `names_no_jurisdiction` | true when the string names no jurisdiction at all |
| `attestation_kind` | how the form was attested: `official_page` (a page on a government site), `other_page` (a non-official page; the link is not published), `bluebook`, `mechanical_variant` (generated from another row), `research_note` |

Internal section identifiers and every source link are deliberately not published.
To find a target in a corpus, match on `state` + `corpus_type` + `section_number` (and `canonical_citation` to break ties).

## Scoring protocol

Run your resolver on `citation` (passing `scope_state_required` as the state when present).
Score each row, then report per `scope_group` and per `outcome`.

Positives, `outcome = resolve`:
- correct: the answer is the `target`, or one of `acceptable_alternatives`, or a provision whose printed citation equals the target's `canonical_citation` in the same state (an alias).
  If `match_level` is set the answer must also carry that level. If `repair_kind` is set it must carry that repair. The right provision without them is WRONG.
- wrong: any other provision answered.
- none: no answer. A miss, but not a wrong answer.
- Report `correct_rate = correct / rows` and `wrong_rate = wrong / rows` separately. Never fold `none` into either.

Negatives, refuse over guess:
- `refuse` (`not_held`, `ambiguous`, `out_of_scope`): the provision is not held, or the string does not pick one provision. Correct is no answer. Any section answered is WRONG.
- `flag_or_refuse` (`container_level`, `multi_section`): the string names a chapter, article, title or a plural. Correct is no answer, or an answer explicitly flagged as a container or multiple. An unflagged single section is WRONG.
- Report the negative wrong rate (a guess where a refusal was right). A resolver that answers everything scores well on positives and badly here.

State-scope rows: rows with `scope_state_required` (shared acronyms such as `CR 56`) are scored with the state supplied.
Rows with `names_no_jurisdiction` and no scope state test that the resolver refuses or asks rather than choosing a state.

Pinpoints: when `expected_subsection` or `expected_subsection_path` is present, score the pinpoint on its own report line.
It never changes the row's outcome. Pinpoint forms in `synthetic_forms` assert the LAW only.

`session_law` rows (922) expect a hit in a session-law registry, not a section. The registry identifier is withheld, so these rows are not scoreable from this folder alone; exclude them from headline rates.

Statutes versus rules: the headline scope is `scope_group = statutes`.
`rules` (court rules), `regulations`, `constitutions` and `other` are included for completeness and are scored the same way. Counts are below.

## Counts

Total 35,466 rows.

| scope_group | rows |
| --- | ---: |
| statutes | 15,378 |
| rules | 12,426 |
| regulations | 3,828 |
| constitutions | 3,361 |
| other | 473 |

| outcome | rows |
| --- | ---: |
| resolve | 32,400 |
| flag_or_refuse | 1,264 |
| refuse | 880 |
| session_law | 922 |

| area | rows | note |
| --- | ---: | --- |
| statutes_am | 2,958 | researched |
| statutes_nw | 5,064 | researched |
| constitutions | 1,768 | researched |
| regulations | 2,668 | researched |
| court_rules | 11,361 | researched |
| federal_sessionlaws | 5,364 | researched (federal code, rules, registers, session laws) |
| synthetic_forms | 6,283 | generated typed variants of real sections |

Other counts: 3,064 rows need a state scope, 515 name no jurisdiction, 1,314 carry a pinpoint, 178 carry a `match_level`, 70 carry a `repair_kind`, 778 list alternatives.
Full tables are in `stats/`.

## How the rows were made and labelled

Built 2026-10-06 from six research passes after a prospect's list of 1,274 citations came back 53 percent unresolved.
Each pass catalogued how lawyers actually cite a jurisdiction's law, with an attestation per form (a Bluebook rule, a court or legislature page, a published opinion), then expected outcomes were read from the live corpus, never guessed.
`synthetic_forms` was added 2026-10-26: mechanical variants (no periods, no section sign, Unicode dash, `et seq.`, a pinpoint, a range, a chapter) of sections drawn at random from the corpus, stratified by jurisdiction, corpus and form family.

Labels are produced by researchers and tools, not by a panel of independent annotators, and are not double-blind adjudicated.
Expected outcomes were adjudicated in passes on 2026-10-07, 2026-10-08 and 2026-10-22, which promoted negatives to positives when the corpus held the provision, added state scopes to shared acronyms and added acceptable alternatives.

Re-keying, 2026-10-30: 72 expected targets were corrected from corpus evidence (the provision the citation names, read from the corpus, differed from the key).
Targets drift: if many rows turn wrong at once, suspect the corpus before the resolver.
We expect residual label errors. Report them.

## What was left out of this release

Rows whose target is in a third-party snapshot or a state regulation slice we do not redistribute as text are published as citations with corpus metadata only (canonical citation, title, status). No section text is included in this folder.
Rows whose target id no longer exists in the corpus were removed or re-keyed.

## Licence and attribution

The data in this folder (the citations, answer keys and metadata) is licensed under CC BY-NC 4.0, the same as the corpus snapshot. Scripts are Apache-2.0. See the repository `LICENSE` for code.
