# Statute subsection pinpoint benchmark

A small answer key for one question: given the text of a US state statute section, can a system list its subdivisions (`(a)`, `(1)`, `Subd. 2`, and so on) as nested paths with character offsets?
Five states, 120 sections each, 600 sections and 6,044 labelled nodes.

THE LABELS WERE PRODUCED BY AI (Claude Sonnet).
No human labelled, reviewed or adjudicated any section.
The Vaquill structure engine whose output this benchmark scores WAS TUNED AGAINST THESE SAME SECTIONS.
Read "Honest limits" before quoting any number.

## Contents

| Path | What |
| --- | --- |
| `data/pinpoint_<st>.jsonl.gz` | One row per section: `id`, `state`, `stratum`, `truth_source`, `scoring_excluded` (false on every row), `text_chars`, `outline`, `text` |
| `data_outline_only/pinpoint_<st>.jsonl.gz` | Same rows without `text` (offsets and heads only) |
| `score.py` | Scorer you run against your own outline output |
| `verify.py`, `SHA256SUMS.json`, `COUNTS.json` | Integrity and schema checks |
| `LICENSE-DATA.md` | Licence proposal and per-state source outcome |
| `tools/` | Maintainer scripts (`build.py`, `checksums.py`); `tools/stats.json` holds the agreement counters |

An `outline` item is `{"path": "1/a/2", "start": 283, "end": 425, "head": "..."}`.
`path` is the printed labels outermost to innermost joined by `/`, brackets, periods and words like `Subd.` stripped, case kept (`(1m)` is `1m`).
`start` is the offset of the node's printed label in `text`; `end` is the start of the next node that is not a descendant, or the end of the body before a trailing History/Source line.
`head` is the first words of the node's own text after its label, copied from the section.
Offsets are Python string indices (code points) into `text`, and they are derived mechanically from the labellers' heads (labellers wrote path and head only, not offsets).
The scorer uses paths only; offsets are provided for pinpoint-extraction use.
Ids are `<STATE>:<section number>` (for example `MN:62Q.481`). The `text` is the section as served, with the heading line included.

Counts per state (all five have the same design: 100 `units` = sections that print subdivisions, 12 `flat` = none printed, 8 `long` = over 7,000 characters, all under 25,000):

| State | Sections | Nodes |
| --- | --- | --- |
| MN | 120 | 1,555 |
| IA | 120 | 1,206 |
| MO | 120 | 995 |
| WI | 120 | 1,223 |
| MI | 120 | 1,065 |

Sections were drawn at random (fixed seed) from each state's current statute collection, scanning at most 900 candidates per state, so they are a random sample of the first sections that parsed, not a stratified sample of the whole code.

## Method

1. Blind labeller A (Claude Sonnet) labelled each section BY EYE from its plain text alone, following `briefs/LABELLER_BRIEF.md`. It never saw the engine output and used no script.
   Sections whose first-pass labels had come from a script were redone by eye (`labels/A/mn_redo.json`, 2 sections, both MN: `MN:35.01`, `MN:51A.02`). These two are now scored like every other section.
2. Comparison: A's path sequence was compared with the engine outline recorded at sampling time. Any non-identical sequence is a disagreement.
3. Blind labeller B (a separate Claude Sonnet run, `briefs/LABELLER_BRIEF_B.md`, which adds rules for repealed or renumbered stubs and inline enumerations) labelled ONLY the disagreeing sections, again from text alone.
4. Adjudication: for MN, a further pass (`labels/B/mn_adj.json`, 2 sections: `MN:302A.821`, `MN:507.50`) re-labelled two sections whose B labels contained repealed or renumbered stub subdivisions.
5. Truth per section, in order of precedence: adjudication (`mn_adj`), then A's by-eye redo (`mn_redo`), then B's label where B labelled the section, then A's label. In 9 sections B differs from A. 8 of those 9 take B by rule with no tie-break (3 MN, 5 IA); 1 was adjudicated (MN). The other adjudicated MN section agrees with A after adjudication.

### Agreement numbers (exact path-sequence equality)

"A = engine" is labeller A against the engine output recorded when the sample was drawn, before any tuning on these sections.
Because B only saw disagreements, there is no full A-versus-B agreement over all 600 sections.

| State | Sections | A = engine | Disagreements | B labelled | B = A | B = engine | B differs from A | Final truth = engine |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| MN | 120 | 106 (88.3%) | 14 | 14 | 10 | 2 | 4 (1 adjudicated) | 108 (90.0%) |
| IA | 120 | 110 (91.7%) | 10 | 10 | 5 | 4 | 5 | 114 (95.0%) |
| MO | 120 | 118 (98.3%) | 2 | 2 | 2 | 0 | 0 | 118 (98.3%) |
| WI | 120 | 115 (95.8%) | 5 | 5 | 5 | 0 | 0 | 115 (95.8%) |
| MI | 120 | 120 (100%) | 0 | 0 | - | - | - | 120 (100%) |
| All | 600 | 569 (94.8%) | 31 | 31 | 22 | 6 | 9 | 575 (95.8%) |

In MN, 2 of the 14 B labels agree with neither A nor the engine; in IA, 1 of 10.

### Labeller conventions you should know when scoring

- A fused label such as `(3) (a)` printed at the start of one paragraph yields a PARENT node `3` and a child `3/a`; the parent starts where the fused label starts and shares its head position with the child.
- A stub subdivision (a label followed by nothing but a note such as "Repealed" or "Renumbered") counts as a node in B's pass and in the final truth where B labelled the section. A's brief did not state that rule, so A's first pass may omit such stubs. A repeated label is listed twice, so a few truths contain a duplicated path (for example MN repealed-subdivision runs). `MN:35.01` and `MN:51A.02` were previously excluded from scoring; they were relabelled by eye and are now scored (the `scoring_excluded` field is kept for schema compatibility and is false everywhere).
- Inline run-in lists count when each item starts with its own label (`(1) one thing, (2) another`), at any depth. This is the most heuristic convention: where a long sentence embeds `(1)`/`(2)` items, labellers and engines disagree most. Treat those nodes as capped-confidence.
- Not nodes: the section number and title, trailing History/Source notes, cross-references in running prose (`under sub. (2)(a)`), labels that only occur inside quoted rules.
- The Iowa file `labels/A` carried a stale title code for one section (`654A.7`); it was matched to the sample by section number.

## Scoring your outliner

```
python score.py --data data --pred my_predictions.json
python score.py --data data_outline_only --pred my_predictions.json   # same answer key, no text shipped
python score.py --data data --self-test                                # sanity: truth against itself is 100%
```

`my_predictions.json` is `{"MN:62Q.481": ["1", "1/a", "1/a/1", ...], ...}` (strings or `{"path": ...}` objects), one entry per section you outline.
Missing sections count as empty outlines.
Reported per state, per state and stratum, and overall: `exact` (whole sequence equal, order and duplicates matter) and micro node precision and recall (multiset of paths).
Standard library only.

## Verification

```
python verify.py
```
checks every SHA-256 in `SHA256SUMS.json`, row and node counts, unique ids, that no engine-derived field is present, that offsets are inside the text and non-decreasing, that the first three words of each head sit inside its own span (heads are labeller-typed and in about three nodes of 6,044 the tail of a head deviates from the text by a dropped or added word), and that the scorer self-test passes on both variants.

## Honest limits

- The labels are AI output. Both labellers are the same model family, so their errors are correlated, and B only saw sections where A and the engine already differed. A section where A and the engine are both wrong in the same way was never re-examined. No human reviewed any label.
- The engine was TUNED AGAINST THESE SECTIONS after the comparison, using the disagreements. Its score on this benchmark is therefore optimistic and is a regression check, not an out-of-sample accuracy estimate. Re-run on this engine's current build (a tuned build) against the final truth, all 600 sections: exact 97.5% overall (585/600; MN 95.0%, IA 95.0%, MO 99.2%, WI 98.3%, MI 100%). The recorded sampling-time engine scores 95.8% exact (575/600). Both figures are on sections the engine was tuned on, so neither is a held-out figure.
- 120 sections per state is enough to see gross failures, not to rank two systems that differ by a point or two. A 95% interval on a 120-section exact rate near 95% is about plus or minus 4 points.
- Offsets are derived by locating the labellers' heads in the text, not hand-marked.
- Statutes change; the text is a snapshot of the sections as served on the sampling date.
- Only five states, only the statutes collection. No claim is made about other states or about regulations.

## Licence and sources

See `LICENSE-DATA.md`. The statutory text is US state law published by each legislature's own site; the answer key and compilation are released under CC BY-NC 4.0, the scorer under Apache-2.0. Nothing here is legal advice.
