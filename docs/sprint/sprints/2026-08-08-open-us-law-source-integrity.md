---
id: "2026-08-08-open-us-law-source-integrity"
status: planning
current_role: planner
branch: sprint/2026-08-08-open-us-law-source-integrity
locked_by: "codex:planner"
locked_at: "2026-08-08T19:06:14Z"
last_agent: "codex:manager"
last_updated: "2026-08-08T19:06:14Z"
lint: null
evaluator: custom
evaluator_command: null
total_items: 0
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

Pre-Planner state — define bounded items and RED acceptance tests from the
review artifact.

## Dev Complete

None.

## Completed

None.

## Evaluation Notes

None — evaluator discovery is Planner-owned.

## QA Notes

None.

## Context Dump

- Determine the exact Hawaii scraper identity defect and its safe repair.
- Locate or explicitly bound the unavailable post-scrape publishing pipeline.
- Separate upstream HI/FED data work from LexGraph AR/ID/TX B1 work.
- Preserve source bytes, ordering, identity lineage, and reproducible manifests.
- Escalate if a corrected release cannot be produced from repository-owned code.
