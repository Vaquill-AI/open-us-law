# Repo Profile — open-us-law

```yaml
platform: codex
governance_skill: none
runbook_path: scripts/state_scrapers/README.md
evaluator_default: custom
evaluator_commands:
  source_integrity: ".venv/bin/python -m unittest discover -s tests/source_integrity -t . -v"
  contract_lint: ".venv/bin/python scripts/sprint/contract_lint.py docs/sprint/sprints/2026-08-08-open-us-law-source-integrity.md"
test_roots:
  - tests/source_integrity
registries: []
venv_setup: "python -m venv .venv && .venv/bin/python -m pip install -r requirements.txt"
gemma_local_spawning: false
notes: |
  The repository ships no pre-existing test root, test runner configuration, or
  CI workflow. This sprint establishes stdlib unittest discovery at the listed
  root; its runtime dependencies are the existing requirements.txt. The
  contract linter is sprint tooling only. The source-release materializer and
  Hugging Face publication workflow are absent from this checkout and must not
  be inferred from the dataset card. Scope is coordinated with LexGraph sprint
  2026-08-04-defs-us-preamble.
```

## Live environment

```yaml
live_environment:
  frontend_base_url: unknown
  backend_base_url: unknown
  health_endpoints: []
  notes: |
    No deployed runtime is in scope. Source acquisition and release publishing
    dependencies must be identified by the Planner rather than assumed.
```
