#!/usr/bin/env python3
"""Minimal sprint-contract lint for planner-owned test and handoff metadata.

This is intentionally repository-local sprint tooling, not a release or
scraper component.  It avoids a YAML dependency because the contract front
matter is a small, fixed subset.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path


REQUIRED = {
    "id",
    "status",
    "current_role",
    "branch",
    "locked_by",
    "locked_at",
    "evaluator",
    "evaluator_command",
    "qa_cycles",
}

VALID_STATUSES = {
    "planning",
    "planned",
    "in-progress",
    "dev-complete",
    "qa-in-progress",
    "qa-fail",
    "review",
    "done",
    "blocked",
}

VALID_CURRENT_ROLES = {"planner", "developer", "qa"}


def front_matter(path: Path) -> dict[str, str]:
    text = path.read_text(encoding="utf-8")
    match = re.match(r"^---\n(.*?)\n---\n", text, re.DOTALL)
    if not match:
        raise ValueError("missing YAML front matter")
    fields: dict[str, str] = {}
    for line in match.group(1).splitlines():
        if match := re.match(r"^([a-z_]+):\s*(.*)$", line):
            fields[match.group(1)] = match.group(2).strip().strip('"')
    return fields


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("usage: contract_lint.py <sprint-contract.md>", file=sys.stderr)
        return 2
    try:
        fields = front_matter(Path(argv[1]))
    except (OSError, ValueError) as error:
        print(f"CONTRACT LINT FAIL: {error}", file=sys.stderr)
        return 1
    missing = sorted(REQUIRED - fields.keys())
    errors: list[str] = []
    if missing:
        errors.append(f"missing fields: {', '.join(missing)}")
    if fields.get("status") not in VALID_STATUSES:
        errors.append("status is not a recognized sprint state")
    if fields.get("current_role") not in VALID_CURRENT_ROLES:
        errors.append("current_role is not a recognized sprint role")
    try:
        qa_cycles = int(fields.get("qa_cycles", ""))
    except ValueError:
        errors.append("qa_cycles must be an integer from 0 through 5")
    else:
        if not 0 <= qa_cycles <= 5:
            errors.append("qa_cycles must be an integer from 0 through 5")
    if errors:
        print("CONTRACT LINT FAIL: " + "; ".join(errors), file=sys.stderr)
        return 1
    print("CONTRACT LINT PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
