"""Canonical payload builder for the `statutes_us` corpus.

The single place a payload should be constructed. It replaces the ~45 hand-built
dicts the 2026-08 audit found (every ingest path invented its own dict, so field
names drifted: amendment history under 4 names, per-state `issuing_agency*`, two
duplicate URL-key pairs). Builders pass their typed fields here; this:

  * renames deprecated / duplicate keys to their canonical name
    (payload_schema.ALIASES) so `history_notes`/`history_note_raw`/`history_action`
    all collapse to `history`, `r2_txt_url` -> `r2_section_text_url`, etc.;
  * fills the invariant defaults (`jurisdiction`/`country_code` = "US");
  * validates the result against payload_schema and returns the audit alongside
    the payload so the caller (or the ingest chokepoint) can log drift.

It deliberately does NOT drop keys or invent domain values: a key outside the
schema is surfaced (via the returned audit / drift guard), not silently deleted,
and a missing REQUIRED field stays missing so the bug is visible.

Adoption is incremental and per-data-type: each ingest builder swaps its dict
literal for a `build_payload(...)` call. Until a builder adopts it, the drift
guard (app/tests/unit/test_us_corpus_payload_schema.py) still protects the
declared surface.
"""

from __future__ import annotations

import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Any

# Repo root on sys.path BEFORE the `scripts.` import below. Ingest scripts are
# launched by absolute path (that is how the worker runs them), so a sibling
# that imports this module inherits a sys.path with no repo root and the import
# raises ModuleNotFoundError. Pinned by test_corpus_script_bootstrap.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from scripts.lib import payload_schema  # noqa: E402

_DEFAULTS: dict[str, str] = {"jurisdiction": "US", "country_code": "US"}


def canonicalize(fields: Mapping[str, Any]) -> dict[str, Any]:
    """Rename deprecated/duplicate keys to their canonical name.

    When both a deprecated alias and its canonical key are supplied, the
    canonical value wins (the alias is dropped) so a half-migrated builder can
    set both during a transition without the alias clobbering the canonical.
    """
    out: dict[str, Any] = {}
    # First pass: copy canonical keys so they take precedence over aliases.
    for key, value in fields.items():
        if key not in payload_schema.ALIASES:
            out[key] = value
    # Second pass: fold aliases into their canonical name only if unset.
    for key, value in fields.items():
        if key in payload_schema.ALIASES:
            out.setdefault(payload_schema.ALIASES[key], value)
    return out


def build_payload(
    fields: Mapping[str, Any], *, fill_defaults: bool = True
) -> tuple[dict[str, Any], payload_schema.PayloadAudit]:
    """Return (canonical payload, schema audit) for a builder's field dict.

    The payload is alias-normalized and (optionally) has the US jurisdiction
    defaults filled. The audit reports any missing-required or undeclared keys
    so the caller can log/act on drift without this function silently mutating
    domain data.
    """
    payload = canonicalize(fields)
    if fill_defaults:
        for key, value in _DEFAULTS.items():
            if not payload.get(key):
                payload[key] = value
    return payload, payload_schema.validate_payload(payload)
