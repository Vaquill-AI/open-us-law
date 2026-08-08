"""Materialize validated source JSONL into a deterministic Parquet snapshot.

This module intentionally creates release *artifacts* only.  It never contacts
Hugging Face and therefore cannot publish a dataset or use credentials.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.parquet as pq


class MaterializationError(ValueError):
    """Base class for release input errors."""


class IdentityError(MaterializationError):
    """Raised when an emitted document identity is malformed."""


class DuplicateIdentityError(IdentityError):
    """Raised when two rows normalize to the same identity."""


class ProvenanceError(MaterializationError):
    """Raised when source provenance is missing or invalid."""


PARQUET_FIELDS = (
    "act_id", "citation", "citation_short", "state", "jurisdiction", "document_type",
    "title_number", "title_name", "chapter", "chapter_name", "section_number",
    "section_title", "breadcrumb", "display_path", "act_status", "text", "word_count",
    "source_url", "last_amended_year", "subsection_count", "cross_references_usc",
    "cross_references_cfr", "public_laws_referenced", "year",
)

PARQUET_SCHEMA = pa.schema([
    pa.field("act_id", pa.string()),
    pa.field("citation", pa.string()),
    pa.field("citation_short", pa.string()),
    pa.field("state", pa.string()),
    pa.field("jurisdiction", pa.string()),
    pa.field("document_type", pa.string()),
    pa.field("title_number", pa.string()),
    pa.field("title_name", pa.string()),
    pa.field("chapter", pa.string()),
    pa.field("chapter_name", pa.string()),
    pa.field("section_number", pa.string()),
    pa.field("section_title", pa.string()),
    pa.field("breadcrumb", pa.list_(pa.string())),
    pa.field("display_path", pa.string()),
    pa.field("act_status", pa.string()),
    pa.field("text", pa.string()),
    pa.field("word_count", pa.int64()),
    pa.field("source_url", pa.string()),
    pa.field("last_amended_year", pa.int64()),
    pa.field("subsection_count", pa.int64()),
    pa.field("cross_references_usc", pa.list_(pa.string())),
    pa.field("cross_references_cfr", pa.list_(pa.string())),
    pa.field("public_laws_referenced", pa.list_(pa.string())),
    pa.field("year", pa.int64()),
])

# The final segment must be a section identity, rather than a chapter-level ID.
_ACT_ID = re.compile(r"^[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)*_S(?P<section>[A-Z0-9]+(?:[.:-][A-Z0-9]+)*)$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
PARQUET_ENCODING = {
    "compression": "NONE",
    "use_dictionary": False,
    "write_statistics": False,
    "version": "2.6",
    "data_page_version": "1.0",
    "row_group_size": 65536,
}


@dataclass(frozen=True)
class MaterializationResult:
    rows: list[dict[str, Any]]
    quarantine: list[dict[str, Any]]
    manifest: dict[str, Any]
    lineage: list[dict[str, str]]


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _normalized_identity(value: str) -> str:
    return " ".join(value.split()).upper()


def _require_text(row: Mapping[str, Any], key: str) -> str:
    value = row.get(key)
    if not isinstance(value, str) or not value.strip():
        raise MaterializationError(f"row requires non-empty {key!r}")
    return value


def _validate_provenance(row: Mapping[str, Any]) -> None:
    source_url = row.get("source_url")
    parsed = urlparse(source_url) if isinstance(source_url, str) else None
    if not parsed or parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ProvenanceError("row requires an absolute HTTP(S) source_url")


def _validate_identity(row: Mapping[str, Any]) -> str:
    act_id = _require_text(row, "act_id")
    normalized = _normalized_identity(act_id)
    match = _ACT_ID.fullmatch(normalized)
    section_number = _require_text(row, "section_number")
    if not match or match.group("section") != _normalized_identity(section_number):
        raise IdentityError(f"malformed act_id: {act_id!r}")
    return normalized


def _string_list(value: Any, key: str) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise MaterializationError(f"{key!r} must be a list of strings")
    return list(value)


def _optional_int(value: Any, key: str) -> int | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise MaterializationError(f"{key!r} must be an integer or null")
    return value


def _release_row(source: Mapping[str, Any]) -> dict[str, Any]:
    _validate_provenance(source)
    _validate_identity(source)
    for key in ("citation", "section_title", "text"):
        _require_text(source, key)

    row: dict[str, Any] = {}
    for key in PARQUET_FIELDS:
        value = source.get(key)
        if key in {"breadcrumb", "cross_references_usc", "cross_references_cfr", "public_laws_referenced"}:
            row[key] = _string_list(value, key)
        elif key in {"word_count", "last_amended_year", "subsection_count", "year"}:
            row[key] = _optional_int(value, key)
        else:
            if value is None:
                row[key] = ""
            elif not isinstance(value, str):
                raise MaterializationError(f"{key!r} must be a string")
            else:
                row[key] = value
    row["citation_short"] = source.get("citation_short") or row["citation"]
    if row["word_count"] is None:
        row["word_count"] = len(row["text"].split())
    return row


def _schema_description() -> list[dict[str, str]]:
    return [{"name": field.name, "type": str(field.type)} for field in PARQUET_SCHEMA]


def _producer_binding(code_revision: str) -> dict[str, Any]:
    return {
        "code_revision": code_revision,
        "materializer": {
            "name": "scripts.release.materialize_snapshot",
            "version": "1",
            "python_version": platform.python_version(),
            "pyarrow_version": pa.__version__,
        },
        "parquet_encoding": PARQUET_ENCODING,
    }


def materialize_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    producer: Mapping[str, Any],
    duplicate_policy: str = "error",
) -> MaterializationResult:
    """Validate rows, sort them by identity, and build deterministic metadata.

    ``duplicate_policy='quarantine'`` removes *every* colliding row.  It never
    chooses or combines a statutory body.
    """
    if duplicate_policy not in {"error", "quarantine"}:
        raise MaterializationError("duplicate_policy must be 'error' or 'quarantine'")
    code_revision = producer.get("code_revision")
    if not isinstance(code_revision, str) or not code_revision.strip():
        raise ProvenanceError("producer requires a non-empty code_revision")

    source_rows = list(rows)
    prepared = [_release_row(row) for row in source_rows]
    identities: dict[str, list[int]] = {}
    for index, source in enumerate(source_rows):
        identities.setdefault(_validate_identity(source), []).append(index)
    colliders = {index for indexes in identities.values() if len(indexes) > 1 for index in indexes}
    collision_count = sum(1 for indexes in identities.values() if len(indexes) > 1)
    if colliders and duplicate_policy == "error":
        raise DuplicateIdentityError("normalized act_id collision; no artifact was materialized")

    quarantined = [prepared[index] for index in sorted(colliders, key=lambda index: _normalized_identity(source_rows[index]["act_id"]))]
    output_rows = [row for index, row in enumerate(prepared) if index not in colliders]
    output_rows.sort(key=lambda row: _normalized_identity(row["act_id"]))

    lineage = []
    for source, row in zip(source_rows, prepared):
        if row in quarantined:
            continue
        for old_id in _string_list(source.get("lineage_from"), "lineage_from"):
            if not old_id.strip():
                raise IdentityError("lineage_from cannot contain an empty identity")
            lineage.append({"old_id": old_id, "new_id": row["act_id"]})
    lineage.sort(key=lambda item: (item["old_id"], item["new_id"]))

    supplied_input_hash = producer.get("input_sha256")
    if supplied_input_hash is not None and (not isinstance(supplied_input_hash, str) or not _SHA256.fullmatch(supplied_input_hash)):
        raise ProvenanceError("producer input_sha256 must be a lowercase SHA-256 digest")
    input_hash = supplied_input_hash or _sha256(_canonical_bytes(source_rows))
    manifest = {
        "producer": _producer_binding(code_revision),
        "inputs": {"input_sha256": input_hash},
        "schema": _schema_description(),
        "outputs": {"rows_sha256": _sha256(_canonical_bytes(output_rows))},
        "row_count": len(output_rows),
        "collision_count": collision_count,
        "quarantined_count": len(quarantined),
        "lineage_count": len(lineage),
    }
    return MaterializationResult(output_rows, quarantined, manifest, lineage)


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows = []
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError as error:
            raise MaterializationError(f"invalid JSONL at line {number}: {error.msg}") from error
        if not isinstance(value, dict):
            raise MaterializationError(f"JSONL line {number} must be an object")
        rows.append(value)
    return rows


def _write_json(path: Path, value: Any) -> None:
    path.write_bytes(_canonical_bytes(value) + b"\n")


def _write_jsonl(path: Path, values: Iterable[Any]) -> None:
    path.write_bytes(b"".join(_canonical_bytes(value) + b"\n" for value in values))


def _write_parquet(path: Path, rows: list[dict[str, Any]]) -> None:
    columns = {field.name: [row[field.name] for row in rows] for field in PARQUET_SCHEMA}
    table = pa.Table.from_pydict(columns, schema=PARQUET_SCHEMA)
    pq.write_table(table, path, **PARQUET_ENCODING)


def materialize_file(
    input_path: Path,
    output_dir: Path,
    *,
    code_revision: str,
    hf_repo_id: str,
    hf_revision: str,
    duplicate_policy: str = "error",
    dry_run: bool = False,
) -> MaterializationResult:
    """Write a local release artifact; only ``dry_run`` is supported."""
    if not dry_run:
        raise MaterializationError("publication is external; this materializer requires --dry-run")
    if not hf_repo_id.strip() or not hf_revision.strip():
        raise ProvenanceError("--hf-repo-id and --hf-revision are required for a dry run")
    raw_input = input_path.read_bytes()
    result = materialize_rows(
        _read_jsonl(input_path),
        producer={"code_revision": code_revision, "input_sha256": _sha256(raw_input)},
        duplicate_policy=duplicate_policy,
    )

    output_dir.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="release-materialize-", dir=output_dir.parent) as temporary:
        stage = Path(temporary)
        parquet_path = stage / "us_hi_statutes.parquet"
        _write_parquet(parquet_path, result.rows)
        manifest = dict(result.manifest)
        manifest["outputs"] = dict(manifest["outputs"])
        manifest["outputs"]["rows_sha256"] = _sha256(parquet_path.read_bytes())
        manifest["publication_target"] = {
            "repository_id": hf_repo_id,
            "revision": hf_revision,
            "upload_performed": False,
        }
        _write_json(stage / "producer-manifest.json", manifest)
        _write_jsonl(stage / "identity-lineage.jsonl", result.lineage)
        _write_jsonl(stage / "quarantine.jsonl", result.quarantine)
        if output_dir.exists():
            if any(output_dir.iterdir()):
                raise MaterializationError(f"output directory is not empty: {output_dir}")
            output_dir.rmdir()
        os.replace(stage, output_dir)
    return MaterializationResult(result.rows, result.quarantine, manifest, result.lineage)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--producer-code-revision", required=True)
    parser.add_argument("--hf-repo-id", required=True)
    parser.add_argument("--hf-revision", required=True)
    parser.add_argument("--duplicate-policy", choices=("error", "quarantine"), default="error")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    try:
        materialize_file(
            args.input, args.output_dir, code_revision=args.producer_code_revision,
            hf_repo_id=args.hf_repo_id, hf_revision=args.hf_revision,
            duplicate_policy=args.duplicate_policy, dry_run=args.dry_run,
        )
    except (OSError, MaterializationError) as error:
        print(f"materialization failed: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
