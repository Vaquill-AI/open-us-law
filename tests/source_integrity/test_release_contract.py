"""Release artifact contract for the missing JSONL-to-Parquet materialization seam."""

from __future__ import annotations

import importlib
import hashlib
import json
import os
import platform
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


ROOT = Path(__file__).resolve().parents[2]
FIXTURE = ROOT / "tests" / "source_integrity" / "fixtures" / "release_rows.jsonl"
PARQUET_SCHEMA = [
    "act_id", "citation", "citation_short", "state", "jurisdiction", "document_type",
    "title_number", "title_name", "chapter", "chapter_name", "section_number",
    "section_title", "breadcrumb", "display_path", "act_status", "text", "word_count",
    "source_url", "last_amended_year", "subsection_count", "cross_references_usc",
    "cross_references_cfr", "public_laws_referenced", "year",
]
PARQUET_ENCODING = {
    "compression": "NONE",
    "use_dictionary": False,
    "write_statistics": False,
    "version": "2.6",
    "data_page_version": "1.0",
    "row_group_size": 65536,
}


def release_module(testcase: unittest.TestCase):
    """Fail (rather than error) until the required production seam exists."""
    try:
        return importlib.import_module("scripts.release.materialize_snapshot")
    except ModuleNotFoundError as error:
        testcase.fail(
            "required release behavior is absent: "
            "scripts.release.materialize_snapshot must materialize and validate rows "
            f"({error})"
        )


def valid_row(**overrides):
    row = {
        "act_id": "STATE_HI_D2_T24_C431_S431:15-304",
        "citation": "Haw. Rev. Stat. § 431:15-304",
        "state": "hi",
        "jurisdiction": "HI",
        "document_type": "statute",
        "title_number": "24",
        "title_name": "TITLE 24",
        "chapter": "431",
        "chapter_name": "INSURANCE",
        "section_number": "431:15-304",
        "section_title": "Actions by and against rehabilitator.",
        "breadcrumb": ["Hawaii", "TITLE 24", "431", "431:15-304"],
        "display_path": "TITLE 24 > 431 > 431:15-304",
        "act_status": "in_force",
        "text": "(a) statutory body bytes preserved.",
        "source_url": "https://www.capitol.hawaii.gov/example/431-15-304.htm",
        "year": 2026,
    }
    row.update(overrides)
    return row


class ReleaseContractTests(unittest.TestCase):
    def assert_identity_accepted(self, row) -> None:
        release = release_module(self)
        try:
            result = release.materialize_rows([row], producer={"code_revision": "test"})
        except Exception as error:
            self.fail(
                f"well-formed {row['section_number']!r} identity was rejected: "
                f"{type(error).__name__}: {error}"
            )
        self.assertEqual(result.rows[0]["act_id"], row["act_id"])
        self.assertEqual(result.rows[0]["section_number"], row["section_number"])
        self.assertEqual(result.rows[0]["citation"], row["citation"])

    def assert_reproducibility_binding(self, producer) -> None:
        self.assertEqual(
            producer,
            {
                "code_revision": producer.get("code_revision"),
                "materializer": {
                    "name": "scripts.release.materialize_snapshot",
                    "version": "1",
                    "python_version": platform.python_version(),
                    "pyarrow_version": pa.__version__,
                },
                "parquet_encoding": PARQUET_ENCODING,
            },
        )

    def test_plain_numeric_identity_remains_compatible(self) -> None:
        self.assert_identity_accepted(valid_row(
            act_id="STATE_PA_T3_C15_S1521", section_number="1521",
            citation="3 Pa.C.S. § 1521", state="pa", jurisdiction="PA",
            title_number="3", chapter="15", section_title="Purpose.",
            source_url="https://www.palegis.us/statutes/consolidated/view-statute?txtType=HTM&ttl=03",
        ))

    def test_hyphenated_identity_remains_compatible(self) -> None:
        self.assert_identity_accepted(valid_row(
            act_id="STATE_ID_T18_C40_S18-4003", section_number="18-4003",
            citation="Idaho Code § 18-4003", state="id", jurisdiction="ID",
            title_number="18", chapter="40", section_title="Degrees of murder.",
            source_url="https://legislature.idaho.gov/statutesrules/idstat/title18/t18ch40/sect18-4003/",
        ))

    def test_colon_identity_remains_compatible(self) -> None:
        self.assert_identity_accepted(valid_row())

    def test_dotted_identity_remains_compatible(self) -> None:
        self.assert_identity_accepted(valid_row(
            act_id="STATE_WI_C940_S940.01", section_number="940.01",
            citation="Wis. Stat. § 940.01", state="wi", jurisdiction="WI",
            title_number="", chapter="940", section_title="First-degree intentional homicide.",
            source_url="https://docs.legis.wisconsin.gov/statutes/statutes/940/i/01",
        ))

    def test_pinned_alaska_dotted_structural_component_remains_compatible(self) -> None:
        """Preserve the pinned dataset's dotted chapter component, not just its suffix."""
        self.assert_identity_accepted(valid_row(
            act_id="STATE_AK_T11_C11.76_S11.76.115", section_number="11.76.115",
            citation="Alaska Stat. § 11.76.115", state="ak", jurisdiction="US",
            title_number="11", chapter="11.76",
            section_title="Misconduct involving confidential information in the second degree.",
            source_url="https://www.akleg.gov/basis/statutes.asp?title=11#11.76.115",
        ))

    def test_dotted_structural_prefix_still_requires_terminal_section_agreement(self) -> None:
        """Accepting dotted structural tokens must not relax the terminal section check."""
        release = release_module(self)
        with self.assertRaises(release.IdentityError):
            release.materialize_rows(
                [valid_row(
                    act_id="STATE_AK_T11_C11.76_S11.76.116", section_number="11.76.115",
                    citation="Alaska Stat. § 11.76.115", state="ak", jurisdiction="US",
                    title_number="11", chapter="11.76",
                    source_url="https://www.akleg.gov/basis/statutes.asp?title=11#11.76.115",
                )],
                producer={"code_revision": "test"},
            )

    def test_duplicate_normalized_identity_fails_closed_without_text_merge(self) -> None:
        release = release_module(self)
        first = valid_row(text="first statutory body")
        second = valid_row(text="second statutory body")
        with self.assertRaises(release.DuplicateIdentityError):
            release.materialize_rows([first, second], producer={"code_revision": "test"})

    def test_quarantine_policy_keeps_every_collision_out_of_the_artifact(self) -> None:
        release = release_module(self)
        result = release.materialize_rows(
            [valid_row(text="first statutory body"), valid_row(text="second statutory body")],
            producer={"code_revision": "test"},
            duplicate_policy="quarantine",
        )
        self.assertEqual(result.rows, [])
        self.assertEqual(len(result.quarantine), 2)
        self.assertEqual(result.manifest["collision_count"], 1)
        self.assertEqual(result.manifest["quarantined_count"], 2)

    def test_missing_provenance_is_rejected(self) -> None:
        release = release_module(self)
        with self.assertRaises(release.ProvenanceError):
            release.materialize_rows([valid_row(source_url="")], producer={"code_revision": "test"})

    def test_inconsistent_identity_is_rejected_before_output(self) -> None:
        release = release_module(self)
        with self.assertRaises(release.IdentityError):
            release.materialize_rows(
                [valid_row(act_id="STATE_HI_D2_T24_C431_S431:15-305")],
                producer={"code_revision": "test"},
            )

    def test_well_formed_row_preserves_bytes_and_deterministic_order(self) -> None:
        release = release_module(self)
        later = valid_row(
            act_id="STATE_HI_D2_T24_C431_S431:15-305",
            section_number="431:15-305",
            citation="Haw. Rev. Stat. § 431:15-305",
            text="exact bytes: \u00a7 \u2014 remain unchanged\n",
        )
        result = release.materialize_rows([later, valid_row()], producer={"code_revision": "test"})
        self.assertEqual([row["act_id"] for row in result.rows], sorted(row["act_id"] for row in result.rows))
        self.assertEqual(result.rows[1]["text"], "exact bytes: \u00a7 \u2014 remain unchanged\n")

    def test_manifest_and_lineage_are_deterministic_and_bind_outputs(self) -> None:
        release = release_module(self)
        old_id = "STATE_HI_D2_T24_C431_S431"
        corrected = valid_row(lineage_from=[old_id])
        one = release.materialize_rows([corrected], producer={"code_revision": "abc123", "input_sha256": "0" * 64})
        two = release.materialize_rows([corrected], producer={"code_revision": "abc123", "input_sha256": "0" * 64})
        self.assertEqual(one.manifest, two.manifest)
        self.assertEqual(one.manifest["producer"]["code_revision"], "abc123")
        self.assert_reproducibility_binding(one.manifest["producer"])
        self.assertEqual(one.manifest["row_count"], 1)
        self.assertEqual(
            set(one.manifest),
            {
                "producer", "inputs", "schema", "outputs", "row_count", "collision_count",
                "quarantined_count", "lineage_count",
            },
        )
        self.assertEqual(one.manifest["inputs"]["input_sha256"], "0" * 64)
        self.assertIn("rows_sha256", one.manifest["outputs"])
        self.assertEqual(one.lineage, [{"old_id": old_id, "new_id": corrected["act_id"]}])

    def test_production_pyarrow_runtime_is_exactly_pinned(self) -> None:
        pins = [
            line.split("#", 1)[0].strip()
            for line in (ROOT / "requirements.txt").read_text(encoding="utf-8").splitlines()
            if line.split("#", 1)[0].strip().lower().startswith("pyarrow")
        ]
        self.assertEqual(pins, [f"pyarrow=={pa.__version__}"])

    def test_materializer_refuses_any_non_dry_run_before_reading_input(self) -> None:
        release = release_module(self)
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(release.MaterializationError):
                release.materialize_file(
                    Path(tmp) / "does-not-exist.jsonl", Path(tmp) / "output",
                    code_revision="test", hf_repo_id="owner/dataset",
                    hf_revision="release", dry_run=False,
                )

    def test_cli_materializes_deterministically_quarantines_and_dry_runs_offline(self) -> None:
        """Exercise the planned executable seam without mocking release output."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            input_path = root / "source.jsonl"
            input_path.write_bytes(FIXTURE.read_bytes())
            first = root / "first"
            second = root / "second"
            base = [
                sys.executable, "-m", "scripts.release.materialize_snapshot",
                "--input", str(input_path), "--producer-code-revision", "test-revision",
                "--hf-repo-id", "test-org/open-us-law-source-integrity",
                "--hf-revision", "test-v2026.08-source-integrity",
                "--dry-run",
            ]
            env = {**os.environ, "HF_ENDPOINT": "http://127.0.0.1:9"}
            env.pop("HF_TOKEN", None)
            env.pop("HUGGINGFACE_HUB_TOKEN", None)
            run_one = subprocess.run(
                [*base, "--output-dir", str(first)], cwd=ROOT, env=env,
                text=True, capture_output=True, check=False,
            )
            self.assertEqual(run_one.returncode, 0, run_one.stderr)
            expected = {
                "us_hi_statutes.parquet", "producer-manifest.json",
                "identity-lineage.jsonl", "quarantine.jsonl",
            }
            self.assertEqual({path.name for path in first.iterdir()}, expected)
            manifest = json.loads((first / "producer-manifest.json").read_text())
            self.assertEqual(manifest["producer"]["code_revision"], "test-revision")
            self.assert_reproducibility_binding(manifest["producer"])
            self.assertEqual(
                manifest["publication_target"],
                {
                    "repository_id": "test-org/open-us-law-source-integrity",
                    "revision": "test-v2026.08-source-integrity",
                    "upload_performed": False,
                },
            )
            self.assertIn("rows_sha256", manifest["outputs"])
            parquet = first / "us_hi_statutes.parquet"
            table = pq.read_table(parquet)
            self.assertEqual(table.schema.names, PARQUET_SCHEMA)
            self.assertEqual(table.num_rows, 2)
            columns = table.to_pydict()
            self.assertEqual(
                columns["act_id"],
                [
                    "STATE_HI_D2_T24_C431_S431:15-304",
                    "STATE_HI_D2_T24_C431_S431:15-305",
                ],
            )
            self.assertEqual(
                columns["text"],
                [
                    "(a) Any court in this State before which an action is pending shall stay the action.",
                    "(a) The rehabilitator may appeal in the manner provided by law.",
                ],
            )
            self.assertEqual(
                hashlib.sha256(parquet.read_bytes()).hexdigest(),
                manifest["outputs"]["rows_sha256"],
            )
            run_two = subprocess.run(
                [*base, "--output-dir", str(second)], cwd=ROOT, env=env,
                text=True, capture_output=True, check=False,
            )
            self.assertEqual(run_two.returncode, 0, run_two.stderr)
            for name in expected:
                self.assertEqual((first / name).read_bytes(), (second / name).read_bytes(), name)

            collision_path = root / "collision.jsonl"
            row = valid_row()
            collision_path.write_text(
                "\n".join((json.dumps(row), json.dumps({**row, "text": "must never merge"}))) + "\n",
                encoding="utf-8",
            )
            failed = subprocess.run(
                [*base, "--input", str(collision_path), "--output-dir", str(root / "fail")],
                cwd=ROOT, env=env, text=True, capture_output=True, check=False,
            )
            self.assertNotEqual(failed.returncode, 0)
            self.assertFalse((root / "fail" / "us_hi_statutes.parquet").exists())
            quarantined = subprocess.run(
                [*base, "--input", str(collision_path), "--output-dir", str(root / "quarantine"),
                 "--duplicate-policy", "quarantine"],
                cwd=ROOT, env=env, text=True, capture_output=True, check=False,
            )
            self.assertEqual(quarantined.returncode, 0, quarantined.stderr)
            self.assertEqual(
                len((root / "quarantine" / "quarantine.jsonl").read_text().splitlines()), 2,
            )
            quarantined_table = pq.read_table(root / "quarantine" / "us_hi_statutes.parquet")
            self.assertEqual(quarantined_table.schema.names, PARQUET_SCHEMA)
            self.assertEqual(quarantined_table.num_rows, 0)
            self.assertNotIn("must never merge", "\n".join(quarantined_table.column("text").to_pylist()))
