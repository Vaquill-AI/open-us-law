"""Release artifact contract for the missing JSONL-to-Parquet materialization seam."""

from __future__ import annotations

import importlib
import unittest


def release_module():
    return importlib.import_module("scripts.release.materialize_snapshot")


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
    def test_duplicate_normalized_identity_fails_closed_without_text_merge(self) -> None:
        release = release_module()
        first = valid_row(text="first statutory body")
        second = valid_row(text="second statutory body")
        with self.assertRaises(release.DuplicateIdentityError):
            release.materialize_rows([first, second], producer={"code_revision": "test"})

    def test_quarantine_policy_keeps_every_collision_out_of_the_artifact(self) -> None:
        release = release_module()
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
        release = release_module()
        with self.assertRaises(release.ProvenanceError):
            release.materialize_rows([valid_row(source_url="")], producer={"code_revision": "test"})

    def test_malformed_identity_is_rejected_before_output(self) -> None:
        release = release_module()
        with self.assertRaises(release.IdentityError):
            release.materialize_rows([valid_row(act_id="STATE_HI_D2_T24_C431_S431")], producer={"code_revision": "test"})

    def test_well_formed_row_preserves_bytes_and_deterministic_order(self) -> None:
        release = release_module()
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
        release = release_module()
        old_id = "STATE_HI_D2_T24_C431_S431"
        corrected = valid_row(lineage_from=[old_id])
        one = release.materialize_rows([corrected], producer={"code_revision": "abc123", "input_sha256": "0" * 64})
        two = release.materialize_rows([corrected], producer={"code_revision": "abc123", "input_sha256": "0" * 64})
        self.assertEqual(one.manifest, two.manifest)
        self.assertEqual(one.manifest["producer"]["code_revision"], "abc123")
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
