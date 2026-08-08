"""USC statutory-body boundaries must follow GovInfo's typed source markup."""

from __future__ import annotations

import importlib
import os
import sys
import unittest
from pathlib import Path
from urllib.request import urlopen


ROOT = Path(__file__).resolve().parents[2]
FIXTURE = ROOT / "tests" / "source_integrity" / "fixtures" / "usc_title1_section1_excerpt.html"
LIVE_URL = "https://www.govinfo.gov/content/pkg/USCODE-2024-title1/html/USCODE-2024-title1-chap1-sec1.htm"


def segments_module(testcase: unittest.TestCase):
    """Keep missing implementation RED as an assertion failure, never an error."""
    try:
        return importlib.import_module("scripts.federal.usc_source_segments")
    except ModuleNotFoundError as error:
        testcase.fail(
            "required typed GovInfo statutory-boundary behavior is absent: "
            f"scripts.federal.usc_source_segments ({error})"
        )


class UscSourceSegmentTests(unittest.TestCase):
    def test_typed_statute_segment_excludes_editorial_notes(self) -> None:
        parser = segments_module(self)
        text = parser.extract_statutory_body(FIXTURE.read_text(encoding="utf-8"))
        self.assertEqual(
            text,
            "In determining the meaning of any Act of Congress, unless the context indicates otherwise—\n\n"
            "words importing the singular include and apply to several persons, parties, or things;",
        )
        self.assertNotIn("Editorial Notes", text)
        self.assertNotIn("Pub. L. 112", text)

    def test_missing_typed_statute_segment_fails_closed(self) -> None:
        parser = segments_module(self)
        with self.assertRaises(parser.SourceStructureError):
            parser.extract_statutory_body("<p>Editorial Notes only</p>")

    def test_multiple_typed_statute_segments_fail_closed(self) -> None:
        parser = segments_module(self)
        with self.assertRaises(parser.SourceStructureError):
            parser.extract_statutory_body(
                "<!-- field-start:statute --><p>First</p><!-- field-end:statute -->"
                "<!-- field-start:statute --><p>Second</p><!-- field-end:statute -->"
            )

    @unittest.skipUnless(os.environ.get("LIVE_SOURCE_INTEGRITY") == "1", "set LIVE_SOURCE_INTEGRITY=1")
    def test_live_govinfo_section_has_a_typed_statutory_body(self) -> None:
        parser = segments_module(self)
        with urlopen(LIVE_URL, timeout=30) as response:  # nosec B310: pinned public government source
            html = response.read().decode("utf-8")
        text = parser.extract_statutory_body(html)
        self.assertIn("words importing the singular", text)
        self.assertNotIn("Editorial Notes", text)

    def test_download_usc_existing_html_call_site_excludes_editorial_notes(self) -> None:
        """The direct helper used by download_usc._fetch_one_section is the call seam."""
        from scripts.federal import download_usc

        text = download_usc.html_to_text(FIXTURE.read_text(encoding="utf-8"))
        self.assertIn("words importing the singular", text)
        self.assertNotIn("Editorial Notes", text)
        self.assertNotIn("Pub. L. 112", text)

    def test_parse_usc_zip_existing_html_call_site_excludes_editorial_notes(self) -> None:
        """parse_usc_zip imports and invokes this concrete parsing call seam."""
        sys.path.insert(0, str(ROOT / "scripts" / "federal"))
        try:
            parse_usc_zip = importlib.import_module("parse_usc_zip")
        finally:
            sys.path.pop(0)

        text = parse_usc_zip.html_to_text(FIXTURE.read_text(encoding="utf-8"))
        self.assertIn("words importing the singular", text)
        self.assertNotIn("Editorial Notes", text)
        self.assertNotIn("Pub. L. 112", text)
