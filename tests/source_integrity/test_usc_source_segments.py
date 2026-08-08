"""USC statutory-body boundaries must follow GovInfo's typed source markup."""

from __future__ import annotations

import importlib
import os
import unittest
from pathlib import Path
from urllib.request import urlopen


ROOT = Path(__file__).resolve().parents[2]
FIXTURE = ROOT / "tests" / "source_integrity" / "fixtures" / "usc_title1_section1_excerpt.html"
LIVE_URL = "https://www.govinfo.gov/content/pkg/USCODE-2024-title1/html/USCODE-2024-title1-chap1-sec1.htm"


def segments_module():
    return importlib.import_module("scripts.federal.usc_source_segments")


class UscSourceSegmentTests(unittest.TestCase):
    def test_typed_statute_segment_excludes_editorial_notes(self) -> None:
        parser = segments_module()
        text = parser.extract_statutory_body(FIXTURE.read_text(encoding="utf-8"))
        self.assertEqual(
            text,
            "In determining the meaning of any Act of Congress, unless the context indicates otherwise—\n\n"
            "words importing the singular include and apply to several persons, parties, or things;",
        )
        self.assertNotIn("Editorial Notes", text)
        self.assertNotIn("Pub. L. 112", text)

    def test_missing_typed_statute_segment_fails_closed(self) -> None:
        parser = segments_module()
        with self.assertRaises(parser.SourceStructureError):
            parser.extract_statutory_body("<p>Editorial Notes only</p>")

    @unittest.skipUnless(os.environ.get("LIVE_SOURCE_INTEGRITY") == "1", "set LIVE_SOURCE_INTEGRITY=1")
    def test_live_govinfo_section_has_a_typed_statutory_body(self) -> None:
        parser = segments_module()
        with urlopen(LIVE_URL, timeout=30) as response:  # nosec B310: pinned public government source
            html = response.read().decode("utf-8")
        text = parser.extract_statutory_body(html)
        self.assertIn("words importing the singular", text)
        self.assertNotIn("Editorial Notes", text)
