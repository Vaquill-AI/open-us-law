"""Source-faithful HI identity tests against the real scraper emission seam."""

from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

from bs4 import BeautifulSoup


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts" / "state_scrapers"))
from src.scrapers.us.states.hi.statutes import scrapeHI  # noqa: E402
from src.utils.pydanticModels import Node  # noqa: E402


FIXTURE = ROOT / "tests" / "source_integrity" / "fixtures" / "hi_431_15_304.html"


class HawaiiIdentityTests(unittest.TestCase):
    def test_colon_bearing_section_number_is_not_truncated(self) -> None:
        heading = "§ 431:15-304 Actions by and against rehabilitator. (a) Body."
        self.assertEqual(scrapeHI._extract_section_number(heading), "431:15-304")
        self.assertEqual(
            scrapeHI._extract_section_name(heading),
            "Actions by and against rehabilitator.",
        )
        self.assertEqual(scrapeHI._strip_section_heading(heading), "(a) Body.")

    def test_well_formed_hyphenated_identifier_remains_compatible(self) -> None:
        self.assertEqual(scrapeHI._extract_section_number("§ 431-10 Existing form."), "431-10")

    def test_distinct_colon_sections_remain_distinct_identities(self) -> None:
        self.assertEqual(
            scrapeHI._extract_section_number("§ 431:15-304 Actions."),
            "431:15-304",
        )
        self.assertEqual(
            scrapeHI._extract_section_number("§ 431:15-305 Appeals."),
            "431:15-305",
        )

    def test_real_scraper_emits_full_id_heading_url_and_body(self) -> None:
        """Observe the Node passed to the existing sink; do not reimplement it."""
        source_url = (
            "http://www.capitol.hawaii.gov/hrscurrent/Vol09_Ch0431-0435H/"
            "HRS0431/HRS_0431-0015-0304.htm"
        )
        chapter = Node(
            id="us/hi/statutes/division=2/title=24/chapter=431",
            node_type="structure",
            level_classifier="chapter",
            number="431",
            top_level_title="24",
        )
        emitted = []
        soup = BeautifulSoup(FIXTURE.read_text(encoding="utf-8"), "html.parser")
        def capture(node, *_args, **_kwargs):
            emitted.append(node)
            return node

        with patch.object(scrapeHI, "insert_node", side_effect=capture):
            scrapeHI._process_section_page(soup, source_url, chapter)
        self.assertEqual(len(emitted), 1)
        section = emitted[0]
        self.assertEqual(
            section.node_id,
            "us/hi/statutes/division=2/title=24/chapter=431/section=431:15-304",
        )
        self.assertEqual(section.citation, "Haw. Rev. Stat. § 431:15-304")
        self.assertEqual(str(section.link), source_url)
        self.assertEqual(section.node_name, "Actions by and against rehabilitator.")
        self.assertEqual(section.node_text.to_list_text(), [
            "(a) Any court in this State before which an action is pending shall stay the action.",
            "(b) The rehabilitator may intervene in the action.",
        ])
