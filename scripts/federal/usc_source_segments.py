"""Extract statutory text from GovInfo's typed USC source segments."""

from __future__ import annotations

import re
from html.parser import HTMLParser


class SourceStructureError(ValueError):
    """Raised when a GovInfo section lacks one unambiguous statute segment."""


_STATUTE_BOUNDARY = re.compile(
    r"<!--\s*field-(?P<kind>start|end):statute\s*-->", re.IGNORECASE
)


class _StatutoryTextExtractor(HTMLParser):
    """Render the already-selected statutory HTML using USC paragraph spacing."""

    def __init__(self) -> None:
        super().__init__()
        self._text_parts: list[str] = []
        self._skip = False

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in ("script", "style", "head"):
            self._skip = True
        elif tag in ("p", "div", "br", "li", "tr", "h1", "h2", "h3", "h4"):
            self._text_parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        if tag in ("script", "style", "head"):
            self._skip = False
        elif tag in ("p", "div", "li", "tr"):
            self._text_parts.append("\n")

    def handle_data(self, data: str) -> None:
        if not self._skip:
            self._text_parts.append(data)

    def get_text(self) -> str:
        raw = "".join(self._text_parts)
        raw = re.sub(r"[ \t]+", " ", raw)
        raw = re.sub(r"\n{3,}", "\n\n", raw)
        return raw.strip()


def extract_statutory_body(html: str) -> str:
    """Return text inside exactly one GovInfo ``statute`` source segment.

    Editorial material is intentionally not inspected or inferred from prose:
    the trusted, typed source boundaries are the sole authority for inclusion.
    """
    boundaries = list(_STATUTE_BOUNDARY.finditer(html))
    if len(boundaries) != 2:
        raise SourceStructureError("expected exactly one typed GovInfo statute segment")

    start, end = boundaries
    if start.group("kind").lower() != "start" or end.group("kind").lower() != "end":
        raise SourceStructureError("malformed typed GovInfo statute segment")

    parser = _StatutoryTextExtractor()
    parser.feed(html[start.end():end.start()])
    parser.close()
    return parser.get_text()
