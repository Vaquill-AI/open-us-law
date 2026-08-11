"""Canonical PDF text extraction for the ingesters in this repo.

The single place PDF bytes should be turned into text. Several ingesters had
each independently reimplemented the same PyMuPDF loop after separately
discovering the same problem: pdfplumber leaks page-tree memory across
repeated opens, enough to take one long-running scraper's RSS into the
gigabytes. One shared function means the next fix (or the next library swap)
happens once.

Usage:
    from scripts.lib.pdf_extract import pdf_to_text

    text = pdf_to_text(pdf_bytes)               # single-column, the common case
    text = pdf_to_text(pdf_bytes, columns=2)    # two-column layout (e.g. the WI constitution)
    text = pdf_to_text(pdf_bytes, sep="\\n")     # a caller's own page-join convention

For anything beyond plain per-page text (custom cropping, images, embedded
fonts), use `open_pdf` directly and work with PyMuPDF's own `Page` API rather
than growing this module's parameter list to cover every future need.

PyMuPDF is a hard dependency here, not an optional one: it is pinned in
requirements.txt and a large share of the state sources in this corpus are
PDF-only. A missing install therefore raises `PdfExtractionUnavailable`
instead of degrading, because every call site treats an empty string as
"this source published an empty document" and drops the unit. A silent ""
would turn a one-line install problem into nine state scrapers that emit
zero sections and still report success. Callers that swallow per-document
extraction errors must let that one exception through; it is a broken
environment, not a bad document.

The import stays lazy so a run that only touches HTML sources does not need
PyMuPDF installed at all.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import fitz


class PdfExtractionUnavailable(RuntimeError):
    """PDF extraction cannot run at all in this environment.

    Distinct from a per-document failure so callers can keep their
    drop-this-unit-and-continue handling for real PDF problems while still
    letting an unusable environment stop the run.
    """


def _import_fitz():
    """PyMuPDF, or a PdfExtractionUnavailable that names the fix.

    PyMuPDF's ImportError on its own reads as `No module named 'fitz'`, which
    is not obviously the same thing as the `pymupdf` distribution the caller
    has to install (the import name and the package name differ).
    """
    try:
        import fitz  # PyMuPDF
    except ImportError as exc:  # pragma: no cover - depends on the environment
        raise PdfExtractionUnavailable(
            "PyMuPDF is required for PDF text extraction but is not installed. "
            "Install it with `pip install pymupdf` (it is already pinned in "
            "requirements.txt); the import name is `fitz`."
        ) from exc
    return fitz


@contextmanager
def open_pdf(pdf_bytes: bytes) -> Iterator[fitz.Document]:
    """Open PDF bytes as a PyMuPDF document, closed automatically on exit.

    For callers needing page-level access beyond `pdf_to_text` (custom
    cropping, per-page metadata). Plain text extraction should use
    `pdf_to_text` instead.
    """
    fitz = _import_fitz()

    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    try:
        yield doc
    finally:
        doc.close()


def page_text(page: fitz.Page, *, columns: int = 1) -> str:
    """Page text read in column order (left column top-to-bottom, then the
    next column), not PyMuPDF's default row-wise reading order, which
    interleaves columns mid-sentence on multi-column layouts. `columns=1`
    (the default in `pdf_to_text`) skips the cropping and reads the whole page.
    """
    fitz = _import_fitz()

    if columns <= 1:
        return page.get_text("text")
    rect = page.rect
    col_width = rect.width / columns
    # Small gutter overlap so a character straddling a column boundary is not
    # clipped out of both crops.
    gutter = 4
    parts = []
    for i in range(columns):
        x0 = max(rect.x0, rect.x0 + i * col_width - (gutter if i > 0 else 0))
        x1 = min(rect.x1, rect.x0 + (i + 1) * col_width + gutter)
        clip = fitz.Rect(x0, rect.y0, x1, rect.y1)
        parts.append(page.get_text("text", clip=clip))
    return "\n".join(parts)


def pdf_to_text(pdf_bytes: bytes, *, columns: int = 1, sep: str = "\n\n") -> str:
    """Extract a PDF's embedded text layer, page by page, newlines preserved.

    `sep`: how pages are joined. The default "\\n\\n" (a visible page break) is
    right for most callers; "\\n" and "" exist because real call sites were
    written against their own page-join convention plus their own
    whitespace-collapse pass, and forcing them all onto one convention would
    silently change their section-splitting regexes' input.

    `columns`: 1 (the default) reads each page as a single block, correct for
    the large majority of government PDFs in this corpus. Pass 2 for a
    genuinely two-column layout. See `page_text` for why plain extraction
    produces interleaved, wrong-order text on those pages.

    Does NOT OCR, so a scanned PDF with no text layer returns "". That is
    deliberate: this function is offline, free and deterministic. Callers
    should treat an empty return as a hard failure signal (most already do,
    via their own drop-counter guard) rather than silently proceeding.

    Raises whatever PyMuPDF raises on a malformed PDF. That is not swallowed
    here because callers already wrap their own fetch-and-parse step with
    logging, and catching in both places just hides the traceback twice.
    """
    parts: list[str] = []
    with open_pdf(pdf_bytes) as doc:
        for page in doc:
            t = page_text(page, columns=columns)
            if t.strip():
                parts.append(t)
    return sep.join(parts)
