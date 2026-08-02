"""ILGA section HTML -> plain text, and the manifest-code -> file URL mapping.

ILCS section files on the ILGA FTP tree are old HTML 4.01: the section body
lives in a single `<div align="justify">` full of `<code><font size="2"
face="Times New Roman">` wrappers and `&nbsp;` indentation, e.g.

    <div align="justify"> ... (5 ILCS 5/1) (from Ch. 1, par. 301) ...
      Sec. 1.  Whenever ... </div>

We keep the whole div text: the leading "(5 ILCS 5/1)" current-citation and the
"(from Ch. N, par. M)" source note are genuine statutory provenance, and
"Sec. N." is the heading. Only formatting whitespace is normalized.

Determinism matters (point_id = md5(act_id::chunk_index::sha1(text))): the walk
is a plain get_text + whitespace collapse, stable across runs.

File URL is fully derived from the manifest code, so no directory crawl:
    code    = ccccaaaaat...    (e.g. 000500050K1)
    chapter = code[0:4]        -> dir "Ch 0005"
    act4    = code[4:8]        -> dir "Act 0005"   (first 4 of the 5-digit act)
    file    = "{code}.html"
    -> https://www.ilga.gov/ftp/ILCS/Ch%200005/Act%200005/000500050K1.html
"""
from __future__ import annotations

import re

from bs4 import BeautifulSoup

FTP_BASE = "https://www.ilga.gov/ftp/ILCS"
_WS = re.compile(r"\s+")


def section_url(manifest_code: str) -> str:
    """Derive the section file URL from a raw manifest code (no crawl needed)."""
    cccc = manifest_code[0:4]
    act4 = manifest_code[4:8]
    # Spaces in the IIS path are %20; the filename is the code verbatim + .html.
    return f"{FTP_BASE}/Ch%20{cccc}/Act%20{act4}/{manifest_code}.html"


def html_to_text(html: str) -> str:
    """Extract the section text from an ILGA section HTML file.

    Returns the normalized text of the justify div (falls back to the whole
    body if the div is absent), whitespace collapsed. Empty string if nothing.
    """
    if not html or not html.strip():
        return ""
    soup = BeautifulSoup(html, "html.parser")
    div = soup.find("div", align="justify")
    node = div if div is not None else soup
    # get_text with a space separator so adjacent <code>/<font> runs and the
    # &nbsp; indentation do not fuse words together; then collapse whitespace.
    text = node.get_text(separator=" ")
    text = _WS.sub(" ", text).strip()
    return text
