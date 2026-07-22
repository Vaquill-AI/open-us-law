#!/usr/bin/env python3
"""Parse the New Jersey Permanent Statutes Database RTF into sections.

Source: the official daily zip at
    https://pub.njleg.state.nj.us/Statutes/STATUTES-TEXT.zip
which contains STATUTES.RTF (~94 MB). The RTF carries paragraph style tags
that make segmentation unambiguous where the plain-text member does not:

    \\s1  New Jersey Permanent Statutes Database  (document title, ignored)
    \\s2  TITLE headings           -> starts a new title
    \\s3  section headnotes        -> "1:1-1.  General rules of construction"
    (no style / s0)  normal body   -> the section text

NJ section numbers are "Title:Section" (e.g. 2C:11-3). The flat bulk carries
no chapter level, so we derive an internal chapter from the section-number
prefix (2C:11-3 -> chapter 11) purely so the act_id keeps its T_C_S shape; the
user-facing citation is rendered independently and is unaffected.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterator

# Leading token of an s3 headnote: "2C:11-3." or "40A:26b-2  " etc.
_CIT_RE = re.compile(r"^\s*(\d+[A-Za-z]?):([0-9][0-9A-Za-z.\-]*?)\.?\s")
# Fallback for headnotes with no trailing space (rare): whole token.
_CIT_RE2 = re.compile(r"^\s*(\d+[A-Za-z]?):([0-9][0-9A-Za-z.\-]*)")

# Compiled once; matched with an explicit position so we never slice the chunk
# (chunk[i:] per backslash is O(n^2) on backslash-dense RTF).
_CTRL_RE = re.compile(r"\\([a-zA-Z]+)(-?\d+)? ?")


@dataclass(frozen=True)
class Section:
    title: str          # "2C"
    section: str        # "11-3"  (the part after the colon)
    catchline: str      # "Murder."
    body: str           # full section text

    @property
    def chapter(self) -> str:
        # Internal only: prefix of the section number before the first dash.
        return self.section.split("-", 1)[0]

    def citation(self) -> str:
        return f"N.J. Stat. § {self.title}:{self.section}"


def _rtf_to_text(chunk: str) -> str:
    """Extract plain text from one \\pard-delimited RTF chunk.

    Drops control words and {\\*..} destination groups (bookmarks, comments),
    decodes \\'xx and \\uN, maps \\par/\\line/\\tab to whitespace.
    """
    out: list[str] = []
    i, n = 0, len(chunk)
    skip_depth = 0          # >0 while inside a {\* ...} destination
    depth = 0
    while i < n:
        ch = chunk[i]
        if ch == "\\":
            # control word / symbol
            m = _CTRL_RE.match(chunk, i)
            if m:
                word = m.group(1)
                i = m.end()
                if skip_depth:
                    continue
                if word in ("par", "line", "tab", "cell", "row"):
                    out.append(" ")
                elif word == "u":  # \uN unicode
                    try:
                        cp = int(m.group(2))
                        if cp < 0:
                            cp += 65536
                        out.append(chr(cp))
                    except (TypeError, ValueError):
                        pass
                    # skip the following fallback char
                    if i < n and chunk[i] not in "\\{}":
                        i += 1
                # any other control word: drop
                continue
            # control symbol: \'xx hex, or escaped literal \{ \} \\
            if chunk[i:i + 2] == "\\'":
                hexs = chunk[i + 2:i + 4]
                i += 4
                if not skip_depth:
                    try:
                        out.append(bytes([int(hexs, 16)]).decode("cp1252", "replace"))
                    except ValueError:
                        pass
                continue
            nxt = chunk[i + 1] if i + 1 < n else ""
            i += 2
            if not skip_depth and nxt in "{}\\":
                out.append(nxt)
            continue
        if ch == "{":
            depth += 1
            # is this a destination group {\* ... } ?
            if chunk[i + 1:i + 3] == "\\*":
                skip_depth = depth
            i += 1
            continue
        if ch == "}":
            if skip_depth and depth == skip_depth:
                skip_depth = 0
            depth -= 1
            i += 1
            continue
        if ch in "\r\n":
            # Raw CR/LF in RTF source is insignificant line-wrapping (often
            # mid-word, e.g. "u\r\nnless"); drop it so words rejoin. Real
            # breaks come through \par / \line above.
            i += 1
            continue
        if not skip_depth:
            out.append(ch)
        i += 1
    text = "".join(out)
    text = re.sub(r"[ \t]+", " ", text)
    return text.strip()


def _style_of(chunk: str) -> int:
    m = re.match(r"\s*(?:\\plain)?\s*\\s(\d+)\b", chunk)
    return int(m.group(1)) if m else 0


def iter_sections(rtf: str) -> Iterator[Section]:
    chunks = rtf.split("\\pard")
    cur_title: str | None = None
    sec_title = sec_num = catchline = None
    body: list[str] = []

    def _flush() -> Section | None:
        if sec_title and sec_num:
            return Section(sec_title, sec_num, catchline or "", "\n".join(body).strip())
        return None

    for chunk in chunks:
        style = _style_of(chunk)
        if style == 1:
            continue
        if style == 2:  # TITLE heading
            s = _flush()
            if s:
                yield s
            sec_title = sec_num = catchline = None
            body = []
            txt = _rtf_to_text(chunk)
            mt = re.search(r"TITLE\s+(\w+)", txt)
            cur_title = mt.group(1) if mt else cur_title
            continue
        if style == 3:  # section headnote
            s = _flush()
            if s:
                yield s
            body = []
            txt = _rtf_to_text(chunk)
            m = _CIT_RE.match(txt) or _CIT_RE2.match(txt)
            if m:
                sec_title, sec_num = m.group(1), m.group(2)
                catchline = txt[m.end():].strip()
            else:
                sec_title = sec_num = None
                catchline = None
            continue
        # body paragraph
        if sec_title:
            t = _rtf_to_text(chunk)
            if t:
                body.append(t)
    s = _flush()
    if s:
        yield s
