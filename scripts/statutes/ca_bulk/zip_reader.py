"""Range-reading reader for the CA Legislature bulk zip.

downloads.leginfo.legislature.ca.gov serves ``Accept-Ranges: bytes`` for
pubinfo_<session>.zip (~1.1 GB), so we never have to download the whole archive
to read a few tables out of it. This module speaks just enough of the ZIP
format to:

    1. locate the End Of Central Directory (EOCD, ZIP64-aware)
    2. enumerate members from the central directory
    3. range-fetch + inflate a single member

Why this host at all: leginfo.legislature.ca.gov (the web app our scraper
crawls) publishes ``robots.txt`` with ``User-agent: * / Disallow: /``. robots
is per-origin, and this downloads host serves no robots.txt, so the bulk export
is the sanctioned way to take this data. It is also the official database
export rather than scraped HTML, which is why it carries effective dates,
amendment history, and per-row timestamps the HTML never exposed.
"""
from __future__ import annotations

import struct
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator

import requests

BASE = "https://downloads.leginfo.legislature.ca.gov"


def session_zip_url(session: str) -> str:
    """CA publishes one zip per 2-year legislative session, e.g. pubinfo_2025.zip."""
    return f"{BASE}/pubinfo_{session}.zip"


@dataclass(frozen=True)
class Member:
    name: str
    method: int
    comp_size: int
    uncomp_size: int
    local_header_off: int


class RemoteZip:
    """Read members out of a remote zip using HTTP range requests."""

    def __init__(self, url: str, timeout: float = 120.0):
        self.url = url
        self.timeout = timeout
        self._members: dict[str, Member] | None = None
        self._etag: str | None = None
        self._size: int | None = None

    # -- low level ---------------------------------------------------------

    def head(self) -> tuple[int, str | None, str | None]:
        r = requests.head(self.url, timeout=self.timeout)
        r.raise_for_status()
        self._size = int(r.headers["Content-Length"])
        self._etag = r.headers.get("ETag")
        return self._size, self._etag, r.headers.get("Last-Modified")

    def _range(self, start: int, end: int) -> bytes:
        r = requests.get(
            self.url, headers={"Range": f"bytes={start}-{end}"}, timeout=self.timeout
        )
        r.raise_for_status()
        return r.content

    # -- central directory -------------------------------------------------

    def _find_eocd(self, total: int) -> tuple[int, int]:
        tail = self._range(max(0, total - 65_557), total - 1)
        i = tail.rfind(b"PK\x05\x06")
        if i < 0:
            raise ValueError("no EOCD in zip tail")
        cd_size, cd_off = struct.unpack("<II", tail[i + 12 : i + 20])
        if cd_off == 0xFFFFFFFF:  # ZIP64
            j = tail.rfind(b"PK\x06\x06")
            if j < 0:
                raise ValueError("ZIP64 sentinel but no EOCD64")
            cd_size = struct.unpack("<Q", tail[j + 40 : j + 48])[0]
            cd_off = struct.unpack("<Q", tail[j + 48 : j + 56])[0]
        return cd_off, cd_size

    @staticmethod
    def _parse_cd(cd: bytes) -> Iterator[Member]:
        p = 0
        while p + 46 <= len(cd) and cd[p : p + 4] == b"PK\x01\x02":
            method = struct.unpack("<H", cd[p + 10 : p + 12])[0]
            csize, usize = struct.unpack("<II", cd[p + 20 : p + 28])
            nlen, elen, clen = struct.unpack("<HHH", cd[p + 28 : p + 34])
            lho = struct.unpack("<I", cd[p + 42 : p + 46])[0]
            name = cd[p + 46 : p + 46 + nlen].decode("utf-8", "replace")
            extra = cd[p + 46 + nlen : p + 46 + nlen + elen]
            if 0xFFFFFFFF in (csize, usize, lho) and extra:
                q = 0
                while q + 4 <= len(extra):
                    hid, hsz = struct.unpack("<HH", extra[q : q + 4])
                    if hid == 0x0001:
                        # ZIP64 packs only the fields that overflowed, in a
                        # fixed order, so consume them positionally.
                        vals, k = extra[q + 4 : q + 4 + hsz], 0
                        if usize == 0xFFFFFFFF:
                            usize = struct.unpack("<Q", vals[k : k + 8])[0]
                            k += 8
                        if csize == 0xFFFFFFFF:
                            csize = struct.unpack("<Q", vals[k : k + 8])[0]
                            k += 8
                        if lho == 0xFFFFFFFF:
                            lho = struct.unpack("<Q", vals[k : k + 8])[0]
                        break
                    q += 4 + hsz
            yield Member(name, method, csize, usize, lho)
            p += 46 + nlen + elen + clen

    def members(self) -> dict[str, Member]:
        if self._members is None:
            total = self._size or self.head()[0]
            cd_off, cd_size = self._find_eocd(total)
            cd = self._range(cd_off, cd_off + cd_size - 1)
            self._members = {m.name: m for m in self._parse_cd(cd)}
        return self._members

    # -- member data -------------------------------------------------------

    def read(self, name: str) -> bytes:
        m = self.members()[name]
        # The local header's name/extra lengths can differ from the central
        # directory's, so re-read them rather than trusting the CD copy.
        hdr = self._range(m.local_header_off, m.local_header_off + 29)
        nlen, elen = struct.unpack("<HH", hdr[26:30])
        off = m.local_header_off + 30 + nlen + elen
        raw = self._range(off, off + m.comp_size - 1)
        return raw if m.method == 0 else zlib.decompress(raw, -15)

    def read_text(self, name: str) -> str:
        return self.read(name).decode("utf-8", "replace")


class LocalZip:
    """Same surface as RemoteZip, backed by a downloaded copy.

    Range reads are perfect for probing a few tables, but a full ingest touches
    ~161k .lob members and one HTTP round trip each (~4/s) is ~11 hours. The
    whole archive is 1.11 GB, so downloading once and reading locally turns that
    into a couple of minutes plus local inflate. Use ``ensure_local`` to fetch
    it, then hand this to the same code that took a RemoteZip.
    """

    def __init__(self, path: Path):
        import zipfile

        self.path = Path(path)
        self._zf = zipfile.ZipFile(self.path)
        self._names = set(self._zf.namelist())

    def head(self) -> tuple[int, str | None, str | None]:
        return self.path.stat().st_size, None, None

    def members(self) -> dict[str, object]:
        # Only membership is used by callers, so a name->info map is enough.
        return {n: self._zf.getinfo(n) for n in self._names}

    def read(self, name: str) -> bytes:
        return self._zf.read(name)

    def read_text(self, name: str) -> str:
        return self.read(name).decode("utf-8", "replace")


def ensure_local(url: str, dest: Path, chunk: int = 1 << 22) -> Path:
    """Download ``url`` to ``dest`` unless a same-sized copy is already there."""
    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, stream=True, timeout=600) as r:
        r.raise_for_status()
        total = int(r.headers.get("Content-Length", "0"))
        if dest.exists() and total and dest.stat().st_size == total:
            print(f"  cache hit: {dest} ({total:,} bytes)", flush=True)
            return dest
        got = 0
        with open(dest, "wb") as fh:
            for block in r.iter_content(chunk_size=chunk):
                fh.write(block)
                got += len(block)
                if total and got % (1 << 26) < chunk:
                    print(f"  {got / total * 100:5.1f}%  {got:,}/{total:,}", flush=True)
    print(f"  downloaded {got:,} bytes -> {dest}", flush=True)
    return dest
