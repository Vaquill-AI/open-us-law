"""OGP PDF parsing for Puerto Rico codes.

Emits `ingest_pr_codes.Article` objects (so the downstream record builder stays
byte-for-byte schema-consistent with the existing PR corpus) but does the section split
itself, because the OGP consolidated PDFs need three things the legacy single-line
splitter got wrong:

1. Multi-line headings. An OGP header reads
   `Seccion 6070.59. - Periodos de Exencion Contributiva aplicables a las Zonas de
   Oportunidad. - (13 L.P.R.A. § 48588)` with the title wrapping across a line break. The
   legacy `[^\\n]*` capture clipped the title at the wrap ("...Zonas de") and, when the
   cite itself wrapped (`§\\n45017`), missed the LPRA number too. We capture the heading
   across newlines up to the ` - (NN L.P.R.A. § NNNN)` terminator.

2. Inline LPRA cite -> secondary field. The cite that terminates the heading is lifted
   into `citation_lpra` (see FEASIBILITY.md: LPRA stays secondary, never the primary key)
   and dropped from both the title and the body, so the body is the provision text alone.

3. TOC-safe dedup. Every OGP PDF carries a `Tabla de Contenido` whose entries
   ("Seccion 6070.72. Vigencia ..... 286") match the header regex too. We group by section
   number and keep the copy with the longest body; the real articulado always beats the
   dot-leader TOC stub. On Incentives 2019 this collapses the raw matches to 251 clean
   sections with 0 residual junk (the feasibility "one record per distinct section" rule).
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

_STATUTES_DIR = Path(__file__).resolve().parent.parent
if str(_STATUTES_DIR) not in sys.path:
    sys.path.insert(0, str(_STATUTES_DIR))
import ingest_pr_codes as legacy

# Section / article header prefix: "Seccion 1000.01. - Titulo" / "Articulo 12. - ...". The
# period after the number is optional, but the separator dash MUST be followed by
# whitespace: this is what distinguishes a real header from an inline hyphenated federal
# cross-reference such as "Seccion 1400Z-2(f)(1) del Codigo de Rentas Internas Federal"
# (internal hyphen, no following space) which must never be minted as a section.
_SEP = r"(?:[–—]|-\s)\s*"  # em/en dash, or a hyphen FOLLOWED BY a space
_SEC_HDR = re.compile(r"Secci[óo]n\s+(\d+(?:\.\d+){0,2}[A-Za-z]?)\s*\.?\s*" + _SEP)
_ART_HDR = re.compile(r"Art[íi]culo\s+(\d+(?:\.\d+)?[A-Za-z]?)\s*\.?\s*" + _SEP)
# Heading terminator: "- (13 L.P.R.A. § 45001)" / "(13 L.P.R.A. § 45001 nota)". The dash is
# optional and the section id may carry a "nota" suffix or spaces, so match up to the ")".
_LPRA_TERM = re.compile(
    r"[-–—]?\s*\(\s*(\d+[A-Za-z]?)\s*L\.?\s*P\.?\s*R\.?\s*A\.?\s*§\s*([^)]+?)\s*\)"
)
# Body start cues used only in the no-cite fallback split.
_BODY_CUE = re.compile(r"\n\s*(?:\([a-z0-9]+\)|\(\d+\)|Este |Esta |Los |Las |El |La |Un |Una )")

# Optional intra-code structure. Some codes (Civil) are divided into Libros; capturing the
# Libro per article preserves the existing breadcrumb ("Libro Primero: ...") instead of
# flattening every article under one part label. Enabled per catalog entry via
# meta["structure"] == "libro".
_LIBRO_RE = re.compile(r"(?m)^\s*LIBRO\s+(PRIMERO|SEGUNDO|TERCERO|CUARTO|QUINTO|SEXTO)\b")
_LIBRO_LABELS = {
    "PRIMERO": "Libro Primero: Las Relaciones Jurídicas",
    "SEGUNDO": "Libro Segundo: Las Instituciones Familiares",
    "TERCERO": "Libro Tercero: Los Derechos Reales",
    "CUARTO": "Libro Cuarto: Las Obligaciones",
    "QUINTO": "Libro Quinto: Los Contratos y otras Fuentes de las Obligaciones",
    "SEXTO": "Libro Sexto: La Sucesión por Causa de Muerte",
}
_PRE_LIBRO_LABEL = "Título Preliminar"


def _libro_bounds(text: str) -> list[tuple[int, str]]:
    """Start offset -> Libro label, first occurrence of each Libro only (TOC dups skipped)."""
    bounds: list[tuple[int, str]] = []
    seen: set[str] = set()
    for m in _LIBRO_RE.finditer(text):
        key = m.group(1)
        if key in seen:
            continue
        seen.add(key)
        bounds.append((m.start(), _LIBRO_LABELS[key]))
    return sorted(bounds)


def _label_at(offset: int, bounds: list[tuple[int, str]], default: str) -> str:
    label = default
    for off, lab in bounds:
        if off <= offset:
            label = lab
        else:
            break
    return label


def _clean_body(raw: str) -> str:
    raw = re.sub(r"\n\s*\d+\s*\n", "\n", raw)  # drop bare page-number lines
    body = re.sub(r"\s+", " ", raw).strip()
    return body.lstrip(".—–- );").strip()  # drop a stray heading-tail period/cite bracket


def _split_heading_body(span: str) -> tuple[str, str, str]:
    """Return (heading, citation_lpra, body) for one section span.

    Primary split is at the LPRA terminator; the fallback (section prints no cite) takes
    the heading up to the first body cue or sentence period, capped so a runaway match
    can never swallow the provision text.
    """
    term = _LPRA_TERM.search(span[:700])
    if term:
        return (
            span[: term.start()],
            f"{term.group(1)} L.P.R.A. § {term.group(2)}",
            span[term.end() :],
        )
    # No cite: the heading ends at the earliest of a sentence period, a "[Nota ...]"
    # editorial annotation, or a body cue; hard-capped so a bad match can never swallow
    # the provision text.
    cuts = [200]
    for pat in (r"\.\s", r"\[Nota"):
        mm = re.search(pat, span[:200])
        if mm:
            cuts.append(mm.start())
    cue = _BODY_CUE.search(span[:200])
    if cue:
        cuts.append(cue.start())
    cut = min(cuts)
    return span[:cut], "", span[cut:]


def parse_pdf(
    pdf_bytes: bytes, slug: str, meta: dict, part_label: str, source_url: str
) -> list[legacy.Article]:
    """Parse one OGP code PDF into deduped, LPRA-annotated Articles."""
    text = legacy._pdf_text(pdf_bytes)
    # Strip a trailing dot-leader Tabla de Contenido index, but ONLY when one really exists.
    # Some codes carry a real dot-leader index at the end (Incentives/Civil) whose last
    # entry's run-to-EOF span would otherwise beat the real final section in keep-longest.
    # Others (Municipal) have NO trailing index: their end-of-doc "Tabla de Contenido" is a
    # real Libro VIII chapter heading + a real enacted article titled "Tabla de Contenido"
    # (arts 8.001-8.005), so a blind rfind-strip would delete real articles. Gate the strip
    # on an actual dot-leader (`…`) run after the marker.
    toc = text.rfind("Tabla de Contenido")
    if toc > len(text) * 0.85 and re.search(r"…{5,}", text[toc:]):
        text = text[:toc]
    # marker "mixed" -> a law that uses BOTH Articulo and Seccion headers (e.g. Articulos
    # each holding Secciones). Collect both header types, and compute each header's span
    # against the NEXT header of EITHER type so an Articulo does not swallow its Secciones.
    if meta.get("marker") == "mixed":
        specs = [(_ART_HDR, "Artículo"), (_SEC_HDR, "Sección")]
    elif meta.get("marker") == "seccion":
        specs = [(_SEC_HDR, "Sección")]
    else:
        specs = [(_ART_HDR, "Artículo")]
    libros = _libro_bounds(text) if meta.get("structure") == "libro" else []

    # Only accept headers at the start of a line. Real section headers begin a line; a
    # mid-sentence "la Seccion 1400Z-2(f)(1) del Codigo de Rentas Internas Federal ..." is
    # an inline cross-reference (here, to federal IRC 1400Z) and must not become a section.
    hdrs: list[tuple[int, int, str, str]] = []  # (start, end, num, unit)
    for hdr, unit in specs:
        for m in hdr.finditer(text):
            if m.start() == 0 or text[m.start() - 1] == "\n":
                hdrs.append((m.start(), m.end(), m.group(1).strip(), unit))
    hdrs.sort()
    raw: list[legacy.Article] = []
    for i, (start, hend, num, unit) in enumerate(hdrs):
        end = hdrs[i + 1][0] if i + 1 < len(hdrs) else len(text)
        span = text[hend:end]
        heading_raw, cite, body_raw = _split_heading_body(span)
        heading = re.sub(r"\s+", " ", heading_raw).strip().rstrip(". ")
        body = _clean_body(body_raw)
        if len(body) < 20:
            continue
        plabel = _label_at(start, libros, _PRE_LIBRO_LABEL) if libros else part_label
        a = legacy.Article(
            code_slug=slug,
            code_name=meta["name"],
            code_name_en=meta["name_en"],
            citation_prefix=meta["citation"],
            year=meta["year"],
            part_label=plabel,
            article_num=num,
            article_heading=heading,
            raw_text=body,
            source_url=source_url,
            unit=unit,
        )
        a.lpra_citation = cite  # type: ignore[attr-defined]
        raw.append(a)

    # TOC-safe dedup. A section can appear as a real articulado copy AND as a dot-leader
    # entry in the trailing index or a per-subtitle mini-contents; the latter can outrun
    # the real body (its span runs into the following subtitle), so plain keep-longest is
    # not enough. Prefer a NON-TOC copy (dot-leaders "……" are the tell); only fall back to
    # longest-body among copies of the same kind.
    def _is_toc(a: legacy.Article) -> bool:
        # Dot-leader ellipsis is the usual tell; some codes (Municipal) instead carry a
        # "Tabla de Contenido Libro N" heading with a numbered index body. A section that is
        # ONLY a TOC (Electoral art 1.2) has no non-TOC rival, so it still survives dedup.
        return (
            "…" in a.article_heading
            or "…" in a.raw_text[:80]
            or a.article_heading.startswith("Tabla de Contenido")
            or a.raw_text[:60].startswith("Tabla de Contenido")
        )

    # Key on (unit, num) so a mixed law's "Articulo 5" and "Seccion 5" stay distinct
    # (their act_ids differ by the ART/SEC prefix too).
    best: dict[tuple[str, str], legacy.Article] = {}
    for a in raw:
        key = (a.unit, a.article_num)
        cur = best.get(key)
        if cur is None:
            best[key] = a
            continue
        a_toc, c_toc = _is_toc(a), _is_toc(cur)
        if a_toc != c_toc:
            if not a_toc:
                best[key] = a
        elif len(a.raw_text) > len(cur.raw_text):
            best[key] = a
    return list(best.values())
