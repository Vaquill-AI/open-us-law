"""Catalog of Puerto Rico codes/major laws available as consolidated PDFs from the
official OGP Biblioteca Virtual (bvirtualogp.pr.gov).

OGP publishes one consolidated PDF per law, named `{law-number}-{year}.pdf`. These PDFs
are official, public domain, current (amendment-consolidated), and print the LPRA cite
inline next to each section, e.g. `Seccion 1000.01. - Titulo. - (13 L.P.R.A. § 45001)`.
`pr_bulk.parse` extracts that cite into the secondary `citation_lpra` field.

Language is Spanish, the enacted authoritative language. We deliberately never ingest the
copyrighted LexisNexis English LPRA (see the statutes-corpus-ingest skill, Puerto Rico
edge case, and FEASIBILITY.md).

Each entry:
    slug -> {
        name, name_en, citation (prefix), year,
        marker: "seccion" | "articulo"  (which unit header the PDF uses),
        parts:  [(part_label, pdf_url), ...]  (one consolidated PDF per part),
    }

The `slug` becomes the `title_code` / code grouping in the API (e.g. `pr_incentivos`), so
keep it stable and distinct from the existing LexJuris codes (civil, penal, rentas).

Before wiring a new code: fetch its OGP URL and confirm it returns a real PDF (magic bytes
`%PDF-`), then dry-run pr_bulk to eyeball the section count before embedding.
"""

OGP_BASE = "https://bvirtualogp.pr.gov/ogp/Bvirtual/leyesreferencia/PDF"
# OGP splits its PDFs across TWO SharePoint folders: leyesreferencia (1,890 files) and
# LeyesOrganicas (390 agency/organic acts, incl. the Insurance Code 77-1957). A stem in
# one folder 404s in the other, so fetches must try both.
OGP_BASE_ORG = "https://bvirtualogp.pr.gov/ogp/Bvirtual/LeyesOrganicas/PDF"
OGP_BASES = (OGP_BASE, OGP_BASE_ORG)

OGP_CODES: dict[str, dict] = {
    # Data fields (name/citation) carry Spanish accents to match the existing PR corpus
    # (Codigo Civil/Penal/Rentas records use accented forms); this is source data, not our
    # prose, so the US-spelling/no-accent house rule does not apply here.
    "incentivos": {
        "name": "Código de Incentivos de Puerto Rico (2019)",
        "name_en": "Incentives Code of Puerto Rico (2019)",
        "citation": "Cód. Inc. P.R.",
        "year": 2019,
        "marker": "seccion",
        "parts": [
            ("Código de Incentivos", f"{OGP_BASE}/60-2019.pdf"),
        ],
    },
    # REFRESH of the existing pr_civil (was ingested from LexJuris originals). The OGP
    # consolidated Civil Code is the current amended text (through 2026) and prints the LPRA
    # cite inline. act_id reproduction vs the live pr_civil is 100% (1,819/1,819), so this is
    # an act_id-scoped reconcile within STATE_PR_CIVIL_*, NOT a new code. "structure": "libro"
    # preserves the "Libro Primero: ..." breadcrumb instead of flattening it.
    "civil": {
        "name": "Código Civil de Puerto Rico (2020)",
        "name_en": "Civil Code of Puerto Rico (2020)",
        "citation": "Cód. Civ. P.R.",
        "year": 2020,
        "marker": "articulo",
        "structure": "libro",
        "parts": [
            ("Código Civil", f"{OGP_BASE}/55-2020.pdf"),
        ],
    },
    # NEW additive code (no existing PR points): Código Electoral de Puerto Rico de 2020.
    "electoral": {
        "name": "Código Electoral de Puerto Rico (2020)",
        "name_en": "Electoral Code of Puerto Rico (2020)",
        "citation": "Cód. Elect. P.R.",
        "year": 2020,
        "marker": "articulo",
        "parts": [
            ("Código Electoral", f"{OGP_BASE}/58-2020.pdf"),
        ],
    },
    # NEW additive named laws (Spanish-only; the "<english>"/"«english»" tag some carry is a
    # stray nav artifact, not a bilingual half - verified). All use "Artículo" headers.
    "transito": {
        "name": "Ley de Vehículos y Tránsito de Puerto Rico (2000)",
        "name_en": "Vehicles and Traffic Act of Puerto Rico (2000)",
        "citation": "Ley Veh. y Tráns. P.R.",
        "year": 2000,
        "marker": "articulo",
        "parts": [("Ley de Vehículos y Tránsito", f"{OGP_BASE}/22-2000.pdf")],
    },
    # 580-page Código Municipal. Its end-of-doc "Tabla de Contenido" is a real Libro VIII
    # chapter heading + a real enacted article 8.002 "Tabla de Contenido" (not a dot-leader
    # index), so the trailing-TOC strip is now gated on an actual "……" run (see parse.py) -
    # arts 8.001-8.005 are preserved. (art 8.002 is inherently a contents-listing article,
    # low retrieval value, title slightly off - acceptable.)
    "municipal": {
        "name": "Código Municipal de Puerto Rico (2020)",
        "name_en": "Municipal Code of Puerto Rico (2020)",
        "citation": "Cód. Mun. P.R.",
        "year": 2020,
        "marker": "articulo",
        "parts": [("Código Municipal", f"{OGP_BASE}/107-2020.pdf")],
    },
    "condominios": {
        "name": "Ley de Condominios de Puerto Rico (2020)",
        "name_en": "Condominium Act of Puerto Rico (2020)",
        "citation": "Ley Condominios P.R.",
        "year": 2020,
        "marker": "articulo",
        "parts": [("Ley de Condominios", f"{OGP_BASE}/129-2020.pdf")],
    },
    "lpau": {
        "name": "Ley de Procedimiento Administrativo Uniforme (2017)",
        "name_en": "Uniform Administrative Procedure Act (2017)",
        "citation": "L.P.A.U. P.R.",
        "year": 2017,
        "marker": "seccion",
        "parts": [("Ley de Procedimiento Administrativo Uniforme", f"{OGP_BASE}/38-2017.pdf")],
    },
    "ambiental": {
        "name": "Ley sobre Política Pública Ambiental (2004)",
        "name_en": "Environmental Public Policy Act (2004)",
        "citation": "Ley Pol. Púb. Amb. P.R.",
        "year": 2004,
        "marker": "articulo",
        "parts": [("Ley sobre Política Pública Ambiental", f"{OGP_BASE}/416-2004.pdf")],
    },
    "incapacidad": {
        "name": "Ley de Beneficios por Incapacidad Temporal (1968)",
        "name_en": "Temporary Disability Benefits Act (1968)",
        "citation": "Ley Incap. Temp. P.R.",
        "year": 1968,
        "marker": "seccion",
        "parts": [("Ley de Beneficios por Incapacidad Temporal", f"{OGP_BASE}/139-1968.pdf")],
    },
    "transparencia": {
        "name": "Ley de Transparencia y Acceso a la Información Pública (2019)",
        "name_en": "Transparency and Access to Public Information Act (2019)",
        "citation": "Ley Transp. P.R.",
        "year": 2019,
        "marker": "articulo",
        "parts": [
            ("Ley de Transparencia y Acceso a la Información Pública", f"{OGP_BASE}/141-2019.pdf")
        ],
    },
    "notarial": {
        "name": "Ley Notarial de Puerto Rico (1987)",
        "name_en": "Notarial Act of Puerto Rico (1987)",
        "citation": "Ley Not. P.R.",
        "year": 1987,
        "marker": "articulo",
        "parts": [("Ley Notarial", f"{OGP_BASE}/75-1987.pdf")],
    },
    "armas": {
        "name": "Ley de Armas de Puerto Rico (2020)",
        "name_en": "Weapons Act of Puerto Rico (2020)",
        "citation": "Ley de Armas P.R.",
        "year": 2020,
        "marker": "articulo",
        "parts": [("Ley de Armas", f"{OGP_BASE}/168-2019.pdf")],
    },
    # --- Further OGP targets (verify URL returns %PDF- first; all purely additive) ---
    #   Political Code; Commercial Code; Labor laws. NOTE: not every PR law is on OGP
    #   leyesreferencia - 54-1989 (domestic violence), 77-1957 (insurance) 404 there (try
    #   LexJuris). SKIPPED: 4-1971 (Certificados de Divorcio, 6 arts, trivial); 8-2017 (HR,
    #   MIXED Artículo+Sección markers - needs multi-marker parse support first).
}
