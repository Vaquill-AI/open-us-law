"""Canonical payload schema for the `statutes_us` corpus.

SINGLE SOURCE OF TRUTH for the per-section payload field set. Historically every
ingest path hand-built its own payload dict (~45 sites: `node_to_chunks`, the
federal `chunk_and_ingest`, and ~40 bespoke `to_chunk_record` / `_build_one`
builders), so fields drifted in NAME, PRESENCE, and TYPE across states and data
types (audit 2026-08). This module is the contract they must converge on.

How it is enforced:
  * runtime  - `missing_required()` is checked at the one write chokepoint
    (lib/embed_and_upsert.py) so a payload that would silently break retrieval
    (missing `category`/`state`/... -> wrong-corpus fallback) is surfaced loudly.
  * dev-time - a drift-guard test asserts each builder's payload keys are a
    subset of `ALL_KNOWN` and a superset of `REQUIRED`, so a new/renamed/dropped
    key must be reflected here first (a conscious decision, not silent drift).

Field tiers:
  REQUIRED  - universal + retriever/API-critical; a payload missing any is invalid.
  EXPECTED  - present on essentially every real section; missing => thin/suspect.
  OPTIONAL  - legitimate type-specific / enrichment fields (absent by design is fine).
  DEPRECATED- exists in stored data but should be removed/normalized (see ALIASES);
              tolerated by the guard, flagged for the convergence migration.

`ALL_KNOWN` = REQUIRED | EXPECTED | OPTIONAL | DEPRECATED. Any key NOT in
ALL_KNOWN is drift: either add it here (if legitimate) or drop it from the
producer. Keep this in sync with the human spec at
docs/us-corpus/CORPUS_PIPELINE_CONTRACT.md.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

# ---------------------------------------------------------------------------
# REQUIRED: universal + retriever/API-critical. Every point carries these
# (federal sections use state="federal"). Omitting/mis-typing `category`,
# `document_type`, `state`, or `country_code` makes the retriever silently
# fall back to the wrong corpus; `text`/`chunk_index`/`total_chunks` drive the
# empty-text filter and section-head reconstruction. section_number/title live
# in EXPECTED because agency/FR/exec docs legitimately lack a section number.
# ---------------------------------------------------------------------------
REQUIRED: frozenset[str] = frozenset(
    {
        "text",
        "act_id",
        "category",
        "document_type",
        "jurisdiction",
        "country_code",
        "state",
        "chunk_index",
        "total_chunks",
    }
)

# NOTE on `document_type` values: it is a controlled but DELIBERATELY GRANULAR
# vocabulary, not a bug. Most corpora use a coarse word (statute / regulation /
# constitution / court_rule / ruling), but agency guidance keeps the specific
# kind (irs_rev_proc / irs_notice / irs_rev_rul / irs_announcement) because the
# Downstream API consumers rely on that granularity. This schema
# enforces the PRESENCE of document_type, not a closed value set; do not coarsen
# the IRS values without a deliberate vocabulary migration across mirror + API.

# ---------------------------------------------------------------------------
# EXPECTED: present on essentially every real section across all data types.
# A builder omitting these is producing a thin/suspect payload.
# ---------------------------------------------------------------------------
EXPECTED: frozenset[str] = frozenset(
    {
        "section_number",
        "section_title",
        "citation",
        "citation_short",
        "display_label",
        "display_title",
        "display_path",
        "breadcrumb",
        "title",
        "title_name",
        "top_level_title",
        "act_status",
        "level_classifier",
        "word_count",
        "full_text_sha1",
        "raw_node_id",
        # canonical body-text pointer (R2 .txt of the section); see ALIASES.
        "r2_section_text_url",
    }
)

# ---------------------------------------------------------------------------
# OPTIONAL: legitimate, may-be-absent-by-design. Grouped by origin.
# ---------------------------------------------------------------------------
_OPTIONAL_HIERARCHY = {
    "title_number",
    "cfr_title_number",  # federal-regs variant of the "title" concept; unify later
    "volume",  # top-level container above "part", e.g. USCIS Policy Manual Vol. 12
    "chapter",
    "chapter_name",
    "subchapter",
    "subchapter_name",
    "part",
    "part_name",
    "subpart",
    "subpart_name",
    "year",
    "sort_key",
    "title_code",
    "parent_id",
    "parent_chunk_id",
}
_OPTIONAL_ENRICHMENT = {
    "subsection_count",
    "subsection_letters",
    "numbered_paragraph_count",
    "document_word_count",
    "cross_references_usc",
    "cross_references_cfr",
    "cross_references_count",
    # Regulation/court-rule -> its OWN state's enabling statute section (VA's
    # Authority field, for example, links directly to the exact Code of
    # Virginia section, not to USC/CFR -- see docs/us-corpus/CORPUS_RICHNESS_TODO.md
    # #2.1). Structured (not a bare cite string like cross_references_usc/cfr)
    # because a state citation needs the state + a display label to stay
    # unambiguous, the same reasoning already applied to statutory_authority/
    # implementing_regulations below.
    "cross_references_state_statute",
    "amendment_years",
    "amendments_count",
    "last_amended_year",
    "public_laws_referenced",
    "public_laws_count",
    "public_law_cites",
    "public_law_cites_count",
    "renumbered_to",
    "transferred_to",
    "effective_date",
    "effective_date_text",
    "original_enactment_date",
    "amendment_history",
    "prior_effective_dates",
    "source_note",
    "source_credit_raw",
    "current_through",
    "currency_note",
    "currency_year",
    "referenced_short_titles",
    "implementing_regulations",
    "implementing_regulations_count",
    "statutory_authority",
    "statutory_authority_count",
    "rule_amplifies",
    "federal_register_citations",
    "authority",
}
_OPTIONAL_URLS = {
    "source_url",
    # Canonical whole-document/whole-set source mirror (a PDF, usually). Read
    # layers (statutes_us_source._normalize_row, StatuteSection.from_payload)
    # derive the public `state_html_url` from this at query time when the
    # latter is absent, so builders should set THIS field, not state_html_url
    # directly, unless they genuinely have a distinct state-publisher HTML
    # page. Must stay OUT of ALIASES: it is not a duplicate of state_html_url.
    "r2_source_url",
    "state_html_url",
    "r2_html_url",
    "r2_pdf_url",
    "r2_xml_url",
    "r2_json_url",
    "r2_docx_url",
    "r2_urls",
    "r2_formats_available",
    "govinfo_html_url",
    "govinfo_pdf_url",
}
_OPTIONAL_GOVINFO = {
    "usckey",
    "exp_cite",
    "granule_id",
    "package_id",
    "document_id",
    "item_sort_key",
    "pdf_page",
}
_OPTIONAL_SNAPSHOT = {
    "is_snapshot",
    "snapshot_source",
    "snapshot_dataset",
    "snapshot_license",
    "has_text",
    "citation_conflict",
}
_OPTIONAL_COURT_RULES = {
    "rule_set",
    "rule_set_code",
    "rule_subtype",
    "committee_comment",
    "committee_note",
    "history_note",
    "citation_long",
    "cross_references",
    "editors_notes",
    # Case-annotation text (how courts have applied the rule) -- MD-specific
    # today, see docs/us-corpus/CORPUS_RICHNESS_TODO.md #2.5. Declared here,
    # not aliased to committee_note/editors_notes, because it is a distinct
    # kind of content (case law applying the rule, not an editorial note).
    "notes_of_decisions",
}
# Per-state regulation provenance / history. Every state reg ingester invented
# its own names for these concepts; declared so the guard locks the surface,
# but the redundant amendment-history names are ALIASED to canonical `history`
# below (a convergence target, not four real fields).
_OPTIONAL_STATE_REG_PROVENANCE = {
    "issuing_agency",
    "issuing_agency_code",
    "issuing_department",
    "issuing_department_code",
    "department_name",
    "history",
    "history_action",
    "part_history",
    "register_citations",
    "session_law_citations",
    "register_publication",
    "last_amended_date",
    "published_date",
    "review_date",
    # TX's "Last Updated: <date>" mirror stamp and NY/CA's raw effective-date
    # tag captured alongside the parsed effective_date (added 2026-08-05
    # merging corpus/regs-major).
    "edition",
    "effective_tag",
    "promulgated_under",
    "expiration_date",
    "derived_from",
    "relates_to",
    "necessity_function_conformity",
    "repealed_and_replaced",
    "rcw_citations",
    "wsr_filings",
    "rule_order_numbers",
    "rule_id",
    "rule_version_id",
    "source_book",
    "source_format",
    "umbrella_major",
    "umbrella_minor",
    "article_name",
    "article_number",
    "part_number",
    "subpart_letter",
    # NY/CA regulations carry a subchapter number distinct from part/article
    # (added 2026-08-05 merging corpus/regs-major).
    "subchapter_number",
    "subtitle",
    "subtitle_name",
    "citation_part",
    "section_note",
    "language_code",
}
# Char/page provenance (which bytes/pages of the source doc a chunk came from).
_OPTIONAL_CHAR_PROVENANCE = {
    "source_char_start",
    "source_char_end",
    "source_page_start",
    "source_page_end",
}
_OPTIONAL_FEDERAL_REGISTER = {
    "fr_type",
    "fr_subtype",
    "fr_action",
    "fr_agencies",
    "fr_agency_slugs",
    "fr_docket_ids",
    "fr_regulation_id_numbers",
    "fr_significant",
    "fr_volume",
    "fr_start_page",
    "fr_end_page",
    "fr_page_length",
    "fr_publication_date",
    "fr_effective_on",
    "fr_comments_close_on",
    "fr_topics",
    "fr_president",
    "fr_dates_text",
    "fr_correction_of",
    "fr_corrections",
    "fr_regulations_dot_gov_url",
    "abstract",
    "president",
    "signing_date",
    "publication_date",
}
_OPTIONAL_AGENCY_GUIDANCE = {
    "doc_type",
    "doc_number",
    "doc_index",
    "irb_reference",
    "irb_references_in_body",
    "code_sections_referenced",
    "regs_referenced",
    "prior_rev_procs_cited",
    "amplifies",
    "clarifies",
    "modifies",
    "obsoletes",
    "supersedes",
    "superseded_by",
    "applicable_date",
    "issued_date",
    "issue_date",
    "subject",
    "topic",
    "taxonomy_keywords",
    # SSA rulings + admin guidance
    "program",
    "program_label",
    "ruling_number",
    "rulings_referenced",
    "statutes_referenced",
    "regulations_referenced",
    "rescinds",
    "subject_label",
    "subject_number",
    "headline",
    "published_in_fed_reg",
    "statutory_citation",
    # Named source within a shared `category`/`corpus_type` (e.g. GUIDANCE_SOURCES
    # registry key), so the coverage catalog can list several distinct sources
    # that fold into one AGENCY_GUIDANCE token. See
    # scripts/us_corpus/federal/ingest_agency_guidance_docs.py.
    "source",
    # Third parties who requested an adjudicative letter/ruling (CFTC no-action
    # and exemptive letters name the requesting firm(s); reusable by any future
    # source with the same shape, e.g. SEC/FinCEN letters).
    "requesters",
    # The paired Consumer Affairs (CA) citation for a dually-numbered Federal
    # Reserve SR letter (e.g. "CA 21-2" alongside citation "SR 21-4 / CA 21-2").
    # A structured companion field so a CA number is independently filterable/
    # resolvable, not just a substring of `citation`.
    "ca_citation",
    # NLRB Division of Advice memoranda are keyed to an individual unfair-
    # labor-practice CASE (not a sequential guidance number); the case number
    # and the employer/party name are both genuinely part of how these are
    # cited and discussed in practice (e.g. "Starbucks Corp., Case No.
    # 03-CA-310035"), so each gets its own filterable field rather than living
    # only inside `citation`/`title` prose.
    "case_number",
    "case_name",
    # NLRB advice memos carry a distinct PUBLIC RELEASE date separate from
    # `issued_date` (the Division's own issuance date) -- often over a year
    # later, since release follows NLRB v. Sears, Roebuck & Co., 421 U.S. 132
    # (1975) and case-closure timing, not the memo's own drafting date.
    "release_date",
    # The date a document stopped being current guidance (rescinded or
    # withdrawn), distinct from `issued_date`/`applicable_date`. NLRB General
    # Counsel Memoranda commonly print this in-title, e.g. "[Rescinded
    # 2/14/2025 by Memorandum GC 25-05]"; reusable by any future source with
    # the same in-band rescission-date shape.
    "rescinded_on",
    # A negotiated Resolution Amount or an imposed Civil Money Penalty dollar
    # figure (HHS OCR HIPAA enforcement actions). String, not a parsed
    # int/float, to match this corpus's convention of preserving the source's
    # own printed form (compare `doc_number`, `citation_short`) rather than
    # normalizing currency at ingest time.
    "settlement_amount",
}
OPTIONAL: frozenset[str] = frozenset(
    _OPTIONAL_HIERARCHY
    | _OPTIONAL_ENRICHMENT
    | _OPTIONAL_URLS
    | _OPTIONAL_GOVINFO
    | _OPTIONAL_SNAPSHOT
    | _OPTIONAL_COURT_RULES
    | _OPTIONAL_STATE_REG_PROVENANCE
    | _OPTIONAL_CHAR_PROVENANCE
    | _OPTIONAL_FEDERAL_REGISTER
    | _OPTIONAL_AGENCY_GUIDANCE
)

# ---------------------------------------------------------------------------
# DEPRECATED: present in stored data but slated for removal/normalization by
# the convergence migration. ALIASES maps each historical duplicate/variant to
# its canonical name. `corpus_type` is redundant (derive from `category` via
# app.services.us_statutes_taxonomy.category_to_corpus_type) and can even drift
# from `category` (IRS stores corpus_type="agency_guidance", category="irs_rev_proc").
# ---------------------------------------------------------------------------
ALIASES: dict[str, str] = {
    "r2_txt_url": "r2_section_text_url",  # exact-duplicate value
    # State reg ingesters store the amendment/adoption history NOTE under
    # several names; canonical = `history`. (`history_action` is a distinct
    # action code and `part_history` is part-level history -- both kept as
    # their own OPTIONAL fields, NOT aliased.)
    "history_notes": "history",
    "history_note_raw": "history",
}
DEPRECATED: frozenset[str] = frozenset({"corpus_type", *ALIASES.keys()})

ALL_KNOWN: frozenset[str] = REQUIRED | EXPECTED | OPTIONAL | DEPRECATED


@dataclass(frozen=True)
class PayloadAudit:
    """Result of validating one payload against the canonical schema."""

    missing_required: tuple[str, ...] = ()
    missing_expected: tuple[str, ...] = ()
    unknown: tuple[str, ...] = ()
    deprecated: tuple[str, ...] = ()

    @property
    def ok(self) -> bool:
        """True when nothing retriever-critical is missing and no key is undeclared."""
        return not self.missing_required and not self.unknown


def missing_required(payload: Mapping[str, Any]) -> frozenset[str]:
    """REQUIRED fields absent from (or empty in, for scalars) the payload.

    A present-but-empty list/int/False is allowed (many enrichment fields start
    empty); only a REQUIRED key that is entirely absent or None/"" is a miss.
    Cheap enough to call per record at the write chokepoint.
    """
    miss = set()
    for key in REQUIRED:
        val = payload.get(key)
        if val is None or (isinstance(val, str) and val == ""):
            miss.add(key)
    return frozenset(miss)


def validate_payload(payload: Mapping[str, Any]) -> PayloadAudit:
    """Full audit of a payload against the canonical schema (for tests/tools)."""
    keys = set(payload.keys())
    return PayloadAudit(
        missing_required=tuple(sorted(missing_required(payload))),
        missing_expected=tuple(sorted(EXPECTED - keys)),
        unknown=tuple(sorted(keys - ALL_KNOWN)),
        deprecated=tuple(sorted(keys & DEPRECATED)),
    )


def canonical_key(key: str) -> str:
    """Resolve a deprecated/duplicate key to its canonical name (identity if none)."""
    return ALIASES.get(key, key)
