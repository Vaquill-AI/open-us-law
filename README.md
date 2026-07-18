# Open US Law

Open, structured **US primary law** — state statutory codes, the United States Code, the Code of Federal Regulations, state administrative regulations, state and federal constitutions, and court rules — together with the ingestion pipeline that builds it.

US law is public domain. In *Georgia v. Public.Resource.Org* (2020) the Supreme Court reaffirmed the government-edicts doctrine: statutes, regulations, constitutions, and the official materials legislators produce cannot be copyrighted. Yet clean, structured, bulk access to the **compiled 50-state statutory codes** effectively does not exist in the open — case law (CourtListener, the Caselaw Access Project) and federal law (govinfo USLM XML) are open, but the state codes sit behind commercial APIs. This project publishes that missing layer.

## Status

Published incrementally. **Federal USC + CFR ingestion is here now**; the state statute, regulation, and court-rule scrapers are being added as their public editions land. The [coverage manifest](coverage.yml) tracks what is dump-ready.

## What's here

- **Ingestion scripts** that fetch, parse, and normalize each source from its official government origin into structured JSONL.
- A **coverage manifest** ([`coverage.yml`](coverage.yml)) that gates, per jurisdiction, whether coverage is complete enough to publish.

## What's not here

- **Not current law.** Snapshots are archives as of their date; statutes change continuously. Always verify a section against its official source.
- **Not the retrieval layer.** Embeddings, semantic indexes, the citation graph, and ranking are intentionally out of scope — this repo is the open substrate; those are a separate product.
- **Not legal advice.**

## Repository layout

```
open-us-law/
  scripts/
    federal/       # USC + CFR: download, extract, parse to JSONL
    statutes/      # state statutory codes (being added)
    regulations/   # state administrative codes (being added)
    court_rules/   # state court rules (being added)
  coverage.yml     # per-jurisdiction coverage + dump-ready gate
  data/LICENSE.md  # CC BY 4.0 (for published data snapshots)
  LICENSE          # Apache-2.0 (for the scripts)
```

## Quick start

```bash
pip install -r requirements.txt

# Federal: download the US Code (USLM XML zips), extract, and parse to JSONL
python scripts/federal/download_usc_zips.py --help
python scripts/federal/extract_usc_zips.py --help
python scripts/federal/parse_ecfr_streaming.py --help
```

Each script has a module docstring describing its source, arguments, and output. Every emitted record carries `act_id`, `citation`, the title/chapter/section hierarchy, a breadcrumb, the section `text`, and a `source_url` back to the authoritative government page.

## Coverage

Coverage is uneven, and the manifest is honest about it. A jurisdiction is dump-eligible **only** when `section_count >= floor` **and** `coverage_verified: true`. A high section count does not prove completeness — a code can be missing whole titles and still look large — so `coverage_verified` is set by a human after a completeness audit, never inferred from raw counts. See [`coverage.yml`](coverage.yml).

## Licensing & commercial use

The **law itself is public domain** (US government edicts — *Georgia v. Public.Resource.Org*), so you may use the underlying statutory text freely. On top of that:

- **Scripts** — Apache-2.0 ([`LICENSE`](LICENSE)). Free, including commercial use.
- **Data / compilation** — CC BY 4.0 ([`data/LICENSE.md`](data/LICENSE.md)). Free with attribution.

**Need more than the open snapshot?** Email **contact@vaquill.ai**:

- **Live, always-fresh data + API** — the open dumps are quarterly, point-in-time archives; production systems usually need current law and low-latency access.
- **Retrieval-ready data** — pre-chunked, embedded, and citation-linked for RAG.
- **Bulk delivery, SLA, and support.**
- **Custom coverage** — a jurisdiction, code, or corpus you don't see here.
- **Commercial data license** — an attribution waiver and/or a warranty & indemnity, if CC BY 4.0's "as-is, attribution-required" terms don't fit your compliance needs.

## Contributing

New-jurisdiction parsers, coverage fixes, and normalization improvements are welcome — this is where community help compounds. If you can improve a thin state (see the `hold` / `pending_review` entries in the manifest), open a PR against its ingestion script.

## Provenance

All data derives from official government sources (state legislature and secretary-of-state sites, uscode.house.gov, the eCFR, the Federal Register, GPO govinfo). Each record records the source URL it was ingested from.

## Maintained by

[Vaquill](https://www.vaquill.ai). This open corpus is the substrate; Vaquill's API adds continuous freshness, retrieval, and citation resolution on top of it.

---

*The law is public. Making it usable should be too.*
