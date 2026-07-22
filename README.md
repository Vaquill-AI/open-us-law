# Open US Law

**Open, structured US primary law - plus the scrapers that build it.**  
State statutory codes, the US Code, the Code of Federal Regulations, state administrative regulations, state and federal constitutions, and court rules - normalized to a single schema, overwhelmingly from official government sources.

US law is public domain. In *Georgia v. Public.Resource.Org* (2020) the Supreme Court reaffirmed the government-edicts doctrine: statutes, regulations, constitutions, and the official materials legislators produce cannot be copyrighted. Yet clean, structured, bulk access to the **compiled 50-state statutory codes** does not exist in the open - case law (CourtListener, the Caselaw Access Project) and federal law (govinfo USLM XML) are open, but the state codes sit behind commercial APIs. This project publishes that missing layer, and the tooling to reproduce it.

## Download the data

The built snapshot is published on Hugging Face. **You do not need to run any of the scrapers below to use it** - they are here so the corpus is reproducible and auditable.

### **[huggingface.co/datasets/vaquill/open-us-law](https://huggingface.co/datasets/vaquill/open-us-law)**

```python
from datasets import load_dataset

ds = load_dataset("vaquill/open-us-law", "statutes", split="train")
ca = load_dataset("vaquill/open-us-law", data_files="us_ca_statutes.parquet")
```

Prefer a direct download? Everything is mirrored on Cloudflare R2 (zero egress, range-request friendly): browse **[oss-data-us.vaquill.ai](https://oss-data-us.vaquill.ai)**, grab the [combined tarball](https://oss-data-us.vaquill.ai/v2026.07/open-us-law-v2026.07-parquet.tar), or read the [manifest](https://oss-data-us.vaquill.ai/index.json).

Snapshot `v2026.07` contains **2,046,009 sections**:

| Corpus | Sections | Jurisdictions |
|---|---:|---|
| State & territorial statutes | 1,983,394 | 50 states + DC + Puerto Rico |
| United States Code | 54,853 | federal |
| Constitutions | 7,762 | 52 |

Parquet, one 24-column schema across every jurisdiction, CC BY 4.0. Sections carry `act_status` (`in_force`, `repealed`, `reserved`, `renumbered`, …), citation, full title/chapter hierarchy, and cross-references into the USC and CFR. New dated snapshots quarterly.

This README doubles as the **table of contents** - the file tree is deep, so every scraper is linked below.

## Contents

- [Download the data](#download-the-data)
- [Quick start](#quick-start)
- [What you get (output format)](#what-you-get-output-format)
- [Coverage & script index](#coverage--script-index)
  - [Federal](#federal)
  - [State statutes (all 50 states)](#state-statutes-all-50-states)
  - [State regulations](#state-regulations)
  - [State court rules](#state-court-rules)
  - [State constitutions](#state-constitutions)
- [Important caveats (proxies, breakage)](#important-caveats-please-read)
- [Licensing & commercial use](#licensing--commercial-use)
- [Contributing](#contributing)

---

## Quick start

```bash
pip install -r requirements.txt

# Federal: US Code (USLM XML) and the eCFR
python scripts/federal/download_usc_zips.py --help
python scripts/federal/parse_ecfr_streaming.py --help

# A state statutory code (Colorado, HTTP - no browser needed):
cd scripts/state_scrapers
OUT_DIR=./data python -m src.scrapers.us.states.co.statutes.scrapeCO
#   -> ./data/us_co_statutes.jsonl   (one JSON object per statutory node)
```

Swap `co` / `scrapeCO` for any state in the table below. Each script is self-documenting - run it with `--help`, or read its module docstring for the exact source and options.

## What you get (output format)

Every scraper writes **JSONL** - one normalized node/section per line - to `$OUT_DIR` (default `./data`). No database, no cloud storage, no credentials. Typical fields:

| Field | Meaning |
|---|---|
| `id` / `act_id` | Stable hierarchical identifier, e.g. `us/co/statutes/title=3/article=1/section=3-1-101` |
| `citation` | Human citation, e.g. `C.R.S. § 3-1-101` |
| `node_name` / `section_title` | Heading of the section |
| `node_text` / `text` | The statutory text |
| `level_classifier` | `jurisdiction` / `corpus` / `title` / `article` / `section` … |
| `link` / `source_url` | Back-link to the authoritative government page |

## Coverage & script index

### Federal

| Source | Script |
|---|---|
| US Code - download USLM XML zips | [download_usc_zips.py](scripts/federal/download_usc_zips.py) |
| US Code - extract zips | [extract_usc_zips.py](scripts/federal/extract_usc_zips.py) |
| Code of Federal Regulations (eCFR) | [parse_ecfr_streaming.py](scripts/federal/parse_ecfr_streaming.py) |
| Federal Register (rules) | [ingest_federal_register_bulk.py](scripts/federal/ingest_federal_register_bulk.py) |
| IRS Internal Revenue Bulletin | [ingest_irs_irb.py](scripts/federal/ingest_irs_irb.py) |
| SSA rulings (SSR/AR) | [ingest_ssa_rulings.py](scripts/federal/ingest_ssa_rulings.py) |
| US Code - GovInfo API downloader | [download_usc.py](scripts/federal/download_usc.py) |
| US Code - parse ZIPs to JSONL | [parse_usc_zip.py](scripts/federal/parse_usc_zip.py) |
| eCFR - API downloader | [download_ecfr.py](scripts/federal/download_ecfr.py) |
| Presidential docs (EOs, proclamations) | [ingest_federal_register_presidential.py](scripts/federal/ingest_federal_register_presidential.py) |
| Public-law cite parser (USC) | [parse_public_law_cites.py](scripts/federal/parse_public_law_cites.py) |
| eCFR authority-cite parser | [parse_authority_citations.py](scripts/federal/parse_authority_citations.py) |

### State statutes (all 50 states)

Run via `cd scripts/state_scrapers && OUT_DIR=./data python -m src.scrapers.us.states.<xx>.statutes.scrape<XX>`. States marked **proxy** geo-restrict non-US IPs - see [caveats](#important-caveats-please-read). A few states also have an **official-source** alternative scraper noted in the last column. All 50 states plus DC and Puerto Rico have complete statutory coverage, with one exception: Pennsylvania, whose Consolidated Statutes are complete but whose older unconsolidated (Purdon's) statutes are a separate backfill. The **Sections** column is the section count in the published `v2026.07` snapshot; the live count is always available from the API.

Many states also have a newer **bulk-source ingester** at [`scripts/statutes/ingest_<state>_bulk.py`](scripts/statutes/) that pulls from an official bulk source (XML zip, API, or PDF) instead of scraping HTML. These share a small pipeline in [`scripts/state_scrapers/vaquill_pipeline/`](scripts/state_scrapers/vaquill_pipeline/) (fetch, chunk, record-build) and per-state parsers in `scripts/statutes/<state>_bulk/`. Run e.g. `OUT_DIR=./data python scripts/statutes/ingest_ny_bulk.py`.

| State | Statute scraper | Sections (v2026.07) | Notes |
|---|---|---|---|
| Alaska (`ak`) | [scrapeAK.py](scripts/state_scrapers/src/scrapers/us/states/ak/statutes/scrapeAK.py) | 17,935 |  |
| Alabama (`al`) | [scrapeAL.py](scripts/state_scrapers/src/scrapers/us/states/al/statutes/scrapeAL.py) | 45,984 | proxy |
| Arkansas (`ar`) | [scrapeAR.py](scripts/state_scrapers/src/scrapers/us/states/ar/statutes/scrapeAR.py) | 36,936 |  |
| Arizona (`az`) | [scrapeAZ.py](scripts/state_scrapers/src/scrapers/us/states/az/statutes/scrapeAZ.py) | 22,674 |  |
| California (`ca`) | [scrapeCA.py](scripts/state_scrapers/src/scrapers/us/states/ca/statutes/scrapeCA.py) | 161,429 |  |
| Colorado (`co`) | [scrapeCO.py](scripts/state_scrapers/src/scrapers/us/states/co/statutes/scrapeCO.py) | 34,231 |  |
| Connecticut (`ct`) | [scrapeCT.py](scripts/state_scrapers/src/scrapers/us/states/ct/statutes/scrapeCT.py) | 16,082 | proxy |
| Delaware (`de`) | [scrapeDE.py](scripts/state_scrapers/src/scrapers/us/states/de/statutes/scrapeDE.py) | 21,649 |  |
| Florida (`fl`) | [scrapeFL.py](scripts/state_scrapers/src/scrapers/us/states/fl/statutes/scrapeFL.py) | 24,866 |  |
| Georgia (`ga`) | [scrapeGA.py](scripts/state_scrapers/src/scrapers/us/states/ga/statutes/scrapeGA.py) | 28,154 |  |
| Hawaii (`hi`) | [scrapeHI.py](scripts/state_scrapers/src/scrapers/us/states/hi/statutes/scrapeHI.py) | 16,446 |  |
| Iowa (`ia`) | [scrapeIA.py](scripts/state_scrapers/src/scrapers/us/states/ia/statutes/scrapeIA.py) | 28,223 |  |
| Idaho (`id`) | [scrapeID.py](scripts/state_scrapers/src/scrapers/us/states/id/statutes/scrapeID.py) | 22,754 |  |
| Illinois (`il`) | [scrapeIL.py](scripts/state_scrapers/src/scrapers/us/states/il/statutes/scrapeIL.py) | 72,456 |  |
| Indiana (`in`) | [scrapeIN.py](scripts/state_scrapers/src/scrapers/us/states/in/statutes/scrapeIN.py) | 83,148 | proxy |
| Kansas (`ks`) | [scrapeKS.py](scripts/state_scrapers/src/scrapers/us/states/ks/statutes/scrapeKS.py) | 24,361 |  |
| Kentucky (`ky`) | [scrapeKY.py](scripts/state_scrapers/src/scrapers/us/states/ky/statutes/scrapeKY.py) | 20,894 |  |
| Louisiana (`la`) | [scrapeLA.py](scripts/state_scrapers/src/scrapers/us/states/la/statutes/scrapeLA.py) | 43,474 |  |
| Massachusetts (`ma`) | [scrapeMA.py](scripts/state_scrapers/src/scrapers/us/states/ma/statutes/scrapeMA.py) | 23,152 |  |
| Maryland (`md`) | [scrapeMD.py](scripts/state_scrapers/src/scrapers/us/states/md/statutes/scrapeMD.py) | 39,552 |  |
| Maine (`me`) | [scrapeME.py](scripts/state_scrapers/src/scrapers/us/states/me/statutes/scrapeME.py) | 25,316 |  |
| Michigan (`mi`) | [scrapeMI.py](scripts/state_scrapers/src/scrapers/us/states/mi/statutes/scrapeMI.py) | 40,658 |  |
| Minnesota (`mn`) | [scrapeMN.py](scripts/state_scrapers/src/scrapers/us/states/mn/statutes/scrapeMN.py) | 27,747 |  |
| Missouri (`mo`) | [scrapeMO.py](scripts/state_scrapers/src/scrapers/us/states/mo/statutes/scrapeMO.py) | 29,296 |  |
| Mississippi (`ms`) | [scrapeMS.py](scripts/state_scrapers/src/scrapers/us/states/ms/statutes/scrapeMS.py) | 158,688 |  |
| Montana (`mt`) | [scrapeMT.py](scripts/state_scrapers/src/scrapers/us/states/mt/statutes/scrapeMT.py) | 30,514 |  |
| North Carolina (`nc`) | [scrapeNC.py](scripts/state_scrapers/src/scrapers/us/states/nc/statutes/scrapeNC.py) | 26,685 |  |
| North Dakota (`nd`) | [scrapeND.py](scripts/state_scrapers/src/scrapers/us/states/nd/statutes/scrapeND.py) | 29,042 |  |
| Nebraska (`ne`) | [scrapeNE.py](scripts/state_scrapers/src/scrapers/us/states/ne/statutes/scrapeNE.py) | 25,997 |  |
| New Hampshire (`nh`) | [scrapeNH.py](scripts/state_scrapers/src/scrapers/us/states/nh/statutes/scrapeNH.py) | 25,375 | proxy |
| New Jersey (`nj`) | [scrapeNJ.py](scripts/state_scrapers/src/scrapers/us/states/nj/statutes/scrapeNJ.py) | 55,897 |  |
| New Mexico (`nm`) | [scrapeNM.py](scripts/state_scrapers/src/scrapers/us/states/nm/statutes/scrapeNM.py) | 34,455 |  |
| Nevada (`nv`) | [scrapeNV.py](scripts/state_scrapers/src/scrapers/us/states/nv/statutes/scrapeNV.py) | 48,190 |  |
| New York (`ny`) | [scrapeNY.py](scripts/state_scrapers/src/scrapers/us/states/ny/statutes/scrapeNY.py) | 40,102 | proxy |
| Ohio (`oh`) | [scrapeOH.py](scripts/state_scrapers/src/scrapers/us/states/oh/statutes/scrapeOH.py) | 33,161 | also [official-source](scripts/statutes/ingest_oh_statutes.py) |
| Oklahoma (`ok`) | [scrapeOK.py](scripts/state_scrapers/src/scrapers/us/states/ok/statutes/scrapeOK.py) | 35,329 |  |
| Oregon (`or`) | [scrapeOR.py](scripts/state_scrapers/src/scrapers/us/states/or/statutes/scrapeOR.py) | 36,202 |  |
| Pennsylvania (`pa`) | [scrapePA.py](scripts/state_scrapers/src/scrapers/us/states/pa/statutes/scrapePA.py) | 14,547 (Consolidated; Purdon's pending) |  |
| Rhode Island (`ri`) | [scrapeRI.py](scripts/state_scrapers/src/scrapers/us/states/ri/statutes/scrapeRI.py) | 21,107 |  |
| South Carolina (`sc`) | [scrapeSC.py](scripts/state_scrapers/src/scrapers/us/states/sc/statutes/scrapeSC.py) | 29,947 |  |
| South Dakota (`sd`) | [scrapeSD.py](scripts/state_scrapers/src/scrapers/us/states/sd/statutes/scrapeSD.py) | 39,589 |  |
| Tennessee (`tn`) | [scrapeTN.py](scripts/state_scrapers/src/scrapers/us/states/tn/statutes/scrapeTN.py) | 32,693 |  |
| Texas (`tx`) | [scrapeTX.py](scripts/state_scrapers/src/scrapers/us/states/tx/statutes/scrapeTX.py) | 122,535 |  |
| Utah (`ut`) | [scrapeUT.py](scripts/state_scrapers/src/scrapers/us/states/ut/statutes/scrapeUT.py) | 25,880 | also [official-source](scripts/statutes/ingest_ut_statutes.py) |
| Virginia (`va`) | [scrapeVA.py](scripts/state_scrapers/src/scrapers/us/states/va/statutes/scrapeVA.py) | 33,856 |  |
| Vermont (`vt`) | [scrapeVT.py](scripts/state_scrapers/src/scrapers/us/states/vt/statutes/scrapeVT.py) | 23,521 |  |
| Washington (`wa`) | [scrapeWA.py](scripts/state_scrapers/src/scrapers/us/states/wa/statutes/scrapeWA.py) | 51,498 |  |
| Wisconsin (`wi`) | [scrapeWI.py](scripts/state_scrapers/src/scrapers/us/states/wi/statutes/scrapeWI.py) | 18,158 |  |
| West Virginia (`wv`) | [scrapeWV.py](scripts/state_scrapers/src/scrapers/us/states/wv/statutes/scrapeWV.py) | 25,460 |  |
| Wyoming (`wy`) | [scrapeWY.py](scripts/state_scrapers/src/scrapers/us/states/wy/statutes/scrapeWY.py) | 10,219 |  |

> Puerto Rico statutes: complete, 23,636 sections, ingested from the official OGP portal (bvirtualogp.pr.gov).

### State regulations

State administrative codes. Some geo-restrict - see [caveats](#important-caveats-please-read).

| State | Regulations scraper |
|---|---|
| Colorado (`co`) | [ingest_co_regulations.py](scripts/regulations/ingest_co_regulations.py) |
| Idaho (`id`) | [ingest_id_regulations.py](scripts/regulations/ingest_id_regulations.py) |
| Illinois (`il`) | [ingest_il_regulations.py](scripts/regulations/ingest_il_regulations.py) |
| Kansas (`ks`) | [ingest_ks_regulations.py](scripts/regulations/ingest_ks_regulations.py) |
| Kentucky (`ky`) | [ingest_ky_regulations.py](scripts/regulations/ingest_ky_regulations.py) |
| Maryland (`md`) | [ingest_md_regulations.py](scripts/regulations/ingest_md_regulations.py) |
| Maine (`me`) | [ingest_me_regulations.py](scripts/regulations/ingest_me_regulations.py) |
| Minnesota (`mn`) | [ingest_mn_regulations.py](scripts/regulations/ingest_mn_regulations.py) |
| New Mexico (`nm`) | [ingest_nm_regulations.py](scripts/regulations/ingest_nm_regulations.py) |
| Ohio (`oh`) | [ingest_oh_regulations.py](scripts/regulations/ingest_oh_regulations.py) |
| South Carolina (`sc`) | [ingest_sc_regulations.py](scripts/regulations/ingest_sc_regulations.py) |
| Virginia (`va`) | [ingest_va_regulations.py](scripts/regulations/ingest_va_regulations.py) |
| Washington (`wa`) | [ingest_wa_regulations.py](scripts/regulations/ingest_wa_regulations.py) |
| Wisconsin (`wi`) | [ingest_wi_regulations.py](scripts/regulations/ingest_wi_regulations.py) |

### State court rules

| State | Court-rules scraper |
|---|---|
| Arizona (`az`) | [ingest_az_court_rules.py](scripts/court_rules/ingest_az_court_rules.py) |
| Maryland (`md`) | [ingest_md_court_rules.py](scripts/court_rules/ingest_md_court_rules.py) |
| Minnesota (`mn`) | [ingest_mn_court_rules.py](scripts/court_rules/ingest_mn_court_rules.py) |
| Nevada (`nv`) | [ingest_nv_court_rules.py](scripts/court_rules/ingest_nv_court_rules.py) |
| New York (`ny`) | [ingest_ny_court_rules.py](scripts/court_rules/ingest_ny_court_rules.py) |
| Florida (`fl`) | [ingest_fl_court_rules.py](scripts/court_rules/ingest_fl_court_rules.py) |
| Texas (`tx`) | [ingest_tx_court_rules.py](scripts/court_rules/ingest_tx_court_rules.py) |
| New Jersey (`nj`) | [ingest_nj_court_rules.py](scripts/court_rules/ingest_nj_court_rules.py) |
| Multi-state (CA, MT, …) | [ingest_state_court_rules.py](scripts/court_rules/ingest_state_court_rules.py) |

### State constitutions

| Source | Script |
|---|---|
| 50 state constitutions | [ingest_state_constitutions.py](scripts/constitutions/ingest_state_constitutions.py) |
| US Constitution + federal court rules | [ingest_const_rules_v2.py](scripts/constitutions/ingest_const_rules_v2.py) |

---

## Important caveats (please read)

**1. Some sources need a US IP / proxy.** A number of state sites geo-restrict non-US traffic or throttle aggressively. Those scripts read `WEBSHARE_USERNAME` / `WEBSHARE_PASSWORD` (a US rotating proxy) from the environment - see [`.env.example`](.env.example) - or you can run from a US host. State **statute** scrapers currently needing a proxy: **AL, CT, IN, NH, NY** (several regulation scrapers too, e.g. MN, WA, WI). If a run returns almost nothing, geo-blocking is the usual cause.

**2. Some scripts may stop working over time.** These scrapers target **live government websites**. Those sites get redesigned, move URLs, change HTML, or add anti-bot measures - so a scraper that worked at publish time can break later. When that happens it usually needs a small parser update, not a rewrite. If you hit one, please [open an issue or PR](#contributing); fixes to individual state parsers are exactly where community help compounds.

**3. A browser is needed for a few states.** Most states use plain HTTP (`requests`/`BeautifulSoup`). A handful render statutes via JavaScript and use **Selenium** - you'll need Chrome/Chromium + `chromedriver` on your `PATH` for those. If a scraper imports Selenium and no driver is found, that's why.

**4. Snapshots are point-in-time, not current law.** Statutes change continuously. Output is an archive as of the run date - **always verify a section against its official source** before relying on it. This is **not legal advice**.

## Licensing & commercial use

The **law itself is public domain** (US government edicts - *Georgia v. Public.Resource.Org*). On top of that:

- **Scripts** - Apache-2.0 ([`LICENSE`](LICENSE)). Free, including commercial use.
- **Data / compilation** - CC BY 4.0 ([`data/LICENSE.md`](data/LICENSE.md)). Free with attribution.

**Need more than the open scrapers?** Email **contact@vaquill.ai**:

- **Live, always-fresh data + API** - no scraping, no breakage; current law, low latency.
- **Retrieval-ready data** - pre-chunked, embedded, and citation-linked for RAG.
- **Bulk delivery, SLA, and support.**
- **Custom coverage** - a jurisdiction or corpus you don't see here.
- **Commercial data license** - an attribution waiver and/or warranty & indemnity, if CC BY 4.0's terms don't fit your compliance needs.

## Contributing

New-jurisdiction parsers, coverage fixes, and - especially - **repairs to state scrapers that broke when a government site changed** are welcome. Open a PR against the relevant script in the tables above.

## Provenance

Most data derives from official government sources (state legislature / secretary-of-state sites, uscode.house.gov, the eCFR, the Federal Register, GPO govinfo), and those records keep the exact source URL they were ingested from.

A minority of state statutory codes were obtained from commercial aggregators rather than an official publisher. In the published dataset those records carry **no** `source_url` rather than linking to a third party, and the per-jurisdiction table in the [dataset card](https://huggingface.co/datasets/vaquill/open-us-law) marks them. We would rather state that plainly than imply the whole corpus is officially sourced. The retrieval layer (embeddings, semantic index, citation graph) is intentionally out of scope here.

## Maintained by

[Vaquill AI](https://www.vaquill.ai). This open corpus is the substrate; Vaquill AI's API adds continuous freshness, retrieval, and citation resolution on top of it.

---

*The law is public. Making it usable should be too.*
