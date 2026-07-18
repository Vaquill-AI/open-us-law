# Open US Law

**Open, structured US primary law - plus the scrapers that build it.**  
State statutory codes, the US Code, the Code of Federal Regulations, state administrative regulations, state and federal constitutions, and court rules - normalized to JSONL, from official government sources only.

US law is public domain. In *Georgia v. Public.Resource.Org* (2020) the Supreme Court reaffirmed the government-edicts doctrine: statutes, regulations, constitutions, and the official materials legislators produce cannot be copyrighted. Yet clean, structured, bulk access to the **compiled 50-state statutory codes** does not exist in the open - case law (CourtListener, the Caselaw Access Project) and federal law (govinfo USLM XML) are open, but the state codes sit behind commercial APIs. This project publishes that missing layer, and the tooling to reproduce it.

This README doubles as the **table of contents** - the file tree is deep, so every scraper is linked below.

## Contents

- [Quick start](#quick-start)
- [What you get (output format)](#what-you-get-output-format)
- [Coverage & script index](#coverage--script-index)
  - [Federal](#federal)
  - [State statutes (all 50 states)](#state-statutes-all-50-states)
  - [State regulations](#state-regulations)
  - [State court rules](#state-court-rules)
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

### State statutes (all 50 states)

Run via `cd scripts/state_scrapers && OUT_DIR=./data python -m src.scrapers.us.states.<xx>.statutes.scrape<XX>`. States marked **proxy** geo-restrict non-US IPs - see [caveats](#important-caveats-please-read). A few states also have an **official-source** alternative scraper noted in the last column.

| State | Statute scraper | Notes |
|---|---|---|
| Alaska (`ak`) | [scrapeAK.py](scripts/state_scrapers/src/scrapers/us/states/ak/statutes/scrapeAK.py) |  |
| Alabama (`al`) | [scrapeAL.py](scripts/state_scrapers/src/scrapers/us/states/al/statutes/scrapeAL.py) | proxy |
| Arkansas (`ar`) | [scrapeAR.py](scripts/state_scrapers/src/scrapers/us/states/ar/statutes/scrapeAR.py) |  |
| Arizona (`az`) | [scrapeAZ.py](scripts/state_scrapers/src/scrapers/us/states/az/statutes/scrapeAZ.py) |  |
| California (`ca`) | [scrapeCA.py](scripts/state_scrapers/src/scrapers/us/states/ca/statutes/scrapeCA.py) |  |
| Colorado (`co`) | [scrapeCO.py](scripts/state_scrapers/src/scrapers/us/states/co/statutes/scrapeCO.py) |  |
| Connecticut (`ct`) | [scrapeCT.py](scripts/state_scrapers/src/scrapers/us/states/ct/statutes/scrapeCT.py) | proxy |
| Delaware (`de`) | [scrapeDE.py](scripts/state_scrapers/src/scrapers/us/states/de/statutes/scrapeDE.py) |  |
| Florida (`fl`) | [scrapeFL.py](scripts/state_scrapers/src/scrapers/us/states/fl/statutes/scrapeFL.py) |  |
| Georgia (`ga`) | [scrapeGA.py](scripts/state_scrapers/src/scrapers/us/states/ga/statutes/scrapeGA.py) |  |
| Hawaii (`hi`) | [scrapeHI.py](scripts/state_scrapers/src/scrapers/us/states/hi/statutes/scrapeHI.py) |  |
| Iowa (`ia`) | [scrapeIA.py](scripts/state_scrapers/src/scrapers/us/states/ia/statutes/scrapeIA.py) |  |
| Idaho (`id`) | [scrapeID.py](scripts/state_scrapers/src/scrapers/us/states/id/statutes/scrapeID.py) |  |
| Illinois (`il`) | [scrapeIL.py](scripts/state_scrapers/src/scrapers/us/states/il/statutes/scrapeIL.py) |  |
| Indiana (`in`) | [scrapeIN.py](scripts/state_scrapers/src/scrapers/us/states/in/statutes/scrapeIN.py) | proxy |
| Kansas (`ks`) | [scrapeKS.py](scripts/state_scrapers/src/scrapers/us/states/ks/statutes/scrapeKS.py) |  |
| Kentucky (`ky`) | [scrapeKY.py](scripts/state_scrapers/src/scrapers/us/states/ky/statutes/scrapeKY.py) |  |
| Louisiana (`la`) | [scrapeLA.py](scripts/state_scrapers/src/scrapers/us/states/la/statutes/scrapeLA.py) |  |
| Massachusetts (`ma`) | [scrapeMA.py](scripts/state_scrapers/src/scrapers/us/states/ma/statutes/scrapeMA.py) |  |
| Maryland (`md`) | [scrapeMD.py](scripts/state_scrapers/src/scrapers/us/states/md/statutes/scrapeMD.py) |  |
| Maine (`me`) | [scrapeME.py](scripts/state_scrapers/src/scrapers/us/states/me/statutes/scrapeME.py) |  |
| Michigan (`mi`) | [scrapeMI.py](scripts/state_scrapers/src/scrapers/us/states/mi/statutes/scrapeMI.py) |  |
| Minnesota (`mn`) | [scrapeMN.py](scripts/state_scrapers/src/scrapers/us/states/mn/statutes/scrapeMN.py) |  |
| Missouri (`mo`) | [scrapeMO.py](scripts/state_scrapers/src/scrapers/us/states/mo/statutes/scrapeMO.py) |  |
| Mississippi (`ms`) | [scrapeMS.py](scripts/state_scrapers/src/scrapers/us/states/ms/statutes/scrapeMS.py) |  |
| Montana (`mt`) | [scrapeMT.py](scripts/state_scrapers/src/scrapers/us/states/mt/statutes/scrapeMT.py) |  |
| North Carolina (`nc`) | [scrapeNC.py](scripts/state_scrapers/src/scrapers/us/states/nc/statutes/scrapeNC.py) |  |
| North Dakota (`nd`) | [scrapeND.py](scripts/state_scrapers/src/scrapers/us/states/nd/statutes/scrapeND.py) |  |
| Nebraska (`ne`) | [scrapeNE.py](scripts/state_scrapers/src/scrapers/us/states/ne/statutes/scrapeNE.py) |  |
| New Hampshire (`nh`) | [scrapeNH.py](scripts/state_scrapers/src/scrapers/us/states/nh/statutes/scrapeNH.py) | proxy |
| New Jersey (`nj`) | [scrapeNJ.py](scripts/state_scrapers/src/scrapers/us/states/nj/statutes/scrapeNJ.py) |  |
| New Mexico (`nm`) | [scrapeNM.py](scripts/state_scrapers/src/scrapers/us/states/nm/statutes/scrapeNM.py) |  |
| Nevada (`nv`) | [scrapeNV.py](scripts/state_scrapers/src/scrapers/us/states/nv/statutes/scrapeNV.py) |  |
| New York (`ny`) | [scrapeNY.py](scripts/state_scrapers/src/scrapers/us/states/ny/statutes/scrapeNY.py) | proxy |
| Ohio (`oh`) | [scrapeOH.py](scripts/state_scrapers/src/scrapers/us/states/oh/statutes/scrapeOH.py) | also [official-source](scripts/statutes/ingest_oh_statutes.py) |
| Oklahoma (`ok`) | [scrapeOK.py](scripts/state_scrapers/src/scrapers/us/states/ok/statutes/scrapeOK.py) |  |
| Oregon (`or`) | [scrapeOR.py](scripts/state_scrapers/src/scrapers/us/states/or/statutes/scrapeOR.py) |  |
| Pennsylvania (`pa`) | [scrapePA.py](scripts/state_scrapers/src/scrapers/us/states/pa/statutes/scrapePA.py) |  |
| Rhode Island (`ri`) | [scrapeRI.py](scripts/state_scrapers/src/scrapers/us/states/ri/statutes/scrapeRI.py) |  |
| South Carolina (`sc`) | [scrapeSC.py](scripts/state_scrapers/src/scrapers/us/states/sc/statutes/scrapeSC.py) |  |
| South Dakota (`sd`) | [scrapeSD.py](scripts/state_scrapers/src/scrapers/us/states/sd/statutes/scrapeSD.py) |  |
| Tennessee (`tn`) | [scrapeTN.py](scripts/state_scrapers/src/scrapers/us/states/tn/statutes/scrapeTN.py) |  |
| Texas (`tx`) | [scrapeTX.py](scripts/state_scrapers/src/scrapers/us/states/tx/statutes/scrapeTX.py) |  |
| Utah (`ut`) | [scrapeUT.py](scripts/state_scrapers/src/scrapers/us/states/ut/statutes/scrapeUT.py) | also [official-source](scripts/statutes/ingest_ut_statutes.py) |
| Virginia (`va`) | [scrapeVA.py](scripts/state_scrapers/src/scrapers/us/states/va/statutes/scrapeVA.py) |  |
| Vermont (`vt`) | [scrapeVT.py](scripts/state_scrapers/src/scrapers/us/states/vt/statutes/scrapeVT.py) |  |
| Washington (`wa`) | [scrapeWA.py](scripts/state_scrapers/src/scrapers/us/states/wa/statutes/scrapeWA.py) |  |
| Wisconsin (`wi`) | [scrapeWI.py](scripts/state_scrapers/src/scrapers/us/states/wi/statutes/scrapeWI.py) |  |
| West Virginia (`wv`) | [scrapeWV.py](scripts/state_scrapers/src/scrapers/us/states/wv/statutes/scrapeWV.py) |  |
| Wyoming (`wy`) | [scrapeWY.py](scripts/state_scrapers/src/scrapers/us/states/wy/statutes/scrapeWY.py) |  |

> Puerto Rico statutes: [ingest_pr_codes.py](scripts/statutes/ingest_pr_codes.py) (LexJuris PDFs).

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
| Multi-state (CA, MT, …) | [ingest_state_court_rules.py](scripts/court_rules/ingest_state_court_rules.py) |

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

All data derives from official government sources (state legislature / secretary-of-state sites, uscode.house.gov, the eCFR, the Federal Register, GPO govinfo). Each record keeps the source URL it was ingested from. The retrieval layer (embeddings, semantic index, citation graph) is intentionally out of scope here.

## Maintained by

[Vaquill AI](https://www.vaquill.ai). This open corpus is the substrate; Vaquill AI's API adds continuous freshness, retrieval, and citation resolution on top of it.

---

*The law is public. Making it usable should be too.*
