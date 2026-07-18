# State statute scrapers (all 50 states)

Scrapers for the compiled statutory code of every US state. Each builds
structured `Node` objects and writes them as JSONL via the local sink in
`src/utils/utilityFunctions.py` — no database, no cloud storage, no credentials.

## Run one state

```bash
cd scripts/state_scrapers
OUT_DIR=./data python -m src.scrapers.us.states.ut.statutes.scrapeUT
# -> ./data/us_ut_statutes.jsonl  (one JSON object per statutory node)
```

Swap `ut` / `scrapeUT` for any state (e.g. `ca` / `scrapeCA`).

## Notes

- **Selenium states** (AZ, CA, DE, FL, HI, IL, KY, NM, SD, UT, VA) need
  Chrome/Chromium + chromedriver on PATH. The other 39 use plain HTTP.
- **Geo-restricted sources**: some state sites block non-US IPs. Set
  `WEBSHARE_USERNAME` / `WEBSHARE_PASSWORD` (a US rotating proxy) or run from a
  US host. See `.env.example`.
- Output: one JSONL file per state at `$OUT_DIR/us_<state>_statutes.jsonl`,
  one normalized statutory node per line.
