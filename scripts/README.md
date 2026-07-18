# Scripts

Each script fetches a source from its **official government origin**, parses it, and
writes **JSONL** — one normalized section per line. Nothing here uploads anywhere or
depends on hosted infrastructure; you run it, you get files.

## Output contract

Every emitted record uses the same shape:

| Field | Meaning |
|-------|---------|
| `act_id` | Stable identifier encoding the hierarchy, e.g. `USC_T42_C21_S1983` |
| `citation` | Human citation, e.g. `42 U.S.C. § 1983` |
| `corpus_type` | `usc`, `cfr`, `state_statute`, `state_regulation`, `court_rule`, … |
| `jurisdiction` / `state` | `federal`, or a 2-letter state/territory code |
| title / chapter / section | The parsed hierarchy fields |
| `breadcrumb` | Ordered hierarchy for display |
| `text` | The section text |
| `source_url` | Link back to the authoritative government page |

## Layout

- **`federal/`** — US Code (download → extract → parse) and the eCFR.
- **`statutes/`** — state statutory codes *(being added)*.
- **`regulations/`** — state administrative codes *(being added)*.
- **`court_rules/`** — state court rules *(being added)*.

## Running

Each script is standalone and self-documenting — start with `--help`:

```bash
python scripts/federal/download_usc_zips.py --help
python scripts/federal/extract_usc_zips.py --help
python scripts/federal/parse_ecfr_streaming.py --help
```

Some state sources are geo-restricted and require a US proxy; those scripts read
`WEBSHARE_PROXY_HOST` / `WEBSHARE_PROXY_PORT` / `WEBSHARE_PROXY_USER` /
`WEBSHARE_PROXY_PWD` from the environment (see `.env.example`). No credentials are
hardcoded anywhere.
