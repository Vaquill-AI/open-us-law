"""Indiana Code (IC) official-bulk-HTML ingest package.

Source: the state's own "Current Indiana Code (all titles): ZIP (HTML only)"
export from iga.in.gov/laws/ic/downloads (one HTML file per title). We parse it
rather than scrape the bot-walled SPA or call the token-gated API.

Modules:
  walk   - INSection model + Title-Article-Chapter-Section node_id / act_id path
  parse  - parse a title HTML file into (section_id, heading, body, status)

See ingest_in_bulk.py for the orchestrator.
"""
