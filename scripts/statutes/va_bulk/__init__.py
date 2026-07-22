"""Virginia Code bulk ingest via the official law.lis.virginia.gov JSON API.

The VA scraper (state_scrapers/.../va/statutes/scrapeVA.py) only reached 27 of
the ~76 titles of the Code of Virginia. This package replaces it with the
Commonwealth's own structured JSON web service, which returns the full
Title -> Subtitle -> Part -> Chapter -> Article -> SubPart -> Section hierarchy
plus per-section body HTML, so the whole Code can be ingested completely and
kept fresh by construction.
"""
