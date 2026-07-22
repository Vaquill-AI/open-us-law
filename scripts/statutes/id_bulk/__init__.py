"""Idaho Code bulk ingest from the official legislature.idaho.gov statutes site.

The Idaho scraper (state_scrapers/.../id/statutes/scrapeID.py) only reached ~17
of the 74 titles of the Idaho Code (titles 1-14, 16, 17, 18), leaving whole
codes (incl. Title 19 Criminal Procedure, 32 Domestic Relations, 48 Consumer
Protection, 63 Revenue and Taxation) entirely missing. This package replaces it
with a complete crawl of the Commonwealth's own server-rendered statutes site,
following the same Title -> Chapter -> Section hierarchy the scraper used, so the
whole Code is ingested completely and kept fresh.

Idaho publishes no bulk zip / JSON API (unlike NJ / NY / VA); the authoritative
current source is the official HTML at legislature.idaho.gov/statutesrules/idstat/,
which is exactly what produced the existing Idaho act_ids. This package reuses
``vaquill_pipeline.node_to_payload.node_to_chunks`` so act_id / point_id /
citation / chunking / R2 upload match the scraper path byte-for-byte, and the
act_id scheme (e.g. 18-4003 -> STATE_ID_T18_C40_S18-4003) reproduces the existing
one exactly. Verified against Qdrant by ``id_bulk/verify_act_ids.py`` before any
full run.
"""
