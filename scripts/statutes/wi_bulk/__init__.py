"""Wisconsin Statutes bulk ingest from the official docs.legis.wisconsin.gov site.

See ``ingest_wi_bulk.py`` for the orchestrator and the module docstring for the
source model. Parser modules:

    walk.py    -- WISection model + node_id / act_id path construction
    parse.py   -- HTML -> section bodies (flat qsatxt_* grouping), anchors, history
    client.py  -- proxied fetch of chapter/section HTML pages and chapter PDFs
    verify_act_ids.py -- act_id reproduction gate + PDF-TOC completeness cross-check
"""
