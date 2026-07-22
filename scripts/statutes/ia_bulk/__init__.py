"""Iowa Code bulk ingest via the official legis.iowa.gov per-chapter slim XML.

The Iowa scraper (state_scrapers/.../ia/statutes/scrapeIA.py) fetched one RTF per
section over a latency-bound crawl and only ever completed 6 of Iowa's 16 titles
(I, IV, V, VI, VII, VIII), leaving whole codes missing, including Title XVI
(Criminal Law and Procedure, e.g. chapter 707 murder), Title XII (Business
Entities), Title XIV (Property), Title XV (Judicial Procedures), Title XI
(Natural Resources), Title II (Elections), Title IX (Local Government), Title X
(Financial Resources), and Title XIII (Commerce).

This package replaces that with the Legislature's own structured bulk surface:
one ``slim`` XML per chapter at
``/docs/publications/ICC/{year}/attachments/{chapter}_slim.xml``. Each XML
carries the chapter's full section list, per-section headnote, nested body
paragraphs, and amendment history, so the whole Code is ingested completely from
~1,400 chapter fetches instead of ~19,000 per-section RTF fetches.

Reusing ``vaquill_pipeline.node_to_payload.node_to_chunks`` means act_id /
point_id / citation / chunking / R2 upload match the scraper path exactly. The
act_id scheme (Title -> Chapter -> Section, e.g. 707.2 ->
STATE_IA_TXVI_C707_S707.2) is reproduced against Qdrant by verify_act_ids.py
before any full run.
"""
