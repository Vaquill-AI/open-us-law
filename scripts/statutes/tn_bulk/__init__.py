"""Tennessee Code Annotated bulk ingest from Justia, fetched via ScrapFly.

Justia Cloudflare-403s our own residential proxy; ScrapFly's `asp=true` datacenter
bypass (reused from `ar_bulk.client.scrapfly_html`, ~1 credit/page, `cache=true`)
returns the real HTML past Cloudflare, so both the TOC walk and every section
render deterministically. See ingest_tn_bulk.py for orchestration and rationale.
"""
