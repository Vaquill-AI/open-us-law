"""Missouri Revised Statutes (RSMo) bulk ingest via the official revisor.mo.gov site.

The MO scraper (state_scrapers/.../mo/statutes/scrapeMO.py) reached only ~385
statute sections of the full Revised Statutes of Missouri. The cause was an
enumeration bug, not a text-extraction bug: ``_scrape_chapter`` did
``outer.find("table")`` and iterated only the FIRST table on each chapter page,
while revisor.mo.gov groups a chapter's sections across several tables (one per
subchapter heading). Every section past the first table was silently dropped.

This package replaces that path. It walks the Commonwealth's own surfaces:

    Home.aspx (title groups -> chapter links)
      -> OneChapter.aspx?chapter=N   (ALL section rows across ALL tables)
      -> OneSection.aspx?section=S   (section body paragraphs + history note)

Missouri publishes no bulk statute export (only House bill XML), so the official
per-section HTML is the authoritative source, fetched through the US proxy the
same way ingest_va_bulk.py fetches Virginia.

act_id reproduction is exact by construction: the node id path is
``us/mo/statutes/chapter={chapter}/section={section}`` (no title level, matching
the scraper) so ``node_to_chunks`` rebuilds ``STATE_MO_C{chapter}_S{section}``
byte-for-byte. The chapter is the section number's integer prefix (RSMo numbers
sections ``{chapter}.{rest}``, e.g. 565.020 is in chapter 565), which is also the
chapter page each section is enumerated from, so the two always agree.
Citation format: ``Mo. Rev. Stat. § {section}``.
"""
