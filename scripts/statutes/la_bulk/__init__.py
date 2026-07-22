"""Louisiana statutes bulk ingest via the official legis.la.gov site.

The old LA scraper (state_scrapers/.../la/statutes/scrapeLA.py) pulled from the
FindLaw mirror and reached only ~10,216 sections: the Revised Statutes were a
quarter present (6,077 of ~46,000) and the Code of Civil Procedure was almost
entirely missing (21 of ~1,250 articles). This package replaces it with the
State Legislature's own Laws site, which publishes every body completely.

Louisiana is a civil-law state, so its statutes span the Revised Statutes
(Title:Section) PLUS separate article-numbered codes. Six statute bodies are
covered here (the Constitution + Constitution Ancillaries stay in the separate
constitution corpus; the Louisiana Administrative Code is regulations, not
statutes; House/Senate/Joint Rules are not statutes):

    RS   Revised Statutes            La. Rev. Stat. § {title}:{section}
    CC   Civil Code                  La. Civ. Code art. {n}
    CCP  Code of Civil Procedure     La. Code Civ. Proc. art. {n}
    CCRP Code of Criminal Procedure  La. Code Crim. Proc. art. {n}
    CE   Code of Evidence            La. Code Evid. art. {n}
    CHC  Children's Code             La. Ch. Code art. {n}

Source model (all GET, no postbacks needed for the leaves):
  - Enumeration: each body is a set of ``Laws_Toc.aspx?folder=<id>`` pages that
    list every section as a flat ``Law.aspx?d=<docid>`` anchor. Revised Statutes
    have one folder per title (54 present); each code body is a single folder.
    Folders are discovered by scanning the folder-id range and keeping those
    whose ``LabelHeader`` names one of the six bodies, so no folder id is
    hard-coded.
  - Body: each ``Law.aspx?d=<docid>`` page carries ``LabelName`` (e.g.
    ``RS 14:30``) for the citation identity and ``LabelDocument`` (a run of
    ``<p>`` blocks) for the text.

Reusing ``vaquill_pipeline.node_to_payload.node_to_chunks`` means act_id /
point_id / citation / chunking / R2 upload match the scraper path exactly. The
node id path ``code={slug}/(title=.../section=... | article=...)`` reproduces the
existing LA act_id scheme (e.g. ``STATE_LA_Crevised-statutes_T14_S30``).
"""
