# Licence proposal and source check

Licence: answer key (paths, offsets, heads, ids, strata) and the compilation of texts under CC BY-NC 4.0, consistent with the open-us-law data licence from v2026.09; `score.py`, `verify.py` and `tools/` under Apache-2.0.
The statutory text itself is a state-government edict (*Georgia v. Public.Resource.Org*, 2020) and NC does not reach it; the card must say so.

## Per-state source check

Checked 2026-10-30 with a read-only look at the Qdrant payload of every one of the 600 sampled sections: `source_url` host, `is_snapshot`, `official_status`, `document_type`.
The corpus-release rule is that third-party-licensed rows (the RegLab snapshot, filtered on `is_snapshot=True`) and vendor-hosted sources are excluded or outline-only.

| State | source_url host (all 120 sections) | is_snapshot | Vendor or third party | Outcome |
| --- | --- | --- | --- | --- |
| MN | www.revisor.mn.gov | none set | no, Office of the Revisor | text included |
| IA | www.legis.iowa.gov | none set | no, the legislature | text included |
| MO | revisor.mo.gov | none set | no, Joint Committee / Revisor | text included |
| WI | docs.legis.wisconsin.gov | none set | no, the legislature | text included |
| MI | www.legislature.mi.gov | none set | no, the legislature | text included |

All five are official legislature or revisor hosts, none on the vendor-platform list, and none is a RegLab snapshot row, so no state falls to outline-only or exclusion.
A scan of every shipped text for aggregator brands (Justia, FindLaw, Lexis, Westlaw, Casetext, Cornell) found none.
Not verified here (no external probing was done): each state's own site terms of use or any state claim to copyright in annotations or compilation.
`data_outline_only/` is shipped alongside as the fallback if a state's text is later withdrawn.
