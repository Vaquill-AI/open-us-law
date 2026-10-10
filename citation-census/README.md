# US Legal Citation Census

Every non-case legal citation that appears in 10,745,929 US court opinions, with how often it is cited.
Statutes, the U.S. Code, regulations, court rules (federal, state and local), constitutions, session laws and administrative registers.
Case citations (U.S., F.2d, So. 2d and the rest), treatises and Westlaw or Lexis cites are not included.

**2,950,812 distinct citations, 35,006,606 occurrences, in 331 CSV files.**

## Layout

```
citations/<category>/<jurisdiction>.csv     most-cited first; files over 250,000 rows are split as <jurisdiction>-001.csv, -002.csv
stats/by_type.csv  stats/by_jurisdiction.csv  stats/summary.json
stats/top_cited/<category>.csv               the 1,000 most-cited citations per category
samples/<category>.csv                       100 random rows per category, to look at before downloading
MANIFEST.json  SHA256SUMS                    every file with its row count, size and checksum
```

Categories: `statutes`, `court-rules`, `regulations`, `constitutions`, `administrative-registers`, `session-laws`.
Jurisdiction files use the two-letter state code in lower case, `federal` for U.S.-level law, and `unknown` where neither the citation nor the court says.

## Columns

| column | meaning |
| --- | --- |
| `citation` | normalised citation, pinpoints removed (`18 U.S.C. § 1956`, not `18 U.S.C. § 1956(a)(1)(B)`) |
| `citation_key` | the citation lower-cased with spacing collapsed. Variants that differ only in case or spacing (`Tex. Fam. Code Ann. § 161.001` and `TEX. FAM. CODE ANN. § 161.001`) are separate rows; group by this key to merge them. `opinions` and `courts` cannot be summed across variants |
| `type` | Statute, Court Rule, Regulation, Constitution, Session Law or Admin. Register |
| `subtype` | for example `usc`, `cfr`, `federal_rule`, `local_rule`, `state_statute`, `state_regulation`, `federal_register` |
| `jurisdiction` | two-letter code or `US`. A guess: taken from the citation when it names a jurisdiction, otherwise from the deciding court's state |
| `occurrences` | times the citation appears across all opinions |
| `opinions` | distinct opinions that cite it |
| `courts` | distinct courts whose opinions cite it |
| `first_year`, `last_year` | earliest and latest decision year citing it |
| `resolved` | `true` if Vaquill's citation resolver returned a section for it (run 2026-10-10) |
| `answer_kind` | `exact`, `container` (a chapter or rule group), `multiple` (a range or several candidates), `repaired` (a known miscitation corrected), `register_reference`, or `unresolved` |

`resolved` describes our resolver at one date and is not a statement about the law.
`unresolved` includes law we do not hold, repealed or renumbered law, old numbering, malformed citations and extraction errors.
Internal section identifiers and source URLs are deliberately not published.

## What is and is not in it

- The extractor flags each citation `strong` or weak. Only strong ones are published: 496,867 weak fragments, 2,344 known artifacts such as court docket headers, and 23 strings that carried a publisher parenthetical, a database cite or a URL were left out.
- Extraction is not perfect. Expect roughly 80 to 85 percent recall and 92 to 98 percent precision on unseen text.
  It misses a bare `§ 1983` with no code named, rules cited without their set, and named acts with no code, and it wrongly keeps some treatises that look like codes.
- The jurisdiction column is a guess for citations that do not name one.
- No opinion text, parties, judges or opinion identifiers are included, only citation strings and counts.

## Source

Opinions come from CourtListener, a project of Free Law Project, read through a self-hosted mirror.
Extractor version 0.6.0, built 2026-10-10.

## Licence and attribution

This folder does not carry a data licence of its own yet. The repository `LICENSE` (Apache-2.0) covers the code.
The opinions behind these counts come from CourtListener, a project of Free Law Project. Please attribute: "Derived from CourtListener, a project of Free Law Project".
