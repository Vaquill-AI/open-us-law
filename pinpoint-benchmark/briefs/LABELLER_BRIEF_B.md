# Subsection labelling task

You are building an answer key. For each statute section in your input file, list its subsections exactly as the text prints them.
Work only from the text. Do not look at any other file, repo or tool except to read your input and write your output.

## What counts as a subsection node
A node is a subdivision the section's own text labels: `(a)`, `(1)`, `(i)`, `(A)`, `(1m)`, `1.`, `a.`, `A.`, `Subd. 2.`, `Subsection 3`.
Inline run-in lists count when each item starts with its own label: `(1) one thing, (2) another`.
Not nodes: the section number and title at the top; a trailing History/credit/source/amendment note; cross-references in running prose (`under sub. (2)(a)`, `section 5(b)`); a label that only appears inside a quoted rule or a cited statute.

## Path
The printed labels from outermost to innermost, joined by `/`, with brackets, periods and words like Subd. stripped, case kept as printed.
`(a)` under `(1)` is `1/a`. A `(i)` under `(a)` under `(1)` is `1/a/i`. `Subd. 2.` is `2`. `(1m)` is `1m`.
A child nests under the nearest earlier node of the next shallower level. If a section restarts `(a)` after `(2)`, that `(a)` is `2/a`.
If the text starts mid-list (no `(1)` printed), use the labels as printed.
If a section has no subdivisions, give an empty list.
If a label is genuinely ambiguous (for example `(i)` could be a letter or a roman numeral), choose the reading that makes the sequence consistent and add `"unsure": true` to that node.

## head
The first 8 words of the node's own text after its label, copied verbatim from the input. Not the label itself.

## Output
Write ONE JSON file with the Write tool at the path you are given. Shape:
{"<act_id>": [{"path": "1", "head": "first eight words of the text"}, {"path": "1/a", "head": "..."}], "<act_id2>": []}
Every act_id in the input must appear. Document order. No commentary in the file.
Then reply with one line: the file path and how many sections you labelled.

## Extra rules for this second pass (these settle questions the first pass left open)
- A subdivision that prints a label and then only "Repealed", "Renumbered" or a short note still COUNTS as a node. Include it. If the same label is printed twice, list both, in order.
- An inline enumeration counts when it is clearly a list of separate items, each starting with its own `(1)`, `(2)` label, even inside one long sentence. Deeper levels inside a node (for example `(i)`, `(ii)` inside `(a)`) count too, at every depth.
- Numbered items inside a quoted form or affidavit template that is part of the statute's text count only if they carry a label of the same style as the statute's own list; if you are unsure, include them with `"unsure": true`.
- Be exhaustive about depth: go through each node's text and check for nested labels before moving on.
