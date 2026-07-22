"""Louisiana section/article model + node id path and citation construction.

A ``LASection`` is built from one ``Law.aspx`` page: its ``LabelName`` gives the
body prefix and number (``RS 14:30``, ``CC 1``, ``CCP 1``, ...) and its
``LabelDocument`` gives the text. The node id path reproduces the existing LA
act_id scheme (a ``code=`` level first, matching the old FindLaw scrape):

    RS   us/la/statutes/code=revised-statutes/title={title}/section={section}
           -> STATE_LA_Crevised-statutes_T{title}_S{section}   La. Rev. Stat. § {title}:{section}
    CC   us/la/statutes/code=civil-code/article={n}
           -> STATE_LA_Ccivil-code_A{n}                        La. Civ. Code art. {n}
    CCP  code=code-of-civil-procedure/article={n}              La. Code Civ. Proc. art. {n}
    CCRP code=code-of-criminal-procedure/article={n}           La. Code Crim. Proc. art. {n}
    CE   code=code-of-evidence/article={n}                     La. Code Evid. art. {n}
    CHC  code=childrens-code/article={n}                       La. Ch. Code art. {n}

Revised-Statutes sub-numbered sections use dots on legis.la.gov (14:30.1,
14:95.1.1); the old FindLaw scrape wrote dashes (14:95-1). So the RS act_ids
reproduce the base sections but not the dotted sub-sections byte-for-byte, and
the article codes cannot reproduce the FindLaw roman-numeral title at all. That
is why the cutover reconciles state-scoped (document_type=statute), gated by a
normalized citation-overlap check, rather than act_id-scoped.
"""

from __future__ import annotations

from dataclasses import dataclass

COUNTRY = "us"
STATE = "la"
CORPUS = "statutes"


# body prefix -> (LabelHeader on the TOC page, code slug, citation style, citation prefix)
#   style "rs"      -> "La. Rev. Stat. § {title}:{section}"
#   style "article" -> "{prefix} art. {number}"
BODIES: dict[str, dict] = {
    "RS": {
        "header": "Revised Statutes",
        "slug": "revised-statutes",
        "style": "rs",
        "prefix": "La. Rev. Stat.",
    },
    "CC": {
        "header": "Civil Code",
        "slug": "civil-code",
        "style": "article",
        "prefix": "La. Civ. Code",
    },
    "CCP": {
        "header": "Code of Civil Procedure",
        "slug": "code-of-civil-procedure",
        "style": "article",
        "prefix": "La. Code Civ. Proc.",
    },
    "CCRP": {
        "header": "Code of Criminal Procedure",
        "slug": "code-of-criminal-procedure",
        "style": "article",
        "prefix": "La. Code Crim. Proc.",
    },
    "CE": {
        "header": "Code of Evidence",
        "slug": "code-of-evidence",
        "style": "article",
        "prefix": "La. Code Evid.",
    },
    "CHC": {
        "header": "Children's Code",
        "slug": "childrens-code",
        "style": "article",
        "prefix": "La. Ch. Code",
    },
}

# LabelHeader string -> body prefix, for classifying a TOC folder to a body.
HEADER_TO_PREFIX: dict[str, str] = {v["header"]: k for k, v in BODIES.items()}


@dataclass(frozen=True)
class LASection:
    body: str  # RS / CC / CCP / CCRP / CE / CHC
    title: str  # Revised Statutes title number (RS only), else ""
    number: str  # section number (RS) or article number (codes)
    heading: str  # section/article heading text (node_name)

    @property
    def _cfg(self) -> dict:
        return BODIES[self.body]

    def citation(self) -> str:
        cfg = self._cfg
        if cfg["style"] == "rs":
            return f"{cfg['prefix']} § {self.title}:{self.number}"
        return f"{cfg['prefix']} art. {self.number}"

    def node_id_pairs(self) -> list[tuple[str, str]]:
        cfg = self._cfg
        pairs: list[tuple[str, str]] = [("code", cfg["slug"])]
        if cfg["style"] == "rs":
            pairs.append(("title", self.title))
            pairs.append(("section", self.number))
        else:
            pairs.append(("article", self.number))
        return pairs

    def node_id(self) -> str:
        head = f"{COUNTRY}/{STATE}/{CORPUS}"
        tail = "/".join(f"{cls_}={num}" for cls_, num in self.node_id_pairs())
        return f"{head}/{tail}"

    def top_level_title(self) -> str:
        """What node_to_chunks records as top_level_title.

        Revised Statutes carry a real numeric title; the article codes have no
        title level, so the body slug stands in (mirrors the old scrape, which
        used the code slug as top_level_title for the flat article codes).
        """
        return self.title if self._cfg["style"] == "rs" else self._cfg["slug"]
