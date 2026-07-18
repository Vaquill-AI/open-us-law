"""West Virginia Code scraper.

Source: https://code.wvlegislature.gov/  (also reachable via
https://www.wvlegislature.gov/wvcode/, which 301s to the same select-based TOC).
Hierarchy: chapter -> article -> section
URL patterns:
    TOC:     https://code.wvlegislature.gov/
    Chapter: https://code.wvlegislature.gov/{chapter}/
    Article: https://code.wvlegislature.gov/{chapter}-{article}/
    Section: https://code.wvlegislature.gov/{chapter}-{article}-{section}/

Citation format: W. Va. Code SS {chapter}-{article}-{section}
"""
from __future__ import annotations

import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

current_file = Path(__file__).resolve()
src_directory = current_file.parent
while src_directory.name != "src" and src_directory.parent != src_directory:
    src_directory = src_directory.parent
project_root = src_directory.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.utils.pydanticModels import Addendum, AddendumType, Node, NodeText
from src.utils.scrapingHelpers import (
    get_url_as_soup,
    insert_jurisdiction_and_corpus_node,
    insert_node,
)

COUNTRY = "us"
JURISDICTION = "wv"
CORPUS = "statutes"
TABLE_NAME = f"{COUNTRY}_{JURISDICTION}_{CORPUS}"

BASE_URL = "https://code.wvlegislature.gov"
# www.wvlegislature.gov/wvcode/ and www.wvlegislature.gov/WVCODE/Code.cfm both
# 301-redirect to code.wvlegislature.gov/, so we always hit the canonical host.
TOC_URLS = (
    f"{BASE_URL}/",
    "https://www.wvlegislature.gov/wvcode/",
)

RESERVED_KEYWORDS = ["repealed", "expired", "reserved", "renumbered", "transferred"]

# Last-resort fallback — verified against the live TOC select on 2026-05-12
# (138 chapters). Only consulted when *all* live TOC URLs fail. Kept current
# so a network outage cannot silently shrink the corpus by 18 chapters as the
# previous 131-entry snapshot did.
_FALLBACK_CHAPTERS: list[tuple[str, str]] = [
    ("1", "CHAPTER 1. THE STATE AND ITS SUBDIVISIONS."),
    ("2", "CHAPTER 2. COMMON LAW, STATUTES, LEGAL HOLIDAYS, DEFINITIONS AND LEGAL CAPACITY."),
    ("3", "CHAPTER 3. ELECTIONS."),
    ("4", "CHAPTER 4. THE LEGISLATURE."),
    ("5", "CHAPTER 5. GENERAL POWERS AND AUTHORITY OF THE GOVERNOR, SECRETARY OF STATE AND ATTORNEY GENERAL; BOARD OF PUBLIC WORKS; MISCELLANEOUS AGENCIES, COMMISSIONS, OFFICES, PROGRAMS, ETC."),
    ("5A", "CHAPTER 5A. DEPARTMENT OF ADMINISTRATION."),
    ("5B", "CHAPTER 5B. ECONOMIC DEVELOPMENT ACT OF 1985."),
    ("5C", "CHAPTER 5C. BASIC ASSISTANCE FOR INDUSTRY AND TRADE."),
    ("5D", "CHAPTER 5D. PUBLIC ENERGY AUTHORITY ACT."),
    ("5E", "CHAPTER 5E. VENTURE CAPITAL COMPANY."),
    ("5F", "CHAPTER 5F. REORGANIZATION OF THE EXECUTIVE BRANCH OF STATE GOVERNMENT."),
    ("5G", "CHAPTER 5G. PROCUREMENT OF ARCHITECT-ENGINEER SERVICES BY STATE AND ITS SUBDIVISIONS."),
    ("5H", "CHAPTER 5H. SURVIVOR BENEFITS."),
    ("6", "CHAPTER 6. GENERAL PROVISIONS RESPECTING OFFICERS."),
    ("6A", "CHAPTER 6A. EXECUTIVE AND JUDICIAL SUCCESSION."),
    ("6B", "CHAPTER 6B. PUBLIC OFFICERS AND EMPLOYEES; ETHICS; CONFLICTS OF INTEREST; FINANCIAL DISCLOSURE."),
    ("6C", "CHAPTER 6C. PUBLIC EMPLOYEES."),
    ("6D", "CHAPTER 6D. PUBLIC CONTRACTS."),
    ("7", "CHAPTER 7. COUNTY COMMISSIONS AND OFFICERS."),
    ("7A", "CHAPTER 7A. CONSOLIDATED LOCAL GOVERNMENT."),
    ("8", "CHAPTER 8. MUNICIPAL CORPORATIONS."),
    ("8A", "CHAPTER 8A. LAND USE PLANNING."),
    ("9", "CHAPTER 9. HUMAN SERVICES."),
    ("9A", "CHAPTER 9A. VETERANS' AFFAIRS."),
    ("10", "CHAPTER 10. PUBLIC LIBRARIES; PUBLIC RECREATION; ATHLETIC ESTABLISHMENTS; MONUMENTS AND MEMORIALS; ROSTER OF SERVICEMEN; EDUCATIONAL BROADCASTING AUTHORITY."),
    ("11", "CHAPTER 11. TAXATION."),
    ("11A", "CHAPTER 11A. COLLECTION AND ENFORCEMENT OF PROPERTY TAXES."),
    ("11B", "CHAPTER 11B. DEPARTMENT OF REVENUE."),
    ("12", "CHAPTER 12. PUBLIC MONEYS AND SECURITIES."),
    ("13", "CHAPTER 13. PUBLIC BONDED INDEBTEDNESS."),
    ("14", "CHAPTER 14. CLAIMS DUE AND AGAINST THE STATE."),
    ("15", "CHAPTER 15. PUBLIC SAFETY."),
    ("15A", "CHAPTER 15A. DEPARTMENT OF HOMELAND SECURITY."),
    ("16", "CHAPTER 16. PUBLIC HEALTH."),
    ("16A", "CHAPTER 16A. MEDICAL CANNABIS ACT."),
    ("16B", "CHAPTER 16B. INSPECTOR GENERAL."),
    ("17", "CHAPTER 17. ROADS AND HIGHWAYS."),
    ("17A", "CHAPTER 17A. MOTOR VEHICLE ADMINISTRATION, REGISTRATION, CERTIFICATE OF TITLE, AND ANTITHEFT PROVISIONS."),
    ("17B", "CHAPTER 17B. MOTOR VEHICLE DRIVER'S LICENSES."),
    ("17C", "CHAPTER 17C. TRAFFIC REGULATIONS AND LAWS OF THE ROAD."),
    ("17D", "CHAPTER 17D. MOTOR VEHICLE SAFETY RESPONSIBILITY LAW."),
    ("17E", "CHAPTER 17E. UNIFORM COMMERCIAL DRIVER'S LICENSE ACT."),
    ("17F", "CHAPTER 17F. ALL-TERRAIN VEHICLES."),
    ("17G", "CHAPTER 17G. RACIAL PROFILING DATA COLLECTION ACT."),
    ("17H", "CHAPTER 17H. FULLY AUTONOMOUS VEHICLE ACT."),
    ("18", "CHAPTER 18. EDUCATION."),
    ("18A", "CHAPTER 18A. SCHOOL PERSONNEL."),
    ("18B", "CHAPTER 18B. HIGHER EDUCATION."),
    ("18C", "CHAPTER 18C. STUDENT LOANS; SCHOLARSHIPS AND STATE AID."),
    ("19", "CHAPTER 19. AGRICULTURE."),
    ("20", "CHAPTER 20. NATURAL RESOURCES."),
    ("21", "CHAPTER 21. LABOR"),
    ("21A", "CHAPTER 21A. UNEMPLOYMENT COMPENSATION."),
    ("22", "CHAPTER 22. ENVIRONMENTAL RESOURCES."),
    ("22A", "CHAPTER 22A. MINERS' HEALTH, SAFETY AND TRAINING."),
    ("22B", "CHAPTER 22B. ENVIRONMENTAL BOARDS."),
    ("22C", "CHAPTER 22C. ENVIRONMENTAL RESOURCES; BOARDS, AUTHORITIES, COMMISSIONS AND COMPACTS."),
    ("23", "CHAPTER 23. WORKERS' COMPENSATION."),
    ("24", "CHAPTER 24. PUBLIC SERVICE COMMISSION."),
    ("24A", "CHAPTER 24A. COMMERCIAL MOTOR CARRIERS."),
    ("24B", "CHAPTER 24B. GAS PIPELINE SAFETY."),
    ("24C", "CHAPTER 24C. UNDERGROUND FACILITIES DAMAGE PREVENTION."),
    ("24D", "CHAPTER 24D. CABLE TELEVISION."),
    ("24E", "CHAPTER 24E. STATEWIDE ADDRESSING AND MAPPING."),
    ("24F", "CHAPTER 24F. VETERANS' GRAVE MARKERS."),
    ("25", "CHAPTER 25. DIVISION OF CORRECTIONS."),
    ("26", "CHAPTER 26. STATE HEALTH FACILITIES."),
    ("27", "CHAPTER 27. MENTALLY ILL PERSONS."),
    ("28", "CHAPTER 28. STATE CORRECTIONAL AND PENAL INSTITUTIONS."),
    ("29", "CHAPTER 29. MISCELLANEOUS BOARDS AND OFFICERS."),
    ("29A", "CHAPTER 29A. STATE ADMINISTRATIVE PROCEDURES ACT."),
    ("29B", "CHAPTER 29B. FREEDOM OF INFORMATION."),
    ("29C", "CHAPTER 29C. UNIFORM NOTARY ACT."),
    ("30", "CHAPTER 30. PROFESSIONS AND OCCUPATIONS."),
    ("31", "CHAPTER 31. CORPORATIONS."),
    ("31A", "CHAPTER 31A. BANKS AND BANKING."),
    ("31B", "CHAPTER 31B. UNIFORM LIMITED LIABILITY COMPANY ACT."),
    ("31C", "CHAPTER 31C. CREDIT UNIONS."),
    ("31D", "CHAPTER 31D. WEST VIRGINIA BUSINESS CORPORATION ACT."),
    ("31E", "CHAPTER 31E. WEST VIRGINIA NONPROFIT CORPORATION ACT."),
    ("31F", "CHAPTER 31F. WEST VIRGINIA BENEFIT CORPORATION ACT."),
    ("31G", "CHAPTER 31G. BROADBAND ENHANCEMENT AND EXPANSION POLICIES."),
    ("31H", "CHAPTER 31H. SMALL WIRELESS FACILITIES DEPLOYMENT ACT."),
    ("31I", "CHAPTER 31I. TRUST COMPANIES."),
    ("31J", "CHAPTER 31J. WIRELESS TOWER FACILITIES."),
    ("32", "CHAPTER 32. UNIFORM SECURITIES ACT."),
    ("32A", "CHAPTER 32A. LAND SALES; FALSE ADVERTISING; ISSUANCE AND SALE OF CHECKS, DRAFTS, MONEY ORDERS, ETC."),
    ("32B", "CHAPTER 32B. THE WEST VIRGINIA COMMODITIES ACT."),
    ("33", "CHAPTER 33. INSURANCE."),
    ("34", "CHAPTER 34. ESTRAYS, DRIFT AND DERELICT PROPERTY."),
    ("35", "CHAPTER 35. PROPERTY OF RELIGIOUS, EDUCATIONAL AND CHARITABLE ORGANIZATIONS."),
    ("35A", "CHAPTER 35A. NAMES, EMBLEMS, ETC., OF ASSOCIATIONS, LODGES, ETC."),
    ("36", "CHAPTER 36. ESTATES AND PROPERTY."),
    ("36A", "CHAPTER 36A. CONDOMINIUMS AND UNIT PROPERTY."),
    ("36B", "CHAPTER 36B. UNIFORM COMMON INTEREST OWNERSHIP ACT."),
    ("37", "CHAPTER 37. REAL PROPERTY."),
    ("37A", "CHAPTER 37A. ZONING."),
    ("37B", "CHAPTER 37B. MINERAL DEVELOPMENT."),
    ("37C", "CHAPTER 37C. MINERAL DEVELOPMENT."),
    ("38", "CHAPTER 38. LIENS."),
    ("39", "CHAPTER 39. RECORDS AND PAPERS."),
    ("39A", "CHAPTER 39A. ELECTRONIC COMMERCE."),
    ("39B", "CHAPTER 39B. UNIFORM POWER OF ATTORNEY ACT."),
    ("40", "CHAPTER 40. ACTS VOID AS TO CREDITORS AND PURCHASERS."),
    ("41", "CHAPTER 41. WILLS."),
    ("42", "CHAPTER 42. DESCENT AND DISTRIBUTION."),
    ("43", "CHAPTER 43. DOWER AND VALUATION OF LIFE ESTATES."),
    ("44", "CHAPTER 44. ADMINISTRATION OF ESTATES AND TRUSTS."),
    ("44A", "CHAPTER 44A. WEST VIRGINIA GUARDIANSHIP AND CONSERVATORSHIP ACT."),
    ("44B", "CHAPTER 44B. UNIFORM PRINCIPAL AND INCOME ACT."),
    ("44C", "CHAPTER 44C. UNIFORM ADULT GUARDIANSHIP AND PROTECTIVE PROCEEDINGS JURISDICTION ACT."),
    ("44D", "CHAPTER 44D. UNIFORM TRUST CODE."),
    ("45", "CHAPTER 45. SURETYSHIP AND GUARANTY."),
    ("46", "CHAPTER 46. UNIFORM COMMERCIAL CODE."),
    ("46A", "CHAPTER 46A. WEST VIRGINIA CONSUMER CREDIT AND PROTECTION ACT."),
    ("46B", "CHAPTER 46B. REGULATION OF THE RENTAL OF CONSUMER GOODS UNDER RENT-TO-OWN AGREEMENTS."),
    ("47", "CHAPTER 47. REGULATION OF TRADE."),
    ("47A", "CHAPTER 47A. WEST VIRGINIA LENDING AND CREDIT RATE BOARD."),
    ("47B", "CHAPTER 47B. UNIFORM PARTNERSHIP ACT."),
    ("48", "CHAPTER 48. DOMESTIC RELATIONS."),
    ("49", "CHAPTER 49. CHILD WELFARE."),
    ("50", "CHAPTER 50. MAGISTRATE COURTS."),
    ("51", "CHAPTER 51. COURTS AND THEIR OFFICERS."),
    ("52", "CHAPTER 52. JURIES."),
    ("53", "CHAPTER 53. EXTRAORDINARY REMEDIES."),
    ("54", "CHAPTER 54. EMINENT DOMAIN."),
    ("55", "CHAPTER 55. ACTIONS, SUITS AND ARBITRATION; JUDICIAL SALE."),
    ("56", "CHAPTER 56. PLEADING AND PRACTICE."),
    ("57", "CHAPTER 57. EVIDENCE AND WITNESSES."),
    ("58", "CHAPTER 58. APPEAL AND ERROR."),
    ("59", "CHAPTER 59. FEES, ALLOWANCES AND COSTS; NEWSPAPERS; LEGAL ADVERTISEMENTS."),
    ("60", "CHAPTER 60. STATE CONTROL OF ALCOHOLIC LIQUORS."),
    ("60A", "CHAPTER 60A. UNIFORM CONTROLLED SUBSTANCES ACT."),
    ("60B", "CHAPTER 60B. DONATED DRUG REPOSITORY PROGRAM."),
    ("61", "CHAPTER 61. CRIMES AND THEIR PUNISHMENT."),
    ("62", "CHAPTER 62. CRIMINAL PROCEDURE."),
    ("63", "CHAPTER 63. REPEAL OF STATUTES."),
    ("64", "CHAPTER 64. LEGISLATIVE RULES."),
]


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def main() -> None:
    corpus_node: Node = insert_jurisdiction_and_corpus_node(COUNTRY, JURISDICTION, CORPUS)
    scrape_all_chapters(corpus_node)


# ---------------------------------------------------------------------------
# Resume tracking (chapter-level)
# ---------------------------------------------------------------------------


def _chapters_done_path():
    from vaquill_pipeline.config import SETTINGS

    return SETTINGS.chunks_dir / "state_wv_chapters_done.txt"


def _load_chapters_done() -> set[str]:
    try:
        path = _chapters_done_path()
    except Exception:
        return set()
    if not path.exists():
        return set()
    return {line.strip() for line in path.read_text().splitlines() if line.strip()}


def _mark_chapter_done(number: str) -> None:
    try:
        path = _chapters_done_path()
    except Exception:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a") as fh:
        fh.write(f"{number}\n")
        fh.flush()


# ---------------------------------------------------------------------------
# Chapter-level scrape
# ---------------------------------------------------------------------------


def _discover_chapters_live() -> list[tuple[str, str]]:
    """Try each TOC URL until one yields chapters from the sel-chapter select.

    Returns an empty list if every live URL fails. Caller falls back to
    _FALLBACK_CHAPTERS only in that case.
    """
    for toc_url in TOC_URLS:
        try:
            soup = get_url_as_soup(toc_url)
        except Exception as e:
            print(f"[scrapeWV] live TOC failed at {toc_url}: {e!r}", flush=True)
            continue
        select = soup.find("select", id="sel-chapter")
        if select is None:
            continue
        chapters: list[tuple[str, str]] = []
        for option in select.find_all("option"):
            value = (option.get("value") or "").strip()
            if not value:
                continue
            name = _clean_text(option.get_text())
            chapters.append((value, name))
        if chapters:
            print(f"[scrapeWV] live TOC: {len(chapters)} chapters from {toc_url}", flush=True)
            return chapters
    return []


def scrape_all_chapters(corpus_node: Node) -> None:
    """Discover all chapters live from the TOC dropdown and scrape each one.

    Concurrency is set by env var VAQUILL_TITLE_WORKERS (default 8); WV calls
    its top-level units "chapters" but we reuse the DE knob so ops stays
    consistent across state scrapers.

    Resume: completed chapters are persisted in state_wv_chapters_done.txt
    and skipped on rerun. Set VAQUILL_FORCE_RESCRAPE=1 to override.
    """
    chapter_list = _discover_chapters_live()
    if not chapter_list:
        print(
            f"[scrapeWV] WARNING: live TOC unreachable; using stale fallback "
            f"({len(_FALLBACK_CHAPTERS)} chapters). Verify network access.",
            flush=True,
        )
        chapter_list = _FALLBACK_CHAPTERS

    done = set() if os.environ.get("VAQUILL_FORCE_RESCRAPE") else _load_chapters_done()
    if done:
        print(f"[scrapeWV] resume: {len(done)} chapters already done", flush=True)

    work: list[Node] = []
    for ch_number, ch_name in chapter_list:
        link = f"{BASE_URL}/{ch_number}/"
        node_id = f"{corpus_node.node_id}/chapter={ch_number}"
        status = _check_reserved(ch_name)

        chapter_node = Node(
            id=node_id,
            link=link,
            top_level_title=ch_number,
            node_type="structure",
            level_classifier="chapter",
            number=ch_number,
            node_name=ch_name,
            parent=corpus_node.node_id,
            status=status,
        )
        # Insert the chapter structure node up front (idempotent).
        insert_node(chapter_node, TABLE_NAME, ignore_duplicate=True, debug_mode=False)

        if status:
            continue
        if ch_number in done:
            continue
        work.append(chapter_node)

    def _do_chapter(node: Node):
        try:
            scrape_chapter(node)
            _mark_chapter_done(str(node.number))
            return (node.number, "ok", None)
        except Exception as e:  # noqa: BLE001
            return (node.number, "fail", str(e)[:200])

    workers = int(os.environ.get("VAQUILL_TITLE_WORKERS", "8"))
    print(
        f"[scrapeWV] running {len(work)} chapters with {workers} parallel workers",
        flush=True,
    )
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for fut in as_completed(ex.submit(_do_chapter, n) for n in work):
            num, status, err = fut.result()
            if status == "fail":
                print(f"[scrapeWV] chapter {num}: {status}: {err}", flush=True)
            else:
                print(f"[scrapeWV] chapter {num}: {status}", flush=True)


# ---------------------------------------------------------------------------
# Article-level scrape
# ---------------------------------------------------------------------------


def scrape_chapter(chapter_node: Node) -> None:
    """Fetch a chapter page and iterate over its article links."""
    soup = get_url_as_soup(str(chapter_node.link))

    art_heads = soup.find_all(class_="art-head")
    for ah in art_heads:
        a_tag = ah.find("a")
        if a_tag is None:
            continue

        href: str = a_tag["href"].strip()
        art_name: str = _clean_text(ah.get_text())

        # href may be absolute (e.g. from Wayback) or relative. Normalise to
        # the canonical live URL.
        art_url = _canonical_url(href)

        # Derive article number from URL: .../1-3/ -> "3"
        art_number = _article_number_from_url(art_url, str(chapter_node.number))
        if art_number is None:
            continue

        node_id = f"{chapter_node.node_id}/article={art_number}"
        status = _check_reserved(art_name)

        article_node = Node(
            id=node_id,
            link=art_url,
            top_level_title=chapter_node.top_level_title,
            node_type="structure",
            level_classifier="article",
            number=art_number,
            node_name=art_name,
            parent=chapter_node.node_id,
            status=status,
        )
        insert_node(article_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)

        if not status:
            scrape_article(article_node)


# ---------------------------------------------------------------------------
# Section-level scrape
# ---------------------------------------------------------------------------


def scrape_article(article_node: Node) -> None:
    """Fetch an article page and scrape each section it lists."""
    soup = get_url_as_soup(str(article_node.link))

    for sec_head in soup.find_all(class_="sec-head"):
        a_tag = sec_head.find("a")
        if a_tag is None:
            continue

        href: str = a_tag["href"].strip()
        sec_name: str = _clean_text(sec_head.get_text())

        if not sec_name:
            continue

        sec_url = _canonical_url(href)

        # Section number: last path segment of .../1-1-3/ -> "3"
        sec_number = _section_number_from_url(sec_url)
        if sec_number is None:
            continue

        # Full section identifier for citation, e.g. "1-1-3"
        sec_id_str = _section_id_from_url(sec_url)
        citation = f"W. Va. Code § {sec_id_str}"

        node_id = f"{article_node.node_id}/section={sec_number}"
        status = _check_reserved(sec_name)

        if status:
            section_node = Node(
                id=node_id,
                link=sec_url,
                top_level_title=article_node.top_level_title,
                node_type="content",
                level_classifier="section",
                number=sec_number,
                node_name=sec_name,
                parent=article_node.node_id,
                citation=citation,
                status=status,
            )
            insert_node(section_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)
            continue

        node_text, addendum = _fetch_section_content(sec_url)

        section_node = Node(
            id=node_id,
            link=sec_url,
            top_level_title=article_node.top_level_title,
            node_type="content",
            level_classifier="section",
            number=sec_number,
            node_name=sec_name,
            parent=article_node.node_id,
            citation=citation,
            node_text=node_text,
            addendum=addendum,
        )
        insert_node(section_node, TABLE_NAME, ignore_duplicate=True, debug_mode=True)


# ---------------------------------------------------------------------------
# Section content fetch
# ---------------------------------------------------------------------------


def _fetch_section_content(url: str):
    """Return (NodeText | None, Addendum | None) for a single section URL."""
    soup = get_url_as_soup(url)

    # The section text lives in <div class="sectiontext hid"> (or just
    # "sectiontext"). It contains an <h4> header and <p> paragraphs.
    sec_div = soup.find(class_="sectiontext")
    if sec_div is None:
        return None, None

    node_text = NodeText()
    history_text = ""

    for element in sec_div.find_all(recursive=False):
        tag = element.name
        cls_list = element.get("class", [])
        cls_str = " ".join(cls_list)

        # Skip the heading (it duplicates node_name)
        if tag == "h4":
            continue

        # History paragraph if explicitly classed
        if "history" in cls_str or "sec-history" in cls_str:
            history_text = _clean_text(element.get_text(separator=" "))
            continue

        raw = element.get_text(separator=" ")
        text = _clean_text(raw)
        if text:
            node_text.add_paragraph(text=text)

    addendum = None
    if history_text:
        addendum = Addendum()
        addendum.history = AddendumType(type="history", text=history_text)

    return node_text, addendum


# ---------------------------------------------------------------------------
# URL utilities
# ---------------------------------------------------------------------------


def _canonical_url(href: str) -> str:
    """Normalise a possibly-Wayback href to the canonical live URL."""
    # Wayback Machine rewrites hrefs like:
    #   /web/20250116042435/https://code.wvlegislature.gov/1-1/
    # Strip the archive prefix.
    m = re.search(r"https?://code\.wvlegislature\.gov(/[^\"']*)", href)
    if m:
        return BASE_URL + m.group(1)
    # Relative href from the live site: /1-1/ or /1-1-1/
    if href.startswith("/"):
        return BASE_URL + href
    # Already absolute live URL
    if href.startswith("https://code.wvlegislature.gov"):
        return href
    return href


# Path component: digits then optional uppercase suffix (e.g. "5A", "17H").
_PATH_TOKEN = r"\d+[A-Z]?"


def _article_number_from_url(url: str, chapter_number: str) -> str | None:
    """Extract article number from a URL like .../1-3/  ->  '3'."""
    m = re.search(rf"/(?:{_PATH_TOKEN})-({_PATH_TOKEN})/?$", url)
    if m:
        return m.group(1)
    return None


def _section_number_from_url(url: str) -> str | None:
    """Extract section number from a URL like .../1-1-3/  ->  '3'."""
    m = re.search(rf"/(?:{_PATH_TOKEN})-(?:{_PATH_TOKEN})-({_PATH_TOKEN})/?$", url)
    if m:
        return m.group(1)
    return None


def _section_id_from_url(url: str) -> str:
    """Extract full section id from a URL like .../1-1-3/  ->  '1-1-3'."""
    m = re.search(rf"/({_PATH_TOKEN}-{_PATH_TOKEN}-{_PATH_TOKEN})/?$", url)
    if m:
        return m.group(1)
    # Fallback: strip BASE_URL and slashes
    path = url.replace(BASE_URL, "").strip("/")
    return path


# ---------------------------------------------------------------------------
# Text helpers
# ---------------------------------------------------------------------------


def _clean_text(raw: str) -> str:
    text = (
        raw.replace("﻿", "")
        .replace("\xa0", " ")
        .replace(" ", " ")
        .replace("§", "§")
    )
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _check_reserved(text: str) -> str | None:
    lower = text.lower()
    for kw in RESERVED_KEYWORDS:
        if kw in lower:
            return "reserved"
    return None


if __name__ == "__main__":
    main()
