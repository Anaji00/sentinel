"""Filing change (Lazy Prices): shared/utils/filing_change.py."""
from datetime import date

from shared.utils import filing_change as fc

BASE = " ".join(f"word{i % 300} revenue customers products services" for i in range(400))
RISK = " ".join(f"risk{i % 200} competition regulation supply chain" for i in range(300))


def doc(risk_body: str, extra: str = "") -> str:
    return ("Table of contents Item 1A. Risk Factors 12 Item 2. Properties 30 " + BASE + extra
            + " Item 1A. Risk Factors " + risk_body + " Item 1B. Unresolved Staff Comments none Item 2. Properties hq")


def test_an_unchanged_filing_scores_zero_and_a_changed_one_more():
    same = fc.change(doc(RISK), doc(RISK))
    assert same == (0.0, 0.0)
    new_risk = RISK + " " + " ".join(f"litigation{i} bankruptcy going concern" for i in range(300))
    whole, risk = fc.change(doc(RISK), doc(new_risk))
    assert whole > 0 and risk > whole                    # the change is concentrated in Item 1A


def test_the_last_heading_is_the_section_not_the_contents_entry():
    body = fc.risk_section(doc(RISK))
    assert body.strip().startswith("risk0") and "Unresolved" not in body
    assert fc.risk_section("no headings here") == ""


def test_a_10q_has_no_risk_measure_and_short_documents_none():
    assert fc.change(doc(RISK), doc(RISK), risk=False)[1] is None
    assert fc.change("short text", "short text") == (None, None)


def test_pairs_compare_like_with_like():
    k1 = ("10-K", date(2024, 2, 1), date(2023, 12, 31), "k1")
    k2 = ("10-K", date(2025, 2, 1), date(2024, 12, 31), "k2")
    q1 = ("10-Q", date(2024, 5, 1), date(2024, 3, 31), "q1")
    q2 = ("10-Q", date(2024, 8, 1), date(2024, 6, 30), "q2")
    q5 = ("10-Q", date(2025, 5, 1), date(2025, 3, 31), "q5")
    got = {(a[3], b[3]) for a, b in fc.pairs([k1, k2, q1, q2, q5])}
    assert got == {("k1", "k2"), ("q1", "q5")}           # q2 has no same quarter a year before


def test_filings_from_the_recent_block_and_older_pages():
    subs = {"filings": {"recent": {"form": ["10-K", "8-K", "10-Q"], "filingDate": ["2025-02-01", "2025-03-01", "2019-05-01"],
                                   "reportDate": ["2024-12-31", "", "2019-03-31"], "accessionNumber": ["a", "b", "c"],
                                   "primaryDocument": ["k.htm", "e.htm", "q.htm"]},
                        "files": [{"name": "p1.json", "filingFrom": "2015-01-01", "filingTo": "2023-06-30"},
                                  {"name": "p0.json", "filingFrom": "2010-01-01", "filingTo": "2014-12-31"}]}}
    since = date(2022, 1, 1)
    assert [f[0][3] for f in fc.recent_filings(subs, since)] == ["a"]
    assert fc.older_pages(subs, since) == ["p1.json"]
    page = {"form": ["10-Q"], "filingDate": ["2023-05-01"], "reportDate": ["2023-03-31"], "accessionNumber": ["d"],
            "primaryDocument": ["q.htm"]}
    assert [f[0][3] for f in fc.recent_filings(page, since)] == ["d"]
