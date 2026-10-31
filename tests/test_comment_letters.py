"""Replies to SEC comment letters (shared/utils/comment_letters.py)."""
from datetime import date

from services.agents.comment_letter_job import replies
from shared.utils import comment_letters as cl


def test_severity_and_topics():
    assert cl.severity(cl.Reading(quote="x" * 30, topics=["revenue_recognition"], restates=True)) == 1.0
    assert cl.severity(cl.Reading(quote="x" * 30, topics=["non_gaap", "other"])) == 0.5
    assert cl.severity(cl.Reading(quote="x" * 30, topics=["disclosure_only"])) == 0.25
    assert cl.severity(cl.Reading(quote="x" * 30, topics=["procedural"])) == 0.0
    assert cl.Reading(topics=["Revenue_Recognition", "made up"]).topics == ["revenue_recognition"]


def test_the_quote_must_be_in_the_reply():
    text = "In response to the Staff's comment, the Company will revise its non-GAAP presentation in future filings."
    assert cl.grounded(cl.Reading(quote="the Company will revise its non-GAAP presentation"), text)
    assert not cl.grounded(cl.Reading(quote="the Company will restate its 2024 financial statements"), text)


def test_only_text_replies_since_the_cutoff():
    rec = {"form": ["CORRESP", "UPLOAD", "CORRESP", "CORRESP"], "filingDate": ["2025-02-13", "2025-01-10", "2020-01-01",
                                                                           "2025-03-01"],
           "accessionNumber": ["a", "b", "c", "d"], "primaryDocument": ["r.htm", "l.pdf", "old.htm", "scan.pdf"]}
    assert replies(rec, date(2023, 1, 1)) == [(date(2025, 2, 13), "a", "r.htm")]
