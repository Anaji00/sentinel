"""Surveillance in the federal courts: cases, judges, and disclosed ties to vendors."""

import pathlib
import sys
from datetime import date

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import courts as ct  # noqa: E402

SEARCH = {"results": [
    {"docket_id": 74942289, "caseName": "Murkerson v. Flock Group, Inc.", "court_id": "gand", "dateFiled": "2026-10-08",
     "dateTerminated": None, "assignedTo": "Leigh Martin May", "assigned_to_id": 2089,
     "suitNature": "440 Civil Rights: Other", "docketNumber": "1:26-cv-01",
     "docket_absolute_url": "/docket/74942289/murkerson-v-flock-group-inc/"},
    {"docket_id": None, "caseName": "broken"},
    {"docket_id": 1, "caseName": "Unassigned v. X", "assigned_to_id": None, "assignedTo": None},
]}


def test_search_results_become_cases() -> None:
    cases = ct.parse_search(SEARCH, "plate readers")
    assert [c.docket_id for c in cases] == [74942289, 1]
    c = cases[0]
    assert (c.judge, c.judge_id, c.court_id, c.date_filed) == ("Leigh Martin May", 2089, "gand", date(2026, 10, 8))
    assert c.url == "https://www.courtlistener.com/docket/74942289/murkerson-v-flock-group-inc/"
    assert cases[1].judge_id is None and cases[1].date_filed is None


def test_disclosure_lines_are_classified_by_vendor() -> None:
    assert ct.vendor_of("Axon Enterprises Inc (AAXN)") == "Axon"
    assert ct.vendor_of("TASER International") == "Axon"
    assert ct.vendor_of("Motorola Solutions Inc (MSI)") == "Motorola Solutions"
    assert ct.vendor_of("Motorola, common stock") == "Motorola Solutions"
    assert ct.vendor_of("Motorola Mobility bond") is None          # not the surveillance business
    assert ct.vendor_of("RELX PLC ADR") == "RELX (LexisNexis)"
    assert ct.vendor_of("Schwab Moderate Track - Equity Portfolio") is None
    body = {"results": [
        {"id": 1, "description": "Axon Enterprises Inc (AAXN)", "financial_disclosure": f"{ct.API}/financial-disclosures/32255/",
         "gross_value_code": "K", "redacted": False},
        {"id": 2, "description": "Axon Enterprises Inc (AAXN)", "financial_disclosure": f"{ct.API}/financial-disclosures/9/",
         "gross_value_code": None, "redacted": True},
        {"id": 3, "description": "Bank of America", "financial_disclosure": f"{ct.API}/financial-disclosures/9/"},
    ]}
    assert ct.parse_holdings(body) == [(1, 32255, "Axon", "Axon Enterprises Inc (AAXN)", "K")]


def test_ties_are_context_unless_the_vendor_is_a_party() -> None:
    holdings = [{"vendor": "Axon", "description": "Axon Enterprises Inc (AAXN)", "gross_value_code": "K", "year": 2019},
                {"vendor": "Axon", "description": "Axon Enterprises Inc (AAXN)", "gross_value_code": "J", "year": 2017}]
    t = ct.judge_ties({"case_name": "Murkerson v. Flock Group, Inc."}, holdings)
    assert t["ties"][0]["years"] == [2019, 2017] and t["ties"][0]["latest_value"] == "$15,001–50,000"
    assert t["possible_455_conflict"] is False and "not parties" in t["note"]
    t = ct.judge_ties({"case_name": "Digital Ally, Inc. v. Axon Enterprise, Inc."}, holdings)
    assert t["possible_455_conflict"] is True and t["party_holdings"] == ["Axon"] and "§455(b)(4)" in t["note"]
    assert ct.judge_ties({"case_name": "X v. Y"}, [])["note"] is None


def test_party_names_match_whole_words() -> None:
    assert ct.is_party("Motorola Solutions", "Hytera v. Motorola Solutions")
    assert not ct.is_party("Axon", "Saxony Holdings v. Doe")


def test_cases_are_civil_or_criminal_and_defendants_collapse() -> None:
    assert ct.case_kind("Murkerson v. Flock Group, Inc.", "1:26-cv-04411", "440 Civil Rights: Other") == "civil"
    assert ct.case_kind("United States v. Vaughns", "2:26-cr-00512-3", None) == "criminal"
    assert ct.case_kind("United States v. Mertens", None, None) == "criminal"
    assert ct.case_kind("Doe v. City", None, None) == "civil"
    rows = [{"case_name": "United States v. Vaughns", "court_id": "cacd", "date_filed": "2026-10-07", "judge": None},
            {"case_name": "United States v. Vaughns", "court_id": "cacd", "date_filed": "2026-10-07", "judge": "J. Doe",
             "judge_id": 9},
            {"case_name": "Murkerson v. Flock Group, Inc.", "court_id": "gand", "date_filed": "2026-10-08"}]
    out = ct.collapse(rows)
    assert len(out) == 2 and out[0]["dockets"] == 2 and out[0]["judge"] == "J. Doe" and out[0]["judge_id"] == 9
