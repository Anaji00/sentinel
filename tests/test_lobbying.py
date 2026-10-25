"""Federal lobbying by surveillance vendors (LDA filings)."""

import pathlib
import sys
from datetime import date

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import lobbying as lb  # noqa: E402

BODY = {"results": [
    {"filing_uuid": "22b8", "filing_type": "Q1", "filing_year": 2025, "filing_period_display": "1st Quarter (Jan 1 - Mar 31)",
     "income": "120000.00", "expenses": None, "dt_posted": "2025-04-15T16:59:03-04:00",
     "filing_document_url": "https://lda.gov/filings/public/filing/22b8/print/",
     "registrant": {"name": "MERCURY PUBLIC AFFAIRS, LLC"}, "client": {"name": "FLOCK GROUP INC D/B/A FLOCK SAFETY"},
     "lobbying_activities": [
         {"general_issue_code_display": "Homeland Security", "description": "issues related to safety and security",
          "government_entities": [{"name": "SENATE"}, {"name": "HOUSE OF REPRESENTATIVES"}]},
         {"general_issue_code_display": "Law Enforcement/Crime/Criminal Justice", "description": " ALPR policy ",
          "government_entities": [{"name": "SENATE"}]}]},
    {"filing_uuid": "in-house", "filing_type": "Q2", "filing_year": 2025, "income": None, "expenses": "50000",
     "registrant": {"name": "AXON ENTERPRISE"}, "client": {"name": "AXON ENTERPRISE, INC."}, "lobbying_activities": []},
    {"filing_uuid": None},
]}


def test_filings_carry_who_how_much_on_what_and_whom() -> None:
    rows = lb.parse_filings(BODY, "Flock Safety")
    assert [r["filing_uuid"] for r in rows] == ["22b8", "in-house"]
    f = rows[0]
    assert f["amount"] == 120000.0 and f["in_house"] is False and f["registrant"] == "MERCURY PUBLIC AFFAIRS, LLC"
    assert f["issues"] == ["Homeland Security", "Law Enforcement/Crime/Criminal Justice"]
    assert f["entities"] == ["HOUSE OF REPRESENTATIVES", "SENATE"]
    assert f["description"] == "issues related to safety and security | ALPR policy"
    assert f["posted"] == date(2025, 4, 15) and f["year"] == 2025
    assert rows[1]["amount"] == 50000.0 and rows[1]["in_house"] is True


def test_client_names_must_contain_the_query() -> None:
    assert lb.matches_vendor("FLOCK GROUP INC D/B/A FLOCK SAFETY", "Flock Group")
    assert not lb.matches_vendor("FLOCKHART GROUP LLC", "Flock Group")
    assert not lb.matches_vendor(None, "Palantir")
