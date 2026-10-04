"""Schedule 13D and 13G: who crosses 5%, and who means to act.

Since late 2024 EDGAR files these as "SCHEDULE 13D"/"SCHEDULE 13G" with a
structured primary_doc.xml; the old "SC 13D" type returns nothing from the
current feed. A dry run on 4 October produced 68 events from the live feeds,
among them new activist stakes (Gate City Capital 9.35% of CPHC, Alpine Fox
21.5% of SSM) -- and JPMorgan "reporting 100% of MHD", a closed-end fund's
preferred class, which the common-equity filter now drops.
"""

import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace

from shared.utils.beneficial import headline, is_common_equity, parse_schedule, score_schedule

ROOT = Path(__file__).resolve().parents[1]

SCHEDULE_13D = """<?xml version="1.0"?>
<edgarSubmission xmlns="http://www.sec.gov/edgar/schedule13D">
  <headerData><submissionType>SCHEDULE 13D</submissionType></headerData>
  <formData>
    <coverPageHeader><securitiesClassTitle>Common Stock, $0.01 par value</securitiesClassTitle>
      <dateOfEvent>09/30/2026</dateOfEvent>
      <issuerInfo><issuerCIK>0001235912</issuerCIK><issuerName>CVRx, Inc.</issuerName></issuerInfo>
    </coverPageHeader>
    <reportingPersons>
      <reportingPersonInfo><reportingPersonName>Fund LP</reportingPersonName>
        <aggregateAmountOwned>1650000</aggregateAmountOwned><percentOfClass>7.2</percentOfClass>
        <typeOfReportingPerson>PN</typeOfReportingPerson></reportingPersonInfo>
      <reportingPersonInfo><reportingPersonName>Fund GP LLC</reportingPersonName>
        <aggregateAmountOwned>1700000</aggregateAmountOwned><percentOfClass>7.4</percentOfClass>
        <typeOfReportingPerson>OO</typeOfReportingPerson></reportingPersonInfo>
    </reportingPersons>
    <items1To7><item4><transactionPurpose>The Reporting Persons intend to engage with the Board
      regarding strategic alternatives, including a sale of the Issuer.</transactionPurpose></item4></items1To7>
  </formData>
</edgarSubmission>"""

SCHEDULE_13G_A = """<?xml version="1.0"?>
<edgarSubmission xmlns="http://www.sec.gov/edgar/schedule13g">
  <headerData><submissionType>SCHEDULE 13G/A</submissionType></headerData>
  <formData><coverPageHeader><securitiesClassTitle>Ordinary Shares</securitiesClassTitle>
    <issuerInfo><issuerName>GOWell Energy Technology</issuerName></issuerInfo>
    <eventDateRequiresFilingThisStatement>09/25/2026</eventDateRequiresFilingThisStatement></coverPageHeader>
    <coverPageHeaderReportingPersonDetails><reportingPersonName>Michael Blitzer</reportingPersonName>
      <classPercent>9.35</classPercent></coverPageHeaderReportingPersonDetails>
  </formData>
</edgarSubmission>"""


def test_a_13d_parses_to_its_group_and_largest_stake():
    p = parse_schedule(SCHEDULE_13D)
    assert p["is_13d"] and not p["is_amendment"]
    assert p["issuer_name"] == "CVRx, Inc." and p["issuer_cik"] == "0001235912"
    assert [x["name"] for x in p["persons"]] == ["Fund LP", "Fund GP LLC"]
    assert p["pct"] == 7.4 and p["event_date"] == "09/30/2026"
    assert "strategic alternatives" in p["purpose"]
    assert p["class_title"].startswith("Common Stock")


def test_a_13g_amendment_reads_its_own_field_names():
    p = parse_schedule(SCHEDULE_13G_A)
    assert not p["is_13d"] and p["is_amendment"]
    assert p["pct"] == 9.35 and p["event_date"] == "09/25/2026"
    assert parse_schedule("not xml") is None


def test_scores_put_the_activist_first():
    initial_13d = {"is_13d": True, "is_amendment": False, "pct": 7.4}
    exit_13d = {"is_13d": True, "is_amendment": True, "pct": 3.1}
    built_13d = {"is_13d": True, "is_amendment": True, "pct": 12.0}
    initial_13g = {"is_13d": False, "is_amendment": False, "pct": 6.0}
    quiet_13g_a = {"is_13d": False, "is_amendment": True, "pct": 6.2}
    assert score_schedule(initial_13d, None) == 0.85
    assert score_schedule(exit_13d, 6.0) == 0.7
    assert score_schedule(built_13d, 8.0) == 0.8
    assert score_schedule(initial_13g, None) == 0.4
    assert score_schedule(quiet_13g_a, 6.0) == 0.25
    assert headline(built_13d, "CVRX", "Fund GP LLC", 8.0) == \
        "Activist 13D amendment: Fund GP LLC reports 12.00% of CVRX (from 8.00%)"


def test_only_the_controlling_class_counts():
    assert is_common_equity("Common Stock, $0.01 par value", "CVRX")
    assert is_common_equity("Ordinary Shares", "GOW")
    assert not is_common_equity("Variable Rate Demand Preferred Shares", "MHD")
    assert not is_common_equity("Common Stock", "GPUS-PD")


def test_the_feed_pairs_subject_and_filer_by_accession():
    spec = importlib.util.spec_from_file_location("beneficial_job_t", ROOT / "services/collector-filings/beneficial.py")
    job = importlib.util.module_from_spec(spec)
    sys.modules["beneficial_job_t"] = job
    spec.loader.exec_module(job)
    link = "https://www.sec.gov/Archives/edgar/data/1235912/000119380526001332/0001193805-26-001332-index.htm"
    entries = [
        {"title": "SCHEDULE 13D - CVRx, Inc. (0001235912) (Subject)", "link": link},
        {"title": "SCHEDULE 13D - Fund GP LLC (0002042077) (Filed by)", "link": link},
        {"title": "SCHEDULE 13G - Lonely Filer (0000000001) (Filed by)",
         "link": "https://www.sec.gov/Archives/edgar/data/1/000000000126000001/x-index.htm"},
    ]
    groups = job.group_entries(entries)
    assert list(groups) == ["000119380526001332"]
    g = groups["000119380526001332"]
    assert g["subject"] == ("CVRx, Inc.", "1235912") and g["filer"] == ("Fund GP LLC", "2042077")
    ev = job.schedule_event("000119380526001332", g, parse_schedule(SCHEDULE_13D), "CVRX", None)
    assert ev.anomaly_score == 0.85 and "activist" in ev.tags and "exposes:CVRX" in ev.tags
    assert ev.source == "sec_13d"
