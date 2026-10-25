"""Form 144: insiders' notices of proposed sales, parsed from EDGAR."""

import pathlib
import sys
from datetime import date

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import form144 as f144  # noqa: E402

NOTICE = """<?xml version="1.0" encoding="UTF-8"?>
<own:edgarSubmission xmlns:own="http://www.sec.gov/edgar/ownership" xmlns:com="http://www.sec.gov/edgar/common">
<own:headerData><own:submissionType>144</own:submissionType></own:headerData>
<own:formData>
 <own:issuerInfo><own:issuerCik>0000074046</own:issuerCik><own:issuerName>Oil-Dri Corporation of America</own:issuerName>
  <own:nameOfPersonForWhoseAccountTheSecuritiesAreToBeSold>Amy Ryan</own:nameOfPersonForWhoseAccountTheSecuritiesAreToBeSold>
  <own:relationshipsToIssuer><own:relationshipToIssuer>Director</own:relationshipToIssuer></own:relationshipsToIssuer></own:issuerInfo>
 <own:securitiesInformation><own:securitiesClassTitle>Common Stock</own:securitiesClassTitle>
  <own:noOfUnitsSold>1100</own:noOfUnitsSold><own:aggregateMarketValue>89100</own:aggregateMarketValue>
  <own:noOfUnitsOutstanding>10245493</own:noOfUnitsOutstanding><own:approxSaleDate>10/16/2026</own:approxSaleDate></own:securitiesInformation>
 <own:securitiesToBeSold><own:natureOfAcquisitionTransaction>Restricted Stock Award</own:natureOfAcquisitionTransaction></own:securitiesToBeSold>
 <own:noticeSignature><own:noticeDate>10/16/2026</own:noticeDate></own:noticeSignature>
</own:formData></own:edgarSubmission>"""

FEED = """<feed xmlns="http://www.w3.org/2005/Atom">
<entry><title>144 - Ryan Amy (0001860576) (Reporting)</title>
<link href="https://www.sec.gov/Archives/edgar/data/1860576/000162828026066039/0001628280-26-066039-index.htm"/></entry>
<entry><title>144 - Oil-Dri Corp of America (0000074046) (Subject)</title>
<link href="https://www.sec.gov/Archives/edgar/data/74046/000162828026066039/0001628280-26-066039-index.htm"/></entry>
</feed>"""


def test_a_notice_names_seller_size_and_date() -> None:
    n = f144.parse_notice(NOTICE)
    assert n["issuer_cik"] == "74046" and n["seller"] == "Amy Ryan" and n["relationship"] == "Director"
    assert n["shares"] == 1100 and n["market_value"] == 89100
    assert n["pct_outstanding"] == round(1100 / 10245493 * 100, 4)
    assert n["approx_sale_date"] == date(2026, 10, 16) and n["acquired_how"] == "Restricted Stock Award"
    assert f144.parse_notice("<not xml") is None


def test_each_filing_is_listed_once_with_its_primary_document() -> None:
    out = f144.parse_feed(FEED)
    assert out == [{"accession": "0001628280-26-066039",
                    "doc_url": "https://www.sec.gov/Archives/edgar/data/1860576/000162828026066039/primary_doc.xml"}]
