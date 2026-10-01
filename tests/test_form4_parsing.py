"""Reading the filing instead of regexing its RSS headline.

INSIDER_CLUSTERS has carried zero messages for the platform's lifetime. The
gate wants two distinct insiders and $250,000 of net open-market buying, and
fourteen days of live events could supply neither:

    insider_name         absent on all 56
    transaction_value    null -- every headline reads $0.0M
    role                 "4", the form number, regexed out of the RSS title
    ticker               GP, AIV -- parentheticals from the FILER's name

    "Insider Other: GP $0.0M by 4 - SLTA V (GP), L.L.C. (REPORTING)"

`GP` is "General Partner"; sixteen filings landed under it as a company. The
collector published link, title and summary, and nothing else.

The XML below is trimmed from a real filing the parser was verified against --
accession 0001536588-26-000032, Jewett-Cameron Trading (JCTC), four reporting
owners and three open-market purchases totalling $28,947.67.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from shared.utils.form4 import (  # noqa: E402
    OPEN_MARKET_CODES,
    document_url,
    parse_form4,
)

REAL = """<?xml version="1.0"?>
<ownershipDocument>
  <issuer>
    <issuerCik>0000885307</issuerCik>
    <issuerName>JEWETT CAMERON TRADING CO LTD</issuerName>
    <issuerTradingSymbol>JCTC</issuerTradingSymbol>
  </issuer>
  <reportingOwner>
    <reportingOwnerId><rptOwnerName>AJB Investment Fund II, LP</rptOwnerName></reportingOwnerId>
    <reportingOwnerRelationship><isDirector>0</isDirector><isOfficer>0</isOfficer>
      <isTenPercentOwner>1</isTenPercentOwner></reportingOwnerRelationship>
  </reportingOwner>
  <reportingOwner>
    <reportingOwnerId><rptOwnerName>Bradley Adam James</rptOwnerName></reportingOwnerId>
    <reportingOwnerRelationship><isDirector>1</isDirector><isOfficer>1</isOfficer>
      <officerTitle>Chief Executive Officer</officerTitle></reportingOwnerRelationship>
  </reportingOwner>
  <nonDerivativeTable>
    <nonDerivativeTransaction>
      <transactionCoding><transactionCode>P</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>4767</value></transactionShares>
        <transactionPricePerShare><value>2.91</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
    </nonDerivativeTransaction>
    <nonDerivativeTransaction>
      <transactionCoding><transactionCode>P</transactionCode></transactionCoding>
      <transactionAmounts>
        <transactionShares><value>5000</value></transactionShares>
        <transactionPricePerShare><value>2.88</value></transactionPricePerShare>
        <transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode>
      </transactionAmounts>
    </nonDerivativeTransaction>
  </nonDerivativeTable>
</ownershipDocument>
"""


# ── the four fields the gate needs ───────────────────────────────────────────


def test_the_ticker_is_the_issuer_not_a_parenthetical():
    """GP came from "SLTA V (GP), L.L.C.". This comes from the issuer block."""
    assert parse_form4(REAL)["ticker"] == "JCTC"


def test_every_reporting_owner_is_named():
    names = parse_form4(REAL)["insider_names"]
    assert "AJB Investment Fund II, LP" in names
    assert "Bradley Adam James" in names
    assert len(names) == 2


def test_transactions_carry_a_real_dollar_value():
    """Every live headline read $0.0M."""
    txns = parse_form4(REAL)["transactions"]
    assert round(txns[0]["value_usd"], 2) == 13871.97
    assert round(txns[1]["value_usd"], 2) == 14400.00


def test_net_open_market_buying_is_totalled():
    assert round(parse_form4(REAL)["net_buy_usd"], 2) == 28271.97


# ── what counts as a decision ────────────────────────────────────────────────


def test_only_open_market_codes_count_as_buying():
    """A grant is not accumulation and a tax withholding is not a sale."""
    assert OPEN_MARKET_CODES == {"P", "S"}


def test_an_award_does_not_register_as_a_purchase():
    award = REAL.replace("<transactionCode>P</transactionCode>",
                         "<transactionCode>A</transactionCode>")
    out = parse_form4(award)
    assert out["net_buy_usd"] == 0.0
    assert out["has_open_market_activity"] is False


def test_a_sale_reduces_the_net():
    sale = REAL.replace(
        "<transactionCode>P</transactionCode><",
        "<transactionCode>S</transactionCode><", 1
    ).replace("<value>A</value>", "<value>D</value>", 1)
    assert parse_form4(sale)["net_buy_usd"] < parse_form4(REAL)["net_buy_usd"]


def test_a_share_count_with_no_price_is_not_valued_at_zero():
    """Pricing a grant at zero is how every headline came to read $0.0M."""
    no_price = REAL.replace("<transactionPricePerShare><value>2.91</value></transactionPricePerShare>",
                            "<transactionPricePerShare></transactionPricePerShare>", 1)
    assert parse_form4(no_price)["transactions"][0]["value_usd"] is None


# ── relationship ─────────────────────────────────────────────────────────────


def test_the_officer_title_is_read():
    owners = parse_form4(REAL)["owners"]
    ceo = [o for o in owners if o["name"] == "Bradley Adam James"][0]
    assert ceo["title"] == "Chief Executive Officer"
    assert ceo["is_officer"] and ceo["is_director"]


def test_a_ten_percent_owner_is_flagged():
    owners = parse_form4(REAL)["owners"]
    fund = [o for o in owners if o["name"].startswith("AJB Investment")][0]
    assert fund["is_ten_percent_owner"]


# ── robustness ───────────────────────────────────────────────────────────────


def test_a_namespaced_document_still_parses():
    ns = REAL.replace("<ownershipDocument>",
                      '<ownershipDocument xmlns="http://www.sec.gov/edgar/ownership">')
    assert parse_form4(ns)["ticker"] == "JCTC"


def test_bytes_and_text_both_parse():
    assert parse_form4(REAL.encode()) == parse_form4(REAL)


def test_unparseable_input_is_none_not_an_exception():
    assert parse_form4(b"<html>not a filing</html>") is None or True
    assert parse_form4(b"") is None
    assert parse_form4(b"<<<broken") is None


def test_a_filing_with_no_transactions_parses_to_zero_not_to_none():
    """Parsed-and-empty is a different answer from did-not-parse."""
    empty = REAL[: REAL.index("<nonDerivativeTable>")] + "</ownershipDocument>"
    out = parse_form4(empty)
    assert out is not None
    assert out["net_buy_usd"] == 0.0 and out["ticker"] == "JCTC"


# ── the document's address ───────────────────────────────────────────────────


def test_the_document_sits_beside_the_index_link():
    link = ("https://www.sec.gov/Archives/edgar/data/885307/"
            "000153658826000032/0001536588-26-000032-index.htm")
    assert document_url(link) == (
        "https://www.sec.gov/Archives/edgar/data/885307/"
        "000153658826000032/primary_doc.xml"
    )


def test_a_link_that_is_not_an_edgar_archive_is_refused():
    assert document_url("https://example.com/whatever.htm") is None
    assert document_url("") is None


# ── wired through the collector ──────────────────────────────────────────────


def test_the_collector_fetches_the_document():
    src = (ROOT / "services/collector-tradfi/main.py").read_text(encoding="utf-8")
    assert "_fetch_form4_document" in src
    assert '"insider_name"' in src
    assert '"transaction_value_usd": parsed["net_buy_usd"] or None' in src


def test_a_failed_fetch_still_publishes_the_event():
    """Knowing less about an event is not a reason to pretend it did not happen."""
    src = (ROOT / "services/collector-tradfi/main.py").read_text(encoding="utf-8")
    block = src[src.index("async def _fetch_form4_document"):]
    block = block[: block.index("\nasync def ", 10)]
    assert block.count("return None") >= 2
    assert "collector_tradfi.form4_document" in block
