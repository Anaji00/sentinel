"""Rule 10b5-1 plan trades are routine and stay out of insider clusters."""

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils.form4 import parse_form4  # noqa: E402


def _doc(checkbox: str = "", footnote: str = "") -> str:
    return f"""<?xml version="1.0"?>
<ownershipDocument>
  <issuer><issuerTradingSymbol>ACME</issuerTradingSymbol><issuerName>Acme Corp</issuerName></issuer>
  {checkbox}
  <reportingOwner><reportingOwnerId><rptOwnerName>Doe Jane</rptOwnerName></reportingOwnerId>
    <reportingOwnerRelationship><isOfficer>1</isOfficer><officerTitle>CFO</officerTitle></reportingOwnerRelationship></reportingOwner>
  <nonDerivativeTable><nonDerivativeTransaction>
    <transactionCoding><transactionCode>S</transactionCode></transactionCoding>
    <transactionAmounts><transactionShares><value>1000</value></transactionShares>
      <transactionPricePerShare><value>50</value></transactionPricePerShare>
      <transactionAcquiredDisposedCode><value>D</value></transactionAcquiredDisposedCode></transactionAmounts>
  </nonDerivativeTransaction></nonDerivativeTable>
  <footnotes>{footnote}</footnotes>
</ownershipDocument>"""


def test_the_2023_checkbox_marks_a_planned_trade() -> None:
    assert parse_form4(_doc("<aff10b5One>1</aff10b5One>"))["planned_10b5_1"] is True
    assert parse_form4(_doc("<aff10b5One>0</aff10b5One>"))["planned_10b5_1"] is False


def test_older_filings_disclose_plans_in_footnotes() -> None:
    planned = _doc(footnote='<footnote id="F1">Sold pursuant to a Rule 10b5-1 trading plan adopted May 2022.</footnote>')
    assert parse_form4(planned)["planned_10b5_1"] is True
    denied = _doc(footnote='<footnote id="F1">This sale was not made pursuant to a Rule 10b5-1 plan.</footnote>')
    assert parse_form4(denied)["planned_10b5_1"] is False


def test_the_cluster_keeps_planned_trades_out_of_its_arithmetic() -> None:
    src = (ROOT / "services/agents/quant_trading_engine.py").read_text(encoding="utf-8")
    assert 'if raw.get("planned_10b5_1") and tx_type in ("BUY", "SELL"):' in src
