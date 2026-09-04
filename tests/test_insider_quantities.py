"""A filing that does not state a quantity must not be given one.

shares defaulted to 1000.0 and price to 0.0, with total_usd computed as
shares * price. A Form 4 missing its share count was therefore sized at a
thousand shares times whatever price it did carry -- at a $150 print, $150,000
of insider buying that nobody filed.

The cluster gate fires on two distinct buyers and $250,000 net bought, and the
z-score is log-scaled off the same figure, so two such filings publish an
insider accumulation cluster that never happened.

Nothing had ever exercised this: there are no insider events in the database at
all, because the SEC filings collector exists in the tree and had no compose
service. It is deployed now, so this path is about to receive real data.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from services.agents.quant_trading_engine import _positive_or_none  # noqa: E402


def test_a_stated_quantity_is_used():
    assert _positive_or_none(500) == 500.0
    assert _positive_or_none(None, "250.5") == 250.5


def test_the_first_positive_candidate_wins():
    assert _positive_or_none(None, 0, 300) == 300.0


def test_an_absent_quantity_is_none_not_a_thousand():
    assert _positive_or_none(None) is None
    assert _positive_or_none(None, None, None) is None


def test_a_zero_is_not_a_quantity():
    """A share count of zero is not a trade; a price of zero is not a price."""
    assert _positive_or_none(0) is None
    assert _positive_or_none(0.0, 0) is None


def test_a_negative_is_not_a_quantity():
    assert _positive_or_none(-100) is None


def test_junk_does_not_raise():
    """This runs inside a message handler."""
    assert _positive_or_none("n/a", [], {}) is None


def _total(raw):
    """The sizing expression the engine now uses."""
    shares = _positive_or_none(raw.get("shares"), raw.get("qty"))
    price = _positive_or_none(raw.get("price"), raw.get("price_per_share"))
    stated = _positive_or_none(raw.get("total_value_usd"), raw.get("notional_usd"))
    return stated if stated is not None else (
        shares * price if (shares is not None and price is not None) else None
    )


def test_a_complete_filing_is_sized():
    assert _total({"shares": 1000, "price": 150.0}) == 150000.0


def test_a_stated_total_is_preferred():
    assert _total({"shares": 10, "price": 2.0, "total_value_usd": 999.0}) == 999.0


def test_a_filing_without_shares_is_not_sized():
    """The defect: this returned 1000 * 150 = 150,000."""
    assert _total({"price": 150.0}) is None


def test_a_filing_without_a_price_is_not_sized():
    assert _total({"shares": 1000}) is None


def test_the_dollar_aggregation_skips_unsizeable_trades():
    trades = [
        {"tx_type": "BUY", "total_usd": 200_000.0},
        {"tx_type": "BUY", "total_usd": None},
        {"tx_type": "BUY", "total_usd": 100_000.0},
    ]
    total = sum(t["total_usd"] for t in trades if t.get("total_usd") is not None)
    assert total == 300_000.0


def test_the_engine_uses_the_none_safe_aggregation():
    source = (ROOT / "services/agents/quant_trading_engine.py").read_text(encoding="utf-8")
    assert 'sum(t.get("total_usd", 0) for t in buy_trades)' not in source
    assert 'if t.get("total_usd") is not None' in source


# ── Transaction direction ─────────────────────────────────────────────────────
#
# tx_code defaulted to "P" and anything outside four buy strings was classified
# SELL, so every Form 4 code beyond those became insider selling. Form 4 defines
# more than a dozen -- A (award), G (gift), M (option exercise), F (tax
# withholding), J (other) -- and the enricher's own fallback for an unparseable
# code is "J", which was being counted as a sale against the buying side of the
# cluster gate.


def _tx_type(raw):
    """The classification the engine now applies."""
    code = str(raw.get("transaction_code") or raw.get("trade_type") or "").upper()
    if code in ("P", "PURCHASE", "BUY", "ACQUISITION"):
        return "BUY"
    if code in ("S", "SALE", "SELL", "D", "DISPOSITION"):
        return "SELL"
    return "OTHER"


def test_a_purchase_is_a_buy():
    assert _tx_type({"transaction_code": "P"}) == "BUY"
    assert _tx_type({"trade_type": "purchase"}) == "BUY"


def test_a_sale_is_a_sell():
    assert _tx_type({"transaction_code": "S"}) == "SELL"
    assert _tx_type({"transaction_code": "D"}) == "SELL"


def test_an_award_is_neither():
    """A grant of shares is not the insider buying on the open market."""
    assert _tx_type({"transaction_code": "A"}) == "OTHER"


def test_a_gift_and_a_tax_withholding_are_neither():
    assert _tx_type({"transaction_code": "G"}) == "OTHER"
    assert _tx_type({"transaction_code": "F"}) == "OTHER"


def test_the_unparseable_fallback_is_not_a_sale():
    """"J" is what the enricher writes when it cannot read the code."""
    assert _tx_type({"transaction_code": "J"}) == "OTHER"


def test_a_missing_code_is_not_a_purchase():
    """It defaulted to "P", which made an unknown filing insider buying."""
    assert _tx_type({}) == "OTHER"


def test_other_trades_are_excluded_from_both_sides():
    trades = [
        {"tx_type": "BUY", "total_usd": 300_000.0},
        {"tx_type": "OTHER", "total_usd": 900_000.0},
        {"tx_type": "SELL", "total_usd": 100_000.0},
    ]
    buys = sum(t["total_usd"] for t in trades
               if t["tx_type"] == "BUY" and t.get("total_usd") is not None)
    sells = sum(t["total_usd"] for t in trades
                if t["tx_type"] == "SELL" and t.get("total_usd") is not None)
    assert buys == 300_000.0 and sells == 100_000.0


def test_the_engine_no_longer_defaults_to_a_purchase():
    source = (ROOT / "services/agents/quant_trading_engine.py").read_text(encoding="utf-8")
    assert 'raw.get("trade_type") or "P"' not in source
    assert 'tx_type = "BUY" if is_buy else "SELL"' not in source
