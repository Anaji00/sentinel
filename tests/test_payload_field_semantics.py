"""
tests/test_payload_field_semantics.py

Fields that did not mean what they were named, found by reading payloads.

Crypto transfers:

    {"pair": "USDC", "price": 1.0, "size_tokens": 29.88,
     "trade_type": "WHALE_TRANSFER"}

Three things wrong in one record. `size_tokens` held the USD notional, not a
token count. `price` was pinned to 1.0 so that arithmetic stayed
self-consistent -- true for USDT, USDC and DAI, and wrong for WBTC, which came
through at price 1.0000 on 180 events and understated the position by five
orders of magnitude. And `trade_type` was the constant "WHALE_TRANSFER" on all
6,960 transfers in an hour, from a size of 0.00 to 356,965,122, so anything
filtering on it received the dust as well.

The collector was already sending both `amount` and `notional_usd`. The enricher
discarded both and invented the price.

Options sweeps had the mirror problem: notional_usd null on 896 of 896 while
strike and volume sat in the same record.

None of this appears as an error, a null rate, or a flat score. It requires
opening a record and asking whether the numbers mean what they say.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.enrichment.enrichers.crypto import _implied_price  # noqa: E402
from services.enrichment.enrichers.tradfi import _option_notional  # noqa: E402


# -- crypto: price is measured, not assumed ------------------------------------

def test_wrapped_bitcoin_is_not_worth_one_dollar():
    """The defect: 180 WBTC transfers priced at 1.0000."""
    assert _implied_price(250_000.0, 2.5) == 100_000.0


def test_a_stablecoin_still_comes_out_at_one():
    """The old constant was right here, which is why it survived."""
    assert _implied_price(29.88, 29.88) == 1.0


def test_an_unknown_token_count_does_not_fabricate_a_price():
    assert _implied_price(1000.0, 0) == 0.0
    assert _implied_price(1000.0, None) == 0.0


def test_a_malformed_amount_does_not_raise():
    for amount in ("abc", [], {}):
        assert _implied_price(1000.0, amount) == 0.0


def test_the_crypto_model_can_hold_a_notional():
    """size_tokens was carrying it, which is why price had to be fictional."""
    from shared.models.events import CryptoData

    data = CryptoData(pair="WBTC", trade_type="WHALE_TRANSFER", side="TRANSFER",
                      price=100_000.0, size_tokens=2.5, notional_usd=250_000.0)
    assert data.size_tokens == 2.5
    assert data.notional_usd == 250_000.0


def test_the_label_agrees_with_the_size():
    """One constant label across a range from 0.00 to 356,965,122."""
    source = (ROOT / "services" / "enrichment" / "enrichers" / "crypto.py").read_text(encoding="utf-8")
    assert 'trade_type="WHALE_TRANSFER" if is_whale else "TRANSFER"' in source


# -- options: notional is computed, not omitted --------------------------------

def test_an_options_notional_is_what_the_position_controls():
    """Premium is what it cost. On a live sweep the two differed by 34x."""
    assert _option_notional(37.5, 200) == 750_000.0


def test_a_sweep_without_a_strike_is_not_given_an_invented_size():
    assert _option_notional(None, 200) is None


# -- vessels: the region was computed and discarded ----------------------------

def test_the_vessel_payload_records_where_the_vessel_is():
    """It was classified for the headline and the tags but never stored, so a
    consumer had to parse prose to learn the chokepoint."""
    source = (ROOT / "services" / "enrichment" / "enrichers" / "maritime.py").read_text(encoding="utf-8")
    assert "last_seen_region = region," in source
