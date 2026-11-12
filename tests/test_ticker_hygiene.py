"""G3: placeholders, joined fields and crypto pairs never become instruments."""

import pathlib
import sys
from datetime import datetime, timezone

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils.signal_families import observations  # noqa: E402
from shared.utils.ticker_hygiene import clean_all, clean_symbols, is_crypto_pair  # noqa: E402

NOW = datetime(2026, 10, 5, 12, tzinfo=timezone.utc)


def test_placeholders_are_dropped():
    for raw in ("NONE", "N/A", "", "  none ", "NA", None):
        assert clean_symbols(raw) == []


def test_joined_fields_are_split_in_order_without_repeats():
    assert clean_symbols("HEI, HEI.A") == ["HEI", "HEI.A"]
    assert clean_symbols("BCDA;BCDAW") == ["BCDA", "BCDAW"]
    assert clean_symbols("GEF/GEF.B GEF") == ["GEF", "GEF.B"]
    assert clean_symbols("Z AND ZG") == ["Z", "AND", "ZG"]


def test_futures_and_share_classes_pass_unchanged():
    assert clean_symbols("CL=F") == ["CL=F"]
    assert clean_symbols("brk.b") == ["BRK.B"]


def test_crypto_pairs_are_dropped_but_equities_that_look_similar_stay():
    for pair in ("BTCUSD", "ETHUSD", "DOGEUSD", "SOLUSDT"):
        assert is_crypto_pair(pair) and clean_symbols(pair) == []
    assert clean_symbols("USD") == ["USD"]          # the ProShares fund
    assert clean_symbols("SPY") == ["SPY"]


def test_clean_all_keeps_order_and_drops_repeats():
    assert clean_all(["AAPL", "NONE", "AAPL, MSFT", "BTCUSD"]) == ["AAPL", "MSFT"]


def test_an_insider_filing_without_a_listing_makes_no_observation():
    row = {"event_id": "1", "type": "insider_trade", "created_at": NOW, "primary_entity_id": "NONE",
           "financial_data": {"ticker": "NONE"}, "tags": ["insider_opportunistic", "open_market_buy"]}
    assert observations(row) == []


def test_a_joined_ticker_field_becomes_its_first_symbol():
    row = {"event_id": "1", "type": "insider_cluster", "created_at": NOW, "financial_data": {"ticker": "HEI, HEI.A"}}
    assert [(o.family, o.instrument) for o in observations(row)] == [("insider_cluster", "HEI")]


def test_crypto_pair_exposure_is_not_an_instrument_of_company_news():
    row = {"event_id": "1", "type": "headline", "source": "alpaca_news", "created_at": NOW,
           "tags": ["stock_news", "tone_positive", "exposes:BTCUSD", "exposes:COIN"]}
    assert {o.instrument for o in observations(row)} == {"COIN"}


def test_wrapped_and_venue_prefixed_symbols_are_unwrapped() -> None:
    # Found in the 11 Nov bar-less investigation (reports/Bar-less instruments.md).
    for raw, want in (("(CALX)", "CALX"), ("[DXPE]", "DXPE"), ('"""TELA"""', "TELA"), ("?RDW?", "RDW"),
                      ("NASDAQ:SVC", "SVC"), ("(NYSE:FBC)", "FBC"), ("NYSE:EVC", "EVC"), ("NMFC]", "NMFC")):
        assert clean_symbols(raw) == [want], raw
    assert clean_symbols("TSX:D") != ["D"]                       # a foreign venue is not stripped to a US ticker


def test_bare_berkshire_and_brown_forman_map_to_their_priced_class() -> None:
    assert clean_symbols("BRK") == ["BRK.B"] and clean_symbols("BF") == ["BF.B"]
    assert clean_symbols("BRK.A") == ["BRK.A"] and clean_symbols("BRK, BRK.B") == ["BRK.B"]
