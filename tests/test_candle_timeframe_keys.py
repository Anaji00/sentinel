"""Pins the candle key vocabulary that broke 1h and 4h charts.

Two producers disagreed on how to spell the same duration. The crypto path
passed bare minute counts, which normalised to "60m" and "240m"; the equity
collector hand-built keys with "1h" and "4h". Every consumer asked for the
label form, so hourly and four-hourly candles existed for equities and were
unreachable for crypto -- `sentinel:candles:1h:BTCUSDT` was never written,
only `sentinel:candles:60m:BTCUSDT`.

The endpoint accepted the request and returned an empty list with a 200, which
renders as an empty chart rather than an error.
"""
import pathlib
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from shared.utils.candles import (  # noqa: E402
    TIMEFRAMES_MINUTES,
    candle_cache_key,
    normalize_timeframe,
    timeframe_aliases,
)


@pytest.mark.parametrize("spelling", [60, "60", "60m", "1h", "1hr", "H1"])
def test_every_spelling_of_one_hour_is_the_same_key(spelling):
    assert candle_cache_key("BTCUSDT", spelling) == "sentinel:candles:1h:BTCUSDT"


@pytest.mark.parametrize("spelling", [240, "240", "240m", "4h", "4hr", "H4"])
def test_every_spelling_of_four_hours_is_the_same_key(spelling):
    assert candle_cache_key("BTCUSDT", spelling) == "sentinel:candles:4h:BTCUSDT"


def test_minute_counts_and_labels_agree_for_every_produced_timeframe():
    """The exact mismatch: a producer passing minutes and a consumer passing a
    label must not write and read different keys."""
    for minutes in TIMEFRAMES_MINUTES:
        from_minutes = candle_cache_key("X", minutes)
        from_label = candle_cache_key("X", normalize_timeframe(minutes))
        assert from_minutes == from_label, f"{minutes} min disagrees with its label"


def test_day_and_week_also_collapse():
    assert candle_cache_key("X", 1440) == candle_cache_key("X", "1d")
    assert candle_cache_key("X", 10080) == candle_cache_key("X", "1w")


def test_aliases_put_the_canonical_form_first():
    """Readers try these in order; the canonical key must be checked first."""
    for label in ("1h", "4h", "1d", "1w"):
        aliases = timeframe_aliases(label)
        assert aliases[0] == label
        assert len(aliases) > 1, f"{label} has no legacy spelling to fall back on"


def test_legacy_keys_remain_reachable_during_transition():
    """Candles already cached as 60m/240m must still be found."""
    assert "60m" in timeframe_aliases("1h")
    assert "240m" in timeframe_aliases("4h")


def test_ticker_is_upper_cased_consistently():
    assert candle_cache_key("btcusdt", "1h") == candle_cache_key("BTCUSDT", "1h")


def test_short_timeframes_are_unchanged():
    """The fix must not disturb the spellings that already agreed."""
    for label in ("1m", "5m", "10m", "15m", "30m"):
        assert candle_cache_key("X", label) == f"sentinel:candles:{label}:X"


def test_both_producers_together_cover_what_the_api_advertises():
    """A timeframe the API accepts but nothing produces returns an empty chart
    with a 200, indistinguishable from a quiet market.

    Two producers share the job: the crypto aggregator runs a structural
    inference pass per timeframe so it keeps a short list, while the equity
    collector maintains the longer one. What matters is that between them they
    cover everything the endpoint offers, in the same vocabulary.
    """
    import importlib.util, sys, pathlib
    crypto = {normalize_timeframe(m) for m in TIMEFRAMES_MINUTES}

    src = (ROOT / "services/collector-tradfi/main.py").read_text(encoding="utf-8")
    # Read the equity producer's declared timeframes without importing the
    # module, which would pull in Kafka and network clients.
    import re
    declared = re.findall(r'"(\w+)":\s*\{"minutes"', src)
    assert declared, "could not read the equity producer's timeframes"
    equity = {normalize_timeframe(m) for m in declared}

    produced = crypto | equity
    for advertised in ("1m", "5m", "10m", "15m", "30m", "1h", "4h", "1d", "1w"):
        assert advertised in produced, f"{advertised} is offered but never produced"


def test_month_is_not_confused_with_minute():
    """"1M" is a month and "1m" is a minute -- a 43,200x difference.

    Case-insensitive normalisation silently returned minute candles for a
    monthly request, with no error to notice.
    """
    assert normalize_timeframe("1M") == "1M"
    assert normalize_timeframe("1m") == "1m"
    assert candle_cache_key("X", "1M") != candle_cache_key("X", "1m")


# ── one response shape, whichever producer wrote the candle ──────────────────

from shared.utils.candles import normalize_candle  # noqa: E402

CRYPTO_RAW = {"bucket_id": "1787443200", "open": 77133.45, "high": 77408.73,
              "low": 76888.9, "close": 77306.73, "volume": 259.18, "count": 82,
              "start_ts": "2026-08-23T00:10:04.361112+00:00"}
EQUITY_RAW = {"h": 71.27, "c": 70.89, "ts": "2026-08-12T19:00:00+00:00",
              "l": 70.78, "o": 71.27, "v": 1296}


def test_both_producer_schemas_render_identically():
    """Crypto writes open/high/low/close, equities write o/h/l/c.

    The API returned whichever it found untouched, so a chart reading `open`
    got nulls for equities and one reading `o` got nulls for crypto -- an empty
    series drawn next to perfectly good data.
    """
    for raw in (CRYPTO_RAW, EQUITY_RAW):
        c = normalize_candle(raw, "X")
        assert set(c) == {"ts", "open", "high", "low", "close", "volume", "ticker"}
        for field in ("open", "high", "low", "close"):
            assert isinstance(c[field], float), f"{field} missing from {raw}"
        assert c["ts"], "a candle without a timestamp cannot be plotted"


def test_crypto_timestamp_comes_from_the_bucket_boundary():
    """`bucket_id` is the candle's period; `start_ts` is when it was first written."""
    c = normalize_candle(CRYPTO_RAW, "BTCUSDT")
    assert c["ts"].startswith("2026-08-23T00:00:00"), c["ts"]
    assert not c["ts"].startswith("2026-08-23T00:10"), "used start_ts, not the bucket"


def test_ohlc_values_are_not_transposed():
    c = normalize_candle(EQUITY_RAW, "AAPL")
    assert (c["open"], c["high"], c["low"], c["close"]) == (71.27, 71.27, 70.78, 70.89)


def test_missing_volume_becomes_zero_not_none():
    """A chart summing volume must not meet None mid-series."""
    c = normalize_candle({"o": 1, "h": 2, "l": 0.5, "c": 1.5, "ts": "2026-01-01T00:00:00+00:00"}, "X")
    assert c["volume"] == 0.0


def test_unparseable_bucket_falls_back_rather_than_dropping_the_candle():
    c = normalize_candle({"bucket_id": "not-a-number", "start_ts": "2026-01-01T00:00:00+00:00",
                          "open": 1, "high": 2, "low": 0.5, "close": 1.5}, "X")
    assert c["ts"] == "2026-01-01T00:00:00+00:00"
