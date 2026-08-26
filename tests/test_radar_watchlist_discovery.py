"""
tests/test_radar_watchlist_discovery.py

The dynamic watchlist could not grow, because nothing reached the radar.

sentinel:watched:equities is the tradfi collector's universe: whatever it holds
is what gets polled, enriched and reasoned about. RadarAgent is what is meant to
grow it from observed anomalies, capped at 45. It held 7.

Every path into it was blocked by one quantity read from the wrong key:

  - collector-radar computes notional_flow = volume * close_price and refuses to
    emit below $150,000 -- then sends {ticker, volume, close_price, z_score} and
    drops the number. RadarAgent re-checks notional against its own $50,000
    floor via p.get("notional_usd"), reads the absent key as 0.0, and returns
    None. Measured on a real event: CGCP, 8,100 shares at $42.15 = $341,415 of
    flow, 6.8x the agent's own threshold, arriving as $0.

  - enriched equity anomalies were read via premium_usd, an options field. Over
    three days, 425 market_anomaly events carried it with a maximum value of 0.

  - 501 earnings_report events carried premium_usd null. An earnings surprise
    has no notional -- there is no trade behind it -- so a dollar floor excluded
    the whole category by construction.

Zero of 926 financial events over three days passed the gate.
"""

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.agents.radar_agent import (  # noqa: E402
    MIN_EARNINGS_SURPRISE_PCT,
    RadarAgent,
    _earnings_surprise_pct,
)

GATE = 50_000


@pytest.fixture(scope="module")
def agent():
    return RadarAgent.__new__(RadarAgent)


def test_the_collectors_own_payload_shape_passes(agent):
    """The exact dict collector-radar sends, with the real CGCP numbers."""
    ticker, _z, notional = agent._extract_radar_params(
        {"raw_payload": {"ticker": "CGCP", "volume": 8100, "close_price": 42.15, "z_score": 3.4}}
    )
    assert ticker == "CGCP"
    assert notional == pytest.approx(341_415.0)
    assert notional >= GATE, "the anomaly the collector raised is dropped again"


def test_an_enriched_equity_anomaly_passes(agent):
    """premium_usd is 0.0 for equities; volume and price are what is real."""
    _t, _z, notional = agent._extract_radar_params({
        "financial_data": {"ticker": "CGCP", "volume": 8100, "close_price": 42.15, "premium_usd": 0.0},
        "anomaly_score": 0.8,
    })
    assert notional >= GATE


def test_genuine_options_premium_still_wins(agent):
    """premium_usd is the right field for options flow and must be preferred."""
    _t, _z, notional = agent._extract_radar_params(
        {"financial_data": {"ticker": "AAPL", "premium_usd": 250_000.0}, "anomaly_score": 0.8}
    )
    assert notional == pytest.approx(250_000.0)


def test_the_collector_now_sends_what_it_computed():
    src = (ROOT / "services/collector-radar/main.py").read_text(encoding="utf-8")
    payload = src[src.index("raw_payload={"):][:1200]
    assert '"notional_usd"' in payload


def test_small_flow_is_still_rejected(agent):
    """The gate must keep doing its job."""
    _t, _z, notional = agent._extract_radar_params(
        {"raw_payload": {"ticker": "XYZ", "volume": 10, "close_price": 2.0, "z_score": 3.0}}
    )
    assert notional < GATE


def test_a_payload_with_nothing_usable_yields_zero(agent):
    _t, _z, notional = agent._extract_radar_params({"raw_payload": {"ticker": "XYZ"}})
    assert notional == 0.0


def test_junk_values_do_not_raise(agent):
    _t, _z, notional = agent._extract_radar_params(
        {"raw_payload": {"ticker": "XYZ", "volume": "many", "close_price": None}}
    )
    assert notional == 0.0


# ── earnings are judged on surprise, not flow ────────────────────────────────

def test_a_large_earnings_surprise_is_investigated():
    assert abs(_earnings_surprise_pct({"financial_data": {"eps_surprise_pct": 12.5}})) >= MIN_EARNINGS_SURPRISE_PCT


def test_a_large_miss_counts_the_same_as_a_beat():
    """A -14% miss is at least as interesting as a +14% beat."""
    assert abs(_earnings_surprise_pct({"financial_data": {"eps_surprise_pct": -14.0}})) >= MIN_EARNINGS_SURPRISE_PCT


def test_a_routine_beat_is_noise():
    """Consensus is set to be beaten by a little."""
    assert abs(_earnings_surprise_pct({"financial_data": {"eps_surprise_pct": 1.2}})) < MIN_EARNINGS_SURPRISE_PCT


def test_surprise_is_found_wherever_the_producer_put_it():
    for container in ("financial_data", "raw_payload", "trigger"):
        assert _earnings_surprise_pct({container: {"eps_surprise_pct": 11.0}}) == 11.0


def test_a_non_earnings_event_has_no_surprise():
    assert _earnings_surprise_pct({"financial_data": {"ticker": "CGCP", "volume": 8100}}) == 0.0


def test_the_gate_admits_either_signal():
    """Flow OR surprise, not flow AND surprise."""
    src = (ROOT / "services/agents/radar_agent.py").read_text(encoding="utf-8")
    gate = src[src.index("HEIGHTENED ANOMALY FLOW GATEKEEPER"):][:900]
    assert "surprise_pct < MIN_EARNINGS_SURPRISE_PCT and notional_usd < 50_000" in gate
