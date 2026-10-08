"""Signal families: what each event counts as, and what never counts."""

import pathlib
import sys
from datetime import datetime, timedelta, timezone

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils.signal_families import (  # noqa: E402
    PRICE_DIRECTED, backfill_universe, episodes, observations,
)

NOW = datetime(2026, 10, 5, 12, tzinfo=timezone.utc)


def _fams(row):
    return {(o.family, o.instrument, o.direction) for o in observations(row)}


def test_escalation_moves_crude_up_and_taiwan_chips_down():
    obs = _fams({"event_id": "1", "type": "headline", "created_at": NOW,
                 "headline": "Iran attacks tanker in the Strait of Hormuz", "tags": []})
    assert ("zone_escalation", "CL=F", 1) in obs and ("zone_escalation", "FRO", 1) in obs
    tw = _fams({"event_id": "2", "type": "headline", "created_at": NOW,
                "headline": "PLA missile strikes near Taiwan", "tags": []})
    assert ("zone_escalation", "TSM", -1) in tw


def test_a_ceasefire_reverses_the_expected_move():
    obs = _fams({"event_id": "1", "type": "headline", "created_at": NOW,
                 "headline": "Ceasefire agreed between Israel and Hamas in Gaza", "tags": []})
    assert ("zone_deescalation", "BZ=F", -1) in obs


def test_sector_news_needs_clear_sentiment():
    row = {"event_id": "1", "type": "headline", "created_at": NOW,
           "headline": "Chipmakers rally on AI chip demand", "tags": [], "sentiment": 0.1}
    assert not any(f.startswith("sector_news") for f, _, _ in _fams(row))
    row["sentiment"] = 0.6
    assert ("sector_news_positive", "SOXX", 1) in _fams(row)


def test_small_options_sweeps_are_noise():
    row = {"event_id": "1", "type": "options_flow", "created_at": NOW, "primary_entity_id": "TLT",
           "financial_data": {"ticker": "TLT", "premium_usd": 300.0, "volume_oi_ratio": 5, "option_type": "CALL"}}
    assert _fams(row) == set()
    row["financial_data"]["premium_usd"] = 250_000.0
    assert _fams(row) == {("options_call_sweep", "TLT", 1)}


def test_weak_volume_spikes_are_noise_and_strong_ones_take_direction_from_the_tape():
    row = {"event_id": "1", "type": "market_anomaly", "source": "alpaca_quant_radar", "created_at": NOW,
           "primary_entity_id": "TXT", "financial_data": {"ticker": "TXT", "z_score": 3.2}}
    assert _fams(row) == set()
    row["financial_data"]["z_score"] = 6.0
    assert _fams(row) == {("volume_spike", "TXT", 0)}
    assert "volume_spike" in PRICE_DIRECTED


def test_crypto_candles_are_not_trading_anomalies():
    row = {"event_id": "1", "type": "market_anomaly", "source": "coinbase_candles", "created_at": NOW,
           "primary_entity_id": "BTC", "financial_data": {"z_score": 9}}
    assert _fams(row) == set()


def test_timing_is_when_the_platform_knew_not_when_it_happened():
    row = {"event_id": "1", "type": "insider_cluster", "primary_entity_id": "ACME",
           "occurred_at": NOW - timedelta(days=3), "created_at": NOW, "tags": []}
    assert observations(row)[0].known_at == NOW


def test_signals_never_count_as_evidence_for_signals():
    row = {"event_id": "1", "type": "trade_signal", "created_at": NOW, "primary_entity_id": "CL=F",
           "tags": ["trade_signal", "exposes:CL=F"]}
    assert observations(row) == []


def test_filings_cluster_over_weeks_not_hours():
    rows = [{"event_id": str(i), "type": "insider_cluster", "primary_entity_id": "ACME",
             "created_at": NOW + timedelta(days=i), "tags": []} for i in range(5)]
    eps = episodes(o for r in rows for o in observations(r))
    assert len(eps) == 1


def test_dark_vessels_count_by_region_before_zone_tags_existed():
    row = {"event_id": "1", "type": "vessel_dark", "created_at": NOW, "region": "Persian Gulf",
           "tags": ["dark_vessel", "exposes:CL=F"]}
    assert ("dark_vessel_in_zone", "CL=F", 1) in _fams(row)


def test_the_backfill_universe_is_listed_instruments_with_benchmarks():
    u = backfill_universe()
    assert "SPY" in u and "ITA" in u and "FRO" in u
    assert not any(s.endswith("=F") for s in u)


def test_the_engine_reads_only_signal_inputs_and_is_kill_switched():
    src = (ROOT / "services/agents/signal_engine.py").read_text(encoding="utf-8")
    assert '"trade_signal"' not in src.split("SIGNAL_EVENT_TYPES = [")[1].split("]")[0]
    assert 'self.flags.is_enabled("trade_signals"' in src
    assert 'v.get("verdict") == "proven"' in src


def test_the_track_record_counts_misses():
    from services.api_gateway.routes.signals import track_record

    tr = track_record([
        {"hit": True, "return_bps": 40, "families": ["a"]},
        {"hit": False, "return_bps": -25, "families": ["a"]},
        {"hit": None, "return_bps": None, "families": ["a"]},
    ])
    assert tr["closed"] == 3 and tr["scored"] == 2 and tr["hit_rate"] == 0.5
    assert tr["by_family"]["a"]["mean_return_bps"] == 7.5


def test_institutional_flows_point_both_ways():
    base = {"event_id": "1", "type": "thirteen_f", "created_at": NOW, "primary_entity_id": "INTC",
            "financial_data": {"ticker": "INTC"}}
    assert _fams({**base, "tags": ["institutional_accumulation"]}) == {("institutional_accumulation", "INTC", 1)}
    assert _fams({**base, "tags": ["institutional_distribution"]}) == {("institutional_distribution", "INTC", -1)}


def test_8k_items_become_families_with_their_priors():
    row = {"event_id": "1", "type": "filing", "created_at": NOW, "primary_entity_id": "ACME",
           "financial_data": {"ticker": "ACME"}, "tags": [],
           "summary": "Material 8-K Event for ACME: 4.02: Non-Reliance on Prior Financials; 5.02: Departure of Directors. Filed on 2026-10-05."}
    fams = _fams(row)
    assert ("8k_nonreliance", "ACME", -1) in fams
    assert ("8k_officer_change", "ACME", 0) in fams
    assert "8k_officer_change" in PRICE_DIRECTED


def test_a_plain_filing_is_not_an_8k_signal():
    row = {"event_id": "1", "type": "filing", "created_at": NOW, "primary_entity_id": "ACME",
           "financial_data": {"ticker": "ACME"}, "tags": [],
           "summary": "SEC filing Form 10-Q for Acme (ACME) on 2026-10-05."}
    assert _fams(row) == set()


def test_a_short_volume_spike_points_down():
    row = {"event_id": "1", "type": "market_anomaly", "source": "finra_short_volume", "created_at": NOW,
           "primary_entity_id": "AA", "financial_data": {"ticker": "AA"},
           "tags": ["short_volume_spike", "tradfi", "exposes:AA"]}
    assert _fams(row) == {("short_volume_spike", "AA", -1)}


def test_the_study_admits_finra_short_spikes():
    src = (ROOT / "services/agents/signal_engine.py").read_text(encoding="utf-8")
    assert "NOT IN ('alpaca_quant_radar', 'finra_short_volume')" in src
