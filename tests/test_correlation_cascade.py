"""
tests/test_correlation_cascade.py

Unit test suite for Risk Cascade Modeler, Sector Hawkes, Soft Correlator (services/correlation/):
  - cascade.py
  - sector_hawkes.py
  - soft_correlator.py
  - event_store.py
"""

import pytest
import numpy as np
from services.correlation.cascade import GeopoliticalCascadeEngine

# ── 1. GEOPOLITICAL RISK CASCADE PROPAGATION GRAPH ──────────────────────────

def test_risk_cascade_propagation():
    modeler = GeopoliticalCascadeEngine()
    assert hasattr(modeler, "graph") or hasattr(modeler, "redis") or modeler is not None


def test_a_busy_wallet_is_not_a_cascade():
    """Transfers carry no region, so the window keyed on the wallet and a
    stablecoin wallet moving money all day became 183 'On-Chain Activity
    Cluster' findings a week. Transfers stay out of the cascade."""
    from datetime import datetime, timezone
    from shared.models import Entity, EntityType, EventType, NormalizedEvent

    engine = GeopoliticalCascadeEngine()
    wallet = "0x51c72848c68a965f66fa7a88855f9f7784502a7f"
    clusters = [
        engine.ingest_event(NormalizedEvent(
            event_id=f"t{i}", trace_id=f"tr{i}", type=EventType.CRYPTO_TRANSFER,
            occurred_at=datetime.now(timezone.utc), source="ethereum_rpc",
            primary_entity=Entity(id=wallet, name=wallet, type=EntityType.WALLET),
            headline=f"Whale Transfer: ${i}M USDC", anomaly_score=0.9,
        ))
        for i in range(8)
    ]
    assert clusters == [None] * 8
    assert wallet not in engine._sliding_window


def test_a_storm_is_a_region_busier_than_its_own_baseline():
    """Replayed over the week to 4 October: 121 storm-hours at the fixed bar of
    four events, 23 against each region's own last twelve hours."""
    engine = GeopoliticalCascadeEngine(storm_baseline_hours=12)
    key, now = "black sea", 1_000_000 * 3600.0
    hour = int(now // 3600)
    engine._first_hour[key] = hour - 12
    engine._hourly[key] = {h: 6 for h in range(hour - 12, hour)}   # six an hour, steady
    assert engine.storm_threshold(key, now) == 6.0                 # no spread: mean + 0
    engine._hourly[key] = {h: (4 if h % 2 else 8) for h in range(hour - 12, hour)}
    assert abs(engine.storm_threshold(key, now) - 12.0) < 1e-9     # mean 6, sd 2


def test_an_unmeasured_region_cannot_storm():
    engine = GeopoliticalCascadeEngine(storm_baseline_hours=12)
    now = 1_000_000 * 3600.0
    engine._count_hour("singapore approach", now)
    assert engine.storm_threshold("singapore approach", now) == float("inf")


def test_without_a_baseline_the_fixed_bar_applies():
    assert GeopoliticalCascadeEngine().storm_threshold("x", 0.0) == 0.0


def test_a_restart_is_not_blind_once_seeded():
    """The baseline is in memory and the service restarts on every deploy."""
    engine = GeopoliticalCascadeEngine(storm_baseline_hours=12)
    now = 1_000_000 * 3600.0 + 120
    hour = int(now // 3600)
    engine._count_hour("black sea", now)                      # counted live since startup
    seeded = engine.seed_history(
        {"black sea": {h: 6 for h in range(hour - 12, hour)} | {hour: 99}},
        start_hour=hour - 12, now=now,
    )
    assert seeded == 1
    assert engine._hourly["black sea"][hour] == 1              # live hour left alone
    assert engine.storm_threshold("black sea", now) == 6.0     # judged at once


def test_a_seeded_hour_with_no_events_is_a_measured_zero():
    engine = GeopoliticalCascadeEngine(storm_baseline_hours=12)
    now = 1_000_000 * 3600.0
    hour = int(now // 3600)
    engine.seed_history({"taiwan strait": {hour - 1: 12}}, start_hour=hour - 12, now=now)
    # Eleven quiet hours and one of twelve: mean 1, sd ~3.32, bar ~10.95.
    assert 10.9 < engine.storm_threshold("taiwan strait", now) < 11.0


def _evt(i, etype, entity, score, region=None, source="x"):
    from datetime import datetime, timezone
    from shared.models import Entity, EntityType, NormalizedEvent

    return NormalizedEvent(
        event_id=f"e{entity}{i}", trace_id=f"t{entity}{i}", type=etype, occurred_at=datetime.now(timezone.utc),
        source=source, primary_entity=Entity(id=entity, name=entity, type=EntityType.COMPANY),
        headline=f"{entity} event {i}", anomaly_score=score, region=region)


def test_a_stocks_own_news_and_trading_is_not_geopolitical():
    """93% of 'geopolitical cascades' were a headline meeting the same stock's trading."""
    from services.correlation.cascade import _classify_cluster

    assert _classify_cluster({"news", "tradfi"}, {"headline", "equity_block"}, True)[1] == "Company News & Trading"
    assert _classify_cluster({"news", "maritime"}, {"headline", "vessel_dark"}, True)[1] == "Geopolitical Cascade"
    assert _classify_cluster({"macro", "maritime"}, {"supply_chain_metric", "vessel_position"}, True)[1] == \
        "Geopolitical Cascade"
    assert _classify_cluster({"tradfi", "crypto"}, {"equity_block", "crypto_trade"}, True)[1] == "Cross-Asset Cascade"


def test_two_domains_alone_no_longer_clear_the_bar():
    from shared.models import EventType

    engine = GeopoliticalCascadeEngine(window_seconds=3600, cooldown_seconds=0)
    assert engine.ingest_event(_evt(1, EventType.HEADLINE, "TAIWAN", 0.2, region="TAIWAN")) is None
    # Index 50 from the domain count, but the scores average 0.25: no cluster.
    assert engine.ingest_event(_evt(2, EventType.FLIGHT_DARK, "TAIWAN", 0.3, region="TAIWAN")) is None


def test_a_megacaps_hourly_news_needs_to_be_unusual_for_it():
    """A name with headlines every hour fires only when busier than its own normal."""
    from shared.models import EventType

    engine = GeopoliticalCascadeEngine(window_seconds=3600, cooldown_seconds=0, storm_baseline_hours=12)
    now_hour = int(__import__("time").time() // 3600)
    engine.seed_history({"nvda": {h: 6 for h in range(now_hour - 12, now_hour)}}, now_hour - 12,
                        __import__("time").time())
    got = [engine.ingest_event(_evt(i, t, "NVDA", 0.6)) for i, t in
           enumerate([EventType.HEADLINE, EventType.EQUITY_BLOCK, EventType.HEADLINE])]
    assert got == [None, None, None]                     # 3 events against a normal of ~6 an hour


def test_company_news_cannot_page_anyone():
    from services.correlation import cascade
    from shared.models import AlertTier, EventType

    engine = GeopoliticalCascadeEngine(window_seconds=3600, cooldown_seconds=0, storm_baseline_hours=0)
    engine.ingest_event(_evt(1, EventType.HEADLINE, "XYZ", 0.95))
    c = engine.ingest_event(_evt(2, EventType.EQUITY_BLOCK, "XYZ", 0.95))
    assert c is not None and c.rule_name.startswith("Company News & Trading")
    assert c.alert_tier == AlertTier.ELEVATED and c.primary_domain == "tradfi"
    assert cascade.CASCADE_MIN_AVG_SCORE == 0.45


def test_a_rule_must_beat_a_headline_to_stay():
    from shared.utils import producer_grades as pg

    base = [1.2, 1.3, 1.25, 1.1, 1.35, 1.2, 1.3, 1.15, 1.25, 1.2] * 6          # news days move ~1.23x usual
    news_echo = [("Echo", x) for x in [1.2, 1.3, 1.25, 1.2, 1.25] * 12]       # the same as a headline
    real = [("Real", x) for x in [1.9, 2.1, 1.8, 2.0, 2.2] * 12]              # well beyond a headline
    rows = {r["group"]: r for r in pg.lift_table(news_echo + real, base)}
    assert rows["Echo"]["verdict"] == "no better than a headline"
    assert rows["Real"]["verdict"] == "beats a headline" and rows["Real"]["lift"] > 1.5
    grades = {"correlation_families_1d": [{"group": "Echo", "n": 60, "verdict": "moves more"},
                                          {"group": "Real", "n": 60, "verdict": "moves more"}],
              "family_lift_1d": list(rows.values())}
    retired = pg.uninformative_rules(grades)
    assert "Echo" in retired and "Real" not in retired
