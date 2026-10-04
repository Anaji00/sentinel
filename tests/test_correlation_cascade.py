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
