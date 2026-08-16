"""
tests/test_api_gateway.py

Consolidated Test Suite: API Gateway Metrics, Route Helpers, Market Series & Raw/Normalized Event Models.
Combines:
  - test_api_gateway_metrics.py
  - test_entity_normalization_and_routes.py
  - test_market_series_route.py
  - test_events.py
"""

import asyncio
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
from pydantic import ValidationError
from fastapi.testclient import TestClient

from services.api_gateway.routes.main import app
from services.api_gateway.helpers import get_clean_entity_label
from services.api_gateway.routes.radar import get_market_series
from shared.models.events import RawEvent, NormalizedEvent, EventType, Entity, EntityType, CorrelationCluster, AlertTier, FinancialData, RawIngestEnvelope, compute_payload_hash
from shared.utils.equities import parse_occ_option_symbol


# ── 1. API GATEWAY METRICS & AUTH TEST ───────────────────────────────────────

def test_api_gateway_metrics_endpoints_bypass_auth():
    client = TestClient(app)
    res_metrics = client.get("/metrics")
    assert res_metrics.status_code == 200

    res_json = client.get("/metrics/json")
    assert res_json.status_code == 200
    assert isinstance(res_json.json(), dict)

    res_health_metrics = client.get("/api/v1/health/metrics")
    assert res_health_metrics.status_code == 200

def test_api_gateway_protected_endpoint_requires_auth():
    client = TestClient(app)
    # 1. No credentials -> 403
    res = client.get("/api/v1/events/recent")
    assert res.status_code == 403

    # 2. Forged / random cookie string -> 403 (Fixes 1.1 authentication bypass)
    client.cookies.set("sentinel_session", "fake_unverified_session_cookie")
    res_fake = client.get("/api/v1/events/recent")
    assert res_fake.status_code == 403

    # 3. Expired JWT token cookie -> 403
    from services.api_gateway.dependencies import create_jwt_token, verify_session_token, verify_websocket_api_key
    expired_jwt = create_jwt_token({"sub": "admin@sentinel.io"}, expires_in_seconds=-3600)
    client.cookies.set("sentinel_session", expired_jwt)
    res_expired = client.get("/api/v1/events/recent")
    assert res_expired.status_code == 403

    # 4. Valid JWT token cookie -> 200 (or passes auth dependency)
    valid_jwt = create_jwt_token({"sub": "admin@sentinel.io"}, expires_in_seconds=3600)
    is_valid, sub = verify_session_token(valid_jwt)
    assert is_valid is True
    assert sub == "admin@sentinel.io"

    # 5. Invalid / forged secret JWT -> False
    forged_jwt = create_jwt_token({"sub": "admin@sentinel.io"}, secret="wrong-secret-key")
    is_forged_valid, _ = verify_session_token(forged_jwt)
    assert is_forged_valid is False

def test_websocket_auth_cryptographic_verification():
    import asyncio
    from unittest.mock import AsyncMock, MagicMock
    from services.api_gateway.dependencies import verify_websocket_api_key, create_jwt_token

    async def _run():
        # 1. Unauthenticated websocket
        ws_unauth = MagicMock()
        ws_unauth.cookies = {}
        ws_unauth.headers = {}
        ws_unauth.query_params = {}
        ws_unauth.close = AsyncMock()
        assert await verify_websocket_api_key(ws_unauth) is False
        ws_unauth.close.assert_called_with(code=4003, reason="Invalid API key or session cookie")

        # 2. Forged cookie websocket -> rejected
        ws_forged = MagicMock()
        ws_forged.cookies = {"sentinel_session": "forged_random_cookie_token"}
        ws_forged.headers = {}
        ws_forged.query_params = {}
        ws_forged.close = AsyncMock()
        assert await verify_websocket_api_key(ws_forged) is False

        # 3. Valid JWT session cookie websocket -> accepted
        valid_jwt = create_jwt_token({"sub": "ws_client@sentinel.io"}, expires_in_seconds=3600)
        ws_valid = MagicMock()
        ws_valid.cookies = {"sentinel_session": valid_jwt}
        ws_valid.headers = {}
        ws_valid.query_params = {}
        assert await verify_websocket_api_key(ws_valid) is True

    asyncio.run(_run())


# ── 2. ROUTE HELPERS & ENTITY NORMALIZATION ──────────────────────────────────

def test_get_clean_entity_label():
    assert get_clean_entity_label("Apple Inc", "AAPL") == "Apple Inc"
    assert get_clean_entity_label(None, "NVDA") == "NVDA"
    assert get_clean_entity_label(None, None) == "Global Entity"

def test_native_model_primary_entity_auto_derivation():
    cluster = CorrelationCluster(
        rule_id="RULE_01", rule_name="Test Rule", alert_tier=AlertTier.ALERT,
        trigger_event_id="evt_01", entity_ids=["NVDA"], entity_names=["NVIDIA Corp"],
        description="Test correlation"
    )
    assert cluster.primary_entity_id == "NVDA"
    assert cluster.primary_entity_name == "NVIDIA Corp"


# ── 3. MARKET SERIES ROUTE TEST ──────────────────────────────────────────────

def test_get_market_series_with_mock_db():
    async def _run():
        mock_db = AsyncMock()
        mock_db.query.return_value = [
            {"primary_entity_id": "BTCUSD", "primary_entity_name": "Bitcoin / USD", "occurred_at": "2026-08-05T12:00:00Z", "anomaly_score": 0.75, "financial_data": {"current_price": 67800.0, "volume": 12500}, "crypto_data": None},
            {"primary_entity_id": "TLT", "primary_entity_name": "iShares 20+ Year Treasury Bond ETF", "occurred_at": "2026-08-05T12:00:00Z", "anomaly_score": 0.40, "financial_data": {"current_price": 92.80, "volume": 45000}, "crypto_data": None}
        ]
        res = await get_market_series(symbols="BTCUSD,TLT", limit=20, db=mock_db)
        assert "BTCUSD" in res["symbols"]
        assert "TLT" in res["symbols"]
        assert "BTCUSD" in res["series"]

    asyncio.run(_run())


# ── 4. RAW & NORMALIZED EVENT MODEL CONTRACTS ────────────────────────────────

def test_raw_event_auto_generates_fields():
    payload = {"source": "news_scraper", "raw_payload": {"title": "Test Headline"}}
    event = RawEvent(**payload)
    assert event.event_id is not None
    assert event.source == "news_scraper"
    assert event.envelope.payload_hash != ""

def test_raw_ingest_envelope_auto_generates_fields_and_sha256_hash():
    raw_payload = {"ticker": "AAPL", "price": 180.5, "volume": 1000}
    env = RawIngestEnvelope(source_id="alpaca_options", payload=raw_payload)
    assert env.source_id == "alpaca_options"
    assert env.payload_hash == compute_payload_hash(raw_payload)

def test_normalized_event_rejects_invalid_anomaly_score():
    valid_data = {
        "event_id": "123e4567-e89b-12d3-a456-426614174000", "type": EventType.HEADLINE,
        "occurred_at": datetime.now(timezone.utc), "collected_at": datetime.now(timezone.utc),
        "source": "reuters", "primary_entity": {"id": "ent_1", "name": "Taiwan", "type": "country", "flags": []},
        "anomaly_score": 1.5
    }
    with pytest.raises(ValidationError) as exc_info:
        NormalizedEvent(**valid_data)
    assert "anomaly_score" in str(exc_info.value)

def test_parse_occ_option_symbol():
    call_res = parse_occ_option_symbol("AAPL240816C00220000")
    assert call_res == {"ticker": "AAPL", "expiry": "2024-08-16", "option_type": "CALL", "strike": 220.0}

def test_payload_readability_and_summaries():
    now = datetime.now(timezone.utc)
    event = NormalizedEvent(
        type=EventType.OPTIONS_FLOW, occurred_at=now, source="alpaca_options",
        primary_entity=Entity(id="AAPL", name="Apple Inc.", type=EntityType.COMPANY),
        headline="Apple Inc $1.5M Call Option Sweep", anomaly_score=0.88,
        financial_data=FinancialData(ticker="AAPL", premium_usd=1500000.0)
    )
    summary_text = event.to_readable_summary()
    assert "[OPTIONS_FLOW]" in summary_text
    assert "Apple Inc." in summary_text


def test_covered_call_endpoint():
    from unittest.mock import AsyncMock
    from fastapi.testclient import TestClient
    from services.api_gateway.routes.main import app
    from services.api_gateway.dependencies import get_redis_optional, get_db_optional, create_jwt_token

    client = TestClient(app)
    client.cookies.set("sentinel_session", create_jwt_token({"sub": "admin@sentinel.io"}))

    mock_redis = AsyncMock()
    mock_redis.raw.get.return_value = None
    app.dependency_overrides[get_redis_optional] = lambda: mock_redis

    try:
        # 1. Explicit current_price supplied -> succeeds
        response = client.get(
            "/api/v1/radar/options/covered-calls?ticker=NVDA&z_score=2.8&current_price=130.0"
        )
        assert response.status_code == 200
        data = response.json()
        assert data["strategy"] == "COVERED_CALL_OVERLAY"
        assert data["ticker"] == "NVDA"
        assert data["recommended_strike"] > 130.0
        assert "risk_free_rate_note" in data

        # 2. current_price omitted & not in Redis -> returns HTTP 400 explicit error (no fabricated default)
        mock_redis.raw.get.side_effect = lambda k: None
        err_res = client.get(
            "/api/v1/radar/options/covered-calls?ticker=NVDA&z_score=2.8"
        )
        assert err_res.status_code == 400
        assert "No live price cached" in err_res.json()["detail"]

        # 3. current_price omitted & found in Redis cache -> retrieves cached price and succeeds
        mock_redis.raw.get.side_effect = lambda k: "135.5" if "quotes" in k else None
        cached_res = client.get(
            "/api/v1/radar/options/covered-calls?ticker=NVDA&z_score=2.8"
        )
        assert cached_res.status_code == 200
        assert cached_res.json()["underlying_price"] == 135.5
    finally:
        app.dependency_overrides.pop(get_redis_optional, None)


def test_fetch_on_the_spot_historical_2y_yield():
    import asyncio
    from unittest.mock import AsyncMock
    from services.api_gateway.routes.radar import fetch_on_the_spot_historical

    async def _run():
        mock_redis = AsyncMock()
        mock_redis.raw.get.return_value = None

        # Fetch for 2Y Treasury Yield symbols
        pts = await fetch_on_the_spot_historical("US2Y", limit=10, redis=mock_redis)
        assert isinstance(pts, list)
        assert len(pts) > 0
        p = pts[-1]
        assert "price" in p
        assert "provider" in p
        assert "source_type" in p
        # Ensure yield is realistic (e.g. 1% to 10%)
        assert 0.5 <= p["price"] <= 15.0

    asyncio.run(_run())


def test_candles_multi_timeframe_and_cagg_fallback():
    from services.api_gateway.dependencies import get_redis_optional, get_db_optional, create_jwt_token
    client = TestClient(app)
    client.cookies.set("sentinel_session", create_jwt_token({"sub": "admin@sentinel.io"}))

    # 1. Multi-timeframe invalid timeframe validation
    inv_res = client.get("/api/v1/radar/candles/NVDA?timeframe=invalid_tf")
    assert inv_res.status_code == 400

    # 2. Database CAGG fallback when Redis is empty
    mock_redis = MagicMock()
    mock_redis.raw.lrange = AsyncMock(return_value=[])  # Cold cache

    mock_db = MagicMock()
    mock_db.query = AsyncMock(return_value=[
        {"ts": datetime(2026, 8, 15, 12, 0, 0, tzinfo=timezone.utc), "open": 130.0, "high": 132.0, "low": 129.5, "close": 131.5, "volume": 50000.0}
    ])

    app.dependency_overrides[get_redis_optional] = lambda: mock_redis
    app.dependency_overrides[get_db_optional] = lambda: mock_db

    try:
        for tf in ["1m", "5m", "15m", "1h", "1d", "1w", "1M"]:
            res = client.get(f"/api/v1/radar/candles/NVDA?timeframe={tf}&limit=10")
            assert res.status_code == 200
            data = res.json()
            assert data["ticker"] == "NVDA"
            assert data["timeframe"] == tf
            assert data["count"] == 1
            assert data["candles"][0]["close"] == 131.5
    finally:
        app.dependency_overrides.pop(get_redis_optional, None)
        app.dependency_overrides.pop(get_db_optional, None)


def test_covered_calls_zscore_view_lookup():
    from services.api_gateway.dependencies import get_redis_optional, get_db_optional, create_jwt_token
    client = TestClient(app)
    client.cookies.set("sentinel_session", create_jwt_token({"sub": "admin@sentinel.io"}))

    mock_redis = MagicMock()
    mock_redis.raw.get = AsyncMock(side_effect=lambda k: "130.0" if "quotes" in k else None)

    mock_db = MagicMock()
    mock_db.query_one = AsyncMock(return_value={"z_score": 3.1})

    app.dependency_overrides[get_redis_optional] = lambda: mock_redis
    app.dependency_overrides[get_db_optional] = lambda: mock_db

    try:
        # Omit z_score -> triggers query against tradfi_bars_5m_zscore view (§2.6)
        res = client.get("/api/v1/radar/options/covered-calls?ticker=NVDA")
        assert res.status_code == 200
        data = res.json()
        assert data["strategy"] == "COVERED_CALL_OVERLAY"
        assert data["ticker"] == "NVDA"
        assert data["underlying_price"] == 130.0
    finally:
        app.dependency_overrides.pop(get_redis_optional, None)
        app.dependency_overrides.pop(get_db_optional, None)


