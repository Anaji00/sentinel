# tests/test_events.py
import asyncio
import pytest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
from pydantic import ValidationError
from shared.models.events import RawEvent, NormalizedEvent, EventType


def test_raw_event_auto_generates_fields():
    """Test that RawEvent automatically generates an ID and Timestamp if missing."""
    payload = {
        "source": "news_scraper",
        "raw_payload": {"title": "Test Headline"}
    }
    
    event = RawEvent(**payload)
    
    assert event.event_id is not None
    assert type(event.event_id) == str
    assert event.collected_at is not None
    assert event.source == "news_scraper"
    assert event.envelope is not None
    assert event.envelope.source_id == "news_scraper"
    assert event.envelope.payload == {"title": "Test Headline"}
    assert event.envelope.payload_hash != ""


def test_raw_ingest_envelope_auto_generates_fields_and_sha256_hash():
    """Test that RawIngestEnvelope computes source_id, ISO8601 ingest_timestamp, and SHA-256 payload_hash."""
    from shared.models.events import RawIngestEnvelope, compute_payload_hash

    raw_payload = {"ticker": "AAPL", "price": 180.5, "volume": 1000}
    env = RawIngestEnvelope(source_id="alpaca_options", payload=raw_payload)

    assert env.source_id == "alpaca_options"
    assert "T" in env.ingest_timestamp
    assert env.payload_hash == compute_payload_hash(raw_payload)

    # Test alias fallback from source / raw_payload
    env_alias = RawIngestEnvelope(source="aisstream", raw_payload={"mmsi": "123456789"})
    assert env_alias.source_id == "aisstream"
    assert env_alias.payload == {"mmsi": "123456789"}
    assert env_alias.payload_hash == compute_payload_hash({"mmsi": "123456789"})


def test_raw_event_preserves_primary_entity_names_end_to_end():
    """Verify RawEvent integration and that primary entity names are preserved upon normalization."""
    from shared.models.events import RawEvent, NormalizedEvent, EventType, Entity, EntityType

    raw = RawEvent(
        source_id="coinbase_spot",
        raw_payload={"symbol": "BTC-USD", "price": 65000.0, "entity_name": "Bitcoin Core"}
    )
    assert raw.source_id == "coinbase_spot"
    assert raw.envelope is not None
    assert raw.envelope.source_id == "coinbase_spot"

    norm = NormalizedEvent(
        type=EventType.CRYPTO_TRADE,
        occurred_at=datetime.now(timezone.utc),
        source=raw.source_id,
        primary_entity=Entity(id="BTC-USD", name="Bitcoin Core", type=EntityType.INSTRUMENT),
        anomaly_score=0.3
    )

    assert norm.primary_entity.id == "BTC-USD"
    assert norm.primary_entity.name == "Bitcoin Core"
    assert "Bitcoin Core" in norm.to_readable_summary()



def test_normalized_event_rejects_invalid_anomaly_score():
    """Test that the data contract rejects scores outside the 0.0-1.0 range."""
    # Build a valid baseline event
    valid_data = {
        "event_id": "123e4567-e89b-12d3-a456-426614174000",
        "type": EventType.HEADLINE,
        "occurred_at": datetime.now(timezone.utc),
        "collected_at": datetime.now(timezone.utc),
        "source": "reuters",
        "primary_entity": {"id": "ent_1", "name": "Taiwan", "type": "country", "flags": []},
        "anomaly_score": 1.5  # <--- INVALID SCORE! Must be 0.0 to 1.0
    }
    
    # Pydantic should instantly throw a ValidationError
    with pytest.raises(ValidationError) as exc_info:
        NormalizedEvent(**valid_data)
    
    # Verify the error is specifically about the anomaly_score
    assert "anomaly_score" in str(exc_info.value)


def test_options_flow_enrichment():
    """Verify that TradFiEnricher correctly processes options flow raw events."""
    from services.enrichment.enrichers.tradfi import TradFiEnricher
    
    async def run_test():
        redis_mock = MagicMock()
        redis_mock.raw.get = AsyncMock(return_value=b'{}')
        graph_mock = MagicMock()
        graph_mock.producer = AsyncMock()
        scorer_mock = MagicMock()
        scorer_mock.check_watchlist = AsyncMock(return_value=True)
        scorer_mock.track_frequency = AsyncMock(return_value=0.10)
        
        enricher = TradFiEnricher(scorer=scorer_mock, redis_client=redis_mock, graph_writer=graph_mock)
        
        raw_event = RawEvent(
            source="alpaca_options",
            raw_payload={
                "ticker": "AAPL",
                "option_symbol": "AAPL240621C00200000",
                "price": 5.20,
                "volume": 200.0,
                "premium_usd": 104000.0
            }
        )
        
        normalized = await enricher.enrich(raw_event)
        
        assert normalized is not None
        assert normalized.type == EventType.OPTIONS_FLOW
        assert normalized.primary_entity.id == "AAPL"
        assert normalized.financial_data.instrument_type == "option"
        assert normalized.financial_data.side == "CALL"
        assert normalized.financial_data.strike == 200.0
        assert normalized.financial_data.expiry == "2024-06-21"
        assert normalized.financial_data.premium_usd == 104000.0
        assert "options_sweep" in normalized.tags
        assert normalized.anomaly_score > 0.0
        
    asyncio.run(run_test())


def test_parse_occ_option_symbol():
    """Verify OCC option symbol parser correctly extracts ticker, expiry, option_type, and strike."""
    from shared.utils.equities import parse_occ_option_symbol
    
    call_res = parse_occ_option_symbol("AAPL240816C00220000")
    assert call_res == {
        "ticker": "AAPL",
        "expiry": "2024-08-16",
        "option_type": "CALL",
        "strike": 220.0
    }
    
    put_res = parse_occ_option_symbol("NVDA250117P00135000")
    assert put_res == {
        "ticker": "NVDA",
        "expiry": "2025-01-17",
        "option_type": "PUT",
        "strike": 135.0
    }
    
    assert parse_occ_option_symbol("INVALID") is None



def test_entity_resolver_publishes_unknown_entities_on_miss():
    """Verify that EntityResolver publishes to Topics.UNKNOWN_ENTITIES with raw_entity_name, context_event_id, and timestamp on resolution miss."""
    from services.enrichment.entity_resolver import EntityResolver
    from shared.kafka import Topics

    async def run_test():
        redis_mock = MagicMock()
        redis_mock.raw.get = AsyncMock(return_value=None)
        neo4j_mock = MagicMock()
        neo4j_mock.execute_and_fetch = AsyncMock(return_value=[])
        producer_mock = MagicMock()
        producer_mock.send = AsyncMock()

        resolver = EntityResolver(redis_mock, neo4j_mock, producer=producer_mock)

        # 1. Force resolution miss on resolve_vessel
        res_vessel = await resolver.resolve_vessel(
            "999999999",
            ais_meta=None,
            context_event_id="evt-vessel-123",
            timestamp="2026-07-29T18:00:00Z"
        )

        assert res_vessel is None
        producer_mock.send.assert_called_once()
        topic, payload = producer_mock.send.call_args[0][:2]
        assert topic == Topics.UNKNOWN_ENTITIES
        assert payload["raw_entity_name"] == "999999999"
        assert payload["context_event_id"] == "evt-vessel-123"
        assert payload["timestamp"] == "2026-07-29T18:00:00Z"

        producer_mock.send.reset_mock()

        # 2. Force resolution miss on generic resolve
        res_generic = await resolver.resolve(
            "unknown_corp_456",
            context_event_id="evt-corp-456",
            timestamp="2026-07-29T18:05:00Z"
        )

        assert res_generic is None
        producer_mock.send.assert_called_once()
        topic_gen, payload_gen = producer_mock.send.call_args[0][:2]
        assert topic_gen == Topics.UNKNOWN_ENTITIES
        assert payload_gen["raw_entity_name"] == "unknown_corp_456"
        assert payload_gen["context_event_id"] == "evt-corp-456"
        assert payload_gen["timestamp"] == "2026-07-29T18:05:00Z"

    asyncio.run(run_test())


def test_payload_readability_and_summaries():
    """Verify that NormalizedEvent and CorrelationCluster generate clean, readable summaries."""
    from shared.models.events import NormalizedEvent, EventType, Entity, EntityType, CorrelationCluster, AlertTier, FinancialData

    now = datetime.now(timezone.utc)
    event = NormalizedEvent(
        type=EventType.OPTIONS_FLOW,
        occurred_at=now,
        source="alpaca_options",
        primary_entity=Entity(id="AAPL", name="Apple Inc.", type=EntityType.COMPANY),
        headline="Apple Inc $1.5M Call Option Sweep",
        anomaly_score=0.88,
        financial_data=FinancialData(ticker="AAPL", premium_usd=1500000.0)
    )

    summary_text = event.to_readable_summary()
    assert "[OPTIONS_FLOW]" in summary_text
    assert "Apple Inc." in summary_text
    assert "AAPL" in summary_text
    assert "$1,500,000.00" in summary_text
    assert "0.88" in summary_text

    cluster = CorrelationCluster(
        rule_id="RULE_001",
        rule_name="Cross-Domain Threat Convergence",
        alert_tier=AlertTier.CRITICAL,
        primary_domain="maritime",
        confidence_score=0.92,
        summary_headline="🚨 High Risk Vessel STS Transfer in Black Sea",
        supporting_headlines=["Vessel DARK AIS signal in Sevastopol", "Flight Anomaly near Crimea"],
        trigger_event_id="evt-1",
        entity_names=["Vessel Titanic", "USAF Recon"],
        description="Correlated maritime dark activity with military reconnaissance flights."
    )

    cluster_summary = cluster.to_readable_summary()
    assert "CRITICAL" in cluster_summary
    assert "MARITIME" in cluster_summary
    assert "High Risk Vessel STS Transfer" in cluster_summary
    assert "92%" in cluster_summary
    assert "Vessel Titanic" in cluster_summary
    assert "Supporting Evidence:" in cluster_summary