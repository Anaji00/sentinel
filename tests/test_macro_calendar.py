"""
tests/test_macro_calendar.py

Unit and integration tests for Macroeconomic Calendar Collector & Event Serialization (§5.1, §5.2).
"""

import pytest
import importlib.util
from pathlib import Path
from datetime import datetime, timezone
from shared.models import NormalizedEvent, EventType, EntityType, Entity, MacroReleaseData

# Dynamically load services/collector-macro/economic_calendar.py
macro_cal_path = Path(__file__).resolve().parents[1] / "services" / "collector-macro" / "economic_calendar.py"
spec = importlib.util.spec_from_file_location("economic_calendar", macro_cal_path)
macro_cal_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(macro_cal_module)

EconomicCalendarCollector = macro_cal_module.EconomicCalendarCollector
INDICATOR_PROFILES = macro_cal_module.INDICATOR_PROFILES


class MockProducer:
    def __init__(self):
        self.sent = []

    async def send(self, topic, message, key=None):
        self.sent.append((topic, message, key))


@pytest.mark.anyio
async def test_macro_release_event_structure():
    """Validates MacroReleaseData and NormalizedEvent schema for economic calendar releases."""
    macro_data = MacroReleaseData(
        indicator="CPI",
        country="US",
        period="2026-07",
        actual=3.1,
        forecast=2.9,
        previous=3.0,
        surprise=0.2,
        surprise_pct=6.90,
        unit="%",
        impact_bias="INFLATIONARY_HAWKISH",
        raw_source="finnhub_calendar",
    )

    event = NormalizedEvent(
        event_id="macro_cpi_test_001",
        type=EventType.MACRO_RELEASE,
        occurred_at=datetime.now(timezone.utc),
        collected_at=datetime.now(timezone.utc),
        source="finnhub_calendar",
        source_reliability=0.98,
        headline="Macro Release: CPI Actual 3.1% vs Forecast 2.9%",
        summary="US CPI inflation exceeded consensus forecast by 0.20%.",
        region="US",
        country_code="US",
        primary_entity=Entity(
            id="MACRO:CPI",
            name="US CPI (YoY)",
            type=EntityType.INSTRUMENT,
        ),
        anomaly_score=0.75,
        macro_data=macro_data,
        tags=["macro_release", "indicator:cpi", "bias:inflationary_hawkish"],
    )

    dumped = event.model_dump()
    assert dumped["type"] == EventType.MACRO_RELEASE.value
    assert dumped["macro_data"]["indicator"] == "CPI"
    assert dumped["macro_data"]["actual"] == 3.1
    assert dumped["macro_data"]["forecast"] == 2.9
    assert dumped["macro_data"]["surprise"] == 0.2
    assert dumped["macro_data"]["impact_bias"] == "INFLATIONARY_HAWKISH"
    assert event.is_financial() is True


@pytest.mark.anyio
async def test_economic_calendar_collector_surprise_computation():
    """Tests surprise magnitude, directional bias, and anomaly score calculations."""
    collector = EconomicCalendarCollector()

    # 1. Higher CPI = Inflationary / Hawkish surprise
    surprise, surprise_pct, bias, anomaly_score = collector.compute_surprise(
        indicator="CPI", actual=3.4, forecast=3.1, previous=3.0
    )
    assert surprise == 0.3
    assert surprise_pct > 0
    assert "INFLATIONARY_HAWKISH" in bias
    assert anomaly_score >= 0.75

    # 2. Lower Unemployment = Expansionary / Hawkish
    surprise, surprise_pct, bias, anomaly_score = collector.compute_surprise(
        indicator="UNEMPLOYMENT", actual=3.8, forecast=4.1, previous=4.0
    )
    assert surprise == -0.3
    assert "INVERSE" in bias
    assert anomaly_score >= 0.60

    # 3. Exactly on consensus
    surprise, surprise_pct, bias, anomaly_score = collector.compute_surprise(
        indicator="GDP", actual=2.5, forecast=2.5, previous=2.4
    )
    assert surprise == 0.0
    assert surprise_pct == 0.0
    assert bias == "IN_LINE_WITH_CONSENSUS"
    assert anomaly_score <= 0.40


@pytest.mark.anyio
async def test_economic_calendar_collector_poll_and_publish():
    """Tests building and publishing macro release events via MockProducer."""
    producer = MockProducer()
    collector = EconomicCalendarCollector(producer=producer)

    # Mock fetch_finnhub_calendar
    async def mock_calendar():
        return [
            {
                "event": "US CPI (YoY)",
                "actual": 3.2,
                "estimate": 2.9,
                "prev": 3.0,
                "country": "US",
                "period": "2026-07",
                "time": "2026-08-15 08:30:00",
            },
            {
                "event": "US Non-Farm Payrolls",
                "actual": 250.0,
                "estimate": 180.0,
                "prev": 190.0,
                "country": "US",
                "period": "2026-07",
                "time": "2026-08-15 08:30:00",
            },
        ]

    collector.fetch_finnhub_calendar = mock_calendar
    events = await collector.poll_and_publish()

    assert len(events) == 2
    assert len(producer.sent) == 4  # RAW_EVENTS and ENRICHED_EVENTS for both

    cpi_event = events[0]
    assert cpi_event.macro_data.indicator == "CPI"
    assert cpi_event.macro_data.actual == 3.2
    assert cpi_event.macro_data.surprise == 0.3
    assert cpi_event.primary_entity.id == "MACRO:CPI"

    nfp_event = events[1]
    assert nfp_event.macro_data.indicator == "NFP"
    assert nfp_event.macro_data.actual == 250.0
    assert nfp_event.macro_data.surprise == 70.0
    assert nfp_event.primary_entity.id == "MACRO:NFP"
