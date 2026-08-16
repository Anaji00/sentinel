"""
services/collector-macro/economic_calendar.py

ECONOMIC CALENDAR COLLECTOR (§5.1)
==================================
Polls and ingests scheduled macroeconomic releases:
- CPI (Consumer Price Index YoY / MoM)
- Core CPI
- NFP (Non-Farm Payrolls)
- Unemployment Rate
- FOMC Rate Decisions / Dot Plot
- GDP Growth (Annualized QoQ)
- ISM Manufacturing PMI
- PCE / Core PCE Inflation

Produces structurally distinct NormalizedEvent records with:
- type = EventType.MACRO_RELEASE
- actual, forecast, prior, surprise, and directional bias
"""

import asyncio
import json
import logging
import os
import uuid
import aiohttp
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Optional, Any

from shared.models import NormalizedEvent, EventType, EntityType, Entity, MacroReleaseData, AnomalyBreakdown
from shared.kafka import SentinelProducer, Topics

logger = logging.getLogger("feed.macro_calendar")

FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")

# Standard macro indicators with their economic transmission bias rules
INDICATOR_PROFILES = {
    "CPI": {
        "unit": "%",
        "description": "US Consumer Price Index (YoY)",
        "higher_is": "INFLATIONARY_HAWKISH",
        "aliases": ["CPI", "CONSUMER PRICE INDEX"],
    },
    "CORE_CPI": {
        "unit": "%",
        "description": "US Core CPI Ex-Food & Energy (YoY)",
        "higher_is": "INFLATIONARY_HAWKISH",
        "aliases": ["CORE CPI", "CPI EX FOOD"],
    },
    "NFP": {
        "unit": "k",
        "description": "US Non-Farm Payrolls Employment Change",
        "higher_is": "EXPANSIONARY_HAWKISH",
        "aliases": ["NFP", "NON-FARM", "NONFARM", "PAYROLLS"],
    },
    "UNEMPLOYMENT": {
        "unit": "%",
        "description": "US Unemployment Rate",
        "higher_is": "CONTRACTIONARY_DOVISH",
        "aliases": ["UNEMPLOYMENT", "JOBLESS RATE"],
    },
    "FOMC_RATE_DECISION": {
        "unit": "bps",
        "description": "Federal Reserve Target Fed Funds Rate Upper Bound",
        "higher_is": "HAWKISH_TIGHTENING",
        "aliases": ["FOMC", "FED FUNDS", "RATE DECISION", "INTEREST RATE"],
    },
    "GDP": {
        "unit": "%",
        "description": "US Real GDP Annualized Growth (QoQ)",
        "higher_is": "EXPANSIONARY",
        "aliases": ["GDP", "GROSS DOMESTIC PRODUCT"],
    },
    "ISM_MANUFACTURING_PMI": {
        "unit": "index",
        "description": "ISM Manufacturing Purchasing Managers Index",
        "higher_is": "EXPANSIONARY",
        "aliases": ["ISM", "PMI", "MANUFACTURING PMI"],
    },
    "PCE": {
        "unit": "%",
        "description": "US PCE Price Index (YoY)",
        "higher_is": "INFLATIONARY_HAWKISH",
        "aliases": ["PCE PRICE INDEX", "PCE DEFLATOR"],
    },
    "CORE_PCE": {
        "unit": "%",
        "description": "US Core PCE Price Index (Fed's Preferred Gauge)",
        "higher_is": "INFLATIONARY_HAWKISH",
        "aliases": ["CORE PCE", "PCE CORE"],
    },
}


class EconomicCalendarCollector:
    """
    Polls scheduled macroeconomic calendar events and publishes
    EventType.MACRO_RELEASE events with quantified surprise metrics.
    """

    def __init__(self, producer: Optional[SentinelProducer] = None, poll_interval_sec: int = 120):
        self.producer = producer
        self.poll_interval_sec = poll_interval_sec
        self._session: Optional[aiohttp.ClientSession] = None
        self._seen_releases: set = set()

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10))
        return self._session

    def compute_surprise(
        self, indicator: str, actual: float, forecast: Optional[float], previous: Optional[float]
    ) -> Tuple[float, float, str, float]:
        """
        Calculates surprise magnitude, surprise percentage, impact bias, and anomaly score.
        """
        expected = forecast if forecast is not None else previous
        if expected is None or abs(expected) < 1e-6:
            surprise = 0.0
            surprise_pct = 0.0
        else:
            surprise = round(actual - expected, 4)
            surprise_pct = round((surprise / abs(expected)) * 100.0, 2)

        # Determine directional bias
        profile = INDICATOR_PROFILES.get(indicator, {})
        higher_bias = profile.get("higher_is", "EXPANSIONARY")
        if surprise > 0:
            bias = higher_bias
        elif surprise < 0:
            bias = f"INVERSE_{higher_bias}"
        else:
            bias = "IN_LINE_WITH_CONSENSUS"

        # Calculate anomaly score based on surprise magnitude
        abs_surprise_pct = abs(surprise_pct)
        if abs_surprise_pct >= 25.0 or (indicator in ("CPI", "CORE_PCE") and abs(surprise) >= 0.3):
            anomaly_score = 0.90
        elif abs_surprise_pct >= 10.0 or abs(surprise) >= 0.15:
            anomaly_score = 0.75
        elif abs_surprise_pct >= 3.0 or abs(surprise) >= 0.05:
            anomaly_score = 0.60
        else:
            anomaly_score = 0.35

        return surprise, surprise_pct, bias, anomaly_score

    async def fetch_finnhub_calendar(self) -> List[Dict[str, Any]]:
        """Fetches economic calendar from Finnhub API."""
        if not FINNHUB_API_KEY:
            return []

        try:
            session = await self._get_session()
            url = f"https://finnhub.io/api/v1/calendar/economic?token={FINNHUB_API_KEY}"
            async with session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("economicCalendar", [])
        except Exception as e:
            logger.debug(f"Finnhub economic calendar fetch fallback: {e}")
        return []

    def build_macro_release_event(
        self,
        indicator: str,
        actual: float,
        forecast: Optional[float] = None,
        previous: Optional[float] = None,
        country: str = "US",
        period: Optional[str] = None,
        source: str = "economic_calendar",
    ) -> NormalizedEvent:
        """Constructs a validated NormalizedEvent for a macroeconomic release."""
        surprise, surprise_pct, bias, anomaly_score = self.compute_surprise(
            indicator, actual, forecast, previous
        )

        profile = INDICATOR_PROFILES.get(indicator, {})
        unit = profile.get("unit", "")
        desc = profile.get("description", f"{indicator} Release")
        period_str = f" ({period})" if period else ""

        macro_data = MacroReleaseData(
            indicator=indicator,
            country=country,
            period=period,
            actual=actual,
            forecast=forecast,
            previous=previous,
            surprise=surprise,
            surprise_pct=surprise_pct,
            unit=unit,
            impact_bias=bias,
            raw_source=source,
        )

        event = NormalizedEvent(
            event_id=f"macro_{indicator.lower()}_{uuid.uuid4().hex[:8]}",
            type=EventType.MACRO_RELEASE,
            occurred_at=datetime.now(timezone.utc),
            collected_at=datetime.now(timezone.utc),
            source=source,
            source_reliability=0.98,
            primary_entity=Entity(
                id=f"MACRO:{indicator.upper()}",
                name=f"{desc}{period_str}",
                type=EntityType.INSTRUMENT,
                flags=["macro_economic_release"],
            ),
            headline=f"📊 Macro Release: {indicator} Actual {actual}{unit} vs Forecast {forecast or 'N/A'}{unit} (Surprise: {surprise:+0.2f}{unit}, Bias: {bias})",
            summary=f"Official {desc} release for {country}{period_str}. Actual: {actual}{unit}, Consensus: {forecast or 'N/A'}{unit}, Prior: {previous or 'N/A'}{unit}. Surprise magnitude: {surprise_pct:+.1f}%. Directional bias: {bias}.",
            region="US",
            country_code=country,
            anomaly_score=anomaly_score,
            anomaly_breakdown=AnomalyBreakdown(
                composite_score=anomaly_score,
                volatility_z_score=min(3.5, abs(surprise_pct) / 10.0),
                is_significant=(anomaly_score >= 0.70),
                domain="financial",
            ),
            macro_data=macro_data,
            tags=["macro_release", f"indicator:{indicator.lower()}", f"bias:{bias.lower()}"],
        )
        return event

    async def poll_and_publish(self) -> List[NormalizedEvent]:
        """Polls economic calendar releases and publishes new events to Kafka."""
        events_published = []
        raw_calendar = await self.fetch_finnhub_calendar()

        for item in raw_calendar:
            event_name = (item.get("event") or "").upper()
            matched_indicator = None
            for ind, prof in INDICATOR_PROFILES.items():
                aliases = prof.get("aliases", [ind])
                if any(a in event_name for a in aliases):
                    matched_indicator = ind
                    break

            if not matched_indicator or item.get("actual") is None:
                continue

            release_key = f"{matched_indicator}_{item.get('time')}_{item.get('actual')}"
            if release_key in self._seen_releases:
                continue
            self._seen_releases.add(release_key)

            actual = float(item["actual"])
            forecast = float(item["estimate"]) if item.get("estimate") is not None else None
            previous = float(item["prev"]) if item.get("prev") is not None else None

            event = self.build_macro_release_event(
                indicator=matched_indicator,
                actual=actual,
                forecast=forecast,
                previous=previous,
                country=item.get("country", "US"),
                period=item.get("period"),
                source="finnhub_calendar",
            )

            if self.producer:
                await self.producer.send(Topics.RAW_TRADFI, event.model_dump(), key=event.primary_entity.id)
                await self.producer.send(Topics.ENRICHED_EVENTS, event.model_dump(), key=event.primary_entity.id)
                logger.info(f"📊 Emitted Macro Release Event: {event.headline}")

            events_published.append(event)

        return events_published

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
