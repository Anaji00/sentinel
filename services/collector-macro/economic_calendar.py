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
import sys
import uuid
import aiohttp
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Optional, Any

from shared.models import NormalizedEvent, EventType, EntityType, Entity, MacroReleaseData, AnomalyBreakdown, RawEvent
from shared.kafka import SentinelProducer, Topics

from shared.utils.quiet_failures import dropped, swallowed

import importlib.util as _ilu
import time as _time
from pathlib import Path as _Path


def _load_fred():
    """The sibling module, by path: `collector-macro` is not importable by name."""
    spec = _ilu.spec_from_file_location(
        "collector_macro_fred_releases", str(_Path(__file__).parent / "fred_releases.py")
    )
    module = _ilu.module_from_spec(spec)
    # Registered first: @dataclass resolves its module through sys.modules.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


fred = _load_fred()

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
        "unit": "%",
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
    # Market conditions from FRED, announced on a large weekly move.
    "HY_OAS": {"unit": "%", "description": "US High Yield Option-Adjusted Spread",
               "higher_is": "CREDIT_STRESS", "aliases": []},
    "IG_OAS": {"unit": "%", "description": "US Investment Grade Option-Adjusted Spread",
               "higher_is": "CREDIT_STRESS", "aliases": []},
    "CURVE_10Y2Y": {"unit": "%", "description": "10-Year minus 2-Year Treasury Spread",
                    "higher_is": "STEEPENING", "aliases": []},
    "CURVE_10Y3M": {"unit": "%", "description": "10-Year minus 3-Month Treasury Spread",
                    "higher_is": "STEEPENING", "aliases": []},
    "NFCI": {"unit": "", "description": "Chicago Fed National Financial Conditions Index",
             "higher_is": "TIGHTENING", "aliases": []},
}


class EconomicCalendarCollector:
    """
    Polls scheduled macroeconomic calendar events and publishes
    EventType.MACRO_RELEASE events with quantified surprise metrics.
    """

    def __init__(
        self,
        producer: Optional[SentinelProducer] = None,
        poll_interval_sec: int = 120,
        redis_client: Any = None,
    ):
        self.producer = producer
        self.poll_interval_sec = poll_interval_sec
        self.redis = redis_client
        self._session: Optional[aiohttp.ClientSession] = None
        self._seen_releases: set = set()
        self._fred_polled_at: float = 0.0

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
                    return (await resp.json()).get("economicCalendar", [])

                # Anything else is said out loud, once.
                #
                # aiohttp does not raise on a 4xx, so this fell through to
                # `return []` and the `except` below never saw it. The loop then
                # polled every 120 seconds, received nothing, and logged nothing
                # -- the caller only logs when `events` is non-empty.
                #
                # Measured consequence: this endpoint is premium on Finnhub and
                # answers 403 for this key, so the platform has ingested
                # **zero** CPI, payroll, FOMC, PCE or GDP releases in its entire
                # history. Every one of the 4,219 `macro_release` events on the
                # platform comes from the freight poller. The absence was
                # indistinguishable from a quiet calendar.
                #
                # `dropped` counts and escalates rather than logging every two
                # minutes forever: loud on the first, at powers of ten, and once
                # per interval after that.
                dropped(
                    "collector_macro.economic_calendar_unavailable",
                    f"Finnhub /calendar/economic returned {resp.status}"
                    + (" (premium endpoint, not available on this key)"
                       if resp.status == 403 else ""),
                    logger,
                    detail="no CPI/NFP/FOMC/PCE/GDP releases will be ingested",
                )
        except Exception as e:
            swallowed("collector_macro.economic_calendar_fetch", e, logger)
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
        occurred_at: Optional[datetime] = None,
        detail: str = "",
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

        # Measured against the prior reading when no consensus was supplied,
        # and worded that way. FRED publishes values, not forecasts, and a
        # change from last month called a "surprise" claims an expectation
        # nobody gave.
        has_consensus = forecast is not None
        extra = f" ({detail})" if detail else ""
        prior_text = f"{previous}{unit}" if previous is not None else "N/A"
        if has_consensus:
            headline = (
                f"📊 Macro Release: {indicator} Actual {actual}{unit} vs Forecast {forecast}{unit} "
                f"(Surprise: {surprise:+0.2f}{unit}, Bias: {bias})"
            )
            summary = (
                f"Official {desc} release for {country}{period_str}. Actual: {actual}{unit}, "
                f"Consensus: {forecast}{unit}, Prior: {prior_text}. "
                f"Surprise magnitude: {surprise_pct:+.1f}%. Directional bias: {bias}."
            )
        else:
            headline = (
                f"📊 Macro Release: {indicator} {actual}{unit} vs prior {prior_text} "
                f"(change {surprise:+0.2f}{unit}){extra}"
            )
            summary = (
                f"Official {desc} release for {country}{period_str}. Actual: {actual}{unit}, "
                f"Prior: {prior_text}{extra}. No consensus forecast is available from this "
                f"source, so the change is measured against the prior reading. Directional "
                f"reading of the change: {bias}."
            )

        event = NormalizedEvent(
            event_id=f"macro_{indicator.lower()}_{uuid.uuid4().hex[:8]}",
            type=EventType.MACRO_RELEASE,
            occurred_at=occurred_at or datetime.now(timezone.utc),
            collected_at=datetime.now(timezone.utc),
            source=source,
            source_reliability=0.98,
            primary_entity=Entity(
                id=f"MACRO:{indicator.upper()}",
                name=f"{desc}{period_str}",
                type=EntityType.INSTRUMENT,
                flags=["macro_economic_release"],
            ),
            headline=headline,
            summary=summary,
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
            tags=["macro_release", f"indicator:{indicator.lower()}", f"bias:{bias.lower()}"]
                 + ([] if has_consensus else ["no_consensus"]),
        )
        return event

    async def poll_and_publish(self) -> List[NormalizedEvent]:
        """Polls economic calendar releases and publishes new events to Kafka."""
        # FRED when it is configured, and not Finnhub as well: the Finnhub
        # calendar is premium on this key and can only ever answer 403.
        if fred.FRED_API_KEY:
            return await self.poll_fred()

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
                # Enriched only.
                #
                # `build_macro_release_event` returns a NormalizedEvent -- it
                # computes the surprise, the bias and the score itself, because
                # a scheduled release is a thing the collector understands
                # better than a generic scorer does. Sending the same object to
                # RAW_TRADFI as well did not enrich it twice: the consumer
                # parses raw topics as RawEvent, a NormalizedEvent has no
                # `raw_payload`, so it arrived with an empty one, matched no
                # branch, and incremented `enrichment.tradfi.unrouted_source`.
                #
                # That counter exists to find feeds being thrown away. Every
                # macro release was adding to it, which is how a signal becomes
                # unreadable -- the one instrument for this class of defect,
                # reporting a deliberate duplicate as a lost feed.
                await self._publish(event)
                logger.info(f"📊 Emitted Macro Release Event: {event.headline}")

            events_published.append(event)

        return events_published

    async def _publish(self, event: NormalizedEvent) -> None:
        """Through enrichment, so the release is persisted as well as read.

        Sent straight to ENRICHED_EVENTS it reached the agents and nothing
        wrote it to the events table. Wrapped as a RawEvent whose payload
        carries the finished event, the tradfi enricher passes it through
        unchanged -- it does not re-score it -- and the enrichment service
        stores and publishes it like any other.
        """
        raw = RawEvent(
            source=event.source,
            occurred_at=event.occurred_at,
            raw_payload={"normalized_event": event.model_dump(mode="json")},
        )
        await self.producer.send(Topics.RAW_TRADFI, raw.model_dump(), key=event.primary_entity.id)

    async def poll_fred(self) -> List[NormalizedEvent]:
        """New releases from FRED, at most once per FRED_POLL_INTERVAL_SEC."""
        now_mono = _time.monotonic()
        if self._fred_polled_at and now_mono - self._fred_polled_at < fred.FRED_POLL_INTERVAL_SEC:
            return []
        self._fred_polled_at = now_mono

        published: List[NormalizedEvent] = []
        session = await self._get_session()
        raw = getattr(self.redis, "raw", self.redis) if self.redis is not None else None
        now = datetime.now(timezone.utc)
        for series in fred.FRED_SERIES:
            try:
                observations, last_updated = await fred.fetch_series(session, series)
            except Exception as e:
                swallowed("collector_macro.fred_fetch", e, logger, variant=series.series_id)
                continue
            release = fred.release_from_observations(series, observations)
            if release is None:
                dropped(
                    "collector_macro.fred_insufficient_history",
                    "not enough observations to compute the release",
                    logger, variant=series.series_id,
                )
                continue
            last_seen = None
            if raw is not None:
                try:
                    stored = await raw.hget(fred.FRED_STATE_KEY, series.series_id)
                    last_seen = stored.decode() if isinstance(stored, bytes) else stored
                except Exception as e:
                    # Without the record a restart would re-announce; skip
                    # this series until it can be read.
                    swallowed("collector_macro.fred_state_read", e, logger)
                    continue
            new = fred.is_new_release(series, release, last_seen, last_updated, now)
            if raw is not None:
                await raw.hset(fred.FRED_STATE_KEY, series.series_id, fred.state_marker(series, release))
            if not new:
                continue
            event = self.build_macro_release_event(
                indicator=series.indicator,
                actual=release["actual"],
                forecast=None,
                previous=release["previous"],
                period=release["period"],
                source="fred",
                occurred_at=last_updated,
                detail=release.get("detail", ""),
            )
            if self.producer:
                await self._publish(event)
            logger.info("📊 Emitted FRED release: %s", event.headline)
            published.append(event)
        published += await self._poll_fred_context(session, raw)
        return published

    async def _poll_fred_context(self, session, raw) -> List[NormalizedEvent]:
        """Spreads, curve and conditions: stored as context, announced on a big move."""
        readings: Dict[str, Dict[str, Any]] = {}
        announced: List[NormalizedEvent] = []
        for series in fred.FRED_CONTEXT_SERIES:
            try:
                observations, last_updated = await fred.fetch_series(session, series)
            except Exception as e:
                swallowed("collector_macro.fred_context_fetch", e, logger, variant=series.series_id)
                continue
            reading = fred.context_reading(series, observations)
            if reading is None:
                continue
            readings[series.indicator] = reading
            if not reading["notable"] or raw is None:
                continue
            marker = f"move:{reading['date']}"
            try:
                if await raw.hget(fred.FRED_STATE_KEY, series.series_id) in (marker, marker.encode()):
                    continue
                await raw.hset(fred.FRED_STATE_KEY, series.series_id, marker)
            except Exception as e:
                swallowed("collector_macro.fred_context_state", e, logger)
                continue
            event = self.build_macro_release_event(
                indicator=series.indicator,
                actual=reading["value"],
                forecast=None,
                previous=reading["week_ago"],
                period=reading["date"],
                source="fred",
                occurred_at=last_updated,
                detail="1-week change" + (", crossed zero" if reading["crossed_zero"] else ""),
            )
            if self.producer:
                await self._publish(event)
            logger.info("📊 Emitted FRED market move: %s", event.headline)
            announced.append(event)
        if readings and raw is not None:
            try:
                await raw.set(
                    fred.FRED_CONTEXT_KEY,
                    json.dumps({"line": fred.context_line(readings), "readings": readings,
                                "as_of": datetime.now(timezone.utc).isoformat()}),
                    ex=3 * 86400,
                )
            except Exception as e:
                swallowed("collector_macro.fred_context_store", e, logger)
        return announced

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
