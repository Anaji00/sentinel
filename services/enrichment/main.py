import asyncio
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Optional
import json

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

import warnings
warnings.filterwarnings("ignore", message=".*Failed to initialize NumPy.*")

from shared.utils.logging import setup_sentinel_logging

logger = setup_sentinel_logging("enrichment", level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")))

from shared.kafka import SentinelProducer, SentinelConsumer, Topics
from shared.models import RawEvent, NormalizedEvent, CrossDomainSignal
from shared.db import get_redis, get_timescale, get_neo4j
from shared.db.bootstrap import bootstrap_database
from shared.utils.heartbeat import start_heartbeat_task
from shared.utils.source_freshness import mark_sources_seen

from services.enrichment.anomaly_scorer import DynamicAnomalyScorer
from services.enrichment.db_writer import DBWriter
from services.enrichment.graph_writer import GraphWriter
from services.enrichment.entity_resolver import EntityResolver
from services.enrichment.gap_detector import VesselGapDetector

from shared.utils.tasks import safe_create_task
from shared.utils.live_feed import worth_broadcasting

# --- THE NEW ENRICHERS ---
from services.enrichment.enrichers.maritime import MaritimeEnricher
from services.enrichment.enrichers.aviation import AviationEnricher
from services.enrichment.enrichers.news import NewsEnricher
from services.enrichment.enrichers.cyber import CyberEnricher
from services.enrichment.enrichers.tradfi import TradFiEnricher
from services.enrichment.enrichers.crypto import CryptoEnricher
from services.enrichment.enrichers.prediction import PredictionEnricher



async def _attach_cross_domain_signals(events: list, redis_client):
    """
    Pre-computed cross-domain signal injection.
    Cache recent high-anomaly events in Redis by entity/region, and attach
    matching signals from OTHER domains to each NormalizedEvent before publishing.
    """
    if not redis_client or not events:
        return

    try:
        pipe = redis_client.raw.pipeline()
        for evt in events:
            if not isinstance(evt, NormalizedEvent):
                continue

            entity_id = evt.primary_entity.id if evt.primary_entity else None
            region = evt.region

            # Cache current high-anomaly event (TTL 4h)
            if evt.anomaly_score >= 0.5:
                sig_data = json.dumps({
                    "event_id": str(evt.event_id),
                    "event_type": str(evt.type.value if hasattr(evt.type, "value") else evt.type),
                    "domain": evt.source.split("_")[0] if "_" in evt.source else evt.source,
                    "entity_id": entity_id or "unknown",
                    "entity_name": evt.primary_entity.name if evt.primary_entity else None,
                    "headline": evt.headline,
                    "anomaly_score": evt.anomaly_score,
                    "occurred_at": evt.occurred_at.isoformat() if evt.occurred_at else None,
                    "region": region,
                })
                if entity_id:
                    pipe.lpush(f"sentinel:recent_signals:entity:{entity_id.upper()}", sig_data)
                    pipe.ltrim(f"sentinel:recent_signals:entity:{entity_id.upper()}", 0, 9)
                    pipe.expire(f"sentinel:recent_signals:entity:{entity_id.upper()}", 14400)
                if region:
                    pipe.lpush(f"sentinel:recent_signals:region:{region.lower()}", sig_data)
                    pipe.ltrim(f"sentinel:recent_signals:region:{region.lower()}", 0, 9)
                    pipe.expire(f"sentinel:recent_signals:region:{region.lower()}", 14400)

            # Query existing cached signals
            if entity_id:
                pipe.lrange(f"sentinel:recent_signals:entity:{entity_id.upper()}", 0, 5)
            elif region:
                pipe.lrange(f"sentinel:recent_signals:region:{region.lower()}", 0, 5)

        results = await pipe.execute()

        # Parse results and populate cross_domain_signals
        res_idx = 0
        for evt in events:
            if not isinstance(evt, NormalizedEvent):
                continue

            entity_id = evt.primary_entity.id if evt.primary_entity else None
            region = evt.region

            # Skip write pipeline commands indices
            if evt.anomaly_score >= 0.5:
                if entity_id:
                    res_idx += 3
                if region:
                    res_idx += 3

            if entity_id or region:
                raw_signals = results[res_idx] if res_idx < len(results) else []
                res_idx += 1
                if raw_signals:
                    current_domain = evt.source.split("_")[0] if "_" in evt.source else evt.source
                    cross_signals = []
                    for item in raw_signals:
                        try:
                            s = json.loads(item if isinstance(item, str) else item.decode("utf-8"))
                            if s.get("domain") != current_domain and s.get("event_id") != str(evt.event_id):
                                cross_signals.append(CrossDomainSignal(**s))
                        except Exception:
                            pass
                    evt.cross_domain_signals = cross_signals[:3]
    except Exception as e:
        logger.debug(f"Cross-domain signal attachment warning: {e}")



# Display-text formatting. Extracted from the batch loop because it is cosmetic
# and must never be able to halt ingestion: SecurityData.severity/.vector/.product
# and FlightData.altitude_ft/.ground_speed_kts were referenced here but do not
# exist on those models, so the AttributeError escaped the batch loop and killed
# the enrichment service outright. Cyber events were produced by the tens of
# thousands and never reached the database. Out here it is unit-testable against
# the real models, so a renamed field fails in CI instead of in production.

def _cvss_severity(score: Optional[float]) -> str:
    """CVSS v3 qualitative band. SecurityData carries a score, not a label."""
    if score is None:
        return "UNSCORED"
    if score >= 9.0:
        return "CRITICAL"
    if score >= 7.0:
        return "HIGH"
    if score >= 4.0:
        return "MEDIUM"
    if score > 0.0:
        return "LOW"
    return "NONE"


def _m_to_ft(meters: Optional[float]) -> float:
    """Altitudes are stored SI (`*_m`); aviation reads in feet."""
    return (meters or 0.0) * 3.28084


def _ms_to_kts(ms: Optional[float]) -> float:
    """Velocities are stored SI (`*_ms`); aviation reads in knots."""
    return (ms or 0.0) * 1.94384


def apply_display_text(e) -> None:
    """Fills in a human-readable headline and summary when either is missing."""
    ent_name = e.primary_entity.name if (e.primary_entity and e.primary_entity.name) else (e.primary_entity.id if e.primary_entity else str(e.type))

    # 1. Headline clarity enforcement
    if not e.headline or len(e.headline) < 5 or e.headline.startswith("UNKNOWN"):
        if e.financial_data and e.financial_data.ticker:
            fd = e.financial_data
            p_str = f" @ ${fd.underlying_price:.2f}" if fd.underlying_price else ""
            e.headline = f"📈 TRADFI MARKET EVENT: {fd.ticker}{p_str} (Anomaly: {e.anomaly_score:.2f})"
        elif e.crypto_data and e.crypto_data.pair:
            cd = e.crypto_data
            e.headline = f"₿ CRYPTO MARKET EVENT: {cd.pair} @ ${cd.price:.2f} (Anomaly: {e.anomaly_score:.2f})"
        elif e.prediction_market_data:
            pm = e.prediction_market_data
            e.headline = f"🎯 PREDICTION MARKET EVENT: {pm.question or pm.ticker} ({pm.outcome})"
        elif e.vessel_data:
            vd = e.vessel_data
            e.headline = f"🚢 MARITIME AIS FIX: {ent_name} (MMSI: {vd.mmsi}) in {e.region or 'International Waters'}"
        elif e.flight_data:
            fl = e.flight_data
            e.headline = f"✈️ AVIATION ADSB FIX: Flight {fl.callsign or ent_name} (ICAO: {fl.icao24}) @ {_m_to_ft(fl.baro_altitude_m or fl.geo_altitude_m):,.0f}ft"
        elif e.security_data:
            sec = e.security_data
            e.headline = f"🔐 CYBER THREAT DETECTED: {sec.cve_id or ent_name} ({_cvss_severity(sec.cvss_score)})"
        else:
            e.headline = f"🎯 SENTINEL INTELLIGENCE EVENT: {ent_name} [{e.source}]"

    # 2. Executive summary clarity enforcement
    if not e.summary or len(e.summary) < 15:
        if e.financial_data and e.financial_data.ticker:
            fd = e.financial_data
            p_str = f" @ ${fd.underlying_price:.2f}" if fd.underlying_price else ""
            vol_str = f" Notional volume: ${fd.premium_usd/1e6:.2f}M across {fd.volume:,.0f} shares/contracts." if fd.premium_usd else ""
            e.summary = f"Institutional Market Intelligence for {fd.ticker}{p_str}.{vol_str} Anomaly score: {e.anomaly_score:.2f}. Provenance tags: {', '.join(e.tags or [])}."
        elif e.crypto_data and e.crypto_data.pair:
            cd = e.crypto_data
            fr_str = f" Perpetual funding rate: {cd.funding_rate*100:.4f}%." if cd.funding_rate is not None else ""
            e.summary = f"Cryptocurrency Intelligence for {cd.pair} trading @ ${cd.price:.2f}.{fr_str} Anomaly score: {e.anomaly_score:.2f}. Provenance tags: {', '.join(e.tags or [])}."
        elif e.prediction_market_data:
            pm = e.prediction_market_data
            pm_price = getattr(pm, "price_usd", 0.0) or getattr(pm, "price", 0.0)
            e.summary = f"Prediction Market Intelligence for contract '{pm.question or pm.market_id}' ({pm.outcome}). Current price/probability: {(pm_price*100):.1f}%. Anomaly score: {e.anomaly_score:.2f}."
        elif e.vessel_data:
            vd = e.vessel_data
            e.summary = f"AIS Maritime position fix for {vd.vessel_type or 'Vessel'} '{ent_name}' (MMSI: {vd.mmsi}) in {e.region or 'International Waters'}. Speed: {vd.speed_knots or 0.0} knots, Heading: {vd.heading or 0}°. Anomaly score: {e.anomaly_score:.2f}."
        elif e.flight_data:
            fl = e.flight_data
            e.summary = f"ADS-B Aviation position fix for aircraft {fl.callsign or ent_name} (ICAO: {fl.icao24}). Altitude: {_m_to_ft(fl.baro_altitude_m or fl.geo_altitude_m):,.0f} ft, Ground speed: {_ms_to_kts(fl.velocity_ms):,.0f} knots. Anomaly score: {e.anomaly_score:.2f}."
        elif e.security_data:
            sec = e.security_data
            e.summary = f"Cyber Threat Intelligence disclosure for {sec.cve_id or ent_name}. Severity: {_cvss_severity(sec.cvss_score)}, Vector: {sec.exposure_type or 'Network'}. Target org: {sec.affected_org or 'Unknown'}. Anomaly score: {e.anomaly_score:.2f}."
        else:
            e.summary = f"Sentinel Multi-Domain Event for {ent_name}. Source: {e.source}. Anomaly score: {e.anomaly_score:.2f}. Region: {e.region or 'Global'}. Provenance tags: {', '.join(e.tags or [])}."


async def _heartbeat_loop(state: dict):
    """Periodic heartbeat for operational visibility."""
    while True:
        await asyncio.sleep(60)
        elapsed = state["elapsed"]()
        rate = state["processed"] / elapsed if elapsed > 0 else 0
        logger.info(
            f"⏱ HEARTBEAT | processed={state['processed']} "
            f"errors={state['errors']} rate={rate:.1f}/s "
            f"uptime={int(elapsed)}s"
        )

# Which index the volatility regime is measured from, in preference order.
#
# QQQ first because it carries the deepest bar history on this deployment --
# 530 bars with 342 distinct closes in a day, against SPY's 278 and 204.
VOLATILITY_INDEXES = ["QQQ", "SPY"]
VOLATILITY_INTERVAL_SEC = 900
VOLATILITY_LOOKBACK_BARS = 240


async def _volatility_loop(timescale, redis_client) -> None:
    """Publishes realised index volatility for the radar's thresholds.

    The radar scales its Z-score threshold by market volatility and read a key
    nothing wrote, so it resolved to a hardcoded 20.0 on every call and never
    scaled at all. VIX is not collected anywhere on this platform and the free
    tiers do not carry it, so this measures what the platform does record.

    Refusal is deliberate when the sample is thin: the radar falls back to the
    same constant it used before, which is no worse than the previous behaviour
    and is at least labelled as an assumption.
    """
    from shared.utils.volatility import REALISED_VOL_KEY, realised_volatility

    await asyncio.sleep(120)
    while True:
        try:
            measured = None
            for ticker in VOLATILITY_INDEXES:
                rows = await timescale.query(
                    "SELECT close FROM tradfi_bars WHERE ticker = $1 "
                    "ORDER BY time DESC LIMIT $2",
                    ticker, VOLATILITY_LOOKBACK_BARS,
                )
                closes = [r["close"] for r in (rows or []) if r.get("close") is not None]
                # Oldest first, so returns run forward in time.
                measured = realised_volatility(list(reversed(closes)), bar="1m")
                if measured is not None:
                    await redis_client.raw.set(REALISED_VOL_KEY, str(measured), ex=3600)
                    logger.info(
                        "Realised volatility from %s: %.2f%% annualised over %s bars.",
                        ticker, measured, len(closes),
                    )
                    break

            if measured is None:
                logger.info(
                    "Realised volatility not measurable from %s; the radar will "
                    "use its stated assumption rather than a measurement.",
                    ", ".join(VOLATILITY_INDEXES),
                )
        except Exception as e:
            logger.error(f"Volatility measurement failed: {e}")
        await asyncio.sleep(VOLATILITY_INTERVAL_SEC)


async def _reference_data_loop(redis_client, graph_writer):
    """Refreshes sector, industry and index membership daily.

    The function this calls has existed, complete and tested, with zero callers.
    The cost of that was quiet: `sector`, `industry` and `index_membership` are
    carried on every equity event and were null on all of them, so anything
    reading them saw an empty field rather than a missing feed.

    It matters more now that peers are derived. Shared sector or index is what
    corroborates a measured correlation -- two names that co-move *and* sit in
    the same index are a better contagion path than two that merely co-move --
    and with reference data absent, every peer edge was carrying the realised
    half of its evidence alone.

    Daily, offset past startup so it does not compete with the initial backfill
    for the same Finnhub rate limit.
    """
    from services.enrichment.ref_data import refresh_watchlist_reference_data

    await asyncio.sleep(300)
    while True:
        try:
            await refresh_watchlist_reference_data(redis_client, graph_writer=graph_writer)
        except Exception as e:
            logger.error(f"Reference data refresh failed: {e}")
        await asyncio.sleep(86400)


# A detector whose output never varies ranks nothing.
#
# Seven were found this way, each with a different cause: a category overwriting
# a measurement, an absolute threshold measuring the ocean, a TypeError killing
# the graph edge novelty depends on, a divisor saturating at five sigma, a
# baseline for events that have not happened, an EMA seeded with the very
# observation it was meant to judge. The only thing they shared was the symptom,
# and every one of them was invisible until somebody counted.
#
# So the system counts. This does not diagnose -- the causes have nothing in
# common and reading the arithmetic is still the work -- it only says which
# detector has stopped discriminating, which is the part nobody notices for
# months.
SCORE_DIVERSITY_INTERVAL_SEC = 3600
SCORE_DIVERSITY_WINDOW = "1 hour"

# Below this many events there is no expectation of variety; a detector that
# fired twice may honestly have two identical answers.
SCORE_DIVERSITY_MIN_EVENTS = 30

# A detector reporting no more than this many distinct scores has stopped
# ranking, whatever its volume. Three, because two values is a two-class
# classifier and three is a ladder -- both of which downstream percentiles and
# z-scores treat as a continuous measurement and cannot.
SCORE_DIVERSITY_MIN_DISTINCT = 3

# Above this volume, diversity is judged as a share rather than a count: a
# detector emitting 2,000 events an hour across eight values is as flat in
# practice as one emitting eighty across two.
SCORE_DIVERSITY_RATIO_MIN_EVENTS = 200
SCORE_DIVERSITY_MIN_RATIO = 0.02

# Event types whose score is legitimately constant. vessel_static is a
# registration record -- a name and a callsign are not an anomaly, and 0.000 is
# the correct answer every time.
SCORE_DIVERSITY_EXEMPT = {"vessel_static"}


async def _score_diversity_loop(timescale) -> None:
    """Reports detectors whose anomaly score has stopped varying."""
    await asyncio.sleep(600)
    while True:
        try:
            rows = await timescale.query(
                f"""
                SELECT type,
                       COUNT(*)                                  AS n,
                       COUNT(DISTINCT ROUND(anomaly_score::numeric, 3)) AS distinct_scores
                FROM events
                WHERE occurred_at > NOW() - INTERVAL '{SCORE_DIVERSITY_WINDOW}'
                GROUP BY type
                HAVING COUNT(*) >= {SCORE_DIVERSITY_MIN_EVENTS}
                """
            )
            # `<= 1` was the test, and it caught nothing that was actually flat.
            #
            # A detector that has stopped varying reports one value; a detector
            # that never varied by more than a step reports two or three, and
            # every flat detector in this deployment is the second kind --
            # bgp_anomaly runs 2,723 events a day at 114/hour with two distinct
            # scores and passed this check on every sweep. This file's own
            # account of that detector says it: two values over seven thousand
            # events is a classifier with two classes, and every threshold and
            # percentile computed against it is arithmetic on a constant.
            #
            # The bound is now proportional. A handful of distinct values across
            # hundreds of events is flat whatever the absolute count, and a
            # detector with genuinely few events is left alone by the volume
            # floor above rather than by a second guess here.
            flat = []
            for r in (rows or []):
                if str(r.get("type")) in SCORE_DIVERSITY_EXEMPT:
                    continue
                n = int(r.get("n") or 0)
                distinct = int(r.get("distinct_scores") or 0)
                if distinct <= SCORE_DIVERSITY_MIN_DISTINCT:
                    flat.append(r)
                elif n >= SCORE_DIVERSITY_RATIO_MIN_EVENTS and (distinct / n) < SCORE_DIVERSITY_MIN_RATIO:
                    flat.append(r)
            for row in flat:
                logger.warning(
                    "Detector %s emitted %s events in the last %s and %s distinct "
                    "score(s). A detector whose output barely varies ranks nothing; "
                    "the cause is never the same twice, so read its arithmetic.",
                    row.get("type"), row.get("n"), SCORE_DIVERSITY_WINDOW,
                    row.get("distinct_scores"),
                )
            if not flat:
                logger.info(
                    "Score diversity: %s detector(s) checked, none flat.",
                    len(rows or []),
                )
        except Exception as e:
            logger.error(f"Score diversity check failed: {e}")
        await asyncio.sleep(SCORE_DIVERSITY_INTERVAL_SEC)


async def _ofac_sync_loop():
    """Syncs OFAC sanctions list on startup then every 24 hours.
    
    Downloads real Treasury SDN & ALT CSV lists from treasury.gov and updates Aho-Corasick automaton.
    """
    from shared.utils.sanctions import rebuild_sanctions_from_list, SANCTIONED_KEYWORDS
    from services.enrichment.ofac_sync import fetch_ofac_keywords
    import json

    while True:
        try:
            logger.info("Starting OFAC sanctions sync from US Treasury SDN list...")
            fetched_keywords = await fetch_ofac_keywords()
            if fetched_keywords:
                all_keywords = list(set(SANCTIONED_KEYWORDS + fetched_keywords))
                rebuild_sanctions_from_list(all_keywords)
                logger.info(f"OFAC sanctions sync complete. Total entities in automaton: {len(all_keywords)}")
                
                try:
                    redis = await get_redis()
                    await redis.raw.set("sentinel:config:sanctions", json.dumps({"keywords": all_keywords}))
                    await redis.raw.publish("sentinel:config:updates", "ofac_rebuild")
                except Exception as re:
                    logger.debug(f"Redis OFAC broadcast skipped: {re}")
            else:
                logger.warning("OFAC download returned 0 keywords. Retaining baseline SANCTIONED_KEYWORDS.")
                rebuild_sanctions_from_list(SANCTIONED_KEYWORDS)
        except Exception as e:
            logger.error(f"OFAC sync failed: {e}. Retaining baseline SANCTIONED_KEYWORDS.")
            rebuild_sanctions_from_list(SANCTIONED_KEYWORDS)
        await asyncio.sleep(86_400)  # 24 hours

# Tags that exempt an event from the fan-out floor, whatever it scored.
#
# A sanctions match on a $15 transfer is still a sanctions match; the platform's
# own OFAC path is a floor rather than a lift for exactly this reason.
FANOUT_EXEMPT_TAGS = frozenset({
    "sanctioned", "suspect_wallet", "watched_wallet_transfer",
    "emergency", "squawk", "dark_vessel", "token_supply_event",
})


def _is_below_fanout_floor(event) -> bool:
    """Whether an event is one the scorer already judged to carry nothing."""
    try:
        score = float(getattr(event, "anomaly_score", 0.0) or 0.0)
    except (TypeError, ValueError):
        return False
    if score > 0.0:
        return False
    tags = set(getattr(event, "tags", None) or [])
    return not (tags & FANOUT_EXEMPT_TAGS)


async def main():
    logger.info("=" * 60)
    logger.info("SENTINEL  Enrichment Service (Multi-Domain Edition)")
    logger.info("=" * 60)

    await bootstrap_database()  # Ensure DB schema is ready before processing

    timescale = await get_timescale()
    redis     = await get_redis()
    safe_create_task(_ofac_sync_loop(), name="ofac-sync")
    safe_create_task(
        _volatility_loop(timescale, redis), name="realised-volatility"
    )
    safe_create_task(
        _score_diversity_loop(timescale), name="score-diversity"
    )
    
    # Wait for databases to come online
    producer = SentinelProducer()
    dlq = SentinelProducer()
    await producer.start()
    neo4j = await get_neo4j()
    scorer = DynamicAnomalyScorer(redis, neo4j_client=neo4j)
    db = DBWriter(timescale)
    graph = GraphWriter(producer)
    safe_create_task(
        _reference_data_loop(redis, graph), name="reference-data-refresh"
    )
    resolver = EntityResolver(redis, neo4j, producer=producer)

# STRICT DEPENDENCY INJECTION ALIGNMENT: (scorer, redis, graph, [resolver])
    maritime = MaritimeEnricher(scorer, redis, graph, resolver)
    aviation = AviationEnricher(scorer, redis, graph, resolver)
    news = NewsEnricher(scorer, redis, graph)
    cyber = CyberEnricher(scorer, redis, graph)
    tradfi = TradFiEnricher(scorer, redis, graph, db=timescale)
    crypto = CryptoEnricher(scorer, redis, graph)
    prediction = PredictionEnricher(scorer, redis, graph)
    
    enrichers_tuple = (maritime, aviation, news, cyber, tradfi, crypto, prediction)

    consumer = SentinelConsumer(
        topics=[
            Topics.RAW_MARITIME, Topics.RAW_AVIATION, Topics.RAW_NEWS, 
            Topics.RAW_SOCIAL, Topics.RAW_FILINGS, Topics.RAW_CYBER, 
            Topics.RAW_TRADFI, Topics.RAW_CRYPTO, Topics.RAW_PREDICTION, 
            Topics.RAW_RADAR
        ],
        group_id="enrichment-service",
    )
    await consumer.start()

    from services.enrichment.aviation_gap_detector import AviationGapDetector
    gap = VesselGapDetector(producer, scorer, db, redis)
    gap_task = safe_create_task(gap.run(), name="vessel-gap-detector")

    av_gap = AviationGapDetector(producer, scorer, db, redis)
    av_gap_task = safe_create_task(av_gap.run(), name="aviation-gap-detector")

    import time as _time
    _start_time = _time.monotonic()
    processed = 0
    errors = 0
    _fanout_skipped = 0
    heartbeat_state = {
        "processed": 0,
        "errors": 0,
        "elapsed": lambda: _time.monotonic() - _start_time,
    }
    heartbeat_task = safe_create_task(_heartbeat_loop(heartbeat_state), name="enrichment-heartbeat")

    # Published to Redis so the gateway's /metrics can see them.
    #
    # bind_redis() is what moves a process-local counter into the cross-process
    # aggregate the /metrics endpoint sums. Only the collectors, the agents and
    # the reasoning engine were calling it, so every counter this service keeps
    # -- scoring failures, uninitialised detectors, quote-cache and
    # open-interest baseline errors -- incremented into a dict that was never
    # read by anything and was discarded on restart. Each of those is a signal
    # that a detector has stopped working correctly, which is precisely the
    # class of failure that is silent without a counter.
    try:
        from shared.utils.metrics import bind_redis
        await bind_redis(redis, service_name=os.getenv("SENTINEL_SERVICE", "enrichment"))
    except Exception as e:
        logger.debug("Metrics binding skipped: %s", e)

    # §1.1 Universal heartbeat — shared telemetry for data-health dashboard
    hb_shared_task = safe_create_task(start_heartbeat_task(redis, "enrichment"))

    logger.info("Enrichment Pipeline LIVE. Listening for raw telemetry...")
    
    try:
        while True:
            batches = await consumer.get_batch(timeout_ms=1000)
            if not batches:
                continue

            for tp, messages in batches.items():
                batch_to_write = []
                
                # ── 1. ASYNC CONCURRENT ENRICHMENT ───────────────────────────
                topic_to_enricher = {
                    Topics.RAW_MARITIME: enrichers_tuple[0],
                    Topics.RAW_AVIATION: enrichers_tuple[1],
                    Topics.RAW_NEWS: enrichers_tuple[2],
                    Topics.RAW_SOCIAL: enrichers_tuple[2],
                    Topics.RAW_FILINGS: enrichers_tuple[4],
                    Topics.RAW_CYBER: enrichers_tuple[3],
                    Topics.RAW_TRADFI: enrichers_tuple[4],
                    Topics.RAW_CRYPTO: enrichers_tuple[5],
                    Topics.RAW_PREDICTION: enrichers_tuple[6],
                    Topics.RAW_RADAR: enrichers_tuple[4]
                }
                
                # Group raw events by topic
                raw_events_by_topic = {}
                pending_dlq_tasks = []
                for msg in messages:
                    try:
                        raw_data = json.loads(msg.value.decode('utf-8'))
                        if isinstance(raw_data, dict) and not raw_data.get("source"):
                            raw_data["source"] = msg.topic.split(".")[-1] if "." in msg.topic else msg.topic
                        raw_event = RawEvent(**raw_data)
                        raw_events_by_topic.setdefault(msg.topic, []).append(raw_event)
                    except Exception as e:
                        logger.error(f"POISON PILL / Invalid RawEvent dropped: {e}", exc_info=True)
                        pending_dlq_tasks.append(dlq.send(Topics.DLQ, {"error": f"Invalid RawEvent: {e}", "topic": msg.topic, "raw": str(msg.value)}))
                
                enrich_tasks = []
                for topic, raw_events in raw_events_by_topic.items():
                    enricher = topic_to_enricher.get(topic)
                    if enricher:
                        if hasattr(enricher, "enrich_batch"):
                            enrich_tasks.append(enricher.enrich_batch(raw_events))
                        else:
                            # Fallback if enrich_batch is not implemented
                            async def _fallback_batch(e_batch, e_inst=enricher):
                                return await asyncio.gather(*[e_inst.enrich(e) for e in e_batch], return_exceptions=True)
                            enrich_tasks.append(_fallback_batch(raw_events))

                # Execute all batches simultaneously 
                results = await asyncio.gather(*enrich_tasks, return_exceptions=True)
                
                # ── 2. ASYNC CONCURRENT PRODUCER DISPATCH & ERROR ROUTING ──
                produce_tasks = []
                for batch_result, (topic, raw_events) in zip(results, list(raw_events_by_topic.items())):
                    if isinstance(batch_result, Exception):
                        logger.error(f"Batch enrichment failed for topic {topic}: {batch_result}", exc_info=batch_result)
                        for re in raw_events:
                            pending_dlq_tasks.append(
                                dlq.send(
                                    Topics.DLQ,
                                    {
                                        "error": f"Batch enrichment error: {batch_result}",
                                        "topic": topic,
                                        "raw": re.model_dump()
                                    }
                                )
                            )
                    elif isinstance(batch_result, list):
                        def _flatten(items):
                            flat = []
                            for item in items:
                                if isinstance(item, list):
                                    flat.extend(_flatten(item))
                                elif item is not None:
                                    flat.append(item)
                            return flat

                        vessel_positions_to_write = []
                        flat_events = _flatten(batch_result)
                        for enriched in flat_events:
                            if isinstance(enriched, NormalizedEvent):
                                batch_to_write.append(enriched)
                                if enriched.vessel_data and getattr(enriched.vessel_data, 'mmsi', None):
                                    vd = enriched.vessel_data
                                    vessel_positions_to_write.append((
                                        vd.mmsi,
                                        enriched.occurred_at,
                                        enriched.latitude or 0.0,
                                        enriched.longitude or 0.0,
                                        getattr(vd, 'speed_knots', 0.0) or 0.0,
                                        getattr(vd, 'heading', 0) or 0,
                                        getattr(vd, 'nav_status', 'underway') or 'underway'
                                    ))
                            elif isinstance(enriched, Exception):
                                logger.error(f"Enrichment item failed for topic {topic}: {enriched}", exc_info=enriched)
                                pending_dlq_tasks.append(
                                    dlq.send(
                                        Topics.DLQ,
                                        {
                                            "error": f"Item enrichment error: {enriched}",
                                            "topic": topic,
                                        }
                                    )
                                )
                        if vessel_positions_to_write:
                            pending_dlq_tasks.append(db.write_vessel_positions_batch(vessel_positions_to_write))

                batch_success = True

                # Await all pending DLQ sends & vessel writes before proceeding
                if pending_dlq_tasks:
                    pending_res = await asyncio.gather(*pending_dlq_tasks, return_exceptions=True)
                    for r in pending_res:
                        if isinstance(r, Exception):
                            logger.error(f"DLQ delivery / aux dispatch failed: {r}", exc_info=r)
                            batch_success = False  # Gate offset commit on DLQ delivery success

                # Pre-computed cross-domain signal injection pass & payload summary enrichment
                if batch_to_write:
                    await _attach_cross_domain_signals(batch_to_write, redis)

                    # Ensure 100% of payloads carry rich human-readable headlines & executive summaries
                    for e in batch_to_write:
                        try:
                            apply_display_text(e)
                        except Exception as exc:  # cosmetics must not stop the pipeline
                            logger.warning(
                                "Display-text formatting failed for event %s: %s",
                                getattr(e, "event_id", "?"), exc,
                            )

                for enriched in batch_to_write:
                    # An event the scorer has already judged worthless does not
                    # take the fan-out.
                    #
                    # Measured live on 4 September: 6,969 of 17,506 events in
                    # thirty minutes scored exactly 0.000, and 6,534 of those
                    # were sub-$10k crypto transfers. The dust-scoring repair
                    # earlier in this audit was right that a $15 stablecoin
                    # movement scores zero -- but it changed only the number.
                    # Each one was still published to ENRICHED_EVENTS and
                    # delivered into ten agent inboxes, which is two fifths of
                    # the platform's whole message volume spent distributing a
                    # verdict of "nothing".
                    #
                    # It is still written to Timescale above and still fully
                    # queryable; what it stops doing is costing what a finding
                    # costs. Anything with a categorical reason to be seen --
                    # sanctions, a watchlist, an emergency -- carries a tag and
                    # is exempt, because those are facts about the subject
                    # rather than judgements about magnitude.
                    if _is_below_fanout_floor(enriched):
                        _fanout_skipped += 1
                        if _fanout_skipped % 20000 == 1:
                            logger.info(
                                "Held %s zero-scored event(s) out of the agent fan-out. "
                                "They remain queryable in Timescale.", _fanout_skipped,
                            )
                        continue

                    entity_key = enriched.primary_entity.id if (enriched.primary_entity and enriched.primary_entity.id) else "unknown"
                    produce_tasks.append(
                        producer.send(
                            Topics.ENRICHED_EVENTS,
                            enriched.model_dump(),
                            key=entity_key,
                        )
                    )
                    try:
                        live_dict = enriched.model_dump(mode="json")
                        live_dict["event_id"] = str(enriched.event_id)
                        # The enricher already said whether this is worth
                        # anyone's attention; this reads that verdict rather
                        # than broadcasting everything and letting a person
                        # filter a $6 transfer out by eye.
                        if worth_broadcasting(live_dict):
                            safe_create_task(redis.raw.publish("sentinel:events:live", json.dumps(live_dict)), name="live-feed-pub")
                    except Exception as pub_err:
                        logger.debug(f"Redis live feed publish bypass: {pub_err}")

                if produce_tasks:
                    produce_results = await asyncio.gather(*produce_tasks, return_exceptions=True)
                    failed_produce_dlq_tasks = []
                    successful_produces = 0
                    for enriched, produce_res in zip(batch_to_write, produce_results):
                        if isinstance(produce_res, Exception):
                            logger.error(
                                f"Kafka produce to {Topics.ENRICHED_EVENTS} failed for event {enriched.event_id}: {produce_res}",
                                exc_info=produce_res,
                            )
                            errors += 1
                            batch_success = False
                            failed_produce_dlq_tasks.append(
                                dlq.send(
                                    Topics.DLQ,
                                    {
                                        "error": f"KAFKA_PRODUCE_FAILED: {produce_res}",
                                        "topic": Topics.ENRICHED_EVENTS,
                                        "raw": enriched.model_dump(),
                                    },
                                )
                            )
                        else:
                            successful_produces += 1

                    if failed_produce_dlq_tasks:
                        dlq_res = await asyncio.gather(*failed_produce_dlq_tasks, return_exceptions=True)
                        for r in dlq_res:
                            if isinstance(r, Exception):
                                logger.error(f"DLQ delivery for failed Kafka produce failed: {r}", exc_info=r)

                    processed += successful_produces
                    heartbeat_state["processed"] = processed
                    heartbeat_state["errors"] = errors
                    if processed % 250 == 0:
                        logger.info(f"Processed {processed} successfully")

                # ── 3. FAULT-TOLERANT DB WRITES ──────────────────────────────
                if batch_to_write:
                    for attempt in range(3):
                        try:
                            # FIX: write_events_batch is async — call it directly,
                            # not via run_in_executor (which is for sync functions).
                            await db.write_events_batch(batch_to_write)

                            # When a feed goes quiet, make the silence legible.
                            #
                            # The platform knew whether a *collector* was alive
                            # and nothing about whether its feed still produced.
                            # Ten sources went an hour without an event during
                            # this audit and the only way to tell a dead feed
                            # from a slow one was to read the poller's source.
                            # One hash write per batch, against each source's
                            # own learned cadence.
                            safe_create_task(
                                mark_sources_seen(
                                    redis, {e.source for e in batch_to_write if e.source}
                                ),
                                name="source-freshness",
                            )

                            # Broadcast enriched events to Redis PubSub for real-time WebSocket live feed
                            try:
                                pub_pipe = redis.raw.pipeline()
                                for evt in batch_to_write:
                                    as_dict = evt.model_dump()
                                    # Same rule as the publisher above. Two
                                    # sites broadcasting on different rules is
                                    # how one of them ends up forgotten.
                                    if not worth_broadcasting(as_dict):
                                        continue
                                    payload = json.dumps(as_dict, default=str)
                                    pub_pipe.publish("sentinel:events:live", payload)
                                await pub_pipe.execute()
                            except Exception as pub_err:
                                logger.debug(f"Redis pubsub publish warning: {pub_err}")
                            break # Success
                        except Exception as write_err:
                            if attempt == 2: 
                                errors += len(batch_to_write)
                                heartbeat_state["errors"] = errors
                                logger.error(f"FATAL DB WRITE ERROR: {write_err}. Routing batch to DLQ to prevent data loss.", exc_info=True)
                                # Send the failed DB batch with full payload to DLQ rather than crashing the service
                                dlq_tasks = [
                                    dlq.send(
                                        Topics.DLQ,
                                        {
                                            "error": f"DB_WRITE_FAILED: {write_err}",
                                            "topic": Topics.ENRICHED_EVENTS,
                                            "raw": e.model_dump()
                                        }
                                    )
                                    for e in batch_to_write
                                ]
                                await asyncio.gather(*dlq_tasks, return_exceptions=True)
                                batch_success = False
                            else:
                                await asyncio.sleep(2 ** attempt) 

                # ── 4. COMMIT ────────────────────────────────────────────────
                if batch_success:
                    await consumer.commit()
                
    except asyncio.CancelledError:
        logger.info("Shutdown signal received. Closing consumer...")
    except Exception as e:
        logger.critical(f"Fatal error in main loop: {e}", exc_info=True)
    finally:
        heartbeat_task.cancel()
        hb_shared_task.cancel()
        gap_task.cancel()
        try:
            await asyncio.gather(gap_task, heartbeat_task, return_exceptions=True)
        except asyncio.CancelledError:
            pass
        logger.info(f"Final — processed: {processed}  errors: {errors}")

        await producer.close()
        await dlq.close()
        await consumer.close()

if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())