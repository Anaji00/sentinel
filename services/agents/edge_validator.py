"""
services/agents/edge_validator.py

EMPIRICAL GRAPH EDGE VALIDATOR & BACKTESTING ENGINE (§3.4, Task 3)
=================================================================

Periodically backtests LLM-proposed ontology edges (SUPPLIES, COMMODITY_EXPOSURE,
POSITIVE_EXPOSURE_TO, INVERSE_EXPOSURE_TO) against realized market outcomes in
TimescaleDB, and adjusts each edge's `confidence` property based on whether it
actually predicted anything. This makes edge confidence earned rather than
self-reported by a one-shot LLM call.
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any

import math
import re

from scipy.stats import binom

from shared.kafka import Topics
from shared.models.events import UNRATED_EDGE_CONFIDENCE
from services.agents.base import SentinelAgent
from shared.utils.quiet_failures import swallowed
from shared.utils.tasks import safe_create_task

logger = logging.getLogger("agent.edge_validator")

EXPOSURE_PREDICATES = ["SUPPLIES", "COMMODITY_EXPOSURE", "POSITIVE_EXPOSURE_TO", "INVERSE_EXPOSURE_TO"]
REACTION_WINDOW_HOURS = 24          # how long after the source-entity event to look for a reaction
MIN_SAMPLES_BEFORE_TRUST = 5        # don't promote/decay off one data point
CONFIDENCE_STEP = 0.05              # EWMA learning rate for confidence updates
LOOKBACK_DAYS = 30                  # evidence window, shared by the test and its null

# An edge is only evidence if the ticker reacts more often after the source
# event than it does anyway.
#
# The hit rate this used to score on counted how often the ticker had *any*
# anomaly >= 0.5 within 24 hours of a source event -- with no comparison to how
# often it has one in any 24 hours at all. For a liquid name that is most days,
# so a spurious edge scored near 1.0, and since the sweep runs every five
# minutes over the same fixed window the EWMA reached that rate within hours.
# Every exposure edge on an active ticker converged to confidence 1.0 and
# published a `quant_discovery` announcing it had been empirically validated.
# The mechanism built to make confidence earned was handing it out.
#
# The null is the ticker's own arrival rate: fit lambda over the lookback,
# take P(at least one reaction in 24h) = 1 - exp(-24*lambda), and ask how
# unlikely the observed hit count is under it. Confidence tracks 1 - p, so an
# edge earns it by beating its own base rate rather than by pointing at
# something busy.
REACTION_THRESHOLD = 0.5            # anomaly score that counts as a reaction

# Above this base rate the ticker reacts so often that no window can
# discriminate: 19 times in 20 a random day would "confirm" any edge. Scoring
# these produces confidence from arithmetic rather than evidence, so they are
# left where they are instead.
MAX_TESTABLE_BASE_RATE = 0.95


def _word_pattern(symbol: str) -> str:
    """A Postgres regex matching `symbol` as a whole word.

    `headline ILIKE '%BP%'` matched "abrupt", "BPO" and "subpoena". Short
    tickers are common and this ran on every source event and every reaction
    window, so the false positives went straight into the hit count the
    confidence was derived from.
    """
    return r"\m" + re.escape(symbol) + r"\M"


async def _base_rate(timescale_client: Any, ticker: str) -> Optional[float]:
    """P(at least one reaction from `ticker` in a 24h window), from its own history.

    Poisson rather than a day count: reactions cluster, and the arrival rate is
    what a 24-hour window actually integrates. Returns None when there is no
    history to fit, which is not the same as a rate of zero.
    """
    query = """
        SELECT count(*)::float AS n,
               EXTRACT(EPOCH FROM (NOW() - min(occurred_at))) AS span_sec
        FROM events
        WHERE occurred_at > NOW() - INTERVAL '%s days'
          AND (primary_entity_id = $1 OR headline ~* $2)
          AND anomaly_score >= $3
    """ % LOOKBACK_DAYS
    try:
        rows = await timescale_client.query(
            query, ticker, _word_pattern(ticker), REACTION_THRESHOLD
        )
    except Exception as e:
        swallowed("edge_validator.base_rate_query", e, logger, detail=ticker)
        return None

    if not rows:
        return None
    n = float(rows[0].get("n") or 0.0)
    span = float(rows[0].get("span_sec") or 0.0)
    if n <= 0 or span <= 0:
        # No reaction in the whole lookback. The edge cannot be confirmed, but
        # neither is the null degenerate: use the smallest rate the window can
        # resolve rather than zero, which would make any single hit infinitely
        # significant.
        span = LOOKBACK_DAYS * 86400.0
        n = 0.5
    lam = n / span
    p0 = 1.0 - math.exp(-lam * REACTION_WINDOW_HOURS * 3600.0)
    return min(max(p0, 1e-6), 1.0 - 1e-6)


def _evidence(hits: int, trials: int, base_rate: float) -> float:
    """How much better than chance, as a number in [0, 1].

    One-sided binomial tail: P(X >= hits | trials, base_rate). Confidence
    tracks 1 - p, so an edge whose hits are exactly what the base rate predicts
    earns about 0.5 and decays toward it, while one that beats the rate earns
    the difference.
    """
    if trials <= 0:
        return 0.0
    p_value = float(binom.sf(hits - 1, trials, base_rate))
    return min(max(1.0 - p_value, 0.0), 1.0)


async def validate_edges(
    neo4j_client: Any,
    timescale_client: Any,
    redis_client: Optional[Any] = None,
    producer: Optional[Any] = None,
) -> Dict[str, Any]:
    """
    Periodically backtests LLM-proposed ontology edges against realized market outcomes in TimescaleDB.
    Nudges confidence up for predictive edges and decays unproven guesses.

    Strictly uses parameterized Cypher queries to prevent injection vulnerabilities.
    """
    if neo4j_client is None or timescale_client is None:
        logger.warning("Neo4j or TimescaleDB client uninitialized. Skipping edge validation.")
        return {"validated": 0, "promoted": 0, "decayed": 0}

    # Match all exposure edges connecting entities to instruments
    query = """
    MATCH (a:Entity)-[r:SUPPLIES|COMMODITY_EXPOSURE|POSITIVE_EXPOSURE_TO|INVERSE_EXPOSURE_TO]->(b:Entity {type: 'instrument'})
    RETURN a.id AS source_id, type(r) AS predicate, b.id AS ticker,
           coalesce(r.confidence, $unrated) AS confidence,
           coalesce(r.validation_samples, 0) AS samples
    """

    try:
        edges = await neo4j_client.query(query, {"unrated": UNRATED_EDGE_CONFIDENCE})
    except Exception as e:
        logger.error(f"Failed querying Neo4j exposure edges: {e}")
        return {"validated": 0, "promoted": 0, "decayed": 0}

    if not edges:
        logger.debug("No exposure edges found in Neo4j graph to validate.")
        return {"validated": 0, "promoted": 0, "decayed": 0}

    validated_count = 0
    promoted_count = 0
    decayed_count = 0

    for edge in edges:
        source_id = str(edge.get("source_id", "")).strip().upper()
        predicate = str(edge.get("predicate", "")).strip()
        ticker = str(edge.get("ticker", "")).strip().upper()
        old_conf = float(edge.get("confidence", 0.5))
        prev_samples = int(edge.get("samples", 0))

        if not source_id or not ticker or predicate not in EXPOSURE_PREDICATES:
            continue

        # 1. Pull historical events tagged to source_id from TimescaleDB
        #
        # Exact match on the entity id and a word-boundary regex on the
        # headline. The ILIKE '%%SOURCE%%' this replaces matched any headline
        # containing the letters anywhere, which for a two-letter ticker is
        # most of them.
        events_query = """
            SELECT event_id, type, headline, anomaly_score, occurred_at
            FROM events
            WHERE occurred_at > NOW() - INTERVAL '%s days'
              AND (
                primary_entity_id = $1
                OR headline ~* $2
              )
            ORDER BY occurred_at DESC
            LIMIT 50
        """ % LOOKBACK_DAYS
        try:
            rows = await timescale_client.query(
                events_query, source_id, _word_pattern(source_id)
            )
        except Exception as e:
            swallowed("edge_validator.source_events_query", e, logger, detail=source_id)
            rows = []

        if not rows:
            continue

        # The null this edge has to beat, fitted once per ticker rather than
        # once per event.
        base_rate = await _base_rate(timescale_client, ticker)
        if base_rate is None:
            continue
        if base_rate > MAX_TESTABLE_BASE_RATE:
            logger.debug(
                "Skipping %s -[%s]-> %s: base reaction rate %.3f leaves the test no power.",
                source_id, predicate, ticker, base_rate,
            )
            continue

        hits = 0
        trials = 0

        # 2. For each source event, check if target ticker had a reaction in 24h post-event
        for event in rows:
            event_ts = event.get("occurred_at")
            if not event_ts:
                continue

            # Query TimescaleDB for ticker events in the [event_ts, event_ts + 24h] reaction window
            reaction_query = """
                SELECT event_id, type, anomaly_score
                FROM events
                WHERE (primary_entity_id = $1 OR headline ~* $2)
                  AND occurred_at >= $3
                  AND occurred_at <= $3 + INTERVAL '%s hours'
                  AND anomaly_score >= $4
                LIMIT 1
            """ % REACTION_WINDOW_HOURS
            try:
                rx_rows = await timescale_client.query(
                    reaction_query, ticker, _word_pattern(ticker), event_ts, REACTION_THRESHOLD
                )
            except Exception as e:
                # A failed query is not a miss. Counting it as one biased every
                # confidence downward in exactly the conditions -- database
                # under load -- where the bias is largest, and said nothing.
                swallowed("edge_validator.reaction_query", e, logger, detail=f"{source_id}->{ticker}")
                continue
            trials += 1
            if rx_rows:
                hits += 1

        if trials <= 0:
            continue

        # `validation_samples` is the size of the evidence, not a running total.
        #
        # It was `prev_samples + len(rows)`, and the sweep re-reads the same
        # fixed 30-day window every five minutes -- so it grew by up to 50 per
        # sweep, roughly 14,000 a day, off one unchanged body of evidence. The
        # MIN_SAMPLES_BEFORE_TRUST gate it feeds was satisfied within minutes of
        # startup for every edge, permanently.
        new_total_samples = trials
        hit_rate = hits / trials
        evidence = _evidence(hits, trials, base_rate)

        # 3. Only promote/decay if sample size >= MIN_SAMPLES_BEFORE_TRUST
        if new_total_samples >= MIN_SAMPLES_BEFORE_TRUST:
            # EWMA step towards how much the hits beat the ticker's own base rate
            new_conf = round(max(0.0, min(1.0, (1.0 - CONFIDENCE_STEP) * old_conf + CONFIDENCE_STEP * evidence)), 4)

            if abs(new_conf - old_conf) >= 0.001:
                # 4. Write back via fully parameterized Cypher
                update_query = """
                MATCH (a:Entity {id: $source_id})-[r]->(b:Entity {id: $ticker, type: 'instrument'})
                WHERE type(r) = $predicate
                SET r.confidence = $new_confidence,
                    r.validation_samples = $samples,
                    r.last_validated = timestamp()
                """
                try:
                    await neo4j_client.query(update_query, {
                        "source_id": source_id,
                        "ticker": ticker,
                        "predicate": predicate,
                        "new_confidence": new_conf,
                        "samples": new_total_samples,
                    })

                    logger.info(
                        f"📊 Edge Confidence Updated | {source_id} -[{predicate}]-> {ticker} | "
                        f"Confidence: {old_conf:.3f} → {new_conf:.3f} | "
                        f"Hit Rate: {hit_rate:.1%} vs base {base_rate:.1%} | "
                        f"Evidence: {evidence:.3f} ({hits}/{trials} hits)"
                    )

                    # Invalidate Redis exposure cache key so macro engine picks up change
                    if redis_client and hasattr(redis_client, "raw"):
                        try:
                            # SCAN and delete in batches. KEYS blocks the whole
                            # server, and this runs on every promoted edge.
                            stale = []
                            async for k in redis_client.raw.scan_iter(
                                match=f"sentinel:cache:exposure:{source_id}:*", count=500
                            ):
                                stale.append(k)
                                if len(stale) >= 500:
                                    await redis_client.raw.delete(*stale)
                                    stale = []
                            if stale:
                                await redis_client.raw.delete(*stale)
                        except Exception as cache_err:
                            # Said out loud: a cache that fails to invalidate
                            # serves a stale exposure figure to the macro
                            # engine, which is a wrong answer rather than a
                            # missing one.
                            logger.warning(
                                "Could not invalidate exposure cache for %s: %s",
                                source_id, cache_err,
                            )

                    # If edge promoted above 0.80, publish quant discovery event for RuleSynthesizer
                    if new_conf >= 0.80 and old_conf < 0.80 and producer:
                        discovery_payload = {
                            "type": "quant_discovery",
                            "source": "edge_validator",
                            "description": f"Empirically validated high-confidence relationship: {source_id} {predicate} {ticker} (confidence: {new_conf:.2f})",
                            "correlated_assets": [source_id, ticker],
                            "confidence": new_conf,
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                        }
                        await producer.send(Topics.QUANT_DISCOVERIES, discovery_payload, key=ticker)

                    if new_conf > old_conf:
                        promoted_count += 1
                    else:
                        decayed_count += 1
                    validated_count += 1

                except Exception as e:
                    logger.error(f"Failed updating Neo4j edge confidence for {source_id}->{ticker}: {e}")

    return {
        "validated": validated_count,
        "promoted": promoted_count,
        "decayed": decayed_count,
    }


class EdgeValidatorAgent(SentinelAgent):
    """
    Agent wrapper running EdgeValidator as a scheduled 5-minute background loop.
    """

    @property
    def output_topic(self) -> str:
        return Topics.QUANT_DISCOVERIES

    async def run(self):
        """Launches the periodic edge validation loop alongside reactive handler."""
        validation_task = safe_create_task(self._run_scheduled_validation())
        try:
            await super().run()
        finally:
            validation_task.cancel()

    async def _run_scheduled_validation(self):
        await asyncio.sleep(10)  # Initial startup delay
        while True:
            try:
                res = await validate_edges(
                    neo4j_client=self.neo4j,
                    timescale_client=self.db,
                    redis_client=self.redis,
                    producer=self._producer,
                )
                if res.get("validated", 0) > 0:
                    logger.info(f"⚡ Edge Validation Sweep: {res['validated']} edges evaluated ({res['promoted']} promoted, {res['decayed']} decayed)")
            except Exception as e:
                logger.error(f"Edge validation loop failed: {e}", exc_info=True)
            await asyncio.sleep(300)  # 5-minute cadence

    async def handle(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        # EdgeValidator operates on a scheduled cadence; reactive messages return None
        return None
