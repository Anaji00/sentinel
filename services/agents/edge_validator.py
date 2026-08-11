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

from shared.kafka import Topics
from services.agents.base import SentinelAgent

logger = logging.getLogger("agent.edge_validator")

EXPOSURE_PREDICATES = ["SUPPLIES", "COMMODITY_EXPOSURE", "POSITIVE_EXPOSURE_TO", "INVERSE_EXPOSURE_TO"]
REACTION_WINDOW_HOURS = 24          # how long after the source-entity event to look for a reaction
MIN_SAMPLES_BEFORE_TRUST = 5        # don't promote/decay off one data point
CONFIDENCE_STEP = 0.05              # EWMA learning rate for confidence updates


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
           coalesce(r.confidence, 0.5) AS confidence,
           coalesce(r.validation_samples, 0) AS samples
    """

    try:
        edges = await neo4j_client.query(query)
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
        events_query = """
            SELECT event_id, type, headline, anomaly_score, occurred_at
            FROM events
            WHERE occurred_at > NOW() - INTERVAL '30 days'
              AND (
                primary_entity_id ILIKE $1 
                OR headline ILIKE $2
              )
            ORDER BY occurred_at DESC
            LIMIT 50
        """
        try:
            rows = await timescale_client.query(events_query, source_id, f"%{source_id}%")
        except Exception as e:
            logger.debug(f"Failed querying events for {source_id}: {e}")
            rows = []

        if not rows:
            continue

        hits = 0
        total_eval_samples = len(rows)

        # 2. For each source event, check if target ticker had a reaction in 24h post-event
        for event in rows:
            event_ts = event.get("occurred_at")
            if not event_ts:
                continue

            # Query TimescaleDB for ticker events in the [event_ts, event_ts + 24h] reaction window
            reaction_query = """
                SELECT event_id, type, anomaly_score
                FROM events
                WHERE (primary_entity_id ILIKE $1 OR headline ILIKE $2)
                  AND occurred_at >= $3
                  AND occurred_at <= $3 + INTERVAL '24 hours'
                  AND anomaly_score >= 0.5
                LIMIT 1
            """
            try:
                rx_rows = await timescale_client.query(reaction_query, ticker, f"%{ticker}%", event_ts)
                if rx_rows:
                    hits += 1
            except Exception:
                pass

        new_total_samples = prev_samples + total_eval_samples
        hit_rate = hits / max(1, total_eval_samples)

        # 3. Only promote/decay if sample size >= MIN_SAMPLES_BEFORE_TRUST
        if new_total_samples >= MIN_SAMPLES_BEFORE_TRUST:
            # EWMA step towards empirical hit rate
            new_conf = round(max(0.0, min(1.0, (1.0 - CONFIDENCE_STEP) * old_conf + CONFIDENCE_STEP * hit_rate)), 4)

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
                        f"Confidence: {old_conf:.3f} → {new_conf:.3f} | Hit Rate: {hit_rate:.1%} "
                        f"({hits}/{total_eval_samples} hits, total samples: {new_total_samples})"
                    )

                    # Invalidate Redis exposure cache key so macro engine picks up change
                    if redis_client and hasattr(redis_client, "raw"):
                        try:
                            keys = await redis_client.raw.keys(f"sentinel:cache:exposure:{source_id}:*")
                            if keys:
                                await redis_client.raw.delete(*keys)
                        except Exception:
                            pass

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
        validation_task = asyncio.create_task(self._run_scheduled_validation())
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
