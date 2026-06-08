"""
services/correlation/main.py

Consumes enriched.events.
Runs every rule against every event.
Emits CorrelationCluster to correlations.detected when a rule fires.

Rules live in rules/:
  maritime.py     — MARITIME_001
  financial.py    — FINANCIAL_001
  aviation.py     — AVIATION_001
  news.py         — NEWS_001
  cross_domain.py — CROSS_001
"""

import asyncio
import json
import logging
import os
import sys
from pathlib import Path
import inspect

# ── 1. ENVIRONMENT & PATH SETUP ───────────────────────────────────────────────
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

from shared.kafka import SentinelProducer, SentinelConsumer, Topics
from shared.models import NormalizedEvent, CorrelationCluster
from shared.db import get_redis
from services.correlation.event_store import EventStore
from services.correlation.rules import ALL_RULES

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
    format="%(asctime)s [%(name)s] %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("correlation")


async def main():
    logger.info("=" * 60)
    logger.info("SENTINEL  Correlation Engine")
    logger.info(f"Rules loaded: {len(ALL_RULES)}")
    logger.info("=" * 60)

    # ── 2. INITIALIZE INFRASTRUCTURE ──────────────────────────────────────────
    redis_client = await get_redis()
    store    = EventStore(redis_client)
    producer = SentinelProducer()
    consumer = SentinelConsumer(
        topics=[Topics.ENRICHED_EVENTS],
        group_id="correlation-engine",
        auto_offset_reset="latest",
    )

    processed  = 0
    corr_fired = 0

    await producer.start()
    await consumer.start()

    try:
        # ── 3. THE EVENT PUMP (MAIN LOOP) ─────────────────────────────────────
        while True:
            try:
            # CORRECTED: Use native async batch fetching to prevent event-loop blocking
                batches = await consumer.get_batch(timeout_ms=1000)
                
                if not batches:
                    continue

                for tp, messages in batches.items():
                    for message in messages:
                        try:
                            # Parse the JSON payload safely before feeding to Pydantic
                            raw_data = json.loads(message.value.decode('utf-8'))
                            event = NormalizedEvent(**raw_data)

                            try:
                                store.add_event(event)
                            except Exception as e:
                                logger.error(f"Failed to store event {event.event_id}: {e}", exc_info=True)
                                continue

                            # ── 4. RULE ENGINE (PLUGIN PATTERN) ───────────────────
                            for rule_fn in ALL_RULES:
                                try:
                                    # Dynamically support both synchronous math rules AND asynchronous I/O rules
                                    if inspect.iscoroutinefunction(rule_fn):
                                        cluster: CorrelationCluster = await rule_fn(event, store)
                                    else:
                                        cluster: CorrelationCluster = rule_fn(event, store)
                                except Exception as e:
                                    logger.error(f"Rule {rule_fn.__name__} error: {e}", exc_info=True)
                                    continue

                                if cluster is None:
                                    continue

                                # ── 5. PERSIST & FORWARD ALERTS ───────────────────
                                store.save_correlation(cluster)
                                
                                # CORRECTED: Await the Kafka producer network call
                                await producer.send(
                                    Topics.CORRELATIONS,
                                    cluster.model_dump(),
                                    key=cluster.correlation_id,
                                )
                                corr_fired += 1
                                logger.info(
                                    f"🔗 [{cluster.alert_tier.name}] {cluster.rule_name} — "
                                    f"{cluster.description[:120]}"
                                )

                            processed += 1
                            if processed % 100 == 0:
                                logger.info(f"Heartbeat | Processed {processed} events | Total correlations: {corr_fired}")

                        except Exception as e:
                            logger.error(f"Correlation loop error: {e}", exc_info=True)
                            try:
                                # CORRECTED: Await the DLQ network call
                                await producer.send(
                                    Topics.DLQ,
                                    data={
                                        "topic": Topics.ENRICHED_EVENTS,
                                        "error": str(e),
                                        "raw": str(message.value)
                                    }
                                )
                            except Exception as dlq_e:
                                logger.error(f"FATAL: Failed to route to DLQ: {dlq_e}. Shutting down.")
                
                    
                    # CORRECTED: Commit offsets after batch is fully processed to prevent infinite replay
                    await consumer.commit()

            except Exception as outer_err:
                # Catch broad event loop/network exceptions without crashing the worker
                logger.error(f"Kafka consumer network/batch error. Backing off. {outer_err}")
                await asyncio.sleep(5)
                
    except asyncio.CancelledError:
        logger.info("Shutting down correlation engine...")
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        # CORRECTED: Await graceful closure of TCP sockets
        await producer.close()
        await consumer.close()
        logger.info(f"Final — processed: {processed}  correlations: {corr_fired}")


if __name__ == "__main__":
    # CORRECTED: OS-level event loop policy enforcement and proper coroutine execution
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())