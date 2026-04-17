"""
services/correlation/main.py  —  run loop only.

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

import logging
import os
import sys
from pathlib import Path

# ── 1. ENVIRONMENT & PATH SETUP ───────────────────────────────────────────────
# BEST PRACTICE: We dynamically calculate the project root directory and add it 
# to the system path. This ensures that Python can find our custom 'shared' modules 
# (like shared.kafka or shared.models) no matter which directory we run this script from.
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
    format="%(asctime)s [%(name)s] %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("correlation")

from shared.kafka import SentinelProducer, SentinelConsumer, Topics
from shared.models import NormalizedEvent, CorrelationCluster
from shared.db import get_redis
from services.correlation.event_store import EventStore

from services.correlation.rules import ALL_RULES


def main():
    logger.info("=" * 60)
    logger.info("SENTINEL  Correlation Engine")
    logger.info(f"Rules loaded: {len(ALL_RULES)}")
    logger.info("=" * 60)

    # ── 2. INITIALIZE INFRASTRUCTURE ──────────────────────────────────────────
    # EventStore: Connects to PostgreSQL (TimescaleDB) to allow rules to query past events.
    # Producer: The "Mailman" that sends generated alerts to the next microservice.
    # Consumer: The "Inbox" that receives standardized events from the Enrichment service.
    redis_client = get_redis()
    store    = EventStore(redis_client)
    producer = SentinelProducer()
    consumer = SentinelConsumer(
        topics=[Topics.ENRICHED_EVENTS],
        group_id="correlation-engine",
        auto_offset_reset="latest",
    )

    processed  = 0
    corr_fired = 0

    try:
        # ── 3. THE EVENT PUMP (MAIN LOOP) ─────────────────────────────────────
        # The 'while True' keeps the application running forever.
        while True:
            try:
                # The consumer pauses (blocks) here until a new message arrives from Kafka.
                for message in consumer:
                    try:
                        # Unpack the raw JSON message into our strictly-typed Python object.
                        event = NormalizedEvent(**message.value)

                        # ── 4. RULE ENGINE (PLUGIN PATTERN) ───────────────────
                        # ARCHITECTURE TIP: The Strategy/Plugin Pattern.
                        # Instead of writing massive, hard-to-read "if/else" blocks, we loop 
                        # through a list of independent rule functions (`ALL_RULES`). 
                        # Each rule evaluates the event and decides if an alert should be triggered.
                        for rule_fn in ALL_RULES:
                            try:
                                cluster: CorrelationCluster = rule_fn(event, store)
                            except Exception as e:
                                # TARGETED ERROR HANDLING: If one specific rule crashes (e.g., 
                                # due to a bad data lookup), we log the error and `continue`. 
                                # This prevents a single buggy rule from taking down the whole engine.
                                logger.error(f"Rule {rule_fn.__name__} error: {e}", exc_info=True)
                                continue

                            # If the rule returns None, it means "Nothing suspicious found."
                            if cluster is None:
                                continue

                            # ── 5. PERSIST & FORWARD ALERTS ───────────────────
                            # If a rule returned a cluster, an anomaly pattern was detected!
                            # First, save a permanent record to our database.
                            store.save_correlation(cluster)
                            # Second, send it to Kafka so the Alert Manager and Reasoning 
                            # engine can notify analysts or generate AI briefings.
                            producer.send(
                                Topics.CORRELATIONS,
                                cluster.dict(),
                                key=cluster.correlation_id,
                            )
                            corr_fired += 1
                            logger.info(
                                f"🔗 [{cluster.alert_tier.name}] {cluster.rule_name} — "
                                f"{cluster.description[:120]}"
                            )

                        processed += 1
                        if processed % 5000 == 0:
                            logger.info(f"Processed {processed} | Correlations fired {corr_fired}")

                    except Exception as e:
                        logger.error(f"Correlation loop error: {e}", exc_info=True)

            except StopIteration:
                logger.debug("Consumer iterator exhausted — restarting")
                continue

    except KeyboardInterrupt:
        # Graceful shutdown when the user presses Ctrl+C
        logger.info("Shutting down...")
    finally:
        # CLEANUP: Always close network connections to prevent memory/socket leaks.
        producer.close()
        consumer.close()
        logger.info(f"Final — processed: {processed}  correlations: {corr_fired}")


if __name__ == "__main__":
    main()