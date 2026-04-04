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

from services.correlation.event_store import EventStore
from services.correlation.rules import ALL_RULES


def main():
    logger.info("=" * 60)
    logger.info("SENTINEL  Correlation Engine")
    logger.info(f"Rules loaded: {len(ALL_RULES)}")
    logger.info("=" * 60)

    store    = EventStore()
    producer = SentinelProducer()
    consumer = SentinelConsumer(
        topics=[Topics.ENRICHED_EVENTS],
        group_id="correlation-engine",
        auto_offset_reset="latest",
    )

    processed  = 0
    corr_fired = 0

    try:
        while True:
            try:
                for message in consumer:
                    try:
                        event = NormalizedEvent(**message.value)

                        for rule_fn in ALL_RULES:
                            try:
                                cluster: CorrelationCluster = rule_fn(event, store)
                            except Exception as e:
                                logger.error(f"Rule {rule_fn.__name__} error: {e}", exc_info=True)
                                continue

                            if cluster is None:
                                continue

                            store.save_correlation(cluster)
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
        logger.info("Shutting down...")
    finally:
        producer.close()
        consumer.close()
        logger.info(f"Final — processed: {processed}  correlations: {corr_fired}")


if __name__ == "__main__":
    main()