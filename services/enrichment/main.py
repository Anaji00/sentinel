"""
services/enrichment/main.py  —  run loop only.
 
Starts all components, then runs a Kafka consume loop in a thread executor
so the asyncio event loop stays free for the gap detector background task.
 
FIX (code review — Bug 1): Event loop deadlock.
  asyncio.create_task(gap.run()) then a blocking `for message in consumer`
  loop meant the gap detector task was scheduled but never ran — the sync
  loop held the event loop indefinitely. Fix: run the blocking Kafka consume
  loop in loop.run_in_executor() (a thread pool) so the gap detector coroutine
  gets CPU time between consume iterations.
 
FIX (code review — Bug 2): Dead EntityResolver.
  resolver was instantiated but never passed to any enricher. MaritimeEnricher
  now receives the resolver and uses it as the first lookup step before
  falling back to the inline Redis cache read.
"""
 
import asyncio
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Optional
 
from dotenv import load_dotenv
 
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
    format="%(asctime)s [%(name)s] %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("enrichment")
 
from shared.kafka import SentinelProducer, SentinelConsumer, Topics
from shared.models import RawEvent, NormalizedEvent
from shared.db import get_timescale, get_neo4j, get_redis

from services.enrichment.anomaly_scorer import AnomalyScorer
from services.enrichment.db_writer import DBWriter
from services.enrichment.graph_writer import GraphWriter
from services.enrichment.entity_resolver import EntityResolver
from services.enrichment.gap_detector import VesselGapDetector
from services.enrichment.enrichers.maritime import MaritimeEnricher
from services.enrichment.enrichers.financial import FinancialEnricher
from services.enrichment.enrichers.aviation import AviationEnricher
from services.enrichment.enrichers.news import NewsEnricher
from services.enrichment.enrichers.cyber import CyberEnricher

def _consume_loop(consumer, maritime, aviation, 
                  news, cyber, financial, db, producer, dlq):
    """
    Blocking Kafka consume loop — runs in ThreadPoolExecutor so the
    asyncio event loop stays free for the gap detector task.
    
    BEGINNER EXPLANATION:
    This function is the "Mail Sorting Room". It constantly pulls raw messages 
    from Kafka, looks at the topic (e.g., is it maritime or financial?), and 
    hands it to the correct "Enricher" to process.
    """
    processed = 0
    errors  = 0

    while True:
        try:
            for message in consumer:
                topic = message.topic
                raw_data = message.value
                
                # Reset the enriched event for each new message
                enriched: Optional[NormalizedEvent] = None
                try:
                    raw = RawEvent(**raw_data)
                    
                    # ── ROUTER ───────────────────────────────────────────────
                    # Pass the raw data to the specialized domain enricher.
                    if topic == Topics.RAW_MARITIME:
                        enriched = maritime.enrich(raw)
                    elif topic == Topics.RAW_FINANCIAL:
                        enriched = financial.enrich(raw)
                    elif topic == Topics.RAW_AVIATION:
                        enriched = aviation.enrich(raw)
                    elif topic == Topics.RAW_NEWS:
                        enriched = news.enrich(raw)
                    elif topic == Topics.RAW_CYBER:
                        enriched = cyber.enrich(raw)
                    
                    if enriched:
                        # ── PERSIST & FORWARD ────────────────────────────────
                        # Save the enriched event to the database, then send it
                        # back to Kafka so the Correlation Engine can see it.
                        db.write_event(enriched)
                        producer.send(
                            Topics.ENRICHED_EVENTS,
                            enriched.model_dump(),
                            key=enriched.primary_entity.id,
                        )
                        processed += 1
                        if processed % 500 == 0:
                            logger.info(f"Processed {processed} | Errors {errors}")

                except Exception as e:
                    errors += 1
                    logger.error(f"[{topic}] {e}", exc_info=True)
                    try:
                        # DLQ (Dead Letter Queue) PATTERN:
                        # If a message causes a crash (e.g., malformed JSON), we don't
                        # want it to block the pipeline, but we also don't want to lose it.
                        # We send it to a special "error" topic (DLQ) for later debugging.
                        dlq.send(Topics.DLQ, {"error": str(e), "topic": topic, "raw": raw_data})
                    except Exception as e:
                        logger.error(f"DLQ error: {e}", exc_info=True)
                        pass
        except KeyboardInterrupt:
            break
        except StopIteration:
            continue
        except Exception as e:
            logger.error(f"Consumer error: {e}", exc_info=True)
            break

    return processed, errors

async def main():
    logger.info("=" * 60)
    logger.info("SENTINEL  Enrichment Service")
    logger.info("=" * 60)

    # 1. Initialize Database Connections
    timescale = get_timescale()
    neo4j     = get_neo4j()
    redis     = get_redis()
    
    # 2. Init Shared Tools (The building blocks used by the enrichers)
    scorer = AnomalyScorer(redis)
    db = DBWriter(timescale)
    graph = GraphWriter(neo4j)
    resolver = EntityResolver(redis, neo4j)

    # 3. FACTORY: Initialize all Enrichers (Dependency Injection)
    # BEST PRACTICE: Dependency Injection. We create the DB connections 
    # once and pass them IN to the enrichers, rather than having each 
    # enricher create its own connections. This saves memory and prevents connection leaks.
    maritime = MaritimeEnricher(scorer, graph, db, redis, resolver)
    aviation = AviationEnricher(scorer, graph)  # FIXED: Typo 'aviaton'
    news = NewsEnricher(scorer)
    cyber = CyberEnricher(scorer)
    financial = FinancialEnricher(scorer)

    # 4. Initialize Kafka infrastructure
    producer = SentinelProducer()
    dlq = SentinelProducer()
    consumer = SentinelConsumer(
        topics=Topics.ALL_RAW,  # This array automatically includes RAW_FINANCIAL and RAW_CYBER
        group_id="enrichment-service",
        auto_offset_reset="latest",
    )

    # Start the background Gap Detector task (async)
    gap = VesselGapDetector(producer, scorer, db, redis)
    gap_task = asyncio.create_task(gap.run())
    logger.info("Gap detector started")
    logger.info(f"Consuming: {Topics.ALL_RAW}")

    loop = asyncio.get_running_loop()

    # 5. EXECUTION: Pass our newly built enrichers into the thread pool loop
    # ARCHITECTURE TIP: Mixing Sync and Async code.
    # The Kafka consumer is "blocking" (it pauses the program until a message arrives).
    # If we ran it directly in `asyncio`, the `gap_task` would never get a chance to run.
    # By putting the consumer in a `ThreadPoolExecutor`, it runs in a separate thread,
    # allowing the main asyncio loop to keep ticking.
    with ThreadPoolExecutor(max_workers=1, thread_name_prefix="consume") as pool:
        processed, errors = await loop.run_in_executor(
            pool, 
            _consume_loop, 
            consumer, maritime, aviation, news, cyber, financial, db, producer, dlq,
        )
    
    gap_task.cancel()
    try:
        await gap_task
    except asyncio.CancelledError:
        pass

    producer.close()
    dlq.close()
    consumer.close()
    logger.info(f"Final — processed: {processed}  errors: {errors}")

if __name__ == "__main__":
    asyncio.run(main())
