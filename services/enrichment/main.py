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

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
    format="%(asctime)s [%(name)s] %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("enrichment")

from shared.kafka import SentinelProducer, SentinelConsumer, Topics
from shared.models import RawEvent, NormalizedEvent
from shared.db import get_async_redis, get_timescale, get_neo4j
from shared.db.bootstrap import bootstrap_database

from services.enrichment.anomaly_scorer import DynamicAnomalyScorer
from services.enrichment.db_writer import DBWriter
from services.enrichment.graph_writer import GraphWriter
from services.enrichment.entity_resolver import EntityResolver
from services.enrichment.gap_detector import VesselGapDetector

# --- THE NEW ENRICHERS ---
from services.enrichment.enrichers.maritime import MaritimeEnricher
from services.enrichment.enrichers.aviation import AviationEnricher
from services.enrichment.enrichers.news import NewsEnricher
from services.enrichment.enrichers.cyber import CyberEnricher
from services.enrichment.enrichers.tradfi import TradFiEnricher
from services.enrichment.enrichers.crypto import CryptoEnricher
from services.enrichment.enrichers.prediction import PredictionEnricher

def _consume_loop(consumer, maritime, aviation, news, cyber, 
                  tradfi, crypto, prediction, db, producer, dlq):
    """Blocking Kafka consume loop."""
    processed = 0
    errors  = 0

    while True:
        try:
            for message in consumer:
                topic = message.topic
                raw_data = message.value
                
                enriched: Optional[NormalizedEvent] = None
                try:
                    raw = RawEvent(**raw_data)
                    
                    # ── THE UPDATED ROUTER ───────────────────────────────────
                    if topic == Topics.RAW_MARITIME:
                        enriched = maritime.enrich(raw)
                    elif topic == Topics.RAW_AVIATION:
                        enriched = aviation.enrich(raw)
                    elif topic == Topics.RAW_NEWS:
                        enriched = news.enrich(raw)
                    elif topic == Topics.RAW_CYBER:
                        enriched = cyber.enrich(raw)
                    elif topic == Topics.RAW_TRADFI:
                        enriched = tradfi.enrich(raw)
                    elif topic == Topics.RAW_CRYPTO:
                        enriched = crypto.enrich(raw)
                    elif topic == Topics.RAW_PREDICTION:
                        enriched = prediction.enrich(raw)
                    
                    
                    if enriched:
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
                        dlq.send(Topics.DLQ, {"error": str(e), "topic": topic, "raw": raw_data})
                    except:
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
    logger.info("SENTINEL  Enrichment Service (Multi-Domain Edition)")
    logger.info("=" * 60)

    timescale = get_timescale()
    neo4j     = await get_neo4j()
    redis     = await get_async_redis()
    producer = SentinelProducer()
    dlq = SentinelProducer()
    await producer.start()
    await dlq.start()
    
    scorer = DynamicAnomalyScorer(redis)
    db = DBWriter(timescale)
    graph = GraphWriter(producer)
    resolver = EntityResolver(redis, None)

    # Instantiate all 7 enrichers
    maritime = MaritimeEnricher(scorer, graph, db, redis, resolver)
    aviation = AviationEnricher(scorer, graph)
    news = NewsEnricher(scorer)
    cyber = CyberEnricher(scorer)
    tradfi = TradFiEnricher(scorer, redis)
    crypto = CryptoEnricher(scorer, redis)
    prediction = PredictionEnricher(scorer, redis)
    
    # Listen to ALL raw topics
    consumer = SentinelConsumer(
        topics=[
            Topics.RAW_MARITIME, Topics.RAW_AVIATION, Topics.RAW_NEWS, 
            Topics.RAW_CYBER, Topics.RAW_TRADFI, Topics.RAW_CRYPTO, Topics.RAW_PREDICTION
        ],
        group_id="enrichment-service",
    )
    await consumer.start()

    gap = VesselGapDetector(producer, scorer, db, redis)
    gap_task = asyncio.create_task(gap.run())

    loop = asyncio.get_running_loop()
    processed = 0
    errors = 0

    logger.info("Enrichment Pipeline LIVE. Listening for raw telemetry...")
    
    try:
        while True:
            # ASYNC BATCHING
            batches = await consumer.get_batch(timeout_ms=1000)
            if not batches:
                continue

            for tp, messages in batches.items():
                batch_to_write = []

                for msg in messages:
                    topic = msg.topic
                    enriched: Optional[NormalizedEvent] = None

                    try:
                        # CRITICAL FIX: Safe Byte-to-JSON Deserialization
                        # This prevents malformed data from causing an uncatchable C-level crash.
                        raw_data = json.loads(msg.value.decode('utf-8'))
                        raw = RawEvent(**raw_data)

                        # ── THE ASYNC ROUTER ───────────────────────────────────
                        # All enrichers must be awaited since they now interact with async ML/DB tools
                        if topic == Topics.RAW_MARITIME:       enriched = await maritime.enrich(raw)
                        elif topic == Topics.RAW_AVIATION:     enriched = await aviation.enrich(raw)
                        elif topic == Topics.RAW_NEWS:         enriched = await news.enrich(raw)
                        elif topic == Topics.RAW_CYBER:        enriched = await cyber.enrich(raw)
                        elif topic == Topics.RAW_TRADFI:       enriched = await tradfi.enrich(raw)
                        elif topic == Topics.RAW_CRYPTO:       enriched = await crypto.enrich(raw)
                        elif topic == Topics.RAW_PREDICTION:   enriched = await prediction.enrich(raw)

                        if enriched:
                            batch_to_write.append(enriched)
                            await producer.send(
                                Topics.ENRICHED_EVENTS,
                                enriched.model_dump(),
                                key=enriched.primary_entity.id,
                            )
                            processed += 1
                            if processed % 500 == 0:
                                logger.info(f"Processed {processed} | Errors {errors}")
                    except json.JSONDecodeError as e:
                        errors += 1
                        logger.error(f"POISON PILL JSON dropped: {e}", exc_info=True)
                        await dlq.send(Topics.DLQ, {"error": "Invalid JSON bytes", "topic": topic, "raw": str(msg.value)})
                    except Exception as e:
                        errors += 1
                        logger.error(f"[{topic}] Enrichment fault: {e}", exc_info=True)
                        await dlq.send(Topics.DLQ, {"error": str(e), "topic": topic})
                    
                if batch_to_write:
                    await loop.run_in_executor(None, db.write_events_batch, batch_to_write)
                
                # 8. THE AT-LEAST-ONCE CHECKPOINT
                # Only explicitly commit offsets for this partition AFTER the database 
                # write succeeds and all network sends are awaited.
            await consumer.commit()
        
    except asyncio.CancelledError:
        logger.info("Shutdown signal received. Closing consumer...")
    except Exception as e:
        logger.critical(f"Fatal error in main loop: {e}", exc_info=True)
    finally:
        gap_task.cancel()
        try:
            await gap_task
        except asyncio.CancelledError:
            pass

        await producer.close()
        await dlq.close()
        await consumer.close()


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())