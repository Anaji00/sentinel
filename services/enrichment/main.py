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
    neo4j     = get_neo4j()
    redis     = get_redis()
    
    scorer = AnomalyScorer(redis)
    db = DBWriter(timescale)
    graph = GraphWriter(neo4j)
    resolver = EntityResolver(redis, neo4j)

    # Instantiate all 7 enrichers
    maritime = MaritimeEnricher(scorer, graph, db, redis, resolver)
    aviation = AviationEnricher(scorer, graph)
    news = NewsEnricher(scorer)
    cyber = CyberEnricher(scorer)
    tradfi = TradFiEnricher(scorer, redis)
    crypto = CryptoEnricher(scorer, redis)
    prediction = PredictionEnricher(scorer, redis)

    producer = SentinelProducer()
    dlq = SentinelProducer()
    
    # Listen to ALL raw topics
    consumer = SentinelConsumer(
        topics=[
            Topics.RAW_MARITIME, Topics.RAW_AVIATION, Topics.RAW_NEWS, 
            Topics.RAW_CYBER, Topics.RAW_TRADFI, Topics.RAW_CRYPTO, Topics.RAW_PREDICTION
        ],
        group_id="enrichment-service",
        auto_offset_reset="latest",
    )

    gap = VesselGapDetector(producer, scorer, db, redis)
    gap_task = asyncio.create_task(gap.run())

    loop = asyncio.get_running_loop()

    with ThreadPoolExecutor(max_workers=1, thread_name_prefix="consume") as pool:
        processed, errors = await loop.run_in_executor(
            pool, 
            _consume_loop, 
            consumer, maritime, aviation, news, cyber, tradfi, crypto, prediction, db, producer, dlq,
        )
    
    gap_task.cancel()
    try:
        await gap_task
    except asyncio.CancelledError:
        pass

    producer.close()
    dlq.close()
    consumer.close()

if __name__ == "__main__":
    asyncio.run(main())