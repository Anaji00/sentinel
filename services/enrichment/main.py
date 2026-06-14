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
from shared.db import get_redis, get_timescale
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


async def process_single_message(msg, enrichers, dlq):
    """Encapsulates a single message's processing to run concurrently."""
    maritime, aviation, news, cyber, tradfi, crypto, prediction = enrichers
    topic = msg.topic
    
    try:
        raw_data = json.loads(msg.value.decode('utf-8'))
        raw = RawEvent(**raw_data)
        
        enriched = None
        if topic == Topics.RAW_MARITIME:       enriched = await maritime.enrich(raw)
        elif topic == Topics.RAW_AVIATION:     enriched = await aviation.enrich(raw)
        elif topic == Topics.RAW_NEWS:         enriched = await news.enrich(raw)
        elif topic == Topics.RAW_CYBER:        enriched = await cyber.enrich(raw)
        elif topic == Topics.RAW_TRADFI:       enriched = await tradfi.enrich(raw)
        elif topic == Topics.RAW_CRYPTO:       enriched = await crypto.enrich(raw)
        elif topic == Topics.RAW_PREDICTION:   enriched = await prediction.enrich(raw)
        
        return {"status": "success", "enriched": enriched, "topic": topic}
        
    except json.JSONDecodeError as e:
        logger.error(f"POISON PILL JSON dropped: {e}", exc_info=True)
        await dlq.send(Topics.DLQ, {"error": "Invalid JSON bytes", "topic": topic, "raw": str(msg.value)})
        return {"status": "error", "error": e}
    except Exception as e:
        logger.error(f"[{topic}] Enrichment fault: {e}", exc_info=True)
        await dlq.send(Topics.DLQ, {"error": str(e), "topic": topic})
        return {"status": "error", "error": e}


async def main():
    logger.info("=" * 60)
    logger.info("SENTINEL  Enrichment Service (Multi-Domain Edition)")
    logger.info("=" * 60)

    bootstrap_database()  # Ensure DB schema is ready before processing

    timescale = get_timescale()
    redis     = await get_redis()
    producer = SentinelProducer()
    dlq = SentinelProducer()
    await producer.start()
    await dlq.start()
    
    scorer = DynamicAnomalyScorer(redis)
    db = DBWriter(timescale)
    graph = GraphWriter(producer)
    resolver = EntityResolver(redis, timescale)

    maritime = MaritimeEnricher(scorer, graph, redis, resolver)
    aviation = AviationEnricher(scorer, graph, redis, resolver)
    news = NewsEnricher(scorer, graph, redis)
    cyber = CyberEnricher(scorer, graph, redis)
    tradfi = TradFiEnricher(scorer, graph, redis)
    crypto = CryptoEnricher(scorer, graph, redis)
    prediction = PredictionEnricher(scorer, graph, redis)
    
    enrichers_tuple = (maritime, aviation, news, cyber, tradfi, crypto, prediction)

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

    logger.info("Enrichment Pipeline LIVE. Listening for raw telemetry...")
    
    try:
        while True:
            batches = await consumer.get_batch(timeout_ms=1000)
            if not batches:
                continue

            for tp, messages in batches.items():
                batch_to_write = []
                
                # ── 1. ASYNC CONCURRENT ENRICHMENT ───────────────────────────
                # Pack all messages in this partition into async tasks
                tasks = [process_single_message(msg, enrichers_tuple, dlq) for msg in messages]
                
                # Execute all enrichments simultaneously 
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # ── 2. ASYNC CONCURRENT PRODUCER DISPATCH ────────────────────
                produce_tasks = []
                for res in results:
                    if isinstance(res, dict) and res.get("status") == "success" and res.get("enriched"):
                        enriched: NormalizedEvent = res["enriched"]
                        batch_to_write.append(enriched)
                        
                        # Queue the Kafka dispatches to run concurrently
                        produce_tasks.append(
                            producer.send(
                                Topics.ENRICHED_EVENTS,
                                enriched.model_dump(),
                                key=enriched.primary_entity.id,
                            )
                        )
                
                if produce_tasks:
                    await asyncio.gather(*produce_tasks, return_exceptions=True)
                    processed += len(produce_tasks)
                    if processed % 500 == 0:
                        logger.info(f"Processed {processed} successfully")

                # ── 3. FAULT-TOLERANT DB WRITES ──────────────────────────────
                if batch_to_write:
                    for attempt in range(3):
                        try:
                            await loop.run_in_executor(None, db.write_events_batch, batch_to_write)
                            break # Success
                        except Exception as write_err:
                            if attempt == 2: 
                                logger.error(f"FATAL DB WRITE ERROR: {write_err}. Routing batch to DLQ to prevent data loss.", exc_info=True)
                                # CRITICAL FIX: Send the failed DB batch to DLQ rather than crashing the service
                                dlq_tasks = [dlq.send(Topics.DLQ, {"error": "DB_WRITE_FAILED", "event_id": e.event_id}) for e in batch_to_write]
                                await asyncio.gather(*dlq_tasks, return_exceptions=True)
                            else:
                                await asyncio.sleep(2 ** attempt) 

                # ── 4. COMMIT ────────────────────────────────────────────────
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