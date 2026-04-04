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
    """
    processed = 0
    errors  = 0

