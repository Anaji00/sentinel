"""
services/collector-equities/main.py

ENTERPRISE EQUITIES COLLECTOR
=============================
Ingests real-time US Equity data via Alpaca WebSockets.
Features:
- Block Trade Detection (> $500k notional value)
- Minute-Bar Volume Spike Detection (compared to 30-min rolling average)
"""

import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
import websockets

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

from shared.kafka import SentinelProducer, Topics
from shared.models import RawEvent
from shared.db import get_redis

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s — %(message)s")
logger = logging.getLogger("collector.equities")

ALPACA_API_KEY = os.getenv("ALPACA_API_KEY")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY")