"""
services/dlq-worker/main.py

Subscribes to Topics.DLQ.
Saves corrupted/failed payloads to the `failed_events` Postgres table.
Pings an admin via Telegram with a strict rate limit to prevent spam loops.
"""

import asyncio
import json
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import aiohttp
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
    format="%(asctime)s [%(name)s] %(levelname)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("dlq-worker")

from shared.kafka import SentinelConsumer, Topics
from shared.db import get_timescale

# --- SECRETS & CONFIG ---
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
# Fallback to the standard chat ID if an admin-specific one isn't set
TELEGRAM_ADMIN_CHAT_ID = os.getenv("TELEGRAM_ADMIN_CHAT_ID") or os.getenv("TELEGRAM_CHAT_ID")
TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

# Spam Protection: Only ping the admin once every 15 minutes, 
# even if 10,000 events fail in a row.
ALERT_COOLDOWN_SECONDS = 900  

async def _send_telegram_alert(session: aiohttp.ClientSession, topic: str, error: str):
    """Sends a markdown-formatted alert to the admin."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_ADMIN_CHAT_ID:
        return

    msg = (
        f"🚨 *SENTINEL DLQ ALERT*\n\n"
        f"Pipeline failure on topic: `{topic}`\n"
        f"*Error*: `{error[:250]}...`\n\n"
        f"Check the `failed_events` DB table for full payload."
    )

    try:
        async with session.post(
            f"{TELEGRAM_API}/sendMessage",
            json={"chat_id": TELEGRAM_ADMIN_CHAT_ID, "text": msg, "parse_mode": "Markdown"},
            timeout=5
        ) as resp:
            if resp.status != 200:
                logger.error(f"Telegram alert failed: {await resp.text()}")
    except Exception as e:
        logger.error(f"Failed to reach Telegram API: {e}")

def _consume_loop(consumer, db, async_loop, session):
    """Blocking loop that reads from Kafka and writes to Postgres."""
    last_alert_time = 0

    while True:
        try:
            for message in consumer:
                # payload is a dict formatted in enrichment/main.py
                payload = message.value
                
                original_topic = payload.get("topic", "unknown")
                error_msg = payload.get("error", "No error provided")
                raw_data = payload.get("raw", {})

                # 1. Save to PostgreSQL
                try:
                    db.execute("""
                        INSERT INTO failed_events (original_topic, error_message, raw_payload)
                        VALUES (%s, %s, %s)
                    """, (original_topic, error_msg, json.dumps(raw_data)))
                    logger.info(f"Saved failed event from {original_topic} to DB.")
                except Exception as e:
                    logger.error(f"FATAL: Could not save to DLQ database: {e}")
                    continue # Skip alerting if we couldn't even save it

                # 2. Rate-Limited Admin Alert
                now = time.time()
                if now - last_alert_time >= ALERT_COOLDOWN_SECONDS:
                    last_alert_time = now
                    # We are in a sync thread, so we schedule the async HTTP request 
                    # safely back onto the main asyncio loop.
                    asyncio.run_coroutine_threadsafe(
                        _send_telegram_alert(session, original_topic, error_msg),
                        async_loop
                    )

        except KeyboardInterrupt:
            break
        except StopIteration:
            continue
        except Exception as e:
            logger.error(f"DLQ consume loop error: {e}", exc_info=True)
            time.sleep(5) # Backoff before retrying

async def main():
    logger.info("=" * 60)
    logger.info("SENTINEL  DLQ Worker Service")
    logger.info("=" * 60)

    db = get_timescale()
    
    consumer = SentinelConsumer(
        topics=[Topics.DLQ],
        group_id="dlq-worker",
        auto_offset_reset="earliest", # Always process from the beginning of failures
    )

    loop = asyncio.get_running_loop()
    connector = aiohttp.TCPConnector(limit=5)
    
    async with aiohttp.ClientSession(connector=connector) as session:
        with ThreadPoolExecutor(max_workers=1, thread_name_prefix="dlq_consume") as pool:
            await loop.run_in_executor(
                pool,
                _consume_loop,
                consumer, db, loop, session
            )

    consumer.close()

if __name__ == "__main__":
    asyncio.run(main())