"""
services/dlq-worker/main.py

Subscribes to Topics.DLQ.
Saves corrupted/failed payloads to the `failed_events` Postgres table.
Pings an admin via Telegram with a strict rate limit to prevent spam loops.
"""

import asyncio
import hashlib
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

from shared.utils.logging import setup_sentinel_logging, BatchLogger

logger = setup_sentinel_logging("dlq-worker", level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")))
batch_logger = BatchLogger(logger, "dlq-worker", flush_interval_sec=10.0)

from shared.kafka import SentinelConsumer, SentinelProducer, Topics
from shared.db import get_timescale, get_redis
from shared.utils.heartbeat import start_heartbeat_task
from shared.utils.tasks import safe_create_task

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


# Retries are capped so a message that cannot be handled stops circulating.
MAX_RETRIES = 3
# How long a retry counter outlives its message. Long enough that a retry storm
# is bounded, short enough that a genuinely transient failure hours later gets
# a fresh set of attempts rather than inheriting an exhausted one.
RETRY_COUNTER_TTL_SEC = 3600


def _message_fingerprint(topic: str, raw_data) -> str:
    """Stable identity for a dead letter, whatever shape its payload has.

    The retry counter cannot live inside the payload: the payloads are of
    unrelated shapes and adding a field to an arbitrary one corrupts it for the
    consumer it is being replayed to.
    """
    try:
        body = json.dumps(raw_data, sort_keys=True, default=str)
    except Exception:
        body = str(raw_data)
    digest = hashlib.sha256(f"{topic}|{body}".encode("utf-8", "replace")).hexdigest()[:32]
    return f"sentinel:dlq:retry:{digest}"


async def _get_retry_count(redis_client, key: str) -> int:
    """Current attempt count. A store that cannot answer returns 0, which
    retries rather than discards -- losing a recoverable event is the worse
    of the two failures."""
    try:
        raw = await redis_client.raw.get(key)
        return int(raw) if raw is not None else 0
    except Exception as e:
        logger.debug(f"Retry counter read failed for {key} (treating as first attempt): {e}")
        return 0


async def _set_retry_count(redis_client, key: str, value: int) -> None:
    try:
        await redis_client.raw.set(key, str(value), ex=RETRY_COUNTER_TTL_SEC)
    except Exception as e:
        # A counter that cannot be written means the ceiling cannot be enforced
        # for this message, which is the defect this replaces. Say so loudly.
        logger.warning(f"Retry counter write failed for {key}; retry ceiling not enforced: {e}")


async def _republish_after_backoff(producer, topic: str, raw_data, attempt: int) -> None:
    """Wait out the backoff off the consume loop, then republish."""
    await asyncio.sleep(min(2 ** attempt, 30))
    try:
        await producer.send(topic, raw_data)
    except Exception as e:
        logger.error(f"Failed to re-publish to {topic}: {e}")


async def _consume_loop(consumer, db, session, producer, redis_client):
    """Blocking loop that reads from Kafka and writes to Postgres with retry logic."""
    last_alert_time = 0

    while True:
        try:
            batches = await consumer.get_batch(timeout_ms=1000)
            if not batches:
                continue
            
            for tp, messages in batches.items():

                for message in messages:
                    # payload is a dict formatted in enrichment/main.py
                    try:
                        payload = json.loads(message.value.decode("utf-8"))
                    except Exception:
                        payload = {"raw": str(message.value), "error": "Unparseable bytes"}
                    
                    original_topic = payload.get("topic", "unknown")
                    error_msg = payload.get("error", "No error provided")
                    raw_data = payload.get("raw", {})
                    # If raw_data is a string, attempt to parse it back into a dictionary
                    if isinstance(raw_data, str):
                        try:
                            parsed_raw = json.loads(raw_data)
                            if isinstance(parsed_raw, dict):
                                raw_data = parsed_raw
                        except Exception as parse_err:
                            logger.debug(f"DLQ raw payload JSON parse failed (non-fatal): {parse_err}")

                    # Resolve the retry count.
                    #
                    # This was read from and written to `raw_data["raw_payload"]`,
                    # a key only event-shaped messages carry. An ontology
                    # proposal is {entity_id, action, data} and has no such key,
                    # so the count was never persisted: the republished message
                    # failed again, was wrapped in a fresh envelope with no
                    # counter, and resolved to 0 on every pass. The `< 3`
                    # ceiling was unreachable and the retry could not terminate.
                    #
                    # The counter now lives in Redis under a fingerprint of the
                    # message itself, which every payload shape has.
                    fingerprint = _message_fingerprint(original_topic, raw_data)
                    retry_count = payload.get("retry_count")
                    if retry_count is None:
                        retry_count = await _get_retry_count(redis_client, fingerprint)

                    # 1. Evaluate Retry vs Permanent Failure
                    is_poison_pill = not isinstance(raw_data, dict) or any(
                        p.lower() in error_msg.lower() for p in (
                            "jsondecodeerror", "invalid json", "validationerror",
                            "invalid rawevent", "field required", "enrichment error",
                            "type=", "input_value=", "rawevent", "dict expected", "unparseable"
                        )
                    )

                    if retry_count < MAX_RETRIES and original_topic != "unknown" and not is_poison_pill:
                        retry_count += 1
                        await _set_retry_count(redis_client, fingerprint, retry_count)
                        logger.info(
                            f"Retrying failed event from {original_topic}, attempt {retry_count}/{MAX_RETRIES}"
                        )
                        # The backoff used to be `await asyncio.sleep(2 ** n)`
                        # inside this loop, so every dead letter waited behind
                        # the one before it -- a single worker draining at most
                        # 0.5 messages a second against a queue of 96,874.
                        # The republish is now scheduled with its own delay and
                        # the loop keeps consuming.
                        safe_create_task(
                            _republish_after_backoff(producer, original_topic, raw_data, retry_count),
                            name=f"dlq-retry-{original_topic}",
                        )
                        continue

                    # 2. Save to PostgreSQL (permanently failed or couldn't retry)
                    permanently_failed = (retry_count >= MAX_RETRIES) or is_poison_pill
                    try:
                        await db.execute("""
                            INSERT INTO failed_events (original_topic, error_message, raw_payload, retry_count, permanently_failed)
                            VALUES ($1, $2, $3, $4, $5)
                        """, original_topic, error_msg, json.dumps(raw_data), retry_count, permanently_failed)
                        batch_logger.add(category=f"{original_topic}_perm={permanently_failed}")
                    except Exception as e:
                        logger.error(f"FATAL: Could not save to DLQ database: {e}. Terminating worker.")
                        try:
                            await _send_telegram_alert(session, original_topic, f"FATAL DLQ WORKER DB FAILURE: {e}")
                        except Exception as alert_err:
                            logger.debug(f"Telegram alert failed during fatal DB error (notification lost): {alert_err}")
                        sys.exit(1)
 
                    # 3. Rate-Limited Admin Alert
                    now = time.time()
                    if permanently_failed and (now - last_alert_time >= ALERT_COOLDOWN_SECONDS):
                        last_alert_time = now
                        safe_create_task(_send_telegram_alert(session, original_topic, error_msg))
                    
            await consumer.commit()  # Commit offsets after processing the batch

        except KeyboardInterrupt:
            break
        except StopIteration:
            continue
        except Exception as e:
            logger.error(f"DLQ Processor encountered error: {e}", exc_info=True)
            await asyncio.sleep(5) # Backoff before retrying

async def main():
    logger.info("=" * 60)
    logger.info("SENTINEL  DLQ Worker Service (with Retry)")
    logger.info("=" * 60)

    db = await get_timescale()
    redis_client = await get_redis()
    
    producer = SentinelProducer()
    await producer.start()

    consumer = SentinelConsumer(
        topics=[Topics.DLQ],
        group_id="dlq-worker",
        auto_offset_reset="earliest", # Always process from the beginning of failures
    )
    await consumer.start()

    # §1.1 Universal heartbeat — silent DLQ death is catastrophic
    hb_task = safe_create_task(start_heartbeat_task(redis_client, "dlq-worker"))
    
    connector = aiohttp.TCPConnector(limit=5)
    
    try:
        async with aiohttp.ClientSession(connector=connector) as session:
            await _consume_loop(consumer, db, session, producer, redis_client)
    finally:
        hb_task.cancel()
        await producer.close()
        await consumer.close()

if __name__ == "__main__":
    # OS-level event loop policy enforcement to prevent Windows Proactor loop crashes
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())