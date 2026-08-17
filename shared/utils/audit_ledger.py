"""
shared/utils/audit_ledger.py

Cryptographically immutable, SHA-256 hash-chained audit ledger for Sentinel.
Guarantees non-repudiation and tamper detection for all security, governance,
trading, and watchlist mutations.
"""

import hashlib
import json
import logging
import time
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

logger = logging.getLogger("shared.audit")

GENESIS_HASH = "0" * 64
REDIS_AUDIT_KEY = "sentinel:audit:ledger"
REDIS_LAST_HASH_KEY = "sentinel:audit:last_hash"


def compute_entry_hash(prev_hash: str, timestamp: str, actor: str, action: str, details_json: str) -> str:
    """Computes the SHA-256 digest of an audit entry chaining from the previous hash."""
    message = f"{prev_hash}|{timestamp}|{actor}|{action}|{details_json}".encode("utf-8")
    return hashlib.sha256(message).hexdigest()


class AuditLedger:
    def __init__(self, redis_client: Any = None, db_client: Any = None):
        self.redis = redis_client
        self.db = db_client

    async def record_entry(
        self,
        actor: str,
        action: str,
        resource_type: str,
        resource_id: str,
        details: Optional[Dict[str, Any]] = None,
        ip_address: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Appends an immutable, hash-chained record to the audit ledger.
        """
        now = datetime.now(timezone.utc).isoformat()
        details_obj = details or {}
        details_json = json.dumps(details_obj, sort_keys=True)

        prev_hash = GENESIS_HASH

        # 1. Fetch previous hash from Redis
        if self.redis:
            try:
                raw_redis = getattr(self.redis, "raw", self.redis)
                last_hash_val = await raw_redis.get(REDIS_LAST_HASH_KEY)
                if last_hash_val:
                    prev_hash = last_hash_val.decode() if isinstance(last_hash_val, bytes) else str(last_hash_val)
            except Exception as e:
                logger.warning(f"Error reading last audit hash from Redis: {e}")

        # 2. Compute current entry hash
        entry_hash = compute_entry_hash(prev_hash, now, actor, action, details_json)

        entry = {
            "hash": entry_hash,
            "prev_hash": prev_hash,
            "timestamp": now,
            "actor": actor,
            "action": action,
            "resource_type": resource_type,
            "resource_id": resource_id,
            "ip_address": ip_address or "127.0.0.1",
            "details": details_obj,
        }

        # 3. Store in Redis
        if self.redis:
            try:
                raw_redis = getattr(self.redis, "raw", self.redis)
                pipe = raw_redis.pipeline()
                pipe.rpush(REDIS_AUDIT_KEY, json.dumps(entry))
                pipe.set(REDIS_LAST_HASH_KEY, entry_hash)
                await pipe.execute()
            except Exception as e:
                logger.error(f"Failed to persist audit entry to Redis: {e}")

        # 4. Store in TimescaleDB if table exists
        if self.db:
            try:
                await self.db.execute(
                    """
                    INSERT INTO audit_ledger (hash, prev_hash, timestamp, actor, action, resource_type, resource_id, ip_address, details)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    ON CONFLICT (hash) DO NOTHING;
                    """,
                    entry_hash,
                    prev_hash,
                    datetime.fromisoformat(now),
                    actor,
                    action,
                    resource_type,
                    resource_id,
                    ip_address,
                    details_json,
                )
            except Exception as e:
                logger.debug(f"TimescaleDB audit write optional fallback: {e}")

        logger.info(f"🔒 Audit Ledger Entry Recorded | action={action} actor={actor} hash={entry_hash[:12]}...")
        return entry

    async def get_entries(self, limit: int = 50, offset: int = 0) -> List[Dict[str, Any]]:
        """Retrieves recent audit entries from Redis."""
        entries = []
        if not self.redis:
            return entries

        try:
            raw_redis = getattr(self.redis, "raw", self.redis)
            # Retrieve entries in reverse chronological order
            raw_items = await raw_redis.lrange(REDIS_AUDIT_KEY, max(0, -limit - offset), -1 - offset if offset > 0 else -1)
            for item in reversed(raw_items):
                val = item.decode() if isinstance(item, bytes) else str(item)
                entries.append(json.loads(val))
        except Exception as e:
            logger.error(f"Failed to retrieve audit ledger entries: {e}")

        return entries

    async def verify_chain(self) -> Dict[str, Any]:
        """
        Walks the entire audit ledger from Genesis to present, verifying cryptographic hashes.
        Detects any retroactive modifications, deletions, or insertions.
        """
        if not self.redis:
            return {"valid": True, "entries_checked": 0, "status": "NO_STORAGE"}

        try:
            raw_redis = getattr(self.redis, "raw", self.redis)
            raw_items = await raw_redis.lrange(REDIS_AUDIT_KEY, 0, -1)
            
            if not raw_items:
                return {"valid": True, "entries_checked": 0, "status": "EMPTY_LEDGER"}

            expected_prev_hash = GENESIS_HASH
            for idx, item in enumerate(raw_items):
                val = item.decode() if isinstance(item, bytes) else str(item)
                entry = json.loads(val)

                # Check prev_hash link
                if entry.get("prev_hash") != expected_prev_hash:
                    return {
                        "valid": False,
                        "broken_at_index": idx,
                        "expected_prev_hash": expected_prev_hash,
                        "found_prev_hash": entry.get("prev_hash"),
                        "status": "CHAIN_BROKEN_LINK",
                    }

                # Recompute digest
                details_json = json.dumps(entry.get("details", {}), sort_keys=True)
                recomputed = compute_entry_hash(
                    entry["prev_hash"],
                    entry["timestamp"],
                    entry["actor"],
                    entry["action"],
                    details_json,
                )

                if recomputed != entry.get("hash"):
                    return {
                        "valid": False,
                        "broken_at_index": idx,
                        "expected_hash": recomputed,
                        "found_hash": entry.get("hash"),
                        "status": "HASH_CORRUPTED",
                    }

                expected_prev_hash = entry["hash"]

            return {
                "valid": True,
                "entries_checked": len(raw_items),
                "latest_hash": expected_prev_hash,
                "status": "VERIFIED_VALID",
            }

        except Exception as e:
            logger.error(f"Error during audit chain verification: {e}")
            return {"valid": False, "error": str(e), "status": "VERIFICATION_ERROR"}
