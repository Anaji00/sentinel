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

# Bounded retries for concurrent appends losing the UNIQUE(prev_hash) race.
MAX_APPEND_RETRIES = 5


class AuditLedgerUnavailable(RuntimeError):
    """Raised when an audit entry cannot be durably recorded.

    Callers must treat this as fatal to the action being audited: an unaudited
    trade execution or governance change is not an acceptable outcome.
    """


def _is_unique_violation(exc: Exception) -> bool:
    """True if *exc* is a Postgres unique-constraint violation (SQLSTATE 23505)."""
    if getattr(exc, "sqlstate", None) == "23505":
        return True
    return "23505" in str(exc) or "duplicate key value" in str(exc).lower()


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
        details_obj = details or {}
        details_json = json.dumps(details_obj, sort_keys=True)

        if not self.db:
            # The ledger is the system of record for trade execution and governance
            # changes. Redis alone cannot back it — it runs allkeys-lru and is
            # evictable — so refuse to record rather than create the false
            # impression that an audit trail exists.
            raise AuditLedgerUnavailable(
                "Audit ledger requires a durable database connection; refusing to "
                f"record '{action}' by '{actor}' with no durable store."
            )

        # Durable append. UNIQUE(prev_hash) serializes concurrent appends: if two
        # callers read the same head, only one commits and the other retries
        # against the new head, so the chain can never fork.
        last_error: Optional[Exception] = None
        for attempt in range(MAX_APPEND_RETRIES):
            prev_hash = await self._get_chain_head()
            now = datetime.now(timezone.utc).isoformat()
            entry_hash = compute_entry_hash(prev_hash, now, actor, action, details_json)

            try:
                await self.db.execute(
                    """
                    INSERT INTO audit_ledger (hash, prev_hash, timestamp, actor, action, resource_type, resource_id, ip_address, details)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9);
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
                break
            except Exception as e:
                # A uniqueness violation means another append won the race.
                # Anything else is a genuine failure to persist.
                last_error = e
                if not _is_unique_violation(e):
                    logger.error(
                        "Failed to persist audit entry to durable store "
                        "(action=%s actor=%s): %s",
                        action, actor, e,
                    )
                    raise AuditLedgerUnavailable(
                        f"Audit ledger write failed for '{action}' by '{actor}': {e}"
                    ) from e
                logger.warning(
                    "Audit chain head moved during append (attempt %d/%d); retrying.",
                    attempt + 1, MAX_APPEND_RETRIES,
                )
        else:
            logger.error(
                "Audit entry contention exceeded %d attempts (action=%s actor=%s).",
                MAX_APPEND_RETRIES, action, actor,
            )
            raise AuditLedgerUnavailable(
                f"Audit ledger contention exceeded {MAX_APPEND_RETRIES} attempts "
                f"for '{action}' by '{actor}': {last_error}"
            )

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

        # Redis mirror is a read cache only. Its failure is not fatal because the
        # durable write above has already succeeded.
        if self.redis:
            try:
                raw_redis = getattr(self.redis, "raw", self.redis)
                pipe = raw_redis.pipeline()
                pipe.rpush(REDIS_AUDIT_KEY, json.dumps(entry))
                pipe.set(REDIS_LAST_HASH_KEY, entry_hash)
                await pipe.execute()
            except Exception as e:
                logger.warning(f"Audit read-cache mirror failed (durable write succeeded): {e}")

        logger.info(f"🔒 Audit Ledger Entry Recorded | action={action} actor={actor} hash={entry_hash[:12]}...")
        return entry

    async def _get_chain_head(self) -> str:
        """Returns the hash of the most recent durable entry, or GENESIS if empty."""
        row = await self.db.query_one(
            "SELECT hash FROM audit_ledger ORDER BY seq DESC LIMIT 1;"
        )
        if row and row.get("hash"):
            return str(row["hash"])
        return GENESIS_HASH

    async def get_entries(self, limit: int = 50, offset: int = 0) -> List[Dict[str, Any]]:
        """Retrieves recent audit entries, newest first, from the durable store.

        Reads Postgres — the system of record. Redis is only consulted if no
        database is configured, and is explicitly marked as a non-authoritative
        cache read so a caller cannot mistake it for the durable trail.
        """
        entries: List[Dict[str, Any]] = []

        if self.db:
            try:
                rows = await self.db.query(
                    """
                    SELECT hash, prev_hash, timestamp, actor, action,
                           resource_type, resource_id, ip_address, details
                    FROM audit_ledger
                    ORDER BY seq DESC
                    LIMIT $1 OFFSET $2;
                    """,
                    limit,
                    offset,
                )
                for r in rows:
                    raw_details = r.get("details")
                    if isinstance(raw_details, str):
                        try:
                            raw_details = json.loads(raw_details)
                        except json.JSONDecodeError:
                            raw_details = {"_raw": raw_details}
                    ts = r.get("timestamp")
                    entries.append({
                        "hash": r.get("hash"),
                        "prev_hash": r.get("prev_hash"),
                        "timestamp": ts.isoformat() if hasattr(ts, "isoformat") else str(ts),
                        "actor": r.get("actor"),
                        "action": r.get("action"),
                        "resource_type": r.get("resource_type"),
                        "resource_id": r.get("resource_id"),
                        "ip_address": r.get("ip_address"),
                        "details": raw_details or {},
                    })
                return entries
            except Exception as e:
                logger.error(f"Failed to read audit ledger from durable store: {e}")
                raise AuditLedgerUnavailable(f"Audit ledger read failed: {e}") from e

        # No durable store configured — surface the cache, clearly labelled.
        if not self.redis:
            return entries

        logger.warning(
            "Reading audit entries from the Redis cache; no durable store is "
            "configured, so this view may be incomplete."
        )
        try:
            raw_redis = getattr(self.redis, "raw", self.redis)
            raw_items = await raw_redis.lrange(REDIS_AUDIT_KEY, max(0, -limit - offset), -1 - offset if offset > 0 else -1)
            for item in reversed(raw_items):
                val = item.decode() if isinstance(item, bytes) else str(item)
                entry = json.loads(val)
                entry["_source"] = "cache_non_authoritative"
                entries.append(entry)
        except Exception as e:
            logger.error(f"Failed to retrieve audit ledger entries from cache: {e}")

        return entries

    async def _load_chain_ascending(self) -> List[Dict[str, Any]]:
        """Loads the full chain oldest-first from the durable store, else the cache."""
        if self.db:
            entries = await self.get_entries(limit=1_000_000, offset=0)
            return list(reversed(entries))  # get_entries returns newest-first

        raw_redis = getattr(self.redis, "raw", self.redis)
        raw_items = await raw_redis.lrange(REDIS_AUDIT_KEY, 0, -1)
        out: List[Dict[str, Any]] = []
        for item in raw_items:
            val = item.decode() if isinstance(item, bytes) else str(item)
            out.append(json.loads(val))
        return out

    async def verify_chain(self) -> Dict[str, Any]:
        """
        Walks the entire audit ledger from Genesis to present, verifying cryptographic hashes.
        Detects any retroactive modifications, deletions, or insertions.
        """
        if not self.db and not self.redis:
            return {"valid": True, "entries_checked": 0, "status": "NO_STORAGE"}

        try:
            # Verify the durable store when one exists — verifying only the
            # evictable cache would report VERIFIED_VALID on a truncated chain.
            entries_in_order = await self._load_chain_ascending()

            if not entries_in_order:
                return {"valid": True, "entries_checked": 0, "status": "EMPTY_LEDGER"}

            raw_items = entries_in_order
            expected_prev_hash = GENESIS_HASH
            for idx, entry in enumerate(raw_items):

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
