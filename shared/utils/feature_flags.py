"""
shared/utils/feature_flags.py

Enterprise-grade Redis-backed Feature Flags & Emergency Kill Switch Manager.
Enables instant platform-wide disabling or gradual rollouts of specific signal types
(covered calls, Granger causality, Hawkes contagion, insider flow, etc.) without redeploying.
"""

import hashlib
import json
import logging
import time
from typing import Any, Dict, List, Optional, Set

logger = logging.getLogger("shared.feature_flags")


def rollout_bucket(flag_name: str, subject: str) -> int:
    """Maps (flag, subject) to a stable bucket in [0, 100).

    Uses SHA-256 rather than the builtin ``hash()``: Python randomizes string
    hashing per process unless PYTHONHASHSEED is pinned, which would place the
    same ticker in different buckets across workers and re-roll it on every
    restart — making a canary unobservable and a rollout irreproducible.
    """
    digest = hashlib.sha256(f"{flag_name}:{subject}".encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") % 100

DEFAULT_SIGNAL_FLAGS = {
    "order_execution": {
        "description": "Broker order submission. Halting this stops trade execution "
                       "at the gateway, independently of signal generation.",
        "enabled": True,
        "rollout_pct": 100.0,
        "enabled_tickers": [],
        "kill_switched": False,
        "reason": "Default active",
        "updated_at": "2026-01-01T00:00:00Z",
        "updated_by": "system",
    },
    "covered_calls": {
        "description": "Closed-form covered call overlay recommendation engine",
        "enabled": True,
        "rollout_pct": 100.0,
        "enabled_tickers": [],
        "kill_switched": False,
        "reason": "Default active",
        "updated_at": "2026-01-01T00:00:00Z",
        "updated_by": "system",
    },
    "granger_causality": {
        "description": "Directional Granger causality discovery and graph edge proposals",
        "enabled": True,
        "rollout_pct": 100.0,
        "enabled_tickers": [],
        "kill_switched": False,
        "reason": "Default active",
        "updated_at": "2026-01-01T00:00:00Z",
        "updated_by": "system",
    },
    "hawkes_contagion": {
        "description": "Cross-domain and intra-sector Hawkes point process excitation boosting",
        "enabled": True,
        "rollout_pct": 100.0,
        "enabled_tickers": [],
        "kill_switched": False,
        "reason": "Default active",
        "updated_at": "2026-01-01T00:00:00Z",
        "updated_by": "system",
    },
    "insider_clustering": {
        "description": "SEC Form 4 C-suite accumulation and insider flow tracking",
        "enabled": True,
        "rollout_pct": 100.0,
        "enabled_tickers": [],
        "kill_switched": False,
        "reason": "Default active",
        "updated_at": "2026-01-01T00:00:00Z",
        "updated_by": "system",
    },
    "crypto_liquidations": {
        "description": "High-frequency perpetual contract liquidation cascade detection",
        "enabled": True,
        "rollout_pct": 100.0,
        "enabled_tickers": [],
        "kill_switched": False,
        "reason": "Default active",
        "updated_at": "2026-01-01T00:00:00Z",
        "updated_by": "system",
    },
    "macro_releases": {
        "description": "Scheduled economic calendar release ingestion and surprise signals",
        "enabled": True,
        "rollout_pct": 100.0,
        "enabled_tickers": [],
        "kill_switched": False,
        "reason": "Default active",
        "updated_at": "2026-01-01T00:00:00Z",
        "updated_by": "system",
    },
    "peer_discovery": {
        "description": "Quant peer discovery and structural catalyst classification",
        "enabled": True,
        "rollout_pct": 100.0,
        "enabled_tickers": [],
        "kill_switched": False,
        "reason": "Default active",
        "updated_by": "system",
    },
    "black_litterman": {
        "description": "Black-Litterman multi-asset portfolio equilibrium allocation",
        "enabled": True,
        "rollout_pct": 100.0,
        "enabled_tickers": [],
        "kill_switched": False,
        "reason": "Default active",
        "updated_at": "2026-01-01T00:00:00Z",
        "updated_by": "system",
    },
}


class FeatureFlagManager:
    """
    Manages platform-wide signal feature flags and instantaneous kill switches.
    Features:
      - 5-second in-memory cache with Redis fallback
      - Deterministic hash-based gradual rollout percentage (0-100%)
      - Ticker-specific whitelist targeting
      - Platform-wide master kill switch and per-flag emergency trips
    """

    def __init__(self, redis_client=None):
        self.redis = redis_client
        self._local_cache: Dict[str, Dict[str, Any]] = {}
        self._cache_ts: float = 0.0
        self._cache_ttl: float = 5.0  # 5 seconds local cache
        self._master_kill_cache: Optional[Dict[str, Any]] = None

    async def _refresh_cache_if_needed(self):
        now = time.time()
        if (now - self._cache_ts) < self._cache_ttl and self._local_cache:
            return

        if not self.redis:
            if not self._local_cache:
                self._local_cache = {k: dict(v) for k, v in DEFAULT_SIGNAL_FLAGS.items()}
            return

        try:
            # Check master kill switch
            master_raw = await self.redis.raw.get("sentinel:flags:master_kill_switch")
            if master_raw:
                self._master_kill_cache = json.loads(master_raw if isinstance(master_raw, str) else master_raw.decode("utf-8"))
            else:
                self._master_kill_cache = {"active": False, "reason": ""}

            # Check individual signal flags
            flags: Dict[str, Dict[str, Any]] = {}
            for flag_name, default_val in DEFAULT_SIGNAL_FLAGS.items():
                raw = await self.redis.raw.get(f"sentinel:flags:signal_types:{flag_name}")
                if raw:
                    flags[flag_name] = json.loads(raw if isinstance(raw, str) else raw.decode("utf-8"))
                else:
                    flags[flag_name] = dict(default_val)

            self._local_cache = flags
            self._cache_ts = now
        except Exception as e:
            logger.debug(f"Redis feature flag cache refresh fallback: {e}")
            if not self._local_cache:
                self._local_cache = {k: dict(v) for k, v in DEFAULT_SIGNAL_FLAGS.items()}

    async def is_enabled(
        self,
        flag_name: str,
        ticker: Optional[str] = None,
        user_id: Optional[str] = None,
    ) -> bool:
        """
        Evaluates whether a specific signal or feature is currently enabled.
        """
        await self._refresh_cache_if_needed()

        # 1. Master Kill Switch Check
        if self._master_kill_cache and self._master_kill_cache.get("active", False):
            return False

        # 2. Flag Existence Check
        flag = self._local_cache.get(flag_name)
        if not flag:
            flag = DEFAULT_SIGNAL_FLAGS.get(flag_name, {"enabled": True, "kill_switched": False, "rollout_pct": 100.0})

        # 3. Emergency Kill Switch Check
        if flag.get("kill_switched", False) or not flag.get("enabled", True):
            return False

        # 4. Ticker Whitelist Check (if defined)
        enabled_tickers = flag.get("enabled_tickers", [])
        if enabled_tickers and ticker:
            ticker_clean = ticker.strip().upper()
            if ticker_clean not in {t.strip().upper() for t in enabled_tickers}:
                return False

        # 5. Gradual Rollout Percentage Check
        rollout_pct = float(flag.get("rollout_pct", 100.0))
        if rollout_pct < 100.0:
            key_subject = ticker or user_id or flag_name
            if rollout_bucket(flag_name, key_subject) >= rollout_pct:
                return False

        return True

    async def set_flag(
        self,
        flag_name: str,
        enabled: bool,
        rollout_pct: float = 100.0,
        enabled_tickers: Optional[List[str]] = None,
        reason: str = "Operator updated",
        updated_by: str = "operator",
    ) -> Dict[str, Any]:
        """Updates a feature flag configuration and persists to Redis."""
        await self._refresh_cache_if_needed()
        flag = self._local_cache.get(flag_name, dict(DEFAULT_SIGNAL_FLAGS.get(flag_name, {})))
        flag["enabled"] = bool(enabled)
        flag["rollout_pct"] = max(0.0, min(100.0, float(rollout_pct)))
        flag["enabled_tickers"] = enabled_tickers if enabled_tickers is not None else flag.get("enabled_tickers", [])
        flag["reason"] = reason
        flag["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        flag["updated_by"] = updated_by

        self._local_cache[flag_name] = flag
        self._cache_ts = time.time()

        if self.redis:
            try:
                await self.redis.raw.set(f"sentinel:flags:signal_types:{flag_name}", json.dumps(flag))
                await self.redis.raw.publish("sentinel:flags:updates", json.dumps({"flag": flag_name, "action": "set"}))
            except Exception as e:
                logger.error(f"Failed to persist feature flag {flag_name} to Redis: {e}")

        logger.info(f"🚩 Feature flag '{flag_name}' updated: enabled={enabled}, rollout={rollout_pct}%, tickers={enabled_tickers}")
        return flag

    async def trip_kill_switch(
        self,
        flag_name: str,
        reason: str = "Emergency manual kill switch tripped",
        updated_by: str = "operator",
    ) -> Dict[str, Any]:
        """Instantly disables a specific signal source platform-wide."""
        await self._refresh_cache_if_needed()
        flag = self._local_cache.get(flag_name, dict(DEFAULT_SIGNAL_FLAGS.get(flag_name, {})))
        flag["kill_switched"] = True
        flag["enabled"] = False
        flag["reason"] = reason
        flag["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        flag["updated_by"] = updated_by

        self._local_cache[flag_name] = flag
        self._cache_ts = time.time()

        if self.redis:
            try:
                await self.redis.raw.set(f"sentinel:flags:signal_types:{flag_name}", json.dumps(flag))
                await self.redis.raw.publish("sentinel:flags:updates", json.dumps({"flag": flag_name, "action": "kill"}))
            except Exception as e:
                logger.error(f"Failed to persist kill switch for {flag_name} to Redis: {e}")

        logger.warning(f"🛑 KILL SWITCH TRIPPED for signal '{flag_name}' | Reason: {reason} | By: {updated_by}")
        return flag

    async def trip_master_kill_switch(
        self,
        reason: str = "Platform-wide emergency shutdown",
        updated_by: str = "operator",
    ) -> Dict[str, Any]:
        """Instantly halts ALL automated signals across the entire platform."""
        state = {
            "active": True,
            "reason": reason,
            "tripped_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "tripped_by": updated_by,
        }
        self._master_kill_cache = state
        self._cache_ts = time.time()

        if self.redis:
            try:
                await self.redis.raw.set("sentinel:flags:master_kill_switch", json.dumps(state))
                await self.redis.raw.publish("sentinel:flags:updates", json.dumps({"flag": "MASTER", "action": "kill"}))
            except Exception as e:
                logger.error(f"Failed to persist master kill switch to Redis: {e}")

        logger.critical(f"🚨 MASTER KILL SWITCH ACTIVATED | Reason: {reason} | By: {updated_by}")
        return state

    async def reset_kill_switch(
        self,
        flag_name: str,
        updated_by: str = "operator",
    ) -> Dict[str, Any]:
        """Resets a signal's kill switch back to enabled status."""
        if flag_name.upper() == "MASTER":
            state = {"active": False, "reason": "Reset to normal", "updated_by": updated_by}
            self._master_kill_cache = state
            if self.redis:
                try:
                    await self.redis.raw.set("sentinel:flags:master_kill_switch", json.dumps(state))
                    await self.redis.raw.publish("sentinel:flags:updates", json.dumps({"flag": "MASTER", "action": "reset"}))
                except Exception as e:
                    logger.error(f"Failed to reset master kill switch: {e}")
            return state

        await self._refresh_cache_if_needed()
        flag = self._local_cache.get(flag_name, dict(DEFAULT_SIGNAL_FLAGS.get(flag_name, {})))
        flag["kill_switched"] = False
        flag["enabled"] = True
        flag["reason"] = "Kill switch reset to normal"
        flag["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        flag["updated_by"] = updated_by

        self._local_cache[flag_name] = flag
        self._cache_ts = time.time()

        if self.redis:
            try:
                await self.redis.raw.set(f"sentinel:flags:signal_types:{flag_name}", json.dumps(flag))
                await self.redis.raw.publish("sentinel:flags:updates", json.dumps({"flag": flag_name, "action": "reset"}))
            except Exception as e:
                logger.error(f"Failed to reset kill switch for {flag_name}: {e}")

        logger.info(f"✅ Kill switch RESET for signal '{flag_name}' | By: {updated_by}")
        return flag

    async def get_all_flags(self) -> Dict[str, Any]:
        """Returns the dictionary of all signal flags and master kill switch status."""
        await self._refresh_cache_if_needed()
        return {
            "master_kill_switch": self._master_kill_cache or {"active": False, "reason": ""},
            "signals": self._local_cache or {k: dict(v) for k, v in DEFAULT_SIGNAL_FLAGS.items()},
        }
