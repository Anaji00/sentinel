"""
shared/utils/circuit_breaker.py

Circuit breaker for external data sources.

Every Sentinel collector polls a free upstream -- Yahoo Finance, Coinbase, SEC
EDGAR, OpenSky. Timeouts are applied consistently, but a timeout only bounds a
single call. When an upstream degrades rather than fails outright, a polling
loop keeps issuing requests at full rate against a service that is already
struggling, which is how a free tier becomes a blocked tier.

The breaker converts repeated failure into backoff:

    CLOSED     normal. Failures are counted; `failure_threshold` consecutive
               failures open the circuit.
    OPEN       calls are refused immediately without touching the network,
               for `recovery_timeout` seconds.
    HALF_OPEN  one trial call is admitted. Success closes the circuit; failure
               reopens it with the timeout doubled, up to `max_recovery_timeout`.

The doubling matters: an upstream that is down for an hour should not be probed
every thirty seconds for that hour.
"""

import asyncio
import logging
import time
from enum import Enum
from typing import Any, Awaitable, Callable, Dict, Optional, TypeVar

logger = logging.getLogger("sentinel.circuit_breaker")

T = TypeVar("T")


class CircuitState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitOpenError(RuntimeError):
    """Raised when a call is refused because the circuit is open.

    Distinct from the upstream's own errors so callers can tell "we did not
    try" from "we tried and it failed" -- they warrant different log levels and
    different metrics.
    """

    def __init__(self, name: str, retry_after: float):
        self.name = name
        self.retry_after = retry_after
        super().__init__(f"circuit '{name}' is open; retry in {retry_after:.0f}s")


class CircuitBreaker:
    """Async circuit breaker around one external dependency.

        breaker = CircuitBreaker("yahoo", failure_threshold=5)
        try:
            data = await breaker.call(fetch_quote, "NVDA")
        except CircuitOpenError:
            metrics.upstream_error("yahoo")   # skipped, not attempted
    """

    def __init__(
        self,
        name: str,
        *,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        max_recovery_timeout: float = 900.0,
        success_threshold: int = 1,
    ):
        self.name = name
        self.failure_threshold = max(1, failure_threshold)
        # Guard against a nonsensical timeout (zero or negative would make the
        # circuit reopen instantly), without imposing a policy minimum: a
        # sub-second recovery is legitimate for a fast local dependency.
        self.base_recovery_timeout = max(0.01, recovery_timeout)
        self.max_recovery_timeout = max(self.base_recovery_timeout, max_recovery_timeout)
        self.success_threshold = max(1, success_threshold)

        self._state = CircuitState.CLOSED
        self._failures = 0
        self._successes = 0
        self._opened_at = 0.0
        self._current_timeout = self.base_recovery_timeout
        self._lock = asyncio.Lock()

        # Cumulative, for reporting.
        self.total_calls = 0
        self.total_failures = 0
        self.total_rejections = 0

    # ── state ────────────────────────────────────────────────────────────────

    @property
    def state(self) -> CircuitState:
        return self._state

    @property
    def retry_after(self) -> float:
        """Seconds until the next trial call is admitted (0 when not open)."""
        if self._state is not CircuitState.OPEN:
            return 0.0
        return max(0.0, self._current_timeout - (time.monotonic() - self._opened_at))

    def _should_attempt(self) -> bool:
        if self._state is CircuitState.CLOSED:
            return True
        if self._state is CircuitState.OPEN:
            if time.monotonic() - self._opened_at >= self._current_timeout:
                self._state = CircuitState.HALF_OPEN
                self._successes = 0
                logger.info("Circuit '%s' half-open; admitting a trial call.", self.name)
                return True
            return False
        return True  # HALF_OPEN admits the trial

    # ── outcomes ─────────────────────────────────────────────────────────────

    async def record_success(self) -> None:
        async with self._lock:
            if self._state is CircuitState.HALF_OPEN:
                self._successes += 1
                if self._successes >= self.success_threshold:
                    logger.info("Circuit '%s' closed; upstream recovered.", self.name)
                    self._state = CircuitState.CLOSED
                    self._failures = 0
                    # Reset backoff only on genuine recovery, so a flapping
                    # upstream does not get probed aggressively forever.
                    self._current_timeout = self.base_recovery_timeout
            else:
                self._failures = 0

    async def record_failure(self, exc: Optional[BaseException] = None) -> None:
        async with self._lock:
            self.total_failures += 1

            if self._state is CircuitState.HALF_OPEN:
                # The trial failed: reopen with a longer timeout.
                self._current_timeout = min(self._current_timeout * 2, self.max_recovery_timeout)
                self._state = CircuitState.OPEN
                self._opened_at = time.monotonic()
                logger.warning(
                    "Circuit '%s' reopened after failed trial; next attempt in %.0fs (%s)",
                    self.name, self._current_timeout, exc or "no detail",
                )
                return

            self._failures += 1
            if self._failures >= self.failure_threshold and self._state is CircuitState.CLOSED:
                self._state = CircuitState.OPEN
                self._opened_at = time.monotonic()
                logger.warning(
                    "Circuit '%s' opened after %d consecutive failures; "
                    "pausing calls for %.0fs (%s)",
                    self.name, self._failures, self._current_timeout, exc or "no detail",
                )

    # ── invocation ───────────────────────────────────────────────────────────

    async def call(self, fn: Callable[..., Awaitable[T]], *args: Any, **kwargs: Any) -> T:
        """Invoke *fn*, or raise CircuitOpenError without invoking it."""
        async with self._lock:
            self.total_calls += 1
            if not self._should_attempt():
                self.total_rejections += 1
                raise CircuitOpenError(self.name, self.retry_after)

        try:
            result = await fn(*args, **kwargs)
        except Exception as e:
            await self.record_failure(e)
            raise
        await self.record_success()
        return result

    def snapshot(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "state": self._state.value,
            "consecutive_failures": self._failures,
            "retry_after_sec": round(self.retry_after, 1),
            "total_calls": self.total_calls,
            "total_failures": self.total_failures,
            "total_rejections": self.total_rejections,
        }


_BREAKERS: Dict[str, CircuitBreaker] = {}


def get_breaker(name: str, **kwargs: Any) -> CircuitBreaker:
    """Process-wide breaker for *name*, created on first use.

    Shared per source so every call site polling the same upstream trips the
    same circuit -- a breaker per call site would defeat the purpose.
    """
    if name not in _BREAKERS:
        _BREAKERS[name] = CircuitBreaker(name, **kwargs)
    return _BREAKERS[name]


def all_breakers() -> Dict[str, Dict[str, Any]]:
    """Snapshot of every breaker, for health endpoints."""
    return {name: b.snapshot() for name, b in _BREAKERS.items()}
