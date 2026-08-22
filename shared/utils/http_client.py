"""
shared/utils/http_client.py

Guarded HTTP access to external data sources.

Collectors previously called `aiohttp` directly at 20+ sites. Each applied a
timeout, which bounds a single request, but nothing bounded the *sequence*: a
polling loop against a degraded upstream keeps issuing requests at full rate,
which is how a free tier becomes a blocked tier.

This wraps three concerns that were previously either absent or duplicated:

  - a circuit breaker per upstream, shared across every call site for that host
  - throughput and failure counters, attributed to the calling service
  - a default timeout, so a call site cannot forget one

Use it wherever a collector reaches outside the cluster:

    from shared.utils.http_client import guarded_get

    payload = await guarded_get(
        "https://api.example.com/v1/quotes",
        source="example",
        service="collector-tradfi",
    )
    if payload is None:
        return          # already counted and logged; nothing to do
"""

import asyncio
import logging
from typing import Any, Dict, Optional

import aiohttp

from shared.utils.circuit_breaker import CircuitOpenError, get_breaker
from shared.utils.metrics import MetricsCollector

logger = logging.getLogger("sentinel.http")

DEFAULT_TIMEOUT_SEC = 10.0

# Free public APIs are unhappy with unidentified clients, and several
# (SEC EDGAR in particular) require a contactable agent string.
DEFAULT_HEADERS = {
    "User-Agent": "Sentinel-Intelligence-Platform/1.0 (self-hosted research)",
    "Accept": "application/json",
}


async def guarded_request(
    method: str,
    url: str,
    *,
    source: str,
    service: str = "unknown",
    session: Optional[aiohttp.ClientSession] = None,
    timeout: float = DEFAULT_TIMEOUT_SEC,
    headers: Optional[Dict[str, str]] = None,
    expect_json: bool = True,
    **kwargs: Any,
) -> Optional[Any]:
    """Perform an HTTP request behind a circuit breaker.

    Returns the decoded body, or ``None`` when the request failed or was
    refused. Returning None rather than raising is deliberate: a collector loop
    should skip this cycle and continue, and every failure mode is already
    counted here, so call sites do not each need their own try/except.

    Args:
        source: Upstream identity ("yahoo", "coinbase", "sec"). Call sites
            hitting the same upstream must use the same value, or each gets its
            own breaker and none of them ever trips.
        service: The calling service, used to attribute metrics.
    """
    breaker = get_breaker(source)
    own_session = session is None

    async def _do() -> Any:
        nonlocal session
        if own_session:
            session = aiohttp.ClientSession(
                headers={**DEFAULT_HEADERS, **(headers or {})},
                timeout=aiohttp.ClientTimeout(total=timeout),
            )
        try:
            async with session.request(
                method,
                url,
                timeout=aiohttp.ClientTimeout(total=timeout),
                headers={**DEFAULT_HEADERS, **(headers or {})} if not own_session else None,
                **kwargs,
            ) as resp:
                if resp.status >= 500:
                    # Server-side failure: the upstream is unwell, so it counts
                    # toward opening the circuit.
                    raise aiohttp.ClientResponseError(
                        resp.request_info, resp.history,
                        status=resp.status, message=f"upstream {resp.status}",
                    )
                if resp.status == 429:
                    # Rate limited. Explicitly a signal to back off, so it also
                    # counts toward the breaker.
                    raise aiohttp.ClientResponseError(
                        resp.request_info, resp.history,
                        status=429, message="rate limited",
                    )
                if resp.status >= 400:
                    # Client-side (404, 401): our request is wrong, not the
                    # upstream's health. Recorded, but must not trip the circuit
                    # or one bad symbol would disable an entire source.
                    MetricsCollector.increment(f"http_client_errors:{service}:{source}")
                    logger.warning("HTTP %s from %s for %s", resp.status, source, url)
                    return None
                # content_type=None: several sources return JSON labelled as
                # text/plain, which aiohttp otherwise refuses to decode.
                return await (resp.json(content_type=None) if expect_json else resp.text())
        finally:
            if own_session and session is not None:
                await session.close()

    try:
        result = await breaker.call(_do)
        MetricsCollector.increment(f"http_success_total:{service}:{source}")
        return result

    except CircuitOpenError as e:
        # Not attempted. Distinct from a failure, and deliberately quiet: this
        # fires on every cycle while the circuit is open, and logging it at
        # warning would bury the original cause.
        MetricsCollector.increment(f"http_circuit_open_skips:{service}:{source}")
        logger.debug("Skipped %s: circuit open, retry in %.0fs", source, e.retry_after)
        return None

    except asyncio.TimeoutError:
        MetricsCollector.increment(f"http_timeouts_total:{service}:{source}")
        logger.warning("Timeout after %.0fs calling %s (%s)", timeout, source, url)
        return None

    except Exception as e:
        MetricsCollector.increment(f"http_failures_total:{service}:{source}")
        logger.warning("Request to %s failed: %s: %s", source, type(e).__name__, e)
        return None


async def guarded_get(url: str, **kwargs: Any) -> Optional[Any]:
    """GET behind a circuit breaker. See `guarded_request`."""
    return await guarded_request("GET", url, **kwargs)


async def guarded_post(url: str, **kwargs: Any) -> Optional[Any]:
    """POST behind a circuit breaker. See `guarded_request`."""
    return await guarded_request("POST", url, **kwargs)
