"""
shared/utils/metrics.py

SENTINEL UNIFIED METRICS & OBSERVABILITY ENGINE
================================================
Lightweight Prometheus & JSON metrics collection module for all microservices.

Tracks:
  - Inference latency per agent and model tier
  - Prediction accuracy and Brier calibration scores
  - Pipeline throughput (events enriched / correlated / scenarios synthesized per min)
  - Anomaly distribution across spatial/temporal domains

Exposes data for API gateway health endpoints and monitoring scrapers.
"""

import asyncio
import logging
import os
import time
from collections import defaultdict
from typing import Any, Dict, List, Optional

logger = logging.getLogger("sentinel.metrics")

# Process-local metric stores. These are the write path: recording a metric is a
# synchronous dict update that can be called from anywhere, including code with
# no event loop.
_LATENCY_HISTOGRAMS: Dict[str, List[float]] = defaultdict(list)
_COUNTER_METRICS: Dict[str, int] = defaultdict(int)
_GAUGE_METRICS: Dict[str, float] = defaultdict(float)

# ── Cross-process aggregation ────────────────────────────────────────────────
# Sentinel runs ~20 processes but exposes /metrics from one of them (the API
# gateway). Process-local counters therefore never reach Prometheus: a counter
# incremented in a collector lives and dies inside that container.
#
# Every service already holds a Redis connection, so Redis is the natural
# aggregation point. Services publish their own counters under a per-service
# key; the gateway sums across all of them when rendering. The local dicts stay
# authoritative for the writing process, so recording a metric never blocks on
# I/O and never fails because Redis is briefly unavailable.
METRICS_KEY_PREFIX = "sentinel:metrics"
_SERVICE_NAME: str = os.getenv("SENTINEL_SERVICE", "unknown")
_redis_client: Any = None
_publish_task: Optional["asyncio.Task"] = None

# Counters are cumulative per process. Republishing the absolute value (rather
# than incrementing) makes publication idempotent, so a retry or an overlapping
# flush cannot double-count.
PUBLISH_INTERVAL_SEC = 15
METRICS_TTL_SEC = 90  # > 2 intervals: a stale key means the service stopped.


class MetricsCollector:
    """Singleton helper for recording metrics across Sentinel services."""

    @staticmethod
    def increment(metric_name: str, value: int = 1) -> None:
        """Increments a counter metric."""
        _COUNTER_METRICS[metric_name] += value

    @staticmethod
    def set_gauge(metric_name: str, value: float) -> None:
        """Sets a gauge metric."""
        _GAUGE_METRICS[metric_name] = float(value)

    @staticmethod
    def observe_latency(metric_name: str, latency_seconds: float) -> None:
        """Records an execution latency observation in seconds."""
        h = _LATENCY_HISTOGRAMS[metric_name]
        h.append(latency_seconds)
        if len(h) > 1000:
            # Keep rolling window of last 1000 observations
            _LATENCY_HISTOGRAMS[metric_name] = h[-1000:]

    @staticmethod
    def get_summary() -> Dict[str, Any]:
        """Returns a JSON-serializable summary of all metrics."""
        latency_summary = {}
        for k, vals in _LATENCY_HISTOGRAMS.items():
            if vals:
                sorted_vals = sorted(vals)
                n = len(sorted_vals)
                latency_summary[k] = {
                    "count": n,
                    "avg_ms": round((sum(vals) / n) * 1000, 2),
                    "p50_ms": round(sorted_vals[int(n * 0.50)] * 1000, 2),
                    "p95_ms": round(sorted_vals[int(n * 0.95)] * 1000, 2),
                    "p99_ms": round(sorted_vals[int(n * 0.99)] * 1000, 2),
                }

        return {
            "counters": dict(_COUNTER_METRICS),
            "gauges": dict(_GAUGE_METRICS),
            "latencies": latency_summary,
        }

    @staticmethod
    def to_prometheus_format() -> str:
        """Renders metrics in standard Prometheus text exposition format."""
        lines = []
        for k, v in _COUNTER_METRICS.items():
            clean_name = k.replace(":", "_").replace(".", "_")
            lines.append(f"# TYPE sentinel_{clean_name} counter")
            lines.append(f"sentinel_{clean_name} {v}")

        for k, v in _GAUGE_METRICS.items():
            clean_name = k.replace(":", "_").replace(".", "_")
            lines.append(f"# TYPE sentinel_{clean_name} gauge")
            lines.append(f"sentinel_{clean_name} {v}")

        for k, vals in _LATENCY_HISTOGRAMS.items():
            if vals:
                clean_name = k.replace(":", "_").replace(".", "_")
                n = len(vals)
                avg = sum(vals) / n
                lines.append(f"# TYPE sentinel_{clean_name}_seconds summary")
                lines.append(f'sentinel_{clean_name}_seconds{{quantile="0.5"}} {sorted(vals)[int(n * 0.5)]}')
                lines.append(f'sentinel_{clean_name}_seconds{{quantile="0.95"}} {sorted(vals)[int(n * 0.95)]}')
                lines.append(f'sentinel_{clean_name}_seconds_sum {sum(vals)}')
                lines.append(f'sentinel_{clean_name}_seconds_count {n}')

        return "\n".join(lines) + "\n"


async def bind_redis(redis_client: Any, service_name: Optional[str] = None) -> None:
    """Attach a Redis client so this process publishes its metrics.

    Call once during startup. Without it the process still records metrics
    normally -- they simply stay local and never reach the gateway's /metrics.
    """
    global _redis_client, _SERVICE_NAME, _publish_task
    _redis_client = redis_client
    if service_name:
        _SERVICE_NAME = service_name

    await publish_now()
    if _publish_task is None or _publish_task.done():
        _publish_task = asyncio.create_task(_publish_loop())
    logger.info("Metrics publishing enabled for service=%s", _SERVICE_NAME)


async def publish_now() -> bool:
    """Push this process's current metrics to Redis. Returns success."""
    if not _redis_client:
        return False
    try:
        raw = getattr(_redis_client, "raw", _redis_client)
        key = f"{METRICS_KEY_PREFIX}:{_SERVICE_NAME}"
        payload: Dict[str, str] = {}
        for name, value in _COUNTER_METRICS.items():
            payload[f"c|{name}"] = str(value)
        for name, value in _GAUGE_METRICS.items():
            payload[f"g|{name}"] = str(value)
        if not payload:
            return True

        pipe = raw.pipeline()
        pipe.hset(key, mapping=payload)
        pipe.expire(key, METRICS_TTL_SEC)
        pipe.sadd(f"{METRICS_KEY_PREFIX}:services", _SERVICE_NAME)
        await pipe.execute()
        return True
    except Exception as e:
        # Never let telemetry failure disturb the work being measured.
        logger.debug("Metrics publish skipped: %s", e)
        return False


async def _publish_loop() -> None:
    while True:
        try:
            await asyncio.sleep(PUBLISH_INTERVAL_SEC)
            await publish_now()
        except asyncio.CancelledError:
            await publish_now()  # final flush on shutdown
            raise
        except Exception as e:
            logger.debug("Metrics publish loop: %s", e)


async def collect_all(redis_client: Any = None) -> Dict[str, Dict[str, float]]:
    """Aggregate metrics across every publishing service.

    Returns {"counters": {...}, "gauges": {...}} summed over services, plus
    per-service breakdown under a `service` label suffix. Falls back to the
    local process when Redis is unavailable, so /metrics degrades to
    single-process reporting instead of failing.
    """
    local = {
        "counters": {k: float(v) for k, v in _COUNTER_METRICS.items()},
        "gauges": dict(_GAUGE_METRICS),
    }
    client = redis_client or _redis_client
    if not client:
        return local

    try:
        raw = getattr(client, "raw", client)
        services = await raw.smembers(f"{METRICS_KEY_PREFIX}:services")
        counters: Dict[str, float] = defaultdict(float)
        gauges: Dict[str, float] = {}

        for svc in services or []:
            name = svc.decode() if isinstance(svc, bytes) else str(svc)
            entries = await raw.hgetall(f"{METRICS_KEY_PREFIX}:{name}")
            if not entries:
                continue  # key expired: that service is no longer publishing
            for field, value in entries.items():
                f = field.decode() if isinstance(field, bytes) else str(field)
                v = value.decode() if isinstance(value, bytes) else str(value)
                kind, _, metric = f.partition("|")
                try:
                    num = float(v)
                except ValueError:
                    continue
                if kind == "c":
                    # Counters sum across services, and are also exposed
                    # per-service so a single stalled producer is visible.
                    counters[metric] += num
                    counters[f"{metric}{{service=\"{name}\"}}"] = num
                elif kind == "g":
                    gauges[f"{metric}{{service=\"{name}\"}}"] = num

        # Include this process's own values, which may be newer than its last publish.
        for k, v in _COUNTER_METRICS.items():
            counters.setdefault(k, 0.0)
        return {"counters": dict(counters), "gauges": gauges}
    except Exception as e:
        logger.warning("Metrics aggregation failed, reporting local only: %s", e)
        return local


class TimerContext:
    """Context manager for timing code execution blocks."""

    def __init__(self, metric_name: str):
        self.metric_name = metric_name
        self.start_time = 0.0

    def __enter__(self):
        self.start_time = time.monotonic()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        elapsed = time.monotonic() - self.start_time
        MetricsCollector.observe_latency(self.metric_name, elapsed)


def time_block(metric_name: str) -> TimerContext:
    """Convenience function returning a TimerContext manager."""
    return TimerContext(metric_name)
