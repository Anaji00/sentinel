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

import time
import asyncio
import logging
from typing import Dict, Any, List, Optional
from collections import defaultdict

logger = logging.getLogger("sentinel.metrics")

# Global in-memory metrics counters
_LATENCY_HISTOGRAMS: Dict[str, List[float]] = defaultdict(list)
_COUNTER_METRICS: Dict[str, int] = defaultdict(int)
_GAUGE_METRICS: Dict[str, float] = defaultdict(float)


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
