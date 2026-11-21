"""Per-route latency for the gateway: count, mean, p50, p95, max and slow-request count over each route's recent requests, in this process.

Routes are keyed by their template (/api/v1/stocks/{ticker}/snapshot), never the concrete path, so the table stays small and carries no tickers or
user data. A rolling window of the last WINDOW samples per route bounds memory. Pure; no I/O, no clock reads (the caller times the request).
"""
from __future__ import annotations

import threading
from collections import deque
from typing import Any, Deque, Dict, List, Optional

WINDOW = 500
SLOW_MS = 1000.0
MAX_ROUTES = 400


class LatencyBook:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._samples: Dict[str, Deque[float]] = {}
        self._count: Dict[str, int] = {}
        self._slow: Dict[str, int] = {}
        self._errors: Dict[str, int] = {}

    def record(self, route: str, ms: float, status: int) -> None:
        with self._lock:
            if route not in self._samples and len(self._samples) >= MAX_ROUTES:
                route = "(other)"
            self._samples.setdefault(route, deque(maxlen=WINDOW)).append(float(ms))
            self._count[route] = self._count.get(route, 0) + 1
            if ms >= SLOW_MS:
                self._slow[route] = self._slow.get(route, 0) + 1
            if status >= 500:
                self._errors[route] = self._errors.get(route, 0) + 1

    @staticmethod
    def _pct(sorted_xs: List[float], q: float) -> float:
        pos = q * (len(sorted_xs) - 1)
        lo = int(pos)
        hi = min(lo + 1, len(sorted_xs) - 1)
        return sorted_xs[lo] + (sorted_xs[hi] - sorted_xs[lo]) * (pos - lo)

    def table(self, min_count: int = 1, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            rows = []
            for route, dq in self._samples.items():
                if self._count.get(route, 0) < min_count or not dq:
                    continue
                xs = sorted(dq)
                rows.append({"route": route, "requests": self._count[route], "mean_ms": round(sum(xs) / len(xs), 1),
                             "p50_ms": round(self._pct(xs, 0.5), 1), "p95_ms": round(self._pct(xs, 0.95), 1), "max_ms": round(xs[-1], 1),
                             "slow": self._slow.get(route, 0), "errors": self._errors.get(route, 0), "window": len(xs)})
        return sorted(rows, key=lambda r: -r["p95_ms"])[:limit]


BOOK = LatencyBook()


def route_template(scope_route: Optional[Any], path: str) -> str:
    """The route's template when the router matched one, else a fixed label (never the raw path)."""
    template = getattr(scope_route, "path", None)
    return str(template) if template else "(unmatched)"


__all__ = ["BOOK", "LatencyBook", "MAX_ROUTES", "SLOW_MS", "WINDOW", "route_template"]
