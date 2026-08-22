"""
shared/utils/collector_metrics.py

Per-collector instrumentation.

Collectors previously emitted a heartbeat and nothing else. A heartbeat proves
the process is running; it says nothing about whether the process is doing its
job. A collector whose upstream started returning 403, or whose parser silently
rejects every record after a schema change, keeps answering its heartbeat while
producing zero events -- and on the dashboard it is indistinguishable from a
healthy one.

These counters make that distinguishable. The signal to alert on is the
*ingest rate* and the *reject ratio*, not liveness:

    sentinel_collector_ingested_total{service="collector-crypto"}
    sentinel_collector_rejected_total{service="collector-crypto"}
    sentinel_collector_upstream_errors_total{service="collector-crypto"}
    sentinel_collector_last_success_epoch{service="collector-crypto"}

`last_success_epoch` is a gauge rather than a counter because the useful query
is `time() - last_success`, i.e. staleness.
"""

import logging
import time
from typing import Any, Optional

from shared.utils.metrics import MetricsCollector, bind_redis

logger = logging.getLogger("sentinel.collector_metrics")


class CollectorMetrics:
    """Counters for one collector.

    Deliberately tiny: a collector should be able to adopt this in three lines,
    because instrumentation that is laborious to add does not get added.

        metrics = CollectorMetrics("collector-crypto")
        await metrics.start(redis_client)
        ...
        metrics.ingested(len(batch))     # published successfully
        metrics.rejected("missing_price") # parsed but unusable
        metrics.upstream_error("binance") # the source failed us
    """

    def __init__(self, name: str):
        self.name = name
        self._ingested = 0
        self._rejected = 0
        self._errors = 0
        # None until the first record arrives, so the watcher can tell
        # "never received anything" from "received some, now quiet".
        self._last_ingest_at: Optional[float] = None

    async def start(self, redis_client: Any = None) -> None:
        """Bind Redis so these counters reach the gateway's /metrics."""
        # Seed the gauges so a collector that has not yet ingested anything is
        # visible as 0 rather than absent -- an absent series is easy to miss.
        MetricsCollector.set_gauge(f"collector_last_success_epoch:{self.name}", 0.0)
        MetricsCollector.increment(f"collector_ingested_total:{self.name}", 0)
        MetricsCollector.increment(f"collector_rejected_total:{self.name}", 0)
        MetricsCollector.increment(f"collector_upstream_errors_total:{self.name}", 0)
        if redis_client is not None:
            try:
                await bind_redis(redis_client, service_name=self.name)
            except Exception as e:
                logger.debug("Metrics binding skipped for %s: %s", self.name, e)

    def ingested(self, count: int = 1) -> None:
        """Records records successfully published downstream."""
        if count <= 0:
            return
        self._ingested += count
        self._last_ingest_at = time.time()
        MetricsCollector.increment(f"collector_ingested_total:{self.name}", count)
        MetricsCollector.set_gauge(f"collector_last_success_epoch:{self.name}", time.time())

    def rejected(self, reason: str = "unspecified", count: int = 1) -> None:
        """Records records received but not usable.

        A steady trickle is normal. A reject ratio that jumps toward 1.0 means
        the upstream changed shape, which is the failure this exists to catch.
        """
        if count <= 0:
            return
        self._rejected += count
        MetricsCollector.increment(f"collector_rejected_total:{self.name}", count)
        MetricsCollector.increment(f"collector_rejected_reason:{self.name}:{reason}", count)

    def upstream_error(self, source: str = "unspecified", count: int = 1) -> None:
        """Records a failure attributable to the data source, not to us."""
        if count <= 0:
            return
        self._errors += count
        MetricsCollector.increment(f"collector_upstream_errors_total:{self.name}", count)
        MetricsCollector.increment(f"collector_upstream_error_source:{self.name}:{source}", count)

    async def watch_for_starvation(
        self,
        expect_within_sec: float = 300.0,
        check_interval_sec: float = 60.0,
        source: str = "upstream",
    ) -> None:
        """Logs loudly when a connected source stops producing.

        A collector that connects successfully and then receives nothing is the
        hardest failure to notice: no exception, no restart, a healthy heartbeat,
        and an empty panel that looks identical to a quiet market. The AIS
        collector sat in exactly this state -- "Subscribed" and then silence for
        an hour, because the upstream accepts an inactive API key without ever
        returning an error.

        Run this as a background task alongside the ingest loop:

            asyncio.create_task(metrics.watch_for_starvation(source="aisstream"))

        It never raises and never exits on its own: the collector should keep
        trying, but the operator should be told it is trying in vain.
        """
        import asyncio

        started = time.time()
        warned = False

        while True:
            try:
                await asyncio.sleep(check_interval_sec)
                idle = time.time() - (self._last_ingest_at or started)

                if self._ingested == 0 and idle >= expect_within_sec:
                    if not warned:
                        logger.error(
                            "%s has received NOTHING from %s in %.0fs despite a "
                            "successful connection. The credential may be inactive, "
                            "or the subscription may match no data. This is not a "
                            "quiet period -- zero messages have ever arrived.",
                            self.name, source, idle,
                        )
                        warned = True
                    MetricsCollector.set_gauge(f"collector_starved:{self.name}", 1.0)

                elif self._ingested > 0 and idle >= expect_within_sec:
                    logger.warning(
                        "%s has produced nothing for %.0fs (%d records total). "
                        "The upstream may have gone quiet or stopped.",
                        self.name, idle, self._ingested,
                    )
                    MetricsCollector.set_gauge(f"collector_starved:{self.name}", 1.0)

                else:
                    if warned:
                        logger.info("%s is receiving data again.", self.name)
                        warned = False
                    MetricsCollector.set_gauge(f"collector_starved:{self.name}", 0.0)

            except asyncio.CancelledError:
                raise
            except Exception as e:
                # Telemetry must never take down the collector it observes.
                logger.debug("Starvation watch error for %s: %s", self.name, e)

    @property
    def reject_ratio(self) -> Optional[float]:
        """Rejected / (ingested + rejected), or None before anything arrived."""
        total = self._ingested + self._rejected
        return None if total == 0 else self._rejected / total

    def snapshot(self) -> dict:
        """Current counts, for heartbeat metadata and health endpoints."""
        return {
            "ingested": self._ingested,
            "rejected": self._rejected,
            "upstream_errors": self._errors,
            "reject_ratio": self.reject_ratio,
        }
