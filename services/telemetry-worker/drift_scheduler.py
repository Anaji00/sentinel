"""
services/telemetry-worker/drift_scheduler.py

Model Drift Detection & Retrain Scheduling.
Monitors ONNX anomaly score distributions over rolling 24-hour windows.
Computes Population Stability Index (PSI) and Kolmogorov-Smirnov (KS) drift statistics.
Dispatches model retrain triggers on Kafka topic Topics.RULES_FEEDBACK when drift exceeds tolerance.
"""

import asyncio
import json
import logging
import math
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple
import numpy as np

from shared.kafka import SentinelProducer, Topics
from shared.utils.metrics import MetricsCollector

logger = logging.getLogger("telemetry.drift_scheduler")

# Where the latest drift evaluation is published for downstream consumers.
DRIFT_REPORT_KEY = "sentinel:ml:drift_report"


# Observations required on each side before a PSI comparison means anything.
MIN_DRIFT_SAMPLES = 20

# How much of the live distribution to freeze as the baseline.
MIN_BASELINE_SNAPSHOT = 500

# The baseline ages out so drift is measured against a recent normal rather
# than against whatever the system looked like the first time it ran.
BASELINE_TTL_SEC = 14 * 86400


class ModelDriftScheduler:
    """
    Periodic background scheduler for monitoring anomaly distribution drift.
    """

    def __init__(self, redis_client=None, producer=None, check_interval_sec: int = 3600):
        self.redis = redis_client
        self.producer = producer
        self.check_interval_sec = check_interval_sec
        self._running = False
        self._task = None

    def calculate_psi(self, baseline: List[float], current: List[float], num_buckets: int = 10) -> float:
        """
        Calculates Population Stability Index (PSI).
        PSI < 0.1: No change
        0.1 <= PSI < 0.25: Slight drift
        PSI >= 0.25: Significant drift (retrain recommended)
        """
        if not baseline or not current:
            return 0.0

        b_arr = np.array(baseline, dtype=np.float64)
        c_arr = np.array(current, dtype=np.float64)

        buckets = np.linspace(0.0, 1.0, num_buckets + 1)
        b_counts, _ = np.histogram(b_arr, bins=buckets)
        c_counts, _ = np.histogram(c_arr, bins=buckets)

        b_pct = np.maximum(1e-4, b_counts / float(len(b_arr)))
        c_pct = np.maximum(1e-4, c_counts / float(len(c_arr)))

        psi = np.sum((c_pct - b_pct) * np.log(c_pct / b_pct))
        return float(psi)

    async def check_model_drift(self) -> Dict[str, Any]:
        """
        Fetches score histories, calculates PSI, and dispatches retrain trigger if drift >= 0.25.
        """
        baseline_scores = []
        current_scores = []

        if self.redis:
            try:
                raw_b = await self.redis.raw.lrange("sentinel:ml:baseline_scores", 0, 1000)
                raw_c = await self.redis.raw.lrange("sentinel:ml:current_scores", 0, 1000)
                baseline_scores = [float(x) for x in raw_b if x]
                current_scores = [float(x) for x in raw_c if x]
            except Exception as e:
                logger.debug(f"Redis fetch error in drift scheduler: {e}")

        # Seed the baseline from the first full sample, then compare against it.
        #
        # Nothing wrote either list, so this guard tripped on its first line
        # every hour and the loop returned drift_detected=False -- an absence
        # reported as a finding, from the component whose job is catching
        # exactly that. The scorer now records live scores; the baseline is the
        # snapshot this takes the first time there are enough of them.
        if len(current_scores) >= MIN_DRIFT_SAMPLES and len(baseline_scores) < MIN_DRIFT_SAMPLES and self.redis:
            try:
                pipe = self.redis.raw.pipeline()
                pipe.delete("sentinel:ml:baseline_scores")
                for value in current_scores[:MIN_BASELINE_SNAPSHOT]:
                    pipe.rpush("sentinel:ml:baseline_scores", float(value))
                pipe.expire("sentinel:ml:baseline_scores", BASELINE_TTL_SEC)
                await pipe.execute()
                logger.info(
                    "Model drift baseline seeded from %s live scores.",
                    min(len(current_scores), MIN_BASELINE_SNAPSHOT),
                )
            except Exception as e:
                logger.debug(f"Could not seed drift baseline: {e}")
            return {"drift_detected": None, "psi": None, "status": "baseline_seeded"}

        if len(baseline_scores) < MIN_DRIFT_SAMPLES or len(current_scores) < MIN_DRIFT_SAMPLES:
            # drift_detected is None, not False. "We did not measure" and "we
            # measured and found none" are different answers and were the same
            # value.
            return {
                "drift_detected": None,
                "psi": None,
                "status": "insufficient_samples",
                "baseline_count": len(baseline_scores),
                "current_count": len(current_scores),
            }

        psi = self.calculate_psi(baseline_scores, current_scores)
        drift_detected = psi >= 0.25

        report = {
            "drift_detected": drift_detected,
            "psi": round(psi, 4),
            "baseline_count": len(baseline_scores),
            "current_count": len(current_scores),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        if drift_detected:
            MetricsCollector.increment("model_drift_detected_total")
            logger.warning(f"⚠️ MODEL DRIFT DETECTED | PSI: {psi:.4f} >= 0.25 | Triggering Retrain Dispatch.")

            if self.producer:
                try:
                    await self.producer.send(
                        Topics.RULES_FEEDBACK,
                        {
                            "event_type": "retrain_trigger",
                            "reason": "PSI drift threshold exceeded",
                            "psi_score": psi,
                            "dispatched_at": datetime.now(timezone.utc).isoformat(),
                        },
                        key="model_drift",
                    )
                except Exception as e:
                    logger.error(f"Failed to publish retrain trigger: {e}")

        # Publish the report so consumers (model cards, explainability audit
        # trails) can quote measured drift instead of a hardcoded placeholder.
        # TTL slightly over two check intervals: a stale key means the scheduler
        # stopped, and a consumer should report drift as unknown rather than
        # keep showing the last value indefinitely.
        if self.redis:
            try:
                await self.redis.raw.set(
                    DRIFT_REPORT_KEY,
                    json.dumps(report),
                    ex=max(120, int(self.check_interval_sec * 2.5)),
                )
            except Exception as e:
                logger.warning(f"Failed to publish drift report: {e}")

        return report

    async def start(self):
        """Starts background periodic drift evaluation loop."""
        self._running = True
        logger.info(f"🚀 Starting ModelDriftScheduler loop (Interval: {self.check_interval_sec}s)")
        while self._running:
            try:
                await self.check_model_drift()
            except Exception as e:
                logger.error(f"Error in ModelDriftScheduler cycle: {e}")
            await asyncio.sleep(self.check_interval_sec)

    def stop(self):
        self._running = False
