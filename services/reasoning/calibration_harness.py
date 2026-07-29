"""
services/reasoning/calibration_harness.py

Threshold Calibration & Backtest Harness.
Consumes empirical scenario_tracker outcome data and historical signal accuracy
to optimize system-wide thresholds (z_score_threshold, SIMILARITY_THRESHOLD, VIX bounds)
using precision/recall optimization instead of static heuristics.
"""

import json
import logging
import time
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone

logger = logging.getLogger("reasoning.calibration")


class ThresholdCalibrationHarness:
    """
    Backtests and calibrates Sentinel detection & similarity thresholds
    against empirical pattern outcome history.
    """

    def __init__(self, db_client=None, redis_client=None):
        self.db = db_client
        self.redis = redis_client

    async def fetch_historical_outcomes(self) -> List[Dict[str, Any]]:
        """Fetches confirmed and denied pattern outcome records from database or Redis."""
        outcomes = []
        if self.redis:
            try:
                raw_outcomes = await self.redis.raw.lrange("sentinel:calibration:outcomes_history", 0, 500)
                for item in raw_outcomes:
                    outcomes.append(json.loads(item if isinstance(item, str) else item.decode("utf-8")))
            except Exception as e:
                logger.debug(f"Redis calibration fetch fallback: {e}")

        if not outcomes and self.db:
            try:
                query = "SELECT scenario_id, status, confidence_overall, payload FROM scenarios WHERE status IN ('CONFIRMED', 'DENIED') ORDER BY created_at DESC LIMIT 500"
                rows = await self.db.fetch_all(query)
                for r in rows:
                    outcomes.append({
                        "scenario_id": str(r["scenario_id"]),
                        "status": r["status"],
                        "confidence": float(r.get("confidence_overall", 50)),
                        "payload": json.loads(r["payload"]) if isinstance(r.get("payload"), str) else (r.get("payload") or {}),
                    })
            except Exception as e:
                logger.debug(f"DB calibration fetch fallback: {e}")

        return outcomes

    def evaluate_threshold_combination(
        self,
        outcomes: List[Dict[str, Any]],
        z_score_thresh: float,
        sim_thresh: float,
    ) -> Tuple[float, float, float]:
        """
        Evaluates precision, recall, and F1-score for a specific threshold pair.
        """
        if not outcomes:
            return 0.0, 0.0, 0.0

        tp, fp, fn = 0, 0, 0
        for item in outcomes:
            status = item.get("status")
            conf = item.get("confidence", 50.0) / 100.0
            anomaly = item.get("payload", {}).get("anomaly_score", 0.5)

            # Signal triggered if anomaly >= z_score_thresh / 3.0 and conf >= sim_thresh
            triggered = (anomaly >= (z_score_thresh / 3.0)) and (conf >= sim_thresh)
            is_positive = status == "CONFIRMED"

            if triggered and is_positive:
                tp += 1
            elif triggered and not is_positive:
                fp += 1
            elif not triggered and is_positive:
                fn += 1

        precision = tp / max(1, tp + fp)
        recall = tp / max(1, tp + fn)
        f1 = (2 * precision * recall) / max(1e-5, precision + recall)

        return precision, recall, f1

    async def calibrate_optimal_thresholds() -> Dict[str, Any]:
        """
        Grid-searches optimal thresholds and saves recommendations to Redis.
        """
        outcomes = await self.fetch_historical_outcomes()
        if not outcomes:
            logger.info("No empirical outcome history available for calibration. Returning default thresholds.")
            default_config = {
                "z_score_threshold": 1.5,
                "similarity_threshold": 0.72,
                "vix_bounds": {"calm": 15.0, "elevated": 25.0, "extreme": 35.0},
                "calibrated_at": datetime.now(timezone.utc).isoformat(),
                "sample_count": 0,
            }
            return default_config

        z_grid = [1.0, 1.25, 1.5, 1.75, 2.0]
        sim_grid = [0.65, 0.70, 0.72, 0.75, 0.80]

        best_f1 = -1.0
        best_z = 1.5
        best_sim = 0.72

        for z in z_grid:
            for sim in sim_grid:
                p, r, f1 = self.evaluate_threshold_combination(outcomes, z, sim)
                if f1 > best_f1:
                    best_f1 = f1
                    best_z = z
                    best_sim = sim

        calibrated = {
            "z_score_threshold": round(best_z, 2),
            "similarity_threshold": round(best_sim, 2),
            "vix_bounds": {"calm": 15.0, "elevated": 25.0, "extreme": 35.0},
            "best_f1_score": round(best_f1, 4),
            "sample_count": len(outcomes),
            "calibrated_at": datetime.now(timezone.utc).isoformat(),
        }

        if self.redis:
            try:
                await self.redis.raw.set("sentinel:calibration:latest", json.dumps(calibrated), ex=86400 * 7)
            except Exception as e:
                logger.warning(f"Failed to persist calibrated thresholds to Redis: {e}")

        logger.info(f"🎯 Threshold calibration complete | Z-Threshold: {best_z} | Sim-Threshold: {best_sim} | F1: {best_f1:.3f}")
        return calibrated
