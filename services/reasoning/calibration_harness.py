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
                query = "SELECT scenario_id, status, confidence_overall, payload FROM scenarios WHERE LOWER(status) IN ('confirmed', 'denied') ORDER BY created_at DESC LIMIT 500"
                rows = await self.db.query(query)
                for r in rows:
                    outcomes.append({
                        "scenario_id": str(r["scenario_id"]),
                        "status": r["status"],
                        "confidence": float(r.get("confidence_overall", 50)),
                        "payload": json.loads(r["payload"]) if isinstance(r.get("payload"), str) else (r.get("payload") or {}),
                    })
            except Exception as e:
                logger.warning(f"DB calibration fetch failed: {e}")

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

    async def calibrate_optimal_thresholds(self) -> Dict[str, Any]:
        """
        Grid-searches optimal thresholds and saves recommendations to Redis.
        """
        default_config = {
            "z_score_threshold": 1.5,
            "similarity_threshold": 0.72,
            "vix_bounds": {"calm": 15.0, "elevated": 25.0, "extreme": 35.0},
            "min_correlation_coef": 0.65,
            "max_p_value": 0.05,
            "min_granger_f_stat": 3.84,
            "min_hawkes_branching_ratio": 0.25,
            "min_cointegration_p_value": 0.05,
            "calibrated_at": datetime.now(timezone.utc).isoformat(),
            "sample_count": 0,
        }

        outcomes = await self.fetch_historical_outcomes()
        if not outcomes:
            logger.info("No empirical outcome history available for calibration. Returning default thresholds.")
            if self.redis:
                try:
                    await self.redis.raw.set("sentinel:calibration:latest", json.dumps(default_config), ex=86400 * 7)
                    await self.redis.raw.set("sentinel:calibration:correlation_thresholds", json.dumps(default_config), ex=86400 * 7)
                except Exception as e:
                    logger.warning(f"Failed to persist default thresholds to Redis: {e}")
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
            "min_correlation_coef": round(max(0.55, min(0.85, best_sim * 0.90)), 2),
            "max_p_value": 0.05,
            "min_granger_f_stat": 3.84,
            "min_hawkes_branching_ratio": 0.25,
            "min_cointegration_p_value": 0.05,
            "best_f1_score": round(best_f1, 4),
            "sample_count": len(outcomes),
            "calibrated_at": datetime.now(timezone.utc).isoformat(),
        }

        if self.redis:
            try:
                await self.redis.raw.set("sentinel:calibration:latest", json.dumps(calibrated), ex=86400 * 7)
                await self.redis.raw.set("sentinel:calibration:correlation_thresholds", json.dumps(calibrated), ex=86400 * 7)
            except Exception as e:
                logger.warning(f"Failed to persist calibrated thresholds to Redis: {e}")

        logger.info(f"🎯 Threshold calibration complete | Z-Threshold: {best_z} | Sim-Threshold: {best_sim} | Min-Corr: {calibrated['min_correlation_coef']} | F1: {best_f1:.3f}")
        return calibrated


class DynamicBayesianCalibrator:
    """
    Real-time dynamic scenario probability recalibration using Bayes' Theorem:
        P(S_i | E_obs) = (P(E_obs | S_i) * P(S_i)) / Sum_j (P(E_obs | S_j) * P(S_j))
    """

    @staticmethod
    def recalibrate_hypotheses(
        hypotheses: List[Dict[str, Any]],
        watch_hits_by_index: Dict[int, List[str]],
        deny_hits_by_index: Dict[int, List[str]],
    ) -> Tuple[List[Dict[str, Any]], int, str]:
        """
        Recalibrates hypothesis probabilities P(S_i) and overall scenario confidence
        based on observed watch/deny signal hits.
        """
        if not hypotheses:
            return hypotheses, 50, "No hypotheses to recalibrate"

        n = len(hypotheses)
        priors = [max(1.0, float(h.get("probability", 100.0 / n))) for h in hypotheses]
        prior_sum = sum(priors)
        priors = [p / prior_sum for p in priors]

        likelihoods = [1.0] * n
        notes = []

        for idx in range(n):
            w_hits = watch_hits_by_index.get(idx, [])
            d_hits = deny_hits_by_index.get(idx, [])

            if w_hits:
                w_factor = min(0.85 + 0.05 * (len(w_hits) - 1), 0.95)
                likelihoods[idx] *= w_factor
                for j in range(n):
                    if j != idx:
                        likelihoods[j] *= (1.0 - w_factor)
                notes.append(f"H{idx+1} watch hit (+Bayes): {w_hits[:2]}")

            if d_hits:
                d_factor = max(0.10 - 0.02 * (len(d_hits) - 1), 0.02)
                likelihoods[idx] *= d_factor
                for j in range(n):
                    if j != idx:
                        likelihoods[j] *= 0.45
                notes.append(f"H{idx+1} deny hit (-Bayes): {d_hits[:2]}")

        numerators = [likelihoods[i] * priors[i] for i in range(n)]
        marginal_likelihood = sum(numerators)

        if marginal_likelihood <= 0:
            posteriors = priors
        else:
            posteriors = [num / marginal_likelihood for num in numerators]

        pcts = [round(p * 100) for p in posteriors]
        diff = 100 - sum(pcts)
        if pcts:
            pcts[0] += diff

        updated_hypotheses = []
        for i, h in enumerate(hypotheses):
            h_copy = dict(h)
            h_copy["probability"] = max(0, min(100, pcts[i]))
            updated_hypotheses.append(h_copy)

        leading_posterior = max(posteriors) if posteriors else 0.5
        overall_confidence = int(round(leading_posterior * 100))

        audit_str = "; ".join(notes) if notes else "Bayesian baseline intact"
        return updated_hypotheses, overall_confidence, audit_str

