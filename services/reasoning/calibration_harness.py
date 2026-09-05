"""
services/reasoning/calibration_harness.py

Threshold Calibration & Backtest Harness.
Consumes empirical scenario_tracker outcome data and historical signal accuracy
to optimize system-wide thresholds (z_score_threshold, SIMILARITY_THRESHOLD, VIX bounds)
using precision/recall optimization instead of static heuristics.
"""

import json
import logging
import math
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
                # `payload` is not a column on scenarios and never has been, so
                # this query raised every time it ran -- 96 times in one day,
                # four to nine an hour, all day. The handler logs and returns an
                # empty list, so evaluate_threshold_combination scored (0, 0, 0)
                # and no threshold was ever calibrated from an outcome.
                #
                # 212 confirmed scenarios were sitting in the table while this
                # ran. The anomaly score it wants is reachable: a scenario comes
                # from a correlation, and that correlation names the event that
                # triggered it.
                query = """
                    SELECT s.scenario_id,
                           s.status,
                           s.confidence_overall,
                           e.anomaly_score
                    FROM scenarios s
                    LEFT JOIN correlations c ON s.correlation_id = c.correlation_id
                    LEFT JOIN events e ON e.event_id = c.trigger_event_id
                    WHERE LOWER(s.status) IN ('confirmed', 'denied')
                    ORDER BY s.created_at DESC
                    LIMIT 500
                """
                rows = await self.db.query(query)
                for r in rows:
                    anomaly = r.get("anomaly_score")
                    outcomes.append({
                        "scenario_id": str(r["scenario_id"]),
                        "status": r["status"],
                        "confidence": float(r.get("confidence_overall") or 50),
                        "payload": {"anomaly_score": float(anomaly) if anomaly is not None else 0.5},
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
            # Case-insensitive: the column stores "confirmed", not "CONFIRMED",
            # so this comparison was false for every confirmed outcome and the
            # harness would have scored precision at zero even with data.
            is_positive = str(status or "").strip().upper() == "CONFIRMED"

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
                await self.redis.raw.set("sentinel:calibration:correlation_thresholds", json.dumps(calibrated), ex=86400 * 7)
            except Exception as e:
                logger.warning(f"Failed to persist calibrated thresholds to Redis: {e}")

        logger.info(f"🎯 Threshold calibration complete | Z-Threshold: {best_z} | Sim-Threshold: {best_sim} | Min-Corr: {calibrated['min_correlation_coef']} | F1: {best_f1:.3f}")
        return calibrated


# How much a refuted hypothesis costs the scenario's overall confidence.
#
# At 0.8, deny evidence against every hypothesis takes a leading posterior of
# 45 down to 9, which is below the tracker's DENY_THRESHOLD of 25 and so
# reaches the denied state that 672 scenarios had never once reached. Deny
# evidence against only the front-runner costs proportionally less, because
# less of what was believed has been contradicted.
DENY_MASS_WEIGHT = 0.8

# Total refutation still leaves a floor rather than zero: the signals are
# matched heuristically, and a confidence of exactly 0 asserts a certainty the
# matching cannot support.
MIN_EVIDENTIAL_SUPPORT = 0.05


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

        # Largest-remainder apportionment, so the residual is not a thumb on
        # the scale for whichever hypothesis happens to be listed first.
        #
        # `pcts[0] += diff` dumped the whole rounding residual on index 0, which
        # is by convention the leading hypothesis -- nudging the published
        # probability of the front-runner up by as much as two points, in the
        # same direction, in every scenario where rounding did not land exactly.
        # Three hypotheses near parity round to 33/33/33 and H1 became 34.
        scaled = [p * 100.0 for p in posteriors]
        pcts = [int(math.floor(v)) for v in scaled]
        residual = 100 - sum(pcts)
        if residual > 0 and pcts:
            # Hand the spare points to the largest fractional parts, which is
            # the standard apportionment rule and is order-independent.
            order = sorted(range(len(scaled)), key=lambda i: scaled[i] - math.floor(scaled[i]), reverse=True)
            for i in order[:residual]:
                pcts[i] += 1
        elif residual < 0 and pcts:
            order = sorted(range(len(scaled)), key=lambda i: scaled[i] - math.floor(scaled[i]))
            for i in order[:-residual]:
                pcts[i] -= 1

        updated_hypotheses = []
        for i, h in enumerate(hypotheses):
            h_copy = dict(h)
            h_copy["probability"] = max(0, min(100, pcts[i]))
            updated_hypotheses.append(h_copy)

        leading_posterior = max(posteriors) if posteriors else 0.5

        # Confidence in the scenario, not just in its front-runner.
        #
        # overall_confidence was max(posteriors) alone. Posteriors are
        # normalised to sum to 1, so that number measures how far the leading
        # hypothesis is ahead of its rivals -- a purely relative quantity. It
        # cannot fall below 1/n: 33 for three hypotheses, 50 for two.
        #
        # CONFIRM_THRESHOLD (65) and DENY_THRESHOLD (25) in the tracker read it
        # as an absolute claim about whether the scenario is true. Against a
        # floor of 33 the deny branch is unreachable, and the database agreed:
        # 213 scenarios confirmed and 0 denied out of 672, with the observed
        # minimum confidence sitting at 35.
        #
        # Worse than unreachable, it was inverted. Deny hits on every hypothesis
        # left confidence at exactly 45 no matter how many fired, because
        # normalisation divides the shared penalty straight back out. Refuting
        # the whole scenario changed nothing at all.
        #
        # So the refuted share of prior belief is carried separately here. It is
        # the mass that was believed before this evidence and has since been
        # contradicted; normalisation cannot cancel it because it is measured
        # before the division. A scenario with no deny hits is untouched, which
        # leaves every existing confirmation exactly where it was.
        refuted_mass = sum(
            priors[i] for i in range(n) if deny_hits_by_index.get(i)
        )
        support = max(
            MIN_EVIDENTIAL_SUPPORT, 1.0 - DENY_MASS_WEIGHT * refuted_mass
        )
        overall_confidence = int(round(leading_posterior * support * 100))
        if refuted_mass > 0:
            notes.append(
                f"scenario support {support:.2f} "
                f"({refuted_mass:.0%} of prior belief refuted)"
            )

        audit_str = "; ".join(notes) if notes else "Bayesian baseline intact"
        return updated_hypotheses, overall_confidence, audit_str

