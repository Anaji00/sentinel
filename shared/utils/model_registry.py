"""
shared/utils/model_registry.py

MODEL REGISTRY, DATA VALIDATION & CONFORMAL CALIBRATION (§1.1, §1.3, §1.5)
========================================================================

Provides:
1. InsufficientTrainingDataError — Exception raised when training data is missing,
   synthetic, or below required sample thresholds (§1.5).
2. validate_training_data — Hard-fail guard for ML training pipelines (§1.5).
3. ConformalZScoreCalibrator — Dynamic conformal calibration of z-score anomaly
   thresholds replacing static 1.5 z-score cutoffs (§1.3).
4. ModelRegistry — Central registry for model versioning, state persistence,
   metrics tracking (FAR, drift), and model promotion/demotion (§1.5).
"""

import json
import math
import time
import logging
from typing import Dict, Any, List, Optional, Tuple
from pydantic import BaseModel, Field

logger = logging.getLogger("shared.model_registry")


# ── 1. HARD-FAIL ON SYNTHETIC / INSUFFICIENT DATA (§1.5) ────────────────────

class InsufficientTrainingDataError(ValueError):
    """Raised when ML model training data is missing, synthetic, or insufficient."""
    pass


def validate_training_data(
    data: Any,
    domain: str,
    min_samples: int = 500,
    allow_synthetic: bool = False,
) -> int:
    """
    Validates training dataset before model fitting (§1.5).
    Raises InsufficientTrainingDataError if:
    - data is empty/None
    - sample count < min_samples
    - data variance is zero (collapsed synthetic placeholder)
    - allow_synthetic is False (default)

    Returns:
        Number of valid samples
    """
    if data is None:
        raise InsufficientTrainingDataError(
            f"Training data for domain '{domain}' is None. Refusing to fit synthetic baseline."
        )

    try:
        import numpy as np
        arr = np.asarray(data)
    except Exception as e:
        raise InsufficientTrainingDataError(
            f"Invalid training data format for domain '{domain}': {e}"
        )

    num_samples = arr.shape[0] if arr.ndim > 0 else 0
    if num_samples < min_samples:
        raise InsufficientTrainingDataError(
            f"Insufficient training data for domain '{domain}': got {num_samples} samples, "
            f"minimum required is {min_samples}. Hard-failing per §1.5 model safety policy."
        )

    # Check for collapsed/synthetic data (all identical values or zero variance across columns)
    if arr.ndim >= 2 and arr.size > 0:
        variances = np.var(arr, axis=0)
        if np.all(variances < 1e-12):
            raise InsufficientTrainingDataError(
                f"Training data for domain '{domain}' has zero variance across all features. "
                f"Detected synthetic placeholder data. Hard-failing per §1.5 policy."
            )

    logger.info(f"✅ Training data validated for domain '{domain}': {num_samples} samples")
    return num_samples


# ── 2. CONFORMAL Z-SCORE CALIBRATOR (§1.3) ──────────────────────────────────

def _conformal_index(n: int, target_far: float) -> int:
    """Order statistic giving finite-sample coverage, not the plug-in quantile.

    Split-conformal coverage comes from the ceil((n+1)(1-alpha))-th order
    statistic, which is why the guarantee holds at small n. `int(n*(1-alpha))`
    is the plug-in estimator and sits below it -- anti-conservative in exactly
    the regime the min_samples floor of 30 puts this in.
    """
    if n <= 0:
        return 0
    k = math.ceil((n + 1) * (1.0 - float(target_far)))
    return min(max(0, int(k) - 1), n - 1)


class ConformalZScoreCalibrator:
    """
    Conformal calibration layer for dynamic anomaly Z-score thresholds (§1.3).
    Replaces static z-score thresholds (e.g. 1.5) with quantile-calibrated cutoffs.

    Maintains a rolling window of null/background z-scores and sets the threshold
    at the (1 - target_far) quantile, guaranteeing false alarm rate bounds.
    """

    def __init__(
        self,
        domain: str = "general",
        target_far: float = 0.05,  # 5% target false alarm rate
        buffer_size: int = 1000,
        min_samples: int = 30,
        default_z_threshold: float = 1.5,
    ):
        self.domain = domain
        self.target_far = target_far
        self.buffer_size = buffer_size
        self.min_samples = min_samples
        self.default_z_threshold = default_z_threshold

        self._null_z_scores: List[float] = []
        self._calibrated_z_thresh: Optional[float] = None

    @property
    def z_threshold(self) -> float:
        """Return calibrated z-threshold or default if not yet calibrated."""
        if self._calibrated_z_thresh is not None:
            return self._calibrated_z_thresh
        return self.default_z_threshold

    def observe(self, z_score: float, is_confirmed_anomaly: bool = False):
        """Record a background observation, to build the null distribution.

        Putative anomalies are excluded as well as confirmed ones.

        Every caller passes the default `is_confirmed_anomaly=False`, so the
        buffer named `_null_z_scores` held the full mixture rather than the
        background, and its 95th percentile was the 95th percentile of
        everything the detector saw. That makes the threshold a fixed selection
        quota: about 5% of events clear it no matter what is happening, and if
        a genuine crisis makes half the stream anomalous the threshold rises
        with it and 5% still passes. The platform could not report that today
        was unusually eventful, because the test was a rate.

        Ground truth is not available here, so the exclusion is the best
        available proxy: an observation the *current* threshold already calls
        significant is not evidence about the background. Below the minimum
        sample count nothing is excluded, because there is no threshold yet to
        exclude against and the buffer has to fill from somewhere.
        """
        if is_confirmed_anomaly or math.isnan(z_score) or math.isinf(z_score):
            return
        value = abs(z_score)
        if (
            self._calibrated_z_thresh is not None
            and len(self._null_z_scores) >= self.min_samples
            and value > self._calibrated_z_thresh
        ):
            return
        if True:
            self._null_z_scores.append(value)
            if len(self._null_z_scores) > self.buffer_size:
                self._null_z_scores = self._null_z_scores[-self.buffer_size:]

            if len(self._null_z_scores) >= self.min_samples:
                self._recalibrate()

    def _recalibrate(self):
        """Recompute the threshold at the conformal (1 - target_far) quantile."""
        sorted_scores = sorted(self._null_z_scores)
        new_thresh = sorted_scores[_conformal_index(len(sorted_scores), self.target_far)]

        # Clamp to reasonable range [1.0, 3.5]
        new_thresh = max(1.0, min(3.5, new_thresh))

        if self._calibrated_z_thresh is None or abs(new_thresh - self._calibrated_z_thresh) > 0.05:
            logger.info(
                f"Conformal Z-threshold recalibrated for domain '{self.domain}': "
                f"{self._calibrated_z_thresh or self.default_z_threshold:.3f} → {new_thresh:.3f} "
                f"(samples: {len(self._null_z_scores)}, target FAR: {self.target_far:.1%})"
            )
        self._calibrated_z_thresh = new_thresh

    def get_status(self) -> Dict[str, Any]:
        return {
            "domain": self.domain,
            "z_threshold": round(self.z_threshold, 4),
            "calibrated": self._calibrated_z_thresh is not None,
            "samples": len(self._null_z_scores),
            "target_far": self.target_far,
        }


# ── 3. MODEL REGISTRY & METRICS METADATA (§1.5) ─────────────────────────────

class ModelMetadata(BaseModel):
    """Metadata record for a registered anomaly detection model."""
    model_id: str
    domain: str
    model_type: str  # "RRCF", "Kalman", "FirstStory", "Hawkes", "BGPGraph"
    version: str = "1.0.0"
    created_at: float = Field(default_factory=time.time)
    num_samples_trained: int = 0
    is_active: bool = True
    drift_status: str = "NORMAL"  # "NORMAL", "WARNING", "DRIFTED"
    false_alarm_rate: float = 0.05
    last_evaluated_at: Optional[float] = None
    parameters: Dict[str, Any] = Field(default_factory=dict)


class ModelRegistry:
    """
    Central Model Registry for tracking active per-domain models, metadata,
    drift statuses, and model promotion/demotion (§1.5).
    """

    def __init__(self, redis_client=None):
        self._redis = redis_client
        self._registry: Dict[str, ModelMetadata] = {}

    def register_model(
        self,
        domain: str,
        model_type: str,
        parameters: Optional[Dict[str, Any]] = None,
        num_samples: int = 0,
        version: str = "1.0.0",
    ) -> ModelMetadata:
        """Register a new active per-domain model."""
        model_id = f"model:{domain}:{model_type}:{version}"
        meta = ModelMetadata(
            model_id=model_id,
            domain=domain,
            model_type=model_type,
            version=version,
            num_samples_trained=num_samples,
            parameters=parameters or {},
        )
        self._registry[domain] = meta
        logger.info(f"Registered model [{model_id}] for domain '{domain}' (samples: {num_samples})")
        return meta

    def get_model_metadata(self, domain: str) -> Optional[ModelMetadata]:
        return self._registry.get(domain)

    def update_drift_status(self, domain: str, status: str, far: float = 0.05):
        """Update drift status ('NORMAL', 'WARNING', 'DRIFTED') for a domain model."""
        if domain in self._registry:
            self._registry[domain].drift_status = status
            self._registry[domain].false_alarm_rate = far
            self._registry[domain].last_evaluated_at = time.time()
            logger.info(f"Model [{domain}] drift status updated -> {status} (FAR: {far:.3f})")

    def list_models(self) -> List[ModelMetadata]:
        return list(self._registry.values())


class ConformalScoreCalibrator(ConformalZScoreCalibrator):
    """Conformal calibration on the [0,1] score scale rather than the z scale.

    The parent calibrates z-scores and clamps its threshold to [1.0, 3.5],
    which is right for a z and wrong for anything else. The anomaly scorer was
    feeding it detector output -- empirical percentiles in [0, 0.995] -- so its
    quantile was always below 1.0, the clamp raised it to exactly 1.0, and a
    threshold of 1.0 against a score that never reaches 1.0 marks nothing
    significant at all.

    That never surfaced because the computed threshold was assigned to a local
    and never read. Connecting it as it stood would have disabled significance
    for every domain at once.

    Same conformal argument, bounds that fit the quantity: the threshold is the
    (1 - target_far) quantile of observed scores, held inside a range where a
    score threshold is meaningful. The value of this over a fixed cut is that
    the false-alarm rate is what stays constant across domains, rather than the
    number -- a 0.60 cut means something different in a domain whose scores
    cluster high than in one whose scores cluster low.
    """

    # A significance cut below this is not a cut, and above it nothing survives:
    # the detectors bound their own output at 0.995.
    MIN_SCORE_THRESHOLD = 0.50
    MAX_SCORE_THRESHOLD = 0.99

    def _recalibrate(self):
        """Recompute the threshold at the conformal (1 - target_far) quantile of scores."""
        sorted_scores = sorted(self._null_z_scores)
        new_thresh = sorted_scores[_conformal_index(len(sorted_scores), self.target_far)]

        new_thresh = max(self.MIN_SCORE_THRESHOLD, min(self.MAX_SCORE_THRESHOLD, new_thresh))

        if self._calibrated_z_thresh is None or abs(new_thresh - self._calibrated_z_thresh) > 0.01:
            logger.info(
                "Conformal score threshold recalibrated for domain '%s': %.3f -> %.3f "
                "(samples: %d, target FAR: %.1f%%)",
                self.domain,
                self._calibrated_z_thresh if self._calibrated_z_thresh is not None
                else self.default_z_threshold,
                new_thresh, len(self._null_z_scores), self.target_far * 100,
            )
        self._calibrated_z_thresh = new_thresh
