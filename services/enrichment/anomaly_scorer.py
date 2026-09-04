import asyncio
import math
import os
import json
import time
import numpy as np
import logging
from shared.utils.metrics import MetricsCollector
from shared.utils import quant_calc
from shared.utils.streaming_detectors import (
    FALLBACK_MAX_SCORE,
    RRCFDetector,
    KalmanResidualFilter,
    HawkesIntensityTracker,
    FirstStoryDetector,
    BGPGraphFeatureExtractor,
    sts_zone_risk_multiplier,
)
from shared.utils.model_registry import (
    ModelRegistry, ConformalZScoreCalibrator, ConformalScoreCalibrator,
)

from typing import Optional, List, Dict, Any, Tuple

# The maritime gap distribution, mirroring the aviation detector's.
#
# Same phenomenon, same null model, same band -- the two had drifted six times
# apart while scoring the same thing, and these constants are deliberately
# identical to _GAP_SAMPLE_CAP / _MIN_GAP_SAMPLES / _NOTABLE_PERCENTILE /
# _SCORE_FLOOR / _SCORE_CEILING in aviation_gap_detector.py so they cannot
# quietly diverge again.
_VESSEL_GAP_SAMPLES_KEY = "sentinel:maritime:gap_samples:{region}"
_VESSEL_GAP_SAMPLE_CAP = 500
_MIN_VESSEL_GAP_SAMPLES = 60
_VESSEL_NOTABLE_PERCENTILE = 0.90
_VESSEL_SCORE_FLOOR = 0.35
_VESSEL_SCORE_CEILING = 0.92


logger = logging.getLogger("enrichment.anomaly_scorer")

DYNAMIC_NORMALIZE_LUA = """
local key_base = KEYS[1]
local raw_val = tonumber(ARGV[1])
local alpha = tonumber(ARGV[2])

local mean_key = key_base .. ":mean"
local var_key = key_base .. ":var"

local mean = redis.call('GET', mean_key)
local var = redis.call('GET', var_key)

local m = mean and tonumber(mean) or raw_val
local v = var and tonumber(var) or 1.0

local std_dev = math.sqrt(v) + 1e-5
local norm_score = (raw_val - m) / std_dev

local new_m = (alpha * raw_val) + ((1.0 - alpha) * m)
local new_v = (alpha * (raw_val - m)^2) + ((1.0 - alpha) * v)

redis.call('SET', mean_key, tostring(new_m), 'EX', 604800)
redis.call('SET', var_key, tostring(new_v), 'EX', 604800)

return tostring(norm_score)
"""

# The significance gate.
#
# `mean + z*std` is an unbounded test applied to a bounded variable, and for
# four of the six live domains it had drifted past the top of the scale: to be
# significant, a crypto_trade needed 1.30, a crypto_candle 1.11, a kinematic
# event 1.03, and a bgp_anomaly 0.90 against a maximum observed score of 0.85.
# A domain whose scores are both high and variable could never produce a
# significant event, which is exactly backwards -- and the feedback made it
# worse over time, since every anomaly a domain emits raises its own bar.
#
# The form was the problem, not the constant. `mean + z*std` is an additive
# offset on a variable that cannot exceed 1.0, so for a wide distribution the
# bar leaves the scale entirely, and for a tight one it sits just above the
# maximum -- either way, unreachable. Capping it does not help: a cap turns the
# gate into "above this domain's average", which fired on two thirds of a flat
# series when measured.
#
# So the test is now the same null model the gap detectors use: where does this
# score sit in the distribution of what this domain actually produces? That is
# always answerable, always reachable, and fixes the false-alarm rate at
# roughly 1 - SIGNIFICANCE_PERCENTILE by construction -- which is what the
# conformal calibrator's target_far was reaching for and could not deliver
# through an additive threshold.
EMA_GATEKEEPER_LUA = """
local samples_key = KEYS[1]
local cap = tonumber(ARGV[1])
local min_samples = tonumber(ARGV[2])
local pct = tonumber(ARGV[3])

local raw = redis.call('LRANGE', samples_key, 0, cap - 1)
local vals = {}
for i = 1, #raw do
    local n = tonumber(raw[i])
    if n then table.insert(vals, n) end
end

local threshold = nil
if #vals >= min_samples then
    table.sort(vals)
    local idx = math.ceil(pct * #vals)
    if idx < 1 then idx = 1 end
    if idx > #vals then idx = #vals end
    threshold = vals[idx]
end

local results = {}
for i = 4, #ARGV do
    local score = tonumber(ARGV[i])
    if threshold ~= nil and score > threshold then
        table.insert(results, 1)
    else
        table.insert(results, 0)
    end
    redis.call('LPUSH', samples_key, tostring(score))
end

redis.call('LTRIM', samples_key, 0, cap - 1)
redis.call('EXPIRE', samples_key, 604800)

return results
"""

# How many recent scores define a domain's distribution, and the minimum before
# the gate is allowed to call anything significant. Mirrors the gap detectors'
# _GAP_SAMPLE_CAP / _MIN_GAP_SAMPLES, for the same reason.
SIGNIFICANCE_SAMPLE_CAP = 500
SIGNIFICANCE_MIN_SAMPLES = 60

# Where in its own distribution a score becomes notable. The same 0.90 the gap
# detectors use, and it fixes the false-alarm rate at roughly 10% by
# construction -- which is what the conformal calibrator's target_far was
# reaching for and could not deliver through an additive threshold.
SIGNIFICANCE_PERCENTILE = 0.90

# BGP hijack scoring. A confirmed hijack starts here and the structural signals
# decide where in the remaining headroom it lands, so two hijacks stay rankable
# against each other rather than both sitting on the ceiling.
HIJACK_BASE_SCORE = 0.70

# Shares of the headroom above the base. They sum to 1.0, so only the most
# extreme event reaches 1.0.
HIJACK_NOVELTY_WEIGHT = 0.5
HIJACK_CENTRALITY_WEIGHT = 0.3
HIJACK_VELOCITY_WEIGHT = 0.2

# Two features the graph can measure without the GDS plugin. Weighted below the
# three above because they describe the announcing AS rather than the event, and
# scaled against a reference degree so a well-connected transit provider does
# not saturate the term on its own.
BGP_DEGREE_WEIGHT = 0.25
BGP_SPECIFICITY_WEIGHT = 0.25
BGP_DEGREE_REFERENCE = 200.0


def _as_float(value):
    """Redis returns bytes, strings or None; a bad value is not a measurement."""
    if value is None:
        return None
    try:
        return float(value.decode() if isinstance(value, (bytes, bytearray)) else value)
    except (TypeError, ValueError):
        return None


# Burst detection. A boost is earned by exceeding an entity's own established
# rate, not by repeating: the previous implementation added 0.05 per repeat and
# capped at 0.20, so any entity emitting four or more events in the window
# scored the maximum forever.
BURST_RATIO_THRESHOLD = 2.0      # twice its normal rate before anything counts
FREQUENCY_BOOST_PER_MULTIPLE = 0.05
FREQUENCY_BOOST_CAP = 0.20
BASELINE_EMA_ALPHA = 0.25        # slow enough that one odd window is not the new normal
BASELINE_RETENTION_WINDOWS = 24  # remember an entity's rate for a day of windows


# The most of a score's headroom that all boosts together may consume.
MAX_TOTAL_LIFT_SHARE = float(os.getenv("ANOMALY_MAX_TOTAL_LIFT", "0.55"))


def lift_score(anomaly: float, weight: float, spent: float = 0.0) -> float:
    """Raises a score by a share of the headroom above it, within a budget.

    The alternative -- `min(1.0, anomaly + boost)` -- is why boosted events
    cluster on the ceiling: addition has no notion of how much room is left, so
    a 0.85 score with two 0.15 boosts is simply 1.0, indistinguishable from a
    0.99 score with the same boosts. The tradfi enricher was converted to a
    headroom lift for exactly this reason; the crypto paths were not, and their
    candle events still piled a third of the population into the top decile
    after the detector itself had been recalibrated.

    `spent` is the fraction of headroom already consumed on this event, so a
    sequence of boosts shares one allowance instead of each taking a share of
    what the last one left.
    """
    try:
        base = float(anomaly)
        w = float(weight)
    except (TypeError, ValueError):
        return anomaly
    if not (0.0 <= base <= 1.0) or w <= 0:
        return anomaly
    remaining = max(0.0, MAX_TOTAL_LIFT_SHARE - max(0.0, float(spent)))
    if remaining <= 0.0:
        return anomaly
    return round(base + (1.0 - base) * min(remaining, w), 6)


# The significance cut used before a domain has calibrated one, and where Redis
# is unavailable. The value the no-Redis path has always used, kept so that an
# uncalibrated domain behaves exactly as it did.
SIGNIFICANCE_FALLBACK_CUT = 0.60


class DynamicAnomalyScorer:
    def __init__(self, redis_client, hawkes_tracker: Optional[HawkesIntensityTracker] = None, neo4j_client=None):
        if redis_client is None:
            raise ValueError("Redis client is required for DynamicAnomalyScorer to function properly.")
        self.redis = redis_client
        self.alpha = 0.1
        self.z_score_threshold = 1.5
        
        self._thresholds_cache = {}
        self._thresholds_last_loaded = 0.0
        self._thresholds_ttl = 60.0  # 60 seconds config cache TTL
        # Said once per process, not once per event.
        self._thresholds_announced = False

        # ── PER-DOMAIN STREAMING RRCF DETECTORS (§1.1) ───────────────────────
        # Distinct, independent RRCF models for all 8 Sentinel domains
        self._rrcf_detectors: Dict[str, RRCFDetector] = {
            "maritime":   RRCFDetector(num_trees=40, window_size=256),
            "aviation":   RRCFDetector(num_trees=40, window_size=256),
            "tradfi":     RRCFDetector(num_trees=40, window_size=256, shingle_size=3),
            "crypto":     RRCFDetector(num_trees=40, window_size=256, shingle_size=3),
            "macro":      RRCFDetector(num_trees=30, window_size=256),
            "cyber":      RRCFDetector(num_trees=30, window_size=128),
            "news":       RRCFDetector(num_trees=30, window_size=256),
            "prediction": RRCFDetector(num_trees=30, window_size=256),
        }

        # ── CONFORMAL Z-SCORE CALIBRATORS (§1.3) ─────────────────────────────
        # Per-domain conformal calibrators for dynamic false-alarm rate bounds
        # Calibrated on the scale the detectors actually emit.
        #
        # These were ConformalZScoreCalibrator, which clamps to [1.0, 3.5]
        # because a z lives there. They are fed detector scores, which live in
        # [0, 0.995] -- so every one of them pinned to exactly 1.0, a threshold
        # no score can reach. The default of 0.60 below is the same cut the
        # no-Redis path has always used, so an uncalibrated domain behaves
        # exactly as before and calibration only ever refines it.
        self._conformal_z_calibrators: Dict[str, ConformalScoreCalibrator] = {
            domain: ConformalScoreCalibrator(
                domain=domain, target_far=0.05, default_z_threshold=SIGNIFICANCE_FALLBACK_CUT
            )
            for domain in self._rrcf_detectors.keys()
        }

        # ── MODEL REGISTRY (§1.5) ────────────────────────────────────────────
        self.registry = ModelRegistry(redis_client=redis_client)
        for domain, detector in self._rrcf_detectors.items():
            self.registry.register_model(
                domain=domain,
                model_type="RRCF",
                parameters={"num_trees": detector.num_trees, "window_size": detector.window_size},
                num_samples=0,
            )

        # Per-entity Kalman filters for kinematic domains (keyed by MMSI/ICAO24)
        self._kalman_filters: Dict[str, KalmanResidualFilter] = {}
        self._kalman_max_entities = 10000  # LRU eviction threshold

        # Multivariate Hawkes process tracker (shared across all enrichers)
        self.hawkes = hawkes_tracker or HawkesIntensityTracker()

        # First Story Detection for news novelty scoring
        self._fsd = FirstStoryDetector(window_size=500)

        # BGP graph-topology feature extractor
        self._bgp_extractor = BGPGraphFeatureExtractor(neo4j_client=neo4j_client)

        logger.info("⚡ Per-domain streaming anomaly detectors & ModelRegistry initialized (8 domain RRCFs + Conformal Z + Kalman + Hawkes + FSD + BGP)")

    async def load_thresholds(self):
        """Loads dynamic ML thresholds from Redis cache."""
        import time, json
        now = time.time()
        if self._thresholds_cache and (now - self._thresholds_last_loaded < self._thresholds_ttl):
            return self._thresholds_cache

        try:
            if self.redis and hasattr(self.redis, "raw"):
                raw_cfg = await self.redis.raw.get("sentinel:ml:thresholds")
                if raw_cfg:
                    self._thresholds_cache = json.loads(raw_cfg)
                    self._thresholds_last_loaded = now
        except Exception as e:
            logger.debug(f"Could not load custom ML thresholds: {e}")
        return self._thresholds_cache

    async def _get_thresholds_config(self, key: str) -> dict:
        """Operator overrides for a scorer's constants, from sentinel:ml:thresholds.

        Two things were wrong here, and they hid each other.

        The key has never existed. Every scorer is written as
        `config = {hardcoded defaults}; config.update(await
        self._get_thresholds_config(name))`, so the update was always empty and
        the hardcoded value always won -- vessel_dark's 48-hour divisor, the
        prediction-trade divisor and the rest were effectively constants while
        reading like configuration. That is now said out loud, once, so the
        control surface is discoverable instead of theoretical.

        And the absence was never cached: `_thresholds_last_loaded` was only
        advanced when a value came back, so a missing key meant a fresh Redis
        GET on every scored event rather than one per TTL. At 240,984 aviation
        events in 48 hours that is a round trip per event to learn the same
        nothing.
        """
        now = time.time()
        if now - self._thresholds_last_loaded < self._thresholds_ttl:
            return self._thresholds_cache.get(key, {})

        # Advanced before the read, so a miss and an error are both cached for
        # the TTL rather than retried on the hot path.
        self._thresholds_last_loaded = now
        try:
            if self.redis:
                raw_cfg = await self.redis.raw.get("sentinel:ml:thresholds")
                if raw_cfg:
                    self._thresholds_cache = json.loads(raw_cfg)
                    if not self._thresholds_announced:
                        self._thresholds_announced = True
                        logger.info(
                            "Scorer threshold overrides loaded from sentinel:ml:thresholds: %s",
                            sorted(self._thresholds_cache.keys()),
                        )
                elif not self._thresholds_announced:
                    self._thresholds_announced = True
                    logger.info(
                        "No sentinel:ml:thresholds key present; every scorer is running on its "
                        "hardcoded defaults. Write a JSON object keyed by scorer name to that "
                        "key to override them."
                    )
        except Exception as e:
            logger.debug(f"Could not load custom ml thresholds from Redis: {e}")

        return self._thresholds_cache.get(key, {})
        
    # Event types whose feature vectors are a different quantity from the rest
    # of their domain's and therefore need their own detector history.
    #
    # A detector's score is an empirical percentile against what it has recently
    # seen, which is only meaningful if the observations are commensurable. The
    # crypto detector was being fed four unrelated feature families:
    #
    #   crypto_trade          [normalised notional z, normalised qty z, 0, 0, 0]
    #   crypto_perp_funding   [funding bps, basis bps, mark/index ratio, 0, 0]
    #   crypto_candle         [range %, body %, normalised notional, ...]
    #   crypto_liquidation    [notional, ...]
    #
    # Funding rates in basis points are numerically an order of magnitude above
    # normalised z-scores, so a funding observation was extreme against a history
    # made mostly of trades no matter what the funding rate actually was.
    # Measured: 97.9% of published crypto_perp_funding events scored above 0.8,
    # and 100% of crypto_trade did. The percentile was ranking feature families
    # against each other rather than each event against its own kind.
    _OWN_DETECTOR_EVENT_TYPES = frozenset({
        "crypto_perp_funding",
        "crypto_candle",
        "crypto_liquidation",
        "options_flow",
    })

    def _detector_key(self, event_type: str) -> str:
        """Which detector history this event type is scored against.

        Its own where the feature vector is a different quantity from the rest of
        the domain's, the shared domain detector otherwise -- pooling is what
        gives a sparse domain enough history for a percentile to mean anything,
        and is right wherever the features are commensurable.
        """
        evt = (event_type or "").lower()
        if evt in self._OWN_DETECTOR_EVENT_TYPES:
            return evt
        return self._get_domain(event_type)

    def _detector_for(self, event_type: str) -> Optional[RRCFDetector]:
        """The detector for this event type, created on first use.

        Built from the same parameters as its domain's, so an event type that
        splits off is not also silently re-tuned.
        """
        key = self._detector_key(event_type)
        detector = self._rrcf_detectors.get(key)
        if detector is not None:
            return detector

        domain = self._get_domain(event_type)
        domain_detector = self._rrcf_detectors.get(domain)
        if domain_detector is None:
            return None
        detector = RRCFDetector(
            num_trees=domain_detector.num_trees,
            window_size=domain_detector.window_size,
            shingle_size=getattr(domain_detector, "shingle_size", 1),
        )
        self._rrcf_detectors[key] = detector
        logger.info(
            "Split %s onto its own detector: its features are not commensurable "
            "with the rest of the %s domain's.",
            key, domain,
        )
        return detector

    def _get_domain(self, event_type: str) -> str:
        """Map event type to explicit per-domain model key (§1.1)."""
        evt = (event_type or "").lower()
        if any(k in evt for k in ["vessel", "mmsi", "ais", "dark", "sts"]):
            return "maritime"
        elif any(k in evt for k in ["flight", "icao", "adsb", "aircraft"]):
            return "aviation"
        elif any(k in evt for k in ["equity", "stock", "option", "tradfi", "financial"]):
            return "tradfi"
        elif any(k in evt for k in ["crypto", "token", "liquidation", "candle"]):
            return "crypto"
        elif any(k in evt for k in ["macro", "cpi", "gdp", "rate", "fed"]):
            return "macro"
        elif any(k in evt for k in ["bgp", "cyber", "dns", "ddos", "hijack"]):
            return "cyber"
        elif any(k in evt for k in ["news", "headline", "narrative"]):
            return "news"
        elif any(k in evt for k in ["prediction", "polymarket", "kalshi"]):
            return "prediction"
        return "tradfi"



    async def _dynamic_normalize(self, ticker: str, feature_name: str, raw_value: float) -> float:
        res = await self._dynamic_normalize_batch([(ticker, feature_name, raw_value)])
        return res[0]
        
    async def _dynamic_normalize_batch(self, requests: list) -> list:
        # requests = [(ticker, feature_name, raw_value), ...]
        if not requests:
            return []
        if not self.redis or not getattr(self.redis, "raw", None):
            return [r[2] / 1000.0 for r in requests]
            
        pipe = self.redis.raw.pipeline()
        for ticker, feature_name, raw_value in requests:
            key_base = f"sentinel:stats:{ticker}:{feature_name}"
            pipe.eval(DYNAMIC_NORMALIZE_LUA, 1, key_base, float(raw_value), float(self.alpha))
        
        res = await pipe.execute()
        return [float(r) for r in res]

    async def _check_ema_gatekeeper(self, event_type: str, raw_score: float) -> bool:
        res = await self._check_ema_gatekeeper_batch(event_type, [raw_score])
        return res[0]
        
    async def _check_ema_gatekeeper_batch(self, event_type: str, raw_scores: list) -> list:
        if not raw_scores:
            return []

        # The per-domain conformal cut for this event type.
        domain = self._get_domain(event_type)
        calibrator = self._conformal_z_calibrators.get(domain)
        score_thresh = calibrator.z_threshold if calibrator else SIGNIFICANCE_FALLBACK_CUT

        # Observe scores to dynamically calibrate the conformal threshold.
        if calibrator:
            for score in raw_scores:
                calibrator.observe(score)

        if not self.redis or not getattr(self.redis, "raw", None):
            # Read, not discarded.
            #
            # This branch used a hardcoded 0.60 while the calibrated threshold
            # sat unused in a local three lines above -- the platform's only
            # self-tuning control, computed on every batch and thrown away.
            # What the calibration buys is that the *false-alarm rate* is what
            # holds constant across domains rather than the number: 0.60 means
            # something different in a domain whose scores cluster high than in
            # one whose scores cluster low, and the domains here differ exactly
            # that way.
            return [score > score_thresh for score in raw_scores]

        samples_key = f"sentinel:ml:score_samples:{event_type}"

        res = await self.redis.raw.eval(
            EMA_GATEKEEPER_LUA, 1, samples_key,
            int(SIGNIFICANCE_SAMPLE_CAP), int(SIGNIFICANCE_MIN_SAMPLES),
            float(SIGNIFICANCE_PERCENTILE),
            *[float(s) for s in raw_scores]
        )
        return [bool(r) for r in res]

    async def check_cusum_drift(self, entity_id: str, series: list, threshold: float = 4.0) -> bool:
        """
        Applies CUSUM change-point detection on a rolling feature/return series.
        Returns True if a structural regime drift is detected.
        """
        from shared.utils import quant_calc
        change_points = quant_calc.cusum_change_detection(series, threshold=threshold)
        return len(change_points) > 0
    
    async def score_event(self, event_type: str, entity_id: str, features: list) -> dict:
        res = await self.score_event_batch(event_type, [entity_id], [features])
        return res[0]
        
    # Score samples retained for drift comparison. A thousand is enough for a
    # stable PSI and small enough that the list stays a rounding error in Redis.
    DRIFT_SAMPLE_CAP = 1000
    DRIFT_SAMPLE_TTL_SEC = 14 * 86400

    async def _record_score_sample(self, scores: list) -> None:
        """Records anomaly scores for the model-drift monitor.

        Best-effort and never raises: this sits on the hot scoring path, and a
        telemetry write must not be able to fail an enrichment.
        """
        if not scores or not self.redis or not getattr(self.redis, "raw", None):
            return
        try:
            pipe = self.redis.raw.pipeline()
            for score in scores[:50]:
                pipe.lpush("sentinel:ml:current_scores", float(score))
            pipe.ltrim("sentinel:ml:current_scores", 0, self.DRIFT_SAMPLE_CAP - 1)
            pipe.expire("sentinel:ml:current_scores", self.DRIFT_SAMPLE_TTL_SEC)
            await pipe.execute()
        except Exception as e:
            logger.debug("Drift sample not recorded: %s", e)

    async def score_event_batch(self, event_type: str, entities: list, features_list: list) -> list:
        """Score events using streaming RRCF detectors (replaces ONNX IsolationForest)."""
        domain = self._get_domain(event_type)
        detector = self._detector_for(event_type)
        
        if not detector or not features_list:
            MetricsCollector.increment("streaming_scoring_uninitialized_total")
            return [{"score": 0.5, "is_significant": False, "domain": domain, "scoring_degraded": True}
                    for _ in features_list]
            
        try:
            loop = asyncio.get_running_loop()
            points = [np.array(f, dtype=np.float32) for f in features_list]
            
            # RRCF insert is sequential per-tree but fast (~0.1ms per point per tree)
            scores = await loop.run_in_executor(None, detector.insert_batch, points)
            
            is_significant_list = await self._check_ema_gatekeeper_batch(event_type, scores)
            # Feed the drift monitor, which had nothing to measure.
            #
            # The scheduler reads sentinel:ml:current_scores and
            # sentinel:ml:baseline_scores, needs twenty observations in each,
            # and nothing wrote either -- so it returned
            # {"drift_detected": false, "psi": 0.0, "status": "initializing"}
            # every hour for the life of the deployment, reporting an absence
            # as a finding.
            await self._record_score_sample(scores)
            return [{"score": round(s, 4), "is_significant": sig, "domain": domain}
                    for s, sig in zip(scores, is_significant_list)]
                
        except Exception as e:
            MetricsCollector.increment("streaming_scoring_failures_total")
            MetricsCollector.increment(f"streaming_scoring_failures_{domain}")
            logger.error(f"Streaming scoring failed for {event_type}: {e}", exc_info=True)
            return [{"score": 0.5, "is_significant": False, "domain": domain, "scoring_degraded": True}
                    for _ in features_list]

    # ── KINEMATIC SCORING (Maritime + Aviation) ──────────────────────────────

    def _get_or_create_kalman(self, entity_id: str) -> KalmanResidualFilter:
        """Get or create a Kalman filter for a kinematic entity (MMSI/ICAO24)."""
        if entity_id not in self._kalman_filters:
            # LRU eviction: remove oldest if at capacity
            if len(self._kalman_filters) >= self._kalman_max_entities:
                oldest_key = next(iter(self._kalman_filters))
                del self._kalman_filters[oldest_key]
            self._kalman_filters[entity_id] = KalmanResidualFilter()
        return self._kalman_filters[entity_id]

    async def score_kinematic_event(
        self,
        entity_id: str,
        lat: float, lon: float, speed: float, heading: float,
        timestamp: float,
        extra_features: Optional[list] = None,
    ) -> dict:
        """Score a single kinematic event using Kalman residuals + RRCF."""
        res = await self.score_kinematic_event_batch(
            [entity_id], [lat], [lon], [speed], [heading], [timestamp],
            [extra_features] if extra_features else None,
        )
        return res[0]

    async def score_kinematic_event_batch(
        self,
        entities: list,
        lats: list, lons: list, speeds: list, headings: list,
        timestamps: list,
        extra_features_list: Optional[list] = None,
    ) -> list:
        """
        Score kinematic events using Kalman prediction residuals fed into RRCF.
        
        The Kalman filter predicts where each entity should be based on its
        previous trajectory. The *residuals* (predicted vs actual) are the
        features that catch spoofing, dark-period jumps, and impossible maneuvers.
        """
        # Determine specific kinematic domain (maritime vs aviation) (§1.1)
        sample_entity = str(entities[0]).lower() if entities else ""
        domain = "aviation" if sample_entity.startswith("icao") or "adsb" in sample_entity else "maritime"
        detector = self._rrcf_detectors.get(domain) or self._rrcf_detectors.get("maritime")
        if not detector:
            return [{"score": 0.5, "is_significant": False, "domain": domain, "scoring_degraded": True}
                    for _ in entities]

        results = []
        points = []
        for i, entity_id in enumerate(entities):
            kf = self._get_or_create_kalman(entity_id)
            residuals = kf.predict_and_update(
                lats[i], lons[i], speeds[i], headings[i], timestamps[i]
            )

            # Feature vector: Kalman residuals + raw kinematic features
            feat = [
                residuals["residual_distance"],
                residuals["residual_speed"],
                residuals["residual_heading"] / 180.0,  # Normalize to [0, 1]
                speeds[i] / 30.0,  # Normalize speed (max ~30 kts for vessels)
                residuals["prediction_confidence"],
            ]
            # Append extra features if provided (e.g., region multiplier, sanctions flag)
            if extra_features_list and i < len(extra_features_list) and extra_features_list[i]:
                feat.extend(extra_features_list[i])

            points.append(np.array(feat, dtype=np.float32))
            results.append(residuals)

        loop = asyncio.get_running_loop()
        scores = await loop.run_in_executor(None, detector.insert_batch, points)
        is_significant_list = await self._check_ema_gatekeeper_batch("kinematic", scores)

        final = []
        for i, (score, sig, residuals) in enumerate(zip(scores, is_significant_list, results)):
            final.append({
                "score": round(score, 4),
                "is_significant": sig,
                "domain": "spatial",
                "residual_distance": residuals["residual_distance"],
                "residual_speed": residuals["residual_speed"],
                "residual_heading": residuals["residual_heading"],
                "prediction_confidence": residuals["prediction_confidence"],
            })
        return final

    # ── HAWKES INTENSITY ─────────────────────────────────────────────────────

    def get_hawkes_intensity(self, domain: str) -> float:
        """Get current Hawkes process excitation ratio for a domain."""
        return self.hawkes.get_excitation_ratio(domain, time.time())

    def record_hawkes_event(self, domain: str) -> dict:
        """Record an anomalous event in the Hawkes tracker."""
        return self.hawkes.record_event(domain, time.time())

    # ── NEWS NOVELTY SCORING (First Story Detection) ─────────────────────────

    async def score_news_novelty(
        self,
        headline: str,
        summary: str,
        named_entities: list,
        sentiment: float,
        reliability: float,
    ) -> Tuple[float, list]:
        """
        Score news event using First Story Detection (TDT novelty) as the
        primary signal, with sentiment/reliability/entities as secondary modifiers.
        
        Replaces the old sentiment × reliability formula which measured the
        wrong thing: what we actually want is novelty relative to known stories.
        """
        config = {"entity_boost": 0.02, "max_boost": 0.3}
        try:
            cfg = await self._get_thresholds_config("news")
            config.update(cfg)
        except Exception:
            pass

        # Primary signal: First Story Detection novelty
        loop = asyncio.get_running_loop()
        novelty = await loop.run_in_executor(
            None, self._fsd.score_novelty, headline, summary
        )

        # Secondary modifiers (same as before, but additive on novelty base)
        semantic_boost = 0.0
        semantic_tags = []
        watchlist_boost = 0.0
        frequency_boost = 0.0

        if self.redis and getattr(self.redis, "raw", None) is not None and named_entities:
            pipe = self.redis.raw.pipeline()
            for tag in named_entities:
                pipe.get(f"sentinel:semantic_sentiment:{tag.lower()}")
                pipe.zscore("sentinel:watched:equities", tag)
                pipe.zscore("sentinel:watched:vessels", tag)
                
            results = await pipe.execute()
            vals = []
            
            for i, tag in enumerate(named_entities):
                res = results[i*3]
                is_eq_zset = results[i*3 + 1]
                is_ves_zset = results[i*3 + 2]

                if res:
                    val = float(res)
                    semantic_boost += abs(val) * 0.1
                    vals.append(val)
                    tag_label = "positive" if val > 0 else "critical" if val <= -1.5 else "negative"
                    semantic_tags.append(f"semantic:{tag_label}")

                if (is_eq_zset is not None) or (is_ves_zset is not None):
                    watchlist_boost = 0.15

                f_boost = await self.track_frequency(tag, "news")
                frequency_boost = max(frequency_boost, f_boost)

            if vals:
                blended_val = sum(vals) / len(vals)
                sentiment = sentiment * 0.5 + blended_val * 0.5

        # Composite: novelty is primary, modifiers are additive
        entity_boost = min(config["max_boost"], len(named_entities) * config["entity_boost"])
        
        # Sentiment extremity adds to novelty (novel + extreme sentiment = high priority)
        sentiment_modifier = abs(sentiment) * 0.15
        
        # Reliability scales the whole score (unreliable source dampens even novel stories)
        scaled_reliability = 0.5 + 0.5 * reliability
        
        final_score = novelty * 0.55 + sentiment_modifier + entity_boost + semantic_boost + watchlist_boost + frequency_boost
        final_score = round(min(1.0, final_score * scaled_reliability), 3)
        
        return final_score, semantic_tags

    # ── BGP GRAPH-TOPOLOGY SCORING ───────────────────────────────────────────

    async def score_bgp_event(
        self,
        origin_as: str,
        prefix: str,
        is_hijack: bool,
        velocity: float,
        as_path: Optional[list] = None,
    ) -> dict:
        """
        Score a BGP event using graph-structural features from Neo4j + RRCF.
        
        BGP anomalies are fundamentally graph-structural (a hijack is a topology
        violation). Flat feature scoring throws away the one signal that actually
        distinguishes a hijack from noise.
        """
        # 1. Extract graph features from Neo4j
        graph_features = await self._bgp_extractor.extract_features(origin_as, prefix)
        
        # 2. Upsert AS-path into Neo4j for future novelty detection
        await self._bgp_extractor.upsert_as_path(origin_as, prefix, as_path)
        
        # 3. Build feature vector for RRCF
        feat = np.array([
            graph_features["betweenness_centrality"],
            graph_features["degree"],
            graph_features["path_novelty"],
            min(1.0, velocity),  # Normalized velocity score
            graph_features["prefix_specificity"],
        ], dtype=np.float32)
        
        # 4. Score through RRCF cyber detector
        detector = self._rrcf_detectors.get("cyber")
        if detector:
            loop = asyncio.get_running_loop()
            rrcf_score = await loop.run_in_executor(None, detector.insert, feat)
        else:
            rrcf_score = 0.5
        
        # 5. Blend the structural signals instead of stacking a floor and a
        #    multiplier into the ceiling.
        #
        #    This raised a hijack to a floor of 0.85 and then multiplied a novel
        #    AS-path by 1.3 -- 1.105, clamped to 1.0. A previously unseen
        #    AS/prefix pair is novel by definition the first time it appears, so
        #    virtually every hijack landed on exactly 1.0: measured over 24
        #    hours, all 1,852 bgp_anomaly events shared a single distinct score.
        #    A detector whose output never varies ranks nothing.
        #
        #    Each signal now takes a bounded share of the headroom above the
        #    base, so a hijack on a high-centrality AS with a novel path still
        #    reaches 1.0 while an ordinary one sits well below it, and the two
        #    stay comparable.
        base = HIJACK_BASE_SCORE if is_hijack else rrcf_score
        headroom = max(0.0, 1.0 - base)
        #    The blend above is correct and its inputs were not.
        #
        #    Re-measured after that fix: bgp_anomaly still carried exactly two
        #    distinct scores across 2,723 events a day. The arithmetic explains
        #    it. betweenness_centrality is permanently 0 -- this Neo4j has no GDS
        #    plugin -- velocity is a step function that returns exactly 0.0 below
        #    its threshold and a BGP prefix rarely repeats a hundred times in a
        #    minute, and path_novelty is 1.0 for a hijack by definition. Three
        #    weighted terms, two structurally zero and one structurally one, so
        #    a hijack always scored 0.70 + 0.30 x 0.5 = 0.85.
        #
        #    degree and prefix_specificity do vary per AS and per prefix, are
        #    already extracted, and were only being fed to the RRCF detector --
        #    whose answer is then discarded for hijacks, because `base` replaces
        #    it. Including them is what gives the detector something to rank by
        #    on the evidence this deployment can actually collect.
        degree = float(graph_features.get("degree") or 0.0)
        # Log-scaled: an AS with 400 peers is not forty times more interesting
        # than one with ten, and the raw count would swamp every other term.
        degree_signal = min(1.0, math.log1p(degree) / math.log1p(BGP_DEGREE_REFERENCE))
        contribution = (
            HIJACK_NOVELTY_WEIGHT * min(1.0, float(graph_features.get("path_novelty") or 0.0))
            + HIJACK_CENTRALITY_WEIGHT * min(1.0, float(graph_features.get("betweenness_centrality") or 0.0))
            + HIJACK_VELOCITY_WEIGHT * min(1.0, float(velocity or 0.0))
            + BGP_DEGREE_WEIGHT * degree_signal
            + BGP_SPECIFICITY_WEIGHT * min(1.0, float(graph_features.get("prefix_specificity") or 0.0))
        )
        rrcf_score = round(min(1.0, base + headroom * min(1.0, contribution)), 4)
        
        is_significant = await self._check_ema_gatekeeper("bgp_anomaly", rrcf_score)
        
        return {
            "score": round(rrcf_score, 4),
            "is_significant": is_significant,
            "path_novelty": graph_features["path_novelty"],
            "centrality": graph_features["betweenness_centrality"],
            "degree": graph_features["degree"],
            "prefix_specificity": graph_features["prefix_specificity"],
            "domain": "cyber",
        }

    async def score_vessel_dark(self, mmsi: str, gap_hours: float, region: Optional[str], flags: list, heading: int) -> float:
        """How unusual this vessel's silence is, for the water it went quiet in.

        A 3-hour gap near a known STS transfer zone is NOT the same event as a
        3-hour gap in open ocean — per current maritime-intel practice.

        This scored `gap_hours / 48.0`, which is the shape the aviation detector
        was rebuilt away from during this audit. The two are the same
        phenomenon — a tracked object stops transmitting — and they were scored
        six times apart: over thirty hours, 906 flight_dark events averaged
        0.632 while 46 vessel_dark events averaged 0.111, and no vessel cleared
        0.2. An absolute divisor cannot express "unusual", only "long": it says
        a ship must be silent for two full days before its silence means
        anything, regardless of whether every ship in that region reports
        hourly or daily.

        The null model is the same one aviation uses. Record what gaps this
        region actually produces, and ask where this one sits among them.
        """
        # Every gap feeds the distribution, including the unremarkable ones.
        # A distribution built only from gaps that scored well is truncated at
        # its own threshold, which is the circularity that makes every
        # observation look extreme.
        await self._record_vessel_gap(region, gap_hours)

        samples = await self._vessel_gap_samples(region)
        if len(samples) < _MIN_VESSEL_GAP_SAMPLES:
            # Not enough history to say anything about this region yet. The old
            # absolute divisor is the bootstrap rather than the answer, and it
            # is deliberately kept so a cold start still produces an ordering.
            config = {"base_divisor": 48.0, "sanctioned_multiplier": 1.5}
            try:
                config.update(await self._get_thresholds_config("vessel_dark"))
            except Exception as e:
                logger.debug("vessel_dark threshold config unavailable: %s", e)
            base = min(1.0, gap_hours / config["base_divisor"])
        else:
            rank = quant_calc.percentile_rank(samples, gap_hours)
            if rank < _VESSEL_NOTABLE_PERCENTILE:
                # More ordinary than 90% of what this region produces. Scored
                # low rather than suppressed: unlike the aviation detector this
                # is a scoring function, and its caller has already decided the
                # event exists.
                base = round(_VESSEL_SCORE_FLOOR * (rank / max(1e-9, _VESSEL_NOTABLE_PERCENTILE)), 4)
            else:
                tail = (rank - _VESSEL_NOTABLE_PERCENTILE) / max(1e-9, 1.0 - _VESSEL_NOTABLE_PERCENTILE)
                base = round(_VESSEL_SCORE_FLOOR + tail * (_VESSEL_SCORE_CEILING - _VESSEL_SCORE_FLOOR), 4)
            config = {"sanctioned_multiplier": 1.5}
            try:
                config.update(await self._get_thresholds_config("vessel_dark"))
            except Exception as e:
                logger.debug("vessel_dark threshold config unavailable: %s", e)

        if "sanctioned" in " ".join(flags).lower():
            # Headroom lift, not multiplication.
            #
            # `min(1.0, base * 1.5)` is the same ceiling-clustering defect the
            # additive form had, in a shape the earlier sweep did not look for:
            # any base above 0.67 lands on exactly 1.0. Live on 4 September, 67
            # vessel_dark events sat at 1.000 -- a detector reporting certainty
            # about a vessel that stopped transmitting, which is the one thing
            # AIS silence can never establish.
            base = lift_score(base, config.get("sanctioned_multiplier", 1.5) - 1.0)

        # Contextual gap significance: STS transfer zone proximity multiplier
        zone_mult = sts_zone_risk_multiplier(region)
        # Same lift, for the same reason. zone_mult reaches 3.0 in the watched
        # chokepoints, which multiplied any base above 0.33 straight to the top.
        base = lift_score(base, max(0.0, zone_mult - 1.0))

        # Bounded below certainty, like every other detector in this system.
        return round(min(FALLBACK_MAX_SCORE, base), 3)

    async def _record_vessel_gap(self, region: Optional[str], gap_hours: float) -> None:
        """Adds this observation to its region's empirical gap distribution."""
        try:
            if not self.redis or gap_hours is None or gap_hours < 0 or gap_hours >= 48.0:
                return
            key = _VESSEL_GAP_SAMPLES_KEY.format(region=region or "Default")
            pipe = self.redis.raw.pipeline()
            pipe.lpush(key, round(float(gap_hours), 3))
            pipe.ltrim(key, 0, _VESSEL_GAP_SAMPLE_CAP - 1)
            pipe.expire(key, 30 * 86400)
            await pipe.execute()
        except Exception as e:
            logger.debug("Failed recording vessel gap sample: %s", e)

    async def _vessel_gap_samples(self, region: Optional[str]) -> list:
        """This region's observed gaps, newest first."""
        try:
            if not self.redis:
                return []
            key = _VESSEL_GAP_SAMPLES_KEY.format(region=region or "Default")
            raw = await self.redis.raw.lrange(key, 0, _VESSEL_GAP_SAMPLE_CAP - 1)
            out = []
            for item in raw or []:
                try:
                    out.append(float(item if isinstance(item, (str, int, float)) else item.decode("utf-8")))
                except (TypeError, ValueError):
                    continue
            return out
        except Exception as e:
            logger.debug("Failed reading vessel gap samples: %s", e)
            return []


    async def score_crypto_trade_batch(self, trades: list) -> list:
        if not trades: return []
        req_notional = [(f"crypto:{t[0]}", "notional", t[1]) for t in trades]
        req_qty = [(f"crypto:{t[0]}", "qty", t[2]) for t in trades]
        
        norm_notional = await self._dynamic_normalize_batch(req_notional)
        norm_qty = await self._dynamic_normalize_batch(req_qty)
        
        features_list = []
        entities = []
        for t, n, q in zip(trades, norm_notional, norm_qty):
            features_list.append([n, q, 0.0, 0.0, 0.0])
            entities.append(t[0])
            
        res = await self.score_event_batch("crypto_trade", entities, features_list)
        return [r["score"] for r in res]

    async def score_crypto_candle(self, asset: str, features: list) -> Dict[str, Any]:
        """Scores a candle. The caller's feature list is left as it was found.

        This normalised in place -- `features[2] = ...` on the list the caller
        passed -- so index 2 stopped being a notional in dollars the moment the
        score was taken, and became a z-score. Every reader downstream still
        believed it was money: the crypto and tradfi structural-anomaly
        headlines both render it as `${notional/1e6:.1f}M vol`, which is how a
        genuine $3.2M ETHUSDT bar came to be published as "on $-0.0M vol" -- a
        z-score of about -0.03, divided by a million.

        Nothing was wrong with the volume. It survives correctly all the way
        into the stored event's size_tokens; only the headline's copy was
        overwritten, by the scoring call sitting between the two reads.

        Scoring is unchanged: the normalised value is still what reaches
        score_event. It is now computed into a copy, so producing a score stops
        being a side effect on the caller's data.
        """
        scored = list(features)
        if len(scored) >= 3:
            scored[2] = await self._dynamic_normalize(f"crypto:{asset}", "candle_notional", scored[2])
        # Full result: the per-domain significance gate's answer travels with
        # the score rather than being computed and dropped.
        return await self.score_event("crypto_candle", asset, (scored + [0.0] * 5)[:5])

    async def score_financial_trade(self, domain: str, ticker: str, notional: float, volume: float) -> float:
        res = await self.score_financial_trade_batch(domain, [(ticker, notional, volume)])
        return float(res[0].get("score", 0.5)) if res else 0.5
        
    async def score_financial_trade_batch(self, domain: str, trades: list) -> list:
        if not trades: return []
        req_notional = [(f"{domain}:{t[0]}", "notional", t[1]) for t in trades]
        req_volume = [(f"{domain}:{t[0]}", "volume", t[2]) for t in trades]
        
        norm_notional = await self._dynamic_normalize_batch(req_notional)
        norm_volume = await self._dynamic_normalize_batch(req_volume)
        
        features_list = []
        entities = []
        for t, n, v in zip(trades, norm_notional, norm_volume):
            features_list.append([n, v, 0.0, 0.0, 0.0])
            entities.append(t[0])
            
        # The full result, not just the score.
        #
        # This returned [r["score"] for r in res] and dropped `is_significant`
        # on the floor -- the calibrated per-domain gate runs on every batch and
        # its answer had no reader anywhere in the platform, while consumers
        # downstream re-derived significance as `anomaly >= 0.65`, a hardcoded
        # constant that knows nothing about the domain's distribution.
        return await self.score_event_batch("tradfi_trade", entities, features_list)

    async def score_market_candle(self, domain: str, ticker: str, features: list) -> Dict[str, Any]:
        """As score_crypto_candle: scores a copy, never the caller's list."""
        scored = list(features)
        if len(scored) >= 3:
            scored[2] = await self._dynamic_normalize(f"{domain}:{ticker}", "candle_notional", scored[2])
        return await self.score_event("tradfi_candle", ticker, (scored + [0.0] * 5)[:5])


    async def score_prediction_anomaly(
        self, 
        asset_id: str, 
        notional: float, 
        delta_p: float = 0.0, 
        yes_val: float = 0.0, 
        no_val: float = 0.0
    ) -> Dict[str, float]:
        """
        Centralized dynamic prediction market anomaly scorer.
        Evaluates trade size/notional, probability change (delta_p), and YES/NO bid sizes.
        Computes dynamic rolling Z-scores for volume surges and probability shifts.
        Returns dict with 'score', 'z_score_volume', and 'z_score_prob'.
        """
        config = {"divisor": 100_000.0}
        try:
            cfg = await self._get_thresholds_config("prediction_trade")
            config.update(cfg)
        except Exception:
            pass

        # Multi-dimensional feature vector: [normalized_notional, abs(delta_p)*10, yes_val/1000, no_val/1000, 0]
        feat = [
            notional / config["divisor"],
            abs(delta_p) * 10.0,
            yes_val / 1000.0,
            no_val / 1000.0,
            0.0
        ]
        res = await self.score_event("prediction_market_trade", asset_id, feat)
        rrcf_score = res.get("score", 0.0)

        # Dynamic rolling Z-scores in Redis
        z_vol = 0.0
        z_prob = 0.0
        try:
            if notional > 0:
                z_vol = await self._dynamic_normalize(f"prediction:{asset_id.lower()}", "volume", notional)
            if abs(delta_p) > 0:
                z_prob = await self._dynamic_normalize(f"prediction:{asset_id.lower()}", "prob_shift", abs(delta_p))
        except Exception:
            pass

        # Purely dynamic combined score based on RRCF and max Z-score significance
        max_z = max(z_vol, z_prob)
        if max_z > 3.0:
            z_boost = min(0.35, (max_z - 3.0) * 0.1)
        elif max_z > 1.5:
            z_boost = (max_z - 1.5) * 0.05
        else:
            z_boost = 0.0

        final_score = min(1.0, max(rrcf_score, z_boost))

        return {
            "score": round(final_score, 4),
            "z_score_volume": round(z_vol, 2),
            "z_score_prob": round(z_prob, 2),
        }

    async def score_prediction_volume_anomaly(self, asset_id: str, notional: float, volume_delta: float = 0.0) -> float:
        res = await self.score_prediction_anomaly(asset_id, max(notional, volume_delta))
        return res["score"]
        
    async def check_watchlist(self, entity_id: str, watchlist_type: str) -> bool:
        """
        Queries Redis to check if an entity (e.g., ticker, MMSI, callsign, wallet) is on a watchlist.
        watchlist_type: 'equities', 'vessels', 'aircraft', 'wallets'
        """
        if not self.redis or not entity_id:
            return False
        try:
            key = f"sentinel:watched:{watchlist_type}"
            is_member = await self.redis.raw.sismember(key, entity_id)
            if is_member:
                return True
            score = await self.redis.raw.zscore(key, entity_id)
            return score is not None
        except Exception:
            return False

    async def track_frequency(self, entity_id: str, domain: str, window_seconds: int = 3600) -> float:
        """Boost for an entity behaving unusually *for itself*.

        This used to return 0.05 per repeat within the window, capped at 0.20 --
        so the fourth and every subsequent event from an entity scored the
        maximum. That rewards repetition, which is the opposite of what an
        anomaly detector is for: an address emitting eleven transfers in thirty
        minutes is the least surprising thing in the stream, and it was earning
        the largest boost. It also produced a constant: every suspect crypto
        transfer scored 0.4 + 0.15 + 0.20 = exactly 0.75, across 29,150 events
        in a day, indistinguishable from one another.

        Burst is now measured against the entity's own rolling baseline. An
        address that always emits 100 events an hour emitting 100 is not
        anomalous; one that normally emits 1 and suddenly emits 20 is. A steady
        stream therefore decays to no boost as the baseline catches up.
        """
        if not self.redis or not entity_id:
            return 0.0
        try:
            ident = entity_id.lower()
            key = f"sentinel:frequency:{domain}:{ident}"
            last_key = f"sentinel:frequency:last:{domain}:{ident}"
            baseline_key = f"sentinel:frequency:baseline:{domain}:{ident}"

            pipe = self.redis.raw.pipeline()
            pipe.incr(key)
            # TTL, not a fresh EXPIRE.
            #
            # Re-arming the expiry on every event meant the counter for an
            # active entity never rolled over: `count` grew monotonically for as
            # long as the entity kept emitting, so the `count <= 1` branch below
            # never ran again and the baseline it advances stayed frozen at its
            # first value. `ratio` then climbed without bound and this returned
            # FREQUENCY_BOOST_CAP forever -- the constant score this rewrite
            # exists to remove, reintroduced for precisely the high-rate
            # entities it was measured on. The window is armed once, when the
            # key is created, and is then allowed to expire.
            pipe.ttl(key)
            pipe.get(baseline_key)
            pipe.get(last_key)
            results = await pipe.execute()

            count = float(results[0] or 0)
            baseline = _as_float(results[2])
            previous_window = _as_float(results[3])

            # -1 is "exists with no expiry", -2 is "gone". Either way the window
            # needs arming, and only then.
            try:
                current_ttl = int(results[1]) if results[1] is not None else -2
            except (TypeError, ValueError):
                current_ttl = -2
            if current_ttl < 0:
                await self.redis.raw.expire(key, window_seconds)

            # The baseline moves only at a window boundary. Updating it on every
            # event lets it chase the count inside the window, so a burst is
            # compared against a baseline the burst itself just raised -- which
            # silently cancels the very signal being measured.
            if count <= 1:
                folded = (
                    previous_window if baseline is None
                    else BASELINE_EMA_ALPHA * previous_window + (1 - BASELINE_EMA_ALPHA) * baseline
                ) if previous_window is not None else baseline
                if folded is not None:
                    baseline = folded
                    await self.redis.raw.set(
                        baseline_key, str(round(folded, 4)),
                        ex=window_seconds * BASELINE_RETENTION_WINDOWS,
                    )

            await self.redis.raw.set(last_key, str(count), ex=window_seconds * 2)

            # First sighting: nothing to compare against. Calling an unknown
            # entity anomalous on its first appearance would make every new
            # entity a maximum-severity event.
            if baseline is None or baseline <= 0:
                return 0.0

            ratio = count / baseline
            if ratio <= BURST_RATIO_THRESHOLD:
                return 0.0
            return round(
                min(FREQUENCY_BOOST_CAP,
                    (ratio - BURST_RATIO_THRESHOLD) * FREQUENCY_BOOST_PER_MULTIPLE),
                4,
            )
        except Exception:
            return 0.0

    async def score_news(self, named_entities: list, sentiment: float, reliability: float,
                         headline: str = "", summary: str = "") -> tuple:
        """Score news using First Story Detection novelty as primary signal.
        
        Falls back to legacy sentiment × reliability formula if no headline is provided
        (backward compatibility for callers that haven't been updated yet).
        """
        if headline:
            # New path: FSD novelty-based scoring
            return await self.score_news_novelty(headline, summary, named_entities, sentiment, reliability)
        
        # Legacy fallback: sentiment × reliability (preserved for backward compat)
        config = {"entity_boost": 0.02, "max_boost": 0.3}
        try:
            cfg = await self._get_thresholds_config("news")
            config.update(cfg)
        except Exception:
            pass
            
        semantic_boost = 0.0
        semantic_tags = []
        watchlist_boost = 0.0
        frequency_boost = 0.0

        if self.redis and named_entities:
            pipe = self.redis.raw.pipeline()
            for tag in named_entities:
                pipe.get(f"sentinel:semantic_sentiment:{tag.lower()}")
                # Query ZSET scores directly (returns score if member, None if not)
                pipe.zscore("sentinel:watched:equities", tag)
                pipe.zscore("sentinel:watched:vessels", tag)
                
            results = await pipe.execute()
            vals = []
            
            for i, tag in enumerate(named_entities):
                res = results[i*3]
                is_eq_zset = results[i*3 + 1]
                is_ves_zset = results[i*3 + 2]

                if res:
                    val = float(res)
                    semantic_boost += abs(val) * 0.1
                    vals.append(val)
                    tag_label = "positive" if val > 0 else "critical" if val <= -1.5 else "negative"
                    semantic_tags.append(f"semantic:{tag_label}")

                if (is_eq_zset is not None) or (is_ves_zset is not None):
                    watchlist_boost = 0.15

                f_boost = await self.track_frequency(tag, "news")
                frequency_boost = max(frequency_boost, f_boost)

            if vals:
                blended_val = sum(vals) / len(vals)
                sentiment = sentiment * 0.5 + blended_val * 0.5
                    
        # Non-linear reliability scaling to allow low-reliability feeds with extreme sentiment to be scored properly
        scaled_reliability = 0.5 + 0.5 * reliability
        base = abs(sentiment) * scaled_reliability
        
        entity_boost = min(config["max_boost"], len(named_entities) * config["entity_boost"])
        final_score = round(min(1.0, base + entity_boost + semantic_boost + watchlist_boost + frequency_boost), 3)
        return final_score, semantic_tags

    async def score_cyber_event(self, cve_id: str, severity_score: float) -> float:
        """Dynamic anomaly scorer stub for Cyber CVE events based on severity."""
        config = {"divisor": 10.0}
        res = await self.score_event("cyber_anomaly", cve_id, [severity_score / config["divisor"], 0.0, 0.0, 0.0, 0.0])
        return res["score"]



    async def _compute_ewma_volatility(
        self, entity_id: str, new_return: float, lam: float = 0.94
    ) -> float:
        """
        Incremental EWMA volatility update stored in Redis.
        σ²_t = λ * σ²_{t-1} + (1-λ) * r²_t
        """
        if not self.redis:
            return 0.0
        try:
            key = f"sentinel:ml:ewma_var:{entity_id}"
            prev_var = await self.redis.raw.get(key)
            prev_var = float(prev_var) if prev_var else new_return ** 2

            new_var = lam * prev_var + (1.0 - lam) * new_return ** 2
            await self.redis.raw.set(key, str(new_var), ex=604800)

            import math
            return math.sqrt(max(0.0, new_var))
        except Exception:
            return 0.0
