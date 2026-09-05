"""
shared/utils/streaming_detectors.py

STREAMING ANOMALY DETECTION ALGORITHMS
=======================================
Pure-algorithm module for all streaming/online anomaly detection.
Follows the quant_calc.py pattern: no IO, no Redis, no Kafka — just math.

Components:
  - RRCFDetector:            Robust Random Cut Forest (Guha et al., ICML 2016)
  - KalmanResidualFilter:    Constant-velocity Kalman filter for kinematic prediction
  - HawkesIntensityTracker:  Multivariate self/cross-exciting point process
  - FirstStoryDetector:      TF-IDF + nearest-neighbor novelty (Topic Detection & Tracking)
  - BGPGraphFeatureExtractor: Neo4j GDS graph-structural feature extraction for BGP

References:
  [1] Guha et al., "Robust Random Cut Forest Based Anomaly Detection on Streams", ICML 2016
  [2] Leveni et al., "Online Isolation Forest", ICML 2024
  [3] Ogata, "Statistical Models for Earthquake Occurrences", JASA 1988 (Hawkes process)
  [4] Allan et al., "Topic Detection and Tracking", Kluwer 2002

Usage:
  from shared.utils.streaming_detectors import (
      RRCFDetector, KalmanResidualFilter, HawkesIntensityTracker,
      FirstStoryDetector, BGPGraphFeatureExtractor,
  )
"""

import math
import logging
from collections import deque
from typing import Dict, List, Optional, Tuple, Any

import numpy as np

logger = logging.getLogger("sentinel.streaming_detectors")

# Try importing rrcf; fall back gracefully
try:
    import rrcf
    HAS_RRCF = True
except ImportError:
    HAS_RRCF = False
    logger.warning("rrcf not installed. RRCFDetector will use z-score fallback.")


# ════════════════════════════════════════════════════════════════════════════════
# SECTION 1: ROBUST RANDOM CUT FOREST
# ════════════════════════════════════════════════════════════════════════════════

# Scoring bounds for the z-score fallback used when rrcf is unavailable.
FALLBACK_MIN_HISTORY = 64      # observations before an empirical percentile means anything
FALLBACK_HISTORY_SIZE = 512    # how much recent history the percentile is taken against
FALLBACK_MAX_SCORE = 0.995     # never exactly 1.0 -- see _insert_fallback


class RRCFDetector:
    """
    Streaming anomaly detector using Robust Random Cut Forest.
    
    Each tree maintains a sliding window of the most recent `window_size` points.
    On each insert, the oldest point is evicted and the new point is inserted.
    The anomaly score is the average CoDisp (Collusive Displacement) across all trees,
    normalized to [0, 1] via a sigmoid squash.
    
    Thread safety: NOT thread-safe. Each domain should have its own instance.
    
    Args:
        num_trees:   Number of trees in the forest (more = more robust, slower).
        window_size: Sliding window capacity per tree.
        shingle_size: If > 1, concatenates `shingle_size` consecutive points into
                      a single higher-dimensional vector (captures temporal patterns).
    """

    def __init__(
        self,
        num_trees: int = 40,
        window_size: int = 256,
        shingle_size: int = 1,
    ):
        self.num_trees = num_trees
        self.window_size = window_size
        self.shingle_size = shingle_size
        self._index = 0

        # Shingle buffer: stores the last `shingle_size` raw points
        self._shingle_buffer: deque = deque(maxlen=shingle_size)
        # How many times a corrupted tree has been replaced. Surfaced rather
        # than silently absorbed: a forest quietly running below its configured
        # size produces scores that look ordinary and rank badly.
        self._tree_resets: int = 0

        # Recent magnitudes, on both paths: the positional score needs history
        # whichever detector produced the number.
        self._z_history: deque = deque(maxlen=FALLBACK_HISTORY_SIZE)

        if HAS_RRCF:
            self._forest = [rrcf.RCTree() for _ in range(num_trees)]
        else:
            self._forest = None
            # Fallback: EMA-based z-score detector
            self._ema_mean = None
            self._ema_var = None
            self._ema_alpha = 0.05

    def insert(self, point: np.ndarray) -> float:
        """
        Insert a point and return an anomaly score in [0, 1].
        
        Args:
            point: Feature vector, shape (n_features,)
            
        Returns:
            Anomaly score ∈ [0, 1]. Higher = more anomalous.
        """
        point = np.asarray(point, dtype=np.float64)

        # Shingling: concatenate recent points into one vector
        self._shingle_buffer.append(point)
        if len(self._shingle_buffer) < self.shingle_size:
            return 0.0  # Not enough history for a full shingle
        shingled = np.concatenate(list(self._shingle_buffer))

        if self._forest is not None:
            return self._insert_rrcf(shingled)
        else:
            return self._insert_fallback(shingled)

    def _insert_rrcf(self, point: np.ndarray) -> float:
        """Insert into RRCF forest, return normalized CoDisp."""
        avg_codisp = 0.0
        valid_trees = 0
        idx = self._index
        self._index += 1

        for i, tree in enumerate(self._forest):
            try:
                # Evict oldest point if at capacity
                if len(tree.leaves) >= self.window_size:
                    oldest = min(tree.leaves.keys())
                    tree.forget_point(oldest)

                # Insert new point
                tree.insert_point(point, index=idx)

                # CoDisp requires >1 leaf in tree to compute displacement
                if len(tree.leaves) > 1:
                    codisp = tree.codisp(idx)
                    if codisp is not None and not math.isnan(codisp):
                        avg_codisp += codisp
                        valid_trees += 1
            except Exception as e:
                # If rrcf internal tree structure gets corrupted (e.g. cut not found or leaf attribute error),
                # reset the corrupted tree instance cleanly -- and say so.
                #
                # `e` was bound and never used: no log line, no counter, in the
                # detector the radar multiplies by five and the correlation
                # layer derives confidence from. A replaced tree holds one leaf,
                # is excluded from valid_trees, and the forest quietly shrinks
                # while still returning a normal-looking score.
                self._tree_resets += 1
                if self._tree_resets in (1, 10, 100) or self._tree_resets % 500 == 0:
                    logger.warning(
                        "RRCF tree reset (%s total) in detector '%s': %s. "
                        "A reset tree contributes nothing until it refills, so "
                        "the forest is running below its configured size.",
                        self._tree_resets, getattr(self, "name", "unnamed"), e,
                    )
                try:
                    fresh_tree = rrcf.RCTree()
                    fresh_tree.insert_point(point, index=idx)
                    self._forest[i] = fresh_tree
                except Exception:
                    pass

        if valid_trees > 0:
            avg_codisp /= valid_trees
        else:
            # Total failure is not calm.
            #
            # This returned 0.0 -- the lowest possible anomaly score -- when
            # every tree in the forest had thrown, so a detector that had
            # completely failed reported the same number as one that looked
            # carefully and found nothing. The fallback path is a real
            # measurement and is what the class already uses when rrcf is
            # unavailable; using it here keeps the score honest and the failure
            # visible in the log above.
            logger.error(
                "RRCF forest produced no valid tree for detector '%s' "
                "(%s trees, %s resets). Falling back to the streaming estimator "
                "rather than reporting zero anomaly.",
                getattr(self, "name", "unnamed"), self.num_trees, self._tree_resets,
            )
            return self._insert_fallback(point)

        # Positioned against this detector's own recent CoDisp values.
        #
        # This was a fixed sigmoid, 1/(1+exp(-0.5*(avg_codisp-4.0))), and the
        # constants were guesses made before the system ran. CoDisp on a
        # shingled five-feature vector routinely reaches the twenties, where
        # that curve is long since saturated -- so live equity blocks scored
        # exactly 1.000 for 378 of ~400 samples with 45% of the population in
        # the top decile, and everything downstream inherited it: radar
        # multiplies the score by five for its z-score, correlation derives
        # confidence from it, and every ranking in the product rests on it.
        #
        # The same treatment as the fallback path, through the same method, so
        # the two cannot drift into disagreeing about what a score means.
        return self._positional_score(
            avg_codisp,
            warmup_curve=lambda c: 1.0 / (1.0 + math.exp(-0.5 * (c - 4.0))),
        )

    def _positional_score(self, raw: float, warmup_curve) -> float:
        """Where this observation sits among the detector's recent history.

        Both scoring paths -- RRCF CoDisp and the z-score fallback -- produce an
        unbounded magnitude that then has to become a [0,1] score. Each did it
        with its own hand-picked sigmoid, and both curves were exhausted by the
        magnitudes real traffic produces.

        An empirical position is self-calibrating: 0.9 means "more extreme than
        90% of what this detector has lately seen", which is how the number gets
        read anyway, and it spreads across the range whatever the units of the
        underlying signal. The supplied curve is used only while there is too
        little history for a percentile to mean anything.
        """
        self._z_history.append(float(raw))

        if len(self._z_history) < FALLBACK_MIN_HISTORY:
            score = warmup_curve(raw)
        else:
            below = sum(1 for prior in self._z_history if prior < raw)
            score = below / len(self._z_history)

        # Never exactly 1.0. The most extreme thing seen so far is still only
        # that, and a detector reporting certainty leaves nothing to say when
        # something genuinely worse arrives.
        return round(min(FALLBACK_MAX_SCORE, max(0.0, score)), 4)

    def _insert_fallback(self, point: np.ndarray) -> float:
        """Z-score fallback when rrcf is not installed."""
        if self._ema_mean is None:
            self._ema_mean = point.copy()
            self._ema_var = np.ones_like(point)
            return 0.0

        alpha = self._ema_alpha
        diff = point - self._ema_mean
        self._ema_mean = alpha * point + (1 - alpha) * self._ema_mean
        self._ema_var = alpha * diff ** 2 + (1 - alpha) * self._ema_var

        z_scores = np.abs(diff) / (np.sqrt(self._ema_var) + 1e-8)
        max_z = float(np.max(z_scores))

        # Scored against this detector's own recent history, not a fixed curve.
        #
        # This was a sigmoid: 1/(1+exp(-0.8*(max_z-2.5))). Both constants were
        # written before the system ran, and against real traffic the curve is
        # exhausted almost immediately -- z=8 already scores 0.988 and anything
        # past z=15 rounds to exactly 1.0000. Measured on live equity blocks:
        # 378 of ~400 came back at 1.000 before a single adjustment was applied,
        # and the top decile held 45% of the population. A detector whose output
        # is 1.0 for nearly half its input is not ranking that half.
        #
        # The empirical position is self-calibrating: a score of 0.9 means "more
        # extreme than 90% of what this detector has recently seen", which is
        # what an analyst reads it as anyway, and it spreads across the range by
        # construction whatever the units of the underlying features.
        return self._positional_score(max_z, warmup_curve=lambda z: 1.0 / (1.0 + math.exp(-0.8 * (z - 2.5))))

    def insert_batch(self, points: List[np.ndarray]) -> List[float]:
        """Insert multiple points sequentially, return list of scores."""
        return [self.insert(p) for p in points]


# ════════════════════════════════════════════════════════════════════════════════
# SECTION 2: KALMAN FILTER FOR KINEMATIC PREDICTION RESIDUALS
# ════════════════════════════════════════════════════════════════════════════════

class KalmanResidualFilter:
    """
    Lightweight constant-velocity Kalman filter for maritime/aviation kinematics.
    
    State vector: [lat, lon, speed, heading]
    Observation:  [lat, lon, speed, heading]
    
    The key insight from AIS-spoofing literature is that the *residuals* 
    (predicted - actual) are far stronger anomaly signals than raw values.
    A vessel that jumps 50nm in 5 minutes has a huge residual even if both
    positions are individually "normal."
    
    Returns prediction residuals as engineered features for downstream scoring.
    """

    def __init__(self, process_noise: float = 0.001, measurement_noise: float = 0.01):
        self._state: Optional[np.ndarray] = None  # [lat, lon, speed, heading]
        self._P: Optional[np.ndarray] = None       # Covariance matrix
        self._last_ts: Optional[float] = None
        self._process_noise = process_noise
        self._measurement_noise = measurement_noise
        self._initialized = False

    def predict_and_update(
        self,
        lat: float,
        lon: float,
        speed: float,
        heading: float,
        timestamp: float,
    ) -> Dict[str, float]:
        """
        Run one Kalman predict-update cycle.
        
        Returns dict with:
            predicted_lat, predicted_lon: Where the filter expected the entity
            residual_distance: Great-circle distance between predicted and actual (nm)
            residual_speed: |predicted_speed - actual_speed|
            residual_heading: Angular difference in heading (degrees, 0-180)
            prediction_confidence: 1 - normalized residual (higher = more expected)
        """
        measurement = np.array([lat, lon, speed, heading], dtype=np.float64)

        if not self._initialized:
            self._state = measurement.copy()
            self._P = np.eye(4) * self._measurement_noise
            self._last_ts = timestamp
            self._initialized = True
            return {
                "predicted_lat": lat,
                "predicted_lon": lon,
                "residual_distance": 0.0,
                "residual_speed": 0.0,
                "residual_heading": 0.0,
                "prediction_confidence": 1.0,
            }

        dt = max(0.001, timestamp - self._last_ts) / 3600.0  # hours
        self._last_ts = timestamp

        # ── PREDICT ──
        # Constant-velocity model: position advances by speed * dt in heading direction
        speed_kts = self._state[2]
        hdg_rad = math.radians(self._state[3])

        # Approximate lat/lon advance (nm to degrees)
        dlat = (speed_kts * dt * math.cos(hdg_rad)) / 60.0  # 1 nm ≈ 1/60 degree lat
        dlon = (speed_kts * dt * math.sin(hdg_rad)) / (60.0 * max(0.01, math.cos(math.radians(self._state[0]))))

        predicted = np.array([
            self._state[0] + dlat,
            self._state[1] + dlon,
            self._state[2],  # speed unchanged in constant-velocity model
            self._state[3],  # heading unchanged
        ])

        # Process noise: grows with dt (longer gap = less certain prediction)
        Q = np.eye(4) * self._process_noise * (1.0 + dt)
        P_pred = self._P + Q

        # ── UPDATE ──
        H = np.eye(4)  # Direct observation model
        R = np.eye(4) * self._measurement_noise

        innovation = measurement - predicted
        # Wrap heading difference to [-180, 180]
        innovation[3] = ((innovation[3] + 180) % 360) - 180

        S = H @ P_pred @ H.T + R
        try:
            K = P_pred @ H.T @ np.linalg.inv(S)
        except np.linalg.LinAlgError:
            K = np.zeros((4, 4))

        self._state = predicted + K @ innovation
        self._P = (np.eye(4) - K @ H) @ P_pred

        # ── COMPUTE RESIDUALS ──
        residual_distance = self._haversine_nm(
            predicted[0], predicted[1], lat, lon
        )
        residual_speed = abs(predicted[2] - speed)
        residual_heading = abs(((predicted[3] - heading + 180) % 360) - 180)

        # Normalize residuals for confidence score
        # A residual_distance > 5nm is highly anomalous for a 15-min interval
        max_expected_distance = max(0.1, speed_kts * dt)
        norm_residual = min(1.0, residual_distance / max(0.1, max_expected_distance))
        confidence = max(0.0, 1.0 - norm_residual)

        return {
            "predicted_lat": float(predicted[0]),
            "predicted_lon": float(predicted[1]),
            "residual_distance": round(float(residual_distance), 4),
            "residual_speed": round(float(residual_speed), 2),
            "residual_heading": round(float(residual_heading), 2),
            "prediction_confidence": round(float(confidence), 4),
        }

    @staticmethod
    def _haversine_nm(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Great-circle distance in nautical miles."""
        R = 3440.065  # Earth radius in nautical miles
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = (math.sin(dlat / 2) ** 2 +
             math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
             math.sin(dlon / 2) ** 2)
        return R * 2 * math.asin(math.sqrt(min(1.0, a)))


# ════════════════════════════════════════════════════════════════════════════════
# SECTION 3: MULTIVARIATE HAWKES PROCESS
# ════════════════════════════════════════════════════════════════════════════════

# Bounds on the excitation ratio and how its baseline is learned.
#
# A ratio is "how much busier than usual", so it is only meaningful against what
# this deployment actually receives. The seeded constants were a guess made
# before the system ran and were wrong by more than two orders of magnitude for
# the busiest domain.
BASELINE_ALPHA = 0.20              # per window, and windows are 30s apart
BASELINE_MIN_OBSERVATIONS = 3      # windows, not arrivals, before trusting it
BASELINE_WINDOW_SEC = 30.0         # a rate is counted over this, never per arrival
BASELINE_MAX_GAP_SEC = 3600.0      # a longer gap is an outage, not a quiet period

# Reported ratios are capped. Beyond roughly two orders of magnitude the number
# has stopped being a measurement and started being a division artefact, and
# printing "230,093,397.3x baseline" in an operator's alert is worse than
# printing the cap: the first invites belief, the second invites a question.
MAX_EXCITATION_RATIO = 100.0


class HawkesIntensityTracker:
    """
    Multivariate self- and cross-exciting point process tracker.
    
    Models event arrival intensity as:
        λ_d(t) = μ_d + Σ_{d'} Σ_{t_i < t} α_{d',d} * exp(-β_{d',d} * (t - t_i))
    
    Where:
        μ_d         = baseline intensity for domain d
        α_{d',d}    = excitation magnitude from domain d' to domain d
        β_{d',d}    = excitation decay rate
        
    Cross-excitation kernel: events in domain A increase intensity in domain B.
    E.g., crypto liquidation → tradfi anomaly intensity spikes.
    
    This tracker does NOT fit parameters online — it uses fixed priors suitable
    for Sentinel's event domains. For parameter estimation, see tick library.
    """

    # Default cross-excitation matrix (α values)
    # Row = source domain, Column = target domain
    # Higher α = stronger excitation
    # Why the empty cells are still empty.
    #
    # An estimation attempt was made against 1,373 five-minute bins spanning
    # 168 hours of this deployment's own events, using each domain's share of
    # bin activity to control for the system being on or off. It produced 21
    # domain pairs with |r| >= 0.15 and every one of them is an artifact:
    #
    #   Symmetry. crypto->maritime scored 0.604 and maritime->crypto 0.601 --
    #   a gap of 0.002. Excitation is directional; a relationship symmetric to
    #   three decimal places is co-occurrence, two domains busy at the same
    #   time because the collectors are running.
    #
    #   Wrong-way decay. aviation->crypto rose from 0.509 at five minutes to
    #   0.611 at thirty. An influence that strengthens as it ages is not an
    #   influence.
    #
    #   Closure. Shares sum to exactly 1.000 and news averages 62% of every
    #   bin, so every other domain anti-correlates with news by arithmetic
    #   necessity -- which is where news->crypto = -0.817 came from.
    #
    # Filling these cells from that measurement would have produced twenty-one
    # confident coefficients and no knowledge. Estimating them properly needs
    # event-level lead-lag on matched entities, not bin-level activity
    # correlation.
    #
    # That prescription was later checked against the data, and it does not
    # work either -- for a reason worth recording, because it is the reason
    # these cells stay empty rather than a matter of waiting for uptime.
    # Distinct entities per domain over fourteen days, and how many of them
    # tradfi also names:
    #
    #     crypto      87,902        9          aviation   12,258      0
    #     maritime     4,545        0          cyber       1,902      1
    #     news         1,602      196          prediction     52      0
    #
    # Every domain missing a coefficient shares essentially no entities with
    # the domain it would excite. Entity matching cannot measure these paths
    # because they are not entity-mediated: a Suez closure moves crude through
    # a causal channel that names no vessel, and an equity block names no
    # aircraft. The one pair with real overlap, news->tradfi at 12%, is also the
    # one pair already carrying a coefficient.
    #
    # So this needs a mechanism the system does not currently have -- concept
    # or instrument-class association rather than entity identity -- and not
    # more data of the kind already collected. Until then has_excitation_path()
    # keeps the system from forecasting what it cannot represent, which is the
    # honest state and not a placeholder.
    # Measured, 2026-08-31, by scripts/estimate_excitation.py over ten days of
    # this deployment: 1.8M events, one-minute bins, joint Poisson fit of
    #
    #     E[N_d(t)] = mu_d + SUM_d' alpha_{d',d} * w_{d'}(t)
    #
    # with every source domain in the same regression, so each coefficient is a
    # partial effect and "the whole system was busy" is a term rather than a
    # confound. Five of forty-nine ordered pairs survived being positive,
    # significant against a likelihood ratio, and stable across the two halves
    # of the window. The rest are absent because they could not be measured, and
    # has_excitation_path() below refuses to forecast through an absent one.
    #
    # What was here before was invented, and every cell that could be checked
    # was wrong by one to two orders of magnitude:
    #
    #     crypto -> tradfi     0.3   invented    0.0019  measured
    #     crypto -> crypto     0.5   invented    0.1044  measured
    #     tradfi -> tradfi     0.4   invented    unstable across halves
    #     tradfi -> crypto     0.2   invented    unstable across halves
    #     news   -> tradfi     0.1   invented    no excitation
    #     maritime -> tradfi   0.05  invented    unstable across halves
    #     cyber  -> tradfi     0.05  invented    unstable across halves
    #     prediction -> *      0.15  invented    source has 217 events
    #
    # The shape of the result is worth reading before trusting it. Four of the
    # five survivors are self-excitation, which is the effect this data can
    # actually see: a domain's own recent arrivals predict its next ones. Only
    # crypto -> tradfi survives across domains, at 0.0019 -- small enough that
    # the honest summary is "this deployment shows almost no cross-domain
    # excitation", not "here are the cross-domain coefficients".
    #
    # Re-run the script rather than editing these by hand. A coefficient with no
    # provenance is what this table used to be.
    DEFAULT_EXCITATION = {
        ("crypto", "crypto"):     0.1044,   # liquidation cascades
        ("cyber", "cyber"):       0.0957,
        ("maritime", "maritime"): 0.0950,
        ("news", "news"):         0.0783,   # a story begets coverage
        ("crypto", "tradfi"):     0.0019,   # the only cross-domain survivor
    }

    DEFAULT_DECAY = 0.1  # β: events decay over ~10 time units (minutes)

    def __init__(
        self,
        baselines: Optional[Dict[str, float]] = None,
        excitation_matrix: Optional[Dict[Tuple[str, str], float]] = None,
        decay: float = DEFAULT_DECAY,
        max_history: int = 500,
    ):
        self._baselines = baselines or {
            "crypto": 0.01,
            "tradfi": 0.01,
            "prediction": 0.005,
            "news": 0.02,
            "maritime": 0.005,
            "aviation": 0.005,
            "cyber": 0.005,
        }
        self._excitation = excitation_matrix or self.DEFAULT_EXCITATION
        self._decay = decay
        self._max_history = max_history

        # Baselines above are seeds, not measurements.
        #
        # They are a guess at events per second made before the system had ever
        # run, and the excitation ratio divides by them. Crypto was seeded at
        # 0.01/s and actually arrives at roughly 4.7/s on this deployment --
        # about 470 times higher -- so every ratio computed against it was
        # meaningless. Published in an alert at tier 4, that read as
        # "crypto-domain intensity (230,093,397.3x baseline)": eight orders of
        # magnitude, presented to an operator as a measurement.
        #
        # The observed rate replaces the seed once a domain has been seen often
        # enough to estimate it. A ratio is only interesting relative to what
        # this deployment actually receives, which no constant can know.
        self._observed_rate: Dict[str, float] = {}
        self._last_seen: Dict[str, float] = {}
        self._observation_count: Dict[str, int] = {}
        self._window_start: Dict[str, float] = {}
        self._window_count: Dict[str, int] = {}

        # Event history per domain: deque of timestamps
        self._history: Dict[str, deque] = {
            d: deque(maxlen=max_history) for d in self._baselines
        }

    def record_event(self, domain: str, timestamp: float) -> Dict[str, Any]:
        """
        Record an event arrival and return current intensity state.
        
        Args:
            domain: Event domain (e.g., "crypto", "tradfi")
            timestamp: Unix timestamp of the event
            
        Returns:
            {
                "intensity": current intensity for this domain,
                "baseline": baseline intensity μ,
                "excitation_ratio": intensity / baseline (> 1.0 = excited),
                "cross_intensities": {domain: intensity} for all domains,
            }
        """
        if domain not in self._history:
            self._history[domain] = deque(maxlen=self._max_history)

        self._history[domain].append(timestamp)
        self._update_observed_rate(domain, timestamp)

        # Compute intensity for the source domain
        intensity = self._compute_intensity(domain, timestamp)
        baseline = self._effective_baseline(domain)
        ratio = min(MAX_EXCITATION_RATIO, intensity / max(1e-10, baseline))

        # Compute cross-domain intensities
        cross = {}
        for d in self._baselines:
            if d != domain:
                cross[d] = self._compute_intensity(d, timestamp)

        return {
            "intensity": round(float(intensity), 6),
            "baseline": round(float(baseline), 6),
            "excitation_ratio": round(float(ratio), 4),
            "cross_intensities": {k: round(float(v), 6) for k, v in cross.items()},
        }

    def is_baseline_established(self, domain: str) -> bool:
        """Whether this domain's normal rate has actually been measured yet.

        Until it has, _effective_baseline returns the seed -- a constant written
        before the system ran. Dividing by it produces a number with the shape
        of a measurement and none of the content: prediction is seeded at
        0.005/s (one event every 200 seconds) and arrives from Polymarket in
        bursts, so any burst read as enormous and pinned to the reporting cap.
        "100.0x baseline" is the cap saying it gave up, not a finding.

        Callers that publish a ratio check this first. A domain too quiet to
        have established a rate is a domain we cannot yet say anything
        quantitative about, and saying so is better than saying something false.
        """
        return self._observation_count.get(domain, 0) >= BASELINE_MIN_OBSERVATIONS

    def has_excitation_path(self, domain: str) -> bool:
        """Whether anything in the model can excite this domain at all.

        Two of seven now, not five: the coefficients were measured (see
        DEFAULT_EXCITATION above) and crypto, cyber, maritime, news and tradfi
        each gained an inbound term, four of them by exciting themselves.

        Aviation and prediction still have none, and that is a result rather
        than a gap -- aviation produced no stable coefficient from any source,
        and prediction has 217 events in the whole fitted window. For those two
        the excitation sum is empty by construction, so intensity equals
        baseline and the ratio is exactly 1.0 no matter what arrives.

        What must not happen is publishing a "forecast" for such a domain,
        because the number is the baseline restated and carries no information.
        Callers check this before reporting.
        """
        return any(
            alpha > 0.0 for (_, target), alpha in self._excitation.items()
            if target == domain
        )

    def _update_observed_rate(self, domain: str, timestamp: float) -> None:
        """Estimates the domain's normal rate, over a window, not per arrival.

        The baseline is a *rate over a window*, folded in at most once per
        BASELINE_WINDOW_SEC. Two reasons, and the second is the important one.

        Statistically, 1/gap is an estimate from a single inter-arrival and is
        wildly noisy; count/elapsed over a window is not.

        Structurally, folding every arrival lets a burst raise the baseline it
        is about to be measured against. An earlier version of this method did
        exactly that: twenty arrivals in a second each pulled the EMA toward
        20/s, so by the time the ratio was read the burst had already become the
        new normal and registered as *less* excited than the calm stream before
        it. That is the same failure this codebase fixed once already in
        track_frequency -- "a burst compared against a baseline the burst itself
        just raised" -- reintroduced here.
        """
        started = self._window_start.get(domain)
        if started is None:
            self._window_start[domain] = timestamp
            self._window_count[domain] = 1
            self._last_seen[domain] = timestamp
            return

        gap_since_last = timestamp - self._last_seen.get(domain, timestamp)
        self._last_seen[domain] = timestamp

        if gap_since_last > BASELINE_MAX_GAP_SEC:
            # An outage, not a quiet period. The partial window spans the gap
            # and would read as a near-zero rate, which is how the first event
            # after a restart comes to look infinitely excited. Start over.
            self._window_start[domain] = timestamp
            self._window_count[domain] = 1
            return

        self._window_count[domain] = self._window_count.get(domain, 0) + 1
        elapsed = timestamp - started
        if elapsed < BASELINE_WINDOW_SEC:
            return

        rate = self._window_count[domain] / elapsed
        current = self._observed_rate.get(domain)
        self._observed_rate[domain] = (
            rate if current is None
            else BASELINE_ALPHA * rate + (1.0 - BASELINE_ALPHA) * current
        )
        self._observation_count[domain] = self._observation_count.get(domain, 0) + 1
        self._window_start[domain] = timestamp
        self._window_count[domain] = 0

    def _effective_baseline(self, domain: str) -> float:
        """The measured arrival rate once known, else the seed.

        The seed is used only until a domain has been seen enough times to
        estimate its rate. Reporting a ratio against a guess is what produced
        the eight-order-of-magnitude figures.
        """
        if self._observation_count.get(domain, 0) >= BASELINE_MIN_OBSERVATIONS:
            observed = self._observed_rate.get(domain)
            if observed and observed > 0:
                return observed
        return self._baselines.get(domain, 0.01)

    def get_intensity(self, domain: str, timestamp: float) -> float:
        """Get current intensity for a domain without recording an event."""
        return self._compute_intensity(domain, timestamp)

    def get_excitation_ratio(self, domain: str, timestamp: float) -> float:
        """Get intensity / baseline for a domain. >1.0 = excited state."""
        intensity = self._compute_intensity(domain, timestamp)
        baseline = self._effective_baseline(domain)
        return min(MAX_EXCITATION_RATIO, intensity / max(1e-10, baseline))

    def _compute_intensity(self, target_domain: str, t: float) -> float:
        """
        Compute λ_d(t) = μ_d + Σ_{d'} Σ_{t_i < t} α_{d',d} * exp(-β * (t - t_i))
        """
        mu = self._effective_baseline(target_domain)
        excitation_sum = 0.0

        for source_domain, history in self._history.items():
            alpha = self._excitation.get((source_domain, target_domain), 0.0)
            if alpha <= 0.0:
                continue

            for t_i in history:
                if t_i >= t:
                    continue
                dt = t - t_i
                # Prune: contributions older than 5 / β are negligible
                if dt > 5.0 / max(1e-6, self._decay):
                    continue
                excitation_sum += alpha * math.exp(-self._decay * dt)

        return mu + excitation_sum


# ════════════════════════════════════════════════════════════════════════════════
# SECTION 4: FIRST STORY DETECTION (TDT)
# ════════════════════════════════════════════════════════════════════════════════

class FirstStoryDetector:
    """
    Topic Detection and Tracking (TDT) — First Story Detection.
    
    Scores each incoming headline/summary by its novelty relative to
    a sliding window of recently seen stories. Uses TF-IDF vectorization
    and nearest-neighbor cosine distance.
    
    A completely novel story scores near 1.0.
    A story that's a continuation of known coverage scores near 0.0.
    
    This reframes news anomaly detection from "how extreme is the sentiment?"
    to "is this the first time we're seeing this story?" — which is what an
    intelligence analyst actually wants.
    
    No heavy ML model needed — sklearn TfidfVectorizer + scipy sparse ops.
    """

    def __init__(
        self,
        window_size: int = 500,
        novelty_threshold: float = 0.70,
        min_df: int = 1,
        max_features: int = 10000,
    ):
        self._window_size = window_size
        self._novelty_threshold = novelty_threshold
        self._texts: deque = deque(maxlen=window_size)
        self._vectorizer = None
        self._tfidf_matrix = None
        self._dirty = True  # Whether the TF-IDF matrix needs rebuilding

        self._min_df = min_df
        self._max_features = max_features

        # Lazy import to avoid hard dependency at module level
        self._sklearn_available = False
        try:
            from sklearn.feature_extraction.text import TfidfVectorizer
            from sklearn.metrics.pairwise import cosine_similarity
            self._sklearn_available = True
        except ImportError:
            logger.warning(
                "scikit-learn not installed. FirstStoryDetector will use keyword fallback."
            )

    def score_novelty(self, headline: str, summary: str = "") -> float:
        """
        Score the novelty of a news item relative to the sliding window.
        
        Args:
            headline: Article title
            summary: Article summary/description (optional)
            
        Returns:
            Novelty score ∈ [0, 1]. Higher = more novel (first story).
        """
        text = f"{headline} {summary}".strip()
        if not text:
            return 0.5

        if not self._sklearn_available:
            return self._keyword_fallback(headline)

        # Add to window
        self._texts.append(text)
        self._dirty = True

        if len(self._texts) < 3:
            return 0.8  # Too few stories to compare — assume novelty

        return self._compute_novelty(text)

    def _compute_novelty(self, query_text: str) -> float:
        """Compute TF-IDF cosine distance to nearest neighbor."""
        from sklearn.feature_extraction.text import TfidfVectorizer
        from sklearn.metrics.pairwise import cosine_similarity

        try:
            # Rebuild TF-IDF matrix if window changed
            if self._dirty or self._vectorizer is None:
                self._vectorizer = TfidfVectorizer(
                    max_features=self._max_features,
                    min_df=self._min_df,
                    stop_words="english",
                    ngram_range=(1, 2),
                    sublinear_tf=True,
                )
                texts_list = list(self._texts)
                self._tfidf_matrix = self._vectorizer.fit_transform(texts_list)
                self._dirty = False

            # Transform the query (last element is the query itself)
            query_vec = self._vectorizer.transform([query_text])

            # Compare against all OTHER stories in the window (exclude self = last row)
            if self._tfidf_matrix.shape[0] <= 1:
                return 0.8

            history_matrix = self._tfidf_matrix[:-1]  # Exclude the just-added story
            similarities = cosine_similarity(query_vec, history_matrix).flatten()

            # Novelty = 1 - max_similarity
            # If the most similar story is 0.9 similar, novelty = 0.1
            max_sim = float(np.max(similarities)) if len(similarities) > 0 else 0.0
            novelty = 1.0 - max_sim

            return round(max(0.0, min(1.0, novelty)), 4)

        except Exception as e:
            logger.debug(f"TF-IDF novelty computation failed: {e}")
            return 0.5

    def _keyword_fallback(self, headline: str) -> float:
        """
        Simple keyword-overlap novelty when sklearn is not available.
        Not as good as TF-IDF but better than nothing.
        """
        if not self._texts:
            self._texts.append(headline)
            return 0.8

        query_tokens = set(headline.lower().split())
        if not query_tokens:
            return 0.5

        max_overlap = 0.0
        for prev in self._texts:
            prev_tokens = set(prev.lower().split())
            if not prev_tokens:
                continue
            overlap = len(query_tokens & prev_tokens) / max(1, len(query_tokens | prev_tokens))
            max_overlap = max(max_overlap, overlap)

        self._texts.append(headline)
        novelty = 1.0 - max_overlap
        return round(max(0.0, min(1.0, novelty)), 4)


# ════════════════════════════════════════════════════════════════════════════════
# SECTION 5: BGP GRAPH FEATURE EXTRACTION
# ════════════════════════════════════════════════════════════════════════════════

class BGPGraphFeatureExtractor:
    """
    Extracts graph-structural features from Neo4j for BGP anomaly scoring.
    
    BGP hijacks are fundamentally topology violations — scoring them as flat
    feature vectors discards the signal that actually distinguishes a hijack
    from normal routing noise.
    
    Near-term approach: extract centrality/embedding features from the existing
    Neo4j graph and feed them into the RRCF streaming detector.
    
    Requires Neo4j to be reachable. Falls back to empty features if not.
    """

    def __init__(self, neo4j_client=None):
        self._neo4j = neo4j_client
        self._cache: Dict[str, Dict] = {}  # Simple LRU-ish cache
        self._cache_ttl = 300  # 5 minutes
        self._has_gds = None  # Lazily detected

    def set_neo4j_client(self, client):
        """Set or update the Neo4j client (for late initialization)."""
        self._neo4j = client

    async def extract_features(
        self, origin_as: str, prefix: str
    ) -> Dict[str, float]:
        """
        Extract graph-structural features for a BGP event.
        
        Returns:
            {
                "betweenness_centrality": float,  # 0-1, how central this AS is
                "degree": float,                   # normalized peering count
                "path_novelty": float,             # 0 = known path, 1 = never-seen-before
                "prefix_specificity": float,       # /24 → 1.0, /8 → 0.33
            }
        """
        defaults = {
            "betweenness_centrality": 0.0,
            "degree": 0.0,
            "path_novelty": 1.0,  # Assume novel if we can't check
            "prefix_specificity": self._prefix_specificity(prefix),
        }

        if not self._neo4j:
            return defaults

        try:
            features = await self._query_graph_features(origin_as, prefix)
            features["prefix_specificity"] = defaults["prefix_specificity"]
            return features
        except Exception as e:
            # The defaults claim maximum novelty and zero centrality, which is
            # a score of 0.850 for every hijack. Falling back to them is a
            # measurement failure and is reported as one.
            logger.warning(
                "BGP graph features unavailable for %s: %s. Falling back to "
                "assumed-novel defaults, which do not discriminate.",
                origin_as, e,
            )
            return defaults

    async def upsert_as_path(
        self,
        origin_as: str,
        prefix: str,
        as_path: Optional[List[str]] = None,
    ) -> None:
        """
        Maintain the AS-path graph in Neo4j.
        
        Creates/updates:
          - (:AutonomousSystem {id}) node
          - (:Prefix {cidr}) node
          - (as)-[:ANNOUNCES {first_seen, last_seen, path}]->(prefix) relationship
        """
        if not self._neo4j:
            return

        try:
            import time
            try:
                from datetime import datetime, timezone
                now_iso = datetime.now(timezone.utc).isoformat()
            except Exception:
                now_iso = str(time.time())

            # Upsert AS node
            await self._neo4j.query(
                """
                MERGE (a:AutonomousSystem {id: $as_id})
                ON CREATE SET a.first_seen = $now
                SET a.last_seen = $now
                """,
                {"as_id": origin_as, "now": now_iso},
            )

            # Upsert Prefix node
            await self._neo4j.query(
                """
                MERGE (p:Prefix {cidr: $cidr})
                ON CREATE SET p.first_seen = $now
                SET p.last_seen = $now
                """,
                {"cidr": prefix, "now": now_iso},
            )

            # Upsert ANNOUNCES relationship
            #
            # str() on every element, because RIS sends AS paths as integers and
            # ",".join() raises TypeError on the first one. That exception was
            # thrown here -- after the two node upserts and before the
            # relationship -- and caught by the handler below at debug level, so
            # the AS and Prefix nodes were created and the edge between them
            # never was. Zero ANNOUNCES relationships existed against 2,028 AS
            # nodes and 3,548 prefixes.
            #
            # The cost was not the missing edge. Novelty is measured by whether
            # this AS has announced this prefix before, so with no edge ever
            # written the answer was always "no": path_novelty pinned at 1.0,
            # every event scored 0.70 + 0.30 x (0.5 x 1.0) = 0.850, and all 219
            # bgp_anomaly events in a 45-minute window shared one score.
            #
            # A note above this in the collector records that `as_path` was
            # already fixed once, when the enricher was reading a field the
            # producer did not send. The name was corrected and the type was
            # not, so the feature stayed dead through both fixes.
            path_str = ",".join(str(hop) for hop in as_path) if as_path else str(origin_as)
            await self._neo4j.query(
                """
                MATCH (a:AutonomousSystem {id: $as_id})
                MATCH (p:Prefix {cidr: $cidr})
                MERGE (a)-[r:ANNOUNCES]->(p)
                ON CREATE SET r.first_seen = $now, r.path = $path
                SET r.last_seen = $now, r.path = $path
                """,
                {"as_id": origin_as, "cidr": prefix, "now": now_iso, "path": path_str},
            )
        except Exception as e:
            # Warning, not debug. This failing silently is what kept path
            # novelty pinned at 1.0 for the life of the detector.
            logger.warning(
                "BGP AS-path upsert failed for %s %s: %s. Path novelty cannot "
                "be measured without it, so scores will not discriminate.",
                origin_as, prefix, e,
            )

    async def _query_graph_features(
        self, origin_as: str, prefix: str
    ) -> Dict[str, float]:
        """Query Neo4j for graph-structural features."""
        features = {
            "betweenness_centrality": 0.0,
            "degree": 0.0,
            "path_novelty": 1.0,
        }

        # 1. Degree centrality (peering count)
        try:
            res = await self._neo4j.query(
                """
                MATCH (a:AutonomousSystem {id: $as_id})-[r]-(n)
                RETURN count(r) as degree
                """,
                {"as_id": origin_as},
            )
            if res and res[0].get("degree"):
                degree = float(res[0]["degree"])
                # Normalize: a Tier-1 AS has ~500+ peers, normalize by log
                features["degree"] = min(1.0, math.log(1 + degree) / math.log(500))
        except Exception:
            pass

        # 2. Path novelty: has this (AS, prefix) pair been seen before?
        try:
            res = await self._neo4j.query(
                """
                MATCH (a:AutonomousSystem {id: $as_id})-[r:ANNOUNCES]->(p:Prefix {cidr: $cidr})
                RETURN r.first_seen as first_seen
                """,
                {"as_id": origin_as, "cidr": prefix},
            )
            if res and res[0].get("first_seen"):
                # Known path — low novelty
                features["path_novelty"] = 0.0
            else:
                # Never seen this AS announce this prefix — high novelty (hijack signal)
                features["path_novelty"] = 1.0
        except Exception:
            pass

        # 3. Betweenness centrality (if GDS is available)
        if self._has_gds is None:
            self._has_gds = await self._check_gds_available()

        if self._has_gds:
            try:
                res = await self._neo4j.query(
                    """
                    CALL gds.betweenness.stream({
                        nodeProjection: 'AutonomousSystem',
                        relationshipProjection: {
                            PEERS_WITH: { type: 'ANNOUNCES', orientation: 'UNDIRECTED' }
                        }
                    })
                    YIELD nodeId, score
                    WHERE gds.util.asNode(nodeId).id = $as_id
                    RETURN score as centrality
                    """,
                    {"as_id": origin_as},
                )
                if res and res[0].get("centrality") is not None:
                    centrality = float(res[0]["centrality"])
                    # Normalize centrality to [0, 1] range
                    features["betweenness_centrality"] = min(1.0, centrality / 1000.0)
            except Exception as e:
                logger.debug(f"GDS betweenness query failed (non-fatal): {e}")
                self._has_gds = False  # Don't retry

        return features

    async def _check_gds_available(self) -> bool:
        """Check if Neo4j GDS plugin is installed."""
        if not self._neo4j:
            return False
        try:
            res = await self._neo4j.query("RETURN gds.version() AS version")
            if res:
                logger.info(f"Neo4j GDS detected: {res[0].get('version')}")
                return True
        except Exception:
            pass
        return False

    @staticmethod
    def _prefix_specificity(prefix: str) -> float:
        """
        Compute prefix specificity score.
        More specific prefixes (higher CIDR) are more suspicious for hijacks.
        /24 → 1.0, /16 → 0.67, /8 → 0.33
        """
        try:
            if "/" in prefix:
                cidr = int(prefix.split("/")[1])
                return min(1.0, cidr / 24.0)
        except (ValueError, IndexError):
            pass
        return 0.5


# ════════════════════════════════════════════════════════════════════════════════
# SECTION 6: STS TRANSFER ZONE CONSTANTS
# ════════════════════════════════════════════════════════════════════════════════

# Known ship-to-ship (STS) transfer zones used for sanctions evasion.
# A vessel going dark near these zones is far more suspicious than
# going dark in open ocean. Per current maritime-intel practice.
STS_TRANSFER_ZONES = {
    "Strait of Hormuz":    3.0,   # Iranian crude STS hub
    "Iranian Territorial": 3.0,
    "Lamu Anchorage":      2.5,   # East Africa STS for Iranian/Venezuelan crude
    "Ceuta Approaches":    2.0,   # Gibraltar STS zone
    "Kalamata Anchorage":  2.0,   # Greek STS zone (Laconian Gulf)
    "South China Sea":     2.0,   # Chinese crude STS operations
    "Bab-el-Mandeb":       2.5,   # Red Sea transit / Yemeni waters STS
    "Suez Canal":          1.5,   # Transit chokepoint
    "North Korean Waters": 3.0,   # DPRK sanctions evasion
    "Persian Gulf":        2.0,
    "Red Sea":             1.5,
    "Black Sea":           1.5,
    "Somali Territorial":  2.0,   # Piracy + sanctions evasion
}


def sts_zone_risk_multiplier(region: Optional[str]) -> float:
    """
    Returns a risk multiplier for AIS gap significance based on proximity
    to known STS transfer zones. Used by gap_detector.py.
    
    A 3-hour gap approaching a known STS zone is NOT the same event
    as a 3-hour gap in open ocean — per current maritime-intel practice.
    
    Returns:
        1.0 for open ocean (no risk amplification)
        Up to 3.0 for high-risk STS transfer zones
    """
    if region is None:
        return 1.0

    # Falls back to the platform's general region sensitivity rather than to
    # open ocean.
    #
    # This table and get_region_sensitivity_multiplier were written separately
    # and never reconciled. Measured against the 69 regions classify_region can
    # actually return: 18 that the platform rates sensitive were open ocean
    # here, and every one of the 10 regions present in both tables carried a
    # different number.
    #
    # The two highest-traffic regions in the data were among the 18. Strait of
    # Malacca and Taiwan Strait -- 30,611 and 40,891 events in a week, the
    # busiest waterway on earth and the most contested -- returned 1.0, so a
    # vessel going dark there scored exactly as if it had gone dark in open
    # ocean. A 32.6-hour gap in Malacca scored 0.679 where the same gap in the
    # South China Sea scored 1.000.
    #
    # The maximum of the two, because this table exists to *amplify* for
    # ship-to-ship transfer risk. A genuine STS hub keeps its higher weight --
    # Hormuz stays 3.0 against the general table's 1.5 -- and nowhere can score
    # lower than the platform's own view of the region.
    sts = STS_TRANSFER_ZONES.get(region, 1.0)
    try:
        from shared.utils.regions import get_region_sensitivity_multiplier
        return max(sts, get_region_sensitivity_multiplier(region))
    except Exception:
        return sts
