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
                # reset the corrupted tree instance cleanly
                try:
                    fresh_tree = rrcf.RCTree()
                    fresh_tree.insert_point(point, index=idx)
                    self._forest[i] = fresh_tree
                except Exception:
                    pass

        if valid_trees > 0:
            avg_codisp /= valid_trees
        else:
            return 0.0

        # Sigmoid normalization: maps raw CoDisp to [0, 1]
        # CoDisp of ~3-5 should map to ~0.5-0.7 (anomalous territory)
        score = 1.0 / (1.0 + math.exp(-0.5 * (avg_codisp - 4.0)))
        return round(min(1.0, max(0.0, score)), 4)

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

        # Map max z-score to [0, 1] via sigmoid
        score = 1.0 / (1.0 + math.exp(-0.8 * (max_z - 2.5)))
        return round(min(1.0, max(0.0, score)), 4)

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
    DEFAULT_EXCITATION = {
        ("crypto", "tradfi"):     0.3,    # Crypto liquidation cascades hit equities
        ("tradfi", "crypto"):     0.2,    # Equity flash crash hits crypto
        ("crypto", "crypto"):     0.5,    # Crypto self-excitation (liquidation cascades)
        ("tradfi", "tradfi"):     0.4,    # Equity self-excitation (panic selling)
        ("prediction", "tradfi"): 0.15,   # Prediction market shift → equity positioning
        ("prediction", "crypto"): 0.15,   # Prediction market → crypto
        ("news", "tradfi"):       0.1,    # Breaking news → market reaction
        ("news", "crypto"):       0.1,    # Breaking news → crypto reaction
        ("maritime", "tradfi"):   0.05,   # Maritime disruption → commodity/equity
        ("cyber", "tradfi"):      0.05,   # Cyber attack → market fear
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

        # Compute intensity for the source domain
        intensity = self._compute_intensity(domain, timestamp)
        baseline = self._baselines.get(domain, 0.01)
        ratio = intensity / max(1e-10, baseline)

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

    def get_intensity(self, domain: str, timestamp: float) -> float:
        """Get current intensity for a domain without recording an event."""
        return self._compute_intensity(domain, timestamp)

    def get_excitation_ratio(self, domain: str, timestamp: float) -> float:
        """Get intensity / baseline for a domain. >1.0 = excited state."""
        intensity = self._compute_intensity(domain, timestamp)
        baseline = self._baselines.get(domain, 0.01)
        return intensity / max(1e-10, baseline)

    def _compute_intensity(self, target_domain: str, t: float) -> float:
        """
        Compute λ_d(t) = μ_d + Σ_{d'} Σ_{t_i < t} α_{d',d} * exp(-β * (t - t_i))
        """
        mu = self._baselines.get(target_domain, 0.01)
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
            logger.debug(f"Neo4j BGP feature extraction failed for {origin_as}: {e}")
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
            path_str = ",".join(as_path) if as_path else origin_as
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
            logger.debug(f"Neo4j BGP AS-path upsert failed: {e}")

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
    return STS_TRANSFER_ZONES.get(region, 1.0)
