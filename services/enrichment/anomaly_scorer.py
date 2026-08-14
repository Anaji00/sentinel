import asyncio
import os
import json
import time
import numpy as np
import logging
from shared.utils.metrics import MetricsCollector
from shared.utils.streaming_detectors import (
    RRCFDetector,
    KalmanResidualFilter,
    HawkesIntensityTracker,
    FirstStoryDetector,
    BGPGraphFeatureExtractor,
    sts_zone_risk_multiplier,
)
from shared.utils.model_registry import ModelRegistry, ConformalZScoreCalibrator

from typing import Optional, List, Dict, Any, Tuple

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

EMA_GATEKEEPER_LUA = """
local mean_key = KEYS[1]
local var_key = KEYS[2]
local alpha = tonumber(ARGV[1])
local z_thresh = tonumber(ARGV[2])

local current_m = tonumber(redis.call('GET', mean_key) or "0.5")
local current_v = tonumber(redis.call('GET', var_key) or "0.05")

local results = {}
for i = 3, #ARGV do
    local score = tonumber(ARGV[i])
    local current_std = math.sqrt(current_v)
    local dynamic_thresh = current_m + (z_thresh * current_std)
    if score > dynamic_thresh then
        table.insert(results, 1)
    else
        table.insert(results, 0)
    end
    
    local old_m = current_m
    current_m = (alpha * score) + ((1.0 - alpha) * current_m)
    current_v = (alpha * (score - old_m)^2) + ((1.0 - alpha) * current_v)
end

redis.call('SET', mean_key, tostring(current_m))
redis.call('SET', var_key, tostring(current_v))

return results
"""

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
        self._conformal_z_calibrators: Dict[str, ConformalZScoreCalibrator] = {
            domain: ConformalZScoreCalibrator(domain=domain, target_far=0.05, default_z_threshold=1.5)
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
        import time
        now = time.time()
        if self._thresholds_cache and (now - self._thresholds_last_loaded < self._thresholds_ttl):
            return self._thresholds_cache.get(key, {})
            
        try:
            if self.redis:
                raw_cfg = await self.redis.raw.get("sentinel:ml:thresholds")
                if raw_cfg:
                    self._thresholds_cache = json.loads(raw_cfg)
                    self._thresholds_last_loaded = now
        except Exception as e:
            logger.debug(f"Could not load custom ml thresholds from Redis: {e}")
            
        return self._thresholds_cache.get(key, {})
        
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

        # Get conformal z-threshold for this domain (§1.3)
        domain = self._get_domain(event_type)
        calibrator = self._conformal_z_calibrators.get(domain)
        z_thresh = calibrator.z_threshold if calibrator else self.z_score_threshold

        # Observe scores to dynamically calibrate conformal threshold
        if calibrator:
            for score in raw_scores:
                calibrator.observe(score)

        if not self.redis or not getattr(self.redis, "raw", None):
            return [score > 0.60 for score in raw_scores]

        mean_key = f"sentinel:ml:ema_mean:{event_type}"
        var_key = f"sentinel:ml:ema_var:{event_type}"
        
        res = await self.redis.raw.eval(
            EMA_GATEKEEPER_LUA, 2, mean_key, var_key, float(self.alpha), float(z_thresh), *[float(s) for s in raw_scores]
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
        
    async def score_event_batch(self, event_type: str, entities: list, features_list: list) -> list:
        """Score events using streaming RRCF detectors (replaces ONNX IsolationForest)."""
        domain = self._get_domain(event_type)
        detector = self._rrcf_detectors.get(domain)
        
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
        
        # 5. Hijack override: known hijacks always score high regardless of RRCF
        if is_hijack:
            rrcf_score = max(rrcf_score, 0.85)
        
        # 6. Path novelty boost: never-seen AS-prefix pair is strong hijack signal
        if graph_features["path_novelty"] > 0.9:
            rrcf_score = min(1.0, rrcf_score * 1.3)
        
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
        """Score a vessel dark event with contextual STS zone significance.
        
        A 3-hour gap near a known STS transfer zone is NOT the same event
        as a 3-hour gap in open ocean — per current maritime-intel practice.
        Gap anomaly weight scales with dwell time × proximity to sanctioned-transfer zone.
        """
        config = {"base_divisor": 48.0, "sanctioned_multiplier": 1.5}
        try:
            cfg = await self._get_thresholds_config("vessel_dark")
            config.update(cfg)
        except Exception:
            pass

        base = min(1.0, gap_hours / config["base_divisor"])
        if "sanctioned" in " ".join(flags).lower():
            base = min(1.0, base * config["sanctioned_multiplier"])
        
        # Contextual gap significance: STS transfer zone proximity multiplier
        zone_mult = sts_zone_risk_multiplier(region)
        base = min(1.0, base * zone_mult)
        
        return round(min(1.0, base), 3)

    async def score_crypto_trade(self, asset: str, notional: float, qty: float) -> float:
        res = await self.score_crypto_trade_batch([(asset, notional, qty)])
        return res[0]

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

    async def score_crypto_candle(self, asset: str, features: list) -> float:
        if len(features) >= 3:
            features[2] = await self._dynamic_normalize(f"crypto:{asset}", "candle_notional", features[2])
        res = await self.score_event("crypto_candle", asset, (features + [0.0] * 5)[:5])
        return res["score"]

    async def score_financial_trade(self, domain: str, ticker: str, notional: float, volume: float) -> float:
        res = await self.score_financial_trade_batch(domain, [(ticker, notional, volume)])
        return res[0]
        
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
            
        res = await self.score_event_batch("tradfi_trade", entities, features_list)
        return [r["score"] for r in res]

    async def score_market_candle(self, domain: str, ticker: str, features: list) -> float:
        if len(features) >= 3:
            features[2] = await self._dynamic_normalize(f"{domain}:{ticker}", "candle_notional", features[2])
        res = await self.score_event("tradfi_candle", ticker, (features + [0.0] * 5)[:5])
        return res["score"]

    async def score_prediction_trade(self, asset_id: str, notional: float) -> float:
        config = {"divisor": 100_000.0}
        try:
            cfg = await self._get_thresholds_config("prediction_trade")
            config.update(cfg)
        except Exception:
            pass
        res = await self.score_event("prediction_market_trade", asset_id, [notional / config["divisor"], 0.0, 0.0, 0.0, 0.0])
        return res["score"]

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
        """
        Increments a Redis-backed frequency counter for an entity in a given domain over a rolling window.
        Returns a progressive boost: 0.05 per repeat mention, capped at 0.20.
        """
        if not self.redis or not entity_id:
            return 0.0
        try:
            key = f"sentinel:frequency:{domain}:{entity_id.lower()}"
            pipe = self.redis.raw.pipeline()
            pipe.incr(key)
            pipe.expire(key, window_seconds)
            results = await pipe.execute()
            count = results[0]
            if count <= 1:
                return 0.0
            return min(0.20, (count - 1) * 0.05)
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

    async def score_aviation_batch(self, flights: list) -> list:
        """Score batched flight events using Kalman residuals + RRCF.
        
        Each flight entry should be a dict with keys like:
        altitude, speed, latitude, longitude, heading, timestamp
        """
        if not flights:
            return []
        
        entities = []
        lats = []
        lons = []
        speeds = []
        headings = []
        timestamps = []
        extra_features = []
        
        for f in flights:
            if isinstance(f, dict):
                entities.append(str(f.get('icao24', f.get('callsign', 'unknown'))))
                lats.append(float(f.get('latitude', 0)))
                lons.append(float(f.get('longitude', 0)))
                speeds.append(float(f.get('speed', 0)))
                headings.append(float(f.get('heading', 0)))
                timestamps.append(float(f.get('timestamp', time.time())))
                extra_features.append([
                    float(f.get('altitude', 0)) / 45000.0,  # Normalized altitude
                ])
            else:
                entities.append('unknown')
                lats.append(0.0)
                lons.append(0.0)
                speeds.append(0.0)
                headings.append(0.0)
                timestamps.append(time.time())
                extra_features.append([0.0])
        
        results = await self.score_kinematic_event_batch(
            entities, lats, lons, speeds, headings, timestamps, extra_features
        )
        return [r["score"] for r in results]

    async def composite_score_event(
        self,
        event_type: str,
        entity_id: str,
        features: list,
        volume_raw: float = 0.0,
        volatility_raw: float = 0.0,
        hawkes_ratio: float = 0.0,
    ) -> dict:
        """
        Composite anomaly scoring with dimensional breakdown.
        Returns an AnomalyBreakdown dict with per-dimension sub-scores
        so agents receive structured reasoning inputs instead of opaque floats.
        """
        domain = self._get_domain(event_type)
        base_result = await self.score_event(event_type, entity_id, features)
        base_score = base_result.get("score", 0.0)

        # Dimensional sub-scores
        spatial_score = base_score if domain == "spatial" else 0.0
        temporal_score = base_score if domain == "temporal" else 0.0

        # Volume z-score via dynamic normalization
        volume_z = 0.0
        if volume_raw > 0:
            try:
                volume_z = await self._dynamic_normalize(entity_id, "volume", volume_raw)
            except Exception:
                pass

        # Volatility z-score
        volatility_z = 0.0
        if volatility_raw > 0:
            try:
                volatility_z = await self._dynamic_normalize(entity_id, "volatility", volatility_raw)
            except Exception:
                pass

        # EWMA volatility from recent returns stored in Redis
        ewma_vol = await self._compute_ewma_volatility(entity_id, volatility_raw)

        # Composite: weighted combination of dimensional scores
        composite = (
            0.35 * base_score
            + 0.20 * min(1.0, max(0.0, abs(volume_z) / 3.0))
            + 0.20 * min(1.0, max(0.0, abs(volatility_z) / 3.0))
            + 0.25 * temporal_score
        )
        composite = round(min(1.0, max(0.0, composite)), 4)

        is_significant = base_result.get("is_significant", False) or composite > 0.65

        return {
            "composite_score": composite,
            "spatial_score": round(spatial_score, 4),
            "temporal_score": round(temporal_score, 4),
            "volume_z_score": round(volume_z, 4),
            "volatility_z_score": round(volatility_z, 4),
            "cross_domain_correlation_score": round(hawkes_ratio, 4),
            "ewma_volatility": round(ewma_vol, 6),
            "is_significant": is_significant,
            "domain": domain,
        }

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
