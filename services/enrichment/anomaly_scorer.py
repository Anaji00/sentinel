import asyncio
import os
import json
import numpy as np
import onnxruntime as ort
import logging
from shared.utils.metrics import MetricsCollector

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
    def __init__(self, redis_client):
        if redis_client is None:
            raise ValueError("Redis client is required for DynamicAnomalyScorer to function properly.")
        self.redis = redis_client
        self.sessions = {}
        self.alpha = 0.1
        self.z_score_threshold = 1.5
        
        self._thresholds_cache = {}
        self._thresholds_last_loaded = 0.0
        self._thresholds_ttl = 60.0  # 60 seconds config cache TTL

        self._load_onnx_models()

    def _load_onnx_models(self):
        model_dir = "/app/models"
        model_files = {
            "spatial": "spatial_iforest.onnx",
            "temporal": "temporal_lstm.onnx"
        }
        for domain, filename in model_files.items():
            model_path = os.path.join(model_dir, filename)
            try:
                self.sessions[domain] = ort.InferenceSession(model_path, providers=["CPUExecutionProvider"])
                logger.info(f"⚡ Loaded ONNX Engine for {domain}: {filename}")
            except Exception as e:
                logger.critical(f"🚨 Missing ONNX model: {model_path}. Run train_models.py! Error: {e}")

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
        return "spatial" if event_type in ["vessel_position", "vessel_dark", "bgp_hijack"] else "temporal"

    async def _get_temporal_sequence(self, event_type: str, entity_id: str, new_features: list, seq_len: int = 10) -> list:
        if not self.redis:
            return [new_features] * seq_len 

        key = f"sentinel:ml:sequence:{event_type}:{entity_id}"
        pipe = self.redis.raw.pipeline()
        pipe.lpush(key, json.dumps(new_features))
        pipe.ltrim(key, 0, seq_len - 1)
        pipe.lrange(key, 0, -1)
        results = await pipe.execute()

        raw_items = results[2]
        sequence = [json.loads(item) for item in raw_items if item]
        sequence.reverse()
        return sequence

    async def _get_temporal_sequence_batch(self, event_type: str, entities: list, features_list: list, seq_len: int = 10) -> list:
        if not self.redis:
            return [[f] * seq_len for f in features_list]
            
        pipe = self.redis.raw.pipeline()
        for entity_id, new_features in zip(entities, features_list):
            key = f"sentinel:ml:sequence:{event_type}:{entity_id}"
            pipe.lpush(key, json.dumps(new_features))
            pipe.ltrim(key, 0, seq_len - 1)
            pipe.lrange(key, 0, -1)
            
        results = await pipe.execute()
        sequences = []
        for i in range(len(entities)):
            raw_items = results[i*3 + 2]
            seq = [json.loads(item) for item in raw_items if item]
            seq.reverse()
            sequences.append(seq)
        return sequences

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
        if not self.redis or not getattr(self.redis, "raw", None):
            return [score > 0.60 for score in raw_scores]

        mean_key = f"sentinel:ml:ema_mean:{event_type}"
        var_key = f"sentinel:ml:ema_var:{event_type}"
        
        res = await self.redis.raw.eval(
            EMA_GATEKEEPER_LUA, 2, mean_key, var_key, float(self.alpha), float(self.z_score_threshold), *[float(s) for s in raw_scores]
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
        domain = self._get_domain(event_type)
        session = self.sessions.get(domain)
        
        if not session or not features_list:
            MetricsCollector.increment("onnx_scoring_uninitialized_total")
            MetricsCollector.increment(f"onnx_scoring_uninitialized_{domain}")
            return [{"score": 0.0, "is_significant": False, "domain": domain} for _ in features_list]
            
        try:
            loop = asyncio.get_running_loop()
            input_name = session.get_inputs()[0].name
            
            if domain == "spatial":
                X = np.array(features_list, dtype=np.float32)
                predictions = await loop.run_in_executor(None, session.run, None, {input_name: X})
                
                raw_outputs = predictions[1]
                scores = []
                for out in raw_outputs:
                    if isinstance(out, dict):
                        scores.append(float(out.get(-1, 0.5)))
                    else:
                        val = float(np.atleast_1d(out)[0])
                        scores.append(max(0.0, 0.5 - val))
                        
                is_significant_list = await self._check_ema_gatekeeper_batch(event_type, scores)
                return [{"score": round(s, 4), "is_significant": sig, "domain": domain} for s, sig in zip(scores, is_significant_list)]
                
            else:
                seq_len = 10
                sequences = await self._get_temporal_sequence_batch(event_type, entities, features_list, seq_len)
                
                valid_idx = []
                valid_seqs = []
                for i, seq in enumerate(sequences):
                    if len(seq) == seq_len:
                        valid_idx.append(i)
                        valid_seqs.append(seq)
                        
                scores = [0.0] * len(features_list)
                if valid_seqs:
                    input_name = session.get_inputs()[0].name
                    reconstructed_list = []
                    for seq in valid_seqs:
                        x_single = np.array([seq], dtype=np.float32)
                        try:
                            pred_single = await loop.run_in_executor(None, session.run, None, {input_name: x_single})
                            reconstructed_list.append(pred_single[0][0])
                        except Exception:
                            reconstructed_list.append(seq)
                    
                    X = np.array(valid_seqs, dtype=np.float32)
                    reconstructed_X = np.array(reconstructed_list, dtype=np.float32)
                    reconstruction_errors = np.mean(np.square(X - reconstructed_X), axis=(1, 2))
                    
                    for i, err in zip(valid_idx, reconstruction_errors):
                        scores[i] = float(1.0 - np.exp(-err))
                        
                is_significant_list = await self._check_ema_gatekeeper_batch(event_type, scores)
                return [{"score": round(s, 4), "is_significant": sig, "domain": domain} for s, sig in zip(scores, is_significant_list)]
                
        except Exception as e:
            MetricsCollector.increment("onnx_scoring_failures_total")
            MetricsCollector.increment(f"onnx_scoring_failures_{domain}")
            logger.error(f"ONNX Batch Scoring failed for {event_type}: {e}", exc_info=True)
            return [{"score": 0.0, "is_significant": False, "domain": domain} for _ in features_list]

    async def score_vessel_dark(self, mmsi: str, gap_hours: float, region: Optional[str], flags: list, heading: int) -> float:
        config = {"base_divisor": 48.0, "sanctioned_multiplier": 1.5}
        try:
            cfg = await self._get_thresholds_config("vessel_dark")
            config.update(cfg)
        except Exception:
            pass

        base = min(1.0, gap_hours / config["base_divisor"])
        if "sanctioned" in " ".join(flags).lower():
            base = min(1.0, base * config["sanctioned_multiplier"])
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

    async def score_news(self, named_entities: list, sentiment: float, reliability: float) -> tuple:
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
        """Score batched flight events using the spatial ONNX model.
        
        Each flight entry should be a dict with keys like:
        altitude, speed, latitude, longitude, heading
        """
        if not flights:
            return []
        
        features_list = []
        entities = []
        for f in flights:
            if isinstance(f, dict):
                features_list.append([
                    float(f.get('altitude', 0)) / 45000.0,  # Normalize altitude (max ~45k ft)
                    float(f.get('speed', 0)) / 600.0,       # Normalize speed (max ~600 kts)
                    float(f.get('latitude', 0)) / 90.0,      # Normalize latitude
                    float(f.get('longitude', 0)) / 180.0,    # Normalize longitude
                    float(f.get('heading', 0)) / 360.0,      # Normalize heading
                ])
                entities.append(str(f.get('icao24', f.get('callsign', 'unknown'))))
            else:
                features_list.append([0.0, 0.0, 0.0, 0.0, 0.0])
                entities.append('unknown')
        
        results = await self.score_event_batch("vessel_position", entities, features_list)
        return [r["score"] for r in results]

    async def composite_score_event(
        self,
        event_type: str,
        entity_id: str,
        features: list,
        volume_raw: float = 0.0,
        volatility_raw: float = 0.0,
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
            "cross_domain_correlation_score": 0.0,
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
