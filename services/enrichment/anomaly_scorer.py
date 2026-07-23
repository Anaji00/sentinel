import asyncio
import os
import json
import numpy as np
import onnxruntime as ort
import logging
from typing import Optional

logger = logging.getLogger("enrichment.anomaly_scorer")

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
        if not self.redis or not requests:
            return [r[2] / 1000.0 for r in requests]
            
        pipe = self.redis.raw.pipeline()
        for ticker, feature_name, _ in requests:
            pipe.get(f"sentinel:stats:{ticker}:{feature_name}:mean")
            pipe.get(f"sentinel:stats:{ticker}:{feature_name}:var")
        
        res = await pipe.execute()
        
        results = []
        set_pipe = self.redis.raw.pipeline()
        local_cache = {}
        
        idx = 0
        for ticker, feature_name, raw_value in requests:
            key_base = f"sentinel:stats:{ticker}:{feature_name}"
            
            if key_base in local_cache:
                mean, var = local_cache[key_base]
            else:
                mean = float(res[idx] or raw_value)
                var = float(res[idx+1] or 1.0)
            
            idx += 2
            
            std_dev = np.sqrt(var) + 1e-5
            results.append((raw_value - mean) / std_dev)
            
            new_mean = (self.alpha * raw_value) + ((1 - self.alpha) * mean)
            new_var = (self.alpha * (raw_value - mean)**2) + ((1 - self.alpha) * var)
            
            local_cache[key_base] = (new_mean, new_var)
            
        for key_base, (m, v) in local_cache.items():
            set_pipe.set(f"{key_base}:mean", m, ex=604800)
            set_pipe.set(f"{key_base}:var", v, ex=604800)
            
        await set_pipe.execute()
        return results

    async def _check_ema_gatekeeper(self, event_type: str, raw_score: float) -> bool:
        res = await self._check_ema_gatekeeper_batch(event_type, [raw_score])
        return res[0]
        
    async def _check_ema_gatekeeper_batch(self, event_type: str, raw_scores: list) -> list:
        if not self.redis or not raw_scores:
            return [score > 0.60 for score in raw_scores]

        mean_key = f"sentinel:ml:ema_mean:{event_type}"
        var_key = f"sentinel:ml:ema_var:{event_type}"
        
        current_mean = float(await self.redis.raw.get(mean_key) or 0.5)
        current_var = float(await self.redis.raw.get(var_key) or 0.05)
        
        results = []
        for score in raw_scores:
            current_std = np.sqrt(current_var)
            dynamic_threshold = current_mean + (self.z_score_threshold * current_std)
            results.append(score > dynamic_threshold)
            
            old_mean = current_mean
            current_mean = (self.alpha * score) + ((1 - self.alpha) * current_mean)
            current_var = (self.alpha * (score - old_mean)**2) + ((1 - self.alpha) * current_var)

        pipe = self.redis.raw.pipeline()
        pipe.set(mean_key, current_mean)
        pipe.set(var_key, current_var)
        await pipe.execute()
        
        return results
    
    async def score_event(self, event_type: str, entity_id: str, features: list) -> dict:
        res = await self.score_event_batch(event_type, [entity_id], [features])
        return res[0]
        
    async def score_event_batch(self, event_type: str, entities: list, features_list: list) -> list:
        domain = self._get_domain(event_type)
        session = self.sessions.get(domain)
        
        if not session or not features_list:
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
        """Dynamic anomaly scorer stub for batched flight events."""
        if not flights:
            return []
        # Return base anomaly scores for simple positions
        return [0.10 for _ in flights]

