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

    async def _check_ema_gatekeeper(self, domain: str, raw_score: float) -> bool:
        res = await self._check_ema_gatekeeper_batch(domain, [raw_score])
        return res[0]
        
    async def _check_ema_gatekeeper_batch(self, domain: str, raw_scores: list) -> list:
        if not self.redis or not raw_scores:
            return [score > 0.60 for score in raw_scores]

        mean_key = f"sentinel:ml:ema_mean:{domain}"
        var_key = f"sentinel:ml:ema_var:{domain}"
        
        current_mean = float(await self.redis.raw.get(mean_key) or 0.5)
        current_var = float(await self.redis.raw.get(var_key) or 0.05)
        
        results = []
        for score in raw_scores:
            current_std = np.sqrt(current_var)
            dynamic_threshold = current_mean + (self.z_score_threshold * current_std)
            results.append(score > dynamic_threshold)
            
            current_mean = (self.alpha * score) + ((1 - self.alpha) * current_mean)
            current_var = (self.alpha * (score - current_mean)**2) + ((1 - self.alpha) * current_var)

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
                        
                is_significant_list = await self._check_ema_gatekeeper_batch(domain, scores)
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
                    X = np.array(valid_seqs, dtype=np.float32)
                    predictions = await loop.run_in_executor(None, session.run, None, {input_name: X})
                    reconstructed_X = predictions[0]
                    reconstruction_errors = np.mean(np.square(X - reconstructed_X), axis=(1, 2))
                    
                    for i, err in zip(valid_idx, reconstruction_errors):
                        scores[i] = float(1.0 - np.exp(-err))
                        
                is_significant_list = await self._check_ema_gatekeeper_batch(domain, scores)
                return [{"score": round(s, 4), "is_significant": sig, "domain": domain} for s, sig in zip(scores, is_significant_list)]
                
        except Exception as e:
            logger.error(f"ONNX Batch Scoring failed for {event_type}: {e}", exc_info=True)
            return [{"score": 0.0, "is_significant": False, "domain": domain} for _ in features_list]

    async def score_vessel_dark(self, mmsi: str, gap_hours: float, region: Optional[str], flags: list, heading: int) -> float:
        config = {"base_divisor": 48.0, "sanctioned_multiplier": 1.5}
        try:
            if self.redis:
                raw_cfg = await self.redis.raw.get("sentinel:ml:thresholds")
                if raw_cfg:
                    cfg = json.loads(raw_cfg).get("vessel_dark", {})
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
        res = await self.score_event("crypto_trade", asset, (features + [0.0] * 5)[:5])
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
        res = await self.score_event("tradfi_trade", ticker, (features + [0.0] * 5)[:5])
        return res["score"]

    async def score_prediction_trade(self, asset_id: str, notional: float) -> float:
        config = {"divisor": 100_000.0}
        try:
            if self.redis:
                raw_cfg = await self.redis.raw.get("sentinel:ml:thresholds")
                if raw_cfg:
                    cfg = json.loads(raw_cfg).get("prediction_trade", {})
                    config.update(cfg)
        except Exception:
            pass
        res = await self.score_event("prediction_market_trade", asset_id, [notional / config["divisor"], 0.0, 0.0, 0.0, 0.0])
        return res["score"]
        
    async def score_news(self, named_entities: list, sentiment: float, reliability: float) -> tuple:
        config = {"entity_boost": 0.02, "max_boost": 0.3}
        try:
            if self.redis:
                raw_cfg = await self.redis.raw.get("sentinel:ml:thresholds")
                if raw_cfg:
                    cfg = json.loads(raw_cfg).get("news", {})
                    config.update(cfg)
        except Exception:
            pass
            
        semantic_boost = 0.0
        semantic_tags = []
        if self.redis and named_entities:
            pipe = self.redis.raw.pipeline()
            for tag in named_entities:
                pipe.get(f"sentinel:semantic_sentiment:{tag.lower()}")
            results = await pipe.execute()
            for tag, res in zip(named_entities, results):
                if res:
                    val = float(res)
                    semantic_boost += abs(val) * 0.1
                    sentiment = sentiment * 0.5 + (val * 0.5)
                    semantic_tags.append(f"semantic:{'positive' if val > 0 else 'negative' if val == -1.0 else 'critical'}")
                    
        base = abs(sentiment) * reliability
        entity_boost = min(config["max_boost"], len(named_entities) * config["entity_boost"])
        final_score = round(min(1.0, base + entity_boost + semantic_boost), 3)
        return final_score, semantic_tags
