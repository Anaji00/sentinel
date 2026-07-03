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

    async def _get_temporal_sequence(self, event_type: str, new_features: list, seq_len: int = 10) -> list:
        """
        Maintains a rolling window of the last N ticks in Redis.
        Crucial for feeding the LSTM a continuous time-series sequence.
        """
        if not self.redis:
            # Fallback: mock a sequence if Redis is dead
            return [new_features] * seq_len 

        key = f"sentinel:ml:sequence:{event_type}"
        
        # PIPELINE ASYNC REDIS

        pipe = self.redis.raw.pipeline()
        pipe.lpush(key, json.dumps(new_features))
        pipe.ltrim(key, 0, seq_len - 1)
        pipe.lrange(key, 0, -1)
        results = await pipe.execute()

        raw_items = results[2]  # The result of lrange
        sequence = [json.loads(item) for item in raw_items if item]
        sequence.reverse()  # Oldest first
        return sequence

    async def _dynamic_normalize(self, ticker: str, feature_name: str, raw_value: float) -> float:
        """
        Calculates a live Z-score for a specific asset's feature.
        Ensures the ONNX model receives normalized inputs ~ N(0,1), preventing magnitude blindness.
        """
        if not self.redis: return raw_value / 1000.0 

        mean_key = f"sentinel:stats:{ticker}:{feature_name}:mean"
        var_key = f"sentinel:stats:{ticker}:{feature_name}:var"
        
        pipe = self.redis.raw.pipeline()
        pipe.get(mean_key)
        pipe.get(var_key)
        res = await pipe.execute()
        
        mean = float(res[0] or raw_value)
        var = float(res[1] or 1.0)
        std_dev = np.sqrt(var) + 1e-5 # Epsilon to prevent division by zero on illiquid assets
        
        # Smoothly update the baseline (Online Variance Approximation)
        new_mean = (self.alpha * raw_value) + ((1 - self.alpha) * mean)
        new_var = (self.alpha * (raw_value - mean)**2) + ((1 - self.alpha) * var)
        
        pipe = self.redis.raw.pipeline()
        pipe.set(mean_key, new_mean)
        pipe.set(var_key, new_var)
        await pipe.execute()
        
        return (raw_value - mean) / std_dev

    async def _check_ema_gatekeeper(self, domain: str, raw_score: float) -> bool:
        if not self.redis: return raw_score > 0.60

        mean_key = f"sentinel:ml:ema_mean:{domain}"
        var_key = f"sentinel:ml:ema_var:{domain}"  
        current_mean = float(await self.redis.raw.get(mean_key) or 0.5)
        current_var = float(await self.redis.raw.get(var_key) or 0.05)
        current_std = np.sqrt(current_var)

        dynamic_threshold = current_mean + (self.z_score_threshold * current_std)
        is_anomaly = raw_score > dynamic_threshold

        # Update rolling baseline smoothly
        new_mean = (self.alpha * raw_score) + ((1 - self.alpha) * current_mean)
        new_var = (self.alpha * (raw_score - current_mean)**2) + ((1 - self.alpha) * current_var)

        pipe = self.redis.raw.pipeline()
        pipe.set(mean_key, new_mean)
        pipe.set(var_key, new_var)
        await pipe.execute()

        return is_anomaly
    
    async def score_event(self, event_type: str, features: list) -> dict:
        """
        The Master Router: Shapes the data and executes the appropriate ONNX graph.
        """
        domain = self._get_domain(event_type)
        session = self.sessions.get(domain)

        if not session:
            return {"score": 0.0, "is_significant": False, "domain": domain}
        
        try:
            loop = asyncio.get_running_loop()
            input_name = session.get_inputs()[0].name
            if domain == "spatial":
                # ─── ISOLATION FOREST INFERENCE ───
                # Shape: [1, N_features]
                X = np.array(features, dtype=np.float32).reshape(1, -1)
                
                predictions = await loop.run_in_executor(None, session.run, None, {input_name: X})
                
                # ── CRITICAL FIX: DYNAMIC OUTPUT PARSING ──
                # ONNX IsolationForest can output either a ZipMap (dict) or a raw ndarray.
                raw_output = predictions[1][0] 
                
                if isinstance(raw_output, dict):
                    risk_score = float(raw_output.get(-1, 0.5))
                else:
                    # Extract raw float. Use abs() because sklearn's decision_function 
                    # returns negative values for outliers, but our EMA gatekeeper 
                    # requires a positive scale where higher = more anomalous.
                    val = float(np.atleast_1d(raw_output)[0])
                    risk_score = abs(val)
                # ──────────────────────────────────────────

            else:
                # ─── LSTM AUTOENCODER INFERENCE ───
                seq_len = 10
                sequence = await self._get_temporal_sequence(event_type, features, seq_len)
                
                if len(sequence) < seq_len:
                    return {"score": 0.0, "is_significant": False, "domain": domain}

                X = np.array(sequence, dtype=np.float32).reshape(1, seq_len, -1)
                
                predictions = await loop.run_in_executor(None, session.run, None, {input_name: X})
                reconstructed_X = predictions[0]
                reconstruction_error = np.mean(np.square(X - reconstructed_X))
                risk_score = float(1.0 - np.exp(-reconstruction_error))

            # Pass the normalized risk score to the EMA Gatekeeper
            is_significant = await self._check_ema_gatekeeper(domain, risk_score)

            return {
                "score": round(risk_score, 4),
                "is_significant": is_significant,
                "domain": domain
            }
            
        except Exception as e:
            logger.error(f"ONNX Scoring failed for {event_type}: {e}", exc_info=True)
            return {"score": 0.0, "is_significant": False, "domain": domain}
        
    def score_vessel_dark(self, mmsi: str, gap_hours: float, region: Optional[str], flags: list, heading: int) -> float:
        sensitivity = 1.0 if region in ["Strait of Hormuz", "Red Sea"] else 0.5
        base = min(1.0, gap_hours / 48.0)
        if "sanctioned" in " ".join(flags).lower():
            base = min(1.0, base * 1.5)
        return round(min(1.0, base * sensitivity / 3.0 + base * 0.5), 3)

    # Note: All helper methods must now be async and use `await self.score_event(...)`
    async def score_crypto_trade(self, asset: str, notional: float, qty: float) -> float:
        norm_notional = await self._dynamic_normalize(f"crypto:{asset}", "notional", notional)
        norm_qty = await self._dynamic_normalize(f"crypto:{asset}", "qty", qty)

        res = await self.score_event("crypto_trade", [norm_notional, norm_qty, 0.0, 0.0, 0.0])
        return res["score"]

    async def score_crypto_candle(self, asset: str, features: list) -> float:
        res = await self.score_event("crypto_trade", (features + [0.0] * 5)[:5])
        return res["score"]

    async def score_financial_trade(self, domain: str, ticker: str, notional: float, volume: float) -> float:
        norm_notional = await self._dynamic_normalize(f"tradfi:{ticker}", "notional", notional)
        norm_volume = await self._dynamic_normalize(f"tradfi:{ticker}", "volume", volume)
        
        res = await self.score_event("tradfi_trade", [norm_notional, norm_volume, 0.0, 0.0, 0.0])
        return res["score"]

    async def score_market_candle(self, domain: str, ticker: str, features: list) -> float:
        res = await self.score_event("tradfi_trade", (features + [0.0] * 5)[:5])
        return res["score"]

    async def score_prediction_trade(self, asset_id: str, notional: float) -> float:
        res = await self.score_event("prediction_market_trade", [notional / 100_000, 0.0, 0.0, 0.0, 0.0])
        return res["score"]
        
    async def score_news(self, named_entities: list, sentiment: float, reliability: float) -> float:
        base = abs(sentiment) * reliability
        entity_boost = min(0.3, len(named_entities) * 0.02)
        return round(min(1.0, base + entity_boost), 3)
