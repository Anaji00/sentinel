import os
import json
import numpy as np
import onnxruntime as ort
import logging
from shared.db import get_redis

logger = logging.getLogger("enrichment.anomaly_scorer")

class DynamicAnomalyScorer:
    def __init__(self, redis_client):
        self.redis = get_redis()
        self.sessions = {}
        self.alpha = 0.1
        self.z_score_threshold = 2.5

        self._load_onnx_models()

    def _load_onnx_models(self):
        model_dir = "/app/models"
        
        # Explicitly map domains to their specific artifact filenames
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

    def _get_temporal_sequence(self, event_type: str, new_features: list, seq_len: int = 10) -> list:
        """
        Maintains a rolling window of the last N ticks in Redis.
        Crucial for feeding the LSTM a continuous time-series sequence.
        """
        if not self.redis:
            # Fallback: mock a sequence if Redis is dead
            return [new_features] * seq_len 

        key = f"sentinel:ml:sequence:{event_type}"
        
        # 1. Push newest tick to the left
        self.redis.raw.lpush(key, json.dumps(new_features))
        # 2. Trim so we only ever store the exact sequence length
        self.redis.raw.ltrim(key, 0, seq_len - 1)
        
        # 3. Retrieve the sequence
        raw_items = self.redis.raw.lrange(key, 0, -1)
        sequence = [json.loads(item) for item in raw_items]
        
        # 4. Reverse it to chronological order (oldest -> newest)
        sequence.reverse()
        return sequence

    def _check_ema_gatekeeper(self, domain: str, raw_score: float) -> bool:
        if not self.redis: return raw_score > 0.60

        mean_key = f"sentinel:ml:ema_mean:{domain}"
        var_key = f"sentinel:ml:ema_var:{domain}"  
        current_mean = float(self.redis.raw.get(mean_key) or 0.5)
        current_var = float(self.redis.raw.get(var_key) or 0.05)
        current_std = np.sqrt(current_var)

        dynamic_threshold = current_mean + (self.z_score_threshold * current_std)
        is_anomaly = raw_score > dynamic_threshold

        # Update rolling baseline smoothly
        new_mean = (self.alpha * raw_score) + ((1 - self.alpha) * current_mean)
        new_var = (self.alpha * (raw_score - current_mean)**2) + ((1 - self.alpha) * current_var)

        self.redis.raw.set(mean_key, new_mean)
        self.redis.raw.set(var_key, new_var)

        return is_anomaly
    
    def score_event(self, event_type: str, features: list) -> dict:
        """
        The Master Router: Shapes the data and executes the appropriate ONNX graph.
        """
        domain = self._get_domain(event_type)
        session = self.sessions.get(domain)

        if not session:
            return {"score": 0.0, "is_significant": False, "domain": domain}
        
        try:
            input_name = session.get_inputs()[0].name

            if domain == "spatial":
                # ─── ISOLATION FOREST INFERENCE ───
                # Shape: [1, N_features]
                X = np.array(features, dtype=np.float32).reshape(1, -1)
                
                predictions = session.run(None, {input_name: X})
                prob_dict = predictions[1][0] 
                risk_score = float(prob_dict.get(-1, 0.5))

            else:
                # ─── LSTM AUTOENCODER INFERENCE ───
                seq_len = 10
                sequence = self._get_temporal_sequence(event_type, features, seq_len)
                
                if len(sequence) < seq_len:
                    # Cold start: Not enough data yet to form a full time-series sequence
                    return {"score": 0.0, "is_significant": False, "domain": domain}

                # Shape: [1, seq_len, N_features]
                X = np.array(sequence, dtype=np.float32).reshape(1, seq_len, -1)
                
                predictions = session.run(None, {input_name: X})
                reconstructed_X = predictions[0]

                # Math: Mean Squared Error (Reconstruction Loss)
                reconstruction_error = np.mean(np.square(X - reconstructed_X))

                # Normalize the MSE to a standard 0.0 -> 1.0 Risk Score
                risk_score = float(1.0 - np.exp(-reconstruction_error))

            # Pass the normalized risk score to the EMA Gatekeeper
            is_significant = self._check_ema_gatekeeper(domain, risk_score)

            return {
                "score": round(risk_score, 4),
                "is_significant": is_significant,
                "domain": domain
            }
            
        except Exception as e:
            logger.error(f"ONNX Scoring failed for {event_type}: {e}")
            return {"score": 0.0, "is_significant": False, "domain": domain}