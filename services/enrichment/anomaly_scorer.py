import os
import numpy as np
import onnxruntime as ort
import logging
from shared.db import get_redis

logger = logging.getLogger("enrichment.anomaly_scorer")

class DynamicAnomalyScorer:
    def __init__(self):
        self.redis = get_redis()
        self.sessions = {}
        self.alpha = 0.1
        self.z_score_threshold = 2.5

        self._load_onnx_model()

    def _load_onnx_model(self):
        model_dir = "/app/models"
        for domain in ["spatial", "temporal"]:
            model_path = os.path.join(model_dir, f"{domain}_iforest.onnx")
            try:
                self.sessions[domain] = ort.InferenceSession(model_path, providers=["CPUExecutionProvider"])
                logger.info(f"Loaded ONNX model for {domain}")
            except Exception as e:
                logger.critical(f"🚨 Missing ONNX model: {model_path}. Run train_models.py! Error: {e}")
        
    def _get_domain(self, event_type: str) -> str:
        return "spatial" if event_type in ["vessel_position", "vessel_dark", "bgp_hijack"] else "temporal"

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
        domain = self._get_domain(event_type)
        session = self.sessions.get(domain)

        if not session:
            return {"score": 0.0, "is_significant": False, "domain": domain}
        
        try:
            X = np.array(features, dtype=np.float32).reshape(1, 10, -1)
            input_name = session.get_inputs()[0].name

            # 2. Execute inference (Microseconds)
            # Output structure for Sklearn-ONNX IF is usually [Labels, Probabilities]
            predictions = session.run(None, {input_name: X})

            # Extract raw anomaly probability from the ONNX output array
            # Sklearn IF outputs dict of {class: probability}. We extract the anomaly (-1) class prob.
            prob_dict = predictions[1][0] 
            risk_score = float(prob_dict.get(-1, 0.5))

            # 3. Pass to the EMA Gatekeeper
            is_significant = self._check_ema_gatekeeper(domain, risk_score)

            return {
                "score": round(risk_score, 4),
                "is_significant": is_significant,
                "domain": domain
            }
        except Exception as e:
            logger.error(f"ONNX Scoring failed: {e}")
            return {"score": 0.0, "is_significant": False, "domain": domain}
        
    