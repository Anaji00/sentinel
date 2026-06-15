import os
import sys
from pathlib import Path

# ─── 1. SYS.PATH INJECTION ──────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# ─── 2. CONTEXT-AWARE NETWORK ROUTING ───────────────────────────────────────
from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

IN_DOCKER = os.path.exists('/.dockerenv')
if not IN_DOCKER:
    if os.getenv("POSTGRES_HOST") == "timescaledb":
        os.environ["POSTGRES_HOST"] = "localhost"
    if os.getenv("REDIS_URL") and "redis://" in os.getenv("REDIS_URL"):
        os.environ["REDIS_URL"] = os.getenv("REDIS_URL").replace("redis:6379", "localhost:6379")
# ────────────────────────────────────────────────────────────────────────────

import logging
import numpy as np
from sklearn.ensemble import IsolationForest
from skl2onnx import convert_sklearn
from skl2onnx.common.data_types import FloatTensorType
from shared.db import get_timescale

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ml.training")

MODEL_DIR = "/app/models" if IN_DOCKER else str(ROOT / "models")
os.makedirs(MODEL_DIR, exist_ok=True)

def fetch_training_data(domain: str, days: int = 30) -> np.ndarray:
    """
    Fetches historical N-dimensional feature arrays from TimescaleDB.
    If no data is found, generates a synthetic baseline so the pipeline doesn't crash.
    """
    db = get_timescale()
    logger.info(f"Fetching '{domain}' training data for the last {days} days...")
    
    event_types = "('vessel_position', 'bgp_hijack')" if domain == "spatial" else "('tradfi_trade', 'crypto_liquidation')"

    # ─── 3. PUSH-DOWN FEATURE ENGINEERING ───────────────────────────────────
    query = f"""
        SELECT 
            ARRAY[
                COALESCE(anomaly_score, 0.0),
                COALESCE(sentiment, 0.0),
                COALESCE(source_reliability, 0.5),
                COALESCE((financial_data->>'premium_usd')::numeric, 0.0),
                COALESCE((financial_data->>'volume')::numeric, 0.0)
            ]::float[] AS ml_features
        FROM events 
        WHERE type IN {event_types} 
        AND occurred_at > NOW() - INTERVAL '{days} days'
    """
    
    try:
        rows = db.query(query)
    except Exception as e:
        logger.warning(f"Database query failed: {e}")
        rows = []

    if not rows:
        logger.warning(f"No database records found for {domain}. Generating synthetic baseline data to allow model creation.")
        return np.random.normal(loc=0.0, scale=1.0, size=(1000, 5)).astype(np.float32)
        
    logger.info(f"Successfully fetched {len(rows)} valid records from the database.")
    return np.array([row['ml_features'] for row in rows], dtype=np.float32)
    
def train_and_export_onnx(domain: str):
    logger.info(f"Training {domain} model...")
    X_train = fetch_training_data(domain)
    num_features = X_train.shape[1]

    contamination = 0.01 if domain == "spatial" else 0.05
    logger.info(f"Initializing Isolation Forest with contamination rate={contamination} and {num_features} features.")
    
    model = IsolationForest(n_estimators=200, contamination=contamination, random_state=42, n_jobs=1)
    model.fit(X_train)
    
    logger.info("Converting trained sklearn model to ONNX format...")
    initial_type = [('float_input', FloatTensorType([None, num_features]))]
    
    # ─── 4. OPSET PINNING (Fixes Version 4 Unsupported Error) ───────────────
    onnx_model = convert_sklearn(
        model, 
        initial_types=initial_type,
        target_opset={'': 12, 'ai.onnx.ml': 3}
    )
    # ────────────────────────────────────────────────────────────────────────

    model_path = os.path.join(MODEL_DIR, f"{domain}_iforest.onnx")
    with open(model_path, "wb") as f:
        f.write(onnx_model.SerializeToString())
    
    logger.info(f"✅ ONNX model successfully saved to {model_path}")
    logger.info(f"📊 Training summary: Domain '{domain}' trained on {X_train.shape[0]} samples.")

if __name__ == "__main__":
    logger.info("Starting ML model training pipeline...")
    train_and_export_onnx("spatial")
    train_and_export_onnx("temporal")