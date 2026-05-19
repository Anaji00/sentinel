import os
import logging
import numpy as np
from sklearn.ensemble import IsolationForest
from skl2onnx import convert_sklearn
from skl2onnx.common.data_types import FloatTensorType
from shared.db import get_timescale

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ml.training")

# Directory where the compiled ONNX models will be saved
MODEL_DIR = "/app/models"
os.makedirs(MODEL_DIR, exist_ok=True)

def fetch_training_data(domain: str, days: int = 30) -> np.ndarray:
    """
    Fetches historical N-dimensional feature arrays from TimescaleDB.
    If no data is found, generates a synthetic baseline so the pipeline doesn't crash.
    """
    db = get_timescale()
    logger.info(f"Fetching '{domain}' training data for the last {days} days...")
    
    # Determine which types of events belong to which ML domain
    event_types = "('vessel_position', 'bgp_hijack')" if domain == "spatial" else "('tradfi_trade', 'crypto_liquidation')"

    # Query to pull only records that actually have ML features calculated
    query = f"SELECT ml_features FROM events WHERE type IN {event_types} AND occurred_at > NOW() - INTERVAL '{days} days' AND ml_features IS NOT NULL"

    rows = db.query(query)
    if not rows:
        logger.warning(f"No database records found for {domain}. Generating synthetic baseline data to allow model creation.")
        # Fallback: Generate a random normal distribution to prevent the training script from failing on a cold start
        return np.random.normal(loc=0.0, scale=1.0, size=(1000, 5)).astype(np.float32)
        
    logger.info(f"Successfully fetched {len(rows)} valid records from the database.")
    return np.array([row[0] for row in rows], dtype=np.float32)
    
def train_and_export_onnx(domain: str):
    logger.info(f"Training {domain} model...")
    X_train = fetch_training_data(domain)
    num_features = X_train.shape[1]

    # Contamination is the expected proportion of outliers (anomalies) in the dataset.
    # Spatial data (like ships) moves predictably, so anomalies are rarer (1%).
    contamination = 0.01 if domain == "spatial" else 0.05
    logger.info(f"Initializing Isolation Forest with contamination rate={contamination} and {num_features} features.")
    model = IsolationForest(n_estimators=200, contamination=contamination, random_state=42, n_jobs=1)
    model.fit(X_train)
    
    # Convert to pure ONNX C++ Graph. This allows the model to run incredibly fast in the enrichment service.
    logger.info("Converting trained sklearn model to ONNX format...")
    initial_type = [('float_input', FloatTensorType([None, num_features]))]
    onnx_model = convert_sklearn(model, initial_types=initial_type)

    model_path = os.path.join(MODEL_DIR, f"{domain}_iforest.onnx")
    with open(model_path, "wb") as f:
        f.write(onnx_model.SerializeToString())
    
    logger.info(f"✅ ONNX model successfully saved to {model_path}")
    logger.info(f"📊 Training summary: Domain '{domain}' trained on {X_train.shape[0]} samples.")

if __name__ == "__main__":
    logger.info("Starting ML model training pipeline...")
    train_and_export_onnx("spatial")
    train_and_export_onnx("temporal")