# scripts/train_lstm_temporal.py
import os
import sys 
from pathlib import Path
import asyncio
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

IN_DOCKER = os.path.exists('/.dockerenv')

if not IN_DOCKER:
    if os.getenv("POSTGRES_HOST") == "timescaledb":
        os.environ["POSTGRES_HOST"] = "localhost"
        
    if os.getenv("DATABASE_URL") and "timescaledb:5432" in os.getenv("DATABASE_URL"):
        os.environ["DATABASE_URL"] = os.getenv("DATABASE_URL").replace("timescaledb:5432", "localhost:5432")

    if os.getenv("REDIS_URL") and "redis://" in os.getenv("REDIS_URL"):
        os.environ["REDIS_URL"] = os.getenv("REDIS_URL").replace("redis:6379", "localhost:6379")

import torch
import torch.nn as nn
import numpy as np
import logging
from shared.db import get_timescale
from shared.models.events import EventType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ml.lstm_training")

MODEL_DIR = "/app/models" if IN_DOCKER else str(ROOT / "models")
os.makedirs(MODEL_DIR, exist_ok=True)

class TemporalLSTMAutoEncoder(nn.Module):
    def __init__(self, seq_len: int, n_features: int, embedding_dim: int = 64):
        super(TemporalLSTMAutoEncoder, self).__init__()
        self.seq_len = seq_len
        self.n_features = n_features
        self.embedding_dim = embedding_dim

        self.encoder_lstm = nn.LSTM(
            input_size=n_features,
            hidden_size=embedding_dim,
            num_layers=1,
            batch_first=True
        )
        self.decoder_lstm = nn.LSTM(
            input_size=embedding_dim,
            hidden_size=n_features,
            num_layers=1,
            batch_first=True
        )

    def forward(self, x, h0=None, c0=None):
        if h0 is None or c0 is None:
            _, (hidden_n, _) = self.encoder_lstm(x)
        else:
            _, (hidden_n, _) = self.encoder_lstm(x, (h0, c0))
        hidden_n_rep = hidden_n.permute(1, 0, 2).expand(-1, self.seq_len, -1)
        reconstructed, _ = self.decoder_lstm(hidden_n_rep)
        return reconstructed
    
async def fetch_sequential_data(days: int = 40, seq_len: int = 10) -> torch.Tensor:
    """
    Fetches time-ordered historical features from TimescaleDB across ALL 
    schema-defined financial and block trading event categories.
    """
    db = await get_timescale()
    
    financial_types = [
        EventType.OPTIONS_FLOW.value,
        EventType.DARK_POOL.value,
        EventType.FUTURES_COT.value,
        EventType.PRICE_ANOMALY.value,
        EventType.INSIDER_TRADE.value,
        EventType.EQUITY_BLOCK.value,
        EventType.CRYPTO_TRADE.value,
        EventType.MARKET_CANDLE.value,
        EventType.MARKET_ANOMALY.value,
        EventType.CRYPTO_LIQUIDATION.value,
        EventType.PREDICTION_MARKET_TRADE.value,
        EventType.CRYPTO_TRANSFER.value,
    ]
    
    types_placeholder = ", ".join(f"'{t}'" for t in financial_types)
    
    # ─── SPRINT 1 REMEDIATION: PUSH-DOWN FEATURE ENGINEERING ────────────────────
    # PostgreSQL dynamically constructs the 5-dimensional feature vector at read-time
    # using COALESCE to handle nulls and standardizing the output into a float array.
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
        WHERE type IN ({types_placeholder}) 
        ORDER BY occurred_at ASC 
        LIMIT 50000
    """
    # ────────────────────────────────────────────────────────────────────────────

    logger.info(f"Querying macro-financial features for types: {financial_types}")
    rows = await db.query(query)

    if not rows:
        logger.warning(f"No data found for financial parameters {financial_types}. Generating synthetic LSTM baseline.")
        synthetic = np.random.normal(0, 1, (1000, seq_len, 5)).astype(np.float32)
        return torch.tensor(synthetic)
    
    raw_data = np.array([row['ml_features'] for row in rows], dtype=np.float32)
    # Log-scale unconstrained financial features (premium_usd at index 3, volume at index 4)
    raw_data[:, 3] = np.log1p(np.maximum(0, raw_data[:, 3]) / 1000.0)
    raw_data[:, 4] = np.log1p(np.maximum(0, raw_data[:, 4]) / 1000.0)

    # Z-Score standardization across all 5 features (mean=0, std=1)
    mean = np.mean(raw_data, axis=0, keepdims=True)
    std = np.std(raw_data, axis=0, keepdims=True) + 1e-5
    raw_data = (raw_data - mean) / std

    sequences = []
    
    for i in range(len(raw_data) - seq_len):
        sequences.append(raw_data[i:i+seq_len])
    
    return torch.tensor(np.array(sequences), dtype=torch.float32)

async def train_and_export_lstm():
    seq_len = 10
    n_features = 5
    epochs = 20
    
    logger.info("Building Temporal LSTM Autoencoder...")
    model = TemporalLSTMAutoEncoder(seq_len=seq_len, n_features=n_features)
    optimizer = torch.optim.Adam(model.parameters(), lr=0.001)
    criterion = nn.MSELoss() 

    X_train = await fetch_sequential_data(seq_len=seq_len)
    
    logger.info(f"Training LSTM on {X_train.shape[0]} windows across multi-asset data matrices...")
    model.train()
    for epoch in range(epochs):
        optimizer.zero_grad()
        output = model(X_train)
        loss = criterion(output, X_train)
        loss.backward()
        optimizer.step()
        
        if epoch % 5 == 0:
            logger.info(f"Epoch {epoch}/{epochs} | Loss: {loss.item():.4f}")

    logger.info("Exporting PyTorch model to optimized ONNX graph...")
    model.eval()
    
    dummy_input = torch.randn(1, seq_len, n_features)
    dummy_h0 = torch.zeros(1, 1, 64)
    dummy_c0 = torch.zeros(1, 1, 64)
    onnx_path = os.path.join(MODEL_DIR, "temporal_lstm.onnx")
    
    torch.onnx.export(
        model, 
        (dummy_input, dummy_h0, dummy_c0), 
        onnx_path,
        export_params=True,
        opset_version=14,
        do_constant_folding=True,
        input_names=['input_seq', 'h0', 'c0'],
        output_names=['reconstructed_seq'],
        dynamic_axes={
            'input_seq': {0: 'batch_size'},
            'h0': {1: 'batch_size'},
            'c0': {1: 'batch_size'},
            'reconstructed_seq': {0: 'batch_size'}
        }
    )
    logger.info(f"✅ LSTM ONNX artifact saved to {onnx_path}")

if __name__ == "__main__":

    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(train_and_export_lstm())