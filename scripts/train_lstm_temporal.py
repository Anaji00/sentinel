# Standard library for interacting with the operating system (e.g., creating folders)
import os
# PyTorch: The core machine learning library we are using to build the brain
import torch
# PyTorch Neural Networks module: Contains the building blocks (layers) for our brain
import torch.nn as nn
# NumPy: A library for fast mathematical operations on large lists of numbers
import numpy as np
# Logging: Helps us print out what the script is doing in real-time
import logging
# Imports our custom database connection function
from shared.db import get_timescale

# Set up the logger to print INFO level messages to the console
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ml.lstm_training")

# Define where we want to save our finished brain (model) and create the folder if it doesn't exist
MODEL_DIR = "/app/models"
os.makedirs(MODEL_DIR, exist_ok=True)

# Define our Neural Network class. It inherits from nn.Module, which is PyTorch's base class for all neural networks.
# An "AutoEncoder" learns to compress data (Encoder) and then decompress it (Decoder). 
# If it fails to decompress a new piece of data well later, we know that data is an "anomaly"!
class TemporalLSTMAutoEncoder(nn.Module):
    def __init__(self, seq_len: int, n_features: int, embedding_dim: int = 64):
        # Initialize the parent class
        super(TemporalLSTMAutoEncoder, self).__init__()
        # seq_len: How many time steps we look at at once (e.g., 10 minutes of data)
        self.seq_len = seq_len
        # n_features: How many data points exist in each time step (e.g., price, volume, etc. = 5)
        self.n_features = n_features
        # embedding_dim: How small we want to compress the data (the "bottleneck")
        self.embedding_dim = embedding_dim

        # The Encoder: Reads the sequence of data and squashes it down to `embedding_dim` size
        self.encoder_lstm = nn.LSTM(
            input_size=n_features,
            hidden_size=embedding_dim,
            num_layers=1,
            batch_first=True # Tells PyTorch our data is formatted as [Batch Size, Sequence Length, Features]
        )

        # The Decoder: Takes the squashed data and tries to rebuild the original sequence
        self.decoder_lstm = nn.LSTM(
            input_size=embedding_dim,
            hidden_size=n_features,
            num_layers=1,
            batch_first=True
        )

    # The 'forward' function dictates how data flows through our network from start to finish
    def forward(self, x):
        # Step 1: Pass data (x) into the encoder. We only care about the final compressed state (hidden_n).
        _, (hidden_n, _,) = self.encoder_lstm(x)

        # Step 2: The decoder needs an input for every step of the sequence to rebuild it.
        # We take our single compressed state and copy (repeat) it `seq_len` times.
        hidden_n = hidden_n.permute(1, 0, 2).repeat(1, self.seq_len, 1)

        # Step 3: Pass the repeated compressed states into the decoder to get our reconstructed data
        reconstructed, _ = self.decoder_lstm(hidden_n)
        return reconstructed
    
def fetch_sequential_data(days: int = 40, seq_len: int = 10) -> torch.Tensor:
    """
    Fetches historical TradFi/Crypto data and chunks it into rolling windows.
    Requires sequential, time-ordered arrays from Timescale.
    """
    # Connect to the database
    db = get_timescale()
    # Grab up to 50,000 recent financial/crypto ML features, ordered oldest to newest
    query = """
        SELECT ml_features FROM events 
        WHERE type IN ('tradfi_trade', 'crypto_liquidation') 
        ORDER BY occurred_at ASC 
        LIMIT 50000
    """
    rows = db.query(query)

    if not rows:
        # If the database is empty, don't crash!
        logger.warning("No data found. Generating synthetic LSTM baseline.")
        # Create fake, random data structured exactly how the model expects it, so training can still happen.
        synthetic = np.random.normal(0,1, (1000, seq_len, 5)).astype(np.float32)
        return torch.tensor(synthetic)
    
    # Convert database rows into a fast NumPy array
    raw_data = np.array([row[0] for row in rows], dtype=np.float32)
    sequences = []
    # Sliding Window: Create overlapping chunks of data.
    # If data is [A, B, C, D] and seq_len is 2, we make chunks: [A, B], [B, C], [C, D].
    # This teaches the model how events flow into each other over time.
    for i in range(len(raw_data) - seq_len):
        sequences.append(raw_data[i:i+seq_len])
    
    # Convert the final list into a PyTorch Tensor (the data format Neural Networks use)
    return torch.tensor(np.array(sequences))

def train_and_export_lstm():
    # Hyperparameters: The manual settings for our training session
    seq_len = 10
    n_features = 5
    epochs = 20 # How many times the model will see the entire dataset
    
    logger.info("Building Temporal LSTM Autoencoder...")
    # Instantiate our model class
    model = TemporalLSTMAutoEncoder(seq_len=seq_len, n_features=n_features)
    # The Optimizer: The mechanism that updates the brain's weights based on its mistakes. Adam is a standard, fast optimizer.
    optimizer = torch.optim.Adam(model.parameters(), lr=0.001)
    # The Criterion (Loss Function): How we measure if the model is doing a bad job.
    # MSE (Mean Squared Error) measures the difference between the original data and the model's reconstructed data.
    criterion = nn.MSELoss() 

    # Get our training data
    X_train = fetch_sequential_data(seq_len=seq_len)
    
    logger.info("Training LSTM (PyTorch)...")
    # Put the model into "training mode"
    model.train()
    for epoch in range(epochs):
        optimizer.zero_grad() # Step 1: Clear out old mathematical gradients from the last loop
        output = model(X_train) # Step 2: Ask the model to reconstruct the data (Forward Pass)
        loss = criterion(output, X_train) # Step 3: Grade the model. Compare the output to the original input.
        loss.backward() # Step 4: Calculate exactly which internal weights caused the mistakes (Backpropagation)
        optimizer.step() # Step 5: Adjust the weights to be slightly better for next time
        
        if epoch % 5 == 0:
            # Print an update every 5 loops
            logger.info(f"Epoch {epoch}/{epochs} | Loss: {loss.item():.4f}")

    # ---------------------------------------------------------
    # THE TIER 1 SECRET: Exporting PyTorch to ONNX C++ Graph
    # ---------------------------------------------------------
    logger.info("Exporting PyTorch model to optimized ONNX graph...")
    # Put the model into "evaluation mode" (locks the weights so they can't change)
    model.eval()
    
    # ONNX requires a "dummy input" of the correct shape to trace the mathematical graph and convert it to C++
    dummy_input = torch.randn(1, seq_len, n_features)
    onnx_path = os.path.join(MODEL_DIR, "temporal_lstm.onnx")
    
    # Actually run the export process
    torch.onnx.export(
        model, 
        dummy_input, 
        onnx_path,
        export_params=True,
        opset_version=11,
        do_constant_folding=True, # Optimizes the model by pre-calculating constant values
        input_names=['input_seq'], # Name the input so other languages/scripts know how to pass data to it
        output_names=['reconstructed_seq'], # Name the output
        # Dynamic axes mean we don't lock the model to a specific batch size (so we can process 1 event or 100 events at a time)
        dynamic_axes={'input_seq': {0: 'batch_size'}, 'reconstructed_seq': {0: 'batch_size'}}
    )
    logger.info(f"✅ LSTM ONNX artifact saved to {onnx_path}")

if __name__ == "__main__":
    train_and_export_lstm()