"""
services/collector-tradfi/microstructure.py
"""
class MicrostructureAggregator:
    """
    Tracks aggressive vs passive liquidity consumption.
    Replaces the basic OHLCV aggregator in Layer 3.
    """
    def __init__(self, redis_client):
        self.redis = redis_client
        self.buffer = {}

    def add_trade(self, ticker: str, price: float, volume: float, conditions: list):
        """
        Classifies trade aggression based on proximity to presumed bid/ask.
        Note: Requires tick-level quote data or heuristic estimation.
        """
        if ticker not in self.buffer:
            self.buffer[ticker] = {
                "ask_vol": 0.0, "bid_vol": 0.0, "neutral_vol": 0.0, 
                "last_price": price
            }
        
        state = self.buffer[ticker]
        
        # Heuristic Tick Test (Tick-up = Ask Aggression, Tick-down = Bid Aggression)
        # Production systems map this directly to live NBBO quote bounds.
        if price > state["last_price"]:
            state["ask_vol"] += volume
        elif price < state["last_price"]:
            state["bid_vol"] += volume
        else:
            state["neutral_vol"] += volume
            
        state["last_price"] = price

    def get_ofi(self, ticker: str) -> float:
        """
        Returns normalized Order Flow Imbalance.
        Range: -1.0 (Total Sell Dominance) to 1.0 (Total Buy Dominance)
        """
        state = self.buffer.get(ticker)
        if not state: return 0.0
        
        total_directional_vol = state["ask_vol"] + state["bid_vol"]
        if total_directional_vol == 0: return 0.0
        
        return (state["ask_vol"] - state["bid_vol"]) / total_directional_vol

# ─── USAGE ───
# If the Radar Agent deploys surveillance, this module computes the OFI continuously.
# An OFI > 0.6 with high volume indicates aggressive institutional sweeping, triggering
# the primary Alert Manager for human intervention.