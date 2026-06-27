"""
services/collector-radar/regime.py
"""

class MarketRegime:
    def __init__(self, redis_client):
        self.redis = redis_client

    async def get_dynamic_thresholds(self) -> tuple[float, float]:
        """
        Calculates dynamic alpha and Z-Score threshold based on VIX state.
        Default standard: Alpha 0.1, Z-Threshold 3.0
        """
        try:
            # Assume a macro collector continuously updates this key
            raw_vix = await self.redis.raw.get("sentinel:macro:vix")
            vix_current = float(raw_vix) if raw_vix else 15.0
        except Exception:
            vix_current = 15.0

        if vix_current < 12.0:
            # Low Volatility (Complacency Regime): Tighten sensitivity, slow memory
            return 0.05, 2.5
        elif vix_current > 25.0:
            # High Volatility (Panic Regime): Widen threshold, fast memory
            return 0.20, 4.5
        else:
            # Standard Operating Regime
            return 0.10, 3.0

# ─── INJECTION INTO QUANT RADAR ───
# In services/collector-radar/main.py:
#
# def evaluate_volume(self, ticker: str, current_vol: float, regime: MarketRegime) -> tuple[bool, float]:
#     alpha, z_threshold = regime.get_dynamic_thresholds()
#     mean, var = self._get_baseline(ticker)
#     ...
#     new_mean = (alpha * current_vol) + ((1 - alpha) * mean)
#     new_var = (alpha * (current_vol - mean)**2) + ((1 - alpha) * var)
#     ...
#     return z_score > z_threshold, z_score