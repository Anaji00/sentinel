"""
services/collector-radar/regime.py

MARKET REGIME & INTRADAY VWAP VOLUME CURVE ENGINE
==================================================
Calculates dynamic Z-Score thresholds and intraday volume multipliers
to adjust baseline anomaly scoring dynamically.
"""

from datetime import datetime, timezone
import math

class MarketRegime:
    def __init__(self, redis_client):
        self.redis = redis_client

    async def get_dynamic_thresholds(self) -> tuple[float, float]:
        """
        Calculates dynamic alpha and Z-Score threshold based on VIX state.
        Scales Z_threshold dynamically with VIX: Z_th = 3.0 * (VIX / 20.0),
        bounded between 2.5 and 5.0 to prevent false-positive anomaly surges.
        """
        try:
            raw_vix = await self.redis.raw.get("sentinel:macro:vix")
            vix_current = float(raw_vix) if raw_vix else 20.0
        except Exception:
            vix_current = 20.0

        # Dynamic VIX-regime scaling
        z_threshold = max(2.5, min(5.0, round(3.0 * (vix_current / 20.0), 2)))

        if vix_current < 12.0:
            alpha = 0.05
        elif vix_current > 25.0:
            alpha = 0.20
        else:
            alpha = 0.10

        return alpha, z_threshold

    @staticmethod
    def get_intraday_volume_multiplier() -> float:
        """
        Intraday U-shaped volume curve profile multiplier.
        Market open (09:30 EST) and close (16:00 EST) exhibit elevated volume.
        Normalizes volume expectations across trading hours to eliminate false morning/close anomalies.
        """
        now = datetime.now(timezone.utc)
        # Convert to EST minute offset from market open (09:30 EST = 14:30 UTC)
        minute_of_day = now.hour * 60 + now.minute
        open_minute_utc = 14 * 60 + 30  # 14:30 UTC
        close_minute_utc = 21 * 60      # 21:00 UTC

        if open_minute_utc <= minute_of_day <= close_minute_utc:
            progress = (minute_of_day - open_minute_utc) / (close_minute_utc - open_minute_utc)
            # Quadratic U-shaped volume curve: max at 0.0 and 1.0 (multiplier ~1.8x), min at 0.5 (multiplier ~0.8x)
            multiplier = 1.8 - 3.2 * progress * (1 - progress)
            return max(0.8, min(2.0, round(multiplier, 2)))
        return 1.0

    @staticmethod
    def compute_parkinson_volatility(highs: list[float], lows: list[float]) -> float:
        """
        Calculates Parkinson High-Low Volatility.
        sigma_P = sqrt( 1 / (4 * ln(2) * N) * sum( (ln(High_i / Low_i))^2 ) )
        Provides a 5x more statistically efficient volatility estimate than close-to-close variance.
        """
        if not highs or not lows or len(highs) != len(lows):
            return 0.015

        n = len(highs)
        log_ratios_sq_sum = 0.0
        valid_samples = 0

        for h, l in zip(highs, lows):
            if h > 0 and l > 0 and h >= l:
                log_ratio = math.log(h / l)
                log_ratios_sq_sum += log_ratio * log_ratio
                valid_samples += 1

        if valid_samples == 0:
            return 0.015

        variance = (1.0 / (4.0 * math.log(2.0) * valid_samples)) * log_ratios_sq_sum
        return max(0.001, round(math.sqrt(variance), 6))