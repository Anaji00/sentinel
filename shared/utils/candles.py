import json
import logging
import math
from typing import List, Tuple, Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)

TIMEFRAMES_MINUTES = [1, 5, 15, 30, 60, 240]

async def evaluate_multi_timeframe(
    redis_client,
    scorer,
    domain: str,
    asset: str,
    ts: datetime,
    open_p: float,
    high_p: float,
    low_p: float,
    close_p: float,
    volume: float
) -> List[Tuple[int, Dict[str, Any], List[float], float]]:
    """
    Evaluates the incoming tick across multiple timeframes.
    Yields a list of anomalous results: (timeframe_mins, block_data, features, anomaly_score)
    """
    epoch = int(ts.timestamp())
    anomalous_frames = []

    for tf in TIMEFRAMES_MINUTES:
        bucket_epoch = (epoch // (tf * 60)) * (tf * 60)
        bucket_id = str(bucket_epoch)

        block_key = f"{domain}:candle{tf}m:{asset}"
        history_key = f"{domain}:history{tf}m:{asset}:closes"

        current_block_json = await redis_client.raw.get(block_key)
        block = json.loads(current_block_json) if current_block_json else None
        
        if block:
            if block.get("bucket_id") == bucket_id:
                # Same bucket, update it in progress
                block["high"] = max(block["high"], high_p)
                block["low"] = min(block["low"], low_p)
                block["close"] = close_p
                block["volume"] += volume
                block["count"] += 1
            else:
                # Bucket changed! Fully close the old one.
                await redis_client.raw.lpush(history_key, block["close"])
                await redis_client.raw.ltrim(history_key, 0, 14)
                
                block = {
                    "bucket_id": bucket_id,
                    "open": open_p,
                    "high": high_p,
                    "low": low_p,
                    "close": close_p,
                    "volume": volume,
                    "count": 1,
                    "start_ts": ts.isoformat()
                }
        else:
            block = {
                "bucket_id": bucket_id,
                "open": open_p,
                "high": high_p,
                "low": low_p,
                "close": close_p,
                "volume": volume,
                "count": 1,
                "start_ts": ts.isoformat()
            }
            
        await redis_client.raw.set(block_key, json.dumps(block), ex=tf * 60 * 2)

        # ── SMART ANOMALY SCORING (REAL-TIME) ──
        b_open, b_close, b_high, b_low, b_vol = block["open"], block["close"], block["high"], block["low"], block["volume"]
        
        closes_bytes = await redis_client.raw.lrange(history_key, 0, 14)
        closes = [float(c) for c in reversed(closes_bytes)]
        closes.append(b_close)
        
        rsi_normalized = 0.5
        ema_divergence = 0.0
        
        if len(closes) > 1:
            diffs = [closes[i] - closes[i-1] for i in range(1, len(closes))]
            gains = [d if d > 0 else 0.0 for d in diffs]
            losses = [abs(d) if d < 0 else 0.0 for d in diffs]
            avg_gain = sum(gains) / len(diffs)
            avg_loss = sum(losses) / len(diffs)
            
            if avg_loss == 0.0:
                rsi_normalized = 1.0 if avg_gain > 0 else 0.5
            else:
                rs = avg_gain / avg_loss
                rsi_normalized = (100 - (100 / (1 + rs))) / 100.0
                
            if len(closes) >= 5:
                def calc_ema(prices: list[float], n: int) -> float:
                    if len(prices) <= n:
                        return sum(prices) / len(prices) if prices else 0.0
                    alpha = 2 / (n + 1)
                    ema = sum(prices[:n]) / n
                    for p in prices[n:]:
                        ema = p * alpha + ema * (1 - alpha)
                    return ema
                
                fast_ema = calc_ema(closes, 5)
                slow_ema = calc_ema(closes, 14)
                ema_divergence = (fast_ema - slow_ema) / slow_ema if slow_ema != 0 else 0.0

        price_change_pct = (b_close - b_open) / b_open if (b_open and b_open != 0) else 0.0
        volatility_pct   = (b_high - b_low) / b_open if (b_open and b_open != 0) else 0.0
        notional_volume = (b_close or 0.0) * (b_vol or 0.0)
        
        # Sanitize floats against NaN/Inf from bad market feeds
        price_change_pct = 0.0 if math.isnan(price_change_pct) or math.isinf(price_change_pct) else price_change_pct
        volatility_pct   = 0.0 if math.isnan(volatility_pct) or math.isinf(volatility_pct) else volatility_pct
        notional_volume  = 0.0 if math.isnan(notional_volume) or math.isinf(notional_volume) else notional_volume
        rsi_normalized   = 0.5 if math.isnan(rsi_normalized) or math.isinf(rsi_normalized) else rsi_normalized
        ema_divergence   = 0.0 if math.isnan(ema_divergence) or math.isinf(ema_divergence) else ema_divergence
        
        features = [price_change_pct, volatility_pct, notional_volume, rsi_normalized, ema_divergence]
        
        if domain == "crypto":
            anomaly = await scorer.score_crypto_candle(asset, features)
        else:
            anomaly = await scorer.score_market_candle(domain, asset, features)
            
        # Sanitize ML inference score against NaN
        if anomaly is None or math.isnan(anomaly) or math.isinf(anomaly):
            anomaly = 0.0
            
        logger.info(f"🧠 ML INFERENCE | {asset} {tf}-min Structural Candle | Score: {anomaly:.3f} | Change: {price_change_pct*100:.2f}% | Vol: ${notional_volume/1e6:.2f}M | RSI: {rsi_normalized*100:.1f} | Div: {ema_divergence*100:.2f}%")
        
        if anomaly >= 0.6:
            anomalous_frames.append((tf, block, features, anomaly))
            
    return anomalous_frames
