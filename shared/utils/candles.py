import json
import logging
import math
import os
from typing import List, Tuple, Dict, Any
from datetime import datetime, timezone

# Closes required before RSI is computed at all.
#
# Standard RSI uses fourteen periods. Eight is a deliberate compromise: enough
# that a single move cannot define the value, few enough that a timeframe still
# filling its history is not silenced for a week.
# A move has to be large enough to act on, not merely unusual for its window.
#
# 0.15% is below any realistic transaction cost round trip, so a candle moving
# less than that is not a trading signal whatever its z-score. The notional
# floor excludes the near-empty windows -- a 240-minute BCHUSDT candle on
# $10,000 of volume is a quote, not a market.
MIN_MATERIAL_MOVE_PCT = float(os.getenv("MIN_MATERIAL_MOVE_PCT", "0.0015"))
MIN_MATERIAL_NOTIONAL_USD = float(os.getenv("MIN_MATERIAL_NOTIONAL_USD", "25000"))

MIN_RSI_OBSERVATIONS = 8

logger = logging.getLogger(__name__)

# What the crypto aggregator computes per candle close. Each entry costs a
# structural-inference pass, so this list is deliberately short; the equity
# collector maintains its own wider set (10m, 1d, 1w) for tickers.
TIMEFRAMES_MINUTES = [1, 5, 15, 30, 60, 240]

# Canonical Redis key layout for OHLCV candle lists. Every producer and consumer
# MUST route through this helper — divergent hand-built keys (e.g. omitting the
# timeframe segment) silently miss and send callers down cold-start paths.
CANDLE_KEY_PREFIX = "sentinel:candles"

# One label per duration. Passing a bare minute count used to be normalised to
# "{n}m", so 60 became "60m" while every consumer asked for "1h" -- the same
# timeframe stored under two different keys depending on how the caller spelled
# it. Crypto producers pass minutes and equity producers pass labels, so hourly
# and four-hourly candles existed for equities and were unreachable for crypto:
# `sentinel:candles:1h:BTCUSDT` never existed, only `...:60m:BTCUSDT`.
_MINUTES_TO_LABEL = {
    1: "1m", 5: "5m", 10: "10m", 15: "15m", 30: "30m",
    60: "1h", 240: "4h", 1440: "1d", 10080: "1w",
}

# Spellings that mean the same duration. Readers consult these so candles cached
# under the old naming stay reachable until they age out of their retention.
_LABEL_ALIASES = {
    "60m": "1h", "1hr": "1h", "h1": "1h",
    "240m": "4h", "4hr": "4h", "h4": "4h",
    "1440m": "1d", "24h": "1d", "d1": "1d",
    "10080m": "1w", "7d": "1w",
}


# The one timeframe where case carries meaning: "1M" is a month and "1m" is a
# minute. Lower-casing before comparison silently turns a request for monthly
# candles into minute candles -- a 43,200x difference returned without error.
_CASE_SENSITIVE = {"1M"}


def normalize_timeframe(timeframe) -> str:
    """Resolves any accepted spelling of a timeframe to its canonical label."""
    raw = str(timeframe).strip()
    if raw in _CASE_SENSITIVE:
        return raw
    tf = raw.lower()
    if tf.isdigit():
        minutes = int(tf)
        return _MINUTES_TO_LABEL.get(minutes, f"{minutes}m")
    return _LABEL_ALIASES.get(tf, tf)


def timeframe_aliases(timeframe) -> List[str]:
    """Every key spelling a reader should try, canonical form first."""
    canonical = normalize_timeframe(timeframe)
    alternates = [raw for raw, target in _LABEL_ALIASES.items() if target == canonical]
    return [canonical] + alternates


def candle_cache_key(asset: str, timeframe: str) -> str:
    """Builds the canonical candle cache key: ``sentinel:candles:{tf}:{ASSET}``.

    ``timeframe`` accepts a label ("5m", "1h", "1d"), a legacy spelling ("60m",
    "240m") or a bare minute count (5, 60). All of them resolve to one key.
    """
    tf = normalize_timeframe(timeframe)
    return f"{CANDLE_KEY_PREFIX}:{tf}:{str(asset).strip().upper()}"

def get_domain_tag(domain: str, asset: str) -> str:
    """
    Dynamically determines the domain classification tag for structured logging & telemetry.
    Returns: 'CRYPTO', 'MACRO', or 'EQUITY'.
    """
    domain_clean = str(domain or "").lower().strip()
    asset_upper = str(asset or "").upper().strip()

    if domain_clean == "crypto" or asset_upper.endswith(("USDT", "BUSD", "USDC", "-USD", "/USD")):
        return "CRYPTO"
    elif (
        domain_clean == "macro"
        or asset_upper.endswith("=F")
        or asset_upper.startswith("^")
        or asset_upper in {"CL", "NG", "BZ", "GC", "SI", "HG", "TNX", "DXY", "VIX", "^VIX"}
    ):
        return "MACRO"
    elif domain_clean in ("tradfi", "equity", "equities", "stocks"):
        return "EQUITY"
    return domain_clean.upper() if domain_clean else "UNKNOWN"

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
                bar_close = block["close"]
                bar_volume = block["volume"]
                bar_notional = bar_close * bar_volume

                history_vol_key = f"{domain}:history{tf}m:{asset}:volumes"
                history_not_key = f"{domain}:history{tf}m:{asset}:notionals"

                pipe = redis_client.raw.pipeline()
                pipe.lpush(history_key, bar_close)
                pipe.ltrim(history_key, 0, 14)
                pipe.expire(history_key, 604800)
                pipe.lpush(history_vol_key, bar_volume)
                pipe.ltrim(history_vol_key, 0, 14)
                pipe.expire(history_vol_key, 604800)
                pipe.lpush(history_not_key, bar_notional)
                pipe.ltrim(history_not_key, 0, 14)
                pipe.expire(history_not_key, 604800)
                await pipe.execute()
                
                # Also store complete OHLCV bar object under the canonical candle key.
                candles_tf_key = candle_cache_key(asset, tf)
                await redis_client.raw.lpush(candles_tf_key, json.dumps(block))
                await redis_client.raw.ltrim(candles_tf_key, 0, 199)
                # A TTL makes the list evictable. Without one, volatile-lru
                # cannot reclaim it and the instance fills until every write
                # is refused.
                await redis_client.raw.expire(candles_tf_key, 604800)
                
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
        
        # RSI needs a window before it means anything.
        #
        # `len(closes) > 1` admitted two closes, and two closes with one
        # down-tick give avg_gain = 0, which lands on exactly 0.0 -- read
        # downstream as maximally oversold when it actually means "almost no
        # data". Measured over an hour: 431 evaluations reported RSI 0.0 against
        # a sensible spread of 42-48 for everything else, and those degenerate
        # scores are what produced "Bearish Structural Anomaly ... moved -0.00%"
        # -- 74 of them on a price change of exactly zero.
        #
        # The long timeframes are where it bites: crypto:history240m holds two
        # entries because the platform has not been up long enough to fill
        # fifteen four-hour candles, and it will not be for a week.
        #
        # Below the minimum the honest answer is the neutral 0.5 already set
        # above: no view, rather than a maximal one.
        has_history = len(closes) > MIN_RSI_OBSERVATIONS
        if has_history:
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
            candle_result = await scorer.score_crypto_candle(asset, features)
            # Tolerates both shapes. These scorers now return the full result so
            # the significance gate's verdict survives, but a bare float is
            # still a valid score and must not become an AttributeError.
            if isinstance(candle_result, dict):
                anomaly = float(candle_result.get("score", 0.0) or 0.0)
                gate_significant = bool(candle_result.get("is_significant", False))
            else:
                anomaly = float(candle_result or 0.0)
                gate_significant = False
        else:
            candle_result = await scorer.score_market_candle(domain, asset, features)
            # Tolerates both shapes. These scorers now return the full result so
            # the significance gate's verdict survives, but a bare float is
            # still a valid score and must not become an AttributeError.
            if isinstance(candle_result, dict):
                anomaly = float(candle_result.get("score", 0.0) or 0.0)
                gate_significant = bool(candle_result.get("is_significant", False))
            else:
                anomaly = float(candle_result or 0.0)
                gate_significant = False
            
        # Sanitize ML inference score against NaN
        if anomaly is None or math.isnan(anomaly) or math.isinf(anomaly):
            anomaly = 0.0
            gate_significant = False

        # Apply watchlist boost (+0.15) for watched equities
        if hasattr(scorer, "check_watchlist"):
            try:
                is_watched = await scorer.check_watchlist(asset, "equities")
                if is_watched:
                    anomaly = min(1.0, anomaly + 0.15)
            except Exception:
                pass
            
        domain_tag = get_domain_tag(domain, asset)

        # A score is not published for a window that has already said it holds
        # no data.
        #
        # The RSI-zero defect was fixed by substituting the neutral 0.5 below the
        # minimum, which is the right value -- and the score computed from it
        # was still published. Live, the 240-minute timeframe carried
        # "RSI: 50.0 | Div: 0.00%" beside "Score: 0.721": a structural score on
        # the longest and most heavily weighted horizon, derived entirely from
        # two placeholder features. Two of five inputs being neutral defaults is
        # not a measurement of anything, and the ranking downstream cannot tell
        # that apart from a real reading.
        if not has_history:
            logger.debug(
                "[%s] %s %s-min: %d closes, below the %d needed. No structural "
                "score published -- RSI and divergence would be placeholders.",
                domain_tag, asset, tf, len(closes), MIN_RSI_OBSERVATIONS,
            )
            # This window, not the whole evaluation.
            #
            # `return None` here abandoned every remaining timeframe the moment
            # one of them lacked history, and handed the caller None where the
            # signature promises List[Tuple[...]]. The longer horizons are
            # exactly the ones that take longest to accumulate closes, so the
            # frame most likely to trigger this was also the one whose
            # departure took the rest of the list with it.
            continue

        logger.info(f"🧠 ML INFERENCE [{domain_tag}] | {asset} {tf}-min Structural Candle | Score: {anomaly:.3f} | Change: {price_change_pct*100:.2f}% | Vol: ${notional_volume/1e6:.2f}M | RSI: {rsi_normalized*100:.1f} | Div: {ema_divergence*100:.2f}%")
        
        # Unusual is not the same as material, and nothing in this path could
        # express the difference.
        #
        # Measured over three hours: 2,689 "Structural Anomaly" events, fifteen
        # a minute, median absolute move 0.190%, average score 0.812 --
        # against statistically validated volume spikes averaging 0.627. A
        # LINKUSDT 5-min candle that moved -0.02% scored 0.873 and a four-sigma
        # $3.74M volume spike scored 0.566, so noise outranked signal in the one
        # place a trader looks first.
        #
        # The detector is not wrong: a 0.02% move can be several sigma against a
        # tight recent distribution. What was missing is the second question --
        # whether a move that unusual is also large enough to act on. A candle
        # that moves less than a floor, or trades less than a floor, is held
        # below the threshold that competes for an inference slot however
        # extreme it was for its own window.
        material = (
            abs(price_change_pct) >= MIN_MATERIAL_MOVE_PCT
            and notional_volume >= MIN_MATERIAL_NOTIONAL_USD
        )

        if anomaly >= 0.6 and material:
            # The gate's verdict rides along with the score. Consumers were
            # re-deriving significance as `anomaly >= 0.65`, a constant that
            # knows nothing about this domain's distribution, while the
            # calibrated answer was computed and discarded one frame up.
            anomalous_frames.append((tf, block, features, anomaly, gate_significant))
        elif anomaly >= 0.6:
            logger.debug(
                "[%s] %s %s-min scored %.3f but moved %.3f%% on $%.2fM -- below the "
                "materiality floor, so it does not compete for an inference slot.",
                domain_tag, asset, tf, anomaly, price_change_pct * 100, notional_volume / 1e6,
            )
            
    return anomalous_frames

def normalize_candle(raw: Dict[str, Any], ticker: str = "") -> Dict[str, Any]:
    """Renders a cached candle in one shape regardless of which producer wrote it.

    The two aggregators emit different schemas. The crypto path writes
    ``{bucket_id, open, high, low, close, volume}`` while the equity collector's
    Lua script writes ``{ts, o, h, l, c, v}``. The API returned whichever it
    found untouched, so a chart reading ``ts`` and ``open`` got nulls for crypto
    and a client reading ``o`` got nulls for equities -- each rendering as an
    empty series next to perfectly good data.

    ``bucket_id`` is preferred over ``start_ts`` for the timestamp: it is the
    bucket boundary, whereas start_ts records when the bucket was first written.
    """
    def _first(*keys):
        for k in keys:
            if k in raw and raw[k] is not None:
                return raw[k]
        return None

    ts = None
    bucket = _first("bucket_id", "bucket")
    if bucket is not None:
        try:
            ts = datetime.fromtimestamp(int(bucket), tz=timezone.utc).isoformat()
        except (TypeError, ValueError, OSError):
            ts = None
    if ts is None:
        ts = _first("ts", "start_ts", "time", "timestamp")

    def _num(*keys):
        v = _first(*keys)
        try:
            return float(v) if v is not None else None
        except (TypeError, ValueError):
            return None

    return {
        "ts": ts,
        "open": _num("open", "o"),
        "high": _num("high", "h"),
        "low": _num("low", "l"),
        "close": _num("close", "c"),
        "volume": _num("volume", "v") or 0.0,
        "ticker": (raw.get("ticker") or ticker or "").upper() or None,
    }
