"""
services/enrichment/anomaly_scorer.py
 
THE JUDGE (ML UPGRADED)
=======================
Calculates a Score from 0.0 (Normal) to 1.0 (Critical/Anomalous).
Now features Unsupervised Machine Learning (Isolation Forests) for 
financial data to automatically adapt to dynamic market baselines.
"""

import logging
import pandas as pd  # Pandas is used for handling tabular data (like spreadsheets) in memory.
from typing import List, Optional
from concurrent.futures import ThreadPoolExecutor  # For running ML training in the background without blocking the main app.
# Isolation Forest is an unsupervised Machine Learning algorithm.
# It detects anomalies by randomly "cutting" data points. Normal points take many
# cuts to isolate, while weird/anomalous points take very few cuts.
from sklearn.ensemble import IsolationForest

import time
# We import a helper that tells us if a location (like "Strait of Hormuz")
# is a high-risk zone.
from shared.db import get_timescale
from shared.utils.regions import get_region_sensitivity_multiplier

logger = logging.getLogger("enrichment.anomaly_scorer")


# ── SCORING RULES ─────────────────────────────────────────────────────────────
# Think of these as "Points" on a driver's license, but in reverse.
# If a vessel has these flags, we add these points to their risk score.
FLAG_WEIGHTS = {
    "sanctioned_ofac":               0.40,  # CRITICAL: Officially on a US sanctions list.
    "sanctioned_ofac_owner":         0.35,  # HIGH: Owned by a sanctioned company.
    "sanctions_adjacent_flag_state": 0.20,  # MEDIUM: Flagged in a country known for lax rules (e.g., Iran, North Korea).
    "possible_iran_connected":       0.25,  # MEDIUM: Intelligence suggests a link to Iran.
    "dark_pattern":                  0.15,  # LOW-MED: History of turning off tracking (AIS).
    "shell_company":                 0.10,  # LOW: Ownership structure is hidden/complex.
}
 
# KEYWORDS: If we see these words in a news headline, the "Temperature" goes up.
# Used to detect geopolitical unrest.
HIGH_RISK_WORDS = {
    "iran", "irgc", "russia", "north korea", "attack", "missile",
    "sanctions", "seized", "military", "blockade", "explosion",
    "strike", "sabotage", "tanker", "detained", "hijack",
}

class AnomalyScorer:
    """
    The Calculator.
    It needs a Redis connection to look up 'Baselines' (e.g., "What is the
    normal speed for a Tanker?").
    """
    def __init__(self, redis_client):
        # Store the connection to Redis (memory cache) for later use.
        self._redis = redis_client
        self._db = get_timescale()
        self._ml_trade_models = {}
        self._ml_candle_models = {}
        self._crypto_trade_models = {}
        self._crypto_candle_models = {}
        self.MODEL_TTL_SECONDS = 86400

        self._training_locks = set()  # To prevent multiple simultaneous trainings for the same asset
        self._training_pool = ThreadPoolExecutor(max_workers=5, thread_name_prefix="ml_trainer")  # Limit to 5 concurrent trainings to avoid overload
    
    def _trigger_background_training(self, domain: str, ticker: str, model_type: str):
        """Spawns a background thread to train an ML model without blocking the consumer."""
        lock_key = f"{domain}_{model_type}_{ticker}"
        if lock_key in self._training_locks:
            logger.info(f"Training already in progress for {lock_key}, skipping duplicate trigger.")
            return
        
        self._training_locks.add(lock_key)

        def train_task():
            try:
                if model_type == "tradfi_trade":
                    self._train_financial_trade_model(ticker, domain)
                elif model_type == "tradfi_candle":
                    self._train_market_candle_model(ticker, domain)
                elif model_type == "crypto_trade":
                    self._train_crypto_trade_model(ticker)
                elif model_type == "crypto_candle":
                    self._train_crypto_candle_model(ticker)
                logger.info(f"✅ ML Model Ready: {lock_key}")
            except Exception as e:
                logger.error(f"Error during ML training for {lock_key}: {e}")
            finally:
                self._training_locks.remove(lock_key)
        
        self._training_pool.submit(train_task)


    def _train_financial_trade_model(self, ticker: str, domain: str) -> Optional[IsolationForest]:
        """
        Pulls the last 7 days of trade data for a specific ticker to train 
        an unsupervised Isolation Forest model on the fly.
        """
        try:
            # Query recent history to understand what "normal" looks like for this asset
            sql = """
                SELECT (financial_data->>'premium_usd')::numeric as notional, 
                       (financial_data->>'volume')::numeric as size
                FROM events 
                WHERE financial_data->>'ticker' = %s 
                AND financial_data->>'trade_type' = 'RAW_TRADE'
                AND occurred_at > NOW() - INTERVAL '7 days'
                LIMIT 5000
            """
            rows = self._db.query(sql, (ticker,))

            if len(rows) < 50:
                # We need a minimum amount of historical data to know what "normal" is.
                # If we have less than 50 trades, we refuse to train the model to prevent false positives.
                return None
            
            # Convert the raw database rows (dictionaries) into a Pandas DataFrame for easy ML processing.
            df = pd.DataFrame(rows).astype(float).fillna(0)

            # ── MACHINE LEARNING SETUP ──────────────────────────────────────────
            # n_estimators=100: Create 100 "trees" to vote on whether a point is weird.
            # contamination=0.01: We assume only 1% of the training data is actually anomalous.
            # random_state=42: Ensures our results are reproducible (always generates the same trees).
            model = IsolationForest(n_estimators=100, contamination=0.01, random_state=42)
            
            # "Fit" tells the model to study the historical trade sizes and premium amounts.
            model.fit(df[['notional', 'size']])

            self._ml_trade_models[f"{domain}_{ticker}"] = (model, time.time())
           

        except Exception as e:
            logger.error(f"Failed to train ML model for {ticker}: {e}")
            return None
        
    # ── 2. OHLCV CANDLE ML ────────────────────────────────────────────────────

    def _train_market_candle_model(self, ticker: str, domain: str) -> Optional[IsolationForest]:
        """Trains an Isolation Forest specifically on 1-minute market structures."""
        try:
            # Reconstruct the 3D feature vector from historical candle closes
            sql = """
            SELECT 
                ABS(((financial_data->>'close_price')::numeric - (financial_data->>'open_price')::numeric) / NULLIF((financial_data->>'open_price')::numeric, 0)) as price_change_pct,
                ((financial_data->>'high_price')::numeric - (financial_data->>'low_price')::numeric) / NULLIF((financial_data->>'open_price')::numeric, 0) as volatility_pct,
                (financial_data->>'premium_usd')::numeric as notional
            FROM events 
            WHERE financial_data->>'ticker' = %s 
            AND financial_data->>'trade_type' = 'OHLCV_MINUTE_BAR'
            AND occurred_at > NOW() - INTERVAL '7 days'
            LIMIT 5000
        """
            rows = self._db.query(sql, (ticker,))
            valid_rows = [r for r in rows if r['price_change_pct'] is not None and r['volatility_pct'] is not None]

            if len(rows) < 50: return
            
            df = pd.DataFrame(valid_rows).astype(float).fillna(0)
            model = IsolationForest(n_estimators=100, contamination=0.01, random_state=42)
            model.fit(df[['price_change_pct', 'volatility_pct', 'notional']])
            
            self._ml_candle_models[f"{domain}_{ticker}"] = (model, time.time())
        except Exception as e:
            logger.error(f"Failed to train candle model for {ticker}: {e}")
            return None
        
    
    def score_market_candle(self, domain: str, ticker: str, features: List[float]) -> float:
        """
        Scores a live market candle against the ML model. Returns 0.0 to 1.0.
        """
        if not ticker or len(features) != 3:
            return 0.0
        model_key = f"{domain}_{ticker}"
        cache_entry = self._ml_candle_models.get(model_key)

        if cache_entry:
            model, trained_at = cache_entry
            if time.time() - trained_at > self.MODEL_TTL_SECONDS:
                logger.info(f"ML model for {model_key} expired. Triggering retrain.")
                self._trigger_background_training(domain, ticker, "tradfi_candle")
        else:
            self._trigger_background_training(domain, ticker, "tradfi_candle")
            # Mathematical fallback using both price delta and intra-candle volatility
            return round(min(1.0, (features[0] * 15) + (features[1] * 5)), 3)
       
        X_new = pd.DataFrame([{
            "price_change_pct": features[0], 
            "volatility_pct": features[1], 
            "notional": features[2]
        }])
        
        if model.predict(X_new)[0] == 1:
            return 0.1 
        
        raw_score = model.decision_function(X_new)[0]
        return round(min(1.0, 0.75 + abs(raw_score)), 3)
        
    def score_financial_trade(self, domain: str, ticker: str, notional_usd: float, size: float) -> float:
        """
        Scores a live trade against the ML model. Returns 0.0 to 1.0.
        """
        if not ticker or notional_usd <= 0:
            return 0.0
        
        model_key = f"{domain}_{ticker}"
        model = self._ml_trade_models.get(model_key)

        if not model:
            self._trigger_background_training(ticker, domain, "tradfi_trade")

            return round(min(1.0, notional_usd / 2_000_000), 3)
    
        # Prepare the new, incoming live trade in the exact same format as the training data.
        X_new = pd.DataFrame([{"notional": notional_usd, "size": size}])

        # ── ML INFERENCE (PREDICTION) ─────────────────────────────────────────
        # predict() returns an array. [1] means normal (inlier), [-1] means anomaly (outlier).
        if model.predict(X_new)[0] == 1:
            return 0.1  # Normal Baseline, low risk.
        
        # If anomalous (-1), use decision_function to gauge EXACTLY how weird it is.
        # Returns a negative float. The more negative, the more severe the anomaly.
        raw_score = model.decision_function(X_new)[0]
        logger.warning(f"Potential ML Anomaly Detected for {ticker}: Notional ${notional_usd}, Size {size}, Raw Score {raw_score}")
        return round(min(1.0, 0.75 + abs(raw_score)), 3)
        # Convert the negative ML score into a 0.0 to 1.0 percentage for our system.
    
    def _train_crypto_trade_model(self, asset: str) -> Optional[IsolationForest]:
        try:
            # Query the crypto_data column, not financial_data
            sql = """
                SELECT ((crypto_data->>'price')::numeric * (crypto_data->>'size_tokens')::numeric) as notional, 
                       (crypto_data->>'size_tokens')::numeric as size
                FROM events 
                WHERE crypto_data->>'pair' = %s 
                AND crypto_data->>'trade_type' = 'LARGE_SPOT'
                AND occurred_at > NOW() - INTERVAL '7 days'
                ORDER BY occurred_at DESC
                LIMIT 5000
            """
            rows = self._db.query(sql, (asset,))

            if len(rows) < 50: return None
            
            df = pd.DataFrame(rows).astype(float).fillna(0)
            
            # Contamination is slightly higher for crypto due to fatter tails in the distribution
            model = IsolationForest(n_estimators=100, contamination=0.02, random_state=42)
            model.fit(df[['notional', 'size']])
            
            self._crypto_trade_models[asset] = (model, time.time())
            logger.info(f"Trained Crypto Trade model for {asset} with {len(df)} samples.")
        except Exception as e:
            logger.error(f"Failed to train Crypto Trade model for {asset}: {e}")
            return None

    def score_crypto_trade(self, asset: str, notional_usd: float, size: float) -> float:
        if not asset or notional_usd <= 0: return 0.0
        
        cached_entry = self._crypto_trade_models.get(asset)
        if cached_entry:
            model, trained_at = cached_entry
            if time.time() - trained_at > self.MODEL_TTL_SECONDS:
                self._trigger_background_training("crypto", asset, "crypto_trade")
        else:
            self._trigger_background_training("crypto", asset, "crypto_trade")
            return round(min(1.0, notional_usd / 3_000_000), 3)
        
        X_new = pd.DataFrame([{"notional": notional_usd, "size": size}])
        
        if model.predict(X_new)[0] == 1:
            return 0.1 # Normal in-line trade
        
        raw_score = model.decision_function(X_new)[0]
        # Translate the negative anomaly score into our 0.0 to 1.0 spectrum
        return round(min(1.0, 0.70 + abs(raw_score)), 3)

    # ── CRYPTO OHLCV CANDLE ML ────────────────────────────────────────────────

    def _train_crypto_candle_model(self, asset: str) -> Optional[IsolationForest]:
        try:
            # Because we might only store close_price and volume in CryptoData, 
            # we can approximate volatility by comparing consecutive minute closes 
            # using Postgres window functions (LAG).
            sql = """
            SELECT 
                ABS(((crypto_data->>'close_price')::numeric - (crypto_data->>'open_price')::numeric) / NULLIF((crypto_data->>'open_price')::numeric, 0)) as price_change_pct,
                ((crypto_data->>'high_price')::numeric - (crypto_data->>'low_price')::numeric) / NULLIF((crypto_data->>'open_price')::numeric, 0) as volatility_pct,
                ((crypto_data->>'close_price')::numeric * (crypto_data->>'size_tokens')::numeric) as notional
            FROM events 
            WHERE crypto_data->>'pair' = %s 
            AND crypto_data->>'trade_type' = 'OHLCV_1M'
            AND occurred_at > NOW() - INTERVAL '7 days'
            LIMIT 5000
        """
            rows = self._db.query(sql, (asset,))

            # Filter out the first row which will have a NULL price_change_pct due to LAG
            valid_rows = [r for r in rows if r['price_change_pct'] is not None and r['volatility_pct'] is not None]
            
            if len(valid_rows) < 50: return
            
            df = pd.DataFrame(valid_rows).astype(float).fillna(0)
            model = IsolationForest(n_estimators=100, contamination=0.015, random_state=42)
            model.fit(df[['price_change_pct', 'volatility_pct', 'notional']])
            
            self._crypto_candle_models[asset] = (model, time.time())
        except Exception as e:
            logger.error(f"Failed to train Crypto Candle model for {asset}: {e}")
            return None

    def score_crypto_candle(self, asset: str, features: List[float]) -> float:
        """
        features = [price_change_pct, volatility_pct, notional_volume]
        """
        if not asset or len(features) != 3: return 0.0
        
        cache_entry = self._crypto_candle_models.get(asset)
        if cache_entry:
            model, trained_at = cache_entry
            if time.time() - trained_at > self.MODEL_TTL_SECONDS:
                self._trigger_background_training("crypto", asset, "crypto_candle")
        else:
            self._trigger_background_training("crypto", asset, "crypto_candle")
            return round(min(1.0, (features[0] * 20) + (features[1] * 5)), 3)
        # features[0] is price_change_pct, features[2] is notional

        X_new = pd.DataFrame([{
            "price_change_pct": features[0], 
            "volatility_pct": features[1],
            "notional": features[2],
        }])
        
        if model.predict(X_new)[0] == 1:
            return 0.1 
        
        raw_score = model.decision_function(X_new)[0]
        return round(min(1.0, 0.75 + abs(raw_score)), 3)
    

    # ── Vessel Position ───────────────────────────────────────────────────────

    def score_vessel_position(
        self,
        mmsi: str,               # The Vessel ID (Maritime Mobile Service Identity)
        speed: Optional[float],  # Speed in Knots
        region: Optional[str],   # Where is it? (e.g., "Strait of Hormuz")
        flags: List[str],        # Tags we already know (e.g., "sanctioned")
        nav_status: str,         # What it SAYS it's doing (e.g., "Fishing")
        vessel_type: str,        # What kind of ship (e.g., "Tanker")
    ) -> float:
        """
        Calculates risk for a standard position report.
        Logic:
          Risk = (Base Region Risk + Speed Anomaly + Flags) * Region Multiplier
        """
        score = 0.0
        
        # 1. GEOGRAPHY CHECK
        # Get the "Danger Multiplier" for this region.
        # Open Ocean = 1.0 (Neutral)
        # Strait of Hormuz = 3.0 (High Tension)
        mult = get_region_sensitivity_multiplier(region)

        # If the vessel is inside a watched region (like a chokepoint),
        # we add a small base risk just for being there.
        # Logic: Activity in the Strait of Hormuz is inherently more interesting than in the middle of the Atlantic.
        if region:
            score += 0.1 * (mult - 1.0)
        
        # 2. SPEED CHECK (Behavioral Anomaly)
        # Is the ship moving weirdly fast?
        # Example: A Tanker usually moves at 12 knots. If this one is doing 20 knots, that's suspicious (evading?).
        if speed and speed > 0:
            # Fetch the historical average speed for this specific type of ship from Redis.
            baseline = self._get_speed_baseline(vessel_type)
            
            if baseline and baseline > 0:
                ratio = speed / baseline
                # Threshold: If speed is > 150% of normal (1.5x)
                if ratio > 1.5:
                    # Add up to 0.2 points to the score, scaled by how fast it is.
                    score += min(0.2, (ratio - 1.5) * 0.2)
            
        # 3. IDENTITY CHECK (Bad Actors)
        # Add points for every "Red Flag" we know about this vessel.
        # e.g., +0.40 for Sanctions + 0.15 for Dark Pattern = +0.55
        for flag in flags:
            score += FLAG_WEIGHTS.get(flag, 0.05)

        # 4. FINALIZE
        # Multiply by the regional sensitivity (if it's a bad actor in a dangerous place, score skyrockets).
        # Clamp the result so it never exceeds 1.0 (100%).
        # Round to 3 decimal places for neatness.
        return round(min(1.0, score * mult), 3)

    # ── Vessel Dark ───────────────────────────────────────────────────────────
    
    def score_vessel_dark(
            self, 
            mmsi: str,
            gap_hours: float,            # How long was it missing?
            region: Optional[str],       # Where was it last seen?
            flags: List[str],            # Known bad flags
            last_heading: Optional[int], # Direction (0-360 degrees) before vanishing
    ) -> float:
        """
        Scores a "Gap Event" — when a vessel disappears from tracking.
        Disappearing in the open ocean is normal (poor satellite coverage).
        Disappearing in the Persian Gulf while heading toward Iran is suspicious.
        """
        # Start with a base suspicion of 0.3 (30%) just for going dark.
        score = 0.3
        
        # Amplify by region.
        # If in a high-risk zone, the multiplier might be 2.0x or 3.0x.
        mult = get_region_sensitivity_multiplier(region)
        score *= mult

        # INTENT CHECK: Where were they going?
        # Heading 200-320 degrees in the Strait of Hormuz points towards Iranian waters.
        # If they turned off the transponder while pointing that way, it's very suspicious.
        if last_heading and 200 <= last_heading <= 320:
            if region == "Strait of Hormuz":
                score += 0.15
 
        # IDENTITY CHECK:
        # If a sanctioned vessel goes dark, we assume they are up to no good.
        for flag in flags:
            if "sanctioned" in flag or "iran" in flag:
                score += 0.20
 
        return round(min(1.0, score), 3)
 
    # ── News ──────────────────────────────────────────────────────────────────
    def score_news(
        self,
        named_entities: List[str],  # People/Places/Orgs mentioned in the article
        sentiment: float,           # -1.0 (Negative) to +1.0 (Positive)
        reliability: float,         # Source credibility (Reuters=0.95, Blog=0.50)
    ) -> float:
        """
        Scores a news headline.
        We look for "Scary Words" combined with "Negative Sentiment".
        """
        score = 0.0
        
        # 1. KEYWORD MATCHING
        # Check if any named entities (like "Iran", "Missile") are in our HIGH_RISK list.
        entities_lower = "".join(named_entities).lower()
        matches = sum(1 for w in HIGH_RISK_WORDS if w in entities_lower)
        
        # Add 0.10 for each scary word, capped at 0.50 (50%).
        score += min(0.5, matches * 0.10)

        # 2. SENTIMENT CHECK
        # If the news is very negative (War, Crash, Crisis), increase the score.
        # We only care if sentiment is worse than -0.3.
        if sentiment < -0.3:
            score += abs(sentiment) * 0.2
        
        # 3. SOURCE RELIABILITY
        # If a random blog says "War is coming", we discount the score.
        # If Reuters says it, we take the full score.
        return round(min(1.0, score * reliability), 3)

    # ── Redis Baseline Lookup ─────────────────────────────────────────────────

    def _get_speed_baseline(self, vessel_type: str) -> Optional[float]:
        # Fetch from Redis cache.
        # Key format: "baseline:speed:Tanker" -> "12.5"
        val = self._redis.get(f"baseline:speed:{vessel_type}")
        return float(val) if val else None
    
    def update_vessel_baseline(self, mmsi: str, vessel_type: str, speed: float):
        """
        Updates the 'Normal' speed for a vessel type using an Exponential Moving Average (EMA).
        Why EMA? It smooths out noise. One fast ship doesn't change the average instantly,
        but persistent changes will shift the baseline over time.
        """
        if not vessel_type or not speed or speed <= 0:
            return
            
        key     = f"baseline:speed:{vessel_type}"
        current = self._get_speed_baseline(vessel_type)
        
        # Formula: New Average = (95% of Old Average) + (5% of New Value)
        # This makes the baseline very stable, requiring many new data points to shift it.
        updated = (0.95 * current + 0.05 * speed) if current else speed
        
        # Save back to Redis. TTL (Time To Live) = 7 days.
        # If we stop seeing this vessel type, the baseline expires in a week.
        self._redis.set(key, str(round(updated, 3)), ttl=604800)

    # ── Prediction Markets ────────────────────────────────────────────────────

    def score_prediction_trade(self, asset_id: str, notional_usd: float) -> float:
        """
        Evaluates a single prediction market trade (Polymarket).
        Uses an EMA (Exponential Moving Average) stored in Redis to determine baseline.
        """
        if not asset_id or notional_usd < 1000:
            return 0.1

        key = f"baseline:pred:trade:{asset_id}"
        try:
            raw_val = self._redis.get(key)
            current_ema = float(raw_val) if raw_val else None

            if not current_ema:
                # First time seeing this asset, set baseline and return low score
                self._redis.set(key, str(notional_usd), ttl=604800) # 7 days
                return 0.1

            # Check if it's a whale BEFORE updating the EMA
            multiplier = notional_usd / current_ema
            anomaly_score = 0.1

            if multiplier > 5.0: # 5x larger than average
                # Base score of 0.6, scaling up to 1.0 based on size and multiplier
                anomaly_score = min(1.0, 0.6 + (notional_usd / 50_000) * 0.2 + (multiplier / 20) * 0.2)
            
            # Update EMA (slowly, so huge spikes don't ruin the baseline)
            # If it's a massive anomaly, weight it less to preserve the normal baseline
            alpha = 0.01 if multiplier > 5.0 else 0.1
            new_ema = (notional_usd * alpha) + (current_ema * (1 - alpha))
            self._redis.set(key, str(round(new_ema, 2)), ttl=604800)

            return round(anomaly_score, 3)
        
        except Exception as e:
            logger.error(f"Prediction scorer error for {asset_id}: {e}")
            return 0.1

    def score_prediction_spike(self, ticker: str, notional_delta_usd: float) -> float:
        """
        Evaluates a minute-to-minute volume spike (Kalshi).
        """
        if not ticker or notional_delta_usd < 1000:
            return 0.1

        key = f"baseline:pred:vol:{ticker}"
        try:
            raw_val = self._redis.get(key)
            current_ema = float(raw_val) if raw_val else None

            if not current_ema:
                self._redis.set(key, str(notional_delta_usd), ttl=604800)
                return 0.1

            multiplier = notional_delta_usd / current_ema
            anomaly_score = 0.1

            if multiplier > 3.0: # 3x larger than average minute volume
                anomaly_score = min(1.0, 0.6 + (notional_delta_usd / 20_000) * 0.3)

            alpha = 0.05 if multiplier > 3.0 else 0.2
            new_ema = (notional_delta_usd * alpha) + (current_ema * (1 - alpha))
            self._redis.set(key, str(round(new_ema, 2)), ttl=604800)

            return round(anomaly_score, 3)
        
        except Exception as e:
            logger.error(f"Kalshi scorer error for {ticker}: {e}")
            return 0.1
