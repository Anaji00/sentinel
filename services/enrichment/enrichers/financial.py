"""
services/enrichment/enrichers/financial.py
 
Phase 2 — Financial enricher.
Handles options flow events from collector-financial.
Normalises fields, scores anomaly, tags for correlation engine.
 
Data sources this will handle:
  Unusual Whales API  — options sweeps, dark pool prints
  CFTC COT reports    — commitment of traders (weekly)
  SEC Form 4          — insider transactions
"""

# Import the built-in logging module to record application events and errors.
import logging 
# Import datetime and timezone tools to accurately timestamp our events in UTC.
from datetime import datetime, timezone
# Import Optional to indicate that a function might return a specific type OR None.
from typing import Optional

# Import our custom data structures (Pydantic models) used to standardize data across the system.
from shared.models import (
    NormalizedEvent, EventType, Entity, EntityType, FinancialData,
)

# Initialize the logger for this module. (Fixed typo: logging.getLogger instead of logger.getLogger)
logger = logging.getLogger("enrichment.financial")

# ── CONFIGURATION ─────────────────────────────────────────────────────────────
# Maps specific stock/commodity symbols (tickers) to geopolitical themes.
# We use this to see if financial markets are reacting to physical world events.

GEO_INSTRUMENTS = {
    "USO":"oil",  "BNO":"oil",  "UCO":"oil",
    "XOP":"energy", "OIH":"energy",
    "UNG":"gas",  "BOIL":"gas",
    "LMT":"defense", "RTX":"defense", "NOC":"defense", "GD":"defense", "BA":"defense",
    "GLD":"gold", "IAU":"gold", "GOLD":"gold",
    "ZIM":"shipping", "DAC":"shipping", "SBLK":"shipping", "GOGL":"shipping",
    "CL=F":"crude_futures", "BZ=F":"brent_futures",
    "NG=F":"natgas_futures", "HO=F":"heating_oil_futures",
}

class FinancialEnricher:
    """
    Translates raw financial market data into standardized NormalizedEvents.
    It also acts as a filter, dropping small/irrelevant trades and only passing
    along "Whale" (massive) trades that might indicate insider geopolitical knowledge.
    """
    def __init__(self, scorer):
        # Save the anomaly scorer instance passed into this class so we can use it later.
        self.scorer = scorer

    def enrich(self, raw) -> Optional[NormalizedEvent]:
        """
        Main entry point. Routes the raw event to the correct parser based on
        where the data came from.
        """
        # Extract the raw dictionary payload from the incoming event.
        p = raw.raw_payload
        # Identify the origin of this data (e.g., "unusual_whales" or "sec_form4").
        source = raw.source

        # Check the source string and route to the specific helper method.
        if source == "unusual_whales":
            return self._enrich_options(raw, p)
        elif source == "cftc_cot":
            return self._enrich_cot(raw, p)
        elif source == "sec_form4":
            return self._enrich_insider(raw, p)
            
        return None  # Unknown source, skip
    
    # ── Options Flow ──────────────────────────────────────────────────────────
    def _enrich_options(self, raw, p) -> Optional[NormalizedEvent]:
        """
        Parses Options Trades (bets on future stock prices).
        We look for unusually large bets on defense, oil, or shipping stocks.
        """
        # Safely extract the stock symbol. If missing, default to empty string. Convert to uppercase.
        ticker = (p.get("ticker") or "").upper()
        # Extract the trade cost in dollars. We check "premium_usd" first, then "premium", default to 0, cast to float.
        premium = float(p.get("premium_usd") or p.get("premium") or 0)
        # Extract trade direction (Put or Call). Safely default to empty string, convert to uppercase.
        side = (p.get("put_call") or p.get("side") or "").upper()
        # Extract the number of contracts traded, cast to an integer.
        volume = int(p.get("volume") or 0)
        # Extract the total number of currently active contracts (Open Interest), cast to an integer.
        oi = int(p.get("open_interest") or 0)

        # NOISE FILTER: Ignore trades under $100k. 
        # We only care about "Smart Money" (hedge funds, institutions), not retail traders.
        if not ticker or premium < 100_000:  # Filter out low-premium sweeps
            return None
        
        # Look up the ticker in our dictionary to see if it belongs to a geopolitical category (like 'oil' or 'defense').
        category = GEO_INSTRUMENTS.get(ticker)
        
        # Start a list of string tags for this event. We always add "options_flow".
        tags = ["options_flow"]
        # If the ticker matched a category in our dictionary, add that category as a tag.
        if category:
            tags.append(category)
            
        # High premium = potentially anomalous
        if premium > 1_000_000:
            tags.append("large_sweep")
            
        # Volume > Open Interest means a brand new, massive position was just opened.
        # This is a strong signal that someone is acting on new information.
        if volume > oi and oi > 0:
            tags.append("new_position") # Buying more contracts than currently open could indicate a new position, which is more interesting than closing an existing one.
        
        # SCORING: If it's a watched ticker and the trade is huge (>$500k), raise the temperature.
        # Start with a baseline anomaly score of 0.0.
        anomaly = 0.0
        if category and premium >= 500_000:
            # Calculate score: base 0.3 plus a scaled amount based on premium. min() ensures it never exceeds 1.0.
            anomaly = min(1.0, 0.3 + (premium / 5_000_000) * 0.5)  # Up to 0.8 for very large sweeps in key tickers
            
        # Calculate the ratio of volume to open interest, rounding to 3 decimal places. Handle division by zero.
        vol_oi_ratio = round(volume/ oi, 3) if oi > 0 else None # How much volume relative to open interest

        # Create the primary Entity object representing this financial instrument.
        entity = Entity(id=ticker, type = EntityType.INSTRUMENT, name=ticker)

        # Construct and return the fully normalized event, mapping our extracted variables to the standard schema.
        return NormalizedEvent(
            event_id=raw.event_id,
            type=EventType.OPTIONS_FLOW,
            # Ensure we have a valid UTC datetime for when the event occurred.
            occurred_at=raw.occurred_at or datetime.now(timezone.utc),
            source=raw.source,
            primary_entity=entity,
            # Package financial-specific data into a dedicated JSONB block.
            financial_data=FinancialData(
                ticker=ticker,
                side=side,
                instrument_type = "option",
                trade_type = p.get("trade_type", "SWEEP"), # "sweep", "block", "dark_pool_print", etc.
                strike = p.get("strike"),
                expiry = p.get("expiry"),
                premium_usd=premium,
                volume=volume,
                open_interest=oi,
                vol_oi_ratio=vol_oi_ratio,
                exchange=p.get("exchange"),
                implied_volatility=p.get("implied_volatility"),
                underying_price=p.get("underlying_price"),
                otm_percentage=p.get("otm_percentage"),
            ), 
            tags=tags,
            anomaly_score=round(anomaly, 3),
        )
        
    # ── COT Report (Commitments of Traders) ───────────────────────────────────
    def _enrich_cot(self, raw, p) -> Optional[NormalizedEvent]:
        """
        Parses weekly reports from the CFTC showing what hedge funds are holding.
        Focuses heavily on commodities (Oil, Gas, Wheat).
        """
        # Extract the commodity name (e.g., "CRUDE OIL"). Default to empty string and uppercase it.
        commodity = (p.get("commodity") or "").upper()
        if not commodity:
            return None

        # Create the Entity object. We prepend "COT_" to the ID to ensure it is unique.
        entity = Entity(id= f"COT_{commodity}", type=EntityType.INSTRUMENT, name=commodity)
        # Create initial tags. We replace spaces with underscores to keep tags standardized (e.g., "crude_oil").
        tags = ["futures_cot", commodity.lower().replace(" ", "_")]

        # SIGNAL: "Non-commercial" means speculators (Hedge Funds), not farmers/producers.

        # Extract the net positions of speculators. Cast to int.
        net_position = int(p.get("net_noncommercial") or 0) # Net position of non-commercial traders (speculators). Large positive = bullish, large negative = bearish.
        # Base the anomaly score on the absolute magnitude (abs) of the position. Cap at 1.0.
        anomaly      = min(1.0, abs(net_position) / 500_000 * 0.4) if net_position else 0.0 # Scale anomaly based on size of net position. 500k+ contracts is very large in most commodities.

        # If the commodity matches one of these specific, highly strategic energies...
        if commodity in ("CRUDE OIL", "BRENT CRUDE", "NATURAL GAS"):
            tags.append("energy")
            # Double the anomaly score since we care much more about energy geopolitics, capping again at 1.0.
            anomaly = min(1.0, anomaly * 2.0)  # Energy commodities are more geopolitically sensitive, so we boost the anomaly score.

        # Construct and return the NormalizedEvent.
        return NormalizedEvent(
            event_id=raw.event_id,
            type=EventType.FUTURES_COT,
            occurred_at=raw.occurred_at or datetime.now(timezone.utc),
            source=raw.source,
            primary_entity=entity,
            financial_data=FinancialData(
                ticker=commodity,
                instrument_type="futures",
                volume=p.get("total_open_interest"),
            ),
            tags=tags,
            anomaly_score=round(anomaly, 3),
        )
    
    # ── Insider Transaction ───────────────────────────────────────────────────
    def _enrich_insider(self, raw, p) -> Optional[NormalizedEvent]:
        """
        Parses SEC Form 4 filings (When a CEO/Executive buys/sells their own company stock).
        """
        # Safely extract the stock ticker.
        ticker = (p.get("ticker") or "").upper()
        # Safely extract the person making the trade, defaulting to "Unknown".
        insider = p.get("insider_name", "Unknown")
        # Extract the dollar value of the trade. Check multiple potential keys, convert to float.
        value = float(p.get("transaction_value_usd") or p.get("transaction_value") or 0)
        if not ticker:
            return None
        
        # Create the primary entity referencing the stock.
        entity = Entity(id=ticker, type=EntityType.INSTRUMENT, name=ticker) # We could also create a secondary entity for the insider (e.g., EntityType.INSIDER), but for simplicity we'll just put the insider name in tags.
        # Calculate a basic anomaly score. $10M equals a score of 0.3. Cap at 1.0.
        anomaly = min(1.0, value / 10_000_000 * 0.3) # Scale anomaly based on size of transaction. $10M+ is very large for insider trades.

        # Build and return the standardized event.
        return NormalizedEvent(
            event_id=raw.event_id,
            type=EventType.INSIDER_TRADE,
            occurred_at=raw.occurred_at or datetime.now(timezone.utc),
            source=raw.source,
            primary_entity=entity,
            financial_data=FinancialData(
                ticker=ticker,
                instrument_type="equity",
                side=p.get("transaction_type", "").upper(),
                premium_usd=value,
            ),
            # Build a human-readable headline string. We divide value by 1e6 to represent it as "Millions" (M).
            headline=f"Insider {p.get('transaction_type','trade')}: {insider} — {ticker} ${value/1e6:.1f}M",
            tags=["insider_trade", ticker.lower()],
            anomaly_score=round(anomaly, 3),
        )