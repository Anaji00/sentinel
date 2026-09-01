"""
services/enrichment/enrichers/prediction.py

ENTERPRISE PREDICTION MARKET ENRICHER & VOLATILITY SCORER
=========================================================
Enriches PolyMarket & Kalshi prediction events.
Dynamically tracks probability shifts (Delta P) and YES/NO bid sizes.
Delegates statistical Z-score and RRCF anomaly scoring to AnomalyScorer.
"""

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Optional

from shared.models import NormalizedEvent, EventType, Entity, EntityType, PredictionMarketData

logger = logging.getLogger("enrichment.prediction")

# Exact vocabulary for a two-sided market. "outcome 0"/"outcome 1" are kept only
# because events collected before the collector read the right field carry those
# placeholders; new events carry the real names.
_YES_TOKENS = frozenset({"yes", "y", "true", "1", "outcome 0"})
_NO_TOKENS = frozenset({"no", "n", "false", "0", "outcome 1"})


def _is_binary_outcome_set(outcome_names: list) -> bool:
    """True only when the market has exactly two sides that mean yes and no."""
    if not outcome_names or len(outcome_names) != 2:
        return False
    lowered = {str(n).strip().lower() for n in outcome_names}
    return bool(lowered & _YES_TOKENS) and bool(lowered & _NO_TOKENS)


class PredictionEnricher:
    def __init__(self, scorer, redis_client, graph_writer):
        self.scorer = scorer
        self.redis = redis_client
        self.graph = graph_writer

    async def enrich(self, raw) -> Optional[NormalizedEvent]:
        p = raw.raw_payload
        source = raw.source

        if source == "polymarket":
            return await self._enrich_polymarket(raw, p)
        elif source == "kalshi":
            return await self._enrich_kalshi(raw, p)
        
        return None

    async def _enrich_polymarket(self, raw, p) -> Optional[NormalizedEvent]:
        label = p.get("asset_label", "UNKNOWN | UNKNOWN | UNKNOWN")
        parts = label.split(" | ")
        raw_slug = parts[0] if len(parts) > 0 else label
        question = parts[1] if len(parts) > 1 and parts[1] != "UNKNOWN QUESTION" else p.get("question", "UNKNOWN QUESTION")
        outcome = parts[2] if len(parts) > 2 else p.get("outcome_name", "UNKNOWN OUTCOME")
        
        if raw_slug.lower() in ("tradfi", "prediction_market", "prediction", "unknown"):
            slug = p.get("ticker") or p.get("market_id") or (question[:30].replace(" ", "-") if question != "UNKNOWN QUESTION" else "PREDICTION-CONTRACT")
        else:
            slug = raw_slug

        notional = float(p.get("notional_usd", 0))
        shares = float(p.get("size_shares", 0))
        price = float(p.get("price", 0))
        total_vol = float(p.get("total_volume", 0) or notional)
        asset_id = p.get("asset_id", slug)

        # Gatekeeper: Ignore zero/noise updates (<$5 total volume or trade size)
        if total_vol <= 0 and notional <= 0 and shares <= 0:
            return None

        yes_prob = p.get("yes_probability")
        no_prob = p.get("no_probability")
        outcome_names = p.get("outcome_names") or []
        outcome_prices = p.get("outcome_prices") or None
        is_binary = p.get("is_binary")
        if is_binary is None:
            is_binary = _is_binary_outcome_set(outcome_names)

        # A leg of a multi-choice field is still a yes/no bet, and its own
        # yes/no stays correct. What changes is which distribution is worth
        # publishing: "Newsom 15 / Kelly 9 / Cuban 4" is the question a reader
        # is actually asking, and the leg's own {Yes, No} is not.
        choice_name = p.get("choice_name") or ""
        choice_prices = p.get("choice_prices") or None
        choice_space = p.get("choice_space") or []
        is_multi_choice = bool(p.get("is_multi_choice")) and len(choice_space) >= 2
        event_slug = p.get("event_slug") or ""
        if is_multi_choice and choice_prices:
            outcome_prices = choice_prices
        
        # Track previous probabilities in Redis to compute dynamic delta_p
        delta_p = 0.0
        try:
            prev_price_key = f"sentinel:prediction:last_price:{slug}"
            prev_price_raw = await self.redis.raw.get(prev_price_key)
            if prev_price_raw:
                delta_p = price - float(prev_price_raw)
            await self.redis.raw.set(prev_price_key, str(price), ex=86400)
            distribution_key = event_slug if (is_multi_choice and event_slug) else slug
            if outcome_prices:
                # The whole field's current odds, so a categorical prediction can
                # later be resolved against what the market settled on. Without
                # this the resolver has only a single leg's price and no way to
                # tell which outcome led.
                await self.redis.raw.set(
                    f"sentinel:prediction:outcomes:{distribution_key}",
                    json.dumps(outcome_prices),
                    ex=7 * 86400,
                )
        except Exception:
            pass

        # Identify YES vs NO side bid/trade value.
        #
        # Matched exactly, not by substring: `"no" in outcome` reads "None of the
        # above", "Another candidate", "Nominee TBD" and "November winner" as NO
        # bets, and those are ordinary outcome names in a multi-candidate field.
        clean_outcome = outcome.strip().lower()
        if not is_binary and outcome_names:
            # A buy of a named outcome is conviction in that outcome, the same
            # relation a YES token has to its question. There is no second side
            # to attribute the complement to, so none is invented.
            yes_val = notional
            no_val = 0.0
        elif clean_outcome in _YES_TOKENS:
            yes_val = notional
            no_val = 0.0
        elif clean_outcome in _NO_TOKENS:
            no_val = notional
            yes_val = 0.0
        else:
            # `is not None` rather than `or`: 0.0 is a probability the market
            # actually quoted, and `or` would discard it as though it were absent.
            y = float(yes_prob) if yes_prob is not None else (price if price else 0.5)
            n = float(no_prob) if no_prob is not None else (1.0 - price if price else 0.5)
            yes_val = notional * y
            no_val = notional * n

        # Dynamic anomaly scoring via central AnomalyScorer (RRCF + Z-scores)
        anomaly_res = await self.scorer.score_prediction_anomaly(
            asset_id=asset_id,
            notional=notional,
            delta_p=delta_p,
            yes_val=yes_val,
            no_val=no_val
        )
        
        anomaly_score = anomaly_res.get("score", 0.0)
        z_vol = anomaly_res.get("z_score_volume", 0.0)
        z_prob = anomaly_res.get("z_score_prob", 0.0)

        # Record Hawkes cross-domain excitation event
        self.scorer.record_hawkes_event("prediction")

        # Classify tags and headlines purely based on dynamic Z-score and RRCF significance
        tags = ["prediction_market", slug.lower()]
        display_contract = question if question != "UNKNOWN QUESTION" else slug.upper()

        # For a leg of a field, name the field and the choice. "Will Newsom win
        # the nomination? YES @ 15%" reads as a coin flip; "2028 Dem nomination
        # — Newsom @ 15% (2nd of 12)" is the same fact as an analyst would say it.
        rank_note = ""
        if is_multi_choice:
            event_title = p.get("event_title") or ""
            if event_title:
                display_contract = event_title
            if choice_name:
                ranked = sorted((choice_prices or {}).items(), key=lambda kv: kv[1], reverse=True)
                position = next(
                    (i + 1 for i, (n, _) in enumerate(ranked) if n == choice_name), None
                )
                if position:
                    rank_note = f" [{choice_name}: {position} of {len(ranked)}]"
                else:
                    rank_note = f" [{choice_name}]"

        if z_vol >= 2.5 or (notional > 0 and anomaly_score >= 0.75):
            tags.extend(["large_bid", "whale_bet", "volume_spike"])
            if not is_binary and outcome_names:
                side_str = outcome.upper()
            else:
                side_str = "YES" if yes_val > no_val else ("NO" if no_val > yes_val else outcome.upper())
            headline = f"🐋 LARGE POLYMARKET BID ({side_str}): {display_contract}{rank_note} (${notional:,.2f} @ {(price*100):.1f}%)"
        elif z_prob >= 2.5 or (abs(delta_p) > 0 and anomaly_score >= 0.65):
            shift_dir = "▲ PROBABILITY SPIKE" if delta_p > 0 else "▼ PROBABILITY DROP"
            tags.extend(["odd_shift", "probability_spike"])
            headline = f"🎯 {shift_dir}: {display_contract}{rank_note} ({choice_name or outcome} shift: {(delta_p*100):+.1f}%)"
        elif anomaly_score >= 0.50:
            tags.append("volume_spike")
            headline = f"🎯 POLYMARKET VOLUME MOVER: {display_contract}{rank_note} (${notional:,.2f} — {choice_name or outcome})"
        else:
            tags.append("odds_update")
            headline = f"🎯 POLYMARKET ODDS: {display_contract}{rank_note} ({choice_name or outcome} @ {(price*100):.1f}%)"

        try:
            await self.redis.raw.sadd("sentinel:polymarket:watched_slugs", slug)
        except Exception:
            pass

        entity_name = display_contract if display_contract != "UNKNOWN QUESTION" else slug
        entity = Entity(id=slug, type=EntityType.PREDICTION_MARKET, name=entity_name)

        return NormalizedEvent(
            event_id=raw.event_id, trace_id=raw.trace_id,
            type=EventType.PREDICTION_MARKET_TRADE,
            occurred_at=raw.occurred_at or datetime.now(timezone.utc),
            source=raw.source,
            primary_entity=entity,
            prediction_market_data=PredictionMarketData(
                market_id=slug,
                ticker=slug.upper(),
                question=question,
                outcome=outcome,
                notional_usd=notional,
                shares_traded=shares,
                price_usd=price,
                total_volume=total_vol,
                yes_probability=yes_prob,
                no_probability=no_prob,
                outcome_prices=outcome_prices,
                category=p.get("category") or "Macro & Geopolitics",
                resolution_date=p.get("resolution_date")
            ),
            headline=headline,
            tags=tags,
            anomaly_score=anomaly_score,
        )

    async def _enrich_kalshi(self, raw, p) -> Optional[NormalizedEvent]:
        ticker = p.get("ticker", "UNKNOWN")
        title = p.get("title", "Unknown Market")
        price = float(p.get("yes_bid") or p.get("no_bid") or p.get("price") or 0.0)
        current_vol = float(p.get("total_volume", 0))

        # Gatekeeper: Ignore dead Kalshi markets with zero volume
        if current_vol <= 0 and float(p.get("yes_bid") or 0) <= 0 and float(p.get("no_bid") or 0) <= 0:
            return None

        yes_bid = float(p.get("yes_bid") or 0.0)
        no_bid = float(p.get("no_bid") or 0.0)
        yes_prob = float(p.get("yes_probability") or 0.5)
        no_prob = float(p.get("no_probability") or 0.5)

        # Stateful volume & probability delta calculation using Redis
        delta_vol = 0.0
        delta_p = 0.0
        try:
            vol_key = f"sentinel:kalshi:vol:{ticker}"
            prob_key = f"sentinel:kalshi:prob:{ticker}"

            last_vol_str = await self.redis.raw.get(vol_key)
            last_prob_str = await self.redis.raw.get(prob_key)

            if last_vol_str:
                delta_vol = max(0.0, current_vol - float(last_vol_str))
            else:
                delta_vol = current_vol

            if last_prob_str:
                delta_p = yes_prob - float(last_prob_str)

            await self.redis.raw.set(vol_key, str(current_vol), ex=86400)
            await self.redis.raw.set(prob_key, str(yes_prob), ex=86400)
        except Exception:
            delta_vol = current_vol

        notional_usd = delta_vol * price if delta_vol > 0 else current_vol * price
        yes_val = yes_bid * delta_vol
        no_val = no_bid * delta_vol

        # Dynamic anomaly scoring via central AnomalyScorer
        anomaly_res = await self.scorer.score_prediction_anomaly(
            asset_id=ticker,
            notional=notional_usd,
            delta_p=delta_p,
            yes_val=yes_val,
            no_val=no_val
        )

        anomaly_score = anomaly_res.get("score", 0.0)
        z_vol = anomaly_res.get("z_score_volume", 0.0)
        z_prob = anomaly_res.get("z_score_prob", 0.0)

        # Record Hawkes cross-domain excitation event
        self.scorer.record_hawkes_event("prediction")

        # Classify tags and headlines dynamically
        tags = ["kalshi_prediction", ticker.lower()]

        if z_vol >= 2.5 or (notional_usd > 0 and anomaly_score >= 0.75):
            tags.extend(["volume_spike", "whale_bet"])
            side = "YES" if yes_bid >= no_bid else "NO"
            headline = f"🚨 KALSHI WHALE BID ({side}): {ticker} (+${notional_usd:,.2f})"
        elif z_prob >= 2.5 or (abs(delta_p) > 0 and anomaly_score >= 0.65):
            shift_dir = "▲ PROBABILITY SPIKE" if delta_p > 0 else "▼ PROBABILITY DROP"
            tags.extend(["odd_shift", "probability_spike"])
            headline = f"🎯 KALSHI {shift_dir}: {ticker} ({title} shift: {(delta_p*100):+.1f}%)"
        elif anomaly_score >= 0.50:
            tags.append("volume_spike")
            headline = f"🎯 KALSHI VOLUME MOVER: {ticker} (+${notional_usd:,.2f})"
        else:
            tags.append("odds_update")
            headline = f"🎯 KALSHI ODDS: {ticker} ({title})"

        entity = Entity(id=ticker, type=EntityType.PREDICTION_MARKET, name=ticker)

        try:
            await self.redis.raw.sadd("sentinel:kalshi:watched_tickers", ticker)
        except Exception:
            pass

        return NormalizedEvent(
            event_id=raw.event_id, trace_id=raw.trace_id,
            type=EventType.PREDICTION_MARKET_TRADE,
            occurred_at=raw.occurred_at or datetime.now(timezone.utc),
            source=raw.source,
            primary_entity=entity,
            prediction_market_data=PredictionMarketData(
                market_id=ticker,
                ticker=ticker,
                question=title,
                outcome="Volume Spike" if (z_vol >= 2.5 or anomaly_score >= 0.70) else "Market Odds",
                shares_traded=delta_vol,
                notional_usd=notional_usd,
                price_usd=price,
                total_volume=current_vol,
                yes_bid=p.get("yes_bid"),
                no_bid=p.get("no_bid"),
                yes_probability=yes_prob,
                no_probability=no_prob,
                category=p.get("category") or "Macro & Fed Rates",
                resolution_date=p.get("resolution_date")
            ),
            headline=headline,
            tags=tags,
            anomaly_score=anomaly_score,
        )