"""
services/collector-prediction/main.py

ENTERPRISE PREDICTION MARKET COLLECTOR
======================================
Ingests Polymarket and Kalshi.
Stateless Mode: Pipes raw volume and trades directly to Kafka.
Anomaly scoring (Whales/EMA) is handled downstream by the Enrichment service.
"""

import asyncio
import aiohttp
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import websockets
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

from shared.kafka import SentinelProducer, Topics
from shared.models import RawEvent
from shared.db import get_redis
from shared.utils.heartbeat import start_heartbeat_task
from shared.utils.collector_metrics import CollectorMetrics

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s — %(message)s"
)
logger = logging.getLogger("collector.prediction")

KALSHI_BASE_URL = "https://api.elections.kalshi.com/trade-api/v2"

# Outcome names that mean "this market is a straight yes/no bet". Anything else
# is a named outcome in a field of several, where forcing a yes/no reading
# invents a probability for a question nobody asked.
_BINARY_YES = frozenset({"yes", "y", "true"})
_BINARY_NO = frozenset({"no", "n", "false"})


def _json_list(value) -> list:
    """Gamma returns these as JSON-encoded strings, not arrays."""
    if isinstance(value, list):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except (json.JSONDecodeError, TypeError, ValueError):
            return []
        return parsed if isinstance(parsed, list) else []
    return []


def _price_map(names: list, prices: list) -> dict:
    """Outcome name -> probability, for the legs the API priced.

    Returned empty rather than partially guessed when the two arrays disagree in
    length: a misaligned name/price pairing is worse than no distribution, since
    it reads as authoritative.
    """
    if not names or len(names) != len(prices):
        return {}
    out = {}
    for name, price in zip(names, prices):
        try:
            out[str(name)] = round(float(price), 4)
        except (TypeError, ValueError):
            continue
    return out


def _pair_markets_with_events(data: list, queried_slug: str) -> list:
    """Each market paired with the event it belongs to.

    The /markets endpoint answers with a flat list even when every row is one
    leg of the same question -- 20 rows came back for the 2028 nomination, one
    per candidate, with no event wrapper. Grouping is therefore done on each
    market's own nested `events[0]`, which carries the real title
    ("Democratic Presidential Nominee 2028") rather than being inferred from the
    shape of the response.
    """
    pairs = []
    flat = []
    for item in data:
        if not isinstance(item, dict):
            continue
        if "markets" in item and isinstance(item["markets"], list):
            for market in item["markets"]:
                if isinstance(market, dict):
                    pairs.append((market, item))
        else:
            flat.append(item)

    groups = {}
    for market in flat:
        events = market.get("events")
        event = events[0] if isinstance(events, list) and events and isinstance(events[0], dict) else {}
        key = event.get("slug") or queried_slug
        if key not in groups:
            groups[key] = {
                "title": event.get("title") or "",
                "slug": key,
                "markets": [],
            }
        groups[key]["markets"].append(market)

    for group in groups.values():
        for market in group["markets"]:
            pairs.append((market, group))
    return pairs


def _choice_context(market: dict, parent_event) -> dict:
    """The multi-choice field this market is one leg of, if it is one.

    Polymarket prices "who wins" as one yes/no market per candidate. Each leg is
    a real binary bet and stays one; what was missing is that the legs belong to
    a single question. Without the parent, three markets in one nomination race
    look like three unrelated coin flips -- which is exactly how they were being
    stored.
    """
    siblings = (parent_event or {}).get("markets") or []
    choice_name = market.get("groupItemTitle") or None

    # One market under an event is not a field of choices.
    if len(siblings) < 2:
        return {
            "choice_name": choice_name,
            "choice_space": [],
            "choice_prices": {},
            "event_title": (parent_event or {}).get("title") or "",
            "event_slug": (parent_event or {}).get("slug") or "",
            "is_multi_choice": False,
        }

    space, prices = [], {}
    for sib in siblings:
        title = sib.get("groupItemTitle")
        if not title:
            continue
        space.append(str(title))
        # The leg's own p(yes) is that choice's probability within the field.
        leg = _json_list(sib.get("outcomePrices"))
        names = _json_list(sib.get("outcomes"))
        pair = _price_map(names, leg)
        for key, value in pair.items():
            if key.strip().lower() in _BINARY_YES:
                prices[str(title)] = value
                break

    return {
        "choice_name": choice_name,
        "choice_space": space,
        "choice_prices": prices,
        "event_title": (parent_event or {}).get("title") or "",
        "event_slug": (parent_event or {}).get("slug") or "",
        "is_multi_choice": len(space) >= 2,
    }


def _is_binary_market(outcome_names: list) -> bool:
    """True only for a genuine two-sided yes/no market."""
    if len(outcome_names) != 2:
        return False
    lowered = {str(n).strip().lower() for n in outcome_names}
    return bool(lowered & _BINARY_YES) and bool(lowered & _BINARY_NO)

# ── POLYMARKET STREAM ─────────────────────────────────────────────────────────        

async def stream_polymarket(producer: SentinelProducer, redis_client):
    url = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    redis_key = "sentinel:polymarket:watched_slugs"
    id_to_label = {}
    # Structured view of the same tokens. The label is a display string and
    # cannot answer "how many outcomes does this market have", which is the
    # question that decides whether yes/no is even meaningful.
    id_to_meta = {}

    async def update_subscriptions(ws, session):
        base_url = "https://gamma-api.polymarket.com/markets"
        markets_url = "https://gamma-api.polymarket.com/markets?active=true&closed=false&order=volume&ascending=false&limit=100"
        
        while True:
            try:
                # 1. Fetch manual slugs from Redis
                raw_slugs = await redis_client.raw.smembers(redis_key)
                watched_slugs = [s.decode() if isinstance(s, bytes) else s for s in raw_slugs] if raw_slugs else []
                
                # 2. Fetch dynamic active slugs from Polymarket
                try:
                    async with session.get(markets_url, timeout=10) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            # Extract unique slugs from the active markets list
                            dynamic_slugs = list(set(m.get("slug") for m in data if m.get("slug")))
                            
                            # Add dynamic slugs to Redis for visibility to other services
                            if dynamic_slugs:
                                await redis_client.raw.sadd(redis_key, *dynamic_slugs)
                                watched_slugs.extend([s for s in dynamic_slugs if s not in watched_slugs])
                        else:
                            body = await resp.text()
                            logger.error(f"Polymarket dynamic market lookup returned status {resp.status}: {body[:200]}")
                except Exception as e:
                    logger.error(f"Failed to fetch dynamic slugs from Polymarket: {e!r}")
                
                # Fallback if both Redis and API are empty
                if not watched_slugs:
                    watched_slugs = ["us-x-iran-permanent-peace-deal-by"]
                
                logger.info(f"Heartbeat | Polymarket sync. Tracked slugs ({len(watched_slugs)}): {watched_slugs}")
                new_assets = []

                for slug in watched_slugs:
                    try:
                        url = f"{base_url}?event_slug={slug}"
                        async with session.get(url, timeout=10) as resp:
                            if resp.status == 200:
                                data = await resp.json()
                                
                                # Gamma returns Events, which contain Markets. The
                                # parent is kept rather than flattened away: a
                                # multi-choice question ("who wins the nomination")
                                # is modelled as one binary market per candidate,
                                # so the *field* only exists at the event level.
                                # Verified live: 23 of 25 events carry 3+ sibling
                                # markets and every one has a groupItemTitle.
                                if not isinstance(data, list):
                                    continue
                                markets = _pair_markets_with_events(data, slug)

                                for market, parent_event in markets:
                                    if not isinstance(market, dict) or market.get("closed"): 
                                        continue

                                    question = market.get("question", "")
                                    tokens = market.get("clobTokenIds", [])
                                    if isinstance(tokens, str):
                                        try:
                                            tokens = json.loads(tokens)
                                        except (json.JSONDecodeError, TypeError, Exception):
                                            continue
                                    
                                    # Gamma calls these "outcomes" and "outcomePrices".
                                    # Verified against the live API: "outcomeNames" is
                                    # absent from every market, so reading it produced an
                                    # empty list, every token fell through to the
                                    # "Outcome {i}" placeholder, and the real names were
                                    # never seen downstream.
                                    outcomes = _json_list(market.get("outcomes"))
                                    if not outcomes:
                                        outcomes = _json_list(market.get("outcomeNames"))
                                    prices = _json_list(market.get("outcomePrices"))

                                    for i, token_id in enumerate(tokens):
                                        outcome_name = str(outcomes[i]) if i < len(outcomes) else f"Outcome {i}"
                                        is_new = token_id not in id_to_label
                                        id_to_label[token_id] = f"{slug} | {question} | {outcome_name}"
                                        # Rewritten on every poll, not only on first
                                        # sight.
                                        #
                                        # This whole block sat behind the
                                        # "unseen token" guard, so the field's
                                        # odds were captured once when the
                                        # collector first subscribed and never
                                        # again. Enrichment republishes them to
                                        # sentinel:prediction:outcomes:* with a
                                        # seven-day TTL and the agents' categorical
                                        # resolver ranks outcomes from that key to
                                        # decide which one won -- so a race whose
                                        # leader changed after subscribe time was
                                        # graded against the odds as they stood on
                                        # the day the process started.
                                        #
                                        # Only the subscription itself is
                                        # once-per-token; the prices are not.
                                        id_to_meta[token_id] = {
                                            "slug": slug,
                                            "question": question,
                                            **_choice_context(market, parent_event),
                                            "outcome_name": outcome_name,
                                            "outcome_index": i,
                                            "outcome_names": [str(o) for o in outcomes],
                                            # The whole field as of this poll. Trades
                                            # carry one leg's price; this keeps the
                                            # other legs visible and current.
                                            "outcome_prices": _price_map(outcomes, prices),
                                        }
                                        if is_new:
                                            new_assets.append(token_id)
                            else:
                                logger.error(f"Gamma API error for {slug}: HTTP {resp.status}")

                    except Exception as e:
                        logger.error(f"Gamma API connection error for {slug}: {e}")
                
                if new_assets:
                    await ws.send(json.dumps({"assets_ids": new_assets, "type": "market"}))
                    logger.info(f"Polymarket: Subscribed to {len(new_assets)} new outcome tokens.")
                
            except Exception as e:
                logger.error(f"Polymarket sync error: {e}")

            await asyncio.sleep(300)

    while True:
        try:
            async with websockets.connect(url, ping_interval=20) as ws:
                logger.info("Connected to Polymarket CLOB")

                async with aiohttp.ClientSession() as session:
                    injector_task = asyncio.create_task(update_subscriptions(ws, session))

                    try:
                        while True:
                            message = await ws.recv()
                            data = json.loads(message)
                            events = data if isinstance(data, list) else [data]

                            for event in events:
                                if event.get("event_type") in ("trade", "last_trade_price"):
                                    asset_id = event.get("asset_id")
                                    price = float(event.get("price", 0.0))
                                    size = float(event.get("size", 0.0))
                                    notional_usd = price * size

                                    label = id_to_label.get(asset_id, "UNKNOWN")
                                    meta = id_to_meta.get(asset_id, {})

                                    outcome_name = meta.get("outcome_name") or (
                                        label.split(" | ")[-1] if " | " in label else ""
                                    )
                                    outcome_names = meta.get("outcome_names") or []
                                    clean_outcome = outcome_name.strip().lower()

                                    # A CLOB token price is already the probability of
                                    # that one outcome, so the traded leg is known
                                    # exactly whatever the market's shape.
                                    leg_prob = round(price, 4) if price <= 1.0 else round(price / 100.0, 4)
                                    outcome_prices = dict(meta.get("outcome_prices") or {})
                                    if outcome_name:
                                        outcome_prices[outcome_name] = leg_prob

                                    if _is_binary_market(outcome_names) or not outcome_names:
                                        # Two-sided market: the complement is the other
                                        # side, and yes/no is a true description of it.
                                        if clean_outcome in _BINARY_NO:
                                            no_prob = leg_prob
                                            yes_prob = round(max(0.0, 1.0 - leg_prob), 4)
                                        else:
                                            yes_prob = leg_prob
                                            no_prob = round(max(0.0, 1.0 - leg_prob), 4)
                                    else:
                                        # Several named outcomes. 1 - price is the
                                        # probability of "any other candidate", not of
                                        # "no", so publishing it as no_probability
                                        # asserts something the market never priced.
                                        yes_prob = None
                                        no_prob = None

                                    if yes_prob is None:
                                        logger.info(f"Polymarket Trade Found | {size} shares @ ${price} | {label} | "
                                                    f"P({outcome_name}): {leg_prob} of {len(outcome_names)} outcomes")
                                    else:
                                        logger.info(f"Polymarket Trade Found | {size} shares @ ${price} | {label} | YesProb: {yes_prob} | NoProb: {no_prob}")

                                    # STATELESS: Pipe directly to Kafka. Let Enrichment score anomalies.
                                    raw_event = RawEvent(
                                        source="polymarket",
                                        occurred_at=datetime.now(timezone.utc),
                                        raw_payload={
                                            "asset_label": label,
                                            "side": event.get("side", ""),
                                            "price": price,
                                            "size_shares": size,
                                            "notional_usd": notional_usd,
                                            "yes_probability": yes_prob,
                                            "no_probability": no_prob,
                                            "outcome_name": outcome_name,
                                            "outcome_prices": outcome_prices or None,
                                            "outcome_names": outcome_names or None,
                                            "outcome_index": meta.get("outcome_index"),
                                            "is_binary": _is_binary_market(outcome_names) if outcome_names else None,
                                            "market_question": meta.get("question") or "",
                                            "market_slug": meta.get("slug") or "",
                                            # The field this leg belongs to, when it
                                            # belongs to one.
                                            "choice_name": meta.get("choice_name"),
                                            "choice_space": meta.get("choice_space") or None,
                                            "choice_prices": meta.get("choice_prices") or None,
                                            "event_title": meta.get("event_title") or "",
                                            "event_slug": meta.get("event_slug") or "",
                                            "is_multi_choice": bool(meta.get("is_multi_choice"))
                                        }
                                    )
                                    await producer.send(Topics.RAW_PREDICTION, raw_event.model_dump(), key="polymarket")
                    finally:
                        injector_task.cancel()

        except Exception as e:
            logger.error(f"Polymarket WS Error: {e}. Reconnecting...")
            await asyncio.sleep(5)

# ── KALSHI POLLER ─────────────────────────────────────────────────────────────

async def poll_kalshi(producer: SentinelProducer):
    """
    Polls Kalshi REST API for active event volumes.
    STATELESS: Sends raw volume to Kafka for Enrichment to calculate Deltas/Spikes.
    """
    session_timeout = aiohttp.ClientTimeout(total=10)
    
    async with aiohttp.ClientSession(timeout=session_timeout) as session:
        while True:
            try:   
                # Query up to 1000 markets to ensure we catch most active elections and categories
                url = f"{KALSHI_BASE_URL}/markets?limit=1000"
                async with session.get(url) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        markets = data.get("markets", [])
                        
                        # 1. Filter only open, non-expired markets with trading activity
                        open_markets = []
                        for market in markets:
                            if market.get("status") != "open":
                                continue
                                
                            exp_ts = market.get("expiration_ts")
                            if exp_ts:
                                try:
                                    exp_dt = datetime.fromisoformat(exp_ts.replace('Z', '+00:00'))
                                    if exp_dt < datetime.now(timezone.utc):
                                        continue
                                except Exception:
                                    pass
                            
                            if market.get("volume", 0) <= 0:
                                continue
                            
                            open_markets.append(market)
                            
                        # 2. Sort open markets by volume descending to dynamically keep top active bets
                        open_markets.sort(key=lambda m: m.get("volume", 0), reverse=True)
                        
                        # 3. Publish the top 100 most active markets
                        for market in open_markets[:100]:
                            ticker = market.get("ticker")
                            vol = market.get("volume", 0)
                            
                            # Support both legacy cent-based integers and modern dollar-based fields
                            yes_bid = market.get("yes_bid")
                            no_bid = market.get("no_bid")
                            yes_bid_dollars = market.get("yes_bid_dollars")
                            no_bid_dollars = market.get("no_bid_dollars")
                            
                            # Standardize probability values (0.0 to 1.0)
                            if yes_bid_dollars is not None and float(yes_bid_dollars) > 0:
                                yes_prob = round(float(yes_bid_dollars), 4)
                            elif yes_bid is not None and yes_bid > 0:
                                yes_prob = round(yes_bid / 100.0, 4)
                            elif market.get("last_price_dollars") is not None:
                                yes_prob = round(float(market.get("last_price_dollars")), 4)
                            elif market.get("last_price") is not None:
                                yes_prob = round(market.get("last_price") / 100.0, 4)
                            else:
                                yes_prob = 0.50
                                
                            if no_bid_dollars is not None and float(no_bid_dollars) > 0:
                                no_prob = round(float(no_bid_dollars), 4)
                            elif no_bid is not None and no_bid > 0:
                                no_prob = round(no_bid / 100.0, 4)
                            else:
                                no_prob = round(max(0.0, 1.0 - yes_prob), 4)
                                
                            price_usd = yes_prob if yes_prob is not None else 0.50
                            
                            event = RawEvent(
                                source="kalshi",
                                occurred_at=datetime.now(timezone.utc),
                                raw_payload={
                                    "ticker": ticker,
                                    "title": market.get("title"),
                                    "total_volume": vol,
                                    "price": price_usd,
                                    "yes_bid": yes_bid_dollars if yes_bid_dollars is not None else yes_bid,
                                    "no_bid": no_bid_dollars if no_bid_dollars is not None else no_bid,
                                    "yes_probability": yes_prob,
                                    "no_probability": no_prob
                                }
                            )
                            await producer.send(Topics.RAW_PREDICTION, event.model_dump(), key=ticker)
                            
                    else:
                        text = await resp.text()
                        logger.error(f"Kalshi API Rejected Connection: HTTP {resp.status} - {text}")    
            except Exception as e:
                logger.error(f"Kalshi polling error: {e}", exc_info=True)
            
            await asyncio.sleep(60) 

# ── ORCHESTRATION ─────────────────────────────────────────────────────────────

async def main():
    logger.info("=" * 60)
    logger.info("SENTINEL PREDICTION MARKET COLLECTOR (STATELESS)")
    logger.info("=" * 60)

    producer = SentinelProducer(service_name="collector-prediction")
    await producer.start()
    redis_client = await get_redis()
    
    # §1.1 Universal heartbeat
    # Throughput counters. The heartbeat proves this process is alive;
    # these prove it is still producing.
    metrics = CollectorMetrics("collector-prediction")
    await metrics.start(redis_client)
    hb_task = asyncio.create_task(start_heartbeat_task(redis_client, "collector-prediction"))

    try:
        await asyncio.gather(
            stream_polymarket(producer, redis_client),
            poll_kalshi(producer)
        )

    except KeyboardInterrupt:
        logger.info("Shutting down prediction collector...")
    finally:
        hb_task.cancel()
        await producer.close()

if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())