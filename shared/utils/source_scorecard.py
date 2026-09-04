import re
"""
shared/utils/source_scorecard.py

Per-news-source track record, mirroring SentinelAgent.update_scorecard()
in services/agents/base.py exactly (same Brier-score math, same weight
formula) but keyed by news source name instead of agent name.
"""

from datetime import datetime, timezone
from typing import Any
from pydantic import BaseModel


class SourceScorecard(BaseModel):
    source_name: str
    stories_originated: int = 0
    stories_confirmed: int = 0  # linked scenario later CONFIRMED
    stories_denied: int = 0  # linked scenario later DENIED
    brier_score: float = 0.5
    reliability_weight: float = 0.5
    last_updated: str = ""


async def get_source_scorecard(redis_client: Any, source_name: str) -> SourceScorecard:
    key = f"sentinel:news:source_scorecard:{source_name}"
    client = getattr(redis_client, "raw", redis_client)
    data = await client.get(key)
    if data:
        if isinstance(data, bytes):
            data = data.decode("utf-8")
        return SourceScorecard.model_validate_json(data)
    return SourceScorecard(source_name=source_name)


async def update_source_scorecard(
    redis_client: Any,
    source_name: str,
    story_confirmed: bool,
    story_conviction: float,
) -> SourceScorecard:
    card = await get_source_scorecard(redis_client, source_name)
    card.stories_originated += 1
    if story_confirmed:
        card.stories_confirmed += 1
    else:
        card.stories_denied += 1

    # Normalize story_conviction if passed as percentage > 1.0 (e.g. 75 -> 0.75)
    conviction = float(story_conviction)
    if conviction > 1.0:
        conviction /= 100.0
    conviction = max(0.0, min(1.0, conviction))

    outcome = 1.0 if story_confirmed else 0.0
    total = card.stories_originated

    # Brier score update: BS = (1/N) Σ (forecast - outcome)²
    card.brier_score = (card.brier_score * (total - 1) + (conviction - outcome) ** 2) / total
    card.reliability_weight = max(0.1, 1.0 - card.brier_score)
    card.last_updated = datetime.now(timezone.utc).isoformat()

    key = f"sentinel:news:source_scorecard:{source_name}"
    client = getattr(redis_client, "raw", redis_client)
    await client.set(key, card.model_dump_json(), ex=2592000)  # 30 day TTL (sources move slower than agents)
    return card


# ── STRUCTURAL SOURCE RELIABILITY ────────────────────────────────────────────
#
# What a feed is worth before any of its claims have been scored.
#
# The scorecard above learns a source's reliability from whether its stories are
# later confirmed, and it is the right mechanism -- but it only ever ran on the
# news path, and every other enricher left source_reliability at the model
# default of 1.0. Measured across 1,204 live events: 1,201 carried exactly 1.0.
# A volunteer-fed terrestrial ADS-B aggregator, a volunteer-fed AIS aggregator,
# a raw chain RPC and a licensed equities vendor were rated identically and
# maximally, so the field ranked nothing and the one mechanism the platform has
# for discounting a weak source returned a constant.
#
# These are structural properties of the feeds, not judgements about individual
# messages, which is why they are written down rather than learned:
#
#   - A terrestrial ADS-B or AIS aggregator has coverage holes by construction.
#     Its *positions* are trustworthy; its *absences* are receiver geography,
#     and it cannot tell you which is which.
#   - A chain RPC reports what is in a block. That is authoritative about the
#     chain and says nothing about who controls an address.
#   - An exchange or licensed market-data vendor is reporting its own book.
#   - A regulatory filing is the registrant's own signed statement.
#
# Anything not listed keeps the previous behaviour of 1.0, so adding a feed
# never silently discounts it.
SOURCE_RELIABILITY_BASELINE = {
    # Regulatory and exchange-of-record: the source *is* the fact.
    "sec_form4": 0.99,
    "sec_filings": 0.99,
    "sec": 0.99,
    "fred": 0.98,
    "treasury": 0.98,
    # Venues reporting their own book.
    "coinbase_candles": 0.95,
    "coinbase": 0.95,
    "binance": 0.95,
    "okx": 0.95,
    "kraken": 0.95,
    "finnhub_equities": 0.92,
    "finnhub": 0.92,
    "polygon": 0.92,
    "alpaca": 0.92,
    # Authoritative about the chain, silent about attribution.
    "ethereum_rpc": 0.90,
    "eth_rpc": 0.90,
    "etherscan": 0.85,
    # Prediction venues: a real price, a thin book.
    "polymarket": 0.80,
    "kalshi": 0.80,
    # Volunteer-fed aggregators. Good positions, structurally incomplete
    # coverage -- which is precisely why a gap in one is not an event.
    "opensky": 0.70,
    "adsb": 0.70,
    "adsbexchange": 0.70,
    "aisstream": 0.70,
    "ais": 0.70,
    # Open telemetry and community feeds.
    "ripe_ris": 0.75,
    "bgpstream": 0.75,
    "gdelt": 0.65,
    "reddit": 0.45,
    "telegram": 0.40,
    "twitter": 0.40,
    "social": 0.40,
}

# Feeds not in the table keep the previous behaviour.
DEFAULT_SOURCE_RELIABILITY = 1.0


def baseline_reliability(source_name: str) -> float:
    """The structural reliability of a feed, before its record is considered.

    Matched on the leading token as well as the whole name, so "opensky_rest"
    and "binance_ws" resolve without a row each.
    """
    if not source_name:
        return DEFAULT_SOURCE_RELIABILITY
    key = str(source_name).strip().lower()
    if key in SOURCE_RELIABILITY_BASELINE:
        return SOURCE_RELIABILITY_BASELINE[key]
    head = re.split(r"[^a-z0-9]+", key)[0] if key else ""
    return SOURCE_RELIABILITY_BASELINE.get(head, DEFAULT_SOURCE_RELIABILITY)
