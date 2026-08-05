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
