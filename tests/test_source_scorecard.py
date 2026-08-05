"""
tests/test_source_scorecard.py

Unit tests for Task 2: Source reliability track-record scoring.
"""

import asyncio
from shared.utils.source_scorecard import (
    SourceScorecard,
    get_source_scorecard,
    update_source_scorecard,
)
from services.enrichment.enrichers.news import NewsEnricher
from shared.models import RawEvent


class DummyRedisStore:
    def __init__(self):
        self.store = {}
        self.raw = self

    async def get(self, key: str):
        return self.store.get(key)

    async def set(self, key: str, value: str, ex: int = None):
        self.store[key] = value


class DummyScorer:
    async def score_news(self, unique_entities, sentiment, reliability, headline, summary):
        return 0.60, ["news_tag"]

    def record_hawkes_event(self, domain):
        pass


async def _async_test_source_scorecard_brier_and_weight_updates():
    redis = DummyRedisStore()

    # Source A: reliable (4/5 confirmed)
    for _ in range(4):
        await update_source_scorecard(redis, "reliable_news", story_confirmed=True, story_conviction=0.8)
    await update_source_scorecard(redis, "reliable_news", story_confirmed=False, story_conviction=0.8)

    # Source B: unreliable (1/5 confirmed, 4/5 denied)
    await update_source_scorecard(redis, "unreliable_news", story_confirmed=True, story_conviction=0.8)
    for _ in range(4):
        await update_source_scorecard(redis, "unreliable_news", story_confirmed=False, story_conviction=0.8)

    card_a = await get_source_scorecard(redis, "reliable_news")
    card_b = await get_source_scorecard(redis, "unreliable_news")

    assert card_a.stories_originated == 5
    assert card_a.stories_confirmed == 4
    assert card_a.stories_denied == 1

    assert card_b.stories_originated == 5
    assert card_b.stories_confirmed == 1
    assert card_b.stories_denied == 4

    # Assert reliable source ends with reliability_weight measurably higher than unreliable source
    assert card_a.reliability_weight > card_b.reliability_weight
    assert card_a.reliability_weight > 0.7
    assert card_b.reliability_weight < 0.6

    # Test NewsEnricher with dynamic scorecard lookup
    enricher = NewsEnricher(scorer=DummyScorer(), redis_client=redis, graph_writer=None)

    raw_event_a = RawEvent(
        source="reliable_news",
        raw_payload={"title": "Reliable Market News Headline", "summary": "Detailed summary here"}
    )
    raw_event_b = RawEvent(
        source="unreliable_news",
        raw_payload={"title": "Unreliable Market News Headline", "summary": "Detailed summary here"}
    )

    enriched_a = await enricher.enrich(raw_event_a)
    enriched_b = await enricher.enrich(raw_event_b)

    assert enriched_a is not None
    assert enriched_b is not None

    # Confirm news.py enrichment output for events from low-reliability source shows lower source_reliability
    assert abs(enriched_a.source_reliability - card_a.reliability_weight) < 1e-4
    assert abs(enriched_b.source_reliability - card_b.reliability_weight) < 1e-4
    assert enriched_a.source_reliability > enriched_b.source_reliability
    assert enriched_b.source_reliability < 0.8  # Lower than static default 0.8


def test_source_scorecard_brier_and_weight_updates():
    asyncio.run(_async_test_source_scorecard_brier_and_weight_updates())
