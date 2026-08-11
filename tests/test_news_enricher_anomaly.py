"""
tests/test_news_enricher_anomaly.py

Unit tests for intelligent news anomaly scoring in NewsEnricher.
Verifies routine background news scores in moderate baseline range (< 0.70)
while threat headlines and OFAC sanction mentions score >= 0.80.
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock
from services.enrichment.enrichers.news import NewsEnricher
from shared.models import RawEvent


class DummyScorer:
    def __init__(self, base_score=0.45):
        self.base_score = base_score

    async def score_news(self, unique_entities, sentiment, reliability, headline, summary):
        return self.base_score, ["semantic:tag"]

    def get_hawkes_intensity(self, domain):
        return 1.0

    def record_hawkes_event(self, domain):
        pass


def test_news_enricher_routine_news_moderate_anomaly():
    async def run_test():
        redis_mock = MagicMock()
        redis_mock.raw = None
        scorer = DummyScorer(base_score=0.45)
        enricher = NewsEnricher(scorer=scorer, redis_client=redis_mock, graph_writer=None)

        raw = RawEvent(
            source="reuters",
            raw_payload={
                "title": "UK firefighters receive upgraded equipment for seasonal training",
                "summary": "Local departments update equipment readiness standard."
            }
        )

        event = await enricher.enrich(raw)
        assert event is not None
        assert event.anomaly_score == 0.45
        assert event.anomaly_score < 0.70

    asyncio.run(run_test())


def test_news_enricher_threat_headline_high_anomaly():
    async def run_test():
        redis_mock = MagicMock()
        redis_mock.raw = None
        scorer = DummyScorer(base_score=0.45)
        enricher = NewsEnricher(scorer=scorer, redis_client=redis_mock, graph_writer=None)

        raw = RawEvent(
            source="reuters",
            raw_payload={
                "title": "Nuclear missile strike threat triggers emergency DEFCON status",
                "summary": "Strategic defense grid detects launch sequence."
            }
        )

        event = await enricher.enrich(raw)
        assert event is not None
        assert event.anomaly_score >= 0.80

    asyncio.run(run_test())


def test_news_enricher_ofac_sanction_mention_high_anomaly():
    async def run_test():
        redis_mock = MagicMock()
        redis_mock.raw = None
        scorer = DummyScorer(base_score=0.45)
        enricher = NewsEnricher(scorer=scorer, redis_client=redis_mock, graph_writer=None)

        raw = RawEvent(
            source="reuters",
            raw_payload={
                "title": "Sberbank SDN list entity asset freeze enacted by US Treasury",
                "summary": "Sanctioned entity accounts blocked."
            }
        )

        event = await enricher.enrich(raw)
        assert event is not None
        assert event.anomaly_score >= 0.85

    asyncio.run(run_test())
