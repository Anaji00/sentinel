"""News reaches the instruments it is about.

Measured on 3 October over three days of headlines: 1,612 headlines, none with
an `exposes:` tag, and the "ticker" on 641 of them was a capitalised word --
"Nvidia Hits Record High" had the primary entity AI, and "G7 to release 100
million barrels of oil" reached no crude instrument at all. Company names now
resolve through the SEC alias registry to tickers the platform quotes, and
commodity phrasing tags the commodity.
"""

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from shared.utils.entity_resolution import ALIAS_KEY
from shared.utils.instrument_reach import quoted_company_tickers
from shared.utils.osint_bridges import text_exposure_tags
from shared.utils.quote_cache import quote_key


class FakeRedis:
    def __init__(self, aliases, quoted):
        self.aliases = aliases
        self.quoted = {quote_key(t) for t in quoted}
        self.raw = self

    async def hmget(self, key, fields):
        assert key == ALIAS_KEY
        return [self.aliases.get(f) for f in fields]

    async def exists(self, *keys):
        return sum(1 for k in keys if k in self.quoted)

    async def hset(self, *args, **kwargs):
        return 1


def _run(coro):
    return asyncio.run(coro)


def test_names_resolve_to_quoted_tickers():
    r = FakeRedis({"NVIDIA": "NVDA", "DELTA AIR LINES": "DAL"}, quoted={"NVDA", "DAL"})
    assert _run(quoted_company_tickers(["Nvidia", "Delta Air Lines"], r)) == ["NVDA", "DAL"]


def test_preferred_line_falls_back_to_its_traded_root():
    r = FakeRedis({"BOEING": "BA-PA"}, quoted={"BA"})
    assert _run(quoted_company_tickers(["Boeing"], r)) == ["BA"]


def test_a_fold_is_not_an_alias():
    """"Japan" folds to JAPAN, which is ticker-shaped and names no company."""
    r = FakeRedis({}, quoted={"JAPAN", "AI"})
    assert _run(quoted_company_tickers(["Japan", "AI"], r)) == []


def test_unquoted_resolutions_are_dropped():
    r = FakeRedis({"SHELL": "RYDAF"}, quoted=set())
    assert _run(quoted_company_tickers(["Shell"], r)) == []


def test_no_redis_means_no_lookups():
    assert _run(quoted_company_tickers(["Nvidia"], None)) == []


def test_commodity_phrasing_is_tagged():
    assert text_exposure_tags("G7 to release 100 million barrels of oil") == ["exposes:BZ=F", "exposes:CL=F"]
    assert text_exposure_tags("G7 agrees to release 100mn barrels") == ["exposes:BZ=F", "exposes:CL=F"]
    assert text_exposure_tags("Ukraine wheat exports resume") == ["exposes:ZC=F", "exposes:ZW=F"]
    assert text_exposure_tags("Chipmakers slide on export controls on chips") == ["exposes:SOXX", "exposes:TSM"]
    assert text_exposure_tags("Gold prices hit a record") == ["exposes:GC=F"]


def test_ordinary_words_are_not_commodities():
    assert text_exposure_tags("Cooking oil shortage hits restaurants") == []
    assert text_exposure_tags("Goldman beats estimates") == []
    assert text_exposure_tags("Painter's oil on canvas sells for $2m") == []
    assert text_exposure_tags(None) == []


def test_news_enricher_tags_company_and_commodity():
    from services.enrichment.enrichers.news import NewsEnricher

    scorer = MagicMock()
    scorer.score_news = AsyncMock(return_value=(0.75, []))
    scorer.get_hawkes_intensity.return_value = 1.0
    enricher = NewsEnricher(scorer=scorer, redis_client=None, graph_writer=None)
    raw = SimpleNamespace(
        source="reuters", event_id="e1", trace_id="t1",
        occurred_at=datetime(2026, 10, 3, tzinfo=timezone.utc),
        raw_payload={"title": "Exxon Mobil halts Guyana oil output after pipeline leak",
                     "summary": "", "url": "https://example.test/a"},
    )
    with patch("services.enrichment.enrichers.news.quoted_company_tickers",
               AsyncMock(return_value=["XOM"])):
        event = _run(enricher.enrich(raw))
    assert event is not None
    assert "exposes:XOM" in event.tags
    assert "exposes:CL=F" in event.tags
    assert event.primary_entity.name == "XOM" or event.primary_entity.id == "XOM"


def test_common_names_resolve_where_the_sec_title_differs():
    """"Amazon" folds to AMAZON, and the registry holds AMAZON COM INC."""
    r = FakeRedis({}, quoted={"AMZN", "META", "MCD"})
    assert _run(quoted_company_tickers(["Amazon", "Meta", "McDonald's"], r)) == ["AMZN", "META", "MCD"]


def test_common_names_still_need_a_quote():
    r = FakeRedis({}, quoted=set())
    assert _run(quoted_company_tickers(["Amazon"], r)) == []
