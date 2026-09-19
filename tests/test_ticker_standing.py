"""Where a ticker stands, and the joins that make a movers board intelligence.

Phase A kept the day's move. This is the rest of it.

`moving_average_distances()` has returned sma_20/50/200, the distance to each
as a percentage and an alignment label for as long as it has existed, and was
called from exactly two request handlers that recomputed it and discarded the
result. `tradfi_bars` is retained for 400 days precisely so a 200-day average
is reachable. So a watchlist row could say "NVDA, added Tuesday" and nothing
else, while the material for "+8.4% this week, 12% above its 50-day" sat in a
table nobody joined to it.

And a ranked list of percentages is a commodity. What is not: this platform
holds the news and the sector for every name on that list, and had never put
the two together. A top-20 mover with no news event in the window is the one
worth looking at; eight of twenty sharing a sector is a rotation rather than
eight stories.
"""
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils.ticker_stats import (  # noqa: E402
    MIN_SESSIONS_FOR_STATS,
    compute_ticker_stats,
    read_ticker_stats,
    ticker_stats_key,
    trailing_change_pct,
    write_ticker_stats,
)

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


# -- the arithmetic ----------------------------------------------------------


def test_a_short_history_says_so_rather_than_reporting_zeros():
    """A 200-day line drawn through 40 closes is a 40-day line with a
    misleading name, and a row of zeros reads as "at its average"."""
    assert compute_ticker_stats([100.0] * (MIN_SESSIONS_FOR_STATS - 1)) is None
    assert trailing_change_pct([100.0, 101.0], sessions=5) is None


def test_the_trailing_windows_are_sessions_not_calendar_days():
    """A week-over-week number that spans a holiday is still a week of trading."""
    closes = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 110.0]
    # Five sessions back from 110.0 is 101.0.
    assert trailing_change_pct(closes, sessions=5) == pytest.approx(8.9109, abs=1e-3)


def test_the_standing_carries_the_distances_the_platform_already_computed():
    closes = [100.0 + i * 0.5 for i in range(220)]
    stats = compute_ticker_stats(closes)

    assert stats["sessions"] == 220
    assert stats["change_pct_week"] is not None
    assert stats["change_pct_month"] is not None
    for field in ("dist_sma_20_pct", "dist_sma_50_pct", "dist_sma_200_pct"):
        assert stats[field] is not None, f"{field} was computable and is missing"
    assert stats["ma_alignment"] == "BULLISH_STACK"


def test_an_average_the_history_cannot_support_is_null_not_absent():
    """Null says "not enough history"; a missing key says nothing at all."""
    stats = compute_ticker_stats([100.0 + i * 0.1 for i in range(60)])
    assert "dist_sma_200_pct" in stats
    assert stats["dist_sma_200_pct"] is None
    assert stats["dist_sma_50_pct"] is not None


# -- and it round-trips through the store it shares with the readers --------


class _Raw:
    def __init__(self):
        self.values = {}

    async def set(self, key, value, ex=None):
        self.values[key] = value

    async def get(self, key):
        return self.values.get(key)


class _Redis:
    def __init__(self):
        self.raw = _Raw()


async def test_the_standing_round_trips_under_one_key_definition():
    redis = _Redis()
    stats = compute_ticker_stats([100.0 + i * 0.5 for i in range(220)])
    await write_ticker_stats(redis, "nvda", stats)

    assert ticker_stats_key("nvda") == ticker_stats_key("NVDA") == "sentinel:ta:NVDA"
    assert list(redis.raw.values) == ["sentinel:ta:NVDA"]
    assert await read_ticker_stats(redis, "NVDA") == stats


async def test_the_three_answers_are_three_different_values():
    """"No standing" and "could not ask" are not the same fact.

    Both used to be `{}`, and the docstring told a reader to tell them apart by
    `sessions` -- which is absent from both. The same pass was careful that
    `news_events: null` means the join could not run and zero means no news,
    and was not careful here.
    """
    # The store answered, and has nothing for this ticker.
    assert await read_ticker_stats(_Redis(), "NOPE") == {}
    # There is no store to ask.
    assert await read_ticker_stats(None, "NOPE") is None

    class _Broken(_Redis):
        def __init__(self):
            super().__init__()

            class _Raises:
                async def get(self, key):
                    raise ConnectionError("redis is down")

            self.raw = _Raises()

    # The store was asked and could not answer, which is not "no standing".
    assert await read_ticker_stats(_Broken(), "NVDA") is None


# -- the readers ------------------------------------------------------------


def test_every_surface_that_lists_a_ticker_can_say_what_it_did():
    """Three endpoints listed tickers and none of them carried a number.

    `/watchlists/equities` returned ticker plus a priority score;
    `/radar/anomalies` returned a z-score with no price context at all, and its
    watchlist rows carried the promotion timestamp and nothing else.
    """
    watchlists = (ROOT / "services" / "api_gateway" / "routes" / "watchlists.py").read_text(encoding="utf-8")
    radar = (ROOT / "services" / "api_gateway" / "routes" / "radar.py").read_text(encoding="utf-8")

    assert "read_ticker_stats" in watchlists
    assert "read_movers_snapshot" in watchlists
    assert "read_ticker_stats" in radar
    assert "movers_snapshot_key" in radar
    # The anomaly rows, which is the Phase D half: a volume spike on a name up
    # 14% is a different event from the same spike on one that is flat.
    assert "dist_sma_50_pct" in radar


def test_the_job_that_fills_it_is_scheduled():
    """A computation with no caller is the defect this audit keeps finding."""
    enrichment = (ROOT / "services" / "enrichment" / "main.py").read_text(encoding="utf-8")
    assert "_ticker_stats_loop" in enrichment
    assert 'name="ticker-standing"' in enrichment, "defined and never started"


def test_the_standing_is_scoped_to_the_watchlist_rather_than_the_universe():
    """Forty tickers twice an hour, not eleven thousand.

    Widening it is a different decision with a different cost and should be
    taken on measurement rather than by drifting into it.
    """
    enrichment = (ROOT / "services" / "enrichment" / "main.py").read_text(encoding="utf-8")
    block = enrichment[enrichment.index("async def _ticker_stats_loop"):]
    block = block[: block.index("async def _volatility_loop")]
    assert "WATCHED_EQUITIES_KEY" in block
    assert "tradfi_bars_1d" in block, "daily bars, not the raw tick table"


# -- phase E: the joins ------------------------------------------------------


def test_a_mover_with_no_news_is_named_for_what_it_is():
    """Not "unexplained" as a fact about the world.

    Unexplained *by what this platform collected* is a narrower and checkable
    claim, and `news_events: null` has to mean "the join could not run" rather
    than "no news".
    """
    radar = (ROOT / "services" / "api_gateway" / "routes" / "radar.py").read_text(encoding="utf-8")
    assert "movers_without_news_in_window" in radar
    assert '"news_events"] = None' in radar or 'mover["news_events"] = None' in radar
    assert "sector_concentration" in radar


def test_the_news_join_uses_the_types_the_rules_call_news():
    """So this join and the correlation rules cannot disagree about what news is."""
    from services.api_gateway.routes.radar import _NEWS_TYPES

    correlation = (ROOT / "services" / "correlation" / "main.py").read_text(encoding="utf-8")
    for event_type in _NEWS_TYPES:
        assert f'"{event_type}"' in correlation, (
            f"{event_type} is not a type the correlation rules recognise"
        )


class _DB:
    def __init__(self, rows):
        self.rows = rows
        self.params = None

    async def query(self, sql, *params):
        self.params = params
        return self.rows


async def test_the_news_count_is_a_count_and_not_a_verdict():
    from services.api_gateway.routes.radar import _attach_news_counts

    movers = [{"ticker": "AAA"}, {"ticker": "BBB"}]
    db = _DB([{"ticker": "AAA", "n": 3}])
    await _attach_news_counts(db, movers, ["AAA", "BBB"], 24)

    assert movers[0]["news_events"] == 3
    assert movers[1]["news_events"] == 0, "no news is zero, once the join ran"


async def test_without_a_database_the_join_says_it_did_not_run():
    from services.api_gateway.routes.radar import _attach_news_counts

    movers = [{"ticker": "AAA"}]
    await _attach_news_counts(None, movers, ["AAA"], 24)
    assert movers[0]["news_events"] is None, (
        "null and zero are different answers and a reader acts on them "
        "differently"
    )


async def test_sector_concentration_counts_every_mover():
    from services.api_gateway.routes.radar import _attach_sectors

    class _R:
        def __init__(self):
            self.raw = self

        async def get(self, key):
            if key.endswith("AAA"):
                return json.dumps({"sector": "Semiconductors"})
            if key.endswith("BBB"):
                return json.dumps({"sector": "Semiconductors"})
            return None

    movers = [{"ticker": "AAA"}, {"ticker": "BBB"}, {"ticker": "CCC"}]
    concentration = await _attach_sectors(_R(), movers, [m["ticker"] for m in movers])

    assert concentration["Semiconductors"] == 2
    assert concentration["UNKNOWN"] == 1, "an unresolved sector still counts"
    assert sum(concentration.values()) == len(movers)
    assert movers[2]["sector"] == "UNKNOWN"
