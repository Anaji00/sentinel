"""A commodity move becomes an event the OSINT bridges can join.

tradfi_bars held 741 crude, 687 Brent and 374 wheat bars in the thirty days to
4 October and the events table held none of them: the candle path emits only for
bars with volume on a watched equity, and the macro tier serves futures through
volume-less ETF proxies. Every bridge points at a commodity, so no exposure ever
had a market move to join.
"""

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

from shared.models import EventType
from shared.models.events import event_move_pct
from shared.utils.price_moves import (
    band_crossed, day_move_pct, is_futures_move_subject, move_score,
)


def test_bands_and_scores():
    assert band_crossed(1.4) is None
    assert band_crossed(-1.6) == 1.5
    assert band_crossed(3.2) == 3.0
    assert band_crossed(-7.0) == 5.0
    assert move_score(1.5) == 0.5 and move_score(5.0) == 0.85 and move_score(20) == 0.9
    assert day_move_pct(80.0, 84.0) == 5.0
    assert day_move_pct(0.0, 84.0) is None


def test_only_futures_qualify():
    assert is_futures_move_subject("CL=F") and is_futures_move_subject("ES=F")
    assert not is_futures_move_subject("XLE") and not is_futures_move_subject("AAPL")


class _Redis:
    def __init__(self):
        self.keys = set()
        self.raw = self

    async def set(self, key, value, nx=False, ex=None):
        if nx and key in self.keys:
            return None
        self.keys.add(key)
        return True


def _enricher():
    from services.enrichment.enrichers.tradfi import TradFiEnricher

    e = TradFiEnricher.__new__(TradFiEnricher)
    e.redis_client = _Redis()
    e.db = SimpleNamespace(execute=AsyncMock())
    e.scorer = SimpleNamespace(check_watchlist=AsyncMock(return_value=False))
    return e


def _bar(close, open_=80.0, ticker="CL=F"):
    return SimpleNamespace(
        source="finnhub_equities", event_id=f"bar-{close}", trace_id="t",
        occurred_at=datetime(2026, 10, 5, 14, 0, tzinfo=timezone.utc),
        raw_payload={"ticker": ticker, "trade_type": "OHLCV_MINUTE_BAR", "open": open_,
                     "close": close, "high": max(close, open_), "low": min(close, open_),
                     "volume": None, "name": "Crude Oil Futures",
                     "proxy_symbol": "USO", "is_proxy": True},
    )


def _run(enricher, raw):
    return asyncio.run(enricher._enrich_equity_candle(raw, raw.raw_payload))


def test_a_crude_move_is_an_event_once_per_band():
    e = _enricher()
    assert _run(e, _bar(81.0)) is None                       # +1.25%: below the floor
    first = _run(e, _bar(82.0))                               # +2.5%
    assert first is not None and first.type is EventType.PRICE_ANOMALY
    assert first.primary_entity.id == "CL=F"
    assert event_move_pct(first) == 2.5
    assert "priced via USO" in first.headline
    assert _run(e, _bar(82.2)) is None                        # same band, same day
    assert _run(e, _bar(83.0)).financial_data.change_pct_day == 3.75   # next band
    assert _run(e, _bar(77.0)) is not None                    # the other direction


def test_an_equity_bar_is_untouched():
    e = _enricher()
    assert _run(e, _bar(90.0, ticker="XLE")) is None


def test_an_exposure_joins_the_move_of_the_instrument_it_names():
    """`exposes:cl=f` was one opaque token, so no bridge ever reached a rule."""
    from services.correlation.main import _fold, _identity_tokens, _shares_a_subject

    tanker = SimpleNamespace(named_entities=["538001234"], tags=["strait_of_hormuz", "exposes:CL=F"],
                             primary_entity=SimpleNamespace(id="538001234", name="ATLAS"))
    crude = {"entity_id": "CL=F", "entity_name": "CL=F", "named_entities": ["CL=F"], "tags": ["price_move"]}
    gold = {"entity_id": "GC=F", "entity_name": "GC=F", "named_entities": ["GC=F"], "tags": ["price_move"]}
    other_tanker = {"entity_id": "636000001", "entity_name": "NOVA", "named_entities": [],
                    "tags": ["exposes:CL=F"]}

    identity = _identity_tokens(tanker)
    subjects = identity | _fold(tanker.tags)
    assert _shares_a_subject(identity, subjects, crude)
    assert not _shares_a_subject(identity, subjects, gold)
    # Two vessels exposed to crude are not thereby the same subject.
    assert not _shares_a_subject(identity, subjects, other_tanker)


def test_a_republished_session_is_not_a_new_move():
    """Friday's bar, republished after a weekend restart, reported again as new."""
    e = _enricher()
    friday = _bar(82.4)
    assert _run(e, friday) is not None
    sunday_restart = _bar(82.4)
    sunday_restart.occurred_at = datetime(2026, 10, 4, 3, 0, tzinfo=timezone.utc)
    assert _run(e, sunday_restart) is None
    monday = _bar(84.0, open_=82.6)                  # a new session, a new open
    monday.occurred_at = datetime(2026, 10, 5, 14, 0, tzinfo=timezone.utc)
    moved = _run(e, monday)
    assert moved is not None and moved.financial_data.change_pct_day == 1.695
