"""Senior review of 18 Nov 2026, findings F1, F2, F5, F11 (owner-approved 19 Nov): the trader must not mistake a failed call for an empty account, must not
store a rebuilt book from stale scores, must not send a fractional short, and must not build a limit on a trade that is hours old."""
import asyncio
import json
import pathlib
from datetime import date, datetime, timedelta, timezone

import pytest

from services.agents import auto_trader as tr
from shared.broker import alpaca
from shared.utils import auto_trade as at
from shared.utils.signal_store import FACTOR_REPORT_KEY, factor_scores_key

ROOT = pathlib.Path(__file__).resolve().parents[1]


# ── F1 ───────────────────────────────────────────────────────────────────────

class _Resp:
    def __init__(self, status, body=None):
        self.status, self._body = status, body or []

    async def json(self):
        return self._body

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False


class _Session:
    def __init__(self, resp, *a, **kw):
        self.resp = resp

    def get(self, *a, **kw):
        return self.resp

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False


def _broker(monkeypatch, status):
    monkeypatch.setattr(alpaca.aiohttp, "ClientSession", lambda *a, **kw: _Session(_Resp(status)))
    b = alpaca.AlpacaBroker.__new__(alpaca.AlpacaBroker)
    b.api_key, b.secret_key, b.headers, b.base_url = "k", "s", {}, "http://x"
    return b


def test_a_failed_positions_call_raises_instead_of_looking_like_an_empty_account(monkeypatch):
    with pytest.raises(RuntimeError, match="503"):
        asyncio.run(_broker(monkeypatch, 503).get_positions())
    assert asyncio.run(_broker(monkeypatch, 200).get_positions()) == []


def test_the_trader_stops_when_positions_cannot_be_read():
    src = (ROOT / "services/agents/auto_trader.py").read_text(encoding="utf-8")
    body = src.split("async def run_once")[1].split("async def _halted")[0]
    i = body.index("positions = await self.broker.get_positions()")
    assert "except Exception" in body[i:i + 400] and "positions unavailable" in body[i:i + 700]
    assert i < body.index("await self._send(")


# ── F5 ───────────────────────────────────────────────────────────────────────

def test_a_capped_short_is_whole_shares_and_a_cap_under_one_share_leaves_nothing():
    p = at.Plan("XYZ", 40.0, "sell", "open")
    assert at.liquidity_capped(p, 100.0, 31_000.0, 0.01).qty == 3.0           # 3.1 shares allowed -> 3
    assert at.liquidity_capped(p, 100.0, 5_000.0, 0.01).qty == 0.0            # 0.5 share -> dropped by the caller
    buy = at.Plan("XYZ", 40.0, "buy", "open")
    assert at.liquidity_capped(buy, 100.0, 31_000.0, 0.01).qty == 3.1         # buys may stay fractional
    for adv in (9_000.0, 31_000.0, 123_456.0, 2_000_000.0):
        for reason in ("open", "add", "flip-open"):
            q = at.liquidity_capped(at.Plan("A", 50.0, "sell", reason), 37.13, adv, 0.01).qty
            assert q == int(q)


def test_zero_quantity_plans_are_not_sent():
    src = (ROOT / "services/agents/auto_trader.py").read_text(encoding="utf-8")
    assert "plans = [p for p in plans if p.qty > 0]" in src


# ── F11 ──────────────────────────────────────────────────────────────────────

def test_an_old_or_unreadable_trade_time_is_not_a_limit_reference():
    now = datetime(2026, 11, 19, 15, 0, tzinfo=timezone.utc)
    fresh = (now - timedelta(minutes=2)).isoformat().replace("+00:00", "Z")
    old = (now - timedelta(days=3)).isoformat().replace("+00:00", "Z")
    assert alpaca.trade_is_fresh(fresh, 1800.0, now)
    assert not alpaca.trade_is_fresh(old, 1800.0, now)
    assert not alpaca.trade_is_fresh(None, 1800.0, now) and not alpaca.trade_is_fresh("garbage", 1800.0, now)
    assert not alpaca.trade_is_fresh((now + timedelta(hours=1)).isoformat(), 1800.0, now), "a time in the future is not a trade"


# ── F2 ───────────────────────────────────────────────────────────────────────

class _Raw:
    def __init__(self, store):
        self.store = store

    async def get(self, k):
        return self.store.get(k)

    async def set(self, k, v, **kw):
        self.store[k] = v


class _Db:
    async def query_one(self, sql, *a):
        return {"n": 21}

    async def query(self, sql, *a):
        return []


def _trader(store):
    t = tr.AutoTrader(_Db(), None, broker=None)
    t.raw = _Raw(store)
    return t


def test_a_rebuild_from_stale_scores_is_not_stored(monkeypatch):
    from shared.utils import asset_universe

    async def meta(raw):
        return {"LOW": ["stock", True, True], "HI": ["stock", True, True]}

    monkeypatch.setattr(asset_universe, "load_asset_meta", meta)
    monkeypatch.setattr(tr.at, "MIN_SCORES_TO_REBUILD", 1)
    held = json.dumps({"rebalanced_on": "2026-10-01", "long": ["OLD"], "short": []})
    scores = {"HI": {"percentile": 0.95}, "LOW": {"percentile": 0.03}}
    store = {tr.BOOK_KEY: held, factor_scores_key("monthly"): json.dumps(scores),
             FACTOR_REPORT_KEY: json.dumps({"as_of": "2026-11-10"})}
    t = _trader(store)

    async def behind():
        return 3

    t._scores_sessions_behind = behind
    book = asyncio.run(t._book(date(2026, 11, 19)))
    assert book["long"] == ["OLD"] and store[tr.BOOK_KEY] == held, "stale scores: the stored book stays, byte for byte"

    async def fresh():
        return 0

    t._scores_sessions_behind = fresh
    book = asyncio.run(t._book(date(2026, 11, 19)))
    assert book["long"] == ["HI"] and book["scores_as_of"] == "2026-11-10"
    assert json.loads(store[tr.BOOK_KEY])["scores_as_of"] == "2026-11-10"


def test_a_halted_day_does_not_store_a_rebuild(monkeypatch):
    held = json.dumps({"rebalanced_on": "2026-10-01", "long": ["OLD"], "short": []})
    store = {tr.BOOK_KEY: held}
    t = _trader(store)

    async def halted():
        return "AUTO_TRADE_HALT is set"

    t._halted = halted
    assert asyncio.run(t._book(date(2026, 11, 19)))["long"] == ["OLD"] and store[tr.BOOK_KEY] == held


# ── F12 (owner: "do whats best for accuracy and returns", 20 Nov) ────────────

def test_the_calendar_counts_sessions_the_bars_table_cannot():
    wed, mon = date(2026, 11, 11), date(2026, 11, 16)
    assert at.weekdays_between(wed, mon) == 2                 # Thu, Fri completed; the weekend is not a session
    assert at.weekdays_between(date(2026, 11, 18), date(2026, 11, 19)) == 0
    assert at.weekdays_between(date(2026, 11, 13), date(2026, 11, 20)) == 4   # Mon-Thu: five weekdays old stalls the guard
    assert at.scores_too_old(at.weekdays_between(date(2026, 11, 13), date(2026, 11, 20))) is not None


def test_a_stalled_bar_feed_cannot_make_old_scores_look_fresh():
    class Db:
        async def query_one(self, sql, *a):
            return {"n": 0}                                     # bars stopped, so no SPY day after the as-of date

    t = tr.AutoTrader(Db(), None, broker=None)
    t.raw = _Raw({FACTOR_REPORT_KEY: json.dumps({"as_of": "2026-10-01"})})
    behind = asyncio.run(t._scores_sessions_behind())
    assert behind is not None and behind >= at.MAX_SESSIONS_BEHIND
