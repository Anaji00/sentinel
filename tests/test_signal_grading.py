"""Expired trade signals wait for an exit price instead of closing unpriced."""

import asyncio
import json
import pathlib
import sys
from array import array
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.agents import signal_engine as se  # noqa: E402
from shared.utils.signal_store import ACTIVE_KEY, AWAITING_KEY, OUTCOMES_KEY  # noqa: E402


class _Raw:
    def __init__(self) -> None:
        self.h: Dict[str, Dict[str, str]] = {}
        self.lists: Dict[str, List[str]] = {}

    async def hgetall(self, k: str) -> Dict[str, str]:
        return dict(self.h.get(k, {}))

    async def hset(self, k: str, f: str, v: str) -> None:
        self.h.setdefault(k, {})[f] = v

    async def hdel(self, k: str, f: str) -> None:
        self.h.get(k, {}).pop(f, None)

    async def lpush(self, k: str, v: str) -> None:
        self.lists.setdefault(k, []).insert(0, v)

    async def ltrim(self, k: str, a: int, b: int) -> None:
        self.lists[k] = self.lists.get(k, [])[a:b + 1]


class _Redis:
    def __init__(self) -> None:
        self.raw = _Raw()


def _engine(bars: List[tuple]) -> Any:
    eng = se.SignalEngine.__new__(se.SignalEngine)
    eng.redis = _Redis()

    async def series(_instrument: str, _since: datetime) -> Optional[tuple]:
        if not bars:
            return None
        return array("d", [b[0] for b in bars]), array("d", [b[1] for b in bars])
    eng._series = series
    return eng


def _signal(expires: datetime) -> Dict[str, Any]:
    return {"id": "AAPL:1h:long", "instrument": "AAPL", "horizon": "1h", "direction": "long",
            "probability": 0.6, "families": ["block_sell"], "created_at": (expires - timedelta(hours=1)).isoformat(),
            "expires_at": expires.isoformat(), "entry_price": 100.0, "cost_bps": 10.0}


def test_a_signal_just_expired_waits_off_the_board_until_a_bar_past_expiry_exists(monkeypatch) -> None:
    async def no_record(*_a: Any, **_k: Any) -> None:
        return None
    monkeypatch.setattr(se, "record_outcome", no_record)
    exp = datetime.now(timezone.utc) - timedelta(seconds=30)
    eng = _engine([(exp.timestamp() - 300, 100.5)])                 # only a bar before expiry
    eng.redis.raw.h[ACTIVE_KEY] = {"AAPL:1h:long": json.dumps(_signal(exp))}
    assert asyncio.run(eng.expire_signals()) == 0
    assert not eng.redis.raw.h[ACTIVE_KEY] and len(eng.redis.raw.h[AWAITING_KEY]) == 1
    assert not eng.redis.raw.lists.get(OUTCOMES_KEY)


def test_it_is_graded_from_the_first_bar_at_or_after_expiry(monkeypatch) -> None:
    seen: List[bool] = []

    async def record(_r: Any, _p: float, hit: bool) -> None:
        seen.append(hit)
    monkeypatch.setattr(se, "record_outcome", record)
    exp = datetime.now(timezone.utc) - timedelta(minutes=10)
    eng = _engine([(exp.timestamp() - 300, 99.0), (exp.timestamp() + 120, 101.0), (exp.timestamp() + 420, 90.0)])
    eng.redis.raw.h[AWAITING_KEY] = {"AAPL:1h:long@x": json.dumps(_signal(exp))}
    assert asyncio.run(eng.expire_signals()) == 1
    out = json.loads(eng.redis.raw.lists[OUTCOMES_KEY][0])
    assert out["exit_price"] == 101.0 and out["hit"] is True and round(out["return_bps"]) == 100
    assert seen == [True] and not eng.redis.raw.h[AWAITING_KEY]


def test_past_the_grace_period_it_closes_unpriced() -> None:
    exp = datetime.now(timezone.utc) - se.GRADE_GRACE - timedelta(minutes=1)
    eng = _engine([])
    eng.redis.raw.h[AWAITING_KEY] = {"AAPL:1h:long@x": json.dumps(_signal(exp))}
    assert asyncio.run(eng.expire_signals()) == 1
    out = json.loads(eng.redis.raw.lists[OUTCOMES_KEY][0])
    assert out["hit"] is None and out["note"] == "no price within the grace period"


def test_a_short_pays_its_borrow_over_the_horizon() -> None:
    assert se.borrow_cost_bps(None, timedelta(days=5)) == 0.0
    assert round(se.borrow_cost_bps(36.5, timedelta(days=5)), 6) == 50.0      # 36.5%/yr for five days


def test_a_signal_is_graded_net_of_the_market() -> None:
    sig = {"id": "X", "instrument": "X", "horizon": "30d", "direction": "long", "probability": 0.6, "families": [],
           "created_at": "t", "expires_at": "t", "entry_price": 100.0, "spy_entry": 500.0, "cost_bps": 10.0}
    out = se.grade(sig, 102.0, 510.0)                       # stock +2%, market +2%: no edge
    assert abs(out["return_bps"]) < 1 and out["hit"] is False and out["net_of"] == "SPY"
    assert se.grade(sig, 102.0)["return_bps"] > 190          # no SPY exit: raw, as before


def test_an_hour_signal_needs_the_regular_session_for_its_whole_hour() -> None:
    from datetime import datetime, timezone
    from shared.utils.market_session import Session, session_label_at
    after_close = datetime(2026, 10, 16, 20, 51, tzinfo=timezone.utc)      # 16:51 ET: the BA signal
    mid_session = datetime(2026, 10, 19, 15, 0, tzinfo=timezone.utc)       # 11:00 ET
    assert session_label_at("BA", after_close) != Session.REGULAR.name
    assert session_label_at("BA", mid_session) == Session.REGULAR.name
    src = (ROOT / "services" / "agents" / "signal_engine.py").read_text(encoding="utf-8")
    assert 'hname == "1h" and (session_label_at(instrument, now) != Session.REGULAR.name' in src
