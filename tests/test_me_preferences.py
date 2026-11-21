"""A person's settings are keyed by their verified identity and validated whole."""
import asyncio
import json
import pathlib
import sys
from types import SimpleNamespace
from typing import Any, Dict, Optional

import pytest
from fastapi import HTTPException

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.api_gateway.routes import me  # noqa: E402


def _req(identity: Optional[str]) -> Any:
    return SimpleNamespace(state=SimpleNamespace(identity=identity))


class _Db:
    def __init__(self) -> None:
        self.rows: Dict[str, Any] = {}

    async def query_one(self, sql: str, owner: str) -> Optional[Dict[str, Any]]:
        return {"prefs": self.rows[owner]} if owner in self.rows else None

    async def execute(self, sql: str, owner: str, prefs: Any) -> None:
        self.rows[owner] = prefs


def test_settings_belong_to_the_session_not_to_anything_sent() -> None:
    assert me.owner_key(_req("session:Pat@Example.com")) == "pat@example.com"
    assert me.owner_key(_req("dev-client")) == "dev-client"
    for bad in (None, "", "anonymous"):
        with pytest.raises(HTTPException) as e:
            me.owner_key(_req(bad))
        assert e.value.status_code == 401


def test_an_update_merges_and_is_validated_whole() -> None:
    p = me.merge({"tickers": ["aapl"], "focus": "stocks"}, {"town": {"agency": "Oakland Police Department", "state": "ca"}})
    assert p.tickers == ["AAPL"] and p.focus == "stocks" and p.town and p.town.state == "CA"
    # Duplicates fold; unknown keys are ignored rather than stored.
    assert me.merge(None, {"tickers": ["MSFT", "msft"], "evil": 1}).tickers == ["MSFT"]
    for bad in ({"tickers": ["not a ticker"]}, {"focus": "everything"}, {"town": {"agency": "X", "state": "California"}},
                {"tickers": [f"A{chr(65 + i // 26)}{chr(65 + i % 26)}" for i in range(31)]}):
        with pytest.raises(ValueError):
            me.merge(None, bad)


def test_two_people_never_see_each_others_settings() -> None:
    db = _Db()
    asyncio.run(me.patch_preferences({"tickers": ["NVDA"]}, _req("session:a@x.com"), db=db))
    asyncio.run(me.patch_preferences({"tickers": ["TSLA"], "welcomed": True}, _req("session:b@x.com"), db=db))
    a = asyncio.run(me.get_preferences(_req("session:a@x.com"), db=db))
    b = asyncio.run(me.get_preferences(_req("session:b@x.com"), db=db))
    assert a["tickers"] == ["NVDA"] and a["welcomed"] is False
    assert b["tickers"] == ["TSLA"] and b["welcomed"] is True
    fresh = asyncio.run(me.get_preferences(_req("session:new@x.com"), db=db))
    assert fresh["saved"] is False and fresh["tickers"] == [] and fresh["focus"] == "both"
    # Stored as a JSON object for the pool's codec to encode, not a string.
    assert isinstance(db.rows["a@x.com"], dict) and db.rows["a@x.com"]["tickers"] == ["NVDA"]
    # A row written as a JSON string by the first version still reads.
    db.rows["old@x.com"] = json.dumps({"tickers": ["AMD"]})
    assert asyncio.run(me.get_preferences(_req("session:old@x.com"), db=db))["tickers"] == ["AMD"]


def test_last_seen_and_flags_are_stored_per_person_and_validated():
    import pytest
    from services.api_gateway.routes import me
    p = me.merge({}, {"last_seen_at": "2026-11-12T09:30:00Z", "flags": {"welcomed_digest": True, "seen_alerts": False}})
    assert p.last_seen_at == "2026-11-12T09:30:00+00:00" and p.flags == {"welcomed_digest": True, "seen_alerts": False}
    assert me.merge({"last_seen_at": "2026-11-12T09:30:00+00:00"}, {"flags": {"a": True}}).last_seen_at == "2026-11-12T09:30:00+00:00"
    assert me.merge({}, {"last_seen_at": ""}).last_seen_at is None
    for bad in ({"last_seen_at": "yesterday"}, {"last_seen_at": "2099-01-01T00:00:00Z"}, {"flags": {"Bad Name": True}},
                {"flags": {f"f{i}": True for i in range(31)}}):
        with pytest.raises(ValueError):
            me.merge({}, bad)
