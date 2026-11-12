"""The news-scoring job reads only stocks of $1bn and over, and scores nothing without the market-cap table."""

import asyncio
import pathlib
import sys
from datetime import datetime, timezone

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.agents import llm_news_job as job  # noqa: E402


class _Raw:
    def __init__(self, caps):
        self.caps = caps

    async def hgetall(self, key):
        return self.caps

    async def get(self, key):
        return datetime.now(timezone.utc).date().isoformat()          # features already written today

    async def set(self, key, value):
        return True


class _Db:
    def __init__(self):
        self.calls = []

    async def query(self, sql, *args):
        self.calls.append((sql, args))
        return []

    async def execute(self, *a):
        return None


def _run(caps):
    db = _Db()
    out = asyncio.run(job.LlmNewsJob(db, _Raw(caps)).run_once(llm=None))
    return db, out


def test_the_query_carries_only_the_large_names_in_order() -> None:
    db, out = _run({"AAA": "2e9", "BBB": "5e8", "CCC": "1e10", "DDD": "garbage"})
    unscored = [c for c in db.calls if "stock_news_llm" in c[0] and "NOT EXISTS" in c[0]]
    assert len(unscored) == 1 and unscored[0][1][2] == ["AAA", "CCC"]
    assert "ANY($3::text[])" in job._UNSCORED_SQL and out["scored"] == 0


def test_without_a_cap_table_no_headline_is_read_or_scored() -> None:
    db, out = _run({})
    assert [c for c in db.calls if "NOT EXISTS" in c[0]] == [] and out == {"scored": 0, "requests": 0, "stale_skipped": 0}


def test_the_floor_is_the_model_floor_not_the_calls_floor() -> None:
    src = (ROOT / "services/agents/llm_news_job.py").read_text(encoding="utf-8")
    assert "mc.large_caps(self.raw, mc.MODEL_MIN_MARKET_CAP_USD)" in src
