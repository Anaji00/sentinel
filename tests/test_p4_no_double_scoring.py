"""P4 (owner-approved 20 Nov 2026): a prediction whose durable row is already resolved must not move a scorecard a second time (SR-20261117-001 was
persisted 6 times with outcomes t,t,t,t,f,t)."""
import asyncio
import inspect
import logging
from types import SimpleNamespace

from services.agents.base import SentinelAgent


class _Db:
    def __init__(self, row=None, boom=False):
        self.row, self.boom, self.args = row, boom, None

    async def query_one(self, sql, *a):
        self.args = a
        if self.boom:
            raise RuntimeError("db down")
        return self.row


class _Concrete(SentinelAgent):
    """The base class is abstract only in handle and output_topic, which this test never calls."""

    async def handle(self, *a, **k):
        return None

    @property
    def output_topic(self):
        return None


def _agent(db):
    a = _Concrete.__new__(_Concrete)
    a.name, a.db, a.logger = "stock_correlation_agent", db, logging.getLogger("t")
    return a


def _pred(strategy=None):
    return SimpleNamespace(prediction_id="SR-20261117-001", strategy=strategy)


def test_a_resolved_durable_row_means_already_resolved():
    db = _Db(row={"found": 1})
    assert asyncio.run(_agent(db)._already_resolved(_pred())) is True
    assert db.args == ("SR-20261117-001", "stock_correlation_agent")


def test_no_durable_row_means_score_it():
    assert asyncio.run(_agent(_Db(row=None))._already_resolved(_pred())) is False


def test_an_unreadable_table_keeps_the_old_behaviour_and_is_counted():
    assert asyncio.run(_agent(_Db(boom=True))._already_resolved(_pred())) is False
    assert asyncio.run(_agent(None)._already_resolved(_pred())) is False


def test_the_check_runs_before_the_scorecard_is_touched():
    src = inspect.getsource(SentinelAgent.resolve_due_predictions)
    assert src.index("_already_resolved(pred)") < src.index("await self.update_scorecard(")
    assert "re_resolved" in src and "continue" in src[src.index("_already_resolved(pred)"):src.index("await self.update_scorecard(")]
