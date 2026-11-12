"""A batch keeps its entity locks alive while it works (services/agents/supervisor.py)."""

import asyncio
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.agents import supervisor as sup  # noqa: E402


class _Raw:
    def __init__(self, held):
        self.held, self.calls = dict(held), []

    async def eval(self, script, n, key, token, *rest):
        self.calls.append((key, token))
        return 1 if self.held.get(key) == token else 0


class _Redis:
    def __init__(self, raw):
        self.raw = raw


def _agent(raw):
    a = sup.GraphSupervisor.__new__(sup.GraphSupervisor)
    a.redis = _Redis(raw)
    a.name = "graph_supervisor"
    return a


def test_renewal_extends_each_held_lock_repeatedly_and_stops_when_cancelled(monkeypatch) -> None:
    monkeypatch.setattr(sup, "LOCK_TTL_SEC", 0.06)
    raw = _Raw({"sentinel:lock:neo4j:AAPL": "t1", "sentinel:lock:neo4j:NVDA": "t2"})
    held = {"AAPL": "t1", "NVDA": "t2"}

    async def scenario() -> int:
        task = asyncio.create_task(_agent(raw)._renew_locks(held))
        await asyncio.sleep(0.15)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        n = len(raw.calls)
        await asyncio.sleep(0.1)
        return n - len(raw.calls)

    assert asyncio.run(scenario()) == 0                          # nothing after cancel
    assert {c[0] for c in raw.calls} == {"sentinel:lock:neo4j:AAPL", "sentinel:lock:neo4j:NVDA"} and len(raw.calls) >= 4


def test_a_lost_lock_is_dropped_from_the_renewal_set_and_not_renewed_again(monkeypatch) -> None:
    monkeypatch.setattr(sup, "LOCK_TTL_SEC", 0.06)
    raw = _Raw({"sentinel:lock:neo4j:AAPL": "someone-else"})          # the lease lapsed and another writer holds it
    held = {"AAPL": "t1"}

    async def scenario() -> None:
        task = asyncio.create_task(_agent(raw)._renew_locks(held))
        await asyncio.sleep(0.15)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    asyncio.run(scenario())
    assert held == {} and len(raw.calls) == 1


def test_the_batch_starts_renewal_before_taking_locks_and_cancels_it_in_the_finally() -> None:
    src = (ROOT / "services/agents/supervisor.py").read_text(encoding="utf-8")
    i = src.index("async def execute_batch_proposals")
    body = src[i:src.index("async def execute_proposal", i)]
    assert body.index("self._renew_locks(acquired_locks)") < body.index("await self.acquire_lock(eid)")
    assert "renewer.cancel()" in body
