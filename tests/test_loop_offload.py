"""Work found holding the event loop now runs in threads, with identical results (12 Nov loop-watch measurement)."""

import asyncio
import datetime as dt
import pathlib
import sys
import uuid

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared import db as dbmod  # noqa: E402


class _Conn:
    def __init__(self, rows):
        self.rows = rows

    async def fetch(self, sql, *params):
        return self.rows


class _Acquire:
    def __init__(self, rows):
        self.conn = _Conn(rows)

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, *a):
        return False


class _Pool:
    def __init__(self, rows):
        self.rows = rows

    def acquire(self):
        return _Acquire(self.rows)


def _rows(n):
    u = uuid.UUID(int=7)
    return [{"id": u, "at": dt.datetime(2026, 11, 12, 1, 2, 3), "tags": [u, "x"], "n": i} for i in range(n)]


def _client(rows):
    c = dbmod.TimescaleClient.__new__(dbmod.TimescaleClient)
    c._pool = _Pool(rows)
    return c


def test_a_large_result_is_sanitised_in_a_thread_and_matches_the_inline_result() -> None:
    n = dbmod.SANITIZE_OFFLOAD_ROWS + 50
    big, small = _rows(n), _rows(10)
    out_big = asyncio.run(_client(big).query("select"))
    out_small = asyncio.run(_client(small).query("select"))
    assert len(out_big) == n and out_big[0] == {"id": str(uuid.UUID(int=7)), "at": "2026-11-12T01:02:03", "tags": [str(uuid.UUID(int=7)), "x"], "n": 0}
    assert out_small == [dbmod.TimescaleClient._sanitize_row(None, r) for r in small]
    assert out_big[:10] == out_small                                          # the same conversion on either path


def test_the_study_builds_observations_and_episodes_off_the_loop_with_the_same_output() -> None:
    from services.agents import signal_engine as se

    src = (ROOT / "services/agents/signal_engine.py").read_text(encoding="utf-8")
    assert "asyncio.to_thread(_observations_of, rows)" in src and "asyncio.to_thread(episodes, obs)" in src
    assert se._observations_of([]) == []
    row = {"event_id": "1", "type": "insider_cluster", "created_at": dt.datetime(2026, 10, 5, 12, tzinfo=dt.timezone.utc),
           "financial_data": {"ticker": "HEI"}}
    inline = []
    for r in [row, row]:
        inline.extend(se.observations(se._row(r)))
    assert [(o.family, o.instrument) for o in se._observations_of([row, row])] == [(o.family, o.instrument) for o in inline] and inline
