"""
tests/test_neo4j_desynced_connection_is_discarded.py

A Bolt connection whose stream broke under a non-driver error was kept.

neo4j 5.14.1 retires a connection itself on a socket error, a cancellation or a
server-reported failure. A RuntimeError from two readers on one StreamReader --
"read() called while another coroutine is already waiting for incoming data" --
is none of those, so the connection stayed marked healthy. The session then
released it, the release sent RESET down the broken stream, and the socket
leaked from the pool with its stray reader still attached.

Live, 2026-10-06/07: the supervisor dead-lettered ontology proposals in bursts
of RuntimeError, BufferError ("Existing exports of data") and PackStream
ValueError ("Expected structure, found marker A2").

These run two concurrent calls through one Neo4jClient, as the supervisor's
dispatches do: the call whose stream broke must close its connection, and the
call beside it must complete and release its own normally.
"""

import asyncio
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from neo4j.exceptions import ClientError, ServiceUnavailable

from shared.db import Neo4jClient

DESYNC = "read() called while another coroutine is already waiting for incoming data"


class _FakeResult:
    def __init__(self, session: "_FakeSession") -> None:
        self._session = session

    async def consume(self) -> None:
        await self._session.drain()

    async def data(self) -> List[Dict[str, Any]]:
        await self._session.drain()
        return [{"x": self._session.params.get("x")}]


class _FakeSession:
    """One session on its own connection; raises `error` while draining, if given."""

    def __init__(self, gate: asyncio.Event, error: Optional[BaseException]) -> None:
        self._gate = gate
        self._error = error
        self.params: Dict[str, Any] = {}
        self.cancelled = False
        self.closed = False

    async def __aenter__(self) -> "_FakeSession":
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        self.closed = True

    async def run(self, cypher: str, **params: Any) -> _FakeResult:
        self.params = params
        return _FakeResult(self)

    async def drain(self) -> None:
        # Both calls are in flight on separate connections before either reads.
        await self._gate.wait()
        if self._error is not None:
            raise self._error

    def cancel(self) -> None:
        self.cancelled = True


class _FakeDriver:
    def __init__(self, errors: List[Optional[BaseException]]) -> None:
        self._errors = list(errors)
        self.gate = asyncio.Event()
        self.sessions: List[_FakeSession] = []

    def session(self) -> _FakeSession:
        s = _FakeSession(self.gate, self._errors.pop(0))
        self.sessions.append(s)
        return s


def _client(driver: _FakeDriver) -> Neo4jClient:
    client = Neo4jClient()
    client._driver = driver
    return client


async def _two_concurrent(
    method: str, errors: List[Optional[BaseException]]
) -> tuple:
    driver = _FakeDriver(errors)
    client = _client(driver)
    call = getattr(client, method)
    first = asyncio.ensure_future(call("RETURN $x", {"x": 1}))
    second = asyncio.ensure_future(call("RETURN $x", {"x": 2}))
    await asyncio.sleep(0)
    driver.gate.set()
    results = await asyncio.gather(first, second, return_exceptions=True)
    return driver.sessions, results


def test_execute_discards_a_desynced_connection_and_spares_the_concurrent_call() -> None:
    sessions, results = asyncio.run(
        _two_concurrent("execute", [RuntimeError(DESYNC), None])
    )
    broken, healthy = sessions

    assert isinstance(results[0], RuntimeError)
    assert results[1] is None
    assert broken.cancelled, "the connection that hit the desync went back to the pool"
    assert not healthy.cancelled
    assert broken.closed and healthy.closed


def test_query_discards_a_desynced_connection_and_spares_the_concurrent_call() -> None:
    sessions, results = asyncio.run(
        _two_concurrent("query", [None, BufferError("Existing exports of data")])
    )
    healthy, broken = sessions

    assert results[0] == [{"x": 1}]
    assert isinstance(results[1], BufferError)
    assert broken.cancelled
    assert not healthy.cancelled


def test_packstream_desync_is_discarded() -> None:
    sessions, results = asyncio.run(
        _two_concurrent("execute", [ValueError("Expected structure, found marker A2"), None])
    )
    assert isinstance(results[0], ValueError)
    assert sessions[0].cancelled
    assert not sessions[1].cancelled


def test_driver_reported_failures_stay_with_the_driver() -> None:
    """A server error leaves the stream intact; killing it would only cost a reconnect."""
    sessions, results = asyncio.run(
        _two_concurrent(
            "execute",
            [ClientError("constraint violated"), ServiceUnavailable("down")],
        )
    )
    assert isinstance(results[0], ClientError)
    assert isinstance(results[1], ServiceUnavailable)
    assert not any(s.cancelled for s in sessions)
