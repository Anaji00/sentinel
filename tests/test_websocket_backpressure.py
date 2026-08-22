"""Guards the decoupling that keeps a WebSocket collector connected under load.

`on_message` used to be awaited inline inside `async for raw_msg in ws`, so the
socket was not drained while a handler ran. On the RIPE RIS BGP firehose that
starved the keepalive: the client sent `1011 keepalive ping timeout` and dropped
its own connection 43 times in 30 minutes, so cyber data never landed.

The reader now only enqueues. These tests pin the three properties that fix
depends on: a slow handler cannot stall reading, a full queue sheds the oldest
frames instead of blocking or growing without bound, and a raising handler does
not tear down the connection.
"""

import asyncio
import pathlib
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from shared.utils import websocket as ws_mod  # noqa: E402
from shared.utils.websocket import ResilientWebSocketClient  # noqa: E402


class FakeWS:
    """Yields a fixed set of frames, then stays open so no reconnect occurs."""

    def __init__(self, messages):
        self._messages = list(messages)
        self.sent = []
        self.exhausted = asyncio.Event()

    async def send(self, payload):
        self.sent.append(payload)

    async def __aiter__(self):  # pragma: no cover - replaced below
        raise NotImplementedError

    def __aiter__(self):
        async def gen():
            for m in self._messages:
                yield m
            self.exhausted.set()
            await asyncio.Event().wait()   # hold the connection open
        return gen()


class FakeConnect:
    def __init__(self, ws):
        self._ws = ws

    async def __aenter__(self):
        return self._ws

    async def __aexit__(self, *exc):
        return False


@pytest.fixture
def patch_connect(monkeypatch):
    def _install(fake_ws):
        monkeypatch.setattr(ws_mod.websockets, "connect",
                            lambda url, **kw: FakeConnect(fake_ws))
    return _install


@pytest.mark.anyio
async def test_frames_reach_the_handler(patch_connect):
    seen = []
    fake = FakeWS(["a", "b", "c"])
    patch_connect(fake)

    client = ResilientWebSocketClient(url="ws://x", name="T", on_message=seen.append)
    await client.start()
    await asyncio.wait_for(fake.exhausted.wait(), timeout=5)
    await asyncio.sleep(0.05)
    await client.stop()

    assert seen == ["a", "b", "c"]
    assert client.processed == 3
    assert client.dropped == 0


@pytest.mark.anyio
async def test_on_connect_fires_before_reading(patch_connect):
    fake = FakeWS(["x"])
    patch_connect(fake)

    async def on_connect(ws):
        await ws.send("subscribe")

    client = ResilientWebSocketClient(url="ws://x", name="T", on_connect=on_connect)
    await client.start()
    await asyncio.wait_for(fake.exhausted.wait(), timeout=5)
    await client.stop()

    assert fake.sent == ["subscribe"]


@pytest.mark.anyio
async def test_slow_handler_does_not_stall_the_reader(patch_connect):
    """The property the fix exists for: reading must outpace processing.

    With inline dispatch, reading 50 frames through a handler that takes 20ms
    each would take a second and the socket would go undrained throughout.
    """
    started = asyncio.Event()

    async def slow(_msg):
        started.set()
        await asyncio.sleep(0.02)

    fake = FakeWS([f"m{i}" for i in range(50)])
    patch_connect(fake)

    client = ResilientWebSocketClient(url="ws://x", name="T", on_message=slow)
    await client.start()

    # All 50 frames are read long before the handler has worked through them.
    await asyncio.wait_for(fake.exhausted.wait(), timeout=2)
    await started.wait()
    assert client.received == 50
    assert client.processed < 50, "handler kept pace; the test no longer proves decoupling"

    await client.stop()


@pytest.mark.anyio
async def test_full_queue_sheds_oldest_frames(patch_connect):
    """Overflow drops rather than blocking the reader or growing unbounded."""
    release = asyncio.Event()
    seen = []

    async def blocked(msg):
        await release.wait()
        seen.append(msg)

    fake = FakeWS([f"m{i}" for i in range(60)])
    patch_connect(fake)

    client = ResilientWebSocketClient(
        url="ws://x", name="T", on_message=blocked, queue_size=5,
    )
    await client.start()
    await asyncio.wait_for(fake.exhausted.wait(), timeout=2)

    assert client.received == 60
    assert client.dropped > 0, "queue should have overflowed"
    release.set()
    await asyncio.sleep(0.05)
    await client.stop()

    # Freshness is preferred: the newest frame survives, the oldest are shed.
    assert "m59" in seen
    assert len(seen) <= 60 - client.dropped + 1


@pytest.mark.anyio
async def test_handler_exception_does_not_kill_the_connection(patch_connect):
    seen = []

    def explodes(msg):
        if msg == "bad":
            raise ValueError("malformed frame")
        seen.append(msg)

    fake = FakeWS(["good1", "bad", "good2"])
    patch_connect(fake)

    client = ResilientWebSocketClient(url="ws://x", name="T", on_message=explodes)
    await client.start()
    await asyncio.wait_for(fake.exhausted.wait(), timeout=5)
    await asyncio.sleep(0.05)
    await client.stop()

    assert seen == ["good1", "good2"], "a bad frame must not stop later ones"
    assert client.handler_errors == 1
    assert client.reconnect_count == 0, "a handler error must not force a reconnect"


@pytest.mark.anyio
async def test_stats_exposes_throughput(patch_connect):
    fake = FakeWS(["a"])
    patch_connect(fake)
    client = ResilientWebSocketClient(url="ws://x", name="T", on_message=lambda m: None)
    await client.start()
    await asyncio.wait_for(fake.exhausted.wait(), timeout=5)
    await asyncio.sleep(0.05)
    stats = client.stats
    await client.stop()

    assert set(stats) == {"received", "processed", "dropped", "handler_errors", "reconnects"}
    assert stats["received"] == 1
