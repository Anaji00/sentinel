"""
shared/utils/websocket.py

Resilient WebSocket client with automatic reconnection and exponential backoff.

Reading is decoupled from processing. Previously ``on_message`` was awaited
inline inside ``async for raw_msg in ws``, so the socket was not drained while a
handler ran. On a high-volume feed that starves the keepalive: the RIPE RIS BGP
stream disconnected itself 43 times in 30 minutes with
``sent 1011 ... keepalive ping timeout`` -- the client killing its own
connection because the pong deadline expired while the loop was busy. Widening
the deadline only changed the rate.

The reader now does nothing but move frames into a bounded queue; a worker task
runs the handler. Two properties follow. The socket keeps draining regardless of
handler cost, so pings are answered on time. And when a producer outruns the
handler the queue fills and the oldest frames are dropped, which is the correct
response to a firehose -- shed load and stay connected rather than fall behind
until the connection dies and every message is lost.

Ordering is preserved: a single worker consumes the queue in FIFO order.
"""

import asyncio
import logging
import websockets
from typing import Any, Callable, Optional

logger = logging.getLogger("sentinel.ws")


class ResilientWebSocketClient:
    """Standardized resilient WebSocket client with automatic reconnection and exponential backoff."""

    def __init__(
        self,
        url: str,
        name: str,
        ping_interval: float = 30.0,
        ping_timeout: float = 20.0,
        on_connect: Optional[Callable[[Any], Any]] = None,
        on_message: Optional[Callable[[str], Any]] = None,
        max_backoff: float = 60.0,
        queue_size: int = 1000,
    ):
        self.url = url
        self.name = name
        self.ping_interval = ping_interval
        self.ping_timeout = ping_timeout
        self.on_connect = on_connect
        self.on_message = on_message
        self.max_backoff = max_backoff
        # Bounded on purpose. An unbounded queue converts a throughput problem
        # into a memory problem and still loses everything when the process dies.
        self.queue_size = queue_size
        self.reconnect_count = 0
        self.received = 0
        self.processed = 0
        self.dropped = 0
        self.handler_errors = 0
        self._running = False
        self._task: Optional[asyncio.Task] = None

    @property
    def stats(self) -> dict:
        """Throughput counters. `dropped` rising means the handler cannot keep up."""
        return {
            "received": self.received,
            "processed": self.processed,
            "dropped": self.dropped,
            "handler_errors": self.handler_errors,
            "reconnects": self.reconnect_count,
        }

    async def start(self):
        """Starts the WebSocket client in a background task."""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop())

    async def stop(self):
        """Stops the WebSocket client and cancels the connection loop."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        logger.info(f"[{self.name}] Client stopped.")

    async def _dispatch(self, raw_msg: str) -> None:
        """Runs the handler, tolerating a bad frame without dropping the socket."""
        if not self.on_message:
            return
        try:
            if asyncio.iscoroutinefunction(self.on_message):
                await self.on_message(raw_msg)
            else:
                self.on_message(raw_msg)
            self.processed += 1
        except asyncio.CancelledError:
            raise
        except Exception as e:
            # One bad frame must not tear down a healthy connection. The first
            # is surfaced loudly because a handler that fails on every message
            # would otherwise be invisible; the rest are counted.
            self.handler_errors += 1
            if self.handler_errors == 1:
                logger.warning(f"[{self.name}] Handler error (further errors counted only): {e}")
            else:
                logger.debug(f"[{self.name}] Handler error: {e}")

    async def _worker(self, queue: "asyncio.Queue[str]") -> None:
        """Consumes queued frames so the reader never blocks on handler cost."""
        while True:
            raw_msg = await queue.get()
            try:
                await self._dispatch(raw_msg)
            finally:
                queue.task_done()

    async def _loop(self):
        backoff = 1.0
        while self._running:
            worker: Optional[asyncio.Task] = None
            try:
                logger.info(f"[{self.name}] Connecting to {self.url}...")
                async with websockets.connect(
                    self.url,
                    ping_interval=self.ping_interval,
                    ping_timeout=self.ping_timeout,
                ) as ws:
                    logger.info(f"[{self.name}] Connected successfully.")
                    self.reconnect_count = 0
                    backoff = 1.0

                    queue: "asyncio.Queue[str]" = asyncio.Queue(maxsize=self.queue_size)
                    worker = asyncio.create_task(self._worker(queue))

                    # Fire on_connect callback
                    if self.on_connect:
                        if asyncio.iscoroutinefunction(self.on_connect):
                            await self.on_connect(ws)
                        else:
                            self.on_connect(ws)

                    # Reader: enqueue only. Anything slower than this belongs in
                    # the worker, or the keepalive starves and the peer drops us.
                    async for raw_msg in ws:
                        if not self._running:
                            break
                        self.received += 1
                        try:
                            queue.put_nowait(raw_msg)
                        except asyncio.QueueFull:
                            # Prefer fresh data: discard the oldest frame rather
                            # than the one just read. Stale BGP or market frames
                            # have less value than current ones.
                            self.dropped += 1
                            try:
                                queue.get_nowait()
                                queue.task_done()
                                queue.put_nowait(raw_msg)
                            except (asyncio.QueueEmpty, asyncio.QueueFull):
                                pass
                            if self.dropped % 1000 == 1:
                                logger.warning(
                                    f"[{self.name}] Handler behind: dropped {self.dropped} frames "
                                    f"(queue={self.queue_size}). Filter the subscription or speed up the handler."
                                )

            except websockets.exceptions.ConnectionClosed as e:
                logger.warning(f"[{self.name}] Connection closed: {e} — reconnecting in {backoff:.1f}s")
            except Exception as e:
                logger.error(f"[{self.name}] Connection error: {e} — reconnecting in {backoff:.1f}s", exc_info=True)
            finally:
                if worker is not None:
                    worker.cancel()
                    try:
                        await worker
                    except (asyncio.CancelledError, Exception):
                        pass

            if self._running:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self.max_backoff)
                self.reconnect_count += 1
