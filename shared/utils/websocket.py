import asyncio
import logging
import websockets
from typing import Callable, Optional, Any, Union

logger = logging.getLogger("sentinel.ws")

class ResilientWebSocketClient:
    """Standardized resilient WebSocket client with automatic reconnection and exponential backoff."""
    
    def __init__(
        self,
        url: str,
        name: str,
        ping_interval: float = 30.0,
        ping_timeout: float = 20.0,
        on_connect: Optional[Callable[[websockets.WebSocketClientProtocol], Any]] = None,
        on_message: Optional[Callable[[str], Any]] = None,
        max_backoff: float = 60.0
    ):
        self.url = url
        self.name = name
        self.ping_interval = ping_interval
        self.ping_timeout = ping_timeout
        self.on_connect = on_connect
        self.on_message = on_message
        self.max_backoff = max_backoff
        self.reconnect_count = 0
        self._running = False
        self._task: Optional[asyncio.Task] = None

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

    async def _loop(self):
        backoff = 1.0
        while self._running:
            try:
                logger.info(f"[{self.name}] Connecting to {self.url}...")
                async with websockets.connect(
                    self.url,
                    ping_interval=self.ping_interval,
                    ping_timeout=self.ping_timeout
                ) as ws:
                    logger.info(f"[{self.name}] Connected successfully.")
                    self.reconnect_count = 0
                    backoff = 1.0
                    
                    # Fire on_connect callback
                    if self.on_connect:
                        if asyncio.iscoroutinefunction(self.on_connect):
                            await self.on_connect(ws)
                        else:
                            self.on_connect(ws)
                    
                    # Message loop
                    async for raw_msg in ws:
                        if not self._running:
                            break
                        if self.on_message:
                            if asyncio.iscoroutinefunction(self.on_message):
                                await self.on_message(raw_msg)
                            else:
                                self.on_message(raw_msg)
                                
            except websockets.exceptions.ConnectionClosed as e:
                logger.warning(f"[{self.name}] Connection closed: {e} — reconnecting in {backoff:.1f}s")
            except Exception as e:
                logger.error(f"[{self.name}] Connection error: {e} — reconnecting in {backoff:.1f}s", exc_info=True)
            
            if self._running:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self.max_backoff)
                self.reconnect_count += 1
