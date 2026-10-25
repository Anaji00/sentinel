"""
services/collector-tradfi/option_stream.py

Live option quotes and trades for the names the portfolio holds or is trading,
on the free plan's indicative feed: up to 200 contracts, MessagePack only. The
feed is Alpaca's indicative pricing, not OPRA -- quotes are a guide, not an
executable market -- and that is stated on every row (`source`).

Which contracts: for each name in the live focus (shared/utils/live_focus.py),
in order, the two calls and two puts nearest the stock's price on the next
standard monthly expiry at least a week out -- 50 names at four contracts each.
The selection is rebuilt every RESELECT_SEC as the focus moves.
"""
from __future__ import annotations

import asyncio
import logging
import os
from datetime import date, datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional, Sequence, Tuple

import aiohttp
import msgpack

from shared.utils import live_focus as lf
from shared.utils.quiet_failures import swallowed
from shared.utils.quote_cache import parse_quote, quote_key

logger = logging.getLogger("collector.tradfi.option_stream")

STREAM_URL = os.getenv("ALPACA_OPTION_STREAM_URL", "wss://stream.data.alpaca.markets/v1beta1/indicative")
SOURCE = "alpaca_indicative"
PER_SIDE = 2                     # calls and puts per name: 4 contracts a name
MIN_DAYS_OUT = 7
RESELECT_SEC = 30 * 60
FLUSH_EVERY_SEC = 5.0
RECONNECT_MAX_SEC = 120
INSERT_SQL = """
    INSERT INTO option_ticks (contract, underlying, time, kind, bid, bid_size, ask, ask_size, price, size, source)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
"""


def next_monthly(today: date, third_friday: Callable[[int, int], date]) -> Tuple[int, int]:
    """(year, month) of the first standard monthly expiry at least MIN_DAYS_OUT away."""
    y, m = today.year, today.month
    while third_friday(y, m) < today + timedelta(days=MIN_DAYS_OUT):
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return y, m


async def select_contracts(session: aiohttp.ClientSession, redis_raw: Any, focus: Sequence[str],
                           slots: int = lf.OPTION_QUOTE_SLOTS) -> List[str]:
    """Contracts for the focus, in its order, until the slots are spent."""
    import option_history as oh

    today = datetime.now(timezone.utc).date()
    year, month = next_monthly(today, oh.third_friday)
    out: List[str] = []
    for ticker in focus:
        if len(out) + 2 * PER_SIDE > slots:
            break
        try:
            spot = parse_quote(await redis_raw.get(quote_key(ticker)))
            if not spot:
                continue
            found = await oh._contracts(session, ticker, year, month, (spot * 0.85, spot * 1.15))
            chain = [c for rows in found.values() for c in rows]
            out.extend(lf.pick_contracts(chain, spot, PER_SIDE))
        except Exception as e:
            swallowed("collector.tradfi.option_stream.select", e, logger, detail=ticker)
    return out[:slots]


async def run_stream(credentials: Callable[[], Optional[Dict[str, str]]], get_db: Callable[[], Awaitable[Any]],
                     redis_raw: Any, focus: Callable[[], Sequence[str]]) -> None:
    """Hold the option stream open, re-choosing contracts as the focus moves."""
    backoff = 2.0
    while True:
        headers = credentials()
        if not headers:
            await asyncio.sleep(600)
            continue
        try:
            async with aiohttp.ClientSession(headers=headers) as session:
                while not focus():
                    await asyncio.sleep(30)
                contracts = await select_contracts(session, redis_raw, list(focus()))
                if not contracts:
                    logger.info("Option stream: no contracts for the live focus yet; retrying in 10 minutes.")
                    await asyncio.sleep(600)
                    continue
                async with session.ws_connect(STREAM_URL, heartbeat=30) as ws:
                    await _recv(ws)
                    await ws.send_bytes(msgpack.packb({"action": "auth", "key": headers["APCA-API-KEY-ID"],
                                                       "secret": headers["APCA-API-SECRET-KEY"]}))
                    auth = await _recv(ws)
                    # The handshake carries the key headers, so the server may
                    # have authenticated already: that answer is a success too.
                    if not any(m.get("msg") in ("authenticated", "already authenticated") for m in auth):
                        raise RuntimeError(f"auth refused: {auth}")
                    await ws.send_bytes(msgpack.packb({"action": "subscribe", "quotes": contracts,
                                                       "trades": contracts}))
                    logger.info("Option stream: %s contracts on %s names (indicative feed).",
                                len(contracts), len({lf.OCC.match(c).group(1) for c in contracts}))
                    backoff = 2.0
                    state: Dict[str, Any] = {"rows": [], "ticks": 0}
                    flusher = asyncio.create_task(_flush_forever(state, get_db))
                    throttle = lf.QuoteThrottle()
                    started = asyncio.get_running_loop().time()
                    try:
                        async for msg in ws:
                            if msg.type != aiohttp.WSMsgType.BINARY:
                                if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                    break
                                continue
                            for m in _decode(msg.data):
                                if m.get("T") == "error":
                                    # The key headers already signed the session in.
                                    if m.get("msg") != "already authenticated":
                                        logger.warning("Option stream error: %s", m)
                                    continue
                                row = lf.option_tick_row(m, SOURCE)
                                # The throttle reads (symbol, time, bid, bid_size, ask, ask_size).
                                if row and (row[3] == "t" or throttle.keep((row[0], row[2]) + row[4:8])):
                                    state["rows"].append(row)
                                    state["ticks"] += 1
                            # Re-choose the contracts as the focus moves: reconnect cleanly.
                            if asyncio.get_running_loop().time() - started > RESELECT_SEC:
                                break
                    finally:
                        flusher.cancel()
                        await _flush(state, get_db)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            swallowed("collector.tradfi.option_stream", e, logger)
            await asyncio.sleep(backoff)
            backoff = min(RECONNECT_MAX_SEC, backoff * 2)


def _decode(data: bytes) -> List[Dict[str, Any]]:
    try:
        got = msgpack.unpackb(data, timestamp=3)
    except Exception:
        return []
    return [m for m in (got if isinstance(got, list) else [got]) if isinstance(m, dict)]


async def _recv(ws: Any) -> List[Dict[str, Any]]:
    msg = await ws.receive()
    if msg.type == aiohttp.WSMsgType.BINARY:
        return _decode(msg.data)
    return []


async def _flush(state: Dict[str, Any], get_db: Callable[[], Awaitable[Any]]) -> None:
    rows, state["rows"] = state["rows"], []
    if not rows:
        return
    try:
        db = await get_db()
        await db.execute_many(INSERT_SQL, rows)
    except Exception as e:
        swallowed("collector.tradfi.option_stream.write", e, logger, detail=str(len(rows)))


async def _flush_forever(state: Dict[str, Any], get_db: Callable[[], Awaitable[Any]]) -> None:
    last_report = asyncio.get_running_loop().time()
    while True:
        await asyncio.sleep(FLUSH_EVERY_SEC)
        await _flush(state, get_db)
        now = asyncio.get_running_loop().time()
        if now - last_report >= 300:
            logger.info("Option stream: %s ticks in the last 5 minutes.", state["ticks"])
            state["ticks"] = 0
            last_report = now


__all__ = ["next_monthly", "run_stream", "select_contracts"]
