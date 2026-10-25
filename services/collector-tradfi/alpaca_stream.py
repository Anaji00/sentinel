"""
services/collector-tradfi/alpaca_stream.py

Real-time minute bars for the whole liquid universe from Alpaca's free IEX
stream.

The free plan caps trades and quotes at 30 symbols, and Finnhub's at 50, so
live minute bars covered ~47 names and everything else waited for snapshots.
The minute-bar channel has no symbol cap on the free plan, so the whole
minute universe is subscribed by name. (A wildcard `bars: ["*"]` delivered
7,839 bars for 7,659 symbols once on 15 Oct 2026, then nothing.) This keeps
the bars for the minute universe -- the most liquid names by dollar volume
plus the watchlist -- and writes them straight to tradfi_bars in batches.

IEX bars are one venue's trades, a few percent of the tape. They are
provisional: the settle pass replaces each with the consolidated bar for the
same minute once the vendor serves it (15 minutes on the free plan). Bars for
symbols Finnhub already streams are skipped -- that minute is built from
Finnhub's trades, and a second writer for the same row would flap.

The free plan allows one stream connection per account; nothing else in the
platform opens one.
"""
from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple

import aiohttp

from shared.utils import live_focus as lf
from shared.utils.quiet_failures import swallowed
from shared.utils.market_session import session_label_at
from shared.utils.quote_cache import QUOTE_CACHE_TTL_SEC, quote_key

logger = logging.getLogger("collector.tradfi.alpaca_stream")

STREAM_URL = os.getenv("ALPACA_STREAM_URL", "wss://stream.data.alpaca.markets/v2/iex")
SOURCE = "alpaca_iex_stream"
FLUSH_EVERY_SEC = 5.0
MAX_BATCH = 5000
RECONNECT_MAX_SEC = 120

INSERT_SQL = """
    INSERT INTO tradfi_bars (ticker, time, open, high, low, close, volume, session, source)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
    ON CONFLICT (ticker, time) DO UPDATE
    SET open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low, close = EXCLUDED.close,
        volume = EXCLUDED.volume, session = EXCLUDED.session, source = EXCLUDED.source
    -- Never over a consolidated bar: the stream can redeliver a minute
    -- ("updatedBars") after the settle pass has already written it.
    WHERE tradfi_bars.source IS DISTINCT FROM 'alpaca_sip'
"""


def bar_row(msg: Dict[str, Any]) -> Optional[Tuple]:
    """A stream bar message ({"T": "b", "S", "o", "h", "l", "c", "v", "t"}) as a
    tradfi_bars row, or None for anything that is not a usable bar."""
    if msg.get("T") not in ("b", "u"):
        return None
    sym = str(msg.get("S") or "").upper()
    try:
        ts = datetime.fromisoformat(str(msg.get("t")).replace("Z", "+00:00"))
        o, h, lo, c = (float(msg[k]) for k in ("o", "h", "l", "c"))
        v = float(msg.get("v") or 0.0)
    except (KeyError, TypeError, ValueError):
        return None
    if not sym or c <= 0 or lo <= 0 or h < lo:
        return None
    return (sym, ts, o, h, lo, c, v, session_label_at(sym, ts), SOURCE)


TICK_SOURCE = "alpaca_iex_ticks"
TRADE_SQL = """
    INSERT INTO tick_trades (ticker, time, price, size, trade_id, exchange, conditions, source)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
"""
QUOTE_SQL = """
    INSERT INTO tick_quotes (ticker, time, bid, bid_size, ask, ask_size, source)
    VALUES ($1, $2, $3, $4, $5, $6, $7)
"""


def tick_symbols(focus: Sequence[str], limit: int = lf.ALPACA_TICK_SLOTS) -> List[str]:
    """The free plan's trade-and-quote slots, given to the live focus in order."""
    return list(dict.fromkeys(str(t).upper() for t in focus if t))[:max(0, limit)]


def keep(rows: Iterable[Tuple], universe: Set[str], skip: Set[str]) -> List[Tuple]:
    """Rows for the minute universe, minus symbols another feed builds."""
    return [r for r in rows if r[0] in universe and r[0] not in skip]


async def run_stream(
    credentials: Callable[[], Optional[Dict[str, str]]],
    get_db: Callable[[], Awaitable[Any]],
    redis_raw: Any,
    universe: Callable[[], Set[str]],
    skip: Callable[[], Set[str]],
    focus: Callable[[], Sequence[str]] = lambda: (),
) -> None:
    """Hold the stream open for the life of the collector, reconnecting with
    backoff. `universe` and `skip` are read at every flush, so the minute
    universe and the Finnhub subscription can change without a reconnect.
    `focus` (shared/utils/live_focus.py) takes the plan's 30 trade-and-quote
    slots: every trade and top-of-book change for the names the portfolio is
    trading or holding, stored tick by tick."""
    backoff = 2.0
    while True:
        headers = credentials()
        if not headers:
            logger.warning("Alpaca stream: no credentials; retrying in 10 minutes.")
            await asyncio.sleep(600)
            continue
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(STREAM_URL, heartbeat=30) as ws:
                    await ws.receive_json()
                    await ws.send_json({"action": "auth", "key": headers["APCA-API-KEY-ID"],
                                        "secret": headers["APCA-API-SECRET-KEY"]})
                    auth = await ws.receive_json()
                    if not any(m.get("msg") == "authenticated" for m in auth):
                        raise RuntimeError(f"auth refused: {auth}")
                    # By name, not "*": the bar channel has no symbol cap on the
                    # free plan, and a wildcard subscription delivered one burst
                    # and then nothing (15 Oct).
                    while not universe():
                        await asyncio.sleep(5)
                    subscribed = sorted(universe())
                    # Bars and ticks in separate messages: a tick request over
                    # the plan's symbol limit is refused whole (405), and must
                    # not take the minute bars down with it.
                    await ws.send_json({"action": "subscribe", "bars": subscribed, "updatedBars": subscribed})
                    ticks = tick_symbols(focus())
                    if ticks:
                        await ws.send_json({"action": "subscribe", "trades": ticks, "quotes": ticks})
                    logger.info("Alpaca stream: subscribed to minute bars for %s symbols and trades and quotes "
                                "for %s (IEX).", len(subscribed), len(ticks))
                    backoff = 2.0
                    # One task reads the socket without a timeout; another
                    # flushes on a timer. Wrapping each read in wait_for() and
                    # cancelling it every five seconds stalled the reader after
                    # its first frame (one frame in five minutes, 15 Oct).
                    state = {"pending": [], "received": 0, "kept": 0, "frames": 0,
                             "trades": [], "quotes": [], "ticks": 0, "tick_limit": lf.ALPACA_TICK_SLOTS}
                    throttle = lf.QuoteThrottle()
                    flusher = asyncio.create_task(_flush_forever(state, get_db, redis_raw, universe, skip,
                                                                 ws=ws, subscribed=set(subscribed),
                                                                 focus=focus, tick_subscribed=set(ticks)))
                    try:
                        async for msg in ws:
                            if msg.type != aiohttp.WSMsgType.TEXT:
                                if msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                    break
                                continue
                            state["frames"] += 1
                            for m in msg.json() if msg.data else []:
                                if m.get("T") == "error":
                                    logger.warning("Alpaca stream error: %s", m)
                                    # Over the symbol limit: halve the tick slots;
                                    # the flusher resubscribes with fewer.
                                    if m.get("code") == 405 and state["tick_limit"] > 0:
                                        state["tick_limit"] //= 2
                                        state["retick"] = True
                                    continue
                                kind = m.get("T")
                                if kind == "t":
                                    tr = lf.trade_row(m, TICK_SOURCE)
                                    if tr:
                                        state["trades"].append(tr)
                                        state["ticks"] += 1
                                    continue
                                if kind == "q":
                                    qr = lf.quote_row(m, TICK_SOURCE)
                                    if qr and throttle.keep(qr):
                                        state["quotes"].append(qr)
                                        state["ticks"] += 1
                                    continue
                                row = bar_row(m)
                                if row:
                                    state["pending"].append(row)
                                    state["received"] += 1
                    finally:
                        flusher.cancel()
                    logger.warning("Alpaca stream closed by the server; reconnecting.")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            swallowed("collector.tradfi.alpaca_stream", e, logger)
        await asyncio.sleep(backoff)
        backoff = min(RECONNECT_MAX_SEC, backoff * 2)


async def _flush_forever(state: Dict[str, Any], get_db, redis_raw, universe, skip,
                         ws: Any = None, subscribed: Optional[Set[str]] = None,
                         focus: Callable[[], Sequence[str]] = lambda: (),
                         tick_subscribed: Optional[Set[str]] = None) -> None:
    last_report = asyncio.get_running_loop().time()
    subscribed = subscribed if subscribed is not None else set()
    tick_subscribed = tick_subscribed if tick_subscribed is not None else set()
    while True:
        await asyncio.sleep(FLUSH_EVERY_SEC)
        # Keep the subscription in step with the minute universe.
        if ws is not None:
            want = set(universe())
            add, drop = sorted(want - subscribed), sorted(subscribed - want)
            try:
                if add:
                    await ws.send_json({"action": "subscribe", "bars": add, "updatedBars": add})
                if drop:
                    await ws.send_json({"action": "unsubscribe", "bars": drop, "updatedBars": drop})
                if add or drop:
                    subscribed.clear()
                    subscribed.update(want)
                    logger.info("Alpaca stream: +%s / -%s symbols (%s subscribed).", len(add), len(drop), len(want))
            except Exception as e:
                swallowed("collector.tradfi.alpaca_stream.resubscribe", e, logger)
            # And the tick slots with the live focus.
            want_t = set(tick_symbols(focus(), state.get("tick_limit", lf.ALPACA_TICK_SLOTS)))
            if state.pop("retick", False):
                tick_subscribed.clear()          # the refused request subscribed nothing
            add_t, drop_t = sorted(want_t - tick_subscribed), sorted(tick_subscribed - want_t)
            try:
                if drop_t:
                    await ws.send_json({"action": "unsubscribe", "trades": drop_t, "quotes": drop_t})
                if add_t:
                    await ws.send_json({"action": "subscribe", "trades": add_t, "quotes": add_t})
                if add_t or drop_t:
                    tick_subscribed.clear()
                    tick_subscribed.update(want_t)
                    logger.info("Alpaca stream: tick slots +%s / -%s (%s).", len(add_t), len(drop_t), len(want_t))
            except Exception as e:
                swallowed("collector.tradfi.alpaca_stream.retick", e, logger)
        trades, state["trades"] = state.get("trades", []), []
        quotes, state["quotes"] = state.get("quotes", []), []
        if trades or quotes:
            await _write_ticks(get_db, trades, quotes)
        pending, state["pending"] = state["pending"], []
        if pending:
            rows = keep(pending, universe(), skip())
            state["kept"] += len(rows)
            if rows:
                await _write(get_db, redis_raw, rows)
        now = asyncio.get_running_loop().time()
        if now - last_report >= 300:
            logger.info("Alpaca stream: %s frames, %s bars received, %s kept for the %s-name minute universe, "
                        "%s ticks for %s names in the last 5 minutes.", state["frames"], state["received"],
                        state["kept"], len(universe()), state.get("ticks", 0), len(tick_subscribed))
            state.update(received=0, kept=0, frames=0, ticks=0)
            last_report = now


async def _write_ticks(get_db: Callable[[], Awaitable[Any]], trades: List[Tuple], quotes: List[Tuple]) -> None:
    try:
        db = await get_db()
        if trades:
            await db.execute_many(TRADE_SQL, trades)
        if quotes:
            await db.execute_many(QUOTE_SQL, quotes)
    except Exception as e:
        swallowed("collector.tradfi.alpaca_stream.write_ticks", e, logger, detail=f"{len(trades)}/{len(quotes)}")


async def _write(get_db: Callable[[], Awaitable[Any]], redis_raw: Any, rows: List[Tuple]) -> None:
    try:
        db = await get_db()
        await db.execute_many(INSERT_SQL, rows)
    except Exception as e:
        swallowed("collector.tradfi.alpaca_stream.write", e, logger, detail=str(len(rows)))
        return
    # The latest price for each name, so quote readers see the whole universe
    # rather than the streamed fifty.
    try:
        latest: Dict[str, float] = {}
        for r in rows:
            latest[r[0]] = r[5]
        async with redis_raw.pipeline() as pipe:
            for sym, px in latest.items():
                pipe.set(quote_key(sym), px, ex=QUOTE_CACHE_TTL_SEC)
            await pipe.execute()
    except Exception as e:
        swallowed("collector.tradfi.alpaca_stream.quotes", e, logger)


__all__ = ["SOURCE", "bar_row", "keep", "run_stream"]
