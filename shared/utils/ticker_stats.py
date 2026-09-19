"""What the platform knows about where a ticker stands, per ticker.

The day's move comes from the radar's snapshot sweep, which sees every symbol
and nothing historical. This is the other half: where the price sits relative
to its own recent history, which needs daily bars rather than a snapshot.

All of it was already computable and none of it was stored.
`moving_average_distances()` has returned `sma_20`, `sma_50`, `sma_200`, the
distance to each as a percentage and an alignment label for as long as it has
existed, and is called from exactly two places -- a trading advisory and one
explainability endpoint -- both of which recompute it per request and throw the
result away. `tradfi_bars` is retained for 400 days precisely so a 200-day
average is reachable, a decision migration 0020 made deliberately.

So a watchlist row could say "NVDA, added Tuesday" and nothing else, while the
data to say "+3.1% today, +8.4% this week, 12% above its 50-day, aligned" sat
in two places that never met.

One definition of the key, for the writer in the enrichment service and the
readers in the gateway, because a key spelled out in two places is how
`sentinel:watched:equities` came to be read under a name nobody wrote.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional, Sequence

from shared.utils.quiet_failures import swallowed

logger = logging.getLogger("shared.ticker_stats")

TICKER_STATS_PREFIX = "sentinel:ta:"

# A session's worth of margin past a daily refresh. Long enough that a Monday
# read still finds Friday's figures, short enough that a delisted ticker leaves.
TICKER_STATS_TTL_SEC = 5 * 86400

# Sessions in a week and a month, for the trailing windows. Trading days, not
# calendar days: a week-over-week number that spans a holiday is still a week
# of trading.
SESSIONS_PER_WEEK = 5
SESSIONS_PER_MONTH = 21

# Below this the averages mean nothing. A 200-day line drawn through 40 closes
# is a 40-day line with a misleading name, and the platform has already shipped
# one estimator that reported a warm-up guess as a measurement.
MIN_SESSIONS_FOR_STATS = 25


def ticker_stats_key(ticker: str) -> str:
    return f"{TICKER_STATS_PREFIX}{str(ticker).upper().strip()}"


def trailing_change_pct(closes: Sequence[float], sessions: int) -> Optional[float]:
    """Percent change over the last `sessions` closes, or None.

    None rather than zero when the history is too short: a ticker listed three
    days ago has not been flat for a month.
    """
    if not closes or len(closes) <= sessions:
        return None
    then = closes[-(sessions + 1)]
    now = closes[-1]
    if not then or then <= 0 or not now or now <= 0:
        return None
    return round((now / then - 1.0) * 100.0, 4)


def compute_ticker_stats(closes: Sequence[float]) -> Optional[Dict[str, Any]]:
    """Where this ticker stands, from its daily closes. Oldest first.

    None when there is not enough history to say anything, which is a different
    answer from a row of zeros and has to stay different: the whole point of
    these numbers is that a reader can act on them.
    """
    prices = []
    for close in closes or []:
        try:
            value = float(close)
        except (TypeError, ValueError):
            continue
        if value == value and value > 0:
            prices.append(value)

    if len(prices) < MIN_SESSIONS_FOR_STATS:
        return None

    from shared.utils.quant_calc import moving_average_distances

    distances = moving_average_distances(prices) or {}
    stats: Dict[str, Any] = {
        "last_close": round(prices[-1], 6),
        "sessions": len(prices),
        # When these numbers were last true.
        #
        # `tradfi_bars_1d` is a continuous aggregate whose policy carries
        # `end_offset => INTERVAL '1 day'` and refreshes daily, so it
        # deliberately excludes the current day. A watchlist row therefore
        # carries a live `day_pct` from the snapshot sweep beside a
        # `change_pct_week` that ends a session or two earlier, and neither used
        # to say so. `as_of_sessions_back` is how far behind the close this
        # standing is, in sessions, filled by the caller that knows.
        "as_of_sessions_back": None,
        "change_pct_week": trailing_change_pct(prices, SESSIONS_PER_WEEK),
        "change_pct_month": trailing_change_pct(prices, SESSIONS_PER_MONTH),
    }
    # Only the averages the history can actually support. `moving_average_
    # distances` returns None for one it cannot compute, and a null here says
    # "not enough history" rather than "at its average".
    for field in (
        "sma_20", "dist_sma_20_pct",
        "sma_50", "dist_sma_50_pct",
        "sma_200", "dist_sma_200_pct",
        "ma_alignment",
    ):
        stats[field] = distances.get(field)
    return stats


async def write_ticker_stats(redis_client: Any, ticker: str, stats: Dict[str, Any]) -> None:
    if redis_client is None or not stats:
        return
    try:
        raw = getattr(redis_client, "raw", redis_client)
        await raw.set(
            ticker_stats_key(ticker), json.dumps(stats), ex=TICKER_STATS_TTL_SEC
        )
    except Exception as _exc:
        swallowed("shared.ticker_stats.write", _exc, logger)


async def read_ticker_stats(redis_client: Any, ticker: str) -> Optional[Dict[str, Any]]:
    """This ticker's standing; {} if it has none; None if the store could not answer.

    Three different facts used to be one empty dict: the store was unreachable,
    the job has not run yet, and this ticker has too little history. Its own
    docstring told a reader to tell them apart by `sessions` and then said
    `sessions` is absent in all three.

    The same pass was careful that `news_events: null` means "the join could not
    run" and zero means "no news", and was not careful here. So:

      None  the store could not be read -- say nothing about this ticker
      {}    no standing stored -- the job has not reached it, or its history is
            too short for any of these numbers to mean anything
      dict  the standing
    """
    if redis_client is None:
        return None
    try:
        raw = getattr(redis_client, "raw", redis_client)
        blob = await raw.get(ticker_stats_key(ticker))
        if not blob:
            return {}
        text = blob if isinstance(blob, str) else blob.decode("utf-8")
        loaded = json.loads(text)
        return loaded if isinstance(loaded, dict) else {}
    except Exception as _exc:
        swallowed("shared.ticker_stats.read", _exc, logger)
        return None


__all__ = [
    "TICKER_STATS_PREFIX",
    "TICKER_STATS_TTL_SEC",
    "MIN_SESSIONS_FOR_STATS",
    "SESSIONS_PER_WEEK",
    "SESSIONS_PER_MONTH",
    "ticker_stats_key",
    "trailing_change_pct",
    "compute_ticker_stats",
    "write_ticker_stats",
    "read_ticker_stats",
]
