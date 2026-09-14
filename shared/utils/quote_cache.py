"""One definition of the latest-price cache, shared by every writer.

`sentinel:quotes:latest:{TICKER}` is written by six services and read by nine.
Every writer had its own `ex=3600` literal, and the effect was that the whole
cache drained an hour after the closing bell and stayed empty until the next
open: 347 keys during the session, 7 overnight. Anything pricing an instrument
in the first minutes of a session -- the quant advisory, the covered-call
overlay, the options enricher, the prediction resolver -- got nothing.

An hour was chosen on the reasoning that a stale price is worse than no price.
That is the wrong trade for a cache whose name is "latest": the last known
close *is* the correct latest price for an instrument that has not traded
since, and the platform already agrees with itself on this point -- the agent
price lookup falls back to `tradfi_bars` and the crypto candle lists precisely
so that an expired key does not become an unanswerable question.

So the TTL is sized to span the gap the market actually leaves: a Friday close
to a Tuesday open across a Monday holiday, plus room for a collector restart.
It is not sized to make a price look fresh. Consumers that must not act on a
stale quote should read the durable bar history, which carries its own
timestamps, rather than inferring freshness from this key's existence.

The yield-curve writer in the macro collector already used two days for exactly
this reason; this constant generalises that judgement to the rest of the cache.
"""

import json
from typing import Final, Optional

# Four days: Friday's close to Tuesday's open covers the longest ordinary
# market closure, and leaves a margin for a collector that restarts over the
# weekend. Long holiday closures exceed it, and should: a price eight days old
# is one the cache is right to forget.
QUOTE_CACHE_TTL_SEC: Final[int] = 4 * 86400


def quote_key(ticker: str) -> str:
    """Cache key for an instrument's latest price."""
    return f"sentinel:quotes:latest:{str(ticker).upper().strip()}"


def parse_quote(raw) -> Optional[float]:
    """The price out of whatever this cache holds, or None.

    Every writer in the tree stores a bare number -- `str(price)`,
    `str(current_price)`, `close_p` -- across six services. Nothing writes an
    object.

    `json.loads("93.23")` returns a float perfectly happily, so a reader that
    then calls `.get("price")` raises AttributeError into whatever swallows it
    and returns None for every ticker that exists, every time. The agent tier
    hit exactly that: the prediction resolver read it as "unverifiable, so
    uncounted", no prediction was ever scored and no scorecard ever moved.

    That was found and repaired in `base.py` alone, and the paper broker kept
    the broken shape -- then a later repair copied it a third time, with a test
    whose fixture wrote the object format nobody produces.

    So the parse lives here, next to the key it belongs to, and reads the bare
    number the writers actually store while still accepting an object in case
    one ever does write one.
    """
    if raw is None:
        return None
    text = raw if isinstance(raw, str) else raw.decode("utf-8")
    try:
        quote = json.loads(text)
    except (ValueError, TypeError):
        quote = text

    if isinstance(quote, bool):
        return None
    if isinstance(quote, (int, float)):
        value = float(quote)
    elif isinstance(quote, str):
        try:
            value = float(quote.strip())
        except (ValueError, TypeError):
            return None
    elif isinstance(quote, dict):
        for field in ("price", "close", "last", "c"):
            if quote.get(field) is not None:
                try:
                    value = float(quote[field])
                    break
                except (TypeError, ValueError):
                    return None
        else:
            return None
    else:
        return None

    return value if value > 0 else None
