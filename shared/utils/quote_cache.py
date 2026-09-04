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

from typing import Final

# Four days: Friday's close to Tuesday's open covers the longest ordinary
# market closure, and leaves a margin for a collector that restarts over the
# weekend. Long holiday closures exceed it, and should: a price eight days old
# is one the cache is right to forget.
QUOTE_CACHE_TTL_SEC: Final[int] = 4 * 86400


def quote_key(ticker: str) -> str:
    """Cache key for an instrument's latest price."""
    return f"sentinel:quotes:latest:{str(ticker).upper().strip()}"
