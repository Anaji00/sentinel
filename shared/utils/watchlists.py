"""The watchlist keys, in one place, because the last time they were not.

`sentinel:watched:equities` is written and read by six modules and had a named
constant in exactly one of them. One site in the quant engine typed
`sentinel:watched:watchlist:` instead. The zrange came back empty on every
call, `watched_set` resolved to None, and
`generate_covered_call_recommendation` skips its scoping check entirely when
that argument is None -- so the covered-call overlay was evaluated for every
ticker rather than the 44 on the watchlist. It failed open, which is why
nothing ever looked wrong.

`sentinel:watched:vessels` had the opposite problem: a constant in the maritime
enricher and two hand-typed copies in the anomaly scorer. Importing it from the
enricher is not available -- maritime imports `lift_score` from the scorer, so
the two cannot import each other. A key convention shared by two modules
belongs in neither of them.

Both sides of every watchlist read the same name from here now.
"""
from __future__ import annotations

# Tickers the platform is actively tracking. A sorted set, scored by the time
# the ticker was last promoted, so the radar agent can evict the coldest.
WATCHED_EQUITIES_KEY = "sentinel:watched:equities"

# Hulls flagged by sanctions screening or by appearing in a headline, so a
# vessel is recognised the way a watched ticker is.
WATCHED_VESSELS_KEY = "sentinel:watched:vessels"
WATCHED_VESSELS_TTL_SEC = 30 * 86400

__all__ = [
    "WATCHED_EQUITIES_KEY",
    "WATCHED_VESSELS_KEY",
    "WATCHED_VESSELS_TTL_SEC",
]
