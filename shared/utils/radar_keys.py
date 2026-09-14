"""The radar's Redis keys, defined once, for the collector and the gateway.

`/radar/sweeps` reported a baseline count by scanning
`sentinel:radar:mean:*`. The collector writes `sentinel:radar:1m_mean:{ticker}`.
The scan matched nothing on every request, `mean_count` was always zero, and
the response fell through `"tracked_baselines": mean_count or 1840` to a
plausible constant -- for as long as the endpoint has existed.

It is the third key-spelling defect this codebase has paid for, after the
watchlist zrange that always came back empty and the calibration hashes typed
out in the gateway. Two modules that have to agree about a key need one place
that says what it is.

The movers keys are new, and are here for the same reason: they are written by
the radar collector and read by the gateway, which is exactly the shape that
has gone wrong three times.
"""
from __future__ import annotations

from typing import Final

# Per-ticker EWMA baseline of one-minute volume, and its variance and count.
RADAR_BASELINE_PREFIX: Final[str] = "sentinel:radar:1m_mean:"
RADAR_VARIANCE_PREFIX: Final[str] = "sentinel:radar:1m_var:"
RADAR_OBSERVATIONS_PREFIX: Final[str] = "sentinel:radar:1m_n:"


def radar_baseline_key(ticker: str) -> str:
    return f"{RADAR_BASELINE_PREFIX}{ticker}"


def radar_variance_key(ticker: str) -> str:
    return f"{RADAR_VARIANCE_PREFIX}{ticker}"


def radar_observations_key(ticker: str) -> str:
    return f"{RADAR_OBSERVATIONS_PREFIX}{ticker}"


# What the last sweep actually covered, so the endpoint reporting it does not
# have to hold a literal. `/radar/sweeps` published
# `"total_universe_scanned": 4500` from a local constant while the collector's
# own comments put the figure at 11,631.
RADAR_SWEEP_STATE_KEY: Final[str] = "sentinel:radar:last_sweep"

# ── movers ──────────────────────────────────────────────────────────────────
#
# One sorted set scored by the day's move answers both questions: ZREVRANGE for
# gainers, ZRANGE for losers. The per-ticker hash carries the components, so a
# reader can say *why* something moved -- gapped open, or ground higher through
# the session -- rather than only by how much.
MOVERS_DAY_ZSET: Final[str] = "sentinel:movers:day"
MOVERS_SNAPSHOT_PREFIX: Final[str] = "sentinel:movers:snapshot:"

# Long enough to survive a weekend and a collector restart, short enough that a
# delisted ticker leaves. The zset itself is rewritten each sweep.
MOVERS_TTL_SEC: Final[int] = 4 * 86400


def movers_snapshot_key(ticker: str) -> str:
    return f"{MOVERS_SNAPSHOT_PREFIX}{ticker}"


__all__ = [
    "RADAR_BASELINE_PREFIX",
    "RADAR_VARIANCE_PREFIX",
    "RADAR_OBSERVATIONS_PREFIX",
    "radar_baseline_key",
    "radar_variance_key",
    "radar_observations_key",
    "RADAR_SWEEP_STATE_KEY",
    "MOVERS_DAY_ZSET",
    "MOVERS_SNAPSHOT_PREFIX",
    "MOVERS_TTL_SEC",
    "movers_snapshot_key",
]
