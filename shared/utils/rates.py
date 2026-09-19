"""The risk-free rate this platform actually collects.

There was a whole path for this and no reader. A collector polls the New York
Fed, the tradfi enricher handles `source == "nyfed_sofr"`, writes the value to
two Redis keys with a 24-hour TTL, and logs "Updated live Federal Reserve
risk-free rate in Redis." Each of those keys appeared exactly once in the
repository -- at the line that wrote it.

Meanwhile `sharpe_ratio()` takes `risk_free_rate: float = 0.0` and both callers
omitted it, so every Sharpe the platform published assumed money is free; and
the covered-call backtester priced Black-Scholes with `r=0.045` typed in, a
stale approximation of the number sitting in Redis.

One definition of the key, one reader, one fallback -- and the fallback is
labelled, because a rate nobody measured is an assumption and should be
readable as one.
"""
from __future__ import annotations

import logging
from typing import Any, Optional

from shared.utils.quiet_failures import swallowed

logger = logging.getLogger("shared.rates")

# Written by the tradfi enricher on every nyfed_sofr event.
RISK_FREE_RATE_KEY = "sentinel:macro:risk_free_rate"
SOFR_RATE_KEY = "sentinel:macro:sofr_rate"

# Used only when the live rate is unavailable. Not zero: a Sharpe ratio
# computed against 0% overstates every strategy, in the same direction, by the
# whole of the short rate. Not silently plausible either -- callers that report
# to a user should say which of the two they used.
ASSUMED_RISK_FREE_RATE = 0.04

# A short rate outside this band is a parse error, not a policy change.
_PLAUSIBLE = (-0.01, 0.25)


def _parse(raw: Any) -> Optional[float]:
    if raw is None:
        return None
    text = raw.decode() if isinstance(raw, bytes) else str(raw)
    try:
        value = float(text.strip())
    except (TypeError, ValueError):
        return None
    if value != value:
        return None
    # Tolerates either convention: 4.3 means 4.3%, 0.043 means the same.
    if value > 1.0:
        value = value / 100.0
    return value if _PLAUSIBLE[0] <= value <= _PLAUSIBLE[1] else None


async def risk_free_rate(redis_client: Any) -> tuple:
    """(rate, is_live). The live SOFR where it is known, else the assumption.

    Returned as a pair rather than a bare float so a caller can label the
    number it publishes. A figure derived from a measured rate and one derived
    from a constant are different claims, and this platform has spent four
    hundred findings on the difference.
    """
    if redis_client is None:
        return ASSUMED_RISK_FREE_RATE, False
    try:
        raw = getattr(redis_client, "raw", redis_client)
        for key in (RISK_FREE_RATE_KEY, SOFR_RATE_KEY):
            value = _parse(await raw.get(key))
            if value is not None:
                return value, True
    except Exception as _exc:
        swallowed("shared.rates.risk_free_rate", _exc, logger)
    return ASSUMED_RISK_FREE_RATE, False


__all__ = [
    "RISK_FREE_RATE_KEY",
    "SOFR_RATE_KEY",
    "ASSUMED_RISK_FREE_RATE",
    "risk_free_rate",
]
