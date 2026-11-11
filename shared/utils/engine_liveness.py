"""
shared/utils/engine_liveness.py

Is the factor engine alive? (approved 11 Nov 2026 after the 54-minute silent stall of 6 Nov.)

The factor-model service runs the nightly model, the paper-account trader and about thirty jobs in one event loop, and
nothing watched it: it stopped for 54 minutes and nothing said so. It now beats every HEARTBEAT_SEC on the loop it
shares with the trader (shared/utils/heartbeat.py, component 'factor-engine'), so a blocked loop silences the beat. A
watchdog in another service (services/agents/factor_watchdog.py) reads the beat and, when it is older than
STALE_AFTER_SEC, raises a STALE_FACTOR_ENGINE alert and sets a halt flag that the trader checks before any order.
The flag is transient: it expires, and is cleared as soon as the beat is fresh again. Nothing held is sold.

Pure: a heartbeat payload and a clock in, a verdict out.
"""
from __future__ import annotations

import json
from datetime import datetime, time, timezone
from typing import Any, Optional, Tuple

COMPONENT = "factor-engine"
HEARTBEAT_SEC = 60
STALE_AFTER_SEC = 180
HEARTBEAT_TTL_SEC = 900                    # the key outlives the stale bar, so 'old' and 'gone' are told apart
HALT_KEY = "sentinel:factor_engine:halt"
HALT_TTL_SEC = 300
SEEN_KEY = "sentinel:factor_engine:seen"      # set once a beat has been read; before that, 'missing' is not an outage
ALERT_ONCE_KEY = "sentinel:factor_engine:stale_alerted"
ALERT_ONCE_TTL_SEC = 3600
MARKET_OPEN_UTC = time(13, 30)             # 09:30 New York in winter; the extra hour in summer is covered by the alert tier only
MARKET_CLOSE_UTC = time(21, 0)


def heartbeat_age(payload: Any, now: Optional[datetime] = None) -> Optional[float]:
    """Seconds since the heartbeat's own timestamp; None when absent or unreadable."""
    now = now or datetime.now(timezone.utc)
    if payload is None:
        return None
    try:
        data = json.loads(payload) if isinstance(payload, (str, bytes)) else payload
        ts = datetime.fromisoformat(str(data["ts"]))
    except (ValueError, TypeError, KeyError):
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return max(0.0, (now - ts).total_seconds())


def verdict(age: Optional[float], limit: float = STALE_AFTER_SEC) -> Tuple[str, str]:
    """('ok' | 'stale' | 'missing', reason)."""
    if age is None:
        return "missing", "no readable heartbeat from the factor engine"
    if age > limit:
        return "stale", f"the factor engine has been silent for {age:.0f}s (limit {limit:.0f}s)"
    return "ok", f"last heartbeat {age:.0f}s ago"


def in_market_hours(now: datetime) -> bool:
    n = now.astimezone(timezone.utc)
    return n.weekday() < 5 and MARKET_OPEN_UTC <= n.time() < MARKET_CLOSE_UTC


__all__ = ["ALERT_ONCE_KEY", "ALERT_ONCE_TTL_SEC", "COMPONENT", "HALT_KEY", "HALT_TTL_SEC", "HEARTBEAT_SEC", "SEEN_KEY",
           "HEARTBEAT_TTL_SEC", "STALE_AFTER_SEC", "heartbeat_age", "in_market_hours", "verdict"]
