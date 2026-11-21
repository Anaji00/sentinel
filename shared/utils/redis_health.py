"""Redis memory health: how full it is against its limit and how many keys it has evicted since the last look. Eviction is silent loss of
volatile caches (64 on 13 Nov 2026 at 82% of 400 MB), so any new eviction, or 85% full, is an alarm. Pure; no I/O."""
from __future__ import annotations

from typing import Any, Dict, Mapping, Optional, Tuple

ALARM_SHARE = 0.85


def summarize(mem: Mapping[str, Any], stats: Mapping[str, Any], baseline: Optional[int]) -> Dict[str, Any]:
    used = float(mem.get("used_memory") or 0)
    limit = float(mem.get("maxmemory") or 0)
    evicted = int(stats.get("evicted_keys") or 0)
    since = None if baseline is None else max(0, evicted - int(baseline))
    share = round(used / limit, 3) if limit > 0 else None
    alarms = []
    if share is not None and share >= ALARM_SHARE:
        alarms.append(f"memory {share:.0%} of its limit")
    if since:
        alarms.append(f"{since} keys evicted since monitoring began on this Redis run")
    return {"used_mb": round(used / 1e6, 1), "max_mb": round(limit / 1e6, 1) if limit > 0 else None, "used_share": share,
            "policy": mem.get("maxmemory_policy"), "evicted_keys": evicted, "evicted_since_baseline": since, "alarms": alarms}


WINDOW_SEC = 86400


def plan_baseline(stored: Any, run_id: str, now: int, evicted: int, window_sec: int = WINDOW_SEC) -> Tuple[Optional[int], Optional[str]]:
    """(baseline for this answer, new value to store or None). The stored value is "run_id|window_start|count". Within a window of the same Redis run the
    baseline is the stored count; a new Redis run starts its counter at zero, so its baseline is 0; an expired window or an unreadable value opens a fresh window
    whose baseline is the count now, so an eviction alarm lasts at most about a day and clears by itself. Never raises on a corrupt value."""
    text = stored.decode("utf-8", "replace") if isinstance(stored, bytes) else (str(stored) if stored is not None else "")
    parts = text.split("|")
    if len(parts) == 3 and parts[1].isdigit() and parts[2].lstrip("-").isdigit():
        same_run = parts[0] == run_id
        start, count = int(parts[1]), int(parts[2])
        if same_run and now - start < window_sec:
            return count, None
        if not same_run:
            return 0, f"{run_id}|{now}|0"                  # the new run's counter began at zero
        return evicted, f"{run_id}|{now}|{evicted}"        # same run, window over: start a new one from here
    return evicted, f"{run_id}|{now}|{evicted}"            # nothing stored, or garbage: start now


__all__ = ["ALARM_SHARE", "WINDOW_SEC", "plan_baseline", "summarize"]
