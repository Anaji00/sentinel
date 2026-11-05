"""
shared/utils/source_health.py

Whether each source is still delivering, judged against its own rhythm.

Failures were counted per call; a source going quiet was counted nowhere. On
30 Oct 2026 an audit found four event types silent for weeks -- and all four
had been switched off on purpose (aviation positions, vessel static data and
on-chain transfers to spare enrichment, cyber retired). So silence alone is
not a fault: a source is judged against its own last 14 days, and deliberate
silences are listed here with their reason. Pure.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Dict, List, Mapping, Optional, Sequence, Tuple

BASELINE_DAYS = 14
MIN_GAP_H = 6.0             # never flag a source quieter than this
MAX_GAP_H = 7 * 24.0
GAP_MULTIPLE = 4.0          # silent after this many average gaps (no gap history)
GAP_MULTIPLE_OF_LONGEST = 1.25   # silent past the longest normal gap, with this much room
THIN_SHARE = 0.3            # a day under this share of the quietest normal day is thin
THIN_MIN_DAILY = 20.0       # sources rarer than this a day are judged on silence alone
# The platform being down is not a source being quiet. An hour with under this
# share of the median hour's events, for at least this many hours running, is an
# outage: left out of every source's baseline, or a four-day shutdown would teach
# each source that five days of silence is normal for the next two weeks.
OUTAGE_SHARE = 0.05
OUTAGE_MIN_HOURS = 2

# Silence that is expected, with the reason: off on purpose, or a source that is
# quiet by nature. Matched on type or on source.
RETIRED: Dict[str, str] = {
    "ownership_changes": "published once per 13F quarter, after the filing deadline",
    "sec_edgar_13f": "13Fs cluster at the quarterly deadlines",
    "openfda": "a handful of recalls a month",
    "cnbc_finance": "shares URLs with cnbc_world, whose earlier poll is credited with the story",
    "krebs_security": "cyber retired; a news feed left running",
    "flight_position": "aviation is an anomaly feed: emergency squawks only (collector-adsb)",
    "vessel_static": "routine identity reports held out of events by enrichment",
    "crypto_transfer": "on-chain whales off by default (ONCHAIN_WHALES=0)",
    "ransomware": "cyber collectors retired",
    "bgp_anomaly": "cyber collectors retired",
    "vulnerability": "cyber collectors retired",
    "infrastructure_degraded": "cyber collectors retired",
}


def outage_windows(hourly: Mapping[datetime, float], first: datetime, last: datetime) -> List[Tuple[datetime, datetime]]:
    """(start, end) of each outage between the whole hours `first` and `last`,
    from events per hour across all sources; an hour missing from `hourly` had none."""
    hours: List[datetime] = []
    h = first
    while h <= last:
        hours.append(h)
        h += timedelta(hours=1)
    counts = sorted(float(hourly.get(x, 0.0)) for x in hours)
    if not counts:
        return []
    floor = OUTAGE_SHARE * counts[len(counts) // 2]
    out: List[Tuple[datetime, datetime]] = []
    start: Optional[datetime] = None
    for x in hours + [last + timedelta(hours=1)]:
        low = x <= last and float(hourly.get(x, 0.0)) <= floor
        if low and start is None:
            start = x
        elif not low and start is not None:
            if (x - start) >= timedelta(hours=OUTAGE_MIN_HOURS):
                out.append((start, x))
            start = None
    return out


def assess(rows: Sequence[Mapping[str, object]], now: datetime,
           retired: Mapping[str, str] = RETIRED) -> List[Dict[str, object]]:
    """Each (source, type) row -- last_seen, n_base (events over BASELINE_DAYS),
    n_24h, max_gap_h (longest gap in the baseline), min_day (quietest full day with
    events) and n_days (full days with events)
    -- with a status: ok, thin, silent or retired, and why."""
    out: List[Dict[str, object]] = []
    for r in rows:
        source, etype = str(r.get("source") or ""), str(r.get("type") or "")
        last: Optional[datetime] = r.get("last_seen")              # type: ignore[assignment]
        n_base = float(r.get("n_base") or 0)
        n_24h = float(r.get("n_24h") or 0)
        daily = n_base / BASELINE_DAYS
        hours = (now - last).total_seconds() / 3600 if last else None
        # The longest gap the source has had in its baseline, with room: a
        # market feed is quiet every weekend, and on its first Saturday this
        # flagged 28 sources silent against an average-gap rule.
        longest = float(r.get("max_gap_h") or 0)
        if longest > 0:
            allowed = min(MAX_GAP_H, max(MIN_GAP_H, GAP_MULTIPLE_OF_LONGEST * longest))
        else:
            allowed = min(MAX_GAP_H, max(MIN_GAP_H, GAP_MULTIPLE * 24.0 / daily)) if daily > 0 else MAX_GAP_H
        # A day with no events does not appear in the per-day counts, so a
        # source with any empty day in its baseline (a weekend) has a quietest
        # day of zero, and is never called thin for a quiet day.
        full_days = int(r.get("full_days") or BASELINE_DAYS - 1)
        quietest = float(r.get("min_day") or 0) if int(r.get("n_days") or full_days) >= full_days else 0.0
        row: Dict[str, object] = {"source": source, "type": etype, "per_day": round(daily, 1), "last_24h": int(n_24h),
                                  "hours_since": round(hours, 1) if hours is not None else None,
                                  "allowed_gap_h": round(allowed, 1)}
        reason = retired.get(etype) or retired.get(source)
        if reason:
            row.update(status="retired", why=reason)
        elif hours is None or hours > allowed:
            row.update(status="silent", why=f"nothing for {row['hours_since']} h; usual gap allows {row['allowed_gap_h']} h")
        elif daily >= THIN_MIN_DAILY and quietest > 0 and n_24h < THIN_SHARE * quietest:
            row.update(status="thin", why=f"{int(n_24h)} in 24 h; its quietest day in {BASELINE_DAYS} had {int(quietest)}")
        else:
            row.update(status="ok", why="")
        out.append(row)
    order = {"silent": 0, "thin": 1, "ok": 2, "retired": 3}
    return sorted(out, key=lambda x: (order[str(x["status"])], -float(x["per_day"])))  # type: ignore[arg-type]


__all__ = ["BASELINE_DAYS", "RETIRED", "assess", "outage_windows"]
