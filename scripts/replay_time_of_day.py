"""Replay of the registered phase 3 test on stored market-anomaly events (shadow; nothing is written).

For each event, volume of its 5-minute bar is scored three ways against the previous days' bars for the same ticker:
  live   the stored anomaly score,
  flat   robust z of volume against ALL bars of those days (no time of day),
  slot   robust z of volume against bars from the SAME session bucket of those days (median and MAD, the current bar excluded).
Bars: (1) parity across the five regular-session buckets (share of events in the top 5% per bucket, against the five-bucket mean), (2) the
share of top-5% events followed within 30 minutes by a move of at least 2 typical bar ranges, against the live score's.
Run inside a service container: python /tmp/rtd.py"""

import asyncio
import bisect
import collections
import statistics
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from shared.db import get_timescale

ET = ZoneInfo("America/New_York")
BUCKETS = [("before the open", 0, 570), ("opening 09:30-10:00", 570, 600), ("morning 10:00-11:30", 600, 690), ("midday 11:30-14:00", 690, 840),
           ("afternoon 14:00-15:30", 840, 930), ("closing 15:30-16:00", 930, 960), ("after the close", 960, 1440)]
RTH = [b[0] for b in BUCKETS[1:6]]
LOOKBACK_DAYS = 14
MIN_BASE = 30


def bucket_of(dt_et: datetime) -> str:
    minute = dt_et.hour * 60 + dt_et.minute
    for name, lo, hi in BUCKETS:
        if lo <= minute < hi:
            return name
    return BUCKETS[-1][0]


def robust_z(x: float, base: list) -> float | None:
    if len(base) < MIN_BASE:
        return None
    med = statistics.median(base)
    mad = statistics.median(abs(v - med) for v in base)
    scale = 1.4826 * max(mad, 0.05 * med, 1.0)
    return (x - med) / scale


async def main() -> None:
    db = await get_timescale()
    ev = await db.query(
        "SELECT primary_entity_id AS t, created_at, anomaly_score FROM events WHERE type = 'market_anomaly' AND anomaly_score IS NOT NULL "
        "AND created_at > NOW() - INTERVAL '14 days' AND primary_entity_id IS NOT NULL")
    tickers = sorted({r["t"] for r in ev or []})
    print("events", len(ev or []), "| tickers", len(tickers))
    bars_by = {}
    for r in await db.query(
            "SELECT ticker, bucket_time, high, low, close, volume FROM tradfi_bars_5m WHERE ticker = ANY($1::text[]) AND bucket_time > NOW() - INTERVAL '30 days' "
            "AND volume IS NOT NULL AND close > 0 ORDER BY ticker, bucket_time", tickers) or []:
        ts = r["bucket_time"] if isinstance(r["bucket_time"], datetime) else datetime.fromisoformat(str(r["bucket_time"]).replace("Z", "+00:00"))
        bars_by.setdefault(r["ticker"], []).append((ts, float(r["high"] or r["close"]), float(r["low"] or r["close"]), float(r["close"]), float(r["volume"])))
    rows = []
    for r in ev or []:
        bars = bars_by.get(r["t"])
        if not bars:
            continue
        ts = r["created_at"] if isinstance(r["created_at"], datetime) else datetime.fromisoformat(str(r["created_at"]).replace("Z", "+00:00"))
        times = [b[0] for b in bars]
        i = bisect.bisect_right(times, ts) - 1
        if i < 15 or i + 6 >= len(bars) or ts - bars[i][0] > timedelta(minutes=10):
            continue
        et = ts.astimezone(ET)
        if et.weekday() >= 5:
            continue
        day = et.date()
        slot = bucket_of(bars[i][0].astimezone(ET))
        flat, same = [], []
        for ts2, _h, _l, _c, v in bars:
            et2 = ts2.astimezone(ET)
            if et2.date() >= day or (day - et2.date()).days > LOOKBACK_DAYS:
                continue
            flat.append(v)
            if bucket_of(et2) == slot:
                same.append(v)
        z_flat, z_slot = robust_z(bars[i][4], flat), robust_z(bars[i][4], same)
        rng = statistics.mean((b[1] - b[2]) / b[3] for b in bars[i - 14:i])
        hit = abs(bars[i + 6][3] / bars[i][3] - 1.0) >= 2.0 * rng
        if z_flat is None or z_slot is None:
            continue
        rows.append({"slot": slot, "live": float(r["anomaly_score"]), "flat": z_flat, "slot_z": z_slot, "hit": hit})
    print("events scored in all three ways:", len(rows), "| overall 30-minute large-move rate", f"{sum(x['hit'] for x in rows) / max(1, len(rows)):.3f}")
    if len(rows) < 200:
        print("too few events to judge")
        return
    cuts = {}
    for key in ("live", "flat", "slot_z"):
        vals = sorted(x[key] for x in rows)
        cuts[key] = vals[int(0.95 * len(vals))]
    print("per bucket: events | share in the top 5% by live / flat / slot")
    per = collections.defaultdict(lambda: collections.Counter())
    for x in rows:
        per[x["slot"]]["n"] += 1
        for key in ("live", "flat", "slot_z"):
            per[x["slot"]][key] += x[key] >= cuts[key]
    mean_share = {}
    for key in ("live", "flat", "slot_z"):
        shares = [per[b][key] / per[b]["n"] for b in RTH if per[b]["n"] >= 20]
        mean_share[key] = sum(shares) / len(shares) if shares else 0.0
    for name, _, _ in BUCKETS:
        c = per[name]
        if c["n"]:
            print(f"  {name:24s} {c['n']:6d}  " + "  ".join(f"{c[k] / c['n']:.3f}" for k in ("live", "flat", "slot_z")))
    for key in ("live", "flat", "slot_z"):
        ratios = [(per[b][key] / per[b]["n"]) / mean_share[key] for b in RTH if per[b]["n"] >= 20 and mean_share[key] > 0]
        inside = all(0.5 <= r <= 2.0 for r in ratios)
        print(f"parity bar ({key}): RTH bucket ratios to the mean {['%.2f' % r for r in ratios]} -> {'PASS' if inside else 'FAIL'}")
    for key in ("live", "flat", "slot_z"):
        top = [x for x in rows if x[key] >= cuts[key]]
        print(f"large-move rate in the top 5% by {key}: {sum(x['hit'] for x in top) / max(1, len(top)):.3f} (n={len(top)})")


asyncio.run(main())
