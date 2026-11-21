"""Shadow measurement: what would retiring the generic macro fan-out rules do (reports/Upgrade pre-registrations - 12 Nov.md).

Read-only. From the stored `correlations` and `scenarios` tables for the last --days days (default 30; run it with 14 as well):
tier-1 correlation count with and without the rules, the share of CONFIRMED scenarios whose correlation came from them, and the
5-session abnormal-move ratio (shared/utils/producer_grades.py move_ratio, the producer grader's own measure) of the alerts that
would remain against all alerts. Prints PASS/FAIL per registered bar. Run inside an agents container:
    python shadow_macro_fanout.py [--days 30]
"""
import argparse
import asyncio
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from shared.db import get_timescale  # noqa: E402
from shared.utils import producer_grades as pg  # noqa: E402
from shared.utils.macro_fanout import fanout_verdicts, is_generic_fanout_rule, mean_or_none  # noqa: E402

TICKER_RE = "^[A-Z]{1,5}([.][A-Z])?$"
K = 5
# One row per rule, stock and day, as the producer grader reads them.
_CORR_SQL = f"""
    SELECT rule_name, primary_entity_id AS ticker, min(detected_at) AS at, min(alert_tier) AS tier FROM correlations
    WHERE detected_at > NOW() - make_interval(days => $1) AND primary_entity_id ~ '{TICKER_RE}'
      AND rule_id IS DISTINCT FROM 'USER_SCENARIO_REQUEST'
    GROUP BY 1, 2, date(detected_at)
"""
_COUNT_SQL = """
    SELECT rule_name, alert_tier AS tier, count(*) AS n FROM correlations
    WHERE detected_at > NOW() - make_interval(days => $1) GROUP BY 1, 2
"""
_SCEN_SQL = """
    SELECT s.status, c.rule_name FROM scenarios s JOIN correlations c ON c.correlation_id = s.correlation_id
    WHERE s.created_at > NOW() - make_interval(days => $1)
"""
_BARS_SQL = """
    SELECT ticker, day, open, close FROM daily_bars WHERE ticker = ANY($1::text[]) AND day >= $2::date ORDER BY ticker, day
"""


def ts(x):
    return datetime.fromisoformat(x.replace("Z", "+00:00")) if isinstance(x, str) else x


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=30)
    a = ap.parse_args()
    db = await get_timescale()

    counts = await db.query(_COUNT_SQL, a.days) or []
    by_rule = defaultdict(int)
    t1_total = t1_retired = 0
    for r in counts:
        retired = is_generic_fanout_rule(r["rule_name"])
        if int(r["tier"] or 0) == 1:
            t1_total += int(r["n"])
            if retired:
                t1_retired += int(r["n"])
        if retired:
            by_rule[(r["rule_name"], int(r["tier"] or 0))] += int(r["n"])
    print(f"window: last {a.days} days")
    print("retired-rule correlations by rule and tier:")
    for (name, tier), n in sorted(by_rule.items(), key=lambda kv: -kv[1]):
        print(f"  tier {tier}: {n:>6}  {name}")

    scen = await db.query(_SCEN_SQL, a.days) or []
    conf = [r for r in scen if str(r["status"]).lower() == "confirmed"]
    lost = [r for r in conf if is_generic_fanout_rule(r["rule_name"])]
    print(f"scenarios linked to a correlation: {len(scen)}; confirmed: {len(conf)}; confirmed from the retired rules: {len(lost)}")

    rows = await db.query(_CORR_SQL, a.days) or []
    tickers = sorted({str(r["ticker"]).upper() for r in rows} | {"SPY"})
    since = date.today() - timedelta(days=a.days + 160)
    bars = defaultdict(lambda: ([], [], []))
    for b in await db.query(_BARS_SQL, tickers, since) or []:
        d, o, c = bars[str(b["ticker"]).upper()]
        d.append(b["day"])
        o.append(float(b["open"] or b["close"]))
        c.append(float(b["close"]))
    sd, so, sc = bars.get("SPY", ([], [], []))
    spy = {d: (o, c) for d, o, c in zip(sd, so, sc)}
    all_r, kept_r, gone_r = [], [], []
    ungraded = 0
    for r in rows:
        if int(r["tier"] or 0) != 1:
            continue
        d, o, c = bars.get(str(r["ticker"]).upper(), ([], [], []))
        m = pg.move_ratio(d, o, c, spy, ts(r["at"]), K) if d else None
        if m is None:
            ungraded += 1
            continue
        all_r.append(m[0])
        (gone_r if is_generic_fanout_rule(r["rule_name"]) else kept_r).append(m[0])
    print(f"tier-1 rule-stock-days with a {K}-session move ratio: {len(all_r)} (ungraded: {ungraded}); "
          f"retired rules {len(gone_r)}, remaining {len(kept_r)}")
    print(f"mean {K}-session move ratio: all {mean_or_none(all_r)}, remaining {mean_or_none(kept_r)}, retired {mean_or_none(gone_r)}")

    print("\nRegistered bars:")
    for bar, value, ok in fanout_verdicts(t1_total, t1_retired, len(conf), len(lost), mean_or_none(all_r), mean_or_none(kept_r)):
        print(f"  {'PASS' if ok else 'FAIL'}  {bar}: {value}")


if __name__ == "__main__":
    asyncio.run(main())
