"""Replay of the registered alpaca_news score proposal on the stored events (reports/Upgrade pre-registrations - 12 Nov.md).

Each stored event carries its stock; the 6-hour article count up to the event and the count over the 20 days before that window
are re-read from `stock_news` (every article since 2011), so the replay does not wait for the counts newly stored on live events.
Proposal: 0.5 + 0.25 * min(1, max(0, z) / 4), z = (n - mu) / max(0.5, sqrt(mu)), mu the name's mean count per 6-hour window over
the 20 days before. Bars: >= 10 distinct values over 1,000 events; no count above a cut changes.
Read-only. Run inside an agents container:  python replay_news_score.py [--limit 5000]
"""
import argparse
import asyncio
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from shared.db import get_timescale  # noqa: E402
from shared.utils.score_resolution import ALERT_CUTS, counts_above_cut, distribution, news_score, news_verdicts, news_z  # noqa: E402

_EVENTS_SQL = """
    SELECT event_id::text AS id, upper(financial_data->>'ticker') AS ticker, occurred_at AS at, anomaly_score AS score,
           financial_data->>'news_n_6h' AS n6, financial_data->>'news_n_20d' AS n20s
    FROM events WHERE source = 'alpaca_news' AND anomaly_score IS NOT NULL AND financial_data ? 'ticker'
    ORDER BY occurred_at DESC LIMIT $1
"""
_COUNTS_SQL = """
    SELECT count(*) FILTER (WHERE created_at > $2::timestamptz - INTERVAL '6 hours' AND created_at <= $2::timestamptz) AS n6,
           count(*) FILTER (WHERE created_at <= $2::timestamptz - INTERVAL '6 hours') AS n20
    FROM stock_news WHERE symbols @> ARRAY[$1]::text[]
      AND created_at > $2::timestamptz - INTERVAL '20 days 6 hours' AND created_at <= $2::timestamptz
"""


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=5000)
    ap.add_argument("--span", type=float, default=0.25, help="score range above 0.5; 0.14 is the revised range (max 0.64)")
    ap.add_argument("--cuts", type=float, nargs="*", default=list(ALERT_CUTS))
    a = ap.parse_args()
    db = await get_timescale()
    events = await db.query(_EVENTS_SQL, a.limit) or []
    old, new, zs = [], [], []
    stored = failed = 0
    for e in events:
        if e["n6"] is not None and e["n20s"] is not None:
            stored += 1
            n6, n20 = int(e["n6"]), int(e["n20s"])
            z = news_z(n6, n20)
            zs.append(z)
            old.append(float(e["score"]))
            new.append(news_score(z, a.span))
            continue
        try:
            at = datetime.fromisoformat(e["at"].replace("Z", "+00:00")) if isinstance(e["at"], str) else e["at"]
            r = await db.query_one(_COUNTS_SQL, e["ticker"], at)
            n6, n20 = int(r["n6"]), int(r["n20"])
        except Exception as exc:
            failed += 1
            if failed <= 3:
                print(f"count failed for {e['id']}: {exc}")
            continue
        z = news_z(n6, n20)
        zs.append(z)
        old.append(float(e["score"]))
        new.append(news_score(z, a.span))
    print(f"stored alpaca_news events read: {len(events)}; replayed {len(old)}; count queries failed {failed}; "
          f"events replayed from stored counts: {stored} (the rest from stock_news)")
    if not old:
        return
    s = sorted(zs)
    print(f"z quantiles: p10 {s[len(s) // 10]:.2f}, p50 {s[len(s) // 2]:.2f}, p90 {s[9 * len(s) // 10]:.2f}, max {s[-1]:.2f}")
    print("old score distribution:", distribution(old, (0.5, 0.6, 0.65, 0.7)))
    print("new score distribution:", distribution(new, (0.5, 0.6, 0.65, 0.7)))
    print("counts at or above each cut (old, new):", counts_above_cut(old, new, a.cuts))
    print("\nRegistered bars:")
    for bar, value, ok in news_verdicts(old, new, cuts=a.cuts):
        print(f"  {'PASS' if ok else 'FAIL'}  {bar}: {value}")


if __name__ == "__main__":
    asyncio.run(main())
