"""Shadow replay of customer-to-supplier contagion (registered phase 3 test). Read-only.

For every session, a customer whose excess return (against SPY) is at least SHOCK in size is a shock; it propagates to its suppliers
through customer_links (the supplier's revenue share, only links filed before that session) with damping; a supplier that has moved under a
quarter of its expected shock that day is a second-order candidate. Outcome: the supplier's excess return over the next 5 sessions, in the
shock's direction. Registered bars: at least 100 episodes and sign agreement of at least 55%.
Run inside a service container: python /tmp/sc.py"""

import asyncio
import collections
import statistics

from shared.db import get_timescale
from shared.utils import contagion as cg

SHOCK = 0.04
MIN_EXPECTED = 0.003
FWD = 5


async def main() -> None:
    db = await get_timescale()
    links = await db.query("SELECT supplier, customer_ticker, pct, filed FROM customer_links WHERE customer_ticker IS NOT NULL AND pct IS NOT NULL AND pct > 0")
    tickers = sorted({r["supplier"] for r in links} | {r["customer_ticker"] for r in links} | {"SPY"})
    rows = await db.query("SELECT ticker, day, close FROM daily_bars WHERE ticker = ANY($1::text[]) AND day > CURRENT_DATE - 900 AND close > 0 ORDER BY day", tickers)
    px = collections.defaultdict(dict)
    for r in rows or []:
        px[r["ticker"]][r["day"]] = float(r["close"])
    days = sorted(px["SPY"])
    idx = {d: i for i, d in enumerate(days)}

    def ret(t, i):
        a, b = px[t].get(days[i - 1]), px[t].get(days[i])
        return (b / a - 1.0) if a and b else None

    def fwd(t, i):
        a, b = px[t].get(days[i]), px[t].get(days[i + FWD])
        s0, s1 = px["SPY"].get(days[i]), px["SPY"].get(days[i + FWD])
        return ((b / a - 1.0) - (s1 / s0 - 1.0)) if a and b and s0 and s1 else None

    by_customer = collections.defaultdict(list)
    for r in links:
        by_customer[r["customer_ticker"]].append((r["supplier"], float(r["pct"]) / 100.0, r["filed"]))
    print("links", len(links), "| customers", len(by_customer), "| suppliers", len({r['supplier'] for r in links}), "| sessions", len(days))
    episodes = []
    for i in range(1, len(days) - FWD):
        d = days[i]
        shocks = {}
        for c, outs in by_customer.items():
            rc, rs = ret(c, i), ret("SPY", i)
            if rc is None or rs is None:
                continue
            if abs(rc - rs) >= SHOCK and any(f < d for _, _, f in outs):
                shocks[c] = rc - rs
        if not shocks:
            continue
        edges = collections.defaultdict(dict)
        for c in shocks:
            for sup, w, filed in by_customer[c]:
                if filed < d:
                    edges[c][sup] = max(edges[c].get(sup, 0.0), min(1.0, w))
        prop = cg.propagate(edges, shocks, gamma=0.5, hops=2)
        own = {s: (ret(s, i) or 0.0) - (ret("SPY", i) or 0.0) for s in prop}
        for row in cg.unadjusted(prop, own, MIN_EXPECTED):
            f = fwd(row["ticker"], i)
            if f is None:
                continue
            sign = 1.0 if float(row["expected_shock"]) > 0 else -1.0
            episodes.append({"day": d, "t": row["ticker"], "expected": float(row["expected_shock"]), "aligned": f * sign, "hops": row["hops"]})
    n = len(episodes)
    print("episodes:", n)
    if n < 20:
        return
    agree = sum(1 for e in episodes if e["aligned"] > 0) / n
    print(f"sign agreement over {FWD} sessions: {agree:.1%} | mean aligned excess return {statistics.mean(e['aligned'] for e in episodes) * 1e4:+.0f} bps | median {statistics.median(e['aligned'] for e in episodes) * 1e4:+.0f} bps")
    half = sorted(episodes, key=lambda e: e["day"])
    for name, part in (("first half", half[: n // 2]), ("second half", half[n // 2:])):
        print(f"  {name}: n={len(part)} agreement {sum(1 for e in part if e['aligned'] > 0) / len(part):.1%} mean {statistics.mean(e['aligned'] for e in part) * 1e4:+.0f} bps")
    one = [e for e in episodes if e["hops"] == 1]
    two = [e for e in episodes if e["hops"] > 1]
    print(f"  1-hop n={len(one)} agreement {sum(1 for e in one if e['aligned'] > 0) / max(1, len(one)):.1%} | 2-hop n={len(two)} agreement {sum(1 for e in two if e['aligned'] > 0) / max(1, len(two)):.1%}")
    neg = [e for e in episodes if e["expected"] < 0]
    pos = [e for e in episodes if e["expected"] > 0]
    print(f"  after customer drops n={len(neg)} agreement {sum(1 for e in neg if e['aligned'] > 0) / max(1, len(neg)):.1%} | after rises n={len(pos)} agreement {sum(1 for e in pos if e['aligned'] > 0) / max(1, len(pos)):.1%}")
    # distinct supplier-days: the same supplier fires on several customers; count independent supplier-weeks
    weeks = {(e["t"], e["day"].isocalendar()[:2]) for e in episodes}
    print("independent supplier-weeks:", len(weeks))
    print("bars: at least 100 episodes and 55% agreement ->", "PASS" if n >= 100 and agree >= 0.55 else "NOT MET")


asyncio.run(main())
