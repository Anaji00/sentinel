"""Read-only diagnostic of the stored B1 book and its executions (no orders, no writes).

Reports: (1) the stored book's size, age and ex-ante beta / 63-day volatility by leg, from daily_bars; (2) the short leg's quoted borrow fees
(borrow_rates, latest day) against the flat 44 bps a month the trials charge; (3) the daily "drift" trades (reason add / trim) the trader sends
against the book's own targets: count, size, share of NAV, and their cost at the observed slippage; (4) the live equity record's daily volatility
and worst day. Output is JSON on stdout.
Run: MSYS_NO_PATHCONV=1 docker exec -i -w /app sentinel-api_gateway python - < scripts/research_quant_live_book.py
"""
import asyncio
import json
import math
import statistics
import sys
from collections import defaultdict

sys.path.insert(0, "/app")
from shared.db import get_redis, get_timescale  # noqa: E402
from shared.utils.signal_store import AUTO_TRADE_BOOK_KEY  # noqa: E402


def beta_vol(rets, spy):
    xs = [(rets[d], spy[d]) for d in rets if d in spy]
    if len(xs) < 60:
        return None, None
    a, m = [x[0] for x in xs], [x[1] for x in xs]
    ma, mm = sum(a) / len(a), sum(m) / len(m)
    var = sum((x - mm) ** 2 for x in m)
    beta = sum((x - ma) * (y - mm) for x, y in zip(a, m)) / var if var else None
    vol = statistics.pstdev(a[-63:]) * math.sqrt(252)
    return beta, vol


async def main() -> None:
    db, redis = await get_timescale(), await get_redis()
    raw = getattr(redis, "raw", redis)
    book = json.loads(await raw.get(AUTO_TRADE_BOOK_KEY) or "{}")
    longs, shorts = book.get("long", []), book.get("short", [])
    out = {"rebalanced_on": book.get("rebalanced_on"), "n_long": len(longs), "n_short": len(shorts), "scores_as_of": book.get("scores_as_of")}
    names = sorted(set(longs) | set(shorts) | {"SPY"})
    bars = defaultdict(dict)
    for r in await db.query("SELECT ticker, day, close FROM daily_bars WHERE ticker = ANY($1::text[]) AND day > CURRENT_DATE - 260 AND close > 0 ORDER BY ticker, day", names) or []:
        bars[r["ticker"]][r["day"]] = float(r["close"])

    def daily(t):
        ds = sorted(bars[t])
        return {d: bars[t][d] / bars[t][p] - 1 for p, d in zip(ds, ds[1:])}
    spy = daily("SPY")
    leg = {}
    for side, ts in (("long", longs), ("short", shorts)):
        bs, vs = [], []
        for t in ts:
            if t in bars and len(bars[t]) > 80:
                b, v = beta_vol(daily(t), spy)
                if b is not None:
                    bs.append(b)
                    vs.append(v)
        leg[side] = {"n": len(bs), "mean_beta": round(sum(bs) / len(bs), 3) if bs else None,
                     "median_vol": round(statistics.median(vs), 3) if vs else None,
                     "mean_vol": round(sum(vs) / len(vs), 3) if vs else None}
    if leg["long"]["mean_beta"] is not None and leg["short"]["mean_beta"] is not None:
        leg["net_beta_at_half_each"] = round(0.5 * (leg["long"]["mean_beta"] - leg["short"]["mean_beta"]), 3)
    out["legs"] = leg

    fees = await db.query("SELECT ticker, fee_rate, available FROM borrow_rates WHERE day = (SELECT max(day) FROM borrow_rates) AND ticker = ANY($1::text[])", shorts) or []
    fr = [float(r["fee_rate"]) for r in fees if r["fee_rate"] is not None]
    out["short_fees_pct_annual"] = {"quoted": len(fr), "of": len(shorts), "mean": round(sum(fr) / len(fr), 2) if fr else None,
                                    "median": round(statistics.median(fr), 2) if fr else None, "p90": round(sorted(fr)[int(0.9 * len(fr))], 2) if fr else None,
                                    "trial_flat_equiv_annual_pct": round(0.44 * 12, 2),
                                    "above_5pct": sum(1 for x in fr if x > 5)}

    rows = await db.query("""SELECT day, reason, side, filled_qty, fill_price, ref_price FROM auto_trade_orders
        WHERE filled_qty > 0 AND fill_price > 0 AND day >= '2026-11-05'""") or []
    eq = {r["day"]: float(r["equity"]) for r in await db.query("SELECT day, equity FROM auto_trade_equity WHERE portfolio = 'b1_extremes'") or []}
    per = defaultdict(lambda: [0.0, 0.0, 0.0])
    for r in rows:
        if r["reason"] not in ("add", "trim"):
            continue
        usd = float(r["filled_qty"]) * float(r["fill_price"])
        slip = 0.0
        if r["ref_price"]:
            ref = float(r["ref_price"])
            slip = (float(r["fill_price"]) / ref - 1) * 1e4 if r["side"] == "buy" else (1 - float(r["fill_price"]) / ref) * 1e4
        per[r["day"]][0] += usd
        per[r["day"]][1] += usd * slip / 1e4
        per[r["day"]][2] += 1
    days = sorted(per)
    nav = sum(eq.values()) / len(eq) if eq else None
    out["drift_trades"] = {"days": len(days), "mean_usd_per_day": round(sum(per[d][0] for d in days) / len(days), 0) if days else None,
                           "mean_share_of_nav_per_day": round(sum(per[d][0] for d in days) / len(days) / nav, 4) if days and nav else None,
                           "mean_orders_per_day": round(sum(per[d][2] for d in days) / len(days), 1) if days else None,
                           "slippage_usd_total": round(sum(per[d][1] for d in days), 1)}
    if out["drift_trades"]["mean_share_of_nav_per_day"]:
        s = out["drift_trades"]["mean_share_of_nav_per_day"]
        out["drift_trades"]["extra_turnover_per_21_sessions_x_nav"] = round(21 * s, 2)
        out["drift_trades"]["cost_bps_of_nav_per_21_sessions_at_10bps_a_side"] = round(21 * s * 10, 1)

    ds = sorted(eq)
    rets = [eq[b] / eq[a] - 1 for a, b in zip(ds, ds[1:])]
    out["equity_record"] = {"sessions": len(ds), "daily_mean_pct": round(100 * sum(rets) / len(rets), 3) if rets else None,
                            "daily_sd_pct": round(100 * statistics.pstdev(rets), 3) if len(rets) > 2 else None,
                            "worst_day_pct": round(100 * min(rets), 2) if rets else None,
                            "since_first_pct": round(100 * (eq[ds[-1]] / eq[ds[0]] - 1), 2) if ds else None}
    print(json.dumps(out, indent=1, default=str))


asyncio.run(main())
