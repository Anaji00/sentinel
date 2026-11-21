"""Registered 18 Nov 2026 (before the run): what the trader's daily rebalance-to-target costs against a drifting book and a no-trade band.
Read-only, mechanical, no orders. Random dollar-neutral baskets stand in for B1's picks, so selection and alpha play no part: only the
rule for trading between the 21-session rebuilds differs.

Universe: the 400 most dollar-traded names among the model's ~1,000 scored names with a full 253-session history and a last price of at least $5. 300 baskets (seed 20261118), each
89 longs and 95 shorts drawn at random, each leg 0.5 of NAV, equal weight. Rebuild (reset to equal weight) every 21 sessions in every rule.
Rules: R0 drift between rebuilds; R1 daily rebalance to target (what auto_trader.py does); R2 and R3 daily band, trading a name back to target
only when |weight - target| exceeds 25% (R2) or 50% (R3) of its target. Cost 10 bps a side on traded notional; borrow and interest ignored
(identical across rules to first order).
Reading, fixed now: R2 or R3 is worth proposing if its mean net return per 21 sessions beats R1 by at least 5 bps, it beats R1 in at least 90%
of baskets, and its gross (pre-cost) return is no more than 3 bps per 21 sessions below R1's. Descriptive: one 252-session path, so the
t-statistic over 12 non-overlapping blocks is shown but is not a bar.
Run: MSYS_NO_PATHCONV=1 docker exec -i -w /app sentinel-api_gateway python - < scripts/research_quant_drift_band.py
"""
import asyncio
import json
import sys
from collections import defaultdict

import numpy as np

sys.path.insert(0, "/app")
from shared.db import get_timescale  # noqa: E402

COST = 10e-4
NL, NS, SESS, REB = 89, 95, 252, 21


def simulate(R: np.ndarray, longs: np.ndarray, shorts: np.ndarray, band) -> dict:
    """band: None = drift, 0.0 = daily to target, x = trade a name only if |w - t| > x * |t|. Returns per-block net and gross returns."""
    idx = np.concatenate([longs, shorts])
    tgt = np.concatenate([np.full(NL, 0.5 / NL), np.full(NS, -0.5 / NS)])
    T = R.shape[0]
    v, c = tgt.copy(), 1.0
    nav0 = 1.0
    nav_net, nav_gross, traded = [nav0], [nav0], 0.0
    cost_paid = 0.0
    for d in range(T):
        v = v * (1.0 + R[d, idx])
        nav = c + v.sum()
        if (d + 1) % REB == 0:
            trade = tgt * nav - v
        elif band is None:
            trade = None
        else:
            w = v / nav
            off = np.abs(w - tgt) > band * np.abs(tgt) if band > 0 else np.ones_like(w, dtype=bool)
            trade = np.where(off, tgt * nav - v, 0.0)
        if trade is not None:
            amt = np.abs(trade).sum()
            fee = COST * amt
            c -= trade.sum() + fee
            v = v + trade
            traded += amt
            cost_paid += fee
        nav_net.append(c + v.sum())
        nav_gross.append(c + v.sum() + cost_paid)
    nn, ng = np.array(nav_net), np.array(nav_gross)
    blocks = np.arange(0, T + 1, REB)
    net = nn[blocks[1:]] / nn[blocks[:-1]] - 1.0
    gross = ng[blocks[1:]] / ng[blocks[:-1]] - 1.0
    return {"net": net, "gross": gross, "turnover_per_block": traded / (T / REB)}


async def main() -> None:
    db = await get_timescale()
    from shared.db import get_redis
    from shared.utils.signal_store import factor_scores_key
    redis = await get_redis()
    scores = json.loads(await getattr(redis, "raw", redis).get(factor_scores_key("monthly")) or "{}")
    names = sorted(str(t).upper() for t in scores) + ["SPY"]
    rows = await db.query("SELECT ticker, day, close, volume FROM daily_bars WHERE ticker = ANY($1::text[]) AND day > CURRENT_DATE - 420 AND close > 0 ORDER BY ticker, day", names) or []
    px, dv = defaultdict(dict), defaultdict(float)
    for r in rows:
        px[r["ticker"]][r["day"]] = float(r["close"])
        dv[r["ticker"]] += float(r["close"]) * float(r["volume"] or 0)
    spy_days = sorted(d for d in px["SPY"])[-(SESS + 1):]
    ok = [t for t in px if all(d in px[t] for d in spy_days) and px[t][spy_days[-1]] >= 5]
    ok = sorted(ok, key=lambda t: -dv[t])[:400]
    P = np.array([[px[t][d] for t in ok] for d in spy_days])
    R = P[1:] / P[:-1] - 1.0
    R = np.clip(R, -0.5, 1.0)
    rng = np.random.default_rng(20261118)
    rules = {"R0_drift": None, "R1_daily": 0.0, "R2_band25": 0.25, "R3_band50": 0.5}
    acc = {k: {"net": [], "gross": [], "turn": []} for k in rules}
    for _ in range(300):
        pick = rng.permutation(len(ok))[: NL + NS]
        longs, shorts = pick[:NL], pick[NL:]
        for k, b in rules.items():
            s = simulate(R, longs, shorts, b)
            acc[k]["net"].append(s["net"])
            acc[k]["gross"].append(s["gross"])
            acc[k]["turn"].append(s["turnover_per_block"])
    out = {"universe": len(ok), "sessions": R.shape[0], "from": str(spy_days[0]), "to": str(spy_days[-1])}
    base = np.array(acc["R1_daily"]["net"])
    for k in rules:
        n, g = np.array(acc[k]["net"]), np.array(acc[k]["gross"])
        d = n - base
        blockmean = d.mean(axis=0)
        t = blockmean.mean() / (blockmean.std(ddof=1) / np.sqrt(len(blockmean))) if blockmean.std(ddof=1) > 0 else None
        out[k] = {"net_bps_per_21": round(float(n.mean() * 1e4), 1), "gross_bps_per_21": round(float(g.mean() * 1e4), 1),
                  "turnover_x_nav_per_21": round(float(np.mean(acc[k]["turn"])), 2),
                  "net_minus_R1_bps": round(float(d.mean() * 1e4), 1), "gross_minus_R1_bps": round(float((g - np.array(acc["R1_daily"]["gross"])).mean() * 1e4), 1),
                  "baskets_beating_R1": round(float((d.mean(axis=1) > 0).mean()), 3), "t_over_blocks_vs_R1": None if t is None else round(float(t), 2)}
    print(json.dumps(out, indent=1))


asyncio.run(main())
