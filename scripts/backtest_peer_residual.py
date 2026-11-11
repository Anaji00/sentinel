"""Backtest of the registered multi-horizon peer-residual rule P1 (reports/Hardening audit - twelve-layer brief.md).

Counts, per New York session, how many of the most liquid stocks the rule flags, horizons merged, using only data
before each moment (betas and sigmas from earlier sessions). Reports the rate against the registered bars and
whether NVDA fires on 9 Nov 2026. Read-only. Run in a capped one-off container, never inside agents-heavy.
"""
import asyncio
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from shared.db import get_timescale  # noqa: E402
from shared.utils import peer_residual as pr  # noqa: E402

UNIVERSE = 500
SESSIONS = 85
WARM = 20                      # sessions of minute history before a stock-session can be judged
TOD = 26                       # fifteen-minute buckets in the regular session
MIN_MEMBERS = 3
MIN_GROUP = 5

LIQUID = "SELECT ticker FROM daily_bars WHERE day >= CURRENT_DATE - 30 AND close > 0 GROUP BY ticker HAVING avg(close*volume) > 2e7 ORDER BY avg(close*volume) DESC LIMIT $1"
DAILY = "SELECT ticker, day, close FROM daily_bars WHERE ticker = ANY($1::text[]) AND day >= CURRENT_DATE - 220 AND close > 0 ORDER BY day"
BARS = """
    SELECT ticker, time_bucket('15 minutes', time, 'America/New_York') AS b,
           first(open, time) AS o, last(close, time) AS c, sum(volume) AS v
    FROM tradfi_bars
    WHERE ticker = ANY($1::text[]) AND time >= now() - interval '125 days' AND close > 0
      AND (time AT TIME ZONE 'America/New_York')::time >= '09:30' AND (time AT TIME ZONE 'America/New_York')::time < '16:00'
    GROUP BY 1, 2
"""
PEERS = "SELECT ticker, peer, corr FROM sympathy_peers"
SIC = "SELECT ticker, sic FROM company_profile WHERE sic IS NOT NULL"
HORIZONS_ON = (sys.argv[1] if len(sys.argv) > 1 else "15m,1h,session,5d").split(",")      # a subset is a decision, not a new threshold


def prior_stat(a: np.ndarray, fn, win: int) -> np.ndarray:
    """For a (days, tod) array: fn over the previous `win` days at each time of day, never including the day itself."""
    out = np.full(a.shape, np.nan)
    for d in range(win, a.shape[0]):
        out[d] = fn(a[d - win:d], axis=0)
    return out


async def main() -> None:
    db = await get_timescale()
    tickers = [r["ticker"] for r in await db.query(LIQUID, UNIVERSE)]
    col = {t: i for i, t in enumerate(tickers)}
    daily = pd.DataFrame(await db.query(DAILY, tickers)).pivot(index="day", columns="ticker", values="close").sort_index()
    daily = daily.reindex(columns=tickers)
    parts = []
    for i in range(0, len(tickers), 40):                 # one query for all 500 exceeds the client timeout
        parts.append(pd.DataFrame(await db.query(BARS, tickers[i:i + 40])))
    bars = pd.concat([p for p in parts if len(p)], ignore_index=True)
    print("tickers", len(tickers), "bucket rows", len(bars), flush=True)

    bars["b"] = pd.to_datetime(bars["b"], utc=True)
    local = bars["b"].dt.tz_convert("America/New_York")
    bars["day"] = local.dt.date
    bars["slot"] = ((local.dt.hour * 60 + local.dt.minute) - 570) // 15
    days = sorted(bars["day"].unique())[-SESSIONS:]
    di = {d: i for i, d in enumerate(days)}
    bars = bars[bars["day"].isin(di) & (bars["slot"] >= 0) & (bars["slot"] < TOD)]
    shape = (len(days), TOD, len(tickers))
    O, C, V = (np.full(shape, np.nan) for _ in range(3))
    ix = (bars["day"].map(di).to_numpy(), bars["slot"].to_numpy(), bars["ticker"].map(col).to_numpy())
    O[ix], C[ix], V[ix] = bars["o"].astype(float).to_numpy(), bars["c"].astype(float).to_numpy(), bars["v"].astype(float).to_numpy()
    last_close = np.array([np.nanmax(np.where(np.isfinite(C[d]), np.arange(TOD)[:, None], -1), axis=0) for d in range(len(days))])
    close_day = np.array([[C[d, int(last_close[d, j]), j] if last_close[d, j] >= 0 else np.nan for j in range(len(tickers))] for d in range(len(days))])
    prev_close = np.vstack([np.full((1, len(tickers)), np.nan), close_day[:-1]])

    # horizon returns, shape (days, tod, tickers)
    r15 = C / O - 1.0
    r1h = np.full(shape, np.nan)
    r1h[:, 4:] = C[:, 4:] / C[:, :-4] - 1.0
    rsess = C / prev_close[:, None, :] - 1.0
    cumv = np.nancumsum(np.where(np.isfinite(V), V, 0.0), axis=1)
    v1h = np.full(shape, np.nan)
    v1h[:, 3:] = sum(np.where(np.isfinite(V[:, 3 - k:TOD - k]), V[:, 3 - k:TOD - k], 0.0) for k in range(4))
    vol = {"15m": V, "1h": v1h, "session": cumv}
    ret = {"15m": r15, "1h": r1h, "session": rsess}

    # references
    peers = {}
    for r in sorted(await db.query(PEERS), key=lambda x: -float(x["corr"])):
        if r["ticker"] in col and r["peer"] in col and len(peers.setdefault(r["ticker"], [])) < 8:
            peers[r["ticker"]].append(r["peer"])
    sic = {str(r["ticker"]): int(r["sic"]) for r in await db.query(SIC)}
    by_sic = {}
    for t in tickers:
        if t in sic:
            by_sic.setdefault(sic[t], []).append(t)
    refs = {t: {k: v for k, v in (("sympathy", peers.get(t, [])),
                                  ("industry", [x for x in by_sic.get(sic.get(t), []) if x != t] if len(by_sic.get(sic.get(t), [])) - 1 >= MIN_GROUP else []))
                if len(v) >= MIN_MEMBERS} for t in tickers}

    dret = daily.pct_change()
    dates = [d for d in dret.index]
    flagged = np.zeros((len(days), len(tickers)), dtype=bool)
    by_horizon = {h: np.zeros((len(days), len(tickers)), dtype=bool) for h in pr.HORIZONS}
    nvda_note = []

    for t, rs in refs.items():
        j = col[t]
        for name, members in rs.items():
            mj = [col[m] for m in members]
            for h in [x for x in ("15m", "1h", "session") if x in HORIZONS_ON]:
                R = ret[h]
                with np.errstate(all="ignore"):
                    ref = np.nanmean(R[:, :, mj], axis=2)
                    nmem = np.isfinite(R[:, :, mj]).sum(axis=2)
                    opp = ((R[:, :, mj] > 0) != (R[:, :, [j]] > 0)) & (R[:, :, mj] != 0) & np.isfinite(R[:, :, mj])
                    breadth = opp.sum(axis=2) / np.where(nmem > 0, nmem, np.nan)
                ref = np.where(nmem >= MIN_MEMBERS, ref, np.nan)
                # beta from the 60 daily sessions before each day
                dref = dret[members].mean(axis=1)
                sd = dret[t]
                cov = sd.rolling(pr.BETA_SESSIONS).cov(dref).shift(1)
                var = dref.rolling(pr.BETA_SESSIONS).var().shift(1)
                bser = (cov / var)
                bmap = {pd.Timestamp(k).date(): v for k, v in bser.items()}
                b = np.array([bmap.get(d, np.nan) for d in days])
                resid = R[:, :, j] - b[:, None] * ref
                sigma = prior_stat(resid, np.nanstd, pr.SIGMA_SESSIONS_INTRADAY)
                atr = prior_stat(np.abs(R[:, :, j]), np.nanmean, pr.SIGMA_SESSIONS_INTRADAY)
                vmean = prior_stat(vol[h][:, :, j], np.nanmean, pr.SIGMA_SESSIONS_INTRADAY)
                with np.errstate(all="ignore"):
                    z = pr.residual_z(R[:, :, j], ref, b[:, None], sigma)
                    rvol = vol[h][:, :, j] / vmean
                ok = pr.qualifies_array(z, breadth, R[:, :, j], atr, rvol, z_min=pr.Z_MIN_BY_HORIZON[h])
                ok[:WARM] = False
                hit = ok.any(axis=1)
                by_horizon[h][:, j] |= hit
                flagged[:, j] |= hit
                if t == "NVDA" and "2026-11-09" in [str(d) for d in days]:
                    d9 = di[pd.Timestamp("2026-11-09").date()]
                    if hit[d9]:
                        slot = int(np.argmax(ok[d9]))
                        nvda_note.append(f"{h}/{name} first at slot {slot} (z {z[d9, slot]:.1f}, breadth {breadth[d9, slot]:.2f}, rvol {rvol[d9, slot]:.2f})")
            # five-session horizon from dailies, evaluated at each close
            r5 = daily[t].pct_change(5)
            ref5 = daily[members].pct_change(5).mean(axis=1)
            cnt5 = daily[members].pct_change(5).notna().sum(axis=1)
            opp5 = ((daily[members].pct_change(5).gt(0)).ne((r5 > 0), axis=0) & daily[members].pct_change(5).notna()).sum(axis=1) / cnt5.replace(0, np.nan)
            cov = dret[t].rolling(pr.BETA_SESSIONS).cov(dret[members].mean(axis=1)).shift(1)
            var = dret[members].mean(axis=1).rolling(pr.BETA_SESSIONS).var().shift(1)
            resid5 = r5 - (cov / var) * ref5
            sig5 = resid5.rolling(pr.BETA_SESSIONS).std().shift(1)
            atr5 = r5.abs().rolling(pr.BETA_SESSIONS).mean().shift(1)
            ok5 = pr.qualifies_array((resid5 / sig5).to_numpy(), opp5.to_numpy(), r5.to_numpy(), atr5.to_numpy(), np.ones(len(r5)), use_rvol=False, z_min=pr.Z_MIN_BY_HORIZON["5d"])
            ok5 &= (cnt5 >= MIN_MEMBERS).to_numpy()
            for k, d in enumerate(dates):
                if "5d" in HORIZONS_ON and d in di and ok5[k] and di[d] >= WARM:
                    by_horizon["5d"][di[d], j] = True
                    flagged[di[d], j] = True
                    if t == "NVDA" and str(d) == "2026-11-09":
                        nvda_note.append(f"5d/{name}")

    split = WARM + (len(days) - WARM) // 2          # calibration before, test from here on (P2 registration)

    def summary(lo: int, hi: int) -> dict:
        per = flagged[lo:hi].sum(axis=1)
        med, p90 = float(np.median(per)), float(np.percentile(per, 90))
        return {"sessions": hi - lo, "median": med, "mean": round(float(per.mean()), 1), "p90": p90, "max": int(per.max()),
                "by_horizon_median": {h: float(np.median(a[lo:hi].sum(axis=1))) for h, a in by_horizon.items()},
                "pass": med <= 10 and p90 <= 25}

    print(json.dumps({"universe": len(tickers), "calibration": summary(WARM, split), "test": summary(split, len(days)),
                      "last_sessions": {str(days[i]): int(flagged[i].sum()) for i in range(max(WARM, len(days) - 14), len(days))},
                      "nvda_9nov": nvda_note or "did not fire"}, indent=1))


asyncio.run(main())
