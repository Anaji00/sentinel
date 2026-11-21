"""Registered replays R1 to R4 (reports/Upgrade pre-registrations - 12 Nov.md, 21 Nov 2026). Read-only; nothing is written.

Run inside a service container off the hot path:
    docker cp scripts/replay_score_rescale.py sentinel-factor-model:/tmp/rsc.py
    docker exec -w /app -e PYTHONPATH=/app sentinel-factor-model python /tmp/rsc.py r1 r2 r3 r4
"""
import asyncio
import bisect
import collections
import statistics
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from shared.db import get_timescale  # noqa: E402
from shared.utils import score_rescale as sc  # noqa: E402
from shared.utils.score_resolution import ALERT_CUTS, cut_changes, five_session_abs_move, share_at_or_above, spearman, top_share_mean  # noqa: E402

ET = ZoneInfo("America/New_York")
Bars = Dict[str, Tuple[List[date], List[float], List[float]]]


def ts(x) -> datetime:
    return datetime.fromisoformat(x.replace("Z", "+00:00")) if isinstance(x, str) else x


def verdict(name: str, value: str, ok: bool) -> None:
    print(f"  {'PASS' if ok else 'FAIL'}  {name}: {value}")


async def load_bars(db, tickers: Sequence[str], lo: date, hi: date) -> Bars:
    out: Bars = {}
    for i in range(0, len(tickers), 500):
        for b in await db.query(
                "SELECT ticker, day, close, volume FROM daily_bars WHERE ticker = ANY($1::text[]) AND day >= $2::date AND day <= $3::date "
                "AND close > 0 ORDER BY ticker, day", list(tickers[i:i + 500]), lo, hi) or []:
            d, c, v = out.setdefault(b["ticker"], ([], [], []))
            d.append(b["day"])
            c.append(float(b["close"]))
            v.append(float(b["volume"] or 0))
    return out


def adv_before(bars: Bars, ticker: str, d: date) -> Optional[float]:
    t = bars.get(ticker)
    if not t:
        return None
    i = bisect.bisect_left(t[0], d)
    return sc.adv_usd(t[1][max(0, i - 20):i], t[2][max(0, i - 20):i])


def outcome_report(label: str, rows: List[Tuple[object, float, float, float]], min_n: int) -> bool:
    res = sc.date_cluster_diff(rows)
    if res is None:
        verdict(f"{label} outcome Spearman, new not worse than old", "too few rows", False)
        return False
    ok = sc.not_worse(res, min_n)
    verdict(f"{label} outcome Spearman, new not worse than old (n>={min_n}, diff>=0, lower>=-0.02)",
            f"n {int(res['n'])} on {int(res['dates'])} dates; old {res['rho_old']:+.4f} new {res['rho_new']:+.4f} diff {res['diff']:+.4f} "
            f"[{res['lo']:+.4f}, {res['hi']:+.4f}]", ok)
    return ok


def cut_line(old: Sequence[float], new: Sequence[float]) -> Tuple[bool, str]:
    ch = cut_changes(old, new)
    return ch["newly_above_cut"] == 0, f"{ch['newly_above_cut']} newly above, {ch['newly_below_cut']} newly below (informational)"


# ---- R1 ------------------------------------------------------------------------------------------------------------------------------

async def r1(db) -> None:
    print("\n=== R1 earnings_report (pre-announcement) ===")
    ev = await db.query(
        "SELECT upper(primary_entity_id) AS t, financial_data->>'earnings_report_date' AS rd, anomaly_score AS s FROM events "
        "WHERE type = 'earnings_report' AND anomaly_score IS NOT NULL AND created_at > NOW() - INTERVAL '90 days'") or []
    tickers = sorted({r["t"] for r in ev})
    bars = await load_bars(db, tickers, date.today() - timedelta(days=700), date.today())
    old, new, rows, no_r = [], [], [], 0
    jump_cache: Dict[Tuple[str, date], Optional[float]] = {}
    for r in ev:
        try:
            rd = date.fromisoformat(str(r["rd"]))
        except (TypeError, ValueError):
            continue
        t = bars.get(r["t"])
        o = float(r["s"])
        r_val = None
        if t:
            key = (r["t"], rd)
            if key not in jump_cache:
                i = bisect.bisect_left(t[0], rd)
                window = t[1][max(0, i - 253):i]
                jump_cache[key] = sc.jumpiness([b / a - 1.0 for a, b in zip(window, window[1:])])
            r_val = jump_cache[key]
        if r_val is None:
            no_r += 1
        n = sc.earnings_pre_score(o, r_val)
        old.append(o)
        new.append(n)
        if t:
            m = sc.normalized_abs_move(t[0], t[1], rd - timedelta(days=1), sessions=2)
            if m is not None:
                rows.append((rd, o, n, m))
    mv_o, sh_o = sc.mode_share(old)
    mv_n, sh_n = sc.mode_share(new)
    print(f"events {len(old)} (90 days); jumpiness unknown for {no_r}; with an outcome {len(rows)}")
    verdict(">= 30 distinct values over >= 1,000 events", f"{len({round(x, 3) for x in new})} distinct over {len(new)} (old {len({round(x, 3) for x in old})})",
            len({round(x, 3) for x in new}) >= 30 and len(new) >= 1000)
    verdict("no single value holds more than 15%", f"new mode {mv_n:.3f} at {sh_n:.1%} (old {mv_o:.3f} at {sh_o:.1%})", sh_n <= 0.15)
    outcome_report("R1", rows, 500)
    ok, msg = cut_line(old, new)
    verdict("no event newly above a cut", msg, ok)


# ---- R2 ------------------------------------------------------------------------------------------------------------------------------

async def r2(db) -> None:
    print("\n=== R2 prediction_market_trade (Kalshi quiet cap) ===")
    ev = await db.query(
        "SELECT primary_entity_id AS m, occurred_at AS at, anomaly_score AS s, (prediction_market_data->>'notional_usd')::float AS n, "
        "(prediction_market_data->>'yes_probability')::float AS p FROM events WHERE type = 'prediction_market_trade' AND source = 'kalshi' "
        "AND anomaly_score IS NOT NULL AND created_at > NOW() - INTERVAL '60 days' ORDER BY primary_entity_id, occurred_at") or []
    by: Dict[str, List[Tuple[datetime, Optional[float]]]] = collections.defaultdict(list)
    for r in ev:
        by[r["m"]].append((ts(r["at"]), r["p"]))
    times = {m: [x[0] for x in v] for m, v in by.items()}
    old, new, rows = [], [], []
    for r in ev:
        o = float(r["s"])
        n = sc.kalshi_quiet_score(o, r["n"], r["p"])
        old.append(o)
        new.append(n)
        if abs(o - sc.KALSHI_QUIET_CAP) > 1e-9 or r["p"] is None:
            continue
        t0 = ts(r["at"])
        tl = times[r["m"]]
        j = bisect.bisect_right(tl, t0)
        k = bisect.bisect_right(tl, t0 + timedelta(hours=6)) - 1
        while k >= j and by[r["m"]][k][1] is None:
            k -= 1
        if k >= j:
            rows.append((t0.date(), o, n, abs(by[r["m"]][k][1] - r["p"])))
    mv_o, sh_o = sc.mode_share(old)
    mv_n, sh_n = sc.mode_share(new)
    print(f"Kalshi events {len(old)} (60 days); capped group {sum(1 for o in old if abs(o - 0.3) < 1e-9)}; with a later event within 6 h {len(rows)}")
    verdict("share on one value under 15%", f"new mode {mv_n:.3f} at {sh_n:.1%} (old {mv_o:.3f} at {sh_o:.1%})", sh_n < 0.15)
    res = sc.date_cluster_diff(rows)
    if res is None:
        verdict("within the capped group, Spearman >= +0.05, lower end > 0", "too few rows", False)
    else:
        ok = res["n"] >= 1000 and res["rho_new"] >= 0.05 and res["lo"] > 0
        verdict("within the capped group, Spearman(new, later |dp|) >= +0.05, interval lower end > 0, n >= 1000",
                f"n {int(res['n'])} on {int(res['dates'])} dates; rho {res['rho_new']:+.4f}; diff-vs-old interval [{res['lo']:+.4f}, {res['hi']:+.4f}]", ok)
    moved = {c: (sum(1 for o in old if o >= c), sum(1 for n in new if n >= c)) for c in ALERT_CUTS}
    changed = {c: v for c, v in moved.items() if v[0] != v[1]}
    verdict("no count above any cut changes", "none" if not changed else str(changed), not changed)


# ---- R3 ------------------------------------------------------------------------------------------------------------------------------

async def r3(db) -> None:
    print("\n=== R3 filings (sec_edgar) and equity blocks ===")
    fil = await db.query(
        "SELECT upper(coalesce(filing_data->>'ticker', primary_entity_id)) AS t, occurred_at AS at, anomaly_score AS s FROM events "
        "WHERE type = 'filing' AND source = 'sec_edgar' AND anomaly_score IS NOT NULL AND primary_entity_id IS NOT NULL") or []
    blk = await db.query(
        "SELECT upper(primary_entity_id) AS t, occurred_at AS at, anomaly_score AS s, (financial_data->>'notional_usd')::float AS n FROM events "
        "WHERE type = 'equity_block' AND anomaly_score IS NOT NULL AND primary_entity_id IS NOT NULL "
        "AND created_at > NOW() - INTERVAL '90 days'") or []
    tickers = sorted({r["t"] for r in fil} | {r["t"] for r in blk})
    bars = await load_bars(db, tickers, date.today() - timedelta(days=400), date.today())
    print(f"sec_edgar filings {len(fil)}; equity blocks {len(blk)}; tickers {len(tickers)}; with bars {len(bars)}")

    def run(label: str, events, fn, min_n: int, share_level: float, share_bar: float, top: bool) -> None:
        old, new, rows, moves, raw = [], [], [], [], []
        known = 0
        for r in events:
            o = float(r["s"])
            at = ts(r["at"])
            d = at.date()
            adv = adv_before(bars, r["t"], d)
            n = fn(o, r, adv)
            known += adv is not None
            old.append(o)
            new.append(n)
            t = bars.get(r["t"])
            m = sc.normalized_abs_move(t[0], t[1], d) if t else None
            raw_m = five_session_abs_move(t[0], t[1], d) if t else None
            moves.append(m)
            raw.append(raw_m)
            if m is not None:
                rows.append((d, o, n, m))
        print(f"\n-- {label}: events {len(old)}; ADV known {known}; with an outcome {len(rows)}")
        so, sn = share_at_or_above(old, 0.8), share_at_or_above(new, 0.8)
        verdict(f"share at 0.8 or more below {share_bar:.0%}", f"new {sn:.1%} (old {so:.1%})", sn < share_bar)
        outcome_report(label, rows, min_n)
        pr = [(o, m) for o, m in zip(new, raw) if m is not None]
        po = [(o, m) for o, m in zip(old, raw) if m is not None]
        print(f"  (information) Spearman with the RAW 5-session move: old {spearman([x[0] for x in po], [x[1] for x in po])} "
              f"new {spearman([x[0] for x in pr], [x[1] for x in pr])}")
        ok, msg = cut_line(old, new)
        verdict("no event newly above a cut", msg, ok)
        if top:
            tn, to = top_share_mean(new, moves), top_share_mean(old, moves)
            verdict("top 1% by new keeps old top 1% mean normalised outcome",
                    "n/a" if tn is None or to is None else f"new {tn:.4f} vs old {to:.4f}", tn is not None and to is not None and tn >= to)
        print(f"  (information) distinct values: old {len({round(x, 3) for x in old})} new {len({round(x, 3) for x in new})}; "
              f"share below 0.6: old {sum(1 for x in old if x < 0.6) / len(old):.1%} new {sum(1 for x in new if x < 0.6) / len(new):.1%}")

    run("filings (sec_edgar)", fil, lambda o, r, adv: sc.filing_score(o, adv), 500, 0.8, 0.15, True)
    run("equity blocks", blk, lambda o, r, adv: sc.block_score(o, r["n"], adv), 1000, 0.8, 0.10, False)


# ---- R4 ------------------------------------------------------------------------------------------------------------------------------

def _slot_crypto(tt: str, t: datetime) -> Tuple[Tuple[str, int, bool], Tuple[str, bool]]:
    wk = t.weekday() >= 5
    return sc.slot_key(tt, t.hour, wk), (tt, wk)


def _bucket_et(t: datetime) -> str:
    e = t.astimezone(ET)
    m = e.hour * 60 + e.minute
    for name, lo, hi in (("pre", 0, 570), ("open", 570, 600), ("morning", 600, 690), ("midday", 690, 840), ("afternoon", 840, 930),
                         ("close", 930, 960), ("after", 960, 1440)):
        if lo <= m < hi:
            return name
    return "after"


def parity(label: str, slots: Dict[object, List[Tuple[float, float]]], min_n: int = 200) -> Tuple[bool, str]:
    """slots: key -> [(old, new)]. Share at or above 0.8 per slot against the source mean, over slots with >= min_n events."""
    allp = [p for v in slots.values() for p in v]
    res = {}
    for which, idx in (("old", 0), ("new", 1)):
        mean = sum(1 for p in allp if p[idx] >= 0.8) / max(1, len(allp))
        ratios = [(sum(1 for p in v if p[idx] >= 0.8) / len(v)) / mean for v in slots.values() if len(v) >= min_n] if mean > 0 else []
        inside = sum(1 for r in ratios if 0.5 <= r <= 2.0)
        res[which] = (inside, len(ratios), mean)
    ok = res["new"][1] > 0 and res["new"][0] / res["new"][1] >= 0.8
    return ok, (f"new {res['new'][0]}/{res['new'][1]} slots inside 0.5-2x of the mean {res['new'][2]:.2%}; "
                f"old {res['old'][0]}/{res['old'][1]} (mean {res['old'][2]:.2%})")


async def r4(db) -> None:
    print("\n=== R4 market_anomaly time of day (walk-forward, 14-day history, test window 30 days) ===")
    today = datetime.now(timezone.utc).date()
    start_test = today - timedelta(days=30)
    start_hist = start_test - timedelta(days=14)
    cb = await db.query(
        "SELECT occurred_at AS at, anomaly_score AS s, crypto_data->>'trade_type' AS tt, crypto_data->>'pair' AS pair, (crypto_data->>'price')::float AS px "
        "FROM events WHERE type = 'market_anomaly' AND source = 'coinbase_candles' AND anomaly_score IS NOT NULL "
        "AND occurred_at >= $1::date AND occurred_at < $2::date ORDER BY occurred_at", start_hist, today) or []
    print(f"coinbase_candles events {len(cb)} ({start_hist} to {today})")
    cb = [(ts(r["at"]), float(r["s"]), r["tt"] or "", r["pair"] or "", r["px"]) for r in cb]
    byday: Dict[date, List[int]] = collections.defaultdict(list)
    for i, x in enumerate(cb):
        byday[x[0].date()].append(i)
    # slot-neutral forward move for every event: first later event of the same pair 30 to 90 minutes on
    series: Dict[str, List[Tuple[datetime, float]]] = collections.defaultdict(list)
    for x in cb:
        if x[4]:
            series[x[3]].append((x[0], x[4]))
    stimes = {p: [a for a, _ in v] for p, v in series.items()}
    fwd: List[Optional[float]] = []
    for x in cb:
        v = series.get(x[3])
        m = None
        if v and x[4]:
            j = bisect.bisect_left(stimes[x[3]], x[0] + timedelta(minutes=30))
            if j < len(v) and v[j][0] <= x[0] + timedelta(minutes=90):
                m = abs(v[j][1] / x[4] - 1.0)
        fwd.append(m)
    slot_moves: Dict[Tuple[str, int, bool], List[Tuple[date, float]]] = collections.defaultdict(list)
    for x, m in zip(cb, fwd):
        if m is not None:
            slot_moves[_slot_crypto(x[2], x[0])[0]].append((x[0].date(), m))
    old, new, rows, parity_slots, used = [], [], [], collections.defaultdict(list), collections.Counter()
    for day in sorted(d for d in byday if d >= start_test):
        hist_idx = [i for d in range(1, 15) for i in byday.get(day - timedelta(days=d), [])]
        pooled = sorted(cb[i][1] for i in hist_idx)
        fine, coarse = collections.defaultdict(list), collections.defaultdict(list)
        for i in hist_idx:
            k1, k2 = _slot_crypto(cb[i][2], cb[i][0])
            fine[k1].append(cb[i][1])
            coarse[k2].append(cb[i][1])
        for v in list(fine.values()) + list(coarse.values()):
            v.sort()
        for i in byday[day]:
            at, o, tt, _pair, _px = cb[i]
            k1, k2 = _slot_crypto(tt, at)
            if len(fine.get(k1, [])) >= 50:
                n, used["slot"] = sc.slot_quantile_score(o, fine[k1], pooled), used["slot"] + 1
            elif len(coarse.get(k2, [])) >= 50:
                n, used["coarse"] = sc.slot_quantile_score(o, coarse[k2], pooled), used["coarse"] + 1
            else:
                n, used["old"] = o, used["old"] + 1
            old.append(o)
            new.append(n)
            parity_slots[k1].append((o, n))
            if fwd[i] is not None:
                base = [m for d, m in slot_moves.get(k1, []) if day - timedelta(days=14) <= d < day]
                if len(base) >= 20:
                    med = statistics.median(base)
                    if med > 0:
                        rows.append((day, o, n, fwd[i] / med))
    print(f"test-window events {len(old)}; scored from slot {used['slot']}, coarse slot {used['coarse']}, unchanged {used['old']}; with a slot-neutral outcome {len(rows)}")
    ok, msg = parity("coinbase", parity_slots)
    verdict("parity: >= 80% of slots (n>=200) within 0.5-2x of the source mean share at 0.8+", msg, ok)
    hw = collections.defaultdict(list)
    for (tt, h, wk), v in parity_slots.items():
        hw[(h, wk)].extend(v)
    print("  (information) parity on (UTC hour, weekend) slots:", parity("hw", hw)[1])
    outcome_report("R4 coinbase", rows, 5000)
    co, cn = sum(1 for o in old if o >= 0.8), sum(1 for n in new if n >= 0.8)
    verdict("count at 0.8 or more changes by no more than 30%", f"old {co} new {cn} ({(cn - co) / max(1, co):+.1%})", co > 0 and abs(cn - co) <= 0.3 * co)
    print(f"  (information) distinct values old {len({round(x, 3) for x in old})} new {len({round(x, 3) for x in new})}")

    # equities: parity and volume only
    eq = await db.query(
        "SELECT occurred_at AS at, anomaly_score AS s FROM events WHERE type = 'market_anomaly' AND source = 'alpaca_quant_radar' "
        "AND anomaly_score IS NOT NULL AND occurred_at >= $1::date AND occurred_at < $2::date ORDER BY occurred_at", start_hist, today) or []
    eq = [(ts(r["at"]), float(r["s"])) for r in eq if ts(r["at"]).astimezone(ET).weekday() < 5]
    byd: Dict[date, List[Tuple[datetime, float]]] = collections.defaultdict(list)
    for x in eq:
        byd[x[0].date()].append(x)
    o2, n2, slots = [], [], collections.defaultdict(list)
    for day in sorted(d for d in byd if d >= start_test):
        hist = [x for d in range(1, 15) for x in byd.get(day - timedelta(days=d), [])]
        pooled = sorted(x[1] for x in hist)
        per = collections.defaultdict(list)
        for x in hist:
            per[_bucket_et(x[0])].append(x[1])
        for v in per.values():
            v.sort()
        for at, o in byd[day]:
            b = _bucket_et(at)
            n = sc.slot_quantile_score(o, per.get(b, []), pooled)
            o2.append(o)
            n2.append(n)
            slots[b].append((o, n))
    print(f"\n-- alpaca_quant_radar (equities): test-window weekday events {len(o2)}; 0.8+ old {sum(1 for o in o2 if o >= 0.8)} new {sum(1 for n in n2 if n >= 0.8)}")
    ok, msg = parity("equities", slots)
    print(f"  (information; the bar needs the 0.8+ rate to be measurable, it is {share_at_or_above(o2, 0.8):.2%}) parity: {msg}")


async def main() -> None:
    which = sys.argv[1:] or ["r1", "r2", "r3", "r4"]
    db = await get_timescale()
    for name, fn in (("r1", r1), ("r2", r2), ("r3", r3), ("r4", r4)):
        if name in which:
            try:
                await fn(db)
            except Exception as exc:  # one replay failing must not hide the others
                print(f"{name} FAILED TO RUN: {type(exc).__name__}: {exc}")


if __name__ == "__main__":
    asyncio.run(main())
