"""Which instruments the reaction study could not price, and why. Rebuilds the study's instrument set and classifies the ones
with no price series. Read-only. Run inside agents-heavy: python /tmp/ib.py"""

import asyncio
import collections
import re
from datetime import datetime, timedelta, timezone

from services.agents import signal_engine as se
from shared.db import get_redis, get_timescale
from shared.utils.asset_universe import load_asset_meta, priceable_now
from shared.utils.signal_families import episodes, observations


def shape(sym: str) -> str:
    if sym.endswith("=F"):
        return "future"
    if re.search(r"[0-9]", sym):
        return "has digit"
    if "." in sym or "-" in sym or "/" in sym:
        return "dot/dash"
    if len(sym) > 5:
        return "long (>5)"
    if len(sym) == 5 and sym[-1] in "WRU":
        return "5-letter W/R/U"
    return "plain"


async def main() -> None:
    db, redis = await get_timescale(), await get_redis()
    eng = se.SignalEngine(db, redis)
    now = datetime.now(timezone.utc)
    since = now - timedelta(days=se.STUDY_LOOKBACK_DAYS)
    obs = await eng._load_observations(since, now)
    obs += await eng._load_observations(now - timedelta(days=se.HISTORY_LOOKBACK_DAYS), since, se.DEEP_HISTORY_TYPES, step_days=30)
    eps = episodes(obs)
    by_inst: dict = {}
    for e in eps:
        by_inst.setdefault(e.instrument, []).append(e)
    intel = {i for i, es in by_inst.items() if any(e.family not in se.PRICE_DIRECTED for e in es)}
    print("episodes", len(eps), "| instruments", len(by_inst), "| intel (counted)", len(intel))

    meta = await load_asset_meta(redis.raw)
    print("asset meta rows", len(meta))
    insts = sorted(i for i in intel if not i.endswith("=F"))
    has_daily = {r["t"] for r in await db.query(
        "SELECT t FROM unnest($1::text[]) AS t WHERE EXISTS (SELECT 1 FROM daily_bars d WHERE d.ticker = t)", insts) or []}
    rest = [i for i in insts if i not in has_daily]
    print("with daily bars", len(has_daily), "| without daily bars", len(rest))
    nobars = []
    for inst in rest:
        first = min(e.known_at for e in by_inst[inst])
        intr = await eng._series(inst, since - timedelta(days=1))
        if intr is None:
            nobars.append(inst)
    print("no series at all", len(nobars))

    listed_kind = collections.Counter()
    shapes_unlisted = collections.Counter()
    for i in nobars:
        row = meta.get(i)
        listed_kind[row[0] if row else "NOT LISTED"] += 1
        if not row:
            shapes_unlisted[shape(i)] += 1
    print("\nno-series by Alpaca listing:", dict(listed_kind))
    print("not-listed by ticker shape:", dict(shapes_unlisted))

    import json
    json.dump({i: [len(by_inst[i]), fam_name] for i, fam_name in ((i, collections.Counter(e.family for e in by_inst[i]).most_common(1)[0][0]) for i in nobars)}, open("/tmp/nobars.json", "w"))
    pri = [i for i in nobars if priceable_now(i, meta)]
    unp = [i for i in nobars if not priceable_now(i, meta)]
    print("\npriceable (queued for backfill):", len(pri), "| untradeable:", len(unp))

    def fam(i: str) -> str:
        c = collections.Counter(e.family for e in by_inst[i])
        return ",".join(f"{k}:{v}" for k, v in c.most_common(2))

    print("\nTop untradeable by episodes:")
    for i in sorted(unp, key=lambda x: -len(by_inst[x]))[:25]:
        print(f"  {i:12s} eps={len(by_inst[i]):4d} listed={meta.get(i, ['-'])[0]:8s} {fam(i)}")
    print("\nTop priceable-but-no-bars by episodes:")
    for i in sorted(pri, key=lambda x: -len(by_inst[x]))[:30]:
        print(f"  {i:12s} eps={len(by_inst[i]):4d} {meta[i][0]:6s} {meta[i][3][:34]:34s} {fam(i)}")
    ep_unp = sum(len(by_inst[i]) for i in unp)
    ep_pri = sum(len(by_inst[i]) for i in pri)
    print(f"\nepisodes lost: untradeable {ep_unp} of {len(eps)} ({ep_unp / len(eps):.1%}); priceable-no-bars {ep_pri} ({ep_pri / len(eps):.1%})")
    fam_unp = collections.Counter(e.family for i in unp for e in by_inst[i])
    print("untradeable episodes by family:", fam_unp.most_common(8))
    shapes_all = collections.Counter(shape(i) for i in unp)
    print("untradeable by shape:", dict(shapes_all))
    print("sample untradeable plain tickers:", [i for i in unp if shape(i) == "plain"][:30])


asyncio.run(main())
