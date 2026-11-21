"""Report the S5 shadow against B1: for each recorded rebalance, the buy-and-hold return of the S0 (equal) and S5 (conviction) weights to the next rebalance (or
the latest close), their difference, and the running count. The registered bar (reports/Upgrade pre-registrations - 12 Nov.md, 'Conviction weights (S5), one forward
test') needs months of data (about 21-session blocks on eight offset calendars); this prints what has accumulated and says plainly that it is not yet judgeable.
Read-only. Run inside sentinel-api_gateway or agents-heavy:  python /tmp/rbs.py
"""
import asyncio
import json
from datetime import date

from shared.db import get_timescale
from shared.utils import book_shadow as bs


async def main() -> None:
    db = await get_timescale()
    rows = await db.query("SELECT rebalanced_on, variant, weights, scores_as_of FROM book_shadow_books ORDER BY rebalanced_on, variant")
    late = {}
    books: dict = {}
    for r in rows or []:
        d = r["rebalanced_on"] if hasattr(r["rebalanced_on"], "toordinal") else date.fromisoformat(str(r["rebalanced_on"])[:10])
        w = r["weights"] if isinstance(r["weights"], dict) else json.loads(r["weights"])
        books.setdefault(d, {})[r["variant"]] = w
        so = r.get("scores_as_of")
        so = so if (so is None or hasattr(so, "toordinal")) else date.fromisoformat(str(so)[:10])
        late[d] = bool(so is not None and (so - d).days > 3)    # scores dated after the rebalance: look-ahead, not valid for the test
    if not books:
        print("no shadow books recorded yet")
        return
    days = sorted(books)
    names = sorted({t for b in books.values() for w in b.values() for t in w})
    last = (await db.query_one("SELECT max(day) m FROM daily_bars WHERE ticker = 'SPY'"))["m"]
    last = last if hasattr(last, "toordinal") else date.fromisoformat(str(last)[:10])
    closes: dict = {}
    for r in await db.query("SELECT ticker, day, close FROM daily_bars WHERE ticker = ANY($1::text[]) AND day >= $2 ORDER BY ticker, day", names, days[0]) or []:
        d = r["day"] if hasattr(r["day"], "toordinal") else date.fromisoformat(str(r["day"])[:10])
        closes.setdefault(r["ticker"], []).append((d, float(r["close"])))
    diffs = []
    for i, d in enumerate(days):
        end = days[i + 1] if i + 1 < len(days) else last
        if end <= d:
            continue
        r0 = bs.hold_return(books[d].get("S0", {}), closes, d, end)
        r5 = bs.hold_return(books[d].get("S5", {}), closes, d, end)
        done = i + 1 < len(days)
        flag = "  [NOT VALID: weights used scores dated after the rebalance]" if late.get(d) else ""
        print(f"{d} -> {end} ({'closed' if done else 'open'}){flag}: S0 {None if r0 is None else round(r0 * 1e4, 1)} bps, S5 {None if r5 is None else round(r5 * 1e4, 1)} bps")
        if done and r0 is not None and r5 is not None and not late.get(d):
            diffs.append(r5 - r0)
    print(f"closed rebalance periods: {len(diffs)}; the registered test needs months (about 21-session blocks on eight offset calendars): not yet judgeable")


asyncio.run(main())
