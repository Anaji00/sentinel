"""Read-only. Layer-5 audit: every agent's resolved calls, beta-adjusted and date-clustered, plus output hygiene.

Per agent (90 days): calls, calls/ticker-day (duplicates), direction mix, distinct confidences, one observation per date of the mean signed
`excess_beta` (positive = call right), t over dates, hit share by date, and a confidence-bin check (does higher stated confidence mean a
higher date-weighted signed excess?). Writes nothing.
Run: docker exec -i -w /app sentinel-api_gateway python - < scripts/audit_layer_agents.py
"""
import asyncio
import math
import sys
from collections import defaultdict

sys.path.insert(0, "/app")
from shared.db import get_timescale  # noqa: E402

_SQL = """
SELECT agent_name, horizon_hours AS h, date(occurred_at) AS d, ticker, direction, confidence, outcome_correct,
       CASE direction WHEN 'up' THEN excess_beta WHEN 'down' THEN -excess_beta END AS s
FROM agent_predictions
WHERE occurred_at > NOW() - INTERVAL '90 days' AND agent_name IS NOT NULL
"""


def t_over(xs: list[float]) -> float | None:
    n = len(xs)
    if n < 3:
        return None
    m = sum(xs) / n
    sd = math.sqrt(sum((x - m) ** 2 for x in xs) / (n - 1))
    return None if sd == 0 else m / (sd / math.sqrt(n))


def fmt(x: float | None, nd: int = 2) -> str:
    return "  n/a" if x is None else f"{x:+.{nd}f}"


async def main() -> None:
    db = await get_timescale()
    rows = await db.query(_SQL) or []
    by: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        by[r["agent_name"]].append(r)
    print("agent | rows | up% down% neutral% | distinct conf | dup ticker-days(>1 call) | graded beta | dates | mean bps by date | t | hit by date | top3 date share")
    for ag in sorted(by, key=lambda a: -len(by[a])):
        rs = by[ag]
        n = len(rs)
        dirs = defaultdict(int)
        for r in rs:
            dirs[r["direction"] or "none"] += 1
        confs = {round(r["confidence"], 3) for r in rs if r["confidence"] is not None}
        td: dict[tuple, int] = defaultdict(int)
        for r in rs:
            td[(r["ticker"], r["d"], r["h"])] += 1
        dup = sum(1 for v in td.values() if v > 1)
        gr = [r for r in rs if r["s"] is not None]
        per_date: dict = defaultdict(list)
        hit: dict = defaultdict(list)
        for r in gr:
            per_date[r["d"]].append(r["s"])
            hit[r["d"]].append(1.0 if r["s"] > 0 else 0.0)
        dm = [sum(v) / len(v) for v in per_date.values()]
        hm = [sum(v) / len(v) for v in hit.values()]
        sizes = sorted((len(v) for v in per_date.values()), reverse=True)
        top3 = sum(sizes[:3]) / max(1, sum(sizes))
        pct = lambda k: 100.0 * dirs.get(k, 0) / max(1, n)  # noqa: E731
        print(
            f"{ag} | {n} | {pct('up'):.0f}/{pct('down'):.0f}/{pct('neutral'):.0f} | {len(confs)} | {dup} | {len(gr)} | {len(dm)} | "
            f"{fmt(10000 * sum(dm) / len(dm) if dm else None, 0)} | {fmt(t_over(dm))} | "
            f"{(sum(hm) / len(hm)) if hm else float('nan'):.3f} | {top3:.2f}"
        )
        # confidence bins (date-clustered within bin): low vs high half by median confidence
        cs = sorted(r["confidence"] for r in gr if r["confidence"] is not None)
        if len(cs) >= 40 and len(set(cs)) > 3:
            med = cs[len(cs) // 2]
            for lab, sel in (("low", lambda c: c < med), ("high", lambda c: c >= med)):
                pd: dict = defaultdict(list)
                for r in gr:
                    if r["confidence"] is not None and sel(r["confidence"]):
                        pd[r["d"]].append(r["s"])
                m = [sum(v) / len(v) for v in pd.values()]
                print(f"    conf {lab} (median {med:.2f}): dates {len(m)}, mean bps {fmt(10000 * sum(m) / len(m) if m else None, 0)}, t {fmt(t_over(m))}")


asyncio.run(main())
