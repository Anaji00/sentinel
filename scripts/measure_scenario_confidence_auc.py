"""P6 measurement (read-only): does the first recorded confidence of a scenario separate the ones later confirmed from the ones denied (AUC, clustered by day)?
Run: MSYS_NO_PATHCONV=1 docker exec -i -w /app sentinel-api_gateway python - < scripts/measure_scenario_confidence_auc.py"""
import asyncio
import collections
import json
import sys

sys.path.insert(0, "/app")
from shared.db import get_timescale  # noqa: E402


def auc(pos, neg):
    if not pos or not neg:
        return None
    wins = sum((p > n) + 0.5 * (p == n) for p in pos for n in neg)
    return wins / (len(pos) * len(neg))


async def main() -> None:
    db = await get_timescale()
    rows = await db.query("SELECT created_at::date AS d, status, confidence_history, confidence_overall FROM scenarios "
                          "WHERE status IN ('confirmed','denied') AND created_at > NOW() - INTERVAL '90 days'")
    firsts = []
    for r in rows:
        h = r["confidence_history"]
        h = json.loads(h) if isinstance(h, str) else h
        if h:
            firsts.append((r["d"], r["status"] == "confirmed", h[0].get("confidence")))
    pos = [c for _, y, c in firsts if y and c is not None]
    neg = [c for _, y, c in firsts if not y and c is not None]
    print(f"resolved with a history: {len(firsts)}; confirmed {len(pos)} ({len(pos) / max(len(firsts), 1):.0%}); first-confidence AUC {auc(pos, neg)}")
    by_day = collections.defaultdict(list)
    for d, y, c in firsts:
        if c is not None:
            by_day[d].append((y, c))
    daily = []
    for d, v in by_day.items():
        a = auc([c for y, c in v if y], [c for y, c in v if not y])
        if a is not None:
            daily.append(a)
    print(f"days with both outcomes {len(daily)}; mean within-day AUC {sum(daily) / max(len(daily), 1):.3f}")
    print("distinct first confidences:", len({c for _, _, c in firsts}), "mean pos", sum(pos) / max(len(pos), 1), "mean neg", sum(neg) / max(len(neg), 1))


asyncio.run(main())
