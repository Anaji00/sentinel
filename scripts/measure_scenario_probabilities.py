"""P6 measurement (read-only): how informative are the model-written hypothesis probabilities in stored scenarios, and is there any calibration record?

Prints: the share of scenarios on the most common probability triples, whether hypothesis 1 is always the leader, the spread of the leading probability, the
reliability buckets and Brier the tracker has kept in Redis, and the Brier of a flat 1/3 split for comparison.
Run: MSYS_NO_PATHCONV=1 docker exec -i -w /app sentinel-api_gateway python - < scripts/measure_scenario_probabilities.py
"""
import asyncio
import collections
import json
import sys

sys.path.insert(0, "/app")
from shared.db import get_redis, get_timescale  # noqa: E402


async def main() -> None:
    db = await get_timescale()
    rows = await db.query("SELECT scenario_id, status, confidence_overall, hypotheses FROM scenarios WHERE created_at > NOW() - INTERVAL '90 days'")
    triples, lead_first, leads, n = collections.Counter(), 0, [], 0
    statuses = collections.Counter(r["status"] for r in rows)
    for r in rows:
        hyp = r["hypotheses"]
        if isinstance(hyp, str):
            try:
                hyp = json.loads(hyp)
            except ValueError:
                continue
        probs = [int(h.get("probability") or 0) for h in (hyp or []) if isinstance(h, dict)]
        if len(probs) < 2:
            continue
        n += 1
        triples[tuple(probs)] += 1
        leads.append(max(probs))
        lead_first += probs[0] == max(probs)
    print(f"scenarios {len(rows)} (90 days); with 2+ hypotheses {n}; status {dict(statuses)}")
    print("top triples:", [(t, c, f"{c / max(n, 1):.0%}") for t, c in triples.most_common(8)])
    print(f"hypothesis 1 is the leader in {lead_first / max(n, 1):.0%}; leading probability mean {sum(leads) / max(len(leads), 1):.1f}, distinct {len(set(leads))}")
    distinct_triples = len(triples)
    print(f"distinct triples {distinct_triples} of {n}")
    redis = await get_redis()
    raw = getattr(redis, "raw", redis)
    for b in range(10):
        nn, hit = await raw.get(f"sentinel:scenario_calibration:bucket:{b}:n"), await raw.get(f"sentinel:scenario_calibration:bucket:{b}:hit")
        if nn:
            print(f"bucket {b * 10}-{b * 10 + 9}%: n {int(nn)}, lead realised {int(hit or 0)}")
    tot_b = tot_c = 0.0
    async for k in raw.scan_iter(match="sentinel:scenario_calibration:brier_sum:*"):
        tot_b += float(await raw.get(k) or 0)
        tot_c += float(await raw.get(k.replace(b"brier_sum", b"count") if isinstance(k, bytes) else k.replace("brier_sum", "count")) or 0)
    print(f"tracker Brier: {tot_b / tot_c:.3f} over {int(tot_c)} scored scenarios (flat 1/3 split scores 0.667)" if tot_c else "tracker Brier: no scored scenarios")


asyncio.run(main())
