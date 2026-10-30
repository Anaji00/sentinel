"""
scripts/llm_cost_estimate.py

What the platform's model calls would cost on a hosted API. Reads the usage
meter (shared/utils/ollama.py: sentinel:llm_usage:{day}, calls and tokens per
caller and model), takes each caller's tokens per call, and prices a month at
two volumes: the calls per hour the local host actually serves, and the
inference budget's own ceiling (one call per INFERENCE_MIN_GAP_SEC), which is
what the agents would ask for if the host were not the bottleneck.

Run inside a container that reaches Redis:
    docker exec -w /app sentinel-agents-heavy python scripts/llm_cost_estimate.py --per-hour 40

Prices are list prices per million tokens and change; pass --prices to
override (JSON {"name": [input, output]}).
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
from typing import Dict, List, Tuple

# Per million tokens, input and output, as listed in October 2026.
DEFAULT_PRICES: Dict[str, Tuple[float, float]] = {
    "claude-haiku-5-5": (0.10, 0.50),
    "gpt-6-luna": (0.10, 0.50),
    "gpt-5.4-nano": (0.20, 1.25),
    "gemini-3.1-flash-lite": (0.25, 1.50),
    "gemini-2.5-flash-lite": (0.10, 0.40),
}
DAYS = 30


def per_call(fields: Dict[str, int]) -> Dict[str, Tuple[int, float, float]]:
    """{caller|model: (calls, input per call, output per call)} from meter fields."""
    acc: Dict[str, Dict[str, int]] = {}
    for f, v in fields.items():
        site, _, metric = f.rpartition("|")
        acc.setdefault(site, {})[metric] = acc.get(site, {}).get(metric, 0) + int(v)
    out = {}
    for site, m in acc.items():
        n = m.get("calls", 0)
        if n:
            out[site] = (n, m.get("in", 0) / n, m.get("out", 0) / n)
    return out


def month_tokens(sites: Dict[str, Tuple[int, float, float]], per_hour: float,
                 extra: List[Tuple[float, float, float]]) -> Tuple[float, float]:
    """Tokens in and out for a month: per_hour calls split by each caller's share
    of measured calls, plus scheduled extras (calls a month, in, out per call)."""
    total = sum(n for n, _, _ in sites.values())
    calls = per_hour * 24 * DAYS
    tin = sum(calls * n / total * i for n, i, _ in sites.values()) if total else 0.0
    tout = sum(calls * n / total * o for n, _, o in sites.values()) if total else 0.0
    for c, i, o in extra:
        tin, tout = tin + c * i, tout + c * o
    return tin, tout


def cost(tin: float, tout: float, price: Tuple[float, float]) -> float:
    return tin / 1e6 * price[0] + tout / 1e6 * price[1]


async def _meter() -> Dict[str, int]:
    from shared.db import get_redis

    raw = (await get_redis()).raw
    fields: Dict[str, int] = {}
    async for k in raw.scan_iter("sentinel:llm_usage:*"):
        for f, v in (await raw.hgetall(k)).items():
            f = f.decode() if isinstance(f, bytes) else f
            fields[f] = fields.get(f, 0) + int(v)
    return fields


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--per-hour", type=float, default=40.0, help="agent calls an hour served today")
    ap.add_argument("--prices", default="", help='JSON {"name": [input, output]} per million tokens')
    ap.add_argument("--analyst-per-day", type=float, default=float(os.getenv("ANALYST_DAILY_CAP", "24")) * 3)
    args = ap.parse_args()
    prices = {k: tuple(v) for k, v in json.loads(args.prices).items()} if args.prices else DEFAULT_PRICES
    sites = per_call(asyncio.run(_meter()))
    analyst = [v for k, v in sites.items() if k.startswith("analyst_agent|")]
    agents = {k: v for k, v in sites.items() if not k.startswith("analyst_agent|")}
    a_in, a_out = (analyst[0][1], analyst[0][2]) if analyst else (700.0, 150.0)
    extra = [(args.analyst_per_day * DAYS, a_in, a_out)]
    ceiling = 3600 / float(os.getenv("INFERENCE_MIN_GAP_SEC", "30"))
    print("measured per call:")
    for k, (n, i, o) in sorted(sites.items()):
        print(f"  {k:55s} calls {n:5d}  in {i:7.0f}  out {o:6.0f}")
    for label, ph in (("served today", args.per_hour), ("budget ceiling", ceiling)):
        tin, tout = month_tokens(agents, ph, extra)
        print(f"\n{label}: {ph:.0f} agent calls/h + analyst {args.analyst_per_day:.0f}/day -> "
              f"{tin / 1e6:.1f}M in, {tout / 1e6:.1f}M out a month")
        for name, p in prices.items():
            print(f"  {name:24s} ${cost(tin, tout, p):8.2f}/month   (output x2 for thinking: "
                  f"${cost(tin, 2 * tout, p):.2f})")


if __name__ == "__main__":
    main()
