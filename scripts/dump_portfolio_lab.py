import asyncio
import json

from shared.db import get_redis
from shared.utils import portfolio_lab as pl


async def main() -> None:
    r = await get_redis()
    raw = await getattr(r, "raw", r).get(pl.PORTFOLIO_LAB_KEY)
    lab = json.loads(raw) if raw else {}
    keep = {}
    for k in ("as_of", "model", "live", "industry_neutral", "beta_neutral", "short_hedged", "rank_weighted",
              "cluster_neutral", "adjusted", "meta_sized", "equity_regime", "ran_at"):
        v = lab.get(k)
        if k == "meta_sized" and isinstance(v, dict):
            v = {a: v.get(a) for a in ("periods", "base", "meta", "mean_size")}
        keep[k] = v
    print(json.dumps(keep, indent=1, default=str))


asyncio.run(main())
