import asyncio
import json
from collections import Counter

from shared.db import get_redis
from shared.utils.signal_store import STUDY_KEY


async def main() -> None:
    r = await get_redis()
    raw = await getattr(r, "raw", r).get(STUDY_KEY)
    d = json.loads(raw) if raw else {}
    print("top keys", list(d)[:12], "generated", d.get("generated_at") or d.get("as_of"))
    cells = d.get("cells") or d.get("results") or []
    if isinstance(cells, dict):
        cells = list(cells.values())
    print("cells", len(cells))
    if cells:
        print("sample", json.dumps(cells[0], default=str)[:600])
    status = Counter(str(c.get("verdict") or c.get("status")) for c in cells if isinstance(c, dict))
    print("verdicts", dict(status))
    reasons = Counter(str(c.get("reason") or c.get("failed"))[:60] for c in cells if isinstance(c, dict) and not c.get("proven"))
    print("why not proven", reasons.most_common(8))
    proven = [c for c in cells if isinstance(c, dict) and c.get("proven")]
    for c in proven[:20]:
        print("PROVEN", {k: c.get(k) for k in ("family", "horizon", "units", "mean_bps", "t", "q", "hit_rate", "validation_t")})
    fam = Counter(str(c.get("family")) for c in cells if isinstance(c, dict))
    print("families", len(fam))


asyncio.run(main())
