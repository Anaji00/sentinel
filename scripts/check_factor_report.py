"""When the factor model's stored report was written and which session it scores through. Run in a service container."""

import asyncio
import json

from shared.db import get_redis
from shared.utils.signal_store import FACTOR_REPORT_KEY


async def main() -> None:
    r = await get_redis()
    blob = await r.raw.get(FACTOR_REPORT_KEY)
    rep = json.loads(blob if isinstance(blob, str) else blob.decode()) if blob else {}
    print({k: rep.get(k) for k in ("as_of", "generated_at", "finished_at", "run_at", "champion", "model") if k in rep} or sorted(rep)[:15])


asyncio.run(main())
