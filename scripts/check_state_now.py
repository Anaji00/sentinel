"""One-shot state check: factor-engine heartbeat, study OTC count, quota status, nightly run markers. Run in a service container."""

import asyncio
import json

from shared.db import get_redis
from shared.utils import engine_liveness as el
from shared.utils import model_quota as mq
from shared.utils.signal_store import STUDY_KEY


async def main() -> None:
    r = await get_redis()
    print("heartbeat age:", await el.heartbeat_age(r.raw) if asyncio.iscoroutinefunction(el.heartbeat_age) else el.heartbeat_age(r.raw))
    blob = await r.raw.get(STUDY_KEY)
    s = json.loads(blob if isinstance(blob, str) else blob.decode())
    print("study", s["generated_at"], "| untradeable", s["instruments_untradeable"], "| without bars", s["instruments_without_bars"],
          "| of which OTC", s.get("instruments_otc_without_bars"))
    for m in await mq.snapshot(r.raw):
        print(m["provider"], m["model"], m["status"], "cool", m["cooling_down_seconds"], "req/day", m["requests_today"], "/", m["requests_per_day_limit"], "share", m["share_of_limit_used"])


asyncio.run(main())
