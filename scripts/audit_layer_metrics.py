"""Read-only: dump the live per-service metrics (collector ingest/reject/error, quiet-failure counters).
Run: docker exec -w /app sentinel-api_gateway python scripts/audit_layer_metrics.py
No writes. No secrets printed."""
import asyncio
from shared.db import get_redis
from shared.utils.metrics import collect_all


async def main() -> None:
    r = await get_redis()
    data = await collect_all(r)
    for svc in sorted(data):
        for k, v in sorted(data[svc].items()):
            if any(t in k for t in ("collector_", "swallowed", "dropped", "reject", "error", "last_success")):
                print(f"{svc}\t{k}\t{v}")

asyncio.run(main())
