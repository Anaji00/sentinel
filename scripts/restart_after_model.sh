#!/usr/bin/env bash
# Wait for the factor model's report as_of to move past $1 (default 2026-11-06), then recreate factor-model and correlation so they load
# the cap rule, the stale-model alarm, the forecast flag and the retest beta skip. Gives up after 7 hours without touching anything.
set -u
OLD="${1:-2026-11-06}"
END=$(( $(date +%s) + 7 * 3600 ))
cd "$(dirname "$0")/.."
while [ "$(date +%s)" -lt "$END" ]; do
  CUR=$(MSYS_NO_PATHCONV=1 docker exec -w /app -e PYTHONPATH=/app sentinel-api_gateway python -c "
import asyncio, json
from shared.db import get_redis
from shared.utils.signal_store import FACTOR_REPORT_KEY
async def main():
    r = await get_redis()
    print(json.loads(await r.raw.get(FACTOR_REPORT_KEY) or '{}').get('as_of'))
asyncio.run(main())" 2>/dev/null | tail -n 1)
  echo "$(date -u +%H:%M) as_of=$CUR"
  if [ -n "$CUR" ] && [ "$CUR" != "None" ] && [[ "$CUR" > "$OLD" ]]; then
    echo "model as_of moved to $CUR; recreating factor-model and correlation"
    docker compose --profile agents --profile collectors up -d --no-build --force-recreate factor-model correlation 2>&1 | tail -n 4
    exit 0
  fi
  sleep 300
done
echo "gave up: as_of still $OLD after 7 hours; nothing restarted"
exit 1
