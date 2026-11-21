#!/usr/bin/env bash
# Run the registered top-of-ranking calibration test (reports/Upgrade pre-registrations - 12 Nov.md) in its own capped container, not before
# 06:00 UTC (the registration's rule: nothing else heavy running). Output goes to the file given as $1.
set -u
cd "$(dirname "$0")/.."
OUT="${1:-calibration_top.out}"
while [ "$(date -u +%H%M)" -lt "0600" ] && [ "$(date -u +%H)" -lt "12" ]; do sleep 60; done
echo "starting $(date -u +%H:%M) UTC" > "$OUT"
MSYS_NO_PATHCONV=1 docker run --rm --name sentinel-calibration-top --memory 9g --network sentinel-network --env-file .env \
  -v "$(pwd -W 2>/dev/null || pwd)/shared:/app/shared:ro" -v "$(pwd -W 2>/dev/null || pwd)/services:/app/services:ro" \
  -v "$(pwd -W 2>/dev/null || pwd)/scripts:/app/scripts:ro" -w /app -e PYTHONPATH=/app \
  sentinel-agents-heavy python scripts/research_calibration_top.py >> "$OUT" 2>&1
echo "exit $? $(date -u +%H:%M) UTC" >> "$OUT"
