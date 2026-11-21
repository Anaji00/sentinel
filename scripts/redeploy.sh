#!/usr/bin/env bash
# Redeploy one service and keep its log first (layer audit Addendum 3, finding 4: every recreate erased the container's history, so a defect in the hours
# before a deploy left no trace). Saves the whole current log to logs/archive/<service>-<UTC time>.log, then recreates the service as CLAUDE.md describes.
# Usage: scripts/redeploy.sh <compose-service> [container-name]   (container defaults to sentinel-<service>; pass it when the names differ, e.g. enrichment -> sentinel-enrichment-1)
# Refuses to run for factor-model between 22:00 and 06:00 UTC unless FORCE=1, because a restart there can kill the nightly model run.
set -euo pipefail
svc="${1:?usage: redeploy.sh <compose-service> [container-name]}"
container="${2:-sentinel-$svc}"
cd "$(dirname "$0")/.."

hour=$(date -u +%H)
if [ "$svc" = "factor-model" ] && { [ "$hour" -ge 22 ] || [ "$hour" -lt 6 ]; } && [ "${FORCE:-0}" != "1" ]; then
  echo "refusing: factor-model between 22:00 and 06:00 UTC (now $(date -u +%H:%M)); check that its nightly run has finished, then rerun with FORCE=1" >&2
  exit 2
fi

mkdir -p logs/archive
out="logs/archive/${svc}-$(date -u +%Y%m%dT%H%M%SZ).log"
if docker inspect "$container" >/dev/null 2>&1; then
  docker logs "$container" >"$out" 2>&1 || true
  echo "saved $(wc -l <"$out") log lines to $out"
else
  echo "container $container not found; nothing saved" >&2
fi

docker compose --profile agents --profile collectors up -d --no-build --force-recreate "$svc"
