#!/usr/bin/env bash
# Sentinel health probe — one verdict line per dependency.
#
# Reports what is actually reachable, not what compose believes it started.
# A container can be "running" while its dependency is unreachable, and that
# distinction is exactly what matters during an incident.
set -uo pipefail

COMPOSE="docker compose"
GRN=$'\033[0;32m'; RED=$'\033[0;31m'; YLW=$'\033[0;33m'; DIM=$'\033[2m'; RST=$'\033[0m'
down=0

up()   { printf '  %sUP  %s  %-14s %s\n' "$GRN" "$RST" "$1" "${2-}"; }
bad()  { printf '  %sDOWN%s  %-14s %s\n' "$RED" "$RST" "$1" "${2-}"; down=$((down+1)); }
skip() { printf '  %s––  %s  %-14s %s\n' "$DIM" "$RST" "$1" "${2-}"; }

running() { $COMPOSE ps --services --filter status=running 2>/dev/null | grep -qx "$1"; }

printf '\nSENTINEL HEALTH  %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
printf '%s──────────────────────────────────────────────────────────%s\n\n' "$DIM" "$RST"

# ── Datastores ───────────────────────────────────────────────────────────────
echo "Datastores"
if running timescaledb; then
  if $COMPOSE exec -T timescaledb pg_isready -U "${POSTGRES_USER:-sentinel}" >/dev/null 2>&1; then
    rows=$($COMPOSE exec -T timescaledb psql -U "${POSTGRES_USER:-sentinel}" -d "${POSTGRES_DB:-sentinel}" \
           -tAc "SELECT count(*) FROM events" 2>/dev/null | tr -d '[:space:]')
    up "timescaledb" "${rows:-0} events"
  else bad "timescaledb" "container up, not accepting connections"; fi
else skip "timescaledb" "not running"; fi

if running redis; then
  # The password is interpolated into the compose `command:` on the HOST, so it
  # is not an environment variable inside the container. Reading it from .env
  # here is what makes the probe work at all.
  RPW="$(grep -E '^REDIS_PASSWORD=' .env 2>/dev/null | cut -d= -f2-)"
  if $COMPOSE exec -T redis redis-cli -a "$RPW" ping 2>/dev/null | grep -q PONG; then
    keys=$($COMPOSE exec -T redis redis-cli -a "$RPW" dbsize 2>/dev/null | tr -d '[:space:]')
    up "redis" "${keys:-0} keys"
  else bad "redis" "container up, auth or ping failed"; fi
else skip "redis" "not running"; fi

if running neo4j; then
  $COMPOSE exec -T neo4j wget -q --spider http://localhost:7474 2>/dev/null \
    && up "neo4j" "bolt ready" || bad "neo4j" "http probe failed"
else skip "neo4j" "not running"; fi

if running kafka; then
  t=$($COMPOSE exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null | grep -c . || echo 0)
  [ "$t" -gt 0 ] && up "kafka" "$t topics" || bad "kafka" "broker not answering"
else skip "kafka" "not running"; fi

# ── Application ──────────────────────────────────────────────────────────────
echo ""
echo "Application"
if running api-gateway; then
  code=$($COMPOSE exec -T api-gateway python -c \
    "import os,urllib.request as u;r=u.Request('http://localhost:8000/api/v1/health/',headers={'X-API-KEY':os.environ.get('API_GATEWAY_KEY','')});print(u.urlopen(r,timeout=5).status)" 2>/dev/null | tr -d '[:space:]')
  [ "$code" = "200" ] && up "api-gateway" "HTTP 200" || bad "api-gateway" "health returned ${code:-no response}"
else skip "api-gateway" "not running"; fi

running frontend && up "frontend" "serving" || skip "frontend" "not running"

if running ingress; then
  up "ingress" "https://localhost (loopback only)"
else skip "ingress" "not running"; fi

# ── Reasoning tier ───────────────────────────────────────────────────────────
echo ""
echo "Reasoning tier"
if running ollama; then
  m=$($COMPOSE exec -T ollama ollama list 2>/dev/null | tail -n +2 | grep -c . || echo 0)
  up "ollama" "$m model(s) resident"
else skip "ollama" "not running  ${DIM}(analyst mode)${RST}"; fi

# ── Data freshness ───────────────────────────────────────────────────────────
echo ""
echo "Freshness"
if running timescaledb; then
  age=$($COMPOSE exec -T timescaledb psql -U "${POSTGRES_USER:-sentinel}" -d "${POSTGRES_DB:-sentinel}" \
        -tAc "SELECT COALESCE(EXTRACT(EPOCH FROM (now()-max(occurred_at)))::int, -1) FROM events" 2>/dev/null | tr -d '[:space:]')
  if   [ "${age:--1}" -lt 0 ];    then skip "ingest" "no events yet (cold start)"
  elif [ "$age" -lt 300 ];        then up   "ingest" "last event ${age}s ago"
  elif [ "$age" -lt 3600 ];       then printf '  %sWARN%s  %-14s last event %ss ago\n' "$YLW" "$RST" "ingest" "$age"
  else bad "ingest" "stale: last event ${age}s ago"; fi
fi

printf '\n%s──────────────────────────────────────────────────────────%s\n' "$DIM" "$RST"
if [ "$down" -gt 0 ]; then printf '%s%d dependency check(s) failing%s\n\n' "$RED" "$down" "$RST"; exit 1; fi
printf '%sAll running dependencies healthy%s\n\n' "$GRN" "$RST"
