#!/usr/bin/env bash
# Sentinel restore — rehydrate from a snapshot produced by backup.sh.
#
#   ./scripts/restore.sh backups/2026-08-21T120000Z
#
# Destructive by nature: it overwrites live data. Requires explicit confirmation
# because an accidental restore is as damaging as an accidental drop.
set -euo pipefail

SRC="${1:-}"
COMPOSE="docker compose"
PGUSER="${POSTGRES_USER:-sentinel}"
PGDB="${POSTGRES_DB:-sentinel}"

[ -z "$SRC" ] && { echo "usage: $0 <snapshot-dir>"; exit 1; }
[ -d "$SRC" ] || { echo "not found: $SRC"; exit 1; }

echo ""
echo "RESTORE FROM $SRC"
[ -f "$SRC/MANIFEST" ] && sed 's/^/  /' "$SRC/MANIFEST"
echo ""

# Integrity first: restoring a corrupt dump over good data is the worst outcome.
if [ -f "$SRC/SHA256SUMS" ]; then
  echo "Verifying checksums ..."
  ( cd "$SRC" && sha256sum -c SHA256SUMS --quiet ) \
    && echo "  integrity OK" \
    || { echo "  CHECKSUM MISMATCH — refusing to restore corrupt data"; exit 1; }
fi

echo ""
echo "This OVERWRITES the live database, graph, and cache."
printf "Type 'restore' to proceed: "
read -r reply
[ "$reply" = "restore" ] || { echo "Aborted."; exit 1; }

echo ""
echo "Restoring timescaledb ..."
$COMPOSE exec -T timescaledb psql -U "$PGUSER" -d postgres \
  -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='$PGDB' AND pid<>pg_backend_pid();" >/dev/null
$COMPOSE exec -T timescaledb pg_restore -U "$PGUSER" -d "$PGDB" --clean --if-exists < "$SRC/timescale.dump"

if [ -f "$SRC/neo4j.cypher" ] && [ -s "$SRC/neo4j.cypher" ]; then
  echo "Restoring neo4j ..."
  $COMPOSE exec -T neo4j cypher-shell -u "${NEO4J_USER:-neo4j}" -p "${NEO4J_PASSWORD}" \
    "MATCH (n) DETACH DELETE n" >/dev/null 2>&1 || true
  $COMPOSE exec -T neo4j cypher-shell -u "${NEO4J_USER:-neo4j}" -p "${NEO4J_PASSWORD}" < "$SRC/neo4j.cypher" >/dev/null
fi

if [ -f "$SRC/redis.rdb" ]; then
  echo "Restoring redis ..."
  $COMPOSE stop redis >/dev/null
  $COMPOSE cp "$SRC/redis.rdb" redis:/data/dump.rdb
  $COMPOSE start redis >/dev/null
fi

echo ""
echo "Restore complete. Verifying ..."
./scripts/health.sh
