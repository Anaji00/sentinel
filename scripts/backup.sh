#!/usr/bin/env bash
# Sentinel backup — consistent snapshot of every durable store.
#
# Named Docker volumes are not a backup: they die with `docker compose down -v`,
# with a disk failure, and with a corrupt write. This produces a restorable
# artifact on the host filesystem.
#
# The audit ledger is dumped separately as well as inside the main dump. It is
# the compliance record behind trade execution, so it gets its own recoverable
# copy that does not depend on the larger file being intact.
set -euo pipefail

COMPOSE="docker compose"
STAMP="$(date -u '+%Y-%m-%dT%H%M%SZ')"
DEST="${BACKUP_DIR:-backups}/$STAMP"
PGUSER="${POSTGRES_USER:-sentinel}"
PGDB="${POSTGRES_DB:-sentinel}"

mkdir -p "$DEST"
echo "Snapshot -> $DEST"

# ── TimescaleDB ──────────────────────────────────────────────────────────────
# Custom format (-Fc): compressed and restorable selectively with pg_restore.
echo "  timescaledb ..."
$COMPOSE exec -T timescaledb pg_dump -U "$PGUSER" -d "$PGDB" -Fc > "$DEST/timescale.dump"

# Audit ledger, isolated. Plain SQL so it is readable and verifiable without
# the tooling — a compliance artifact should not require a working pg_restore.
echo "  audit ledger ..."
$COMPOSE exec -T timescaledb pg_dump -U "$PGUSER" -d "$PGDB" \
  --table=audit_ledger --data-only --column-inserts > "$DEST/audit_ledger.sql" 2>/dev/null \
  || echo "    (audit_ledger table absent — skipped)"

# ── Neo4j ────────────────────────────────────────────────────────────────────
# APOC cypher export keeps the graph restorable while the database is online.
echo "  neo4j ..."
$COMPOSE exec -T neo4j cypher-shell -u "${NEO4J_USER:-neo4j}" -p "${NEO4J_PASSWORD}" \
  "CALL apoc.export.cypher.all(null,{stream:true,format:'cypher-shell'}) YIELD cypherStatements RETURN cypherStatements" \
  > "$DEST/neo4j.cypher" 2>/dev/null || echo "    (export unavailable — APOC export may be restricted)"

# ── Redis ────────────────────────────────────────────────────────────────────
# BGSAVE then copy the RDB. Redis holds caches and rate-limit state; it is the
# least critical store here, which is why the ledger no longer lives in it.
echo "  redis ..."
$COMPOSE exec -T redis sh -c 'redis-cli -a "$REDIS_PASSWORD" BGSAVE' >/dev/null 2>&1 || true
sleep 2
$COMPOSE cp redis:/data/dump.rdb "$DEST/redis.rdb" 2>/dev/null || echo "    (no RDB yet)"

# ── Configuration ────────────────────────────────────────────────────────────
# Schema and compose travel with the data: a dump you cannot reconstruct the
# surrounding system for is only half a backup. Secrets are NOT copied.
cp docker-compose.yml "$DEST/" 2>/dev/null || true
cp shared/db/init.sql "$DEST/" 2>/dev/null || true

# ── Manifest ─────────────────────────────────────────────────────────────────
{
  echo "created_utc=$STAMP"
  echo "git_commit=$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
  echo "git_dirty=$(git status --porcelain 2>/dev/null | grep -c . || echo 0)"
  for f in "$DEST"/*; do
    [ -f "$f" ] && echo "file=$(basename "$f") bytes=$(wc -c < "$f")"
  done
} > "$DEST/MANIFEST"

# Checksums make silent corruption detectable at restore time.
( cd "$DEST" && sha256sum ./* > SHA256SUMS 2>/dev/null || true )

echo ""
echo "Complete: $(du -sh "$DEST" | cut -f1) in $DEST"
echo "Verify:   sha256sum -c $DEST/SHA256SUMS"
echo "Restore:  ./scripts/restore.sh $DEST"

# Retain the 14 most recent snapshots.
KEEP="${BACKUP_KEEP:-14}"
ls -1dt "${BACKUP_DIR:-backups}"/*/ 2>/dev/null | tail -n +$((KEEP+1)) | while read -r old; do
  echo "Pruning $old"; rm -rf "$old"
done
