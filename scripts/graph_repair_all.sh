#!/usr/bin/env bash
#
# The four graph repairs, in the only order they work in.
#
# Each step is scripted, dry-run-verified and evidence-backed; what they have in
# common is that they write to the shared Neo4j instance, which the agent
# session could not do. Run this from the repository root.
#
#   1. relabel wallets      255,518 nodes :Entity -> :Wallet
#   2. repair flag mislabel       9 nodes :Company -> :Flag  (agent's own bug)
#   3. merge split nodes      ~3,373 groups / ~3,628 nodes removed
#   4. add constraints            17 uniqueness constraints
#
# Order matters. Constraints go last because a uniqueness constraint cannot be
# created while a duplicate it would forbid still exists, and step 3 is what
# removes those duplicates. Steps 1 and 2 go before 3 so that the merge sees
# nodes under their final labels.
#
# Each step prints a plan and waits for confirmation. Nothing runs unattended.

set -euo pipefail

CONTAINER="${SENTINEL_AGENT_CONTAINER:-sentinel-agents-fast}"
NEO4J_CONTAINER="${SENTINEL_NEO4J_CONTAINER:-sentinel-neo4j}"
: "${NEO4J_PASSWORD:?set NEO4J_PASSWORD (see .env)}"
NEO4J_URI="${NEO4J_URI:-bolt://neo4j:7687}"

run() { docker exec -e NEO4J_PASSWORD="$NEO4J_PASSWORD" -e NEO4J_URI="$NEO4J_URI" "$CONTAINER" "$@"; }

confirm() {
  printf '\n>>> %s\n' "$1"
  read -r -p "Proceed? [y/N] " reply
  [[ "$reply" == "y" || "$reply" == "Y" ]]
}

echo "Staging scripts into $CONTAINER ..."
docker cp scripts/relabel_wallet_nodes.py    "$CONTAINER:/tmp/relabel.py"
docker cp scripts/merge_split_graph_nodes.py "$CONTAINER:/tmp/merge_split.py"

# ── 1. wallets ───────────────────────────────────────────────────────────────
echo; echo "=== STEP 1: relabel wallet nodes (dry run) ==="
run python /tmp/relabel.py
if confirm "STEP 1 will relabel the nodes above. It deletes nothing."; then
  run python /tmp/relabel.py --apply
fi

# ── 2. the agent's own mislabelling ──────────────────────────────────────────
echo; echo "=== STEP 2: fold ship registries back into :Flag ==="
echo "Nine :Company nodes created from flag codes; every edge on them is"
echo "REGISTERED_IN from a Vessel. Those edges are preserved by the merge."
if confirm "STEP 2 will merge those nine nodes into their :Flag nodes."; then
  docker exec -i "$NEO4J_CONTAINER" cypher-shell -u neo4j -p "$NEO4J_PASSWORD" \
    -f /dev/stdin < scripts/repair_flag_mislabel.cypher
fi

# ── 3. the split-node merge ──────────────────────────────────────────────────
echo; echo "=== STEP 3: merge split nodes (dry run) ==="
run python /tmp/merge_split.py
echo
echo "NOTE: this DELETES nodes. A backup of every affected group is written"
echo "first, automatically, and the run aborts if that backup cannot be saved."
echo "Groups labelled Company+Flag (DE=Germany/Deere, KR=South Korea/Kroger)"
echo "are refused by two independent guards and are NOT merged."
if confirm "STEP 3 will merge the groups listed above and remove the duplicates."; then
  run python /tmp/merge_split.py --apply --backup /tmp/graph_merge_backup.jsonl
  docker cp "$CONTAINER:/tmp/graph_merge_backup.jsonl" ./graph_merge_backup.jsonl
  echo "Backup copied to ./graph_merge_backup.jsonl"
fi

# ── 4. constraints ───────────────────────────────────────────────────────────
echo; echo "=== STEP 4: uniqueness constraints (dry run) ==="
run python /tmp/merge_split.py --constraints
echo
echo "A refusal here names a label that still holds duplicates. That is a"
echo "finding, not a failure: re-run step 3 and read what it skipped."
if confirm "STEP 4 will create the constraints listed above."; then
  run python /tmp/merge_split.py --constraints --apply
fi

# ── verification ─────────────────────────────────────────────────────────────
echo; echo "=== Verifying ==="
docker exec -i "$NEO4J_CONTAINER" cypher-shell -u neo4j -p "$NEO4J_PASSWORD" --format plain <<'CYPHER'
MATCH (n:Entity) RETURN 'Entity nodes remaining' AS check, count(*) AS value
UNION ALL
MATCH (n:Wallet) RETURN 'Wallet nodes' AS check, count(*) AS value
UNION ALL
MATCH (c:Company) WHERE EXISTS { MATCH (c)<-[:REGISTERED_IN]-(:Vessel) }
RETURN 'Companies with vessel registrations (want 0)' AS check, count(*) AS value;
CYPHER

# SHOW cannot appear inside a UNION, so it is its own statement. Checked
# against the running database rather than assumed -- the combined form parses
# as far as `SHOW` and then fails, which would have made the last line of this
# runbook an error message.
docker exec -i "$NEO4J_CONTAINER" cypher-shell -u neo4j -p "$NEO4J_PASSWORD" --format plain   "SHOW CONSTRAINTS YIELD name RETURN count(*) AS uniqueness_constraints;"

echo
echo "Done. Expected afterwards:"
echo "  Entity nodes remaining          ~3,100   (was ~258,000)"
echo "  Wallet nodes                  ~255,500   (was 0)"
echo "  Companies with vessel regs           0   (was 9)"
echo "  Uniqueness constraints              17   (was 0)"
