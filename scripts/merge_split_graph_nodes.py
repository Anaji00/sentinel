"""Collapse nodes that are one real thing stored as several, and then constrain.

WHY THERE ARE DUPLICATES
------------------------
The graph supervisor writes `MERGE (n:{label} {name: $name})`, so the *label is
part of the identity*. Two producers that disagree about what something is do
not merge into one node, they create two. Every producer picks its own label and
passes it in, and when it does not, the value falls to "Entity".

On top of that, the database has **no uniqueness constraints at all** -- 305,000
nodes, 20 RANGE indexes, zero constraints. A RANGE index makes a lookup fast; it
does not make a key unique. So two concurrent MERGEs on the same key both find
nothing and both create, and `BLACK SEA` exists twice with identical properties.

Measured scope:

    cross-label groups   2,737 groups /  5,717 nodes
    same-label groups      657 groups /  1,328 nodes

WHAT THIS SCRIPT WILL NOT DO
----------------------------
Not every pair of same-named nodes is a duplicate. 18 groups are labelled
`Company` and `Flag`, and they are:

    BZ  IQ  SE  AZ  DE  BN  MH  BB  KR  PH  AG  IR  AU  MA  PL  ET  CI  GH

Those are two-letter country codes that collide with real tickers. `DE` is both
Germany and Deere & Company; `KR` is South Korea and Kroger; `MA` is Morocco and
Mastercard; `IQ` is Iraq and iQIYI. Merging them would fuse a vessel registry
into an equity, and the result would look like perfectly ordinary data. `US`
(Country/Entity/Flag) and `RU` (Entity/Flag) are the same problem.

So this script merges only label combinations on an explicit allowlist, and
treats every other group as a collision to be reported rather than resolved.
A name being shared is evidence of nothing on its own.

USAGE
-----
    python scripts/merge_split_graph_nodes.py                 # report only
    python scripts/merge_split_graph_nodes.py --apply         # merge
    python scripts/merge_split_graph_nodes.py --constraints   # after merging

`--apply` mutates and deletes. It is never the default, and `--constraints` is
separate because a uniqueness constraint cannot be created while a duplicate it
would forbid still exists -- so the order is: report, merge, verify, constrain.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from typing import Dict, List, Optional, Sequence, Set, Tuple

try:
    from neo4j import GraphDatabase
except ImportError:  # pragma: no cover
    print("neo4j driver not installed", file=sys.stderr)
    raise


# Label combinations that denote one real thing recorded under different
# conventions. Anything not listed is reported and left alone.
MERGEABLE: Set[frozenset] = {
    # Same label twice: a plain MERGE race, no disagreement about what it is.
    frozenset({"Entity"}),
    frozenset({"Company"}),
    frozenset({"Vessel"}),
    frozenset({"Region"}),
    frozenset({"Flag"}),
    frozenset({"Aircraft"}),
    # A specific label against the generic one. The generic side is a producer
    # that declined to say what it was writing, not a claim that it is
    # something else.
    frozenset({"Company", "Entity"}),
    frozenset({"AutonomousSystem", "Entity"}),
    frozenset({"Entity", "Vessel"}),
    frozenset({"Entity", "Vulnerability"}),
    frozenset({"CryptoAsset", "Entity"}),
    frozenset({"Entity", "Region"}),
    frozenset({"Entity", "Prefix"}),
    frozenset({"Aircraft", "Entity"}),
    # BTCUSDT, ETHUSDT, XRPUSDT, DOGEUSDT: a crypto pair that one producer
    # called a Company. Verified by inspection before being listed.
    frozenset({"Company", "CryptoAsset", "Entity"}),

    # Wallets, after the relabel moved 255,677 Ethereum addresses off `Entity`.
    # A duplicate address is a plain MERGE race on a 40-hex key -- there is no
    # disagreement about what it is, and no other kind of thing is spelled that
    # way.
    frozenset({"Wallet"}),
    frozenset({"Entity", "Wallet"}),

    # Instruments the label resolver now types correctly, against the node the
    # old writer left behind under `Company`. These pairs did not exist before
    # this session: crude oil, gold, wheat, QQQ, SPY and TLT were all stored as
    # companies, and each now has a correctly-typed node beside the old one.
    #
    # Safe because it is the same symbol under an old wrong label and a new
    # right one -- the opposite of the Flag collisions, where one spelling
    # genuinely names two different things. LABEL_PRIORITY already ranks every
    # instrument label above Company, so the survivor is the typed node.
    frozenset({"Commodity", "Company"}),
    frozenset({"Commodity", "Company", "Entity"}),
    frozenset({"Company", "Index"}),
    frozenset({"Company", "Entity", "Index"}),
    frozenset({"Company", "MacroFactor"}),
    frozenset({"Company", "CryptoAsset"}),
}

# A shared name across these labels is a spelling coincidence, never an
# identity. Kept as a second, independent guard so that adding a combination to
# MERGEABLE by mistake still cannot fuse a country into a corporation.
NEVER_MERGE_ACROSS: Set[str] = {"Flag", "Country"}

# Which label survives, most specific first. "Entity" is last because it is the
# fallback that created most of these groups in the first place.
LABEL_PRIORITY: Sequence[str] = (
    "Vessel", "Aircraft", "CryptoAsset", "Commodity", "Index", "MacroFactor",
    "Company", "Vulnerability", "AutonomousSystem", "Prefix", "Region",
    "Sector", "Country", "Flag", "Organization", "Person",
    "Wallet", "InstitutionalFiler", "SupplyChainMetric",
    "Entity",
)

# Labels to constrain once the graph is clean.
CONSTRAIN_LABELS: Sequence[str] = (
    "Entity", "Company", "Vessel", "Aircraft", "Region", "Sector", "Flag",
    "CryptoAsset", "Commodity", "Index", "MacroFactor", "Vulnerability",
    "AutonomousSystem", "Prefix", "Country", "Organization", "Person",
    "Wallet", "InstitutionalFiler", "SupplyChainMetric",
)


FIND_GROUPS = """
MATCH (n)
WHERE coalesce(n.name, n.id) IS NOT NULL
WITH coalesce(n.name, n.id) AS nm,
     collect(DISTINCT head(labels(n))) AS labs,
     collect(elementId(n)) AS ids,
     count(*) AS c
WHERE c > 1
RETURN nm, labs, ids, c
"""

# Ordered so the survivor is chosen by the same rule the report printed:
# the preferred label first, then the node carrying the most relationships.
NODES_IN_GROUP = """
UNWIND $ids AS eid
MATCH (n) WHERE elementId(n) = eid
RETURN elementId(n) AS eid,
       head(labels(n)) AS label,
       size([(n)--() | 1]) AS degree
"""

# Everything needed to put a merged group back: each node's labels and
# properties, and every relationship it carries with the endpoint named.
#
# A whole-database dump was the first choice and is not available here --
# `apoc.export.file.enabled` is false and `neo4j-admin database dump` requires
# stopping the database. What has to be reversible is this operation, not the
# graph, and this covers exactly the nodes it touches.
BACKUP_GROUP = """
UNWIND $ids AS eid
MATCH (n) WHERE elementId(n) = eid
RETURN elementId(n) AS eid,
       labels(n) AS labels,
       properties(n) AS props,
       [(n)-[r]->(m) | {
           dir: 'out', type: type(r), props: properties(r),
           other: coalesce(m.name, m.id), other_labels: labels(m)
       }] AS out_rels,
       [(n)<-[r]-(m) | {
           dir: 'in', type: type(r), props: properties(r),
           other: coalesce(m.name, m.id), other_labels: labels(m)
       }] AS in_rels
"""

# apoc.refactor.mergeNodes UNIONS the labels of everything it merges, so the
# survivor comes out carrying every label in the group. That is not what the
# survivor rule decided: merging a `Company` and an `Entity` node is supposed to
# produce a Company, not a `:Company:Entity`.
#
# Found by running it. The flag repair merged eight ship registries into their
# Flag nodes and every one came back `["Company","Flag"]` -- edges intact,
# identity still ambiguous, and `head(labels(n))` (which four read paths use as
# the node's type) answering "Company" for the flag of Singapore.
#
# apoc.create.setLabels replaces the label set outright, so the survivor ends up
# with exactly the one label the plan printed.
MERGE_NODES = """
MATCH (survivor) WHERE elementId(survivor) = $survivor_id
UNWIND $doomed_ids AS did
MATCH (d) WHERE elementId(d) = did
WITH collect(DISTINCT survivor) + collect(d) AS nodes
CALL apoc.refactor.mergeNodes(nodes, {
    properties: 'discard',
    mergeRels: true
}) YIELD node
WITH node
CALL apoc.create.setLabels(node, [$survivor_label]) YIELD node AS relabelled
RETURN elementId(relabelled) AS eid
"""


def _label_rank(label: str) -> int:
    try:
        return LABEL_PRIORITY.index(label)
    except ValueError:
        return len(LABEL_PRIORITY)


def _classify(labels: Sequence[str]) -> Tuple[bool, str]:
    """(mergeable, reason). Reason is printed for anything skipped."""
    distinct = {l for l in labels if l}
    if not distinct:
        return False, "no labels"
    if len(distinct) > 1 and (distinct & NEVER_MERGE_ACROSS):
        shared = ", ".join(sorted(distinct & NEVER_MERGE_ACROSS))
        return False, (
            f"name collision across {shared} -- a country or flag code that "
            f"happens to be spelled like a ticker is not that ticker"
        )
    if frozenset(distinct) not in MERGEABLE:
        return False, f"label set {sorted(distinct)} is not on the allowlist"
    return True, ""


def _pick_survivor(rows: List[dict]) -> Tuple[str, List[str]]:
    """The node everything else folds into: best label, then highest degree."""
    ordered = sorted(rows, key=lambda r: (_label_rank(r["label"]), -r["degree"]))
    return ordered[0]["eid"], [r["eid"] for r in ordered[1:]]


def _write_backup(session, planned, path: str) -> None:
    """Dump every node in every planned group, with its relationships.

    Written as JSON lines, one group per line, so a partial file is still
    readable -- if the process dies halfway through, the groups already written
    are still recoverable, which a single JSON array would not give.
    """
    with open(path, "w", encoding="utf-8") as fh:
        for nm, labs, survivor_label, survivor_id, doomed in planned:
            rows = [dict(r) for r in session.run(
                BACKUP_GROUP, ids=[survivor_id, *doomed]
            )]
            fh.write(json.dumps({
                "name": nm,
                "labels_in_group": sorted(set(labs)),
                "survivor_id": survivor_id,
                "survivor_label": survivor_label,
                "doomed_ids": doomed,
                "nodes": rows,
            }, default=str) + chr(10))


def run(uri: str, user: str, password: str, apply: bool, limit: Optional[int],
        backup_path: str, backup_only: bool = False) -> int:
    driver = GraphDatabase.driver(uri, auth=(user, password))
    merged_groups = merged_nodes = 0
    skipped: Counter = Counter()
    examples: Dict[str, List[str]] = {}

    try:
        with driver.session() as session:
            groups = list(session.run(FIND_GROUPS))
            print(f"Found {len(groups)} duplicate name groups.\n")

            planned: List[Tuple[str, List[str], str, str, List[str]]] = []
            for rec in groups:
                nm, labs, ids = rec["nm"], rec["labs"], rec["ids"]
                ok, reason = _classify(labs)
                if not ok:
                    skipped[reason] += 1
                    examples.setdefault(reason, [])
                    if len(examples[reason]) < 6:
                        examples[reason].append(str(nm))
                    continue
                rows = [dict(r) for r in session.run(NODES_IN_GROUP, ids=ids)]
                if len(rows) < 2:
                    continue
                survivor, doomed = _pick_survivor(rows)
                survivor_label = next(r["label"] for r in rows if r["eid"] == survivor)
                planned.append((str(nm), labs, survivor_label, survivor, doomed))

            if limit:
                planned = planned[:limit]

            print(f"MERGEABLE: {len(planned)} groups, "
                  f"{sum(len(d) for *_, d in planned)} nodes to be removed.")
            for nm, labs, survivor_label, _sid, doomed in planned[:15]:
                print(f"  {nm:<28} {sorted(set(labs))} -> :{survivor_label} "
                      f"(absorbing {len(doomed)})")
            if len(planned) > 15:
                print(f"  ... and {len(planned) - 15} more")

            print(f"\nSKIPPED: {sum(skipped.values())} groups")
            for reason, count in skipped.most_common():
                print(f"  {count:>5}  {reason}")
                print(f"         e.g. {', '.join(examples.get(reason, []))}")

            if backup_only:
                # The backup without the merge, so it can be inspected and kept
                # before anything is deleted. Worth its own mode rather than
                # being a side effect of --apply: the point of a backup is to
                # exist at a moment when you can still decide not to proceed.
                print(f"\nWriting backup of {len(planned)} groups to {backup_path} ...")
                _write_backup(session, planned, backup_path)
                print("Backup written. Nothing was merged.")
                return 0

            if not apply:
                print("\nDry run. Nothing was changed. Re-run with --apply to merge.")
                return 0

            # The backup is written before anything is merged, and a failure to
            # write it stops the run. It is not behind a flag: a flag that
            # guards an irreversible operation is a flag someone forgets once.
            print(f"\nWriting backup of {len(planned)} groups to {backup_path} ...")
            try:
                _write_backup(session, planned, backup_path)
            except Exception as exc:
                print(f"Backup failed, nothing was merged: {exc}", file=sys.stderr)
                return 1
            print("Backup written.")

            print("\nApplying...")
            # The survivor chosen during planning is the one used here. Looking
            # it up again would let the plan that was printed and reviewed
            # differ from the merge that actually runs, which is the one thing
            # a dry-run-first script must not allow.
            for nm, _labs, _sl, survivor_id, doomed in planned:
                try:
                    session.run(
                        MERGE_NODES, survivor_id=survivor_id,
                        doomed_ids=doomed, survivor_label=_sl,
                    ).consume()
                    merged_groups += 1
                    merged_nodes += len(doomed)
                except Exception as exc:
                    print(f"  FAILED {nm}: {exc}")
            print(f"Merged {merged_groups} groups, removed {merged_nodes} nodes.")
    finally:
        driver.close()
    return 0


def add_constraints(uri: str, user: str, password: str, apply: bool) -> int:
    """Uniqueness on (label, name), which is what MERGE has been assuming.

    Per label rather than global, deliberately. A global uniqueness rule on
    `name` would forbid Germany and Deere from both being called DE, and they
    are genuinely different things that share a spelling. The constraint that
    matters is that there is only ever one `:Company {name: 'DE'}`.

    Two things have to happen first, and the first version of this did neither.

    A plain RANGE index on the same (label, property) blocks the constraint --
    Neo4j refuses with IndexAlreadyExists, because a uniqueness constraint
    creates its own backing index and will not sit on top of another. All
    twenty labels here already had one, so all twenty refused. The old index is
    dropped first and the constraint's own index replaces it, serving the same
    lookups.

    And duplicates are checked *before* the index is dropped rather than
    discovered after. Dropping an index and then failing to create the
    constraint would leave the label with neither.
    """
    driver = GraphDatabase.driver(uri, auth=(user, password))
    created = skipped = failed = 0
    try:
        with driver.session() as session:
            for label in CONSTRAIN_LABELS:
                dupes = session.run(
                    f"MATCH (n:{label}) WHERE n.name IS NOT NULL "
                    "WITH n.name AS nm, count(*) AS c WHERE c > 1 "
                    "RETURN count(*) AS groups"
                ).single()["groups"]
                if dupes:
                    skipped += 1
                    print(f"  skip :{label}: {dupes} duplicate name group(s) "
                          f"remain; merge them before constraining")
                    continue

                # The RANGE index standing in the constraint's way, by name --
                # the naming is not uniform (`entity_name_idx` beside
                # `idx_company_name`), so it is looked up rather than guessed.
                existing = [
                    r["name"] for r in session.run(
                        "SHOW INDEXES YIELD name, type, labelsOrTypes, properties, "
                        "owningConstraint "
                        "WHERE type = 'RANGE' AND owningConstraint IS NULL "
                        "AND labelsOrTypes = [$label] AND properties = ['name'] "
                        "RETURN name", label=label,
                    )
                ]

                stmt = (
                    f"CREATE CONSTRAINT uniq_{label.lower()}_name IF NOT EXISTS "
                    f"FOR (n:{label}) REQUIRE n.name IS UNIQUE"
                )
                if not apply:
                    for idx in existing:
                        print(f"  would run: DROP INDEX {idx}")
                    print(f"  would run: {stmt}")
                    continue

                try:
                    for idx in existing:
                        session.run(f"DROP INDEX {idx} IF EXISTS").consume()
                    session.run(stmt).consume()
                    created += 1
                    print(f"  ok   :{label}"
                          + (f" (replaced index {', '.join(existing)})" if existing else ""))
                except Exception as exc:
                    failed += 1
                    print(f"  FAIL :{label}: {exc}")
                    # Put the plain index back rather than leaving the label
                    # with no index at all.
                    for idx in existing:
                        try:
                            session.run(
                                f"CREATE INDEX {idx} IF NOT EXISTS "
                                f"FOR (n:{label}) ON (n.name)"
                            ).consume()
                            print(f"       restored index {idx}")
                        except Exception as restore_exc:
                            print(f"       COULD NOT RESTORE {idx}: {restore_exc}")
        if apply:
            print()
            print(f"{created} constraints created, {skipped} skipped, "
                  f"{failed} refused.")
            if skipped:
                print("A skip names a label that still holds duplicates. Re-run "
                      "the merge step and read what it refused.")
    finally:
        driver.close()
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true",
                    help="actually merge; without it nothing is changed")
    ap.add_argument("--constraints", action="store_true",
                    help="create uniqueness constraints (run after merging)")
    ap.add_argument("--backup-only", action="store_true",
                    help="write the pre-merge dump and stop, changing nothing")
    ap.add_argument("--backup", default="graph_merge_backup.jsonl",
                    help="where the pre-merge dump of affected nodes is written")
    ap.add_argument("--limit", type=int, default=None,
                    help="cap the number of groups, for a staged first run")
    ap.add_argument("--uri", default=os.getenv("NEO4J_URI", "bolt://localhost:7687"))
    ap.add_argument("--user", default=os.getenv("NEO4J_USER", "neo4j"))
    args = ap.parse_args()

    password = os.getenv("NEO4J_PASSWORD")
    if not password:
        print("NEO4J_PASSWORD is not set.", file=sys.stderr)
        return 2

    if args.constraints:
        return add_constraints(args.uri, args.user, password, args.apply)
    return run(args.uri, args.user, password, args.apply, args.limit,
               args.backup, args.backup_only)


if __name__ == "__main__":
    raise SystemExit(main())
