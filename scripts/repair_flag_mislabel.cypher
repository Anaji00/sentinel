// Fold ship-registry codes that were written as :Company back into their :Flag.
//
// WHAT HAPPENED
// A first version of resolve_node_label() let the instrument classifier
// override any producer label. Vessel flag codes are ticker-shaped, so the
// AIS enricher's correct `Flag` label was overruled and nine registries were
// created as companies during a 57-second window on 2026-09-17:
//
//   TW 21 edges   SG 4   VN 4   LR 2   PA 2   BS 1   HK 1   RW 1   TR 1 (x2)
//
// Every edge on every one of them is REGISTERED_IN from a Vessel -- 37 in
// total -- which is what makes this safe to automate: the selection below does
// not trust the timestamp alone, it requires that the node has edges and that
// *all* of them are vessel registrations. A real company with that shape does
// not exist.
//
// The fix in the code is `_REFINABLE` in shared/models/ontology.py: the symbol
// classifier may only refine a label that is already financial or absent, and
// tests/test_domain_bridges.py pins all eighteen colliding codes.
//
// The vessel edges are preserved by mergeRels -- they move to the Flag node,
// which is where they always belonged. The pre-repair state including every
// vessel MMSI is in flagfix_snapshot.txt.
//
// SELECTED BY SHAPE, NOT BY DATE
// This first gated on the 57-second window the bug ran in. The criterion is
// better than the timestamp and makes the window unnecessary: a node whose
// every edge is a vessel registration is a registry under any date. Dropping
// the window also catches MH, an older instance of the same corruption that
// predates the bug by weeks.
//
// It still refuses GH, which carries a vessel registration *and* an
// OPERATES_IN edge to a Sector. Something classified that one as a real company
// in an industry, so it is genuinely ambiguous -- the DE/KR problem again --
// and folding it into a flag would destroy the company half. It is left alone
// deliberately, and it needs a person rather than a rule.
//
// RUN:
//   docker exec -i sentinel-neo4j cypher-shell -u neo4j -p "$NEO4J_PASSWORD" \
//     -f /dev/stdin < scripts/repair_flag_mislabel.cypher
//
// Verify afterwards -- this should return zero rows:
//   MATCH (c:Company) WHERE size(c.name) = 2
//     AND EXISTS { MATCH (c)<-[:REGISTERED_IN]-(:Vessel) }
//   RETURN c.name;

MATCH (c:Company)
WHERE size([(c)--() | 1]) > 0
  AND all(x IN [(c)-[r]-(o) | type(r) + '|' + head(labels(o))]
          WHERE x = 'REGISTERED_IN|Vessel')
WITH c.name AS nm, collect(DISTINCT c) AS dupes
MATCH (f:Flag {name: nm})
WITH nm, dupes, collect(f)[0] AS survivor
CALL apoc.refactor.mergeNodes([survivor] + dupes, {
    properties: 'discard',
    mergeRels: true
}) YIELD node
RETURN nm,
       head(labels(node)) AS label_after,
       size([(node)--() | 1]) AS edges_after
ORDER BY nm;

// ── SECOND CASE: nodes that are genuinely both ──────────────────────────────
//
// TW, TR and GH carry vessel registrations *and* company evidence. TW is the
// Taiwan flag and Tradeweb Markets; TR is Turkey and Tootsie Roll; GH is Ghana
// and a company something placed in a sector. The peers fetcher added real
// COMPETES_WITH edges to the very nodes the label bug created, so these are now
// one node doing two jobs -- the DE/KR collision, arriving live.
//
// The merge above refuses them, correctly: folding Tradeweb into the flag of
// Taiwan would destroy the company. What they need is the opposite operation.
// Only the REGISTERED_IN edges move, to the Flag node where they belong; the
// Company node keeps its own edges and stays a company.
//
// apoc.refactor.to() redirects the END of a relationship, which is the right
// end here: the pattern is (vessel)-[:REGISTERED_IN]->(flag).
//
// RUN AFTER the merge above. Verify with:
//   MATCH (c:Company)<-[:REGISTERED_IN]-(:Vessel) RETURN c.name;   // want none

MATCH (v:Vessel)-[r:REGISTERED_IN]->(c:Company)
MATCH (f:Flag {name: c.name})
WITH r, collect(f)[0] AS flag
CALL apoc.refactor.to(r, flag) YIELD output
RETURN count(*) AS registrations_moved_to_flag;
