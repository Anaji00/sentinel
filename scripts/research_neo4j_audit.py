"""Read-only Neo4j graph audit (18 Nov). Run inside an existing container:

    MSYS_NO_PATHCONV=1 docker exec -i -w /app sentinel-api_gateway python - < scripts/research_neo4j_audit.py [section ...]

Sections are chosen with the AUDIT_SECTIONS env var (comma list); default is all.
Every statement is checked to be read-only before it is sent.
"""
import asyncio
import json
import os
import re
import time

from shared.db import get_neo4j

WRITE = re.compile(r"\b(MERGE|CREATE|DELETE|DETACH|SET|REMOVE|DROP|LOAD|FOREACH)\b", re.I)
BETA = ["SPY", "QQQ", "IWM", "DIA", "VOO", "IVV", "ES=F", "NQ=F", "YM=F", "RTY=F"]

SECTIONS = {
    "inventory": [
        ("labels", "MATCH (n) UNWIND labels(n) AS l RETURN l AS label, count(*) AS n ORDER BY n DESC LIMIT 60"),
        ("types", "MATCH ()-[r]->() RETURN type(r) AS rel, count(*) AS n ORDER BY n DESC LIMIT 60"),
        ("totals", "MATCH (n) WITH count(n) AS nodes MATCH ()-[r]->() RETURN nodes, count(r) AS rels"),
        ("rel_pattern", "MATCH (a)-[r]->(b) WITH labels(a)[0] AS la, type(r) AS t, labels(b)[0] AS lb, count(*) AS n RETURN la, t, lb, n ORDER BY n DESC LIMIT 60"),
        ("constraints", "SHOW CONSTRAINTS YIELD name, labelsOrTypes, properties, type RETURN name, labelsOrTypes, properties, type LIMIT 100"),
        ("indexes", "SHOW INDEXES YIELD name, labelsOrTypes, properties, type, state RETURN name, labelsOrTypes, properties, type, state LIMIT 150"),
        ("rel_props", "CALL db.schema.relTypeProperties() YIELD relType, propertyName, propertyTypes, mandatory RETURN relType, propertyName, propertyTypes, mandatory LIMIT 400"),
        ("node_props", "CALL db.schema.nodeTypeProperties() YIELD nodeLabels, propertyName, mandatory RETURN nodeLabels, propertyName, mandatory LIMIT 400"),
    ],
    "quality": [
        ("self_loops", "MATCH (a)-[r]->(a) RETURN type(r) AS rel, count(*) AS n ORDER BY n DESC LIMIT 30"),
        ("beta_edges", "MATCH (a)-[r]->(b) WHERE a.name IN $beta OR b.name IN $beta RETURN type(r) AS rel, count(*) AS n ORDER BY n DESC LIMIT 30"),
        ("beta_stat", "MATCH (a)-[r:STATISTICALLY_CORRELATED_WITH]->(b) WHERE a.name IN $beta OR b.name IN $beta RETURN count(*) AS n, min(r.created_at) AS first, max(r.created_at) AS last"),
        ("stat_total", "MATCH ()-[r:STATISTICALLY_CORRELATED_WITH]->() RETURN count(*) AS n, min(r.created_at) AS first, max(r.created_at) AS last, count(r.coefficient) AS has_coef, count(r.p_value) AS has_p, count(r.window) AS has_window, count(r.method) AS has_method, count(r.source) AS has_source"),
        ("reversed_stat", "MATCH (a)-[r:STATISTICALLY_CORRELATED_WITH]->(b) WHERE a.name < b.name AND exists((b)-[:STATISTICALLY_CORRELATED_WITH]->(a)) RETURN count(*) AS reversed_pairs"),
        ("reversed_peer", "MATCH (a)-[r:PEER_OF]->(b) WHERE a.name < b.name AND exists((b)-[:PEER_OF]->(a)) RETURN count(*) AS reversed_pairs"),
        ("peer_total", "MATCH ()-[r:PEER_OF]->() RETURN count(*) AS n, min(r.created_at) AS first, max(r.created_at) AS last, count(r.coefficient) AS has_coef, count(r.source) AS has_source"),
        ("granger_total", "MATCH ()-[r:GRANGER_CAUSES]->() RETURN count(*) AS n, min(r.created_at) AS first, max(r.created_at) AS last, count(r.lag) AS has_lag, count(r.f_stat) AS has_f, count(r.p_value) AS has_p, count(r.source) AS has_source"),
        ("hawkes_total", "MATCH ()-[r:HAWKES_EXCITES]->() RETURN count(*) AS n, min(r.created_at) AS first, max(r.created_at) AS last, count(r.branching_ratio) AS has_br, count(r.source) AS has_source"),
        ("per_type_props", "MATCH ()-[r]->() WITH type(r) AS t, count(*) AS n, count(r.source) AS src, count(r.weight) AS w, count(r.confidence) AS c, count(r.created_at) AS ca, count(r.updated_at) AS ua, count(r.method) AS m, count(r.coefficient) AS coef RETURN t, n, src, w, c, ca, ua, m, coef ORDER BY n DESC LIMIT 60"),
        ("source_by_type", "MATCH ()-[r]->() WITH type(r) AS t, coalesce(r.source,'<none>') AS s, count(*) AS n RETURN t, s, n ORDER BY n DESC LIMIT 60"),
        ("created_per_day", "MATCH ()-[r]->() WHERE r.created_at > timestamp() - 7*86400000 WITH type(r) AS t, date(datetime({epochMillis: r.created_at})) AS d, count(*) AS n RETURN t, toString(d) AS day, n ORDER BY day DESC, n DESC LIMIT 120"),
        ("created_per_day_source", "MATCH ()-[r]->() WHERE r.created_at > timestamp() - 3*86400000 WITH coalesce(r.source,'<none>') AS s, type(r) AS t, count(*) AS n RETURN s, t, n ORDER BY n DESC LIMIT 40"),
        ("updated_age", "MATCH ()-[r]->() WITH type(r) AS t, count(*) AS n, sum(CASE WHEN r.updated_at IS NULL THEN 1 ELSE 0 END) AS no_upd, sum(CASE WHEN r.updated_at < timestamp() - 30*86400000 THEN 1 ELSE 0 END) AS older30d, sum(CASE WHEN r.updated_at < timestamp() - 7*86400000 THEN 1 ELSE 0 END) AS older7d RETURN t, n, no_upd, older7d, older30d ORDER BY n DESC LIMIT 40"),
        ("junk_terms", "MATCH (a)-[r]->(b) WHERE a.name IN ['AI','EPS','GDP','CEO','IPO','ETF','SEC','FDA','USD','US','UK','EU','CPI','FED','NEWS'] OR b.name IN ['AI','EPS','GDP','CEO','IPO','ETF','SEC','FDA','USD','US','UK','EU','CPI','FED','NEWS'] RETURN labels(a)[0] AS la, a.name AS a, type(r) AS t, labels(b)[0] AS lb, b.name AS b, r.source AS s LIMIT 60"),
        ("source_named_nodes", "MATCH (n) WHERE toLower(n.name) CONTAINS 'seekingalpha' OR toLower(n.name) CONTAINS 'benzinga' OR toLower(n.name) CONTAINS 'reuters' OR toLower(n.name) CONTAINS 'marketwatch' OR toLower(n.name) CONTAINS 'yahoo' OR toLower(n.name) CONTAINS '_currents' RETURN labels(n)[0] AS l, n.name AS name, count{ (n)--() } AS deg LIMIT 40"),
        ("stat_non_company", "MATCH (a)-[r:STATISTICALLY_CORRELATED_WITH]->(b) WITH labels(a)[0] AS la, labels(b)[0] AS lb, count(*) AS n RETURN la, lb, n ORDER BY n DESC LIMIT 30"),
        ("duplicate_pairs", "MATCH (a)-[r]->(b) WITH a, b, type(r) AS t, count(*) AS c WHERE c > 1 RETURN t, count(*) AS pairs, sum(c) AS edges, max(c) AS maxc ORDER BY pairs DESC LIMIT 30"),
        ("stat_never_replicated_sample", "MATCH (a)-[r:STATISTICALLY_CORRELATED_WITH]->(b) RETURN a.name AS a, b.name AS b, r.coefficient AS coef, r.p_value AS p, r.created_at AS c, r.updated_at AS u ORDER BY r.created_at ASC LIMIT 10"),
        ("stat_sign_split", "MATCH ()-[r:STATISTICALLY_CORRELATED_WITH]->() RETURN sum(CASE WHEN r.coefficient > 0 THEN 1 ELSE 0 END) AS pos, sum(CASE WHEN r.coefficient < 0 THEN 1 ELSE 0 END) AS neg, avg(abs(r.coefficient)) AS mean_abs, avg(r.p_value) AS mean_p"),
    ],
    "entity": [
        ("dup_names", "MATCH (n) WHERE n.name IS NOT NULL WITH n.name AS name, collect(DISTINCT labels(n)[0]) AS ls, count(*) AS c WHERE c > 1 RETURN count(*) AS names_with_dups, sum(c) AS nodes, max(c) AS maxc"),
        ("dup_names_sample", "MATCH (n) WHERE n.name IS NOT NULL WITH n.name AS name, collect(DISTINCT labels(n)[0]) AS ls, count(*) AS c WHERE c > 1 RETURN name, ls, c ORDER BY c DESC LIMIT 25"),
        ("dup_case", "MATCH (n:Company) WITH toUpper(n.name) AS u, collect(n.name) AS names, count(*) AS c WHERE c > 1 RETURN u, names, c ORDER BY c DESC LIMIT 15"),
        ("untyped", "MATCH (n) WITH count(n) AS total, sum(CASE WHEN n.type IS NULL THEN 1 ELSE 0 END) AS no_type, sum(CASE WHEN n.id IS NULL THEN 1 ELSE 0 END) AS no_id, sum(CASE WHEN n.name IS NULL THEN 1 ELSE 0 END) AS no_name RETURN total, no_type, no_id, no_name"),
        ("label_vs_type", "MATCH (n) WHERE n.type IS NOT NULL AND NOT n.type IN labels(n) RETURN labels(n)[0] AS label, n.type AS type, count(*) AS n ORDER BY n DESC LIMIT 20"),
        ("orphans_by_label", "MATCH (n) WHERE NOT (n)--() RETURN labels(n)[0] AS l, count(*) AS n ORDER BY n DESC LIMIT 30"),
        ("degree_by_label", "MATCH (n) WITH labels(n)[0] AS l, count{ (n)--() } AS d RETURN l, count(*) AS nodes, avg(d) AS avg_deg, max(d) AS max_deg, sum(CASE WHEN d=0 THEN 1 ELSE 0 END) AS deg0, sum(CASE WHEN d=1 THEN 1 ELSE 0 END) AS deg1 ORDER BY nodes DESC LIMIT 30"),
        ("top_degree", "MATCH (n) WITH n, count{ (n)--() } AS d ORDER BY d DESC LIMIT 20 RETURN labels(n)[0] AS l, n.name AS name, d"),
        ("top_degree_types", "MATCH (n) WITH n, count{ (n)--() } AS d ORDER BY d DESC LIMIT 12 MATCH (n)-[r]-() RETURN n.name AS name, type(r) AS t, count(*) AS c ORDER BY name, c DESC LIMIT 80"),
        ("located_in", "MATCH (a)-[r:LOCATED_IN]->(b) RETURN labels(a)[0] AS la, labels(b)[0] AS lb, count(*) AS n, count(DISTINCT a) AS srcs, count(DISTINCT b) AS tgts ORDER BY n DESC LIMIT 15"),
        ("vessel_edges_share", "MATCH (v:Vessel)-[r]-() RETURN type(r) AS t, count(*) AS n ORDER BY n DESC LIMIT 10"),
        ("override_victims", "MATCH (n:Company) WHERE n.name =~ '.*=F' OR n.name IN ['SPY','QQQ','IWM','DIA','GLD','USO'] RETURN n.name AS name, count(*) AS n LIMIT 30"),
        ("ticker_twins", "MATCH (a:Company), (b:Entity) WHERE a.name = b.name RETURN a.name AS name LIMIT 20"),
    ],
    "followup": [
        ("company_named_like_flag", "MATCH (c:Company), (f:Flag) WHERE c.name = f.name RETURN count(DISTINCT c) AS company_nodes_sharing_flag_name"),
        ("company_flag_rels", "MATCH (c:Company)-[r]-(x) WHERE EXISTS { MATCH (f:Flag {name: c.name}) } WITH c, r, x WITH type(r) AS t, startNode(r) = c AS outgoing, labels(x)[0] AS lx, r.source AS s, count(*) AS n RETURN t, outgoing, lx, s, n ORDER BY n DESC LIMIT 25"),
        ("company_flag_top", "MATCH (c:Company) WHERE EXISTS { MATCH (f:Flag {name: c.name}) } WITH c, count{ (c)--() } AS d ORDER BY d DESC LIMIT 12 RETURN c.name AS name, d"),
        ("holds_into_2letter", "MATCH (f)-[r:HOLDS_STAKE_IN]->(c:Company) WHERE size(c.name) <= 2 RETURN c.name AS name, count(*) AS holders ORDER BY holders DESC LIMIT 15"),
        ("no_id_labels", "MATCH (n) WHERE n.id IS NULL RETURN labels(n)[0] AS l, count(*) AS n ORDER BY n DESC LIMIT 10"),
        ("no_type_labels", "MATCH (n) WHERE n.type IS NULL RETURN labels(n)[0] AS l, count(*) AS n ORDER BY n DESC LIMIT 10"),
        ("entity_names", "MATCH (n:Entity) WITH n, count{ (n)--() } AS d ORDER BY d DESC LIMIT 40 RETURN n.name AS name, d"),
        ("entity_names_all_sample", "MATCH (n:Entity) WHERE size(n.name) <= 4 OR n.name =~ '[A-Za-z ]+' RETURN n.name AS name LIMIT 60"),
        ("lowercase_company", "MATCH (c:Company) WHERE c.name <> toUpper(c.name) RETURN c.name AS name, count{ (c)--() } AS d LIMIT 25"),
        ("company_with_space", "MATCH (c:Company) WHERE c.name CONTAINS ' ' RETURN count(*) AS n"),
        ("self_loop_sympathy", "MATCH (a)-[r:SYMPATHY_MOVER]->(a) RETURN a.name AS name, r.source AS s, r.created_at AS c LIMIT 8"),
        ("self_loop_other", "MATCH (a)-[r]->(a) WHERE type(r) <> 'SYMPATHY_MOVER' RETURN type(r) AS t, a.name AS name LIMIT 8"),
        ("beta_by_name", "MATCH (a)-[r]->(b) WHERE a.name IN $beta OR b.name IN $beta WITH CASE WHEN a.name IN $beta THEN a.name ELSE b.name END AS beta, type(r) AS t, count(*) AS n RETURN beta, t, n ORDER BY n DESC LIMIT 40"),
        ("beta_peer_fresh", "MATCH (a)-[r:PEER_OF]->(b) WHERE (a.name IN $beta OR b.name IN $beta) RETURN sum(CASE WHEN r.created_at > timestamp() - 5*86400000 THEN 1 ELSE 0 END) AS created_last5d, sum(CASE WHEN r.updated_at > timestamp() - 5*86400000 THEN 1 ELSE 0 END) AS updated_last5d, count(*) AS total"),
        ("beta_granger_fresh", "MATCH (a)-[r:GRANGER_CAUSES]->(b) WHERE (a.name IN $beta OR b.name IN $beta) RETURN sum(CASE WHEN r.created_at > timestamp() - 5*86400000 THEN 1 ELSE 0 END) AS created_last5d, sum(CASE WHEN r.updated_at > timestamp() - 5*86400000 THEN 1 ELSE 0 END) AS updated_last5d, count(*) AS total"),
        ("beta_sympathy_fresh", "MATCH (a)-[r:SYMPATHY_MOVER]->(b) WHERE (a.name IN $beta OR b.name IN $beta) RETURN sum(CASE WHEN r.created_at > timestamp() - 5*86400000 THEN 1 ELSE 0 END) AS created_last5d, count(*) AS total, sum(CASE WHEN a=b THEN 1 ELSE 0 END) AS selfloops"),
        ("beta_peer_sample", "MATCH (a)-[r:PEER_OF]->(b) WHERE (a.name IN $beta OR b.name IN $beta) RETURN a.name AS a, b.name AS b, r.coefficient AS coef, r.method AS m, r.net_of AS net_of ORDER BY r.created_at DESC LIMIT 8"),
        ("granger_no_stats", "MATCH ()-[r:GRANGER_CAUSES]->() WHERE r.lag IS NULL RETURN coalesce(r.source,'<none>') AS s, coalesce(r.method,'<none>') AS m, count(*) AS n, min(r.created_at) AS first, max(r.created_at) AS last ORDER BY n DESC LIMIT 8"),
        ("granger_sig", "MATCH ()-[r:GRANGER_CAUSES]->() WHERE r.p_value IS NOT NULL RETURN count(*) AS n, sum(CASE WHEN r.p_value <= 0.01 THEN 1 ELSE 0 END) AS p01, sum(CASE WHEN r.p_value > 0.05 THEN 1 ELSE 0 END) AS p_gt05, avg(r.p_value) AS mean_p"),
        ("granger_bidir", "MATCH (a)-[r:GRANGER_CAUSES]->(b) WHERE a.name < b.name AND exists((b)-[:GRANGER_CAUSES]->(a)) RETURN count(*) AS bidirectional_pairs"),
        ("competes_reversed", "MATCH (a)-[r:COMPETES_WITH]->(b) WHERE a.name < b.name AND exists((b)-[:COMPETES_WITH]->(a)) RETURN count(*) AS reversed_pairs"),
        ("competes_total_pairs", "MATCH (a)-[r:COMPETES_WITH]->(b) RETURN count(*) AS edges, count(DISTINCT a) AS srcs"),
        ("sympathy_profile", "MATCH (a)-[r:SYMPATHY_MOVER]->(b) RETURN count(*) AS n, sum(CASE WHEN r.reasoning IS NOT NULL THEN 1 ELSE 0 END) AS has_reasoning, sum(CASE WHEN r.last_validated IS NOT NULL THEN 1 ELSE 0 END) AS validated, sum(CASE WHEN r.conviction IS NOT NULL THEN 1 ELSE 0 END) AS has_conv, avg(r.confidence) AS mean_conf, sum(CASE WHEN r.updated_at < timestamp()-14*86400000 THEN 1 ELSE 0 END) AS stale14d"),
        ("sympathy_sample", "MATCH (a)-[r:SYMPATHY_MOVER]->(b) WHERE a <> b RETURN a.name AS a, b.name AS b, r.confidence AS c, left(coalesce(r.reasoning,''),80) AS why LIMIT 6"),
        ("filer_dups", "MATCH (f:InstitutionalFiler) WHERE f.name =~ '(?i).*(blackrock|vanguard|state street).*' RETURN f.name AS name, count{ (f)--() } AS d LIMIT 20"),
        ("top_company_deg", "MATCH (c:Company) WITH c, count{ (c)--() } AS d ORDER BY d DESC LIMIT 12 RETURN c.name AS name, d"),
        ("top_filer_deg", "MATCH (c:InstitutionalFiler) WITH c, count{ (c)--() } AS d ORDER BY d DESC LIMIT 6 RETURN c.name AS name, d"),
        ("holds_dir_dups", "MATCH (f:InstitutionalFiler)-[r:HOLDS_STAKE_IN]->(c:Company) RETURN count(*) AS n, count(DISTINCT f) AS filers, count(DISTINCT c) AS companies, sum(CASE WHEN r.updated_at < timestamp()-45*86400000 THEN 1 ELSE 0 END) AS older45d"),
        ("supplier_edges", "MATCH (a)-[r:SUPPLIER_TO]->(b) RETURN a.name AS a, b.name AS b, r.source AS s, r.confidence AS c, r.created_at AS ca ORDER BY r.created_at DESC LIMIT 130"),
        ("customer_of", "MATCH ()-[r:CUSTOMER_OF]->() RETURN count(*) AS n"),
        ("persons_ins", "MATCH (p:Person)-[r:INSIDER_OF]->(c:Company) RETURN count(*) AS n, count(DISTINCT c) AS companies, count(DISTINCT p) AS persons"),
        ("cryptoasset_orphans", "MATCH (n:CryptoAsset) WHERE NOT (n)--() RETURN n.name AS name LIMIT 8"),
        ("crypto_company_dups", "MATCH (c:CryptoAsset), (d:Company) WHERE c.name = d.name RETURN c.name AS name LIMIT 10"),
    ],
}


async def main() -> None:
    wanted = [s for s in os.getenv("AUDIT_SECTIONS", "").split(",") if s] or list(SECTIONS)
    neo = await get_neo4j()
    for sec in wanted:
        print(f"===== {sec} =====", flush=True)
        for name, cy in SECTIONS[sec]:
            assert not WRITE.search(re.sub(r"'[^']*'", "''", cy)), f"write keyword in {name}"
            t0 = time.time()
            try:
                rows = await asyncio.wait_for(neo.query(cy, {"beta": BETA}), timeout=60)
                print(f"--- {name} ({time.time()-t0:.1f}s, {len(rows)} rows)")
                for r in rows[:120]:
                    print(json.dumps(r, default=str))
            except Exception as e:
                print(f"--- {name} FAILED after {time.time()-t0:.1f}s: {type(e).__name__}: {str(e)[:200]}")
    await neo.close()


asyncio.run(main())
