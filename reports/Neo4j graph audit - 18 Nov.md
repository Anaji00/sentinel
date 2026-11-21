# Neo4j graph audit, 18 Nov 2026

Read-only. Every number below was measured on the live graph on 18 Nov (about 21:00 UTC) with read Cypher through `sentinel-api_gateway`; scripts are `scripts/research_neo4j_audit.py` (sections `inventory`, `quality`, `entity`, `followup`), `research_neo4j_customers.py`, `research_neo4j_survival.py`, `research_neo4j_readers.py`, `research_neo4j_delisted.py`, `research_neo4j_junk.py`, `research_neo4j_summary.py`. Nothing was written, restarted or committed.

## Answer in one paragraph

Proper relations are being made for the structural half of the graph (13F holdings, Form 4 insiders, Finnhub peers, sector and country membership, 10-K supplier links, vessel and aircraft registry and position). They carry source, weight, confidence and timestamps, and they do not blow up: 67k proposals a day collapse to about 500 to 1,700 new edges a day (up to 8,300 on a 13F day), because the supervisor MERGEs. The statistical half is weaker. 88% of correlation edges have no replication record, the peer-graph path has no market-beta filter, nothing ever deletes an edge, and **four readers query a label (`:Entity`) that holds 0.6% of the nodes, so they silently read nothing.** The "AI" and "EPS" subjects never reached the graph (AI is C3.ai, a real ticker with 58 edges; there is no EPS node; no source-name nodes exist).

## 1. Inventory

70,849 nodes, 221,043 relationships (the "442,000 edges" quoted in `ontology.py:203` and `supervisor.py:516` is stale: Wallet, AutonomousSystem, Prefix and Vulnerability nodes are gone, RELATED_TO is 70 not 272,124). On disk: store 363 MB, transaction logs 515 MB. Neo4j container 1.47 GiB of 2 GiB (74%).

| Label | Nodes | Label | Nodes |
|---|---:|---|---:|
| Vessel | 25,542 | Entity | 458 |
| Aircraft | 25,007 | CryptoAsset | 347 |
| Company | 9,028 | Flag | 265 |
| Person | 8,362 | Country | 72 |
| InstitutionalFiler | 1,642 | Sector 53, Region 34, Commodity 14, Index 13, MacroFactor 8, SupplyChainMetric 3, DescriptiveEntityType 1 | |

| Relationship | Edges | Main pattern |
|---|---:|---|
| LOCATED_IN | 102,118 | Aircraft to Region 69,029; Vessel to Region 33,001 |
| REGISTERED_IN | 49,880 | Aircraft/Vessel to Flag 47,580; Company to Country 2,296 |
| HOLDS_STAKE_IN | 27,380 | InstitutionalFiler to Company (13F 23,735, 13G 3,115, 13D 530) |
| COMPETES_WITH | 19,770 | Company to Company (finnhub_peers 17,105) |
| INSIDER_OF | 9,152 | Person to Company (form4) |
| SYMPATHY_MOVER | 4,435 | Company to Company, LLM narrative |
| OPERATES_IN 2,461; GRANGER_CAUSES 1,913; PEER_OF 1,799; POSITIVE_EXPOSURE_TO 552; STATISTICALLY_CORRELATED_WITH 433; INVERSE_EXPOSURE_TO 352; HAS_EXPOSURE_IN 159; SUPPLIER_TO 122; AFFILIATED_WITH 109; OWNS 90; OPERATED_BY 83; CONTRACTED_BY 72; RELATED_TO 70; COMMODITY_EXPOSURE 43; HAWKES_EXCITES 12; ADJACENT_TO 12; LISTED_AS 11; MACRO_CORRELATED 10; FLAGGED_BY 2; TARGETS 2; MEMBER_OF 1 | | |

Constraints: 19 uniqueness constraints on `name` (Aircraft, AutonomousSystem, Commodity, Company, Country, CryptoAsset, Entity, Flag, Index, InstitutionalFiler, MacroFactor, Organization, Person, Prefix, Region, Sector, SupplyChainMetric, Vessel, Vulnerability, Wallet). Range indexes on `name` for Government, Industry, Location, RegulatoryAction, UnknownEntity. All ONLINE. Uniqueness is per label, so it does not stop the same name existing under two labels (section 3).

Written versus existing. `supervisor.py:528-598` writes nodes and relationships only through `MERGE (e:{label} {name})` and `MERGE (a)-[r:{relation}]->(b)`, labels from `ALLOWED_NODE_LABELS` (`ontology.py:96`), predicates from `VALID_PREDICATES` (`ontology.py:21`, 50 predicates). Present in the graph but outside the allowlist: `DescriptiveEntityType` (1 node). Allowed but empty: Wallet, AutonomousSystem, Prefix, Vulnerability, Organization, Government, Location, Industry, RegulatoryAction, UnknownEntity, and 27 of 50 predicates (including CUSTOMER_OF, SUPPLIES, FX_EXPOSURE_TO, OWNED_BY, SUBSIDIARY_OF). `knowledge_graph_engine` is disabled and wrote nothing identifiable.

Producers seen in `r.source`: 13F/13G/13D, form4, finnhub_peers, finnhub_profile2, ais, adsb, sec_edgar_13f, usaspending, edgar_submissions, callsign_designator, domain_bridge_prior, osint_bridges. **Blank source** on: LOCATED_IN 102,071 of 102,119, GRANGER_CAUSES 1,875 are `""` and 38 absent of 1,913, PEER_OF 1,799 (1,646 `""`, 153 absent), STATISTICALLY_CORRELATED_WITH 433, SYMPATHY_MOVER 4,435, OPERATES_IN 2,461, SUPPLIER_TO 101.

## 2. Quality by relationship type

New edges per day (UTC `created_at`, from the 12 Nov to 18 Nov window): 1,762; 4,948; 3,115; 482; 1,466; 8,345; 1,128 (18 Nov so far). Last three days by source: 13F 6,994, form4 1,366, location edges 844, 13G 748, ais 553, Granger 157, finnhub_peers 146, PEER_OF 87, sympathy 42, STATISTICALLY_CORRELATED_WITH 13, 13D 59. The vessel LOCATED_IN edges are 46% of all edges (102,118 of 221,043) but only about 300 new a day; `graph_writer.py:70-85` proposes once per region entry, so the 67k a day is almost all deduplicated before or at the MERGE.

| Check | Result |
|---|---|
| Self-loops | 118: SYMPATHY_MOVER 114 (NVDA to NVDA, GWW, PM, WCC, AMGN, ...), RELATED_TO 3 (XRP, XRPUSDT, DOGEUSDT), SUPPLIER_TO 1 (MAT to MAT) |
| Reversed duplicate pairs, symmetric relations | COMPETES_WITH 3,317 pairs stored both ways (6,634 of 19,770 edges); STATISTICALLY_CORRELATED_WITH 59 pairs; PEER_OF 0. GRANGER_CAUSES 470 bidirectional pairs (legitimate: two directional claims) |
| Same pair same type twice | only REGISTERED_IN, 22 pairs (44 edges) |
| Market-beta anchor on either end (SPY, QQQ, IWM, DIA, VOO, IVV, ES=F, NQ=F, YM=F, RTY=F) | 874 edges: GRANGER_CAUSES 530 (QQQ 263, SPY 257), PEER_OF 212 (QQQ 65, IWM 48, ES=F 48, NQ=F 46, SPY 5), SYMPATHY_MOVER 119, AFFILIATED_WITH 5, RELATED_TO 4, HAS_EXPOSURE_IN 4. STATISTICALLY_CORRELATED_WITH: **0**, so the 12-13 Nov filter holds on that path |
| Beta edges still being created | PEER_OF 30 created and 150 refreshed in the last 5 days; GRANGER 6 created, 40 refreshed; SYMPATHY_MOVER 0 created |
| Missing properties | STATISTICALLY_CORRELATED_WITH: coefficient, p_value, window, method on 433 of 433, source on 5 of 433 (as `""` on 428). PEER_OF: coefficient 1,799 of 1,799. GRANGER_CAUSES: lag, f_stat, p_value on 926 of 1,913 (987 older edges lack all three, first seen 28 Aug). COMPETES_WITH: confidence on 5 of 19,770. HOLDS_STAKE_IN, INSIDER_OF, SUPPLIER_TO, OPERATED_BY, CONTRACTED_BY: complete. SYMPATHY_MOVER: weight/confidence on 4,425 of 4,435 |
| Staleness (`updated_at` older than 30 days) | LOCATED_IN 85,596 of 102,119 (84%); REGISTERED_IN 36,825 of 49,881; COMPETES_WITH 10,976 of 19,770; AFFILIATED_WITH, OPERATED_BY, HAS_EXPOSURE_IN, MACRO_CORRELATED 100%; STATISTICALLY_CORRELATED_WITH 248 of 433; GRANGER 705 of 1,913; SYMPATHY_MOVER 1,712 older than 30 days, 4,056 older than 14 |
| Delisted or priceless endpoints (against `daily_bars`, newest bar 18 Nov) | No STATISTICALLY_CORRELATED_WITH, PEER_OF or GRANGER edge has an endpoint with no bar in 30 days. Endpoints with no daily bar at all are futures only (BZ=F, CL=F, ES=F, GC=F, NG=F, NQ=F, SI=F), which are expected. SYMPATHY_MOVER: 281 edges with an endpoint never in `daily_bars` (000550.SZ, 600006.SS, AGY.L, ...) and 21 stale. COMPETES_WITH: 4,600 edges (23%) have an endpoint with no daily bar, 372 more stale |
| Non-instrument nodes | Company `$325.18` (a price, 2 edges). 8 numeric Entity nodes. 119 Entity nodes with long free-text names ("cryptocurrency intelligence for usdt trading @ $1.00"). Entity nodes named market_anomaly (13 edges), finnhub_equities (14), crypto_transfer (8), watched_wallet_transfer (7), Headline, Network, Global. No source-name nodes (seekingalpha, benzinga, reuters: 0). AI is Company C3.ai (58 edges). No EPS node |

Edge survival (`edge_survival.py`, `statistical_discovery.py:884-980`). It gates publication, it does not prune. A new Pearson or Granger proposal is held in Redis, and released to Kafka only if the re-test passes. Totals in `sentinel:edges:survival`: 2,713 tested, 869 survived (32%); Pearson 540 of 933 (58%), Granger 329 of 1,780 (18%). Failure reasons: no_longer_significant 783, decayed_below_threshold 713, sign_reversed 348. Redis holds 410 registered, 338 held, 356 replicated. A failed edge is simply never sent. **No Python in `services/` or `shared/` contains a graph DELETE**, so an edge written before the gate existed (or one that later failed its refresh) stays. Replication of the edges in the graph: STATISTICALLY_CORRELATED_WITH 43 of 433 appear in the replicated hash (24 in the cohort, 380 in neither); GRANGER_CAUSES 200 of 1,913 (1,506 in neither); PEER_OF 77 of 1,799 (PEER_OF is not gated at all). Caveat: the replicated hash expires after 30 days and the cohort after 90 (`edge_survival.py:47,51`), so "in neither" means no record, not proven failure. `edge_validator.py` only adjusts `confidence` and `last_validated` (`:505-507`); 18 of 4,435 SYMPATHY_MOVER edges carry a validation.

## 3. Entity resolution

- Duplicate names across labels: 236 names exist more than once (504 nodes). Almost all are two-letter codes present as Country, Flag and Company (BH, EG, IR, NG, PH, CI, DE, OM, SG, TR, ...). 70 Company nodes share a name with a Flag. Most are real tickers (PG 83 edges, SO 64, ZM 62, PH 61, ES 57, MA 57), so the labels are correct; the cost is to readers that match on name without a label (section 4). Seven Flag nodes carry `type = 'Company'`.
- Other duplicates: Entity and Company twins XAU, SOXL, USDC, PFF; CryptoAsset and Company twins BTC and XRP; `ADOBE` and `adobe` (the lowercase one has 0 edges). Index/futures nodes still labelled Company: ES=F, IWM, GLD (from before `resolve_node_label`). Filer name variants: BLACKROCK (1,705 edges), "BLACKROCK, INC." (68), "BLACKROCK PORTFOLIO MANAGEMENT LLC" (5); VANGUARD 477, VANGUARD CAPITAL MANAGEMENT LLC 138, VANGUARD PORTFOLIO MANAGEMENT LLC 111.
- Label override: `graph.label_overridden_by_symbol` (`ontology.py:304`) is the counter for a producer insisting on a wrong label. The residue in the graph is the three Company-labelled instruments above, so the override works going forward; the counter firing hundreds of times is a producer still sending a wrong label, not a graph defect.
- Missing properties on nodes: no `type` on 886 (Entity 346, CryptoAsset 313, Flag 198, Region 27); no `id` on 3,271 (Vessel 2,368, Company 587, CryptoAsset 313).
- Orphans (degree 0): 1,415 nodes: Vessel 727, CryptoAsset 327, Company 268, Entity 92, Aircraft 1.
- Hubs, top 20 by degree: Regions SINGAPORE APPROACH 13,196, MEDITERRANEAN SEA 10,596, SOUTH CHINA SEA 10,329, BLACK SEA 9,467, TURKISH STRAITS 8,014, TAIWAN TERRITORIAL 7,269, UKRAINE AIRSPACE 6,124, TAIWAN STRAIT 5,917, STRAIT OF MALACCA 5,385, PERSIAN GULF 4,794, RUSSIAN AIRSPACE 3,554, RED SEA 2,934, STRAIT OF HORMUZ 2,852, GULF OF OMAN 2,031; Flags CHINA 5,370, SG 2,627, LR 2,609, PA 2,443, TURKEY 1,921, MH 1,858. All are the vessel and aircraft registry and position edges, which is what a flag or a chokepoint should hold. Top degree among companies is 197 (NVDA, GOOGL 190, AAPL 185, MU 182) and among filers 1,781 (FIDELITY, MORGAN_STANLEY 1,724, BLACKROCK 1,705). These are real.
- Vessel and region edges: LOCATED_IN is 102,118 edges, 46% of the graph, and REGISTERED_IN another 49,880 (23%); together 69%. They are append-only history: a vessel keeps an edge to every region it has entered (24,812 vessels, 33,002 edges; 24,208 aircraft, 69,029 edges) and nothing removes one when it leaves. The only code that names LOCATED_IN is the writer (`graph_writer.py:85`), `ref_data.py:518-545` and comments; the other readers take neighbours generically (section 4). 4,001 Company nodes (44%) are connected by COMPETES_WITH only.

## 4. Readers and whether their assumptions hold

| Reader | Assumption | Stored reality | Effect |
|---|---|---|---|
| `adversarial_wargamer.py:445` | `(a:Entity {id})-[*1..3]-(b:Entity)` | 458 nodes are `:Entity`; NVDA is `:Company` and has 0 matches (197 edges exist) | Wargamer's graph context is empty for every listed company |
| `macro_intelligence_engine.py:901` | `(e:Entity {type:'instrument'})` | 0 nodes have `type = 'instrument'` (9,035 have `type = 'Company'`) | Macro-asset exposure list always empty, fails closed by design, so no exposed tickers are ever paired |
| `statistical_discovery.py:180` | `(a:Entity)-[...]->(b:Entity)` for structural candidate pairs | 30 matches, against 26,902 for the same predicates on all labels | Structural seeding of Granger and cointegration candidates is effectively absent (the comment at `:183-195` describes filtering a flood that does not arrive) |
| `correlation/main.py:2965` | `(e:Entity {id})` degree for `centrality_mult` | NVDA degree via `:Entity` is 0, via name is 197 | `centrality_mult` is 1.0 for every listed company |
| `context_builder.py:329` | `(v)-[:FLAGGED_AS]->(f:Flag)` | 0 `FLAGGED_AS` edges (the vocabulary has FLAGGED_BY, 2 edges) | Flag rows always empty |
| `context_builder.py:315-326` | `LIMIT 100` over `MATCH (v)-[r]-(n)` with no ORDER BY | two tickers (NVDA, AAPL) return 382 rows | 74% of rows dropped, in storage order, not by weight or recency |
| `graph.py:99,254`, `context_builder.py:315`, `quant_trading_engine.py:923` | unlabelled `MATCH (n) WHERE n.name = $x` (or `toLower(...)`) | a bare two-letter code matches Company, Flag and Country | "PH", "PG", "SO" can return a flag and a company together |
| `quant_trading_engine.py:926` | `SUPPLIER_TO\|CUSTOMER_OF\|SUPPLIES\|PURCHASES_FROM` | only SUPPLIER_TO exists (122) | works, but 3 of 4 predicates are dead |
| `quant_trading_engine.py:1012`, `edge_validator.py:326` | statistical and exposure predicates | present | match what is stored |

Timing at current size, all through the same client: the context lookup 0.5 s, the graph explorer entity lookup 1.0 s (an all-nodes scan with `toLower`, linear in node count), the degree ranking over all 221k edges 1.6 s. Two-hop path counts: NVDA 49,782, MU 46,052, SINGAPORE APPROACH 50,812. Nothing exceeded 2 s. The `*1..3` query in the wargamer has `LIMIT 15` and returns early, and the explorer caps hops at 2. No query would time out today; the unlabelled name scans and the 3-hop pattern are the ones that grow first if the node count doubles.

Frontend: `frontend/src/lib/connections.ts` and `components/Connections.tsx` read `/graph/...` through the gateway; `StateStrip.tsx` and `SystemHealthHUD.tsx` read only the `neo4j_connected` flag.

## 5. Customer and supply-chain links

- Neo4j SUPPLIER_TO: 122 edges: 101 from the 10-K job and 21 `domain_bridge_prior` priors at 0.55 (NVDA to AMZN, TSM to QCOM, ...). CUSTOMER_OF: 0 (never written; the table's customers are written as SUPPLIER_TO from the supplier).
- `customer_links`: 265 rows, 227 with a ticker, 110 distinct supplier-to-customer pairs, filed 8 Nov 2023 to 24 Sep 2026; 217 by pattern, 48 by model; 38 rows with no resolved ticker (for example BWA to Volkswagen, DE to SAT).
- Table pairs missing from the graph: 14 of 110 (13%), for example AAOI to MSFT, AEIS to AMAT, AMKR to AAPL, AVGO to AAPL, ANET to META; both endpoints exist as Company nodes, and some pairs are linked by PEER_OF or STATISTICALLY_CORRELATED_WITH, so the 10-K edge itself was never written.
- Graph pairs not in the table, which are the name-gate removals of 12 Nov still present: 5, all consumer or retail pairs: ELF to TGT, FCFS to ONON, LII to ONON, REGN to ONON, RPM to ONON. They are still in Neo4j because nothing deletes.
- Edge content. The 101 10-K edges have `revenue_pct` on 0, `filed` on 0, `method` blank, `source` blank, confidence 1.0 and weight 1.0 on every one, including those extracted by the model. `customer_links_job.py:192-195` puts `revenue_pct`, `filed` and `method` in `data`, but the supervisor reads only `data["properties"]` for extras (`supervisor.py:496,501,509`), so they are dropped. The edges that look wrong on a read: KIM to BURL and ACI (a landlord's tenants), HST to DIS, SEI to EOG, PEP to KO (a competitor), MAT to MAT (self-loop).

## 6. Defects, ranked

1. **Four readers query `:Entity` and read nothing** (`adversarial_wargamer.py:445`, `macro_intelligence_engine.py:901`, `statistical_discovery.py:180`, `correlation/main.py:2965`). Scenario: a macro event on CL=F calls `_exposed_tickers`; the query needs `type:'instrument'` on an `:Entity`, finds 0 nodes, and returns `[]`, so no ticker is paired with the commodity. Likewise a wargame on NVDA gets no subgraph, and every company's `centrality_mult` is 1.0. Fix: match without the label (`MATCH (e {name:$id})`) or `:Company|Index|Commodity|CryptoAsset`, and test `e.type IN [...]` or the label. Safe additive for the wargamer. **Owner decision** for the macro pairing and `centrality_mult`, because both change which names are scored and how.
2. **PEER_OF has no market-beta filter and no survival gate** (`correlation/main.py:3281-3290` publishes `peer_graph.derive_peers` output directly; `peer_graph.py` contains no beta or index exclusion). 212 PEER_OF edges touch an anchor (QQQ 65, IWM 48, ES=F 48, NQ=F 46); 30 were created in the last 5 days. Scenario: AAPL PEER_OF ES=F at 0.60 is read by `quant_trading_engine.py:944` as a peer. Fix: apply `is_market_beta_pair` in the peer loop, and skip anchors in `build_candidate_pairs` for the peer path. Safe additive (drops writes); deleting the 212 old edges needs an owner decision.
3. **Nothing ever removes or marks an edge.** No Python DELETE exists. 88% of correlation edges (380 of 433) and 79% of Granger edges (1,506 of 1,913) have no replication record, and 18% of Granger re-tests survive. Scenario: a Pearson edge discovered 28 Aug at 0.62 that reversed sign in October is still returned by `context_builder.py:315` at its original confidence. Fix: a nightly job that stamps `r.replicated` and `r.last_survival_check` from the Redis hashes (additive), and a separate prune of failed edges (**owner decision**; deletion).
4. **Provenance is blank on most edges.** `r.source` is `""` or absent on 102,071 LOCATED_IN, all 433 correlation, 1,913 Granger and 1,799 peer edges, and 4,435 sympathy edges, so "who asserted this" cannot be answered, and the `domain_bridge_prior` mechanism at `supervisor.py:555-567` only helps producers that set it. Fix: have the supervisor default `source` from the Kafka message producer or topic key. Safe additive.
5. **SUPPLIER_TO loses its evidence** (`customer_links_job.py:192-195` against `supervisor.py:496`): 0 of 101 edges carry revenue share, filing date or method, all have confidence 1.0 including model-extracted ones, 14 table pairs are not in the graph, 5 gated pairs remain. Fix: send these keys inside `properties` and set confidence from `method` (pattern 1.0, model lower). Safe additive for new writes; backfill and the 5 removals need an owner decision.
6. **Self-loops and out-of-universe names get through the supervisor.** 118 self-loops (114 SYMPATHY_MOVER) and 281 SYMPATHY_MOVER endpoints with no price series (`$325.18`, `000550.SZ`). No `a = b` guard exists at `supervisor.py:492` or `:689`. Fix: skip a proposal whose two ids resolve to the same node; reject names that fail `looks_like_ticker`. Safe additive; removal of existing rows needs an owner decision.
7. **Truncated and ambiguous context reads.** `context_builder.py:315-326` returns the first 100 rows with no ORDER BY, shared across all requested entities (382 rows for two tickers); unlabelled name matches mix Flag, Country and Company for 70 codes. Fix: `ORDER BY coalesce(r.weight,0) DESC, last_updated DESC`, a per-entity limit, and a `:Company|Index|Commodity|CryptoAsset` label filter on ticker lookups. Safe additive.
8. **Dead predicate in the context builder** (`context_builder.py:329`, FLAGGED_AS has 0 edges, FLAGGED_BY has 2). Fix: use FLAGGED_BY or drop the query. Safe additive.
9. **Position edges are history, not state** (LOCATED_IN 102,118 edges, 84% older than 30 days; 69,029 aircraft edges for 24,208 aircraft). Nothing reads them as current position, and they are 46% of the graph. Fix, if a reader wants current location: an `is_current` flag set when a newer region edge is proposed, or a TTL prune. **Owner decision** (storage and meaning).
10. **Minor hygiene.** 886 nodes with no `type`, 3,271 with no `id`, 1,415 orphans, 458 Entity nodes holding extraction output (market_anomaly, finnhub_equities, `$0.80`), seven Flag nodes with `type='Company'`, COMPETES_WITH stored both ways for 3,317 pairs, filer name variants for BlackRock and Vanguard, stale cap comments at `ontology.py:203` and `supervisor.py:516`. All safe, low value.

## 7. Healthy

- The allowlist, label resolution and `MERGE`-by-label work: no Wallet, AS, Prefix or Vulnerability nodes remain, RELATED_TO fell to 70, no source-name or EPS nodes exist, AI is a real ticker, and only 3 mislabelled instruments are left.
- Dedup is effective: roughly 67k proposals a day produce 0.5 to 1.7k new edges (8.3k on a 13F day); uniqueness constraints hold; the same pair exists twice under one type only 22 times (REGISTERED_IN).
- The market-beta filter on STATISTICALLY_CORRELATED_WITH holds: 0 anchor edges; held proposals released at retest are filtered too (`statistical_discovery.py:949`).
- The structural sources are complete and well-formed: HOLDS_STAKE_IN, INSIDER_OF, OPERATED_BY, CONTRACTED_BY and the 13F OWNS edges carry source, weight, confidence and timestamps on 100%.
- Survival gating works for new Pearson and Granger edges: 2,713 re-tested, failures withheld (1,844), failure reasons recorded.
- No statistical edge touches an endpoint with no bar in 30 days; hubs are real chokepoints, flags and large filers.
- Performance: every read query tested finished in under 2 s; two-hop paths are bounded by about 50k.
