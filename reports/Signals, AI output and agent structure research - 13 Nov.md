# Signals, AI output and agent structure: research (13 Nov 2026)

Read-only. Stored data only (TimescaleDB, Redis, Kafka metadata, container logs, code). Two scratch scripts, both read-only: `scripts/research_family_cluster.py` (section A) and `scripts/research_llm_rubric.py` (section B). Nothing was restarted, committed or changed; both scripts ran in an existing container with under 1 GiB. Memory was checked first (`bash scripts/memory_watch.sh 80`: all under 80%, then timescaledb at 81% once, which only read queries touched). Free data, paper trading and the no-cyber rule are respected; every proposed model call states trigger, cap, admission check and consumer (section B). Interim notes for A, B and C went to the parent session; the (i) / (ii) / (iii) classes below match those notes: (i) additive and safe now, (ii) needs a registered test first, (iii) owner decision.

## A. Signals

### A1. Which producers carry information

The stored grades (`sentinel:grades:producers`, 90 days) test each alert as an independent observation against 1.0 and report "moves more". Alerts cluster on few days (`family_days`: 13 days for 2,939 peer-decoupling alerts, 4 for the US2Y rule, 35 for 6,679 semantic alerts), and a stock that fires an alert often has just moved, so those t values are inflated. I re-graded with a different design, fixed before running: one unit per (family, ticker, detection date); the 1- and 5-session abnormal-move ratio from the first open after detection net of SPY (the grader's own `move_ratio`); a same-day control (the mean ratio of 120 random stocks on that date); the statistic is the mean per-date excess over the control with a t over dates. 43,614 correlations became 19,749 units.

| Family (5-session unless noted) | Units | Dates | Mean excess over same-day control | t over dates | Reading |
|---|---|---|---|---|---|
| Peer Cointegration & Decoupling (1 session) | 2,935 | 11 | +0.43 | 3.16 | the one survivor; 54% of units on 3 days |
| Company News & Trading (1 session) | 148 | 9 | +0.28 | 1.97 | marginal, 9 days |
| Corporate Filing & Options Positioning | 56 | 22 | +0.45 | 1.34 | not shown |
| Headline Market Impact Convergence | 230 | 28 | +0.09 | 0.60 | none |
| Cross-Domain Semantic Convergence | 6,643 | 32 | +0.01 | 0.15 | none (stored grade says t = 5.0) |
| Geopolitical Cascade | 609 | 27 | -0.01 | -0.14 | none |
| Equity Block & Options Convergence | 2,191 | 35 | -0.10 | -1.95 | below control |
| Cross-Domain Hawkes Excitation | 3,141 | 16 | -0.09 | -2.01 | below control (stored: t = 9.65) |
| Insider Form 4 & Microstructure | 73 | 20 | -0.20 | -2.25 | below control |
| Market Anomaly Cluster | 287 | 32 | -0.26 | -4.16 | below control |
| Informed Trading Sequence | 31 | 16 | -0.43 | -3.36 | below control |
| Crypto Venue Stress | 51 | 26 | -0.59 | -4.49 | below control |
| Insider Sale Into Options Accumulation | 26 | 8 | -0.53 | -6.79 | below control |

So the "moves more" verdicts are mostly a volatility-regime and clustering effect; only the peer-decoupling rule carries a (concentrated) signal, and four to five families move significantly less than a random stock. The scenario statuses behave the same way: confirmed 1.43, developing 1.85, hypothesis 1.41, denied 0.96 against 1.0 at one session, on 42 to 106 scenarios and few days.

Agent calls (stored `agent_skill`, Heidke against chance): quant_trading_engine hit 0.448 on 402 resolved (z -2.43; mean -136 bps over 374 calls, t -3.6; **withdrawn as a finding, see the correction below: those statistics treat calls as independent, and clustered by date the effect is not significant**), macro engine 0.608 against a chance hit of 0.605 (it predicts "down" in 72 of 77 calls), analyst 0.526 against 0.518, analyst_shadow 0.500, wargamer entity claims -0.152 against the placebo (171 claims). The one standout, `stock_correlation_agent.baseline` (hit 0.59, +50 bps, t 6.3 on 824), rests on 808 ticker-days and has not been graded clustered by date.

### A2. Double counting

Last 14 days: 3,810 ticker-days carry 11,748 correlations (3.1 per ticker-day; 779, or 20%, have two or more distinct rules); 1,760 agent ticker-days carry 2,087 predictions (161 ticker-days with two or more agents, the four `stock_correlation_agent` variants emitting 825, 816, 194 and 85 rows). Across layers the overlap is small: 523 of the 1,760 agent ticker-days (30%) coincide with a correlation on the name; 11 of 55 scenario ticker-days have an agent call; 93 of 428 recommendation ticker-days have a correlation. The same story is therefore counted several times inside a layer (rules, agent variants) but the layers barely agree with each other.

### A3. Where information is lost between producer and alert

Hard counts from the suppressed-failure counters (the heartbeats) and the logs: the correlation agent refuses 9,129 correlations as non-equity, 1,672 as unquoted, 1,619 for no macro exposure, 749 under the cap, 326 over its sample cap; the correlation engine skips 804 market-beta pairs, 222 repeated sessions, 144 non-instruments; analyst_agent skips 296; the alert manager's log since 5 Nov holds 8,630 "WATCH (no alert)" lines and 1,209 "!!" lines, each of which is one message actually sent to Telegram (peer decoupling alone is 848 of the 1,209). The gate is `services/alert_manager/main.py:172-174`: a cluster whose tier is WATCH is logged and dropped. In the database that tier is the integer 1 (`ALERT_TIER_DB_VALUE` in `shared/models/events.py`: WATCH 1, ALERT 2, ELEVATED 3, INTELLIGENCE 4, CRITICAL 5), so the 7,413 tier-1 correlations of the last 7 days (74% of 9,953) are exactly the ones that can never alert; the ALERT-or-higher 26% then pass a per-rule, per-entity dedupe and a 10-an-hour cap per rule before Telegram. Most of this loss is deliberate and counted; the point is that the surviving stream is dominated by one family.

### A4. Candidate new signals from free data already stored

Existing model features already cover momentum, reversal, volatility, liquidity, short-volume ratio, options skew and spread, days to cover, earnings reaction, fundamentals, 8-K event types, insider buyers and sellers, filing-text change, comment letters, LLM news tone, customer momentum, text peers and 13D activism. What is stored but unused, with the history that decides testability:

| Candidate | Stored history | Prior from the literature | Testable now |
|---|---|---|---|
| 13F ownership breadth change (positions Q/Q, from `first_public`) | 28 quarters, 2019q4 to 2026q3, 16,406 tickers | Chen, Hong and Stein: falling breadth precedes low returns ([NBER 8151](https://www.nber.org/papers/w8151)); Choi, Jin and Yan find the sign depends on weighting ([NBER w16591](https://www.nber.org/papers/w16591)), so a two-sided test | yes |
| Form 144 planned insider sales | 21 days (1,723 notices) | negative drift plausible, no strong evidence found | no: register for 12 months |
| Analyst EPS estimate revisions | 31 daily snapshots since 9 Oct | revision momentum (Chan, Jegadeesk and Lakonishok, 1996) weaker in large caps | no |
| Borrow-fee changes | 32 days | high fees precede low returns (short-selling constraints literature) | no |
| Federal awards, lobbying filings | 2003 to 2026, 2,571 awards | weak, small-cap | low priority |

Registered test for the one testable candidate: feature = percentage change in reporting positions per ticker, Q/Q, visible from `first_public`; per rebalance date rank within the tradable set; statistic = mean spread of the top against the bottom quintile in 21-session excess return, date-clustered bootstrap (10,000 draws); bar |t| of at least 2 in both halves with the sign of the full sample; two-sided; if it passes it enters only as one extra feature through the existing walk-forward gate, never by hand.

### A5. One calibrated, uncertainty-aware score without the plateau problem

The plateau arises because a monotone map is extrapolated by holding the last knot flat past the support, so every high-raw name gets one forecast with no sense of how sure it is. Design that avoids it, built from pieces already in the repository:
1. Per cell (the five tradable cells, graded families, the model percentile) take the posterior mean of the net signed return from `hier_bayes` (zero, family, size band), with effective n from weekly units (the corrected size-band study already does this).
2. Add a split-conformal half-width per cell from a blocked calibration set of weekly units (non-overlapping horizons; 35-day blocks for 30-day cells), using recency-weighted quantiles to allow for drift ([Barber et al., Annals of Statistics 2023](https://doi.org/10.1214/23-AOS2276)) and an online miscoverage update ([Gibbs and Candès 2021](https://arxiv.org/abs/2106.00170)).
3. Report the lower bound (posterior mean minus half-width) as the score and show the width; mark a cell "flat" when its support is thin (the `calibration_flat` flag already exists). Never extrapolate: cap at the highest supported knot's lower bound.
4. Do not add signals that share a source: take the largest lower bound among independent sources and list the others as corroboration.
Acceptance (to register): over a rolling 20 weeks the 80% interval covers 70% to 90% of realised weekly net signed returns; a positive lower bound requires at least 30 effective weekly units; no cell's score rises when a duplicate source is added.

## B. AI output

### B1. Rubric (fixed before sampling)

R1 at least two numeric facts in the free text; R2 a falsifiable claim (scenarios: a watch or deny signal with a numeric threshold and a comparator; analyst: p_beat at least 0.03 from 0.5; correlation agent: a non-neutral direction); R3 cited evidence exists (scenario event ids resolve in `events` or `signal_event_archive`, at least 80%); R4 probabilities are not templated (a triple used by three or more scenarios); R5 named entities are in the packet; R6 length and tokens; R7 a consumer exists.

### B2. Sample and results

Hand-read: 46 outputs (6 scenarios in full plus 12 more headlines, 3 wargamer, 5 analyst or shadow, 3 rule synthesizer, 3 correlation agent, 3 macro, 3 quant, 4 divergence briefs, 6 llm_news). Programmatic over 14 days: 63 scenarios, 141 analyst and 234 shadow rows, 105 correlation-agent and 32 wargamer outputs (96 moves), 36 rule-synthesizer runs (73 rules), 14,360 llm_news scores.

| Producer | Pass R1, R2, R3, R5 | Findings |
|---|---|---|
| scenario_generator (63) | 100%, 98%, 100%, 98% | form is fine; 14 (22%) state an "Overall confidence N%" in the text that differs from the stored `confidence_overall`; 14 (22%) label themselves artifact, noise or weak ("Algorithmic Spurious Cointegration Artifact (70%)"); the 55/30/15 triple (20 scenarios, 4 to 12 Nov) has stopped, but round triples repeat (60/30/10 x8, 65/25/10 x7, 60/25/15 x5); median 1,081 characters; about 4.4k tokens a scenario |
| analyst_agent (local, 141) | 43% have two or more numbers | 91% of p_beat values lie in 0.47 to 0.53; median latency 61 s; consumer: grading only (its `agent_predictions` rows feed `producer_grades`); the Redis list `sentinel:analyst:views`, commented "for the dashboard", has no reader in the gateway or the frontend |
| analyst_shadow (hosted, 234 rows) | 83% | 38% outside 0.47 to 0.53, 24 distinct values; chance skill; same-second duplicate rows (140) are logged repeats of one batch output, not extra calls |
| stock_correlation_agent (105) | evidence_for non-empty 77% | conviction 0.5 in 46, 0.4 in 24; one value of 3.2 accepted (range is 0 to 1); expected_direction neutral in 43 |
| wargamer v2 hosted (32 runs, 96 moves) | numbers present | disruption_potential_percent templated (85 x31, 70 x26, 40 x17); content is adversary role-play ("coordinated disinformation campaign"); no falsifiable claim; consumer: one dashboard route |
| rule_synthesizer (36 runs, 73 rules) | n/a | 60% of rules have min_anomaly at or below 0.4; 15 proposed at ALERT tier; generic names ("Natural Gas Price Spike to Microsoft Equity Block Convergence") whose families only ever reach WATCH |
| macro engine (77) | n/a | 72 "down", 5 "up"; 13 distinct confidences (0.6101 for AMD and for NBIS) |
| quant engine (30) | n/a | 4 distinct confidences (0.4577 for three different names) |
| llm_news (14,360) | n/a | scores only -1, 0, +1 (2,142, 7,456, 4,762); 23% of headlines (3,317) are analyst price-target notices; when the LLM scores a "Raises price target" headline it is consistent (15 of 436 non-stale rows, 3.4%, scored 0; 399 scored +1; "Lowers" averages -0.36); 17% of rows carry a "#stale" model tag and are scored 0 by default with no event type (540 of the 976 "Raises" rows in one 7-day window), which is what an earlier reading of this table mistook for LLM inconsistency |
| divergence briefs (87, deterministic) | all numbers present | the best-formed output: gap, sigma, peers, catalyst and a rule-derived regime label; 58 predate the 12 Nov regime annotation |

### B3. Cost against use

Seven days, hosted tokens 2.19 M: llm_news 976k (45%), comment_letter 276k (13%), stock_correlation_agent 256k (12%), scenario_generator 240k (11%), analyst_shadow 136k, rule_synthesizer 124k, 13D 88k, wargamer 59k. Local model time 13,001 s, of which analyst_agent 11,733 s (90%) for about 86 distinct outputs (136 s each). Tokens per useful output: a scenario costs 4.4k tokens and a confirmed one 34k (7 of 55); peer-decoupling alerts, the only informative family, cost roughly 800 hosted tokens plus 14 local seconds each.

### B4. Fixes that need no extra inference

1. Schema validation with rejection and a counter on every LLM output (class i): numeric fields range-checked (conviction 0 to 1), narrative confidence must equal the stored field, probabilities summing to 100, repeated triples flagged, p_beat within 0.03 of 0.5 marked "no view".
2. Scenario admission gate (class ii): do not call the generator when the correlation is an unrelated-pair cointegration (macro or bond ETF against a single stock, entities with no sector relation) or a non-company subject; the generator already concludes "artifact" for 22%. Trigger: tier-1 correlation on a company entity; dedupe: one per ticker-day; cap: the current daily cap; consumer: alert manager and dashboard.
3. Deterministic scoring of analyst price-target headlines (class ii) with registration text sent in interim B.
4. Numbers-first packets: keep `regime_packet` as the template (divergence briefs show it works) and pass the same packet to the scenario generator; ask the model only for the mechanism and falsifiers, not for probabilities (the probabilities are weak: verbalised confidence is better than raw token probabilities but still needs measuring, [Tian et al. 2023](https://arxiv.org/abs/2305.14975)).
5. Retire or gate outputs with no consumer or no skill (class iii): analyst_agent local path, wargamer v2.
6. Caching and dedupe: the telemetry duplicates are logging; real dedupe gap is the four stock_correlation_agent variants on one ticker-day (merge into one call).
7. A verification pass on scenario claims in the style of Chain-of-Verification ([Dhuliawala et al. 2023](https://arxiv.org/abs/2309.11495)) would add a model call, so state it only as an option: trigger = scenarios that pass admission and have confidence of at least 70; cap = 10 a day; admission = numeric self-consistency fails the deterministic check; consumer = the scenario status. Not recommended until the free post-checks above are in.
Where a smaller or hosted model suffices: the correlation agent already runs on the hosted 20b model at 1.5 s; the analyst's local path should move to the existing hosted shadow (decision 1 of Addendum 1).

## C. Correlation, reasoning and agentic structure

### C1. Flow with measured counts (7 days)

Events 66,867 -> enrichment (same) -> correlations 9,953 (tier 1 7,413; tier 2 2,531; tier 3 9) -> alert manager (tier WATCH = database tier 1 = 7,413 of 9,953, dropped at `alert_manager/main.py:172-174`; the rest pass dedupe and the 10-an-hour-per-rule cap; since 5 Nov, 1,209 Telegram messages sent) -> scenarios 55 (7 confirmed, 1 denied) -> agent predictions 858 -> recommendation ideas 234 -> in-app notification rows 12 (19 in total; Telegram is the live human channel, token and chat id set, with timeouts and retries on the host's flaky DNS). Kafka (3-day retention): raw maritime 841,609, tradfi 238,643, enriched 102,960, ontology proposals 201,544, correlations 14,866, scenarios 63, agent outputs 32, rules synthesized 80, intel briefs 0, alerts.outbound 0.

### C2. No consumer, no producer, redundant

- No producer or consumer: `alerts.outbound` (143,745 lifetime offsets, nothing in code publishes or reads it); `agents.ontology.updates` and `agents.ontology.unknown_entities` (no consumer group; the knowledge-graph engine is disabled); `agents.intel.briefs` has no producer.
- Output read only by itself or by the dashboard: `agents.consensus.reports` (consensus engine only); analyst views (dashboard list); wargamer predictions (one dashboard route).
- Fan-out: `sentinel.correlations` has 11 consumer groups, about 163k handler invocations per 3 days, yet 74% of correlations are WATCH tier (database tier 1) and can never alert. `agent-quant-trading` subscribes to 12 topics and is worse than chance.
- Redundant: peer divergence is answered by the decoupling rule, four correlation-agent variants, the scenario generator, two analysts, the divergence watch and a recommendation: seven voices for one idea.
- Bottlenecks: supervisor is one consumer for about 67k graph proposals a day (lag zero at the last look); `radar_agent` logs 828 slow dispatches; both are watch items, not defects.
- Deterministic replacement candidates: price-target headline scoring, rule synthesis from a fixed catalogue (the model proposes 66 distinct names a week, most never reach ALERT), macro and rates briefs (already deterministic), divergence briefs (already deterministic).

### C3. Proposed target structure

| Box | Owns | Inputs | Test of value |
|---|---|---|---|
| Collectors and enrichment | raw events, scores | free feeds | freshness, no dead or constant scores |
| Signal registry (deterministic) | the graded producers: peer decoupling, the five tradable cells, the factor model; everything else is stored "watch only" | events, bars | day-clustered excess over a same-day control, n_eff shown |
| Evidence packet builder (deterministic) | one numbers-first packet per ticker-day | registry, filings, insider, options, short, model rank | every number traceable; a missing item is reported as missing |
| One hosted explainer | mechanism and falsifiers for alert-tier items after admission and dedupe | the packet | schema check, numeric consistency, later grading of falsifiers |
| B1 trader and idea list | factor model picks; ideas with calibrated lower bounds | model, registry | net Sharpe on strict accounting; interval coverage |
| Dashboard and audit trail | the packet, the explanation, the grades | all of the above | every card carries its n_eff and date |

Delete or retire (owner): `alerts.outbound`, the two ontology topics if the knowledge-graph engine stays off, wargamer v2, analyst_agent local path, Merge: the four correlation-agent variants into one call; analyst and shadow into the shadow. Gate: agents read ALERT-tier correlations only; generic WATCH-only rules stop being fanned out; scenario generation behind the admission gate; llm_news and comment_letter behind an add-or-stop test (neither is read by the live model).

### C4. Practice that changed a recommendation

- Multi-agent versus single: failures in multi-agent systems come mostly from system design, not model ability, and gains over a single agent are often small ([Cemri et al., MAST, 2025](https://arxiv.org/abs/2503.13657)); that supports collapsing the seven voices into one explainer on a deterministic packet.
- Calibration: verbalised confidence beats raw token probabilities but is not reliable by itself ([Tian et al. 2023](https://arxiv.org/abs/2305.14975)), so every probability the model writes should be graded, not trusted.
- Verification: separate verification questions answered without the draft reduce hallucination ([Dhuliawala et al. 2023](https://arxiv.org/abs/2309.11495)); here deterministic checks are cheaper and come first.
- Time series intervals: weighted and adaptive conformal methods cope with drift ([Barber et al. 2023](https://doi.org/10.1214/23-AOS2276); [Gibbs and Candès 2021](https://arxiv.org/abs/2106.00170)).
- Correlation versus causation in event graphs: the day-clustered, same-day-control grade in section A is the control I could apply with stored data; a causal claim in the graph (a "decoupling cause") stays a hypothesis until it passes that grade.

## Top 10 (ranked by benefit x confidence / effort)

| # | Recommendation | Evidence | Class | Risk | Falsifying test |
|---|---|---|---|---|---|
| 1 | Grade families day-clustered with a same-day control; publish as extra keys, retire on it only after the test | only 1 of 14 families survives; stored t values 5 to 26 against 13 to 35 dates | (i) publish, (ii) retire | low | per-date one-sided t under 1.65 in both halves retires a family |
| 2 | Retire the analyst local path and wargamer v2 | 90% of local model time for 86 outputs; 91% of p_beat near 0.5; wargamer skill -0.152 | (iii) | none | n/a (stop; 20 independent days to re-admit) |
| 3 | Validate every LLM output and count rejects | 22% confidence mismatch; conviction 3.2 accepted; repeated triples | (i) | none | reject rate reported weekly |
| 4 | Gate agent input to ALERT-tier correlations | 11 groups read 14,866 messages in 3 days, 74% are WATCH tier (7,413 of 9,953 in 7 days) | (ii) | graded outputs could change | at least 90% overlap of graded predictions under the replay |
| 5 | Test whether llm_news_30 and ev_comment_letter help the walk-forward at all; if not, stop the calls | 45% and 13% of hosted tokens; both are stored features that the live model does not read | (ii) | none to the live model | adding the feature gains at least 0.03 net Sharpe on 6 of 8 calendars or the calls stop |
| 6 | Deterministic price-target scoring (token saving only) | 23% of headlines are price-target notices; the LLM scores them consistently when it scores (3.4% zero), so this saves tokens, not quality | (ii) | feature series change | not worse by 0.02 Sharpe on 6 of 8; moot if the llm_news ablation stops the calls |
| 7 | Scenario admission gate (unrelated-pair cointegration, non-company subjects) | 22% self-labelled artifact; "AI" and MSFT-versus-TLT cases | (ii) | fewer scenarios | confirmed-scenario count unchanged, total calls down 20% or more |
| 8 | Keep the quant engine's current 24-hour calls under the registered date-clustered skill test; no demotion yet (corrected 14 Nov) | date-clustered over 29 days: -156 bps, t -1.37; the 24-hour arm (the only horizon emitted since 14 Oct) +9 bps, t 0.36; the losses sit in 56 thirty-day calls from one week (8 to 14 Oct) | (ii) | none | clustered Heidke lower 90% bound above 0 over 20 independent days, else demote |
| 9 | Topic and consumer audit guard test; delete dead topics | alerts.outbound, ontology.updates, unknown_entities, briefs | (i) guard, (iii) delete | none | test fails on a new topic with no producer or consumer |
| 10 | Calibrated lower-bound score (shrinkage plus weighted conformal) and the 13F breadth test | plateau; breadth has 28 quarters | (ii) | interval miscoverage | 80% interval covers 70% to 90% over 20 weeks; breadth |t| of at least 2 in both halves |

## Looked at and sound

Scenario form (numbers, thresholds, evidence ids, entities all at 98% to 100%); the deterministic divergence brief; hosted routing and quotas; Kafka lag zero on the topics read; the correlation agent on the hosted 20b model at 1.5 s; the market-beta skip in discovery; the days-to-cover trial's handling after amendment DTC-1.

## Not done or not measured

Whether the Telegram messages are read or acted on (1,209 sent since 5 Nov, about 140 a day, with timeouts in the log); value of the Neo4j graph beyond customer links; the clustered grade of `stock_correlation_agent.baseline`; any test that needs the heavy panel (all are proposals with registration text, none run).

## Consumers: what reads each output, and which tokens have one

### Share of LLM spend whose output is read by something other than a display or a grade (7 days)

| Producer | Hosted tokens (7 d) | What reads it |
|---|---|---|
| llm_news | 976k | stored feature `llm_news_30` in `stock_features_daily` (not in the live model's 29-feature list); case files |
| comment_letter job | 276k | stored feature `ev_comment_letter` (not in the live model's feature list) |
| scenario_generator | 240k | alert manager (scenario briefing, `scenarios.generated`), reasoning, wargamer and rule_synthesizer subscriptions, dashboard, grading |
| rule_synthesizer | 124k | the correlation engine's dynamic rules (which then create WATCH correlations) |
| 13D reader | 88k | stored feature `ev_13d_activist` (not in the live model's feature list) |
| stock_correlation_agent | 256k | `agent_predictions` (grading), dashboard calls; no downstream consumer found beyond grading and display |
| analyst_shadow | 136k | grading only |
| wargamer v2 | 59k | one dashboard route (`routes/agents.py`) and grading |
| analyst_agent (local) | 11,733 model-seconds | grading only |

Correction (13 Nov, from the parent): an earlier version said 78% of hosted tokens feed a model feature. The live model's feature list (29 names in the factor report) contains none of `llm_news_30`, `ev_comment_letter` or `ev_13d_activist`; they are stored but not read by the live model. So at least 57% of hosted tokens (llm_news 976k plus comment letters 276k of 2,190k; 61% with the 13D reader) feed no live-model input, only stored features, case files and displays. Only scenario_generator and rule_synthesizer (about 17%) feed live alert paths. The ablation question therefore becomes "does adding these features help the walk-forward at all", and if not the calls should stop.

### Consumer list for every retire, merge or gate candidate (for the owner)

| Candidate | Class | Who reads it today | What breaks if it goes |
|---|---|---|---|
| analyst_agent local path | (iii) | grading (`agent_predictions`); nothing reads `sentinel:analyst:views` | the analyst series loses its local arm; the hosted shadow keeps running |
| wargamer v2 | (iii) | `routes/agents.py` wargame panel; entity-claim grading | the dashboard panel shows nothing; no alert or model input depends on it |
| quant_trading_engine alert role | (ii) (was iii; see correction) | produces `agents.quant.discoveries` (read by consensus, edge validator, macro, radar, rule synthesizer, correlation agent: 7 groups) and `agents.financial.advice` (consensus); its model-written advisory has been off since 29 Oct | demoting only its alert role leaves discoveries flowing; removing the agent would starve those 7 groups, so demote, do not delete |
| consensus_engine | (iii) | its report is read by `reasoning/context_builder.py` (`_fetch_consensus_report`) for scenario context; `agents.consensus.reports` has no other reader | scenarios lose one context block; measure whether scenarios that carried it differ before deleting |
| `alerts.outbound` topic | (iii) | no producer, no consumer | nothing; the constant can be removed with its exemption |
| `agents.ontology.updates`, `agents.ontology.unknown_entities` | (iii) | no consumer group; producer and consumer (knowledge-graph engine) disabled | nothing today |
| `agents.intel.briefs` | (iii) | reasoning, alert manager, quant, consensus, rule synthesizer, wargamer subscribe; the producer is disabled | nothing today; a restored producer would reach all six |
| four stock_correlation_agent variants | (ii) merge | grading, dashboard calls | one call per ticker-day instead of four; the grading series would be split by variant |
| generic WATCH-only rules and their fan-out to 11 consumer groups | (ii) gate | the alert manager drops WATCH (`main.py:172-174`); agents read them; the correlation-funnel route counts them | agents lose WATCH inputs; the replay bar (at least 90% overlap of graded predictions) decides |
| llm_news and comment_letter calls | (ii) add-or-stop test | stored features only; not in the live model | test adding them to the walk-forward; if they do not clear the bar the calls stop |
| rule_synthesizer | (ii) | the correlation engine's dynamic rules; `agents.rules.synthesized` read by consensus | fewer WATCH rules; a fixed catalogue replaces the free-form names |
| Neo4j ontology proposals (67k a day) | (iii) measure first | supervisor writes Neo4j; customer-link and graph routes read it | not measured: find the readers before gating the vessel-to-region edges |


## Correction (14 Nov, #1-priority re-check): the quant engine is not shown to be worse than chance

The earlier statement rested on the stored agent-skill statistics (hit 0.448 on 402, z -2.43), which count each call as an independent observation. Re-checked on the 357 resolved calls that have an excess return against SPY (`agent_predictions.excess_spy`), signed so that a positive number means the call was right, 15 Sep to 12 Nov, 307 names, 29 dates:
- Clustered by date (one observation per date): mean -155.6 bps, t -1.37. Not significant. Unclustered the same calls read -106 bps; the 68 calls on 7 Oct and 53 on 5 Oct, near zero, dominate the count.
- By horizon: the 24-hour calls (291 units, 27 dates) average +8.6 bps, t 0.36: chance. The 7-day calls are one date. The 30-day calls (56 resolved) sit on 4 clean dates between 8 and 14 Oct, average -1,546 bps, and are the whole of the loss: defensives called "down" (ATO, AWK, TMUS, WELL, SBAC) rose and cyclicals and AI hardware called "up" (SNPS, TSLA, GEV, TSEM) fell, one rotation shared by every call that week, the same episode as the monthly ideas. The engine has emitted only 24-hour calls since 14 Oct (24 calls in 5 days).
- By direction, clustered: up -156 bps (t -1.31, 27 dates), down -79 bps (t -0.39, 14 dates).
- Confidence is not informative (hit 0.39 to 0.50 across the 0.46 to 0.49 values that make up 319 of the 420 calls; the 15 calls at 1.00 hit 0.467).
So the right reading is "no demonstrated skill, effective sample about 29 days, and one regime episode accounts for the large negative number", not "worse than chance". Consequence: the demotion recorded as an owner decision rests on an overstated statistic. Keep the engine as it is, and let the registered test decide: restore or demote on the lower 90% bound of its date-clustered skill over 20 independent days of the 24-hour calls (about 20 more trading days at the present rate).

No alert path from the quant engine: the alert manager subscribes only to correlations, scenarios and intel briefs (`services/alert_manager/main.py:347`), and the quant engine publishes agents.quant.discoveries and agents.financial.advice, neither of which it reads. Its calls reach grading, the dashboard calls list and the seven discovery consumer groups, so "demoting its alert role" has nothing to demote today.

## Correction addendum: every agent re-graded one observation per date (14 Nov, `scripts/research_agent_cluster.py`)

Mean signed excess return over SPY (positive = right), one observation per detection date, t over dates, last 90 days, calls that have an excess return:

| Agent (horizon) | Calls | Dates | Mean bps by date | t over dates | Calls on 3 biggest dates |
|---|---|---|---|---|---|
| macro_intelligence_engine (24 h) | 296 | 24 | +84.2 | 2.03 | 23% |
| stock_correlation_agent.baseline (24 h) | 824 | 7 | +105.4 | 1.87 (stored t was 6.3) | 71% |
| stock_correlation_agent.sympathy (24 h) | 812 | 8 | +14.2 | 0.45 | 81% |
| stock_correlation_agent (24 h) | 301 | 4 | +22.9 | 0.43 | 100% |
| stock_correlation_agent.evidence (24 h) | 85 | 5 | -18.3 | -0.73 | 71% |
| analyst_shadow (168 h) | 61 | 5 | +383.5 | 0.93 | 92% |
| analyst_agent (168 h) | 45 | 6 | -426.3 | -1.31 | 91% |
| quant_trading_engine, all | 373 | 30 | -189.0 | -1.65 | 43% |
| quant_trading_engine 24 h | 291 | 27 | +8.6 | 0.36 | 55% |
| quant_trading_engine 720 h | 46 | 5 | -1,467.8 | -3.74 (5 dates, one week) | 96% |

Reading: no agent has demonstrated skill. Macro is the only one near the bar (t 2.03 over 24 dates, but its hit share by date is 0.44, so a few large wins carry a mean that is also 94% "down" calls, and with eight agents tested it does not survive a multiplicity correction); the correlation-agent variants and both analysts have 4 to 8 dates, far under the 20 independent days the registered rule needs, so they are "not judged", not "no skill" and not "skill"; `.baseline`'s stored t of 6.3 falls to 1.87 with 71% of its calls on three dates. The earlier "none" verdicts for analyst and macro stand; the earlier "worse than chance" for the quant engine does not.

## Further check (14 Nov, `scripts/research_macro_beta.py`): the macro engine's edge is partly beta

The macro engine's clustered +84 bps (t 2.03) came from calls that are 94% "down" on very high-beta names (NVDA, AVGO, SPCX, TSM, TSLA, AMD, BE, MRNA). Mean 126-session beta of the called names is 2.29 and SPY fell 50 bps on average over the call windows, so shorts on those names beat SPY with no stock-specific information. Beta-adjusted (stock return minus beta times SPY return, signed, one observation per date, 24 dates): **+47 bps, t 1.58**, not significant; about 44% of the raw mean was beta. `stock_correlation_agent.baseline`: mean beta 0.58, raw +105 bps (t 1.87 over 7 dates) becomes **+9.8 bps, t 0.44** after the adjustment. The grading in `agent_predictions.excess_spy` (migration 0094) subtracts SPY's return without scaling by beta, so it credits high-beta shorts in a falling market and high-beta longs in a rising one. Fix (additive): store `excess_beta` (stock return minus trailing beta times SPY return) beside `excess_spy` and grade on it; judge agents on the beta-adjusted, date-clustered figure.
