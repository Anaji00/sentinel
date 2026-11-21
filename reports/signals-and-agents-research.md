# Signals, AI output and agent structure: research and plan (13 Nov 2026)

> **Superseded (18 Nov).** A duplicate of `reports/Signals, AI output and agent structure research - 13 Nov.md`, written before I knew that session existed. That report governs. In particular, my statements here that the quant engine is "worse than chance" (hit 0.448, mean -136 bps) are withdrawn: clustered by date it is not significant (see its 14 Nov corrections). Do not build from this file.

Read-only. I read the backend code and reports, searched the web, and changed nothing else. I ran no queries and no jobs. Every number about Sentinel's own results comes from the backend session's message or from `reports/` and `MODEL_TRIALS.md`; I did not re-run them. Effort: S under a day, M one to three days, L a week or more.

## 0. What I read, and how far to trust this

Code: `services/agents/` (analyst_agent, consensus_engine, supervisor, edge_validator, stock_correlation_agent, adversarial_wargamer, rule_agent, grader, grades_v2_job, excess_grading), `services/correlation/` (peer_graph, edge_survival), `services/reasoning/` (scenario_generator, scenario_tracker, market_calibration), and in `shared/utils/`: grades_v2, producer_grades, event_study, signal_families, evidence_fusion, corroboration, alert_dedup, situation, alert_view, expected_returns, confidence_calibration, forecast_skill, meta_label, case_file, fact_check, evidence_bundle, deterministic_briefs, inference_budget, llm_policy, hosted_llm, llm_news, rule_probation, champion_gate. Docs: `DATA_PIPELINE.md`, `MODEL_TRIALS.md`, and the `reports/` files on grading, pre-registrations and system research. I did not read `GUIDE.md` in full.

**Much of what you might ask for already exists**, so most advice below is about gaps and order. Already built: date-clustered family grades with same-day controls (`grades_v2`), Benjamini-Hochberg control per rule (`producer_grades`), a date-clustered event-study module (`event_study`), Heidke and Brier skill (`forecast_skill`), market-neutral grading of agent calls (`excess_grading`), a case file with fact ids (`case_file`), a number-and-ticker checker for free text (`fact_check`), a citation check for the analyst (`analyst.verified`), isotonic calibration (`confidence_calibration`), a probation path for model-written rules, a champion/challenger gate, and an inference budget. Registrations A1, A2, A3 and A5 from the earlier research pass are in `reports/Upgrade pre-registrations - 12 Nov.md`; I do not repeat them, and I say where I agree or want to change one.

**Source quality.** Every external paper below was reached through web-search abstracts, indexes or summaries. I did not read any full text. I mark a source **weak** when it is a blog, a summary site or a vendor page, and **unverified** when I could not confirm a specific claim. Several claims in code comments (for example the three 2025 papers on debate amplification in `consensus_engine.py`) I did not check.

## 1. The five findings that shape everything else

1. **Almost nothing in the system is shown to have skill once dates are counted, not alerts.** Per your facts: 2,935 peer-decoupling alerts sit on 11 days; only Peer Cointegration and Decoupling beats a same-day control at one session (+0.43, t 3.16 over 11 dates, 54% of its units on 3 days); Company News and Trading is marginal; four families sit below the control. With 11 dates the test has little power, so "no difference" is not "no effect". The right output is a verdict with a power line: how many dates would be needed to see an effect of the size that matters.
2. **The current grade measures attention, not direction.** `producer_grades` and `grades_v2` use the absolute move divided by the stock's own typical move over the prior 60 sessions. That ratio is unsigned: a family can score high and still be wrong about direction. Most families in `signal_families.py` carry a prior direction, so they should also be graded on signed excess return.
3. **A likely artifact in the "below control" result.** A stock that fires an alert has usually just moved, and its 60-session "typical move" then includes that spike, so its ratio is biased low. The control is random stocks on the same date with no spike. This could explain why four families look significantly *worse* than random. This is my hypothesis from reading `grades_v2.py` and `producer_grades.py`, not a measured result; it is cheap to test (change 1 below). **Do not retire anything on the below-control result until it is.**
4. **No AI agent shows skill, and the layer that aggregates them has nothing to aggregate.** From reports: analyst 46 resolved calls, hit 0.543, Brier 0.251 (a coin is 0.25); stock-correlation variants Brier 0.252 to 0.271; quant engine hit 0.448 on 402 calls, mean -136 bps; macro engine 0.608 against 0.605 by chance; on excess return vs SPY the pooled hit rate is 0.378 (t -2.74 over 37 days, though only 30 to 37 call days exist). The models are useful as writers over verified facts, not as sources of direction.
5. **Probabilities shown to users must come from outcome tables, not from a model's number.** Scenario probabilities were templated until 12 Nov and are ungraded; the monthly forecast map is flat at the top, and the top-of-ranking test was "not judged" (101 overlapping dates, p 0.536 and 0.083).

## 2. Part A: signals

### A1. Which families get more or less weight and inference

Based on the clustered result you gave me (90 days, 19,749 units). "Weight" means the family's influence on ranking and on whether it earns an LLM call; it does not touch sizing or the book.

| Family | Result (your facts) | Recommendation | Why |
|---|---|---|---|
| Peer Cointegration and Decoupling | +0.43, t 3.16, 11 dates | **Keep; the only family that earns inference.** Do not raise its weight yet | 54% of units on 3 days means it may be a few sector-wide days. Check it per market regime and with a matched control before trusting it |
| Company News and Trading | t 1.97, 9 dates | **Keep on watch, no extra inference** | Marginal, below the registered 1.65 only by a hair on a one-sided test, but 9 dates is thin |
| Semantic Convergence, Equity Block and Options, Hawkes, Geopolitical Cascade | no difference | **Stop spending inference on them; display as context only** | Cheap to run deterministically; not worth a model call |
| Crypto Venue Stress, Insider Sale Into Options, Market Anomaly Cluster (5d), Informed Trading Sequence | below control | **Demote to display-only now; decide retirement after the matched-control test (change 1)** | Reversible step first |

Rule I suggest for "earns inference": a family is eligible for a model call only if its clustered signed-excess t over dates is at least 1.65 over at least 8 dates **and** its lower bound beats the matched control. Everything else gets rule-written text (see `deterministic_briefs.py`, which already shows the pattern).

### A2. How to grade without leakage and with date-clustered statistics

What each tool is for, and where Sentinel stands:

| Need | Method | Source | Status here |
|---|---|---|---|
| Alerts on one day are not independent | One unit per date; t over dates; report the number of dates | Kolari and Pynnönen show that event-date clustering makes ordinary event-study tests reject too often even at low correlation ([RFS 2010](https://papers.ssrn.com/abstract=1830364), abstract only) | `grades_v2` does this |
| Dates share neighbours (5-session returns overlap) | Non-overlapping blocks, or a block bootstrap over dates with a data-driven block length | [Politis and Romano 1994](https://gnosis.library.ucy.ac.cy/handle/7/57533); [Politis and White 2004, with the Patton correction](https://ideas.repec.org/a/taf/emetrv/v23y2004i1p53-70.html) | Used by hand (35-day blocks for days-to-cover); not standard in `grades_v2` |
| Control that matches the event | Control stocks on the same date matched on trailing volatility and on the size of the move just before the alert | My reasoning (finding 3); no single source | **Missing** |
| Many families, many horizons | Benjamini-Hochberg across all family-by-horizon cells | [Benjamini and Hochberg 1995](https://planosphere.stowers.org/pub/197) (assumes independent tests; dependence is a caveat) | Per rule in `producer_grades`; not across families in `grades_v2` |
| Strategy-level results after trying many variants | Deflated Sharpe: discount for the number of trials, skew and fat tails | [Bailey and López de Prado 2014](https://papers.ssrn.com/abstract=2460551) (abstract only) | Not used. Count every B-trial, S-variant and O-trial in N |
| A higher bar for new factors | A t of about 3 or more instead of 2 | [Harvey, Liu and Zhu 2016](https://nber.org/papers/w20592) (abstract; one summary says 3.0 may still be too low, which is a student note, weak) | Bars are t 2 with BH; fine for rules, too lenient for anything that moves money |
| Fitted models must not train on what overlaps the test | Purge training rows whose label window overlaps the test, then embargo a gap | López de Prado, *Advances in Financial Machine Learning*, ch. 7 (book; reached only via [reading notes](https://reasonabledeviations.com/notes/adv_fin_ml), weak) | The factor model and `meta_label` use walk-forward with an embargo; the report says so. Rule grading needs none of this |

Add one more thing to every published grade: **minimum detectable effect and dates needed.** With 11 dates, an effect that would matter commercially is likely too small to detect; saying so stops "not significant" being read as "worthless".

### A3. How to retire or gate rules from the clustered result

A four-state lifecycle, using only dates and signed results, written before it is applied:

1. **Probation** (new, or fewer than 8 dates): capped at WATCH, never calls a model, never raises a tier (this is `rule_probation.py` already).
2. **Active**: one-sided t over dates at least 1.65 on signed excess, matched-control result not below zero.
3. **Display-only** (reversible): t under 1.65 over at least 8 dates, or below control. Removes tier and inference; the family still records so it can recover.
4. **Retired**: display-only for 8 consecutive weekly checks **and** negative in both halves of the dates. Owner approval, as registered in A1.

Reverse path: a display-only family returns to Active only on new dates, not on re-running old ones.

### A4. Cheap new free-data signals with a real prior, and ones to drop

Evidence is academic and mostly old; effects shrink after publication, and Sentinel's book trades large, liquid names, which removes most of the effect for several of these. Treat each as a hypothesis to register, not a feature to add.

| Idea | Prior | What I found | Verdict |
|---|---|---|---|
| **Customer-supplier lag** (a supplier reacts late to its customer's news) | Strongest match to data already held (customers named in 10-Ks, `customer_links.py`) | Cohen and Frazzini report a customer-momentum strategy at about 1.55% a month with little exposure to standard factors, and controls for industry lead-lag ([AQR page for the paper, secondary](https://www.aqr.com/insights/research/journal-article/economic-links-and-predictable-returns)); replication reported, not checked by me | **Register a test** if not already done. I could not tell from the code whether the customer feature has a standalone result |
| **Insider trades split into routine and opportunistic** | Good, and the data are held (Form 4, `insider_history.py`, `insider_clusters.py`) | Cohen, Malloy and Pomorski: opportunistic trades earn about 82 bps a month value-weighted, routine about zero ([J. Finance 2012](https://econpapers.repec.org/RePEc:bla:jfinan:v:67:y:2012:i:3:p:1009-1043), summary pages) | Check that the existing "opportunistic" label follows the paper's rule (same calendar month for several years running = routine). The days-to-cover trial's opportunistic-sell cell was t -1.82 on tradable names, near the bar |
| **Option skew** | Moderate; data held since Feb 2024 (`option_iv_history`, `option_chain_daily`) | Steeper smirks underperform by about 10.9% a year, lasting at least 6 months ([Xing, Zhang and Zhao, JFQA 2010](https://econpapers.repec.org/RePEc:cup:jfinqa:v:45:y:2010:i:03:p:641-662_00)) | Already an input (`opt_*` in the factor model). Ask whether skew has its own date-clustered test; 2.7 years is about 33 monthly dates at most |
| **Post-earnings drift** | **Low for large caps** | Drift is near zero for large stocks since about 2006 in one study; two 2025 papers dispute it, turning on how it is measured and whether microcaps are included ([Martineau, Critical Finance Review](https://nowpublishers.com/article/Details/CFR-0122); [UCLA Anderson brief](https://anderson-review.ucla.edu/is-post-earnings-announcement-drift-a-thing-again)) | Do not build a drift collector. `earnings_reaction` already exists; test it on tradable names only |
| **13F breadth change** | Sign is contested | Chen, Hong and Stein found falling breadth precedes low returns; Nagel found no average relation over a longer sample ([NBER](https://www.nber.org/papers/w8151), summaries) | A3 registration is correctly two-sided; low prior, keep at 12 months of data |
| **Short-interest changes** | Weak for single stocks here | Aggregate short interest predicts market returns ([Rapach, Ringgenberg and Zhou 2016](https://econpapers.repec.org/RePEc:eee:jfinec:v:121:y:2016:i:1:p:46-65)); that is a market-level result. The stock-level days-to-cover trial found no effect on tradable names | Use as a market-regime input, not a stock signal |
| **Index inclusion** | **Dropped** | The S&P 500 addition effect went from about 7.6% in the 1990s to under 1% in the last decade ([Greenwood and Sammon, J. Finance 2025](https://papers.ssrn.com/abstract=4294297)) | Do not build |
| CFTC positioning, Reg SHO threshold list, Treasury auctions | **Dropped** | Already tested in your studies: no accepted signal; Reg SHO names are 1.3% tradable | Keep stored as research; build nothing |

**Fix the inputs before adding inputs.** Constant or saturated scores (alpaca_news flat at 0.50, short-volume on its cap, vessel_spoof flat, extended hours saturated) and headline sentiment of exactly 0 on 82% of company headlines make the existing families blind to differences. Treat a lexicon no-match as **missing**, not neutral. The standard finance word lists were built because general-purpose lists mislabel finance words (73.8% of Harvard-list negatives were not negative in 10-Ks, [Loughran and McDonald 2011](https://papers.ssrn.com/abstract=1331573)). Separately, `llm_news.py` records that FinBERT tone on 361,126 stock-days had rank IC 0.0005 (t 0.2): there is no case for spending CPU on FinBERT as a signal. The score-replay results (finra form B, news range 0.5 to 0.64) are already in the grading report; I have nothing to add except that outcome Spearman near zero means those re-scales remove noise and do not add information.

## 3. Part B: AI output

### B1. What exists, and the gap

Good: schemas with Pydantic and a retry loop; the analyst writes reasoning before the number and must cite fact ids that exist (`analyst.AnalystView`, `verified`); `fact_check.py` checks numbers and tickers in free text; the scenario generator requires watch and deny signals with comparators and discards echoes of its template; macro briefs are rules over measurements. Gaps: probabilities that a model writes (scenarios, analyst `p_beat`) are not turned into calibrated numbers before display; there is no sentence-level citation check across all model text; there is no frozen evaluation set; and `scripts/research_llm_rubric.py` (R1 to R7: numbers, falsifiable, evidence resolves, not templated, consistent entities, length and cost, consumer) is written but I have no result for it. **Run it first**; it is the baseline for everything below.

### B2. Recommendations

1. **Numbers are computed; models only write sentences over them.** Every figure on a card comes from a stored fact id. A sentence with no id, or whose numbers `fact_check` cannot find, is dropped, and if nothing is left the card says "No written view." (M; uses `case_file`, `fact_check`).
2. **Never show a model-written probability.** Show the family's empirical right-rate with its count and dates, taken from the graded table (`confidence_calibration` already does isotonic mapping with a floor on sample size). Verbalized confidence is often better calibrated than raw token probabilities in the models tested, and listing several candidate answers before stating confidence helped most, but chain-of-thought did not reliably help ([Tian et al., EMNLP 2023](https://arxiv.org/pdf/2305.14975), abstract and figure captions). That is a reason to measure the model's confidence against outcomes, not a reason to display it. Until scenario probabilities are graded, show hypotheses unranked, or ranked by a deterministic score, without percentages (S, plus UI).
3. **Abstain by default.** An output of "insufficient evidence" must be a normal result. The analyst should publish a view only when (a) its family is Active, (b) at least N facts are cited, and (c) the stated probability differs from the family base rate by a stated margin. Report accuracy against coverage (the share of cases answered) weekly. Selective prediction with a threshold on confidence, and conformal risk control to set the threshold, is the standard framing ([survey-style results in recent preprints, weak: e.g. arXiv 2605.14098](https://arxiv.org/pdf/2605.14098); I did not read them). Agreement between samples is a useful but incomplete confidence signal ([same results page, weak]). (M)
4. **Keep schemas small and put the reasoning before the number.** Forcing JSON through constrained decoding lowered reasoning accuracy in one study, with classification less affected ([Tam et al., EMNLP 2024 industry track](https://arxiv.org/pdf/2408.02442); the schema tested was tiny, so extra care when generalising). Sentinel's analyst already orders `rationale` before `p_beat`; the scenario generator's large nested schema (several hypotheses, each with signals) is the larger risk, so test it on the evaluation set against a two-step "write freely, then format" variant (M).
5. **Use an LLM judge only on a small weekly sample and only for groundedness and clarity.** Judges show position, verbosity and self-preference bias, though GPT-4 agreed with humans over 80% in one test ([Zheng et al. 2023](https://arxiv.org/abs/2306.05685)). Use a judge from a different model family than the writer, randomise order, and never use it for market correctness: prices grade that. Keep it to about 30 outputs a week (S).
6. **Build the evaluation set from the past, and grade only what does not need hindsight.** Freeze packets (the case file as of time T) with the later outcome attached. Models can recall what prices did before their training cutoff; lookahead bias was found in tests of Llama-2 on earnings-call text ([Sarkar and Vafa](https://arxiv.org/pdf/2502.21206v2) is the follow-on listing; the original is a Harvard working paper, abstracts only). So: score **prices** only on events after a model's recorded cutoff (what `llm_news_job` already enforces), and use the whole history only for outcome-free checks: format, citation validity, abstention on thin packets (M).
7. **Hallucination tests that need no judge.** (a) Strip the facts: the model must abstain. (b) Swap the ticker: no old name may appear. (c) Change one number: the output's number must change. (d) Ask for a source id that does not exist: it must be refused. Run on every prompt or model change (S to M).
8. **Show it honestly in the UI.** A range, the family's right-rate with "n calls over d days", a visible "no view" tile, and grey for anything ungraded. See `reports/frontend-research.md` findings 1, 2 and 5.
9. **Cap inference cost by consumer, not only by rate.** `inference_budget.py` limits intake. Add a ledger row per call: trigger, dedupe key, admission rule, model and cutoff, tokens, consumer id, and whether a person or process read the result in 7 days. Cut calls whose output has no reader. You have already found this twice: the briefs topic has no producer and `agent_intel` is empty (`reports/System improvement research - 13 Nov.md`, addendum 2, item E), and the analyst spends about 17 minutes of local CPU a day at coin-flip Brier. Route cheaper first: deterministic text, then a small hosted model, then a large one only for Active families (the cascade idea; [FrugalGPT](https://arxiv.org/pdf/2305.05176), authors' own figures, up to 98% cost saving on their tasks, so treat as an upper bound). Hosted first, local only at the cap, as in your standing rules. Moving a graded producer to a hosted model resets its series, which is your decision (see section 6).

## 4. Part C: structure

### C1. Correlation engine

- **Switch the unit of evidence from "alert" to "situation".** `situation.py` already does this for divergences (DEVELOPING to RESOLVED, one object per stock). Extend it: one row per stock per session listing the independent families behind it, using `evidence_fusion`'s rule that a family counts once and families sharing a source count together. Dedupe at `(stock, family, session)` already happens in `alert_dedup`; it does not catch the same source feeding two families.
- **One event-study protocol for every family.** Event time is the first moment the information was public; the return is stock minus beta times market minus sector (the sector ETF mapping already exists in `excess_return.py`); windows of 1 and 5 sessions; units clustered by date; the Kolari-Pynnönen caution applies when dates cluster. Report signed excess for families with a prior direction and the unsigned move ratio only for attention-type families, the way `signal_families.PRICE_DIRECTED` already separates them.
- **Peer sets.** Peer decoupling is the one family that works, so spend effort here. `peer_graph.py` requires realised co-movement plus structure; keep that. Improve by (a) re-estimating beta and peer residual sigma on a rolling window with the current day excluded (the `peer_residual_scan` stage two does this), (b) logging peer-set size and breadth with each alert so the grade can be split by them, and (c) trying customer-supplier peers as a second peer type, because the lag evidence above is of that kind.
- **Regime conditioning.** `regime_annotation.py` labels alerts but changes nothing. Use the label as a conditioning variable in grading (per-regime clustered result) and shrink small cells with the hierarchical code already in `hier_bayes.py`. Beware: splitting 11 dates into regimes makes every cell tiny; use shrinkage and report cells without ranking them.
- **Check the result is not just a few market days.** Re-run the peer-decoupling grade excluding the three heaviest dates, and with controls matched to the same sector (S).

### C2. Reasoning layer: hypotheses with watch and deny signals that get graded

The design in `scenario_generator.py` is right: hypotheses, an "ordinary explanation" slot, watch and deny signals with comparator and threshold, and `scenario_tracker.py` to resolve them. What is missing is the grade. Recommendations:

1. **Resolve signals numerically, not by keyword.** The tracker still matches text against headlines ("not perfect", per its docstring). Where a signal has a numeric threshold on a price, volume or filing, resolve it from the data and store the result with the date. Do not build the embedding-similarity phase; require machine-checkable signals instead (M).
2. **Grade the hypothesis list as a forecast.** For each scenario, record each hypothesis's probability at creation and which signals fired. Score with Brier and the Brier skill against the family base rate (`forecast_skill.py`). Only 44 scenarios came from about 10,000 correlations (7 confirmed) in the last 7 days; that is too few to grade soon, so generate common-case hypotheses by rule (measured numbers, fixed signal templates) and keep the model for rare families (M).
3. **Store with the scenario** the case-file hash and the model and its cutoff, so a later grader can tell what the writer could have known.

### C3. The agent swarm

| Agent | Earns its inference cost? | Action |
|---|---|---|
| analyst_agent (local qwen2.5:7b, about 17 min of CPU a day) | No: Brier 0.251, hit 0.543 on 46 | Keep the shadow on a hosted model for the registered test (50 resolved, paired bootstrap); do not widen |
| stock_correlation_agent (four variants) | No: Brier 0.252 to 0.271; four variants emit on the same ticker-days, so they double-count | One variant at a time; grade clustered by date |
| quant_trading_engine | No: hit 0.448, mean -136 bps | Already model-disabled; keep as a deterministic peer discovery and insider clustering job; keep out of alerts (owner decision, A2) |
| macro_intelligence_engine, radar_agent | Replaced by deterministic code | Correct as is |
| adversarial_wargamer, knowledge_graph_engine | No reader | Already disabled; remove the dead subscriptions and the dead `agent_intel` read, or restore with a reader |
| rule_agent (rule synthesizer) | Unproven; on probation | Keep on probation; cap by date count (A3) |
| supervisor | Not a planner: the single gatekeeper for Neo4j writes | Fix the known gap (held index-beta pairs; already found in the 13 Nov review) and stop there |
| consensus_engine | Aggregates agents that show no skill | Keep, but weight by the producer scorecard (zero weight until a producer is Active) |

**Supervisor and consensus design.** The consensus engine's comment argues for independent estimates with weighted aggregation, not debate loops. The multi-agent failure study (MAST: 14 failure modes in specification, misalignment between agents, and verification, from 1,600+ annotated traces) supports being cautious about agent-to-agent loops ([Cemri et al. 2025](https://arxiv.org/abs/2503.13657)); the engine's own claims about debate amplification cite three other papers I did not check. Averaging fusion is the right choice for overlapping, non-independent opinions, as the comment says.

**Disagreement as a signal.** Cheap test, no new model calls: record the spread among independent producers on each stock-day and test whether high spread precedes a larger absolute move (a volatility signal) rather than a direction. Register before running; if it passes, show "producers disagree" on the stock page as a caution flag.

**Evaluation harness.** Merge `grades_v2`, `producer_grades`, `excess_grading`, `forecast_skill` and the A1/A2 rules into one scorecard table: `producer, horizon, window, n_units, n_dates, signed_excess, t_over_dates, matched_control_diff, min_detectable_effect, bh_q, state`. One job writes it; the dashboard, the lifecycle and the agent weights all read it. This removes the situation where the same producer has four different grades.

**When rules beat a model.** Use rules when the input is a number or a field (rates briefs, residual z, score scales, dedupe, tier, breadth). Use a model only for reading text into fields (8-K items, filings, customers; `extract_audit_job` already audits this) and for writing short cited explanations of a deterministic result. Do not ask a model for a probability or a direction until it has beaten a rule on forward data.

### C4. Target architecture and migration order

```
L0 collectors  ->  L1 event identity and episodes (one id per observation; one episode per story)
  ->  L2 deterministic signal families (prior direction, registered test, tier)
  ->  L3 one grader (date-clustered, matched control, signed excess, BH, power)  -> family state
  ->  L4 situation per stock-session (independent families counted once)
  ->  L5 case file (fact ids, sources, as-of times)
  ->  L6 narrator (optional; hosted first; cited; may abstain) + validators
  ->  L7 dashboard (range, record, n; no ungraded numbers)
```
Inference happens only at L6 and in text extraction in L0/L1. Each L6 call has a trigger, a dedupe key, an admission check (family Active, packet complete), and a named consumer, as CLAUDE.md requires.

**Persist before publish at each step.** The situation row (L4) and the case-file hash are committed first; the narrator request carries the situation id; the narrator output is stored with the hash, model and cutoff before any card, Kafka message or notification refers to it; consumers check the row exists before spending a call. A migration adds the new columns before any code writes them; each new job writes a shadow table and is read by nothing until its registered test passes.

**Migration order** (each step stands alone and can be stopped):
1. Add matched control and signed excess to `grades_v2` output (shadow keys only).
2. Add power and dates-needed lines.
3. Scorecard table and the four-state lifecycle in shadow (computes states, applies none).
4. UI: stop showing ungraded probabilities; add "No written view".
5. Citation and number validator on every model text path (shadow count first, then drop).
6. Inference ledger with consumer id; cut unread outputs.
7. Evaluation set and canary tests.
8. Situation per stock-session across all families.
9. Apply lifecycle states (owner approval): display-only first.
10. Pre-registered tests for customer lag, insider routine/opportunistic, disagreement spread.

## 5. Top ten changes, ranked by expected value per unit of effort

| # | Change | Why it ranks here | Effort |
|---|---|---|---|
| 1 | **Matched control and signed excess in the family grade** | Decides whether four families are truly bad or only mis-measured, and what "works" means; everything downstream depends on it | S to M |
| 2 | **Power and dates-needed on every grade, minimum 8 dates before any verdict** | Stops "not significant" and "significant" being over-read; nearly free | S |
| 3 | **Stop displaying any model-written or ungraded probability; add "No written view"** | Removes the largest honesty risk the user sees; prices cannot yet grade these | S |
| 4 | **Demote the four below-control families and the four no-difference families to display-only (reversible)** | Saves inference and noise now; keep retirement for later and for approval | S |
| 5 | **Sentence-level citation and number validator on all model text** | Makes output checkable; builds on `fact_check` and `case_file` | M |
| 6 | **Inference ledger with named consumer and 7-day read check** | Finds and cuts calls nobody reads; the system has already shown two such cases | S to M |
| 7 | **Treat lexicon no-match as missing; finish the saturated scores; stop FinBERT as a signal** | Fixes blind inputs; measured nulls exist | S |
| 8 | **One scorecard table and one lifecycle** | Replaces four grades per producer; makes weights and the dashboard consistent | M |
| 9 | **Check the peer-decoupling result without the three heaviest dates and by regime; try customer-supplier peers** | The only family with a result; protects against a three-day artifact and tests the best-prior free idea | M |
| 10 | **Evaluation set plus canary hallucination tests** | A fixed yardstick for every prompt, model or schema change; no judge needed for most checks | M |

## 6. Three things not worth doing

1. **More agents, debate loops, or a smarter consensus layer on top of agents with no skill.** There is nothing to aggregate. The documented failure modes of multi-agent systems ([MAST](https://arxiv.org/abs/2503.13657)) point the same way. Spend the same effort on the grader and the case file.
2. **Collectors for signals whose published effect has faded, or whose own test came back null**: index inclusion ([Greenwood and Sammon](https://papers.ssrn.com/abstract=4294297)), post-earnings drift in large caps ([Martineau](https://nowpublishers.com/article/Details/CFR-0122), contested), Reg SHO, CFTC and Treasury auctions for stock picking. Keep the stored data; build nothing on it.
3. **Tuning scores or thresholds for rank correlation or for per-alert t values.** With 11 to 37 dates, any such tuning mostly fits noise, and the replay already shows what happens: the outcome Spearman of the old scores was negative or zero. Wait for dates; use the lifecycle.

## 7. Questions only the owner can decide

**None of my recommendations changes sizing, caps, the kill switch, or execution.** Items marked ⚑ are adjacent and need explicit approval before anyone builds them.

1. ⚑ Is the signal engine's tradability gate (`tradable_cells`) or `meta_label` sizing ever to read the new family states? I recommend no, for now.
2. Retirement of any family (step 9 above): who approves, at what false-discovery rate (I suggest BH q 0.10 and negative in both halves, as registered in A1)?
3. Routing the analyst's main series to a hosted model resets its graded series, per your standing rule. Wait for the shadow test (50 resolved calls, Brier lower by 0.01, paired p under 0.05)? I recommend yes.
4. Should the dashboard show family track records, or only the stock-level record? More honesty costs some "wow".
5. Is hiding all unranked scenario probabilities acceptable even though some users like the percentages? My view: yes, until 30 graded scenarios exist.
6. How many dates must a family have before it can lose its tier? I suggest 8 for display-only and 8 more weeks for retirement.
7. Should `quant_trading_engine`'s calls be removed from alerts now (A2's owner decision)? I recommend yes: hit 0.448, mean -136 bps.
8. Is a weekly LLM-judge sample (about 30 outputs) worth the hosted-token cost, or should groundedness stay fully programmatic?

## 8. What I did not check

- I did not run `research_llm_rubric.py`, so I have no result on how templated or uncited current model text is.
- I could not tell whether customer-supplier lag, insider routine/opportunistic labels, or option skew already have standalone date-clustered tests; I name them as "register if missing".
- The matched-control artifact (finding 3) is a hypothesis from reading code.
- I did not open the full text of any paper; claims are as stated in abstracts and summary pages. Where those pages were blogs or summary sites I said so.
