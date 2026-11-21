# Layer quality audit (18-19 Nov 2026, read-only)

Measured 19 Nov 05:00 UTC from stored data only (TimescaleDB, Redis through the app, container logs, code). Nothing was edited, restarted, committed or pushed. New read-only scripts, all run inside existing containers: `scripts/audit_layer_metrics.py` (live collector and failure counters), `audit_layer_agents.py` (agent grades on `excess_beta`, date-clustered), `audit_layer_corr.py` (divergence "fade" test), `audit_layer_regrade.py` (session-aligned re-grade), `audit_layer_caps.py` (market-cap sanity). `scripts/research_family_cluster.py` was re-run unchanged. Earlier sweeps (13 Nov, Addendum 6) are extended, not repeated; where a number differs from them I say so.

"Additive" = new counter, filter flag, test or label that changes no stored result; "behaviour" = changes what is stored, sent or graded.

## Top 15 (ranked by expected accuracy / returns gain)

| # | Finding | Layer | Gain | Fix kind | Evidence in one line |
|---|---|---|---|---|---|
| 1 | The alert stream is 81% one family whose grade is "moves more", with no direction, a saturated confidence, and a negative backtest Sharpe in 54% of alerts | 3 | Alert precision; stops 1,600 undirected Telegram messages a week from reading as trade ideas | behaviour (tiering) | 1,607 of 1,993 ALERT-tier correlations in 7 days; confidence 0.954 with 6 distinct values; median backtest Sharpe -0.35 |
| 2 | Event-graph primary entity is wrong for most "company" headlines (EPS, AI, US, IPO, "Rip", "III") and `AI` collides with the real ticker AI (C3.ai) | 2 | Every headline-to-equity correlation, scenario and agent call built on it | additive (stoplist + name-to-ticker preference) | Hand check: 19 of 27 company-typed primaries not a tradable subject (70%, 95% CI 52-84%); "EPS" is primary on 497 headlines a week |
| 3 | Agent grades are mis-windowed: every Friday 24 h call grades as exactly 0, and pre-open calls start from a close that has not happened | 5 | Clean grades feed consensus base rate, kelly win probability, producer grades | behaviour (grading) | 541 Friday rows with 100% zero return (20% of correlation-agent 24 h calls); re-grade does not change any verdict (see 5.2) |
| 4 | Divergence alerts carry "fade_buy / fade_sell / go_long" actions that the same record's own study says are unsupported | 3/4 | Prevents a directional label with no evidence | behaviour (label) | 63 fade or go actions on 138 divergence alerts in 14 days; "no continuation or reversal at 1 or 5 sessions (t 1.6, 0.2)" printed in the same alert; my 5-date test: fade -29 bps, t -0.90 |
| 5 | Hosted tokens: wargamer v2 is now 17% of spend (350k tokens in 7 days) for 7 graded calls; llm_news is 25% | 5 | Frees about 40% of hosted tokens for the admitted producers | behaviour (gate/stop) | token meter 11-18 to 11-12; wargamer first row 11-11, 7 of 192 graded beta-adjusted |
| 6 | analyst_agent local model: 4.1 h a week of the scarcest resource for 95% "no view" answers | 5 | Local inference freed for the 24 h agents | behaviour (retire) | 182 completions in 7 days, 95% of p_beat within 0.47-0.53, 5 distinct p values, 14,720 s |
| 7 | Reddit has been dead 36 h: every subreddit returns HTTP 404, and the upstream-error counter says 0 | 1 | One of the three social sources; also proves finding 8 | additive | 1,504 drops logged since 17 Nov 17:51; last event 17 Nov 17:42 |
| 8 | The collector reject counter is wired in one place, so "rejected = 0" for all 11 collectors means nothing | 1 | Makes parser breakage visible (the failure mode the module was written for) | additive | `.rejected(` called once in `services/` (ais json_decode) |
| 9 | Half of `sec_edgar` events are prospectus supplements at a constant anomaly 0.20 | 1/2 | Less enrichment and storage work; cleaner filing family | behaviour (filter) | 1,532 of 3,081 events in 7 days are 424B2/B3/B5; 1,310 are structured notes from 8 banks |
| 10 | Scenario generator: 86% of scenarios come from the peer family; the 55/30/15 triple is still 54% of them; narrative confidence text is never refreshed | 4 | Honest confidence on the dashboard; saves calls | behaviour | 64 of 119 scenarios in 14 days; all 9 status-updated scenarios sampled show a stored/narrative confidence gap |
| 11 | Market caps are wrong for ADRs: TSM $9.4 tn (about 5x), EQNR $946 bn (about 10x), NGG $427 bn (about 6x), E $171 bn | 2 | Size bands and the $10 bn gate for foreign names | behaviour (cap table) | `audit_layer_caps.py`; guard only fires when a vendor USD figure exists and the ratio is over 5x |
| 12 | Same-agent and sibling contradictions: 184 ticker-days where one agent called both up and down, and 221 of 221 two-variant ticker-days disagree | 5 | Removes self-cancelling output and double counting in consensus | additive (flag) then behaviour | 30-day `agent_predictions` |
| 13 | The collector "starved" gauge is market-hours blind: 14,682 warnings from radar, gauge stuck at 1 for ais and radar while data flows | 1 | Real stalls stop being hidden by false ones | behaviour (alarm) | log and `collector_starved` gauge |
| 14 | 9% of daily bars are flat carry-forward rows (1,139 of 12,122 on 17 Nov; 383 with zero volume) | 1/2 | Features for illiquid names; whether the factor model filters them is unverified | additive (check) | `daily_bars` 12-18 Nov |
| 15 | Display text fails on 1,655 events since 12 Nov (about 236 a day, `sec_form4` among them) and is counted under the wrong name | 2 | Summaries fall back to placeholder text; counter name misleads | additive | `pipeline_errors_total:cross_domain_signals` = 441 since restart; log "unsupported format string passed to NoneType" |

The strongest single reading of the sweep: accuracy is lost mostly *between* layers (a wrong subject, an undirected alert read as a call, a grade that starts on the wrong session), not inside any one model. No agent and no correlation family has a date-clustered, beta-adjusted, **directional** edge after this audit either; the one surviving family signal is about size of move, not sign.

---

## 1. Collection

### 1.1 Freshness and timestamps (`events`, 7 days, 80 sources)

Healthy: the live streams (coinbase_candles 17,231, kalshi 13,715, aisstream 9,230, polymarket 4,110, gap_detector 2,847) have median event-to-ingest lag 0-1 s and p99 under 30 s; no source writes `occurred_at` in the future (one source, `therecord`, one row, `dark_reading` one row, `eff` one row: under 6 min skew). `sec_edgar` median lag 94 s (the 2.4-day p99 is weekend catch-up: 13 of 1,455 8-Ks later than a day, filing date correct). `sec_form4`, `sec_13d`, `finnhub_earnings` are on time.

Silent or thin (hours since last collected row): `reddit` 35 (**dead, finding 1.2**), `ownership_changes` 41 (batch, last 17 Nov), `OpenSky` 32.5 (12 events a week), `usaspending` 90 (6), `fred` 155 (1 row, retired), `openfda` 145 (1), `themarkup` 116 (3), `sec_edgar_13f` 28 (8, quarterly). `usaspending`, `themarkup`, `fred`, `openfda` had no row in the last 3 days and appear retired; no source is documented as retired in the code, so they read as silent.

Back-filled timestamps (event time is the article date, ingest is days or weeks later) are correct by design: `epic`, `bellingcat`, `krebs_security`, `eia_today`, `themarkup`, `war_on_rocks` show p50 lags of 15-60 days from first-run backfills of low-volume feeds. They are fine as events and would be a look-ahead trap in any backtest that keys on `collected_at`; no consumer does.

### 1.2 Reddit has produced nothing for 36 hours (finding 7)

Evidence: `docker logs sentinel-social` shows `Dropped input at collector_social.reddit ... http_404 (wallstreetbets)` and the six other subreddits, 1,504 times, from 17 Nov 17:51 onward (also 1,268 DNS failures earlier); last event 17 Nov 17:42; the code reads `https://www.reddit.com/r/{sub}/new/.rss?limit=15`. The failure *is* counted (`dropped`), but `collector_upstream_errors_total{collector-social}` is 0 and `collector_last_success_epoch{collector-social}` is fresh because Telegram still feeds it, so nothing on the dashboard says the Reddit leg is gone.
Impact: 1,355 events a week (about 1.8% of events, 52% of `social_signal`) and a corroboration source for retail-attention events vanish; no price effect measured.
Fix: call `metrics.upstream_error("reddit")` at the 404 branch and add a per-source freshness line to the collector heartbeat. A replacement endpoint is a separate decision (the unauthenticated RSS path appears to be blocked). Additive. Test: a collector unit test that feeds a 404 and asserts the counter increments.

### 1.3 The reject counter is not wired (finding 8)

`collector_rejected_total` is 0 for every collector. `grep "\.rejected("` finds one call in `services/` (collector-ais, json decode). A parser that silently skips records after an upstream schema change would show zero. The `dropped(...)` telemetry exists and caught reddit, so the information is partly there, but it is not what the ingest/reject ratio and `reject_ratio` property in `collector_metrics.py` report.
Fix: call `metrics.rejected(reason)` beside every `dropped(...)` that discards a parsed record in the collectors. Additive. Test: guard test that every collector module which imports `dropped` also references `metrics.rejected`.

### 1.4 The starvation alarm is blind to market hours (finding 13)

`collector_starved` is 1 for collector-ais and collector-radar right now. AIS has data (aisstream 9,230 events a week); its flag comes from `has received NOTHING ... in 300s` lines (last 18 Nov 00:36) that were never cleared. collector-radar logged `produced nothing for N s (6,683,405 records total)` **14,682 times**, because the US cash session is closed 17.5 h a day and the check uses a fixed window. A genuine mid-session stall would look like the other 14,681.
Fix: market-hours aware `expect_within_sec` for radar/tradfi; clear the flag on the next record. Behaviour (alarm only). Test: unit test of `watch()` with a fake clock inside and outside the session.

### 1.5 Duplicates and low-value volume (finding 9)

Within-source duplicate URLs are zero (`sec_edgar`: 3,081 events, 3,081 accession numbers). Cross-source headline duplicates are small: 17 clusters, 34 events in a week. The real redundancy is content: `sec_edgar` form mix in 7 days: 8-K 1,455 (47%, anomaly 0.74, 16 distinct scores), **424B2 1,310 (42.5%)**, 424B3 158, 424B5 63, others 95. Every prospectus supplement has anomaly exactly 0.20 (one distinct value). Eight banks account for 1,000+ of the 424B2 (Barclays 225, UBS 223, Morgan Stanley 150, Citi 146, TD 139, BNS 124, BMO 84, RBC 83).
Impact: half of the event type "filing" carries no information, passes through enrichment and (via entity "BARCLAYS BANK PLC (ATMP)") into entity matching; it also dilutes the filing family's sample.
Fix: skip 424B* forms in the collector (or tag `low_value` and bypass enrichment). Behaviour (fewer events). Test: collector unit test per form type; replay on the stored week shows no 8-K/13D/Form 4 lost.

### 1.6 Bars (feeds the factor model)

`daily_bars`: 12,122-12,137 names a day for 12-17 Nov; 11,726 for 18 Nov (397 missing names, all dormant: only 2 had over $10 m dollar volume on 17 Nov). Fine. 5-minute bars: 2,019 tickers on 17 Nov, mean 88 bars a ticker, p10 26-51: thin names have large gaps; known and only matters for the intraday signal engine. **Flat rows (finding 14):** 9.4% of daily rows are `open=high=low=close` (1,139 of 12,122 on 17 Nov) and about 3% have zero volume; the 18 Nov rows show 0 zero-volume rows because the carry-forward pass has not yet written them. I did not verify whether the factor model's liquidity filter removes these before momentum and reversal features. Check by counting flat-bar names inside the traded book's candidate set (additive; test: a guard that no B1 pick has a flat bar in the last 3 sessions).

### 1.7 Free-tier and error counters

Live counters since restart: ingested 6,847,083 (radar 6,683,405, ais 115,882, tradfi 26,335, macro 13,394, crypto 3,094, prediction 2,912, news 1,817, social 215, sar 25, filings 3, adsb 1); `upstream_errors` 1 (al_monitor); `rejected` 0. `collector-adsb` last success is 21.6 h old with 1 record since start: the collector is nominally up and delivering nothing; `OpenSky` shows 12 events a week. The news collector (6,577 traceback lines per Addendum 6, finding 3) still logs a traceback per failed feed host. I did not re-measure free-tier headroom (Alpaca, Finnhub, SEC); the 30-IEX-tick and 50-Finnhub allocations in memory are unchanged by this audit.

---

## 2. Enrichment

### 2.1 Entity and ticker resolution, hand-checked (finding 2)

Method: 40 headlines drawn at random (`ORDER BY random()`, `type='headline'`, 3 days, excluding `alpaca_news`), primary entity compared with what the headline is about. 27 carried `primary_entity_type = company`. Of those, 8 were a correct tradable subject (GOOGL, KHC, NVDA twice, TSLA, CBS, HD, GM). The other 19 (70%; Wilson 95% interval 52-84%) were not: `AI` (3 headlines, a concept), `III`, `RIP`, `FINANCIAL STOCKS`, `BOND AUCTIONS TAIL`, `CREW SURVEY HIGHLIGHTS PROGRESS`, `FIRST POST-HUANG EARNINGS WEEK` (the story is Nvidia, which sat in `named_entities`), `LNG`, `GAZA`, `SAUDI ARABIA`, `NSA`, `ITF`, `ANTHONY ALBANESE` (a person typed company), `LOWE` (Lowe's, ticker LOW, not mapped), `USAR` (the headline is about the Army). Misses with a better entity available: Freeport Indonesia (primary "Indonesia"), SoftBank typed `media_source`, Cyabra typed `media_source` ("seekingalpha_currents"). Small sample; the direction is not in doubt.

Scale across the week (9,027 headlines): primary entities `EPS` 497, `NVDA` 444, `AI` 335, `US` 209, `UK` 126, `Trump` 125, `CHINA` 110, `IPO` 76, `EU` 73, `UN` 61, `FED` 30. `EPS` is the first capitalised token of every "GAAP EPS of -$0.01" earnings brief (the company name is second in `named_entities`). `AI` is worse than a concept error: **AI is a real ticker (C3.ai)**, and the peer-cointegration alert "QuantTradingEngine found peers for AI: RPRX, ABVX, MARA, NCNO, SOUN, EVCM" repeated on 3 of the last 3 days.
Downstream: share of 7-day correlations whose primary entity is not a name in `daily_bars`: "Headline Followed by Equity Block Activity" 63%, "Headline to Equity Block Activity" 59%, "Macro Headline to Equity Block" 47%, "Headline Market Impact Convergence" 43%, "Headline to Trade Signal" 39%, "Headline to Equity Block Convergence" 26%. These are the families that are WATCH tier and dropped, but they are still delivered to the agents (Section 5). The factor model's news feature is not affected: `stock_news_llm` takes the vendor-assigned ticker (6,995 rows a week, 2,410 tickers, all `^[A-Z]{1,5}$`).
Fix: (a) stoplist of about 40 tokens that are never subjects (EPS, AI, IPO, US, UK, EU, UN, CEO, CFO, GDP, CPI, ETF, FDA, SEC, NSA, FED, OPEC, NATO, country adjectives); (b) prefer, in order, an explicit `$TICKER`, then a `named_entities` item that maps through `issuer_ticker`/company names, and only then the first token; (c) never type a person or country `company`. Additive in effect (relabels `primary_entity_*`), behaviour for the correlations that follow. Test: golden file of these 40 headlines with expected subject; guard that no primary entity is in the stoplist.

### 2.2 Scores: constant, saturated or coarse

Fresh counts, 7 days: no event type is stuck at the cap (share at 0.99+: earnings_reaction 15%, social_signal 5%, everything else 0-2%) or the floor (insider_trade 11%, options_flow 7%). Constant or near-constant: `alpaca_news` anomaly is one value (still 0.50, 1,020 events; Addendum 6 waited on a replay), `sec_edgar` 19 values, `sec_13d` 20, 424B* one value 0.20, `thirteen_f` three values with 83% on 0.70, `copernicus_sentinel1` 80% exactly 0, `prediction_market_trade` 44% on one value (0.30). Sentiment: 53% of headlines are exactly 0.00 (4,768 of 9,027); news sources hold only 6-14 distinct values (a lexicon, not a model), and `stock_news_llm` is -1/0/+1 with 13.8% of rows `#stale` (down from 17%). Not a defect by itself; it caps what any sentiment feature can add (the earlier ablation plan already covers the add-or-stop test).

### 2.3 Look-ahead and units

No look-ahead found in event times (section 1.1). One unit issue stands: **market caps** (finding 11). The hash holds 4,964 names against 11,944 priced names (41.6%), 2,648 under $1 bn. Mega-cap values are right (AAPL $5.2 tn, MSFT $3.9 tn, NVDA $4.0 tn). ADRs are not: TSM $9,404 bn (25.9 bn ordinary shares times an ADS price that is 5 shares, true cap about $1.9 tn; the vendor figure is `non_usd` so no ratio guard applies), EQNR $946 bn (vendor figure is the NOK number in a USD field; real about $90 bn), NGG $427 bn (about 6x), E $171 bn (about 3x), SHEL $537 bn against a vendor $208 bn (2.6x, under the 5x guard). The 10,000x cap/ADV bound and the 5x vendor ratio catch the extreme cases only; 6 of the 15 names with the highest cap/ADV above $10 bn are visibly wrong (AGNCZ is a preferred share).
Impact: only the size gate and size-band cell assignment; every wrong name is large either way, so the $1 bn and $10 bn gates are not breached by these examples, but size-band studies and any "size" ordering are skewed. No effect on B1's trades measured.
Fix: tighten the ADS guard to compare computed cap with 20-day dollar volume times a name-specific ceiling (cap/ADV above about 3,000 for names with ADV under $500 m), and treat `non_usd` as "unknown" rather than "no guard". Behaviour (cap table). Test: fixture of TSM/EQNR/NGG/E/SHEL/AGNCZ asserting each is dropped or corrected.

### 2.4 Display-text failures (finding 15)

`apply_display_text` raised on 1,655 events since 12 Nov (about 236 a day), message `unsupported format string passed to NoneType.__format__`, one sampled event is a `sec_form4` insider trade. They are counted, but under `pipeline_errors_total:cross_domain_signals` (441 since the last restart), the name of a different step; so the counter that should show display failures reads like a cross-domain-signal failure. The event keeps the collector's own headline, so the cost is a placeholder summary, not a lost event. Fix: guard the `None` format fields in `apply_display_text` and rename the counter. Additive. Test: a Form 4 event with `None` premium and price.

---

## 3. Correlation

### 3.1 Flow, tiers and latency (7 days)

4,596 correlations (13 Nov: 9,953): tier 1 (WATCH, dropped by `alert_manager/main.py:172-174`) 2,603 (56.6%), tier 2 1,878, tier 3 115. Volume halved with the WATCH share down from 74% to 57%; **81% of what reaches ALERT is one family** (peer cointegration 1,607 of 1,993). Latency is not a problem: trigger event to correlation median 0.1 min (block, AIS, anomaly rules), 1.6 min (peer, p90 11 min), 4-6 min for headline joins (p90 8-12 min).

### 3.2 Precision by family (clustered, controlled; `research_family_cluster.py`, 90 days)

One unit per (family, ticker, date); statistic = mean per-date excess of |abnormal move| over a same-day control of 120 random stocks; t over dates. Rows at 1 session unless stated:

| Family | Units | Dates | Excess | t over dates | Reading |
|---|---|---|---|---|---|
| Peer Cointegration & Decoupling | 4,181 | 17 | +0.25 | 2.67 | only survivor; 5-session excess +0.08, t 0.84 |
| Company News & Trading | 211 | 13 | +0.19 | 1.48 | not shown (5-session +0.26, t 1.81, 9 dates) |
| Corporate Filing & Options Positioning | 65 | 27 | +0.14 | 0.75 | not shown |
| Geopolitical Cascade | 611 | 28 | +0.03 | 0.41 | none |
| Cross-Domain Hawkes Excitation | 3,151 | 16 | +0.04 | 0.38 | none |
| Cross-Domain Semantic Convergence | 6,660 | 35 | -0.03 | -0.54 | none |
| Headline Market Impact Convergence | 355 | 39 | -0.06 | -0.73 | none |
| Market Anomaly Cluster | 370 | 42 | -0.11 | -1.79 | below control |
| Equity Block & Options Convergence | 2,609 | 43 | -0.11 | -3.65 | **below control; 41 of its 262 weekly alerts reach ALERT tier** |
| Insider Form 4 & Microstructure | 110 | 29 | -0.27 | -2.80 | below control |
| Informed Trading Sequence | 43 | 23 | -0.28 | -2.05 | below control |
| Crypto Venue Stress | 73 | 37 | -0.44 | -4.27 | below control |

Against 13 Nov the peer family's 1-session excess fell from +0.43 (t 3.16, 11 dates) to +0.25 (t 2.67, 17 dates): the estimate is shrinking as dates accumulate, which is what a concentrated early effect does. **Every family grade here is a magnitude test (|move| against a control); none tests direction**, so "carries information" means "something will move", which is not a long or short.

### 3.3 The peer family: what the alert says and how it is scored (finding 1)

The alert text says "QuantTradingEngine found peers for X ... This does not say the stock is moving against them" (no direction). In 7 days: 1,608 alerts, 1,065 names, 54% with a negative backtest `sharpe_ratio` (median -0.35), 17% with a single peer, **confidence 0.954 on average and 6 distinct values**; 1,325 of the 1,443 entity-days have exactly one alert, so dedupe is working. All 1,607 pass the ALERT gate and about 848 of every 1,209 Telegram messages since 5 Nov were this family (13 Nov count).
Impact: precision of the stream. Its one measured property (larger move over one session) is real but unsigned, and a backtest Sharpe that is negative for the majority is not in the confidence or the tier.
Fix: tier by a registered rule (for example peer_count at least 2 and backtest Sharpe above 0), publish the residual's *sign* so a direction can be graded, and replace the constant confidence with the measured excess (+0.25, t 2.67) so the label is not 0.95. Behaviour. Test (register first): among peer alerts with Sharpe above 0 versus below 0, the date-clustered excess difference has |t| of at least 2 on the next 20 dates, else tiering by Sharpe is dropped.

### 3.4 Divergence alerts: labels versus evidence (finding 4)

"Stock Moving Against Its Peers" (138 alerts in 14 days; 2 reference groups a day) attach a regime label and action: UNEXPLAINED_DISLOCATION `fade_buy` 28 / `fade_sell` 29, CONFIRMED_CATALYST_DRIFT `go_long` 6, CROWDED_NO_EDGE `abstain` 15, FUNDAMENTAL_IMPAIRMENT_TRAP `veto_buy` 2, no label 58. The same record states "After divergences with news the study found no continuation or reversal at 1 or 5 sessions (t 1.6, 0.2)" and "A price gap is a measurement, not a forecast; direction after the alert is not established". My test (`audit_layer_corr.py`: entry = close of the detection date, exit +1 and +5 sessions, minus SPY, fade-signed, one value per date): 105 units on 5 dates, mean fade-signed -29 bps, t -0.90; `fade_buy` -41 bps (t -0.58, 4 dates), `fade_sell` +94 bps (t 0.82, 3 dates), `go_long` +213 bps on 2 dates (not testable). Five dates cannot support or refute anything, so the actions are not evidence-based yet and the file contradicts itself.
Also, the description names the first six peers ("the mean of MSFT, ORCL, PLTR, CRWV, NOW, CRWD") while `mean_return` averages the whole reference group; one outlier peer (CRWD +13.4% on 18 Nov) is shared by the VEEV and ADBE alerts the same evening, so a single peer move can flag several names at once.
Fix: show the regime as a description ("unexplained dislocation") and drop `fade_*`/`go_long` from the action field until a registered test passes (at least 20 dates and a clustered |t| of 2); use the group median instead of the mean for the reference return, which is at least as defensible and removes the outlier. Behaviour (label and reference). Test: replay stored alerts with median reference; counts of flagged names per outlier-peer day fall.

### 3.5 Dedupe and tiering notes

Entity-day dedupe is fine for the peer family (above). `Cross-Domain Hawkes Excitation Forecast` has `primary_entity_id = HAWKES` for all 35 weekly alerts (19 ALERT tier) and a 5-session excess of -0.08 (t -1.18): a subject that is not a security. Generic "Natural Gas Price Spike to Microsoft Equity Block Convergence"-style rules (82-87 a week) are still produced by the rule synthesizer and never leave WATCH. Neither is a precision problem (WATCH is dropped), but the 82-84 rows a week of the second still cost agent inference (Section 5).

---

## 4. Reasoning

### 4.1 Scenarios (119 in 14 days)

Status: hypothesis 83, developing 23, confirmed 10, denied 3. 102 of 119 (86%) descend from the peer-cointegration family; of those 25 became developing or confirmed (25%). Probabilities are still templated: the triple 55/30/15 is the first hypothesis of **64 of 119 (54%)**, and 60/30/10 (13), 60/25/15 (10), 65/25/10 (8) cover another 26%. The 13 Nov note said the 55/30/15 triple "had stopped"; it had not (9 of 11 scenarios a day on 14-17 Nov). Illustrative headlines show the entity-quality problem passing through: "NVDA shows unexpected price co-movement with Treasury bond ETF TLT and rival AMD", "META ... with TLT, SNAP, PINS, and GOOGL amid Italy lawsuit news", "BTC liquidation burst ... with CVX and BTCUSDT"; 21 of 119 scenarios have a hypothesis that calls the link an artifact or spurious.
Confidence text: 119 of 119 narrative summaries state "Overall confidence N%". In hypothesis-status scenarios it matches the stored integer; in every scenario whose status moved (developing, confirmed, denied) it does not (differences 1 to 43 points, mean 4.3 across all 119) because the stored figure is updated from `confidence_history` and the narrative is not. The 13 Nov figure of 22% mismatch conflated this with a defect; the defect is stale text, not a wrong number, and a reader of a confirmed scenario sees the old figure.
Fix: (a) generate a fixed set of falsifiers and let probabilities come from the packet or be dropped (the 13 Nov plan item), (b) strip the confidence sentence from the narrative or regenerate it on update, (c) skip the generator when `primary_entity_id` is not in `daily_bars` or the correlation is a macro-or-bond-ETF pair with an equity. Behaviour. Test: stored scenarios replayed; no stored text contains a confidence differing from the field; non-security subjects produce no call (confirmed-scenario count unchanged, calls down).

### 4.2 Cost per useful output (hosted tokens, 7 complete days 12-18 Nov, input + output)

Total about 2.0 M hosted tokens (plus 99k local-model tokens). Shares: llm_news 508k (25%), adversarial_wargamer_v2 351k (17.5%), scenario_generator 336k (16.8%), stock_correlation_agent 293k (14.7%), rule_synthesizer 152k (7.6%), analyst_shadow 141k (7.1%), activist_13d 110k (5.5%), comment_letter 82k (4.1%). Wargamer v2 was 59k in the 13 Nov table; it has grown six-fold since it started on 11 Nov and has 7 beta-adjusted graded calls out of 192. The scenario generator's 336k tokens in 7 days buy roughly 60 scenarios (119 in 14 days), about 5.6k tokens each, and a confirmed one costs several times that (the 13 Nov figure was 34k). llm_news still makes one request per headline at quiet hours (Addendum 6, finding 4).
Fix: see Section 5 gating; llm_news admission of 10 headlines or 30 minutes. Behaviour. Test: tokens per scored headline down at equal scored count.

---

## 5. Agentic output

### 5.1 Date-clustered, beta-adjusted grades (`audit_layer_agents.py`, `excess_beta`, 90 days, 24 h unless noted)

Positive = the call was right. One observation per date.

| Agent | Calls graded | Dates | Mean bps by date | t | Hit (by date) | Note |
|---|---|---|---|---|---|---|
| macro_intelligence_engine | 305 | 26 | +46 | +1.68 | 0.42 | 91% "down"; the nearest to an effect; aligned re-grade +80 bps, t +1.79 (25 dates) |
| quant_trading_engine | 393 | 33 | -119 | -1.65 | 0.38 | the 30-day arm: -1,049 bps, t -3.3, 8 dates (known); 24 h arm +20 bps, t 0.76; last call 12 Nov |
| stock_correlation_agent (hosted) | 302 | 5 | +116 | +2.18 | 0.63 | 5 dates, 99% of calls on 3 dates, last call 12 Nov |
| stock_correlation_agent.baseline | 1,062 | 11 | +7 | +0.46 | 0.33 | deterministic exposure sign |
| stock_correlation_agent.sympathy | 878 | 12 | +10 | +0.49 | 0.36 | 75% of calls on 3 dates |
| stock_correlation_agent.evidence | 129 | 9 | -33 | -1.35 | 0.28 | |
| analyst_shadow (hosted, 168 h) | 112 | 9 | +101 | +0.62 | 0.56 | |
| analyst_agent (local, 168 h) | 82 | 10 | -126 | -0.88 | 0.46 | |
| adversarial_wargamer_v2_hosted | 7 | 3 | +253 | +1.34 | 0.67 | 7 graded of 192 |

No agent reaches |t| of 2 over at least 20 dates. The two |t| above 2 rest on 5-9 dates and 99% of calls on three dates. Confidence is uninformative: high-confidence halves are no better than low (macro +62 vs +61 bps; sympathy +12 vs -1; analyst_agent **+279 low vs -255 high**, t -1.79 for the high half). Brier scores are 0.248-0.272 for every agent with at least 100 calls (0.25 is a coin).

### 5.2 Grading windows (finding 3) and what the fix would change

`shared/utils/excess_return.call_window` uses the UTC date of the call as start and the UTC date of call + horizon as end, returns `close_on_or_before(end) / close_on_or_before(start) - 1`. Two consequences measured:
1. **Friday calls grade 0.** A 24 h call on a Friday ends on Saturday, `close_on_or_before(Saturday)` is Friday's close, so the stored return is exactly 0.0 (and SPY 0.0, `excess_beta` 0). All 557 Friday and Saturday calls by the four correlation-agent variants in 30 days have `stock_return = 0` and `spy_return = 0` (dow 5: 541 rows, zero share 1.00; dow 6: 16). Zero-return rows across the 60-day window: sympathy 268 of 878 (31%), stock_correlation_agent 95 of 302 (31%), evidence 32 of 129 (25%), macro 70 of 303 (23%), baseline 162 of 1,062 (15%), quant 34 of 375 (9%). In those rows `outcome_correct` is a mix (120 of 268 true for sympathy), because it comes from the live scorer's price, so the two grades disagree on the same row.
2. **Pre-open calls start a session late.** A call at 03:31 UTC (22:31 New York the evening before) takes the *next calendar date's* close as its base. Share of graded calls made before 14:00 UTC: baseline 12%, sympathy 16%, quant 32%, macro 5%, analyst_shadow 87%, analyst_agent 72%.
Check that it does not hide an effect (`audit_layer_regrade.py`: entry = last New York close at or before the call, exit one session later, SPY beta from `beta_126d`, one value per entry date; 2,959 of 2,960 24 h calls re-graded): macro +80 bps (t +1.79, 25 dates) against the stored +48 (t 1.68); quant +54 (t 1.27) against +45 (1.36); correlation agent +120 (t 1.09) against +106 (2.52, 7 dates); baseline -3 (-0.11) against +11 (0.65); sympathy +4 (0.16) against +26 (1.02); evidence -28 (-0.74) against -15 (-0.73). The verdict "no clustered beta-adjusted edge" survives; the stored grade overstated two of the three correlation-agent arms. So the flaw is measurement noise and a hit-rate bias, not a hidden edge, and it is not free: `consensus_engine` takes its base rates from the raw `outcome_correct` of all resolved rows (lines 125-146), and per the scorer's comments the win rate feeds kelly sizing.
Fix: in `call_window`, roll an exit date that is not a session to the next session in `daily_bars`, and start from the close of the last completed session at call time; park rows that cannot be placed rather than store 0. Behaviour (grades change). Test: Friday 24 h call has a non-zero window; a 03:31 UTC call starts at the previous New York close; replay shows the date-clustered t for each agent within 0.5 of the figures above.

### 5.3 Duplicates and contradictions (finding 12)

30 days, correlation-agent variants: 2,473 ticker-days with a directional call. 221 have two variants; **all 221 disagree** (baseline vs evidence 152 of 152, correlation agent vs sympathy 30 of 30, baseline vs sympathy 20 of 20). A further 184 ticker-days have the *same* variant calling both up and down. Across all agents 365 ticker-days have two or more agents. I cannot tell from the table whether the arms are recorded only when they differ (by design, as ablation contrasts) or whether a sign is inverted somewhere; the code records baseline and evidence "on every pair" (line 795 comment), which argues the second. A check costs one query on a sample pair.
Fix: add `variant_group` and a flag `contradicts_sibling` to the stored row so consensus counts one vote per ticker-day, and alert on 100% disagreement. Additive. Test: guard that two variants agree on at least some pairs on a fixed fixture.

### 5.4 Inference spent on output with no consumed result (findings 5 and 6)

- `analyst_agent` (local `qwen2.5:7b`): 182 completions in 7 days, 14,720 s (4.1 h) of local compute, 95% of `p_beat` within 0.47-0.53 on 5 distinct values, identical output twice in a row for NYT; its graded rows are 82 at -126 bps (t -0.88). The shadow runs the same facts hosted at 31.9 s average.
- `adversarial_wargamer_v2_hosted`: 350k tokens for 192 predictions (76 directional), 7 graded, no falsifiable claim (13 Nov rubric); 25 predictions in the last 24 h.
- The hosted correlation agent used 293k tokens in 7 days; the 13 Nov count shows it reading every correlation on the topic (the gate at ALERT tier is not applied), including headline families whose primary entity is often not a security (Section 2.1). I did not re-measure its input mix.
Absent from this run: the `agent_predictions` table shows no `analyst_agent` or `analyst_shadow` rows after 11 Nov, but their horizon is 168 h and rows are written at resolution, so that is lag, not a stop (the telemetry shows 182 and 303 completions in the last 7 days). The quant engine's last call is 12 Nov and macro's 17 Nov; whether that is deliberate I did not check.
Fix: retire analyst_agent's local path (owner), stop wargamer v2 until it has a falsifiable claim and 20 independent dates, route correlation agents only ALERT-tier or security-subject correlations. Behaviour. Test: graded-call overlap of at least 90% on a stored-week replay, tokens down.

### 5.5 Calibration of stated confidence

Macro engine: 135 distinct confidences, hit 0.42 by date, high and low halves equal. Quant: 54 distinct, hits 0.38. Correlation agents: 21-45 distinct, confidence ties to the exposure rank not to outcome. Peer alerts 0.954 constant. Divergence confidences vary (0.65-0.69, 38 distinct) and are z-derived. None is calibrated against outcome, and the 5.1 splits show no monotone relation. Proposed in the 13 Nov plan (lower bound with conformal width); nothing here changes that.

---

## What looked sound

Live-stream freshness (section 1.1), no future timestamps, no within-source duplicate URLs, bars completeness for the liquid universe, event-to-correlation latency (0.1-6 min), same-entity-day dedupe in the peer family, anomaly scores not saturated for 9 of 9 high-volume types, `stock_news_llm` ticker validity, the deterministic divergence brief's numbers, the 5x vendor-ratio guard for the ADR cases it covers, EDGAR filing timestamps.

## Not done or not verified

Free-tier headroom per vendor (no new measurement); whether B1's feature build removes flat carry-forward bars; whether the quant engine and macro engine stops are deliberate; the Neo4j graph; Telegram read rates; a directional test of any correlation family other than divergence (none carries a direction today); the C3.ai point (3 days of repeats, not whether the AI-ticker alert is informative); whether baseline-vs-evidence disagreement is by design (5.3). All fixes are proposals; none was applied. The hand check in 2.1 is 40 headlines, one draw.

---

## Addendum 1 (19 Nov): what the alert stream would look like with the peer family at WATCH only

Requested for the owner's decision on finding 1. Read-only: `scripts/audit_layer_alert_tier.py` (the family-cluster method restricted to `alert_tier >= 2`, 90 days, one unit per family, ticker and date, same-day control of 120 random stocks, t over dates), correlation counts from `correlations`, Telegram counts from the alert-manager log (`!!` lines, which are messages that passed the tier gate, per-rule per-entity dedupe and the 10-an-hour cap; 5 to 19 Nov).

### Volume per day (5-19 Nov, correlations at ALERT tier or higher)

| Day type | ALERT-tier correlations a day, all | Of which peer family | Without peer family | Telegram messages a day, all | Of which peer family | Without peer family |
|---|---|---|---|---|---|---|
| Weekdays (10 days) | 284-1,130, median about 429 | 193-931, median about 357 | 55-199, median about 87 | 117-221, mean about 185 | 47-147 (47 on 12 Nov, otherwise 104-147) | 44-88, median about 66 |
| Weekend days (4) | 36-56 | 26-43 | 7-15 | 23-60 | 19-35 | 4-25 |

Totals over the 15 days: 5,385 ALERT-tier correlations of which 4,397 peer (82%); 2,022 Telegram messages of which 1,323 peer (65%) and 699 other. So with the peer family at WATCH only, ALERT-tier volume falls by about 80% and Telegram falls by about 65%, from roughly 135 to roughly 47 messages a day over the period (weekdays about 185 to about 66). The WATCH lines in the log would rise by the same 1,323 over 15 days (peer alerts would still be stored, scored and graded; only the message and the tier change). Tier 3 and above is 100% non-peer (118 correlations in the window, 54 on 17 Nov; I did not list the rules, the divergence rules are the known ones), so no top-tier alert is lost.

### What the clustered, controlled grade says about ALERT tier

| Group (ALERT tier or higher) | Horizon | Units | Dates | Mean excess over same-day control | t over dates | Reading |
|---|---|---|---|---|---|---|
| Peer family | 1 session | 4,181 | 17 | +0.33 | 3.30 | the only positive group |
| Peer family | 5 sessions | 2,931 | 11 | -0.11 | -1.58 | gone by one week |
| ALERT tier without peer family | 1 session | 13,063 | 52 | -0.00 | -0.01 | no information |
| ALERT tier without peer family | 5 sessions | 12,861 | 46 | -0.11 | -2.51 | below control |
| ALERT tier, all | 1 session | 17,244 | 54 | +0.05 | 1.16 | peer family's gain is diluted to nothing |
| ALERT tier, all | 5 sessions | 15,792 | 48 | -0.12 | -3.40 | below control |

Within the non-peer ALERT tier at 1 session the 11 largest families sit between +0.0 and -0.84; Equity Block & Options Convergence (1,485 units) is -0.11 (t -1.72), Market Anomaly Cluster -0.23 (t -2.09), Crypto Venue Stress -0.40 (t -3.73), Informed Trading Sequence -0.56 (t -3.29); none is positive with |t| above 1. The two big non-peer contributors by count are Cross-Domain Semantic Convergence (6,660 units, -0.05, t -0.80) and Hawkes Excitation (3,141 units, 5 sessions -0.11, t -1.89).

### Reading for the decision

1. Demoting the peer family removes about two thirds of the Telegram volume and 81% of ALERT-tier rows, and what remains is, by this measure, **no better than a random stock** at one session and worse at five. So the choice is not "noise or signal": it is between a stream that is 65% a family with a real but unsigned one-session size-of-move effect, and a smaller stream with no measured effect. A third option is to keep the peer family at ALERT but tier it (finding 1, proposed rule: peer_count of at least 2 and positive backtest Sharpe), which needs the registered test in section 3.3 first.
2. The peer effect is a one-session effect: at 5 sessions it is -0.11 (t -1.58). An alert whose message says "larger move than usual" should be read as a next-session statement; nothing here supports holding.
3. Caveats: the grades are magnitude tests, not direction; the date count for the peer family is 17 (11 at 5 sessions) with 38% of units on three days; the same-day control is a random draw, and re-running changed the peer excess from +0.25 (t 2.67, the main audit run) to +0.33 (t 3.30) on identical units, so treat each excess as about plus or minus 0.08 and the t values as about plus or minus 0.6; the 90-day non-peer ALERT set includes rows tiered under earlier rule versions (Semantic Convergence and Hawkes were ALERT tier then), so it describes what ALERT tier has delivered, not only what it delivers today; Telegram counts are messages logged since the log starts on 5 Nov.
4. The owner-held decision is unchanged by this: nothing was edited, and tier is set in the rule definitions, not in this audit.

## Addendum 2 (19 Nov): flat carry-forward bars do not reach the model's names (resolves finding 14)

`scripts/audit_layer_flatbars.py` (read-only), raised by quant-research. `_PANEL_SQL` (`services/agents/factor_engine.py:225`) filters only `close > 0`, but the names it is asked for are the model-scored set, and those are clean. Over 252 sessions: all priced names 2,026,615 rows, 8.35% flat (open=high=low=close), 3.07% zero volume; the 998 model-scored names (Redis `factor_scores:monthly`) 173,652 rows, **0.00% flat, 0.00% zero volume** (21 sessions: 13,972 rows, 0.00%); the stored B1 book (184 names, 175 inside the scored set) 32,016 rows, 0.01% flat. Quant-research's bar was 1%; no action on the model inputs. The flat rows sit in the illiquid tail outside the scored universe, so the carry-forward pass is a data-hygiene note for any future work that widens the universe (add `volume > 0` there first), not a defect for B1. Finding 14 is downgraded from "unverified" to "no effect on B1 or the model".

---

## Addendum 3 (21 Nov 02:55-03:20 UTC, Friday evening New York): end-to-end check, is the data holding up?

Read-only; stored data, container state and logs. agents-heavy (4 min) and factor-model (2 min) had just been recreated by another session when I looked, so their logs hold nothing older than that. Redis memory could not be read (the `redis-cli` call returned nothing); not re-measured.

### Holding up

| Stage | Check | Result |
|---|---|---|
| Containers | 31 sentinel containers | all up, 29 healthy; factor-model and qdrant report no health check; timescaledb 82% of its memory limit, correlation 68%, enrichment 67% |
| Database | connections | 26 |
| Collection | events in 24 h | 10,202, latest 02:55; coinbase, kalshi, aisstream, polymarket, form 4, 13D, telegram all under 10 minutes old; equity feeds quiet after the close as expected |
| Bars | `daily_bars` 20 Nov | 11,743 names; last 5-minute close before 21:00 UTC agrees with the daily close within 1% on 1,576 of 1,588 names (4 beyond 3%) |
| Enrichment to correlation | 24 h | 973 correlations, latest 02:45; no model-scored event is missing an `events` row; 0 of the 12 scenarios of the last 24 h came from a correlation with a missing trigger |
| Reasoning | scenarios, recommendations, news scores | 12 scenarios, 36 recommendations, 647 `stock_news_llm` rows (latest 01:02) |
| Model | factor scores | 998 names on all four horizons, `as_of` 20 Nov |
| Trader | 20 Nov | 102 orders: 100 submitted and filled, 1 unknown, 1 rejected |
| Grades | orphan or duplicate | 0 predictions pointing at a missing correlation; 1 duplicated `prediction_id` (migration 0100 added the unique index); 0 unresolved calls older than 9 days |
| Tracebacks since container start | tradfi 486, crypto 284, civic 214, adsb 191, others under 10 | tradfi, crypto, adsb: DNS failures ("Temporary failure in name resolution", websocket connect timeouts at 20:00-21:00 UTC), as in the known DNS pattern; civic: the ICE spreadsheet link is gone (209 of 214, last 31 Oct, a retired source); correlation, agents-fast, api_gateway: one Neo4j blip at 20 Nov 15:02 (the driver falls back to `bolt://localhost`, which is not a Neo4j) |
| Kafka lag | consumer groups over 500 | only agent-analyst, wargamer v2 on `sentinel.correlations` (978) and wargamer on `events.raw.news` (878), right after agents-heavy restarted; no other group |

The 5-minute and daily series reconcile, the stages carry data through to the book, and the one place the data is visibly thin is the ALERT-tier and agent layers already audited above, not the plumbing.

### What is not holding up

1. **No trader orders on Thursday 19 Nov, and the cause cannot be recovered.** Orders exist for 16, 17, 18 and 20 Nov and none for the 19th (101 to 147 a day otherwise); the book's stored `rebalanced_on` is still 2026-10-29. The trader logs a skip through `dropped(...)` only to the container log, which a recreate wipes, and it persists no per-day outcome. Fix (additive): write one row or one Redis key a day with `{day, orders, skip_reason}`. Test: a run that skips for stale scores leaves a row with the reason. Whether 19 Nov was a deliberate skip I could not establish.
2. **Orders that never confirm.** Since 11 Nov five fills read `unknown` after 5 checks (UMC buy 5.44 on 20 Nov, NDAQ 18 Nov, MRNA 16 Nov, GMAB 11 Nov, all small or fractional), and 17 submitted orders since 1 Nov are not marked filled. `SCCO` add short of 0.9788 shares was rejected by the venue (422, fractional orders cannot be sold short), and `USAR` shorts were rejected on 9, 10 and 11 Nov (the venue marks it unshortable). A top-up that cannot be placed leaves the book short of its target weight. Changes to execution logic need the owner's approval; the read-only step is to reconcile those 17 submitted and 5 unknown orders against the venue's positions. Test: the reconcile output equals the book's drift.
3. **Stored factor-score history is sparse.** `factor_scores_daily` holds monthly-horizon `as_of` dates 12, 13, 16, 18 and 20 Nov and, before that, only 21 Oct, 23, 27, 29, 30, 3 Nov (94 names, the known partial night), 4, 5, 6 Nov; nights between are absent (9, 10, 11, 17, 19 Nov), although the trader traded on 9-11 and 17 Nov. If only some nights are kept by design (the code says it grades every kept night) this is fine; if not, any replay of the book from stored scores has holes. Question for the factor-model owner, not a verified defect.
4. **Logs do not survive a recreate.** Every deploy erases the container's history, so a defect in the hours before a recreate (finding 1 above) has no trace. Fix (additive): the compose logging driver or a log volume with rotation. Test: a recreated container still shows the previous run's last 100 lines.
5. **Grade backlog.** 523 resolved calls have no excess return yet and 1,387 have an excess return but no beta-adjusted one (the beta pass refills the rows the re-grade rewrote). They clear as the daily grader runs; recheck in two days. Until then date-clustered beta-adjusted tables use fewer rows than exist.
6. **Reddit is still silent** (no event in the last 6 hours); the counter now says so, the endpoint is blocked, and the OAuth credentials are the owner's.
7. **timescaledb at 82% of its memory limit** (above the 80% watch line in `scripts/memory_watch.sh`); it was 82% on 14 Nov as well, so this is stable, not rising.

### Verdict

The data holds up from collection to the dashboard's inputs: fresh where the market is open, series that agree with each other, no orphaned rows, no model-scored gaps, and the tracebacks are the known DNS bursts and one retired source. What does not hold up is accountability at the end of the chain: a missing trading day with no record, orders that stay unconfirmed, and stored scores that would not support a full replay. Those are the three items to look at before trusting the book's history.
