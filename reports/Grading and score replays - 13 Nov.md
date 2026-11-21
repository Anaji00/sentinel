# Grading and score replays - 13 Nov 2026

Closes four items registered in `reports/Upgrade pre-registrations - 12 Nov.md` ("Score resolution, macro fan-out rules and new free datasets").
Everything below was run read-only against the live database from `sentinel-agents-heavy` (one job at a time, under 1.1 GiB), except the
`agent_predictions` backfill, which writes only the new columns. No new scoring is live: it sits behind `SCORE_RESOLUTION_V2` (default `0`).

## A. Market-neutral grading of agent predictions (report only)

- Migration `0094_agent_prediction_excess_return` (new columns `stock_return, spy_return, sector_etf, sector_return, excess_spy, excess_sector,
  excess_graded_at, excess_attempts`) and `0095_agent_prediction_excess_pending_index`: **applied**. `outcome_correct`, `direction`, `brier_score` untouched.
- Pure logic: `shared/utils/excess_return.py`. Job: `services/agents/excess_grading.py` (`grade_pending`), also called once a day from the grader's
  producer pass (`services/agents/grader.py`). Backfill and report: `scripts/backfill_excess_returns.py` (idempotent: a graded row is never reselected;
  a row with no bars is parked after 3 attempts; each row's error is contained and counted).
- Method: close-to-close on `daily_bars`, from the last close on or before the call's date to the last close on or before call time + horizon.
  The same bars price the stock, SPY and the sector ETF (sector from `company_profile.sic` through `shared/utils/sectors.py`, mapped to the eleven XL ETFs).
  Signed excess = direction x (stock - benchmark). Hit = signed excess > 0. t is clustered by call day (one unit per day, equal weight per day).

Backfill output: 2,839 resolved directional rows selected, **2,542 graded**, 297 parked with no bars (unlisted symbols such as US10Y, or too few bars), 0 failed.

```
Excess vs SPY (signed: positive = the call beat the market in its direction)
dir        n  days     hit   hit t  mean bps    ex t  raw hit
up      1162    31   0.389   -3.12    -192.3   -1.61    0.503
down    1380    30   0.368   -1.35      67.2    0.90    0.460
all     2542    37   0.378   -2.74     -85.7   -1.00    0.480

Excess vs sector ETF (2500 rows with a sector ETF)
up      1153    31   0.362   -3.36    -161.5   -1.75    0.502
down    1347    30   0.420   -1.71      55.6    0.99    0.457
all     2500    37   0.393   -2.78     -56.9   -0.96    0.478
```

Reading: the 28-36% down / 53-64% up raw split did largely disappear from the up/down difference once the market is removed (raw 0.503 up, 0.460
down over these rows), but on excess return the hit rate is **below 50% in both directions** (up 0.389, t -3.12 against 0.5). Mean excess is not
distinguishable from zero (|t| < 2). Caveats: only 30-37 distinct call days (the table is young), so the clustered t has few degrees of freedom; "hit" here is
excess > 0 on daily closes over the call's horizon, not the resolver's entry-price rule, so `raw hit` differs from the scorecard's own rates; a
hit rate under 50% with a near-zero mean excess reflects many small negative excesses, not a proven loss. Nothing was gated on this.

## B. Score replays

Pure functions: `shared/utils/score_resolution.py` (tests in `tests/test_grading_and_score_replays.py`). The alert cuts compared are those found in the
code: 0.3, 0.4, 0.5, 0.6, 0.65, 0.7, 0.75, 0.8, 0.9 (agents/base.py, dossier_engine, quant_trading_engine, explain, tradfi collector).
Under the "no event newly above a cut" bar an old score is never lifted (the proposals are capped at the old score and never round upward across a cut);
the number of events newly *below* a cut is printed for information, because a de-saturated score necessarily lowers some.

### finra_short_volume (`scripts/replay_short_volume_score.py`)

z is recomputed for all 521 stored events with the live rule (prior up to 20 sessions at 500,000+ shares from `finra_short_volume`, population sd floored at SHORT_MIN_SD 0.03, matching the tag as the parent changed it; re-run after the change, results moved by one event).
521 replayed, none skipped, 349 with a 5-session move. Old: 21.1% on the 0.95 cap, median 0.855. New: max 0.814, median 0.584, 28.6% at the 0.55 floor.

```
  PASS  share on the 0.95 cap < 5%: 0.0% (old 21.1%)
  FAIL  rank correlation with old >= 0.9: 0.249
  PASS  top 1% by new keeps old top 1% mean |5-session move|: new 0.0270 vs old 0.0264
  PASS  no event newly above a cut: 0 newly above, 2196 newly below (informational)
counts at or above each cut (old -> new): 0.6: 521->212, 0.65: 521->110, 0.7: 521->43, 0.75: 456->11, 0.8: 383->2, 0.9: 172->0
```

**Verdict: one bar failed, so the proposal is not to be switched on as registered.** The rank correlation is 0.25 because the old score is
`0.55 + 1.5 x jump` (the size of the jump) while the proposal ranks on z (jump relative to the name's own spread); they are different orderings, not a
compression of one. The proposal also drops 59% of events below the 0.6 cut and 92% below 0.7 (the old score put every event at 0.70 or above). The
live z tag has not been compared with the replay on tagged events: that check has to be run after new spikes arrive (the replay script already prints the agreement when tags exist).

### alpaca_news (`scripts/replay_news_score.py`)

Stored-count storage added to new events: `services/correlation/stock_news_scorer.py` (note: the task named `services/enrichment/enrichers/stock_news.py`, which
does not exist; alpaca_news events are built in this scorer) now writes `news_n_6h` and `news_n_20d` into `financial_data`, additively, with a counted
failure path; the score changes only under the flag. The replay reads the counts from `stock_news` for all 4,758 stored events (no live counts exist yet).
z = (n - mu) / max(0.5, sqrt(mu)), mu = the name's mean per 6-hour window over the 20 days before (the registration did not fix the spread; this is mine). z quantiles: p10 1.62, median 2.00, p90 9.68.

```
  PASS  >= 10 distinct values over 1000 events: 190 distinct over 4758 events
  FAIL  no count above any cut changes: >=0.6: 0->4328, >=0.65: 0->2031, >=0.7: 0->1944, >=0.75: 0->1323
```

**Verdict: FAIL on the cuts bar.** The registration assumed that 0.50 to 0.75 crosses no cut; the code has cuts at 0.6, 0.65, 0.7 and 0.75, so 91% of events would
newly clear 0.6. Whether the 0.6 to 0.75 cuts apply to this event type (`headline`) was not traced. To pass, the range would have to stay under the lowest cut that
applies to headlines (0.5 + 0.09 x min(1, z/4) keeps it under 0.6, for example); that is a new registration, not something done here.

### vessel_spoof and alpaca_extended_hours (`scripts/measure_score_distributions.py`)

**vessel_spoof**: 2,514 events; 2,335 transit anomalies all at 0.82 (one distinct value), 179 inland at 0.74. The implied speed is in each headline
(median 217.8 kn, p90 9,426, p99 144,276: a large share are garbage fixes, not ships). Proposal `0.62 + 0.20 x min(1, log10(speed/50)/2)` (0.62 at the 50-kn base,
0.82 at 5,000 kn, capped at the old score): 181 distinct values, median 0.684, 12.8% still at or above 0.8; 0 newly above a cut, 6,007 newly below.
**Not measurable against outcomes:** a vessel is not a tradable name, so there is no price move to judge it on. Its value as an input to correlations would need the downstream correlation outcomes, which are not recorded per vessel event.

**alpaca_extended_hours**: 3,127 events (117 with no notional keep their score; 2,780 have a 5-session move). Old: 19.3% at 0.95 or more, 45.4% at 0.8 or more
(the "61%" in the registration was a different window). Proposal `0.5 + 0.45 x min(1, log10(notional/250k)/2)`, capped at old: 3.3% at 0.95 or more, 12.6% at 0.8 or more.

```
  PASS  share on the 0.95 cap < 5%: 3.3% (old 19.2%)
  FAIL  rank correlation with old >= 0.9: 0.799
  PASS  top 1% by new keeps old top 1% mean |5-session move|: new 0.0296 vs old 0.0285
  PASS  no event newly above a cut: 0 newly above, 4796 newly below (informational)
```

Registered bars for these two were "the same treatment"; I applied the four finra bars. Extended hours fails on rank correlation (0.80), so it too is not ready.
Rank correlation may be the wrong bar for a change whose purpose is re-ordering: that is a judgement for the owner, not changed here.

## C. Macro fan-out shadow (`scripts/shadow_macro_fanout.py`, `shared/utils/macro_fanout.py`)

Rules matched: US2Y Yield Drop and D Equity Upswing (2,301), Macro Energy Price Headline to Equity Block Convergence (952), Cross-Asset Yield and Equity
Negative Correlation Detection (762, not 582), US10Y Yield Rise Predicts Nike Selloff (217); all tier 1. "Macro Energy Cost Pressure Convergence" is not in the set.
The data starts 14 Oct and all four rules fired only in the last 14 days, so the 14-day run is the fair one; the 30-day denominator includes weeks before they existed.

```
last 14 days
  FAIL  tier-1 correlations fall >= 50%: 39.7% (4232 of 10651)
  PASS  confirmed scenarios lost < 5%: 0.0% (0 of 11)
  PASS  remaining alerts' 5-session move ratio not lower: after 1.455 vs before 1.436
last 30 days
  FAIL  tier-1 correlations fall >= 50%: 21.4% (4232 of 19787)
  PASS  confirmed scenarios lost < 5%: 0.0% (0 of 21)
  PASS  remaining alerts' 5-session move ratio not lower: after 1.369 vs before 1.366
```

Caveats: the move ratio is graded on only 51 retired rule-stock-days (of 4,232) because most are too recent for a 5-session window and the 60-session lookback, so that bar is
weakly informed (retired rules' own mean ratio 1.28 on those 51). Tier-1 is `alert_tier = 1` (counted per correlation row); counted over all tiers the drop would be 26.5% (14 days). Scenarios are linked only through
`scenarios.correlation_id`; a scenario supported by several correlations is counted once under its linked rule. Result: the hypothesis "they add volume and no information"
is supported on information (no confirmed scenarios, no better moves) but the registered volume bar (>= 50%) is not met by these four rules; retirement stays a separate approval.

## Not measured, and things to know

- `short_vol_z` agreement between live tags and the replay: no tagged events exist yet.
- The live alpaca_news counts (`news_n_6h`, `news_n_20d`) exist only on events created after the scorer restarts; the replay used `stock_news`.
- `agent_predictions` rows from the telemetry worker have no ticker and are not graded; 297 rows with no bars are parked (3 attempts).

## Containers to restart (code is bind-mounted; no compose or `.env` change; flag defaults off)

- `sentinel-factor-model` (runs the grader: daily excess-return pass; migration 0094 is already applied, so it is safe now).
- `sentinel-correlation` (`stock_news_scorer.py`: stores `news_n_6h` / `news_n_20d` on new events).
- `sentinel-filings` (`catalysts.py`: reads `SCORE_RESOLUTION_V2`; with it off the scores are unchanged).
Nothing was restarted, recreated or committed by this session.

## Files

New: `shared/utils/excess_return.py`, `shared/utils/score_resolution.py`, `shared/utils/macro_fanout.py`, `services/agents/excess_grading.py`,
`scripts/backfill_excess_returns.py`, `scripts/replay_short_volume_score.py`, `scripts/replay_news_score.py`, `scripts/measure_score_distributions.py`,
`scripts/shadow_macro_fanout.py`, `tests/test_grading_and_score_replays.py` (21 tests). Edited: `shared/db/migrate.py` (0094, 0095 only), `services/agents/grader.py`
(one try-block), `services/correlation/stock_news_scorer.py`, `services/collector-filings/catalysts.py` (flag-gated).


## Revised-bar outcome test (run 13 Nov, `scripts/outcome_test_scores.py`, read-only)

Registered in "Revised score bars" (`reports/Upgrade pre-registrations - 12 Nov.md`): Spearman of the score with the 5-session absolute move, new >= old, at least 300
events with an outcome, plus the cap, top-1% and no-newly-above-cut bars. Form B is capped at the old score as well (so it cannot move an event up across a cut).

```
--- finra_short_volume, form A: min(old, 0.55 + 0.40 x (1 - exp(-(z-2)/3)))
events 521; with a 5-session move 349
share below 0.6: old 0.0% -> new 59.3%; below 0.7: old 0.0% -> new 91.7%
  PASS  outcome Spearman new >= old on >= 300 events: n 349; new 0.0627 vs old -0.2336
  PASS  share on the 0.95 cap < 5%: 0.0% (old 21.1%)
  PASS  top 1% by new keeps old top 1% mean |5-session move|: new 0.0270 vs old 0.0264
  PASS  no event newly above a cut: 0 newly above, 2196 newly below (informational)
  (information) Spearman vs grader 5-session move ratio, 349 events: old 0.0078, new 0.0010

--- finra_short_volume, form B: min(old, 0.70 + 0.25 x (1 - exp(-(z-2)/3)))
share below 0.6: old 0.0% -> new 0.0%; below 0.7: old 0.0% -> new 0.0%
  PASS  outcome Spearman new >= old on >= 300 events: n 349; new 0.0491 vs old -0.2336
  PASS  share on the 0.95 cap < 5%: 0.0% (old 21.1%)
  PASS  top 1% by new keeps old top 1% mean |5-session move|: new 0.0270 vs old 0.0264
  PASS  no event newly above a cut: 0 newly above, 837 newly below (informational)
  (information) Spearman vs grader 5-session move ratio, 349 events: old 0.0078, new -0.0090

--- alpaca_extended_hours: min(old, 0.5 + 0.45 x min(1, log10(notional/250k)/2))
events 3128; with a 5-session move 2780
share below 0.6: old 36.2% -> new 50.0%; below 0.7: old 44.1% -> new 71.7%
  FAIL  outcome Spearman new >= old on >= 300 events: n 2780; new -0.0539 vs old -0.0375
  PASS  share on the 0.95 cap < 5%: 3.3% (old 19.2%)
  PASS  top 1% by new keeps old top 1% mean |5-session move|: new 0.0296 vs old 0.0285
  PASS  no event newly above a cut: 0 newly above, 4796 newly below (informational)
  (information) Spearman vs grader 5-session move ratio, 2767 events: old -0.0406, new -0.0162
```

Reading, without more weight than it bears:
- **finra, both forms PASS all four bars.** The pass on the outcome bar is a low bar: the old score's Spearman with the move is **negative** (-0.23: the larger the old jump-based score, the *smaller* the next
  5-session move), and the new scores are about zero (+0.05 to +0.06). With 349 events from about 30 days, standard error is roughly 0.054 for each, the events cluster by day, and against the grader's move ratio
  both old and new are indistinguishable from zero. So the proposals remove a negative association, not prove a positive one. Form B keeps every event at or above 0.7 (0 below 0.6 or 0.7) and still clears the
  cap; form A moves 59% below 0.6 and 92% below 0.7. The choice is the owner's cut-loss decision; B has no cut cost.
- **alpaca_extended_hours FAILS the outcome bar**: both old (-0.038) and new (-0.054) rank outcomes slightly *against* the move, and the new is lower. The proposal also takes 72% of events below 0.7 (from 44%). Not adopted.
  Both Spearmans are small (|rho| < 0.06 on n 2,780: roughly one standard error), so neither score ranks the next-5-session move.

## Trace: which cuts apply to `headline` events (alpaca_news)

alpaca_news events are `type = 'headline'`, source `alpaca_news`, tag `stock_news` (`services/correlation/stock_news_scorer.py:146`, `shared/utils/signal_families.py:269`).

Applies to headline events:
- `services/agents/base.py:2209` and `:2217`: `anomaly_score >= 0.5 OR type = 'headline'` for an entity's/headline's recent context. Headlines pass regardless of score, so no cut.
- `services/agents/base.py:2262`: world-state "top ML anomalies": `anomaly_score >= 0.65`, **any event type, headlines included**. This is the lowest cut that applies; the 4 Nov incident (`shared/utils/stock_news.py:36-43`) is exactly this one. A score at or above 0.65 puts an article into every agent's world state.
- `services/api_gateway/routes/explain.py:281`: `is_significant = anomaly_score >= 0.70`, display for any event.
- `services/reasoning/signal_attribution.py:65`: `high_anomaly = anomaly_score >= 0.8`, attributes a correlation's signals; not reachable at 0.75.
- Correlation rule floors (`services/correlation/main.py`, `min_anomaly`): the rules that take `headline` use 0.20 to 0.30 (e.g. `:531`, min_anomaly 0.20); none above 0.5 takes headlines.

Does not apply (event types restricted away from `headline`):
- `services/agents/dossier_engine.py:50` (0.6; `type IN ('equity_block', 'options_flow', 'market_anomaly', 'filing', 'insider_trade', 'insider_cluster', 'price_anomaly', 'earnings_report')`, lines 51-52).
- `services/collector-tradfi/main.py:1245` (0.6; same style type list on the next line).
- `services/correlation/main.py:602` and `:617` (0.60; `flight_anomaly` only).
- `services/enrichment/enrichers/prediction.py:221-232, 368-376` (0.65/0.75: prediction-market events); `shared/utils/candles.py:420-428` (candle anomalies); `services/collector-macro/economic_calendar.py:323` (macro releases).

**Result:** the lowest cut that applies to `headline` is **0.65** (`base.py:2262`), not 0.6. A range that stays under it: `0.5 + 0.14 x min(1, z/4)` (max 0.64); the registered `0.5 + 0.09 x min(1, z/4)` (max 0.59) is under it too. Neither has been replayed; no alpaca_news score is changed.


## alpaca_news revised range replay (`scripts/replay_news_score.py --span 0.14 --cuts 0.5 0.65 0.7 0.8`, 13 Nov)

Range `0.5 + 0.14 x min(1, z/4)` (max 0.64), z as before. Applicable cuts per the trace above: 0.5 (headlines pass it regardless), 0.65, 0.7, 0.8 (0.6 does not apply to headlines).
**Events carrying stored counts: 0 of 4,889** (see the correction below: the count query was failing, not the restart missing), so every event was replayed from `stock_news`.

```
stored alpaca_news events read: 4889; replayed 4889; count queries failed 0; events replayed from stored counts: 0
z quantiles: p10 1.62, p50 2.00, p90 9.62, max 32.84
new score distribution: min 0.5, p10 0.557, p50 0.570, p90 0.640, max 0.640 (27.4% at 0.64), 132 distinct; 41.1% at or above 0.6, 0% at or above 0.65
counts at or above each cut (old, new): 0.5: 4889->4889, 0.65: 0->0, 0.7: 0->0, 0.8: 0->0
  PASS  >= 10 distinct values over 1000 events: 132 distinct over 4889 events
  PASS  no count above any cut changes: none
```

Both bars PASS on the `stock_news` replay. Not yet measured: agreement between the stored live counts and the replay. Re-run `python replay_news_score.py --span 0.14 --cuts 0.5 0.65 0.7 0.8`
after `sentinel-correlation` has been restarted and a week has passed (about 160 events a day, so roughly 1,000 stored-count events a week); the script uses stored counts where present.
The 0.6 cut is not applied to headlines, but 41% of events would sit at 0.6 or more under this range: if a consumer I did not find uses 0.6 on headlines, this would change its counts.
`score_resolution.news_score` now takes a `span` argument (default 0.25, unchanged behaviour); the live scorer still uses the default and is behind the flag.


### Correction: why no event carried stored counts (found by the parent session, 13 Nov)

My earlier "not restarted" explanation was wrong. `sentinel-correlation` had restarted several times (since about 08:30 UTC) and still stored nothing: `_COUNTS_SQL` in
`services/correlation/stock_news_scorer.py`, which I wrote, used `$2 - INTERVAL ...` and left the parameter type ambiguous, so every call failed with
"operator does not exist: timestamp with time zone > interval" (110 swallowed errors, counted by `swallowed()`, so events were still created, just without counts). I had not run that query against a live
database connection from the scorer (only the replay script's copy, which casts `$2::timestamptz`). The parent fixed it by casting every `$2` to `::timestamptz`, added `tests/test_news_counts_sql.py`,
checked it on live data (NVDA: 6-hour count 18, 20-day count 540) and recreated `sentinel-correlation`. The revised-range re-run date stays about 20 Nov, counted from that recreate.
