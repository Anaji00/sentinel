# Upgrade pre-registrations (12 Nov 2026)

Registered before any code for phases 2, 3, 4 and 5 of the seven-phase brief changes behaviour. Decisions taken on 12 Nov: cyber stays
dormant (phase 1, no work); shrinkage runs in shadow after a registered test (phase 2); regime labels annotate alerts only (phase 4);
trader changes are tested as paper variants of B1 (phase 5). Bars below are fixed now; a result that misses a bar is reported as a miss.

## Phase 3: time-of-day baselines for market anomalies

**Measured today** (14 days to 12 Nov, weekdays, `market_anomaly` events, Eastern time, score of 0.8 or more; scripts/baseline_anomaly_rates.py):

| Bucket | Events | Score 0.8+ | Share | Mean score |
|---|---|---|---|---|
| Before the open | 7,441 | 33 | 0.4% | 0.275 |
| Opening 09:30-10:00 | 1,445 | 8 | 0.6% | 0.359 |
| Morning 10:00-11:30 | 3,478 | 133 | 3.8% | 0.412 |
| Midday 11:30-14:00 | 4,701 | 1 | 0.0% | 0.341 |
| Afternoon 14:00-15:30 | 3,185 | 3 | 0.1% | 0.336 |
| Closing 15:30-16:00 | 1,741 | 1 | 0.1% | 0.320 |
| After the close | 7,219 | 208 | 2.9% | 0.315 |

The opening is not the hot spot the brief assumed; the morning and after-hours are, and midday and the close almost never reach the cut.
The normaliser is one exponentially weighted baseline per (ticker, feature) with no time of day (`_dynamic_normalize_batch`).

**Hypothesis.** A same-time-of-day baseline (the seven buckets above) evens out the share of high scores across the regular-session
buckets without losing the events that precede large moves.

**Design.** Shadow only. Compute the bucketed score beside the live one from stored raw features where they exist, else write a 5%
sample of (live score, bucketed score, ticker, time) to a capped Redis list. The live score and every threshold stay as they are.
A robust variant (median and MAD of the same bucket, excluding the current value) is computed in the same shadow.

**Bars (fixed now).**
1. Parity: in the five regular-session buckets, each bucket's share of events at or above the cut lies between 0.5 and 2 times the
   five-bucket mean, over at least 10 weekdays.
2. Information kept: of the top 1% of events by score, the share followed within 30 minutes by a move of at least 2 ATR (from
   `tradfi_bars_5m`) is no more than 10% lower (relative) than the live score's top 1%.
3. Volume guard: the daily count above the cut changes by no more than 30% against the live score's.
All three must hold to switch the live score; a switch is a separate approval.

**Hawkes timestamps.** Before any change, measure the share of Hawkes inputs stamped from `collected_at` rather than `occurred_at`, and
the bulk-flush bursts (more than 20 events with one timestamp). The fit changes only if that share exceeds 10%.

**Supply-chain contagion.** Emit second-order signals in shadow with their provenance path. Bar: at least 100 episodes (an upstream
move of 2 sigma or more, the downstream name not yet moved), and the downstream name's excess return over the next 5 sessions agrees in
sign in at least 55% of them, tested with the same false-discovery step as every other signal cell.

## Phase 2: hierarchical shrinkage and the interaction meta-model (shadow)

**Today.** Cells are signal family by horizon. There is no market-cap or regime dimension, so the hierarchy is built in two steps.

**Step 1: add a cap dimension to the study in shadow.** Cells become family x horizon x cap band (under $1bn, $1bn to $10bn, $10bn and
over; from the cap table). The live study and gate are untouched.

**Step 2: shrinkage.** Level 0 is 0 excess return. Level 1 is the family at that horizon across cap bands. Level 2 is the cap-band cell,
shrunk toward the level 1 posterior mean. Prior strengths are estimated from the data by method of moments (between-cell variance
against within-cell variance), not set by hand; the effective sample size is the cell's N plus its parent's N scaled by the
between-to-within variance ratio.

**Release rule (fixed now).** A cell may leave shadow only if its shrunk posterior mean excess return after costs is positive with
posterior probability 0.95 or more, it passes the existing false-discovery step across all cells, and its effective N is at least 30.
Meeting the rule makes it a candidate; moving the gate is a separate approval, and the five approved cells stay as they are.

**Meta-model.** Features: peer residual x (1 - catalyst presence); signed surprise x residual x catalyst; options flow x gamma regime;
insider cluster x drawdown; supply-chain score. Outcomes are triple-barrier (profit at +1 sigma of the horizon, stop at -1 sigma,
vertical barrier at the horizon), with purged folds (purge = the horizon, embargo = one horizon). Bars: out-of-fold AUC of at least 0.55
and a net-of-cost Sharpe above the unfiltered signal on at least 6 of 8 offset calendars. A miss on either closes the meta-model.

## Phase 4: regime labels as alert annotations

**Labels** (UNEXPLAINED_DISLOCATION, CONFIRMED_CATALYST_DRIFT, FUNDAMENTAL_IMPAIRMENT_TRAP, CROWDED_NO_EDGE) come from a rule-based
classifier over the numbers packet (peer residual, filing and insider changes, options positioning, valuation and short interest). The
classifier uses no tokens. No label changes an order.

**Model call, if any.** Trigger: a candidate enters the day's top 10 by expected value. Dedupe: one call per ticker per day. Admission:
market cap of $10bn or more and a non-empty packet. Cap: 30 calls a day, on gpt-oss-20b (not the 120b, whose 200,000-token day is used by
news scoring). Output: the label, three numbers and one sentence. Consumer: the dashboard's alert card.

**Grading.** Each label is scored by the 5-session excess return of the stock, kept per label from the day it ships. A label is shown as
informative only after 100 graded alerts and a result above chance on the false-discovery step.

## Phase 5: lifecycle exits and covariance-aware sizing as paper variants of B1

**Variants.** V0: B1 as it is. V1: V0 plus a time-decay exit at 1.5 half-lives of the signal's information coefficient. V2: V0 plus an
exit on thesis invalidation (the hard-veto events already in the live book, extended to a direction flip). V3: V0 with shrinkage-covariance
weights in place of equal weights. V4: V0 plus a volatility-trailing stop once the position is up 1.5 sigma of its horizon.

**Harness.** A daily-path simulation on the eight offset calendars used for RB1, costs of 44 bps a month plus the variant's extra
turnover at the same per-trade rate. Built as `scripts/run_b1_variants.py`; the walk-forward in `shared/utils/factor_model.py` steps
monthly and cannot represent daily exits.

**Bar (fixed now).** A variant is a candidate only if it beats V0's net Sharpe on at least 6 of the 8 calendars and by at least 0.10 on
average. B1 beat the plain rule by 0.68 on this protocol, so 0.10 is a modest bar. Nothing changes in the live book until a variant clears
it and the sizing and execution change is approved.

## Phase 6: event-loop blocking (measurement)

Switch on `LOOP_SLOW_CALLBACK_MS=50` in the agents containers (done 12 Nov 02:48 UTC). After 30 minutes of normal operation, rank jobs by
total time holding the loop. A job is moved to a worker thread if it holds the loop over 50 ms more than once an hour. The change
must not alter any result; each moved job gets a test that its output is identical.

## Days-to-cover trial on the short signal cells (registered 12 Nov, before it was run)

**Question.** Do crowded shorts (high days to cover) earn less than uncrowded ones on the tradable short signals, as a squeeze would predict?
**Cells.** 8-K delisting notice, 8-K dilution, insider opportunistic sell, 8-K auditor change, earnings reaction down and stock news negative,
at 5 and 30 days. **Design.** Each family-horizon-week is one unit (the mean of its episodes), aligned so a positive return is the signal
being right. Days to cover is the latest FINRA figure published at least 14 days before the signal. Crowded is 8 or more days to cover.
**Bar.** A cell shows a crowding effect only if the crowded units earn at least 50 bps less than the uncrowded ones with a Welch t of at least
2 in magnitude, in the same direction in both halves of the sample. Fewer than 30 crowded units in a cell: not judged. **If met,** a crowding
filter becomes a candidate for the gate review; **if not,** the finding stands that the B1 leg trial's +0.05 Sharpe was not enough either.


## Top-of-ranking calibration test (registered 13 Nov, before it is run)

**Question.** Is the flat-to-negative top of the monthly alpha calibration (raw alpha of +55 bps and above realised about -11 bps; the isotonic pooling then gives
every such stock one forecast, +32.5 bps) a property of the model, or sample size and volatility? The measured map has about 35 non-overlapping monthly dates, so
a 976-pair top knot has a standard error near 250 bps (assumed monthly sd of about 15%, not stored in the lab) against a spread of 47 bps across the top three knots.
**Design.** Own container, after 06:00 UTC, no other heavy job running. Per date: mean realised excess of the top decile by alpha minus the mean of deciles 4 to 7;
one-sided t over the dates with a date-level bootstrap (10,000 resamples). Second contrast per date: top decile by rank-z alone minus top decile by alpha (alpha = IC x
sigma x z, so the alpha top is as much high-volatility as high-rank). **Bar.** The top decile is judged below the middle only if the bootstrap one-sided p is under 0.05
for the first contrast; it is judged a volatility effect if the second contrast is positive at p under 0.05. Neither: not judged. **If met,** the calibration stops
extrapolating a rising forecast past the last supported knot and the card shows the forecast with its standard error; no change to sizing or the book without approval.
**Note.** The live monthly ideas are one episode (112 graded ideas on 6 dates, 8 to 14 Oct, 50 names, longs concentrated in AI hardware and semiconductor equipment):
effective n is about 1, so their -831 bps against a +88 bps forecast is not evidence about the map either way.


## Analyst on a hosted model (registered 13 Nov, before the shadow has data)

**Question.** Does the analyst do better on a hosted model than on the local CPU model? The local analyst's Brier score is 0.251 (chance is 0.25) and it spends
about 17 minutes of model time a day, while the Gemini free quota is almost unused (5 of 250 a day). **Design.** The existing shadow analyst (`analyst_shadow`)
moves from Groq's 120b (out of token budget most of the day) to `gemini/gemini-3.5-flash-lite`; the main analyst keeps its local model and its graded record.
Both are graded by the same rule on the same alerts. **Bar.** After at least 50 resolved calls on both, the shadow's Brier score is lower than the local analyst's by
at least 0.01 with a paired bootstrap p under 0.05 over the alerts both graded. **If met,** routing the main analyst to the hosted model is a separate step; if not,
the local analyst keeps running unchanged. The shadow's own earlier series (Groq) ends here and is not mixed with the Gemini one.


## Score resolution, macro fan-out rules and new free datasets (registered 13 Nov, before any is built or run)

**Saturated or constant scores.** `finra_short_volume`: 21% of 442 events sit on the 0.95 cap; the z-score and jump are now stored on each new event (tags
`short_vol_z`, `short_vol_jump`) so a replay is possible; the score itself is unchanged. Proposed score: `min(old, 0.55 + 0.40 * (1 - exp(-(z - 2) / 3)))`. Bars for
switching: the share on the cap falls below 5%; rank correlation with the old score at least 0.9; the top 1% by the new score keeps at least the old top 1%'s
mean absolute 5-session move; no event crosses an existing alert cut that it did not cross before. `alpaca_news` (constant 0.50 on 1,433 events): proposed
`0.5 + 0.25 * min(1, max(0, z) / 4)` with z the 6-hour article count against the name's 20-day baseline, range 0.50 to 0.75, so no floor or cut changes; needs the
count stored on the event first. Bars: at least 10 distinct values over 1,000 events, and no change in any count above a cut. `vessel_spoof` (constant 0.82) and
`alpaca_extended_hours` (61% at 0.8 or more) get the same treatment after the first two; nothing is switched without its replay.

**Generic macro fan-out rules** (US2Y Yield Drop 2,301 events in 7 days on 1,084 names at confidence 0.10; Macro Energy Price Headline 952; Cross-Asset Yield 582;
US10Y-Nike 217). Hypothesis: they add volume and no information. Test: retire them in shadow for 14 days. Bars: tier-1 correlations fall by at least 50%; the loss
of confirmed scenarios is under 5%; the 5-session move ratio of the remaining alerts does not fall. Retirement is a separate approval.

**Market-neutral grading of agent predictions.** Store the excess return against SPY (and the sector ETF where one exists) beside each resolved prediction. Raw
direction hit rates are 28 to 36% for down calls against 53 to 64% for up calls, which a falling or rising market alone would produce. Report only: the hit rate on
excess return by direction, with a day-clustered t. No gate changes.

**Datasets with no live collector** (CFTC Commitments of Traders, Treasury auctions, Reg SHO threshold list). Each is collected at release from its free public
source with its publication time recorded. Test per dataset: event study of the next-5-session excess return of the related instruments. Bar: at least 100 events,
t of at least 2 in magnitude, same sign in both halves, and a Benjamini-Hochberg q under 0.10 together with every other cell. Nothing feeds an alert before it passes.

**SEC filing timestamps.** Form 8-K and Form 4 events record collection time as `occurred_at`, so SEC latency cannot be measured (the 13D path already uses the
acceptance time). Bar: at least 95% of new 8-K and Form 4 events carry the acceptance time, and the median collection delay is reported.


### Result of the top-of-ranking calibration test (run 13 Nov, 06:22 UTC, own container, `scripts/research_calibration_top.py`)

101 dates, 20,000 date-level bootstrap resamples. Mean realised excess return per date: top decile by alpha -73.8 bps, middle deciles 4 to 7 -68.5 bps, top decile by
rank z alone -36.9 bps.

- **First contrast (top alpha minus middle):** mean -0.5 bps, t -0.10, one-sided p 0.536. **Bar (p under 0.05 for "below the middle"): not met.** The top of the
  ranking is not shown to earn less than the middle, and not shown to earn more: it is indistinguishable from it.
- **Second contrast (top by rank z minus top by alpha):** mean +36.9 bps, t 1.40, one-sided p 0.083. **Registered bar was p under 0.05: not met**, although the
  script's own flag uses 0.10 and reads true. Direction is consistent with the volatility multiplier hurting (alpha = IC x sigma x z puts high-volatility names in
  the top bucket); not established.
- **Verdict: not judged.** The flat top of the calibration map is neither confirmed as a property of the model nor explained away. The monthly forecast stays
  flagged (`calibration_flat`) and the card does not present it as a forecast; the calibration is not changed. A longer sample (the 101 dates are overlapping
  21-day windows, so the effective number is far smaller) or a second registered test on rank z alone would be needed to move either way.


## Book strategy trial (registered 13 Nov 2026, before it was run)

**Hypothesis.** B1 (equal weight, bottom tenth short if borrowable, 90th to 99th percentile long, rebuilt every 21 sessions) is a strong baseline: the 12 Nov
trial of four lifecycle and sizing variants found none that beat it on 6 of 8 calendars by 0.10. This trial asks whether a different way of building the book from
the same scores does better net of costs. Expected result, stated first: most variants will not clear the bar; B1 stays unless one does. Simulation only: no
trader, sizing, cap, kill-switch or execution code or order changes; a variant that passes is a candidate for the owner's approval and nothing more.

**Scores and calendars.** One purged walk-forward per model (`lgbm_rank`, the live model, and `gbm` for S9), the same monthly-horizon training as B1 (labels 21
sessions ahead, folds of 63 sessions, embargo 21, training dates every 21 sessions), but scoring every session instead of every 21st (`HorizonSpec` with
`test_every=1`), so that weekly, biweekly and staggered books have a score on each rebalance day. The model used on a date is the one the fold fixes, independent of the
calendar, so the eight calendars (offsets 0, 5, 10, 15, 2, 7, 12, 17 sessions; monthly rebalance rows `260 + offset + 21k`) are exactly the ones the B1 trials used.
The panel is `FactorEngine._panel()` cut by `tradable_subset`, as in `run_b1_variants.py`. Names without a finite forward 21-session return on the date are not in its
cross-section (as in every earlier trial), so survivorship is as the earlier trials had it.

**Simulator (new, daily path, `shared/utils/book_strategies.py`).** Simple daily returns of the stocks (missing day = 0). Positions are dollars in units of NAV and
drift between rebalances. A strategy rebalances at the close of its rebalance rows, to target weights of NAV from the scores of that row, with each leg 0.5 of NAV
(dollar-neutral, as `target_weights(leg=0.5)`). Costs, charged identically to every variant including S0: 10 bps per side on every unit of traded notional
(`COST_BPS_PER_SIDE`, the walk-forward's rate; the B1 variants trial charged no rebalance cost because its variants had the same one, which does not hold here) and the
short fee of 44 bps a month on short notional, accrued daily (22 bps a month at a 0.5 short leg, as in `b1_variants`). No interest on cash or proceeds. Because
`run_b1_variants.py` priced exits at 5 bps, the 5 bps rate is reported beside the 10 as a sensitivity for any variant that is a candidate; the bar is judged at 10.
Each calendar's evaluation window starts 21 sessions after its first rebalance (so S7's ramp-up is outside every variant's window) and is cut into non-overlapping
21-session blocks. Net Sharpe is the mean over the standard deviation of block returns times the square root of 252/21 (the convention of the earlier trials); mean net
return is per block; turnover is traded notional per 21 sessions as a multiple of NAV; max drawdown is on the daily NAV.

**Variants (every cell is one test).**
- S0 control: B1 exactly (`auto_trade.b1_book` rule on percentile = rank/(n-1), borrowable names only, equal weights). A test checks it reproduces `b1_book` and
  `target_weights`. No cap filter (the earlier trials had none; the live trader's $1bn rule is a different, snapshot-dated filter).
- S1 industry-neutral: percentile within the two-digit SIC group (`industry_relative`'s rule: groups under 5 names and names without a SIC pooled into one group), B1's bands
  applied within each group, then each group's long and short counts equalised to the smaller by dropping the worse-ranked.
- S2 size-neutral: the same within market-cap terciles, using the best-known cap table (`sentinel:marketcap:best`); only names with a known cap of $1bn or more
  (the trader's rule) enter. **Adaptation:** the table is today's snapshot, so a historical name's tercile and eligibility use today's cap (a look-ahead on size); and S2
  also removes sub-$1bn names that S0 keeps, so its difference to S0 mixes neutrality with that exclusion. A pass would need that separated before it counts.
- S3a decile: long p >= 0.90 (no exclusion of the top 1%), short p <= 0.10 borrowable. S3b quintile: long p >= 0.80, short p <= 0.20 borrowable. S3c: twice B1's
  count in each leg, longs taken downward from the 0.99 cut, shorts upward among the borrowable.
- S4 inverse-volatility weights within each leg (trailing 63-session daily volatility; a name's weight limited to 3 times the equal share, once, then renormalised).
- S5 conviction weights: weight proportional to clip(1 + 0.5 z, 0.5, 1.5), z being the name's score extremity (p for longs, 1-p for shorts) standardised within its leg.
- S6 cadence, five cells: biweekly (10 sessions) and weekly (5 sessions) without a band; monthly (21), biweekly and weekly with a no-trade band. Band: an
  existing position is left alone if its weight is within 25% of the new target weight; entries and exits always trade.
- S7 staggered tranches: four books each of a quarter of NAV, each rebuilt every 21 sessions, offset 0, 5, 10 and 15 sessions from the calendar's first row.
- S8 beta-neutral overlay: the book's beta (trailing 126-session beta of each name to SPY, 1.0 if unknown) hedged to zero with an SPY position inside the simulation,
  reset at each rebalance, 10 bps per side on the hedge's trades, no fee. SPY is never in a selected book and the live trader is not changed.
- S9 blend: percentile of the mean of the `gbm` and `lgbm_rank` percentiles, B1 rule, against `lgbm_rank` alone (S0).
- S10 short filter: drop shorts whose latest FINRA days to cover (the panel's, visible 14 days after settlement) exceeds 8; a short with no figure is kept and the
  share is reported; the leg is not refilled. **Adaptation:** S0 already requires the borrowable list (`borrowable`, easy to borrow, today's list), so "or without
  easy-to-borrow" cannot be tested separately and is part of the control.
- S11 liquidity cap: a name's weight limited to 2% of its trailing 21-session average daily dollar volume at the reference account of $100,000 (the figure the
  trader's documentation uses; the live paper equity was not read); excess stays in cash; a name with no volume is dropped. **Adaptation:** at that size the cap is expected
  never to bind (about $1,100 a name against $3m or more of daily volume), so the primary cell will probably equal S0. Capacity cells at $25m and $250m are reported
  as descriptive only and are not tests.
That is 17 test cells (S1, S2, S3a-c, S4, S5, S6 x5, S7, S8, S9, S10, S11) against S0. No variant is added after seeing results.

**Bar (fixed now).** A cell is a candidate only if (1) its net Sharpe beats S0's on at least 6 of the 8 calendars, (2) the mean over the 8 calendars of the paired
Sharpe difference is at least +0.10, and (3) the date-cluster bootstrap one-sided p that the mean paired Sharpe difference is above 0 is below 0.05: blocks are
grouped by calendar time (21-session bins) and bins are resampled with replacement (10,000 draws, seed 20261113), the statistic being recomputed per draw, p being the
share of draws whose deviation from the observed value reaches the observed value. **Multiple testing:** Benjamini-Hochberg at q < 0.10 over the 17 cells' p-values
(stricter than over 11 variants); a cell that clears (1) to (3) but not BH is reported as not surviving. The report also gives a deflated-Sharpe caution
(`factor_model.deflated_sharpe`, 18 trials including S0) for the best cell. A miss is a miss: no cell is re-run with other parameters, bands or thresholds.

**What each pass would change (stated now).** Selection or weights (S1-S5, S9, S10, S11) change `b1_book`/`target_weights` or the stored book; cadence (S6, S7) changes
`REBALANCE_SESSIONS` and the rebuild logic; S8 would require holding an index fund, which the trader never does, so it could only be approximated with the book's own
names. Every one is a sizing or execution change needing the owner's explicit approval.

**Not measurable here (stated now).** Real borrow availability and fees by date (today's list, flat fee); market impact beyond the flat 10 bps; historical market caps;
delisted names; a historical short-interest list for names with no FINRA figure.

**Amendment BST-1 (13 Nov 2026, written before any run of this trial).** The S4 weight limit above said "limited once, then renormalised". The unit test written for it showed
that does not hold the limit (a very quiet name came back at 94% of its leg). S4 now redistributes capped excess among the uncapped names until no name is above 3 times the
equal share. Nothing else changes; no run has used the earlier wording.


### Result of the days-to-cover trial (run 13 Nov, own container, 2,842 s, 114,468 episodes on 8,819 instruments)

Crowded = 8 or more days to cover, FINRA figure published at least 14 days before the signal. Units are family-horizon-weeks; the signed return is aligned so a
positive number means the signal was right. Registered bar per cell: crowded units earn at least 50 bps less, Welch t of at least 2 in magnitude, same direction in both halves,
at least 30 crowded units.

| Cell | Crowded / uncrowded units | Mean crowded | Mean uncrowded | Difference | Welch t | Halves agree | Verdict |
|---|---|---|---|---|---|---|---|
| 8-K auditor change 5d | 45 / 228 | -155 | +16 | -171 | -1.45 | yes | no effect shown |
| 8-K auditor change 30d | 45 / 224 | -129 | +115 | -243 | -0.86 | yes | no effect shown |
| 8-K delisting notice 5d | 87 / 314 | -449 | +157 | -606 | -2.44 | no | no effect shown |
| 8-K delisting notice 30d | 85 / 309 | -279 | +594 | -873 | -2.43 | no | no effect shown |
| 8-K dilution 5d | 238 / 446 | +54 | +119 | -65 | -0.97 | no | no effect shown |
| **8-K dilution 30d** | **233 / 443** | **+75** | **+537** | **-462** | **-3.40** | **yes** | **CROWDING EFFECT** |
| Earnings reaction down 5d | 205 / 261 | +62 | +16 | +47 | +1.36 | yes | no effect shown |
| Earnings reaction down 30d | 201 / 258 | +211 | +152 | +59 | +0.74 | no | no effect shown |
| Insider opportunistic sell 5d | 388 / 459 | +27 | +6 | +21 | +1.05 | yes | no effect shown |
| Insider opportunistic sell 30d | 385 / 456 | +18 | +82 | -64 | -1.08 | yes | no effect shown |
| Stock news negative 5d | 150 / 171 | +54 | +11 | +42 | +1.06 | no | no effect shown |
| Stock news negative 30d | 146 / 168 | +157 | +100 | +57 | +0.58 | no | no effect shown |

**Verdict.** One of twelve cells meets the bar: crowded 8-K dilution shorts earn 462 bps less than uncrowded ones over 30 days (t -3.40, p about 0.0007, still about 0.008
after a Bonferroni factor of 12; both halves agree). The delisting-notice cells point the same way (t -2.4) but the halves disagree, so they are not judged. No other cell
shows an effect. **Per the registration,** a crowding filter on the 8-K dilution short is a candidate for the gate review (a filter on that one cell, not the book): moving
the gate is a separate approval and nothing was changed. The book-strategy trial's short-leg filter (days to cover above 8) tests the same idea on the whole book.


## Free-dataset event studies: signal definitions (registered 13 Nov, before any of the three studies is run)

The three collectors store data only (`reports/Free datasets - 13 Nov.md`). The signal definitions below were fixed by the builder in the study scripts before any run and are
registered here unchanged; the bars are the ones registered on 13 Nov for the new datasets (at least 100 events per cell, |t| at least 2, same sign in both halves, Benjamini-Hochberg
q under 0.10 jointly across every cell of all three studies). A cell under 100 events fails by definition. Return in every study: log return of the related instrument minus SPY over five
sessions, measured from the close of the first session after the publication date; t clustered by date.

- **CFTC Commitments of Traders** (disaggregated, futures only; commodities only, so no rates, FX or equity index). Managed-money net position over open interest, ranked against its own previous 156
  weeks (at least 104 needed, current week excluded). Event `crowded_long` at rank >= 0.90, `crowded_short` at <= 0.10. Cells: group (energy, metals, grains) x side; instrument: the mapped ETF.
  Published_at is the scheduled Friday 15:30 ET, so entry is Monday.
- **Treasury auctions** (notes and bonds only). Bid-to-cover and the indirect bidder share, each as a z against the previous 12 auctions of the same type and term (at least 8). `weak` at z <= -1,
  `strong` at z >= +1. Cells: bucket (short 2-, 3-, 5-year -> IEI; mid 7-, 10-year -> IEF; long 20-, 30-year -> TLT) x metric x side. The tail against the when-issued yield is not in the free feed and is not tested.
- **Reg SHO threshold list** (Nasdaq's list only). `entry`: a symbol on a list date that was not on the previous list date; `exit`: on the previous and not on this one; the previous list must be at most
  5 calendar days earlier. Cells: Nasdaq category (Q, G, S) x entry or exit. The list is created about 23:00 ET, so entry is the next session.

Nothing from these studies reaches an alert, a gate or the recommendation engine unless a cell passes, and a pass is a candidate for its own approval.


### Results of the free-dataset event studies (run 13 Nov, own containers; raw output in `reports/study_cftc_cot.json`, `study_treasury_auctions.json`, `study_reg_sho.json`)

**Treasury auctions** (476 events from 1,126 note and bond auctions). No cell passes: every |t| is under 2 (largest: short-bucket strong bid-to-cover, -47.7 bps, t -1.95, q 0.46), most cells
have 7 to 88 events, and four of the twelve have halves that disagree. Verdict: no signal shown.

**CFTC Commitments of Traders** (2,291 events, 8 commodity contracts). By the registered bar alone four cells pass: energy crowded long (n 227, -558 bps, t -14.2), energy crowded short
(n 295, -400 bps, t -12.4), metals crowded long (n 413, -54 bps, t -3.3) and metals crowded short (n 418, -60 bps, t -4.2); grains are large and positive on both sides but their halves
disagree. **These are not accepted as signals.** Crowded long and crowded short move the SAME way in each group, which is the signature of the instrument's own drift against SPY (oil and gas
ETFs lose to roll decay; gold and silver trailed SPY), not of positioning information. The registered design compared each event to SPY only and had no baseline: a design flaw found
on reading the result, not a hidden choice. **Amendment DS-1 (registered before any re-run):** each cell's event return is measured against the same instrument's unconditional average
5-session excess return over all weeks (the baseline), and a positioning signal is accepted only if crowded_long minus crowded_short is positive with |t| >= 2, same sign in both halves, BH
q under 0.10. Not yet re-run.

**Regulation SHO threshold list** (2,954 events, 1,017 symbols, Nasdaq only). By the registered bar three cells pass: capital-market entries (n 991, -1,305 bps, t -9.9), capital-market exits
(n 982, -748 bps, t -6.8) and global-market exits (n 416, -313 bps, t -2.6); global-market entries just miss (t -1.98); the select-market cells have under 100 events. Entries and exits are BOTH strongly
negative, so what is measured is that stocks on the threshold list are speculative small caps that trail SPY, not that entering or leaving the list predicts anything. **Not accepted
as a signal, and not tradable by this book's own rules:** threshold securities have persistent fails to deliver, are mostly hard to borrow, and fail the shortable-and-easy-to-borrow and $140.8M
a day liquidity conditions of the tradability gate; symbols without daily bars (delisted) were dropped, which flatters the result. **Amendment DS-2 (registered before any re-run):** compare
entries with a matched control (same Nasdaq category, same quarter, similar market cap and volume, not on the list) and report how many events pass the tradability gate; accept nothing that does not
survive both.


### Amendment DTC-1 to the days-to-cover trial (registered 13 Nov, before the re-run)

A review of the first run found that its 30-day units are single weeks, and a 30-day window spans about 4.3 weeks, so consecutive weekly units share about four of their 4.3
weeks of returns; the Welch t on the 30-day cells is overstated (about -3.4 becomes about -1.6 once adjusted for the overlap, by the reviewer's estimate), and the 8-K dilution 30d cell
that passed probably does not. **Change:** a 30-day unit is a block of 35 calendar days (non-overlapping); 5-day units stay as one week. Everything else in the registration is unchanged
(crowded = 8 or more days to cover, published 14 or more days before the signal, at least 30 crowded units, crowded earn at least 50 bps less, |Welch t| at least 2, same direction in both
halves of the sample). The result of the first run (one cell met the bar) is superseded by this one for the 30-day cells. Also reported, not part of the bar: a paired test over blocks that
contain both crowded and uncrowded episodes. Not changed: no gate or book is touched by this trial.


### Result of the days-to-cover re-run under amendment DTC-1 (13 Nov, own container, 2,315 s; 30-day units are non-overlapping 35-day blocks)

| Cell | Crowded / uncrowded units | Mean crowded | Mean uncrowded | Difference | Welch t | Halves agree | Paired t (n) | Verdict |
|---|---|---|---|---|---|---|---|---|
| 8-K auditor change 5d | 45 / 228 | -155 | +16 | -171 | -1.45 | yes | -1.35 (21) | no effect shown |
| 8-K auditor change 30d | 36 / 87 | -5 | +27 | -32 | -0.10 | no | +0.33 (35) | no effect shown |
| 8-K delisting notice 5d | 87 / 314 | -449 | +157 | -606 | -2.44 | no | -2.33 (64) | no effect shown |
| 8-K delisting notice 30d | 51 / 90 | -385 | +403 | -788 | -2.13 | no | -2.44 (50) | no effect shown |
| 8-K dilution 5d | 238 / 446 | +54 | +119 | -65 | -0.97 | no | -1.37 (231) | no effect shown |
| **8-K dilution 30d** | **90 / 93** | **+13** | **+607** | **-594** | **-3.50** | **yes** | **-3.92 (90)** | **CROWDING EFFECT** |
| Earnings reaction down 5d | 205 / 261 | +62 | +16 | +47 | +1.36 | yes | +1.35 (203) | no effect shown |
| Earnings reaction down 30d | 52 / 54 | +143 | +110 | +33 | +0.35 | no | -0.01 (52) | no effect shown |
| Insider opportunistic sell 5d | 388 / 459 | +27 | +6 | +21 | +1.05 | yes | +0.73 (388) | no effect shown |
| Insider opportunistic sell 30d | 92 / 92 | -4 | +83 | -87 | -1.32 | yes | -1.52 (92) | no effect shown |
| Stock news negative 5d | 150 / 171 | +54 | +12 | +42 | +1.05 | no | +1.02 (150) | no effect shown |
| Stock news negative 30d | 33 / 38 | +146 | +111 | +35 | +0.33 | no | +0.61 (33) | no effect shown |

**Verdict.** The 8-K dilution 30-day cell still meets the registered bar with non-overlapping units: crowded shorts earn +13 bps, uncrowded +607 bps, a difference of -594 bps (Welch t -3.50,
both halves agree, paired t -3.92 over 90 blocks that hold both kinds). The reviewer's estimate that the overlap correction would take the t to about -1.6 did not hold: the weekly units
overlapped, but the effect is large and the 90 blocks are independent. No other cell shows an effect; the delisting-notice cells point the same way (Welch t -2.1 to -2.4, paired t -2.3 to -2.4) but
their halves disagree. **Caveats that keep this a candidate and not a change:** 90 units a side; a mean of unit means dominated by a few large uncrowded winners (uncrowded +607 bps is the unusual number,
crowded is about zero); the sample includes micro and small names the book's liquidity gate refuses. **Amendment DTC-2 (registered before it is run):** repeat the dilution 30d cell on the names that pass the
tradability gate (shortable, easy to borrow, $140.8M a day average dollar volume), report units and the same statistics, and report the median alongside the mean. Only a pass there makes a crowding filter on
that signal a candidate for the gate review; moving the gate remains a separate approval, and nothing was changed.


### Result of the CFTC study under amendment DS-1 (13 Nov; `scripts/study_cftc_cot.py --adjusted`, raw output `reports/study_cftc_cot_ds1.json`)

Unconditional baselines (mean 5-session excess over SPY across every week, bps): grains +184, energy -436, metals -60 (confirming the instrument-drift reading). After subtracting them no cell passes the
registered bar: energy crowded long -123 bps (t -3.13) fails on halves (-297 then +50), energy crowded short +35 (t 1.09), grains and metals cells |t| under 1.6. The registered contrast (crowded_long minus
crowded_short must be POSITIVE, |t| at least 2, same sign in both halves): energy -158 bps (t -3.2, both halves negative) is the wrong sign for the registered direction and is not accepted; grains -57 (t -2.3,
halves disagree); metals +6 (t 0.4). **Verdict: no accepted signal.** The energy result (crowded longs trailing crowded shorts, consistently in both halves) is a pattern in the opposite direction to the one
registered. It is recorded as an observation only: reading it as a signal now would be choosing the direction after seeing the data; testing it needs its own registration and fresh data (the CFTC feed adds one week at a
time, so a crowded week in the two energy contracts arrives only occasionally). Reg SHO amendment DS-2 is not yet run.


### Result of the Reg SHO tradability check under amendment DS-2, first half (13 Nov; `scripts/study_reg_sho.py --tradable`, raw output `reports/study_reg_sho_ds2.json`)

Gate applied per event: average dollar volume of at least $140.8M over the 30 sessions before the list date, and (for the short side) shortable and easy to borrow in the asset list today.
**38 of 2,954 events pass (1.3%)**: G entry 8 of 452, G exit 10 of 416, Q entry 4 of 57, Q exit 3 of 56, S entry 5 of 991, S exit 8 of 982. Every cell has far fewer than the 100 events the registered bar needs, so none can
be judged (the largest, S entry on 5 events, shows -895 bps with t -3.7 on 5 clusters, which is not a result). **Verdict: the Reg SHO threshold list is not tradable by this book's own rules;** the
statistically significant cells of the first run describe names the book cannot trade. **Second half (the matched control) is not run:** non-listed names carry no Nasdaq category in the data held here, and with the
tradable share this small it could not change the verdict; the Reg SHO dataset stays stored as research data only and feeds nothing.


## Revised score bars and a single forward test of conviction weights (registered 13 Nov, before either is run)

**Why revised.** The 13 Nov replays failed `finra_short_volume` on rank correlation with the old score (0.25 against 0.9). That bar fails by construction: the old score is 0.55 + 1.5 x the size of the
jump, the proposal ranks on the jump relative to the name's own spread, and the two are different orderings. The `alpaca_news` bar assumed a 0.50 to 0.75 range crosses no cut, which was false (the
0.6, 0.65, 0.7 and 0.75 cuts exist in code). `alpaca_extended_hours` is a re-order, for which rank correlation with the old score is the wrong test.

**Outcome test for `finra_short_volume` and `alpaca_extended_hours` (replaces the rank-correlation bar; the cap, top-1% and no-newly-above-cut bars stay).** On events that have a 5-session outcome (at least 300; 349
exist for finra), Spearman correlation of the score with the 5-session absolute move (the producer grader's move ratio is reported beside it): the new score's Spearman must be at least the old score's. A new score
that ranks outcomes no better than the old one is not adopted. For finra, the proposal `min(old, 0.55 + 0.40 x (1 - exp(-(z - 2) / 3)))` has a known cost, registered here so it is a decision and not a surprise: **59%
of events fall below 0.6 and 92% below 0.7**; an alternative that keeps the median at or above 0.7 is `0.70 + 0.25 x (1 - exp(-(z - 2) / 3))`. Either form is tested; which one (if any) is adopted is an owner decision
about the cut loss.

**`alpaca_news`.** First trace which cuts (0.6 to 0.75) apply to the event type `headline` in the code that consumes these events; then register a range that stays under the lowest applicable cut (for example
`0.5 + 0.09 x min(1, z / 4)`), z as registered (6-hour article count against the 20-day baseline, stored on new events by the correlation scorer once it is restarted: that was done 13 Nov). No score is
changed before the trace is written down and the bars below pass: at least 10 distinct values over 1,000 events, and no count above any applicable cut changes.

**`vessel_spoof`.** Dropped: a vessel has no price outcome, so no rescoring can be tested until one exists.

**Macro fan-out rules.** The four rules show no information gain (0 confirmed scenarios lost, move ratio not worse) but retiring them removes 39.7% of tier-1 correlations, not 50%, and the move ratio rests on 51
gradable rule-stock-days. Re-run the shadow after about two weeks (about 27 Nov); retirement stays an owner decision. A broader family can be registered if volume reduction is the goal.

**Conviction weights (S5), one forward test.** From the 13 Nov book trial (`reports/Book strategy trials - 13 Nov.md`): S5 won 7 of 8 calendars (p 0.013) but gained +0.0995 against 0.10 and failed
Benjamini-Hochberg over 17 cells (q 0.22, deflated Sharpe 0.16). **Single hypothesis, registered as written there (same slope and bounds: 0.5, clip 0.5 to 1.5; same control S0; same 10 bps a side and fee accounting),
no tuning:** on data NOT used in that trial, S5 beats B1 on at least 6 of 8 calendars with a mean Sharpe gain of at least 0.10 and a one-sided date-bootstrap p under 0.05. No multiple-testing correction because it is one
test. The fresh data are the sessions after 12 Nov, so it needs months to accumulate; a pre-1 Nov holdout would count only if the 13 Nov run provably did not touch it (not checked). As a paper shadow book beside B1
(same names, only the weights argument differs, turnover 1.44x against 1.40x), never replacing it; **running the shadow, and any live use, needs the owner's approval (a sizing change).**


### Results of the revised score tests (13 Nov; `scripts/outcome_test_scores.py`; full output at the end of `reports/Grading and score replays - 13 Nov.md`)

- **Cut trace for `alpaca_news` headlines.** The lowest cut that applies to type `headline` is 0.65 (`services/agents/base.py:2262`, world-state top anomalies, any type), not 0.6. The 0.60 cuts do not apply to
  headlines (`dossier_engine.py:50` and `collector-tradfi/main.py:1245` use type lists without headline; `correlation/main.py:602` and `:617` are flight anomalies only). 0.70 is display only
  (`api_gateway/routes/explain.py:281`) and 0.8 is signal attribution (`reasoning/signal_attribution.py:65`); correlation rules that take headlines use floors of 0.20 to 0.30. A news range under 0.65 is `0.5 + 0.14 x min(1, z/4)`
  (maximum 0.64); the registered 0.09 form (maximum 0.59) is also under it. **Neither has been replayed, so `alpaca_news` is unchanged.**
- **`finra_short_volume`, outcome test (Spearman of score against the 5-session absolute move, 349 events).** Form A (0.55 + 0.40 x ...): PASS on all four bars, Spearman +0.063 against the old score's -0.234; 59.3% of events fall below
  0.6 and 91.7% below 0.7. Form B (0.70 + 0.25 x ..., capped at the old score): PASS on all four bars, Spearman +0.049, and **no cut cost** (0% below 0.6 or 0.7). The pass is weak: the old score is negatively related to the next move and the new
  ones are about zero; the standard error is about 0.054 on 349 events clustered over about 30 days, and against the grader's move ratio both old and new are about 0. Form B is the only candidate without a cut cost; it is an owner decision
  and not recommended on this evidence alone.
- **`alpaca_extended_hours` (2,780 events).** FAIL on the outcome bar (Spearman -0.054 new against -0.038 old); the other bars pass. **Not adopted.** Below 0.6 would go from 36.2% to 50.0% of events and below 0.7 from 44.1% to 71.7%.
- `SCORE_RESOLUTION_V2` stays off; no score changed.


### Result of the days-to-cover trial on tradable names (amendment DTC-2; 13 Nov, `scripts/shadow_crowding.py --tradable`, 2,853 s)

Only instruments that pass the book's gate at the signal (average dollar volume of at least $140.8M over the prior 30 sessions; shortable and easy to borrow today) are kept; non-overlapping 35-day units for 30 days.
**8-K dilution 30d: 30 crowded / 87 uncrowded units, crowded +139 bps, uncrowded +98 bps, difference +41 bps, Welch t +0.21, halves disagree, paired t +0.43 (n 28): no effect.** The 5-day dilution cell is +122 bps (t +1.50, no
effect). Auditor-change and delisting-notice cells have 2 to 5 crowded units: not judged. Insider opportunistic sell 30d points the registered way (-177 bps, t -1.82, halves agree) but misses |t| of 2. Earnings-reaction-down and
stock-news-negative show no effect.
**Verdict: the crowding effect found in the full sample (-594 bps, t -3.50) does not survive the book's tradability gate.** It lived in micro and small names the book already refuses; among tradable names crowded dilution
shorts did not earn less. Per the registration, only a pass on tradable names would have made a crowding filter a candidate for the gate review, so **no crowding filter is a candidate and nothing is changed.** The
book-strategy trial's S10 (short-leg filter) found nothing to act on for the same reason.


**S5 shadow recorder, started 13 Nov (`services/agents/book_shadow.py`).** It records S0 and S5 weights when the trader's stored book changes, weighted by the model's percentiles at the time it records. Its first row
(rebalance of 29 Oct, 184 names) was recorded on 13 Nov with the model's 12 Nov scores, so its S5 weights use scores dated after the rebalance: **that row is look-ahead and is excluded from the test** (the report script flags it).
The valid record starts at the next rebalance (about 26 Nov, 21 sessions after 29 Oct); the registered bar is judged only on rows whose scores are dated within 3 days of their rebalance.


## Registrations from the signals, AI-output and agent-structure research, area A (registered 13 Nov, before any is run; interim A of the research session, `scripts/research_family_cluster.py`)

**A1. Family grades are clustered and controlled (published, not yet used to act).** One unit per (family, ticker, detection date); each unit's 1- and 5-session move ratio is compared with the mean ratio of up to 120
random stocks on the same date; the statistic is the mean of per-date mean differences with a t over DATES. Published as the extra `clustered` key of `/predictions/producer-grades` (daily job `services/agents/grades_v2_job.py`);
`uninformative_rules`, retirement and every gate are unchanged. **Rule to register for acting on it (not applied):** a correlation family is uninformative if, over at least 8 detection dates, the mean per-date excess over the
same-date control has a one-sided t over dates below 1.65; it is retired only if the excess is also below 0 in both halves of the dates. No family is re-run with other parameters; the 90-day window and the seed are fixed.
First reading (90 days, 19,749 units): only Quantitative Equity Peer Cointegration & Decoupling beats the control at one session (+0.43, t 3.16 over 11 days, 54% of units on 3 days) and Company News & Trading marginally (t 1.97, 9 days);
Semantic Convergence +0.01 (t 0.15), Equity Block & Options -0.10 (t -1.95), Hawkes -0.09 (t -2.0), Geopolitical Cascade -0.01; Crypto Venue Stress (-0.59, t -4.5), Insider Sale Into Options (-0.53, t -6.8),
Market Anomaly Cluster 5d (-0.26, t -4.2) and Informed Trading Sequence (-0.43, t -3.4) are significantly below control. Retirement of any of them is a separate approval.

**A2. Agent output stays an alert source only if it shows skill.** Rule: an agent's output is kept as an alert source only if its per-date-clustered Heidke skill score over at least 20 independent days has a lower 90% bound above 0;
`quant_trading_engine` is evaluated first (stored: hit 0.448 on 402 calls, z -2.43, mean -136 bps over 374 calls, t -3.6; macro engine hit 0.608 against chance 0.605; analyst 0.526 against 0.518; `analyst_shadow` 0.500);
`stock_correlation_agent`'s `.baseline` (hit 0.59, +50 bps, t 6.3 on 824 rows, but 808 ticker-days) is re-graded clustered by date. Four `stock_correlation_agent` variants emit on the same ticker-days (161 ticker-days with 2 or more agents),
which is double counting within the layer. Demoting or gating the quant engine's calls from alerts is the owner's decision; nothing is changed by this registration. **Correction 14 Nov:** the stored figures above count calls as independent. Clustered by date the engine is not distinguishable from chance (357 calls, 29 dates, mean -155.6 bps, t -1.37; 24-hour calls +8.6 bps, t 0.36), and nothing in the alert manager reads it. Keep it; A2 decides. The same flaw applies to the other agent_skill and agent_calls rows: re-grade clustered by date before any agent decision. **Clustered re-grade, all agents (14 Nov, `scripts/research_agent_cluster.py`, 90 days, `agent_predictions.excess_spy`, one observation per detection date, t over dates):** macro_intelligence_engine 24h +84.2 bps, t 2.03, 24 dates (94% of calls are "down"; does not survive a correction for 8 agents tested); stock_correlation_agent.baseline +105.4, t 1.87, 7 dates, 71% of calls on 3 dates (stored t was 6.3); .sympathy +14.2, t 0.45, 8 dates; stock_correlation_agent +22.9, t 0.43, 4 dates; .evidence -18.3, t -0.73, 5 dates; analyst_shadow 168h +383.5, t 0.93, 5 dates; analyst_agent 168h -426.3, t -1.31, 6 dates; quant_trading_engine all -189.0, t -1.65, 30 dates (24h +8.6, t 0.36, 27 dates; 720h -1,467.8, t -3.74, 5 dates, one week). Reading: no agent has demonstrated skill. The correlation-agent variants and both analysts have 4 to 8 dates and are not judged under the 20-independent-days rule, which is different from no skill. **Beta-adjusted (18 Nov, migration 0099, `excess_beta` = stock return minus 126-session beta to SPY times SPY's return, beta from bars before the call day; stored beside `excess_spy`, which is kept; the daily grader fills it and the grades v2 job grades both bases, `basis` on each row).** The SPY-only excess credits high-beta shorts in a falling market. 3,274 of 3,277 graded rows filled. Live, date-clustered: macro_intelligence_engine +82.1 bps t 2.14 on excess_spy, +46.3 bps t 1.68 beta-adjusted (26 dates; the 94% 'down' calls on beta-2.3 names were most of the edge); stock_correlation_agent.baseline +71.4 t 1.80 becomes +7.2 t 0.46 (11 dates); quant_trading_engine -192.1 t -1.82 becomes -119.1 t -1.65 (33 dates). No agent has a clustered, beta-adjusted edge; none meets the 20-date rule with a t over 2.5.

**A3. 13F ownership breadth (the one free-data candidate testable now).** Feature: quarter-on-quarter percentage change in the number of reporting positions per ticker (`fund_holdings_monthly`, 28 quarters, 2019q4 to 2026q3), visible only from
`first_public`. Per rebalance date, rank within the tradable set; statistic: mean spread of the top against the bottom quintile's 21-session excess return, date-clustered bootstrap with 10,000 draws; bar: |t| >= 2 in both halves with the same sign as
the full sample, TWO-SIDED (Chen, Hong and Stein 2002 find falling breadth precedes low returns; Choi, Jin and Yan 2013 find the sign depends on weighting). Entered into the model only through the existing walk-forward as one extra feature, never by hand.
Not testable yet, registered for a run at about 12 months of data: Form 144 (21 days of notices), analyst estimate revisions (31 daily snapshots since 9 Oct) and borrow-fee changes (32 days).

**A5. Calibrated score design (to be written in the research session's final report):** hierarchical shrinkage (existing `hier_bayes`) for the posterior mean plus date-blocked split-conformal half-widths per cell; output the conservative bound and flag a plateau
(Barber et al. 2023; Gibbs and Candes 2021). Registered when the design is final.


## Registrations from the AI-output research, area B (registered 13 Nov, before any is run; interim B, rubric fixed first, `scripts/research_llm_rubric.py`)

**B1. Deterministic scoring of analyst price-target headlines.** 3,317 of 14,356 scored headlines (23%) are analyst price-target notices; the model scores 59% of "Raises price target" as 0 (666 of 1,128, 22 as -1)
while "Lowers" averages -0.36 and "Raises" +0.37, so it is inconsistent where a rule is exact; 17% of rows carry the `#stale` tag with no event type. **Registered change:** replace the model's score on headlines matching
`(Raises|Lifts|Boosts) ... Price Target`, `(Lowers|Cuts) ... Price Target`, `Upgrades`, `Downgrades` by +1, -1, +1, -1. **Test (offline, before adoption):** recompute `llm_news_30` under both scorers over the window of the stored 14,356 scores;
(a) agreement on non-price-target headlines must be unchanged by construction; (b) the walk-forward net Sharpe and rank IC with each feature version across the eight offset calendars. **Bar:** adopt only if the deterministic version is not worse by more than
0.02 Sharpe on at least 6 of 8 calendars AND it cuts model calls by at least 20%. No other change. (The feature series is kept on one model for grading, so the switch is a feature-series change and needs this test first.)

**B2. Deterministic self-consistency checks on scenarios (built, count only).** `shared/utils/llm_output_checks.py`, called from the scenario generator after a scenario is built: counts (site `reasoning.scenario_confidence_mismatch`) a narrative that states a
different 'Overall confidence N%' than the stored field (22% of 63 scenarios; for example text 60, field 85), counts round probability templates (variant `60/30/10`, `65/25/10`, `60/25/15`: repeated 8, 7 and 5 times), and counts scenarios that call their own
correlation an artifact or noise (22%). It rejects, rewrites and gates nothing. A later registration may use the counts (for example lowering confidence to the narrative's figure); that is not part of this one.

**Findings that are the owner's to decide (no change made):** `analyst_agent` on the local model spends a median 61 s per output, 91% of its 141 `p_beat` values sit inside 0.47 to 0.53, its consumer is a dashboard list only and its skill is none (Heidke 0.017);
`analyst_shadow` is better formed but also at chance. The adversarial wargamer (v2) made 96 moves in 7 days with templated `disruption_potential_percent` (85 x31, 70 x26, 40 x17), role-play content with no falsifiable claim, one dashboard consumer, and entity-claim skill below its
placebo (hit 0.585 against 0.737 on 171). Both are candidates to retire. Best-formed output: the deterministic divergence briefs (all numbers present, no model call).


## Registrations from the structure research, area C (registered 13 Nov, before any is run; interim C of the research session)

**Funnel, last 7 days:** 66,867 events -> 9,953 correlations (tier 1: 7,413, tier 2: 2,531, tier 3: 9) -> alert-manager decisions since 5 Nov: 1,209 ALERT against 8,630 WATCH (12% reach alert; 70% of alerts, 848, are Peer Cointegration &
Decoupling, the one family that survives day-clustering) -> 55 scenarios (0.55% of correlations; 7 confirmed, 1 denied) -> 858 agent predictions and 234 recommendation ideas -> 12 human notifications. Hosted tokens 2.19M in 7 days; local model time 13,001 s.

**C1. Agents read only alert-tier correlations.** `sentinel.correlations` has 11 consumer groups and 14,866 messages retained over 3 days, about 163,000 handler invocations, yet 74% of correlations (7,413 of 9,953 in 7 days) are the WATCH tier (database alert_tier 1; 2 = ALERT, 3 = ELEVATED, 4 = INTELLIGENCE, 5 = CRITICAL; the gate that stops a WATCH cluster is `services/alert_manager/main.py:172-174`; the earlier 88% came from log lines and is superseded), and the generic macro and block rules (US2Y Yield Drop 2,301, Equity Block & Options 1,661, Macro Energy
Price Headline 952, Cross-Asset Yield 762, Headline to Equity Block 387) only ever reach WATCH with no excess move in the day-clustered grade. **Registered change (not applied):** the agents (`stock_correlation_agent`, both analysts, consensus, macro, quant, wargamer, `rule_synthesizer`, radar) read only
correlations whose database `alert_tier` is 2 (ALERT) or higher, or that a rule marks `agent_input`. **Offline replay on the last 14 days of correlations; fail if (a) is not met:** (a) every graded agent prediction that exists today is also produced under the restricted input (at least 90% overlap by ticker-day); (b) no alert-tier correlation is dropped;
(c) handler invocations fall by at least 50%.

**C2. Dead or no-consumer structure.** Measure first (built: the correlation funnel route below); deleting is the owner's decision. Found: `alerts.outbound` (no producer or consumer in code; 143,745 lifetime offsets, none retained), `agents.ontology.updates` and `agents.ontology.unknown_entities` (no consumer group; `knowledge_graph_engine`
is disabled), `agents.intel.briefs` (no producer), `agents.consensus.reports` (read only by the consensus engine itself), analyst views and wargamer predictions (read only by dashboard routes), and `sentinel.ontology.proposals` (201,544 messages in 3 days, mostly vessel-to-region LOCATED_IN edges, into Neo4j with no measured reader). Five voices on one peer-divergence idea: rule ->
`stock_correlation_agent` (4 variants) -> scenario -> two analysts -> divergence watch -> recommendation.

**C3. Ablate the two biggest hosted spenders.** 7-day hosted tokens: llm_news 976k (45%), comment_letter 276k (13%), stock_correlation_agent 256k (12%), scenario_generator 240k (11%, 4.4k per scenario and 34k per confirmed one), analyst_shadow 136k, rule_synthesizer 124k, 13D 88k, wargamer 59k. Local: `analyst_agent` 11,733 of 13,001 model-seconds (90%) for about 86 distinct outputs.
The two largest hosted spenders only feed model features (`llm_news_30`, `ev_comment_letter`) and were never ablated. **Test:** remove `llm_news_30` (run 1) and `ev_comment_letter` (run 2) from the monthly feature set; walk-forward on the 8 offset calendars used for B1; **keep the calls only if removal costs at least 0.03 net Sharpe on at least 6 of 8 calendars (paired); otherwise stop the calls**
(saves 45% and 13% of hosted tokens). Not run.
**C3 amendment (14 Nov, before any run of the new form; from the research session's check that neither feature is in the live model's 29 names):** removal cannot cost anything, so the ablation form above is vacuous (see the results below, which ran it as written). The test is add-or-stop: **add `llm_news_30` (run 1) and `ev_comment_letter` (run 2) to the monthly walk-forward on the same 8 offset calendars; keep paying for the calls only if adding the feature gains at least 0.03 net Sharpe (paired) on at least 6 of 8 calendars; otherwise stop the calls.** `llm_news_30` has 16 days of history, so run 1 cannot be scored until about 260 sessions exist; until then it is not judgeable and the calls continue only as a cost, to be reviewed by the owner. `ev_comment_letter` has no rows since 11 Jun, so run 2 uses its 2023 to 2026 history. Not run.


**C4. Scenario admission gate (registered 13 Nov from the final research report, not applied).** 22% of scenarios label their own correlation an artifact or noise (for example "Algorithmic Spurious Cointegration Artifact (70%)"), and non-company subjects ("AI") and unrelated pairs (MSFT against TLT) reach the
generator. Registered change: do not build a scenario for a cointegration pair whose members share no sector, supply-chain or peer link, or whose subject is a non-company term (`shared/utils/subjects.py`). **Test (offline replay on the stored scenarios):** the confirmed-scenario count is unchanged and total scenario calls fall by at least 20%.
No change until it passes.


### Result of the agent-input replay C1 (14 Nov; `scripts/replay_agent_input_gate.py`, last 14 days: 2,009 predictions on 1,689 ticker-days, 15,286 correlations)

- **(c) Handler invocations fall by the WATCH share: 10,197 of 15,286 correlations (66.7%): PASS (bar 50%).**
- **(b) No alert-tier correlation is dropped:** true by construction (5,089 of 15,286 kept).
- **(a) Share of graded predictions still produced: NOT JUDGEABLE as registered.** `agent_predictions.correlation_id` is empty for 2,009 of 2,021 rows (99%): the resolver writes those rows from a stored prediction that carried no correlation id, so no
  prediction can be joined to the correlation that produced it. Linked by the same ticker in the 24 hours before the prediction, only 341 of 1,689 ticker-days (20%) have any correlation, because the sympathy agents predict peers of the trigger and not its
  primary entity; of those 341, 318 (93.3%) have an ALERT-tier or `agent_input` correlation. That 93.3% is a proxy over a fifth of the sample, not the registered measurement. By agent (predictions / linked / linked to tier 2 or higher):
  `stock_correlation_agent.baseline` 984 / 213 / 198, `.sympathy` 633 / 47 / 44, `stock_correlation_agent` 130 / 22 / 21, `.evidence` 110 / 28 / 26, `macro_intelligence_engine` 70 / 51 / 45, `analyst_shadow` 39 / 39 / 39, `quant_trading_engine` 24 / 9 / 9.
- **Fix (14 Nov):** predictions now carry their source: the correlation agent's message handler sets `CURRENT_CORRELATION_ID` (a context variable); `record_prediction` stores it in the prediction's features; the resolved-prediction insert writes it to the existing `correlation_id` column (`services/agents/prediction_scoring.py`).
  Coverage is the correlation agent's four variants (92% of predictions); the macro, analyst and quant paths are not yet covered. **Re-run (a) when about 14 days of lineage exist (about 28 Nov)** with the registered join. No agent input is gated until it passes.


### Results of the feature ablation (C3) and the price-target scoring test (B1) (14 Nov, 06:00 UTC, own container; `reports/Feature ablation and price-target trials - 13 Nov.md`, `scripts/run_feature_trials.py`)

**What decided both: neither `llm_news_30` nor `ev_comment_letter` is a model input.** `factor_model.FEATURES` (the live monthly `lgbm_rank` list) contains neither; they are written to `stock_features_daily` for the weekly researcher screen and for pre-registered trials. `llm_news_30` has 16 days of history
(29 Oct to 14 Nov, forward-only); `ev_comment_letter` has 660 rows over 397 tickers from 6 Nov 2023 to 11 Jun 2026 and **none since 11 Jun**. The earlier research statement that these two jobs "feed model features" is corrected: they feed nothing the live book reads.

- **C3 ablation: the literal bar gives 'the calls can stop' (removal costs 0.000 Sharpe on 8 of 8 calendars), but this is vacuous, not a measurement:** removing a column the model does not have returns the same model. Together the two jobs are 58% of 7-day hosted tokens (45% and 13%) and buy nothing for the live book today. What the test cannot show is whether the
  features would help a model; `llm_news_30` has far too little history (260 or more sessions are needed before a first score).
- **B1 price-target scorer: FAIL.** The regexes (fixed before the run) match 11.1% of scored headlines, not the 23% in the registration (that count included notices the regexes exclude, such as 'maintains' and 'reiterates'). Under the job's admission rule, 1,773 of 18,729 headlines that would be scored match: **9.5% against the 20% bar.** The registration's '59% of Raises scored 0'
  does not hold for this model and window: 3.6% do. The stored scores already agree with the regex sign on about 94% (Raises 94.0%, Lowers 92.4%, Upgrades 88.2%, Downgrades 97.9%; 30,455 headlines, 28 Oct to 14 Nov). The rule would change 4.0% of stock-day values (rank correlation between versions 0.993) and save about 9% of news calls, about 4% of all hosted tokens. **Not adopted.**
- **Corrected claim:** I told you about 78% of hosted tokens feed a model feature, the rules or the alert path. At least 58% (news scoring and comment letters) feed no input of the live model; they feed a research screen, trial features and case files.
- **Comment-letter job, new observation:** it reads SEC comment-letter replies every 30 minutes and calls a hosted model per reply (276k tokens in 7 days), but writes a feature row only when a letter is non-procedural and its quote is grounded; none has been written since 11 Jun. It either reads only procedural or ungrounded letters, or something upstream is broken. **Owner decision: stop or throttle it** (recommended: it has produced no stored output in five months).

### Quant registered trials, results (20 Nov 2026; `reports/Quant trial results - 19 Nov.md`, `scripts/trial_quant_*.py`, 29 trials counted)

T1 (re-price borrow, descriptive): S0 mean net Sharpe 0.403 at 0.5% a year, 0.325 at 2.0%, 0.153 at 5.3%; at 2% positive on 8 of 8 calendars (minimum +0.05). The registered reading (at least 0.30 at 2% and positive on all eight) is met: **B1 stands at a fee supported by quoted data.** **Amendment (20 Nov, written after this result, applies to future trials only):** short borrow in book trials is charged at 2.0% a year on short notional, with 0.5% and 5.3% reported beside it; the old flat 44 bps a month stays in force for comparison with trials already run. T10: probability of backtest overfitting 0.62 mean across calendars on the 18 stored cells (0.47 on all 28): the S5 shadow bar is unchanged. Minimum track record for B1: 9.4 years at 2%, 20.5 at 5.3%. No model or veto cell met its bar: T2 (train every 7: -0.09, every 3: -0.06), T6 (multi-horizon -0.16, vol-scaled -0.02), five-session score average -0.01, divergence veto -0.01, 8-K long-only veto 0.00; five-seed rank average +0.095 (7 of 8, q 0.21) and the flat-bar exclusion +0.26 (8 of 8, q 0.19) fail the Benjamini-Hochberg bar (the flat-bar gain is fit noise: 0.03% of name-days flagged, 0 of 998 live candidates and 186 picks with a flat bar). T7 drawdown rule (0.5 gross after a 15% drawdown): max drawdown -30.0% to -22.8%, worst month -15.0% to -9.5%, Sharpe change -0.054 against the -0.05 limit: **missed by 0.004, not adopted**. T8 staggered tranches: variance of Sharpe across 16 start offsets -84%, mean +0.02: meets its bar, but whole-share rounding on a $100k account makes it worse (W3): **not adopted at the current account size**. Deflated Sharpe at 29 trials: S0 0.06, best new cell 0.20. None of these changes sizing, caps, kill switch or execution; no change is made to any.



## Anomaly-score saturation: four registrations (21 Nov 2026, written before any outcome replay is run)

**Measured today** (stored events, 14 days to 21 Nov; `scripts/measure_event_score_distributions.py`, plus per-hour and per-source tables from the same stored rows). These replace the figures in the owner's note.

| Event type / source | n | distinct | mode (share) | at or above 0.8 |
|---|---|---|---|---|
| earnings_report | 247 | 8 | 0.450 (60%) | 0% |
| prediction_market_trade, all | 36,382 | 785 | 0.300 (45%) | 8.3% |
| prediction_market_trade, kalshi | 29,737 | n/a | 0.300 (55.4%) | 5.5% |
| prediction_market_trade, polymarket | 6,648 | n/a | none at 0.300 | 20.9% |
| filing, all | 7,272 | 40 | 0.200 (32%) | 24.2% |
| filing, sec_edgar | 5,053 | n/a | n/a | 34.0% |
| filing, sec_13d | 2,151 | n/a | n/a | 1.9% |
| equity_block, all | 5,380 | 638 | 0.500 (29%) | 21.5% |
| equity_block, alpaca_extended_hours | 571 | n/a | n/a | 55.5% |
| equity_block, finnhub_equities | 4,809 | n/a | 0.500 (30.9%) | 17.4% |

**Where each score comes from (traced in code).**
- *earnings_report* (`tradfi.py::_pre_announcement_score`): 0.20 + 0.25 x proximity + 0.20 x volatility. Proximity is a function of days to the report; the volatility term reads `sentinel:earnings:surprise_var:<ticker>`, which expires after four weeks and is empty for nearly every issuer. So the score is a function of days-out alone: 148 of 247 events are report-day (days-out 0) and carry exactly 0.45; days-out 7 carries 0.23.
- *prediction, 0.300*: not a floor. It is `KALSHI_QUIET_SCORE_CAP` (`prediction.py::kalshi_floor`): a Kalshi trade under $5,000 with under 3 points of probability change is capped at 0.3, and the information below the cap is discarded. 16,583 of 29,737 Kalshi events sit on it.
- *filing*, sec_edgar: the score is a constant per 8-K item (2.02 -> 0.82, 1.01 -> 0.84, 5.02 -> 0.86, 3.01 -> 0.92, 4.02 -> 0.96). Item 2.02 (the earnings release, which the earnings events already cover) alone is 860 of the roughly 1,720 sec_edgar events at 0.8 or more.
- *equity_block*: alpaca_extended_hours holds the saturation (55.5% at 0.8+); finnhub_equities has 31% of events on the 0.50 `uninformative_producer_cap`. The 13 Nov extended-hours proposal (size only, `extended_hours_score`) failed its outcome bar and is not reused.
- *market_anomaly time of day*: the "10:00-11:30 and after the close" clusters are two sources, not the US session. `coinbase_candles` (BTCUSDT and ETHUSDT, all candle timeframes, 24 hours a day) is 30,123 of the 40,663 events; its 0.8+ share by UTC hour runs from 0% to 6.4% on weekdays (hours 03, 13, 15, 19 at 4.3, 3.8, 6.4, 4.1%) and up to 12% on weekends. The 17:00 ET spike (20.5% at 0.8+) is `finra_short_volume`, a once-a-day batch (313 events, 82% at 0.8+) with its own 13 Nov registration; it is excluded here. `alpaca_quant_radar` (equities, 10,233 events) has 0.19% at 0.8+ and no bucket above 0.4%: no cluster to fix. Weekday 0.8+ share by ET bucket, all sources: before the open 0.66%, opening 0.90%, morning 3.40%, midday 0.49%, afternoon 1.78%, close 0.19%, after the close 2.98%.

**Outcome measure used by every stock test below (fixed now).** For an event on day d: |close 5 sessions after the last close on or before d / that close - 1|, divided by (sd of the 60 daily log returns before d x sqrt(5)). It is volatility-normalised on purpose: the raw 5-session move used on 13 Nov rewards any feature that merely tracks size or volatility. Source: `daily_bars` (point in time). The raw move is reported beside it, not judged. Statistic: Spearman of score with outcome, new minus old, with a 95% interval from 2,000 resamples of event DATES (a date is one unit). "Not worse" means the difference is at least 0 and the interval's lower end is at least -0.02. Rank correlation with the old score is reported, not a bar (13 Nov lesson: re-ordering is the purpose). Cuts: `ALERT_CUTS` (0.3 ... 0.9), as on 13 Nov; "newly above a cut" counts events that cross one upward.

### R1. earnings_report (pre-announcement): jumpiness replaces the empty volatility term
**Change.** `new = round(old - 0.20 x (1 - J), 4)`, J = clip((r - 2.5) / 4, 0, 1), r = (mean of the 4 largest absolute daily close-to-close returns in the 252 sessions before the report date) / (sd of those 252 returns). A name that jumps on events keeps its score; a name that never does falls by up to 0.20. Unknown r (fewer than 120 sessions): old score. Never above old, so no cut is crossed upward.
**Outcome:** |close 2 sessions after the last close before the report date / that close - 1| / (sd60 x sqrt(2)); one row per stored event whose report date is past; at least 500 rows with an outcome.
**Bars (all must hold):** (1) at least 30 distinct values over at least 1,000 events (90 days); (2) no single value holds more than 15% of events; (3) outcome Spearman, new not worse than old as defined above; (4) no event newly above a cut.

### R2. prediction_market_trade: spread the Kalshi quiet cap
**Change.** For a Kalshi event whose stored score equals the quiet cap (0.300) and whose notional is under $5,000: `new = round(0.30 + 0.09 x s, 3)`, s = clip(log10(max(notional, 1)) / log10(5000), 0, 1) x 4p(1-p), p the YES probability (0.5 if absent). The range is 0.30 to 0.39, which moves no event across any cut in `ALERT_CUTS` (the next is 0.4). Everything else, Polymarket included, keeps its score.
**Outcome:** |YES probability of the last stored event of the same market within the next 6 hours - YES probability now|, for events that have a later event; Kalshi only (Polymarket stores the traded outcome's price, not YES).
**Bars (all must hold):** (1) the share of Kalshi events on one value under 15% (55% now); (2) within the capped group, Spearman of the new score with the outcome at least +0.05, 95% date-cluster interval lower end above 0, at least 1,000 events with an outcome (the old score is constant in the group, so "old" is 0); (3) no count above any cut changes.

### R3. filings and equity blocks: liquidity-normalised
Both are divided by how much the name normally trades: ADV = mean of close x volume over the 20 sessions before the event day (`daily_bars`); unknown ADV keeps the old score.
**Filings (source sec_edgar only):** `new = round(old x (1 - 0.25 x s), 3)`, s = clip(log10(ADV / 1e7) / 3, 0, 1) (ADV of $10M or less: s = 0; $10bn or more: s = 1). Order within a size class is kept; large names fall furthest.
**Blocks (every equity_block with a notional):** `new = min(old, round(0.35 + 0.60 x clip(log10((notional / ADV) / 1e-4) / 2, 0, 1), 3))`: 0.35 at 0.01% of a day's volume, 0.95 at 1%.
**Bars, per family, each judged and wired alone:** (1) the share at 0.8 or more falls below 15% for sec_edgar filings (34% today) and below 10% for blocks (21.5%); (2) outcome Spearman, new not worse than old, at least 500 events with an outcome (filings) and 1,000 (blocks); (3) no event newly above a cut (by construction); (4) for filings, the top 1% by new score keeps at least the old top 1%'s mean normalised outcome (`top_share_mean`, ties at the cutoff take their group mean).

### R4. market_anomaly time of day: slot-quantile mapping (replaces the 12 Nov phase-3 design for these two sources)
**Change.** `new = Q_pooled(F_slot(old))`: the score's rank among the same slot's scores over the 14 days before the event day, read back through the source's pooled score distribution of the same 14 days. The marginal distribution is kept (the count above any cut is unchanged by construction); only which events carry the high ranks changes. Slots: `coinbase_candles`: (candle timeframe from `crypto_data.trade_type`, UTC hour, weekend flag), falling back to (timeframe, weekend) when the slot has fewer than 50 prior events, and to the old score when that has fewer than 50; `alpaca_quant_radar`: the seven ET buckets of 12 Nov, weekdays only. `finra_short_volume` is excluded.
**Outcome (coinbase only; no crypto bar table exists):** the pair's absolute price move between the event and the first stored coinbase event of the same pair 30 to 90 minutes later (prices are `crypto_data.price`), divided by the median of the same measure over events in the same slot in the prior 14 days (slot-neutral). Equities (0.19% at 0.8+) are judged on parity and volume only; their information bar is not judgeable here.
**Bars (all must hold):** (1) parity: for slots with at least 200 events in the test window, the share at 0.8 or more lies between 0.5 and 2 times the source mean for at least 80% of slots; (2) information kept: outcome Spearman, new not worse than old (interval as above), over at least 5,000 coinbase events; (3) the count at or above 0.8 changes by no more than 30% over the test window.
Test window: events from 21 Oct to 20 Nov (30 days), each scored from the 14 days before its own day (walk-forward, nothing from the future).

### Wiring rule (all four)
A change is wired only if every bar of its registration passes, and only behind `SCORE_CALIBRATION_V2` (default 0; new flag in `shared/utils/score_rescale.py`, separate from `SCORE_RESOLUTION_V2`). A miss is reported as a miss; no bar is relaxed afterwards. No sizing, cap, kill-switch or execution code is touched, and nothing is switched on live.

### Results of R1 to R4 (21 Nov 2026; `reports/Anomaly score calibration - 21 Nov.md`)

R1 FAIL (outcome interval lower end -0.0214 against -0.02). R2 FAIL (one value at 21.8% against 15%; ranking bar met at +0.0516). R3 filings FAIL (share at 0.8+ 18.7% against 15%; outcome and top-1% bars pass). **R3 blocks PASS all four bars** (35.1% to 2.7% at 0.8+; outcome +0.156 better, interval [+0.022, +0.275]); wired behind SCORE_CALIBRATION_V2, default off, not switched on. R4 FAIL (count at 0.8+ +252% against 30%; parity and outcome pass). No bar was changed after a run.

## P6 scenario probabilities (registered 21 Nov 2026)

Phase 1 grading (M1, M2, M3) and the Phase 2 decision rule are registered in `reports/P6 scenario probabilities design - 21 Nov.md` before any initial-state data exists. No run before 200 resolved scenarios carry `initial_state`.
