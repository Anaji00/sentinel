# Model trials O1–O8 (22–23 October 2026)

How the cross-sectional stock model (`shared/utils/factor_model.py`) was tested against eight proposed
improvements, with the rules written down before any result, and what was adopted. In working notes
these were called M1–M8; they are recorded as **O** because trial M (news embeddings, 18 October) already
exists in the README.

## How every trial was run

- **Horizon judged:** monthly, the book the auto-trader trades. Weekly and daily were not run.
- **Same rules as the live engine:** purged walk-forward folds with a 21-day embargo, the 1,000 most liquid
  common stocks each day, the buffered quintile book, 10 bps a side, shorts only where borrowable. Every
  variant was scored on the next month's return net of SPY, whatever it was trained on.
- **One at a time,** in a separate container (8 GB, read-only, writing nothing to the platform), each trial
  building on what earlier ones adopted (`run_sequence.py`, `trials.py` in the session's research folder).
- **Baseline model:** the LightGBM ranker (`lgbm_rank`) trained on the return net of SPY.

## The rules, set in advance

| Trial | Change | Adopted if |
|---|---|---|
| O1 | 11 years of daily bars instead of 6 | IC t ≥ 2.0 and positive folds ≥ 55% on the longer test |
| O2 | Train on beta-adjusted, or industry-relative, returns | Net Sharpe ≥ baseline + 0.10, IC t ≥ baseline − 0.3, long-leg alpha after beta higher |
| O3 | Add fundamentals (book-to-market, ROA, asset growth, accruals, gross profitability, cash-flow and sales yield, value vs industry) and net share issuance | Same as O2 |
| O4 | Average the ranker's and gradient boosting's ranks | Same as O2 |
| O5 | Two-sided ranker: a second LambdaRank fitted on the reversed order to find losers | Same as O2, and the short leg no worse |
| O6 | Insider measures (net purchase ratio, distinct buyers, opportunistic sales) and adverse 8-K events | Same as O2 |
| O7 | Option-chain features from Cboe (open interest change, put/call, IV, skew) | Deferred: the history began 22 October |
| O8 | Commodity exposure: the signed 1- and 3-month move of what a company makes or buys | Same as O2, and at least 30 names a date carry it |

One amendment, made before any O1 result: at the user's direction memory was dropped as a constraint
(O1 originally also required a peak under 2.0 GB), and the model service's limit was raised to 6 GB.

## Gate policy, recorded 4 Nov 2026

The deflated-Sharpe gate stays as coded: variance across every candidate at the horizon, trials counted as candidates x horizons (12), bar 0.95. It is not changed to get a pass. Under other readings the monthly model scored 0.96 (sampling noise only, 12 trials), 0.89 (41 trials) and 0.35 (live variance, 41 trials), so any later change is written here first and applied whichever way it cuts.

While the account is paper, B1 keeps rebalancing on unvalidated scores as the forward test. The gate must pass before any real capital. The trader keeps the old book when fewer than 800 names are scored.

## Results (monthly, out of sample)

| Trial | Months tested | Rank IC | IC t | Positive folds | Net per month | Net Sharpe | Buys vs market after beta | Shorts | Net beta | Verdict |
|---|---|---|---|---|---|---|---|---|---|---|
| Baseline, 6 years | 44 | 0.040 | 2.00 | 67% | +1.01% | 0.79 | −1.23% | +1.67% | 0.44 | — |
| **O1: 11 years** | **101** | **0.037** | **3.12** | **74%** | **+1.09%** | **1.01** | **−0.70%** | **+1.59%** | **0.10** | **Adopted** |
| O2 beta-adjusted target | 101 | 0.023 | 1.60 | 65% | +0.84% | 0.59 | −0.46% | +1.40% | −0.57 | Rejected |
| O2 industry-relative target | 101 | 0.029 | 2.82 | 71% | +1.11% | 0.99 | −0.44% | +1.61% | −0.35 | Rejected |
| O3 fundamentals + issuance | 101 | 0.031 | 2.52 | 73% | +0.83% | 0.74 | −0.81% | +1.43% | 0.13 | Rejected |
| O4 two-model average | 101 | 0.038 | 2.95 | 79% | +1.21% | 0.96 | −0.64% | +1.64% | 0.01 | Rejected |
| O5 two-sided ranker | 101 | 0.039 | 2.98 | 71% | +1.16% | 0.90 | −0.54% | +1.58% | −0.06 | Rejected |
| O6 insider + 8-K | 101 | 0.031 | 2.37 | 65% | +0.95% | 0.82 | −0.76% | +1.50% | 0.06 | Rejected |
| O8 commodity exposure | 101 | 0.038 | 3.12 | 73% | +0.97% | 0.92 | −0.77% | +1.54% | 0.09 | Rejected |

Gradient boosting on 11 years: IC 0.036, t 2.67, 71% positive folds, net Sharpe 0.80.

## What it means

- **More history is the one clear gain.** The same model, tested on 2.3 times as many months including the
  2018, 2020 and 2022 drawdowns, goes from borderline (t 2.00) to clearly significant (t 3.12) and carries
  almost no market exposure. The live engine reads 11 years from 23 October 2026 (`FACTOR_LOOKBACK_YEARS`).
- **The book's edge is still its shorts.** Every variant earned about +1.4% to +1.6% a month on the short
  leg and lost against the market on the long leg after beta.
- **Three near misses** (industry-relative target, the two-model average, the two-sided ranker) earned more
  a month and roughly halved the long leg's shortfall, but swung more, so their Sharpe fell short of the
  bar. On one 101-month history the gaps are within noise. They stay in code as candidates
  (`training_target(..., "industry")`, models `avg2` and `lgbm_rank2`) and can be retested as live
  history accrues.
- **Insider and 8-K data hurt the ranking** although several of those events pass the event study on their
  own (opportunistic insider selling, dilution, delisting notices). They stay as event evidence on the
  Signal Board and in trade ideas, not as model features.
- **Commodity exposure is neutral** at this breadth: about 51 of 1,000 names carry it on a given date, too
  few to move a 1,000-name ranking. Memory-chip and GPU prices have no history yet; the exposure is retested
  when they do, and is better suited to the event and idea layers than to the ranking.

## Where things live

| What | Where |
|---|---|
| Panel history setting | `services/agents/factor_engine.py` `LOOKBACK_YEARS` |
| Training targets | `shared/utils/factor_model.py` `training_target` |
| Candidate models | `shared/utils/factor_model.py` `_RankAverage`, `_TwoSidedRanker` |
| Share issuance | `shared/utils/factor_model.py` `features` (board context) |
| Shared per-stock features | `stock_features_daily`, `shared/utils/stock_features.py`, `services/agents/feature_jobs.py` |
| Commodity, memory and GPU prices | `commodity_prices`, `services/collector-tradfi/commodities.py`, `shared/utils/commodity_links.py` |

# Paper-account book trials B0–B6 (24 October 2026)

The same out-of-sample monthly forecasts (LightGBM ranker, 11 years), seven ways to build the account. Monthly
rebalance, 10 bps a side, shorts only where borrowable; expected returns calibrated only from earlier months.
Adopted if the best net Sharpe beats B0 by 0.10, its worst month is no more than 2 points worse, and its net beta is
within ±0.20. The account changes only after the user reviews the result.

| Book | Construction | Names | Net a month | Net Sharpe | Worst month | Net beta | Verdict |
|---|---|---|---|---|---|---|---|
| B0 | Top and bottom fifth, equal weight (current) | 362 | +0.44% | 0.71 | −9.73% | −0.05 | Baseline |
| B1 | Bottom 10% short, 90th–99th percentile long, equal weight | 168 | +0.64% | 0.82 | −9.84% | −0.07 | Passes |
| B2 | Calibrated expected return ÷ variance, 3% cap | 931 | +0.14% | 0.31 | −6.22% | −0.05 | Fails |
| B3 | B2, beta-neutral legs | 931 | 0.00% | −0.01 | −11.96% | 0.03 | Fails |
| B4 | B3 on industry-relative ranks | 939 | −0.02% | −0.06 | −9.90% | −0.02 | Fails |
| B5 | B3 scaled by the market-stress multiplier | 931 | +0.05% | 0.14 | −7.17% | 0.02 | Fails |
| B6 | B3 with a 15% stop on each short | 931 | −0.04% | −0.09 | −12.00% | 0.03 | Fails |

**Run-to-run variance.** The ranker's own walk-forward in this run, on one more day of data than O1, scored IC t
2.32 and net Sharpe 0.61 (O1: 3.12, 1.01). Moving the monthly test calendar by a day moves results by that much, so
differences of about 0.1 between variants in O1–O8 and here are within noise. Future trials should average several
offsets of the test calendar.

# Feature study (24 October 2026)

Every input on the 11-year panel, monthly, out of sample: univariate rank IC and its t on non-overlapping months,
the top-minus-bottom quintile spread, the ranker's split gain, and redundancy.

- Strongest alone: off-exchange short-volume share (t −2.82), return skew (−2.43), price–volume correlation
  (−2.28), quality versus industry (+2.23), gross profitability (+2.15, not in the model), put/call volume (−2.11,
  ~124 names), illiquidity (−2.07).
- The ranker relies most on market-condition interactions — beta × credit (8.0% of gain), beta × trend (5.8%),
  rate sensitivity × rates (5.3%), momentum × volatility (5.3%) — about a quarter of its splits, though each predicts
  little alone.
- Redundant pairs: volatility and idiosyncratic volatility (0.96), momentum and residual momentum (0.91), dollar
  volume and illiquidity (−0.87).
- 12-1 momentum alone: t 0.38 over 2016–2026. Candidates: same-month seasonality t −0.02, gradual momentum (frog in
  the pan) +1.38, change in momentum −1.63.
- Next trials suggested: retest the regime interactions on 11 years; drop one of each redundant pair; add gross
  profitability alone.

# Trials P1–P3 (24 October 2026)

Monthly ranker, 11 years, each configuration on four offsets of the test calendar (0, 5, 10 and 15 sessions
dropped from the panel's start) and judged on the mean, because one run swings by about 0.4 in net Sharpe from
timing alone. Rules set in advance (P1: +0.10 Sharpe, or within 0.05 with net beta nearer zero; P2: within 0.05;
P3: +0.10; all with IC t no more than 0.3 lower).

| Configuration | Inputs | Mean net Sharpe | Per offset | Mean IC t | Net a month | Buys vs market after beta | Verdict |
|---|---|---|---|---|---|---|---|
| Baseline | 36 | 0.68 | 0.63, 0.49, 0.89, 0.69 | 2.40 | +0.86% | −0.76% | — |
| P1: drop the four regime interactions | 32 | 0.96 | 1.22, 0.83, 1.02, 0.78 | 3.27 | +1.04% | −0.57% | Adopted |
| P2: drop ivol_63, resmom_12_1, amihud_21 | 33 | 0.67 | 0.63, 0.44, 0.95, 0.65 | 2.48 | +0.86% | −0.78% | Adopted |
| P3: add gross profitability | 37 | 0.69 | 0.61, 0.60, 0.94, 0.63 | 2.54 | +0.89% | −0.73% | Rejected |
| P1 + P2 combined | 29 | 0.97 | 1.24, 0.86, 1.08, 0.70 | 3.34 | +1.03% | −0.57% | **Live 24 Oct** |

The regime interactions (trial E, adopted on five years) held about a quarter of the ranker's splits but predicted
little on their own; on eleven years they added noise, and removing them beat the baseline on every offset. Only the
monthly horizon was trialled; the weekly and daily horizons are measured on the new inputs by the nightly run and
are used only where they clear the gates.

The paper account switched to book B1 the same day.

# Book B1 stress test and retirement rules (5 November 2026)

The live `gbm` monthly model, four test calendars (0, 5, 10, 15 sessions), about 100 out-of-sample months each from April
2018, no borrow filter, figures averaged across calendars. The spread is long minus short at 100/100; the 50/50 account
earns half. Scripts stayed outside the repo; nothing live changed. These add 9 book variants to the trial count.

**Log returns inflate this record.** `forward_excess` is a log return net of the market's, and books average it. A
position earns the simple return, and for volatile shorts the two differ by about half the variance. At 10 bps a side:

| B1, estimated fees charged | Short vs universe | Net a month | Net Sharpe | Per calendar |
|---|---|---|---|---|
| Log returns, as trials have scored them | +2.41% | +2.37% | 1.23 | 1.55, 1.07, 1.39, 0.92 |
| Simple returns | +1.12% | +0.92% | 0.49 | 0.71, 0.36, 0.57, 0.31 |

Only the second row was tested against the rules below, which were written before any result. `expm1` of a log excess
is itself an approximation. Trial figures earlier in this file used log returns unless stated, and comparisons between
them are also affected. The gate has not been changed.

| Registered test (simple returns) | Result | Verdict |
|---|---|---|
| S1: short leg after fees above the universe on the mean and on 3 of 4 calendars | +0.68% a month, 4 of 4, t 1.1 to 1.6 | Passes, weakly |
| S2: net Sharpe above 0.5 and positive on all calendars | 0.49, all positive | Fails narrowly |
| B1 rank buffer: beats none on 3 of 4 calendars by 0.10 on the mean | Best +0.01 at 10 bps, +0.08 at 20 bps; turnover 61% to 32% | Fails |
| D1: buffer width chosen from earlier months only | Sharpe 0.46 against 0.49 | Fails |

Fees are fitted on today's borrow file (R² 0.16) and assumed stable, so they are rough. Shorting only names at or under
5% on today's file (look-ahead, and it also drops every name not in the file) cuts the short leg to +0.50% before fees
and Sharpe to 0.19 with the same fee estimate charged, about 0.4 without it. Removing the costliest tenth or fifth of
estimated fees gives 0.42 and 0.31. Much of the short profit sits in names that are costly or unavailable to borrow.
Returns are concentrated late: early half about +0.3% a month, later half about +1.5%.

## Retirement rules, written before the forward record

Monthly B1 net return is standardised by its trailing 24-month volatility, which updates as evidence arrives; the
rule's constants do not. CUSUM: `S = max(0, S + 0.036 - z)`.

- **S reaches 6: formal review, not automatic retirement.** Simulated with fat tails, the median wait is 22 months at a
  true Sharpe of -0.5, 35 at 0, 49 at 0.25 and 72 at 0.5. Within five years it fires 95%, 75%, 59% and 43% of the time.
  It cannot separate a weak model from a dead one quickly, so it opens a review and decides nothing alone.
- **Retire at a review** if the realised net Sharpe since 30 Nov 2026 is below 0 after at least 24 months, on measured
  fills plus borrow fees and with no data or execution fault explaining the shortfall.
- **Halt fast on faults only:** bad data, thin scores, order failures and the kill-switch limits. Poor returns alone
  are not a fast halt, because a healthy model has negative 12-month stretches.
- **Real capital needs all of:** the gate passed on simple returns, a short leg that survives measured borrow, and 24
  months of forward record.
- Changing these needs a dated entry here, written before the data that prompts it is examined.

## B1 leg trial (5 November 2026)

Same forecasts and calendars, simple returns, 10 bps a side, a flat 0.44% a month borrow estimate on the short leg, so
any fee saving from dropping costly names is not credited. Rules registered first: net Sharpe +0.10 on the mean, better on
3 of 4 calendars, worst month no worse by over 2 points. Adds 4 trials. None passes; B1 is unchanged.

| Variant | Net a month | Net Sharpe | Change | Better on | Worst month | Worst 5% of shorts |
|---|---|---|---|---|---|---|
| B1 | +0.87% | 0.47 | | | −25.9% | −48.2% |
| Include the top 1% in longs | +0.92% | 0.49 | +0.02 | 3 of 4 | −25.8% | −48.2% |
| Drop the most crowded fifth of shorts (days to cover) | +0.95% | 0.52 | +0.05 | 4 of 4 | −24.4% | −46.8% |
| Drop the most volatile fifth of shorts | −0.04% | −0.01 | −0.48 | 0 of 4 | −15.0% | −22.5% |
| Size legs by inverse volatility | +0.67% | 0.46 | −0.01 | 2 of 4 | −21.7% | −48.2% |

Almost all the short leg's return is in its most volatile names: without them it falls from +1.12% to +0.22% a month.

## Books scored on simple returns (5 November 2026, approved by the user)

From this date `walk_forward` scores every book on simple returns (`expm1` of the log excess), so `ls_gross_bps`,
`ls_net_bps`, `net_sharpe`, the deflated Sharpe and the leg exposures all read lower for volatile shorts. The old figure
stays in `exposures.net_sharpe_on_log_returns`. The gate's thresholds did not change. Training targets, IC, and the
returns handed to the sizing lab (`collect`) stay on log returns. Figures in this file dated before this entry used log
returns, and are not comparable with later runs. The nightly choice among models now uses the corrected number, so it
can change which model scores the book.

## Tests registered before running (5 November 2026, 23:00 UTC)

Live `gbm`, simple returns, 10 bps a side, flat 0.44% a month short fee, B1 construction unless stated. Logged whichever way they fall.

- **L1 Look-ahead:** features for past rows must be identical when later data is removed. Any material difference voids results until explained.
- **H1 Fresh calendars 2, 7, 12, 17:** never used for a decision. B1 "holds" if mean net Sharpe is at least 0.30 and positive on all four.
- **F1 Attribution:** monthly B1 and each leg regressed on market, size, momentum, volatility and reversal factors built from the same universe. Descriptive: reports loadings and residual alpha, with no pass or fail, except that B1 "has its own edge" only if residual alpha has t of at least 2.
- **R1 B0 against B1 on simple returns:** B1's adoption stands if it is not worse than B0 by over 0.10 on the mean of eight calendars. B2 to B6 need the portfolio lab rerun and are not covered.
- **P1 Borrow proxy:** drop shorts with fails of at least 10,000 shares on 5 consecutive settlement days, public by then. It passes if B1's Sharpe falls by under 0.15; a larger fall means the edge needs failing names. Real threshold lists were not used.
- **T1 Simple-return training label:** retrain on `expm1` excess, four calendars. Passes on +0.10 mean Sharpe, better on 3 of 4, worst month no worse by over 2 points.

These add 5 trials.

### Results of the 5 November registered tests

| Test | Result | Verdict |
|---|---|---|
| L1 look-ahead | 46 features identical at two past cut-offs (last 6 rows each) | Passes |
| H1 fresh calendars 2, 7, 12, 17 | Sharpe 0.32 (0.34, 0.49, 0.19, 0.28), worst month −35.2%; used calendars read 0.47 | Holds, narrowly |
| F1 attribution, mean of 8 calendars | B1 alpha +0.66% a month, t 1.36, R² 0.57; volatility −0.71, momentum +0.46, reversal −0.26, market +0.10 | No own edge shown |
| R1 B0 against B1, 8 calendars | B0 0.21, B1 0.40, but worst month −18.7% against −35.2% | B1 stands |
| P1 persistent-fails drop | Sharpe −0.04; short leg after fee +0.55% to +0.32%; a median 199 names flagged | Passes, but the filter is broad |
| T1 simple-return label | Sharpe unchanged at 0.47, better on 3 of 4 | Fails |

Best current estimate of B1: Sharpe about 0.40 on eight calendars, before real borrow limits and any live decay. Its return is
largely short high volatility plus momentum, and alpha beyond those factors is not statistically distinct from zero. Factors were
built from the same universe at top and bottom 30%, and are not investable as they stand. Costs and fees were charged to B1 but not to the factors.

### First nightly run on simple-return scoring (6 November 2026, run stored 06:00 UTC, as of 5 Nov)

The run took about 8 hours against about 4.5 the night before, with research jobs competing for the CPU for part of it. 1,000 monthly
scores published. IC is unchanged to the third decimal (gbm 0.0457 to 0.0459), as it should be since ranks and training were not touched.

| Monthly model | Net Sharpe before | Net Sharpe now | On log returns now | Deflated now |
|---|---|---|---|---|
| gbm | 1.18 | 0.60 | 1.17 | 0.36 |
| lgbm_rank | 1.07 | 0.80 | 1.09 | 0.58 |
| ridge | 0.28 | -0.12 | 0.28 | 0.01 |
| momentum | -0.03 | 0.06 | -0.03 | 0.03 |

The log figure reproduces the old score, so the drop is the scoring and not the data. Nothing is validated (gate 0.95). The chosen monthly model moved
from gbm to lgbm_rank because the correction cut gbm by more than lgbm_rank: 0.20 apart, inside the run-to-run noise recorded above. The top and bottom
tenths of the new scores share 58 of 100 longs and 42 of about 98 shorts with the day before. The stored B1 book is unaffected until its rebuild (about 30 Nov).

## Proposed, not applied: a margin before the chosen model changes (6 November 2026)

`run_panel` picks each horizon's model as the highest net Sharpe among those that pass, or the highest overall if none pass. The scores that rebuild B1 come
from that pick, so a flip between near-equal models changes the book. Over the 26 stored runs with a monthly choice (since 9 Oct 2026) it flipped on 8
nights (31%). Gap by which the new winner beat the incumbent that night: 0.03, 0.56, 0.01, 0.13, 0.07, 0.02, 0.15, 0.20. Five of eight were under 0.10.
Test calendars alone move a Sharpe by about 0.4, so none of these gaps separates two models.

Replay of the stored runs with a rule "keep the incumbent unless the best beats it by at least M":

| Margin M | Flips | Against 8 |
|---|---:|---|
| 0.05, 0.10, 0.15 | 4 | halved, and the same at all three |
| 0.25 | 3 | |

**Proposal:** M = 0.10 (the point where the count stops falling), per horizon, using the incumbent from the previous stored run, kept while it is still among the
candidates. Fallbacks unchanged: a newly validated model still wins, and an incumbent that errors is replaced.

**Registered test, before any change:** it passes if the replay above, run on the stored history, keeps at least 40% fewer flips and never holds an incumbent more
than 0.10 below the night's best. It already does on these 26 runs; the test is repeated on the first 30 nights after the change. It adds no model, so it adds no trial.

**Not solved by it:** gbm and lgbm_rank are statistically indistinguishable here. If the choice should not depend on the night, averaging their ranks is
the cleaner design and would need its own pre-registered trial. Applying the margin is a change to which model scores the live book, so it waits for approval.

### Applied: the model-switch margin (6 November 2026, approved by the user)

`shared/utils/model_choice.py` holds the rule (margin 0.10, with a 1e-9 tolerance so a challenger winning by exactly the margin counts). `run_panel` takes
`incumbents`, read by the factor engine from the last stored run; each horizon's report now carries `held_over` (the challenger that was held back) and
`switch_margin`. With no incumbent, an unreadable one, or one that no longer qualifies, the choice is the plain maximum as before. 8 unit tests; the
factor-model, portfolio-lab and recommendation suites pass. It loads when `sentinel-factor-model` restarts, so the first run under it is the next one after
that restart, with lgbm_rank as the incumbent. The follow-up test above runs on the first 30 nights after.

The margin loaded on 6 Nov 2026 at 15:07 UTC, when `sentinel-factor-model` was restarted. The first run under it is the 22:00 UTC run that night, with lgbm_rank as the incumbent.

## Portfolio lab scored on simple returns (6 November 2026)

The lab behind B2 to B6 (`shared/utils/portfolio_lab.py`: industry-neutral, beta-neutral and short-hedged books, return-theme neutrality, rank
weighting, momentum-volatility scaling, sentiment tilt, meta-sizing, the equity-regime gauge) still averaged log excess returns, so it carried the
inflation that took B1 from 1.18 to 0.60. `run` now converts the collected returns with `expm1` before any book or the sizing layer reads them; the hedged
books add SPY back as a simple return, `(1 + relative) * (1 + SPY) - 1`. Forecast calibration and the regime IC keep the log values (ranks are unchanged).
Tests: `tests/test_lab_simple_returns.py`; the lab, factor, portfolio, recommendation and meta suites pass (212).

Last report on log returns (gbm, 102 months, saved 6 Nov before the change): live 1.57, industry-neutral 1.57, beta-neutral 1.65, short-hedged 1.80, rank-weighted
1.55, return-theme neutral 1.77, scaled 1.66, sentiment tilt 1.59, meta-sizing 1.28 to 0.85. Expect these to fall by roughly half, as B1's did.

**Live effect to watch:** `adoptions()` turns a change on when it beats its baseline by 0.10 Sharpe. Return-theme ranking is on today (1.77 against 1.57 on logs).
The rule is unchanged; the first simple-return lab run decides whether it stays on. I backdated the stored run stamp so the lab runs after tonight's model run instead
of 12 Nov. Meta-sizing history (`meta_history`) holds three older log-based entries; the new run is the first simple-return one, so do not read it as a fifth agreeing run.

## Registered before running: RB1, a plain rule against B1 (6 November 2026)

Score = z(12-1 momentum) minus z(126-day volatility), equal weights, no model. B1 construction (long 90th to 99th percentile, short bottom 10%, borrowable only,
21-session rebuild), the same 8 calendars, simple returns, 10 bps a side, 0.44% a month short fee. **Reading:** if RB1's mean Sharpe is within 0.15 of B1's (about 0.40),
the model has not been shown to add value over a rule. If B1 leads by 0.15 or more on the mean and on 5 of 8 calendars, the model earns its place. Descriptive, with no
change to the trader either way. Adds 1 trial.

**Amendment RB1a, written before the run (11 Nov 2026).** The panel computes 63-day volatility (`vol_63`), not 126-day, so RB1 uses `vol_63`; the two are highly
correlated and nothing else changes. It runs through the same walk-forward book code as B1 (`factor_model.walk_forward`, model name `rb1_rule`, names with a missing
feature kept as in B1's tree models and scored 0, so never picked), on the eight monthly calendars 0, 5, 10, 15 and 2, 7, 12, 17 sessions. The walk-forward's own net Sharpe
has no borrow fee, so the 0.44% a month short fee is subtracted afterwards: the spread's per-period volatility is recovered as net mean / per-period Sharpe, and
Sharpe = (net mean - 44 bps) / that volatility x sqrt(12). B1 is not re-run (eight lgbm_rank walk-forwards do not fit beside the stack in memory): it is compared as published
above, mean 0.40 over the eight (0.47 on 0, 5, 10, 15; 0.34, 0.49, 0.19, 0.28 on 2, 7, 12, 17). Because the per-calendar B1 values for 0, 5, 10, 15 were not kept, the
"B1 leads on 5 of 8 calendars" half of the reading is judged on the four fresh calendars only (leads on at least 3 of 4) and said so.

### RB1 result (11 Nov 2026; `scripts/run_rb1.py`, own 9 GiB container, about 100 out-of-sample months a calendar)

| Calendar (sessions) | 0 | 5 | 10 | 15 | 2 | 7 | 12 | 17 | Mean |
|---|---|---|---|---|---|---|---|---|---|
| RB1 rank IC | 0.017 | 0.030 | 0.034 | 0.033 | 0.023 | 0.039 | 0.033 | 0.028 | 0.030 |
| RB1 net Sharpe, no borrow fee | -0.15 | 0.02 | -0.02 | 0.02 | -0.06 | -0.04 | -0.02 | -0.05 | -0.04 |
| **RB1 net Sharpe after the 0.44% fee** | -0.39 | -0.23 | -0.26 | -0.20 | -0.33 | -0.28 | -0.24 | -0.28 | **-0.28** |
| B1, as published | | | | 0.47 on 0, 5, 10, 15 | 0.34 | 0.49 | 0.19 | 0.28 | 0.40 |

**Reading: B1 leads by 0.68 on the mean (the bar was 0.15) and by at least 0.15 on all four fresh calendars. The model earns its place over this rule.** The rule's ranks do carry
information (positive rank IC on every calendar, 0.017 to 0.039, in the range the model's own IC is judged against), but the long-short spread they give is roughly zero before costs, so it cannot pay
the borrow fee. Limits: B1 is the published figure, not re-run on this code path; the fee is subtracted after the fact from the report's mean and per-period Sharpe; 63-day volatility stands in for 126-day (RB1a).
Descriptive: no change to the trader. Adds 1 trial.

## More history: what exists (6 November 2026)

`daily_bars` runs from 4 Jan 2016, which is as far back as the free Alpaca plan serves, and the engine already reads 11 years (O1's gain). 13,108 tickers, of which
1,216 stopped trading before Oct 2026 (751 before 2024), so delisted names are in the training set. Going before 2016 would need a second free source with its own
survivorship and adjustment problems; not started. Short-history features are the better backfill target: fails-to-deliver and N-PORT are already loaded to 2016, option
bars reach Feb 2024 (about 33 months, enough only for the weekly and daily horizons), borrow data starts 9 Oct 2026 and cannot be backfilled.


### Run of 6 to 7 November 2026 (stored 05:51 UTC, as of 6 Nov): first under the switch margin and the simple-return lab

Took 7 hours 51 minutes (22:00 to 05:51 UTC), against about 8 hours the night before. `lgbm_rank` stayed the monthly choice; the stored report carries `switch_margin` 0.1 and no held-over challenger, because `gbm` did not lead it.

| Monthly model | IC | IC t | Net Sharpe (simple) | Deflated |
|---|---|---|---|---|
| lgbm_rank | 0.041 | 4.01 | 0.86 | 0.59 |
| gbm | 0.045 | 4.27 | 0.64 | 0.35 |
| ridge | 0.018 | 1.41 | -0.12 | 0.00 |
| momentum | 0.003 | 0.17 | 0.06 | 0.02 |

Nothing passes the 0.95 gate. The night before: lgbm_rank 0.80 / 0.58, gbm 0.60 / 0.36.

**Portfolio lab, first report on simple returns (lgbm_rank, 102 months).** The previous report was on log returns and on `gbm`, so the comparison mixes two changes.

| Construction | Net Sharpe, log and gbm (5 Nov) | Net Sharpe, simple and lgbm_rank (7 Nov) |
|---|---|---|
| Live quintiles | 1.57 | 0.83 |
| Industry-neutral | 1.57 | 1.03 |
| Beta-neutral | 1.65 | 0.79 |
| Short-hedged | 1.80 | 0.68 |
| Rank-weighted | 1.55 | 0.95 |
| Return-theme neutral | 1.77 | 1.32 |
| Adjusted base / momentum-scaled / sentiment tilt | 1.67 / 1.66 / 1.59 | 1.18 / 1.19 / 1.15 (worst month -903 bps) |
| Meta-sizing, equal weight to sized | 1.28 to 0.85 | 0.92 to 0.39 |
| Equity-regime gauge, unscaled to scaled | 1.41 to 1.48 | 0.66 to 0.63 |

Readings. The log inflation was roughly 0.5 to 1.1 Sharpe, largest for the hedged books. The verdict flipped: industry-neutral now beats the live quintiles (1.03 against 0.83). Return-theme neutrality still beats industry-neutral by more than the 0.10 adoption bar (1.32 against 1.03), so it stays on. Momentum-volatility scaling gains 0.01 and the sentiment tilt loses, so neither is adopted. Meta-sizing still does not raise the Sharpe, and the regime gauge no longer helps. The B2 to B6 conclusions that survive on simple returns: sizing and the gauge add nothing; neutralising industries or return themes helps.
