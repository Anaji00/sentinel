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
