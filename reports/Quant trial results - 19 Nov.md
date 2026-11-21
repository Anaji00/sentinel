# Quant trial results (19 Nov 2026)

Registered offline tests of section 4 of `reports/Quant methods and trading bot research - 18 Nov.md`, run exactly as registered; details the report left open were written into the docstrings of `scripts/trial_quant_common.py`, `trial_quant_harness.py`, `trial_quant_scores.py` and `trial_quant_run.py` BEFORE any run. Simulation only: no change to the live model, trader, sizing, caps, kill switch or execution, no order, nothing committed. `shared/utils/factor_model.py` is not edited (the five-seed model and the two label modes are installed at run time in the trial process only).
Raw results: `quant_trials/results.json`, `tables.md`, scores and inputs in the session notes folder.

**Run record.** Stage 1 (nine-cell walk-forwards, own container, 6 GB) ran from 10:01 UTC 19 Nov and exited about 21:00 UTC, before the nightly window (checked from the container exit time and per-cell run times). Stage 2 (offline simulations, 1 CPU, no model fits) was run at about 02:10 to 04:00 UTC 20 Nov, inside the 22:00 to 06:00 window; it is light, but it is a departure from the rule. Panel: 2,736 sessions x 3,494 names to 18 Nov; 2,140 scored sessions from row 575; eight calendars (0, 5, 10, 15, 2, 7, 12, 17), 96 to 101 blocks each; 10 bps a side; simple returns; borrow charged; bootstrap 10,000 draws seed 20261113.
The 13 Nov `book.json` was regenerated: S0 at the 5.3% fee gives mean Sharpe **0.153** (13 Nov: 0.19; the panel gained a week and bars were repaired). Stored-cell paired results reproduce the 13 Nov picture (S5 +0.097, 7 of 8, p 0.014; no other cell near).
**Trial count: 29** (18 stored + 10 new cells run + T8 as registered; `t2_1` not run because t2_7 showed no sign). BH is over the 17 stored cells plus every new cell with a p (27 p-values).
Not run: `t2_1` (rule: only if t2_7 shows any sign; it was worse on 0 of 8 calendars).

## Summary

- **T1 B1 passes at a data-supported fee**: mean net Sharpe 0.325 at 2.0% a year, positive on all eight calendars (minimum +0.05).
- **No model or book change met its bar.** Nearest: five-seed bagging (+0.095, 7 of 8, q 0.21) and the flat-bar cell (+0.26, 8 of 8, q 0.19), both failing BH. The flat result is almost certainly fit noise (below).
- **T7 drawdown rule** misses its bar by 0.004 Sharpe; **T8 staggering** meets its dispersion bar.
- **PBO** is mixed, above 0.5 on average across calendars for the 18 stored cells; S5 shadow bar stays unchanged.

## T1: re-score at flat short fees (descriptive; B1 passes if mean net Sharpe at 2% is at least 0.30 and positive on all eight calendars)

Result: **passes**. S0 mean net Sharpe 0.403 (0.5%), **0.325 (2.0%)**, 0.153 (5.3%). Per calendar at 2.0% (0, 5, 10, 15, 2, 7, 12, 17): +0.56, +0.38, +0.05, +0.20, +0.50, +0.27, +0.44, +0.20.
The weakest calendar is only +0.05: B1's pass rests on a mean from a spread of -0.09 to +0.37 at the old fee.

| Cell | 0.5% | 2.0% | 5.3% | Diff vs S0 at 0.5 / 2.0 / 5.3 | Calendars positive at 2.0% |
|---|---|---|---|---|---|
| S0 | 0.403 | 0.325 | 0.153 | - | 8 of 8 |
| S1 industry | 0.272 | 0.154 | -0.103 | -0.131 / -0.170 / -0.256 | 4 |
| S2 size | 0.181 | 0.087 | -0.119 | -0.222 / -0.238 / -0.273 | 6 |
| S3a decile | 0.417 | 0.339 | 0.169 | +0.013 / +0.014 / +0.016 | 8 |
| S3b quintile | 0.312 | 0.201 | -0.040 | -0.091 / -0.123 / -0.194 | 8 |
| S3c 2x count | 0.324 | 0.214 | -0.025 | -0.079 / -0.110 / -0.178 | 8 |
| S4 inverse-vol | 0.449 | 0.344 | 0.116 | +0.045 / +0.020 / -0.037 | 8 |
| S5 conviction | 0.497 | 0.419 | 0.250 | +0.094 / +0.095 / +0.097 | 8 |
| S6 biweekly | 0.492 | 0.403 | 0.206 | +0.089 / +0.078 / +0.053 | 8 |
| S6 weekly | 0.221 | 0.129 | -0.072 | -0.182 / -0.196 / -0.225 | 6 |
| S6 monthly + band | 0.416 | 0.339 | 0.166 | +0.013 / +0.014 / +0.013 | 8 |
| S6 biweekly + band | 0.504 | 0.414 | 0.218 | +0.101 / +0.089 / +0.064 | 8 |
| S6 weekly + band | 0.221 | 0.130 | -0.069 | -0.182 / -0.195 / -0.222 | 7 |
| S7 tranches | 0.463 | 0.374 | 0.180 | +0.059 / +0.049 / +0.027 | 8 |
| S8 hedge | 0.251 | 0.171 | -0.003 | -0.152 / -0.154 / -0.157 | 7 |
| S9 blend | 0.283 | 0.213 | 0.060 | -0.120 / -0.111 / -0.093 | 8 |
| S10 days to cover | 0.422 | 0.344 | 0.173 | +0.019 / +0.019 / +0.020 | 8 |
| S11 liquidity | 0.403 | 0.325 | 0.153 | 0 / 0 / 0 | 8 |
| S11 at $250m | 0.533 | 0.460 | 0.299 | +0.130 / +0.135 / +0.146 | 8 |

Reading: a lower fee lifts every cell by a similar amount, so **paired differences barely move** (S5 +0.094 to +0.097, S4 flips from -0.037 to +0.045 only at 0.5%). Fee-driven ranking changes are small: the "S11 holds less" gain (+0.146 at $250m) persists at every fee here, so it is not purely a fee artefact in this re-score (it is a smaller-position effect; judge separately). Caveat from the report stands: the history's short edge may sit in names the live book cannot borrow cheaply; this test cannot price that.

## T10: PBO (CSCV, 16 slices) and minimum track record

Matrix: block net returns at the 5.3% accounting, 96 blocks used per calendar, 12,870 splits.

| Set | PBO by calendar (0, 5, 10, 15, 2, 7, 12, 17) | Mean | Bar reading |
|---|---|---|---|
| 18 stored cells | 0.448, 0.884, 0.501, 0.699, 0.480, 0.696, 0.518, 0.739 | **0.621** | above 0.5 on average |
| all 28 cells (stored + new) | 0.373, 0.716, 0.339, 0.740, 0.514, 0.464, 0.322, 0.289 | 0.470 | near chance |

The slope of out-of-sample on in-sample performance for the in-sample best is negative on every calendar (about -0.8 to -1.0): the in-sample winner of the set does worse out of sample than average. Calendar 0 (the live calendar) alone gives 0.448 on 18 cells. **Reading as registered:** PBO above 0.5 on the stored set (mean) means choosing the in-sample best is no better than chance; **the S5 shadow bar is kept unchanged.** The estimate swings 0.29 to 0.88 across calendars, so it is a warning, not a measurement to the second decimal.

Minimum track record (95% one-sided against zero, skew -0.34, kurtosis 4.26, calendar 0):

| Fee | Annualised Sharpe (calendar 0) | Min years, fat tails | Min years, normal returns |
|---|---|---|---|
| 5.3% | 0.37 | 20.5 | 19.5 |
| 2.0% | 0.56 | 9.4 | 8.7 |
| 0.5% | 0.64 | 7.2 | 6.5 |

A 24-month paper record cannot confirm B1 at any fee tested.

## T2: training density (bar: +0.10, 6 of 8, IC t not more than 0.3 below base, BH q < 0.10)

| Cell | Wins of 8 | Mean paired diff | p | q | IC t (base 17.78) | Bar |
|---|---|---|---|---|---|---|
| train_every 7 | 0 | -0.091 | 0.774 | 1.00 | 15.5 | not met |
| train_every 3 | 3 | -0.056 | 0.683 | 1.00 | 16.3 | not met |

`train_every 1` not run (no sign at 7). Denser training did not help and lowered IC t; the first fold starts at the same session (575) in all cells. Mean Sharpe 0.062 and 0.097 against 0.153.

## T3: seed bagging and session averaging (bar: +0.05, 6 of 8, BH q < 0.10; the smaller bar was registered in the report)

| Cell | Wins of 8 | Mean paired diff | p | q | Bar |
|---|---|---|---|---|---|
| A: five seeds, rank average | 7 | **+0.095** | 0.023 | 0.206 | not met (q) |
| B: percentile over rebuild row and 4 earlier sessions | 3 | -0.008 | 0.558 | 1.00 | not met |

Cell A clears the gain and win-count conditions and fails only BH (q 0.21 against 0.10). IC rose (0.0410 against 0.0389, t 18.27 against 17.78). It is the same size as S5's near miss, so with 27 p-values in the family it is a direction, not a result. Cell B did nothing.

## T6: label variants (bar as T2)

| Cell | Wins of 8 | Mean paired diff | p | q | IC t | Bar |
|---|---|---|---|---|---|---|
| multi-horizon (10, 21, 42; embargo 42) | 0 | -0.160 | 0.825 | 1.00 | 16.05 | not met |
| volatility-scaled | 3 | -0.016 | 0.483 | 1.00 | 15.35 | not met |

The volatility-scaled label barely changed Sharpe (-0.016) while cutting drawdown (-28.5% against -30.0%) and the worst block (-7.8% against -15.0%): it suggests B1's edge does not depend on the label's pull toward volatility as much as feared, but it is not a better book.

## T7: drawdown rule (bar: worst block and max drawdown each at least 20% smaller, Sharpe no worse than -0.05)

| | S0 | With rule | Improvement |
|---|---|---|---|
| Mean max drawdown | -30.0% | -22.8% | 24.0% (bar met) |
| Mean worst block | -15.0% | -9.5% | 37.0% (bar met) |
| Mean Sharpe | 0.153 | 0.099 | -0.054 (**bar -0.05 missed by 0.004**) |

Calendars won 0 of 8, p 0.695. **Not met, narrowly**, on the Sharpe condition. The risk reduction is real in simulation; the Sharpe cost is just over the limit.

## T5: vetoes (bar: keep if +0.10 on 6 of 8; remove if S0 beats it by 0.10 on 6 of 8)

| Cell | Wins of 8 | Mean paired diff | p | q | Reading |
|---|---|---|---|---|---|
| S0 + divergence veto (both sides) | 4 | -0.007 | 0.561 | 1.00 | neither: no evidence to keep or remove |
| S0 + 8-K veto, longs only | 5 | +0.000 | 0.359 | 0.97 | neither |

Both vetoes are inert on Sharpe in this simulation (one-session execution delay, three-session lookback at rebuild, as fixed in the docstring). The divergence veto lowered max drawdown (-27.4% against -30.0%) and worst block slightly. Neither bar is met in either direction, so **the divergence veto stays untested-for-removal**; that is a fair reading, not a verdict that it is useful.

## T8: staggered tranches (bar: at least 30% lower variance across sixteen start offsets, mean no lower by more than 0.05)

Sixteen offsets: the eight registered plus 1, 4, 6, 9, 11, 13, 16, 19 (fixed before the run).
Variance of block Sharpe across offsets: S0 0.0226, S7 0.0036: **84% lower** (eight offsets: 91%). Mean Sharpe S0 0.190, S7 0.210 (+0.019). **Bar met.** S0 ranges from -0.09 to +0.38 across start offsets; S7 from +0.11 to +0.34. Per the report, S7 is not recommended at this account size (whole-share rounding on 20-name tranches).

## Flat-bar question and cell

Measurement, live today (`scripts/trial_quant_flat_live.py`): of 998 scored candidates, 89 B1 longs and 97 B1 shorts from the scores, and the stored book (89 long, 95 short), **zero** have a flat or zero-volume bar in the last 3 completed sessions. In the 11-year training panel, flagged name-days are **0.03%** of tradable name-days (0.04% over the three-session window, 8,811 8-K veto filings also loaded). The layer audit's 9.4% flat rows are in the full 12,000-name file, not in the traded top-1,000. Caveat: the 18 Nov bars had not yet been given their carry-forward pass when the audit was written, so today's zero-volume check is as of the completed sessions.

Registered cell (bar +0.05, 6 of 8, BH q < 0.10): **+0.260, 8 of 8, p 0.011, q 0.188, not met.** Mean Sharpe 0.413 against 0.153; IC 0.0396 against 0.0389 (t 17.99 against 17.78); drawdown -18.7% against -30.0%.
**Interpretation: not a finding.** Removing 0.03% of name-days cannot plausibly lift Sharpe by 0.26; it shows how much one LightGBM fit moves with a tiny change in training rows (the same noise the five-seed cell reduces). It also means every single-fit paired difference in this report carries roughly this much fit noise, which is why T2 and T6 negatives of -0.05 to -0.16 should not be over-read. Do not adopt on this.

## Deflated Sharpe (running trial count 29)

| Series | Deflated Sharpe |
|---|---|
| S0 (B1) | 0.057 |
| Best new cell (flat) | 0.202 |
| S5 (stored) | 0.095 |

Far below the 0.95 gate in the model code.

## What I adopt for recommendation

- **Adopt (measurement, no trading change):** treat B1's fee-supported mean net Sharpe as about 0.33 at 2% a year borrow (T1), not 0.15 to 0.19. Report both, with the minimum track record (9 to 20 years).
- **Adopt nothing in the live model:** T2, T3, T6 and the flat cell did not meet their bars. The five-seed average (cell A) is the one worth a re-test with a registered second draw of fits, since it is cheap and variance-reducing by construction.
- Keep the S5 shadow bar unchanged (PBO).

## Needs owner approval

- T7 drawdown rule (kill switch and sizing): bar narrowly missed on Sharpe (-0.054 against -0.05); drawdown and worst-month gains are large (24% and 37%). Owner's call among freeze, de-gross or review trigger; a registered second look at a 0.5x/0.75x variant would be a new trial.
- T8 staggered tranches: bar met in simulation; not before capital grows (rebalance structure and sizing).
- T5 vetoes: neither direction supported; no change proposed.
- `b1_variants.SHORT_FEE_BPS`: a registered amendment to the cost assumption (T1), not a quiet edit.
- Any use of five-seed averaging in the live model (model-switch margin rule applies).

## Not done and not verified

- The per-name quoted-fee version of T1 (needs borrow quotes covering a rebuild, from about 30 Nov).
- `t2_1` not run (rule).
- The 8-K veto uses the historical archive's acceptance times; the divergence z uses my fixed definition (5-session return minus industry median, 252-session standard deviation), not the live divergence watch's exact z.
- Stage 2 (light, 1 CPU) ran inside the nightly window (see run record).
- One run per cell, one seed: single-fit differences carry fit noise of roughly the flat-cell magnitude.

## Changelog

New files: this report; `scripts/trial_quant_common.py`, `trial_quant_harness.py`, `trial_quant_scores.py`, `trial_quant_run.py`, `trial_quant_flat_live.py`. No other repo file edited, nothing committed, nothing deployed, no orders.
Containers: `sentinel-quant-trials` (walk-forwards), `sentinel-quant-run` (simulations), one short read-only live check.
