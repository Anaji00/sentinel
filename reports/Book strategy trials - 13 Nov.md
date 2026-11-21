# Book strategy trials (13 Nov 2026)

Registered before the run in `reports/Upgrade pre-registrations - 12 Nov.md` ("Book strategy trial", plus amendment BST-1 on S4's weight limit, also written before the run).
Run 13 Nov in its own container (`sentinel-book-trials`, 6 GiB), `scripts/run_book_strategies.py`; raw results are in the session notes folder as `book.json`.
Simulation only. Nothing in the trader, sizing, caps, kill switch or execution changed, and no order was sent.

## Verdict

**No cell is a candidate. B1 stays as it is.** Of 17 cells, none meets the bar and none survives Benjamini-Hochberg.
The nearest, S5 (conviction weights), wins 7 of 8 calendars with a bootstrap p of 0.013, but its mean gain is +0.0995 against the 0.10 required and its BH q is 0.22
(needs under 0.10). Its deflated Sharpe, counting 18 trials, is 0.16 (the gate in the model code asks for 0.95). It is a direction worth noting, not a result.

## Design as run

- Scores: one purged walk-forward per model (`lgbm_rank`, mean rank IC 0.040; `gbm`, 0.043), every session scored from row 575 to 11 Nov, 2,136 scored rows, 3,487 panel names.
  The panel's last date is 12 Nov 2026.
- Eight calendars (offsets 0, 5, 10, 15, 2, 7, 12, 17), about 100 non-overlapping 21-session blocks each.
- Costs on every cell including S0: 10 bps per side on all traded notional and 44 bps a month on short notional. This is a stricter and more complete charge than the 12 Nov variants
  trial. S0's mean net Sharpe here is **0.19** (calendars 0.39, 0.00, -0.12, 0.07, 0.46, 0.02, 0.16, 0.53), against 0.40 published for B1 in the walk-forward. The gap is the
  simulator's own: trades are charged on daily-drifted weights, the borrow fee accrues on short notional, and the window runs to 12 Nov. It is the same for every cell, so the
  paired differences are unaffected. It also shows how thin B1's net edge is on this accounting.
- Paired statistic: per calendar, variant Sharpe minus S0 Sharpe, averaged over the eight; bootstrap over 21-session calendar bins (10,000 draws, seed 20261113).

## Results (17 cells against S0)

| Cell | Calendars won of 8 | Mean paired Sharpe diff | at 5 bps | p | BH q | Mean Sharpe | Net / 21 sessions | Turnover / 21 sessions | Max DD | Longs / shorts | Candidate |
|---|---|---|---|---|---|---|---|---|---|---|---|
| S0 (B1) | - | - | - | - | - | 0.19 | +0.13% | 1.40x | -29.7% | 87 / 73 | control |
| S1 industry-neutral | 0 of 8 | -0.302 | -0.263 | 0.989 | 1.000 | -0.11 | -0.09% | 1.42x | -25.4% | 88 / 88 | no |
| S2 size-neutral | 1 of 8 | -0.360 | -0.344 | 0.970 | 1.000 | -0.17 | -0.11% | 1.42x | -29.2% | 78 / 78 | no |
| S3a decile | 8 of 8 | +0.032 | +0.027 | 0.054 | 0.462 | 0.22 | +0.16% | 1.34x | -28.5% | 97 / 73 | no |
| S3b quintile | 0 of 8 | -0.214 | -0.203 | 0.996 | 1.000 | -0.02 | -0.02% | 1.16x | -24.1% | 193 / 155 | no |
| S3c 2x count | 0 of 8 | -0.219 | -0.203 | 0.998 | 1.000 | -0.03 | -0.02% | 1.22x | -24.7% | 174 / 146 | no |
| S4 inverse-vol | 3 of 8 | -0.057 | -0.025 | 0.632 | 1.000 | 0.13 | +0.08% | 1.43x | -22.2% | 87 / 73 | no |
| S5 conviction | 7 of 8 | +0.0995 | +0.098 | 0.013 | 0.219 | 0.29 | +0.23% | 1.44x | -28.1% | 87 / 73 | no (gain 0.0995 < 0.10; q 0.22) |
| S6 biweekly | 5 of 8 | -0.025 | +0.057 | 0.510 | 1.000 | 0.17 | +0.12% | 2.46x | -26.0% | 87 / 73 | no |
| S6 weekly | 1 of 8 | -0.209 | -0.011 | 0.881 | 1.000 | -0.02 | -0.02% | 4.08x | -31.4% | 87 / 73 | no |
| S6 monthly + band | 7 of 8 | +0.009 | +0.007 | 0.084 | 0.478 | 0.20 | +0.14% | 1.38x | -29.6% | 87 / 73 | no |
| S6 biweekly + band | 5 of 8 | -0.018 | +0.060 | 0.490 | 1.000 | 0.17 | +0.12% | 2.41x | -26.4% | 87 / 73 | no |
| S6 weekly + band | 1 of 8 | -0.200 | -0.011 | 0.872 | 1.000 | -0.01 | -0.01% | 3.97x | -31.3% | 87 / 73 | no |
| S7 four tranches | 5 of 8 | +0.011 | +0.021 | 0.403 | 1.000 | 0.20 | +0.14% | 1.40x | -27.3% | 87 / 73 | no |
| S8 beta-hedged | 0 of 8 | -0.137 | -0.123 | 0.781 | 1.000 | 0.05 | +0.02% | 1.63x | -30.7% | 87 / 73 | no |
| S9 gbm + lgbm_rank blend | 1 of 8 | -0.132 | -0.144 | 0.971 | 1.000 | 0.06 | +0.03% | 1.36x | -34.6% | 87 / 71 | no |
| S10 days-to-cover filter | 6 of 8 | +0.013 | +0.013 | 0.199 | 0.844 | 0.20 | +0.14% | 1.40x | -30.2% | 87 / 71 | no |
| S11 liquidity cap ($100k) | 0 of 8 | +0.000 | +0.000 | 1.000 | 1.000 | 0.19 | +0.13% | 1.40x | -29.7% | 87 / 73 | no |

Max drawdown is the mean over calendars of the daily NAV drawdown; "Net / 21 sessions" is the mean block return. No cell reaches +0.10 except S5 at +0.0995, so every cell fails the
gain condition; the cells that fail the calendar count or p are visible in their columns. Deflated-Sharpe caution for the best cell (S5): 0.16 with 18 trials.

## Reading by variant

- **S1, S2 (neutral books): clearly worse** (-0.30, -0.36). Forcing equal counts per industry or size tercile takes the model's picks out of where it ranks best. S2 also drops sub-$1bn names and uses
  today's cap table (see below), so it is not a clean test of size neutrality, but it is not close.
- **S3 (breadth): wider is worse** (quintile -0.21, 2x count -0.22). The decile cell wins every calendar but by +0.03, p 0.054: including the top 1% is not distinguishable from B1's exclusion of it.
- **S4 inverse-volatility** is lower on Sharpe (-0.06) but has the smallest drawdown (-22.2% against -29.7%). It is not a Sharpe candidate.
- **S5 conviction weights** is the only cell with a consistent sign (7 of 8, p 0.013) and the best Sharpe (0.29), at the same turnover and drawdown as B1. Misses the gain bar by 0.0005 and BH by a wide margin.
  Its one losing calendar is offset 17 (-0.07). Because the bar was fixed in advance, this is a miss.
- **S6 cadence: faster is worse**, because costs grow with turnover (weekly 4.1x against 1.4x). At 5 bps biweekly turns slightly positive (+0.06) but still not at the bar. The no-trade band changes almost
  nothing: B1's 21-session rebuild already moves whole names in and out, and held names rarely drift 25% from target. Monthly + band wins 7 of 8 by +0.009.
- **S7 staggered tranches** has the same Sharpe, turnover and drawdown as one monthly book (+0.011). Staggering did not reduce the calendar sensitivity the earlier trials found (S0 itself ranges -0.12 to +0.53 across the offsets).
- **S8 beta overlay** costs Sharpe (-0.14) and adds turnover; B1 is already close to dollar-neutral.
- **S9 blend** is worse than `lgbm_rank` alone (-0.13), on 1 of 8 calendars.
- **S10 days-to-cover filter** is neutral (+0.013, 6 of 8). It had little to act on: days to cover is present for only 29% of panel cells, so about 2 shorts a book were dropped (73 to 71).
- **S11 liquidity cap** at $100,000 never binds and equals S0, as registered. The descriptive capacity cells are not tests and are not in the BH step. At $25m it still equals S0; at $250m it binds, shrinks both legs
  and so cuts gross exposure and the fee on short notional: Sharpe +0.15 against S0, turnover 1.28x. That improvement comes from holding less, not from liquidity, and says only that this book's net edge
  is thin; it is not a reason to cap.

## If a cell were to be taken forward (none is)

For the owner's information only, since nothing passed. S5 would change `target_weights` (the `weights` argument, which already exists for the meta-label and alert-risk weights) to a conviction weight within each leg,
rebuilt at the 21-session rebuild; that is a sizing change needing explicit approval. A second registered run on a longer or fresh sample would be needed first: this one is one series with eight overlapping calendars.

## What was not measurable

- Real borrow availability and fees by date (today's easy-to-borrow list and a flat 44 bps are used for every date).
- Historical market caps: S2 uses today's cap table for every date (a look-ahead on size) and excludes sub-$1bn names that S0 keeps.
- Market impact beyond a flat 10 bps a side, and liquidity at account sizes above the paper account (S11's primary cell is degenerate at $100,000).
- Delisted names (survivorship, as in every earlier trial) and any days-to-cover figure for the 71% of cells that have none.
- The calendars share almost the same history, so the bootstrap's p-values are conditional on one 11-year path; eight calendars are not eight independent samples.
- S8 as a trader change: it needs an index fund, which the trader never holds.

## Changelog

New files: `shared/utils/book_strategies.py`, `tests/test_book_strategies.py`, `scripts/run_book_strategies.py`; this report; an appended section and amendment BST-1 in the pre-registrations file.
Tests: `tests/test_book_strategies.py` and `tests/test_b1_variants.py`, 24 passed; ruff `F,E9` clean. No other file edited, nothing committed, no container restarted.
