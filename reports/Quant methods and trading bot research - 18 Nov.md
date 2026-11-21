# Quant methods and trading bot research (18 Nov 2026)

Read-only research. Nothing in the trader, sizing, caps, kill switch, execution or the model was changed, no order was sent, nothing was committed.
Two new scripts, both read-only and run against stored data: `scripts/research_quant_live_book.py` (live book, borrow quotes, executed drift trades) and
`scripts/research_quant_drift_band.py` (registered in its docstring before the run). Read first, so nothing here repeats a closed trial: CLAUDE.md, MODEL_TRIALS.md,
`reports/Book strategy trials - 13 Nov.md`, `reports/Upgrade pre-registrations - 12 Nov.md`, `reports/Progress and open items.md`, and the code in
`services/agents/auto_trader.py`, `shared/utils/auto_trade.py`, `shared/utils/model_portfolio.py`, `shared/utils/factor_model.py` (walk-forward, specs, models).

**Evidence labels.** "Measured here" = I ran it on stored data today. "Code" = read in the repo. "Literature" = from my memory of the published work, not re-fetched this
session: check the paper before quoting any number. Effect sizes are guesses with a stated range unless marked measured.

## Summary

1. **The biggest finding is a measurement mismatch, not a new strategy.** The trials charge the short leg a flat 44 bps a month (5.3% a year). The 95 names in the live short book
   have quoted borrow fees of mean 0.47% a year, median 0.37%, p90 0.56%, one above 5% (measured, `borrow_rates`, 18 Nov). On the same series that is about +0.2 to +0.3 net Sharpe
   (arithmetic from the published 0.40 / sim 0.19, not a test). It cuts the other way too: the registered P1 test showed the short edge sits in names with persistent delivery fails, and
   those are the names a cheap-borrow book cannot hold. So the live short leg is cheaper and probably weaker than the history says. Both effects need to be on the same page before any
   decision about the short leg.
2. **The model trains on one cross-section in 21** (`HORIZON_SPECS["monthly"].train_every = 21`, code). Training density and seed noise have never been trialled. This is the untested model idea with the best ratio of expected gain to cost and risk.
3. **The live trader trades about 5.8% of NAV a day in drift orders** (add/trim, about 99 orders a day, 1,098 of 1,359 under $200; measured). That is 1.22x NAV per 21 sessions, about as much as the monthly rebuild itself (1.40x),
   about 12 bps of NAV per 21 sessions at 10 bps a side. The book simulator the trials used lets weights drift, so it never paid this. Sizeable against a net edge of +13 to +33 bps per 21 sessions.
4. **B1's Sharpe is partly the market.** B1 is dollar-neutral, not beta-neutral: ex-ante 126-day beta is 1.68 on the long leg and 1.22 on the short leg, net +0.23 at half each (measured). The
   S8 hedge cost 0.14 Sharpe (13 Nov), consistent with a positive market contribution in an 11-year bull run. Treat the 0.40 as "alpha plus some beta".
5. **A paper forward record cannot confirm B1 on any useful time scale.** Minimum track record for a Sharpe of 0.40 against zero at 95% one-sided is about (1.645 / 0.40)^2 = 17 years (normal returns; fat tails and skew lengthen it). The "24 months of forward record" condition is a screen against disaster, not proof of edge. Decisions should lean on the pre-registered historical tests, with the forward record as a tripwire (the CUSUM rule already is one).

## 1. What the live code does, and where it loses money (code read, with measurements)

| # | Finding | Where | Evidence | Size | Status |
|---|---|---|---|---|---|
| W1 | Short fee in every trial is 44 bps a month; quoted fees for the live short book average 0.47% a year | `shared/utils/b1_variants.py:24` `SHORT_FEE_BPS`; `borrow_rates` | Measured. File-wide: mean 10.7%, median 1.22%, 29% above 5% a year. Live shorts (ETB, at most 5%): mean 0.47% | About 20 bps per 21 sessions of NAV overcharged | Measurement fix, no trading change |
| W2 | Daily rebalance-to-target creates a stream of tiny add/trim orders | `auto_trader.py:359-360` (`target_shares`, `plan_orders` every session), `auto_trade.py:253` only skips under $5 | Measured: 10 sessions, 98.7 orders and 5.8% of NAV a day; mean order $100. Observed slippage on them 4 to 16 bps | About 12 bps of NAV per 21 sessions | Needs owner approval |
| W3 | Whole-share shorts make W2 worse on a small account: a $526 target in a $200 stock is a 38% step per share, and `whole_share_leg` hands the marginal share to a different name as prices move | `auto_trade.py:166-181` | Measured: random-basket sim of daily rebalance gives only 0.45x NAV per 21 sessions against 1.22x live; the gap is rounding and live volatility (live names 56% annual vol) | Disappears at larger capital | Part of W2 |
| W4 | Veto exits are direction-blind for 8-K items: delisting notice (3.01), bankruptcy (1.03), non-reliance (4.02) cut a short exactly as they cut a long | `auto_trade.py:141-145` | Code. The repo's own event studies treat these as short signals | Unknown, few names | Needs owner approval |
| W5 | Divergence veto sells a long after a 3 sigma fall against peers and covers a short after a 3 sigma rise: a stop after the move. All 7 vetoes in force today are this type | `auto_trade.py:146-155`; stored book `vetoes` | Measured (7 of 184 names). Literature on short-term reversal points the other way for news-free moves | Small, sign uncertain | Test first |
| W6 | The stored book has `scores_as_of = None`; the module header says it should carry it | stored book (measured); `auto_trader.py:547` writes only three keys | Measured | Audit gap only | Needs approval (trader file) |
| W7 | Drawdown halt at 15% is inside B1's normal range. The 13 Nov simulation shows a mean maximum drawdown of -29.7% and a worst month of -35%. A trip freezes the book (nothing sold, no rebalance) until someone deletes a Redis key | `auto_trader.py:416-418`, `auto_trade.py:310-333` | Sim figures; owner decision 5 (13 Nov) already confirmed "a halt sells nothing" for faults | Policy, not alpha | Needs owner approval |
| W8 | The per-name cap of 3% cannot bind: equal weight is about 0.55% a name | `auto_trader.py:358` | Code | None unless a leg thins out | Informational |
| W9 | No exposure monitoring: net beta, industry mix and volatility imbalance are untracked between rebuilds (the drift alarm assumes beta = sum of weights, about 0, against +0.23 ex-ante) | `auto_trader.py:331` | Measured beta | Alarm threshold shift of about 0.23 x SPY move | Reporting only |
| W10 | First fills on rebuild day were not recorded (fill fields start 5 Nov). There is no measured implementation shortfall for a rebuild | `auto_trade_orders` | Measured: 2 'open' orders with fills | Unknown cost of the largest trade of the month | Dated: 30 Nov rebuild |

Not found, so not listed as weaknesses: stale-score guard (works, `MAX_SESSIONS_BEHIND = 2`), idempotent order ids, thin-score guard, picks-only filter, unshortable skip. Slippage of the drift orders against the live reference (buys +11 to +16 bps weighted, sells +4 to +6 bps) is in line with the 10 bps a side the trials charge; shortfall against yesterday's close is not readable from drift orders (they are chosen after price moves, so it is biased by construction).

## 2. Registered mechanical test run today (W2): trading between rebuilds

Registered in `scripts/research_quant_drift_band.py` before the run. 300 random dollar-neutral baskets (89 longs, 95 shorts) from the 400 most dollar-traded of the model's ~1,000 scored names, 252 sessions to 18 Nov 2026, rebuilt every 21 sessions, 10 bps a side, borrow ignored.
Reading fixed first: a band is worth proposing if it beats daily rebalance by at least 5 bps per 21 sessions net, in at least 90% of baskets, with gross no more than 3 bps lower.

| Rule | Net bps / 21 | Gross bps / 21 | Turnover x NAV / 21 | Net minus daily | Baskets beating daily | t over 12 blocks |
|---|---|---|---|---|---|---|
| R0 drift (the trials' simulator) | -1.3 | -0.3 | 0.10 | +3.7 | 83% | 12.6 |
| R1 daily to target (the live trader) | -5.0 | -0.5 | 0.45 | 0 | | |
| R2 band 25% of target | -1.5 | -0.4 | 0.12 | +3.4 | 93% | 18.0 |
| R3 band 50% of target | -1.4 | -0.3 | 0.11 | +3.6 | 86% | 12.4 |

**Verdict on the registered bar: not met** (+3.4 against 5 bps; R2 passes the 90% and gross conditions). The mechanical saving is real but small, because random baskets have lower volatility and no
whole-share rounding. The live measurement is about three times larger (1.22x against 0.45x turnover), so the live saving is probably 5 to 10 bps per 21 sessions, which this test cannot show. The right
test is a replay on the live order stream (item T5 below). The t-statistics are over one 12-block path and are not a bar.

## 3. Published evidence relevant to a 90-names-a-side monthly book on free data (literature, from memory)

- **Most anomalies are weaker than published and decay.** McLean and Pontiff (2016): out-of-sample and post-publication returns about 26% and 58% lower. Hou, Xue and Zhang (2020): most of 400+ anomalies fail once microcaps are controlled. Harvey, Liu and Zhu (2016): a new factor needs t above 3. This supports the repo's habit of pre-registering and of a low prior for any new feature, and the 0.95 deflated-Sharpe gate.
- **Short legs carry most of the anomaly return and most of the cost.** Drechsler and Drechsler (2016) and Beneish, Lee and Nichols (2015): the shorting premium is concentrated in high-fee, low-supply names. This matches P1 in MODEL_TRIALS.md. The implication for B1 is in W1.
- **Costs, turnover and trading partway.** Novy-Marx and Velikov (2016) and Garleanu and Pedersen (2013): low-turnover rules and partial trading toward the target keep most of the gross return at lower cost; DeMiguel, Martin-Utrera, Nogales and Uppal (2020): combining signals with a turnover penalty raises net Sharpe. Frazzini, Israel and Moskowitz (2018): large managers' real costs are well below academic estimates, so 10 bps a side is not generous for liquid names. In this repo the rank buffer (B1 buffer, +0.01) and the 25% band (S6) added nothing at the monthly rebuild, so the live gain is limited to the between-rebuild trading in W2.
- **Signal combination and ensembling.** Gu, Kelly and Xiu (2020): tree and net ensembles beat linear models, gains concentrated in the extremes, averaged over seeds. Ensembling across seeds reduces variance without adding bias. The repo's trials show averaging two different models (O4, S9) did not help, which is a different question from averaging seeds or training phases of one model.
- **Volatility management.** Barroso and Santa-Clara (2015), Daniel and Moskowitz (2016), Moreira and Muir (2017): scale exposure by the strategy's own trailing volatility to cut momentum crashes. Cederburg et al. (2020) find the gain mostly vanishes out of sample. Local evidence (S4, lab momentum-volatility scaling +0.01, the regime gauge, B5) agrees with the sceptics.
- **Short-term reversal.** Jegadeesh (1990), Nagel (2012): month-ahead reversal is stronger in high-volatility, low-liquidity states and is compensation for liquidity provision. The model already has `rev_5` and `rev_21`. A "stop after a 3 sigma move" rule (W5) trades against it unless the move was news.
- **Honest evaluation.** Bailey and Lopez de Prado: deflated Sharpe (already implemented), probability of backtest overfitting by combinatorially symmetric cross-validation (not implemented), and minimum track record length (see Summary 5). Purged k-fold with embargo is already in `walk_forward`.
- **Target construction.** Rank or Gaussian-rank targets and cross-sectional ranking features are standard; the live `lgbm_rank` is a LambdaRank. Beta, industry and simple-return targets were trialled and rejected (O2, T1). Not trialled: volatility-scaled and multi-horizon averaged labels.

## 4. Top 10 ideas, ranked by expected net return per unit of risk

"Bar" below means the pre-registered bar. Defaults for every test: eight calendars (offsets 0, 5, 10, 15, 2, 7, 12, 17), one unit per date (21-session block bins, bootstrap by bin as in the 13 Nov trial, seed fixed before the run),
simple returns, 10 bps a side on traded notional, borrow charged, paired against S0 (B1), Benjamini-Hochberg q below 0.10 across every cell registered in this report together with the 17 cells already run, deflated Sharpe reported
with the running trial count (18 + the cells below). A miss is a miss.

### T1. Re-price borrow with quoted fees, and report the hard-to-borrow caveat together (measurement; no trading change)

- **Mechanism.** The cost term is wrong by about 10x for the live short book, in B1's favour. The opposite error is that the history's short edge sits partly in names the live book cannot borrow cheaply.
- **Evidence.** Measured (W1) plus P1 in MODEL_TRIALS.md. Borrow quotes only exist from 9 Oct 2026 (37 days, 19,416 names a day), so history cannot be priced per name.
- **Expected effect.** Net Sharpe of the same series rises about +0.2 to +0.3 at a fee of 0.5% a year; at 2% a year about +0.15; at the old 5.3% nothing. Not alpha: it changes which decisions are right (the S11 "holding less helps" reading and the "drop costly names" reading both rest on the wrong fee).
- **Test (registered now).** Re-score S0 and the already-stored cells at three fees: flat 0.5%, 2.0% and 5.3% a year on short notional, plus a per-name version using each name's quoted fee on the rebuild date once the quotes cover one full rebuild (from about 30 Nov). Reading: B1 passes if mean net Sharpe at 2% is at least 0.30 and positive on all eight calendars, which is the existing H1 rule at a fee supported by data. No bar for change; descriptive.
- **Cost.** No inference. Hours of CPU if the walk-forward is re-run in its own 6 GB container; minutes if the 13 Nov per-calendar daily series (`book.json`, not found on disk this session) is re-used.
- **Risk.** None to the book. Risk of over-reading: today's quote file is a snapshot of liquid names.

### T2. Training density: sample every 7, 3 or 1 sessions instead of every 21 (model; untested)

- **Mechanism.** `train_every = 21` feeds the ranker about 130 cross-sections out of ~2,700 available sessions in 11 years. The label overlap is real (21-day returns on adjacent days share 20 days), but adjacent days still carry different feature states, so effective sample size grows well beyond 1x. Averaging over phases also removes the dependence of the fit on which day the sample grid starts, the same kind of calendar luck that moves S0's Sharpe from -0.12 to +0.53.
- **Evidence.** Principle, not a citation: bagging over resampling phases and sequential bootstrap for overlapping labels (Lopez de Prado). No local result. No trial in the repo varies this parameter (grep of reports and scripts found none).
- **Expected effect.** Rank IC +0.002 to +0.006 (the model's IC is 0.040 to 0.043), a Sharpe change of 0 to +0.15, with real chance of nothing. This is the largest untested lever I found; I would not bet on more than the low end.
- **Test.** `walk_forward(..., spec=HorizonSpec("monthly", 21, 21, train_every, 21, min_train_dates))` with `train_every` of 7 and 3 (and 1 only if the 7-session cell shows any sign), `min_train_dates` scaled so the first fold starts at the same session (36 for 7, 84 for 3). Embargo and purge unchanged at 21. Model `lgbm_rank`, same features and seed. **Bar:** mean net Sharpe at least +0.10 over `lgbm_rank` at 21, better on at least 6 of 8 calendars, IC t no lower than 0.3 below baseline, BH q below 0.10. Two cells, so two trials.
- **Cost.** No inference. CPU: fit time grows with rows (3x to 21x on a 1,000-name panel; the model fits in a few seconds to minutes per fold), nightly run time may grow if adopted. Own container, outside 22:00 to 06:00 UTC.
- **Risk.** Overfitting to autocorrelated labels (checked by the purged out-of-sample design); a longer nightly run (the last runs took about 8 hours).

### T3. Seed bagging and score averaging over recent sessions (model; noise reduction)

- **Mechanism.** One LightGBM fit with `random_state=7`, `subsample=0.8`, `colsample_bytree=0.8` (`factor_model.py:949-951`). The top tenth shares only 58 of 100 longs and 42 of about 98 shorts between consecutive nightly runs (MODEL_TRIALS.md, 6 Nov), so part of the book's membership at a rebuild is draw noise. Averaging five seeds, and averaging the percentile over the last 3 to 5 sessions' scores, shrinks that noise at no signal risk.
- **Evidence.** Literature (Gu, Kelly and Xiu average seeds), plus the local stability figure above. A different thing from the O4 and S9 failures, which averaged two different models.
- **Expected effect.** IC +0.001 to +0.004, turnover at rebuild down 5% to 15%, Sharpe +0.00 to +0.08. Small but close to free.
- **Test.** Cell A: `lgbm_rank` with 5 seeds, rank-averaged. Cell B: S0 with percentile averaged over the scores of the rebuild row and the 4 previous sessions (the harness already scores every session). Both on the eight calendars. **Bar:** mean net Sharpe at least +0.05 above S0, better on at least 6 of 8, BH q below 0.10; the smaller bar (0.05, not 0.10) is registered here because the expected gain is small and the cost near zero. Say so in the result.
- **Cost.** No inference. Cell A is 5x training CPU. Cell B reads scores that already exist.
- **Risk.** Averaging over recent sessions delays the signal by up to 4 sessions out of a 21-session horizon (the stale-score cost).

### T4. Replay of a no-trade rule on the live order stream (execution; needs owner approval to apply)

- **Mechanism.** W2/W3. Skip an add or trim unless the dollar change is at least 35% of the name's target dollars and at least $250 (exits, opens, flips, and any order that reverses the sign of a position still go).
- **Evidence.** Measured live (turnover, order size) plus Section 2 (mechanical sim, +3.4 bps).
- **Expected effect.** 5 to 10 bps of NAV per 21 sessions on this $100k account (0.6% to 1.2% a year on gross exposure 1.0), less at larger capital. A real but modest gain, near-zero risk.
- **Test.** Replay 30 sessions of `auto_trade_orders` after 30 Nov: recompute each session's counterfactual orders under the rule from the stored book and prices, count orders and notional saved, and the weight error vs target (mean absolute deviation per name, and the net and gross exposure deviation). **Bar:** at least 40% fewer add/trim orders, mean exposure deviation under 25 bps of NAV, and the savings (at the observed 8 bps a side) at least 5 bps per 21 sessions. Unit of analysis: session.
- **Cost.** None (offline replay, no inference).
- **Risk.** Longer drift from target between rebuilds; the dollar-neutral gap widens slightly. This is an execution change: not applied.

### T5. Divergence and 8-K vetoes: test the direction (needs owner approval to change)

- **Mechanism.** W4 and W5. A direction-aware rule would keep a short through a delisting notice, bankruptcy or non-reliance filing (events the repo's own event studies grade as short signals) and would not sell a long after a news-free 3 sigma fall.
- **Evidence.** Code plus the repo's own event studies (8-K delisting notice, dilution, auditor change among the tradable short cells); literature on reversal. The V2 thesis exit trial (8-K items only, 1 of 8 calendars, -0.043 mean) already says the 8-K veto does not help the book; it did not test the divergence veto and did not test side-awareness.
- **Expected effect.** Between -5 and +10 bps per 21 sessions; affects about 4% of names. Not a priority on size alone.
- **Test.** Two cells added to the book simulator: S0 plus the divergence veto (name's 5-session return minus its industry median, z over 252 sessions, at least 3 sigma, against the position, exit to cash until the next rebuild) against S0; and S0 plus the 8-K veto applied to longs only. 8-K items come from stored filings (the history path exists, 2016 on). **Bar:** the veto cells must beat S0 by 0.10 on 6 of 8 calendars to justify keeping the divergence veto; if S0 beats the veto by 0.10 on 6 of 8, remove it. Either direction is reported.
- **Cost.** No inference. Event reconstruction is a daily-bar computation.
- **Risk.** Little; the book is the same at rebuild.

### T6. Training-label variants: multi-horizon average and volatility-scaled (model; untested)

- **Mechanism.** The 21-day label is noisy. Averaging the rank of the 10-, 21- and 42-day forward excess returns (with purge set to 42) lowers label variance, as in practitioner multi-horizon labelling. A volatility-scaled label (return divided by trailing volatility) removes the pull toward the high-volatility short edge, which F1 (5 Nov) found is most of B1's return.
- **Evidence.** Practitioner use; no reliable academic result. The volatility-scaled variant is more likely to lose Sharpe by moving away from the factor B1 earns on.
- **Expected effect.** Multi-horizon: Sharpe -0.05 to +0.10. Volatility-scaled: negative to flat; useful mainly as a test of how much of B1 is "short high volatility".
- **Test.** `training_target` modes (`"multi_horizon"`, `"vol_scaled"`), eight calendars, same bar as T2 (+0.10, 6 of 8, IC t within 0.3, BH). Two trials.
- **Cost.** No inference; about 1x to 2x training CPU.
- **Risk.** Longer purge for multi-horizon trims usable history by 21 more sessions.

### T7. Exposure report and a drawdown policy decision (monitoring now; policy needs owner approval)

- **Mechanism.** W7 and W9. B1's drawdown halt is expected to trip inside normal operation (a mean sim drawdown of -29.7% against a 15% limit). A halt that freezes the book through a squeeze is the opposite of risk control. Options for the owner: (a) keep freeze-on-fault but make drawdown a "reduce gross to 50% at 15%, flat at 25%" rule; (b) keep as is; (c) a review trigger instead of a halt. A book-level volatility scaler (Barroso and Santa-Clara) is a variant of (a).
- **Evidence.** Sim drawdowns; B6 (stop-loss on shorts) and V4 (volatility trailing stop) failed on Sharpe; book-level scaling has mixed literature (Cederburg et al.) and a flat local result (lab scaling +0.01, regime gauge 0.66 to 0.63).
- **Expected effect.** Sharpe change near 0; maximum drawdown -29.7% down to about -22% to -25% at the cost of 0.00 to 0.05 Sharpe (S4's drawdown of -22.2% came with -0.057 Sharpe). Risk control, not return.
- **Test (registered now).** Simulate S0 with a gross multiplier of 0.5 after a 15% drawdown from the trailing peak and 1.0 again after the NAV recovers by half of the drawdown, on the eight calendars. **Bar:** worst-month and maximum drawdown improved by at least 20% relative, mean Sharpe no worse than -0.05 against S0. One cell.
- **Cost.** None.
- **Risk.** De-grossing at the lows locks losses (whipsaw), the main reason vol management has failed out of sample.
- **Do now, no approval needed:** a daily read-only exposure line (net beta, long and short mean beta, top industries by weight, leg volatilities) in the dashboard or the heartbeat, from `daily_bars` as in `scripts/research_quant_live_book.py`.

### T8. Staggered tranches, judged on dispersion and drawdown (needs owner approval)

- **Mechanism.** S0's Sharpe ranges from -0.12 to +0.53 across the eight calendars (13 Nov table). The live book has one rebuild date, so its realised outcome is one draw from that range. Four tranches (S7) average the draws.
- **Evidence.** S7 on the 13 Nov trial: mean Sharpe +0.011 against S0, drawdown -27.3% against -29.7%, turnover the same. So no expected-return gain, a small drawdown gain.
- **Expected effect.** Mean unchanged; the range of outcomes across start dates narrows by up to about half. Real market-impact benefit appears only at institutional size.
- **Test.** Already run as S7 for the mean. The new, registered question is dispersion: for each of 8 calendar-offset pairs, compare the spread of the sixteen start-date outcomes of S7 against S0 by the variance of block Sharpe across start offsets. **Bar:** at least 30% lower variance with mean no lower by more than 0.05.
- **Cost.** No inference. Operationally the largest change of the ten (four sub-books); on a $100k account whole-share rounding on 20-name tranches makes it worse (W3).
- **Risk.** Complexity and rounding churn; not recommended at current account size.

### T9. Rebuild-day implementation shortfall, then a time-of-day and limit A/B (measurement first; any change needs owner approval)

- **Mechanism.** The largest trade of the month (1.4x NAV) goes at 09:40 ET with a limit up to 0.5% from the last IEX trade (`auto_trader.py:39, 564-568`). The opening minutes have the widest spreads and the largest price discovery. The trials charge 10 bps a side on assumption; the first rebuild with recorded fills is about 30 Nov (W10).
- **Evidence.** Literature on intraday spread patterns (wide at the open); local: drift orders show +11 to +16 bps on buys against +4 to +6 on sells, an asymmetry I cannot explain from stored data.
- **Expected effect.** If true shortfall on rebuild day is 15 bps a side rather than 10, that is -14 bps of NAV a month against the assumption; a later start or a tighter limit might recover 3 to 8 bps a side. Unknown until measured.
- **Test.** (1) At the 30 Nov rebuild record shortfall against the prior close, the 09:40 reference, the 10:30 price and the day's volume-weighted price, by order, with the long and short legs separate, one unit per rebuild day (so a single data point; descriptive only). (2) If shortfall against the 10:30 reference exceeds 8 bps a side, run an A/B on the drift stream: random half of orders at 09:40, half at 10:30, by day, 20 sessions. **Bar:** paired by day, a 10:30 start saves at least 3 bps a side with no more unfilled orders.
- **Cost.** None to measure.
- **Risk.** A later start adds market-move risk between the decision and the fill.

### T10. Honest-evaluation additions: PBO and a power statement (no trading change)

- **Mechanism.** The repo counts trials (deflated Sharpe, BH) but has no probability of backtest overfitting and no statement of what the forward record can show.
- **Evidence.** Bailey, Borwein, Lopez de Prado and Zhu; the minimum-track-record arithmetic in the Summary.
- **Expected effect.** None on return; it protects against adopting a lucky variant (S5 sits at deflated Sharpe 0.16 with 18 trials).
- **Test.** Build the matrix of block net returns for the 18 cells of the 13 Nov trial plus the cells of this report; compute PBO with combinatorially symmetric cross-validation (16 time slices), report the logit distribution and the rank-degradation slope; and a minimum track record length for B1 at its measured Sharpe and fat-tailed variance. Reading fixed now: PBO above 0.5 means choosing the in-sample best of the set is no better than chance; the S5 shadow bar must not be relaxed in that case.
- **Cost.** None (a short script on the stored series, once the series exists).
- **Risk.** None.

## 5. Rejected, with reason

| Idea | Why not |
|---|---|
| Industry-neutral, size-neutral or beta-hedged books; beta or industry targets | S1 -0.30, S2 -0.36, S8 -0.14, O2 rejected. B1's edge sits where the model ranks best; neutralising removes it |
| Wider books (decile, quintile, 2x count) | S3b -0.21, S3c -0.22; S3a +0.03 is not distinguishable from the exclusion of the top 1% |
| Inverse-volatility or shrinkage-covariance weights | S4 -0.06, V3 -0.49. Almost all the short leg's return is in its most volatile names (5 Nov) |
| Stops, time decay, trailing stops, 15% short stop | B6 -0.09, V1 -0.25, V4 -0.05, V2 -0.04 |
| Weekly or biweekly rebalance; rank buffer; 25% band at the rebuild | S6 weekly -0.21, biweekly -0.03; buffer +0.01 at 10 bps; band +0.009. Turnover cost eats the gain |
| Averaging `gbm` and `lgbm_rank` | S9 -0.13 on 1 of 8 calendars; O4 near miss only |
| Regime gauge, market-stress multiplier, momentum-volatility scaling | B5 +0.05 net a month with Sharpe 0.14, lab scaling +0.01, gauge 0.66 to 0.63 |
| Fundamentals, insider, 8-K, commodity, PEAD (`sue`) as model features | O3, O6, O8 and the `sue` trial all lowered or did not move Sharpe |
| Days-to-cover filter on shorts; CFTC, Reg SHO, Treasury auctions | S10 +0.013, 6 of 8; the three studies found no tradable signal |
| Dropping costly-to-borrow shorts to save fee | P1: B1's Sharpe fell to -0.04 without persistent-fails names. Also moot at quoted fees of 0.5% (W1) |
| Liquidity cap at account size | S11 never binds at $100k; the $250m cell's gain came from holding less, which is a fee artefact (W1) |
| Neural networks, factor-zoo deep learning | Needs many more labelled cross-sections than 130 monthly dates of 1,000 names; gains in the literature come from large panels and many seeds |
| Intraday scanning, paid data, options overlays, index hedges | Out of scope (free data, picks-only, execution approval) and P2 dropped intraday scanning |
| Live use of S5 conviction weights now | Recording in shadow; bar not met (gain 0.0995 against 0.10, q 0.22, deflated Sharpe 0.16) |
| Keeping the trader's drift trading as is | Not rejected: moved to T4 |

## 6. Needs owner approval (propose, not applied)

| Item | What it would change | Where | Gate before approval |
|---|---|---|---|
| T4 no-trade rule on add/trim orders | Execution logic: fewer orders between rebuilds | `auto_trade.plan_orders` / `auto_trader.run_once` | Replay on 30 live sessions meets its bar |
| T5 direction-aware 8-K veto; divergence veto kept, narrowed or removed | Risk-off exits | `auto_trade.veto_names`, `auto_trader._vetoes` | Simulation cells meet their bar |
| T7 drawdown policy (de-gross instead of freeze) | Kill switch and sizing | `auto_trade.safety_halts`, `auto_trader._check_and_record` | Simulation bar; owner's choice among (a), (b), (c) |
| T8 staggered tranches | Rebalance structure and sizing | `REBALANCE_SESSIONS`, `_book` | Dispersion test; not before capital is larger |
| T9 later start time or tighter limit | Execution timing and limit offset | `TRADE_AFTER`, `AUTO_TRADE_LIMIT_OFFSET` | Shortfall measured at the 30 Nov rebuild, then A/B |
| W6 write `scores_as_of` into the stored book | Trader bookkeeping (no sizing) | `auto_trader._book` | None needed beyond a unit test; listed because it touches the trader file |
| Any use of T2, T3 or T6 in the live model | Which scores rebuild B1 | `factor_model.py`, nightly run | Bar met and the model-switch margin rule applied |
| The short fee used in decisions (T1) | The cost assumption behind every future book trial | `b1_variants.SHORT_FEE_BPS` | A registered amendment, not a quiet edit |

## 7. What to do first

1. T1 (a registered, no-risk correction of the fee assumption) and T10 (PBO and the power statement), because every later decision uses them.
2. T2 and T3 together in one container run (three cells at train_every 7 and 3, five-seed, five-session score average): about one run of the 13 Nov kind, no inference, the best chance of a real model gain.
3. T4 replay once 30 sessions of fills with fills recorded exist (about 30 Dec); the rebuild on about 30 Nov gives the first T9 measurement.
4. Leave T5, T7 and T8 for the owner, with their tests run first.

Trial count: this report registers T2 (2 cells), T3 (2), T5 (2), T6 (2), T7 (1), T8 (1) = 10 new cells on top of the 18 counted on 13 Nov, so the next deflated-Sharpe count is 28 if all are run. T1, T4, T9 and T10 add none.

## Not done and not verified

- Literature figures are from memory; none re-fetched. Check each paper before quoting.
- No walk-forward was run (hours, 6 GB, not between 22:00 and 06:00 UTC), so T2, T3, T5, T6, T7, T8 are specified, not tested. The expected effects above are my estimates and could be zero.
- The 13 Nov per-calendar series (`book.json`) was not found on disk, so the T1 +0.2 to +0.3 is arithmetic from the published Sharpe and block volatility, not a re-score.
- Live statistics rest on 10 to 13 sessions (drift trades, equity record: -2.11% since 29 Oct against SPY about -5%, consistent with net beta +0.23); they say nothing about edge.

## Changelog

New files: this report, `scripts/research_quant_live_book.py`, `scripts/research_quant_drift_band.py`. No other file edited, nothing committed, nothing deployed, no orders; the scripts only read.
Container runs: `sentinel-api_gateway` (read-only queries and a 300-basket simulation, under two minutes), `sentinel-timescaledb` (read-only `psql`).

## Addendum, 19 Nov: layer-audit findings

- **Flat carry-forward bars (checked by layer-audit, `scripts/audit_layer_flatbars.py`).** Over 252 sessions, 8.35% of all `daily_bars` rows are flat and 3.07% have zero volume. For the 998 model-scored names both figures are 0.00%, and for the B1 book 0.01%. That is under the 1% bar I proposed, so no change to model inputs. `_PANEL_SQL` (`services/agents/factor_engine.py:225`) has no volume filter, so add `volume > 0` first if the universe is ever widened.
- **ADR market caps are wrong in the data (layer-audit item a).** TSM is about 5x, EQNR 10x, NGG 6x, E 3x and SHEL 2.6x. Any size-band result and the $1bn market-cap floor are skewed for foreign names. Treat the size-band cells as provisional until the caps are fixed.
- **Agent grading windows (item c).** They start a session late for pre-open calls and grade Fridays as 0. The re-grade leaves "no clustered beta-adjusted edge" intact. This does not change any B1 figure above.
- **Trials in progress.** `quant-trials` is running T1, T2, T3, T5, T6, T7, T8, T10 and the flat-bar cell; results will be in `reports/Quant trial results - 19 Nov.md`. Trial count 29 (30 if t2_1 runs).
