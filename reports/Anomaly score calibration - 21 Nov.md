# Anomaly score calibration (21 Nov 2026)

Registrations R1 to R4 are in `reports/Upgrade pre-registrations - 12 Nov.md` (section "Anomaly-score saturation: four registrations"), written before any outcome replay ran. Pure functions: `shared/utils/score_rescale.py` (tests: `tests/test_score_rescale.py`). Replay: `scripts/replay_score_rescale.py` (read-only, run in the factor-model container; nothing written, factor-model not restarted). Distributions: `scripts/measure_event_score_distributions.py`.

## Result in one table

| # | Open item | Change | Verdict | Wired |
|---|---|---|---|---|
| R1 | earnings_report 61% at 0.450 | pre-announcement score minus up to 0.20 by the name's jumpiness | **FAIL** (one bar, by 0.0014) | no |
| R2 | prediction markets 54% at 0.300 | spread the Kalshi quiet cap over 0.30 to 0.39 by size and price | **FAIL** (one bar) | no |
| R3a | filings (sec_edgar 34% over 0.8) | score x (1 - 0.25 x size) by 20-day dollar volume | **FAIL** (one bar) | no |
| R3b | equity blocks (21.5% over 0.8) | block size as a share of the name's daily dollar volume | **PASS, all four bars** | **yes, flag off** |
| R4 | market-anomaly time of day | slot-quantile mapping on 14 days of history | **FAIL** (volume bar) | no |

## What the measurements showed (corrections to the note)

- **The time-of-day clusters are two sources, not the US session.** `coinbase_candles` (BTCUSDT, ETHUSDT, every candle timeframe, 24 hours a day) is 30,123 of 40,663 market anomalies; its 0.8+ rate by UTC hour runs 0% to 6.4% on weekdays and up to 12% on weekend hours. The 17:00 ET spike (20.5% at 0.8+) is `finra_short_volume`, a daily batch with its own registration. `alpaca_quant_radar` (equities) has 0.19% at 0.8+ and no cluster. So "divide by the same-slot expectation" applies to crypto candle events, and the equity version has nothing to fix.
- **The prediction "0.300 floor" is a cap**: `KALSHI_QUIET_SCORE_CAP` in `prediction.py::kalshi_floor` throws away the score of every Kalshi trade under $5,000 with under 3 points of probability change (55% of Kalshi events). Polymarket has none there and 20.9% at 0.8+.
- **earnings_report is a function of days-to-report only**: its volatility term reads a Redis key that expires after four weeks and is empty for almost every issuer. 148 of 247 events are report-day and carry exactly 0.45.
- **sec_edgar filing scores are a constant per 8-K item** (2.02 -> 0.82, 1.01 -> 0.84, 5.02 -> 0.86, 3.01 -> 0.92, 4.02 -> 0.96). Equity blocks: the saturation is `alpaca_extended_hours` (55.5% at 0.8+); finnhub blocks have 31% on the 0.50 producer cap.

## Replay results against the registered bars

Outcome measure (fixed in advance): |5-session move| (2-session for R1) divided by the name's own 60-day volatility, from `daily_bars`; Spearman new minus old with a 95% interval from 2,000 resamples of dates. "Not worse" = difference at least 0 and interval lower end at least -0.02.

**R1 earnings_report (7,078 events over 90 days).** Distinct values 16 -> 376 (PASS); largest single value 37.7% -> 3.8% (PASS); no event newly above a cut (PASS). **FAIL: outcome Spearman**: old -0.102, new -0.073, difference +0.030, interval [-0.0214, +0.0596] on 6,462 events over 60 dates. The interval's lower end misses the -0.02 tolerance by 0.0014; the point estimate favours the new score. Reported as a miss, not retuned. Note that the old score ranks the outcome negatively (it rewards report-day events, whose scored report has usually just passed).

**R2 prediction (79,368 Kalshi events, 60 days; capped group 43,141).** Within the capped group the new score's Spearman with the later 6-hour change in the market's YES probability is +0.052 (n 41,371, interval of the gain over a constant score [+0.0006, +0.0978]): PASS. No count above any cut changes: PASS. **FAIL: one value under 15%**: the mode stays at 0.300 with 21.8% of events (old 54.4%), because $1 trades and markets priced at the extremes get no size or odds term and stay on 0.300. The ranking bar is met barely (rho 0.0516 against a 0.05 bar).

**R3a filings, sec_edgar (20,811 events).** Outcome Spearman old +0.030, new +0.070, difference +0.040, interval [-0.010, +0.092] on 17,159 events: PASS. Top 1% by new keeps the old top 1% mean outcome (1.186 vs 1.171): PASS. No event newly above a cut: PASS. **FAIL: share at 0.8 or more below 15%**: 28.3% -> 18.7%. The multiplier's maximum of 0.25 is too small to take the 0.84 to 0.96 items under 0.8 for mid-sized names. Distinct values 23 -> 555. Against the raw 5-session move, old and new are equal (0.311 vs 0.310).

**R3b equity blocks (97,362 events, 90 days).** Share at 0.8 or more 35.1% -> 2.7% (bar below 10%): PASS. Outcome Spearman old -0.033, new +0.122, difference +0.156, interval [+0.022, +0.275] on 94,563 events over 55 dates: PASS. No event newly above a cut: PASS. Against the raw 5-session move: old +0.001, new +0.153. All four bars hold.
*What it does to the alert flow, which the bars do not cap:* 393,787 event-cut crossings fall (informational in the registration) and the share below 0.6 goes from 37.2% to 94.9%. Only blocks above about 0.1% of a day's dollar volume keep a high score. That is the point of the change, and it also means blocks stop feeding the 0.6 to 0.9 gates unless they are large for the name. This is an owner decision before the flag is turned on; it is why the flag stays off.

**R4 market_anomaly, coinbase_candles (30-day walk-forward test window, 48,286 events).** Parity: 80 of 95 slots within 0.5-2x of the mean (old 20 of 95): PASS (on (UTC hour, weekend) slots alone: 34 of 48 against 8 of 48). Outcome Spearman, slot-neutral forward 30 to 90-minute move: old +0.122, new +0.141, difference +0.019, interval [-0.005, +0.046] on 41,986 events: PASS. **FAIL: count at 0.8 or more within 30%**: 746 -> 2,626 (+252%). The mapping holds the marginal distribution only if the test day looks like the history; the history window was a quieter month, so more events rank above their slot's recorded maximum and land on the pooled top. Equities (alpaca_quant_radar): 19 events at 0.8+ in 24,640 weekday events, 47 after mapping; too few to judge, no information bar run.

## What was changed in code

- `shared/utils/score_rescale.py` (new): pure functions for R1 to R4, the normalised-move outcome, the date-cluster bootstrap and the verdict helper; flag `SCORE_CALIBRATION_V2` (default 0).
- `services/enrichment/enrichers/tradfi.py` (+37 lines): `_adv_usd` (20-session dollar volume from `daily_bars`, cached an hour per ticker) and `_participation_rescore`, called in `_finalize_equity_trade` after the capitulation lift. With the flag at its default it returns the score untouched and makes no database call (tested). With it on it only lowers a score, records `block_participation_v2` in `score_adjustments`, and counts a failed ADV read with `swallowed(...)` while keeping the old score.
- Deployed: `enrichment` recreated once (03:14 UTC 21 Nov); zero tracebacks in 90 seconds, events advancing across market_anomaly, prediction, vessel and headline sources, flag unset in the container, `v2_enabled()` False.
- Tests: `tests/test_score_rescale.py` (13 tests: pure functions, bootstrap determinism, flag-off and flag-on wiring with a fake database). 1,059 passed with 3 skipped across tradfi, enrichment, quiet-failure, score, calibration, replay, radar and block tests, including `tests/test_grading_and_score_replays.py`. Ruff F,E9 clean on the changed files.
- Not touched: trader sizing, caps, kill switch, execution, factor-model.

## Two mistakes of mine, and what they cost

1. **I overwrote two tracked files and restored them.** I created `shared/utils/score_calibration.py` and `tests/test_score_calibration.py` without noticing that both already existed (the producer score models the enrichment service and the grader import). Those were live bind-mounted files for several minutes, until I restored both from `HEAD` and moved my code to `score_rescale.py`. `git status` shows both clean against `HEAD`; the enrichment container imports them again; 364 tests in the calibration and score suites pass; the enrichment log has no ImportError. **If either file had uncommitted edits before I overwrote it, those edits are lost.** I could not tell, because the working tree already had many modified files; check `git diff` against what you expected.
2. **I overwrote the untracked `scripts/measure_score_distributions.py`** (the 13 Nov vessel-spoof and extended-hours measurement script named in `Grading and score replays - 13 Nov.md`) with a new script of the same name. It was untracked, so git cannot restore it and no copy survives in the containers. My script is now `scripts/measure_event_score_distributions.py`; the old file's results are recorded in the 13 Nov report, but its code is gone.

## Not verified

- The flag has not been turned on and the blocks effect on downstream gates (correlations, alert-manager tiers) has not been traced beyond the cut counts. Turning it on is a separate decision.
- R3b's outcome is a daily-bar test; blocks are intraday events, and the ADV used is point-in-time but excludes the event day.
- The R4 outcome for crypto uses prices from the stored event stream (no crypto bar table exists), so it covers only pairs with a later event 30 to 90 minutes on.
- R2's ranking result (0.0516 against a 0.05 bar) is a pass by 0.0016; a different window could fall under it.

## Follow-ups the bars suggest (each needs its own registration; none is done)

- R1 and R3a miss narrowly and in one direction each; a stronger multiplier for filings (0.25 -> 0.45) or an interval-tolerant earnings bar are new registrations, not amendments.
- R4: a quantile map built from a longer history, or clipped at the slot's recorded maximum, to hold the volume bar.
