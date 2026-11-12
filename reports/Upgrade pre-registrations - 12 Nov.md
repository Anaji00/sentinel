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
