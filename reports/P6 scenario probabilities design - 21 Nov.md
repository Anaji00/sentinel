# P6: scenario probabilities, design and registration (21 Nov 2026)

Status: design only, nothing built. Measurements are read-only (`scripts/measure_scenario_probabilities.py`, `scripts/measure_scenario_confidence_auc.py`).

## What the data says

90 days, 1,932 scenarios (1,917 with two or more hypotheses); outcomes: 357 confirmed, 1,293 denied, 282 still open (22% of resolved scenarios confirmed).

- The model-written probabilities are not one template: 554 distinct triples in 1,917 scenarios. The most common are 45/35/20 (8%), 43/33/24 (6%), 55/30/15 (4%), 64/27/9 (3%), 70/25/5 (3%); the top eight cover about 30%. The earlier "54% on 55/30/15" does not hold over 90 days (it may have been a shorter window); the remaining finding is order bias: hypothesis 1 is the leader in 74% of scenarios.
- **There is no valid calibration record for the initial probabilities.** The tracker's Brier record (Redis `sentinel:scenario_calibration:*`) holds 15 scored scenarios, Brier 0.049, and every scenario with a leading probability of 70% or more was realised (82 of 82 in those buckets): the realised hypothesis is defined by watch hits, and the same watch hits moved the probabilities before scoring, so the record is circular. For the same reason the first entry in `confidence_history` is not an initial value (it is written after the first tracker pass): its AUC for confirmed against denied is 0.90, which measures the tracker's evidence update, not the generator's judgment.
- So the claim that the probabilities carry no information is neither proven nor refuted. They have never been graded at the moment they were written. Replacing them blind would be a guess.

## Where the numbers are used (what a change touches)

1. `_supported_confidence` (scenario_generator.py:791): the lead margin between the top two probabilities caps overall confidence at `UNSEPARATED_CEILING` when under `CLEAR_LEAD_MARGIN`. This gates the number every reader sees.
2. `_draft_needs_critique` (line 331): the same margin triggers a second model call.
3. The tracker's Bayesian update uses the probabilities as the prior (`DynamicBayesianCalibrator.recalibrate_hypotheses`).
4. Narrative text ("Leading hypothesis: X (N%)", main.py:125-128) and the dashboard.
5. `_record_hypothesis_outcome` (Brier, buckets).

## Design

**Phase 0, additive, no behaviour change (build first).** At creation, store the generator's initial state on the scenario row: `initial_state jsonb` = `{probabilities: [..], confidence_overall, hypothesis_order, rule_family, packet: {family_precision, n_family, correlation_tier}}`. One numbered migration in `shared/db/migrate.py`; written in `_to_scenario`/persist path; failure counted with `swallowed`. Scenarios run about 21 a day, so 200 resolved scenarios with an initial state take about 10 days of creation plus the resolution lag.

**Phase 1, grading (registered now, run when n >= 200 resolved with initial_state, no earlier peek).** Outcome: scenario `confirmed` against `denied` (not the watch-hit winner, which is circular), one observation per scenario, dates clustered (mean within-day AUC and Brier with a bootstrap over days, 2,000 resamples).
- M1: AUC of initial `confidence_overall` for confirmed vs denied.
- M2: Brier of the initial lead probability as a forecast of confirmation, against the base rate (22%) and against a family prior (Beta-shrunk family confirmation rate, prior strength 20, from scenarios before the test date only, no look-ahead).
- M3: does the model's *ordering* matter: confirmation rate of the hypothesis ranked first versus the others, where hypothesis ranking is judged by the watch-hit winner among scenarios with exactly one clean winner, flagged as circular and reported only.

**Phase 2, decision rule (registered now).** Switch the generator to emit mechanism, falsifiers and an *ordering* only, with initial probabilities taken from the packet prior, if and only if: M2 shows the family prior's Brier is lower than the model's initial Brier by at least 0.01 with the bootstrap interval excluding zero, AND M1's AUC for the model's initial confidence is below 0.60 or its interval includes 0.5. If M1 is 0.60 or higher with the interval excluding 0.5, the model's confidence carries information: keep it and only fix the order bias (randomise hypothesis order in the prompt, since hypothesis 1 leads in 74%). A tie keeps today's behaviour.

If the switch is made: probabilities = lead share `p` from the family prior, clamped to [0.15, 0.70], remaining mass split 65/35 by the model's second and third ranks; the output-token saving is the probability field plus its rationale. Behind `SCENARIO_PROBS_FROM_PACKET` (default off), with a replay before enabling: of the stored scenarios of the last 90 days, how many would change their confidence ceiling (`_supported_confidence`) or their critique trigger. Registered limit: no more than 15% change ceiling class; more than that needs a separate decision, because it moves the confidence every reader sees.

**Inference accounting (CLAUDE.md).** No new model call. Trigger, dedupe and cap are the scenario generator's existing ones; the critique trigger changes only if the margin rule is changed, which Phase 2 does not do. Consumers: the scenario tracker's prior, the narrative text, the dashboard.

## What this does not decide

Whether to keep showing a probability to readers at all while Phase 1 is pending; today the dashboard shows the model's figure with the tracker's updates. A cheaper interim option is to show confidence only and hide the per-hypothesis percentages: a frontend decision, not built.

## Phase 0 built (21 Nov 2026, about 03:35 UTC)

Migration `0101_scenario_initial_state` (nullable `scenarios.initial_state jsonb`) applied; `services/reasoning/main.py::_save_scenario` writes `shared/utils/scenario_initial_state.build_initial_state(...)` (probabilities and labels in order, `confidence_overall`, `correlation_id`) on insert; the tracker never updates the column (tested). The rule family and tier are joined from `correlation_id` at grading time rather than stored. Reasoning recreated after the migration, healthy, no tracebacks. Scenarios created before this have null state and are not gradable for M1 and M2. Count with `scripts/check_initial_state.py`; Phase 1 runs at 200 resolved scenarios with state, not before.
