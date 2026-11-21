# Feature ablation (C3) and price-target scoring (B1) trials, run 14 Nov 2026 06:00 UTC

Registered in `Upgrade pre-registrations - 12 Nov.md` (area C item C3, area B item B1); bars unchanged. Code: `shared/utils/feature_ablation.py`, `tests/test_feature_ablation.py` (24 pass), `scripts/run_feature_trials.py` (`--selftest` passes; ruff F,E9 clean). Run once in `sentinel-feature-trials` (6 GiB, after the 06:00 UTC end of the nightly window; memory check passed). Raw results: `reports/feature_trials/feature_trials.json` (taken from the run's stdout, because the `/out` bind mount did not receive the file) and `run.log`. Nothing live changed, nothing committed.

## The finding that decides both tests: neither feature is a model input

`factor_model.FEATURES` (the monthly `lgbm_rank` feature list, which `design()` reads) contains neither `llm_news_30` nor `ev_comment_letter`. Nothing in `factor_engine` or `factor_model` reads them into the panel. They are written to `stock_features_daily` for the weekly researcher screen and for pre-registered trials, and have never been adopted (`stock_features.py` docstring: "only through a pre-registered trial until one is adopted"). The registrations' premise ("removed from the monthly feature set") is therefore false for the live model. A test asserts this, so it will fail loudly if one is ever adopted.

Stored history: `llm_news_30` has 16 days (29 Oct to 14 Nov 2026, 44,392 rows, forward-only by design); `ev_comment_letter` has 660 rows over 397 tickers, 6 Nov 2023 to 11 Jun 2026 (nothing since June). The walk-forward needs 260+ sessions before its first score and years of training rows, so `llm_news_30` could not be a measurable model column even as an add-in.

## C3: ablation. Literal bar result: calls "can stop"; evidential value: none

| Run | Feature in model? | Paired Sharpe diff (ablated minus full), calendars 0,5,10,15,2,7,12,17 | Calendars with a cost of 0.03 or more | Mean IC with / without | p |
|---|---|---|---|---|---|
| 1: remove `llm_news_30` | no | 0.000 on all 8 (identical model) | 0 of 8 | equal | not defined (no difference) |
| 2: remove `ev_comment_letter` | no | 0.000 on all 8 (identical model) | 0 of 8 | equal | not defined |

I did not run the 5-hour walk-forward: removing a column the model does not have returns the same model, and `ablate()` raises for such a column. The paired walk-forward path exists and passes a synthetic selftest (removing a planted signal costs Sharpe; removing nothing gives exactly 0), for use if a feature is ever adopted.

**Verdict (bar as written): FAIL to show value, so "the calls can stop" (saves 45% and 13% of hosted tokens), but this is vacuous, not a measurement.** What it does show: today the two biggest hosted spenders (58% of 7-day hosted tokens) buy nothing for the live book. What it cannot show: whether the features would help a model. A real test of that would be an add-in trial. For `ev_comment_letter` that is feasible on 2023 to 2026 data (not run, a different question from the registered one). For `llm_news_30` it needs months more history; stopping the calls now discards that forward-only accumulation, which is the owner's trade-off.

## B1: deterministic price-target scoring. FAIL (call cut 9.5%, bar 20%)

Regexes (fixed before the run, case-insensitive): `\b(raises|lifts|boosts)\b.*\bprice\s+target`, `\b(lowers|cuts)\b.*\bprice\s+target` (+1 / -1), `\bupgrades\b` (+1), `\bdowngrades\b` (-1). One rule I had to fix for the cases the registration leaves open: a headline whose patterns imply opposite signs keeps the model's score.

(a) Non-matching headlines: the scores are identical in both versions (27,067 of 27,067 checked). Rebuilt version 1 equals the stored `llm_news_30` for 4,883 of 4,906 names on the last day (the rest are headlines scored after the job's daily write).

Agreement of the stored scores (30,455 headlines, gpt-oss-120b, 28 Oct to 14 Nov) with the regex sign:

| Class | n | Agree | Scored 0 | Opposite |
|---|---|---|---|---|
| Raises / Lifts / Boosts price target | 1,595 | 94.0% | 3.6% | 2.4% |
| Lowers / Cuts price target | 1,378 | 92.4% | 6.3% | 1.3% |
| Upgrades | 178 | 88.2% | 7.9% | 3.9% |
| Downgrades | 237 | 97.9% | 2.1% | 0.0% |

The registration's "59% of Raises scored 0" does not hold for this model and window: 3.6% do. The regexes match 11.1% of scored headlines, not 23% (the 23% evidently counted price-target notices the regexes exclude, such as maintains or reiterates).

Effect on the feature: version 2 changes the value for 2,087 of 52,619 stock-days (4.0%); mean daily rank correlation between versions 0.993.

Call cut (job admission rule: single-symbol, ticker pattern, market cap of at least $1bn, 2,327 names; minus the stale skip): 25,551 headlines in the window, 6,822 stale, **18,729 would be scored; 1,773 match = 9.5%** (last 7 days: 445 of 5,444 = 8.2%). Calls go in batches of 20, so token saving is about the same share.

(b) Walk-forward Sharpe by feature version: not measurable (feature is not a model input; 18 days of history against 260 needed). The Sharpe difference is 0.000 on all 8 calendars by construction, so the Sharpe half of the bar is met vacuously.

**Verdict: FAIL.** Adopt only if the Sharpe condition holds AND calls fall at least 20%; they fall 9.5%.

## What each means for the live system

- Nothing to change in the live model from either test. The rule would save about 9% of llm_news calls (about 4% of all hosted tokens) and agrees with the model on about 94% of the headlines it would replace, so it is a harmless but small saving. The stronger lever is the C3 observation: the two biggest spenders feed no model today.
- Owner decisions: whether to keep paying for features that no adopted model reads, and whether to run an add-in trial of `ev_comment_letter` (data exists) before deciding.
- Limitations: one model and a 17-day window for the agreement rates; the admission share applies today's rule to the window, not what the job scored before 12 Nov.
