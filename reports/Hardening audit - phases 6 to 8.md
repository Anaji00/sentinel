# Hardening audit, phases 6 to 8 (11 November 2026)

The phases are numbered as in the pasted brief: 6 sympathy spillover and ML hygiene, 7 multi-agent consensus, 8 alpha decay and portfolio optimisation. The specifications for the phases the first message called 4, 5 and 9 (enrichment and entity resolution, scenarios and pattern library, database and frontend verification) were not supplied, so they are not started.

## Baseline

All named test files exist. Before any change: Phase 6 234 passed, Phase 7 257, Phase 8 231.

## Phase 6: sympathy and ML hygiene

| Requirement | Finding | Action |
|---|---|---|
| Sympathy signal when a customer or peer has a 2-sigma shock, weighted by revenue dependency, gated by what the target already did | **Not built, on evidence.** The connection tests (reports/Intelligence audit.md) found no peer or customer link that carries a shock forward: next-day and 5-day drift after a 4% company-specific move on its own news was −6 and −36 bps for sympathy peers (t −0.7, −1.7), +4 and +11 for text peers, and customer links were negative or nil. Weighting by revenue share does not help: the share does not predict co-movement (Spearman −0.13) | None. A trigger would raise signals the data says carry nothing |
| Train on industry- and beta-neutral excess returns, purged and embargoed | The news model already trains on returns net of SPY with a one-day embargo; the embedding refit is walk-forward with an embargoed month. Training the stock model on beta-adjusted or industry-relative targets was trial O2 and was rejected (it lowered the Sharpe and the IC) | None. A neutral target for the news models would be a new trial to register, not a hardening |
| Block a refit from overwriting the champion unless the challenger improves | **A real gap.** `news_model_job.run_once` stored every walk-forward report in the live key, so one failing week replaced a validated model with an unvalidated report and switched live scoring off. `news_model_refit` replaced the weights whenever the new fit cleared an absolute bar (rank IC 0.02, t 2), however far below the incumbent | `shared/utils/champion_gate.py`: a challenger replaces the champion only when validated and its rank IC is at least the champion's less one standard error of the champion's own estimate. Both refits use it; a held-over challenger is recorded beside the champion. 6 tests in `tests/test_champion_gate.py`. Brier skill is not used: these models forecast returns, not probabilities |
| Industry-neutral grading and base-rate subtraction | Already present: `edge_validator` subtracts the unconditional base rate (`_base_rate`, a binomial tail against it) and the industry-neutral shadow test passes | None |
| Quarantine any family or rule whose rolling 30-day excess IC is 0 or below | **Not built as specified.** A 30-day mean IC at or below zero has a 50% chance for a rule with no skill and about 14% for one with a true IC of 0.02 (daily IC spread 0.10), so the rule would quarantine good rules about one window in seven and every rule eventually | Needs a significance bar, not a sign: one-sided t below about −1.6 over independent periods, or the CUSUM the platform already uses for B1 drift. The existing probation grades families and retires them on a retired grade. Proposed, not built |

Note: `shared/utils/vendor_shares.py` named in the brief does not exist; the file is `services/collector-filings/vendor_shares.py`.

## Phase 7: consensus and providers

| Requirement | Finding | Action |
|---|---|---|
| Discount the vote by effective sample size, using measured pairwise error correlation | The engine already fuses dependent opinions with Josang averaging, so eight copies of one opinion stay one (measured: uncertainty 0.30 to 0.05 under the cumulative rule, unchanged under averaging). Measuring the pairwise error correlation needs graded agent forecasts; none can yet be graded (about 3.6 independent weeks of the 20 needed) | None now. When grading allows it, N_eff = N / (1 + (N−1)·rho) can replace the averaging; with no estimate it must leave weights as they are |
| Agreement gate C = abs(mean c)·(1 − var c) at or above a threshold, else NO_TRADE | The engine already emits competing hypotheses instead of a consensus when fused uncertainty or conflict is high (`ACH_UNCERTAINTY_THRESHOLD`, conflict above 0.3). A second gate with a new threshold would be a number with nothing to calibrate it against | None |
| Route Analyst, Researcher and Wargamer across Groq and Gemini | Today all three run on local models of one family, which is the homogeneity the brief names. The hosted shadow analyst runs on Groq. Moving the wargamer or researcher to a hosted model changes the series each producer is graded on and adds an inference trigger that needs a stated cap | **Decision needed** (below) |
| Role-isolated context | The financial-context and agent-scope tests pass; context is built per consumer already | None |

## Phase 8: decay, shrinkage, optimisation

| Requirement | Finding | Action |
|---|---|---|
| Covariance shrinkage | **A real gap.** The Black-Litterman advisory used `np.cov` on about a dozen names over a few weeks, near singular, then inverted it | `quant_calc.shrunk_covariance`: Ledoit-Wolf toward a scaled identity, intensity estimated from the data (more observations, less shrinkage), plain numpy so no dependency. The advisory uses it and falls back to the sample covariance with a counted notice. Test shows a 12-by-8 case going from singular to well-conditioned and 5,000 observations leaving it almost unchanged |
| View uncertainty from the producer's Brier error and the scenario variance | Today it is built from the agent's own stated conviction. A Brier-based version needs calibration history that does not exist yet | Not built: it would be a formula with nothing to feed it |
| Turnover deadband priced by trading costs | Deadbands and rank buffers exist in B1; changing the trader's churn rule is sizing and execution logic, which needs your approval, and the earlier drift-band test saved only 0.2 to 0.3 points a year | Not changed |
| Decaying signal alpha blended with scenario expected return | A new model with several parameters to fit; the signals' horizons are 30 days and most cells cannot be rated by half-year | Not built. RSI minimum windows are already tested (`test_rsi_minimum_window.py`) |
| Simple returns throughout | Already done for books and the lab (`test_books_score_simple_returns.py`, `test_lab_simple_returns.py`) | None |

## Gate

Phase 6: 234 passed; Phase 7: 257 passed; Phase 8: 232 passed (231 plus the shrinkage test); the new champion-gate tests: 6 passed. Adjacent suites (`-k "news or quant or black_litterman or covariance or correlation or portfolio or model"`): 629 passed, 6 skipped. Lint clean on every changed file; the two older warnings in `quant_calc.py` are unchanged from `HEAD`.

## Not deployed

The champion gate loads when `correlation` (news model job) and `factor-model` (embedding refit) restart; `factor-model` is due to be recreated after today's trader run. The shrinkage loads when `agents-fast` and `agents-heavy` restart.

## Decisions needed

1. Recreate `correlation`, `agents-fast` and `agents-heavy` now to load the champion gate and the shrinkage.
2. Move the wargamer (or researcher) to a hosted model for provider diversity: it resets that producer's grading series and needs a stated per-day cap. Which, if either?
3. Replace the proposed 30-day sign rule with a significance-based quarantine (t below about −1.6 over independent periods), built on the existing probation grading?
