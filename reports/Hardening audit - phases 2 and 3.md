# Hardening audit, phases 2 and 3 (11 November 2026)

Cyber is out of scope (the user's instruction): the cyber bullets and the two cyber tests (`test_bgp_origin_detection`, `test_filing_and_ransomware_scores_vary`) were not run or changed.

## Baseline

All 51 Phase 2 test files and all 49 Phase 3 files named exist. Before any change: 357 and 494 tests passed. The requirements were then read against the code, because a green suite does not show that a requirement is met.

## What was already met (no change)

| Requirement | Where it is met |
|---|---|
| Open-market purchases separate from exercises, grants, withholding | `form4.py`: only codes P and S count (`OPEN_MARKET_CODES`); planned 10b5-1 trades are flagged from the checkbox or the footnote |
| Closed-market quotes are not bars; minute-bar race; daily finality; regular-hours volume | The eight bar tests pass; the stored-bar and corporate-action reload loops exist |
| EPS surprise denominator floor | Present and tested (`test_eps_surprise_denominator.py`) |
| IV solver within no-arbitrage bounds, bracketed | `implied_vol` is a bisection (monotone in volatility, needs no vega) and returns None outside the bounds; no Brent fallback is needed |
| Aggressor sign by the midpoint quote rule; fill times; split-print dedup | `options_flow.aggressor` (Lee and Ready), `QuoteTape`, and the dedup and fill-time tests pass |
| Missing tenors, duplicate rates, dedup of macro quotes | The rate and macro tests pass |
| Dark ships only inside reception coverage | `test_dark_means_inside_coverage`, `test_ais_zone_coverage` pass |
| Issuer-size gate for contracts and legal awards | `test_market_cap_gate`, `test_movers_market_cap_gate` pass |

## Defects found and fixed

| # | Layer | Defect | Fix | Test |
|---|---|---|---|---|
| 1 | 13F ownership | **A split read as institutional accumulation.** 13F shares are as held at the quarter end, unadjusted; a 2-for-1 split shows every holder adding 100%, and the aggregate then published an accumulation event | `aggregate_changes(..., split_tickers=)` leaves out adds and cuts for tickers with a split between the two quarter ends. Split dates come from the broker's corporate-actions feed, which the tradfi collector already reads; it now keeps them (`sentinel:corporate_actions:splits`). Live and history paths both pass them. A first version inferred splits from identical ratios; two existing tests showed that is wrong for realistic data (three managers each adding 50% is plausible), so it was removed | `test_13f_splits.py` (6) |
| 2 | Sanctions | **A near spelling raised the tier.** A fuzzy name match set `sanctioned_ofac`, the flag the tier reads | A fuzzy match is recorded as `sanctioned_fuzzy` ("similar") and does not set it; an MMSI or IMO match or an exact listed name does | one test added to `test_sanctions_matching.py` |
| 3 | Crypto collector | Five handlers logged an error and moved on: no traceback, no counter | Routed through `swallowed(...)` | existing crypto and quiet-failure tests |
| 4 | Prediction markets | No helper to de-vig a one-winner field or to shrink a thin contract to the prior | `devig()` and `shrink_to_prior()` (pure). Not wired into the enricher: the collector cannot yet say which fields have a single winner, and scaling independent contracts would invent a constraint | `test_multi_outcome_predictions.py` |

## Added, descriptive only (not wired into any signal)

| Item | What it is | Why it is not wired |
|---|---|---|
| Dealer gamma exposure and gamma flip | `shared/utils/gex.py`; computed in the Cboe chain summary and kept for SPY, QQQ and IWM in `sentinel:gex:history:{ticker}` (450-day expiry) | The sign convention (customers long calls, short puts) is an assumption open interest cannot confirm, and there is no history yet. **Registered:** after 60 sessions, does the sign of net GEX predict SPY's next-day realised volatility? Pass: a difference with t of 2 or more. Until then the equity-regime classifier does not read it |
| Implied tail probabilities | `option_ev.tail_probabilities()`: P(up) and P(down) for a move of a given size from the ATM level | Lognormal, so it carries no skew; useful as a floor for scenario priors. Not added to the scenario prompt, which would cost free-tier tokens for an unmeasured gain |

## Brief items not built, with reasons

| Item | Reason |
|---|---|
| Short-interest convexity multiplier on positive catalysts, squeeze penalty on shorts, in `signal_engine` and `scenario_generator` | There is no verified positive catalyst to multiply: no buy-side cell is proven (reports/Buy signals.md). For shorts, the registered B1 leg trial dropped the most crowded fifth by days to cover and gained +0.05 Sharpe, under the +0.10 bar. Percent of float needs a shares-outstanding column the table lacks. **Registered:** do high days-to-cover names lose more among the proven short cells (dilution 30d, delisting 30d, insider sell 30d)? Run before any penalty is added |
| 2s10s slope and real-rate velocity into `factor_engine.py` | They are the same number for every stock, so a model that ranks stocks across a day cannot use them as a feature; the earlier regime interactions were removed in trials P1 to P3 for this kind of reason. They can condition the regime gauge, which the lab shows no longer helps (0.66 to 0.63) |
| Crypto liquidation stress into `equity_regime.py` | A new regime input with no history to test it on. Same registration as GEX, with crypto liquidations as the predictor |
| Maritime dwell time against seasonal baselines | A new analytic, not a hardening; needs a year of baseline the AIS history does not yet hold |

## Gate

Phase 2 files: 358 passed. Phase 3 files (cyber left out): 496 passed. New tests: `test_13f_splits`, `test_gex`, plus additions to the option, sanctions and prediction files. Adjacent suites (`-k "ownership or sanction or option or corporate or crypto or prediction or tradfi or enrichment or edgar or filing or collector"`): 672 passed, 15 skipped. `ruff --select F,E9` is clean on every changed file except one older F401 in `collector-crypto/main.py`, unchanged from `HEAD`. The edited services compile.

## Deployed and cleaned up (11 Nov, after approval)

`collector-tradfi`, `collector-filings`, `collector-crypto`, `enrichment`, `agents-fast`, `reasoning` and `agents-heavy` were recreated and came up healthy; `factor-model` waits for today's trader run. `scripts/backfill_split_dates.py` loaded 6,918 split dates on 5,210 tickers since 2016 into `sentinel:corporate_actions:splits`.

Of 2,025 archived 13F flow events, **123 were on a ticker that split in that quarter** and were deleted with `scripts/remove_split_quarter_flows.py` logic (a copy of the rows is saved). The archive is compressed, and TimescaleDB refuses to decompress more than 100,000 rows per DML transaction, so the first attempt removed 5 rows and stopped; the rest went in one transaction with that limit lifted for the transaction only (`SET LOCAL timescaledb.max_tuples_decompressed_per_dml_transaction = 0`). 1,902 flow events remain and none sits on a split quarter. The chunks that were decompressed are recompressed by the existing policy.

Still open: 25 flow rows are duplicates (the same ticker and quarter archived by both the live and history paths); the study merges overlapping episodes, so the harm is small, but the history path should skip a quarter the live path has published.

## Decisions needed

1. Recreate `collector-tradfi` and `collector-filings` (split dates, gamma history, crypto counters), together with the Phase 1 services?
2. Run the split-date backfill since 2016 (free, read-only calls to the broker's feed; writes one Redis hash) and rebuild the archived 13F flows that used unadjusted shares?
3. Run the two registered tests (days to cover on the short cells; GEX after 60 sessions)?
