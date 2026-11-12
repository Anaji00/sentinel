# Research-backed fix plan (12 Nov 2026)

Each finding from the 12 Nov audit, what the research says, what was done, and what is next. "Done" means built, tested and committed;
"live" is noted where the service has been restarted. Where a change would shift alert volume it waits for a registered test
(`Upgrade pre-registrations - 12 Nov.md`). Free data only; SEC requests go through `sec_slot()`.

## Done today

| Finding | Evidence | Research | Fix |
|---|---|---|---|
| STS score was a constant 0.780 | 4,073 events, 1 value | Detection rules in practice: Skylight uses 250 m, 30 minutes, under 4 knots, 10 km from coast ([Skylight](https://support.skylight.global/en_US/standard-rendezvous)); a Kerch Strait study used 500 m, 2 hours, under 1 knot ([SynMax](https://synmax.com/maritime/how-to-detect-ship-to-ship-transfers/)). No source sets one threshold, so the score grades the evidence. | `sts_score` rises with dwell, closeness, speed, sanctions, region; ordinary case stays near 0.78. Live. |
| Scenario probabilities templated | 55/30/15, 45/30/25, 45/35/20 = 52%; prompt example showed 45 and 62 | LLM verbalised probabilities are often poorly calibrated, and base rates or aggregation help ([arXiv 2410.06707](https://arxiv.org/pdf/2410.06707), [FOReCAst](https://arxiv.org/pdf/2502.19676v4)). | Numeric anchors removed; prompt tells the model to derive each from the cited signals. Live. Grading scenarios against outcomes is next. |
| Liquidation bursts capped at 0.900 | 59% of bursts on one value | Size relative to open interest and to a rolling baseline is the common measure; thresholds in practice are hand-picked ([Santiment](https://academy.santiment.net/data-anomaly/liquidations/), [arXiv 2607.27070](https://arxiv.org/pdf/2607.27070)). | Score keeps the 3x/5x/7x anchors and rises to 0.97 above 7x; no event crosses a cut it did not already cross. Live at enrichment. |
| Earnings reactions capped at 1.000 | 42% exactly 1.0 | PEAD and surprise measures are graded by magnitude against a baseline ([Philadelphia Fed](https://www.philadelphiafed.org/-/media/FRBP/Assets/working-papers/2021/wp21-07.pdf), [QuantConnect SUE](https://www.quantconnect.com/tutorials/strategy-library/standardized-unexpected-earnings)). | `1 - 5^(-|move|/8%)`: 0.8 at 8% as before, 0.96 at 16%, 0.99 at 24%. Built; takes effect at the factor-model restart. |
| Graph writes dropped, leases overran | 16 lock timeouts, 10 overruns | A lease that can lapse during the work it guards needs renewal, and the guarded write should tolerate a late holder ([Kleppmann](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html)). The existing lock already used tokens and compare-and-delete. | Atomic renewal every 5 s while a batch runs. Live. Resource-side fencing is not needed while the graph writes are MERGE (idempotent). |
| 38-second event-loop stall | loop watch | Imports are synchronous. | Heavy imports moved into the executor with the model load. Live. |
| 13D/13G rows had no filing date | 125 of 600 filings | EDGAR's atom feed lists each filing with an `updated` time ([SEC EDGAR APIs](https://www.sec.gov/page/sec-api-documentation)). | Producer stores date and acceptance time; route falls back and returns a plain summary. Live. |

## Next, in the order I would do them

1. **Blocked-loop sampler results (phase 6).** The sampler names the exact line; it started on agents-fast and agents-heavy at 18:04 UTC.
   Move whatever holds the loop over 50 ms more than once an hour into a thread, with a test that output is identical.
2. **Time-of-day baselines for market anomalies (phase 3, registered).** Volume follows a U-shaped daily profile; the standard fix is to
   divide by the expected value for the same time slot, and the profile drifts, so it is re-estimated ([arXiv 2505.08180](https://arxiv.org/html/2505.08180v1),
   [econpapers 1810.12099](https://econpapers.repec.org/paper/arxpapers/1810.12099.htm)). Shadow first, three fixed bars.
3. **Anomaly calibration check.** Conformal p-values are uniform for normal data only if scores are exchangeable, and autocorrelated series
   break that ([arXiv 1909.06927](https://arxiv.org/pdf/1909.06927), [nonconform](https://pypi.org/project/nonconform/0.98.76/)). Measure whether
   each domain's scores are uniform over a week; the types with a third of events above 0.8 (filings, equity blocks) are the first suspects.
4. **Sentiment on world headlines.** The Loughran-McDonald word list scored 50.1% accuracy on 965,375 news articles against 72% for FinBERT
   and 74% for a large model ([arXiv 2412.19245](https://arxiv.org/pdf/2412.19245)); our lexicon agreed with FinBERT 26% of the time. Run FinBERT
   on world headlines in shadow and compare against the sector and country signals they feed. GDELT's average tone is a free lexicon-based
   input with mixed out-of-sample evidence ([arXiv 2009.14281](https://arxiv.org/pdf/2009.14281.pdf)); supplementary only.
5. **Correlation discovery on daily returns, market-neutral.** At one-day horizons almost no pairwise relationships survive FDR control over
   a long sample, because the market factor swamps them ([arXiv 1006.1791](https://arxiv.org/pdf/1006.1791)); factor-adjusted residual
   tests and dependence-aware FDR are the better design ([HKUST](https://researchportal.hkust.edu.hk/en/publications/factor-adjusted-multiple-testing-of-correlations/),
   [arXiv 2102.07826](https://arxiv.org/pdf/2102.07826)). Build a daily lane on residuals against SPY and the sector ETF, 250 sessions, for the
   names with daily bars (CVX, GD, LMT, OXY, RTX included). The current 10-hour intraday window stays.
6. **Supply-chain contagion (phase 3).** Customer momentum was found by Cohen and Frazzini (2008); later work finds it partly a small-large
   stock lead-lag and not significant when value-weighted ([arXiv 2301.11394](https://arxiv.org/pdf/2301.11394), [NBER w25201](https://www.nber.org/system/files/working_papers/w25201/w25201.pdf)).
   Test it value-weighted, in shadow, before it emits a signal.
7. **Earnings surprise from free data.** SEC XBRL `companyfacts` and `frames` give reported EPS with no key; consensus is not free, so the
   expectation is the seasonal random walk (EPS minus the same quarter last year), which draws less drift than analyst-based surprise
   (Livnat and Mendenhall, 2006). Add it as a second score beside the return-based one; PEAD has also weakened over time, so test before using it.
8. **Producers that precede smaller moves.** Crypto Venue Stress (0.61), Insider Sale Into Options Accumulation (0.52), Geopolitical Cascade
   (Msft) (0.43). Decide cut or invert once each has 100 graded alerts.
9. **Quant engine confidence.** 4 distinct values in 80 predictions; derive it from the edge and its sample size.
10. **Scenario gaps.** 9% no watch signal, 7% no deny signal, 11% no beneficiary: reject and re-ask when the schema is met but those are empty.

## Sources worth scanning, all free

SEC EDGAR APIs (companyfacts, frames, submissions; no key). GDELT GKG 15-minute files for tone. Alpaca news and bars (already used).
Binance funding and open interest (already attached to liquidation events). 10-K customer-concentration text for supply-chain links
(already parsed in `customer_links`). Loughran-McDonald word list as a baseline only.

## Operational finding

The host was asleep from 03:10 to 17:50 UTC on 12 Nov, so the containers stopped: the nightly model run froze mid-way, the 14:43 trader
run did not happen, and the first attempt after wake-up failed on name resolution (it retried five minutes later and placed 101 orders).
The first live risk-off vetoes (5 names) and alert-weight scaling (60 of 179 names) fired in that run. A keep-awake setting would prevent a repeat.
