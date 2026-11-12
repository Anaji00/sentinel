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


## Follow-up, evening of 12 Nov

**Event-loop measurement, corrected.** asyncio's debug mode records the creation stack of every callback, and it accounted for 509 of 600
seconds in the first run, so the first figure (20% of the time blocked) was mostly the instrument. The watch now uses the stack sampler
alone (debug mode is opt-in). Clean result over 10 minutes: agents-fast held the loop about 1.5 seconds in total, nothing to fix.
agents-heavy about 20 seconds, nearly all in the signal study: database row sanitising on the loop (12.8 s), headline mapping (5.6 s) and
episode building (1.4 s). All three now run in worker threads with identical output (`tests/test_loop_offload.py`).

**Calibration check.** Filings: the score is a materiality class by form and item, not an anomaly. 8-K item 2.02 (earnings results) is 53% of
8-Ks at about 0.82, 424B2, 424B3 and 424B5 are a constant 0.20, Schedule 13G is 0.25 or 0.40 and 13D about 0.85, so a third of filings clear
0.8 by construction. Equity blocks: the score rises with dollar size (mean 0.65, 0.79, 0.90 across three size buckets), so it is a sound
size-aware score; a quarter of the smallest bucket clears 0.8, concentrated in a handful of tickers (NVDA 746 of about 3,000).

**Output gaps are deliberate.** The scenario generator prunes signals it cannot ground in the evidence, so some hypotheses legitimately end
with no watch or deny signal. The quant engine's four confidence values are conviction tiers, not a rounding defect. Neither is changed.

**Daily market-neutral correlation lane (shadow).** `shared/utils/residual_corr.py`: remove the market component, correlate residuals, control
the false-discovery rate across every pair, then require an effect-size floor and split-half stability. On 246 common stocks over 250
sessions: 30,135 pairs tested; 54.6% significant raw, 42.7% after removing the market (the p-value alone is not selective at this sample
size); 1,580 stable pairs (5.2%) after the floor and stability. They are recognisable peer groups: refiners MPC-VLO-PSX, E&Ps
COP-EOG-DVN-OXY-CVX-XOM, semicap AMAT-LRCX-KLAC, defense LMT-RTX-GD, payments MA-V, airlines DAL-UAL, CRWD-PANW, quantum QBTS-RGTI-IONQ.
CVX, GD, LMT, OXY and RTX are now testable. Share-class twins (GOOG-GOOGL) need excluding before any use.

**The live discovery engine publishes market-beta links.** At 18:15 UTC it reported Pearson 200 of 298 pairs significant and published
links such as IONQ-SPY, EWY-SPY, MU-QQQ and ES=F-SPY. A stock's correlation with the index it belongs to is the market factor and says
little about the company, which is what the research on daily pair tests warns about. Next step: publish market-neutral links instead.

**Time-of-day replay (registered test, offline).** 29,125 stored market-anomaly events in 14 days; 9,181 could be scored three ways.
A flat all-day volume baseline fails the registered parity bar badly (bucket ratios 3.09, 0.29, 0.16, 0.16, 1.30). The live score's
top 5% is spread within the bar (ratios 0.57 to 1.39), so its ranking is not badly distorted; the near-zero share above the absolute 0.8
cut at midday is a calibration of the cut, not of the ranking. The same-slot baseline could not be judged: after-hours bars dominated its
top 5%. Result: bar 1 is not the problem it appeared to be. No change made; a better replay ranks within each bucket and needs per-ticker
slot history.

**Operational mistake, mine.** I loaded a second FinBERT into the 2 GiB correlation container for a comparison; the container restarted at
18:19 UTC and came back healthy with no tracebacks. Comparisons that load a model now run in agents-heavy.

**FinBERT against the lexicon on world headlines (400 sampled, shadow).** FinBERT sees a tone in 60.8%, the lexicon in 42.5%. When FinBERT
sees a tone the lexicon agrees 37.9%, calls it neutral 51.4% and is opposite 10.7%. When the lexicon sees a tone FinBERT agrees only 54.1% and
is opposite 15.3%. So on world news the two disagree far more than on company news (where the lexicon was opposite 3 to 4% of the time).
FinBERT is a financial-text model: it reads "US election results could be positive for Lebanon" as +0.86, which is not a market statement,
while the lexicon misses "Webull expands in Europe" and "Tidewater initiates 2027 revenue guidance". Neither is clearly right. No switch.
The only fair judge is the outcome: re-derive the sector and country news cells under each scorer and compare forward returns, which needs
the signal-families study run twice; not done. Until then both stay as they are and the lexicon's zero means "no word matched".


## Phases 2 to 7: what was built (12 Nov evening)

**Phase 3, shadow, no live change.**
- `shared/utils/diurnal.py`: the registered session buckets, a median-and-MAD z that never contains the observation it scores, and a streaming
  same-bucket baseline that scores before it adds. Not wired into the live scorer: the offline replay already showed the live ranking is
  within the parity bar, so the hot path stays as it is.
- Hawkes timestamps (`scripts/measure_hawkes_timestamps.py`): 76% of a week's events carry a stamp within 2 seconds of collection, which is
  simply real time for live streams, so that share cannot trigger the registered 10% rule. A one-second de-duplication was tried and removed:
  crypto genuinely arrives about 4.7 times a second and the existing guard test records it. What is identifiable is a derived, back-filled
  type: earnings_reaction rows (66% stored over an hour after the release, one batch of 422 in a second). They no longer feed the Hawkes fit.
- Contagion (`shared/utils/contagion.py`, replay `scripts/shadow_contagion.py`): damped multi-hop propagation with provenance paths. Replay over
  618 sessions on 219 customer links: 411 second-order episodes, 5-session sign agreement 50.6% against the registered 55% bar; after a
  customer drop 46.2%, after a rise 55.2%; first half 48.8%, second half 52.4%. Bar not met, so it stays in shadow and emits nothing. This
  matches the research that customer momentum is weak and partly a small-versus-large lead-lag.

**Phase 4, live as an annotation only.** `shared/utils/regime_packet.py` builds a numbers-first packet and `classify` gives one of the four
labels by rules, with no tokens; an input that was not measured is never read as "none found". `services/agents/regime_annotation.py` fills the
packet from filings, earnings reactions, insider clusters and short interest, and the divergence watch now adds `regime` to each alert's
metrics and a `regime:<label>` tag. It changes no order. On the last 14 days of divergence alerts (58): 27 unexplained dislocation, 15
confirmed catalyst drift, 8 fundamental impairment trap, 8 crowded or no edge; every input was measured. NVDA, the original case, abstains
(41 articles and no filing or surprise to read them against); MCHP and QXO (both vetoed by the trader today) read as impairment trap and drift.
No model explanation calls were added; the grading of each label on its 5-session excess return starts from the first labelled alert.

**Phase 5, simulator built, run pending.** `shared/utils/b1_variants.py` simulates the five variants on daily paths from the walk-forward's
collected positions (accrual half-life, 3-sigma adverse exit, shrinkage min-variance weights, trailing stop; exits cost 5 bps and leave cash
idle, every variant pays the same 44 bps short fee); `scripts/run_b1_variants.py` applies the registered bar (beat V0 on 6 of 8 calendars and
by 0.10 on average). It needs the whole panel, so it runs in its own capped container after 06:00 UTC, not during the nightly model run.

**Phase 2, module built, shadow run in progress.** `shared/utils/hier_bayes.py`: three levels (zero, family at a horizon, size band) with
method-of-moments prior strengths, floored so a null world is not reported as certain, a cell never surer than its parent, effective N from the
brief's formula, and the registered release rule (posterior probability 0.95, effective N 30, false-discovery step). `scripts/shadow_cap_cells.py`
re-reads the study's archive and splits every cell by point-in-time size band (daily dollar volume against the four size benchmarks).
The gate is untouched.

**Phase 7.** Frontend: `tsc --noEmit` clean, 70 files and 449 tests pass. `check_constructor_arity`: 2 calls in collector-crypto omit
`on_connect` (introduced in 1.1.5 and 1.1.7, not by today's work). `check_reachability`: the 4 known entry points that no service runs
(supervisor, debug_check, deep_history_backfill, ofac_sync), matching the guard test's allowance.

**Phase 2 size-band study, first run (episodes treated as independent) and its correction.** 423 cells (family x horizon x size band) with 10
or more episodes. All five tradable cells are positive after shrinkage in every size band: 8-K delisting notice 30d +547 to +1,760 bps,
8-K dilution 30d +705 to +1,091 bps, insider opportunistic sell 30d +24 to +151 bps (the mid band is the weakest: +24 bps, probability of a
positive net return 0.944, just under the 0.95 bar). The first run found 79 release candidates, but it treated each episode as independent,
so probabilities came out at 1.000 and effective N in the thousands: overlapping episodes (one filing wave, one earnings week) are not
independent evidence. Rerun with each family-horizon-band-week collapsed to one unit, as the live study's independent units do; its result
replaces the 79 and is the only one that counts. No gate change either way.

**Phase 2 size-band study, corrected result (weekly units).** 302 cells with 10 or more weekly units; the 79 release candidates of the first run
fall to **48** once overlapping episodes are collapsed. The five tradable cells: 8-K delisting notice 30d is positive in every band (+313 to
+1,538 bps, probability 1.000) and 8-K dilution 30d too (+615 to +886 bps). **Insider opportunistic sell 30d is fragile**: large +72 bps
(probability 0.970), small +99 bps (0.995), micro +223 bps (1.000), but mid only +36 bps with probability 0.828, so it is not a candidate
there. Candidates outside the five that sit in the bands the liquidity gate allows (large and mid) are 8-K auditor change 30d (+362 and
+372 bps), earnings reaction down 30d (+201 and +203 bps), earnings fade large 30d (+150 bps), stock news negative 30d mid (+239 bps),
8-K dilution 5d (+157 and +156 bps) and 8-K delisting notice 5d (+177 and +191 bps). These are mostly short signals, so each still needs
the instrument's borrow and liquidity check; the micro and small bands fail the $140.8M-a-day liquidity gate whatever their posterior. The
results are before borrow fees. No gate change: the rule makes them candidates for the next gate review, which needs your approval.
