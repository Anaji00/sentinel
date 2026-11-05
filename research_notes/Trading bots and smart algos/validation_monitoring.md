# Validating and Monitoring a Monthly Walk-Forward LightGBM Trading Model

Reading depth. Read in full text: Wiecki et al., Bailey and Lopez de Prado (DSR), Harvey and Liu (Backtesting), in part (targeted passages). Abstract or summary only: Jensen et al., Chen and Zimmermann, Arian et al., McLean and Pontiff, Falck et al., Hoffstein et al., Zarattini and Pagani. Numbers described as "my calculation" were computed for these notes and are not from a source. Sentinel figures come from `MODEL_TRIALS.md` and `services/collector-tradfi/main.py`.

## Combinatorial purged cross-validation, purging and embargo: evidence, and comparison with walk-forward

### Takeaway
Purging and embargo are uncontroversial leakage controls, and Sentinel already uses them. CPCV's claimed superiority over walk-forward rests on thin evidence, mostly one synthetic-data study, so use it as a selection-stage robustness check beside walk-forward, not as a replacement.

### Cited Findings
- Purging removes training observations whose label windows overlap the test set; embargo adds a time buffer after the test set. Both were popularized by Lopez de Prado's *Advances in Financial Machine Learning* (2018) — [Wikipedia, secondary](https://en.wikipedia.org/wiki/Purged_cross-validation)
- Walk-forward yields one chronological path that may hinge on one regime sequence; CPCV gives multiple paths by using more than one fold as the test set — [LuxAlgo, secondary](https://www.luxalgo.com/library/concept/purged-cross-validation/); [Return on Effort, practitioner](https://returnoneffort.beehiiv.com/p/mitigating-overfitting-advanced-crossvalidation)
- Arian, Norouzi Mobarekeh and Seco (*Knowledge-Based Systems*, 2024) report that CPCV beats K-Fold, Purged K-Fold and especially walk-forward, with lower PBO and a better DSR statistic, on synthetic data with an S&P 500 check. They also note how little research critically evaluates these methods — [preprint copy](https://www.scribd.com/document/725401650/SSRN-id4778909); [summary](https://bohrium.dp.tech/paper/arxiv/324dfd175c19894198c6e870b5d6427eec1a3ff441baa75b0dbd5e856e73e5f8)
- A search for independent critiques or replications of CPCV found none of substance. Most available material is secondary explainer content. Absence from one search is weak evidence, not proof that none exist.
- Sentinel's walk-forward already uses purged folds with a 21-day embargo — `MODEL_TRIALS.md` line 11 (local repo).

### Inferences
- CPCV trains on data later than the test block. That is fine for estimating whether a stationary signal exists, but it is not a simulation of what could have been traded, and it mixes future regimes into training. With decay evidence (see below), this could flatter a model. Walk-forward should remain the performance record; CPCV should answer "how fragile is this configuration?"
- The cost is real here: CPCV multiplies model fits. With N groups and k test groups there are C(N,k) splits; for example, C(10,2) = 45 fits per configuration. Run it only for finalists.
- The 1.01 to 0.61 swing is not a walk-forward-versus-CPCV problem at root. It is a path-dependence problem, which offsets and tranching address far more cheaply (final section). Do those first.

### Gaps
- No real-data, independently replicated head-to-head of CPCV versus walk-forward on monthly cross-sectional equity rankers was found. The Arian et al. setting differs from this model.
- The original book was not read here; its claims are cited through secondary sources. Joubert et al. (2024), "Enhanced Backtesting for Practitioners", was named as a lead but not reviewed.

## Deflated Sharpe, PBO and multiple-testing hurdles, and the counter-arguments

### Takeaway
The "replication crisis" debate and the DSR gate answer different questions. Jensen et al. and Chen and Zimmermann say published factors are mostly real; they do not excuse Sentinel's own search, where the gate's result depends heavily on how trials are counted (0.96, 0.89 or 0.35). Keep the gate, freeze its definition, and pair it with PBO and a shrunk Sharpe estimate.

### Cited Findings
**Deflated Sharpe**
- DSR is a probabilistic Sharpe ratio whose rejection threshold is the expected maximum Sharpe under the null, given the number of independent trials and the variance of Sharpe across trials. It also adjusts for skew, kurtosis and sample length — [Bailey and Lopez de Prado 2014](https://www.davidhbailey.com/dhbpapers/deflated-sharpe.pdf)
- N must be the number of *independent* trials. Using M dependent trials overstates the threshold, and the paper derives implied independent trials from average correlation between trials, among other routes — [same](https://www.davidhbailey.com/dhbpapers/deflated-sharpe.pdf)
- In the paper's worked example, the same strategy passes 95% at N=46 (0.9505) and fails with more trials; with Normal returns it would have survived to N=88 — [same](https://www.davidhbailey.com/dhbpapers/deflated-sharpe.pdf)
- Sentinel's gate: variance across every candidate at the horizon, 12 trials, bar 0.95. Alternative readings gave 0.96 (sampling noise only, 12 trials), 0.89 (41 trials) and 0.35 (live variance, 41 trials). The policy is to record any change first and apply it whichever way it cuts — `MODEL_TRIALS.md` line 36.

**Probability of backtest overfitting**
- Bailey, Borwein, Lopez de Prado and Zhu introduce PBO, computed with combinatorially symmetric cross-validation (CSCV), to estimate how likely a configuration selected on backtest is to do poorly out of sample — [SSRN 2326253](https://papers.ssrn.com/abstract=2326253)
- Published decision thresholds conflict: one secondary source rejects at PBO above 0.50, another attributes 0.05 to Bailey et al., and a third says there is no universal cut-off — [LuxAlgo](https://www.luxalgo.com/library/concept/probability-of-backtest-overfitting/); [Kestra blueprint](https://kestra.io/blueprints/backtest-overfitting-guard); [course notes](https://quinfer.github.io/financial-data-science/chapters/10_backtesting_production_ml.html). Treat any threshold as a convention until checked against the paper.

**Multiple-testing hurdles**
- Harvey, Liu and Zhu catalogue 316 factors and argue the bar for a new factor should be about t = 3.0 rather than 2.0 — [course notes, secondary](https://quinfer.github.io/financial-data-science/chapters/10_backtesting_production_ml.html)
- Harvey and Liu show how to convert Sharpe to a t-ratio and adjust with Bonferroni, Holm and BHY. They find the haircut is nonlinear: the 50% rule of thumb is too punitive for very high Sharpes and marginal strategies can lose far more — [Harvey and Liu, Backtesting](https://people.duke.edu/~charvey/Research/Published_Papers/P120_Backtesting.PDF)

**Counter-arguments (the contested part)**
- Jensen, Kelly and Pedersen (*Journal of Finance*, 2023) use a Bayesian hierarchical model and find most factors replicate and work out of sample in 93 countries, and that the evidence is strengthened, not weakened, by the large number of factors — [NBER w28432](https://www.nber.org/papers/w28432)
- A summary of the paper reports a replication rate of about 85% on alpha, with 10 of 13 themes above 75%; Seasonality, Leverage and Size are the exceptions. The rate depends on using CAPM alpha and a Bayesian framework the authors designed — [Alpha Architect](https://alphaarchitect.com/is-there-a-replication-crisis-in-finance/)
- Chen and Zimmermann reproduce predictors: of 180 clearly significant in the original papers, 98% of reproductions have t above 1.96 — [Open Source Cross-Sectional Asset Pricing](https://cfr.ivo-welch.info/published/papers/chen2021open.pdf)
- Chen and Zimmermann's empirical Bayes estimates put the publication-bias correction at only 10 to 15% of in-sample mean returns, with under 10% risk of inference in the wrong direction. They argue hurdles above 3.0 and returns 30 to 50% weaker in alternative portfolio tests are easily misread as publication-bias effects — [arXiv 2209.13623](https://arxiv.org/abs/2209.13623)
- Disagreement remains over reproducibility versus tradability: reproducing a t-stat does not show that a net return survives. The search found no direct Harvey-Liu-Zhu reply to these papers.

### Inferences
- Both sides can be right. The counter-arguments concern the prior probability that published, economically motivated factors are real. The DSR gate concerns selection inside one private search over features, horizons and books. A useful synthesis: features from the literature deserve a friendlier prior, but the *selected configuration's Sharpe* still needs deflating.
- Trial counting is the weak point. My calculation of the expected maximum of N standard normals with the paper's formula: 1.67 for N=12, 2.20 for N=41, 2.53 for N=100. So if the SD of Sharpe across trials is 0.2 (an illustrative value), the null bar is about 0.33 at N=12 and 0.44 at N=41. N matters less than the variance term, which fits the local pattern where switching to live variance moved the score the most (0.89 to 0.35).
- Count of trials: 12 (candidates x horizons) looks low if P1, P2, P3, O1-O8, B1-B6 and feature-study decisions all selected on overlapping data. The principled answer is the paper's implied-independent-trials estimate from the trial return correlation matrix, plus logging every trial. Report 12 and 41 side by side rather than change the gate; the recorded gate policy already requires any change be written first.
- A gate that sits on a single calendar is itself a lottery. Check whether DSR clears 0.95 on each offset and on the tranched return series. A pass on only some offsets should count as a fail.
- PBO needs a matrix of trial returns across the same dates. Sentinel's logged trials should supply this if per-period returns were kept. It answers a different question from DSR (does the in-sample winner hold its rank out of sample?), so the two are complementary.

### Gaps
- HLZ (2016) was not read directly; the 316 and 3.0 figures come through secondary sources and Harvey and Liu's related paper.
- Neither DSR nor PBO was found validated specifically on LightGBM cross-sectional rankers. No empirical study of how well DSR predicts live results was found.

## Alpha decay evidence and live monitoring: IC, feature drift, halting, retiring, forward record length

### Takeaway
Expect decay, and plan on a live Sharpe well below backtest. Performance monitoring is statistically slow: at this model's IC, a monthly test takes years to separate "working" from "dead". So halts should fire on operational and data faults quickly, while retirement decisions should rest on pre-registered sequential tests and be expected to take years.

### Cited Findings
**Decay**
- McLean and Pontiff (97 predictors): returns are 26% lower out of sample and 58% lower post-publication, relative to in-sample. The 32-point gap is their estimate of publication-informed trading, and declines are larger for predictors with higher in-sample returns. The 26% is an upper bound on data mining — [copy](https://tevgeniou.github.io/EquityRiskFactors/bibliography/AcademicReviewFactor.pdf)
- Falck, Rej and Thesmar confirm the post-publication drop. Six of 11 variables predict out-of-sample Sharpe decay, publication date is strongest, and their overfitting haircuts of 25 to 50% are similar in size to the decay — [arXiv 2105.01380](https://arxiv.org/pdf/2105.01380)
- A 2022 *Quantitative Finance* study reports Sharpe decays by about half after publication, with publication year explaining about 30% of the variance of decay — [Taylor and Francis mirror, not checked at length](https://informahealthcare.com/doi/full/10.1080/14697688.2022.2098810)
- Contested: Chen and Zimmermann attribute only 10 to 15% to publication bias, so decay beyond this is more likely trading, costs or regime change than false discovery — [arXiv 2209.13623](https://arxiv.org/abs/2209.13623). Jensen et al. say some post-publication decay is expected and consistent with a stable Bayesian estimate — [Alpha Architect summary](https://alphaarchitect.com/is-there-a-replication-crisis-in-finance/)

**The Quantopian 888 study**
- On 888 strategies with at least 6 months out of sample, backtest Sharpe had almost no predictive value (R-squared below 0.025). Using only the last backtest year raised it to 0.05 — [Wiecki et al. 2016](https://c.mql5.com/forextsd/forum/208/All%20that%20Glitters%20Is%20Not%20Gold%20-%20Comparing%20Backtest%20and%20Out-of-Sample%20Performance%20on%20a%20Large%20Cohort%20o.pdf)
- Volatility (R-squared 0.67) and max drawdown (0.34) persisted far better than returns. More backtesting went with a larger shortfall (Spearman R-squared 0.017, weak but significant). A nonlinear classifier on backtest features reached 0.17 on hold-out, and a portfolio built on its predictions beat one built from top backtest Sharpes — [same](https://c.mql5.com/forextsd/forum/208/All%20that%20Glitters%20Is%20Not%20Gold%20-%20Comparing%20Backtest%20and%20Out-of-Sample%20Performance%20on%20a%20Large%20Cohort%20o.pdf)
- Caveats: a crowd-sourced retail cohort, a short out-of-sample window (as little as 6 months) that is itself noisy, and authors from the platform in question. Low R-squared partly reflects noise in short OOS Sharpes, so it does not show that a disciplined single-model process has no predictive value.

**Monitoring tools**
- Qlib's `SigAnaRecord` reports IC, ICIR, Rank IC and Rank ICIR, plus long-short and long-average returns; `PortAnaRecord` reports backtest excess return with and without costs — [Qlib recorder docs](https://qlib.readthedocs.io/en/latest/component/recorder.html)
- Qlib online serving tags models online, next-online or offline, rolls with `RollingStrategy`, and refreshes stored predictions and labels through updaters — useful as a pattern for comparing the next model with the live one before promotion — [Qlib online docs](https://qlib.readthedocs.io/en/latest/component/online.html)
- CUSUM monitors an information ratio and raises an alarm once evidence says IR is zero or worse; it is related to Wald's SPRT and traded off detection time against false alarms — [factorAnalytics](https://rdrr.io/github/sangeeuw/factorAnalytics/man/cusumActMgr.html); framework credited to Philips, Yashchin and Stein (2003) — [Morgan Stanley IM](https://www.morganstanley.com/im/en-us/capital-seeker/about-us/news-and-insights/articles/when-good-managers-stumble.html). A practitioner recipe: stop when the drawdown of returns centred between an acceptable and a warning Sharpe crosses a threshold — [Paleologo thread](https://threadreaderapp.com/thread/1854746535705076008.html)
- Minimum track record length gives the sample needed to reject that a Sharpe is below a threshold at a chosen confidence, and grows with negative skew and positive excess kurtosis — [Bailey and Lopez de Prado, Sharpe Ratio Efficient Frontier](https://www.risk.net/journal-risk/2223785/sharpe-ratio-efficient-frontier); [formula walkthrough](https://portfoliooptimizer.io/blog/the-probabilistic-sharpe-ratio-bias-adjustment-confidence-intervals-hypothesis-testing-and-minimum-track-record-length/)

### Inferences (my calculations; normal-return approximation, Sharpe against zero)

| True annual Sharpe | 80% confidence | 95% confidence |
|---|---|---|
| 1.00 | about 13 months | about 4.1 years |
| 0.75 | about 19 months | about 6.2 years |
| 0.50 | about 3.2 years | about 12.2 years |

- Sampling error: an 11-year Sharpe has a standard error of about 0.37. The live config's 0.97 is thus roughly 0.2 to 1.7 at 95% before any deflation. Timing noise (the offset spread) sits on top of this. Averaging offsets reduces only the timing noise, not this sampling error, because the market history is the same.
- IC power, using trial O1 (rank IC 0.037, IC t 3.12, 101 months): implied monthly IC SD is about 0.119. Twelve months then has standard error 0.034, so a fully healthy model shows a negative 12-month mean IC about 14% of the time. Reaching t = 2 takes about 41 months on average. A Wald test of IC 0.037 against 0 needs on the order of 33 months (alpha 0.2) to 62 months (alpha 0.05).
- So a rolling 12-month IC is a dashboard item, not a stop rule. Daily or weekly recomputing of a monthly-horizon IC adds little independent evidence because the windows overlap.
- Expected-live planning number: haircut backtest Sharpe by 25 to 50% for planning, which gives about 0.5 to 0.7 from the 0.97 mean. That rests on the Falck and McLean-Pontiff ranges, which apply to published factors and are only an analogy here. At these Sharpes, a forward record *confirms* nothing within 1 to 2 years. It can catch breakage and implementation shortfall sooner.

**Proposed monitoring stack (judgement, not evidence-derived; thresholds need calibrating on the walk-forward record).** Note that CLAUDE.md requires explicit approval for kill-switch or execution changes.

| Layer | Monitor | Action | Speed |
|---|---|---|---|
| Data and ops | Names scored (already under 800 keeps the old book), missing or stale features, score distribution jumps | Hold book, alert | Same day |
| Feature drift | Per-feature PSI or rank-distribution shift against training window; share of NaN; rank correlation of scores with last month's | Investigate; drift alone is not a halt, since markets drift normally | Each rebalance |
| Implementation | Paper fill vs model return, turnover, cost vs assumed, realized beta vs target | Fix or reduce | Monthly |
| Signal | Rank IC by month, cumulative IC, CUSUM against backtest-implied IC; compare with the walk-forward distribution, not zero | Warn, then review | 12+ months |
| Portfolio | Drawdown vs simulated drawdown distribution, volatility vs expected (the 888 study suggests vol and drawdown persist best, so these make faster alarms than return) | Cut risk if outside tail | Weeks to months |
| Retirement | Sequential test crossed, *or* forward results below the backtest's lower band for a pre-set span, *or* economic rationale gone | Retire or re-research | Years |

- Separate halting (reversible, operational, quick, low evidence bar) from retirement (needs strong evidence). Write both rules before the forward test starts, since the local B1 forward test is already running and later rules invite tuning to outcomes.
- Monitor the paper record against the *offset distribution*. A live month is not "off model" unless it is outside what the tranches and offsets show.
- Monthly retraining means model churn is a live risk: track rank correlation between successive models' scores on the same date.

### Gaps
- No primary evidence was found for specific IC or Sharpe halt thresholds, or for how retirement rules perform empirically; the stack above is judgement. Philips, Yashchin and Stein (2003) was not read directly.
- No primary study on PSI-style feature drift predicting trading-model failure was found. PSI's usual cut-offs come from credit-scoring practice and could not be sourced here.
- Lo (2002) on Sharpe standard errors and autocorrelation was not retrieved; the approximation above is the standard one, and overlap or autocorrelation would lengthen the times shown.

## Look-ahead and survivorship tests, ensembling seeds and test-calendar offsets, and reporting

### Takeaway
The 1.01 to 0.61 swing looks like classic rebalance timing luck, and the literature's remedy is tranching across start dates. Report the full offset and seed distribution as the headline, and consider making the live book itself tranched. Sentinel's existing survivorship work is good but incomplete (four in ten delisted names, and delisting returns).

### Cited Findings
**Timing luck and tranching**
- Rebalance timing luck is the standard deviation of returns between identically managed portfolios rebalanced on different dates; equal weighting across N sub-indexes reduces it by about 1/N. Realized luck depends on holdings count, turnover and rebalance frequency — [Hoffstein, Sibears and Faber via Newfound](https://www.thinknewfound.com/rebalance-timing-luck); [CXO summary](https://www.cxoadvisory.com/?p=32143)
- Studies report up to 348 to 350 bp of CAGR dispersion between best and worst rebalance dates: Hoffstein et al. 2020 and, for a concentrated monthly momentum portfolio, Zarattini and Pagani — [CXO summary](https://www.cxoadvisory.com/?p=34592); [Quantpedia](https://quantpedia.com/the-tranching-dilemma/)
- Zarattini and Pagani find luck falls roughly with 1/N tranches with mean returns unchanged, but costs can outweigh the benefit for small accounts (about 2 tranches for $25K) — [Quantpedia](https://quantpedia.com/the-tranching-dilemma/). Only summaries were read, so check the figures against the PDFs.
- Sentinel evidence: a one-day calendar shift moved IC t from 3.12 to 2.32 and net Sharpe from 1.01 to 0.61. The live config's offsets gave 1.24, 0.86, 1.08, 0.70 (mean 0.97, SD 0.24 by my calculation), and the baseline gave 0.63, 0.49, 0.89, 0.69 (mean 0.68, SD 0.17) — `MODEL_TRIALS.md` lines 103-106, 133-139.
- Qlib's default online signal combiner is `AverageEnsemble` over models — [Qlib online docs](https://qlib.readthedocs.io/en/latest/component/online.html).

**Survivorship and delisting**
- Shumway (1997): bankruptcy-type delistings are usually surprises, and correct delisting returns are missing for most such stocks. Shumway and Warther (1999) suggest a corrected return of -55% for missing Nasdaq performance delistings, after which the Nasdaq size effect disappears — [JF 1999 record](https://econpapers.repec.org/RePEc:bla:jfinan:v:54:y:1999:i:6:p:2361-2379); [BYU](https://scholarsarchive.byu.edu/facpub/9278)
- Deep value mean annual return falls from 30.1% to 24.6% when delisting information is included (Price, Beaver and McNichols, as cited) — [Alpha Architect](https://alphaarchitect.com/dealing-with-delistings-a-critical-aspect-for-stock-selection-research/)
- Sentinel loads daily bars for delisted stocks and ranks them point in time, but the free source has history for only about four in ten delisted exchange-listed stocks — `services/collector-tradfi/main.py` lines 2761-2789.

### Inferences
**Reducing the swing.** There are three separate noise sources, and each needs its own treatment:

| Source | Treatment | Reduces sampling error? |
|---|---|---|
| Model seed | Average 5 to 10 LightGBM seeds per fold | No; only model variance |
| Calendar timing | 4+ offsets in research; tranched book live | No; only path luck |
| Market history | Longer sample, DSR, forward record | Yes, but only with more data |

- Do not treat 4 offsets at 0, 5, 10, 15 sessions as 4 independent tests. They are heavily correlated, with the true effective count below 4 and the SD not a clean standard error. Use all 21 monthly offsets for final candidates if compute allows; one training per offset per fold is the cost. Note also that the 1-day shift already moved Sharpe by 0.40, so adjacent offsets are not necessarily near-duplicates in results. That is itself a warning that the top-book selection is unstable.
- Ensemble predictions vs ensemble portfolios. Distinguish (a) mean of per-offset Sharpes, the fair estimate of a *single-calendar* book; (b) Sharpe of the averaged returns, the fair estimate of a *tranched* book, usually higher through diversification. Never report (b) unless the live book really trades tranched, since the live paper book is on one calendar. Fully tranched monthly into 4 weekly sleeves, costs need rerunning since the turnover pattern changes, and any execution change requires approval under CLAUDE.md.
- Look for why it swings. An extremes-based equal-weight book (B1) is likely to be sensitive at the threshold. Measure turnover from names crossing the cut-off and test rank-buffer bands. Also, a one-day shift changing the fold boundaries changes training sets, so separate the effects: fix the model and shift only the trade date, then fix the trade date and shift only the training folds. That decomposition says whether it is execution timing luck or model instability, and it is the cheapest high-value test available.
- Selection on offsets: P1+P2 was chosen on the same four offsets it is reported on, so 0.97 is still selected. Hold out unused offsets (for example 1, 2, 3, 6, 7) and report the live config on those as a clean check. It is not fully independent, but it is cheap.

**Look-ahead test battery (engineering practice; no primary evidence base found).**
1. Label-shuffle: permute labels cross-sectionally within dates; IC and Sharpe should collapse to zero. Anything else means a pipeline leak.
2. Canary: add a feature built from the future return; confirm it shows dominant gain, proving the test detects leaks. Then confirm that a canary shifted past the embargo has no effect.
3. Lag decay: delay signals by 1, 2, 5 days. A collapse with one day of delay suggests same-bar leakage or a fragile close-to-close assumption.
4. Truncation invariance: rerun features with data cut at date T and compare values at T with those from the full history. Any difference means future dependence such as full-sample normalization or restated data.
5. Point-in-time audit: use filing `accepted` timestamps for fundamentals; check universe membership (the 1,000 most liquid) is computed only from prior data.
6. Date-boundary replay: compare a backtest's month with what the live engine produced for that month.

**Survivorship tests.**
- Quantify the missing roughly 60% of delisted names: compare delisting rate and the distress features' extreme-decile weight in the panel vs the known inactive list. Then stress: assign -55% (the Shumway-Warther figure, Nasdaq-specific and old, so a stress value and not an estimate) as the final return for performance delistings in the book, and report the Sharpe change.
- Check the last-bar handling: a delisted stock's bars just stopping can drop the terminal loss from the backtest even for names that do have history. The local docstring covers bars, not terminal returns; this was not traced further.
- Liquidity-filtered, long-biased books should be less exposed than microcap books, but B1's return skew and illiquidity signals could push toward distress names, so the shorts and buys should be checked separately.

**Reporting template (proposed).** Each trial's record should state:
- Config hash, date, pre-registered decision rule, cumulative trial number.
- Offsets table: all offsets, mean, min, max, and seeds per offset. Headline is the mean, or the median with the range, never the best or the default-calendar run.
- IC, IC t on non-overlapping months, ICIR, positive-fold share, gross and net Sharpe with a confidence interval, beta-adjusted excess.
- DSR at the frozen definition per offset, plus sensitivity readings (12 and 41 trials, both variance estimates). PBO over the logged trial matrix.
- Survivorship stress result, cost sensitivity at 2x, sub-period table, leak-test status.
- Planning-case Sharpe after the haircut, clearly labelled as an assumption.

**Recommended order of work.**
1. Trade-date vs fold-boundary decomposition, and the leak tests.
2. Seeds plus held-out offsets; report the distribution, and rerun DSR per offset.
3. PBO from logged trials, plus implied independent trials.
4. Delisting stress.
5. Write down monitoring and retirement rules before more forward data arrives.
6. CPCV on finalists only.

### Gaps
- No primary paper was found on seed ensembling or offset averaging specifically for LightGBM rankers, so seed counts of 5 to 10 are judgement. It is untested how much seed noise this model has; measure it, because the Sentinel record shows offset noise but not seed noise.
- No standard reporting format for offset distributions was found in the literature.
- Point-in-time fundamentals evidence was not retrieved (the search returned nothing useful).
- Whether Sentinel's pipeline already implements any of the leak tests was not audited. Only the survivorship code and trial records were inspected.
- Trade-date vs fold-boundary attribution for the 0.40 swing is unknown. `MODEL_TRIALS.md` line 103 also says that run had one more day of data, and it is not established how much of the swing is calendar alignment versus the extra data.
