# Documented quant firms and open-source trading bots

Researched 2026-11-05. Evidence labels used below:
- **[Primary]**: regulator, official docs, repo or license, or the firm's own fund page.
- **[Book]**: Zuckerman's book, usually seen here through secondary quotes.
- **[Press]**: named outlet citing a database or anonymous sources. Not audited.
- **[Vendor]**: a project's description of itself.
- **[Weak]**: blog or aggregator.

Several pages were read through a summarising fetch tool, and some searches returned only summaries. Check exact wording against the original before quoting.

## What is credibly public about Renaissance, AQR, Two Sigma and DE Shaw, and what do public funds return versus secretive claims?

### Takeaway
Internal research, data and execution methods at these firms are largely undocumented. The strongest evidence is regulatory (Two Sigma) and published execution data (AQR). Medallion's headline return rests on a book rather than audited public statements, and the funds outsiders can buy are far less remarkable, or in AQR's case are recent and strong but much more modest over 10 years.

### Cited Findings

**Renaissance: returns**
- [Book, repeated in a paper] Medallion averaged about 66% a year before fees over 1988-2018. Cornell's December 2019 paper could find no convincing explanation for it — [Institutional Investor](https://www.institutionalinvestor.com/article/b1k2fymby99nj0/Famed-Medallion-Fund-Stretches-Explanation-to-the-Limit-Professor-Claims)
- [Press, relaying Cornell] RIEF and RIDA results were described as mundane and not comparable to Medallion. Renaissance attributes the gap to Medallion's short holding periods, against longer horizons and other holdings in the outside funds — [Institutional Investor](https://www.institutionalinvestor.com/article/b1k2fymby99nj0/Famed-Medallion-Fund-Stretches-Explanation-to-the-Limit-Professor-Claims)
- [Press] RIEF gained 22.7% in 2024, its best year since a 34% 2011. It lost 14.39% in October 2025, then gained 12.65% in November to stand at +2.34% year to date. The figures come from a hedge fund database and an unnamed source, not audited statements — [II, 2024](https://www.institutionalinvestor.com/index%2ephp/article/2e0uykr3vn5booz0smrcw/hedge-funds/renaissances-2024-rebirth); [II, Nov 2025 rebound](https://www.institutionalinvestor.com/article/premium/renaissances-public-funds-stage-double-digit-comeback-november)
- [Press] RIEF is described as global, quantitative and long-biased, aiming to beat the S&P 500 with beta of 0.4 or lower and about 2.5:1 average leverage. Assets are just under $20B, down from nearly $36B in early 2020 — [II](https://www.institutionalinvestor.com/article/premium/renaissances-public-funds-stage-double-digit-comeback-november)
- [Press] Medallion closed to outsiders in 2005, and little is known about it now — [II](https://www.institutionalinvestor.com/index%2ephp/article/2e0uykr3vn5booz0smrcw/hedge-funds/renaissances-2024-rebirth)
- [Weak, unconfirmed] Medallion +76% and RIEF -22.6% in 2020 — [allmind.ai](https://allmind.ai/news/f9df658c389ae8cb643dbdd488557f10f11ede9fd889c3791ea0603e8f90d388)

**Renaissance: method (mostly Book)**
- [Book] Mercer is quoted as saying the fund is right about 50.75% of the time — [Goodreads quotes](https://www.goodreads.com/work/quotes/68288870)
- [Book, via secondary notes] Berlekamp argued for frequent trading: a bare majority of correct trades suffices across many trades, and infrequent trading magnifies each decision — [Goodreads notes](https://www.goodreads.com/notes/46025922-the-man-who-solved-the-market/88044824-alok-kejriwal?page=2)
- [Weak] The 50.75% edge only works net of costs. A vendor's worked example shows $1.50 expected gross per $100 turning negative at $2 of costs, and it says this is not a reconstruction of Medallion — [LuxAlgo](https://luxalgo.com/blog/simons-strategies-renaissance-trading-unpacked)

**AQR**
- [Primary] Equity Market Neutral (QMNIX) returned 26.53% in 2025, against 4.18% for 3-month T-bills, and 6.97% annualised over 10 years to December 2025 — [AQR](https://funds.aqr.com/funds/aqr-equity-market-neutral-fund)
- [Weak] 2024 was 25.85% — [PortfoliosLab](https://portfolioslab.com/tools/stock-comparison/QMNIX/AQMIX)
- [Primary, with conflict] AQR shows a 1.33% adjusted expense ratio; aggregators show 5.48% and 6.03%. These are different measures — [AQR](https://funds.aqr.com/funds/aqr-equity-market-neutral-fund); [StockAnalysis](https://stockanalysis.com/quote/mutf/QMNIX); [MarketXLS](https://marketxls.com/mutual-funds/qmnix)
- [Paper] "Trading Costs" (Frazzini, Israel, Moskowitz) uses $1.7T of live executions across 21 developed markets over 19 years — [SSRN](https://papers.ssrn.com/abstract=3229719)
- [Secondary on the paper] Realised costs were far below earlier academic estimates, and price impact dominated spread because passive limit orders rarely paid the full spread. S&P 500 cost was about 4.81 bps. The figures describe a large institution and do not transfer to retail — [ETF.com](https://www.etf.com/sections/index-investor-corner/swedroe-trading-cost-truths)
- [Primary, abstract only] "Fact, Fiction, and Factor Investing" addresses factor myths, including those arising from weak 2018-2020 performance. Its claims were not read — [AQR](https://www.aqr.com/Insights/Research/Journal-Article/Fact-Fiction-and-Factor-Investing)

**Two Sigma (best-documented failure)**
- [Primary] Staff identified model vulnerabilities by March 2019, which were not addressed until August 2023 and had no written policy covering them. One employee made unauthorised changes to more than a dozen models without supervision. Two Sigma repaid $165M and paid $90M in penalties, without admitting or denying the findings — [SEC](https://www.sec.gov/newsroom/press-releases/2025-15)
- [Secondary] Parameters could be changed without review or approval, and affected accounts diverged by -$165M and +$400M — [Investment Executive](https://www.investmentexecutive.com/from-the-regulators/quant-funds-failed-to-guard-models-sec/)

**DE Shaw**
- [Press, anonymous source] 2025: Composite about +18.5% net (12.9% annualised since 2001, one negative year). Oculus about +28.2% net (14.4% annualised since 2004, no negative year). Both are closed to new capital, so these are not public funds — [Reuters via WSAU](https://wsau.com/2026/01/02/d-e-shaws-flagship-funds-trump-market-volatility-to-beat-sp-500-in-2025-source-says/)

**Return comparison**

| Vehicle | Outsider access | Figure | Evidence |
|---|---|---|---|
| Medallion | No | ~66% gross avg, 1988-2018 | Book/paper, not audited publicly |
| RIEF | Qualified investors | +22.7% 2024; +2.34% YTD Nov 2025 | Press/database |
| QMNIX | Mutual fund | +26.53% 2025; 6.97% 10-yr ann. | Primary |
| DE Shaw Composite | Closed | ~18.5% 2025; 12.9% since 2001 | Press/anonymous |

### Inferences
- Medallion should not be a target for Sentinel. Its stated edge is high-frequency trading with large leverage and cost control, and the firm's own wider-horizon funds do not replicate it. A daily or monthly factor book is much closer to RIEF or QMNIX in type.
- QMNIX's 10-year 6.97% annualised, despite about +26% in each of 2024 and 2025, implies weak earlier years. Evaluating a factor book on a recent window would be misleading.
- Copy from Two Sigma: every model parameter change is versioned, reviewed and attributable, and a known model flaw gets an owner and a deadline.
- Copy from AQR: measure realised implementation cost from one's own fills and split it into spread and impact. Do not borrow AQR's bps numbers for a small account.
- Copy from Berlekamp's argument, cautiously: more independent bets strengthen a small edge, but only if costs stay below it.

### Gaps
- **Rumoured or unverified (recalled, no source checked):** Renaissance's data-cleaning stories, single-model culture, leverage levels, Medallion's capacity cap, and staff profit shares. Do not treat these as documented without checking the book itself.
- Medallion returns after 2018 and full-year RIEF 2025: no reliable source found.
- A 2024 paper by Qiang Liu reportedly argues that 66.1% and 39.1% are arithmetic averages, not CAGRs. This came from a search summary only and neither the paper nor the net 39.1% was verified directly.
- Actual research process, signals, data vendors, risk limits and execution at Two Sigma and DE Shaw: no primary documents were found. Treat them as unknown. Form ADV filings were not reviewed.
- AQR Large Cap Multi-Style and other AQR funds were not retrieved.

## How are the open-source systems designed, and are they maintained in 2026?

### Takeaway
All seven repos were pushed to within days of 2026-11-05 and none is archived. Parity is engineered differently in each: same strategy code in an event engine (Nautilus, and Lean by design), versus research-first vectorised (vectorbt) or model-pipeline (Qlib) tools. Freqtrade's empirical look-ahead test is the most directly copyable idea.

### Cited Findings

**Maintenance, from the GitHub API on 2026-11-05** — each row links its [Primary] repo

| Repo | Stars | Last push | Latest GitHub release | License |
|---|---|---|---|---|
| [Freqtrade](https://github.com/freqtrade/freqtrade) | 56.5k | 2026-11-05 | 2026.10 (Oct 30) | GPL-3.0 |
| [Hummingbot](https://github.com/hummingbot/hummingbot) | 20.9k | 2026-11-04 | v2.17.0 (Sep 22) | Apache-2.0 |
| [Jesse](https://github.com/jesse-ai/jesse) | 8.8k | 2026-11-03 | none on GitHub Releases | MIT |
| [Lean](https://github.com/QuantConnect/Lean) | 22.4k | 2026-11-05 | v2.4.0.1 (2017; a stale marker, code is active) | Apache-2.0 |
| [NautilusTrader](https://github.com/nautechsystems/nautilus_trader) | 31.3k | 2026-11-05 | v1.231.0 (Aug 2) | LGPL-3.0 |
| [vectorbt](https://github.com/polakowo/vectorbt) | 9.7k | 2026-11-03 | v1.2.0 (Oct 19) | Apache-2.0 + Commons Clause |
| [Qlib](https://github.com/microsoft/qlib) | 50.0k | 2026-11-04 | v0.9.8 (Oct 8) | MIT |

**Freqtrade**
- [Primary] `lookahead-analysis` compares a baseline backtest against sliced single-signal backtests and reports changed indicator values or moved entries and exits, without inspecting code — [docs](https://www.freqtrade.io/en/stable/lookahead-analysis/)
- [Primary] Limits: it checks only signals that fire, so false negatives are possible; pairlist-dependent logic, limit orders with custom prices and FreqAI targets can be falsely flagged — [docs](https://www.freqtrade.io/en/stable/lookahead-analysis/)
- [Primary] Lists crypto exchanges only, offers dry-run, and states the software is for education — [docs](https://www.freqtrade.io/en/stable/)
- [Primary] FreqAI retrains at a set frequency, in a background thread when live, emulates that in backtests, and persists models for crash recovery. The page does not describe drift monitoring — [docs](https://www.freqtrade.io/en/stable/freqai/)

**Hummingbot**
- [Primary] V2 uses finite-lifecycle Executors (active to closed or failed; Position, DCA, Grid, TWAP and others), long-running Controllers that orchestrate them, and one Market Data Provider — [docs](https://hummingbot.org/strategies/v2-strategies/)

**Jesse**
- [Vendor] Claims to handle multi-timeframe look-ahead internally through declared extra candles, at a backtest speed cost. This was not independently verified — [docs](https://docs.jesse.trade/docs/routes)
- [Weak] Described as a crypto framework, with Binance, Kraken, KuCoin and Hyperliquid named by a third party — [Stork](https://www.stork.ai/en/jesse)

**QuantConnect Lean**
- [Primary] Reality models cover fill, slippage, fee, brokerage, buying power, settlement, short availability, margin calls and more. Defaults assume highly liquid assets, so high-volume or illiquid trading may need custom models — [docs](https://www.quantconnect.com/docs/v2/writing-algorithms/reality-modeling/key-concepts)

**NautilusTrader**
- [Vendor/Primary] Rust event-driven core with a Python control plane. Backtest, sandbox and live share a core, and the same strategy code runs in each. "Production-grade" is the project's own label — [docs](https://nautilustrader.io/docs/latest/concepts/overview/)
- [Primary] The docs caution that venue, transport, timing and persistence behaviour may not be reproduced in simulation. Execution reconciliation handles state recovery — [docs](https://nautilustrader.io/docs/latest/concepts/overview/)

**vectorbt**
- [Primary] The Commons Clause bars selling, and paid hosting or support where value comes mainly from the software — [license](https://raw.githubusercontent.com/polakowo/vectorbt/master/LICENSE.md)

**Qlib**
- [Primary] Layers are infrastructure (DataServer, Trainer), learning framework, workflow (Information Extractor, Forecast Model, Decision Generator, Execution Env, nestable) and an Analyser interface — [docs](https://qlib.readthedocs.io/en/latest/introduction/introduction.html)
- [Primary] `qrun` takes YAML with `qlib_init` (including `region: cn|us`) and `task` (model, dataset segments, records). The example has adjacent train, valid and test segments and a TopkDropout strategy (top 50, drop 5). That page mentions no purge gap — [docs](https://qlib.readthedocs.io/en/latest/component/workflow.html)
- [Primary] Online serving: OnlineManager keeps a history of which models were online when and can simulate past periods; OnlineTool tags recorders online or offline; the routine updates predictions, prepares and trains tasks, prepares online models, then builds signals, by default an average ensemble — [docs](https://qlib.readthedocs.io/en/latest/component/online.html)
- [Primary] Limits: only next-day prediction updates are supported, not order generation; the default model switch is naive; one method carries a FIXME — [docs](https://qlib.readthedocs.io/en/latest/component/online.html)
- [Primary] The point-in-time store keeps one row per statement (publication date, period, value, next-index), so amended values are not used before publication. It is designed for quarterly and annual data and is not performance-optimised — [docs](https://qlib.readthedocs.io/en/latest/advanced/PIT.html)

### Inferences
- A shared core reduces divergence but does not remove it; Nautilus says as much. Parity should be measured from live fills against simulated fills.
- Qlib's documented online layer is model lifecycle, not drift monitoring. Monitoring would still need to be built.
- GitHub release dates understate activity for Lean and Jesse, so last push is the better signal here. Stars and push dates say nothing about live profitability.

### Gaps
- Not verified: detail of Nautilus's risk engine, fill and latency models (the fetched pages only point elsewhere); Lean's engine architecture and data normalisation; Hummingbot's risk controls; vectorbt's architecture and any look-ahead tooling; Freqtrade's protections and recursive analysis.
- No project was found to publish audited live results. This search did not specifically target them.
- Qlib issue-level health, and whether its bundled public data remains usable, were not checked.

## Which are usable for US equities, and which ideas could a Python event-driven system adopt without new dependencies?

### Takeaway
On verified evidence, Qlib has a US mode for research, while Freqtrade lists crypto only and Jesse appears crypto-focused. Lean and Nautilus are plausible but their US equity adapters were not confirmed here. No framework adoption is needed: Sentinel already has core validation gates, so the best additions are small stdlib-level ideas.

### Cited Findings
- [Primary] Qlib has a `us` region mode, and it must match stored data — [docs](https://qlib.readthedocs.io/en/latest/component/workflow.html)
- [Primary] Freqtrade's documentation lists only crypto exchanges — [docs](https://www.freqtrade.io/en/stable/)
- [Primary] Lean's reality models include settlement and short availability, which matter for equities — [docs](https://www.quantconnect.com/docs/v2/writing-algorithms/reality-modeling/key-concepts)
- [Primary, Sentinel] `MODEL_TRIALS.md` already records purged walk-forward folds with a 21-day embargo and a deflated-Sharpe gate at 0.95 — `C:/Users/najia/sentinel/MODEL_TRIALS.md`, lines 11 and 36.

### Inferences (proposals, none built or tested)
Ranked by value for effort. All use existing dependencies. Any that touch execution or sizing need explicit approval under `CLAUDE.md`.

1. **Truncation look-ahead test (from Freqtrade).** Compute features on full history and on history cut at date T; assert values at or before T match exactly. As a guard test in `tests/`, this also catches rolling and normalisation leaks. Like Freqtrade's, it can only show bias it exercises.
2. **Online-model ledger (from Qlib).** An append-only table of model version, config hash, training window, and valid-from and valid-to dates. It answers which model made a given pick, and gives the replay basis for model trials.
3. **Parameter change control (from the Two Sigma order).** Hash the active model and trading config at load, record every change, and alarm on an unrecorded hash.
4. **Knowledge-date on fundamentals (from Qlib PIT).** Store publication date plus period and read as-of, keeping every amended version. Check the existing tables first, as this may be partly in place.
5. **Execution-shortfall ledger (from AQR and Nautilus).** Per order: decision price, arrival price, fill price, spread and modelled cost, reported as live versus backtest cost. Paper fills are simulated, so this will not establish real impact.
6. **Finite order lifecycle (from Hummingbot).** Model each rebalance order as an explicit state machine ending closed or failed, with every failure counted.
7. **State reconciliation on restart (from Nautilus).** Compare local positions and orders against the broker at startup, and refuse new orders on mismatch.
8. **YAML-style experiment recipe (from Qlib).** Express a trial as a single config object with segments, model, strategy and costs, stored with results. Python's `dataclasses` or Pydantic would suffice.
9. **TopK-dropout style turnover control (from Qlib).** Replace only a limited number of holdings per rebalance. This changes trading behaviour, so it should be trialled against book B1, not adopted by default.

Not worth adopting: a whole framework, since Lean is a separate engine and Qlib's serving would duplicate the Kafka and job design; crypto-focused bots for live use; vectorbt's Commons Clause adds a licence question if the platform is ever sold, and it would be a new dependency.

### Gaps
- Alpaca or US equity support in Lean and Nautilus: the Lean page fetched returned 404, and Nautilus's adapter list was not read. Hummingbot's exchange list was not read.
- Jesse's official exchange list was not found, so "crypto only" is an inference from its self-description.
- Sentinel's current state for items 2-7 was not audited, apart from the `MODEL_TRIALS.md` lines cited. Memory notes say the kill switch, caps, execution and drift alarm were still to build, which may be out of date.
