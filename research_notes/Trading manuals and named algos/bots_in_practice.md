# Trading bots in practice and prediction competitions

Evidence labels:
- **[Primary]**: official docs or the organiser's own page. This does not mean the claim is independently verified.
- **[Self-reported]**: a fund, vendor or builder describing its own results.
- **[Secondary]**: press or a third-party summary.
- **[Anecdote]**: forum or blog post.
- **[Inference]**: my reasoning, or arithmetic on a cited figure.

Reading depth:
- Many items were read through a fetch tool's page summary, not verbatim.
- Items marked "search snippet" were seen only in search-result summaries, and the page itself was not opened.
- No Kaggle page (discussion, leaderboard or winner post) was read directly.

The earlier report (`reports/Trading bots and smart algos.md`) already covers Wiecki et al.'s 888 Quantopian algorithms, the repo activity check, Knight, August 2007, the CFTC bot warning and retail base rates. Those are not repeated here except by reference.

## Numerai: mechanics, documentation and fund results

### Takeaway
Numerai's docs give the most directly transferable validation recipe found: treat each era as one observation, purge overlapping targets, walk forward, and blend across targets. Its fund numbers are self-reported model-investor figures with no audit mentioned on the page read. A claimed 2024 Sharpe of 2.75 sits beside a reported -17% the year before, so a single year says little.

### Cited Findings
**How the tournament works and pays**
- Each era groups observations from one point in time, and the docs say to "strongly consider treating each era as a single data point", so metrics such as mean correlation are computed per era. [Primary] — [Numerai data docs](https://docs.numer.ai/numerai-tournament/data)
- Payout scores are CORR (correlation to target) and MMC (contribution to the Meta Model). Feature Neutral Correlation (FNC), correlation with the stake-weighted Meta Model (CWMM) and Benchmark Model Contribution (BMC) are informational and not paid. Targets are lagged (20D2L means 20 days of returns after a 2-day lag), and a 60-day score resolves about 8 weeks later. Reputation is a 1-year average of final scores. [Primary] — [Numerai scoring docs](https://docs.numer.ai/numerai-tournament/scoring)
- Stake mechanics: per-round stake is TVL / R, with R = 64 unresolved rounds for Numerai and Signals. Positive scores earn NMR and negative scores burn it. Settlement is the weighted score, times the payout factor (1 in v3), clipped to the round's caps, times the stake. Caps are set per round, for example [-1, +1] or [-1, +2]. From round 1343, Classic uses 3 × CORR60 + 15 × MMC60. Legacy clips were 5% for Numerai and 3.5% for Signals. [Primary] — [Numerai staking docs](https://docs.numer.ai/numerai-tournament/staking)
- Secondary figure: about 399,759 NMR burned of an 11M maximum and about 942,392 NMR staked as of September 2026. This comes from a search snippet of a non-official site and is unverified. The docs define burned as max supply minus total supply. [Secondary] — [OneKey](https://onekey.so/blog/ecosystem/nmr-deep-research-report-token-future-development-and-price-outlook); [Numerai NMR docs](https://docs.numer.ai/nmr)
- Signals, the variant closest to bring-your-own-data, has legacy docs showing payout = stake × clip(payout_factor × (FNCv4 + 2 × MMC), -0.05, 0.05), with a formula change from 2 September 2025. Search snippet. [Primary, may be stale] — [Numerai Signals staking docs](https://docs.numer.ai/numerai-signals/staking)

**What the docs say on method**
- Validation is walk-forward. Data is split into chunks of 156 eras, each predicted by a model trained only on earlier data, with a purge of 8 eras for 20D targets and 16 for 60D. [Primary] — [Numerai models docs](https://docs.numer.ai/numerai-tournament/models)
- The docs warn that targets can cover overlapping future periods, so cross-validation needs care. [Primary] — [Numerai data docs](https://docs.numer.ai/numerai-tournament/data)
- Neutralisation regresses predictions on features and subtracts the exposures, per era, with a `proportion` parameter setting how much is removed. [Primary] — [Numerai models docs](https://docs.numer.ai/numerai-tournament/models)
- Ensembling steps are: gaussianise per era, standardise, take a weighted sum, gaussianise, then neutralise if needed. The target-ensemble example is a 50/50 blend of two target-specific LightGBM models. [Primary] — [Numerai models docs](https://docs.numer.ai/numerai-tournament/models)
- Models trained on other targets "can outperform or ensemble well" with the default target. The docs also strongly advise against relying too heavily on a few features, because their predictive power is inconsistent over time. [Primary] — [Numerai data docs](https://docs.numer.ai/numerai-tournament/data)
- Neutralisation is contested. A 2023 paper studies dynamic neutralisation against a time-varying feature subset. A community post argues it makes little sense if the goal is mean correlation. FNC is not paid. Both the paper and the post were seen as search snippets only, and the post's exact page is uncertain. — [arXiv 2301.00790](https://arxiv.org/pdf/2301.00790v2); [Medium, 7 lessons (attribution uncertain)](https://medium.com/@ferrerabertran/one-year-participating-in-the-numerai-machine-learning-competition-7-lessons-learned-d540318bd2c7); [Numerai scoring docs](https://docs.numer.ai/numerai-tournament/scoring)

**Fund results**
- Numerai One's 2024 net return was 25.45% with Sharpe 2.75 (net of fees, 0% risk-free rate) and one down month. AUM went from about $60M to $450M over three years. JPMorgan Asset Management secured $500M of capacity. [Self-reported] — [Numerai blog, 26 Aug 2025](https://blog.numer.ai/jpmorgan-secures-500m-capacity/)
- Those figures are for a model investor from 1 September 2019 paying 1%/20% fees applied retrospectively. Individual results may differ materially. The page gives no volatility or drawdown figure, makes no market-neutral claim and does not mention an audit. [Self-reported] — [Numerai blog](https://blog.numer.ai/jpmorgan-secures-500m-capacity/)
- Press and token-news results report +20% in 2022, -17% in 2023, gains in all years but one, and about +6% net in 2025 to August against 7% for Aurum's quant equity market-neutral index. All came through search snippets. The per-year attribution was not confirmed, because the Invezz page returned 403. [Secondary, ultimately from the fund] — [Benzinga](https://fr.benzinga.com/quote/cryptocurrency/25/08/47341702/numerai-secures-up-to-500m-commitment-from-jpmorgan-asset-management); [Invezz](https://invezz.com/news/2025/08/27/numerai-token-surges-125-in-a-week-as-jpmorgan-commits-500-million/)
- A figure of +49% since inception appears on one page with an unreliable date. Treat it as unverified. — [article mirror](https://wa.dev.favish.com/article/paul-tudor-jones-backed-quant-manager-returns-20-using-crowdsourced-ideas)

### Inferences
- 25.45% at Sharpe 2.75 implies about 9% annualised volatility, approximately (25.45 / 2.75). A swing to or from -17% at that volatility is roughly a 2-sigma year, so 2.75 should not be read as a long-run Sharpe. [Inference]
- The payout weighting (MMC at 5x CORR) pays for uniqueness relative to a meta-model. A solo book has no meta-model, so MMC does not transfer, while the validation recipe does. [Inference]
- Numerai's "target ensemble" is an inexpensive idea for Sentinel: blend models trained on different horizons or residual targets. It counts as a logged trial. [Inference]
- Feature neutralisation should not be adopted on authority. The docs supply the tool, not evidence that it raises net returns, and Sentinel's own beta-neutral-legs trial (B-series, one calendar, Sharpe -0.01 against 0.82, per the earlier report) argues for caution. Partial neutralisation against a few dominant features is a different test and is still untested. [Inference]
- The fund runs at a far larger scale on a staked crowd's predictions, so its returns are not a benchmark for a free-data book. Its reported loss year is the more transferable fact. [Inference]

### Gaps
- No audited Numerai fund statements, full-year 2025 or 2026 return, drawdown, or factor attribution were found.
- The cause of the 2023 loss was not found.
- No figure was found for the share of stakers with positive lifetime NMR returns.
- The docs' forum post on era-wise CV was not read.

## Kaggle-style stock prediction: winners and shake-ups

### Takeaway
What could be sourced points to gradient boosting, or a simple NN blended with it, plus careful time-based validation and feature work. I could not source measured shake-up statistics, so the question of how leaderboards shook up remains mostly unanswered from primary pages. These contests also score prediction or a stylised utility, not net-of-cost monthly long-short returns.

### Cited Findings
**G-Research Crypto**
- The top three all used LightGBM, and winners credited feature engineering above model choice. Teams submitted two models, scored over three months on live data. The prize pool was $125,000. Entries were ranked on weighted Pearson correlation. The organiser noted large jumps and drops in rank, especially early, and attributed them to noisy data. No numeric scores were given. [Primary, organiser] — [G-Research](https://www.gresearch.com/news/wrapping-up-the-g-research-crypto-forecasting-competition/)

**Jane Street Market Prediction (2021)**
- The metric was u = min(max(t, 0), 6) × Σp_i, with t a Sharpe-like annualised ratio, so the Sharpe credit is capped at 6. [Secondary] — [BulletTech](https://bullettech.github.io/BulletTech/Main_Course/Machine_Learning/2021-09-02-Kaggle-janestreet)
- The top solution is described as an XGBoost plus supervised-autoencoder MLP, trained jointly to avoid CV leakage from pre-training the autoencoder. The same blog attributes it to team "Cats Trading". It says private-phase rankings updated on real data and that top teams held position. [Secondary] — [BulletTech 2022](https://bullettech.github.io/BulletTech/Main_Course/Machine_Learning/2022-01-30-Kaggle-exp-s2)
- A university release says Yirun Zhang's team had the highest score over a nine-month contest, and another index credits "1st / 4,245 Yirun Zhang" with a supervised autoencoder and MLP. The team-name and person attributions may refer to the same team, which is unconfirmed. [Secondary] — [King's College London](https://www.kcl.ac.uk/news/kings-student-success-in-market-prediction-ai-competition); [JaneStreetMkt docs (search snippet)](https://janestreetmkt.readthedocs.io)
- A public high-scoring solution used purged group time-series split with XGBoost, and was described as "not complicated". [Secondary] — [BulletTech](https://bullettech.github.io/BulletTech/Main_Course/Machine_Learning/2021-09-02-Kaggle-janestreet)
- A top-team summary describes 5-fold purged group CV and dropping the first 85 days to avoid overfitting the public leaderboard. Search snippet. [Secondary] — [BestHub](https://www.besthub.dev/articles/kaggle-jane-street-market-prediction-competition-summary-and-model-insights-accb0dc2927c)

**JPX Tokyo Stock Exchange (2022)**
- The host confirms the competition ran on Kaggle from April 2022, with awards in December. The winner was Shoki Sakai (kisa031). [Primary] — [JPX](https://www.jpx.co.jp/corporate/news/news-releases/6020/20221206-01.html); [Shizuoka University](https://www.inf.shizuoka.ac.jp/english/news/detail.html?CN=154751)
- A participant's write-up says supplemental data contained the prediction period, so the live public board reflected leak users. Seen as a search snippet only; the page failed on an expired certificate. [Anecdote] — [CSDN](https://blog.youkuaiyun.com/xiuxiuxiu666/article/details/125877329)
- A summary of a 4th-place walkthrough describes a simple return-based model and acknowledges the difficulty of beating chance. Search snippet; the fetched page held no content. [Secondary] — [Recapio](https://recapio.com/digest/kaggle-winners-walkthroughs-px-tokyo-stock-exchange-prediction-with-team-flaty-by-kaggle)

**Optiver Trading at the Close (2023)**
- Only non-winner material was found: 187th of 4,034 with LightGBM at MAE 5.3364 (student deck), and a 9-hour runtime limit with lagged targets unavailable through the API. [Anecdote] — [team deck](https://docs.google.com/presentation/d/1C6eS-og4hsXxU47HZJNA90A4Y6jZf_6M/htmlpresent)
- A third-party repo reimplements top solutions with retrain monitoring, with several stages incomplete. It is not the winner's work. [Anecdote] — [GitHub](https://github.com/GrigoriiTarasov/Optiver-Trading-at-the-Close-retrain-monitoring)

**Ubiquant**
- Nothing usable beyond confirmation that the competition existed, around mid-2022. — [clist](https://clist.by/account/nathanrm/resource/kaggle.com/)

### Inferences
- Plain tree models and feature work winning low-signal contests supports keeping Sentinel's ranker simple. The sample here is small and partly secondary. [Inference]
- G-Research's two-model, frozen, live-scored design is the useful template: commit a small number of models before forward data arrives. It maps onto Sentinel's pre-registered forward test. [Inference]
- JPX shows a public board can be contaminated by leakage, and that a board position does not equal skill. Three months of live scoring in noisy data has large luck in ranks, and the earlier report's power table makes the same point for books. [Inference]
- Do not import winners' results as return expectations. None of the sources gives net-of-cost performance, and intraday or anonymised-feature contests differ in horizon and data from a monthly book. [Inference]

### Gaps
- **Measured shake-up statistics** (public-to-private rank correlation, medal churn) were not found for any contest. Kaggle's pages were not retrieved. Any figure on this would be from memory, so none is given.
- Winner write-ups for JPX, Optiver Trading at the Close, Ubiquant and Jane Street's 2024 Real-Time contest were not retrieved. The earlier search's claims about a specific Optiver winner were not confirmed.
- I found no evidence that any winning model was later traded profitably.

## Open-source strategy libraries and platforms: is anything live-verified?

### Takeaway
I found no verified live record for any community strategy, and the repos themselves disclaim this. The two crowd-platform experiments in the search, Quantopian and QuantConnect Alpha Streams, both ended or were rebuilt. The search was bounded, so this is an absence of evidence found, not proof of none.

### Cited Findings
- Freqtrade community strategy repos tell users to treat strategies as samples and backtest recent periods themselves, and one warns that backtests can differ greatly from live results. These are community repos, not an official Freqtrade position. Search snippets. [Self-reported disclaimers] — [freqtrade-strategies fork](https://github.com/DevZenPro/freqtrade-strategies); [freqtrade-stuff](https://github.com/flessner/freqtrade-stuff)
- Quantopian's Point72 Ventures deal was for up to $250M, partly contingent on performance benchmarks. First allocations ranged from $100,000 to $3M, picked from over 100,000 submissions. [Press] — [Fortune](https://fortune.com/2016/07/27/steven-cohen-250-million-quantopian); [Bloomberg](https://www.bloomberg.com/news/articles/2017-04-06/first-pile-of-steven-cohen-cash-is-injected-into-quantopian-code)
- One analysis says a $50M fund was down 3% in late 2017. It is not a fund statement and covers a single period. Search snippet. [Secondary] — [Institutional Investor](https://www.institutionalinvestor.com/article/2bsvz8k1h4id41opnf4lc/portfolio/steven-cohens-crowdsourced-quant-experiment-gets-real)
- The community platform closed in November 2020 at short notice. The company had said in February it was not making money, and the founder called it a "moonshot" in crowd-sourcing alpha. The article gives no fund returns. [Secondary] — [eFinancialCareers](https://www.efinancialcareers.com/news/2020/11/quantopian-shutdown)
- A practitioner view holds that unique alpha is hard for individuals, the allocation criteria (unique, slower-moving, liquid) put users up against large firms, and competition for allocations discouraged sharing. This comes from a vendor with a competing community to sell. [Opinion, conflicted] — [Robot Wealth](https://robotwealth.com/my-thoughts-on-quantopians-closing/)
- QuantConnect stopped Alpha Streams v1.0 and disabled sales for a rebuild, and a company blog note says it ceased in February 2022. A forum post reports 80-90% submission rejection and a requirement of at least 5 years of backtest. Search snippets. [Primary and forum] — [QuantConnect forum](https://www.quantconnect.com/forum/discussion/13441/alpha-streams-refactoring-2-0/); [rejection thread](https://www.quantconnect.com/forum/discussion/6737/9-ways-to-get-your-alpha-rejected/); [QuantConnect blog](https://www.quantconnect.com/blog/?p=1947)

### Inferences
- These libraries are useful as engineering, not as sources of alpha, which matches the earlier report. [Inference]
- Quantopian's "unique, slow, liquid" criteria closely resemble Sentinel's own niche. A monthly liquid-stock factor book is exactly the competitive space it found hard, so a low-Sharpe prior is warranted. [Inference]

### Gaps
- Quantopian's full fund performance and how the Cohen capital ended were not found.
- No Backtrader or zipline live record was found, and I did not search specifically for Backtrader examples.
- Why Alpha Streams closed is not established by what I read.

## Verified records, survivorship and postmortems

### Takeaway
"Verified" usually means trades were tracked, not that returns were audited or achievable, and Collective2's own disclaimer calls its results hypothetical. I found no broker-verified postmortem of a small builder, so the failure causes below are anecdotal, apart from regulator orders on misrepresented backtests.

### Cited Findings
**Tracked-record platforms**
- Collective2's disclaimer labels performance claims hypothetical, excluding commissions, fees and subscriptions. The platform says it verifies strategy performance going forward and prohibits retroactive deletion or modification of signals. Search snippets. [Self-reported] — [Collective2 support](https://support.collective2.eu/portal/en/kb/articles/why-hypothetical-results-and-performance); [Collective2 listing](https://collective2.com/details/100866695)
- NinjaTrader's partner site states it does not review, verify or audit strategy leaders' performance data. Search snippet. [Primary] — [NinjaTrader Follow](https://follow.ninjatrader.com/details/154653200)
- The only quantified survivorship evidence found covers 193 active stock systems on Collective2 as of 29 July 2009, with mean age 11 months and median 6 months. Younger systems had worse trailing returns, and the author infers positive survivorship bias and high early mortality. No mortality rate or dropout count is reported, the data is old, and it is a snapshot of active systems only. [Secondary analysis] — [CXO Advisory](https://www.cxoadvisory.com/?p=4128)

**Regulator findings on misrepresented records**
- SEC v. F-Squared: a $35M settlement. Hypothetical performance over seven years before the product existed was presented as actual, marketed as "not backtested", and an error inflated it to 135% against a corrected 38%. [Primary, with press detail] — [SEC](https://www.sec.gov/news/press-release/2014-289); [Fortune](https://fortune.com/2014/12/22/sec-f-squared-fraud)
- CFTC on TradeWins (2002): brochures claimed real trading, and results came from back-testing. [Primary] — [CFTC](https://www.cftc.gov/opa/enf02/opa4628-02.htm)
- Alpha Fiduciary (SEC, 2015) was cited for backtests not clearly labelled, with disclosures placed away from the data. Search snippet. [Secondary] — [ThinkAdvisor](https://www.thinkadvisor.com/amp/2015/12/03/sec-enforcement-ria-fined-for-backtested-data-misuse/)

**Institutional wind-down**
- Goldman's Global Alpha lost 6% in 2006, then suffered its worst in the summer 2007 "quant bloodbath", with trouble retaining quants. Search snippet. [Press] — [CNBC](https://www.cnbc.com/id/44549144)

**Builder anecdotes (unverified, no statements)**
- A turtle-style currency algo was eaten by wide gold spreads around market closes that the code never handled. [Anecdote] — [r/algotrading mirror](https://axebps-redlib.hf.space/r/algotrading/comments/146nvvu/what_algo_trading_lesson_have_you_learned_the)
- Curve fitting and broker differences in prices, spreads and feeds were blamed. The forum belongs to a strategy-generator vendor. [Anecdote, vendor forum] — [StrategyQuant forum](https://strategyquant.com/forum/topic/strategyquant-success/page/3/)
- A builder claims to have made and lost over $500k algo trading. Only the title was seen, so the amount is unverified and the cause unknown. [Anecdote] — [r/algotrading mirror](https://redlib.astrial.org/r/algotrading/comments/1jj15c5/i_made_and_lost_over_500k_algotrading/mjoas9t/?context=3)

### Inferences
Verification ladder, strongest first [Inference]:
1. Audited fund statements.
2. Regulator findings.
3. Broker statements disclosed in full, including closed accounts.
4. Platform-tracked forward signals, which are vulnerable to survivors and exclude costs.
5. Self-reported fund pages.
6. Forum posts.

Further inferences:
- Very little of the evidence found reaches levels 1-3 for small builders. [Inference]
- Sentinel's paper record, even if complete, sits around level 4: it is tracked but hypothetical, per the Alpaca limits in the earlier report. It should be labelled that way anywhere it is shown, and pre-inception backtests must never be blended with forward returns, which is the F-Squared failure. [Inference]
- Anecdotes repeatedly name unmodelled spread and overfitting. This matches the earlier report's priority on a cost ledger, but it is not independent measurement. [Inference]

### Gaps
- No broker-verified or audited postmortem of a retail or small-fund builder was found, and no Collective2 cohort study newer than 2009. Darwinex and myfxbook returned nothing.
- CFTC Rule 4.41 and NFA text were not read directly; only platform disclaimers derived from them were.
- Recent (2024-26) enforcement cases were not surfaced by this search.

## What this implies for a free-data monthly long-short book

### Takeaway
Adopt the validation discipline and frozen forward-test habit. Test target blends and a small amount of neutralisation as logged trials. Expect a modest, noisy edge, and label the paper record as hypothetical.

### Cited Findings
- Purge and walk-forward parameters: 156-era chunks, with a purge of 8 eras at 20D and 16 at 60D. [Primary] — [Numerai models docs](https://docs.numer.ai/numerai-tournament/models)
- One-observation-per-era rule. [Primary] — [Numerai data docs](https://docs.numer.ai/numerai-tournament/data)
- Frozen models scored on live data over three months still produced rank volatility. [Primary, organiser] — [G-Research](https://www.gresearch.com/news/wrapping-up-the-g-research-crypto-forecasting-competition/)
- Even a 2.75 Sharpe year sits next to a reported loss year. [Self-reported and secondary] — [Numerai blog](https://blog.numer.ai/jpmorgan-secures-500m-capacity/); [Invezz](https://invezz.com/news/2025/08/27/numerai-token-surges-125-in-a-week-as-jpmorgan-commits-500-million/)

### Inferences
Ranked by evidence and cost; none alters sizing or execution:
1. **Audit, then adopt.** Check that Sentinel's trial engine computes t-stats per month and purges overlapping labels. I did not inspect the code in this task. This one is cheap and has primary support.
2. **Test.** Run a target ensemble blending models on different horizons or residual targets. It costs a logged trial and has weak-to-moderate support.
3. **Test.** Run partial feature-exposure control on the few dominant features, reported per offset. It is unproven, and Sentinel's beta-neutral result lowers the prior.
4. **Adopt as reporting.** Publish a max-feature-exposure diagnostic for each rebuild. This is reporting only.
5. **Keep.** Keep the tree-model ranker and avoid deep-model complexity without net-of-cost evidence.
6. **Adopt as policy.** Keep forward paper returns separate from backtests, and keep the "hypothetical" label on both.
7. **Avoid.** Do not treat a contest rank, a platform "verified" badge or a single fund year as evidence of a strategy's edge.

### Gaps
- No source measures whether Numerai-style neutralisation improves net returns for a small monthly long-short US-stock book.
- No source tests contest-winning methods on a free-data monthly universe net of costs.
