# Systematic trading algorithm families: evidence net of costs, for a free-data US-equities system

Reading depth: no paper was read in full. Tags on each finding are [A] abstract, repository page or search extract of the paper itself; [S] secondary summary (blog, press, vendor research digest). Figures tagged [S] need checking against the paper before they are relied on.

## Which families have evidence net of costs, and how do they fit a ~$100k paper account with free data and a monthly LightGBM long-short book?

### Takeaway
The only families with a credible route to fit are low-turnover cross-sectional ML in liquid stocks (the current design) and, as added features rather than separate books, options-implied signals and earnings data. Every family's net edge is much smaller than its headline figure, and the published evidence for combined anomalies ranges from about 20 bps a month to 1.42% a month depending on the assumptions, so a realistic prior sits near the low end.

### Cited Findings

**Cross-sectional factor ML**
- How it works: rank stocks on many characteristics using a nonlinear model. Trees and neural networks performed best because they capture nonlinear interactions, and the dominant signals are variations on momentum, liquidity and volatility; gains are reported as up to double leading regression-based strategies. [A] — [Gu, Kelly, Xiu, NBER w25398](https://www.nber.org/papers/w25398)
- Excluding microcaps, distressed stocks or high-volatility episodes considerably attenuates ML profits, and reasonable trading costs erode them further through high turnover and extreme positions. Over 1987-2017 the signals held up better in long positions and recent years, with low downside risk. [S] — [Avramov, Cheng, Metzker summary, Hebrew University page](https://cris.huji.ac.il/en/publications/machine-learning-vs-economic-restrictions-evidence-from-stock-ret/)
- Press coverage puts the reductions at about 62% excluding microcaps, 68% excluding non-rated firms and 80% excluding distressed firms. [S] — [finews](https://www.finews.asia/finance/32603-cuhk-ai-based-stock-picking-overrated)
- ML on 320 anomalies reports net out-of-sample returns up to 1.42% a month and an LSTM net alpha of 1.20% (t = 3.46), with turnover above 50%; costs cut average returns by 13% to 40%. Most cost-mitigation techniques lowered turnover but also lowered net returns; a two-month holding period marginally helped. [S, unreplicated working paper, via a vendor digest] — [Quantpedia on Azevedo, Hoegner, Velikov](https://quantpedia.com/the-expected-returns-of-machine-learning-strategies/)
- Restricting that study to already-published anomalies costs the typical strategy about 46 bps a month. [S, search extract] — [AEA 2025 programme entry](https://www.aeaweb.org/conference/2025/program/paper/TihddefB). Conflict: this much higher figure sits against the roughly 20 bps a month net for combined anomalies in [Chen and Velikov](https://www.cambridge.org/core/journals/journal-of-financial-and-quantitative-analysis/article/zeroing-in-on-the-expected-returns-of-anomalies/945133D5A3ECEEAF466AEE91551FD225) [A].

**Pairs and statistical arbitrage**
- How it works: match stocks by historical price distance, then trade divergence and convergence. The rule earned about 11% a year in excess returns for top-pairs portfolios over 1962-2002. [A] — [Gatev, Goetzmann, Rouwenhorst, RFS 2006](https://econpapers.repec.org/RePEc:oup:rfinst:v:19:y:2006:i:3:p:797-827)
- After commissions, market impact and short-selling fees, about 30 bps a month risk-adjusted survives among well-matched, refined-industry pairs over 1963-2009. For the largest 30% of stocks the alpha is 24 bps a month, and the strategy is largely unprofitable after 2002. [A] — [Do and Faff 2012](https://strathprints.strath.ac.uk/41722/)
- A digest of an earlier version gives 0.12% a month net on average, and 0.29% for the best refinements; these differ from the published abstract. [S] — [CXO Advisory](https://www.cxoadvisory.com/technical-trading/pairs-trading-net-profitability/)
- Worsening arbitrage risks explain up to 70% of the drop in profits. [A] — [Do and Faff 2010, FAJ](https://rpc.cfainstitute.org/research/financial-analysts-journal/2010/does-simple-pairs-trading-still-work)

**Time-series trend following**
- How it works: go long assets with positive trailing returns and short those with negative ones, scaled by inverse volatility. The original evidence is in index, currency, commodity and bond futures, not single stocks. [A] — [Moskowitz, Ooi, Pedersen, JFE 2012](https://econpapers.repec.org/RePEc:eee:jfinec:v:104:y:2012:i:2:p:228-250)
- The long-run study covers 67 markets across four asset classes and models a 2/20 fee. The extract shows a net-of-fee Sharpe of 0.77 for 1880-2013, but the table's column labels were ambiguous in extraction. [A, authors are at a firm that sells trend strategies] — [Hurst, Ooi, Pedersen](https://fairmodel.econ.yale.edu/ec439/hurst.pdf)
- Critique: the premium is attributed entirely to time-varying net long market exposure, and it does not subsume cross-sectional momentum in individual stocks. [S] — [Swedroe on Goyal and Jegadeesh](https://www.etf.com/node/93759.md)
- Single stocks: time-series momentum persists after standard risk factors, conditional on market state, information discreteness and sentiment. [S] — [same search set, Lim, Wang, Yao via Hedge Fund Alpha](https://hedgefundalpha.com/?p=1210204). Own-stock moving-average rules underperformed buy-and-hold in NASDAQ stocks even gross. [S] — [Trend-following in US stocks: a revisit](https://law-journals-books.vlex.com/vid/trendfollowing-trading-strategies-in-855656210)

**Short-term reversal**
- How it works: buy recent losers and sell recent winners at a weekly or monthly horizon. Reversal is strongly tied to illiquidity, and contrarian profits are smaller than likely transaction costs. [A] — [Avramov, Chordia, Goyal, JF 2006](https://ideas.repec.org/a/bla/jfinan/v61y2006i5p2365-2394.html)
- Counter-evidence: a later paper attributes the cost problem to small caps and reports 30 to 50 bps a week net in large caps. Authorship was not visible in the extract; treat as unverified. [A] — [EFMA 2011 paper](https://efmaefm.org/0EFMAMEETINGS/EFMA%20ANNUAL%20MEETINGS/2011-Braga/papers/0259_update.pdf)
- Do and Faff's abstract says reversal, like pairs, is largely unprofitable after 2002. [A] — [Do and Faff 2012](https://strathprints.strath.ac.uk/41722/)

**Post-earnings drift**
- How it works: buy positive and short negative earnings surprises, and hold over the following weeks. Prices now reflect surprises on the announcement date; drift has been absent in large stocks since 2006 and only recently faded in microcaps. [A] — [Martineau, CFR 2022](https://nowpublishers.com/article/Details/CFR-0122)
- Over February 2001 to December 2024 the t-statistic is 2.18 with all stocks and 1.43 without microcaps; newer disagreement with Martineau is attributed to research-design choices. [S] — [UCLA Anderson Review on Subrahmanyam 2025](https://anderson-review.ucla.edu/is-post-earnings-announcement-drift-a-thing-again/)

**Event-driven**
- Merger arbitrage: buy the target after a deal is announced. Across 4,750 mergers over 1963-1998 returns resemble selling uncovered index puts, with about 4% a year in excess returns after costs. [A] — [Mitchell and Pulvino, JF 2001](https://econpapers.repec.org/RePEc:bla:jfinan:v:56:y:2001:i:6:p:2135-2175). A digest reports beta near 0 in normal markets, rising to 0.50 in down markets. [S] — [Alpha Architect](https://alphaarchitect.com/facts-fiction-merger-arbitrage/)
- Index changes: the S&P 500 addition effect fell from 7.4% in the 1990s to under 1% over the last decade, and deletions averaged 0.1% over 2010-2020. [A] — [Greenwood and Sammon, JF 2025](https://econpapers.repec.org/RePEc:bla:jfinan:v:80:y:2025:i:2:p:657-698). The NBER version says 0.8%, while a digest says 0.3%, a discrepancy in the recent-decade figure. [A/S] — [NBER w30748](https://www.nber.org/system/files/working_papers/w30748/w30748.pdf); [Alpha Architect](https://alphaarchitect.com/disappearing-index-effect/)
- Spinoffs: positive abnormal returns for spinoffs and parents for up to three years over 1965-88. [S] — [Cusatis, Miles, Woolridge via academia.edu](https://www.academia.edu/87621500/Restructuring_through_spinoffs). Implementing this is described as "fraught with ambiguities" [A] — [McConnell, Ozbilgin, Wahal 2001](https://business.purdue.edu/faculty/mcconnell/publications/spinoffs-ex-ante-2001-vol74.pdf). European excess returns over 1987-2005 are mostly insignificant. [A] — [Stirling repository](https://storre.stir.ac.uk/bitstream/1893/451/1/Spinoff-jbf.pdf)

**Options-implied signals**
- Put-call parity deviations: stocks with relatively expensive calls beat those with expensive puts by 50 bps a week. Predictability is greater with high option liquidity and low stock liquidity, and it falls gradually over the sample. [A] — [Cremers and Weinbaum, JFQA 2010](https://ideas.repec.org/a/cup/jfinqa/v45y2010i02p335-367_00.html)
- Smirk: stocks with the flattest smirks beat the steepest by 10.9% a year risk-adjusted, persisting at least six months. [A] — [Xing, Zhang, Zhao, JFQA 2010](https://ideas.repec.org/a/cup/jfinqa/v45y2010i03p641-662_00.html)
- A preprint reports about half the original smirk magnitude on 2015-2017 data and further weakening after 2020. [S-level confidence: single unreviewed preprint, method not checked] — [arXiv 2608.26115](https://arxiv.org/pdf/2608.26115)

**Market making**
- How it works: earn the spread by quoting both sides, while managing adverse selection. Latency differences explain much of the gap in HFT firms' performance, and revenue concentration is high and not declining. [A] — [Baron, Brogaard, Hagstromer, Kirilenko, JFQA 2019](https://ideas.repec.org/a/cup/jfinqa/v54y2019i03p993-1024_00.html)

**General cost rule**
- Most anomalies with one-sided monthly turnover below 50% keep significant net spreads when designed to mitigate costs; few higher-turnover strategies do. [A] — [Novy-Marx and Velikov, RFS 2016](https://nber.org/papers/w20721)

### Inferences
All entries in this table are my judgement from the findings above, not sourced results. Capacity entries are qualitative because no reviewed source measured capacity at this scale.

| Family | Net-of-cost evidence | Decay | Capacity | Data | Fit |
|---|---|---|---|---|---|
| Cross-sectional ML | Moderate; strong dependence on small, illiquid names | Substantial for published inputs | Far above $100k in liquid names | Prices, fundamentals: free | **Core; keep** |
| Pairs/stat arb | Weak after 2002 | Severe | Fine at $100k | Daily prices: free; borrow cost data is not | Low; skip as a book |
| Stock trend | Thin; mostly market timing | Unclear | Fine | Free | Low; use as a feature or risk input |
| Short-term reversal | Disputed | Reported unprofitable after 2002 | Fine | Free | Low; it would break monthly turnover discipline. Feature only |
| Earnings drift | Weak outside microcaps | Severe | Fine, but the remaining effect is in microcaps | Free via EDGAR for actuals; surprise consensus is the hard part | Feature only |
| Merger arb | About 4% a year historically, with put-like tail risk | Not established here | Fine | EDGAR filings: free, but parsing is costly | Optional later; it adds crash exposure |
| Spinoffs | Old US evidence only | Unknown | Small, few events | EDGAR: free | Research only |
| Index changes | Near zero | Severe | n/a | Free | Skip |
| Options-implied | Strong gross, net unknown | Evidence of fading | Moderate | Limited free history | **Best candidate for new features** |
| Market making | Not attainable | n/a | n/a | Paid, colocated | Exclude |

- Publication decay implies LightGBM features built on published anomalies should be back-tested with post-publication and post-2005 windows; a full-history backtest overstates the edge.
- Avramov et al. suggests the book's backtest should be reported with microcaps and distressed names removed. If most of its return vanishes, the paper results will not be attainable in practice.
- The short leg deserves separate attribution, since the ML evidence favours long positions and short costs apply. This is a measurement suggestion; changes to sizing or execution would need explicit approval under project rules.
- Options signals: project memory says Alpaca option bars reach back only to February 2024, so any test window is under three years and spans few regimes. Treat results as weak evidence.
- A monthly rebalance sits on the right side of the Novy-Marx and Velikov turnover rule if one-sided turnover stays under 50% a month; a 50% to 100% book would be in the zone where few strategies survive.

### Gaps
- Exact Gu, Kelly, Xiu Sharpe ratios and out-of-sample R² were not on the abstract page and are not stated here.
- Avramov et al.'s percentages come from press only. Azevedo et al.'s gross and net Sharpe figures (0.94 and 0.84 in one search extract) could not be tied to a specific version, and its publication status was not confirmed.
- Found no reliable net-of-cost result for single-stock trend following, for the options signals, or for post-2010 US spinoffs.
- Pan and Poteshman (RFS 2006) surfaced only as a citation, so its result and decay are not reported. Its option-volume signal is therefore unassessed here.
- Merger-arbitrage evidence ends in 1998 in what I read; no modern net estimate was found.
- Capacity estimates and cost assumptions for a $100k account were not found in any primary source. Whether paper fills represent real impact and borrow was not researched.
- Market making was covered via HFT evidence only; no study directly measures a small retail participant's market-making results.

## Which families have survived publication?

### Takeaway
Anomalies mostly replicate statistically but shrink sharply after publication and after costs: replication is not the same as tradable profit. Chen and Velikov's estimate of 4 bps a month net for the average anomaly is the most sobering and most relevant number.

### Cited Findings
- Across 97 predictors, returns are 26% lower out-of-sample and 58% lower after publication. The 26% is an upper bound on data mining, and the implied publication-informed trading effect is about 32%. [S] — [Boston College summary of McLean and Pontiff](https://www.bc.edu/content/bc-web/schools/carroll-school/news/2017/pontiff-paper.html); [vLex copy](https://law-journals-books.vlex.com/vid/does-academic-research-destroy-855658380)
- Open-source replication: of 240 characteristics, 118 had clear evidence of significant long-short returns in the original papers, and some apparent failures reflect misclassification. [S] — [Swedroe on Chen and Zimmermann](https://evidenceinvestor.com/post/is-there-a-replication-crisis)
- A Bayesian replication rate of 85% relative to CAPM; over 80% of US factors remain significant after more implementable construction, with similar results across 153 factors in 93 countries. [S] — [same summary of Jensen, Kelly, Pedersen](https://evidenceinvestor.com/post/is-there-a-replication-crisis); [Quantpedia](https://vvv.quantpedia.com/?p=26147)
- Across 204 anomalies, after effective spreads, post-publication decay and the modern era, the average expected return is 4 bps a month. The strongest net at best 10 bps after data-mining control; combinations net about 20 bps. [A] — [Chen and Velikov, JFQA 2023](https://www.cambridge.org/core/journals/journal-of-financial-and-quantitative-analysis/article/zeroing-in-on-the-expected-returns-of-anomalies/945133D5A3ECEEAF466AEE91551FD225)
- Gross decay is about 50% post-publication, rising to 72% post-2005. The top quartile by in-sample net Sharpe earns 21 bps a month, and most dispersion in net means is luck. [A] — [Fed FEDS 2020-039](https://www.federalreserve.gov/econres/feds/zeroing-in-on-the-expected-returns-of-anomalies.htm)

### Inferences
- Jensen et al. and Chen and Velikov are not truly in conflict: the first tests whether factors exist gross, the second whether they can be harvested net of costs today.
- By family, survival is weakest for index effects, large-cap earnings drift and simple pairs. ML combinations and low-turnover factors look relatively better, though the ML net evidence is younger and less replicated.
- At 20 bps a month, or about 2.4% a year, $100k would earn roughly $2,400 before financing and borrow. That makes a paper account useful mainly for validating process, and live noise will swamp the signal for years.

### Gaps
- Jensen et al., Chen and Zimmermann and McLean and Pontiff figures here come from summaries. The Open Source Asset Pricing dataset itself was not inspected.

## Which families require data or speed unavailable for free?

### Takeaway
Only market making and latency arbitrage are ruled out by speed. The main data constraints elsewhere are historical option data, earnings consensus and borrow costs, which limit rigor without necessarily preventing a test.

### Cited Findings
- Speed: performance improves with latency rank through colocation upgrades, in both market making and cross-market arbitrage. [A] — [Baron et al., JFQA 2019](https://ideas.repec.org/a/cup/jfinqa/v54y2019i03p993-1024_00.html)
- Latency arbitrage is estimated at about a 0.5 bp tax on trading, worth about $5 billion a year in global equities. [A] — [Aquilina, Budish, O'Neill search extract, BIS WP 955](https://www.bis.org/publ/work955.pdf)
- Short-selling fees materially affect pairs results, so borrow data matters for any short book. [A] — [Do and Faff 2012](https://strathprints.strath.ac.uk/41722/)

### Inferences
- Free and sufficient: daily prices, EDGAR fundamentals and event filings, so monthly ML, pairs, trend, reversal and rule-based event tests are all feasible.
- Free but limited: short option history under project memory's February 2024 limit, and live option quotes limited to about 200 symbols. This allows forward-collected features rather than long backtests.
- Not free, by my understanding: analyst consensus history, point-in-time borrow fees, and full-depth tick data. These were not verified against vendor pricing, and free-only project rules exclude paying for them. Actual-versus-time-series earnings surprises are the free substitute, with weaker fidelity.

### Gaps
- Did not verify current free-source coverage or terms, including point-in-time correctness of free fundamentals and survivorship in free price sets. Both matter directly to backtest validity.
