# Execution, portfolio construction and risk control for a ~$100k long-short book

Scope: equal-weight book of about 90 longs and 95 shorts, rebuilt every 21 sessions, one batch of marketable limit orders at 9:40 ET. Everything here is a recommendation only; sizing and execution changes need user approval.

Source-quality note: several figures come from abstracts, search summaries or secondary coverage, because full papers could not be read (the NBER PDF for Novy-Marx and Velikov would not parse). These are flagged where they matter. Items under Inferences are my reasoning, not findings. Dollar scenarios are illustrative arithmetic, not measurements of this book. I did not inspect the live book's gross exposure, turnover or fills.

## Execution algorithms and time of day: is any of it worthwhile at this size?

### Takeaway
Scheduling algorithms solve a market-impact problem this book barely has: orders of roughly $500-$1,100 per name. The levers that plausibly matter are spread paid and time of day, and neither can be measured on paper, so a live pilot with arrival-price logging should come before any algorithm work.

### Cited Findings
- Frazzini, Israel and Moskowitz use about $1.7 trillion of live executions across 21 developed markets over 19 years, and find actual costs are "an order of magnitude smaller than previous studies suggest" — [AQR](https://www.aqr.com/Insights/Research/Working-Paper/Trading-Costs)
- Secondary coverage of that paper reports average impact of just under 9 bps for trades completed within a day, median near 6 bps; another summary gives a value-weighted mean just above 15 bps, so the figure depends on weighting and I could not check it against the paper — [ETF.com](https://www.etf.com/sections/index-investor-corner/swedroe-trading-cost-truths); [Alpha Architect](https://alphaarchitect.com/the-best-research-paper-ever-written-on-trading-costs/)
- In that data, trade size relative to daily volume is the main cost driver, impact is concave in size, and smaller and more volatile names cost more; shorting looked costlier but not statistically significantly so — [ETF.com](https://www.etf.com/sections/index-investor-corner/swedroe-trading-cost-truths)
- AQR's algorithm tries to supply liquidity, and it rarely paid the full or even half spread. These costs therefore describe a large passive-first institution, not a retail marketable flow, and the data come from one manager — [Alpha Architect](https://alphaarchitect.com/the-best-research-paper-ever-written-on-trading-costs/)
- Almgren-Chriss trades impact cost against timing risk under a mean-variance objective. A risk-neutral trader gets a linear (TWAP-like) schedule, and infinite risk aversion collapses to immediate execution — [Wikipedia summary](https://en.wikipedia.org/wiki/Almgren%E2%80%93Chriss_model); [original paper](https://quantitativebrokers.com/s/Optimal-Execution-of-Portfolio-Transaction-_-AlmgrenChriss-1999.pdf)
- Closing-auction prices can deviate from the 4pm midpoint. Slides from Bogousslavsky and Muravyev report that deviations reverse fully overnight and by half within twenty minutes after the close; these are working-paper and conference figures — [AEA working paper](https://aeaweb.org/conference/2021/preliminary/paper/H9T4hef7); [discussion deck](https://www.cbr.ru/Content/Document/File/141789/press_3.pdf)
- Closing volume is tied to ETF and passive ownership and to institutional rebalancing — [AEA working paper](https://aeaweb.org/conference/2021/preliminary/paper/H9T4hef7)
- Alpaca supports opening and closing auction orders via `opg` and `cls`. `opg` is rejected after 9:28 ET and `cls` after 15:50 ET until 19:00 ET. Fractional orders are DAY only, so auction orders must be whole-share — [Alpaca orders docs](https://docs.alpaca.markets/docs/orders-at-alpaca)
- The Alpaca order page describes no TWAP, VWAP or POV algorithm, so any schedule would have to be built client-side — [Alpaca orders docs](https://docs.alpaca.markets/docs/orders-at-alpaca)

### Inferences
- **Scale.** $100k long and $100k short across 185 names is about $1,100 per long; at 50/50 gross it is about $540. In a stock trading $20M a day (assumed), $1,100 is about 0.006% of volume. Impact models calibrated to institutional sizes are being extrapolated far below their range, and impact should be close to negligible.
- **TWAP, VWAP, POV, implementation shortfall, Almgren-Chriss: not worthwhile for liquid names.** They all spread a parent order to reduce impact. Slicing $1,100 yields many tiny odd lots, more spread crossings and more operational surface, for an impact saving that is close to nil. POV is meaningless at this participation. The one defensible use is staggering the batch through time so a single bad minute does not hit all 185 names. That diversifies timing risk and is not impact reduction.
- **Passive-first is the only technique with a plausible payoff,** because spread is the main remaining cost. It also carries adverse selection and non-fill risk, and paper cannot evaluate it (see the Alpaca section). A simple form would be: post at or inside the midpoint for a bounded period, then cross, with unfilled residuals counted and reported. Test live before believing it.
- **Time of day.**
  - 9:40 sits in the period usually described as having the widest spreads and most volatility. Moving later is a cheap, reversible experiment, and it needs no algorithm.
  - At a 21-session rebuild, a few hours of delay should cost little signal, unless the model's alpha is concentrated right after the signal date. Test this on B1's own history.
  - MOC is a credible alternative: no spread crossing in the usual sense, and it matches a close-to-close backtest. But the fill is an uncontrolled price that may reverse overnight, whole-share rounding applies, and there is a 15:50 cutoff, so a missed fill slips a day.
  - Priority is to test midday and MOC against 9:40, using arrival-mid and next-open benchmarks.
- **Whole-share rounding** distorts equal weight on high-priced names, more so at $540 per name than at $1,100.
- **Illustrative stakes.** With 200% gross and 50% of the book replaced each rebuild (assumed), traded notional is about $200k per rebuild, or $2.4M a year. Each 10 bps of all-in cost is then about $2,400 a year, or 2.4% of equity. At this leverage, cost per dollar traded matters a lot even though impact does not, so turnover reduction beats execution cleverness.

### Gaps
- I found no recent US study directly comparing small-order costs at the open, midday and closing auction. Search results included claims that marketable orders at 9:30-10:00 cost about 8.76% more than midday, and that volatility is concentrated in the first two minutes after the open and the last minute before the close, but I could not reliably attribute these to a specific paper, so treat them as unverified. Older non-US evidence also exists, for example on [Xetra small caps](https://gfk-cfs.de/media//08_45.pdf).
- Almgren-Thum impact parameters were not retrieved; the search returned nothing.
- AQR execution research beyond "Trading Costs" was not reviewed, and neither were broker algorithm papers.
- Alpaca's live routing quality and price improvement for odd lots are unmeasured. Only a live pilot can answer this.
- SEC and FINRA fee schedules for sells were not checked.

## Optimal rebalancing: partial trading and bands

### Takeaway
This is the best-supported improvement available. Buy/hold bands are the simplest proven cost mitigation, and Garleanu-Pedersen supplies the theory for why partial trading works. A band could save more than any execution algorithm at this size, and it preserves equal weight.

### Cited Findings
- Novy-Marx and Velikov describe the buy/hold spread as the most effective simple way to reduce costs: the requirement for opening a position is stricter than for keeping it. It works less well for very high-turnover strategies — [NBER abstract](https://nber.org/papers/w20721); [RFS record](https://econpapers.repec.org/RePEc:oup:rfinst:v:29:y:2016:i:1:p:104-147.)
- Most anomalies with one-sided monthly turnover under 50% still earn significant net spreads when designed to mitigate costs, while few higher-turnover strategies do — [NBER abstract](https://nber.org/papers/w20721)
- A secondary summary gives a rule of thumb that costs reduce value-weighted long-short returns by about 1% of monthly one-sided turnover. This is not verified against the paper and applies to its cost estimates, which predate or differ from the AQR estimates — [Alpha Architect](https://alphaarchitect.com/trading-costs-destroy-factor-investing/)
- Garleanu and Pedersen derive a closed-form policy with two principles: aim in front of the target, and trade partially towards the current aim. Slower-decaying signals get more weight in the aim. Their application is to commodity futures, not a small equity book — [NBER w15205](https://www.nber.org/system/files/working_papers/w15205/w15205.pdf); [EconPapers](https://econpapers.repec.org/RePEc:nbr:nberwo:15205)
- DeMiguel, Martin-Utrera, Nogales and Uppal find that including transaction costs raises the number of jointly significant characteristics from six to 15, because trades in different characteristics net off when combined — [EconPapers](https://econpapers.repec.org/RePEc:oup:rfinst:v:33:y:2020:i:5:p:2180-2222.); [LBS](https://www.london.edu/faculty-and-research/academic-research/a/a-transactioncost-perspective-on-the-multitude-of-firm-characteris)
- An earlier version reports the marginal cost of trading a characteristic's stocks falls about 65% on average when combined in an optimal portfolio — [EDHEC working paper](https://climateimpact.edhec.edu/portfolio-perspective-multitude-firm-characteristics)

### Inferences
- **Recommend evaluating a rank band on B1:** enter only at the extremes, and hold until a name leaves a wider zone. Choose the width on walk-forward data and report net of a pessimistic cost, with the backtested and live net compared. A band changes selection and turnover but not per-name weighting.
- **Full Garleanu-Pedersen is overkill.** It needs a return-decay estimate and a covariance model, and estimation error is a real risk at this scale. Its useful lesson is qualitative, and bands already capture it. Staggered tranches (rebuilding a share of the book each week rather than all of it every 21 sessions) are the other practical analogue, and they also reduce rebuild-date luck, though they add more orders.
- **Don't trade small weight drift.** Trimming a name back to equal weight for small differences costs spread for no signal. A no-trade threshold is a low-risk part of the same change.
- **DeMiguel's netting** favours one combined score over separately traded sleeves, which is relevant if sleeves are ever added.

### Gaps
- Exact band widths and turnover reductions from the Novy-Marx and Velikov tables could not be extracted, as the PDF would not parse.
- No evidence retrieved tests bands specifically on a 90/95-name monthly book. It needs a backtest on B1.
- B1's actual per-rebuild turnover was not inspected.

## Weighting: equal weight, shrinkage, HRP, beta and sector neutrality

### Takeaway
Equal weight is a strong default, and no alternative here has established out-of-sample superiority. Estimated-risk weighting mainly lowers variance and does not obviously add return. The first thing to do is measure the book's realised beta and sector exposure, then decide on light constraints if they are persistently large.

### Cited Findings
- DeMiguel, Garlappi and Uppal: of 14 models across seven datasets, none was consistently better than 1/N on Sharpe, certainty-equivalent return or turnover. Simulations suggest about 3,000 months of data for 25 assets and 6,000 for 50 before sample mean-variance beats it. These results concern asset allocation, not alpha-sorted long-short books — [EconPapers](https://econpapers.repec.org/RePEc:oup:rfinst:v:22:y:2009:i:5:p:1915-1953); [NBER draft](https://conference.nber.org/confer/2006/si2006/ap/uppal.pdf)
- Norm-constrained minimum-variance portfolios nest Ledoit-Wolf shrinkage as a special case and often reported higher Sharpe than Ledoit-Wolf (2003, 2004) and 1/N, in that paper's own tests — [Management Science](https://pubsonline.informs.org/doi/fpi/10.1287/mnsc.1080.0986)
- HRP evidence is mixed and low quality. A peer-reviewed Brazilian study finds it not the best but sometimes as good as alternatives — [FGV](https://periodicos.fgv.br/rbfin/article/view/89848). Blog simulations find it stable but not better than simple risk parity — [Marti](https://gmarti.gitlab.io/qfin/2019/12/04/hierarchical-risk-parity-part-3.html). A thesis finds it lowest-risk when covariance is very unstable — [CBS](https://research.cbs.dk/en/studentProjects/modern-portfolio-theory-hierarchical-risk-parity/)
- Novy-Marx and Velikov argue BAB's rank weighting acts as a hidden equal-weighting that hugely overweights micro- and nano-caps, its betas are biased, and costs reduce profitability by almost 60%, with remaining returns coming largely from profitability and investment tilts (figures via secondary summaries) — [paper](https://mysimon.rochester.edu/novy-marx/research/BABAB.pdf); [Quantpedia](https://quantpedia.com/biased-betting-against-beta/)
- A counterpoint says those cost estimates may be overstated, and value-weighted BAB still earned 56 bps a month — [Alpha Architect](https://alphaarchitect.com/betting-against-beta-new-insights/)

### Inferences
- **Keep equal weight now.** A book of 185 names at $540-$1,100 each has little room for finely estimated weights, especially after whole-share rounding. Ledoit-Wolf would be the right estimator only if risk modelling is adopted, and HRP has no evidence strong enough to justify the added complexity.
- **Read the Novy-Marx and Velikov critique as a warning about this book too.** Equal weighting tilts toward smaller names whose paper results are least reliable. Check the capitalisation and spread of B1's picks, and apply a liquidity floor if the small tail is material.
- **Beta.** The two legs of an equal-weight book may not offset: dollar neutral is not beta neutral. Since SPY and index hedges are out of bounds under the project rules, any correction has to come from stock selection or leg size. Levering the low-beta leg recreates the BAB concern, so prefer modest, shrunk-beta tolerance bands and judge them on net Sharpe. Treat the result as risk reduction rather than alpha.
- **Sector.** A cap on net sector exposure is a cheap risk control that adds little estimation risk, and is preferable to full neutrality, which would force in lower-ranked names. This is a judgement; see gaps.

### Gaps
- No source retrieved directly tests sector neutrality for long-short factor books, so that recommendation is reasoning only.
- Ledoit-Wolf nonlinear shrinkage results versus 1/N were not retrieved.
- I found no strong peer-reviewed US-equity replication of HRP against equal weight. A practitioner claim of equal weight winning and a vendor claim of HRP winning were both discarded as unsupported.

## Drawdown and volatility controls

### Takeaway
Neither is proven as a return enhancer. Per-name or portfolio stops have only conditional support, and vol targeting's stronger evidence is for tail-risk reduction, with out-of-sample return benefits disputed. Keep the kill switch as operational protection, not as an expected-return tool.

### Cited Findings
- Kaminski and Lo: under a random walk, simple stop-loss rules always reduce expected return; they can add value only when momentum is present — [MIT DSpace](https://dspace.mit.edu/entities/publication/bb69ca4b-0cdc-487f-831d-63b2e84fafee); [RePEc](https://ideas.repec.org/p/hhs/sifrwp/0063.html)
- Moreira and Muir report volatility-managed outperformance. Cederburg et al. confirm the in-sample alphas, but find real-time versions generally earn lower Sharpe and certainty-equivalent returns than unmanaged portfolios across 103 portfolios (53 better, 50 worse), citing unstable regressions — [JFE record](https://econpapers.repec.org/RePEc:eee:jfinec:v:138:y:2020:i:1:p:95-117); [Alpha Architect](https://alphaarchitect.com/does-portfolio-timing-based-on-volatility-signals-outperform-buy-and-hold/)
- Barroso and Detzel are reported to argue transaction costs erode the gains; DeMiguel, Martin-Utrera and Uppal report a conditional multifactor version that does survive out of sample and net of costs — [Journal of Finance](https://lbsresearch.london.edu/id/eprint/3716/1/The%20Journal%20of%20Finance%20-%202024%20-%20DeMIGUEL%20-%20A%20Multifactor%20Perspective%20on%20Volatility%E2%80%90Managed%20Portfolios.pdf)
- Harvey et al. find vol targeting reduces the likelihood of extreme returns — [Duke PDF](https://people.duke.edu/~charvey/Research/Published_Papers/P135_The_impact_of.pdf). A later paper argues the scaling involved look-ahead bias, and an implementable version did not consistently help in international equities — [arXiv](https://arxiv.org/pdf/2212.07288)
- Daniel and Moskowitz: momentum crashes are infrequent, persistent and partly forecastable, tending to arrive in rebounds after severe declines, and a dynamic version reportedly more than doubles Sharpe (in-sample design caveats apply) — [NBER](https://www.nber.org/papers/w20439); [Chicago Booth](https://www.chicagobooth.edu/review/understanding-momentum-crashes)

### Inferences
- **Name-level stops on a 185-name monthly book are not recommended.** They add turnover, and on the short side they would buy into squeezes at exactly the worst spreads. There is no evidence they help a cross-sectional model.
- **Crash risk in a short leg is the main tail,** especially for rebounds if B1 has momentum exposure. Worth checking B1's return in rebounds. A single-short loss cap is more a sizing limit than a stop, and equal weight at under 1% of equity already limits initial exposure.
- **Vol targeting.** A simple, slow, down-only gross reduction is defensible for survivability, but should be judged by drawdown, not expected return. It also adds turnover, which is the cost that matters here. A portfolio-level kill switch and drift alarm are justified as protection against model or system failure, independent of this evidence. Those need approval.

### Gaps
- No retrieved study tests a stop or vol-target rule on a book like this; a B1 backtest is needed with costs included, and its history is likely too short to establish much.

## Short-leg costs and borrow

### Takeaway
This is the largest threat to the strategy's claimed return. Published evidence says much of long-short anomaly return sits in hard-to-borrow shorts and disappears after fees. Paper trading shows none of it, so a backtest should exclude or charge these names before any live decision.

### Cited Findings
- Muravyev, Pearson and Pollet, 162 anomalies: average long-short return is 0.14% a month before short-sale costs and -0.01% after borrow fees, with returns coming from the short leg. Excluding high-fee stock-months (about 12% of stock dates), anomalies are unprofitable even before fees, and high-fee stocks provide the bulk of momentum and profitability returns (abstract and summaries, full paper not read) — [abstract mirror](https://academicnewsletter.sufe.edu.cn/info/358802); [CXO Advisory](https://cxoadvisory.com/short-selling/shorting-costs-kill-stock-return-anomalies); [NBER conference draft](https://conference.nber.org/conf_papers/f184100/f184100.pdf)
- D'Avolio (older data): about 91% of lent stocks cost under 1% a year with a value-weighted mean near 17 bps; about 9% are specials averaging 4.3%. After a recall, the mean (median) wait to re-establish is 23 (nine) trading days — [D'Avolio](https://www.johnhcochrane.com/s/davolio_borrowing_stock_JF.pdf)
- Alpaca: 5,000+ easy-to-borrow names with zero locate and borrow fees for Trading API users. Hard-to-borrow requires an approved locate in 100-share round lots, single-use and non-refundable, plus daily fees of market value x rate / 360, charged to the nearest round lot, with three days over a Friday settlement — [Alpaca margin docs](https://docs.alpaca.markets/docs/margin-and-short-selling)
- Alpaca margin: 2x overnight and 4x intraday buying power, with overnight excess met by a margin call next morning. Short maintenance is the greater of $2.50 a share or 100% under $5, and the greater of $5 a share or 30% at or above $5. Margin interest is listed as 5.00-6.50% a year on debit balances — [Alpaca margin docs](https://docs.alpaca.markets/docs/margin-and-short-selling)

### Inferences
- **The first test to run:** re-run B1 with shorts limited to names that are easy to borrow. If most short-leg return disappears, the strategy is much weaker than paper suggests. Muravyev et al. make this the single most decision-relevant check.
- **HTB is uneconomic at this size.** A $1,050 short in a $50 stock is 21 shares, but locates come in 100-share lots with fees rounded to lots, so effective cost is several times the quoted rate. Recommend an ETB-only short universe, and treat a name's move to HTB as an exit trigger for review. Zero fees on ETB names is a real advantage, though ETB lists can change.
- **Margin headroom.** At $100k long and $100k short on $100k equity, the book sits right at the 2x overnight limit, with no room for drift. Any target gross should leave a cushion. This matters only if gross is really near 200%, which I did not check.
- **Low-priced shorts are capital-hungry.** The $5-a-share floor means a $6 short carries about 83% maintenance rather than 30%. A short price floor would reduce both margin use and squeeze risk.
- **Long-short carry may not be neutral.** Short dividends are a cost that paper omits, and interest on short proceeds is not established in the docs I read.

### Gaps
- Whether Alpaca pays any rebate on short proceeds was not found.
- Current Alpaca HTB rates are only quoted per symbol, so no figure is available.
- Historical ETB status for backtesting is likely unavailable for free. A proxy (for example short interest, price or size) would be needed, and its quality is unknown.
- Recent borrow-fee distributions were not sourced; D'Avolio's data is old.
- Recall behaviour at Alpaca was not documented in the pages read.

## Paper-to-live gap on Alpaca

### Takeaway
Paper validates plumbing and signal logic, but not costs. It omits impact, queue position, price improvement, regulatory fees, dividends and borrow fees, so an accurate paper P&L should be treated as a gross-return estimate that needs explicit cost adjustment.

### Cited Findings
- Paper fills use live quotes without going to an exchange, matched to NBBO, and only when marketable; size is not checked against displayed quantity; about 10% of the time an eligible order gets a random partial fill — [Alpaca paper docs](https://docs.alpaca.markets/docs/paper-trading)
- Not modelled: market impact, information leakage, latency slippage, queue position, price improvement, regulatory fees, dividends, and borrow fees (listed as coming soon) — [Alpaca paper docs](https://docs.alpaca.markets/docs/paper-trading)
- Paper-only accounts get IEX data only, and Alpaca itself says paper does not replace live testing — [Alpaca paper docs](https://docs.alpaca.markets/docs/paper-trading)

### Inferences
- **Direction of the bias varies.** Paper omits price improvement, which could make it pessimistic for small marketable orders in liquid names. It also ignores depth, fees, borrow and dividends, which makes it optimistic for thin names and shorts.
- **Passive-first cannot be evaluated on paper.** A limit fills there only once it becomes marketable, without queue or adverse-selection modelling, so any paper-measured "saving" is not evidence.
- **Dividends bias the book.** Long dividends are also missing, so the net effect depends on the legs' yields; adjust using actual dividend data.
- **Shadow accounting is the cheapest fix.** Add to paper P&L a modelled charge for spread, fees, dividends and HTB exclusions, and record arrival mid, NBBO spread and fill for each order.
- **Suggested order of evidence before any real money, each step subject to user approval:**
  1. Log arrival mid and spread on every paper order now.
  2. Backtest ETB-only shorts, bands and a liquidity floor.
  3. Only then run a very small live pilot in a subset of names, comparing time-of-day variants in randomised batches.
- Going live would itself be a departure from the project's paper-only rule, so it is a user decision and not assumed here.

### Gaps
- I found no independent study measuring Alpaca paper versus live slippage. Anecdotes were not used.
- Live price improvement and routing outcomes for Alpaca odd lots are unknown.
- PDT rules were not covered by the docs read. With monthly rebuilds they are unlikely to bind, but this was not verified.
