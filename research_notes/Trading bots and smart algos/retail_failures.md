# Realistic retail algorithmic trading outcomes and how trading bots fail

Evidence labels used below:
- **[MEASURED]** comes from a regulator's or researchers' dataset.
- **[ORDER]** is a finding in a regulatory order or staff report.
- **[REPORTED]** is press or a fund's own disclosure, not independently audited.
- **[VENDOR]** is a seller or broker claim, or material from an interested party.
- **[ANECDOTE]** is a single case or an unverified comment.

Verification levels differ. The Knight order and the SEC GameStop report were read as primary text. The FINRA notice was read through a page-fetch summary. Abstract-level items and anything marked "via search summary" were not read in full, so check them against the original before quoting exact figures.

## What do brokerage data and marketplace evidence show about retail, day-trader and bot outcomes?

### Takeaway
Measured base rates are consistently poor: roughly 80-97% of active retail or day traders lose money, depending on the market and the holding requirement, and only about 1% earn a meaningful or persistent profit. I found no audited dataset of retail algo or bot users, so any claimed retail-bot success rate is unmeasured.

### Cited Findings
**Measured: retail and day traders**
- [MEASURED] US households, 1991-1996: 66,465 households at a large discount broker. Those trading most earned 11.4% a year against 17.9% for the market. — [Barber & Odean](https://faculty.haas.berkeley.edu/odean/papers/returns/returns.html) (via search summary)
- [MEASURED, disputed framing] A later comment argues the average results do not show that trading harms the average investor's wealth. The headline figure concerns the heaviest traders. — [SEC comment file citing the debate](https://sec.gov/comments/s7-12-23/s71223-361219-887502.pdf) (attribution of this comment to this URL came from a search summary and is not confirmed)
- [MEASURED] Brazil mini-Ibovespa futures, first-time day traders 2013-2015: 19,646 individuals in the regulator's data, of whom 1,551 persisted more than 300 days. 97% of the persisters lost money, and about 1.1% earned above minimum wage. About 0.5% earned above a bank teller's starting pay (the abstract summary says 0.4%, a small discrepancy between sources). The authors found no evidence of learning. — [Chague, De-Losso & Giovannetti](https://econpapers.repec.org/RePEc:spa:wpaper:2019wpecon47); [Quantpedia summary](https://quantpedia.com/?p=4771)
- [MEASURED] Taiwan, complete exchange data 1992-2006: less than 1% of day traders predictably and reliably earn positive abnormal returns net of fees. — [Barber, Lee, Liu & Odean, J. Financial Markets 2014](https://econpapers.repec.org/RePEc:eee:finmar:v:18:y:2014:i:c:p:1-24)
- [MEASURED] An earlier version of the Taiwan work: over 80% of day traders lose money after costs. — [CXO summary](https://www.cxoadvisory.com/individual-investing/do-day-traders-make-money/) (secondary)
- [MEASURED] India equity derivatives, FY22-FY24: 93% of about 11.3 million individual traders lost money, with a combined net loss of about Rs 1.81 lakh crore. The average loss among loss-makers was about Rs 2 lakh. About 1% earned more than Rs 1 lakh after costs, and more than 75% of loss-makers continued after two losing years. — [SEBI study via TaxGuru](https://taxguru.in/sebi/sebi-study-shows-93-percent-individual-fo-traders-incur-losses.html); [Taxmann](https://www.taxmann.com/post/blog/93-of-1-crore-fo-traders-saw-rs-2l-avg-loss-while-top-3-5-faced-rs-28l-avg-loss-incl-costs-sebi/)
- [MEASURED] Same SEBI study: 97% of FPI profits and 96% of proprietary-trader profits in FY24 came from algo entities. This describes institutions, not retail algos. — [Moneylife](https://www.moneylife.in/article/93-percentage-of-individual-traders-lost-rs18-lakh-crore-in-equity-fo-in-past-3-years-sebi/75210.html). [ANECDOTE] One forum commenter says broker auto square-offs inflate the algo count; unverified. — [TradingQnA](https://tradingqna.com/t/sebis-latest-analysis-of-profits-losses-in-the-equity-derivatives-segment-fy22-fy24/173596)
- [MEASURED, weak aggregation] EU CFD accounts: ESMA mandates disclosure of the percentage of retail accounts losing money. — [ESMA](https://www.esma.europa.eu/node/86076). A 2019 third-party tally of 28 providers found 54-83% losing, averaging 76%. A broker-comparison site gives about 72% with unclear method. These are leveraged products and dated. — [BrokerRank](https://brokerrank.net/research/retail-loss-rates); [Investingoal](https://investingoal.com/esma-broker-client-success-rate-stats) (exact source-to-figure mapping came from a search summary)

**Closest evidence for retail-built algos**
- [MEASURED] Quantopian: 888 US equity algorithms with at least 6-12 months of true out-of-sample data. Backtest Sharpe had little predictive value (R² < 0.025), and more backtesting was associated with a bigger gap between backtest and live results. — [Quantpedia on Wiecki et al.](https://quantpedia.com/quantopians-academic-paper-about-in-vs-out-of-sample-performance-of-trading-alg/); [HedgeFundAlpha](https://hedgefundalpha.com/strategies/comparing-backtest-sample-performance-large-cohort-trading-algorithms/). Caveat: the out-of-sample window is short, and the authors were the platform's staff. One summary gives R² < 0.25, which looks like a typo for 0.025.
- [MEASURED, simulated] Robo-style portfolios simulated from 20,622 Belgian brokerage accounts (2003-2012) mainly reduced drawdowns, and results are gross of fees. An Indian broker's portfolio optimizer helped the least diversified investors, while the most diversified paid more with no performance gain. These are optimizers, not trading bots. — [CXO Advisory](https://cxoadvisory.com/?p=32726); [FDIC paper](https://fdic.gov/promises-and-pitfalls-robo-advising.pdf-0) (mapping from search summary)

**Marketplace and signal sellers**
- [VENDOR] MQL5 product and seller pages are marketing, and I found no independent evidence that expert advisors or signals are profitable. Anti-fraud sites warn that such "evidence" can be faked. — [MQL5 example](https://www.mql5.com/en/market/product/85076); [ForexFraud](https://forexfraud.com/?p=5166) (itself not a neutral source)
- [VENDOR] Articles of the "AI bots vs humans: what the data shows" type turned up in search and are not usable as evidence. — [Example](https://www.pulse.bot/ai/news/ai-trading-bots-vs-human-traders-in-2026-what-the-data-actually-shows-8ce19b49-051d-4713-976d-667b68aa3eee/)

### Inferences
- These base rates mostly describe discretionary, high-turnover or leveraged trading. A nightly long-short factor book is a different activity, so "97% lose" is not its predicted outcome. The useful lessons are that costs and turnover dominate, persistence is rare, and people keep going despite losses.
- The Quantopian result is the most relevant one for Sentinel: a good backtest is weak evidence, and more trials make it weaker. Paper-trading results and a count of trials matter more than the best backtest Sharpe.
- Survivorship cuts both ways in Chague. Requiring 300+ days selects persisters, and shorter-tenure traders were more often profitable in the sample, but the short-tenure group's records are also too brief to separate skill from luck.
- The SEBI algo profit share does not imply that retail users of algos profit. It shows profits accrue to well-resourced firms.

### Gaps
- No audited, broker-sourced study of retail algo or bot users was found. This is the central gap, and the search here was limited rather than exhaustive.
- No independent study of signal-seller or EA-marketplace subscriber returns was found. Bailey & López de Prado's PBO paper was not read directly; it is known here only by reference from the Quantopian coverage.

## What documented failures exist, and what controls would have helped?

### Takeaway
Two families recur: operational errors, where a deployment or control gap creates unintended positions in minutes (Knight), and crowding or deleveraging events, where a working strategy loses because similar books unwind together (2007, 2020). Software controls address the first family. Only exposure limits and drawdown tolerance address the second.

### Cited Findings
**Knight Capital, August 1, 2012 [ORDER]** — all from [SEC Release 34-70694](https://www.sec.gov/litigation/admin/2013/34-70694.pdf)
- 212 small retail orders led to millions of child orders, over 4 million executions in 154 stocks and more than 397 million shares in about 45 minutes. Knight ended up about $3.5B net long in 80 stocks and about $3.15B net short in 74, and lost more than $460M.
- Root causes:
  - A technician did not copy new code to one of eight servers.
  - There was no second-person review and no written procedure requiring one.
  - A repurposed flag activated dead Power Peg code, unused since 2003.
  - That code's fill-tracking function was moved in 2005 and never retested, so the server kept sending orders regardless of fills.
- Missed warning: 97 automated "Power Peg disabled" emails went out before the open. They were not designed as alerts and generally were not reviewed.
- Control gaps:
  - No comparison of orders leaving the router with those entering it.
  - No procedure to halt the system on aberrant activity.
  - The 9.5% price collar did not apply to these opening-auction orders.
  - Position limits ignored exposure from outstanding orders.
  - A $2M limit on the 33 Account was not linked to any automated control, and firm-wide capital thresholds did not block orders.
- Knight paid a $12M civil penalty under the order.

**August 2007 quant quake [MEASURED, simulated and transactions data]**
- Unprecedented losses at quant long-short equity funds in the week of August 6. The authors attribute these to deleveraging through July and early August, plus a temporary withdrawal of market-making capital from August 8. There were two unwinds: August 1 from 10:45 to 11:30am, and August 6 from the open to about 1:00pm. Both began in financials and were long book-to-market and short earnings momentum. — [Khandani & Lo, NBER w14465](https://www.nber.org/papers/w14465)
- The authors later describe parts of their hypotheses as speculative and based on indirect evidence. — [Follow-up via search summary](https://www.newyorkfed.org/medialibrary/media/research/conference/2010/cb/Lo1.pdf)

**2020 quant drawdown [REPORTED unless noted]**
- Renaissance's public funds lost 19-31% in 2020, and the firm's filing said its models "have not performed as expected". AQR's Equity Market Neutral fund was down 19% by mid-November. — [Bloomberg](https://www.bloomberg.com/news/articles/2020-11-17/renaissance-two-sigma-see-losses-as-quant-giants-navigate-chaos)
- AQR's own factor note: value fell 4.23 standard deviations from January to October 2020. In November to mid-December value gained 1.78, but momentum fell 2.62 and quality 2.95, so the rebound also hurt. — [The Idea Farm reproduction](https://theideafarm.com/portfolio-management/a-gut-punch/) (secondary reproduction of fund-authored material)
- Robeco dates a quant equity crisis from June 2018 to August 2020, and notes that momentum rather than value crashed in 2009. — [Robeco](https://robeco.com/zh-hk/insights/2021/02/the-quant-equity-crisis-of-2018-2020-cornered-by-big-growth) (manager research)
- One quant fund reportedly shrank 92% from its 2018 peak, which shows the business risk of a long drawdown. — [Bloomberg](https://www.bloombergquint.com/markets/quant-fund-shrinks-92-from-2018-peak-in-factor-investing-crisis)
- Kakushadze's paper explains why dollar-neutral stat-arb drew down in the COVID selloff, showing that dollar neutrality is not risk neutrality. — [Quant Bust 2020](https://arxivlens.com/PaperView/ViewPdf/quant-bust-2020-8042-2057682)

**Flash crash and market-structure events**
- May 6, 2010: the joint SEC-CFTC report describes a large algorithmic sell program in an already stressed market. — [Waterstechnology](https://www.waterstechnology.com/trading-tools/1740190/sell-algo-fuels-may-6-flash-crash); [SEC announcement](https://www.sec.gov/news/press/2010/2010-179.htm) (announcement only, no findings in text). A stock-level circuit breaker followed in June 2010. — [Plan Adviser](https://www.planadviser.com/regulators-release-flash-crash-probe-report/) (mapping from search summary)
- August 24, 2015 [ORDER-level staff note, via secondary]: 1,278 LULD halts, mostly in ETPs (327 ETPs affected), with 60% of halts occurring while trading up from lows. — [ETF.com](https://www.etf.com/sections/blog/key-findings-on-secs-aug-24-data-dump). ETF pauses reflected both a volume spike and a pullback in liquidity supply. — [SEC DERA](https://www.sec.gov/about/divisions-offices/division-economic-risk-analysis/staff-papers-analyses/feb2016-dera-white-paper-etf-volatility)

### Inferences (control mapping for a small automated book)
| Failure seen | Control |
|---|---|
| Stale or partial deploy (Knight) | One deployment artifact, version check at startup across all workers, second review, delete dead code and flags rather than repurposing them |
| Orders ignoring fills (Knight) | Reconcile intended, submitted and filled quantity against broker positions before each batch; stop if they diverge |
| Limits not linked to order entry (Knight) | Pre-trade gross, net, per-name and order-count caps that count open orders and block submission automatically |
| Ignored warnings (Knight) | Treat unexpected errors as alerts with a named action, not as log noise |
| 45 minutes to diagnose while still connected (Knight) | Kill switch first, diagnose second; include cancel-all and a tested flatten procedure |
| Bad or missing data | Refuse to trade if inputs are stale, the universe shrinks sharply, or the target book changes implausibly from the last one |
| Crowded unwinds (2007, 2020) | No code fix; size so a large multi-sigma factor loss is survivable, and pre-commit a drawdown rule |
| Liquidity gaps and halts (2010, 2015) | Avoid unconditional market orders and stops at the open or in thin conditions; handle halted names explicitly |

- Automation errors scale with speed. Knight's cost came from 45 minutes of continued connection, and a nightly rebalance has far fewer orders per minute. Its typical failure would be one wrong book executed in full, so a pre-trade plausibility check matters most.
- This mapping is my inference from the cases, not a regulator's list. It is also relevant to CLAUDE.md's rule that changes to sizing, caps, kill-switch or execution logic need explicit approval.

### Gaps
- Quantified 2007 factor and fund loss figures were not retrieved. Summaries say only "double-digit" for many managers ([NBER](https://www.nber.org/papers/w14465)).
- The primary May 6, 2010 report text was not read, so sell-program size and index figures are omitted here.
- No retail-specific API or data-error incident dataset was found. Such stories are anecdotal, and none is cited here.

## What do regulators advise on automated trading controls, and what do they warn about bot scams?

### Takeaway
FINRA and the SEC set out concrete control practices written for firms, not individuals, but they transfer well to a solo system. The CFTC warns directly that AI does not make bots money machines and treats guaranteed returns as a fraud signal.

### Cited Findings
- [Guidance] FINRA Regulatory Notice 15-09 (March 2015) suggests practices in five areas: — [FINRA](https://www.finra.org/rules-guidance/notices/15-09)
  - Risk assessment: holistic review of trading activity through a cross-disciplinary committee.
  - Development: change management, redundant validation before deploy, code archiving, strategy summaries, a kill switch, phased pilots and real-time monitoring after new code.
  - Testing: independent QA and an environment segregated from production.
  - Trading systems: controls and reconciliation to catch unintended results, recorded risk parameters, message-volume thresholds and limits on overriding controls.
  - Compliance: surveillance of algorithm interactions such as self-trades and wash sales.
- [Rule] SEC Rule 15c3-5 requires broker-dealers with market access to keep controls that prevent erroneous orders and orders over pre-set credit or capital thresholds, with regular review and annual CEO certification. — [SEC Knight order](https://www.sec.gov/litigation/admin/2013/34-70694.pdf)
- [Warning] CFTC advisory, January 25, 2024, "AI Won't Turn Trading Bots into Money Machines": fraudsters use AI hype to sell automated algorithms and signals with unreasonably high or guaranteed returns, and AI cannot predict the future or sudden market changes. It advises researching the seller's background and running reverse image searches. — [CFTC](https://www.cftc.gov/LearnAndProtect/AdvisoriesAndArticles/AITradingBots.html); [Release 8854-24](https://www.cftc.gov/PressRoom/PressReleases/8854-24)
- [Enforcement] The advisory cites Cornelius Steynberg, whose bot scheme promised at least 10% monthly. A secondary source reports an order to pay over $3.4B. — [CFTC](https://www.cftc.gov/LearnAndProtect/AdvisoriesAndArticles/AITradingBots.html) (the $3.4B figure comes via search summary; verify against the CFTC order)
- [Warning] FINRA and the SEC reportedly issued a parallel alert, "Artificial Intelligence (AI) and Investment Fraud", around the same time. — [Orrick](https://infobytes.orrick.com/2024-02-02/securities-regulators-issue-guidance-and-rfc-ai-trading-scams/) (secondary)

### Inferences
- These rules bind registered firms and do not directly apply to an individual paper account. Treat them as a checklist rather than a legal requirement. Whether going live creates any obligation was not researched and needs its own check.
- FINRA's list maps closely to the Knight failures. Phased rollout also supports moving from paper to a small live pilot rather than a full-size launch.

### Gaps
- No FIA market-access or automated-trading recommendations were retrieved, so nothing is claimed about FIA's content.
- CFTC Regulation AT and any later automated-trading rule status were not researched.
- The SEC's own AI-fraud alert and its "AI washing" enforcement release (reported as 2024-36) were not confirmed on sec.gov.

## How big is short-squeeze risk for a book with 95 equal-weight shorts?

### Takeaway
Equal weighting across 95 names limits, but does not remove, single-name squeeze risk: at about 1.05% of the short book per name, a GME-size move costs roughly 17-28% of short-book notional if the name is not trimmed. The larger risk is correlated: short baskets rallying together while long hedges degross, which Goldman data suggest happened in January 2021.

### Cited Findings
- [ORDER, staff report] GME closed at $347.51 on January 27, 2021, more than 1,600% above its January 11 close. Its intraday high-to-low rise was about 2,700% from January 8 to January 28, followed by a fall of over 86% by the end of the first week of February. — [SEC staff report](https://www.sec.gov/files/staff-report-equity-options-market-struction-conditions-early-2021.pdf)
- [ORDER, staff report] GME's price rose 71% in about three hours on January 22 alone. — [SEC staff report](https://www.sec.gov/files/staff-report-equity-options-market-struction-conditions-early-2021.pdf)
- [ORDER, staff report] GME short interest was exceptional: 122.97% of float in January 2021 and a 109.26% high against shares outstanding on December 31, 2020. Few stocks exceed 50% on a given date, and large non-financial stocks are often under 2.5%. — [SEC staff report](https://www.sec.gov/files/staff-report-equity-options-market-struction-conditions-early-2021.pdf)
- [ORDER, staff report] The staff found buy-to-cover was a small fraction of overall buy volume, and "positive sentiment, not the buying-to-cover" sustained the rise. Prices stayed high after covering would have waned. — [SEC staff report](https://www.sec.gov/files/staff-report-equity-options-market-struction-conditions-early-2021.pdf)
- [ORDER, staff report, via press] Staff believed hedge funds broadly were not significantly affected by GME and other meme stocks. — [Financial Planning](https://www.financial-planning.com/articles/sec-gamestop-report-debunks-conspiracies-backs-gensler-plan)
- [REPORTED] Melvin lost about $7B in January 2021, more than half of assets, and took about $2.75B of infusions from Citadel and Point72. Its loss was reported as 49% for Q1. It was later reported to be under SEC inquiry over risk controls, at an early stage. — [BNN Bloomberg](https://ampvideo.bnnbloomberg.ca/melvin-hedge-fund-probed-by-sec-over-risk-controls-client-communications-1.1805065.amp.html) (figures via search summary; mapping to this page unconfirmed). Melvin was a concentrated discretionary fund, not comparable to a 95-name book. The commonly cited 53% figure was not confirmed.
- [VENDOR-ish sell-side, via press] Goldman: its most-shorted basket rallied 98% in three months and had its best month since at least 2008, and David Kostin said the January squeeze showed how leverage plus crowding creates risk. — [Fox Business](https://foxbusiness.com/markets/reddit-traders-could-keep-terrorizing-wall-street-goldman-sachs); [Yahoo](https://uk.sports.yahoo.com/news/game-stop-traders-may-inflict-even-more-pain-on-wall-street-goldman-112554322.html) (figure-to-page mapping via search summary)
- [REPORTED] "Biggest short squeeze in 25 years" and "fastest degrossing since 2009" appear only in reposted coverage of Goldman. — [Business Insider India](https://businessinsider.in/stock-market/news/goldman-sachs-biggest-short-squeeze-in-25-years-caused-hedge-funds-to-de-gross-at-fastest-rate-since-2009/amp_articleshow/80632405.cms). Goldman later said short-covering in early 2023 was faster still. — [Kitco/Reuters](https://www.kitco.com/news/off-the-wire/2023-02-06/update-1-hedge-funds-caught-bigger-squeeze-2021-meme-stock-frenzy). So January 2021 should not be called the record.

### Inferences
My arithmetic, assuming position weight of 1/95 = 1.05% of short-book notional, no trimming and no stop:

| Scenario | Loss, % of short-book notional |
|---|---|
| One short up 100% | 1.05% |
| One short up 1,600% (GME close-to-close) | 16.8% |
| One short up 2,700% (GME intraday extreme) | 28.4% |
| Five shorts up 100% each | 5.3% |
| Whole short book up 20% | 20% before any long offset |
| Short book behaving like Goldman's basket, up 98% | about 98% before long offset; an extreme upper bound, as the basket was selected for high short interest |

- Convert these to NAV by multiplying by short exposure as a share of NAV. Margin rises as losses grow, so forced covering could bite before the final figures above.
- A short's weight grows as it rises, which is the opposite of a long. Daily or weekly trimming back to target caps some of the loss, but buying into a gap does so at bad prices and contributes to the squeeze. An intraday spike between nightly rebalances goes unmanaged.
- A GME-style event needs extreme conditions. A cheap practical filter would be to exclude or cap names with very high short interest, days-to-cover or borrow cost, and to report the book's weighted short interest. An untested inference is that a factor model's "extreme" short bucket may overlap with heavily shorted names, because weak-momentum and low-quality stocks attract short sellers. Measuring that overlap on B1 would answer this; it was not measured here.
- A long side does not fully hedge. During degrossing, crowded longs can fall while shorts rise, as in 2007's unwind and January 2021's reported degrossing, so both legs can lose together.
- Paper trading hides this risk. It may not model borrow availability, fees, recalls or forced buy-ins, so paper short returns should be treated as an optimistic estimate.

### Gaps
- No exact factor return for high-short-interest names in January 2021, and no primary Goldman note, was retrieved.
- Alpaca's actual short rules (hard-to-borrow, recall, margin, paper-account modelling) were not checked and should be read directly.
- B1's actual gross exposure, share of NAV in shorts, and overlap with high short-interest names were not examined, so the table is not stated in NAV terms.
- Borrow-fee drag on an equal-weight short book was not quantified.
