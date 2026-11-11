# Standard trading manuals and what applies to Sentinel

Reading limits, stated once: **no book was read in full.** Each entry says what was actually read. Book content below is paraphrase of tables of contents, publisher blurbs, reviews and the authors' own papers or blogs. Labels: [TOC], [Blurb, promotional], [Review, secondary], [Author] for the author's own paper or blog, [Primary] for regulators and broker docs, [Sentinel] for repo files read today, [My calc], [Inference].

Facts about Sentinel come from `MODEL_TRIALS.md` (the 5 November entries post-date the earlier report), `services/agents/auto_trader.py`, `shared/broker/alpaca.py` and `shared/utils/factor_model.py`.

## Each manual's practical guidance on signals, combination, sizing, costs, risk and process, and where they disagree

### Takeaway
The manuals agree on the things Sentinel already does: honest validation, costs before signals, modest leverage. They add three checks it may lack: a cost budget in Sharpe units, a factor versus idiosyncratic split of P&L, and a transfer coefficient. Their main disagreements are on model complexity, walk-forward testing, forecast-proportional sizing and factor neutrality. On several of these Sentinel's own registered trials are better evidence than any book.

### Cited Findings

**What was read per text**

| Text | Actually read | Depth |
|---|---|---|
| Grinold and Kahn, *Active Portfolio Management* | Book not read. Prep-site summaries, a CFA Institute appendix, Clarke et al. abstract | Thin; the book's alpha-refinement material not reached |
| Lopez de Prado, *Advances in Financial ML* | Book TOC listing; his paper "The 10 Reasons" (GARP copy): all 10 pitfall and solution headings and its opening pages | Moderate; section bodies not read |
| Chan, *Quantitative Trading*, *Algorithmic Trading* | Search excerpts of contents and chapter openings (O'Reilly pages returned 403 on direct fetch), blog snippets, Goodreads | Thin |
| Carver, *Systematic Trading* | Third-party chapter notes (the7circles.uk), his qoppac post | Good for sizing and speed, secondhand for the book |
| Harris, *Trading and Exchanges* (2002) | Bookseller contents only; a CFA review of his **different** 2015 monograph | Very thin; the CFA PDF did not extract |
| Kissell, *Science of Algorithmic Trading* | Elsevier chapter list and blurb (2013 edition; a newer one exists) | TOC only |
| Isichenko, *Quantitative Portfolio Management* | Partial TOC via search, blurbs, arXiv abstract of "Costly Trading" | TOC plus one abstract |
| Narang, *Inside the Black Box* | TOC of the 2nd and 3rd editions, short reviews | TOC only |
| Jansen, *ML for Trading* | GitHub README of the third-edition repo | README only, not notebooks |
| Paleologo, *Advanced Portfolio Management*; *Elements of Quantitative Investing* | One Substack review of the first; publisher material for the second | Thin; review is itself an excerpt |

**Grinold and Kahn**
- Signals and skill: expected active return is roughly transfer coefficient x IC x root breadth x active risk, where breadth counts independent bets a year — [AnalystPrep](https://analystprep.com/study-notes/cfa-level-2/state-and-interpret-the-fundamental-law-of-active-portfolio-management-including-its-component-terms-transfer-coefficient-information-coefficient-breadth-and-active-risk-aggressiveness/) [Review, secondary].
- Limits: it assumes constant ICs and independent forecasts, rarely true in practice — [CFI](https://corporatefinanceinstitute.com/resources/capital-markets/fundamental-law-of-active-management/) [Secondary]. Critics say it was developed without transaction costs; consecutive forecasts being highly correlated limits breadth — [Hull Tactical](https://www.hulltactical.com/?p=696) [Practitioner].
- Constraints: the transfer coefficient scales the law and equals 1.0 for an unconstrained, well-built portfolio — [Clarke et al., via IDEAS](https://ideas.repec.org/a/taf/ufajxx/v72y2016i6p9-26.html) [Paper abstract]. Optimal long-short extension grows with the risk target and shrinks with shorting cost — [Clarke et al. 2008](https://ideas.repec.org/a/taf/ufajxx/v64y2008i1p16-30.html) [Paper abstract].

**Lopez de Prado**
- His ten pitfalls and fixes: Sisyphus research vs a meta-strategy production line; research through backtesting vs feature importance; chronological sampling vs volume clock; integer differentiation vs fractional; fixed-horizon labels vs triple barrier; learning side and size together vs meta-labeling; non-IID samples vs uniqueness weights and sequential bootstrap; CV leakage vs purging and embargo; walk-forward vs combinatorial purged CV; backtest overfitting vs deflated Sharpe — [López de Prado, GARP copy](https://www.garp.org/hubfs/Whitepapers/a1Z1W0000054x6lUAA.pdf) [Author]. The failure rate claim rests on his stated experience, not a dataset (same source).
- Book structure confirms these chapters — [TOC](https://toc.uni.li/FLMF049641.pdf). A Quantpedia summary of an earlier seven-pitfall version gives no empirical evidence for triple barrier or meta-labeling — [Quantpedia](https://quantpedia.com/why-machine-learning-funds-fail/) [Secondary].
- A page claiming numeric gains from these methods has untraceable figures, so they are excluded — [qgtm-docs](https://qgtm-docs.pages.dev/research/gold_silver/paper_lopez_de_prado_afml) [Do not rely].

**Chan**
- Sizing: *Quantitative Trading* has a money and risk chapter built around the Kelly formula and a Gaussian derivation appendix — [listing](https://www.schweitzer-online.de/en/buch/Chan/Quantitative-Trading/9781394378043/A73491944/) [TOC]. A 2026 revised second edition is listed (same source).
- His blog calls half-Kelly usually the maximum and concedes Kelly cannot protect against risks absent from the historical distribution — [Chan blog](https://epchan.blogspot.com/2010/04/how-do-you-limit-drawdown-using-kelly.html?m=1) [Author].
- *Algorithmic Trading* (2013): strategies split into mean-reverting and momentum; leverage set by Kelly or numerical growth optimisation; stop-losses often a poor way to cap drawdown — [excerpt surfaced by search](https://www.oreilly.com/library/view/algorithmic-trading-winning/9781118746912/OEBPS/9781118746912_epub_c08.htm) [Excerpt, snippet only].
- Process: per one reader, he favours equal in-sample and out-of-sample sizes, accepting at least a third out of sample — [Goodreads](https://www.goodreads.com/book/show/58470404) [Reader review].

**Carver**
- Signals and combination: forecasts on a common scale with an average magnitude of 10 and a cap of 20; a diversification multiplier rescales combined forecasts; handcrafted bottom-up weights assume equal Sharpes because data rarely proves otherwise — [the7circles](https://the7circles.uk/?p=24066) [Secondary]; [the7circles on fitting](https://the7circles.uk/?p=29002) [Secondary].
- Sizing: annual cash volatility target is the top risk decision; full Kelly too aggressive, so half-Kelly; negative-skew systems halve again; backtested Sharpes marked down by a quarter to a third; do not trade if within 10% of target — [the7circles](https://the7circles.uk/?p=24214) [Secondary].
- Costs: cost measured in Sharpe units as round-trip cost times turnover; cap at about one third of expected gross Sharpe — [the7circles](https://the7circles.uk/?p=24243) [Secondary]; the limit nets holding costs too — [qoppac](https://qoppac.blogspot.com/2020/04/how-fast-should-we-trade.html) [Author]. The latter post also finds the square-root breadth gain an upper bound rarely met, in tests on futures.

**Harris**
- The 2015 monograph sorts profit-motivated traders into informed, parasitic and liquidity suppliers; explicit costs are easy to estimate, implicit market-impact costs hard — [CFA review](https://rpc.cfainstitute.org/blogs/enterprising-investor/2016/book-review-trading-and-electronic-markets) [Review, different book].
- The 2002 book's contents include a chapter on measuring liquidity and transaction costs — [bookseller](https://www.worldofbooks.com/products/trading-and-exchanges-book-larry-harris-9780195144703) [TOC]. Mirror PDFs and auto-generated summaries were avoided.

**Kissell**
- Chapters cover transaction cost analysis, market-impact models, I-Star estimation, volatility forecasting, decision frameworks, portfolio algorithms and multi-asset cost indices — [Elsevier](https://shop.elsevier.com/books/the-science-of-algorithmic-trading-and-portfolio-management/kissell/978-0-12-401689-7) [TOC]. Its "improve profitability" and "first author" lines are [Vendor] and unsubstantiated.
- I-Star's exact formula is not public in what I found; MathWorks refers users to Kissell Research Group, a commercial party — [MathWorks](https://www.mathworks.com/help/datafeed/krg.istar.html) [Primary for the referral].

**Isichenko**
- Chapters cover forecasting, forecast combining (mean-variance combination, dimensionality reduction, alpha pool management), and multi-period optimisation under impact and forecast uncertainty — [Nice library catalogue](https://accueil-bibliotheque-nice.edhec.edu/KENTIKA-19252762124910709449-Quantitative-Portfolio-Managem.htm) [TOC]; [O'Reilly listing](https://oreilly.com/library/view/quantitative-portfolio-management/9781119821328) [TOC].
- Costs: with proportional costs, optimal trading has a no-trade band; his paper finds width growing with the square root of cost under a stable forecast, against a cube-root result in stochastic models, and argues modest execution gains can matter a lot because alpha and costs are close to balanced — [arXiv](https://arxiv.org/abs/2110.15239v1) [Author, preprint].

**Narang**
- Framework: alpha, risk and transaction-cost models feeding construction; alphas are theory-driven or data-driven and then blended; cost models are flat, linear or piecewise-linear — [3rd edition TOC](https://exlibris.ch/fr/livres/livres-anglais/rishi-k-narang/inside-the-black-box/id/9781119931898); [2nd edition TOC](https://emu.tind.io/record/178427) [TOC]. A search summary surfaced a reader complaint that it lacks implementation detail, but I could not tie that to a specific page, so treat its "evaluator's map, not a build guide" reading as tentative. Claims of "more effective" optimisation in the third edition are [Blurb, promotional].

**Jansen**
- Third edition (27 chapters, nine case studies): workflow with an "evidence boundary" between tuning and evaluation, IC and quantile analysis, BH-FDR plus deflated Sharpe, backtest as falsification with White's Reality Check, TreeSHAP, then portfolio, cost, risk, live and MLOps chapters. It reports boosting as strongest on tabular data across its own case studies, and has a feedback loop to retrain, pause or retire — [repo](https://github.com/stefan-jansen/machine-learning-for-trading) [Author].

**Paleologo**
- Factor returns can quietly erode a stock book, so factor P&L is split from idiosyncratic P&L, which splits into selection, sizing and timing. Reviewer relays anecdotal evidence of mostly selection skill, about neutral sizing skill and little timing skill; risk is framed as losses large enough to stop you investing — [Substack review](https://chasingthevig.substack.com/p/advanced-portfolio-management-book) [Review]. The book is aimed at fundamental PMs with a quant appendix — [O'Reilly](https://www.oreilly.com/library/view/-/9781119789796) [Blurb].
- *The Elements of Quantitative Investing* (Wiley, 2025) covers building and testing factor models, in-house or vendor, and accounting for transaction costs — [Wiley-VCH](https://www.wiley-vch.de/en/areas-interest/finance-economics-law/the-elements-of-quantitative-investing-978-1-394-26545-9) [Blurb].

**Where they disagree**

| Question | One side | Other side | Sentinel's own evidence [Sentinel] |
|---|---|---|---|
| Complexity | Lopez de Prado: linear econometrics is too simplistic | Carver: handcraft and assume equal Sharpes because data cannot separate them | Boosting ranker in use; simple book beat optimised ones |
| Walk-forward | Lopez de Prado: a pitfall, use CPCV | Jansen: walk-forward CV is core | Purged walk-forward, four calendars, DSR gate |
| Labels | Lopez de Prado: fixed horizon is wrong, use barriers | Jansen covers labels and IC, and his and most finance work use horizons (no barrier evidence found) | 21-session cross-sectional target. Barriers fit single-asset events, not a monthly rank [Inference] |
| Does frequency add Sharpe? | Grinold: root breadth | Carver: an upper bound rarely met | Monthly only |
| Sizing | Fundamental law: weights follow forecasts | Carver: cap forecasts; Paleologo's reviewer: sizing skill about neutral | Equal weight beat calibrated sizing; inverse vol changed Sharpe -0.01 |
| Stops | Triple barrier embeds a stop | Chan: stops often problematic | 15% short stops: Sharpe -0.09 |
| Factor exposure | Paleologo: separate factor from idiosyncratic | Grinold-type books permit any active bet | Beta-neutral legs: Sharpe -0.01 (log-era figures). Separating for *measurement* is untested |
| Turnover control | Isichenko, Carver: no-trade band | Sentinel's registered test | Buffer failed: +0.01 at 10 bps, +0.08 at 20 bps |

### Inferences

Calculations use the account at 50/50 legs, which earns half the 100/100 spread. They are rough, and they mix figures from different runs.

- **Volatility** [My calc]: net +0.92% a month on the spread is +0.46% on the account, about 5.5% a year. At Sharpe 0.49 that implies volatility near 11%.
- **Half-Kelly check** [My calc]: mark 0.49 down by a quarter to a third per Carver, giving 0.33-0.37, so full-Kelly volatility of 33-37% and half-Kelly of 16-18%. Short-squeeze skew argues for halving again, 8-9%. About 11% is inside this range, and the worst month of -25.9% on the spread (about -13% on the account) does not argue for more risk. The manuals give no reason to raise leverage.
- **Speed limit** [My calc]: trading at the observed $171k a rebuild and 10 bps is about 2.05% of equity a year, or 0.18 Sharpe units; on the backtest's 61% turnover it is near 0.13, if that figure counts the same way, which I did not confirm. The flat 0.44% a month borrow estimate on the short leg is about 2.6% of equity a year, or 0.23. Added to B1's 0.47 net in the leg trial, gross is about 0.83-0.88, total cost about 0.36-0.41, or **43-48% of gross**, over Carver's one third. Trading alone is within it, about 15-21%; borrow is what breaches it, and paper cannot observe borrow. The rule is his heuristic from futures, not a proof.
- **Band logic**: Isichenko's band widening with cost matches Sentinel's buffer going from +0.01 at 10 bps to +0.08 at 20 bps. So the failed buffer is conditional on cost, and is worth one pre-registered re-test only if measured cost lands well above 10 bps. It also corrects the earlier report, whose rank-buffer item has since been tested and failed.
- **Transfer coefficient** [My calc]: with uniform rank scores and weights +1 on the 90th-99th percentiles, -1 on the bottom decile, correlation between scores and weights is about 0.68; including the top 1% gives only about 0.70. This ignores risk adjustment. The top-1% carve-out costs little, matching its +0.02 Sharpe.
- **Implied breadth** [My calc]: IC 0.037 and gross Sharpe near 0.85 imply about 530 independent bets a year at TC 1, or about 44 a month, and about 96 a month at TC 0.68, against about 190 names held. The IC figure comes from the earlier report and may be on log returns, so this is an order of magnitude. It suggests positions share common bets, which supports attribution.
- **Volatile shorts**: dropping the most volatile fifth of shorts takes Sharpe from 0.47 to -0.01 [Sentinel]. A single characteristic carrying that much makes a Paleologo-style split the most relevant check in these books [Inference]. It may be alpha, or a systematic low-volatility bet available more cheaply elsewhere.
- **Forecast combination**: the trial gate of Sharpe +0.10 asks data to prove a combination better, which Carver says data rarely can. O4 (two-model rank average) was a near miss on higher monthly return and larger swings. A prior for equal blending would argue for looking again, but that would be changing a gate after results, and all such figures are log-era. Any re-test should be registered first.
- **Trial count** is now at least about 54 if 41 + 9 + 4 is additive [My calc]; Jansen's BH-FDR could sit beside DSR as a second reading.

### Gaps
- From memory, **not verified here**: Grinold's rule that alpha = IC x volatility x score, and Kahn's ideas on signal orthogonalisation. The searches did not retrieve them. Likewise Chan's checklist detail, Harris's order-type and adverse-selection material, and Narang's stated view on optimisers.
- No independent test of triple barrier, meta-labeling, fractional differentiation or volume bars for monthly cross-sectional equity books was found. This was a narrow search, so absence is weak evidence.
- Paleologo's concrete sizing and limit rules, Isichenko's chapter bodies and Kissell's I-Star parameters were not accessible.

## What Alpaca's docs and FINRA or exchange rules require or recommend, beyond the earlier report

### Takeaway
Most rules do not bind at about 100% gross and whole-book monthly orders. PDT has gone. The relevant items are per-share short maintenance floors, Rule 201, timeout handling, wash-trade rejections on flips, and the publication lag on short interest. Regulation SHO's threshold lists are also a possible point-in-time borrow-stress input.

### Cited Findings

Not repeated from the earlier report: `cls` cutoff, paper fill limits, 2x overnight, ETB fee of zero, 100-share locate lots.

- **PDT is replaced.** FINRA Notice 26-10 removes the designation and $25,000 minimum in favour of an intraday margin standard, effective 4 June 2026, with firms allowed to phase in until 20 October 2027; maintenance margin is unchanged; habitual unmet deficits lead to a 90-day restriction on new shorts or debits — [FINRA](https://www.finra.org/rules-guidance/notices/26-10) [Primary]. Alpaca's protection page lists an "Intraday Margin Rule" but its summary did not explain it — [Alpaca](https://docs.alpaca.markets/docs/user-protection) [Primary]. A separate 4210 amendment effective 22 Oct 2026 was noted and not examined — [FINRA](https://www.finra.org/rules-guidance/rulebooks/finra-rules/4210) [Primary].
- **Short maintenance.** FINRA: under $5, greater of $2.50 a share or 100%; at $5 or more, greater of $5 a share or 30% — [FINRA 4210](https://www.finra.org/rules-guidance/rulebooks/finra-rules/4210) [Primary]. Alpaca mirrors it, sets higher long tiers (100% under $2.50, 50% to $6, 30% above), and adds 50% where one security is 70% of equity value with a $100,000+ margin balance — [Alpaca](https://docs.alpaca.markets/docs/margin-and-short-selling) [Primary].
- **Margin calls.** Issued next morning, with no extensions, and Alpaca may liquidate without notice if equity risks going negative. Margin interest is 6.5% (5% elite) on end-of-day debit, /360, with 3 days on Friday balances (same Alpaca page) [Primary].
- **Locates.** HTB locates are single-use, fees non-refundable, covering does not replenish; quotes reserve nothing; endpoints are `/v1/locates/quotes` and `/v1/locates` (same Alpaca page) [Primary].
- **Short order buying power** is reserved at the greater of limit price or 3% above the ask; closes do not restore it until executed — [Alpaca orders](https://docs.alpaca.markets/docs/orders-at-alpaca) [Primary].
- **Timeouts.** A timed-out submission may have reached the market, so do not resend or mark it cancelled until status is confirmed. Client IDs help tracking; the page does not promise idempotency — [Alpaca](https://docs.alpaca.markets/docs/working-with-orders) [Primary].
- **Wash-trade protection** rejects with 403 orders that could interact with your own opposing orders, including in paper; bracket, OCO and trailing stop orders are exempt — [Alpaca](https://docs.alpaca.markets/docs/user-protection) [Primary]. Also there: closing-only restriction if a position exceeds 600% of equity.
- **Paper** partially fills an eligible order at random 10% of the time and does not check size against NBBO liquidity. Accounts are no longer reset — [Alpaca](https://docs.alpaca.markets/docs/paper-trading) [Primary].
- **Rule 201.** A 10% one-day fall triggers a short-sale price test for the rest of that day and the next. **Rule 204** requires close-out of short fails by the open of the settlement day after settlement. **Threshold securities** have fails of at least 10,000 shares and 0.5% of outstanding for five consecutive settlement days, with forced close-out after 13; being listed is not in itself negative — [SEC](https://www.sec.gov/investor/pubs/regsho.htm) [Primary].
- **Short interest timing.** Twice monthly positions, reported within two business days and published on the seventh business day after the settlement date — [FINRA Rule 4560](https://www.finra.org/rules-guidance/rulebooks/finra-rules/4560) [Primary]; [FAQ](https://www.finra.org/filing-reporting/regulatory-filing-systems/short-interest/faq) [Primary]. Listed-stock figures reach the public through the listing exchanges per a 2021 notice, which may have changed — [Notice 21-19](https://www.finra.org/rules-guidance/notices/21-19) [Primary].

### Inferences
- These rules bind on brokers and firms. As a customer Sentinel feels them through rejected orders, margin and borrow. Paper may not enforce all of them, and I did not find whether paper enforces Rule 201.
- At legs of about $50k, shorts of about $525: a $6 short carries the $5 floor, about 83% maintenance rather than 30% [My calc]. Measure low-priced shorts' share of the book; the sum may matter little but differs from what a flat 30% assumes.
- Rule 201 most probably affects shorts in names that just fell 10%: these may fill late or not at all at rebuild. A marketable short would be limited to above the national best bid [Inference from the rule's purpose; exact mechanics not read].
- FINRA's lag matters for the "days to cover" filter in the leg trial, which gave +0.05. If the test used settlement dates rather than publication dates, it has look-ahead. I did not check this.
- Threshold lists offer a dated, free record of settlement stress where the backtest has only today's borrow file [Inference]. List retrieval and history depth were not checked, and fails on the list overlap with the earlier report's fails-to-deliver idea.

### Gaps
- Rate limits, Rule 605 and best-execution (Rules 5210, 5310) were not retrieved; they bind on firms, so their value here looks low.
- Whether Alpaca credits interest on short proceeds, and how a recall or buy-in reaches the customer, was not on the pages read. This matters for real capital.
- Alpaca's timetable for adopting the intraday standard was not found. Secondary articles on PDT conflict on details, so only FINRA is used here.

## Which practices Sentinel is plausibly missing

### Takeaway
The research process is already ahead of most of these checklists: purging, embargo, DSR, registered rules, a retirement CUSUM and simple-return scoring. The likely gaps are narrow: one verified order-handling defect, then factor attribution, a cost budget, label-overlap weighting and short-sale restriction handling.

### Cited Findings

**Already present** [Sentinel], and removing the need for book advice: purged walk-forward with 21-day embargo and DSR gate (`MODEL_TRIALS.md` lines 11, 34-38), registered rules and CUSUM retirement (lines 179-193), multiple-calendar results, kill switch and thin-score hold (`auto_trader.py`), marketable limits at a 0.5% offset (`auto_trader.py` lines 415-424). The earlier report's items on writing retirement rules and testing a rank buffer have since been done.

| # | Practice | Source | Evidence for the gap | Suggested action | Approval |
|---|---|---|---|---|---|
| 1 | **Confirm order status after a timeout before calling it failed** | [Alpaca](https://docs.alpaca.markets/docs/working-with-orders) [Primary] | **Verified in code.** `_send` calls `submit_order` without a `client_order_id` and logs any exception, including the 10-second timeout, as `rejected` (`auto_trader.py` lines 422-430; `alpaca.py` line 218). A timed-out order that reached the market would be recorded as rejected. Later position reconciliation may correct the position; I did not trace it. Also conflicts with the repo's rule against reporting wrongly | Deterministic client ID by date, symbol and side; on exception look up by ID | Yes, execution |
| 2 | Factor vs idiosyncratic attribution of B1 | [Paleologo review](https://chasingthevig.substack.com/p/advanced-portfolio-management-book) | No attribution in the trial record read; `exposure_lens_job` exists and was not read, so check it first. The volatile-short result raises the stakes | Regress B1 returns on market, size, value, momentum, low-vol factors, research only | No |
| 3 | Cost budget in Sharpe units | [Carver](https://the7circles.uk/?p=24243) | Estimated 43-48% of gross including borrow, on rough inputs | Add to the shadow-cost ledger: cost share of gross, with one third as a review trigger, not a limit | No |
| 4 | Weights for overlapping labels | [López de Prado](https://www.garp.org/hubfs/Whitepapers/a1Z1W0000054x6lUAA.pdf) | Grep for `sample_weight` and `weight=` in `factor_model.py` and `factor_engine.py` found none; the ranker has `lambdarank` (`factor_model.py` line 946). A different weighting route or sparse sampling was not ruled out. Purging fixes leakage, not sample redundancy | One logged trial: uniqueness weights or non-overlapping train dates | No |
| 5 | Rule 201 awareness | [SEC](https://www.sec.gov/investor/pubs/regsho.htm) | Grep for uptick and short-sale restriction terms found nothing in the trader or broker files | Log failed shorts on names down 10% the previous or same day; adjust only if frequent | Yes, if logic changes |
| 6 | Flip wash-trade check | [Alpaca](https://docs.alpaca.markets/docs/user-protection) | `_await_fill` returns once its wait expires, and the open may then conflict with a working close; caller not traced | Count 403s on rebuilds; audit | Yes, if logic changes |
| 7 | Explain model drift with TreeSHAP or importance | [Jansen](https://github.com/stefan-jansen/machine-learning-for-trading) [Author]; [López de Prado](https://www.garp.org/hubfs/Whitepapers/a1Z1W0000054x6lUAA.pdf) | `shap` and `feature_importance` not found in those files; a dependency check is needed, since the repo avoids new packages | Monthly gain importance from LightGBM's own API, compared across folds | No |
| 8 | Transfer-coefficient and implied-breadth line in reports | [Clarke et al.](https://ideas.repec.org/a/taf/ufajxx/v72y2016i6p9-26.html) | Not seen in the record read | Two numbers per trial | No |
| 9 | Publication-date short interest | [FINRA](https://www.finra.org/filing-reporting/regulatory-filing-systems/short-interest/faq) | Not checked | Audit days-to-cover dates | No |
| 10 | Low-priced short measurement | [FINRA 4210](https://www.finra.org/rules-guidance/rulebooks/finra-rules/4210) | Not seen in the record read | Report margin used against a flat 30% assumption | No |

**Not worth adopting at this stage** [Inference]: volume bars and fractional differentiation (intraday or single-series tools; monthly cross-sections rank features across names); triple barrier and meta-labeling as model changes (no evidence found, and a size model invites more trials; equal weighting beat sizing variants); I-Star and multi-period optimisers (impact is near nil at about $550 orders, per the earlier report); Kelly-based or forecast-scaled sizing (sizing changes need approval and Sentinel's trials argue against them); Carver's 10% inertia as a new feature (already tested in spirit by the failed buffer).

### Inferences
- The manuals mostly assume the buy-side's main problem is finding or scaling alpha. Sentinel's is proving that a weak signal survives borrow, and few of the books as read address that directly. Muravyev-type evidence in the earlier report outranks them on this.
- Items 1 and 6 are the most concrete. Paper accounts do not make them urgent financially, but they corrupt the fill record on which the forward test and the retirement rules depend.
- Ranking is my judgement.

### Gaps
- Code checks were targeted greps and short reads, not an audit; absences in row 4, 5, 7 cover only the named files.
- Reconciliation after a timed-out order, and how trials count "turnover 61%", were not traced.
- Whether the 0.49 figure already includes dividends paid on shorts was not confirmed in the entry read.
