# Reinforcement learning and LLM or news-driven trading agents (state as of Nov 2026)

Source-quality note: arXiv papers are preprints unless a venue is stated. Figures for FinMem, TradingAgents, FINSABER, StockBench and Lopez-Lira and Tang were read from the paper HTML through a summarising fetch tool, not checked by hand against the PDFs. The other arXiv items were read at abstract level only. Alpha Arena figures come from secondary or commercial sources because nof1.ai itself was not read.

## How do TradingAgents, FinMem, FinRL, FinGPT and similar work, and how were they validated?

### Takeaway
The headline agent papers rest on tests of about 3 to 6 months on a handful of hand-picked large-cap or high-profile stocks, without stated trading costs. The strongest independent re-test (FINSABER, KDD '26, 20 years, survivorship-aware universe) found the reported advantage largely vanishes, with no significant alpha and significant underperformance against buy-and-hold.

### Cited Findings
**TradingAgents (multi-agent LLM "trading firm")**
- Uses quick models (gpt-4o-mini, gpt-4o) for summarising and retrieval and a deep model (o1-preview) for analysis and decisions, at 11 LLM calls and 20+ tool calls per prediction — [TradingAgents v7](https://arxiv.org/html/2412.20138v7)
- Backtest ran 1 Jan to 29 Mar 2024. The setup names Apple, Nvidia, Microsoft, Meta and Google, while the results table reports AAPL, GOOGL and AMZN — [TradingAgents v7](https://arxiv.org/html/2412.20138v7)
- Reported cumulative return, Sharpe, max drawdown: AAPL 26.62%, 8.21, 0.91%; GOOGL 24.36%, 6.39, 1.69%; AMZN 23.21%, 5.60, 2.11% — [TradingAgents v7](https://arxiv.org/html/2412.20138v7)
- No commissions, slippage or spreads are stated. The authors attribute the three-month window to LLM cost and say the high Sharpe may reflect few pullbacks in the period. They claim no look-ahead because agents use only data available up to each day — [TradingAgents v7](https://arxiv.org/html/2412.20138v7)
- A 2025 survey says Sharpe values above 3 are exceptional and warrant scrutiny for overfitting and data snooping — [ACM Computing Surveys](https://dl.acm.org/doi/full/10.1145/3733714)

**FinMem (layered memory plus profile and decision modules)**
- Uses GPT-4-Turbo, daily buy, sell or hold of a single share, test window 6 Oct 2022 to 10 Apr 2023 on TSLA, NFLX, AMZN, MSFT and COIN. The paper does not mention transaction costs — [FinMem](https://arxiv.org/html/2311.13743)
- Reported TSLA: +61.78%, Sharpe 2.68, against buy-and-hold at -18.63%. FinMem's training window was only Aug 2021 to Oct 2022 — [FinMem](https://arxiv.org/html/2311.13743)
- Independent re-test on the same TSLA window gave Sharpe 0.927 and return 19.94% (GPT-4o-mini), and Sharpe 0.404 and return 5.31% (GPT-4o), with drawdowns of -30% and -36% — [FINSABER](https://arxiv.org/html/2505.07078)

**FINSABER (re-test framework, published at KDD '26)**
- Extended tests to 2004-2024, using historical S&P 500 constituents including delisted names, reselected each rolling window, with commission of $0.0049 per share and a $0.99 minimum — [FINSABER](https://arxiv.org/html/2505.07078)
- Buy-and-hold significantly beat both FinMem and FinAgent (p as low as 3.0e-6), and neither LLM agent had significant alpha (all p above 0.34). Example: Sharpe 0.703 for buy-and-hold against 0.241 for FinAgent under Volatility Effect selection — [FINSABER](https://arxiv.org/html/2505.07078)
- LLM agents were too conservative in bull markets and too aggressive in bear markets. The authors conclude trend detection and regime-aware risk control matter more than model complexity — [FINSABER](https://arxiv.org/html/2505.07078)
- The authors admit look-ahead is not fully eliminated, since pre-trained models may have seen overlapping data, and argue that any leakage would favour the LLMs — [FINSABER](https://arxiv.org/html/2505.07078)
- Default execution is next open, which avoids same-day close leakage — [FINSABER README](https://github.com/waylonli/FINSABER/blob/main/README.md)

**FinRL (open RL library)**
- Treats transaction cost as a user-set parameter and includes buy-and-hold among its benchmarks — [FinRL](https://arxiv.org/pdf/2011.09607). FinRL-Meta gives 0.1% per trade as an example cost — [FinRL-Meta, NeurIPS 2022](https://papers.neurips.cc/paper_files/paper/2022/file/0bf54b80686d2c4dc0808c2e98d430f7-Paper-Datasets_and_Benchmarks.pdf)
- These are infrastructure papers, so a user's result reflects that user's settings. This is my reading of the sources above, not a claim they make.

**FinGPT (open finance LLM)**
- A data-curation pipeline with low-rank adaptation, proposed as an open alternative to BloombergGPT. Algorithmic trading is listed only as a potential application, and no trading validation appears on the abstract page. The full PDF was not reviewed — [FinGPT](https://arxiv.org/abs/2306.06031)
- FinMem used FinGPT as an LLM baseline, so it has been traded in others' short tests — [FinMem](https://arxiv.org/html/2311.13743)

**StockBench (neutral benchmark)**
- 82 trading days, Mar-Jun 2025, top 20 DJIA stocks, 13 models, and no costs or slippage modelled — [StockBench](https://arxiv.org/html/2510.02209)
- Returns ranged from -2.8% to +2.5% against 0.4% for equal-weight buy-and-hold. All agents underperformed in the downturn window and outperformed in the upturn window — [StockBench](https://arxiv.org/html/2510.02209)
- Internal conflict: the abstract says most models struggle to beat buy-and-hold, while its table shows 8 of 13 beating it on return — [abstract](https://arxiv.org/abs/2510.02209); [full text](https://arxiv.org/html/2510.02209)

### Inferences
- FinMem's and TradingAgents' results are demos and should be treated as unreplicated claims. FinMem has been actively contradicted. I found no independent re-test of TradingAgents itself.
- FinMem's test window ends in April 2023. GPT-4-Turbo's training cutoff probably overlaps it, which is from my background knowledge and was not verified in this research. If so, memorised-outcome contamination is plausible.
- Differences of a couple of points over four months with no costs, as in StockBench, are not evidence of skill in either direction.

### Gaps
- No independent long-horizon re-test of TradingAgents was found.
- FinAgent and FinCon original claims were not read directly.
- FINSABER's appendix on LLM cost was not read because of truncation.

## What do Alpha Arena and other live LLM trading competitions actually show, and how large is the sample?

### Takeaway
They show that LLMs can run a live loop and behave differently from each other, not that they have skill. Alpha Arena amounts to two runs of about two weeks with 6 to 8 models, and leverage-heavy concentrated positions dominate the outcome. No Season 2 result was found.

### Cited Findings
- Season 1: six models, $10,000 each, crypto perpetuals on Hyperliquid — [TradeRank (commercial competitor, self-disclosed)](https://www.traderank.ai/blog/alpha-arena-alternatives-2026)
- Season 1 reportedly ran Oct-Nov 2025 for about 17 days. Qwen3 Max finished near +22% and DeepSeek second, with the other four losing money. One source reports heavy leverage and often a single large position. These come from low-quality or commercial blogs, so check them against nof1.ai — [iWeaver](https://www.iweaver.ai/blog/alpha-arena-ai-trading-season-1-results/); [Upscale](https://upscale.trade/blog/ai-trading-alpha-arena)
- Season 1.5: US equities, about two weeks ending around 5 Dec 2025, with four parallel themes (New Baseline, Monk Mode, Situational Awareness, Max Leverage). Grok 4.2 averaged about +12%, ahead of GPT-5.1 and Gemini 3 — [ForkLog](https://forklog.com/en/ai-model-grok-4-2-triumphs-in-trading-tournament/)
- Sources conflict on the end date and return: one blog gives 3 Dec, and the Season 1.5 return has been reported as both +12.11% and +22.38% — [Buildix (commercial)](https://www.buildix.trade/blog/nof1-alpha-arena-lessons-ai-trading-byok-strategy-advisor-2026); [ForkLog](https://forklog.com/en/ai-model-grok-4-2-triumphs-in-trading-tournament/)
- A browser check on 6 Aug 2026 found models no longer running and no Season 2. A recheck on 27 Aug was blocked, so later status is unknown — [TradeRank (commercial)](https://www.traderank.ai/blog/alpha-arena-alternatives-2026)
- TradeRank reports 69 models over 10 seasons since Jan 2026, 2,921 trades, simulated capital, and 43.1% of model-seasons profitable. This is a self-reported vendor figure, and the capital is not real — [TradeRank](https://www.traderank.ai/blog/alpha-arena-alternatives-2026)
- Others named: BingX AI Arena (real capital, copy trading), RockAlpha (linked to RockFlow copy trading), Strategy Arena (virtual capital) — [TradeRank](https://www.traderank.ai/blog/alpha-arena-alternatives-2026)
- LiveTradeBench: 50 days live, 21 LLMs, US stocks and Polymarket. LMArena scores do not predict trading results, and models show distinct risk styles — [LiveTradeBench](https://arxiv.org/abs/2511.03628)
- Live results cannot be independently replicated, per a benchmark paper discussing Alpha Arena, RockAlpha and LiveTradeBench — [AlphaForgeBench](https://arxiv.org/pdf/2602.18481)

### Inferences
- With 6 to 8 entrants, one market path and correlated, leveraged bets, the ranking is close to a single draw. Winners are not repeatable evidence. The season-to-season change in winners (Qwen, then Grok) fits this but does not prove it.
- Several sponsors have copy-trading or product interests, so treat their arenas as marketing as well as measurement.
- The most useful live finding for builders is that general model ability does not map to trading results, so a newer or bigger model is not a reason to expect better P&L.

### Gaps
- nof1.ai was not read directly, and trade counts, fees and per-model statistics for Alpha Arena remain unverified. I found no reliable source for total trades.
- Whether a Season 2 started after Aug 2026 could not be established.
- No audited multi-year live LLM track record was found.

## What does research show on LLM look-ahead and memorised outcomes, headline-scoring returns (Lopez-Lira and Tang), and whether RL beats simple baselines net of costs?

### Takeaway
Memorisation is measurable and inflates in-sample forecasts, so only post-cutoff tests count. Lopez-Lira and Tang is the best-validated positive result, but it decays quickly and dies at 20 bps round-trip. RL evidence is weak and cost-sensitive, and no independent replication shows FinRL-style agents reliably beating buy-and-hold net of realistic costs.

### Cited Findings
**Look-ahead and memorisation**
- Lookahead Propensity (LAP) asks a date-only recall query to estimate whether a model internalised an outcome. It is substantially positive in-sample and near zero just after the cutoff. Headline-return forecasts are stronger on high-LAP firm-dates, and the interaction loses significance post-cutoff — [Detecting Lookahead Bias](https://arxiv.org/abs/2512.23847)
- Glasserman and Lin separate look-ahead from "distraction" by company identity. Anonymised headlines did better in-sample, implying that distraction outweighed look-ahead in their setting, especially for large firms — [Glasserman and Lin, JFDS](https://arxiv.org/pdf/2309.17322)
- These two findings differ in emphasis. The later paper finds memorisation contamination, while the earlier one finds it smaller than distraction. Both support post-cutoff testing — [LAP](https://arxiv.org/abs/2512.23847); [Glasserman and Lin](https://arxiv.org/pdf/2309.17322)
- Look-Ahead-Bench reports significant alpha decay in Llama 3.1 and DeepSeek 3.2 and better generalisation for "Pitinf" point-in-time models. Vendor flag: the relationship between the single author and PiT-Inference is not stated, so independence is unverified — [Look-Ahead-Bench](https://arxiv.org/abs/2601.13770). A third-party listing says it covers five large tech stocks over two six-month periods — [listing](https://benchmarklist.com/benchmarks/lookahead_bench/)

**Lopez-Lira and Tang (v6, Oct 2025; no journal listed on the arXiv page)**
- Oct 2021 to May 2024, 159,137 firm-headline-dates, 4,123 firms, starting after GPT-4's Sept 2021 cutoff — [v6](https://arxiv.org/html/2304.07619v6)
- Initial reaction is 3.06% with a 93.3% hit rate overnight, which is largely not tradable. Tradable next-day drift is 0.34% a day, 58% hit rate, Sharpe 2.97 before costs — [v6](https://arxiv.org/html/2304.07619v6)
- Cost sensitivity: cumulative return exceeds 300% at 5 bps round-trip, exceeds 100% at 10 bps, and is unprofitable at 20 bps. Turnover is about 190% a day. A 25% rebalancing fraction cuts it to about 46%, with Sharpe 1.34 at 10 bps — [v6](https://arxiv.org/html/2304.07619v6)
- Decay: Sharpe 6.54 (2021 Q4), 3.68 (2022), 2.33 (2023), 1.22 (Jan-May 2024) — [v6](https://arxiv.org/html/2304.07619v6)
- Concentration: stronger in small stocks. The short leg on negative news earns 26 bps a day against 8 bps for the long leg, which depends on shorting being available at low cost — [v6](https://arxiv.org/html/2304.07619v6)

**RL against baselines**
- FINSABER included A2C, PPO, SAC and TD3 among its compared strategies, with buy-and-hold as the main benchmark. Its summary here focuses on the LLM agents, and RL-specific figures were not extracted — [FINSABER](https://arxiv.org/html/2505.07078)
- FinMem's paper used PPO, DQN and A2C as baselines, but it is the party claiming a win against them — [FinMem](https://arxiv.org/html/2311.13743)
- Positive single study: beat buy-and-hold in 36 of 48 cases across three cost levels — [High-dimensional DRL](https://arxiv.org/pdf/2112.04755)
- When costs get too high, the agent stops trading and collapses into passive buy-and-hold or sell-and-hold — [Idealized trading games](https://arxiv.org/pdf/1803.03916)
- Review of 167 articles: RL is said to have advantages, especially in market making, yet implementation quality and domain knowledge often outweigh algorithmic complexity. The abstract gives no effect sizes — [Systematic review](https://arxiv.org/abs/2512.10913)
- Intraday RL review: costs, spreads, slippage and latency are treated inconsistently, and omission is a higher risk of bias — [MDPI 2026](https://www.mdpi.com/2076-3417/16/20/10060)
- A thesis found deep RL on par with or below benchmarks, with strong overfitting in some cases — [Aalto thesis](https://aaltodoc.aalto.fi/bitstreams/a452b13d-30d1-4b2c-9600-d9ff80255040/download)

### Inferences
- Lopez-Lira and Tang shows real information processing, but it is an edge that was fading by 2024 and requires low-cost execution in small names. Extrapolating the Sharpe 2.97 forward would be a mistake. The 1.22 in 2024 is before costs.
- A practical test for any LLM backtest: test period after model cutoff, company-anonymised variant, and a LAP-style recall check.
- RL evidence is mostly small positive studies, publication-biased by nature, and I would rank it below the LLM extraction evidence for a retail paper-trading system.

### Gaps
- No post-May 2024 extension of Lopez-Lira and Tang was found, so current decay is unknown.
- No published independent FinRL replication with realistic cost and slippage models was found. FinRL Contest results were not retrieved.
- FINSABER's RL rows were not extracted, though they would be a direct source if needed.

## Which LLM uses (structured extraction, event labelling) have the strongest evidence, and which can run on free tiers or local models?

### Takeaway
LLMs as text-to-feature tools for a separate, testable signal have better support than LLMs as autonomous traders. Local models can do this but are weaker: open-model extraction adds a small gain on top of FinBERT rather than replacing it, and direct evidence linking local-LLM event labels to net returns is thin.

### Cited Findings
- Headline direction scoring, drift Sharpe by model (before costs): GPT-4 2.97, GPT-3.5 1.66, DistilBART-MNLI 1.26, BART-Large 1.05, Llama2-70b 0.97. Llama2's result may be overstated because its cutoff is Sept 2022 — [Lopez-Lira and Tang v6](https://arxiv.org/html/2304.07619v6)
- FinBERT reaches about 90% on initial reaction but 48% on drift (Sharpe -0.33) and makes directional calls on only 25% of overnight news. Rolling-window embeddings underperform GPT-4 — [v6](https://arxiv.org/html/2304.07619v6)
- Structured extraction with LLaMA-3.1-70B on 41,618 news-stock pairs: alone it is weaker than FinBERT, FinBERT alone is F1 0.576 (nonlinear), combined is 0.600 (p<0.0001), and non-sentiment dimensions add +0.019. This is a preprint with no trading-return test reported on the abstract page — [Beyond Sentiment](https://arxiv.org/abs/2607.28496)
- FinBERT sentiment on earnings calls aligns with investor reaction better than alternative measures, though this tests short-window reaction and not a tradable strategy — [Huang et al., CAR 2023](https://onlinelibrary.wiley.com/doi/10.1111/1911-3846.12832)
- LLM-derived sentiment improved return prediction over finance dictionaries, with open models such as OPT used as a substitute when GPT-4 was unavailable. Read through a secondary review only — [Moonlight review of Kirtac and Germano](https://www.themoonlight.io/review/sentiment-trading-with-large-language-models)
- A fine-tuned open LLaMA gave reasonable NASDAQ-100 direction results, but with more extreme predictions than GPT-4 — [alphaXiv overview](https://www.alphaxiv.org/overview/2306.11025)
- Extractors can hallucinate values or miss context, so validation steps are needed — [KPI extraction paper](https://arxiv.org/pdf/2505.19197)
- Open-weight agent results in StockBench were comparable with proprietary ones: Qwen3-235B +2.4 to 2.5% and GLM-4.5 +2.3%, against GPT-5 +0.3%. This is one short, cost-free run, so it shows comparability at best — [StockBench](https://arxiv.org/html/2510.02209)
- Note that GPT-OSS-20B was the weakest of 13, at -2.8%. Small local models may be materially worse — [StockBench](https://arxiv.org/html/2510.02209)

### Inferences
Ranked by my judgement of evidence strength:
1. Headline and news direction scoring as a drift signal: strongest post-cutoff evidence, but cost-sensitive and decaying.
2. Event labelling and structured extraction fed into a conventional model: plausible and cheap to validate, with small measured gains. Consistent with Sentinel's existing design of local models feeding a factor model.
3. Earnings-call and filing sentiment: good evidence on reaction, weaker on tradable returns.
4. Autonomous multi-agent traders: weakest evidence, and highest inference cost (TradingAgents uses 11 LLM calls and 20+ tool calls per prediction).

Local fit:
- Zero-shot BART-family models and FinBERT are small enough to run locally, and their full-sample drift results (1.05 to 1.26 for BART-family, negative for FinBERT) suggest a local classifier is not automatically useless. This is my inference, and those models were not tested for local throughput.
- For Sentinel, the implied test is to log label plus timestamp, score post-cutoff only, and evaluate next-open drift net of 10 to 20 bps before any signal touches the book. Any change to trading logic needs the user's approval per project rules.

### Gaps
- No study was found comparing local open LLMs with FinBERT on event labels and then validating against net-of-cost returns.
- Current free-tier limits for hosted LLM APIs were not researched, so which free tiers suit this work is unanswered here. Free tiers also change often.
- Kim and Muhn-style financial-statement analysis and its replications were searched for but not retrieved.
- The 2026 review of open LLMs for earnings calls ([Springer](https://link.springer.com/article/10.1007/s44163-026-01334-9)) and FinTagging ([arXiv](https://arxiv.org/pdf/2505.20650)) were found but not read.
