# Named trading algorithms with published rules: evidence and fit with Sentinel

Coverage note: almost no source here was read in full. Many figures come from abstracts, repo files or secondary summaries, and these are labelled. The TRA PDF could not be parsed. Fit statements are my inferences and have not been tested on Sentinel data. I only scanned the headings of the earlier report (`reports/Trading bots and smart algos.md`), so overlap was avoided by topic, not line by line.

## Ranking by usefulness for a free-data book

### Takeaway
The most useful candidates change the target or the timing of the book, because those are classes Sentinel has not yet failed. Portfolio-construction filters and vol scaling look less promising, because they resemble what already failed. No candidate below has a net-of-cost result on a monthly LightGBM long-short US book.

### Cited Findings
The table is my own ranking. The evidence behind each row is cited in the sections below.

| Rank | Algorithm | Free-data feasibility | Evidence grade | Suggested action |
|---|---|---|---|---|
| 1 | Residual or factor-neutral training target (idea from Blitz et al.) | High | Strong for the momentum signal; none found for ML labels | Register a test |
| 2 | Daniel-Moskowitz panic-state conditioning of the short leg | High, needs only market returns and volatility | Published and multi-market, but few crash events | Register a test, with a warning about low power |
| 3 | Decay-weighted aim portfolio (Garleanu-Pedersen) | High | Theory plus commodity futures | Low priority, since buffers already failed |
| 4 | DoubleEnsemble sample reweighting and feature selection | High, code is open | Vendor or author results in China and crypto; inconsistent in Qlib's own table | Optional, cheap test |
| 5 | Alpha158 feature set | High, price and volume only | China only in official tables | Use as a feature audit only |
| 6 | Meta-labeling | High | Mostly proponents and synthetic data | Low priority |
| 7 | Sequential bootstrap and uniqueness weights | High | No controlled financial evidence found | Low priority for bagging; simple overlap weights may suffice |
| 8 | TRA | Medium, needs a deep learning stack | China; table numbers only | Skip |
| 9 | Barroso-Santa-Clara vol scaling | High | Contested out of sample | Skip; closest to the failed vol-sized legs |

### Inferences
- Ranks 1 and 2 are the only items that address the stated problem, a short edge concentrated in the most volatile names.
- Ranks 1 and 2 are separate tests. Running them together would not show which one caused any change.

### Gaps
- I found no study that applies any of these to a monthly gradient-boosted long-short US book net of costs and borrow.

## Qlib model zoo: Alpha158, LightGBM, DoubleEnsemble, TRA

### Takeaway
Qlib's official benchmark tables are for Chinese A-shares, so they say nothing direct about US stocks. Within those tables, DoubleEnsemble's edge over LightGBM flips sign across universes and feature sets, and I found no clean US replication.

### Cited Findings
- Alpha158 on CSI300, mean over 20 seeds:

| Model | IC | ICIR | Annualized return | Information ratio | Max drawdown |
|---|---|---|---|---|---|
| LightGBM | 0.0448 | 0.3660 | 0.0901 | 1.0164 | -0.1038 |
| DoubleEnsemble | 0.0521 | 0.4223 | 0.1158 | 1.3432 | -0.0920 |
| TRA | 0.0440 | 0.3535 | 0.0718 | 1.0835 | -0.0760 |
| XGBoost | 0.0498 | 0.3779 | 0.0780 | 0.9070 | -0.1168 |
| Linear | 0.0397 | 0.3000 | 0.0692 | 0.9209 | -0.1509 |

  Source: [Qlib benchmarks README](https://github.com/microsoft/qlib/blob/main/examples/benchmarks/README.md).
- On CSI300 Alpha360, TRA leads (IC 0.0485, IR 1.28) and DoubleEnsemble trails LightGBM (IR 0.62 against 0.76) — [Qlib README](https://github.com/microsoft/qlib/blob/main/examples/benchmarks/README.md).
- On CSI500 Alpha158, LightGBM has IR 1.565 and DoubleEnsemble 0.172. However, the DoubleEnsemble and Linear rows share identical return, IR and drawdown values, which looks like a table error, so do not rely on them — [Qlib README](https://github.com/microsoft/qlib/blob/main/examples/benchmarks/README.md) (the extraction tool flagged this; I did not view the raw table separately).
- The maintainers say they had very limited resources to tune models, and that backtests from version 0.8.0 are not comparable with earlier ones — [Qlib README](https://github.com/microsoft/qlib/blob/main/examples/benchmarks/README.md).
- The LightGBM Alpha158 config uses CSI300, train 2008-2014, validation 2015-2016, test 2017 to August 2020, TopkDropout with topk 50 and n_drop 5, open cost 5 bp, close cost 15 bp, minimum cost 5, limit threshold 9.5% — [Qlib config](https://raw.githubusercontent.com/microsoft/qlib/main/examples/benchmarks/LightGBM/workflow_config_lightgbm_Alpha158.yaml).
- The DoubleEnsemble paper reports over 51.37% annualized return and a Sharpe of 4.941 in A-shares, plus a crypto test on OKEx — [arXiv 2010.01265](https://arxiv.org/pdf/2010.01265.pdf), via search summary. **Author claim, unreplicated at this size.**
- Mechanism: DoubleEnsemble reweights samples based on training dynamics and selects features by shuffling them — [arXiv 2010.01265](https://arxiv.org/abs/2010.01265v1).
- Mechanism: TRA adds a set of predictors for different trading patterns and a router that selects among them — [Lin et al.](https://arxiv.org/pdf/2106.12950).
- The closest US uses were weak or non-comparable: a factor-generation paper running Qlib on S&P 500 ([AlphaLogics](https://arxiv.org/pdf/2603.20247)), a 5-stock forum backtest ([QuantConnect](https://www.quantconnect.com/forum/discussion/19898/strategy-share-cross-sectional-sp500-strategy-sp500relativestrength)), and a blog reporting underperformance after costs ([Nicolai](https://vadim-blog.vercel.app/qlib-ai-quant-workflow-lightgbm)). All are anecdotal.

### Inferences
- The gap between DoubleEnsemble's paper Sharpe of 4.94 and its IR of 1.34 in Qlib's table should be read as a warning about headline claims. The two are not like for like, since data, period and metric differ.
- The Qlib test appears to be a long-only book against an index, with price-limit rules specific to China. It says little about a long-short US book or about shorting.
- IC differences of 0.005 to 0.007 between models on one 3.6-year test period are thin evidence. Model choice looks a minor lever relative to target and costs.
- Sentinel already uses LightGBM, so the zoo's practical value is a cheap DoubleEnsemble test and a check of Alpha158 features against current ones.

### Gaps
- TRA's own paper tables could not be read, as PDF parsing failed.
- The README page did not state whether its returns are net; the config sets costs, but I did not confirm how the table was produced.
- No independent US replication of DoubleEnsemble or TRA was found. This is a search result, not proof that none exists.

## Meta-labeling, triple barrier, sequential bootstrap, sample uniqueness

### Takeaway
I found no independent, peer-reviewed, out-of-sample proof that these improve a real equity book. The available support comes from proponents, synthetic data and practitioner posts, and the only empirical test found was negative and weak in quality.

### Cited Findings
- Mechanism: a primary model picks direction, and a secondary model predicts whether that bet pays off — [practitioner explainer](https://5x5x5x5.github.io/the-way-of-grace/ml/labeling-problem/).
- The main published framework is Joubert (2022), *JFDS* 4(3) — [citation via Wikipedia](https://en.wikipedia.org/wiki/Meta-Labeling). Follow-ups include [Thumm, Barucca and Joubert (2023)](https://discovery-pp.ucl.ac.uk/id/eprint/10169066) and Meyer, Barziy and Joubert (2023) — [Hudson & Thames repo](https://github.onedrive11.workers.dev/hudson-and-thames/meta-labeling). The repo URL is a mirror, so verify it. **Joubert is linked to Hudson & Thames, a vendor of this tooling (per the repo); treat this as proponent evidence.**
- A summary says improvements were shown with synthetic data and simulated trading environments, without citing a specific study — [Wikipedia](https://en.wikipedia.org/wiki/Meta-Labeling). Unverified.
- Sequential bootstrap lowers the chance of overlapping samples, and the vendor's suggested check is whether it raises average uniqueness — [Hudson & Thames](https://hudsonthames.org/?p=3574). That checks the sampler's mechanics, not trading results.
- A single-author EURUSD tick-bar study with triple-barrier labels found bagged-tree out-of-sample AUC of 0.472 to 0.485, about no skill — [MQL5](https://www.mql5.com/en/articles/23137). A forum comment there argues decorrelation comes mostly from limiting `max_samples` to average uniqueness, with little added by the sequential draw — [MQL5 forum](https://www.mql5.com/en/forum/513058). Low-grade evidence, different asset and frequency.

### Inferences
- Meta-labeling is a filter on the primary model's bets. Sentinel's registered drops of crowded and volatile shorts were also bet filters and failed, so a learned filter starts with an unfavourable prior. It could still differ, since it learns the condition rather than fixing it.
- A meta-model on monthly bets would have few independent labels, so overfitting is a real risk.
- Triple-barrier labels assume path-dependent exits. A book rebalanced monthly has no such exits, so the label would not match how positions are held.
- Monthly forecasts with longer overlapping horizons could justify simple overlap weights, but a monthly label with monthly rebalancing has little overlap, so the benefit may be small. Check Sentinel's actual label horizon first.

### Gaps
- The JFDS papers' quantitative results were not retrieved, since they are paywalled.
- No controlled comparison of sequential against standard bootstrap on held-out financial data was found.

## Fundamental law, signal combination, decay weighting, turnover-aware optimisation

### Takeaway
The fundamental law is a useful accounting identity, not a forecast. Its main warnings for Sentinel are that constraints cut the transfer of skill and that concentrated edge means less breadth than the name count suggests.

### Cited Findings
- Generalised form: IR = TC x IC x sqrt(BR); constraints such as no short sales and concentration limits lower the transfer coefficient — [Clarke, de Silva and Thorley (2002)](https://ideas.repec.org/a/taf/ufajxx/v58y2002i5p48-66.html); [CFA Institute](https://www.cfainstitute.org/insights/professional-learning/refresher-readings/2025/analysis-active-portfolio-management).
- Limits include uncertainty about ex ante IC and defining breadth as independent decisions; estimation error is also argued to undermine use of the law — [CFA Institute](https://www.cfainstitute.org/insights/professional-learning/refresher-readings/2025/analysis-active-portfolio-management); [New Frontier (Michaud et al.), a vendor](https://newfrontieradvisors.com/media/1744/fundamental-law-september-2019.pdf).
- Garleanu-Pedersen: aim in front of the target, and trade partially toward the aim. The aim blends current and expected future Markowitz portfolios, and slower-decaying predictors get more weight — [NBER w15205](https://www.nber.org/papers/w15205).
- Their application to commodity futures found superior net returns against naive benchmarks — [NBER w15205](https://www.nber.org/system/files/working_papers/w15205/w15205.pdf), via search summary. A 2025 thesis on stock-index data for 2009-2024 found it best in-sample and when costs are high — [Copenhagen thesis listing](https://www.math.ku.dk/english/calendar/specialeforsvar/speciale-mdsten).
- Most anomalies below 50% one-sided monthly turnover keep significant net spreads, few higher-turnover ones do, and a buy/hold spread was the most effective mitigation — [Novy-Marx and Velikov (2016)](https://econpapers.repec.org/RePEc:oup:rfinst:v:29:y:2016:i:1:p:104-147.).
- ML performance deteriorates under reasonable costs because of high turnover and extreme positions, and profits weaken under restrictions such as excluding microcaps and distressed firms — [Avramov, Cheng and Metzker (2023)](https://econpapers.repec.org/RePEc:inm:ormnsc:v:69:y:2023:i:5:p:2587-2619); [abstract](https://papers.ssrn.com/abstract=3450322).
- Using NYSE breakpoints and value weights, 64% of 447 anomalies are insignificant at 5%, and 85% at a t cutoff of 3; microcaps are 60% of names but 3% of market cap — [Hou, Xue and Zhang](https://www.nber.org/papers/23394); [summary](https://alphaarchitect.com/replicating-anomalies/).

### Inferences
- If the short edge lives in a small set of volatile names, which likely co-move, effective breadth on that leg is lower than its position count. The law therefore suggests measuring short-leg IC and breadth separately, and estimating effective independent bets from the correlation of short returns.
- Sentinel's failed rank buffer conflicts with the Novy-Marx and Velikov result. The likeliest reconciliations are that its turnover is already low, or that its volatile-name edge decays quickly, but this needs checking. It does not refute the paper.
- Garleanu-Pedersen needs separate decay estimates per signal. LightGBM gives one blended score, so a full version requires forecasts at several horizons, and each adds model risk. A simple test is to blend scores from recent monthly model vintages.
- Signal blending is a sound default only when the signals are not close copies; averaging seeds or vintages of the same model reduces variance but adds little breadth.

### Gaps
- Net Sharpe values from Garleanu-Pedersen were not retrieved.
- Grinold's own alpha-decay and alpha-blending writing was not retrieved, so no claims rest on it here.
- I found no empirical test of decay-weighted forecasts in stock ML books.

## Residual momentum, risk-managed momentum, residual targets, crash conditioning

### Takeaway
Residual momentum has the strongest record among these, while the case for vol scaling weakens in real time. Daniel-Moskowitz gating differs from Sentinel's failed drops because it times the leg by market state rather than removing names, which makes it the most relevant untested idea for a volatile short book.

### Cited Findings
- **Residual momentum mechanism:** regress each stock on the Fama-French three factors over 36 months, then rank on the past 12-month residual returns excluding the latest month, divided by their standard deviation; long-short on deciles — [Blitz, Huij and Martens, full text](https://core.ac.uk/download/18511704.pdf), via search summary.
- **Evidence:** risk-adjusted profits are about twice those of total-return momentum — [Blitz et al.](https://repub.eur.nl/pub/22252). US 1926-2009, gross: Sharpe 0.90 against 0.45, volatility 12.5% against 22.7%; large caps 0.60 against 0.36 — [same paper, via summaries](https://www.cxoadvisory.com/?p=23858). These are gross.
- **Follow-up:** lower time-varying factor exposure holds across universes and out-of-sample periods — [Huij and Lansdorp](https://repub.eur.nl/pub/100801). This work shares an author lineage or institution with the original, so it is not fully independent.
- **Mixed replications:** a Swedish thesis found better risk-adjusted returns in equal-weighted but the reverse in value-weighted portfolios — [Bodin and Börjeson](https://gupea.ub.gu.se/server/api/core/bitstreams/9f029ad8-ee65-4166-a3b7-f301f804a4f2/content). A Portuguese thesis found residual momentum subsumes traditional momentum — [UCP thesis](https://repositorio.ucp.pt/bitstreams/185837aa-e37e-433f-ac1a-0d5d79027b8f/download). Theses are lower grade.
- **Daniel-Moskowitz:** crashes are partly forecastable, occurring in panic states after market declines and high volatility, and coinciding with rebounds. Low expected returns are tied to option-like payoffs of past losers. Dynamic scaling on forecast mean and variance roughly doubles alpha and Sharpe, and holds across periods, international markets and other assets — [NBER w20439](https://www.nber.org/papers/w20439).
- **Barroso-Santa-Clara:** risk management nearly doubles momentum's Sharpe and virtually eliminates crashes — [via summary](https://alphaarchitect.com/avoiding-momentum-crashes/). Daniel and Moskowitz report that their strategy significantly beats constant-volatility scaling — [Swedroe](https://www.etf.com/sections/index-investor-corner/swedroe-downside-momentum).
- **Replication and dispute:** Dierkes and Krupski report Sharpe 1.12 for a conditional variant, against 0.94 constant and 1.03 dynamic, while another paper warns of overfitting a small sample of extreme events — [Dierkes and Krupski](https://core.ac.uk/download/213566226.pdf); [Queen's University paper](https://pureadmin.qub.ac.uk/ws/files/139425062/volatility_scaling_in_momentum_Dec_17.pdf). I did not confirm which of these two PDFs carries each point, or read them, so check before quoting. Whether constant or dynamic scaling is better remains debated — [HSE paper](https://wp.hse.ru/data/2017/09/22/1173030994/61FE2017.pdf).
- **Cederburg et al.:** across 103 strategies, managed versions had higher gross Sharpe in 53, only 8 significant. Including them lowered out-of-sample Sharpe and utility for 72 of 103, and frictions would lower results further — [JFE record](https://econpapers.repec.org/RePEc:eee:jfinec:v:138:y:2020:i:1:p:95-117); [counts via CXO Advisory](https://www.cxoadvisory.com/?p=32455). Note this tests time-series scaling of strategies, not cross-sectional sizing of names.
- **Stambaugh, Yu and Yuan:** the IVOL-return relation is negative among overpriced stocks and positive among underpriced ones, and the overpriced effect is stronger for stocks that are harder to short, reflecting arbitrage asymmetry — [JF abstract](https://ideas.repec.org/a/bla/jfinan/v70y2015i5p1903-1948.html); [NBER w18560](https://www.nber.org/papers/w18560).

### Inferences
- **Sentinel's short pattern matches Stambaugh et al.:** this is a documented mispricing pattern, not necessarily a model artefact. It also explains why dropping volatile shorts failed, since that removes the source of the edge.
- **The same paper is the main warning:** the edge may sit where shorting is hardest. Paper fills on Alpaca may overstate it, so measured short returns need realistic borrow fees and locate availability before being believed. This is the largest open risk, and no named algorithm addresses it.
- **Residual target test:** train on residual forward returns rather than raw ones, using free daily prices and factor data. This could stop the model loading on market, size and value, and may reduce volatile-name beta. It might also shrink the short edge if that edge is partly factor exposure, and that outcome would itself be informative. This extends Blitz's ranking idea to a label and has no direct evidence behind it.
- **Crash gate test:** register a rule that reduces short-leg size only in Daniel-Moskowitz panic states (after market decline and high market volatility). It needs only market data. The live sample may contain few such states, so a null or positive result will be low-powered. It should be judged over a long backtest and not tuned.
- **Order of evidence:** a gate changes sizing logic, so CLAUDE.md requires explicit approval before any live change; research backtests need none.
- **Skip vol scaling:** Barroso-Santa-Clara scales the whole strategy over time, while Sentinel's failed vol-sized legs may differ in detail. Given Cederburg et al. and that failure, it should rank low, though not be treated as already refuted.

### Gaps
- No Hou, Xue and Zhang result specific to residual momentum was found, and no Chang international study.
- Nothing found on ML models trained on residual labels, in gradient boosting or otherwise. Absence from these searches is not proof that none exists.
- Blitz's net-of-cost results were not retrieved. The 2011 figures quoted are gross.
- The Daniel-Moskowitz abstract does not break out short-leg results, so the claim that damage concentrates on the short side is inferred from the losers mechanism and should be checked in the full paper.
- Novy-Marx and Velikov's "Betting against betting against beta" was not retrieved.
- No source gave borrow costs for volatile US shorts.
