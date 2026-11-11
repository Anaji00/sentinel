# Buy signals: what the research says, what we hold, what to test (7 November 2026)

## Where we are

**Nothing on the buy side is proven.** Of the 16 proven reaction-study cells, every one is bearish (short) or a put sweep, and only 5 survive on liquid, borrowable names (reports/Event backtest scope.md, G4). The buy-side families we measure:

| Family | Result in our study (2021 to 2026, size-matched benchmark) |
|---|---|
| Insider opportunistic buy | No edge, and the wrong sign: 30 days −217 bps (t −1.85, 60 units); 5 days −47 bps |
| Insider routine buy | No edge (30 days −104 bps) |
| Positive earnings reaction | Drifts down: 5 days −34 bps for large moves (t −2.9, contradicts the prior); 30 days −159 bps (t −2.5) |
| Positive company news | Drifts down (t −3.3 at 1 day, −4.4 at 5 days), so it is a fade, not a buy |
| Block buys | No edge (1 hour −3 bps, 1 day −20 bps) |
| Call buys, opening calls, guidance raised, 13D activist, contract awards, insider cluster | Fewer than 20 independent units: not yet testable |

B1's own long leg trails the market after beta. So the buy-side question is open, and the first job is to measure it, not to add a signal.

## What the research says (secondary sources; none opened in full)

| Candidate | Evidence found | Caveat |
|---|---|---|
| **Insider cluster buys** | Cohen, Malloy and Pomorski: opportunistic buys earn about 0.82% a month, routine ones nothing. A summary of cluster studies reports 3.8% over 21 days for clusters against 2.0% for single buys. [IBKR summary](https://www.interactivebrokers.com/campus/traders-insight/securities/stocks/what-corporate-insider-buying-can-tell-investors-evidence-from-academic-research/) | Core studies are 1998 to 2016. Size effects are disputed (stronger in small caps per Lakonishok and Lee; not significant per Jeng, Metrick and Zeckhauser). Our own opportunistic-buy cells run the other way |
| **Activist 13D filings** | Initial filing returns of 2% to 4% in early studies, 6% to 7% in others; wolf-pack campaigns about 14%, with most buying in the ten days before the filing. [Management Science](https://pubsonline.informs.org/doi/pdf/10.1287/mnsc.2019.3312), [Columbia Law](https://scholarship.law.columbia.edu/faculty_scholarship/1880) | Much of the move is before the filing. Long-run evidence is mixed. Nothing from the last two years found |
| **Opening call purchases** | The strongest option-flow predictor in Ge, Lin and Pearson (2016); options-to-stock volume also predicts. [Alpha Architect summary](https://alphaarchitect.com/do-option-prices-inform-stock-returns/) | Older data. One study finds option volume is uninformative in normal times. Our put sweeps are proven, which suggests the call side deserves the same test |
| **Buyback announcements** | Underreaction and long-run drift persist in Peyer and Vermaelen; drift is larger in small stocks and for firms citing undervaluation. [Frankfurt seminar abstract](https://www.old.wiwi.uni-frankfurt.de/abteilungen/finance/seminar/finance-seminar-series/summer-term-2005/abstract-peyer.html), [JFQA](https://www.cambridge.org/core/journals/journal-of-financial-and-quantitative-analysis/article/timing-and-source-of-longrun-returns-following-repurchases/3A0AA39EF30BB41483794352CDB40BAC) | Several sources are theses; one finds underperformance. Net share issuance was already tested and rejected as a model feature (O3) |
| **Post-earnings drift** | Weaker over time, and weak in S&P 500 names 2022 to 2024. Chan and Marsh argue it persists as overnight drift after extreme misses. [Mondello summary](https://repositorio.ucp.pt/bitstreams/c9033501-675d-4fda-a389-c1b18a4ffc6a/download) | Our data shows reversal after good news, not drift. Skip as a buy signal |
| **Government contract awards** | Positive but small and often insignificant reactions; stronger for small firms. [UK FTSE 350 study](https://anserpress.org/journal/jea/3/1/43/html) | Few US studies. Free source (USAspending) exists but matching recipients to tickers has the same precision problem as our customer links |
| **FDA approvals** | Next-day positive abnormal returns in small old samples. | Small samples; narrow universe |
| **Gross profitability, 52-week high** | No convincing live long-only 2020 to 2025 record found. Gross profitability added nothing in our own trials (P1 to P3) | Skip |
| **Published anomalies in general** | Returns decay about 50% after publication; net of costs the average survivor is worth a few bps a month. [Chen and Velikov](https://econpapers.repec.org/RePEc:now:jnlcfr:104.00000112) | Do not expect new published factors to be a source of easy long alpha |

## Ranked plan

Each test uses the existing reaction study (independent units, false-discovery control, held-out 40%, liquid-and-borrowable gate from G4), so no new harness is needed. Rules are registered here before any run. Each new family adds four cells to the run's correction.

| # | Candidate | What is needed | Test (pre-registered) | When |
|---|---|---|---|---|
| 1 | **Insider cluster buys** | The 14,215 rebuilt clusters are in the archive (7 Nov) but the study's deep-history list lacks `insider_cluster`, so it still reads 365 days. Add it. | Cells for insider_cluster at 1d, 5d, 30d. Passes the study's bars **and** survives the liquid gate (trailing dollar volume of at least $140.8M a day) at t of 2 or more. A second reading splits clusters by 2, 3 to 5 and 6 or more buyers. Reported whichever way. | Now: one setting, then the 6-hourly study |
| 2 | **Activist 13D** | G2's 10-year EDGAR backfill adds 13D history | The study's activist_13d cells at 1d, 5d, 30d once 20 units exist; restrict to filings whose Item 4 reads as activist | After G2 |
| 3 | **Opening call buys** | Already collected: buyer-initiated and opening-confirmed tags | options_opening_call and options_call_buy cells at 1d and 5d; mirror of the proven put sweeps | About 100 days of data (around February 2027) |
| 4 | **Buyback authorizations** | A detector on 8-K text (Items 7.01 and 8.01: "repurchase program", dollar amount against market cap) | New family `buyback_authorized` with a size filter (authorization of at least 1% of market cap); cells at 1d, 5d, 30d from 2016 | After G2 gives 8-K text; needs a small parser |
| 5 | **Contract awards** | USAspending history, recipient to ticker matching with a name gate | Awards of at least 1% of market cap; checked by precision first (as with customer links) | Later; only if 1 to 4 find something |

## What to skip

Post-earnings drift as a buy signal (it reverses here), gross profitability and 52-week high (already tested or no live record), new published anomalies (decay), FDA approvals (narrow). Kaggle sets for any of these (survivorship).

## One finding to look at first

Our opportunistic insider buys fall after the buy, which is the opposite of the literature. Either the classification (`shared/utils/insider_history.py`, three years of prior trades) is wrong, the size-matched benchmark penalises small-cap buyers, or the effect is concentrated in names the gate removes. Test #1 will show it by size; if clusters also fail on liquid names, the insider-buy features should not be trusted for the long leg.
