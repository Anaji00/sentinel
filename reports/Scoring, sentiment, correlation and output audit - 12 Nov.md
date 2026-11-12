# Scoring, sentiment, correlation and output audit (12 Nov 2026)

Read-only measurement of the live system, then the two defects that were clear and safe to fix. Scripts: `scripts/audit_scores.py`,
`audit_outputs.py`, `audit_outputs2.py`, `sentiment_adequacy.py`, `show_producer_grades.py`, `baseline_anomaly_rates.py`.

## 1. Anomaly scores (events, 7 days)

A calibrated alert score should use many values and clear a 0.8 cut for a few percent of events. Several do neither.

| Event type | Events | Distinct scores | Share at 0.8 or more | Finding |
|---|---|---|---|---|
| vessel_sts | 4,073 | **1** (0.780) | 0% | Hard-coded constant. **Fixed**: score now rises with dwell, closeness, speed, sanctions and region (`shared/utils/sts_score.py`). |
| earnings_reaction | 951 | 383 | **53%** | 42% of events score exactly 1.000: saturated. |
| crypto_liquidation | 197 | 74 | **67%** | 59% score exactly 0.900. |
| earnings_report | 166 | 7 | 0% | 61% score exactly 0.450. |
| prediction_market_trade | 17,915 | 738 | 8% | 54% sit on the 0.300 floor, so the median is the floor. |
| filing | 3,942 | 26 | **34%** | Form-type scores; a third clear the cut. |
| equity_block | 3,020 | 631 | **32%** | Capped at 0.995, a third clear 0.8. |
| market_anomaly | 17,716 | 853 | 1.7% | Healthy. |
| options_flow, insider_trade, vessel_dark | | 424, 311, 649 | 1 to 6% | Healthy. |

Time of day (14 days, weekday `market_anomaly`, Eastern): score 0.8 or more is 3.8% of events from 10:00 to 11:30 and 2.9% after the close,
and 0.0 to 0.1% at midday, in the afternoon and at the close. The opening half hour is 0.6%. The normaliser keeps one baseline per
(ticker, feature) with no time of day. Registered test: `Upgrade pre-registrations - 12 Nov.md`, phase 3.

The enrichment service itself warned that detectors for `trade_signal`, `supply_chain_metric`, `earnings_reaction` and `headline` had gone
silent against their own cadence, and that `earnings_report` emitted 7 distinct scores.

## 2. Sentiment

- The news enricher's lexicon scored **81.6%** of 4,000 company headlines exactly 0 and produced 7 distinct values.
- Where FinBERT or the language model saw a tone, the lexicon agreed 26% and 24% of the time, called it neutral 70% and 73%, and was opposite
  4% and 3%. It is safe and nearly blind. Example: "Dorchester Minerals Q3 EPS $0.54 Up From $0.23 YoY" scored 0.0; FinBERT 0.92, model +1.
- FinBERT and the language model agree on direction about 70% of the time (negative 70%, positive 73%). Where FinBERT says neutral (8,777 of
  14,302 headlines) the model reads positive in 26%.
- Event headline sentiment has 35 distinct values, 47% near zero, 16% saturated; social signals 22 values, 72% near zero. The model's own
  score is -1, 0 or +1 by design (2,484 / 6,337 / 5,481).
- Where the lexicon matters: world headlines mapped to sectors and countries (`signal_families`). Company news already uses FinBERT tone tags.

## 3. Correlations

- Producer grades (5-session, stored): Quantitative Equity Peer Cointegration & Decoupling moves 2.02 times a normal move (n=624);
  Company News & Trading 2.33 (n=56); Hawkes Excitation Forecast 1.22 (n=3,153); Equity Block & Options 1.18 (n=2,070). **Semantic
  Convergence is the largest producer (n=6,665) with the smallest lift, 1.054.** Producers that precede smaller moves than normal:
  Crypto Venue Stress 0.61, Insider Sale Into Options Accumulation 0.52, Geopolitical Cascade (Msft) 0.43, Cross-Asset Cascade 0.88.
- The statistical discovery engine reads the latest 120 five-minute buckets (about 10 hours) for names with minute bars (at most 60). CVX,
  GD, LMT, OXY and RTX have no minute bars, so every pair involving them is unevaluable; DXY, EURUSD and BTC-USD have no rows. A daily-return
  lane over 250 sessions would give those names a test; not built.
- Logs: 8 unevaluable-pair warnings, 11 graph entities refused as non-symbols (MISHELL, MEAN), 9 repeated-episode drops, 3 slow deep-window
  reads, 16 supervisor lock timeouts that dropped graph proposals and 10 lease overruns (the lock lasts 15 s), 5 Qdrant connection failures
  (the known name-resolution flake), 8 stock-correlation drops for messages naming no US equity.

## 4. Language-model and agent output

- Scenarios: 53 in 14 days (101 in 30). Always exactly 3 hypotheses. The probability triplet is templated: 38 distinct among 101, and
  55/30/15 (25), 45/30/25 (14) and 45/35/20 (13) cover 52%. The prompt's own example said `"probability": 45` and
  `"confidence_overall": 62`; 40% of scenarios start at 45. **Fixed**: the example is now a description, and the prompt says to derive each
  probability from the cited signals.
- Content is specific: mean headline 81 characters, significance 314, mechanism 229, 20 distinct confidence values (mean 61, sd 17). Gaps:
  9% of hypotheses have no watch signal, 7% no deny signal, 11% no beneficiary.
- Agent predictions (14 days): stock_correlation_agent 301 with 32 distinct confidences; macro_intelligence_engine 89 with 16; analyst_shadow 57 with 15; analyst_agent 31 with 14; **quant_trading_engine 80 predictions with only 4 distinct confidences**, a near-constant that carries little weight in grading.

## 5. Changed and not changed

Changed (committed): STS score from geometry; scenario prompt without numeric anchors. Tests: `tests/test_sts_score_and_prompt_anchors.py`.
Not changed, each needing a registered rate test or a decision first: the saturated and floor scores above; time-of-day baselines;
FinBERT on world headlines; a daily-return lane for discovery; the supervisor lock lease; the scenario gaps (watch, deny, beneficiary).
