# System stress test: before fixes

111,473 generated cases over 3 seeds (1, 2, 3), four layers, offline against the real code.

| Layer | Cases | Families | Pass | Note | Defect |
|---|---:|---:|---:|---:|---:|
| 1. Collection | 67,637 | 60 | 45 | 9 | 6 |
| 2. Enrichment | 1,062 | 42 | 18 | 18 | 6 |
| 3. Correlation | 1,056 | 54 | 43 | 0 | 11 |
| 4. Reasoning and forecasting | 41,718 | 99 | 72 | 12 | 15 |

## Defects

- **1. Collection | OCC symbols with impossible dates are refused** (seeds [1, 2, 3]): failed on 8 of 8 generated cases
  - `LGL261301C00100000 -> accepted an impossible expiry {'ticker': 'LGL', 'expiry': '2026-13-01', 'option_type': 'CALL', 'strike': 100.0}`
  - `JWX260005C00100000 -> accepted an impossible expiry {'ticker': 'JWX', 'expiry': '2026-00-05', 'option_type': 'CALL', 'strike': 100.0}`
  - `USF260230C00100000 -> accepted an impossible expiry {'ticker': 'USF', 'expiry': '2026-02-30', 'option_type': 'CALL', 'strike': 100.0}`
- **1. Collection | cached prices parse to a positive finite number or nothing** (seeds [1, 2, 3]): failed on 3 of 16 generated cases
  - `'inf' -> returned inf`
  - `'1e999' -> returned inf`
  - `b'\xff\xfe' -> raised UnicodeDecodeError: 'utf-8' codec can't decode byte 0xff in position 0: invalid start byte`
- **2. Enrichment | a bigger premium never scores lower (metamorphic)** (seeds [1, 2, 3]): failed on 5 of 30 generated cases
  - `XYZ CALL -> score fell as premium rose (4972051.0163955875, 0.9988, 6471187.537030578, 0.995)`
  - `TSLA PUT -> score fell as premium rose (4946889.335327243, 0.9977, 5729703.116394985, 0.995)`
  - `XYZ CALL -> score fell as premium rose (4954425.55857983, 0.998, 5172631.325455197, 0.995)`
- **2. Enrichment | one bad field in a print does not lose the print or poison its numbers** (seeds [1, 2, 3]): failed on 12 of 26 generated cases
  - `price=None -> raised TypeError: float() argument must be a string or a real number, not 'Non`
  - `price='abc' -> raised ValueError: could not convert string to float: 'abc'`
  - `volume=None -> raised TypeError: float() argument must be a string or a real number, not 'Non`
- **3. Correlation | broken evidence does not fire: no shared tag** (seeds [1, 2]): failed on 1 of 4 generated cases
  - `rule_crypto_equity_contagion -> still fired with no_shared_tag: rule_crypto_equity_contagion[ELEVATED]; rule_crypto_stress_cascade[ELEVATED]`
- **3. Correlation | every event type a clause lists can satisfy that clause** (seeds [1, 2, 3]): failed on 1 of 65 generated cases
  - `rule_physical_disruption_repricing clause 0: macro_release -> this declared alternative can never satisfy its own clause`
- **3. Correlation | a single event cannot answer two clauses of the same rule** (seeds [1, 2, 3]): failed on 1 of 2 generated cases
  - `rule_crypto_equity_contagion -> fired on ONE market_anomaly event that answers 2 clauses: rule_crypto_equity_contagion[ELEVATED]`
- **4. Reasoning and forecasting | conviction is capped, finite and not negative** (seeds [1, 2, 3]): failed on 3 of 9 generated cases
  - `nan -> returned nan`
  - `-inf -> returned -inf`
  - `-1.0 -> returned -1.0`
- **4. Reasoning and forecasting | the next trading session skips exchange holidays** (seeds [1, 2, 3]): failed on 3 of 5 generated cases
  - `Wednesday before Thanksgiving: next session is Friday -> said 2026-11-26, wanted 2026-11-27`
  - `Thursday before the observed Independence Day holiday: next session is Monday -> said 2026-07-03, wanted 2026-07-06`
  - `Christmas Eve 2026: Christmas Friday is closed, next session is Monday -> said 2026-12-25, wanted 2026-12-28`
- **4. Reasoning and forecasting | a report on the next real session is seen** (seeds [1, 2, 3]): failed on 2 of 2 generated cases
  - `report 2026-11-27 -> report 2026-11-27 not seen from 2026-11-25`
  - `report 2026-07-06 -> report 2026-07-06 not seen from 2026-07-02`
- **4. Reasoning and forecasting | a synthetic day: 240 situations through collection-shaped events, rules and ranking** (seeds [1, 2, 3]): 78 intact situations: 78 produced a finding. 162 deliberately broken situations: 2 produced a finding. Findings by tier: {'ELEVATED': 58, 'ALERT': 19, 'INTELLIGENCE': 3}. Only 3 of 80 are in a tier that earns a model-written scenario at all. Of the 3 admitted against a budget of 36 an hour, 0 span more than one domain. Share of the inference budget by rule: informed_trading_sequence 3
- **4. Reasoning and forecasting | day: what was missed and what leaked** (seeds [1, 2, 3]): missed intact: []; fired on broken: [('rule_crypto_equity_contagion', 'wrong_subject'), ('rule_crypto_equity_contagion', 'wrong_subject')]
  - `('rule_crypto_equity_contagion', 'wrong_subject')`
  - `('rule_crypto_equity_contagion', 'wrong_subject')`
- **3. Correlation | broken evidence does not fire: drop clause** (seeds [2, 3]): failed on 1 of 2 generated cases
  - `rule_crypto_equity_contagion -> still fired with drop_clause: rule_crypto_equity_contagion[ELEVATED]`
- **3. Correlation | broken evidence does not fire: wrong subject** (seeds [3]): failed on 1 of 9 generated cases
  - `rule_crypto_equity_contagion -> still fired with wrong_subject: rule_crypto_equity_contagion[ELEVATED]`

## Notes

- **1. Collection | naive and aware timestamps are handled, not crashed on** (seeds [1, 2, 3]): failed on 2 of 2 generated cases
  - `naive/aware mix -> raised TypeError: can't subtract offset-naive and offset-aware datetimes`
  - `naive/aware mix -> raised TypeError: can't subtract offset-naive and offset-aware datetimes`
- **1. Collection | non-finite inputs do not become finite premiums** (seeds [1, 2, 3]): failed on 12 of 72 generated cases
  - `px=41.340972052934156 size=123456789.123 -> NaN/inf input produced a finite premium`
  - `px=1581133.5561179037 size=1e-09 -> NaN/inf input produced a finite premium`
  - `px=41.340972052934156 size=4.764620842987281e-06 -> NaN/inf input produced a finite premium`
- **1. Collection | quotes that arrive late are discarded, not inserted** (seeds [1, 2, 3]): 1093 late quotes in 1242 were ignored because a newer one was already held. The feed's quotes arrive in order, so this only matters if a quote's timestamp falls back to the poll time.
- **2. Enrichment | session effect on one $400k print** (seeds [1, 2, 3]): same print scored 09:00=0.99, 13:00=0.99, 13:29=0.99, 13:31=0.99, 14:29=0.99, 14:31=0.45, 19:59=0.45, 20:01=0.45, 21:01=0.99, 23:30=0.99, 03:00=0.99 (thin sessions raise the score by design: the same dollars are larger against less liquidity)
- **2. Enrichment | one share class spelled six ways is one entity** (seeds [1, 2, 3]): primary entity ids: 'BRK.B'->'BRK.B', 'BRK-B'->'BRK-B', 'brk.b'->'BRK.B', ' BRK.B '->'BRK.B', 'BRK/B'->'BRK/B', 'BRKB'->'BRKB' (graph identity would split)
- **2. Enrichment | ordinary English is not mistaken for a ticker** (seeds [1, 2, 3]): failed on 3 of 5 generated cases
  - `Time for AI to say no, analysts say next year -> prose treated as tickers: ['AI']`
  - `THE END OF AN ERA IN EUROPE AND ASIA -> prose treated as tickers: ['END', 'ERA']`
  - `A look at what comes next for the US dollar -> prose treated as tickers: ['US']`
- **2. Enrichment | cashtags and exchange-prefixed tickers are found** (seeds [1, 2, 3]): failed on 1 of 4 generated cases
  - `$tsla recall widens -> entities=[]`
- **2. Enrichment | very large headline and summary are bounded** (seeds [1, 2, 3]): headline 60000 chars, summary 1000 chars after enrichment
- **2. Enrichment | opposite headlines get opposite sentiment (metamorphic)** (seeds [1, 2, 3]): failed on 1 of 4 generated cases
  - `wins approval vs denied approval -> positive 0.33 <= negative 0.33`
- **4. Reasoning and forecasting | report card: power to detect real skill** (seeds [1, 2, 3]): a forecaster who is right 60% of the time is recognised in 20% of runs at n=30, 55% of runs at n=100, 93% of runs at n=300; at 55%: 6% at n=30, 14% at n=100, 41% at n=300. Fewer than 20 resolved calls are never graded. Skill takes months of calls to see.
- **4. Reasoning and forecasting | a model that times out** (seeds [1, 2, 3]): process_cluster lets the error escape: RuntimeError: model timed out. The caller must contain it and count it; see the next check.
- **4. Reasoning and forecasting | scenario saved but the broker is down** (seeds [1, 2, 3]): saved, then the publish raised (ConnectionError: broker down): the scenario exists in the database but was never announced, and the live-feed publish and feedback steps are skipped
- **4. Reasoning and forecasting | a huge move with too little history** (seeds [1, 2, 3]): a 1% move on a 6-close history reads z=0.0: with fewer than 20 prior sessions the size in standard deviations is unknown, not zero

## What held

- 1. Collection | OCC symbols round-trip: held on all 150 generated cases
- 1. Collection | OCC parser never raises on junk: held on all 16 generated cases
- 1. Collection | buyer or seller is only read from a usable quote: held on all 450 generated cases
- 1. Collection | a negative trade size never becomes a publishable premium: held on all 4 generated cases
- 1. Collection | extrinsic premium never exceeds what was paid: held on all 450 generated cases
- 1. Collection | tape returns the quote nearest the print among the quotes it kept: held on all 40 generated cases
- 1. Collection | tape memory is bounded after pruning: 20,000 one-second quotes: 20000 held, 1500 after prune (25 minute window)
- 1. Collection | tape refuses unusable quotes: NaN, infinite, zero and missing bids did not raise
- 1. Collection | the collector's trade timestamps are aware datetimes or nothing: held on all 14 generated cases
- 1. Collection | poll capture weight is a probability: held on all 18 generated cases
- 1. Collection | ticker cleaning lets nothing dangerous through: held on all 15 generated cases
- 1. Collection | FINRA files with bad rows store only sane rows: held on all 5 generated cases
- 1. Collection | fails-to-deliver file skips impossible dates and negative sizes: kept 1 of 4 rows (expected the one valid row)
- 1. Collection | sessions and liquidity factors exist at daylight-saving, holiday and boundary times: held on all 12 generated cases
- 1. Collection | open and close boundaries: 14:30 UTC (09:30 ET) is Session.REGULAR, 21:00 UTC (16:00 ET) is Session.AFTER_HOURS
- 2. Enrichment | well-formed option prints enrich to bounded scores: held on all 120 generated cases
- 2. Enrichment | headline and tags state exactly what was observed about buyer and seller: held on all 120 generated cases
- 2. Enrichment | scores stay bounded in every trading session: held on all 11 generated cases
- 2. Enrichment | moneyness has the right sign for calls and puts: held on all 5 generated cases
- 2. Enrichment | moneyness is None without a spot price: otm_percentage=None with no cached price
- 2. Enrichment | hostile headlines are treated as text and do not break scoring: held on all 10 generated cases
- 3. Correlation | every shipped rule fires on evidence built from its own definition: held on all 60 generated cases
- 3. Correlation | broken evidence does not fire: drop clause: held on all 2 generated cases
- 3. Correlation | broken evidence does not fire: single type: held on all 12 generated cases
- 3. Correlation | broken evidence does not fire: wrong order: held on all 1 generated cases
- 3. Correlation | broken evidence does not fire: outside window: held on all 15 generated cases
- 3. Correlation | broken evidence does not fire: wrong subject: held on all 9 generated cases
- 3. Correlation | broken evidence does not fire: wrong region: held on all 2 generated cases
- 3. Correlation | broken evidence does not fire: move too small: held on all 1 generated cases
- 3. Correlation | broken evidence does not fire: too quiet: held on all 15 generated cases
- 3. Correlation | broken evidence does not fire: trigger too weak: held on all 15 generated cases
- 3. Correlation | broken evidence does not fire: same subject when other required: held on all 1 generated cases
- 3. Correlation | position fixes alone never fire a rule: held on all 15 generated cases
- 3. Correlation | low-scoring random noise never produces a finding without support: held on all 40 generated cases
- 3. Correlation | options leg: 4,320-minute limit: 90% of the limit fires=True, 99.9% fires=False, 120% fires=False
- 3. Correlation | flood of 90 triggers on one rule in one store: 90 of 90 published. Tier sequence: ELEVATEDx15 then ALERTx75. Confidence 0.55 on the first, 0.30 on the last. The hourly budget (ELEVATED: 30) was consulted 90 times for 90 triggers.
- 4. Reasoning and forecasting | report card: false alarms on forecasters with no skill: share of 300 simulated forecasters per setting that were wrongly called skilled (worst across markets and sample sizes): coin 3.0%, always_down 0.0%, always_up 0.0%, overconfident 3.3%. A real hit rate does not earn skill if it only follows the market's lean.
- 4. Reasoning and forecasting | report card: forecasters who are wrong more often than right: a 40%-right forecaster is called worse than chance in 94% of runs at n=300 and 49% at n=100
- 4. Reasoning and forecasting | report card: stated confidence is graded, not just direction: a coin-flipper who claims 90% confidence scores a Brier skill of -0.65 (below zero is worse than stating the base rate)
- 4. Reasoning and forecasting | skill grading survives empty, tiny, one-sided and nonsense inputs: held on all 7 generated cases
- 4. Reasoning and forecasting | priority rises with tier: held on all 4 generated cases
- 4. Reasoning and forecasting | priority rises with confidence, evidence and cross-domain spread: held on all 60 generated cases
- 4. Reasoning and forecasting | a cluster citing no events gets no inference: held on all 1 generated cases
- 4. Reasoning and forecasting | hostile confidence values stay bounded: held on all 8 generated cases
- 4. Reasoning and forecasting | priority is deterministic and order-independent: held on all 30 generated cases
- 4. Reasoning and forecasting | a cold detector on a critical cross-domain cluster still outranks a warm ordinary one: critical/cold 0.603 vs elevated/warm 0.517
- 4. Reasoning and forecasting | only the tiers worth an inference reach the model: held on all 5 generated cases
- 4. Reasoning and forecasting | saved before it is announced: save at step 5, publish at step 6
- 4. Reasoning and forecasting | a scenario the database refused is never announced: returned None, published 0
- 4. Reasoning and forecasting | no stored correlation, no inference: model called 0 times for a cluster with no correlations row
- 4. Reasoning and forecasting | a person's request skips the tier gates but not the stored-row check: forced, no row: model called 0 times
- 4. Reasoning and forecasting | a person's request is served even at the lowest tier: forced WATCH: model called 1 times
- 4. Reasoning and forecasting | a model that returns nothing publishes nothing: published 0
- 4. Reasoning and forecasting | a rule graded as having no edge no longer takes an inference: model called 0 times for a retired rule
- 4. Reasoning and forecasting | a failed synthesis is counted and sent to the dead-letter queue: the completion callback retrieves the exception, counts it and dead-letters the payload
- 4. Reasoning and forecasting | citations are read out of anything the model writes: held on all 1 generated cases
- 4. Reasoning and forecasting | a citation to evidence that does not exist is dropped: held on all 4 generated cases
- 4. Reasoning and forecasting | the no-model call only speaks when both signals lean the same way: held on all 64 generated cases
- 4. Reasoning and forecasting | report dates in odd shapes do not break the skip: held on all 7 generated cases
- 4. Reasoning and forecasting | macro statistics survive flat, zero and reversed closes: held on all 7 generated cases
- 3. Correlation | broken evidence does not fire: no shared tag: held on all 4 generated cases
