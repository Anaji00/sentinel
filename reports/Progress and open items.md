# Progress and open items (7 November 2026)

Saved mid-session. Nothing is committed. The same list is on the Sentinel State artifact.

## Running now

| Item | Where | Next |
|---|---|---|
| G1 follow-up: add insider_cluster to the study's deep history | session g1-headline-history, signal_engine.py | Without it the 14,215 rebuilt clusters are not read; then the 6-hourly study measures buy-side cluster cells |
| Write up the G5 stability result | pass finished: daily-horizon cells stable, the G4 survivors cannot be rated (too few units per half-year) | G5 session to append its note to the scope report; explain the stored-vs-rerun mismatch for negative news 1d (799 vs 247 units) |
| Background watcher for the model run | this session | It dies with the session: if no note arrives, use the resume commands on the page |

## Waiting on a trigger or a date

| Item | Where | Next |
|---|---|---|
| Check the insert rate gap: 14,215 rebuilt insider clusters against the live agent | inserted 7 Nov; rebuilt about 3 a day, live about 1 a day since 5 Oct | Find whether the live agent misses filings or the rebuild includes filers it drops; remove with DELETE FROM signal_event_archive WHERE source = 'insider_cluster_g3_history' |
| Apply the deeper study (G1 headlines and insider clusters, G2 EDGAR to 2016) | code in signal_engine.py; all EDGAR history is loaded; not live | Set SIGNAL_STUDY_HISTORY_DAYS=3995, then recreate agents-heavy. Its limit is 1 GiB and it ran at 817 MiB: raise the limit first. The first run replaces the stored study, so proven cells may change |
| RB1: plain momentum-minus-volatility rule against B1 | registered in MODEL_TRIALS.md, not run; the model run has ended so CPU is free | Needs a script on the 6 GB panel (own container); within 0.15 Sharpe of B1 means the model has not beaten a rule |
| Option flow at 150 seconds against 60 | scripts/options_poll_compare.py | After the 11 Nov close |
| B1 rebuild with fills recorded | paper account | About 30 Nov: traded dollars against $171k, cost against the 10 bps assumed |
| Model-switch margin follow-up | registered test | After 30 nights under the margin |
| Nightly lab and model-switch watch | 7 Nov run done: lgbm_rank kept, margin 0.1 live, lab on simple returns | Read the next nights; return-theme neutrality stays adopted (1.32 against 1.03) |
| Short leg on real borrow data | borrow file collected since 9 Oct | From January 2027, about three months |
| Grade forecasting agents against chance | track record has the chance rate | Needs 20 independent periods; today every horizon reads too few |
| LLM ablation: with and without LLM features | forward only, headline scoring since 29 Oct | About six months, after the model's cutoff |
| Cells waiting for sample length | insider_cluster, options, guidance, news fade | 30-day cells need about 19 months; 5-day cells about 100 days |

## Needs your decision

| Item | Where | Next |
|---|---|---|
| Raise reaction-study signals only where they survive G4 | 5 of 16 proven cells survive on liquid, borrowable names; 8 fail (negative news, 1d and 5d delisting and dilution, auditor changes) | Gate the live signal engine by liquidity and borrowability; changes which signals the idea layer sees |
| Heartbeat for the factor engine | it ran silent for 54 minutes on 6 Nov | Adds monitoring to the process that runs the trader |
| Apply alert-based weights only at rebuilds | drift trades were about 30% of the book on 30 Oct | Changes sizing, so it needs approval |
| Trade timing and passive-first orders | needs real fills first | Later |
| More LLM providers | Mistral free tier (needs a key); OpenRouter (one-off $10 for 1,000 a day); OpenAI has no free tier | Your call on keys and spend |
| Commit the work | a large uncommitted tree, nothing committed this session | Say when |

## Proposed, not started

| Item | Where | Next |
|---|---|---|
| Buy-signal program (reports/Buy signals.md) | nothing on the buy side is proven; opportunistic insider buys and positive earnings run the wrong way | Order: insider clusters (now), activist 13D (after G2), opening call buys (about Feb 2027), buyback authorizations (needs an 8-K parser), contract awards (last) |
| Average gbm and lgbm_rank instead of choosing | needs its own pre-registered trial | After the margin test |
| Borrow fees for the surviving shorts | G4 uses an easy-to-borrow flag and a 10 bps cost; delisting and dilution shorts usually cost far more | Test with the borrow file once enough history exists |
| Fails-to-deliver exclusion and N-PORT signal | data loaded; rule to be registered first | Adds trials |
| G6: one ledger for rules, models and agent calls | MODEL_TRIALS.md | Extend with trial counts across everything |
| G3 fixes 1 and 4 to 7 | relabel 'too young', lift the 300-name bars cap, mark untradeable names, delisted history, retire radar_no_ais | See reports/Unmeasurable signals.md |
| Audit the other layers | collection lag by source; enrichment tags checked by two models; alert families against matched no-alert days | Rules first, as with the connection tests |
| Connection strategies left | pairs found by several methods; partial-correlation graph; intraday lead-lag (C5) | Each with its test first |
| Widen the new Gemini quotas | started low (250 a day) because Google does not show limits | From logged 429 messages |
| Customer links: stronger name matching | two alias collisions removed by name only; OTC lines have no prices | Name must resemble the ticker's company |

## Known problems not solved

| Item | Where | Next |
|---|---|---|
| Daily closes in the tradfi view | 13% of liquid and 18% of other stock-days are off the SIP close by more than 0.2% (the tile said 8% on 29 Oct) | The model and the study read daily_bars and are unaffected; fix or retire the view for closes |
| agents-heavy memory | restarted three times by 02:00 UTC on 7 Nov during heavy reads; limit 1 GiB | Raise the limit or move the study before G1 and G2 deepen it |
| Bar-less instruments: 1,937 stored against 3,146 re-counted | gap unexplained | Re-check after the next study run |
| B1 for real capital | no model passes the 0.95 gate; best estimate Sharpe about 0.40; short leg unmeasured with real borrow | Gate, measured borrow and 24 months forward record |
| Frontend test failing | tests/test_frontend_entity_spacing.py (frontend session's FollowTheMoney.tsx) | Frontend session |
| Model run time | 8 hours when other jobs compete for CPU, 4.5 when they do not | Keep side jobs off the 22:00 to 06:00 window |

## What changed in the code and data this week

- Backtest books and the portfolio lab are scored on simple returns; model-switch margin 0.10; order status and fill recording; chance rate in the track record.
- Hosted LLM layer: fallback chains for scenarios, a wider small-model route (Groq gpt-oss-20b, qwen3.8-27b; Gemini 3.5 and 3.1 flash-lite; safeguard-20b). Reasoning service is live; factor-model loads it on recreate.
- Customer links: name gate in `shared/utils/customer_links.py`; 44 bad rows and 14 graph edges removed (rows saved in the scratchpad).
- Audits: `reports/Intelligence audit.md` (connections C1 to C4, precision), `reports/Event backtest scope.md` (G1 to G6), `reports/Unmeasurable signals.md`, `reports/Free LLM providers.md`.
- Scripts: `scripts/audit_connections.py`, `scripts/audit_tradable_signals.py`; sessions G1 to G5 are adding `scripts/audit_unmeasurable.py` and `scripts/audit_signal_stability.py`.
