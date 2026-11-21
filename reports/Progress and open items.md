# Progress and open items (updated 13 November 2026)

Nothing is committed. Status is as of the evening of 11 Nov. Done = built or run and checked; Dated = waits for a date or sample, nothing to build; Decision = needs your call; Open = can be worked now.

## Decisions taken 11 Nov and built (all five deployed 14:50 UTC)

G4 gate (5 cells live, the rest shadow), factor-engine heartbeat and stale watchdog, hosted wargamer under its own producer id (150 a day), P1 session and 5-session rule in the live divergence watch, alert weights fixed at rebuilds with a hard-veto exception. Details and tests: `reports/Hardening audit - twelve-layer brief.md`.

Closed afterwards (11 Nov, 16:30 UTC, deployed): the generic energy- and fuel-named rules (6 removed; "energy" and "fuel" added to the theme guard and the exposure gate, 36 stored rules, none failing); the 13F history path now skips a ticker and quarter the live path already archived; the insider-cluster rate gap (live is 2.9 a day in November against about 3 a day in the rebuilt history, October was the low month); Gemini quotas (no 429 logged, 2 to 9 calls a day against 250 to 500, so no widening); and a hosted-LLM accounting bug found on the way (unused tokens were given back to the minute the call returned in, leaving minute buckets of -1,193 to -6,080; now given back to the minute reserved).

Also closed 11 Nov (afternoon): RB1 (the plain rule scores -0.28 against B1's 0.40 over eight calendars: B1 leads by 0.68, the model earns its place; MODEL_TRIALS.md); G3 fix 1 (a family younger than the horizon now says "too young") and fix 7 (`radar_no_ais` retired); the IRM brief wording (the brief now states the bar of the rule that fired).

Also closed 11 Nov (evening): G3 fixes 4 to 6. Names Alpaca does not list (delisted, foreign OTC, placeholders, warrants) are now counted `instruments_untradeable` in the study instead of as a gap; the study writes a second, daily-only list of up to 1,500 names (`sentinel:signals:needs_daily`) that the collector backfills without minute bars, against 300 before; delisted history was already done by the collector's `_delisted_backfill` (done-flag set). The unreachable-entry-points guard allowance is 4 (the G2 one-off backfill is a documented operator tool); two gateway route tests read the OpenAPI paths, as FastAPI now wraps included routers.

Closed 11 Nov evening, on the recommendations: the work is committed (five commits); trade timing stays as is, with no passive-first orders (revisit if paper fills show slippage above about 10 bps); no more LLM providers until about 50 graded wargamer predictions; chart 1d candles read daily_bars first; the semantic convergence producer, graded uninformative, now passes the same tier cap as every other producer; the buy-signal programme is deferred until a test is registered; 15-minute and 1-hour scanning is not reopened without a new registered hypothesis. Found, not changed (execution logic needs approval): USAR is in the short leg but Alpaca will not sell it short, so that order is rejected each day (counted, harmless); the book builder checked borrow fees but not Alpaca's shortable flag. Fixed 11 Nov on approval: the trader now skips opening or adding to a short on a name the venue marks unshortable (counted as agents.auto_trader.unshortable); factor-model recreated, takes effect at tomorrow's run. The GMAB sell of 11 Nov is status unknown and resolves itself at the trader's next run. Market-cap floor raised to $10bn on 11 Nov for calls, the analyst and correlation agents, the movers board and sympathy peers; the trader and the filing-change and comment-letter jobs keep $1bn (MODEL_MIN_MARKET_CAP_USD) so the validated book and model features are unchanged; factor-model needs a restart after tonight's run for the peers job to pick it up. Bar-less count closed by the 11 Nov 22:06 UTC study: 3,984 names untradeable, 319 without bars, all queued for the daily backfill. Fills 11 Nov: 93 filled, 1 unfilled, 1 rejected (USAR), GMAB unknown until the trader's next run. Two read routes built 12 Nov for the dashboard: GET /api/v1/signals/held-back/{ticker} (which test held a signal back, with the numbers) and GET /api/v1/health/model-quota (free-tier usage against limits); both exempted in the route-reachability test until the frontend calls them. Not built: per-stock historical analogs, per-ticker GEX. Still open: the dated items only.
Earlier note: the unknown GMAB order and today's fills (recorded after 21:05 UTC); the dated items; your decisions on trade timing, more LLM providers and the commit. Superseded: G3 fixes 4 to 6 (lift the 300-name needs-bars cap and add ETFs; mark untradeable names; delisted history; 0 verdict changes, they load the free price API); daily closes in the tradfi view; bar-less instruments gap; the unknown GMAB order (resolves after the close); fills after 21:05 UTC; the remaining dated items; decisions on trade timing, more LLM providers and the commit.

Left from them: watch the first veto, the first hosted wargame and tomorrow's weight freeze; the 5-session horizon fires 20 to 30 a day on 1,000 names while dispersion is high.

## Added 12-13 Nov (built, deployed unless stated)

| Item | State |
|---|---|
| Daily bars stopped for about 5,800 names after 6 Nov; the model's as-of date stuck at 6 Nov | Collector fixed (tracked names unioned into the post-close and 12-hourly top-ups); repair wrote 84,521 bars, names per session back to about 12,070. Factor model recreated 13 Nov 23:25 UTC to re-run on them: check that its as-of date moves |
| B1 variants trial (pre-registered) | Complete. No variant beat V0 on 6 of 8 calendars by 0.10: V1 time-decay 0/8, mean gain -0.253; V2 thesis exit 1/8, -0.043; V3 shrinkage-covariance weights 0/8, -0.486; V4 volatility trailing stop 0/8, -0.054. The live book is unchanged and none is a candidate |
| Days-to-cover trial (pre-registered) | Not judged: the run hit its 8,400 s limit at 3,000 of 8,798 instruments. Rerun needs a longer window or a narrower instrument list; the registration stands unchanged |
| Dashboard routes for the frontend session | `GET /quotes`, `GET /predictions/track-record/slice`, `GET /portfolio/summary`, `GET /stocks/{ticker}/snapshot` (market cap, 52-week range, next earnings, trailing P/E, SEC metrics), `GET /stocks/screener`, `GET /radar/candles/{ticker}?range=&warmup=` |
| Price alerts, in-app and browser (owner decision 13 Nov) | `POST/GET /alerts`, `GET /alerts/triggered`, `POST /alerts/{id}/ack`, `DELETE /alerts/{id}`; migration 0089; evaluated on read from stored minute bars. The frontend wires the bell and the browser Notification API |
| Whole-system research session | Started 13 Nov (read-only); interim findings come back here |

Findings worth acting on from this stretch:

- The paper ledger of trade ideas is poor at the monthly horizon: 345 calls since 8 Oct (237 closed, 228 of them by stop), 18% right, mean -556 bps net of costs. Closed trades alone read 3% right because winners are still open. It is a ledger of ideas with stops, not the B1 book, and its stop sizing deserves a look before the idea card shows it as a headline.
- The gateway's default broker is a local simulator (fixed $100,000, no positions); `/portfolio/account` still reports it. The new summary route uses the Alpaca paper account instead.
- `stock_thesis.py` reads `user.get(...)` from `require_role(...)`, which returns a role, not a dict; check that route for a 500.

## Added 13 Nov, second half (owner: "complete all")

| Item | State |
|---|---|
| Trader guard on stale scores | Built: orders skipped when the model's scores are 2 or more completed sessions behind; transient, nothing sold |
| Redis maxmemory 400 to 640 MB | Live and in compose; `GET /health/redis` reports fullness and new evictions |
| Autovacuum, unused index | Migration 0090 (autovacuum at 2 to 5% on four tables; `idx_fails_ticker` dropped); `VACUUM` on four smaller tables. `daily_bars` was already autovacuumed |
| `pg_stat_statements`, route timing | Database restarted with the library preloaded; migration 0091; `GET /health/latency` |
| Neo4j index-beta edges | 248 of 639 `STATISTICALLY_CORRELATED_WITH` edges deleted, 0 left |
| Polymarket | Collector queried `/markets` with an ignored `event_slug`, so every slug returned the same 20 markets under the wrong label. Now `slug=`, four pages of 100, volume order, dead slugs pruned |
| Analyst shadow | Moved from Groq 120b to Gemini 3.5 flash-lite; main analyst unchanged; comparison pre-registered |
| Intel briefs | Kept: `macro_intelligence_engine` still publishes; retiring it would touch six subscriber lists for no gain |
| Memory | correlation 2.0 to 2.5 GiB, qdrant 1.0 to 1.5 GiB; `scripts/memory_watch.sh`; compose-budget guard raised by 1 with its reason |
| Idea list | At most 3 names a side per industry or return cluster per horizon (events exempt) |
| Market-cap table | 54 inflated names corrected, then 93 more removed by a cap-over-dollar-volume bound of 10,000x (ADRs read as trillions) |
| Non-company subjects | "AI", "EPS", CEO, FOMC and underscore feed names no longer anchor correlations |
| Retries, pacing | Four long-interval collectors retry a failed pass in 15 minutes (4 times); the news job paces to Groq's token bucket |
| Scores | FINRA short-volume events store z and jump; the other score changes await replay |
| SEC timestamps | 8-K and Form 4 events use the acceptance time |

**Not built, registered with bars:** market-neutral grading of agent calls, CFTC COT / Treasury auctions / Reg SHO collectors, shadow retirement of the generic
macro fan-out rules, the `alpaca_news`, `vessel_spoof` and extended-hours score changes. **Waiting:** the registered top-of-ranking calibration test (06:00 UTC, own container).

## Owner decisions: closed with defaults on 13 Nov ("close"); only item 7 and the commit are yours

| # | Decision | Closed as |
|---|---|---|
| 1 | S5 conviction weights | **Running as a shadow only:** `services/agents/book_shadow.py` records B1's equal weights (S0) and S5 at each rebalance in `book_shadow_books` (migration 0098); it places no orders and the trader never reads it. Report with `scripts/report_book_shadow.py`. Judged only against the registered bar, which needs months. Any live use is still your approval |
| 2 | Crowding filter on the 8-K dilution short | Closed: the effect vanishes on tradable names; nothing changed |
| 3 | finra short-volume score | **Not adopted** (form B has no cut cost but the evidence is weak: Spearman +0.05, standard error 0.054). `SCORE_RESOLUTION_V2` stays off. Extended hours not adopted; alpaca_news waits for stored counts (about 20 Nov) |
| 4 | Generic macro fan-out rules | **Not retired.** Shadow re-runs about 27 Nov; the 50% volume bar is not met by these four rules alone |
| 5 | Trader guard on stale scores and exits | **Stated as intended** (comment in `auto_trader.py`): it skips orders including exits for the day, as the silent-engine guard and every halt do; a halt sells nothing; the book rebuilds monthly from fresh scores |
| 6 | Analyst promotion | **Not promoted.** The Gemini shadow runs; the registered bar (50 resolved calls on both, Brier lower by 0.01, paired bootstrap p under 0.05) decides |
| 7 | Frontend: navigation, up/down colours, Options/Crypto/Charts, civic placement | **Yours:** frontend-only design choices nothing backend waits on |
| 8 | VACUUM of daily_bars | Closed: autovacuum already cleared it; per-table autovacuum settings applied (migration 0090) |
| 9 | CFTC TFF report, NYSE Reg SHO list, Treasury tail | **Dropped** (no signal in commodities, Reg SHO untradable, tail needs a paid source) |
| 11 | Retire the local analyst path and the adversarial wargamer? | Research (consumer facts checked 13 Nov): `sentinel:analyst:views` has NO reader in the gateway or frontend (read only by grading); wargamer v2 is read by `routes/agents.py` plus grading; no skill (Heidke 0.017; wargamer entity-claim hit 0.585 against placebo 0.737). About 22% of hosted tokens (451k of 2.19M in 7 days) plus all local analyst time feed only grading or display. **Yours:** recommended |
| 12 | ~~Demote the quant engine's alert role?~~ **Withdrawn 14 Nov: keep, under the registered A2 test** | The earlier figure (hit 0.448 on 402 calls, t -3.6) counted calls as independent. Clustered by date (357 calls, 29 dates): mean -155.6 bps, t -1.37, not significant; 24-hour calls (the only horizon emitted since 14 Oct) +8.6 bps, t 0.36; the 30-day loss (-1,546 bps, 56 calls) is one rotation week, 8-14 Oct. Nothing in the alert manager reads this engine (it reads correlations, scenarios and intel briefs), so there is no alert role to demote; its calls reach grading, the calls list and `agents.quant.discoveries`. A2 decides after about 20 more trading days. **No decision needed** |
| 13 | Structure changes C1, C2, C4 (agent input gate, dead topics, scenario admission gate) | Registered with replay bars, NOT applied. C1: handler load falls 66.7% (pass) but the registered coverage bar needs lineage that did not exist (fixed 14 Nov; re-run about 28 Nov). C3 and B1 are done: see rows 14 and 15 |
| 14 | Stop or throttle the comment-letter job; keep or stop news scoring (`llm_news`) | **Yours.** Neither `llm_news_30` nor `ev_comment_letter` is a model input (ablation is a no-op); together 58% of hosted tokens. The comment-letter job has written no feature row since 11 Jun. News scoring has 16 days of history and is a forward-only research feature; stopping it saves 45% of hosted tokens but ends that history. Recommended: stop comment letters; keep news scoring as a research feature only if you still want the trial |
| 15 | Price-target rule scoring | Closed: fails its bar (cuts calls 9.5%, bar 20%; the model already agrees with the rule on 94%) |
| 16 | Alert tiering of the undirected 'moves more' family (81% of ALERT-tier correlations; layer audit #1) | Held. Wording-only fix is being built; moving the family to WATCH changes what is sent. Layer audit addendum 1 (19 Nov): the peer family is 82% of ALERT-tier correlations and 65% of Telegram messages (about 135 to 47 a day without it), but it is the ONLY ALERT-tier group with a controlled 1-session gain (+0.33, t 3.30, 17 dates; gone by 5 sessions); the rest of ALERT tier shows nothing at 1 session and trails control at 5 (t -2.51). Demoting the peer family would remove the one group with information; the case for change is wording (it predicts size of move, not direction). **Yours** |
| 17 | Retire analyst_agent local model (4.1 h a week, 95% no-view) and stop wargamer v2 (17% of hosted tokens, 7 graded calls) (layer audit #5, #6) | Held, same as items 11 and 12's earlier form. **Yours** |
| 18 | Trader execution and policy proposals from the quant report | Trials run 20 Nov: T7 de-gross rule missed its Sharpe bar by 0.004 (drawdown -30.0% to -22.8%), T8 tranches meet their bar but are worse at $100k, T5 vetoes and T2/T3/T6 model cells fail. Nothing adopted. T4 (no-trade rule) needs 30 sessions of recorded fills (about 30 Dec); T9 needs the 30 Nov rebuild fills. Findings F1, F2, F5, F11 approved and deployed 19 Nov. **No decision needed now** |
| 19 | Neo4j deletions and scoring-affecting reads (212 index-beta PEER_OF edges, 118 self-loops, edge pruning, SUPPLIER_TO backfill; macro pairing and centrality label filters) | Held; additive fixes are being built. **Yours** |
| 20 | Reddit collector is dead (all subreddits 404 since 17 Nov 17:51); a free path exists: Reddit's official OAuth API (free 'script' app, about 100 requests a minute for non-commercial use) | Needs `REDDIT_CLIENT_ID` and `REDDIT_CLIENT_SECRET` in `.env`, which only you can create, and your judgment on the commercial-use terms. The failure is now counted and visible. **Yours** |
| 10 | Commit and push | **Yours:** nothing is committed; about 150 changed files |

**Verified live 13 Nov:** short interest reaches 30 Oct again; Polymarket events flowing; Redis evictions 0; excess-return grading has 2,542 graded rows; news batches paced with no refusals; regime labels visible in the evidence bundle (NLY, AGNC, SMCI, TSLA).

## Done since 7 Nov (closed)

| Item | Result |
|---|---|
| G1 follow-up: insider_cluster in the deep history | In `DEEP_HISTORY_TYPES`; the deeper study read 464,802 episodes |
| G5 write-up | In reports/Event backtest scope.md |
| Apply the deeper study (G1, G2 to 2016) | `SIGNAL_STUDY_HISTORY_DAYS=3995` live; 18 proven cells |
| agents-heavy memory | Limit raised from 1 GiB to 3 GiB |
| Customer links: stronger name matching | Name gate in `customer_links.py`; 44 rows and 14 edges removed |
| G3 fix 2 (insider cluster history) and ticker hygiene | `insider_clusters.py`, `ticker_hygiene.py`, backfill script |
| Heavy alert noise (Yield/Oil rules, repeats, wrong peers) | 17 rules removed; theme guard; per-session dedup; macro exposure gate; peer-alert cooldown |
| 13F split contamination; 25 duplicate flow rows | Fixed live, in history and in the archive |
| Hosted-LLM fallbacks and quotas | Chains, limits, cooldowns live |
| Divergence detector, situation stages, brief | Built (reports/Divergence alert.md, reports/Hardening audit - twelve-layer brief.md) |

## Dated (nothing to build; waits for a date or sample)

| Item | When |
|---|---|
| Option flow at 150 seconds against 60 | Judge after the 11 Nov close, with `scripts/options_poll_compare.py` |
| B1 rebuild with fills recorded | About 30 Nov |
| Model-switch margin follow-up | After 30 nights under the margin |
| Nightly lab and model-switch watch | Read each morning |
| Short leg on real borrow data | From January 2027 |
| Grade forecasting agents against chance | Needs 20 independent periods |
| LLM ablation | About six months |
| Cells waiting for sample length | 5-day cells about 100 days, 30-day cells about 19 months |
| Borrow fees for the surviving shorts; fails-to-deliver exclusion and N-PORT signal | Need borrow history and a registered rule |
| GEX sign test; days-to-cover test on the short cells | After 60 sessions of GEX history; days-to-cover is registered and can run any time |
| Factor-model recreate (champion gate) | After the 14:43 UTC trader run; a watcher is running |
| B1 for real capital | Gate, measured borrow and 24 months of forward record |

## Decision (yours)

| Item | Note |
|---|---|
| Gate the live signal engine to liquid, borrowable names (G4: 5 of 16 proven cells survive) | Changes which signals the idea layer sees |
| Heartbeat for the factor engine | Adds monitoring to the trader's process |
| Apply alert-based weights only at rebuilds | Changes sizing: approval needed |
| Trade timing and passive-first orders | Needs real fills first |
| More LLM providers (Mistral key; OpenRouter $10 one-off) | Keys and spend |
| Route the wargamer or researcher to a hosted model | Resets that producer's grading series; needs a daily cap |
| Move the P1 session and 5-session rule into the live divergence watch | Passes its rate test; changes a deployed detector |
| Commit the work | Large uncommitted tree |

## Open (can be worked now)

| Item | Next |
|---|---|
| RB1: momentum-minus-volatility rule against B1 | Closed: run 11 Nov (`scripts/run_rb1.py`, amendment RB1a); mean net Sharpe -0.28 after borrow fee (-0.04 with none) against B1's 0.40; result in MODEL_TRIALS.md lines 327-338. Not adopted |
| Insert rate gap: 14,215 rebuilt insider clusters against about 1 a day live | Find whether the live agent misses filings or the rebuild includes filers it drops |
| Gemini 503 handling for analyst_shadow (hosted-only, no fallback) | 20 Nov check: no 429s since the last recreate, so quotas stay; one 503 'high demand' at 15:04 UTC caused 10 retry lines. A cool-down or fallback on 503 is optional and not built |
| Daily closes in the tradfi view | 13% and 18% of stock-days off the SIP close; model and study read daily_bars and are unaffected |
| Bar-less instruments: 1,937 stored against 3,146 re-counted | Re-check after the next study run |
| Retire the generic energy-named rules ("Macro Energy Price Headline ...", three copies) | Not covered by the theme list |
| History path should skip a quarter the live path already published | Stops 13F duplicates recurring |
| Audit the other layers; remaining connection strategies; G6 single ledger | Rules first, tests first |
| Buy-signal program; average gbm and lgbm_rank | Each needs its own pre-registered trial |
| Frontend: render situation and brief; failing tests `test_frontend_entity_spacing`, `test_frontend_api_contract` | Handed to the frontend session on 11 Nov |
| Intraday 15-minute and 1-hour scanning | Dropped by the P2 test; do not build without a new registration |

## Decisions taken on the owner's general instruction, 20 Nov 2026 ("do whats best for accuracy and returns")

| Held item | Decision | Why | Reversal |
|---|---|---|---|
| F12 stale-scores guard counts the same bars table | Built: guard now uses the larger of the SPY-bar count and the weekday calendar | A stalled bar feed made old scores look fresh | Revert `_scores_sessions_behind`; deploy needs a factor-model recreate after 06:00 UTC |
| Alert tiering of the peer family | Keep at ALERT; wording fixed | It is the only ALERT-tier group with a controlled one-session gain (+0.33, t 3.30); the rest of ALERT trails control at 5 sessions | n/a |
| Retire analyst_agent local | Retired (`AGENTS_DISABLED`), analyst_shadow on Gemini kept | 95% no-view, 5 distinct p values, 90% of local model time, no reader of its output | Remove `analyst_agent` from `AGENTS_DISABLED` in `.env`, recreate agents-heavy |
| Stop wargamer v2 hosted | Off (`WARGAMER_HOSTED_MODEL=` empty; was groq/qwen/qwen3.8-27b) | 7 graded calls, 17% of hosted tokens, output read by nothing in trading, alerting or UI | Restore the value, recreate agents-heavy |
| Comment-letter job | Off (`COMMENT_LETTER_JOB=0`) | 13% of hosted tokens, no feature row stored since 11 Jun, not a live-model input | Set to 1 |
| Keep llm_news | Kept running | Its add-or-stop test (C3 amendment) needs about 260 sessions of history; stopping would end the only way to test it | n/a |
| Neo4j junk edges | Deleted 349 with a backup: 219 index-anchored PEER_OF, 118 self-loops, 2 price-shaped sympathy, 10 name-gate SUPPLIER_TO | Market factor and bad data, not links between companies | `reports/graph_junk_edges_backup_20nov.jsonl` has every edge and its properties |
| Neo4j scoring-affecting reads (macro exposure pairing, centrality label, discovery label) | Not changed | Unmeasured effect on scores; the discovery label alone would raise its pair count from 30 to about 27,000 | Needs a registered test |
| Reddit credentials | Cannot be done by an agent | Only the owner can create the OAuth app | n/a |
| Trader policy proposals T4, T5, T7, T8, T9 | Still held | Their registered replays and simulations are running (quant-trials) | n/a |

## Closed and decided, 21 Nov 2026

| Item | State |
|---|---|
| RB1, G3 fixes 1 and 4 to 7, Gemini quota widening | Closed or not needed (already built or no 429s); rows removed from "Open". Optional: a cool-down on Gemini 503 for analyst_shadow |
| P5 market-cap bound 10,000 to 4,000 | Live; no B1 name moves |
| P7 headline primary entity | Built, on (`NEWS_PRIMARY_V2=1`); golden 40 of 41, partly fitted; emoji-entity bug found on live data and fixed same day |
| P6 scenario probabilities | Design and registration written; Phase 0 live (`scenarios.initial_state`, migration 0101); Phase 1 grading at 200 resolved scenarios, about 10 days plus lag, no earlier peek |
| Trader per-day record, order reconcile | Live; one order unresolved (RGTI); positions match except MSGK, MSGS, SCCO |
| F4 correlation failure | Counted and parked in the DLQ (replayable) |
| Senior review | All other findings deployed (hash-checked); L3, L6, the extra F1 guard held for the owner |
| Anomaly-score calibration | Equity-block rescore passed all bars, wired off (`SCORE_CALIBRATION_V2`); owner call: it would put about 95% of blocks below 0.6; the other four missed one bar each |
| Shadow ledger (borrow cost at 2%, per-side veto counts) | Live, first rows written 21 Nov (0.40 bps NAV a day, $3.97 per $100k) |
| CFTC check | `cftc_cot` latest report 2026-11-17, as expected |
| Layer audit, end-to-end | Data holds; Addendum 3 findings 1 and 2 built; 4 (logs lost on recreate) and 3 (sparse factor-score history) open as a question |

**Still the owner's:** four frontend design calls, Reddit OAuth credentials, one real price alert, commit and push; L3, L6, the extra trader guard, the Gemini 503 fallback, turning on `SCORE_CALIBRATION_V2`, T4 (about 30 Dec), T9 (30 Nov rebuild).
**Waits on time:** 424B2 counter and radar log (US open and close), P6 Phase 1, alpaca_news replay, macro fan-out shadow (about 27 Nov), agent-input gate rerun (about 28 Nov), extra-seed noise-floor fits (start 06:00 UTC, ETA about 11:00).
