# Progress and open items (updated 11 November 2026)

Nothing is committed. Status is as of the evening of 11 Nov. Done = built or run and checked; Dated = waits for a date or sample, nothing to build; Decision = needs your call; Open = can be worked now.

## Decisions taken 11 Nov and built (all five deployed 14:50 UTC)

G4 gate (5 cells live, the rest shadow), factor-engine heartbeat and stale watchdog, hosted wargamer under its own producer id (150 a day), P1 session and 5-session rule in the live divergence watch, alert weights fixed at rebuilds with a hard-veto exception. Details and tests: `reports/Hardening audit - twelve-layer brief.md`.

Closed afterwards (11 Nov, 16:30 UTC, deployed): the generic energy- and fuel-named rules (6 removed; "energy" and "fuel" added to the theme guard and the exposure gate, 36 stored rules, none failing); the 13F history path now skips a ticker and quarter the live path already archived; the insider-cluster rate gap (live is 2.9 a day in November against about 3 a day in the rebuilt history, October was the low month); Gemini quotas (no 429 logged, 2 to 9 calls a day against 250 to 500, so no widening); and a hosted-LLM accounting bug found on the way (unused tokens were given back to the minute the call returned in, leaving minute buckets of -1,193 to -6,080; now given back to the minute reserved).

Also closed 11 Nov (afternoon): RB1 (the plain rule scores -0.28 against B1's 0.40 over eight calendars: B1 leads by 0.68, the model earns its place; MODEL_TRIALS.md); G3 fix 1 (a family younger than the horizon now says "too young") and fix 7 (`radar_no_ais` retired); the IRM brief wording (the brief now states the bar of the rule that fired).

Also closed 11 Nov (evening): G3 fixes 4 to 6. Names Alpaca does not list (delisted, foreign OTC, placeholders, warrants) are now counted `instruments_untradeable` in the study instead of as a gap; the study writes a second, daily-only list of up to 1,500 names (`sentinel:signals:needs_daily`) that the collector backfills without minute bars, against 300 before; delisted history was already done by the collector's `_delisted_backfill` (done-flag set). The unreachable-entry-points guard allowance is 4 (the G2 one-off backfill is a documented operator tool); two gateway route tests read the OpenAPI paths, as FastAPI now wraps included routers.

Closed 11 Nov evening, on the recommendations: the work is committed (five commits); trade timing stays as is, with no passive-first orders (revisit if paper fills show slippage above about 10 bps); no more LLM providers until about 50 graded wargamer predictions; chart 1d candles read daily_bars first; the semantic convergence producer, graded uninformative, now passes the same tier cap as every other producer; the buy-signal programme is deferred until a test is registered; 15-minute and 1-hour scanning is not reopened without a new registered hypothesis. Found, not changed (execution logic needs approval): USAR is in the short leg but Alpaca will not sell it short, so that order is rejected each day (counted, harmless); the book builder checked borrow fees but not Alpaca's shortable flag. Fixed 11 Nov on approval: the trader now skips opening or adding to a short on a name the venue marks unshortable (counted as agents.auto_trader.unshortable); factor-model recreated, takes effect at tomorrow's run. The GMAB sell of 11 Nov is status unknown and resolves itself at the trader's next run. Still open: the bar-less count (read after the study at about 22:20 UTC) and the dated items.
Earlier note: the unknown GMAB order and today's fills (recorded after 21:05 UTC); the dated items; your decisions on trade timing, more LLM providers and the commit. Superseded: G3 fixes 4 to 6 (lift the 300-name needs-bars cap and add ETFs; mark untradeable names; delisted history; 0 verdict changes, they load the free price API); daily closes in the tradfi view; bar-less instruments gap; the unknown GMAB order (resolves after the close); fills after 21:05 UTC; the remaining dated items; decisions on trade timing, more LLM providers and the commit.

Left from them: watch the first veto, the first hosted wargame and tomorrow's weight freeze; the 5-session horizon fires 20 to 30 a day on 1,000 names while dispersion is high.

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
| RB1: momentum-minus-volatility rule against B1 | Registered in MODEL_TRIALS.md, not run; needs a script on the 6 GB panel in its own container, outside 22:00 to 06:00 UTC |
| Insert rate gap: 14,215 rebuilt insider clusters against about 1 a day live | Find whether the live agent misses filings or the rebuild includes filers it drops |
| Widen the Gemini quotas | From logged 429 messages |
| G3 fixes 1 and 4 to 7 | Relabel 'too young', lift the 300-name bars cap, mark untradeable names, delisted history, retire radar_no_ais |
| Daily closes in the tradfi view | 13% and 18% of stock-days off the SIP close; model and study read daily_bars and are unaffected |
| Bar-less instruments: 1,937 stored against 3,146 re-counted | Re-check after the next study run |
| Retire the generic energy-named rules ("Macro Energy Price Headline ...", three copies) | Not covered by the theme list |
| History path should skip a quarter the live path already published | Stops 13F duplicates recurring |
| Audit the other layers; remaining connection strategies; G6 single ledger | Rules first, tests first |
| Buy-signal program; average gbm and lgbm_rank | Each needs its own pre-registered trial |
| Frontend: render situation and brief; failing tests `test_frontend_entity_spacing`, `test_frontend_api_contract` | Handed to the frontend session on 11 Nov |
| Intraday 15-minute and 1-hour scanning | Dropped by the P2 test; do not build without a new registration |
