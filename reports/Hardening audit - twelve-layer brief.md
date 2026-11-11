# Hardening audit, the twelve-layer brief (11 November 2026)

Cyber items are out of scope (the user's instruction) and were not touched. The brief repeats work done under the earlier numbering, so each phase is mapped to what exists before anything new is built.

## Baseline for the phases not covered earlier

| Phase | Test files named | Result before any change |
|---|---|---|
| 3 (rules, tiers) | 14, all present | 119 passed |
| 4 (explainability, wording) | 8, all present | 64 passed |
| 7 (anomaly, graph, Hawkes, correlation) | 41, all present | 858 passed |
| 8 (scenarios, reasoning) | 9, all present | 77 passed |

## Phase 0.3: post-mortem of the NVDA case, checked against the live tables

| Claim in the brief | What the data shows | Cause | State |
|---|---|---|---|
| The decoupling alert took 26 hours, arriving at 23:50 UTC | **Not what happened.** The first "Peer Decoupling Detected" for NVDA is 19:44 UTC on Sunday 8 Nov, before the Monday drop. 70 such alerts followed, the last at 10:11 UTC on 11 Nov | It was never a measurement of a decoupling: it is the quant engine's peer *discovery* (graph candidates plus a Granger lag test) under the wrong headline. Its metrics were NVDA's own hourly Sharpe and drawdown | Headline and text corrected; the detector that measures the gap is `divergence_watch` |
| About 20 repeats at tier 2 | 70 tier-2 alerts for NVDA over three days (56 in 48 hours, 1,105 across 675 tickers) | No cooldown | Once per ticker per 6 hours; live since the 10:38 UTC restart of `agents-fast` (none after 10:11) |
| Peers were wrong | The text read "NVDA vs SPY, AMD, TLT" | Index and bond funds admitted as peers | Stocks only |
| Tier-1 oil alerts matched to NVDA | 114 "Oil Price Spike" and 93 "US2Y Yield Drop" tier-1 alerts name NVDA in three days | Synthesized rules join clauses by time only; nothing in the conditions mentions oil or yields | **Open** (rule retirement and an exposure gate; below) |
| Insider clusters have no standalone edge | Confirmed by the deeper study (reports/Event backtest scope.md) | None to fix | Weight change below |

Across the three days NVDA collected 100 macro-energy, 236 headline and 242 equity-breadth tier-1 alerts. None stated what moved.

## Phase map

| Phase | Status |
|---|---|
| 0, 1 | Done (hardening audit, phases 0 and 1) |
| 2 (universe-wide scanner) | **Partly met.** `divergence.py` and `divergence_watch.py` scan the universe daily against two references (sympathy peers, SIC4 group), z over 60 sessions, news gate, ALERT or ELEVATED tier, calibrated by backtest (median 2 fires a session). **Not built:** intraday horizons (15m, 1h), beta-adjustment, the 70% opposite-peer breadth, ATR and relative-volume gates, the executive and product graph, the four-stage situation state machine. Each is a rule change that needs pre-registering and a backtest on the 120-session window, and the intraday ones need minute bars for the whole universe, which the free tier covers only for the traded book and a watchlist |
| 3 (rule DSL, tiers, dedup) | Baseline green. **Not built:** the new DSL fields, the t>2 promotion bar over 30 trades, quarantine (see phases 6 to 8 report: a sign rule quarantines good rules; a significance bar is proposed), the economic-exposure gate, tier-1 promotion for backed decoupling, and dedup keyed by symbol, rule family and session |
| 4 (Rich Intelligence Brief) | Baseline green. **Not built.** The seven sections need data several of them do not have (executive links, options aggressor z, supplier shock); a brief with empty sections would be the fabrication the brief forbids |
| 5 | Insider clusters: standalone weight to 0.0 and conditioned filters not done; the conditioned tests (C-suite buys with a 10% holdings change; post-drawdown buys) are new registrations |
| 6, 8 (hosted LLM, optimisation) | Done under the earlier numbering (hosted fallbacks and limits, champion gate, shrinkage) |
| 7 (MAD baselines, Hawkes bound) | Baseline green. Not changed: no defect found by the named tests; a spectral-radius cap is not enforced and is a candidate |
| 9, 10 | Covered by earlier passes |
| 11 | **Built:** `tests/test_universe_peer_decoupling_and_richness.py` (5 tests: an upside divergence in banks, a broad rally that fires nothing, two groups found separately, the discovery-alert cooldown and wording, a cooldown-store failure letting the alert through). With `test_divergence.py` (NVDA replay) 16 pass. The full verification suite was not re-run for this brief |

## Built after "build in optimal order" (11 Nov)

| Item | Result |
|---|---|
| Rule-name theme guard | `rule_agent._is_reusable_rule` now rejects a name that claims a market theme (yield, oil, crude, gold, dollar, rates, ...) when no clause filters on it. The trigger can filter only on event type, anomaly size and region, so such a name was untested. New rules with these names cannot be stored. `tests/test_rule_theme_names.py` (4) |
| Stored rules | `scripts/retire_unreusable_rules.py` (dry run by default; `--apply` removes and publishes the tombstone). Dry run: **17 of the 56 stored rules fail**, all of them the Yield and Oil rules behind the flood (including "US2Y Yield Drop and D Equity Upswing" and "Oil Price Spike Leads to Energy Stock Selloff"). **Not applied; awaiting your yes.** Shipped `rule_*` entries are left alone. The first dry run showed every `syn_<hash>` id tripping the event-shaped-id check, so the script judges content under a neutral id |
| Insider cluster weight | **No change needed.** The evidence layer takes weights from the study verdict, and `insider_cluster` is `no_edge` at all four horizons (1h t 0.58, 1d -0.32, 5d -0.79, 30d -1.99), so it already weighs 0.0. Side note: `insider_routine_buy` at 30d is `contradicts_prior` (t -2.3) and `status_of` maps that to "unstudied" (0.25); not changed |
| Exposure gate for macro alerts, dedup by symbol, family and session | **Not built.** The partial-correlation gate needs an exposure estimate per stock that the platform does not store; the retirement above removes the rules that produced the noise, and the quant-engine cooldown already stops the repeats. A session-level dedup in the 3,000-line correlation engine is worth building only after the retirement shows what remains |

Gate: rule, alert, correlation, evidence, divergence and universe tests 513 passed (3 skipped).

## Registered before testing (11 Nov 2026, evening): multi-horizon peer-residual scanner P1

Built on divergence D2; nothing below was run before this was written.

- **Horizons:** 15 minutes, 1 hour, session so far (against the previous close), 5 sessions.
- **Residual:** for stock i and a reference (D2's two: sympathy peers, SIC4 group): `resid = r_i,h - beta * r_ref,h`, beta from an OLS of i's daily return on the reference's over the previous 60 sessions, applied to every horizon. `Z = resid / sigma`, sigma = standard deviation of the same horizon's residual over the previous 20 sessions (intraday) or 60 (5-session), using only data before the moment judged.
- **Fire** when all hold: `|Z| >= 2.5`; at least 70% of the reference's members that have a return move opposite to the stock; `|r_i,h| >= 2%` or `>= 1.5 x ATR_h` (ATR_h = mean absolute return of that horizon over the same trailing window); relative volume `>= 1.25` (the stock's volume in the window over the mean for the same time of day over the previous 20 sessions; not applied to the 5-session horizon).
- **Test:** over the 85 sessions of minute bars (13 Jul to 11 Nov 2026) for the 300 most liquid names, fires counted per stock-session with horizons merged. **T-rate:** median at most 10 stock-sessions flagged per session, 90th percentile at most 25. **T-NVDA (sanity, not independent):** fires on 9 Nov. Failure of T-rate: tighten, re-register, re-run. No claim about what happens after a fire is made (that is a separate study).

### P1 first run (11 Nov 2026): T-rate failed as registered

65 judged sessions (the first 20 of 85 are warm-up), 500 most liquid names, references inside that set. Stock-sessions flagged per session, horizons merged: **median 48, mean 52, 90th percentile 89, maximum 165** (bars: 10 and 25). By horizon, median per session: 15 minutes 30, 1 hour 24, session so far 6, 5 sessions 0. NVDA on 9 Nov fires (session horizon at the 09:30 bucket, z -6.6 against sympathy peers and -6.9 against its industry; 1 hour from the 11:00 bucket, z -3.9 and -5.4; relative volume 3.9 to 6.7), which is a sanity check only. Cause: the 15-minute and 1-hour horizons are looked at 26 and 23 times a day per stock, so a bar of 2.5 sigma that is right for one look produces many chance fires.

### Amendment P2, registered before the re-run (11 Nov 2026)

- Per-horizon Z bars scaled to the number of looks: **15 minutes 4.0, 1 hour 3.5, session so far 2.5, 5 sessions 2.5**. Every other P1 condition is unchanged.
- **Out-of-sample reading:** the 65 judged sessions are split in time. The bars above were chosen after seeing the full first run, so the first 32 sessions are calibration (no claim) and the **last 33 are the test**: T-rate must hold there (median at most 10, 90th percentile at most 25). If it fails, the horizon that still fails is dropped from the live scanner, not re-tuned.
- NVDA on 9 Nov is reported again as a sanity check.

### P2 result (11 Nov 2026)

- **All four horizons: fails the test half.** Calibration half (32 sessions): median 21, 90th percentile 32. Test half (33 sessions): median 21, mean 32.5, 90th percentile 62, maximum 102 (bars 10 and 25). By horizon on the test half: 15 minutes 10, 1 hour 8, session so far 5, 5 sessions 3 (median per session).
- **Applying the registered consequence (drop the failing horizons, no re-tuning): 15 minutes and 1 hour are dropped.** Session so far plus 5 sessions alone: calibration median 6, 90th percentile 10; **test median 8, 90th percentile 19, maximum 30: passes.** NVDA on 9 Nov fires at the 09:30 bucket (session, z -6.6 sympathy and -6.9 industry, relative volume 6.7), sanity only.
- Limits: references are inside the 500-name set, so groups are smaller than D2's; 65 judged sessions; the pass is a rate calibration, not evidence about what follows a fire. The live detector (divergence_watch, D2) is unchanged; moving it to P1's beta-adjusted session and 5-session form is not done.

## Built after "build the rest" (11 Nov, evening)

| Item | Where | Tests |
|---|---|---|
| One alert per stock, rule family and New York session unless the tier rises; counted drops | `shared/utils/alert_dedup.py`, wired in `services/correlation/main.py` for dynamic rules | `tests/test_alert_dedup.py` (5) |
| Macro exposure gate: a theme-named alert on a stock needs a measured exposure (`macro_exposures`, |partial correlation| 0.25 or more, 440 stocks covered); instruments pass; table unreadable means the gate stands aside, counted | `shared/utils/exposure_gate.py`, same loop, before dedup | `tests/test_exposure_gate.py` (5) |
| Four-stage situation (DEVELOPING, CONFIRMED, ESCALATING, RESOLVED), stored per stock, closed when the gap closes, the stock moves with its reference, or 5 days pass unseen | `shared/utils/situation.py`, `services/agents/divergence_watch.py` | `tests/test_situation_and_brief.py` |
| Seven-section brief (what happened, catalyst, peers, evidence, positioning, watch, limits); sections without data say "not measured" and no model is called | `shared/utils/intel_brief.py`; in `metrics_summary["brief"]` and `description` | same file |
| P1/P2 rule and backtest | `shared/utils/peer_residual.py`, `scripts/backtest_peer_residual.py` | `tests/test_peer_residual.py` (5) |

Not built: the executive and product entity graph (no source of executive links; the brief says "not measured"), dynamic-rule DSL fields, promotion at t above 2 over 30 firings, quarantine (needs the significance bar), intraday 15m/1h scanning (dropped by P2), the frontend rendering (handed to the frontend session).

## Five approvals built and deployed (11 Nov, 14:50 UTC)

Approved by the user in chat: the G4 gate, the factor-engine heartbeat, the hosted wargamer, the P1 rule in the live watch, and alert weights only at rebuilds with a veto exception. `correlation`, `agents-fast`, `agents-heavy` and `factor-model` were recreated after the 14:44 UTC trader run (94 orders, 2 failed: USAR rejected as unshortable, GMAB unknown).

| Approval | What runs now | Tests |
|---|---|---|
| G4 gate | Only delisting 30d, dilution 30d, insider opportunistic sell 30d, put sweep 1d and 1h are raised as signals; every other proven cell, including cells the deeper study added after G4 (officer change 1h and others), is shadow: recorded in `sentinel:signals:shadow`, weight 0. A raised signal also needs a liquid name (at least $140.8M a day) and, for a short, a shortable, easy-to-borrow one | `test_tradable_cells.py` (6) |
| Factor-engine heartbeat | 60 s beat on the engine's own loop (component `factor-engine`, on `/health`); watchdog in `agents-fast` raises `STALE_FACTOR_ENGINE` after 180 s and sets a transient halt flag the trader checks after its kill switch; nothing held is sold. Live: beat 48 s old, no loop lag, flag clear. No Prometheus exporter exists for heartbeats, so none was added | `test_factor_engine_liveness.py` (8) |
| Hosted wargamer | `adversarial_wargamer_v2_hosted` on `groq/qwen/qwen3.8-27b`, 150 a day, no local fallback; the old producer stays cut by policy with its 218 graded predictions untouched. Running, no errors, no wargames yet (nothing met its bar) | `test_wargamer_hosted.py` (7) |
| P1 live (session so far and 5 sessions) | Stage two uses the 5-minute aggregate (0.4 s for 11 names, against 9.7 s on raw minute bars). First live scan, 14:54 UTC: 25 alerts, all on the 5-session horizon. The backtest for that horizon shows 10 to 14 flagged per 500 names in the last two weeks of sessions, so 25 on 1,000 names is the same rate, above the early-October median of 3: dispersion has risen. Expect 20 to 30 a day on this horizon while it lasts; it is a volume to watch, not a defect | `test_peer_residual_live.py` (7) |
| Alert weights at rebuilds | Alert weights are fixed into the stored book at rebuild; a hard veto (8-K item 3.01, 1.03 or 4.02, or a 3-sigma divergence against the position) cuts the name (default exit, `B1_VETO_SCALE=0`), freed weight stays in cash, the veto holds to the next rebuild. Today's run was already done, so the book gets its frozen weights at tomorrow's first cycle (equal to the weights it would have had, so no extra trades) | `test_alert_weights_at_rebuilds.py` (8) |

Full suite after these: 5,822 passed, 6 failed before the last fixes. Two of the six were mine (a silent early return in the wargamer and two `dropped` calls with a non-string reason) and are fixed (50 related tests pass). The other four are not mine: `test_frontend_icon_vocabulary`, `test_innovation_layer` (search routes), `test_operational_lifecycle` (integrations route), and `test_unreachable_entry_points_do_not_grow` (supervisor.py, debug_check.py, deep_history_backfill.py, ofac_sync.py). Ruff `F,E9` is clean on every changed file except two unused imports already in `HEAD` in `adversarial_wargamer.py`.

## Closed on 11 Nov (afternoon)

- **17 stored rules removed** (Yield, Oil and Gold names); 39 remain, none failing the guard. The last firing of a removed rule was 11:51 UTC, at the removal; none since. Generic energy-named rules ("Macro Energy Price Headline to Equity Block Convergence", three copies) are not covered by the theme list ("energy" is not a listed theme) and still fire; not widened without evidence.
- **Review** (a read-only subagent, then checked): fixed (1) the news-model job rerunning every 6 hours after a held-over champion (the age now counts the challenger's run); (2) the theme-tag match, which let a tag such as "old" cover "gold" (now whole-word equality; test added); (3) two peer-validity calls in the quant engine that skipped the asset-type cache. Left: possessive or glued names ("Oil's", "US10Y") are not caught by the guard (it fails open). Gate: 602 tests passed.
- **Redeployed:** `correlation`, `agents-fast`, `agents-heavy` (champion gate, shrinkage, theme guard, cooldown). All healthy. `factor-model` still waits for the 14:43 UTC trader run (watcher running).
- **25 duplicate 13F flow rows deleted** with `scripts/remove_duplicate_flows.py` (earliest of each ticker, period and direction kept; the archive's compression limit lifted for that transaction only). 1,877 flow events remain; the copy is `reports/removed_duplicate_flows.csv`. The history path still does not skip a quarter the live path published, so duplicates can recur.

## Decisions needed

1. Build the intraday multi-horizon scanner? It needs a registered rule, a backtest on daily data first, and a minute-bar source for the universe.
2. Retire or strengthen the underspecified synthesized rules (41 of 56 pass the guard) and add the exposure gate for macro alerts. This is the fix for the oil and yield alerts on NVDA.
3. Set the standalone insider-cluster weight to 0.0 and register the two conditioned tests.
4. Recreate `correlation`, `agents-fast` and `agents-heavy` for the champion gate (the 10:38 restart of `agents-fast` and `agents-heavy` already loaded the shrinkage and the cooldown).
