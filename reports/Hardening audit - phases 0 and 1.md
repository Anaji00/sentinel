# Hardening audit, phases 0 and 1 (11 November 2026)

Scope set by the brief: phases 0 and 1 are specified, phases 2 to 9 are named but not specified, so they are not started. Cyber is out of scope throughout (the user's instruction on 11 Nov): `collector-cyber`, its enricher and its tests were not audited or run.

## Phase 0: baseline

Every file, report and script named in the brief exists. Static scripts ran on the host; runtime scripts ran in capped one-off containers (`p0-*`, 0.5 CPU) or inside the API container, outside the 22:00 to 06:00 UTC model window.

| # | Source | Finding | Verdict |
|---|---|---|---|
| 1 | `check_constructor_arity.py` | `collector-crypto/main.py:232` and `:367` build `ResilientWebSocketClient` without `on_connect` | **False positive.** Both URLs carry the stream (`/ws/!forceOrder@arr`, `/ws/!markPrice@arr`), so no subscribe message is needed and `None` is correct. The script should allow-list it |
| 2 | `check_model_refs.py` | 0 issues, 12 payload models resolved | Clean |
| 3 | `check_reachability.py` | 4 `__main__` entry points no compose service runs: `supervisor.py`, `api_gateway/debug_check.py`, `collector-filings/deep_history_backfill.py`, `enrichment/ofac_sync.py` | `deep_history_backfill.py` is a deliberate manual tool. `supervisor.py` and `ofac_sync.py` need a look in phase 4 (is the supervisor started from `agents/main.py`; is the OFAC list synced by anything else) |
| 4 | `find_unreached_code.py` | 32 never-referenced functions, **all inside `.claude/worktrees/determined-haslett-6fa951`**, a stale full copy of the repo (git-ignored) | The scripts scan the stale copy and report each function twice. Exclude `.claude/` from the scans (noted, not yet changed) |
| 5 | `measure_intelligence_flow.py` (180 s, 10:10 UTC, Europe hours) | collected 12.3 a minute, enriched 100%, correlated 47%, inference started 0 | A quiet window, not a fault. Re-run at the US open before reading it as "nothing reaches a model" |
| 6 | `llm_cost_estimate.py` | Metered window shows all hosted steps plus residual local calls | Superseded by finding 9 |
| 7 | `monday_checks.py` | Its default window starts at 13:30 UTC, still in the future at 10:03 UTC | Empty by construction. Standing insider clusters in Redis (LRN, ASPE, LYEL and others) are real |
| 8 | `audit_connections.py` | Reproduces 7 Nov: sympathy +0.29, text peers +0.11, customer links −0.01 and +0.03; clean customer links +0.013 and +0.031 | Unchanged. Input CSVs are the 7 Nov exports |
| 9 | Usage meter, 10 Nov | 238 calls, **226 hosted (95%)**; local: `analyst_agent` on qwen2.5:7b, 11 calls at 163 s each (about 30 minutes of CPU), `rule_synthesizer` 1 call on the 3B | The brief says production is hosted, not local. It is, except the analyst, which stays on the 7B on purpose: the hosted analyst shadow is graded against it. See decisions |
| 10 | `audit_unmeasurable.py` | 30-day windows closed: 0 episodes; 1h/1d/5d fine; 364 + 55 + 35 + 29 bar-less instruments are delisted, odd-symbol, long-symbol or class forms | Unchanged: sample length, not prices. Resolves from about 17 to 23 Nov |
| 11 | `audit_tradable_signals.py`, `audit_signal_stability.py` | Running in `p0-tradable` and `p0-stability` (one to two hours) | Pending: see the end of this file |
| 12 | Invariant tests named in the brief (zero fabrication, tracebacks kept, quiet failures reach a reader) | 64 pass | Clean |
| 13 | `sentinel:signals:study` and the alert layer | The quant engine's "Peer Decoupling" alert and the underspecified synthesized rules (reports/Divergence alert.md) | Fixed in part on 10 to 11 Nov; rule retirement still open |

## Phase 1: fast lane against the hosted LLMs

**Fast-path isolation: already true.** `auto_trader.py`, `factor_engine.py`, `signal_engine.py` and the new `divergence_watch.py` make no call to `hosted_llm`, `ollama` or Neo4j. The only LLM path in `quant_trading_engine.py` is `_execute_with_telemetry`, which sheds (`InferenceShed`) and abandons the advisory when no current inputs exist (it returns nothing, it does not invent a conviction). Nothing needed fixing.

**Redis regime and meta-label overlay: not built, on evidence.** The brief asks the trader to read regime scalars and a meta-label veto and size from them. Both were measured on 7 Nov (MODEL_TRIALS.md): meta-sizing lowers the Sharpe (0.92 to 0.39) and the regime gauge no longer helps (0.66 to 0.63). Wiring them into B1 would apply sizing the data does not support, and CLAUDE.md requires explicit approval to change sizing or execution. The conservative degradation the brief wants (rules when the LLM quota is gone) is what the trader already does, because it never depended on an LLM.

**Rate limits, failover, sanitising: fixed in `shared/utils/hosted_llm.py` and `services/reasoning/context_builder.py`.**

| Gap found | Change | Test |
|---|---|---|
| A 503, a 5xx without `Retry-After`, a 400/401/403/404, a timeout or a connection error did not cool the step, so every following call re-sent to a failing provider (a dead one costs the full 60 s timeout each time) | Each now cools the step: the provider's own wait, else 30 s (5xx, timeout) or 300 s (refused). Counted through `dropped` / `swallowed` | `test_hosted_limits.py` |
| `Retry-After` was read as a number only; an HTTP-date form fell to 60 s; Google's `retryDelay` in the error body was ignored | `retry_seconds()` reads seconds, an HTTP date, or `retryDelay` | same |
| Counters were per calendar minute, so 8,000 tokens at :59 and 8,000 at :01 both passed an 8,000-a-minute limit that the provider counts as rolling. This matches the Groq 429 times (22:00, 22:15, 23:16 UTC on 6 Nov) | The sliding minute: the share of last minute's requests and tokens still inside 60 seconds is added before the check (`sliding()`); the previous minute's key is kept and read | same |
| `response_cache.py` is a stale-while-revalidate cache for API endpoints, not an LLM prompt cache. A prompt-level cache would also answer the schema-retry path (the same prompt re-sent for a new sample) with the same malformed text | **No cache built.** Added a counter instead (`sentinel:hosted_llm:dup:{day}`, calls and repeats by caller, 10-minute window) so the repeat rate is measured before anything is cached | same |
| `context_builder.py` put the cluster description, event headlines (60 characters each) and recent news into the scenario prompt unsanitised; only the headline section was fenced at render time | Sanitised where collected with `sanitize_untrusted` | `test_context_builder_sanitises.py`; one guard-test anchor updated with a comment |

**Gate:** the 29 test files named in the brief plus the two new ones: 360 passed. The invariant tests: 64 passed. `ruff --select F,E9` is clean on every changed file; one older F401 in `context_builder.py` and two F541 in `quant_trading_engine.py` are unchanged from `HEAD`.

**Not deployed.** The hosted-layer changes load when a service restarts. The code is bind-mounted, so a recreate of `agents-heavy`, `factor-model` and `reasoning` applies them.

## Decisions needed

1. **The local analyst.** Retiring the 7B analyst (about 30 minutes of CPU a day) ends the hosted-against-local grading that exists to decide whether the hosted analyst is as good. Keep until about 20 resolved forecasts each, or retire now?
2. **Redis overlays for B1.** Not built, on the 7 Nov evidence. A read-only overlay that only vetoes (never sizes) from a stale-or-missing check is possible if you want it.
3. **Deploy.** Recreate the three services to load the limit and sanitising changes.

## Phase 0 baseline, long audits (finished 11 Nov, run against the 18 proven cells of the deeper study)

`audit_tradable_signals.py` (liquid and borrowable names). **Survives V2:** 8-K delisting notice 30d (449 bps, t 2.5), 8-K dilution 30d (253 bps, t 2.2), 8-K officer change 1h (34 bps, t 2.6, price-directed),
insider opportunistic sell 30d (84 bps, t 3.3), put sweep 1d (79 bps, t 3.1). **Liquid only:** 8-K dilution 1d, earnings fade 5d, earnings reaction down 30d. **Fails:** 8-K delisting 1d and 5d, dilution 5d,
impairment 5d and 30d, put sweep 1h (now t 1.85, was 2.2), negative news 1d and 5d. Same picture as 7 Nov, with the put sweep 1h cell slipping.

`audit_signal_stability.py`: the daily-horizon cells are stable across 11 half-years (delisting 1d and 5d, dilution 1d and 5d: 9 to 11 of 11 half-years in the same direction); the 30-day cells, including every survivor above,
cannot be rated (too few independent units per half-year). Unchanged from 7 Nov.
