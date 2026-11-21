# Senior code review, 18 Nov 2026

Read-only. No repo source, test or config was edited, nothing was committed, no job was run. One read-only repro script was added: `scripts/review_repros_18_nov.py` (pure functions only: no database, Redis, broker or network). Two read-only `SELECT`s were run against `agent_predictions` (F8).

**Scope covered.** The uncommitted tree (95 modified and about 170 untracked files): the new gateway routes, `auto_trader`, `factor_engine`, `grader`, `excess_grading`, `grades_v2_job`, `book_shadow`, `market_cap_job`, `recommendation_engine`, `llm_news_job`, `signal_engine`, the new `shared/utils` modules, `migrate.py` 0089 to 0099, the filings, macro, news, prediction and SAR collectors, correlation, `docker-compose.yml`. Then by risk: `shared/broker/alpaca.py`, the Kafka wrapper and the agent and correlation consume loops, the DB pool, `rbac`/`verify_api_key`, `response_cache`, `sec_rate`, and a repo-wide pass for blocking calls, SQL built from strings, `eval`/`shell`, naive datetimes and SSRF. **Not covered in depth:** `grader.py` (outside the diff), `signal_engine` internals, the whole of `radar.py`, enrichment, the frontend beyond `marketSession.ts`, 400 test files beyond a structural scan. Addenda 3 and 4 of the 13 Nov research file were read; items they list as fixed are not repeated (M1, M3, M5, M6, M7, H1 were re-checked in the current code and are fixed).

Ranking: **Critical** = can place wrong orders or corrupt scores or grades. Each finding has file:line, scenario, evidence, fix and the test that would catch it.

---

## Critical

### F1. A failed positions call makes the trader think it holds nothing and opens the whole book again
`shared/broker/alpaca.py:122` (`get_positions`: any non-200 answer returns `[]`), used at `services/agents/auto_trader.py:315`.

**Scenario.** On a day after the rebalance the book is held (about 100 longs, 50 shorts). `GET /positions` answers 429 or 503 (the gateway also polls this key every 15 s through `/portfolio/summary`). `get_positions` returns `[]`, so `held = {}`, `foreign = 0`, and `plan_orders(target, {}, managed, prices)` turns every target into an `open` order with today's `client_order_id`. Those ids are new for the day (the first order of the day is not a duplicate of yesterday's), so the venue accepts them: the book is doubled until the next successful run trims it. Names dropped from the book are not closed that day either. Nothing in `run_once` compares `held` with the stored book or with `auto_trade_orders`. `get_account` (line 52) and `submit_order` raise on error; `get_positions` is the only read of the three that fails open.

**Evidence.** `python scripts/review_repros_18_nov.py`: with the correct `held`, `plan_orders` returns `[]`; with `held = {}` it returns `[('AAA','buy',100.0,'open'), ('BBB','sell',50.0,'open')]`. The broker code path is read directly (lines 98 to 122).

**Fix.** `get_positions` raises `RuntimeError` on a non-200 (same as `get_account`); `run_once` also refuses to trade when `held` is empty but `auto_trade_orders` has fills on record for the book and the account has equity above cash. Do not change sizing or caps.
**Test.** `AutoTrader.run_once` with a fake broker whose `get_positions` raises or returns `[]` while the DB shows managed symbols: assert no order is sent and a `dropped(...)` is counted. A broker test that a 503 on `/positions` raises.

---

## High

### F2. The new book is stored before the stale-scores guard runs, so a rebuild from stale scores is frozen for a month
`services/agents/auto_trader.py:291` (`_book` runs, and at `:548` writes `BOOK_KEY` with `rebalanced_on = today`), guard at `:347`.

**Scenario.** It is a rebalance day (21 sessions since the last book) and the model's scores are 2 or more sessions behind (the 6 Nov outage again). `_book` builds the book from the stale percentiles and stores it. Then `scores_too_old` halts the day and returns without `_mark_done`. The next day the scores are fresh, but `rebalance_due` is false because the stored book says it was rebuilt yesterday, so B1 is held on stale-score picks for another 21 sessions. The same applies to a manual `AUTO_TRADE_HALT` or a safety halt on a rebalance day (`_halted`, `reasons` are also checked after `_book`). The guard's own comment says the book "needs fresh scores", but it runs after the book has been written.

**Fix.** Check engine staleness and `scores_too_old` before `_book` can write; when stale, return the stored book unchanged (or `None`) and do not write `BOOK_KEY`. Also store `scores_as_of` in the book (the comment at `auto_trader.py:46` says it is there; `_book` writes only `rebalanced_on`, `long`, `short`).
**Test.** Fake Redis with a book due for rebuild and a report whose `as_of` is 3 sessions old: after `run_once`, `BOOK_KEY` is byte-identical.

### F3. The factor report (with the new `as_of`) is published before the thin-run check, so every freshness guard reads a date the scores do not have
`services/agents/factor_engine.py:529` (`raw.set(FACTOR_REPORT_KEY …)`), thin-run check at `:547`.

**Scenario.** A run that ranks only 94 names (it happened on 3 to 4 Nov) writes the report with tonight's `as_of`, then at `:547` withholds the scores: `FACTOR_SCORES_KEY` and the horizon keys keep the previous full set. `_scores_sessions_behind`, `FactorEngine._behind`, `_warn_if_still_behind`, the strip's `model_as_of` and `BookShadow` all read `as_of` from the report and report "fresh", while the scores the trader ranks from are older. The new stale guard (F2's check) is therefore defeated exactly in the thin-run case it was added for, and `_warn_if_still_behind` never warns.

**Fix.** Write the report only when the scores are published, or write `scores_withheld: true` and a separate `scores_as_of` that guards read; readers use `scores_as_of`.
**Test.** Run `FactorEngine.run_once` with a stubbed `fm.run_panel` returning 94 scores: assert `FACTOR_REPORT_KEY` is unchanged (or carries `scores_withheld`), and that `scores_too_old` sees the old date.

### F4. A correlation event whose processing raises is committed and lost, counted nowhere
`services/correlation/main.py:3157` (`except Exception … return`), commit at `:3577`; the batch wrapper at `:3552` uses `gather(..., return_exceptions=True)`.

**Scenario.** Redis, Neo4j or Postgres blips for 30 s. Each event fails inside `_process_correlation_event`, which logs a traceback and returns normally; `consumer.commit()` then advances the offsets. Those events never produce correlations, are never retried, and `dropped`/`swallowed` is never called, so no counter or heartbeat shows the loss (the comment at `:2511` says "nothing is silently dropped from the offset record", which holds only for the stale-skip path). CLAUDE.md: "Failures are counted, never silent".
**Fix.** Call `swallowed("correlation.event", e, logger)` at `:3157`; for infrastructure errors re-raise so the batch is not committed (at-least-once, which the downstream de-duplication on `event_id` allows), or send the event to the DLQ.
**Test.** `_process_correlation_event` with a store that raises: assert `swallowed` was counted and the offset is not advanced (or a DLQ message was sent).

---

## Medium

### F5. A liquidity-capped short is cut to a fractional quantity, the venue refuses it, and enough of them halt the book until someone deletes a Redis key
`shared/utils/auto_trade.py:293` `liquidity_capped` (`math.floor(... * 10**4)/10**4`); applied at `services/agents/auto_trader.py:368`; halt at `:384` (`execution_halt`, `_halt` writes `HALT_KEY`).

**Scenario.** A 40-share short open or add on a name whose 1% of average dollar volume buys 12.4 shares is sent as `qty=12.4`. Alpaca does not accept fractional short sales, so it is refused ("rejected", counted as failed). If more than 25% of 5 or more orders fail, `_halt` writes `HALT_KEY`, which "is cleared only by hand". Shorts are the bottom decile of $1bn-plus names, where the cap binds most often.
**Evidence.** `scripts/review_repros_18_nov.py`: `Plan("XYZ", 40.0, "sell", "open")` with ADV $31,000 and 1% participation returns qty 12.4. (Trader sizing and caps are not changed by this review; the fix below only keeps a short whole.)
**Fix.** For a sell that opens or adds a short, floor the capped quantity to a whole number, and drop the plan when it floors to 0.
**Test.** `liquidity_capped` property test: for sell-side open/add/flip-open plans the result is integer-valued.

### F6. A hung handler on a sparse topic no longer shows as STALLED
`services/agents/base.py:1417`, `shared/utils/heartbeat.py:66` (`stall_seconds`).

The new rule sets stalled seconds to 0 when the consumer's lag is 0. The agent loop commits and advances its position when it accepts a batch (`base.py:1013`), so a handler that hangs on the only message of a quiet topic (a model call that never returns, a stuck DB call) leaves `lag = 0` and `idle > 0`: no STALLED line, no heartbeat flag, and the loop-blocking sampler does not see it either (the event loop is free). Repro: `stall_seconds(2400, 0) == 0.0`. The 572 false lines the change removed were real noise, but this removes the signal for the case the label was built for.
**Fix.** Track the oldest in-flight dispatch (`self._inflight` already exists): stalled if lag > 0 **or** any dispatch has been running longer than N seconds. **Test.** One in-flight task older than the limit with lag 0 reports a stall.

### F7. The S5 shadow recorder can leave a half-recorded rebalance and weights from the wrong scores
`services/agents/book_shadow.py:51` (`_HAVE_SQL` looks only at variant S0) and `:60` (two separate inserts); scores read at `:55`.

(a) If the S5 insert fails after S0 succeeded, `already recorded` is returned for ever and the paired test loses its S5 leg. (b) The weights use the percentiles in Redis now, not those that formed the book (the book has no `scores_as_of`, see F2), so a restart days after the rebalance records weights from newer scores.
**Fix.** One statement or one transaction for both variants; require the report's `scores_as_of` to equal the book's. **Test.** A `db.execute` that fails on the second call: the next pass records both.

### F8. The same agent prediction is persisted several times, with contradictory outcomes
`services/agents/prediction_scoring.py:949` (`INSERT INTO agent_predictions` with no `ON CONFLICT`); `agent_pred_prediction_idx` (`migrate.py:581`) is not unique.

**Evidence (read-only query, 18 Nov).** 6 `prediction_id`s have more than one row, 41 extra rows in 4,365. `SR-20261117-001` has 6 rows written between 11:21 and 23:13 with `outcome_correct` t, t, t, t, f, t; `SCS-FLASHPOINT-2026-09-20-001` has 3 (t, f, t). They belong to `adversarial_wargamer_v2_hosted`. Each re-resolution adds a row and moves the agent's hit rate and Brier score.
**Fix.** A unique index on `prediction_id` (after collapsing the existing duplicates) and `ON CONFLICT DO NOTHING`; find why the resolver revisits a resolved prediction (the Redis copy is rewritten after the insert, so a failed write re-resolves it). **Test.** Resolve the same prediction twice: one row.

### F9. `sec_slot()` paces one process; the filings poller and firehose bypass it
`shared/utils/sec_rate.py:23` (module-global lock), unpaced SEC calls at `services/collector-filings/main.py:380` and `edgar_firehose.py:123`.

CLAUDE.md: SEC requests "go through sec_slot()". The limiter is per process (the module doc says so) at 6 requests a second, while SEC's limit is 10 a second per client for the whole host. The agents-heavy jobs, the factor-model fundamentals/company-facts loops, the filings collector history loops and `tradfi/sec_holdings` are separate processes, so two of them overlapping already exceeds 10; the live pollers add to it. This is the 8 Oct 2026 incident (13 requests a second, SEC slowed to about 40 s per response). **Fix.** A Redis-based shared slot (`SET key NX PX` token, or a Lua leaky bucket) used by both bulk and live callers. **Test.** Two simulated processes against a fake Redis never exceed the budget in a window.

### F10. `detached.run` reports a task that failed with TimeoutError as "Warming"
`shared/utils/detached.py:39` catches `asyncio.TimeoutError` around `wait_for(shield(task))`; on Python 3.11 that is the builtin `TimeoutError`, which the task itself raises when asyncpg's `command_timeout=60` (`shared/db/__init__.py:272`) expires.

**Scenario.** The breadth window query takes about 43 s cold and is growing. When it passes 60 s the task fails with `TimeoutError`; the caller gets `Warming`, the route answers 503 "Still computing, try in about a minute", the next caller starts a fresh task that fails the same way. The page says "computing" for ever; the only trace is `swallowed(utils.detached.task)` in the log.
**Evidence.** `scripts/review_repros_18_nov.py`: a task that raises `TimeoutError` surfaces as `Warming`.
**Fix.** Time out with `asyncio.timeout`/a sentinel distinct from the task's exception: check `task.done()` after the wait, and re-raise `task.exception()`. **Test.** A coroutine raising `TimeoutError` through `run` raises `TimeoutError`, not `Warming`.

### F11. Limit prices are built from the last IEX trade with no age check
`shared/broker/alpaca.py:140` reads only `p`; the trade time `t` is ignored. A name that has not traded on IEX for days (small and mid caps) gives a stale reference; after a 10% gap the marketable limit (0.5% offset) never fills and the order is logged "unfilled". **Fix.** Return `(price, time)` and fall back to the venue mark or a market order when the trade is older than a few minutes. **Test.** `latest_prices` with an old `t`.

### F12. The stale-scores guard counts sessions from the same `daily_bars` table the model runs on
`_SESSIONS_AFTER_SQL` (`auto_trader.py:62`, `factor_engine.py:53`) counts distinct SPY days after `as_of`. If bar ingestion for everything stops, SPY stops with it, the count stays 0 and trading goes on with scores that are weeks old against a market that kept moving. **Fix.** Compare `as_of` with the broker clock or an NYSE session calendar. **Test.** A report 5 weekdays old with a table that has no newer SPY bars must halt.

### F13. Excess grading parks rows after three daily passes without bars
`services/agents/excess_grading.py:103` (attempt counted on `stock is None or spy is None`), `MAX_ATTEMPTS = 3`. If bar top-ups are down for three days (they were for 5,800 names from 6 Nov), every pending row is parked for good, though a backfill would have priced it; the grader never retries parked rows. Also the start close is the close **of the call's date**, which for a call made during the session is after the call (a half-day bias on the 24-hour horizons; `excess_return.py:63`). **Fix.** Do not count an attempt while `max(day)` of the ticker's bars is older than the exit date (the data is late, not missing); count it only when bars exist beyond the exit date and the ticker still has none. State the start-of-window convention in the report note. **Test.** A row with no bars up to exit date + 1 is not parked; one with later bars and no stock series is.

### F14. `track-record/slice` still drops open trades that have no daily bar
`services/api_gateway/routes/predictions.py:469` uses `JOIN LATERAL ... ON true` (inner) for the open marks. Carry-over of Addendum 4 M4: an open idea with no bar vanishes from `n_calls` instead of being counted as unmarked. Use `LEFT JOIN LATERAL` and report the number unmarked.

### F15. The watch strip reads the whole shadow set on every computation
`services/api_gateway/routes/strip.py:97` `zrevrange(SHADOW_KEY, 0, -1)` returns every member (JSON blobs) each time the cache (10 s) expires for each distinct ticker list. Use a bounded range (the newest N that `held_back_for` needs) or cache the parsed set once.

---

## Low

- **L1. A test that cannot fail.** `tests/test_conflict_zones.py:129` `test_a_429_stops_the_cycle_rather_than_reading_as_empty`: `try: … except m.RateLimited: return` with no failure branch; if `_get_json` returned `[]` on a 429 (the exact bug named) it passes. Add `else: pytest.fail(...)`. Ten more tests have no assertion by design ("does not raise"): `test_db_write_batch_integrity.py:55,78`, `test_entity_decoration.py:64`, `test_inference_slot_utilisation.py:187,194`, `test_oidc_sso.py:174,214`, `test_open_questions_drive_inference.py:122`, `test_response_normalization.py:186`, `test_structured_output_format.py:195`; the two OIDC ones should assert the returned claims.
- **L2. Poison pill path in the agent loop.** `base.py:964` catches only `json.JSONDecodeError`; a non-UTF-8 payload (`UnicodeDecodeError`) or a null value escapes to the outer handler, which sleeps 5 s and drops the rest of the polled batch (all partitions) uncounted and without a DLQ message. Catch `(ValueError, AttributeError)` there.
- **L3. API key prefix in identity.** `dependencies.py:511` `identity = f"apikey:{api_key[:8]}"`; the identity is written to `price_alerts.owner`, the alias `source`, cache keys and rate-limit keys. Eight characters of a credential belong in no store: use a hash. Also a request that carries a wrong `X-API-KEY` and a valid session cookie is rejected (`if api_key and API_KEY … elif session_cookie`).
- **L4. Study retries amplify load.** `signal_engine._study_loop` retries a failed six-hour study four times at 15 minutes. If the failure is database saturation (CLAUDE.md "don't retry overload"), it re-runs the heaviest read four times in an hour. Retry only for connection errors.
- **L5. Index builds in a transaction.** Migrations 0096 and 0097 are plain `CREATE INDEX` on `correlations` (about 400k rows and growing): writers block for the build (seconds now, minutes later). Future large ones should use `CREATE INDEX CONCURRENTLY` with `"transactional": False`.
- **L6. Flip order timing.** `auto_trader._await_fill` returns silently after 30 s; the `flip-open` is then sent while the close is open, and if the venue holds the shares for the close it is refused (counted as failed, no worse). Return a flag and skip the open that day.
- **L7. Zero ratios dropped.** `grades_v2.py:72` `if v and c` discards a legitimate 0.0 move; use `is not None`.
- **L8. Naive timestamps.** `grader.py:263` `datetime.utcnow()`, `enrichment/db_writer.py:56` `datetime.now()`.
- **L9. Redis headroom.** `docker-compose.yml`: 640 MB maxmemory in a 1 GB container; a `BGREWRITEAOF` under write load can approach double the resident set through copy-on-write. Watch `used_memory_peak` plus fork overhead before raising it further. Also the Redis comment still contradicts itself (Addendum 4 L7).
- **L10. Dropped-for-liquidity names leave the book.** `market_cap.cap_rows` omits a name whose cap is over 10,000 times its 30-day average dollar volume; `picks_only` then removes it, and the trader closes the held position. A real company with a short zero-volume stretch (a halt) would be closed on a data artefact. Count it (`dropped`) and keep the previous cap for a held name.

---

## Checked and found sound

- **Auth.** Every new route in `alerts`, `calendar`, `changes`, `evidence`, `market`, `quotes`, `screener`, `stock_snapshot`, `strip` carries `require_role(Role.VIEWER)`; the app-wide `verify_api_key` covers the three new `/health/*` routes (any authenticated caller; none returns user data). `require_role` returns the `Role`, and no route reads it as a dict. No SQL built from request data (only the constant `_COLUMNS` in `alerts.py:103`). Alert ownership is checked on `ack` and `delete`.
- **Price alerts.** `_evaluate` now advances `checked_at` only for alerts whose bars were read; the fired update is guarded by `triggered_at IS NULL`.
- **Order idempotency.** `client_order_id` is stable per (book, day, symbol, side, reason); a duplicate is read as success; a timed-out submit is looked up before it is called failed; shorts are whole shares in `target_shares`; limit prices are marketable and bounded; flips close first.
- **Safety arithmetic.** `safety_halts` day loss, drawdown and the 21-session drift use the recorded history oldest-first and `history[-21]` is right; `cap_weights` leaves the excess as cash; vetoes are applied after normalisation.
- **Excess and beta grading maths.** `window_return` / `close_on_or_before` bound stale bars at five days, `beta_to_spy` excludes the call's own day and returns `None` under 60 returns or zero variance, `clustered_t` takes the spread of day means; the pending SQL and the `direction_sign` word list agree (no starvation).
- **Retry cadence.** `retry_sleep.next_delay` is bounded and resets on success; each collector's loop resets its counter correctly.
- **Migrations 0089 to 0099.** 99 unique, ordered versions; additive columns with `IF NOT EXISTS`; `daily_bars` is a plain table (the autovacuum storage options apply); the partial indexes match the queries.
- **Polymarket changes.** A purge now needs both endpoints to answer empty and cleanly (`events_answered`); `belongs_to_slug` stops the default-20 mislabel; pagination stops on a short or failed page.
- **`short_volume` tag.** The z tag and `score_resolution.short_volume_z` both floor the spread at `SHORT_MIN_SD` (Addendum 4 M6 is fixed).
- **Event loop.** No `requests` or blocking `sleep` in `services/` or `shared/` outside two sampler threads; no `shell=True`, `pickle`, `yaml.load`; CPU work in `grades_v2_job`, `factor_engine` and large `query` results goes through `to_thread`.
- **Latency book.** Bounded by `MAX_ROUTES` and the `(unmatched)` label; no raw paths.
- **Response cache.** One refresher behind an `NX` lock, stale copy kept on failure, failures counted; the per-user routes (`changes`) include the owner in the key.

## Suggested order

F1 first (a one-line raise plus a guard), then F2 and F3 together (both are about when the book and the report are written), F4, F5, F10. F6, F7, F8 and F13 are data-quality repairs on report-only series. None of these changes sizing, caps, the kill switch or execution logic beyond making the existing refusals fire correctly; F1, F2, F5 and F11 touch the trader's code and need the owner's approval under CLAUDE.md before they are made.

---

## Status update, 21 Nov 2026 03:35 UTC (read-only recheck)

Deployed means the file inside the running container has the same SHA-256 prefix as the working-tree file and the container started after the edit (factor-model 03:11, filings 02:55, agents-heavy 02:51, enrichment 03:23 on 21 Nov; correlation, api_gateway, agents-fast, tradfi 20 Nov 14:58). Migration 0100 is applied and `agent_pred_unique_prediction` exists.

- **Fixed and deployed:** F1 (raise on non-200), F2, F3, F5, F6, F7 (single `INSERT ... SELECT unnest`, count check needs both variants, score dates must match, the trader now stores `scores_as_of`), F8, F9, F10, F11, F12, F13, F14, F15, L2, L4, L7, L8 (no `utcnow` or naive `datetime.now()` left in grader, db_writer or shared/utils). L1 is a test file.
- **F4: counted, still lossy.** `correlation/main.py` now calls `swallowed("correlation.event")`, but the batch is still committed afterwards (`:3577`), so a failed event is not redelivered. The original fix suggestion (re-raise on infrastructure errors or DLQ) is not done.
- **F1 extra guard** (refuse to trade when held is empty but fills are on record): not approved, not built.
- **L3, L6:** held for the owner's approval; L3 also needs a migration that re-keys existing rows.
- **L5, L9, L10:** held (guidance, a compose decision, counted only).
