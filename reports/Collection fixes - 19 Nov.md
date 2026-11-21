# Collection fixes - 19 Nov (audit section 1)

Status: items 1-4 and 6 done with tests; item 5 diagnosed (not a bug). Parent session ran ruff and the full suite (green; one arity guard's pinned line moved 367 to 369, updated with a dated comment) and recreated the eight collectors at 09:55 UTC. Live check at 10:03 UTC below. Nothing committed.

## Changes
- `shared/utils/collector_metrics.py`: `ingested(count, source=)`, `expect_sources()`, `source_freshness()`; ingest clears the `collector_starved` flag at once; `watch_for_starvation(window_open=, clock=)`; `us_equity_window_open()` (pre-market to after-hours, weekends and holidays closed; idle counted from the reopen).
- `collector-social`: `upstream_error("reddit")` at the non-200 branch; heartbeat metadata carries `source_age_sec` per leg (telegram, reddit, twitter; `null` = never delivered since start); reject count for entries missing id or title.
- `collector-radar`: starvation watcher uses `us_equity_window_open`.
- `collector-ais`: **root cause of the stuck flag**: `collect()` and `main()` each built their own `CollectorMetrics`, so main's watcher saw zero arrivals forever. Both now use `for_service`.
- `.rejected(reason)` / `.upstream_error()` beside every `dropped()` in filings (beneficial, catalysts, ownership), macro (economic_calendar), news, social, crypto, tradfi. Parsed-record discards count as rejected; fetch failures count as upstream errors.
- `edgar_firehose.py` + new `shared/utils/filing_forms.py`: skip 424B2 (counted as `rejected("424b2_low_value")`, not marked seen). 424B3/4/5 kept (merger, IPO, shelf takedowns, as the per-CIK poller already decided).
- Tests: `tests/test_collection_fixes_19_nov.py` (11 tests: reddit 404 counter, freshness, guard that every collector module importing `dropped` references `.rejected(`, fake-clock watcher through closed/open/alarm/closed, AIS single instance, firehose skip). Related suites (observability, telemetry contract, firehose, prospectus noise): 67 passed, 13 skipped.

## 424B replay (stored 7 days, events table)
sec_edgar: 8-K 1,501 kept; 424B3/4/5 236 kept; other 34 kept; **424B2 1,310 skipped**. sec_13d 1,618, sec_form4 2,575, sec_edgar_13f 8 are other sources and untouched.
Consumers: no study, script or SQL reads 424B events; the frontend only hides them as routine. 55 correlations in 30 days were triggered by a 424B2 (Hawkes cross-domain 34, Corporate Filing & Options 10, "Geopolitical Cascade (Ms/Gs/Td/C)" 10, news 1), all driven by bank names and low confidence (0.10-0.43): noise removed, not signal.
Note: the per-CIK poller already skipped 424B2; the firehose was the leak.

## Reddit decision for the owner
Unauthenticated `.rss` returns 404 for all seven subreddits. No endpoint invented. Free alternative would be Reddit's OAuth API (free app credentials, 100 req/min) via a registered script app; needs the owner to create credentials. Until then the leg is visible (upstream error counter and `source_age_sec.reddit = null`).

## adsb / OpenSky (item 5): not a bug
Logs show sweeps of 3 regions every ~5 min, token refreshing, no errors. `poll_zone` deliberately publishes only emergency squawks (7500/7600/7700); everything else is filtered and the source marked "filtered" for the freshness monitor. 1 record since start and 12 a week are the expected rate. Report only.

## News tracebacks (item 6)
The 14 Nov branch (`ClientError`/`OSError` -> one warning plus `upstream_error`) is in place; live `sentinel-news` log has 0 "Traceback" lines in 29 h. The one remaining `exc_info=True` is the catch-all for unexpected exceptions, intentionally kept.

## Live evidence (19 Nov 10:03 UTC, ~8 min after recreate; Redis metrics hashes and heartbeat)
- Reddit: `collector_upstream_errors_total{collector-social}` = 6 (source `reddit` 6; 7 in the heartbeat a moment later), previously 0. Heartbeat metadata: `source_age_sec {reddit: null, telegram: 157.2, twitter: null}`, `upstream_errors: 7`.
- Rejected: `collector-macro` rejected 2 (`fred_insufficient_history`). Other reasons (form4_unlisted_issuer, beneficial_*, entry_no_title, markprice_*) are event-driven and had not fired yet. Upstream: crypto `divergence_venues` 1, news `al_monitor` 1.
- Starvation: `collector_starved` = 0 for both collector-ais and collector-radar; zero "produced nothing"/"NOTHING" lines since the restart in either log. Caveat: 10:03 UTC is 05:03 ET (pre-market, window open), so the closed-session silence is proven by the fake-clock unit test, not yet live; re-check the radar log after tonight's close.
- 424B2: container has `shared/utils/filing_forms.py` and the firehose is wired (`main.py:765`). No sec_edgar events of any form arrived since 08:30 UTC (overnight), so the `424b2_low_value` counter had not fired; the replay above is the proof of counts. Re-check the counter at the US open.

## Edge cases / unverified
- Live proof still pending for: 424B2 skip counter, closed-session radar silence, and reject reasons other than FRED (all need market-hours traffic).
- Radar alarm treats after-hours as expected data; a stall then is still caught, a fully closed session is not.
- 424B3/B5 not tagged `low_value` (would need an enrichment-side bypass outside my scope).


## 21 Nov 2026 03:05 UTC: trader order reconciliation (read-only, `scripts/reconcile_trader_orders.py`)
Run inside sentinel-agents-heavy against the Alpaca paper account (get_order_status and get_positions only; no order placed, nothing written). Findings:
- Of the 17 submitted-not-filled and 5 'unknown' orders named in the audit, **one** is unresolved: RGTI sell 2 (13 Nov), db `submitted/partial`, broker shows 1 filled at 9.94, status PENDING. The rest were resolved by the trader's fill recorder.
- Broker position equals the db's net filled quantity for every ticker except three: MSGK (-1 held, no trader order: a hand or foreign position), MSGS (db filled -1, broker 0) and SCCO (broker -4.0212 vs -4 filled, fractional residue).
- Net ordered differs from net filled on about 40 names (IOVA ordered -127, filled -88; IFF -14 vs -8): day orders that did not fill. The plan is recomputed from held positions each day, so the shortfall is re-planned rather than lost; not verified name by name.

```
== orders since 2026-11-01 not recorded filled/unfilled (broker's answer, read-only)
2026-11-13 RGTI   sell qty 2        db status submitted/partial    broker: PENDING filled 1.0 @ 9.94

== per-ticker drift: broker position vs net ordered and net filled over all recorded orders (a position held by hand or before the trader differs)
ticker book       broker  db_filled   ordered  broker-filled  broker-ordered
AEHR   short         -14        -14       -15              0               1
AIZ    -               0          0         1              0              -1
AMD    long            0          0         0              0               0
AVAV   short          -7         -7        -9              0               2
B      long            0          0         0              0               0
CELC   short         -11        -11       -10              0              -1
CPAY   short           0          0         0              0               0
ENTG   long        2.679      2.679     0.981              0           1.698
EVRG   short           0          0         0              0               0
EWZ    long            0          0         0              0               0
FANG   long       4.0299     4.0299     4.617              0         -0.5871
GH     long       5.0181     5.0181    4.9706              0          0.0475
GRAL   short          -6         -6        -9              0               3
IBIT   long            0          0         0              0               0
IFF    short          -8         -8       -14              0               6
IOVA   short         -88        -88      -127              0              39
IREN   long            0         -0        -0              0               0
KTOS   short         -22        -22       -25              0               3
LLY    short           0          0         0              0               0
LQDA   short          -6         -6        -2              0              -4
LYB    short         -11        -11       -16              0               5
MANH   -               0          0         1              0              -1
MARA   short         -53        -53        -4              0             -49
MCHP   long            0          0         0              0               0
MOD    long       2.7609     2.7609    2.0825              0          0.6784
MOH    short          -3         -3        -4              0               1
MP     short           0          0         0              0               0
MSGK   -              -1          0         0             -1              -1
MSGS   -               0         -1        -1              1               1
MTSI   long       3.3771     3.3771    2.4853              0          0.8918
NTAP   long       4.3052     4.3052    4.4015              0         -0.0963
NXPI   long       2.8382     2.8382     4.622              0         -1.7838
OKTA   long       3.5886     3.5886    3.9659              0         -0.3773
QXO    short           0          0         0              0               0
RGEN   long       4.2879     4.2879    2.8985              0          1.3894
RGTI   short         -40        -40       -41              0               1
SCCO   short     -4.0212         -4        -4        -0.0212         -0.0212
SNX    short          -3         -3        -4              0               1
SPDN   short           0          0         0              0               0
TEAM   long       5.1265     5.1265    5.3556              0         -0.2291
TKO    -               0          0         1              0              -1
TTAN   -               0          0         3              0              -3
USAR   short           0          0         0              0               0
USFR   short           0          0         0              0               0
VGK    long            0          0         0              0               0
W      short          -6         -6        -7              0               1
WOLF   short         -31        -31       -34              0               3

47 tickers differ of 483 seen
```
