# A stock moving against its peers on its own news: what happened on 9 Nov, and the test for a detector (10 November 2026)

## The case

Sunday 8 Nov, 21:20 UTC: Nvidia's CEO admitted the affair; the board opened an investigation. Monday 9 Nov: NVDA closed -4.3% while AMD rose 13.3%, AVGO 8.8%, MRVL 9.5%, TSM 3.5%, SMH 5.1% and SPY 2.4%. That is a 9-point
gap to its sector on a company-specific story with a headline stream of 20+ articles. A person would call it at once.

What the system produced: about 20 identical tier-2 alerts titled "Peer Decoupling Detected: NVDA vs SPY, AMD, TLT" from Sunday 21:07 UTC, text "discovered cointegrated peer correlation", no returns, no gap, no direction, and metrics that
are NVDA's own hourly Sharpe (-4.1) and drawdown. Beside them, tier-1 alerts such as "Oil Price Spike Leads to Energy Stock Selloff: NVDA" and "US2Y Yield Drop and D Equity Upswing: NVDA", matched on 'news, tradfi'.
No alert said what happened.

## Registered before testing (10 Nov 2026)

**Detector D1 (daily form; the live form would run intraday on the same logic).** For stock i on session d:
- peers: its stored sympathy peers (correlation of 0.35 or more over 120 sessions), stocks only, at least 3 with a return that session;
- `rel` = the stock's simple return minus the mean of its peers' returns that session; sigma = standard deviation of `rel` over the 60 sessions before d; z = rel / sigma;
- **fire** if |z| is at least 3, the stock and the peer mean move in opposite directions, the stock's move is at least 2% and the peers' mean at least 1%;
- **news gate:** 'news divergence' if at least one article naming only that stock fell in the session (after 16:00 New York counts for the next session), else 'unexplained divergence'.

**Tests.**
- T1: D1 fires on NVDA for 9 Nov 2026 as 'news divergence'.
- T2: usable volume. Over the last 120 sessions across the 1,000 most liquid names, the median number of fires per session is at most 10 and of 'news divergence' at most 5. A detector that fires on 50 names a day is not an alert.
- T3 (descriptive, no pass or fail): the stock's return relative to its peers over the next 1 and 5 sessions, signed so that continuation of the move is positive, for 'news divergence' and for 'unexplained divergence', with day-clustered intervals.
  It says whether to word the alert as continuation or as a possible reversal. It adds one family, hence four cells, to the reaction study's correction when that is run.

**Reading.** T1 and T2 pass: build it as a pure function plus an intraday job, with a cooldown per stock-day, text stating the returns and gap, and a tier above the generic rules. T2 fails: tighten the thresholds, re-register, re-run.

## First run (10 Nov 2026): T1 failed as registered

D1 over the last 120 sessions fired a median of 1 time a session (mean 1.2, 90th percentile 3, maximum 11) and a median of 0 'news divergence' (mean 0.8): T2 passes, with room. **T1 failed: it did not fire on NVDA on 9 Nov (z -2.8, bar 3).**
Cause: NVDA's stored sympathy peers are TSM, AAOI, NVT, IREN, ENTG, AXTI, FORM and SMCI. They correlate with it statistically but are not its economic peers, and they are volatile, so the 60-session sigma of the gap is 3.2%.
AMD, AVGO and MRVL, the stocks a person would compare it with, are not in the set. Against a plain semiconductor basket (AMD, AVGO, MRVL, TSM, MU, QCOM, INTC, ARM, SMH) the same day reads z -4.7.

## Amendment D2, registered before the re-run (11 Nov 2026)

A change made after seeing the NVDA case, so that case no longer counts as an independent test; T2 and T3 on all other events are the evidence for the amended rule.
- Two references for each stock: (a) its stored sympathy peers as before; (b) its **industry group**, the mean return of the other liquid stocks with the same four-digit SIC code (at least 5 of them; `company_profile.sic`).
- **Fire** if either reference gives |z| of at least 3 with the stock and that reference moving in opposite directions, the stock's move at least 2% and the reference's at least 1%. The alert records which reference fired.
- Everything else (the 60-session sigma, news gate, T2 and T3) is unchanged. T1 is re-run on NVDA and reported as a sanity check, not as evidence.
- T2 bar unchanged: median at most 10 fires a session, at most 5 'news divergence'. One further reading: how many of the 'industry' fires are on stocks with no peers fire (the references agree) or disagree.

## D2 result (11 Nov 2026; test script `/tmp/d2.py` logic, same data: 1,000 liquid names, closes from 1 Oct 2025 to 9 Nov 2026)

- **T1 (sanity, no longer independent):** NVDA fires on 9 Nov as a news divergence against its industry group: -4.3% against +7.7%, z -5.0.
- **T2: passes.** Over the last 120 sessions, 2 fires a session at the median (mean 2.5, 90th percentile 6, maximum 24), and 1 'news divergence' at the median (mean 1.7, 90th percentile 5). 9 Nov flagged six stocks, all with news: AKAM, DBX, FSLR, HIMS, IONS (-21.4%, z -10) and NVDA.
  Of 695 fires, 332 came from sympathy peers alone, 279 from the industry group alone and 84 from both, so neither reference is redundant.
- **T3 (descriptive).** Relative return in the next sessions, signed so continuation is positive, day-clustered t:

| Group | Next day | Next 5 sessions |
|---|---|---|
| Divergence with news (444) | +42 bps (t 1.6) | +18 bps (t 0.2) |
| Divergence with no news (245) | +8 bps (t 0.3) | -53 bps (t -2.7) |

  With news, the move has not been shown to continue or to fade: do not word it as either. With no news naming the stock, the gap closes within a week (peers catch down or the stock catches up), which supports calling it 'unexplained' rather than a signal.

## Code review of the detection path (the NVDA case as the driver)

| # | Finding | Evidence | Status |
|---|---|---|---|
| 1 | **No detector measured a divergence.** "Peer Decoupling Detected" is the quant engine's peer *discovery* (graph candidates plus a Granger lag test) with the wrong headline; its metrics are the stock's own hourly Sharpe and drawdown | `quant_trading_engine.py` around line 607; text "discovered cointegrated peer correlation" | Replaced by `divergence_watch.py`; headline and description corrected |
| 2 | **It repeated without limit.** 56 identical alerts for NVDA in 48 hours; 1,105 for 675 tickers, including ETHUSDT, BTCUSDT and AI | `correlations`, 9 to 11 Nov | Fixed: one per ticker per 6 hours |
| 3 | **Its peers were not peers.** The list read "NVDA vs SPY, AMD, TLT": an index fund and a bond fund | alert headline | Fixed: stocks only (`is_valid_primary_equity_async`) |
| 4 | **Synthesized rules name things their conditions never test.** "US2Y Yield Drop and D Equity Upswing" fired 1,995 times on 981 stocks in 48 hours: trigger is any headline with anomaly of 0.4 or more; the clause is any market anomaly in the 4 hours before, joined only by time. Nothing mentions yields. Together with the similar rules it is 5,047 tier-1 alerts against 1,289 tier-2 | `sentinel:correlation:dynamic_rules` | **Open.** 41 of 56 stored rules pass today's guard, because a temporal join (`precedes_trigger` plus `within_minutes`) counts as a join. Needs a rule that the name's instrument or theme appear in a clause, or rules named from their conditions |
| 5 | **The earlier fix did not reach old rules.** The guard `_is_reusable_rule` exists but rules stored before it persist | rule store | Open: retire rules that fail a strengthened guard |
| 6 | **The cluster tier hid the answer.** The right alert would have been one line among 6,336 in 48 hours | tier counts | Divergence alerts publish at ALERT, or ELEVATED with news and 4 sigma or more |

Built: `shared/utils/divergence.py` (pure), `services/agents/divergence_watch.py` (scheduled job in the heavy tier, no model call, numpy offloaded, stale-bar guard, persisted before published, at most 3 alerts a stock a day),
`tests/test_divergence.py` (11 tests), and the three quant-engine fixes. 195 adjacent tests pass; lint is clean on the new files (two older warnings in `quant_trading_engine.py` are unchanged).
Not deployed: it needs `agents-heavy` to be recreated. Not yet graded: the new families are not in the reaction study (four more cells when registered).
