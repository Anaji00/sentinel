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
