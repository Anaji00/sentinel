# Stock and options data: how it is collected and how the model reads it

Audited 22 October 2026. Every source below is free; nothing assumes a paid feed.
Alpaca's free plan is the binding constraint: 200 data requests a minute shared by
every collector, consolidated (SIP) history only once it is 15 minutes old, live
trades and quotes for 30 symbols from one exchange (IEX), unlimited live minute bars
from IEX, and 200 live option quotes on an indicative (not OPRA) feed.

## 1. Sources

| Data | Source | How | Cadence | Table | Since |
|---|---|---|---|---|---|
| Daily bars (split and dividend adjusted, every exchange) | Alpaca SIP | REST, `adjustment=all` (`collector-tradfi/historical_backfill.py`) | Deep load once per name, 10-day top-up each pass, final only after 16:30 New York | `daily_bars` | Jan 2015 (6,737 names); 1,078 names that no longer trade, where Alpaca still serves them |
| Minute bars | Alpaca IEX stream, replaced by SIP once 15 min old | websocket `bars` channel (`alpaca_stream.py`), settle pass every 30 min (`_settle_minute_bars_loop`) | Live | `tradfi_bars` (1-minute base, rollups 5m–1mth), kept 400 days | 60 days deep |
| Trade ticks | Finnhub free websocket | `stream_equities` in `main.py` | Live, **50 symbols**: core macro and equity anchors, then the radar's newest discoveries | Kafka → enrichment | — |
| Snapshots for everything not streamed | Alpaca `/v2/stocks/snapshots` | `poll_extended_hours_equities` | Polled | quote cache, events | — |
| Option chains (live) | Alpaca `/v1beta1/options/snapshots/{ticker}` (indicative) | `poll_options` | Polled | options-flow events, positioning in Redis | — |
| Option implied volatility history | Alpaca option daily bars + raw stock closes | `option_history.py`: monthly expiries, contracts within 25% of spot, 7–60 days out | Once from Feb 2024, then each weekday evening (22:00 UTC, last 7 sessions) | `option_iv_history` (ATM IV, skew, call-put spread, put/call volume) | Feb 2024, 600 most traded names |
| Short sale volume | FINRA daily files | `short_data.py` | Hourly until the day's file posts | `finra_short_volume` | Jan 2021 in the table (FINRA publishes from Aug 2018) |
| Short interest | FINRA, twice a month | `collector-filings/short_interest.py` | Per settlement date | `short_interest` | Dec 2017 |
| Borrow fees and availability | Interactive Brokers' public borrow file | `short_data.py` | Every 2 hours, the day's last kept | `borrow_rates` | Oct 2026 (IB publishes no history) |
| Fundamentals | SEC XBRL company facts | `collector-filings/company_facts.py`, `history.py` | Per filing | `fundamental_facts` (+ `issuer_ticker`) | Filings from Jan 2015 |
| Shares outstanding (vendor) | — | `collector-filings/vendor_shares.py` | Daily | `vendor_shares` | Oct 2026 |
| Industry (SIC) | SEC / Finnhub profile | — | Periodic | `company_profile` | 6,297 names |
| Insider trades (Form 4) | SEC EDGAR atom feed | `poll_form4` in `main.py` | Polled | events | — |
| Earnings calendar, estimates, surprises | Finnhub free | `earnings_estimates.py`, `poll_finnhub_earnings` | Calendar daily, surprises weekly, 20 calls/min | estimate snapshots, events | Our own snapshots (no free estimate history) |
| Company news | Alpaca news | `stock_news.py` | Every 2 minutes, history walked back | `stock_news` (scored by FinBERT in correlation) | Nov 2011 |
| Corporate actions | Alpaca `/v1/corporate-actions` | `_corporate_actions_loop` | Periodic | re-adjusts affected history | — |

## 2. What the model reads (`services/agents/factor_engine.py` `_panel`)

| Model input | Table | Point in time |
|---|---|---|
| Prices, volume, opens (momentum, reversal, volatility, beta, liquidity, overnight/intraday) | `daily_bars` | A bar is stored only once its session is final |
| Short volume share (`svr_20`, `svr_chg`) | `finra_short_volume` | Shifted one row: the file posts after the close |
| Days to cover (`dtc`) | `short_interest` | By settlement date |
| Option measures (`opt_*`) | `option_iv_history` | Evening of the session |
| Fundamentals (`ey`, `quality_ind`, board context) | `fundamental_facts` | Visible 1 day after the filing date |
| Industry (two-digit SIC) | `company_profile` | Static |
| Earnings reaction (`ear`) | `signal_event_archive` earnings events | Event date |
| Market news tone (context only) | `stock_news` | Session |
| Rates and credit context | `daily_bars` TLT, HYG | Same session |
| Shortable today | Alpaca asset list (Redis) | Today only (applied to history: a known limitation) |

The universe each day is the 1,000 most liquid names above $3 and a $3M consolidated
dollar-volume floor, from data up to that day.

## 3. Accuracy checks (22 Oct 2026)

| Check | Result |
|---|---|
| High below low, close or open outside the day's range, nulls, weekend rows | None in 17.6M rows |
| Sessions against SPY's calendar | Every stored day is a SPY session |
| Daily close vs Yahoo, 100-stock daily sample | 0 closes off (2026-10-20, 10-21) |
| Seam between the 2015–21 load and existing history | Same rate of large moves at the seam as elsewhere; 5 names (AHG, DFLI, DXJ, KNDI, SKIN) carried an older split adjustment on one row and were reloaded in full |
| Moves over 60% that reverse the next day, liquid names only | 51 in 11 years, mostly real (GME/AMC Jan 2021, BBBY); a handful of single bad prints remain |
| Option IV history | Was stale from 16 Oct: the evening update asked for today's SIP and option bars before the 15-minute delay and every request was refused (403); a date-only end also means the end of that day in New York, so the same requests failed from midnight to 04:00 UTC. Fixed (`sip_end`); 19–22 Oct refilling |
| Fundamentals | Filing-dated, so no look-ahead |

Known gaps: short volume before 2021 and short interest before 2018 are absent, option
measures before Feb 2024 are absent (the model treats these as missing, not zero);
borrowability is today's list applied to history; adjusted prices know about later
splits; only names Alpaca still serves are recoverable (1,078 no longer trading), so survivorship
bias is reduced, not removed.

## 4. Live focus: where the free live slots go (since 23 Oct 2026)

`shared/utils/live_focus.py` ranks every candidate each 5 minutes and publishes
`sentinel:live_focus`: names the auto-trader trades today, then names entering or
leaving the book, held names reporting earnings today, held names by how extreme
their rank is, then the radar's watched names.

| Slots | Feed | Stored |
|---|---|---|
| 15 symbols, trades and quotes (the plan counts both channels against 30) | Alpaca IEX stream, `alpaca_stream.py`; halves itself if the vendor reports the limit | `tick_trades`, `tick_quotes` (quotes at most every 0.25 s per name, only on change); compressed after a day, kept 90 days |
| 200 option contracts, quotes and trades | Alpaca indicative option stream (MessagePack), `option_stream.py`: the 2 calls and 2 puts nearest the money on the next monthly expiry for the first 50 focus names, re-chosen every 30 minutes | `option_ticks` (indicative, not OPRA), 90 days |
| 25 of Finnhub's 50 trade-tick slots | Finnhub websocket; the other 25 stay with the core macro and market anchors | Kafka → enrichment |

Every evening (after 17:00 New York) `cboe_chains.py` pulls the full delayed chain
from Cboe for the 600-name option universe plus the focus, about one request a
second, into `option_chain_daily`: call and put volume and open interest, put/call
ratios, Cboe's 30-day IV, ATM IV and 25-delta skew on the expiry nearest 30 days.

None of these feed the model yet: each is a candidate for a pre-registered trial.
