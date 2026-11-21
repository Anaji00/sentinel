# Frontend overhaul: information architecture plan (12 Nov 2026)

Status: proposal for agreement. Read-only on code. Routes below are the ones the frontend session listed; "(coming)" means the backend does not serve it yet. I did not re-audit the code for this plan; page names come from `app/(dashboard)` (account, agents, audit, calls, charts, connections, crypto, filings, intel, markets, methodology, money, operations, options, performance, watch) and the sidebar.

**Evidence strength.** Little published research exists on how to lay out a retail stock app. The conventions below rest on practitioner sources, regulator pages and the earlier report (`reports/frontend-research.md`). Where a source is weak, the text says so. The strongest anchors are the SEC/FINRA material on extended hours, the Robinhood attention study and the forecast-communication research.

## 1. New information architecture: six screens

Principle: each screen has one job and answers questions in reading order. Anything deeper opens as a drawer or a sub-tab, not another top-level page (progressive disclosure, [NN/g](https://www.nngroup.com/alertbox/progressive-disclosure.html)). Cut the sidebar from 16 links to 6 plus a small "More" group.

### Screen 1: Today (home)
**Job:** "What should I know in two minutes?"
**Questions:** Is the market open? How is the market doing and how broad is the move? What changed since I last looked? How is my paper account? Which of my stocks need attention?
**Blocks, in reading order:**
1. Session strip: market open/closed/pre/after, time to next open or close (`/quotes` timestamps; clock is client side). See idea B.
2. Market weather sentence plus three numbers: index move, share of stocks rising, rates curve shape (`/quotes`, `/stocks/screener` (coming), `/radar/yield-curve`). See idea A.
3. Since you last looked: digest of changes on your stocks and your account (new calls, filings, big moves, orders). See idea D. Source: `/portfolio/summary`, `/filings/*`, `/signals/held-back/{t}`, `/predictions/track-record/slice`.
4. Paper account card: equity, day change, open positions count (`/portfolio/summary`).
5. Your watchlist, compact: price, day %, one flag each (earnings soon, new filing, model call) (`/quotes`, `/stocks/{t}/snapshot` (coming)).
6. Today's model calls, top 5, each with its track-record line (`/predictions/recommendations`, `/predictions/track-record/slice`).
7. System health: one quiet line, "All data feeds current" or the named problem (`/health/*`). Detail lives in More.

### Screen 2: Stock (one page per ticker, `/stock/{t}`)
**Job:** "Should I act on this stock, and what is the evidence?"
**Questions:** What is the price and how old is it? What has it done? What does the model say, and how often has it been right? What is the evidence versus the opinion? Who owns it, what was filed, what is it linked to?
**Blocks, in reading order:**
1. Header: name, price, day change, quote age and session, Follow, Order button (`/quotes`, `/stocks/{t}/snapshot`).
2. Candle chart with range buttons, volume, averages, crosshair, table toggle (`/radar/candles?range=` (coming)).
3. Key facts row: market cap, P/E, 52-week range, next earnings (`/stocks/{t}/snapshot`).
4. Evidence panel, left column: facts with dates and sources (filings, ownership, unusual trading, news) (`/filings/*`, `/ownership/*`, `/signals/held-back/{t}`). See idea E.
5. Opinion panel, right column: model view and bull/bear case, always with its track record and "what would make this wrong" (`/predictions/positioning/{t}`, `/predictions/track-record/slice`).
6. Related stocks and compare (`/stocks/{t}/related`).
7. Order ticket opens from the header, not inline.

### Screen 3: Scan (find something)
**Job:** "Which stocks should I look at?"
**Questions:** What is moving, and why? Which sectors lead? Which stocks match my filter?
**Blocks:**
1. Sector map with breadth (idea C) (`/stocks/screener` (coming)).
2. Sortable table: price, day %, volume vs usual, model view, next event; filters for sector, size, "has a call" (`/stocks/screener`).
3. Movers against their peers (today's Desk list, renamed "Unusual moves") (`/correlations`).
4. Click any row opens the Stock screen.

### Screen 4: My Book (paper account)
**Job:** "How am I doing, and is the model doing what it claims?"
**Questions:** What do I hold? What did I make or lose, after costs? How does the live record compare with the back-test? What orders are open?
**Blocks:**
1. Account summary with equity curve versus SPY as a benchmark line for comparison only; the account itself never trades SPY (`/portfolio/summary`).
2. Positions table: size, cost, day and total result, share of book.
3. Open and recent orders, with status.
4. Honest record: live calls vs back-test, right-rate, average result after costs, sample size (`/predictions/track-record/slice`). Same wording as the stock page.
5. Order ticket (existing three-step) with pre-trade facts.
**Sizing, caps and kill-switch behaviour are untouched by this plan.**

### Screen 5: Research (the world behind the price)
**Job:** "What is happening that might move a stock, and who is connected to it?"
**Questions:** What are the news and filings? Who owns the company? How are companies, ships and people linked? What are rates doing?
**Sub-tabs:** News and events (Intel), Filings and funds (`/filings/*`, `/ownership/*`), Connections (links graph), Rates and macro (`/radar/yield-curve`).
Each tab keeps its time-window selector. Follow the Money and Surveillance Watch stay as sub-tabs here, labelled in plain words (see section 2).

### Screen 6: Trust (how it works and how it is doing)
**Job:** "Can I rely on this, and what is it doing right now?"
**Questions:** What does each AI analyst do and how often has it been right? Which data feeds are fresh? What did the system do and why?
**Sub-tabs:** How it works (methodology), Analysts (agent scorecards), Data feeds (`/health/*`), Audit trail and Operations (admin only).
Aimed at honesty: every claim on other screens links here.

## 2. What happens to each current page

| Current page | Decision | New home | Reason |
|---|---|---|---|
| Home (Desk / Today overview tabs) | **Split** | Today gets the overview; the Desk list becomes "Unusual moves" inside Scan | The 3-pane desk is an analyst tool, not a first screen |
| Desk | Move | Scan, "Unusual moves" block; its middle pane becomes the Stock screen | One stock page, not two |
| Calls | Merge | Today (top 5) and Scan (full list, "has a call" filter) | Calls are a filter, not a place |
| Markets | Merge | Scan (sector map and table) | Same job |
| Performance | Move | My Book, "Honest record" block | One account story |
| Intel | Move | Research, News and events | |
| Connections | Move | Research, Connections | |
| Filings and Fund Holdings | Move | Research, Filings and funds; also Stock page evidence | |
| Follow the Money (`/money`) | Move | Research sub-tab "Who owns whom" | Plain name |
| Surveillance Watch (`/watch`) | Move | Research sub-tab "Public oversight" | Not a trading screen; keep but demote |
| AI Analysts | Move | Trust, Analysts | |
| How It Works | Move | Trust | |
| Operations | Move | Trust, admin only | Not for traders |
| Audit Trail | Move | Trust, admin only | |
| Options, Crypto, Charts | Decide | Options and Crypto: keep as a second row under Scan only if used; Charts: remove once Stock chart covers it | I did not check usage; ask the owner or read access logs before cutting |
| Account | Keep | Profile menu, not sidebar | |

Old addresses should redirect (the sidebar comments say `next.config.ts` already does this for earlier moves).

## 3. Five information-carrying ideas

None of these is decoration; each answers a question and can be removed if it does not.

### A. Market weather
- **Question:** "Is today a normal day, a broad rally, a narrow rally, or a sell-off?"
- **Data:** index change, share of stocks up (advance/decline), share above their 50-day average, volume vs usual, rates curve shape. From `/quotes`, `/stocks/screener` (coming), `/radar/yield-curve`.
- **How a layperson reads it:** one sentence ("Most stocks are up; the rise is broad") plus three small gauges with plain labels, each with a tap-to-explain. Never an icon of a sun or cloud as the only signal.
- **Convention and evidence:** breadth is a standard read of how many stocks take part in a move; the share above the 50-day average is common, but sources disagree on thresholds (70/30 versus 80/20), so show the number and direction and do not label it "overbought" ([StockCharts](https://articles.stockcharts.com/article/three-breadth-indicators-that-could-decide-the-next-market-move/), [Investinglive](https://investinglive.com/education/market-breadth-divergence-what-it-tells-traders-about-the-stock-market/); trading-education sources, weak). The yield curve: the NY Fed uses 10-year minus 3-month to estimate recession odds within 12 months, but inversions give long and variable warning (6 to 24 months), not every one is followed by a recession, and some argue the signal is weaker now ([Schroders](https://www.schroders.com/en/insights/economics/is-the-yield-curve-signalling-recession/), [NBC/CNBC](https://www.nbcphiladelphia.com/news/business/money-report/the-federal-reserves-favorite-recession-indicator-is-flashing-a-danger-sign-again/4119615/), secondary sources; I could not confirm today's reading). So say "rates curve: short rates above long rates (a past warning sign, not a forecast)".
- **Weakness:** the weather sentence is our own composite; pre-register its rules (which thresholds give which words) and show them in a tooltip so it is not a black box.

### B. Session clock
- **Question:** "Can I trade now, and how much should I trust this price?"
- **Data:** US session state (closed, pre-market, regular 9:30-16:00 ET, after-hours), time to next change, quote age (`/quotes`). Holiday calendar needed.
- **How a layperson reads it:** a thin bar at the top: "Market open, closes in 2h 14m" or "Closed. Prices are from Wednesday's close." In extended hours, add: "Fewer trades, wider price gaps; limit orders only on many brokers."
- **Evidence:** the SEC and FINRA warn about thin liquidity and bigger price swings outside regular hours ([SEC Investor.gov bulletin](https://www.investor.gov/additional-resources/news-alerts/alerts-bulletins/investor-bulletin-after-hours-trading), [FINRA](https://finra.org/investors/insights/extended-hours-trading)). Extended-hour windows differ by broker, so show ours as "per Alpaca" and do not hard-code claims.

### C. Sector map with breadth
- **Question:** "Where is the money going today, and is it a few big names or many?"
- **Data:** per stock: sector, market cap, day change (and 5-day, 1-month toggle); per sector: share of stocks up. From `/stocks/screener` (coming).
- **How a layperson reads it:** tiles sized by company size, coloured by change, grouped by sector, with a bar under each sector title "7 of 10 up". The breadth bar fixes the known flaw: a few giant tiles can dominate the picture.
- **Evidence:** Finviz-style maps size by market cap and colour by intraday move; guides note the top handful of boxes dominate and that auto-scaled colours cannot be compared across days ([ThinkInsights on Finviz](https://thinkinsights.net/insights/finviz-free-analysis-tool), [BIT](https://www.bit.com/knowledge-hub/stock-heat-map); blogs, weak). I found no research on treemap readability, so test with two or three real users before building the animated version. Use fixed colour steps (for example ±1%, ±2%, ±3%), with a legend and a sign or arrow, not colour alone.

### D. What changed since you last looked
- **Question:** "What do I need to act on since my last visit?"
- **Data:** per-user last-seen time (new), then events after it: new model calls on followed stocks, filings, large price moves, order fills, track-record changes, data-feed outages. From `/portfolio/summary`, `/filings/*`, `/signals/held-back/{t}`, `/quotes`.
- **How a layperson reads it:** a short list, newest first, each line "AAPL fell 4.1% after a new 10-Q (filed 08:12)", capped at about eight lines with "show all". A marker line separates new from old on other lists.
- **Evidence:** Discourse's digests are the best documented example; they dropped the words "your last visit" from copy because the timestamp was not always last visit, so name the exact cut-off ("since Wed 18:40") ([Discourse Meta](https://meta.discourse.org/t/what-are-discourse-digests/36627); forum evidence only, weak). Risk: digests can read as a full recap, so say "the 8 biggest changes", not "everything".
- **Caution:** this is the idea most likely to drive trading by attention (see the Robinhood study, [Barber et al. 2022](https://econpapers.repec.org/RePEc:bla:jfinan:v:77:y:2022:i:6:p:3141-3190)). Do not rank by "most exciting"; rank by relevance to what the user holds or follows, and show model changes with their record.

### E. Evidence versus opinion
- **Question:** "What is known, and what is the model's guess?"
- **Data:** facts with timestamp and source (filings, ownership, insider and unusual-trading data) versus model view, bull/bear text, and its track record. From `/filings/*`, `/ownership/*`, `/signals/held-back/{t}`, `/predictions/positioning/{t}`, `/predictions/track-record/slice`.
- **How a layperson reads it:** two columns with different looks. Left, "What the records show": plain statements, each with a date and a link. Right, "What the model thinks": a range, a hit-rate line, and "what would prove this wrong". Opinion never uses the green/red reserved for real price moves.
- **Evidence:** calibration and visible track record build trust, and adding uncertainty did not confuse people ([Raftery review](https://arxiv.org/pdf/1408.4812)); in election-forecast sites, text summaries and quantile dotplots earned the most trust, calibration graphics less ([Northwestern 2024](https://mucollective.northwestern.edu/files/2024-dice.pdf), simulated forecasts). The two-column layout itself is our design, with no published test; treat it as a hypothesis and check that people can say which side is fact.

## 4. First-run and empty-state plan

Rule: first-run states teach and offer one action; later empty states stay short and functional ([NN/g empty states](https://nngroup.com/articles/empty-state-interface-design), intro only seen; [Smashing](https://shop.smashingmagazine.com/2017/02/user-onboarding-empty-states-mobile-apps/), [Useberry](https://www.useberry.com/blog/what-makes-an-empty-state-actually-useful/); practitioner sources, weak but consistent).

| Moment | What the user sees | One action |
|---|---|---|
| Very first visit | Today shows market weather, session strip and a three-line welcome: "This is a paper account: practice money, no real orders. Pick a few stocks to follow." | "Pick stocks to follow" (search box plus 10 well-known tickers; never a ranked "top picks" list, to avoid attention bait) |
| Empty watchlist | Same prompt on Today and Scan | Add stocks |
| No orders yet (My Book) | Account card with starting cash; "No orders yet. Orders here use practice money." | "Place a practice order" (opens the ticket pre-filled with a followed stock) |
| No calls today | "No calls today. The model only calls stocks it has strong evidence on; most days that is a few." plus the last call date | See the track record |
| Track record too small | "7 calls so far. Too few to judge" instead of a percentage | Link to how it is measured |
| Data feed down or stale | Name the feed and its age: "Prices last updated 14 minutes ago" | Retry; link to Trust, Data feeds |
| Market closed | Show last close with date; chart ends at the last bar | None needed |
| Digest empty | "Nothing important since Wed 18:40." | None |
| Admin-only pages for non-admin | Not in the sidebar | None |

Also: a one-time, dismissible tour of three hover glossary words (the existing glossary), not a modal sequence. Store "seen" flags per user, not per browser, so the digest cut-off and onboarding state survive device changes (needs backend, step 1 below).

## 5. Build order: ten steps

Effort: S under a day, M one to three days, L a week or more. "Blocked on" names backend routes that must exist first.

| # | Step | Effort | Blocked on |
|---|---|---|---|
| 1 | **Per-user state:** last-seen time, followed stocks, seen flags (store and read) | M | A small user-state route (not on the list; confirm whether `usePreferences` already persists server side) |
| 2 | **Session strip** (idea B), mounted in the layout | S | `/quotes` timestamps (exists); holiday calendar can be a static file |
| 3 | **New navigation shell:** six entries, redirects from old routes, profile menu | M | Nothing |
| 4 | **Stock page as the single place for a ticker:** merge the Desk middle pane, chart, evidence/opinion columns (idea E), order button | L | `/stocks/{t}/snapshot` (coming) for the facts row; `/radar/candles?range=` (coming) for range buttons. Can ship without both using current data, with gaps marked |
| 5 | **My Book:** summary, positions, record block, ticket with pre-trade facts | M | `/portfolio/summary` (listed); pre-trade facts (buying power, holding) if not in summary |
| 6 | **Today screen:** weather (idea A), watchlist block, calls block, paper account card | M | `/stocks/screener` (coming) for breadth; `/radar/yield-curve` (listed) |
| 7 | **Scan screen:** sortable table with filters | M | `/stocks/screener` (coming) |
| 8 | **Sector map with breadth** (idea C) | M | `/stocks/screener` with sector, market cap and multi-period change |
| 9 | **Digest** (idea D) | L | Step 1 (last-seen) plus an events feed that merges filings, calls, large moves and orders by time; none exists as one route |
| 10 | **Research and Trust sub-tab shells:** move Intel, Filings, Connections, Money, Watch, Analysts, Methodology, Data feeds, Operations and Audit under them; first-run states from section 4 | M | `/health/*` summarised for the Today line; otherwise nothing |

Why this order: steps 2 and 3 need no new data and fix wayfinding at once; step 4 is the most valuable page and can ship in two passes; steps 6-8 wait on the screener; the digest comes last because it needs two backend pieces and carries the most risk of encouraging attention-driven trading.

## Open questions for the owner

1. Are Options, Crypto and Charts used? If not, drop them from the sidebar.
2. Keep "Follow the Money" and "Surveillance Watch" in Research, or move them to a separate "Civic" area outside the trading flow?
3. Is the weather sentence's wording acceptable, given it is our own composite with no external standard?
4. Should the sector map cover only the S&P 500 members the system already tracks, or a wider list? This decides the screener's data load (free data only).
