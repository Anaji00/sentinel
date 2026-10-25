# Sentinel's stock system: a plain-English guide

This guide explains how to run Sentinel's stock side and what it is doing, without
the jargon. The technical detail lives in `README.md`.

> **Not financial advice.** Everything Sentinel produces is a model's output. Trades are
> on paper by default: no real money moves unless you deliberately switch on a live
> broker (see [Safety](#safety)).

---

## What it is, in one paragraph

Sentinel watches the US stock market all day and night. It reads prices, company
filings, insider trades, options activity, short selling and company news. From that
it produces a short list of **stocks to buy or to sell short**. Each comes with how far
it expects the stock to move, how much to put in, and, when one is worth it, a specific
**options trade** (strike and expiry). It records every call it makes and checks each
one against what the market actually did. Ideas that keep being wrong lose their say,
and new ideas are only switched on after they prove themselves on years of past data.

The other half of Sentinel watches the watchers. It maps the plate readers, gunshot
microphones and face recognition police use, town by town, from public records; checks
whether each department follows its state's law; shows which agencies help immigration
enforcement; and follows the federal money to the companies that build the tools. It also
gives you a short, practical plan to protect your own privacy.

---

## Running it

### What you need

- **Docker Desktop**, with about **26 GB of memory** given to Docker. The full stock
  system (data collectors plus the models) is close to that limit, which is why some
  parts run in separate modes.
- A free **Alpaca** account for market data: prices, options and news.
  [alpaca.markets](https://alpaca.markets); the paper-trading keys are enough.
- Optional: a free **Finnhub** key (earnings calendar).
- An email address for the SEC's request header. SEC asks every program that reads its
  data to identify itself.

### First time

```bash
make setup
```

This creates your `.env` settings file and generates the secrets it needs. Open `.env`
and fill in:

| Setting | What it is |
|---|---|
| `ALPACA_API_KEY`, `ALPACA_SECRET_KEY` | Your Alpaca keys (market data) |
| `FINNHUB_API_KEY` | Optional: earnings dates |
| `SEC_USER_AGENT` | `"Your Name your@email"`, sent only to SEC servers |
| `BROKER_TYPE` | Leave as `paper` |

### Start everything the stock system uses

```bash
docker compose --profile collectors --profile agents up -d
```

That starts three groups:

- **The core**: databases, the message bus, the website and its API.
- **Collectors**: the services that fetch prices, filings, options and news.
- **Agents**: the stock models. The ranking model, the trade ideas, the portfolio and
  the signal engine live here.

`make up` starts only the core and the collectors (no models), and `make reasoning`
starts the AI agents without the collectors. For the stock system you want both
profiles, as above.

Then open **https://localhost** (or https://127.0.0.1). Your browser will warn about the
certificate on first visit. That's expected: it is a local self-signed one. Create an
account and sign in.

**The first day is quiet.** The ranking model runs each evening at 22:00 UTC, so
until its first run there are no stock ideas. The history loaders (five years of
prices, three years of news, option history since 2024) fill in over the first few
hours.

### Everyday commands

| You want to… | Run |
|---|---|
| See what's running and how much memory it uses | `make ps` |
| A one-line health check per service | `make health` |
| Follow one service's log | `make logs S=factor-model` |
| Stop everything (keeps all data) | `make stop` |
| Restart one service after changing its code | `docker compose restart factor-model` |
| Rebuild the website after changing it | `docker compose build frontend && docker compose up -d --no-build frontend` |
| Apply database changes | `make migrate` |
| Run the test suite | `python -m pytest tests/` |
| Back up all data | `make backup` |

---

## Where to look on the website

| Page (left menu) | What you'll find |
|---|---|
| **Home** (`/`) | On your first visit you choose where to start; home then leads with that. It shows today's top stock calls, **Your stocks** and **Your town** (follow a stock with its Follow button; choose your town with "Make this my town" on its record), then **Who's watching you**: how many cars plate readers logged this month, how many local agencies help immigration enforcement, how much federal money goes to surveillance companies, who profits, this week's news, and a box to look up your town. |
| **Who's Watching → Surveillance Map** (`/watch`) | Search your town or click the map: what watches that place, who runs it, whether it follows the rules, and what you can do. The tabs below cover what's changing, possible rule-breaking, follow the money (who gets paid, who pays) and a privacy plan. |
| **Stock Calls** (`/predictions`) | **The trade ideas**: today's buys and sells for the next day, week and month, each with a forecast, a size, its cost, and an options trade if one is worth it. Start here. |
| **Signal Board** (`/board`) | Every stock with unusual activity in the last three days. It opens on the strongest bullish and bearish leans as cards (green up, red down), then the full list you can filter to bullish or bearish. Click a row to read every signal in plain English. |
| **Trade Signals** (`/signals`) | Short-term signals from events (filings, insider sales, news) that have proven themselves in testing. Signals with the same reason are grouped into one card listing their stocks, with how likely each is to work and its expected move against its cost. |
| **Portfolio & Record** (`/book`) | The model portfolio's holdings, orders and performance against the S&P 500, the lab's comparisons, and how every source of calls has scored against real prices. |
| **Look Up a Stock** (`/ticker`) | One stock: price and chart, what the model thinks and why in plain words, then tabs for what's happening, signals, who owns and trades it, and options. For a company that sells surveillance to police (Palantir, Axon, Motorola Solutions, Cellebrite and others), a card shows what it sells, its federal contracts and its lobbying. |
| **Markets & Economy** (`/charts`) | Price charts for stocks, oil, gold, crypto and rates; the Economy tab adds interest rates, oil stockpiles and betting markets. |
| **Research** (`/strategies`) | Backtests, and which signals actually carry information. |
| **Investigations** (`/cases`) | Follow a story as a case, and write briefs. |
| **How It Works** (`/methodology`) | The method, on the site itself. |

---

## How it works, step by step

Think of it as an analyst who never sleeps, keeps perfect records, and is only allowed
to act on what has already worked.

### 1. Collect

It gathers market data automatically:

- prices for every US stock (five years back);
- SEC filings and insider trades as they are filed;
- options prices and trades;
- short-selling volume and borrow costs;
- earnings dates;
- around 900 company news articles a day.

### 2. Describe each stock

Every evening it measures about 35 things about each stock: recent momentum, how bumpy
the price has been, how heavily it is being shorted, quality and value, what its
options are pricing in, how it reacted to its last earnings. Everything is measured
only from what was known **on that day**, so its tests can never peek at the future.

### 3. Rank

Machine-learning models score roughly the 1,000 most liquid stocks on how likely each
is to beat or lag the market over the next **day**, **week** and **month**. A model is
used only if it beat the market, after trading costs, on years of data it was never
trained on.

### 4. Check the events

Filings, insider sales, earnings surprises and news are tested one type at a time
against what stocks did afterwards. Only the types that held up are allowed to
influence a call. For example, when a company sells new shares the stock tends to fall
about 10% over the next month. Most news headlines, social chatter and AI-written
scenarios did **not** hold up, so they don't move the ideas.

### 5. Forecast

For each idea it estimates:

- how far the stock should move **against the S&P 500**;
- the chance it goes the right way;
- a target price.

The numbers are scaled by how accurate the model has actually been, not by how
confident it feels.

### 6. Size and cost

It works out how much to put in. The method is half of what the Kelly formula
suggests, never more than 5% of the account. That size is net of the real cost of
trading that particular stock at that size, plus borrow fees for a short. Thin,
jumpy stocks get smaller positions, because buying a lot of them moves their price.

When momentum stocks are going through a rough patch (their swings are bigger than
usual), every size is scaled down automatically.

### 7. Pick the options trade

Every call, put and spread available is priced against the forecast at the prices you
would actually pay. The best one is shown with its strikes and expiry, or "none" if
nothing is worth it. Cheap options are bought; expensive ones are sold as
limited-risk spreads.

### 8. Grade everything

Every idea, options trade and signal is logged and scored when its time is up. A paper
ledger trades every idea the way a trading desk would. A model portfolio (the S&P 500
plus the model's long and short picks) is marked against the S&P 500 every day. The
grades feed back into the forecasts.

---

## How to read a trade idea

| On the card | Meaning |
|---|---|
| **BUY / SHORT** | Buy expects it to beat the S&P 500; short (a bet against) expects it to trail it. |
| **Top 1% of its industry** | Where the model ranks it among similar companies. |
| **+2.9% better than the S&P 500 over the next month** | The forecast, against simply owning an S&P 500 fund. |
| **Chance it goes our way: 57%** | The model's probability, from its real track record. |
| **Most to put in: 2.6%** | The largest share of your account it suggests; "skip" means trading costs would eat the gain. |
| **Trading cost: 0.14%** | What buying and later selling costs at that size. |
| **Good / Bad news today** | Today's headlines about it nudge the forecast up or down (already included). |
| **Buy a call / Call spread: bet on a rise** | The best options trade for this forecast, with its chance of profit, what it costs, the most you can lose and the expected profit. |
| **Fully tested / Still being tested** | Whether the model has passed every test we set, or only most of them so far. |
| **really 3.7 separate bets of 6** | Stocks that move together count once, so six buys may really be fewer bets. |

---

## How it keeps itself honest

- **Nothing is switched on because it sounds good.** Every new idea is a written-down
  test with a pass mark decided *before* looking at the result. Most fail. For example,
  "news tone" and "rank within themes" were both tested and turned down.
- **A weekly lab** re-tests how the portfolio is built. The live system applies only what
  the lab's latest run says beats what it replaces. Some changes must win two weeks in a
  row before they go live.
- **Everything is graded on real prices**, net of the S&P 500, after costs.
- **It audits itself.** Live checks have caught real faults, for example:
  - a price snapshot taken 9 minutes into the trading day being treated as the day's close;
  - six "different" stocks that were really one bet on chips.

---

## The daily rhythm (New York time)

| When | What happens |
|---|---|
| All day | Prices, filings, insider trades, options and news arrive; news is scored and read within minutes |
| 9:30 am – 4:00 pm | Short-term signals can fire (one-hour signals only while the market is open); trade ideas refresh |
| 4:35 pm | The day's final prices are saved |
| 6:00 pm (22:00 UTC) | The ranking models re-run; tomorrow's ideas and the portfolio are decided |
| Weekly | The lab re-tests the portfolio's construction and recalibrates the forecasts |
| Monthly | The news-reading model refits itself, keeping the new version only if it still passes |

---

## Troubleshooting

| Symptom | Likely cause and fix |
|---|---|
| Sent back to the sign-in page | Your session expired; sign in again. |
| No trade ideas | The evening model run hasn't happened yet (first day), or a model is withheld because it isn't beating costs right now. The page says which. |
| A service keeps restarting | Usually memory. Check `make ps`; close other heavy apps or give Docker more memory. |
| Empty news or options panels | Check the Alpaca keys in `.env`, then `make logs S=collector-tradfi`. |
| The browser warns about the certificate | Expected for the local self-signed one; proceed to the site. |

---

## Safety

- `BROKER_TYPE=paper` (the default) means every order goes to an internal simulator. No
  real money moves.
- Real orders need *both* an Alpaca live setting and `ENABLE_LIVE_BROKER=true`. Having
  Alpaca keys for market data does **not** switch on live trading.
- Your keys stay in `.env` on your machine. The SEC contact email is only ever sent to
  SEC servers.

---

## Glossary

| Term | Plain meaning |
|---|---|
| **Excess return / against SPY** | How much better or worse than the S&P 500 index fund |
| **Short / sell short** | Borrow shares and sell them, profiting if the price falls |
| **Horizon** | How long the idea is meant to be held: a day, a week, a month |
| **Percentile** | Rank out of 100 (p99 = near the top) |
| **Out-of-sample** | Tested on data the model never saw while learning |
| **Sharpe ratio** | Return per unit of risk; higher is better, above 1 is good |
| **Kelly sizing** | A formula for bet size from edge and risk; we use half of it to be safe |
| **Spread** | The gap between the buying and selling price, a cost of trading |
| **Option, call, put** | Contracts that pay if a stock ends above (call) or below (put) a price (the strike) by a date (the expiry) |
| **Momentum** | The tendency of recent winners to keep winning, until they suddenly don't |
| **Independent bets** | How many genuinely different positions you hold once similar ones are counted together |
