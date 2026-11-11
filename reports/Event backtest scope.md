# Backtesting rules and signals on history: scope (7 November 2026)

## The finding that changes the plan: most of this harness already exists

`shared/utils/reaction_study.py` (run every 6 hours by `services/agents/signal_engine.py`) is an event-study harness. For each signal family and horizon (1 hour, 1 day, 5 days, 30 days) it measures forward moves net of
a size-matched benchmark, collapses overlapping episodes into independent units, and calls a family **proven** only if it has at least 20 units, passes Benjamini-Hochberg false-discovery control (q 0.10), is confirmed on the
latest 40% held out, beats a 10 bp round trip, has a hit rate over 53% and moves the way its reason predicts. Five years of EDGAR history (8-Ks, Form 4, 13F) and earnings reactions were already backfilled into
`signal_event_archive` (800,000 rows).

State of the last study (7 Nov 2026, 282,171 episodes, 53 families, 212 family-and-horizon cells):

| Verdict | Cells | Meaning |
|---|---|---|
| Proven | 16 | Passed every bar |
| No edge | 95 | Measured and empty |
| Insufficient data | 86 (41 families) | Fewer than 20 independent units |
| Unmeasurable | 9 | No price history around the episodes |
| Contradicts prior | 5 | Significant, in the opposite direction to the reason |
| Unconfirmed | 1 | Failed the held-out part |

Proven: 8-K dilution (1d, 5d, 30d), delisting notices (1d, 5d, 30d), auditor changes (30d), impairments (5d, 30d), opportunistic insider selling (30d), earnings reaction down (30d, plus a small version),
earnings fade after large moves (5d), negative stock news (5d), put sweeps (1h, 1d). Contradicting their prior: positive stock news (the stock then drifts down: 1d t -3.3, 5d t -4.4), positive earnings reactions at 5d and 30d.

So the gap is not a missing harness. It is **history for the families that lack it, tradability of the ones that passed, stability over time, and one ledger that also holds models and agent calls.**

## Gaps, in the order I would close them

| # | Gap | What to do | Why it is worth it | Cost and risk |
|---|---|---|---|---|
| G1 | 12 or more headline families sit at 13 units of 20 (country and sector news, zone escalation, de-escalation) because the study reads only 365 days of headlines, though the archive holds headlines from September 2019 | Add `headline` to `DEEP_HISTORY_TYPES` and widen its window to 2019. Headline mapping is keyword rules, not an LLM, so history is not contaminated by model memory | About 6x the episodes at no new data cost | A setting and a restart of the signal engine. Verdicts will change, and with them what the idea layer treats as proven |
| G2 | EDGAR families stop at 5 years, but prices go back to January 2016. Officer changes, material agreements, completed acquisitions, 13D activist filings and bankruptcies sit at 3 to 16 units | Extend `HISTORY_YEARS`, `ARCHIVE_YEARS` and the study window from 5 to 10. Form 4 is in the database since 2016 (no network); 8-K and 13D need EDGAR submissions files, paced through `sec_slot()` | Roughly doubles the units for 8-K, 13D and 13F families | Hours of paced SEC requests in the background, run outside market hours. Older filings use older text formats, so a sample must be checked |
| G3 | 9 families are unmeasurable: 1,937 instruments they point at have no price bars | List why (OTC lines, options without underlying, recent listings), backfill what the free feed has, mark the rest as untradeable | Turns "unmeasurable" into a verdict or an honest "cannot be priced" | Small |
| G4 | The strongest proven cells may not be tradeable. 8-K dilution reads +1,062 bps over 30 days at a 93% hit rate and delisting notices +812 bps at 83%: the profile of micro-caps that cannot be shorted | Re-score every proven cell on the liquid, borrowable top-1,000 universe the book trades, and on the full universe separately | Decides which proven signals can touch the book or the idea layer at all | Pure re-run of existing code on a filtered set |
| G5 | Nothing says whether a proven cell is stable | Rolling half-year windows and market-regime splits (2018 drawdown, 2020, 2022) for each proven cell, plus a monthly re-test that flags decay | This is the "periods and intervals" question: a signal that works in two of ten half-years is not proven | Pure; uses the archive and bars already held |
| G6 | Rules, models and agent calls are judged in three places | One ledger (extend `MODEL_TRIALS.md`): each candidate, its test, its periods, and a running count of trials. Add RB1 (plain rule against B1) and the registered ablations | Keeps the multiple-testing count honest across everything | Documentation and a small table |

## LLM inference: the question to answer, and when

The harness can test LLM-derived signals only **forward**, because a model may remember prices before its training cutoff. Headline scoring (`llm_news_30`, `cust_news_30`) began on 29 Oct 2026. The registered test:
the same pre-registered ranking with and without the LLM features, scored on dates after the model's cutoff, with the model recorded on every score. A first read needs about 6 months (roughly 20 independent monthly periods
is out of reach, so use weekly periods with day-clustered statistics and say how few there are). Until then, inference is justified where it does extraction and explanation, and not yet as a source of calls.
The hourly rule-writing agent proposed earlier is replaced by this: an LLM may *propose* a hypothesis, but it enters only as a pre-registered cell in this ledger.

## Trial accounting

The study already corrects across all 212 cells in a run. Each extension below adds cells and changes the look at the same data, so each is recorded as one registered extension with its cell count, and the BH correction is
always applied across the full set of cells in that run, never to the new ones alone. Expect about 10% of any "proven" list to be false at q 0.10: of today's 16, one or two.

## What this plan leaves out on purpose

Kaggle price or news sets (survivorship and contamination, and no gain over 11 years of our own); training new ML models on more history (the model comparison already runs on 11 years, and a 10-year extension of
the price feed is not available free); an hourly rule or prediction generator (more rows, not more independent periods).

## Decisions I need

1. **G1 first?** It is a setting and changes which headline families count as proven. Recommended.
2. **G2 background run?** Hours of paced SEC requests; I would start it after the model run finishes and outside market hours.
3. **G4 before G2?** G4 is a pure re-score and may show that the strongest proven cells are untradeable; it could be run first and costs nothing. Recommended as the very first step.

## G4 registered before running (7 Nov 2026)

**What is re-scored:** the 16 cells proven in the study stored at 01:08 UTC on 7 Nov, on their own episodes, measured exactly as the study measures them (same benchmarks, horizons, winsorising and units).
**Three sets of episodes:** V0, all episodes (reproduces the published cell, a check on the re-run); V1, episodes where the stock's trailing dollar volume before the event was at least the 1,000th-largest average dollar
volume of the last 60 sessions today (a point-in-time size gate, so a name that grew into the top 1,000 later is excluded at the time it was small); V2, V1 and shortable and easy to borrow on Alpaca today (names
since delisted are not assumed lendable, which is the conservative reading and the one the book uses).
**A cell survives a set** if it has at least 20 independent units, an aligned mean above its cost, |t| of at least 2 on the winsorised units, a hit rate of at least 53% and the same sign as V0.
No false-discovery step is applied: these are restrictions of cells already tested, so no new hypothesis is added; each cell's original q is shown beside.
**Reading:** survives V2, usable by the idea layer as a short signal. Survives V1 only, real but not shortable by the book. Fails V1, the published result came from names too small to trade.
Cells with fewer than 20 units in V1 or V2 are reported as "cannot be tested on tradeable names", not as failed.

## G5 registered before running (7 Nov 2026)

**What is measured:** the 16 cells proven in the stored study, on episodes rebuilt and measured exactly as in G4 (`scripts/audit_signal_stability.py` reuses `SignalEngine._load_observations`, `scaled_horizon_stats`, `measure`, `units` and `evaluate_cell`). The cell's independent units are built once over the whole sample, as the study builds them, then assigned to periods by the date of their entry. The whole-cell sign is the sign of the re-run mean. A re-run that does not reproduce the stored cell's units and mean is reported beside it.
**Periods:** calendar half-years (H1 = Jan-Jun, H2 = Jul-Dec, UTC). Per period: units, aligned mean in bps (raw, as the study reports) and t (on units winsorised at +-5 sd, as the study's t is). A half-year with fewer than 8 units is shown but not counted.
**Regimes (SPY, from daily closes strictly before the unit's entry):** trend = up when the last close is above its 200-day mean, else down; volatility = high when the 20-day realised volatility of daily log returns is above its median over the prior 250 sessions, else low. Chosen over `equity_regime.py` because that gauge needs the factor panel and momentum-factor drawdowns; SPY alone is reproducible from bars held. Reported as units, bps and t per regime; no verdict is drawn from regimes, they are context.
**Verdict, applied in this order:**
1. **Cannot be rated** if fewer than 4 half-years have 8 or more units.
2. **Decaying** if the last two calendar half-years that hold data (pooled, at least 8 units) are weaker in the cell's own direction than all earlier units (at least 16), by a one-sided Welch test on the winsorised aligned moves at p < 0.05.
3. **Stable** if at least 70% of the half-years with 8 or more units have the same sign as the whole cell.
4. Otherwise **unstable**.
Decay is checked before stability because a cell can keep its sign in most half-years and still have lost its edge lately, and that is the case worth acting on.
**Rolling 12-month mean:** at each month end from the first with a full year of history, the units entered in the trailing 365 days: count, mean bps and t. Written to a CSV for a later decay monitor.
No false-discovery step: these are descriptions of cells already tested. 16 cells, two regime splits and a handful of half-years each means some cells will look unstable by chance; a cell rated unstable is a flag for a closer look, not proof the signal is gone.

## G1 result

**What changed** (`services/agents/signal_engine.py`, nothing else): `headline` added to `DEEP_HISTORY_TYPES`, and the default `HISTORY_LOOKBACK_DAYS` (env `SIGNAL_STUDY_HISTORY_DAYS`) widened from 5 years + 30 days to 10 years + 30 days, so the deep pass (30-day steps, daily horizons only) covers the archive's whole span. The archive holds 161,265 headlines from 17 Sept 2019, of which 86,989 are Alpaca `stock_news` (same `headline` type, same start date), so one type covers country, sector, zone and stock news. No `.env` change: no `SIGNAL_STUDY_*` key was set, so the new default applies. The 1h horizon is untouched: it still reads the intraday series only (guard test added in `tests/test_permanent_history.py`).

**Contamination:** `shared/utils/headline_mapping.py` is regexes and keyword tables (themes, conflict zones, country/sector terms); it imports no model. The only model in the stock-news path is FinBERT, a small fixed classifier, not a generative LLM, so it cannot recall later prices the way a chat model can. Its training text predates the study window for the most part, but it is not a point-in-time model; treat `stock_news_*` as slightly weaker evidence than the keyword families.

**Expected extra episodes:** history grows from about 1.1 years to about 7.1 years, roughly 6x, so families at 11 to 13 units should reach 20 where the event rate held; I did not run the study, so the true count is unverified. Mind the dedupe: episodes are collapsed by independence rules, so the gain per family can be less than 6x.

**Apply (not yet run, waiting for the lead):**

```bash
docker compose --profile agents --profile collectors up -d --no-build --force-recreate agents-heavy
```

(confirm the compose service name that owns `sentinel-agents-heavy` with `docker compose config --services` first). The first study after the restart takes longer: about 85 extra monthly queries and many more instruments' daily bars.

**Multiple testing:** the extra cells are tested in the same run, so the Benjamini-Hochberg correction stays across all cells (more cells in the family count, so the bar rises for everyone). Verdicts will change; expect some headline cells to flip to proven and some borderline ones to drop.

## G3 fix 2 registered before running (7 November 2026)

**What is added:** `insider_cluster` events for 1 January 2016 to 30 September 2026, rebuilt from the Form 4 rows in `insider_transactions` (open-market purchases and sales, code P and S; no network, no model). The live agent's rule is copied from `services/agents/quant_trading_engine.py` (`_process_insider_form4`), not invented:
- A purchase (code P) is a BUY; a sale (S) is a SELL. A trade flagged Rule 10b5-1 (`planned`) is OTHER and counts neither way.
- Window: the Form 4s filed in the 14 days up to and including the day under test, per ticker. The live agent keeps a rolling 14-day set keyed by arrival time; the table holds filing dates, so the day is the unit and all filings of a day arrive together.
- Distinct buyers: distinct insider names (owner CIK where the name is missing) among BUY trades in the window.
- Dollars: shares times price when both are positive, otherwise the trade adds no dollars but still counts as a buyer. `net_buy_usd` is BUY dollars minus SELL dollars in the window, floored at zero.
- Gate: at least 2 distinct buyers and `net_buy_usd` of at least $250,000.
- One cluster per ticker per 14 days: after a cluster is emitted, the next may fire only 14 or more days later (the live agent's `cluster_emitted` key lasts 14 days).
- Time stamp: the end of the New York day of the filing that met the gate (`insider_history.known_at`), the same convention as the other Form 4 history; so no cluster is measurable before the market had certainly seen it.
- Tickers pass `clean_symbols` (placeholders, crypto pairs dropped; a joined field uses its first symbol).
**What is not copied:** the live agent's feature flag, and its trades-per-arrival-second ordering within a day.
**Rows:** `type='insider_cluster'`, `source='insider_cluster_g3_history'`, `event_id='g3cluster:<TICKER>:<YYYY-MM-DD>'`, tags `insider_cluster`, `exposes:<TICKER>`, `g3_history`. Insert is `ON CONFLICT DO NOTHING`. The live agent's clusters (from October 2026) have `source='insider_cluster'` and do not overlap the history (table ends 30 Sep). G2's archive rows are `type='insider_trade'` with `f345:` ids, so the two cannot collide.
**Reading:** the family's cells are re-tested in the next study with the Benjamini-Hochberg correction across all cells; no threshold above is tuned on outcomes. Removal: `DELETE FROM signal_event_archive WHERE source = 'insider_cluster_g3_history';`

## G4 result (7 Nov 2026; `scripts/audit_tradable_signals.py`, rules registered above)

V0 reproduces the published cells (for example 8-K dilution 1d 102.9 bps, t 6.4 against 102.4 stored). V1 is liquid names (point-in-time trailing dollar volume of at least $140.8M a day, today's 1,000th-name level), V2 is V1 and shortable and easy to borrow today. Columns: independent units, aligned mean bps, t, hit rate.

| Cell | Orig q | V0 | V1: liquid | V2: liquid and borrowable |
|---|---|---|---|---|
| 8-K delisting notice 30d | 0.0000 | 60, 805, 6.08 | 32, 786, 3.12, 0.81 **survives** | 28, 449, 2.65, 0.79 **survives** |
| 8-K dilution 30d | 0.0000 | 60, 1061, 3.03 | 52, 539, 4.30, 0.75 **survives** | 51, 324, 3.00, 0.67 **survives** |
| Insider opportunistic sell 30d | 0.0055 | 60, 148, 3.60 | 59, 91, 3.42, 0.71 **survives** | 59, 91, 3.33, 0.71 **survives** |
| Put sweep 1d | 0.0156 | 23, 97, 3.58 | 23, 82, 3.21, 0.74 **survives** | 23, 82, 3.21, 0.74 **survives** |
| Put sweep 1h | 0.0458 | 114, 25, 2.63 | 113, 18, 2.23, 0.54 **survives** | 113, 18, 2.23, 0.54 **survives** |
| 8-K dilution 1d | 0.0000 | 1245, 103, 6.41 | 416, 80, 2.48, 0.55 **survives** | 383, 57, 1.90, 0.55 fails |
| Earnings fade (large) 5d | 0.0267 | 250, 34, 2.92 | 247, 35, 2.12, 0.56 **survives** | 247, 30, 1.95, 0.56 fails |
| 8-K auditor change 30d | 0.0002 | 58, 575, 4.34 | 26, 149, 1.14 fails | 25, 101, 1.02 fails |
| 8-K delisting notice 1d | 0.0006 | 1121, 81, 4.12 | 57, -1, 0.76 fails | 51, -30, 0.45 fails |
| 8-K delisting notice 5d | 0.0003 | 263, 192, 4.30 | 54, 71, 0.81 fails | 48, 81, 0.59 fails |
| 8-K dilution 5d | 0.0000 | 264, 277, 7.88 | 200, 102, 1.76 fails | 190, 57, 0.96 fails |
| 8-K impairment 30d | 0.0472 | 45, 423, 2.78 | 27, 348, 1.57 fails | 27, 348, 1.57 fails |
| Earnings reaction down 30d | 0.0018 | 60, 213, 4.50 | 59, 114, 1.98 fails (narrowly) | 59, 85, 1.93 fails |
| Earnings reaction down, small 30d | 0.0058 | 59, 208, 3.56 | 1 unit: cannot be tested | none |
| Stock news negative 1d | 0.0000 | 247, 55, 2.06 | 247, 4, -0.24 fails | 247, -5, -0.64 fails |
| Stock news negative 5d | 0.0000 | 52, 127, 2.56 | 52, 15, 0.46 fails | 52, 0, 0.17 fails |

**Of the 16 proven cells: 5 survive on liquid, borrowable names; 2 survive on liquid names only; 8 fail; 1 cannot be tested.** The 'negative stock news' signal is entirely small-name: 55 bps a day at V0 becomes 4 bps on liquid names.
The one-day and five-day delisting and dilution moves, and auditor changes, were micro-cap effects.

**Caveats.** The size gate uses today's dollar-volume level for events up to five years old, so it is stricter than a point-in-time top-1,000 would be (volumes were lower). 'Borrowable' is today's Alpaca easy-to-borrow flag,
not a borrow fee: a delisting-notice or dilution short usually carries a high fee, and the study charges only 10 bps, so the survivors are upper bounds for a short. Unit counts are small (23 to 59 for the survivors).
No false-discovery step was applied; original q shown beside.

**Consequence (proposed, not done).** The live signal engine raises signals for every proven cell on any instrument it can price. It should raise them only where the cell survives: for the 8 failing cells only on instruments that
pass the V1 gate if at all, and for shorts only on borrowable names. That changes which signals the idea layer sees, so it needs a decision.

G1 follow-up: `insider_cluster` added to `DEEP_HISTORY_TYPES` so the 14,215 clusters G3 rebuilt in the archive (2016 to 1 Oct 2026) are read beyond the 365-day window; the 10-year window covers them back to late 2016.

## G2 result (7 Nov 2026)

**Status: insider and 13F complete; 8-K history about 80% complete and resumable.** The one-off job (`services/collector-filings/deep_history_backfill.py insider 13f 8k`, niceness 15, single task, extra 0.25 s between requests) ran from 02:35 UTC. It stopped before the 8-K step finished, with no error in the log (the log kept only its first line), probably when the exec session ended. The 8-K done set holds 5,370 tickers (key `sentinel:history:edgar8k:done:from2016`); the rest are still to fetch. Nothing has been restarted.

**What changed (uncommitted).** 8-K window opens at 1 Jan 2016 and reads the `filings.files` older pages that reach into it (`history.py`, own done key, retry then park after 3 passes, 400-page cap per company). Insider: archive from 2016; Form 3/4/5 zips loaded from 2013 so 2016-18 trades have their three prior years; quarters 2016q1-2021q4 re-classified and archived from the table. 13F: first period 31 Dec 2015, own done key. All inserts are `ON CONFLICT DO NOTHING`, so reruns duplicate nothing.

**Rows in `signal_event_archive` by year (before -> after)**

| Year | 8-K filings | Insider trades | 13F flows |
|---|---|---|---|
| 2016 | 0 -> 24,198 | 0 -> 16,098 | 0 -> 63 |
| 2017 | 0 -> 25,421 | 0 -> 16,212 | 0 -> 78 |
| 2018 | 0 -> 26,606 | 0 -> 14,431 | 0 -> 133 |
| 2019 | 0 -> 27,831 | 0 -> 13,807 | 0 -> 156 |
| 2020 | 0 -> 32,239 | 0 -> 14,822 | 0 -> 215 |
| 2021 | 9,385 -> 37,120 | 3,528 -> 15,134 | 41 -> 237 |
| 2022 | 40,933 -> 47,344 | 13,017 -> 13,017 | 208 -> 208 |
| 2023 | 44,004 -> 51,093 | 12,620 -> 12,620 | 164 -> 164 |
| 2024 | 46,487 -> 54,071 | 10,296 -> 10,296 | 200 -> 200 |
| 2025 | 50,168 -> 58,599 | 9,021 -> 9,021 | 228 -> 228 |
| 2026 | 40,775 -> 52,333 | 7,069 -> 7,069 | 343 -> 343 |

The 8-K rows for 2022-2026 grew because the older pages also held filings the recent list had dropped for heavy filers. Insider classes in `insider_transactions` rose from 12, 79 and 4,824 classified trades in 2016, 2017 and 2018 to 53,398, 51,675 and 54,321.

**Format check.** 626 8-Ks from 2016-2020 sampled (six large filers, 20 inspected): items comma-separated in today's numbering (1.01, 2.02, 5.02, 5.07, 7.01, 8.01, 9.01 etc.), none empty, acceptance time present; the summary builds exactly as today and the 5.02 family rule fires on a 2016 row (test). No format problems. Two cautions: an after-hours filing has a filing date one day later than its acceptance time (the archive uses acceptance, which is right); and 13F tickers are resolved through today's CUSIP map, so names delisted since 2016 may be missing from the old flows (survivorship, small).

**Remaining.** Resume the 8-K step after the close (idempotent, skips the 5,370 done): `docker exec -d -w /app sentinel-filings sh -c 'python services/collector-filings/deep_history_backfill.py 8k > /tmp/g2.log 2>&1'`. Then recount.

**Recommended `SIGNAL_STUDY_HISTORY_DAYS`: 3995** (1 Jan 2016 to 7 Nov 2026 is 3,963 days, plus a month of margin). Set it once the 8-K step ends; insider and 13F can already use it.
