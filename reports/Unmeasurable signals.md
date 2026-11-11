# Unmeasurable signals: why 9 families have no verdict, and what 1,937 price-less instruments are (G3, 7 November 2026)

Produced by `scripts/audit_unmeasurable.py` (read-only; run in a one-off container with its own 450 MB cap against the stored study of 01:08 UTC on 7 Nov). Nothing was written, queued or restarted by this audit's queries.

## Answer in four lines

1. **The 9 unmeasurable cells are not a price-data problem.** Every one is the **30-day** cell of a family whose first episode is from October 2026. None has an episode older than 30 days (insider_cluster has 2), so no 30-day window has closed. Their instruments are mostly well priced (see table A). Time alone moves them to "insufficient data" between 12 and 23 November; no backfill is needed for that.
2. **"Measurable" is not "testable".** A cell needs 20 independent units, and a unit is one non-overlapping horizon-length block of entries. At 30 days that is about 19 months of episodes; at 5 days about 100 days. Live-only families cannot reach a verdict at 30d until mid-2028 without history. Only `insider_cluster` can be given history cheaply.
3. **The 1,937 bar-less instruments are mostly not tradable names**: placeholder tickers (`NONE`, `N/A`), crypto pairs in news (`BTCUSD`), OTC and foreign lines, and delisted or renamed symbols. Only a few hundred can be filled from the free feed, and they sit in families that already have verdicts, so **filling them changes no cell's verdict** (it adds a few percent more episodes to the insider and news families).
4. Fixes ranked below; the cheapest real gain is `insider_cluster` history (up to 4 cells) plus a small ticker clean-up.

## A. The 9 unmeasurable families

| Family | Episodes | First episode | 30d window closes for earliest | Episodes old enough for 30d | Instruments (with daily bars) | What is wrong with prices |
|---|---|---|---|---|---|---|
| guidance_lowered | 11 | 24 Oct | 23 Nov | 0 | 11 (11) | Nothing |
| guidance_raised | 136 | 17 Oct | 16 Nov | 0 | 136 (136) | Nothing (5 episodes after the last bar) |
| insider_cluster | 30 | 5 Oct | already | 2 | 29 (27) | 2 symbols: `HEI, HEI.A` (two tickers in one field) and one unlisted |
| options_call_buy | 151 | 19 Oct | 18 Nov | 0 | 77 (77) | Nothing; underlying stocks, not OCC symbols |
| options_opening_call | 302 | 20 Oct | 19 Nov | 0 | 127 (127) | Nothing |
| options_opening_put | 232 | 20 Oct | 19 Nov | 0 | 98 (98) | Nothing |
| options_put_buy | 126 | 19 Oct | 18 Nov | 0 | 62 (62) | Nothing |
| radar_no_ais | 80 | 13 Oct | 12 Nov | 0 | 6 (2) | 4 of 6 instruments are futures (`ZC=F, ZW=F, BZ=F, CL=F`): intraday bars only, no `daily_bars`; 8 episodes lack hourly cover |
| stock_news_fade | 2,271 | 18 Oct | 17 Nov | 0 | 1,739 (1,271) | 468 instruments have no bars at all (366 episodes, 16%); 320 episodes fall after the last daily bar (the last day) |

Other horizons of the same families are not unmeasurable: they show 1h to 5d with 1 to 128 units (for example options_call_buy 1h 48 units, stock_news_fade 1h 128). So for these nine, 1h and 1d price coverage is fine and the shortfall is **sample length**.

Why so young: `options_*` depend on the buyer-initiated and opening-confirmed tags, `guidance_*` on the local-model filing extractor, `radar_no_ais` on the radar collector, `stock_news_fade` was registered on 18 Oct (`NEWS_FADE_REGISTERED`), and `insider_cluster` fires from the live Form 4 agent. All are live-forward signals.

## B. The 1,937 instruments (cause table)

The study's stored count is 1,937 (`instruments_without_bars`). Re-reading the archive now, with the study's own rules (no `daily_bars`, no `tradfi_bars` for the last year, at least one non-price-directed family, not `=F`), I find **3,146** (3,269 before the intel filter). I could not reconcile the 1,200 gap: the archive grew by only 76 rows since the study ran, and raw minute bars add nothing (0 of 3,269). Treat my counts as the current picture and the 1,937 as the stored figure; the cause shares are what matters.

| Cause | Instruments | Observations | Examples | Free Alpaca data (sample of 100) |
|---|---|---|---|---|
| Not in Alpaca's active list: delisted, renamed, OTC, non-US | 1,642 | 9,405 | `PFBX, CBKM, CSBB, FMBM, PVBC, CEN` (US bank holding companies; `NONE` also lands here) | 0 recent; 46 history only; 54 nothing |
| Odd characters: lists and joined fields | 642 | 2,241 | `N/A`, `HEI, HEI.A`, `GEF,GEF.B`, `BCDA;BCDAW`, `TSX:PPL` | Not a symbol: needs parsing |
| Alpaca-listed common stock or OTC ADR, but no bars stored | 311 | 1,691 | `BUKS, TTSH, EADSY, VWAGY, BYDDY, CYAN` | 13 recent, 25 history only, 62 nothing |
| 5-letter foreign OTC ordinary or ADR (ends F or Y) | 310 | 842 | `SSNLF, HYMTF, BYDDF, LOVLY` | 0 recent; 2 history; 98 nothing |
| Longer than 5 letters: crypto pairs | 121 | 3,468 | `BTCUSD, ETHUSD, DOGEUSD, XRPUSD, SOLUSD` | Not equities; `BTCUSD` has no bars under that name |
| Dot or dash share class, preferred | 56 | 130 | `NCC.B, LGF.A, SRG.PA, BAJAJ-AUTO` | Needs mapping to Alpaca's class form |
| ETF or fund, Alpaca-listed | 36 | 92 | `EEM, ARKK, VTI, KRE, ICLN, MAGS` | **35 of 36 serve recent bars** |
| Warrants, units, rights, preferreds | 28 | 46 | `AAGRW, BRTMU, LEXXW, GGT.RT` | 5 of 9 recent; warrants mostly nothing; mark untradeable |

Where the observations sit (observations, not collapsed episodes):

| Family | Observations on bar-less instruments | Share of the family |
|---|---|---|
| stock_news_positive / negative | 7,150 / 4,868 | 12% / 15% (mostly the crypto pairs: `BTCUSD` alone is 790) |
| insider_routine_buy | 2,993 | 14% (`NONE` 933, `PFBX` 465, `N/A` 206) |
| insider_opportunistic_buy / sell | 1,426 / 698 | 12% / 3% |
| stock_news_fade | 571 | 20% |
| 8-K and 13D/13F families | under 60 each | under 1% |

Findings that are bugs rather than missing data:
- `NONE` (1,413 observations) and `N/A` (264) are issuers filed without a ticker. They are treated as instruments. A bar-less `NONE` cannot be priced; filtering them out at the source (`_ticker()` in `shared/utils/signal_families.py`) removes 1,677 phantom observations (all insider families).
- News tagged with crypto pairs (`BTCUSD`, `ETHUSD`, ...) is treated as equity signal: about 2,200 observations in the stock-news families. Crypto bars live under another symbol form (daily_bars has `BTC` the equity, not the coin). Either map them to crypto bars or drop them from the equity study.
- Joined fields (`HEI, HEI.A`, `GEF,GEF.B`, `BCDA;BCDAW`) are one filing listing several tickers; they should be split on `,;/ ` before they become instruments.

## C. What can be filled free, and what to mark untradeable

| Group | Instruments | Action |
|---|---|---|
| ETFs and funds | 36 | Fill: 35 of 36 are served. Add to the daily backfill (they bypass the breadth screen, which reads the stock-only valid set) |
| Alpaca-listed common stock and ADRs | 311 | Fill about 13% (recent) and 25% more as history; the rest are served nothing: mark **no price** |
| Delisted or renamed US names | 1,642 | History exists for about 46% of a plain-ticker sample; fill for measurement (survivorship), but they are not tradeable now: mark **untradeable** |
| Foreign OTC lines (F/Y) | 310 | Not served (98%): mark **untradeable / no price** |
| Warrants, units, rights, preferreds | 28 | Mark **untradeable** |
| Placeholder, joined and crypto symbols | 764 + 121 | Fix the symbol (parse, map crypto, drop `NONE`/`N/A`), do not backfill |
| Dot/dash classes | 56 | Map to Alpaca's form, then retry |

Rough upper bound for the free feed: 36 + about 80 (13 recent, 25 history of 100, over 311) + about 750 (history only of the delisted group) = **under 900 of 3,146 instruments, and about 800 of those only as stale history**. The rest need symbol repair or an "untradeable" mark.

Why existing backfills missed them (for the lead's reference): the needs-bars set is capped at 300 names (`MAX_NEEDS_BARS`) sorted by episode count (297 of the current bar-less names are in it, the rest are never asked for); the daily breadth screen reads stocks from the valid set only (11,934) and drops illiquid names (7,300 flagged); the collector's `_clean_equity()` filter excludes dashes, `=` and anything over 6 letters.

## D. Ranked fix list (propose only; nothing is changed)

| # | Fix | Cost | Cells that move | Notes |
|---|---|---|---|---|
| 1 | **Wait**, and relabel: when a family's first episode is under 30 days old, show "too young (n days)" instead of "unmeasurable" | None, or a small label change in `evaluate_cell` | 9 cells go unmeasurable to insufficient_data by 23 Nov on their own; the relabel makes the report honest today | None can be proven at 30d for about 19 months (20 units) |
| 2 | **Reconstruct `insider_cluster` history** from the Form 4 rows already in the archive (the rule is a count of distinct buying insiders in 14 days) and archive them as `insider_cluster` events | Small: SQL plus the existing rule, no network, no inference | Up to 4 (all horizons of insider_cluster go from 4 to 14 units to hundreds); the 30d cell becomes testable | Best yield. Must use the same buy-only filter and thresholds as the live agent, and be registered as an extension |
| 3 | **Ticker hygiene** in `_ticker()` and the exposure parser: drop `NONE`/`N/A`/empty, split joined fields, map or drop crypto pairs | Small, pure function with tests | 0 verdict changes; removes about 1,700 phantom insider observations and about 2,200 crypto-news observations, cleaning 3 to 5 cells' unit counts | Do before any bar backfill: most of the 1,937 are these |
| 4 | **Add ETFs and funds** to the daily backfill list and **lift the 300 cap** (or loop the needs-bars set in batches) for listed names | Small | 0 verdict changes; about 35 + 80 more instruments gain bars | Alpaca free, SIP once 15 minutes old |
| 5 | **Mark untradeable** (no price): foreign F/Y lines, warrants and rights, names Alpaca serves nothing for. Exclude them from `needs_bars` so the count falls to the real gap | Small | Honest "cannot be priced" instead of 1,937 | About 2,000 of the 3,146 |
| 6 | **Delisted history** for the 46% of plain tickers Alpaca still serves | Medium, paced | 0 verdict changes; reduces survivorship in insider and news families | Optional |
| 7 | **Options and guidance families**: forward only. Back-filling options needs historical quotes to classify buyer-initiated and next-day open interest; guidance needs a model to read five years of releases (inference cost, and the model may remember outcomes). `stock_news_fade` is the mirror of stock_news_positive, registered 18 Oct, so back-filling it would break the pre-registration | n/a | 0 now; 5d cells reach 20 units in about 100 days for options, 1d in about 20 days | Retire `radar_no_ais`: 80 episodes on 6 instruments, 4 of them futures |

Expected number of cells moving: **9 to "insufficient data" with time alone (fix 1), up to 4 more to testable (fix 2), 0 from price backfills (fixes 3 to 6).**

## E. Not verified

- The 3,146 vs 1,937 gap is unexplained (above). The cause shares would need re-checking against the next study run.
- The "free feed" figures come from probes of 100 symbols per group (and all 36 ETFs), by plain tickers only; the 311 "Alpaca-listed" group includes OTC names. Extrapolated counts are estimates.
- Crypto: I did not locate where crypto bars are stored; `BTCUSD` has none under that ticker. Not verified whether a crypto price table exists.
- Hourly cover for the 1h horizon was read from the `tradfi_bars_1h` rollup, which can lag the raw table; the raw-table check added nothing.
- Incident: two earlier attempts to run this audit inside `sentinel-agents-heavy` (a shared 1 GiB container) ended with exit 137 and the container restarted (restart count rose). `OOMKilled` read false and the first restart exit code was 0, so I cannot prove which was the cause, but the reads were too heavy for that container next to the signal engine. The final runs used a separate capped container. If the signal engine or the G4 job there stopped around 01:56 to 01:58 UTC on 7 Nov, that is why.
