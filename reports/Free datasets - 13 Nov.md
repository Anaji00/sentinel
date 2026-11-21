# Free datasets with no live collector - built 13 Nov 2026

Three collectors store CFTC Commitments of Traders, Treasury auction results and the Reg SHO threshold list, each with its publication time. They are measurement only: nothing reads
them into an alert, a Kafka topic or the recommendation engine. All three run as loops inside `collector-macro` (own aiohttp session, `swallowed(...)` on failure, bounded retry via
`next_delay`, every 3 h). Only **collector-macro needs recreating**.

## Files
| Area | Files |
|---|---|
| Pure parsing | `shared/utils/cot_parse.py`, `auction_parse.py`, `reg_sho_parse.py` |
| Pure statistics | `shared/utils/event_study.py` (forward 5-session excess return vs SPY, cluster-by-date t, same-sign halves, Benjamini-Hochberg) |
| Collectors | `services/collector-macro/cftc_cot.py`, `treasury_auctions.py`, `reg_sho.py`; hook = 4 lines in `services/collector-macro/main.py` |
| Migrations | `shared/db/migrate.py` 0092 (`cftc_cot`, `treasury_auctions`), 0093 (`reg_sho_threshold`) - applied |
| Studies | `scripts/study_cftc_cot.py`, `study_treasury_auctions.py`, `study_reg_sho.py`, `study_common.py`, `study_free_datasets_combine.py` (joint BH across all cells) |
| Tests | `tests/test_free_datasets.py` (16 pass); each study script has `--selftest` |

## Sources
- **CFTC**: `publicreporting.cftc.gov/resource/72hh-3qpy.json` (disaggregated, futures only). `report_date` = Tuesday as-of; `published_at` = scheduled Friday 15:30 ET (`published_basis`), `fetched_at` = storage time.
- **Treasury**: `treasurydirect.gov/TA_WS/securities/search` (the `auctioned` endpoint ignores date filters and caps at 250 rows). `published_at` = competitive close + 10 min.
- **Reg SHO**: `nasdaqtrader.com/dynamic/symdir/regsho/nasdaqth<YYYYMMDD>.txt`; `published_at` = the file's own creation time.

## Verified from a real fetch (first pass run 13 Nov through the collectors' own code into the live tables, throwaway container)
| Table | Rows | Range | Notes |
|---|---|---|---|
| `cftc_cot` | 187,026 | 2006-06-13 to 2026-11-03 | 662 contracts; 427 s |
| `treasury_auctions` | 4,172 | 2016-01-04 to 2026-11-12 | 3,046 bills, 891 notes, 235 bonds; 180 s |
| `reg_sho_threshold` | 27,764 | 501 list dates, 2024-11-13 to 2026-11-12 | 1,017 symbols; 523 weekdays checked, 0 errors, 22 holidays skipped; 375 s |

Parser tests use trimmed copies of those responses. Real fetch also showed: the 2 Nov positions release Friday 6 Nov; the latest CFTC week (10 Nov) is not out until 13 Nov 15:30 ET.

## Not verified / gaps
- **CFTC is commodities only.** The disaggregated report has no rates, FX or equity-index futures (those are in the Traders in Financial Futures report, not collected). The study covers 8 mapped contracts: energy (WTI, nat gas), metals (gold, silver, copper), grains (corn, wheat, soybeans).
- **CFTC `published_at` is scheduled, not observed**; holiday-shifted releases (Monday) are not modelled. The study enters on the first session after the release date, so a Monday release does not leak.
- **Treasury tail vs when-issued is not in the free feed**; not stored or tested. `published_at` is an approximation (close + 10 min).
- **Reg SHO covers Nasdaq's list only.** NYSE's endpoint returned empty for every parameter tried; NYSE-listed names are absent. Holiday vs "not yet published" days are indistinguishable (both redirect); the last 7 days are re-checked every pass.
- **Study signal definitions were chosen by the builder** (CFTC 90th/10th percentile of managed-money net/OI over 156 weeks; Treasury z of bid-to-cover and indirect share against the last 12 auctions at +-1; Reg SHO entry/exit by Nasdaq category). They need registering before any run. Cells below 100 events fail by definition; CFTC cells will likely be near that line.
- **Studies were not run** (selftests only). Event counts are unknown.
- Reg SHO 4.3k+ names: file creation time is ~23:00 ET, so entry is the next session.

## Expected live check (after `collector-macro` is recreated)
```bash
MSYS_NO_PATHCONV=1 docker exec sentinel-timescaledb sh -c 'psql -U "$POSTGRES_USER" -d "${POSTGRES_DB:-$POSTGRES_USER}" -Atc "select max(report_date), max(published_at) from cftc_cot; select max(auction_date) from treasury_auctions; select max(list_date), count(*) from reg_sho_threshold"'
docker logs sentinel-macro 2>&1 | grep -i "CFTC COT\|Treasury auctions\|Reg SHO\|collector_macro.\(cftc\|treasury\|reg_sho\)" | tail
```
Expect: `max(report_date)` 2026-11-10 after Fri 13 Nov 15:30 ET (3 h loop, so by ~19:00 ET); `max(auction_date)` advancing on each auction day; `max(list_date)` the previous weekday after ~23:00 ET. Log lines appear only when rows are added; failures show as `Suppressed failure at collector_macro.*`.

## Run the studies (when the definitions are registered)
Each script's docstring has the `docker run` line (image `sentinel-agents-heavy`, own capped container). Add `--out reports/study_<name>.json` and run `scripts/study_free_datasets_combine.py` on the three outputs for the joint BH.
