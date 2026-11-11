"""G3 fix 2 (reports/Event backtest scope.md, "G3 fix 2 registered"): insider_cluster history from stored Form 4 trades.

Run in a one-off capped container (it needs the database only):
    python scripts/backfill_insider_clusters.py            # dry run: counts by year, nothing written
    python scripts/backfill_insider_clusters.py --apply    # insert into signal_event_archive (ON CONFLICT DO NOTHING)

Rows carry source 'insider_cluster_g3_history' and ids 'g3cluster:<TICKER>:<YYYY-MM-DD>'. One statement removes them:
    DELETE FROM signal_event_archive WHERE source = 'insider_cluster_g3_history';
"""
import argparse
import asyncio
import json
from collections import Counter
from datetime import date

from shared.db import get_timescale
from shared.utils.insider_clusters import SOURCE, FormFourTrade, event_id, find_clusters, trade_usd
from shared.utils.insider_history import INSIDER_TABLE, known_at
from shared.utils.ticker_hygiene import clean_symbols

FIRST_DAY = date(2016, 1, 1)           # the archive opens with the first daily price bar
LOAD_FROM = date(2015, 12, 1)          # the 14 days before the first emitted day
LAST_DAY = date(2026, 9, 30)           # the table ends here; the live agent's clusters start in October
CHUNK = 150

_CANDIDATES_SQL = f"""
    SELECT ticker FROM {INSIDER_TABLE}
    WHERE code = 'P' AND NOT planned AND filed >= $1 AND filed <= $2
    GROUP BY ticker HAVING count(DISTINCT owner_cik) >= 2
"""
_TRADES_SQL = f"""
    SELECT ticker, filed, code, planned, owner_cik, owner_name, shares, price FROM {INSIDER_TABLE}
    WHERE ticker = ANY($1::text[]) AND filed >= $2 AND filed <= $3 AND code IN ('P', 'S')
"""
_INSERT_SQL = """
    INSERT INTO signal_event_archive
        (event_id, type, source, created_at, occurred_at, headline, summary, tags,
         sentiment, primary_entity_id, financial_data, region)
    VALUES ($1, 'insider_cluster', $2, $3, $3, $4, $5, $6, NULL, $7, $8::jsonb, NULL)
    ON CONFLICT DO NOTHING
"""


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="insert the rebuilt clusters (default: dry run)")
    args = ap.parse_args()
    db = await get_timescale()

    cand = sorted({r["ticker"] for r in await db.query(_CANDIDATES_SQL, LOAD_FROM, LAST_DAY) or []})
    print(f"tickers with 2+ distinct unplanned buyers in the table: {len(cand)}")
    clusters = []
    for k in range(0, len(cand), CHUNK):
        by_sym: dict = {}
        for r in await db.query(_TRADES_SQL, cand[k:k + CHUNK], LOAD_FROM, LAST_DAY) or []:
            syms = clean_symbols(r["ticker"])
            if not syms:
                continue
            who = str(r["owner_name"] or "").strip().upper() or str(r["owner_cik"])
            by_sym.setdefault(syms[0], []).append(
                FormFourTrade(r["filed"], r["code"], bool(r["planned"]), who, trade_usd(r["shares"], r["price"])))
        for sym, trades in by_sym.items():
            clusters += [c for c in find_clusters(sym, trades) if c.day >= FIRST_DAY]

    clusters.sort(key=lambda c: (c.day, c.ticker))
    by_year = Counter(c.day.year for c in clusters)
    print(f"\nclusters rebuilt: {len(clusters)} on {len({c.ticker for c in clusters})} tickers")
    print("| year | clusters |\n|---|---|")
    for y in sorted(by_year):
        print(f"| {y} | {by_year[y]} |")
    buyers = Counter(min(len(c.buyers), 6) for c in clusters)
    print("buyers per cluster (6 = 6 or more):", dict(sorted(buyers.items())))
    sizes = sorted(c.net_buy_usd for c in clusters)
    if sizes:
        print(f"net bought: median ${sizes[len(sizes) // 2]:,.0f}, 90th percentile ${sizes[int(len(sizes) * 0.9)]:,.0f}")
    print("sample:", "; ".join(f"{c.ticker} {c.day} {len(c.buyers)} buyers ${c.net_buy_usd / 1e6:.2f}M" for c in clusters[-5:]))

    if not args.apply:
        print("\ndry run: nothing written")
        return
    rows = []
    for c in clusters:
        rows.append((
            event_id(c.ticker, c.day), SOURCE, known_at(c.day),
            f"Insider buying cluster: {len(c.buyers)} insiders net bought ${c.net_buy_usd / 1e6:.2f}M of {c.ticker} within 14 days",
            "Open-market purchases (Form 4 code P) by distinct insiders: " + ", ".join(c.buyers[:12]),
            ["insider_cluster", f"exposes:{c.ticker}", "g3_history"], c.ticker,
            json.dumps({"ticker": c.ticker, "side": "BUY", "instrument_type": "equity"}),
        ))
    before = (await db.query_one("SELECT count(*) AS n FROM signal_event_archive WHERE source = $1", SOURCE))["n"]
    for i in range(0, len(rows), 1000):
        await db.execute_many(_INSERT_SQL, rows[i:i + 1000])
    after = (await db.query_one("SELECT count(*) AS n FROM signal_event_archive WHERE source = $1", SOURCE))["n"]
    print(f"\ninserted: rows with this source {before} -> {after} (of {len(rows)} rebuilt)")


if __name__ == "__main__":
    asyncio.run(main())
