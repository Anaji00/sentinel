"""
scripts/backfill_perp_funding.py

Fills the gap in perpetual funding history left by the Binance outage.

Binance began answering HTTP 451 to this host -- "unavailable for legal reasons"
-- on both fstream and fapi. The last events it produced are dated 2026-08-10
(4,260 of them); the OKX poller that replaced it starts on 2026-08-23. Between
those dates the platform has no funding history at all, which is a hole in
exactly the series correlation and the agents reason over.

OKX publishes settled funding per instrument going back about 33 days, which
covers the gap. Only what that endpoint actually returns is written:
funding_rate and the realized rate at each settlement. Mark price, index price,
basis and open interest are *not* recoverable for a past moment from this API,
so those stay null rather than being reconstructed from today's values -- a
backfilled row that invents a basis is indistinguishable from a measured one.

Idempotent: rows are keyed on (instrument, settlement time) and re-running skips
what is already present.

    python scripts/backfill_perp_funding.py            # top 12 instruments
    python scripts/backfill_perp_funding.py --dry-run
    python scripts/backfill_perp_funding.py --limit 25
"""

import argparse
import asyncio
import json
import logging
import sys
import urllib.request as urlrequest
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from shared.db import get_timescale  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s [backfill] %(levelname)s %(message)s")
logger = logging.getLogger("backfill.perp_funding")

OKX_BASE = "https://www.okx.com/api/v5"
HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}

# Backfilled rows say so. A row assembled after the fact from a history endpoint
# is not the same evidence as one observed live, and anything reasoning over the
# series should be able to tell the difference.
BACKFILL_SOURCE = "okx_swap_backfill"


def _get(path: str, params: str = "") -> List[dict]:
    try:
        req = urlrequest.Request(f"{OKX_BASE}{path}{params}", headers=HEADERS)
        body = json.loads(urlrequest.urlopen(req, timeout=30).read())
    except Exception as e:
        logger.warning("OKX %s failed: %s", path, e)
        return []
    if str(body.get("code")) != "0":
        logger.warning("OKX %s -> code %s %s", path, body.get("code"), body.get("msg"))
        return []
    return body.get("data") or []


def _f(value, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def top_instruments(limit: int) -> List[str]:
    """Most-traded USDT swaps by dollar notional."""
    tickers = _get("/market/tickers", "?instType=SWAP")
    usdt = [t for t in tickers if str(t.get("instId", "")).endswith("-USDT-SWAP")]
    # Ranked by notional, not base-unit volume: volCcy24h alone puts a token
    # priced at $0.00000001265 above BTC.
    usdt.sort(key=lambda t: _f(t.get("volCcy24h")) * _f(t.get("last")), reverse=True)
    return [t["instId"] for t in usdt[:limit]]


async def existing_settlements(db, instrument: str) -> set:
    """Settlement timestamps already stored for one instrument."""
    rows = await db.query(
        """
        SELECT occurred_at FROM events
        WHERE type = 'crypto_perp_funding'
          AND crypto_data->>'pair' = $1
        """,
        instrument,
    )
    seen = set()
    for r in rows:
        value = r["occurred_at"]
        # TimescaleClient.query() hands back timestamps as ISO strings, not
        # datetimes, so this has to parse before comparing. Getting it wrong is
        # not loud: the set simply stays empty and every settlement looks new,
        # which is how the first run duplicated 300 rows.
        if isinstance(value, str):
            try:
                value = datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                continue
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        seen.add(value.astimezone(timezone.utc).replace(microsecond=0))
    return seen


async def backfill_instrument(db, instrument: str, dry_run: bool) -> int:
    history = _get("/public/funding-rate-history", f"?instId={instrument}&limit=100")
    if not history:
        return 0

    already = await existing_settlements(db, instrument)
    asset = instrument.replace("-USDT-SWAP", "")
    written = 0

    for row in history:
        settled_ms = _f(row.get("fundingTime"))
        if not settled_ms:
            continue
        settled = datetime.fromtimestamp(settled_ms / 1000.0, tz=timezone.utc).replace(microsecond=0)
        if settled in already:
            continue

        funding_rate = _f(row.get("fundingRate"))
        realized = _f(row.get("realizedRate"), funding_rate)

        # Only fields this endpoint actually returns. mark_price, index_price,
        # basis_bps and open_interest are unknowable for a past settlement and
        # are left out rather than filled from today's numbers.
        crypto_data = {
            "pair": instrument,
            "trade_type": "CRYPTO_PERP_FUNDING",
            "side": "POSITIVE" if funding_rate > 0 else "NEGATIVE",
            "price": 0.0,
            "size_tokens": 0.0,
            "funding_rate": funding_rate,
            "realized_rate": realized,
        }
        annualized = abs(funding_rate) * 3 * 365 * 100
        headline = (
            f"⚡ FUNDING SETTLEMENT | {asset} | Rate: {funding_rate:.6f} "
            f"({annualized:.1f}% annualized)"
        )

        if dry_run:
            written += 1
            continue

        await db.query(
            """
            INSERT INTO events (
                type, occurred_at, collected_at, source, source_reliability,
                primary_entity_id, primary_entity_type, primary_entity_name,
                headline, summary, crypto_data, tags, anomaly_score
            ) VALUES (
                'crypto_perp_funding', $1, NOW(), $2, 0.95,
                $3, 'ASSET', $3,
                $4, $5, $6, $7, $8
            )
            """,
            settled, BACKFILL_SOURCE, asset, headline,
            f"Settled funding for {instrument} at {settled.isoformat()}.",
            # The dict, not json.dumps(...): the pool registers a jsonb codec
            # with encoder=json.dumps, so a pre-serialised string is encoded a
            # second time and lands as a jsonb *string* rather than an object.
            # crypto_data->>'pair' then returns NULL, which silently broke both
            # the dedup check here and every downstream reader.
            crypto_data,
            ["crypto", "perp_funding", "backfill", asset.lower()],
            # Historical settlements are context, not live signal. A backfilled
            # row must not out-rank a live one in any anomaly-ordered view.
            0.0,
        )
        written += 1

    return written


async def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--limit", type=int, default=12, help="instruments to backfill")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    instruments = top_instruments(args.limit)
    if not instruments:
        logger.error("Could not list OKX instruments; nothing to do.")
        return 1
    logger.info("Backfilling %s instruments: %s", len(instruments), ", ".join(instruments[:6]) + " ...")

    db = await get_timescale()
    total = 0
    for inst in instruments:
        n = await backfill_instrument(db, inst, args.dry_run)
        total += n
        logger.info("%-22s %s settlements%s", inst, n, " (dry run)" if args.dry_run else "")

    logger.info("Done: %s funding settlements %s.", total, "would be written" if args.dry_run else "written")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
