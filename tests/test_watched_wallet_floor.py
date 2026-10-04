"""
tests/test_watched_wallet_floor.py

A watched wallet bypassed the whale threshold at any size.

Measured on 1 October over one hour: 5,131 on-chain events, 5,096 under the
$250k whale threshold, median $271, one router alone 1,455 transfers averaging
$326 -- 37.5% of every event during a US session, against 2.9% for equities.
Wallets reach the watchlist by being the counterparty to one transfer over $5M,
and most such counterparties are routers and bots that move dust all day.
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

SRC = (ROOT / "services" / "collector-crypto" / "main.py").read_text(encoding="utf-8")


def test_a_watched_wallet_still_needs_a_minimum_size():
    assert (
        "amount_usd >= WHALE_THRESHOLD_USD or "
        "(is_suspect and amount_usd >= WATCHED_WALLET_MIN_USD)"
    ) in SRC
    assert "or is_suspect:" not in SRC


def test_the_floor_is_well_below_the_whale_threshold():
    """Watched wallets must still report moves a whale threshold would miss."""
    floor = float(SRC.split('os.getenv("WATCHED_WALLET_MIN_USD", "')[1].split('"')[0])
    whale = int(SRC.split("WHALE_THRESHOLD_USD = ")[1].split("\n")[0].replace("_", ""))
    assert 0 < floor < whale / 10


def test_the_whale_threshold_is_unchanged():
    assert "WHALE_THRESHOLD_USD = 250_000" in SRC
