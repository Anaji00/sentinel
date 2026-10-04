"""
shared/utils/liquidations.py

Liquidation bursts, judged against the asset's own recent flow.

Binance answers 451 from this host, so no liquidation had reached the platform:
both crypto rules (`rule_crypto_stress_cascade`, `rule_crypto_equity_contagion`)
trigger on one, and had only funding prints left. OKX answers. Its flow is very
uneven between the two assets -- measured on 4 October, BTC liquidated about
$1.2M across twenty hours while ETH liquidated $0.8M in under seven minutes --
so a fixed dollar bar would be silent on one and noisy on the other. A burst is
a five-minute bucket at several times its asset-and-side median.
"""

from __future__ import annotations

from statistics import median
from typing import Dict, Iterable, List, Optional, Tuple

BUCKET_SEC = 300
HISTORY_BUCKETS = 288          # a day of five-minute buckets
MIN_HISTORY_BUCKETS = 48       # four hours before a burst can be judged
BURST_MULTIPLE = 3.0
BURST_FLOOR_USD = 250_000.0    # below this nothing is a burst, whatever the median

# OKX contract sizes for the swaps polled (contracts -> coins). Read live from
# /public/instruments at start; these are the values it returned on 4 October.
DEFAULT_CONTRACT_VALUE = {"BTC": 0.01, "ETH": 0.1}


def bucket_of(ts_ms: int) -> int:
    """The five-minute bucket a millisecond timestamp falls in."""
    return int(ts_ms // 1000 // BUCKET_SEC)


def side_of(pos_side: str) -> str:
    """Which book was liquidated: a long position forced out sells."""
    return "LONG" if str(pos_side).lower() == "long" else "SHORT"


def notional_usd(contracts: float, contract_value: float, price: float) -> float:
    return float(contracts) * float(contract_value) * float(price)


def aggregate(details: Iterable[Dict], contract_value: float, after_ts: int) -> Dict[Tuple[int, str], Dict[str, float]]:
    """Sum fresh liquidation fills into (bucket, side) totals."""
    out: Dict[Tuple[int, str], Dict[str, float]] = {}
    for d in details:
        try:
            ts = int(d["ts"])
            sz = float(d["sz"])
            px = float(d["bkPx"])
        except (KeyError, TypeError, ValueError):
            continue
        if ts <= after_ts or sz <= 0 or px <= 0:
            continue
        key = (bucket_of(ts), side_of(d.get("posSide", "")))
        agg = out.setdefault(key, {"notional": 0.0, "coins": 0.0, "px_weight": 0.0, "fills": 0, "last_ts": 0})
        n = notional_usd(sz, contract_value, px)
        agg["notional"] += n
        agg["coins"] += sz * contract_value
        agg["px_weight"] += px * n
        agg["fills"] += 1
        agg["last_ts"] = max(agg["last_ts"], ts)
    return out


def burst_multiple(total: float, history: List[float]) -> Optional[float]:
    """How many times its baseline a closed bucket is, or None if not a burst.

    None too while the history is short: an unmeasured asset is not quiet.
    """
    if len(history) < MIN_HISTORY_BUCKETS or total < BURST_FLOOR_USD:
        return None
    base = max(median(history), BURST_FLOOR_USD / BURST_MULTIPLE)
    ratio = total / base
    return round(ratio, 2) if ratio >= BURST_MULTIPLE else None


def burst_score(multiple: float) -> float:
    """3x scores 0.50, 5x 0.70, 7x and up 0.90."""
    return round(min(0.90, 0.20 + 0.10 * float(multiple)), 3)
