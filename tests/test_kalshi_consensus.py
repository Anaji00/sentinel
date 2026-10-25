"""A macro consensus from Kalshi's strike ladder."""

import pathlib
import sys
from datetime import datetime, timezone

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import kalshi_consensus as kc  # noqa: E402


def _m(strike, bid, ask, event="KXJOBLESSCLAIMS-26OCT16", close="2026-10-16T12:25:00Z", kind="greater_or_equal"):
    return {"event_ticker": event, "close_time": close, "strike_type": kind, "floor_strike": strike,
            "yes_bid_dollars": f"{bid:.4f}", "yes_ask_dollars": f"{ask:.4f}"}


LADDER = [_m(200000, 0.95, 0.97), _m(210000, 0.80, 0.84), _m(220000, 0.55, 0.59), _m(230000, 0.25, 0.29),
          _m(240000, 0.05, 0.07), _m(250000, 0.01, 1.00)]          # last: no real quote


def test_the_median_is_where_the_market_crosses_one_half() -> None:
    pts = kc.strike_probabilities(LADDER)
    assert [k for k, _ in pts] == [200000, 210000, 220000, 230000, 240000]
    med = kc.implied_median(pts)
    assert abs(med - (220000 + (0.57 - 0.5) / (0.57 - 0.27) * 10000)) < 1e-6


def test_crossed_books_are_made_monotone_and_one_sided_ladders_give_nothing() -> None:
    pts = kc.strike_probabilities([_m(1, 0.6, 0.62), _m(2, 0.7, 0.72), _m(3, 0.3, 0.32)])
    assert [p for _, p in pts] == [0.61, 0.61, 0.31]
    assert kc.implied_median([(1, 0.9), (2, 0.8), (3, 0.7)]) is None
    assert kc.implied_median([(1, 0.9), (2, 0.4)]) is None                  # too few strikes


def test_the_nearest_release_is_used_and_units_are_converted() -> None:
    now = datetime(2026, 10, 16, 5, 0, tzinfo=timezone.utc)
    later = [_m(k, b, a, event="KXJOBLESSCLAIMS-26OCT22", close="2026-10-22T12:25:00Z") for k, b, a in
             [(200000, 0.9, 0.92), (220000, 0.5, 0.52), (240000, 0.1, 0.12)]]
    snap = kc.consensus(LADDER + later, "JOBLESS_CLAIMS", now)
    assert snap["event"] == "KXJOBLESSCLAIMS-26OCT16" and snap["strikes"] == 5
    pay = [_m(k, b, a, event="KXPAYROLLS-26NOV", close="2026-12-04T13:29:00Z", kind="greater")
           for k, b, a in [(0, 0.9, 0.92), (50000, 0.6, 0.62), (100000, 0.3, 0.32)]]
    # 0.61 at 50k, 0.31 at 100k: 50k + 0.11/0.30 x 50k = 68,333 jobs, in thousands as PAYEMS.
    assert abs(kc.consensus(pay, "NFP", now)["median"] - 68.3333) < 1e-3
    assert kc.consensus(LADDER, "GDP", now) is None
