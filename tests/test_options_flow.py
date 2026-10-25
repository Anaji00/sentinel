"""Option prints read for direction: extrinsic premium, trade side, openings."""

import pathlib
import sys
from datetime import datetime, timedelta, timezone

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import options_flow as ofl  # noqa: E402
from shared.utils import signal_families as sf  # noqa: E402


def test_a_deep_in_the_money_put_is_mostly_intrinsic() -> None:
    # 15 Oct 2026: an AMAT $520 put with AMAT at $427 -- $93 intrinsic of a ~$94 price.
    assert ofl.intrinsic("PUT", 520, 427) == 93
    assert round(ofl.extrinsic_premium(94.0, 2960, "PUT", 520, 427)) == 296_000
    assert ofl.extrinsic_premium(94.0, 2960, "PUT", 520, 427) < 0.02 * 94.0 * 2960 * 100
    assert ofl.intrinsic_share(94.0, "PUT", 520, 427) > ofl.INTRINSIC_HEAVY
    assert ofl.extrinsic_premium(2.0, 1000, "CALL", 450, 427) == 200_000          # all extrinsic out of the money
    assert ofl.extrinsic_premium(2.0, 1000, "CALL", 450, None) is None


def test_the_side_comes_from_a_quote_taken_near_the_print() -> None:
    t = datetime(2026, 10, 15, 18, 0, tzinfo=timezone.utc)
    assert ofl.aggressor(2.10, 2.00, 2.12, t, t + timedelta(seconds=5)) == "buy"
    assert ofl.aggressor(2.01, 2.00, 2.12, t, t + timedelta(seconds=5)) == "sell"
    assert ofl.aggressor(2.06, 2.00, 2.12, t, t) is None                           # at the midpoint
    assert ofl.aggressor(2.10, 2.00, 2.12, t, t + timedelta(minutes=10)) is None   # stale quote
    # A few minutes stale: only prints at or through the quote's edges count.
    assert ofl.aggressor(2.12, 2.00, 2.12, t, t + timedelta(minutes=3)) == "buy"
    assert ofl.aggressor(1.98, 2.00, 2.12, t, t + timedelta(minutes=3)) == "sell"
    assert ofl.aggressor(2.10, 2.00, 2.12, t, t + timedelta(minutes=3)) is None
    assert ofl.aggressor(2.10, 2.20, 2.12, t, t) is None                           # crossed


def test_an_opening_needs_open_interest_up_by_half_the_print() -> None:
    assert ofl.opened(1000, 1600, 1000) is True
    assert ofl.opened(1000, 1200, 1000) is False
    assert ofl.opened(None, 1600, 1000) is None


def _row(tags, kind="CALL", premium=200_000.0):
    return {"type": "options_flow", "source": "alpaca_options", "event_id": "e1",
            "created_at": datetime(2026, 10, 15, 18, tzinfo=timezone.utc), "tags": tags,
            "financial_data": {"ticker": "NVDA", "option_type": kind, "premium_usd": premium, "volume_oi_ratio": 0.1}}


def test_bought_and_opening_prints_are_their_own_families() -> None:
    fams = lambda tags, kind="CALL": [(o.family, o.direction) for o in sf.observations(_row(tags, kind))]  # noqa: E731
    assert fams(["buyer_initiated"]) == [("options_call_buy", 1)]
    assert fams(["buyer_initiated", "intrinsic_heavy"], "PUT") == []
    assert fams(["seller_initiated"]) == []
    assert fams(["opening_confirmed", "buyer_initiated"], "PUT") == [("options_opening_put", -1)]


def test_the_tape_finds_the_quote_that_stood_when_a_delayed_print_happened() -> None:
    from datetime import datetime, timedelta, timezone
    t0 = datetime(2026, 10, 19, 14, 0, tzinfo=timezone.utc)
    tape = ofl.QuoteTape()
    tape.record("X", t0, 2.00, 2.10)
    tape.record("X", t0 + timedelta(minutes=1), 2.40, 2.50)
    tape.record("X", t0 + timedelta(minutes=1), 9.0, 9.1)                 # not newer: ignored
    bid, ask, at = tape.at("X", t0 + timedelta(seconds=20))
    assert (bid, ask, at) == (2.00, 2.10, t0)
    # Fifteen minutes on, the print arrives: read against its own moment, a buy.
    assert ofl.aggressor(2.09, bid, ask, t0 + timedelta(seconds=20), at) == "buy"
    assert tape.at("Y", t0) is None
    tape.prune(t0 + timedelta(minutes=27))
    assert len(tape) == 0
