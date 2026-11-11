"""Dealer gamma exposure and the gamma flip (shared/utils/gex.py), and its place in the chain summary."""

import math
import pathlib
import sys
from datetime import date

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import gex  # noqa: E402
from shared.utils import option_chain as oc  # noqa: E402


def chain_rows(call_oi=1000.0, put_oi=1000.0, spot=100.0, n=24):
    """(right, strike, open interest, iv, years) around the spot: calls above and puts below it."""
    rows = []
    for i in range(n):
        k = spot * (0.85 + 0.3 * i / (n - 1))
        rows.append(("C", k, call_oi, 0.25, 30 / 365))
        rows.append(("P", k, put_oi, 0.25, 30 / 365))
    return rows


def test_bs_gamma_is_positive_peaks_at_the_money_and_refuses_bad_inputs() -> None:
    atm, otm = gex.bs_gamma(100, 100, 0.1, 0.25), gex.bs_gamma(100, 130, 0.1, 0.25)
    assert atm and otm and atm > otm > 0
    for bad in ((0, 100, .1, .2), (100, 100, 0, .2), (100, 100, .1, 0), (float("nan"), 100, .1, .2), (100, -1, .1, .2)):
        assert gex.bs_gamma(*bad) is None


def test_calls_add_and_puts_subtract_and_a_thin_chain_gives_nothing() -> None:
    rows = [("C", 1000.0, 0.02)] * 25
    assert gex.net_gex(100.0, rows) == 25 * 0.02 * 1000 * 100 * 100 * 100 * 0.01
    assert gex.net_gex(100.0, [("P", 1000.0, 0.02)] * 25) < 0
    assert gex.net_gex(100.0, [("C", 1000.0, 0.02)] * 5) is None               # fewer than MIN_CONTRACTS
    assert gex.net_gex(0.0, rows) is None and gex.net_gex(float("nan"), rows) is None
    assert gex.net_gex(100.0, [("C", 0.0, 0.02)] * 30 + [("C", 10.0, None)] * 30) is None   # no open interest, no gamma


def test_the_flip_is_where_the_sign_changes_and_absent_when_it_never_does() -> None:
    # Heavy put open interest below the money, heavy call open interest above: the sign flips near the middle.
    rows = [("P", 90.0 + i, 5000.0, 0.25, 30 / 365) for i in range(10)] + [("C", 100.0 + i, 5000.0, 0.25, 30 / 365) for i in range(10)]
    flip = gex.gamma_flip(100.0, rows)
    assert flip is not None and 90.0 < flip < 110.0
    only_calls = [("C", 90.0 + i, 5000.0, 0.25, 30 / 365) for i in range(25)]
    assert gex.gamma_flip(100.0, only_calls) is None
    assert gex.gamma_flip(100.0, rows[:5]) is None and gex.gamma_flip(0.0, rows) is None


def test_regime_uses_the_flip_when_there_is_one_and_the_sign_otherwise() -> None:
    assert gex.regime(5e9, 101, 100) == "positive" and gex.regime(-5e9, 99, 100) == "negative"
    assert gex.regime(5e9, 99, 100) == "negative"                # the flip says spot is below it
    assert gex.regime(-1.0, None, None) == "negative" and gex.regime(None, 100, 100) is None


def _cboe(spot=100.0):
    opts = []
    for i in range(30):
        k = 85 + i
        for right, letter in (("C", "C"), ("P", "P")):
            opts.append({"option": f"SPY261218{letter}{int(k * 1000):08d}", "volume": 10, "open_interest": 500 if right == "C" else 900,
                         "delta": 0.5 if right == "C" else -0.5, "iv": 0.2, "gamma": 0.03})
    return {"data": {"options": opts, "current_price": spot, "iv30": 20.0}}


def test_the_chain_summary_carries_net_gex_and_never_invents_it() -> None:
    s = oc.summarize(_cboe(), date(2026, 11, 20))
    assert s is not None and "net_gex" in s and "gamma_flip" in s and s["net_gex"] is not None and s["net_gex"] < 0   # more put OI
    thin = oc.summarize({"data": {"options": _cboe()["data"]["options"][:6], "current_price": 100.0}}, date(2026, 11, 20))
    assert thin is not None and thin["net_gex"] is None and thin["gamma_flip"] is None
    assert not math.isnan(s["net_gex"])


def test_the_collector_keeps_a_history_for_the_index_etfs_only() -> None:
    src = (ROOT / "services/collector-tradfi/cboe_chains.py").read_text(encoding="utf-8")
    assert 'GEX_HISTORY = ("SPY", "QQQ", "IWM")' in src and "raw.expire(key, 450 * 86400)" in src
