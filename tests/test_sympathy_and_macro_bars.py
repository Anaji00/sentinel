"""Measured sympathy peers (shared/utils/sympathy.py) and macro daily bars
(shared/utils/macro_bars.py)."""
import math
import random
from datetime import date, datetime, timedelta, timezone

from shared.utils import macro_bars as mb
from shared.utils import sympathy as sy


def _walk(days, rets, start=100.0):
    out, p = [], start
    for d, r in zip(days, rets):
        p *= math.exp(r)
        out.append((d, p))
    return out


def test_peers_are_the_stocks_that_move_together_net_of_the_market():
    rnd = random.Random(7)
    days = [date(2026, 1, 1) + timedelta(days=i) for i in range(160)]
    mkt = [rnd.gauss(0, 0.01) for _ in days]
    ind = [rnd.gauss(0, 0.01) for _ in days]                     # a shared industry factor
    def stock(beta_ind):
        return _walk(days, [m + beta_ind * f + rnd.gauss(0, 0.005) for m, f in zip(mkt, ind)])
    closes = {"AAA": stock(1.0), "BBB": stock(1.0), "CCC": stock(0.0)}
    rows = sy.top_peers(sy.excess_returns(closes, _walk(days, mkt)))
    peers = {(t, p) for t, p, _, _ in rows}
    assert ("AAA", "BBB") in peers and ("BBB", "AAA") in peers
    assert not any("CCC" in (t, p) for t, p, _, _ in rows)      # the market alone is not sympathy
    assert all(n >= sy.MIN_OVERLAP and c >= sy.MIN_CORR for _, _, c, n in rows)


def test_too_little_overlap_is_no_peer():
    days = [date(2026, 1, 1) + timedelta(days=i) for i in range(160)]
    rets = [0.01 * math.sin(i) for i in range(160)]
    closes = {"AAA": _walk(days, rets), "NEW": _walk(days[-30:], rets[-30:])}
    assert sy.top_peers(sy.excess_returns(closes, _walk(days, [0.0] * 160))) == []


def test_yahoo_daily_bars_use_the_exchange_date():
    ts = int(datetime(2026, 10, 29, 4, 0, tzinfo=timezone.utc).timestamp())   # 00:00 New York
    chart = {"chart": {"result": [{"meta": {"gmtoffset": -14400}, "timestamp": [ts, ts + 86400],
                                   "indicators": {"quote": [{"close": [71.2, None]}]}}]}}
    assert mb.yahoo_rows("CL=F", chart) == [("CL=F", date(2026, 10, 29), 71.2, "yahoo")]
    assert mb.yahoo_rows("CL=F", {"chart": {"result": None}}) == []


def test_treasury_yields_are_stored_under_every_spelling():
    xml = ('<feed xmlns="http://www.w3.org/2005/Atom" '
           'xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata" '
           'xmlns:d="http://schemas.microsoft.com/ado/2007/08/dataservices"><entry><content><m:properties>'
           '<d:NEW_DATE>2026-10-29T00:00:00</d:NEW_DATE><d:BC_2YEAR>3.61</d:BC_2YEAR>'
           '<d:BC_10YEAR>4.12</d:BC_10YEAR></m:properties></content></entry></feed>')
    rows = {(s, v) for s, _, v, _ in mb.treasury_rows(xml)}
    assert rows == {("US02Y", 3.61), ("US2Y", 3.61), ("US10Y", 4.12)}


def test_backfill_years():
    assert mb.years(date(2026, 10, 30), True)[0] == mb.FIRST_YEAR
    assert mb.years(date(2026, 10, 30), False) == [2026]
    assert mb.years(date(2026, 1, 3), False) == [2025, 2026]


def test_exposures_find_an_oil_stock_and_control_for_the_market():
    import pandas as pd
    from shared.utils import macro_exposure as mx
    rnd = random.Random(3)
    days = [date(2025, 1, 1) + timedelta(days=i) for i in range(300)]
    mkt = pd.Series([rnd.gauss(0, 0.01) for _ in days], index=days)
    oil = pd.Series([rnd.gauss(0, 0.02) for _ in days], index=days)
    stocks = {"OILCO": mkt + 0.5 * oil + pd.Series([rnd.gauss(0, 0.005) for _ in days], index=days),
              "INDEX": mkt * 1.0 + pd.Series([rnd.gauss(0, 0.005) for _ in days], index=days)}
    rows = mx.exposures(stocks, mkt, {"CL=F": oil})
    assert [(t, m) for t, m, *_ in rows] == [("OILCO", "CL=F")]          # the market alone is no exposure
    t, m, beta, tstat, pc, n = rows[0]
    assert abs(beta - 0.5) < 0.05 and tstat > 3 and 0 < pc < 1 and n == mx.WINDOW


def test_yield_changes_are_in_basis_points():
    from shared.utils import macro_exposure as mx
    s = mx.changes("US10Y", [(date(2026, 1, 1), 4.00), (date(2026, 1, 2), 4.05)])
    assert abs(float(s.iloc[0]) - 5.0) < 1e-9
    r = mx.changes("CL=F", [(date(2026, 1, 1), 70.0), (date(2026, 1, 2), 77.0)])
    assert abs(float(r.iloc[0]) - math.log(1.1)) < 1e-9
