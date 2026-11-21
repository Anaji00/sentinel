from datetime import date, timedelta

from shared.utils.excess_return import MIN_BETA_RETURNS, beta_excess, beta_to_spy

D0 = date(2025, 1, 1)


def _series(n, step):
    days = [D0 + timedelta(days=i) for i in range(n)]
    closes, c = [], 100.0
    for i in range(n):
        closes.append(c)
        c *= 1 + step(i)
    return days, closes


def test_a_stock_that_moves_twice_the_market_has_beta_two():
    move = lambda i: 0.01 if i % 2 == 0 else -0.006
    sd, sc = _series(200, move)
    d, c = _series(200, lambda i: 2 * move(i))
    # compounding makes it close to, not exactly, 2
    assert abs(beta_to_spy(d, c, sd, sc, D0 + timedelta(days=190)) - 2.0) < 0.05


def test_the_call_day_is_never_in_the_beta():
    sd, sc = _series(200, lambda i: 0.01 if i % 2 == 0 else -0.006)
    d, c = _series(200, lambda i: 0.02 if i % 2 == 0 else -0.012)
    before = D0 + timedelta(days=150)
    c2 = list(c)
    c2[150] *= 5          # a spike on the call day itself
    assert beta_to_spy(d, c, sd, sc, before) == beta_to_spy(d, c2, sd, sc, before)


def test_too_little_history_gives_no_beta():
    sd, sc = _series(MIN_BETA_RETURNS, lambda i: 0.01 if i % 2 == 0 else -0.006)
    d, c = _series(MIN_BETA_RETURNS, lambda i: 0.02 if i % 2 == 0 else -0.012)
    assert beta_to_spy(d, c, sd, sc, D0 + timedelta(days=MIN_BETA_RETURNS + 5)) is None


def test_a_flat_market_gives_no_beta():
    sd, sc = _series(200, lambda i: 0.0)
    d, c = _series(200, lambda i: 0.01 if i % 2 == 0 else -0.006)
    assert beta_to_spy(d, c, sd, sc, D0 + timedelta(days=190)) is None


def test_beta_excess_removes_the_market_move_scaled_by_beta():
    assert beta_excess(-0.10, -0.05, 2.0) == 0.0          # a high-beta short in a falling market earned nothing beyond beta
    assert abs(beta_excess(-0.10, -0.05, 1.0) - (-0.05)) < 1e-12
    assert beta_excess(None, -0.05, 1.0) is None and beta_excess(0.1, 0.1, None) is None
    assert beta_excess(0.1, float("nan"), 1.0) is None
