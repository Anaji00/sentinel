"""Quant report T7 (read-only exposure of the stored book): shared/utils/book_exposure.py and its route."""
import pathlib
from datetime import date, timedelta

from shared.utils import book_exposure as be

D0 = date(2026, 1, 1)


def _closes(rets, start=100.0):
    out, p = {}, start
    out[D0] = p
    for i, r in enumerate(rets, 1):
        p *= 1 + r
        out[D0 + timedelta(days=i)] = p
    return out


def _spy_rets(n=120):
    return [0.01 if i % 2 else -0.008 for i in range(n)]


def test_beta_is_recovered_and_net_beta_is_long_minus_short() -> None:
    spy = _spy_rets()
    bars = {"SPY": _closes(spy), "L1": _closes([2 * r for r in spy]), "S1": _closes([0.5 * r for r in spy])}
    out = be.exposure({"long": ["L1"], "short": ["S1"], "rebalanced_on": "2026-11-02"}, bars, {"L1": "Tech", "S1": "Energy"})
    assert abs(out["long"]["mean_beta"] - 2.0) < 1e-3 and abs(out["short"]["mean_beta"] - 0.5) < 1e-3
    assert out["net_beta"] == 0.75 and out["rebalanced_on"] == "2026-11-02"
    assert out["top_sectors"]["long"] == [("Tech", 1)] and out["top_sectors"]["short"] == [("Energy", 1)]


def test_short_history_or_missing_names_are_unmeasured_not_zero() -> None:
    spy = _spy_rets()
    bars = {"SPY": _closes(spy), "L1": _closes(spy[:20])}
    out = be.exposure({"long": ["L1", "GONE"], "short": []}, bars, {})
    assert out["long"]["measured"] == 0 and out["long"]["mean_beta"] is None and out["net_beta"] is None
    assert out["long"]["names"] == 2


def test_the_route_is_read_only_and_cached() -> None:
    src = (pathlib.Path(__file__).resolve().parents[1] / "services/api_gateway/routes/portfolio.py").read_text(encoding="utf-8")
    i = src.index('"/b1-exposure"')
    body = src[i:src.index('"/positions"', i)]
    assert "cached_json" in body and "submit" not in body and "execute(" not in body
