"""/explain/signal must explain the signal, not a worked example.

The endpoint returned a fixture. Price 128.50, ATR 3.20, stop 123.70, RSI 58.4,
an `empirical_win_rate_W` of 0.62, sector "Information Technology", a supply
chain of TSM and ASML, and three feature flags reported as strings -- identical
for every signal_id, for every ticker, under the key
"deterministic_math_audit".

An explainability surface is the one place a fabricated number does the most
damage, because it is where someone goes to check.
"""
import pytest
from fastapi import HTTPException

from services.api_gateway.routes import explain as mod

FIXTURE_VALUES = {128.50, 3.20, 4.80, 123.70, 138.10, 0.62, 43.0, 21.5, 58.4, 127.20, 124.80, 122.40, 14.2}


class _DB:
    def __init__(self, bars, settled=0, wins=0):
        self._bars, self._settled, self._wins = bars, settled, wins

    async def query(self, sql, *args):
        if "tradfi_bars" in sql:
            return self._bars
        return [{"wins": float(self._wins), "settled": float(self._settled)}]


def _bars(n=250, start=50.0):
    import datetime as dt

    t0 = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
    out = []
    for i in range(n):
        c = start + i * 0.11
        out.append({
            "time": t0 + dt.timedelta(minutes=i),
            "open": c - 0.05, "high": c + 0.4, "low": c - 0.4, "close": c, "volume": 1000.0,
        })
    return list(reversed(out))          # the route asks for DESC


@pytest.mark.anyio
async def test_no_history_is_a_404_not_a_worked_example():
    with pytest.raises(HTTPException) as e:
        await mod.explain_trading_signal("signal_ZZNOPE", redis=None, db=_DB([]))
    assert e.value.status_code == 404
    assert "ZZNOPE" in e.value.detail


@pytest.mark.anyio
async def test_no_database_is_a_503_not_a_worked_example():
    with pytest.raises(HTTPException) as e:
        await mod.explain_trading_signal("signal_AAPL", redis=None, db=None)
    assert e.value.status_code == 503


@pytest.mark.anyio
async def test_the_numbers_come_from_the_bars():
    out = await mod.explain_trading_signal("signal_AAPL", redis=None, db=_DB(_bars()))
    audit = out["deterministic_math_audit"]

    assert out["bars_used"] == 250
    # Last close of the synthetic series, oldest-first: 50.0 + 249*0.11
    assert audit["current_price"] == pytest.approx(50.0 + 249 * 0.11, abs=1e-6)
    assert audit["calculated_stop_loss"] < audit["current_price"]
    assert audit["calculated_target_price"] > audit["current_price"]
    # Stop and target are the ATR arithmetic the formulas claim.
    assert audit["stop_distance"] == pytest.approx(1.5 * audit["atr_14"], abs=1e-6)


@pytest.mark.anyio
async def test_none_of_the_fixture_values_survive():
    """A different instrument must not produce the old constants."""
    out = await mod.explain_trading_signal("signal_AAPL", redis=None, db=_DB(_bars()))
    audit = out["deterministic_math_audit"]
    seen = {v for v in audit.values() if isinstance(v, (int, float))}
    seen |= {v for v in out["technical_indicator_inputs"].values() if isinstance(v, (int, float))}
    assert not (seen & FIXTURE_VALUES), sorted(seen & FIXTURE_VALUES)


@pytest.mark.anyio
async def test_an_unobserved_win_rate_is_none_not_a_number():
    """0.62 was labelled empirical and had never been measured."""
    out = await mod.explain_trading_signal("signal_AAPL", redis=None, db=_DB(_bars(), settled=3, wins=2))
    kelly = out["deterministic_math_audit"]["half_kelly_inputs"]
    assert kelly["empirical_win_rate_W"] is None
    assert kelly["half_kelly_clamped_pct"] is None
    assert "placeholder" in kelly["note"]


@pytest.mark.anyio
async def test_an_observed_win_rate_produces_a_kelly_fraction():
    out = await mod.explain_trading_signal(
        "signal_AAPL", redis=None, db=_DB(_bars(), settled=100, wins=62)
    )
    kelly = out["deterministic_math_audit"]["half_kelly_inputs"]
    assert kelly["empirical_win_rate_W"] == pytest.approx(0.62)
    assert 0.0 <= kelly["half_kelly_clamped_pct"] <= 25.0


@pytest.mark.anyio
async def test_reference_data_is_absent_rather_than_invented():
    """Every signal claimed Information Technology, TSM and ASML."""
    out = await mod.explain_trading_signal("signal_AAPL", redis=None, db=_DB(_bars()))
    graph = out["graph_topology_precheck"]
    assert graph["sector"] is None
    assert graph["supply_chain_dependencies"] == []
    assert "TSM" not in str(graph)


@pytest.mark.anyio
async def test_feature_flags_are_unknown_rather_than_enabled_when_unreadable():
    out = await mod.explain_trading_signal("signal_AAPL", redis=None, db=_DB(_bars()))
    assert set(out["feature_flag_status"].values()) == {"UNKNOWN"}
