"""The paper trading book, and the risk figures computed from it.

Two surfaces this audit had not opened. What was in them:

  * `get_broker()` built a new `PaperBroker` on every call, and every route
    calls it per request -- so an order filled into an object that was then
    dropped. The book has never held a position for longer than one HTTP
    request, while the audit ledger faithfully recorded each order.
  * A position's `current_price` was written once, at fill, and never again, so
    `unrealized_pl` was structurally zero and `portfolio_value` was the sum of
    entry costs.
  * That stale mark was also consulted *before* the quote cache when pricing a
    fill, so every order after the first in a given symbol executed at the
    first fill's price.
  * `/portfolio/risk` published VaR and CVaR from a hardcoded 1.8% daily
    volatility and a flat 1.05 beta, wrapped in a provenance envelope declaring
    `COMPUTED_DETERMINISTIC`, `is_synthetic=False`, and a model called
    `deterministic_risk_engine`.
"""
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.broker import OrderSide, OrderType, get_broker, reset_paper_book  # noqa: E402
from shared.utils.quote_cache import quote_key  # noqa: E402
from shared.utils.volatility import REALISED_VOL_KEY  # noqa: E402

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


class _Raw:
    def __init__(self, values):
        self.values = values

    async def get(self, key):
        return self.values.get(key)


class _Redis:
    """Only what these code paths touch: `.raw.get`."""

    def __init__(self, **values):
        self.raw = _Raw(values)


def _quotes(**prices):
    """The format the writers actually use: a bare number.

    This fixture wrote `json.dumps({"price": px})`, an object no writer in the
    tree produces -- every one of the six stores `str(price)` or the float
    itself. So these tests passed against a cache shape that does not exist,
    and the mark-to-market they covered could not have worked in production.
    Inventing a fixture is the same failure as trusting a docstring.
    """
    return {quote_key(sym): str(px) for sym, px in prices.items()}


@pytest.fixture(autouse=True)
def fresh_book():
    reset_paper_book()
    yield
    reset_paper_book()


# -- the book exists between requests ---------------------------------------


async def test_the_book_survives_the_request_that_created_it():
    """Every route builds its broker per request, and each one was new.

    `POST /portfolio/orders` filled into a fresh `PaperBroker` with 100,000 in
    cash, updated that instance's dict, and dropped it. The next request built
    another empty one, so `GET /portfolio/positions` returned nothing,
    `/account` reported 100,000 and zero positions whatever had been traded,
    and cancelling an order could never find it.
    """
    redis = _Redis(**_quotes(NVDA=100.0))
    placing = get_broker(redis_client=redis)
    await placing.submit_order("NVDA", 10, OrderSide.BUY, OrderType.MARKET)

    reading = get_broker(redis_client=redis)
    assert reading is placing, "a second call built a second, empty book"
    positions = await reading.get_positions()
    assert [p.symbol for p in positions] == ["NVDA"]
    assert positions[0].qty == 10


async def test_the_account_reflects_what_was_traded():
    redis = _Redis(**_quotes(NVDA=100.0))
    broker = get_broker(redis_client=redis)
    before = await broker.get_account()
    await broker.submit_order("NVDA", 10, OrderSide.BUY, OrderType.MARKET)
    after = await get_broker(redis_client=redis).get_account()

    assert before.positions_count == 0
    assert after.positions_count == 1
    # Cash fell by the cost of the fill; equity is roughly unchanged, less
    # the slippage the simulator charges.
    assert after.cash < before.cash
    assert after.portfolio_value == pytest.approx(before.portfolio_value, rel=0.01)


# -- positions are worth what they are worth now ----------------------------


async def test_a_position_is_marked_to_the_current_quote():
    """`current_price` was written at fill and never again.

    So `market_value` was the cost of the position, `unrealized_pl` was
    structurally zero, and a paper book could not show a gain or a loss however
    the market moved -- which is the entire content of paper trading.
    """
    redis = _Redis(**_quotes(NVDA=100.0))
    broker = get_broker(redis_client=redis)
    await broker.submit_order("NVDA", 10, OrderSide.BUY, OrderType.MARKET)

    # The market moves 20% while the book is held.
    redis.raw.values.update(_quotes(NVDA=120.0))
    position = (await broker.get_positions())[0]

    assert position.current_price == 120.0
    assert position.market_value == pytest.approx(1200.0)
    assert position.unrealized_pl > 190.0
    assert position.unrealized_pl_pct > 19.0


async def test_a_symbol_with_no_quote_keeps_its_last_mark():
    """An absent quote is not a price of zero."""
    redis = _Redis(**_quotes(NVDA=100.0))
    broker = get_broker(redis_client=redis)
    await broker.submit_order("NVDA", 10, OrderSide.BUY, OrderType.MARKET)
    entry_mark = (await broker.get_positions())[0].current_price

    redis.raw.values.clear()
    position = (await broker.get_positions())[0]
    assert position.current_price == entry_mark
    assert position.market_value > 0.0


# -- and fills price from the market ----------------------------------------


async def test_a_second_order_prices_from_the_market_not_the_first_fill():
    """The position's own mark was consulted before the quote cache.

    `limit_price or estimated_market_price or existing_pos.current_price`, with
    the cache read only if all three were absent -- so for a symbol already
    held the third term always answered, and with `current_price` frozen at
    fill time every later order in that symbol executed at the first fill's
    price for the life of the process.
    """
    redis = _Redis(**_quotes(NVDA=100.0))
    broker = get_broker(redis_client=redis)
    first = await broker.submit_order("NVDA", 10, OrderSide.BUY, OrderType.MARKET)

    redis.raw.values.update(_quotes(NVDA=200.0))
    second = await broker.submit_order("NVDA", 10, OrderSide.BUY, OrderType.MARKET)

    assert first.filled_avg_price == pytest.approx(100.05, abs=0.1)
    assert second.filled_avg_price == pytest.approx(200.1, abs=0.2), (
        "the second fill took the first fill's price"
    )


async def test_a_limit_price_still_wins():
    """The order's own price is the order's own price."""
    redis = _Redis(**_quotes(NVDA=100.0))
    broker = get_broker(redis_client=redis)
    order = await broker.submit_order(
        "NVDA", 5, OrderSide.BUY, OrderType.LIMIT, limit_price=90.0
    )
    assert order.filled_avg_price == pytest.approx(90.045, abs=0.05)


def test_the_module_no_longer_claims_a_persistence_it_does_not_have():
    """"position persistence", "Redis-backed", and three unused key constants.

    None of REDIS_PAPER_POSITIONS / _ACCOUNT / _ORDERS was ever read or
    written. They are gone rather than wired up -- and not because Redis would
    drop them: this deployment runs volatile-lru, which evicts only keys
    carrying a TTL, and a position would carry none. A book wants a schema and
    a constraint that two fills cannot race, which is what Postgres has and a
    blob in a cache does not.
    """
    source = (ROOT / "shared" / "broker" / "paper.py").read_text(encoding="utf-8")
    assert 'REDIS_PAPER_POSITIONS = "sentinel:paper:positions"' not in source
    assert "position persistence," not in source
    assert "process-local" in source


# -- the risk figures -------------------------------------------------------


async def _risk(redis):
    from services.api_gateway.routes.portfolio import get_portfolio_risk_metrics

    return await get_portfolio_risk_metrics(redis=redis)


async def test_beta_is_absent_rather_than_one_point_zero_five():
    """It was a constant, printed beside the computed figures.

    The endpoint's docstring promised "Portfolio Beta relative to SPY" and the
    platform holds no per-position return series against SPY at this layer, so
    nothing could have been regressed.
    """
    redis = _Redis(**_quotes(NVDA=100.0))
    await get_broker(redis_client=redis).submit_order(
        "NVDA", 10, OrderSide.BUY, OrderType.MARKET
    )
    out = await _risk(redis)
    assert out["portfolio_beta"] is None
    assert out["positions_count"] == 1


async def test_var_rests_on_a_measured_volatility_when_there_is_one():
    """0.018 was the number, on a platform that measures volatility hourly."""
    values = _quotes(NVDA=100.0)
    # 31.75% annualised is 2% daily.
    values[REALISED_VOL_KEY] = "31.7490"
    redis = _Redis(**values)
    await get_broker(redis_client=redis).submit_order(
        "NVDA", 10, OrderSide.BUY, OrderType.MARKET
    )
    out = await _risk(redis)

    assert out["daily_volatility_is_measured"] is True
    assert out["daily_volatility_used"] == pytest.approx(0.02, abs=0.0005)
    assert out["provenance"]["source_type"] == "computed_deterministic"
    assert out["provenance"]["is_synthetic"] is False
    assert "realised volatility" in out["provenance"]["methodology"]


async def test_an_unmeasured_volatility_is_declared_and_not_dressed_up():
    """The old envelope said COMPUTED_DETERMINISTIC and is_synthetic=False for
    a figure that was a constant times a weight norm."""
    redis = _Redis(**_quotes(NVDA=100.0))
    await get_broker(redis_client=redis).submit_order(
        "NVDA", 10, OrderSide.BUY, OrderType.MARKET
    )
    out = await _risk(redis)

    assert out["daily_volatility_is_measured"] is False
    assert out["provenance"]["source_type"] == "disclosed_placeholder"
    assert out["provenance"]["is_synthetic"] is True
    assert "assumption" in out["provenance"]["methodology"]


async def test_the_sector_breakdown_the_docstring_promised_is_returned():
    """It was returned only by the branch with no positions, as {}."""
    values = _quotes(NVDA=100.0)
    values["sentinel:refdata:NVDA"] = json.dumps({"symbol": "NVDA", "sector": "Semiconductors"})
    redis = _Redis(**values)
    await get_broker(redis_client=redis).submit_order(
        "NVDA", 10, OrderSide.BUY, OrderType.MARKET
    )
    out = await _risk(redis)

    assert out["sector_exposure"].get("Semiconductors", 0.0) > 0.0


async def test_a_position_with_no_reference_data_is_unknown_not_omitted():
    redis = _Redis(**_quotes(NVDA=100.0))
    await get_broker(redis_client=redis).submit_order(
        "NVDA", 10, OrderSide.BUY, OrderType.MARKET
    )
    out = await _risk(redis)
    assert "UNKNOWN" in out["sector_exposure"]


def test_the_per_entity_volatility_store_still_has_no_writer():
    """Recorded, not fixed. `_compute_ewma_volatility` has no callers.

    It writes `sentinel:ml:ewma_var:{entity}`, which is why /portfolio/risk
    cannot use a per-position volatility and falls back to the market-wide
    realised measurement. If a caller appears, the risk endpoint should weight
    positions by their own volatility rather than one number for all of them.
    """
    scorer = (ROOT / "services" / "enrichment" / "anomaly_scorer.py").read_text(
        encoding="utf-8"
    )
    assert scorer.count("_compute_ewma_volatility") == 1, (
        "the per-entity volatility now has a caller; /portfolio/risk should use it"
    )


def test_the_fixture_above_matches_what_the_writers_store():
    """Pinned, because it was wrong and nothing said so.

    If a writer ever starts storing an object, this fails and the fixture is
    updated deliberately rather than the two drifting apart again.
    """
    import re

    writers = []
    for path in (ROOT / "services").rglob("*.py"):
        if "__pycache__" in path.parts:
            continue
        text = path.read_text(encoding="utf-8", errors="replace")
        for match in re.finditer(r"set\(\s*quote_key\([^)]*\),\s*([^,)]+)", text):
            writers.append((path.name, match.group(1).strip()))

    assert writers, "no quote-cache writer found; this test has gone stale"
    for name, written in writers:
        assert "json.dumps" not in written, (
            f"{name} now writes an object to the quote cache: {written}. "
            f"parse_quote handles both, but the fixtures above assume a number."
        )


def test_the_parser_reads_what_the_writers_write():
    from shared.utils.quote_cache import parse_quote

    assert parse_quote("93.23") == 93.23
    assert parse_quote(b"93.23") == 93.23
    assert parse_quote(str(41.0)) == 41.0
    # An object still works, in case a writer ever produces one.
    assert parse_quote('{"price": 12.5}') == 12.5
    # And absence stays absence rather than becoming zero.
    assert parse_quote(None) is None
    assert parse_quote("0") is None
    assert parse_quote("not a price") is None
