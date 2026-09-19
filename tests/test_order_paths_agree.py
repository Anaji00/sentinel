"""Two routes reach a broker. They must answer to the same things.

`POST /portfolio/orders` and `POST /trading/orders/execute` both place orders.
The first checked the kill switch, recorded intent in the audit ledger before
execution and the outcome after, and required ADMIN. The second -- the only one
any UI component has ever called -- did none of those, and built its own
`PaperBroker` per request instead of using the process book.

Measured on the deployment before the fix: an order for 10 AAPL returned
`EXECUTIVE_FILLED` with a bracket, and `GET /portfolio/positions` returned zero
positions with cash unchanged at 100,000. The fill went into an object that was
discarded when the response was written, no ledger entry was made, and the
operator was told a trade had executed.
"""
from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
PORTFOLIO = ROOT / "services" / "api_gateway" / "routes" / "portfolio.py"
SCENARIOS = ROOT / "services" / "api_gateway" / "routes" / "scenarios.py"


def _function(path: Path, name: str, *, with_docstring: bool = True) -> str:
    """The source of one function, decorators included.

    `with_docstring=False` drops the docstring, because a docstring explaining
    a defect naturally quotes the defect. The one-book test below asserts that
    `get_execution_broker` no longer contains `return PaperBroker(`, and failed
    on the sentence documenting that it used to -- the sixth time in this audit
    a check has matched its own explanatory prose.
    """
    source = path.read_text(encoding="utf-8")
    lines = source.split("\n")
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
            start = min([node.lineno] + [d.lineno for d in node.decorator_list])
            body = node.body
            if not with_docstring and body and isinstance(body[0], ast.Expr):
                value = body[0].value
                if isinstance(value, ast.Constant) and isinstance(value.value, str):
                    head = lines[start - 1 : body[0].lineno - 1]
                    tail = lines[body[0].end_lineno : node.end_lineno]
                    return "\n".join(head + tail)
            return "\n".join(lines[start - 1 : node.end_lineno])
    raise AssertionError(f"{name} not found in {path.name}")


ORDER_ROUTES = [
    pytest.param(PORTFOLIO, "submit_trade_order", id="portfolio/orders"),
    pytest.param(SCENARIOS, "execute_trade_order", id="trading/orders/execute"),
]


@pytest.mark.parametrize("path, func", ORDER_ROUTES)
def test_every_order_route_honours_the_kill_switch(path: Path, func: str) -> None:
    """An emergency halt that stops one of two order paths stops nothing.

    `order_execution` is described in the flag table as "Halting this stops
    trade execution at the gateway". It did not stop the path a person can
    reach from the UI, because that path never read the flag.
    """
    src = _function(path, func)
    assert "order_execution" in src, f"{func} does not consult the kill switch"
    assert "423" in src, f"{func} must refuse with 423 when execution is halted"


@pytest.mark.parametrize("path, func", ORDER_ROUTES)
def test_every_order_route_records_intent_before_execution(path: Path, func: str) -> None:
    """The UI states that every order is recorded. It has to be true of both.

    An executed-but-unaudited trade is not a recoverable state; a rejected
    order is. So a ledger that cannot record the intent refuses the order.
    """
    src = _function(path, func)
    assert '"phase": "intent"' in src, f"{func} does not record intent"
    assert '"phase": "execution"' in src, f"{func} does not record the outcome"
    assert "AuditLedgerUnavailable" in src, f"{func} does not refuse an unrecordable order"
    # Intent must be written before the venue is reached, not after.
    assert src.index('"phase": "intent"') < src.index("submit_order"), (
        f"{func} reaches the broker before recording intent"
    )


@pytest.mark.parametrize("path, func", ORDER_ROUTES)
def test_every_order_route_requires_the_same_role(path: Path, func: str) -> None:
    """Two routes into one venue answering differently.

    Session cookies resolve to ANALYST by default, so gating order entry at
    ANALYST makes every authenticated session an order-entry principal --
    which is the reasoning already written in portfolio.py, for the identical
    action.
    """
    src = _function(path, func)
    assert "require_role(Role.ADMIN)" in src, f"{func} does not require ADMIN"


def test_the_paper_book_is_one_book() -> None:
    """`scenarios.py` had its own broker factory that rebuilt the book.

    `shared.broker` grew a `_PAPER_BOOK` singleton specifically to stop a fill
    landing in a per-request object. This second factory kept constructing its
    own, so the defect survived on the only order path the UI calls.
    """
    src = SCENARIOS.read_text(encoding="utf-8")
    factory = _function(SCENARIOS, "get_execution_broker", with_docstring=False)

    # The paper branch must delegate, not construct.
    assert "get_broker(" in factory, "the simulated branch must use the shared book"
    assert not re.search(r"return\s+PaperBroker\(", factory), (
        "constructing a PaperBroker here gives this route its own book, and a "
        "fill placed through it is discarded when the request ends"
    )
    # The import has to exist for the delegation to mean anything.
    assert "from shared.broker import get_broker" in src


def test_the_shared_factory_still_holds_one_book() -> None:
    """The singleton the delegation depends on."""
    src = (ROOT / "shared" / "broker" / "__init__.py").read_text(encoding="utf-8")
    assert "_PAPER_BOOK" in src
    assert "def _paper_broker" in src
