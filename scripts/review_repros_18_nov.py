"""Read-only reproductions for reports/Senior code review - 18 Nov.md. Pure functions only: no database, Redis, broker or network.

Run from the repository root:  python scripts/review_repros_18_nov.py
"""
import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.utils import auto_trade as at  # noqa: E402
from shared.utils import detached  # noqa: E402
from shared.utils.heartbeat import stall_seconds  # noqa: E402


def liquidity_cap_makes_a_fractional_short() -> None:
    plan = at.Plan("XYZ", 40.0, "sell", "open")                  # a short of 40 whole shares
    capped = at.liquidity_capped(plan, price=25.0, adv_usd=31_000.0, max_participation=0.01)
    print("F-liquidity: 40-share short capped ->", capped.qty, "(fractional short; Alpaca rejects fractional short sells)")


def a_positions_outage_doubles_the_book() -> None:
    target = {"AAA": 100.0, "BBB": -50.0}
    held_ok = {"AAA": 100.0, "BBB": -50.0}
    px = {"AAA": 10.0, "BBB": 20.0}
    print("F-positions: held read correctly ->", [(p.symbol, p.side, p.qty, p.reason) for p in at.plan_orders(target, held_ok, set(held_ok), px)])
    # get_positions() returns [] on any non-200, so `held` is {} and `managed` (from the DB) is still full:
    print("F-positions: positions call answered 503 ->", [(p.symbol, p.side, p.qty, p.reason) for p in at.plan_orders(target, {}, {"AAA", "BBB"}, px)])


async def detached_reports_a_failed_task_as_warming() -> None:
    async def boom():
        raise asyncio.TimeoutError("asyncpg command_timeout (60 s) exceeded")      # what a 60 s pool timeout raises

    try:
        await detached.run("repro:breadth", boom, wait_s=20.0)
    except detached.Warming:
        print("F-detached: a task that FAILED with TimeoutError surfaced as Warming (503 'still computing'), not as an error")
    except Exception as exc:                                                      # pragma: no cover
        print("F-detached: surfaced as", type(exc).__name__)


def stall_hidden_when_the_handler_is_hung() -> None:
    # A handler stuck for 40 minutes on the only message in a sparse topic: the consumer's position already moved past it, so lag is 0.
    print("F-stall: idle 2400 s, lag 0 ->", stall_seconds(2400.0, 0), "(no STALLED line)")


if __name__ == "__main__":
    liquidity_cap_makes_a_fractional_short()
    a_positions_outage_doubles_the_book()
    asyncio.run(detached_reports_a_failed_task_as_warming())
    stall_hidden_when_the_handler_is_hung()
