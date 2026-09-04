"""
tests/test_counterparty_selection.py

Every transfer took its primary entity from the receiving address, so the
busiest addresses on the chain became the busiest entities in the system. In one
24-hour window the top ten addresses carried 40.4% of all transfers, led by an
exchange hot wallet with 19,251 of them, and the zero address was named as the
actor 1,391 times.

There was a loop underneath it. Receiving over $5M added an address to the
watched set; an exchange hot wallet clears that every few minutes; and a watched
counterparty made every subsequent transfer to it an alert. The system promoted
the addresses it should ignore, then alerted on the traffic it had promoted them
for.

Infrastructure is measured here rather than listed. Addresses change, lists rot,
and an address that transacts with thousands of distinct counterparties is a
venue whatever anyone calls it.
"""

import asyncio
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils.counterparty import (  # noqa: E402
    INFRASTRUCTURE_DEGREE, choose_primary, counterparty_degree,
    is_infrastructure, is_null_address, note_counterparty,
)

ZERO = "0x0000000000000000000000000000000000000000"
DEAD = "0x000000000000000000000000000000000000dEaD"
HOT_WALLET = "0x28c6c06298d514db089934071355e5743bf21d60"
POOL_MANAGER = "0x000000000004444c5dc75cb358380d2e3de08a90"
ACTOR = "0x8f10b468b06c6fd214b65f87778827f7d113f996"


class _FakeRaw:
    def __init__(self):
        self.hll = {}
        self.kv = {}

    async def pfadd(self, key, value):
        self.hll.setdefault(key, set()).add(value)

    async def pfcount(self, key):
        return len(self.hll.get(key, set()))

    async def expire(self, key, ttl):
        return True

    # note_counterparty only starts a HyperLogLog on an address's SECOND
    # sighting -- one was previously created for every address the chain ever
    # mentioned, 62,281 keys and 60.7% of the Redis instance. The promotion is
    # driven by exists/set-NX/delete on a one-byte marker, and this double
    # implemented none of them, so every call raised AttributeError into
    # note_counterparty's own debug-level except and pfadd never ran. The
    # degree stayed 0 and the failures read as "this wallet is not
    # infrastructure" rather than "the fake is missing three commands".
    async def exists(self, key):
        return 1 if key in self.kv or key in self.hll else 0

    async def set(self, key, value, ex=None, nx=False):
        if nx and key in self.kv:
            return None          # redis-py returns None when NX does not take
        self.kv[key] = value
        return True

    async def delete(self, key):
        return 1 if self.kv.pop(key, None) is not None else 0


class _FakeRedis:
    def __init__(self):
        self.raw = _FakeRaw()


async def _make_venue(redis, address, degree=INFRASTRUCTURE_DEGREE):
    """Give an address `degree` recorded counterparties.

    One more sighting than that is needed to produce them. The first is spent
    on the promotion marker and recorded nowhere -- an address seen exactly
    once cannot be a venue, which is the whole point of deferring the
    HyperLogLog -- so seeding N sightings registers N-1 counterparties and
    seeding the threshold exactly would leave a venue one short of it.
    """
    for i in range(degree + 1):
        await note_counterparty(redis, address, f"0x{i:040x}")


# -- null addresses are protocol constants, not guesses -------------------------

def test_the_zero_address_is_not_an_entity():
    assert is_null_address(ZERO)


def test_the_dead_address_is_not_an_entity():
    """Leading zeros then 'dead' is the other provable burn sink in use."""
    assert is_null_address(DEAD)


def test_an_address_that_merely_starts_with_zeros_is_an_entity():
    """Uniswap V4's pool manager has a vanity prefix of zeros. Matching on the
    prefix rather than the whole body would have swallowed it."""
    assert not is_null_address(POOL_MANAGER)


def test_ordinary_addresses_are_entities():
    assert not is_null_address(HOT_WALLET)
    assert not is_null_address(ACTOR)


def test_malformed_input_is_not_a_null_address():
    for value in ("", None, "not-an-address", "0x123", 12345):
        assert not is_null_address(value)


# -- infrastructure is measured, not listed ------------------------------------

def test_a_fresh_address_is_not_infrastructure():
    async def run():
        redis = _FakeRedis()
        assert not await is_infrastructure(redis, ACTOR)

    asyncio.run(run())


def test_an_address_with_many_counterparties_is_infrastructure():
    async def run():
        redis = _FakeRedis()
        await _make_venue(redis, HOT_WALLET)
        assert await is_infrastructure(redis, HOT_WALLET)

    asyncio.run(run())


def test_a_participant_stays_a_participant():
    """A wallet with a handful of counterparties is an actor, however much
    value moves through it."""
    async def run():
        redis = _FakeRedis()
        # 13 sightings, 12 recorded: the first is spent on the promotion
        # marker. See _make_venue.
        for i in range(13):
            await note_counterparty(redis, ACTOR, f"0x{i:040x}")
        assert not await is_infrastructure(redis, ACTOR)
        assert await counterparty_degree(redis, ACTOR) == 12

    asyncio.run(run())


def test_the_null_address_is_infrastructure_without_any_history():
    async def run():
        redis = _FakeRedis()
        assert await is_infrastructure(redis, ZERO)

    asyncio.run(run())


def test_self_transfers_are_not_counted_as_counterparties():
    async def run():
        redis = _FakeRedis()
        await note_counterparty(redis, ACTOR, ACTOR)
        assert await counterparty_degree(redis, ACTOR) == 0

    asyncio.run(run())


# -- picking the actor ---------------------------------------------------------

def test_a_deposit_to_an_exchange_attributes_to_the_depositor():
    """The defect verbatim: 19,251 events named the exchange."""
    async def run():
        redis = _FakeRedis()
        await _make_venue(redis, HOT_WALLET)
        primary, both_infra = await choose_primary(redis, ACTOR, HOT_WALLET)
        assert primary == ACTOR
        assert both_infra is False

    asyncio.run(run())


def test_a_withdrawal_from_an_exchange_attributes_to_the_recipient():
    async def run():
        redis = _FakeRedis()
        await _make_venue(redis, HOT_WALLET)
        primary, _ = await choose_primary(redis, HOT_WALLET, ACTOR)
        assert primary == ACTOR

    asyncio.run(run())


def test_a_burn_attributes_to_the_sender():
    async def run():
        redis = _FakeRedis()
        primary, both_infra = await choose_primary(redis, ACTOR, ZERO)
        assert primary == ACTOR
        assert both_infra is False

    asyncio.run(run())


def test_a_mint_attributes_to_the_receiver():
    async def run():
        redis = _FakeRedis()
        primary, _ = await choose_primary(redis, ZERO, ACTOR)
        assert primary == ACTOR

    asyncio.run(run())


def test_venue_to_venue_flow_is_flagged_rather_than_given_a_false_actor():
    """An exchange rebalancing to itself is plumbing. Naming one side as the
    actor would be inventing a subject for a sentence that has none."""
    async def run():
        redis = _FakeRedis()
        await _make_venue(redis, HOT_WALLET)
        await _make_venue(redis, POOL_MANAGER)
        _, both_infra = await choose_primary(redis, HOT_WALLET, POOL_MANAGER)
        assert both_infra is True

    asyncio.run(run())


def test_an_unknown_sender_still_resolves_to_the_receiver():
    async def run():
        redis = _FakeRedis()
        primary, _ = await choose_primary(redis, "UNKNOWN", ACTOR)
        assert primary == ACTOR

    asyncio.run(run())
