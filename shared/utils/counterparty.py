"""
shared/utils/counterparty.py

Decides which side of an on-chain transfer is an actor.

Every transfer event took its primary entity from the receiving address, which
makes the busiest addresses on the chain the busiest entities in the system. In
a 24-hour window the top ten addresses accounted for 40.4% of all transfers, led
by an exchange hot wallet with 19,251 of them. "Somebody deposited to Binance"
is not an observation about anybody.

Two kinds of address are not counterparties:

  Null        The zero address and the 0x...dead family. A transfer here is a
              burn, and a transfer *from* the zero address is a mint. Both are
              token supply mechanics; the counterparty is the other side. 1,391
              events a day named the zero address as their actor.

  Infrastructure
              Exchange hot wallets, routers, pool managers, bridges. These have
              no fixed list worth maintaining and the addresses change, so this
              module does not carry one. It measures instead: an address that
              transacts with thousands of distinct counterparties is a venue,
              whatever it is called, and one that transacts with a handful is a
              participant. Counted in a HyperLogLog, which is 12KB per address
              regardless of traffic.

The feedback loop this breaks is worth stating. Receiving over $5M added an
address to the watched set, exchange hot wallets clear that every few minutes,
and a watched counterparty made every subsequent transfer to them an alert --
so the system promoted the addresses it should ignore, then alerted on the
traffic it had promoted them for.
"""

import logging
import re
from typing import Optional, Tuple

logger = logging.getLogger("shared.counterparty")

# Distinct counterparties above which an address is a venue rather than an
# actor. Ordinary wallets do not transact with a thousand distinct addresses;
# the hot wallet at the top of this system's distribution reached 19,251 events
# in a day. The gap between the two populations is orders of magnitude, so the
# exact cut matters much less than having one.
INFRASTRUCTURE_DEGREE = 1000

# Where each address's distinct-counterparty estimate lives.
_DEGREE_KEY = "sentinel:crypto:counterparties:{address}"

# Long enough to survive restarts and quiet periods; short enough that an
# address which stops behaving like a venue is eventually reassessed.
_DEGREE_TTL_SEC = 14 * 86400

_HEX_ADDRESS = re.compile(r"^0x[0-9a-f]{40}$")


def is_null_address(address: Optional[str]) -> bool:
    """The burn and mint address, and the 0x...dead convention.

    Structural rather than a list: these are protocol constants, not a guess
    about who owns something, and they are the one case where writing the
    literal down is the honest implementation.
    """
    if not address:
        return False
    addr = str(address).strip().lower()
    if not _HEX_ADDRESS.match(addr):
        return False
    body = addr[2:]
    if body == "0" * 40:
        return True
    # 0x000...dead is the other provable burn sink in common use. The zeros
    # are leading, so they strip from the left.
    return body.lstrip("0") == "dead"


async def note_counterparty(redis_client, address: str, other: str) -> None:
    """Records that `address` transacted with `other`.

    HyperLogLog rather than a set: an exchange hot wallet would otherwise store
    millions of members to answer a question that only needs an order of
    magnitude.
    """
    if not address or not other or address == other:
        return
    try:
        key = _DEGREE_KEY.format(address=str(address).lower())
        await redis_client.raw.pfadd(key, str(other).lower())
        await redis_client.raw.expire(key, _DEGREE_TTL_SEC)
    except Exception as e:
        logger.debug(f"Could not record counterparty for {str(address)[:10]}: {e}")


async def counterparty_degree(redis_client, address: str) -> int:
    """Estimated distinct counterparties seen for this address."""
    if not address:
        return 0
    try:
        return int(await redis_client.raw.pfcount(_DEGREE_KEY.format(address=str(address).lower())))
    except Exception:
        return 0


async def is_infrastructure(redis_client, address: str) -> bool:
    """True when this address behaves like a venue rather than a participant."""
    if is_null_address(address):
        return True
    return await counterparty_degree(redis_client, address) >= INFRASTRUCTURE_DEGREE


async def choose_primary(
    redis_client, sender: str, receiver: str
) -> Tuple[str, bool]:
    """(address to attribute the event to, whether both sides are infrastructure).

    Prefers the side that is a participant. When both sides are venues the
    transfer is plumbing -- an exchange rebalancing to itself, a router hop --
    and the caller is told so rather than being handed a misleading actor.
    """
    sender_ok = bool(sender) and sender != "UNKNOWN"
    receiver_ok = bool(receiver) and receiver != "UNKNOWN"

    sender_infra = await is_infrastructure(redis_client, sender) if sender_ok else True
    receiver_infra = await is_infrastructure(redis_client, receiver) if receiver_ok else True

    if receiver_ok and not receiver_infra:
        return receiver, False
    if sender_ok and not sender_infra:
        return sender, False
    # Both venues, or neither usable. Keep the receiver for continuity so the
    # event still resolves to something, and flag it.
    return (receiver if receiver_ok else sender), True
