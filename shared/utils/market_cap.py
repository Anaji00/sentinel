"""
shared/utils/market_cap.py

How big the company behind a ticker is, cached, so a board can refuse to rank
things that are not companies.

The movers board is built from the radar's full sweep -- 11,579 symbols on the
live deployment -- scored by the day's percentage move and nothing else. What
that produces, measured during a regular session:

    gainers   MSAIW +18,800%   MGN +2,720%   SFWL +1,233%   JAGX +1,053%
    losers    BBLGW -88.9%     FGIWW -86.8%  SCAGW -85.6%   SBFMW -83.9%

MSAIW is a warrant that went from $0.0001 to $0.0189. Six of those eight
tickers end in W, which is the warrant suffix. Not one recognisable company
appears in either direction, because a percentage move is trivially dominated by
instruments whose previous close was a rounding error.

Market capitalisation is the gate that fixes it, and it is expensive in exactly
the way the sweep is not: Finnhub prices it one symbol at a time, and the sweep
runs every sixty seconds over eleven thousand symbols. So it is cached here
with a long TTL -- a company's size does not move meaningfully in a week -- and
resolved by a background walk rather than on the request path.

**Unknown is not "small".** A symbol whose market cap has not been resolved is
excluded from a gated board rather than admitted, because the whole purpose of
the gate is to say "everything here is at least this big", and a board that
silently includes unmeasured names cannot say that. The cost is that a gated
board is sparse until the backfill has run; the alternative is a board that
makes a claim it cannot support.
"""

from typing import Optional

# A week. Market capitalisation moves with the share price, so this is
# deliberately stale-tolerant: the gate asks "is this a billion-dollar company",
# not "what is it worth right now", and the answer to the first survives a week
# of ordinary price movement. A symbol that crosses the boundary during that
# week is the error this accepts.
MARKET_CAP_TTL_SEC = 7 * 24 * 3600

# What the gate defaults to. One billion US dollars.
DEFAULT_MIN_MARKET_CAP_USD = 1_000_000_000.0

MARKET_CAP_PREFIX = "sentinel:marketcap:usd:"

# Written when a lookup succeeds and the provider says the symbol has no market
# capitalisation -- a warrant, a unit, a right, an index. Distinguishes "asked
# and there is none" from "not asked yet", which is the same distinction the
# rest of this platform keeps between absence and zero.
NOT_A_COMPANY = "none"

# Written when the provider HAS a figure and it is not denominated in dollars.
#
# Finnhub's profile2 reports `marketCapitalization` in the currency of the
# primary listing, and this cache is denominated in USD by its own key name. TSM
# resolves to the Taiwan Stock Exchange with currency TWD, so its figure was
# stored as 61,719,038,554,688 "USD" -- about fifty times the company's value
# and larger than world GDP. Nothing caught it because the gate only asks
# whether a number exceeds a billion, and a number inflated fifty-fold passes
# that test with enthusiasm.
#
# The direction of the error is the dangerous one: every currency worth less
# than a dollar inflates, so the gate that exists to admit only billion-dollar
# companies was admitting foreign-listed ones worth a fraction of that. A
# 1.5bn-yen company reads as $1.5bn and clears it.
#
# Converting would need an FX rate this platform does not carry. So the honest
# state is a third one: asked, answered, and not expressible in this cache's
# unit. `parse_market_cap` returns None for it exactly as it does for
# NOT_A_COMPANY -- every size gate refuses it, which is the documented rule that
# unknown is not small -- while `has_been_resolved` stays true so the backfill
# does not re-ask a question whose answer will not change.
NOT_IN_USD = "non_usd"


def market_cap_key(ticker: str) -> str:
    return f"{MARKET_CAP_PREFIX}{str(ticker).upper().strip()}"


def parse_market_cap(raw) -> Optional[float]:
    """The cached value as US dollars, or None when it is unknown.

    Returns None for both "never resolved" and "resolved, and the provider had
    no figure". A caller gating on size must treat those the same way -- it
    cannot assert a minimum for either -- while the cache keeps them apart so a
    backfill does not re-ask a question that has already been answered.
    """
    if raw is None:
        return None
    text = raw if isinstance(raw, str) else raw.decode("utf-8", "replace")
    text = text.strip()
    if not text or text in (NOT_A_COMPANY, NOT_IN_USD):
        return None
    try:
        value = float(text)
    except (TypeError, ValueError):
        return None
    return value if value > 0 else None


async def cached_market_cap(redis_client, ticker: str) -> Optional[float]:
    """The cached market capitalisation in USD, or None."""
    if not redis_client or not ticker:
        return None
    try:
        raw = getattr(redis_client, "raw", redis_client)
        return parse_market_cap(await raw.get(market_cap_key(ticker)))
    except Exception:
        return None


async def has_been_resolved(redis_client, ticker: str) -> bool:
    """Whether this symbol has been asked about at all.

    A backfill uses this rather than `cached_market_cap`, so that a warrant --
    correctly resolved as having no market capitalisation -- is not asked again
    on every pass for the life of the deployment.
    """
    if not redis_client or not ticker:
        return False
    try:
        raw = getattr(redis_client, "raw", redis_client)
        return bool(await raw.exists(market_cap_key(ticker)))
    except Exception:
        return False


async def store_market_cap(
    redis_client, ticker: str, usd: Optional[float], currency: Optional[str] = None
) -> None:
    """Record a resolved market capitalisation, or why there is no USD figure.

    `currency` is what the provider said the figure was denominated in. Anything
    other than USD is recorded as NOT_IN_USD rather than converted or believed:
    see that constant for what this cost before it was checked.
    """
    if not redis_client or not ticker:
        return
    if usd is not None and currency and str(currency).strip().upper() != "USD":
        try:
            raw = getattr(redis_client, "raw", redis_client)
            await raw.set(market_cap_key(ticker), NOT_IN_USD, ex=MARKET_CAP_TTL_SEC)
        except Exception:
            return
        return
    value = NOT_A_COMPANY
    if usd is not None:
        try:
            as_float = float(usd)
            if as_float > 0:
                value = repr(as_float)
        except (TypeError, ValueError):
            value = NOT_A_COMPANY
    try:
        raw = getattr(redis_client, "raw", redis_client)
        await raw.set(market_cap_key(ticker), value, ex=MARKET_CAP_TTL_SEC)
    except Exception:
        return


__all__ = [
    "DEFAULT_MIN_MARKET_CAP_USD",
    "MARKET_CAP_PREFIX",
    "MARKET_CAP_TTL_SEC",
    "NOT_A_COMPANY",
    "NOT_IN_USD",
    "cached_market_cap",
    "has_been_resolved",
    "market_cap_key",
    "parse_market_cap",
    "store_market_cap",
]
