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

from typing import Any, Dict, Iterable, Optional, Set, Tuple

from shared.utils.quiet_failures import dropped as count_dropped

# A week. Market capitalisation moves with the share price, so this is
# deliberately stale-tolerant: the gate asks "is this a billion-dollar company",
# not "what is it worth right now", and the answer to the first survives a week
# of ordinary price movement. A symbol that crosses the boundary during that
# week is the error this accepts.
MARKET_CAP_TTL_SEC = 7 * 24 * 3600

# What the gate defaults to for calls, peers and the dashboard movers: ten billion US dollars (11 Nov 2026; one
# billion before). The trader and the model's feature jobs keep the one-billion floor they were validated on.
DEFAULT_MIN_MARKET_CAP_USD = 10_000_000_000.0
MODEL_MIN_MARKET_CAP_USD = 1_000_000_000.0

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


# Every stock's best-known capitalisation (USD), rebuilt by market_cap_job: shares
# outstanding (SEC, else the vendor) times the latest close, or the Finnhub
# figure above, whichever is larger. The Finnhub cache alone left 324 of the
# 999 ranked stocks unresolved (HD, CRM, COF among them, 30 Oct 2026), and an
# SEC share count can cover one class only (BRK.B read $0.78bn), so neither
# source decides alone. Missing from this hash means unknown, which is not large.
CAPS_KEY = "sentinel:marketcap:best"
CAPS_TTL_SEC = 3 * 86400


# A computed cap more than this many times the vendor's is a share count that does not match the traded unit (ordinary shares against
# ADS, or a count from before a reverse split), not a company that large: AKTX read $596bn against the vendor's $13m (13 Nov 2026; 54
# of the 1,942 names with both figures were over 5x, three of them past the $10bn gate).
IMPLAUSIBLE_RATIO = 5.0


def best_cap(computed: Optional[float], cached: Optional[float]) -> Optional[float]:
    """The larger of the two estimates: one share class or a stale count can make
    the computed figure too small, rarely too large. When it is far larger than
    the vendor's figure, the vendor's is used."""
    if computed and cached and computed > 0 and cached > 0 and computed > IMPLAUSIBLE_RATIO * cached:
        return cached
    vals = [v for v in (computed, cached) if v is not None and v > 0]
    return max(vals) if vals else None


# Foreign ADRs whose computed cap (SEC ordinary share count times the ADS price) is overstated by the number of ordinary shares one ADS stands for, and
# one whose vendor figure is in the home currency in a field read as USD (layer audit 18 Nov, finding 11): TSM read $9,404bn (true about $1.9tn), NGG about 6x,
# E about 3x, SHEL 2.6x (under the 5x vendor guard), EQNR $946bn (a NOK figure; about $90bn). Curated and small; an ADR not listed is unchanged.
ADS_ORDINARY_SHARES: Dict[str, float] = {"TSM": 5.0, "NGG": 5.0, "E": 2.0, "SHEL": 2.0}
VENDOR_FIGURE_NOT_USD = frozenset({"EQNR"})


def adr_adjusted(ticker: str, computed: Optional[float], cached: Optional[float]) -> Tuple[Optional[float], Optional[float]]:
    """(computed, cached) corrected for a known ADR: the computed cap divided by its ordinary shares per ADS; a vendor figure known to be in another currency dropped."""
    t = str(ticker).upper()
    ratio = ADS_ORDINARY_SHARES.get(t)
    if ratio and computed:
        computed = computed / ratio
    if t in VENDOR_FIGURE_NOT_USD:
        cached = None
    return computed, cached


async def large_caps(redis_client: Any, min_usd: float = DEFAULT_MIN_MARKET_CAP_USD) -> Optional[Set[str]]:
    """Tickers whose best-known capitalisation is at least min_usd, or None when
    the table is missing: a caller must not read that as "every stock qualifies"."""
    try:
        raw = getattr(redis_client, "raw", redis_client)
        table: Dict[Any, Any] = await raw.hgetall(CAPS_KEY) or {}
    except Exception:
        return None
    if not table:
        return None
    out = set()
    for k, v in table.items():
        try:
            if float(v.decode() if isinstance(v, bytes) else v) >= min_usd:
                out.add((k.decode() if isinstance(k, bytes) else str(k)).upper())
        except (TypeError, ValueError):
            continue
    return out


async def is_large_cap(redis_client: Any, ticker: str, min_usd: float = DEFAULT_MIN_MARKET_CAP_USD) -> bool:
    """True when the stock's best-known capitalisation is at least min_usd; an
    unknown or unreadable one is False."""
    try:
        raw = getattr(redis_client, "raw", redis_client)
        v = await raw.hget(CAPS_KEY, str(ticker).upper())
        return v is not None and float(v.decode() if isinstance(v, bytes) else v) >= min_usd
    except Exception:
        return False


# A company is not worth more than this many days of its own dollar trading volume: Apple is about 350, a thinly traded real company a few
# thousand. LTM read $27 trillion (cap over 20-day volume: 549,000), BSAC $6 trillion, BCH $3.9 trillion and AMX $1.5 trillion (13 Nov 2026):
# foreign ADRs whose SEC share count is ordinary shares, multiplied by the price of an ADS. Such a name is unknown, not large.
# 21 Nov 2026 (owner-approved P5): 4,000, down from 10,000. Preferred shares and ADRs counted in ordinary shares slipped under the old bound
# (AGNCZ 9,212, IMOS 5,558, WF 4,407, PAM 4,474); real names top out near 400 (Apple 297, Berkshire B 418). Only BRK.A of the 25 names above
# 4,000 reaches the model and none is in the B1 book.
MAX_CAP_OVER_ADV = 4_000.0


def plausible_against_liquidity(cap: float, adv_usd: Optional[float]) -> bool:
    """False when the cap is more than MAX_CAP_OVER_ADV times the stock's average daily dollar volume; unknown volume cannot disprove it."""
    return adv_usd is None or adv_usd <= 0 or cap / adv_usd <= MAX_CAP_OVER_ADV


def cap_rows(computed: Dict[str, float], cached: Dict[str, Optional[float]],
             tickers: Iterable[str], adv: Optional[Dict[str, float]] = None) -> Dict[str, float]:
    """{ticker: best cap} for every ticker with at least one estimate; a cap that fails the liquidity bound is left out (unknown)."""
    out = {}
    for t in tickers:
        b = best_cap(*adr_adjusted(t, computed.get(t), cached.get(t)))
        if b is not None and (adv is None or plausible_against_liquidity(b, adv.get(t))):
            out[t] = b
        elif b is not None:
            # Left out as implausible; counted so a halted real company dropped on a data artefact is visible (review L10), not kept or invented.
            count_dropped("market_cap.implausible_vs_liquidity", "cap over the liquidity bound; name left out of the cap table", detail=f"{t} cap {b:.0f}")
    return out


__all__ = [
    "CAPS_KEY",
    "CAPS_TTL_SEC",
    "ADS_ORDINARY_SHARES", "VENDOR_FIGURE_NOT_USD", "adr_adjusted",
    "best_cap", "plausible_against_liquidity", "MAX_CAP_OVER_ADV",
    "cap_rows",
    "is_large_cap",
    "large_caps",
    "DEFAULT_MIN_MARKET_CAP_USD",
    "MODEL_MIN_MARKET_CAP_USD",
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
