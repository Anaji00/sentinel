"""
shared/utils/auto_trade.py

The model portfolio's stock picks as broker orders. The portfolio engine
(services/agents/portfolio_engine.py) decides the book and values it on paper
beside an S&P 500 core; the account holds the picks alone -- no index fund --
so what it makes or loses is the stock picking and nothing else.

Pure: books, sizing, account value and prices in; target shares and orders out.

  weights   each long at `leg` x its share of the long leg, each short at
            `leg` x short tilt x its share of the short leg (equal shares, or the
            meta-label weights when the lab confirmed them); index funds are
            never held, even if one were ranked
  shares    longs may be fractional (the venue accepts them on market day
            orders); shorts must be whole shares. Rounding each short alone
            would drop most of a small leg (on $100,000, 146 of 195 shorts were
            under one share and the leg came to a fifth of its size), so the
            leg's whole shares are allocated together: its dollar total stays on
            target, each share going to the name furthest below its weight
  orders    only for names this trader manages (its target, or a name it has
            traded before): it never sells what someone else bought in the same
            account. A position that changes side is closed first and opened
            in a second order, because the venue rejects one order that does both.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

# Funds that hold the market rather than pick from it.
INDEX_FUNDS = frozenset({"SPY", "QQQ", "IWM", "DIA", "VOO", "IVV", "VTI", "RSP", "SPLG", "QQQM"})
# The book (trial B1, adopted 24 Oct 2026; MODEL_TRIALS.md): the monthly
# model's extremes only. Short the bottom tenth that can be borrowed; buy the
# 90th to 99th percentile, leaving out the very top 1%, which lost money in the
# model's own calibration. Equal weight in each leg, rebuilt every 21 sessions.
# Over 98 out-of-sample months it beat the top-and-bottom-fifth book by 0.20% a
# month (net Sharpe 0.82 against 0.71) with the same worst month.
B1_LONG = (0.90, 0.99)
B1_SHORT_MAX = 0.10
REBALANCE_SESSIONS = 21

# Orders worth less than this are not sent: they cost more in attention than
# they move the book.
MIN_ORDER_USD = 5.0
# Fractional quantities as the venue accepts them.
QTY_DECIMALS = 4


@dataclass(frozen=True)
class Plan:
    symbol: str
    qty: float          # positive: shares to trade
    side: str           # "buy" or "sell"
    reason: str         # "open", "add", "trim", "close", "flip-close", "flip-open"


def b1_book(percentiles: Mapping[str, float], borrowable: Set[str]) -> Tuple[List[str], List[str]]:
    """(longs, shorts) from the monthly scores' percentiles, index funds excluded."""
    longs, shorts = [], []
    for t, p in percentiles.items():
        t = str(t).upper()
        if t in INDEX_FUNDS or p is None or not math.isfinite(float(p)):
            continue
        p = float(p)
        if B1_LONG[0] <= p < B1_LONG[1]:
            longs.append(t)
        elif p <= B1_SHORT_MAX and t in borrowable:
            shorts.append(t)
    return sorted(longs), sorted(shorts)


def picks_only(names: Iterable[str], stocks: Optional[Set[str]]) -> List[str]:
    """The names that are companies' shares. INDEX_FUNDS stopped ten broad index
    funds; on 29 Oct 2026 B1 still bought EWZ, VGK and IBIT and shorted SPDN and
    USFR -- country, bitcoin, inverse and Treasury funds -- because the model's
    universe carries them. `stocks` is the set the venue's asset names classify
    as common stock (asset_universe.classify); without it, only INDEX_FUNDS go."""
    keep = []
    for t in names:
        s = str(t).upper()
        if s in INDEX_FUNDS or (stocks is not None and s not in stocks):
            continue
        keep.append(s)
    return keep


def rebalance_due(sessions_since: Optional[int]) -> bool:
    """A new book when none is held or REBALANCE_SESSIONS have passed."""
    return sessions_since is None or sessions_since >= REBALANCE_SESSIONS


def target_weights(long: Iterable[str], short: Iterable[str], leg: float, short_tilt: float = 1.0,
                   weights: Optional[Mapping[str, float]] = None) -> Dict[str, float]:
    """Signed share of the account per stock: the long picks and the short picks."""
    out: Dict[str, float] = {}

    def leg_(names: List[str], size: float, sign: int) -> None:
        ws = {t: (float(weights.get(t, 1.0)) if weights else 1.0) for t in names}
        total = sum(w for w in ws.values() if w > 0)
        if total <= 0 or size <= 0:
            return
        for t, w in ws.items():
            if w > 0:
                out[t] = out.get(t, 0.0) + sign * size * w / total

    picks = lambda names: sorted({str(t).upper() for t in names} - INDEX_FUNDS)  # noqa: E731
    leg_(picks(long), float(leg), 1)
    leg_(picks(short), float(leg) * float(short_tilt or 1.0), -1)
    return out


def alert_risk_weights(names: Iterable[str], flagged: Set[str], scale: float) -> Dict[str, float]:
    """Relative weight per name for target_weights: `scale` for a name with a
    recent alert from a family graded to precede bigger moves, 1 otherwise.
    Within a leg, so a flagged name's share goes to the others. A size forecast
    shrinks the bet; it says nothing about the direction."""
    s = min(1.0, max(0.0, float(scale)))
    return {str(t).upper(): (s if str(t).upper() in flagged else 1.0) for t in names}


def whole_share_leg(dollars: Mapping[str, float], prices: Mapping[str, float]) -> Dict[str, int]:
    """Whole shares for one leg's dollar targets (positive), totalling as near
    the leg's dollars as whole shares allow."""
    want = {t: d for t, d in dollars.items() if d > 0 and prices.get(t, 0) > 0}
    shares = {t: int(d // prices[t]) for t, d in want.items()}
    left = sum(want.values()) - sum(shares[t] * prices[t] for t in want)
    while True:
        # The name furthest below its target, in shares, among those a share of
        # which brings the leg's total nearer its target.
        fits = [t for t in want if prices[t] / 2 < left]
        if not fits:
            break
        t = max(fits, key=lambda x: (want[x] - shares[x] * prices[x]) / prices[x])
        shares[t] += 1
        left -= prices[t]
    return {t: q for t, q in shares.items() if q}


def target_shares(weights: Mapping[str, float], equity: float, prices: Mapping[str, float]) -> Dict[str, float]:
    """Share counts for the weights at `equity`: longs fractional, shorts whole
    and allocated across the leg. A symbol without a price is left out."""
    out: Dict[str, float] = {}
    if equity <= 0:
        return out
    ok = {t: w for t, w in weights.items() if prices.get(t) and prices[t] > 0 and math.isfinite(prices[t])}
    for t, w in ok.items():
        if w > 0:
            q = round(w * equity / prices[t], QTY_DECIMALS)
            if q:
                out[t] = q
    for t, q in whole_share_leg({t: -w * equity for t, w in ok.items() if w < 0}, prices).items():
        out[t] = -float(q)
    return out


def unplaced(weights: Mapping[str, float], shares: Mapping[str, float]) -> List[str]:
    """Names the book holds that the account does not (no price, or a short whose share went elsewhere)."""
    return sorted(t for t, w in weights.items() if w != 0 and t not in shares)


def plan_orders(target: Mapping[str, float], held: Mapping[str, float], managed: Set[str],
                prices: Mapping[str, float], min_order_usd: float = MIN_ORDER_USD) -> List[Plan]:
    """Orders moving the managed part of the account from `held` to `target`.

    Exits and trims come first, so the cash they free funds the buys."""
    first: List[Plan] = []
    then: List[Plan] = []
    for t in sorted(set(target) | (set(held) & set(managed))):
        have, want = float(held.get(t, 0.0)), float(target.get(t, 0.0))
        px = prices.get(t) or 0.0
        if have and want and (have > 0) != (want > 0):
            first.append(Plan(t, abs(have), "sell" if have > 0 else "buy", "flip-close"))
            then.append(Plan(t, abs(want), "buy" if want > 0 else "sell", "flip-open"))
            continue
        delta = round(want - have, QTY_DECIMALS)
        if delta == 0 or (px and abs(delta) * px < min_order_usd and want != 0):
            continue
        if want == 0:
            first.append(Plan(t, abs(have), "sell" if have > 0 else "buy", "close"))
        elif have == 0:
            then.append(Plan(t, abs(want), "buy" if want > 0 else "sell", "open"))
        elif abs(want) < abs(have):
            first.append(Plan(t, abs(delta), "sell" if want > 0 else "buy", "trim"))
        else:
            then.append(Plan(t, abs(delta), "buy" if want > 0 else "sell", "add"))
    return first + then


# ── Safety: per-name caps, liquidity, limit prices, halts ────────────────────
#
# FIA and CFTC guidance for automated trading: a kill switch that stops new
# orders, automatic pre-trade limits per name and on order size, and limits
# that do not let one bad day or a broken execution path run on. Pure here;
# AutoTrader applies them and owns the halt flag.

def cap_weights(weights: Mapping[str, float], max_name: float) -> Dict[str, float]:
    """Each name's |weight| clipped to `max_name`; the excess stays as cash
    rather than being spread over the others."""
    m = abs(float(max_name))
    return {t: max(-m, min(m, float(w))) for t, w in weights.items()}


def liquidity_capped(plan: Plan, price: float, adv_usd: Optional[float], max_participation: float) -> Plan:
    """An opening or adding order shrunk to `max_participation` of the name's
    average daily dollar volume. Closes and trims are never shrunk: an exit
    that cannot complete is the larger risk."""
    if plan.reason not in ("open", "add", "flip-open") or not adv_usd or adv_usd <= 0 or price <= 0:
        return plan
    cap = math.floor(max_participation * adv_usd / price * 10 ** QTY_DECIMALS) / 10 ** QTY_DECIMALS
    return Plan(plan.symbol, min(plan.qty, cap), plan.side, plan.reason) if cap < plan.qty else plan


def limit_price(side: str, ref: float, offset: float) -> float:
    """A marketable limit: up to `offset` worse than the reference, never more.
    Cents at $1 and above, hundredths of a cent below (the venue's ticks)."""
    px = ref * (1.0 + offset) if side == "buy" else ref * (1.0 - offset)
    return round(px, 2 if ref >= 1.0 else 4)


def safety_halts(history: Sequence[Tuple[float, float]], equity: float, spy: Optional[float],
                 max_day_loss: float, max_drawdown: float, max_underperf_21: float,
                 beta: float = 1.0) -> List[str]:
    """Reasons to stop trading, from the book's recorded (equity, SPY close) per
    session, oldest first, and today's equity and SPY:
      - today's loss on the last recorded equity beyond max_day_loss;
      - drawdown from the recorded peak beyond max_drawdown;
      - trailing its market benchmark by more than max_underperf_21 over 21
        sessions (the drift alarm: the live book is not behaving like the
        tested one). The benchmark is `beta` -- the book's net exposure -- times
        SPY's return: 0 for B1's dollar-neutral book, which would otherwise read
        any strong rally as drift."""
    out: List[str] = []
    eq = [e for e, _ in history if e and e > 0]
    if eq and equity > 0 and equity / eq[-1] - 1 < -abs(max_day_loss):
        out.append(f"day loss {equity / eq[-1] - 1:.1%} beyond {max_day_loss:.0%}")
    peak = max(eq + [equity]) if eq else equity
    if peak > 0 and equity / peak - 1 < -abs(max_drawdown):
        out.append(f"drawdown {equity / peak - 1:.1%} beyond {max_drawdown:.0%}")
    if len(history) >= 21 and spy and history[-21][0] and history[-21][1]:
        excess = (equity / history[-21][0] - 1.0) - float(beta) * (spy / history[-21][1] - 1.0)
        if excess < -abs(max_underperf_21):
            out.append(f"trailing its benchmark ({beta:+.2f} x SPY) by {-excess:.1%} over 21 sessions (drift)")
    return out


def execution_halt(sent: int, failed: int, max_fail_share: float, min_orders: int = 5) -> Optional[str]:
    """A day whose orders mostly failed: the execution path is broken."""
    n = sent + failed
    if n >= min_orders and failed / n > max_fail_share:
        return f"{failed} of {n} orders failed"
    return None


__all__ = ["B1_LONG", "B1_SHORT_MAX", "REBALANCE_SESSIONS", "b1_book", "rebalance_due", "INDEX_FUNDS", "MIN_ORDER_USD", "Plan", "plan_orders", "target_shares", "target_weights", "unplaced",
           "whole_share_leg", "alert_risk_weights", "cap_weights", "execution_halt", "limit_price", "liquidity_capped", "picks_only",
           "safety_halts"]
