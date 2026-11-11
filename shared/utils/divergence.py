"""
shared/utils/divergence.py

A stock moving against its peers on its own news.

On 9 Nov 2026 NVDA closed -4.3% on a company-specific scandal while AMD rose 13%, AVGO 9%, MRVL 9% and the
semiconductor ETF 5%: a nine-point gap to its sector, with twenty headlines naming it. The system produced twenty
identical "Peer Decoupling Detected" alerts that carried no returns and no gap (reports/Divergence alert.md). This
is the measurement that alert should have been: the stock's move, its reference group's move, the gap between
them in units of that gap's own recent variation, and whether news named the stock alone.

Two references per stock, because either alone misses cases (the NVDA case read z -2.8 against its statistical
peers, which are volatile and are not its industry, and -5.0 against its four-digit-SIC group): its measured
sympathy peers and its industry group. It fires when either does.

Pure: numbers in, a verdict out.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Mapping, Optional, Sequence, Tuple

Z_MIN = 3.0             # the gap must be this many standard deviations of its own last 60 sessions
MOVE_MIN = 0.02         # the stock itself moved at least 2%
REF_MIN = 0.01          # its reference moved at least 1%, the other way
MIN_HISTORY = 40        # sessions of gap history needed to judge a gap
MIN_MEMBERS = 3         # peers with a return, or the reference is not a reference
ELEVATED_Z = 4.0        # with news and at least this, the alert is ELEVATED rather than ALERT

Reference = Tuple[Optional[float], Sequence[float], Sequence[str]]    # (return now, past gaps, members)


@dataclass(frozen=True)
class Divergence:
    ticker: str
    reference: str                  # "sympathy" or "industry"
    ret: float                      # the stock's move since the previous close
    ref_ret: float                  # its reference group's move
    gap: float                      # ret - ref_ret
    z: float                        # gap over the standard deviation of the last 60 sessions' gaps
    news: bool                      # an article naming only this stock in the session
    members: Tuple[str, ...]

    @property
    def tier(self) -> str:
        return "ELEVATED" if self.news and abs(self.z) >= ELEVATED_Z else "ALERT"

    @property
    def kind(self) -> str:
        return "news" if self.news else "unexplained"

    def headline(self) -> str:
        label = "its industry group" if self.reference == "industry" else "its closest peers"
        tail = "on news naming it" if self.news else "with no news naming it"
        return (f"{self.ticker} {self.ret:+.1%} while {label} {self.ref_ret:+.1%} ({self.z:+.1f} sigma gap), {tail}")

    def description(self) -> str:
        names = ", ".join(self.members[:6])
        return (f"{self.ticker} moved {self.ret:+.2%} since the previous close while the mean of {names} moved "
                f"{self.ref_ret:+.2%}: a gap of {self.gap:+.2%}, {abs(self.z):.1f} times the usual variation of that gap. "
                f"{'News names the stock alone this session.' if self.news else 'No news names the stock alone this session.'}")


@dataclass(frozen=True)
class PeerDivergence(Divergence):
    """A divergence found by the registered multi-horizon rule P1 (shared/utils/peer_residual.py): the move less what the
    group's move implies through the stock's own beta, in units of that residual's usual size at this horizon.

    `ref_ret` is the group's actual move and `gap` the beta-adjusted residual; `z` is the residual's z."""
    horizon: str = "session"        # 'session' (since the previous close) or '5d'
    breadth: float = 0.0            # share of the group's members moving opposite to the stock
    beta: float = 1.0
    rvol: Optional[float] = None    # volume so far against the same clock time on earlier sessions; None for 5d

    def headline(self) -> str:
        label = "its industry group" if self.reference == "industry" else "its closest peers"
        span = "over 5 sessions" if self.horizon == "5d" else "today"
        tail = "on news naming it" if self.news else "with no news naming it"
        return (f"{self.ticker} {self.ret:+.1%} {span} while {label} {self.ref_ret:+.1%} "
                f"({self.z:+.1f} sigma beyond its beta to them), {tail}")

    def description(self) -> str:
        names = ", ".join(self.members[:6])
        span = "the last five sessions" if self.horizon == "5d" else "the previous close"
        vol = f" Volume is {self.rvol:.1f} times its usual for this time of day." if self.rvol else ""
        return (f"{self.ticker} moved {self.ret:+.2%} since {span} while the mean of {names} moved {self.ref_ret:+.2%}. "
                f"With its beta of {self.beta:.2f} to them that leaves {self.gap:+.2%} unexplained, {abs(self.z):.1f} times "
                f"its usual residual; {self.breadth:.0%} of them moved the other way.{vol} "
                f"{'News names the stock alone this session.' if self.news else 'No news names the stock alone this session.'}")


def mean_return(returns: Sequence[Optional[float]], min_members: int = MIN_MEMBERS) -> Optional[float]:
    """Mean of the finite returns, or None when fewer than `min_members` have one."""
    xs = [float(r) for r in returns if r is not None and math.isfinite(float(r))]
    return sum(xs) / len(xs) if len(xs) >= min_members else None


def gap_z(gap: float, history: Sequence[float]) -> Optional[float]:
    """The gap in standard deviations of its past gaps; None when the history is short or flat."""
    xs = [float(g) for g in history if g is not None and math.isfinite(float(g))]
    if len(xs) < MIN_HISTORY or not math.isfinite(gap):
        return None
    m = sum(xs) / len(xs)
    sd = math.sqrt(sum((x - m) ** 2 for x in xs) / (len(xs) - 1))
    return gap / sd if sd > 0 else None


def detect(ticker: str, ret: Optional[float], references: Mapping[str, Reference], news: bool) -> Optional[Divergence]:
    """The reference with the largest qualifying gap, or None.

    A reference qualifies when the stock and the reference moved in opposite directions, the stock by at least
    MOVE_MIN and the reference by at least REF_MIN, and the gap is at least Z_MIN of its own variation."""
    if ret is None or not math.isfinite(ret) or abs(ret) < MOVE_MIN:
        return None
    best: Optional[Divergence] = None
    for name, (ref_ret, history, members) in references.items():
        if ref_ret is None or not math.isfinite(ref_ret) or abs(ref_ret) < REF_MIN or (ret > 0) == (ref_ret > 0):
            continue
        gap = ret - ref_ret
        z = gap_z(gap, history)
        if z is None or abs(z) < Z_MIN:
            continue
        cand = Divergence(ticker=ticker, reference=name, ret=ret, ref_ret=ref_ret, gap=gap, z=z, news=news,
                          members=tuple(members))
        if best is None or abs(cand.z) > abs(best.z):
            best = cand
    return best


__all__ = ["Divergence", "PeerDivergence", "ELEVATED_Z", "MIN_HISTORY", "MIN_MEMBERS", "MOVE_MIN", "REF_MIN", "Z_MIN", "detect", "gap_z", "mean_return"]
