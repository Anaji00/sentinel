"""Score resolution: replay-ready rescoring of saturated or constant producer scores (registered 13 Nov 2026).

Nothing here is live unless SCORE_RESOLUTION_V2=1 (default 0). Producers call `v2_enabled()` and otherwise keep their old
score. The functions are pure so the replays (scripts/replay_*_score.py) and the live producers use the same arithmetic.
"""
from __future__ import annotations

import bisect
import math
import os
import re
from datetime import date
from typing import Dict, List, Optional, Sequence, Tuple

FLAG = "SCORE_RESOLUTION_V2"
# Alert cuts found in the code (agents/base.py, dossier, quant engine, explain, tradfi): an event is compared on each.
ALERT_CUTS: Tuple[float, ...] = (0.3, 0.4, 0.5, 0.6, 0.65, 0.7, 0.75, 0.8, 0.9)
SHORT_Z_FLOOR = 2.0          # the spike gate already requires z >= 2
NEWS_BASELINE_DAYS = 20
NEWS_WINDOW_HOURS = 6
NEWS_WINDOWS_IN_BASELINE = NEWS_BASELINE_DAYS * 24 // NEWS_WINDOW_HOURS     # 80
NEWS_MIN_SD = 0.5            # Poisson-style floor so a name with no baseline does not divide by ~0


def v2_enabled() -> bool:
    return os.getenv(FLAG, "0").strip() == "1"


def short_volume_score(old: float, z: Optional[float]) -> float:
    """min(old, 0.55 + 0.40 * (1 - exp(-(z - 2) / 3))); the old score when z is missing."""
    if z is None or not math.isfinite(z):
        return old
    zz = max(z, SHORT_Z_FLOOR)
    return min(old, round(0.55 + 0.40 * (1.0 - math.exp(-(zz - SHORT_Z_FLOOR) / 3.0)), 3))


def short_volume_score_b(old: float, z: Optional[float]) -> float:
    """Registered alternative (13 Nov, revised bars): min(old, 0.70 + 0.25 * (1 - exp(-(z - 2) / 3))), which keeps the old
    score's 0.70 minimum at the gate. Capped at the old score so no event moves up across a cut."""
    if z is None or not math.isfinite(z):
        return old
    zz = max(z, SHORT_Z_FLOOR)
    return min(old, round(0.70 + 0.25 * (1.0 - math.exp(-(zz - SHORT_Z_FLOOR) / 3.0)), 3))


def short_volume_z(ratio: float, history: Sequence[float], min_sd: float = 0.03) -> Optional[float]:
    """The z the live tag `short_vol_z` stores: (ratio - mean) / population sd of the prior history; the sd is floored at the gate's SHORT_MIN_SD
    (0.03), as the live tag now is."""
    if len(history) < 2:
        return None
    mu = sum(history) / len(history)
    sd = math.sqrt(sum((h - mu) ** 2 for h in history) / len(history))
    sd = max(sd, min_sd)
    return (ratio - mu) / sd


def news_z(n_window: int, n_baseline: int) -> float:
    """Article count in the 6-hour window against the name's per-window mean over the 20 days before it.

    z = (n - mu) / max(NEWS_MIN_SD, sqrt(mu)): the count is a Poisson-like arrival process, so its spread is taken as sqrt(mu).
    """
    mu = max(n_baseline, 0) / NEWS_WINDOWS_IN_BASELINE
    return (n_window - mu) / max(NEWS_MIN_SD, math.sqrt(mu))


def news_score(z: Optional[float], span: float = 0.25) -> float:
    """0.5 + span * min(1, max(0, z) / 4): 0.50 to 0.75 as first registered; span 0.14 (max 0.64) is the revised range that stays
    under the lowest cut that applies to headlines (0.65)."""
    if z is None or not math.isfinite(z):
        return 0.5
    return round(0.5 + span * min(1.0, max(0.0, z) / 4.0), 3)


def ranks(values: Sequence[float]) -> List[float]:
    """1-based ranks, ties given their average rank."""
    order = sorted(range(len(values)), key=lambda i: values[i])
    out = [0.0] * len(values)
    i = 0
    while i < len(order):
        j = i
        while j + 1 < len(order) and values[order[j + 1]] == values[order[i]]:
            j += 1
        avg = (i + j) / 2.0 + 1.0
        for k in range(i, j + 1):
            out[order[k]] = avg
        i = j + 1
    return out


def spearman(a: Sequence[float], b: Sequence[float]) -> Optional[float]:
    """Rank correlation; None when either series is constant or the lengths are below 3."""
    if len(a) != len(b) or len(a) < 3:
        return None
    ra, rb = ranks(a), ranks(b)
    ma, mb = sum(ra) / len(ra), sum(rb) / len(rb)
    sa = math.sqrt(sum((x - ma) ** 2 for x in ra))
    sb = math.sqrt(sum((x - mb) ** 2 for x in rb))
    if sa == 0 or sb == 0:
        return None
    return sum((x - ma) * (y - mb) for x, y in zip(ra, rb)) / (sa * sb)


def top_share_mean(scores: Sequence[float], values: Sequence[Optional[float]], share: float = 0.01) -> Optional[float]:
    """Mean of `values` over the top `share` of events by score. Events tied at the cutoff contribute their group mean for the
    slots that remain, which is the expected value of breaking the tie at random (a score on its cap is a large tie)."""
    pairs = [(s, v) for s, v in zip(scores, values) if v is not None]
    if not pairs:
        return None
    k = max(1, math.ceil(share * len(pairs)))
    pairs.sort(key=lambda p: -p[0])
    cut = pairs[k - 1][0]
    above = [v for s, v in pairs if s > cut]
    tied = [v for s, v in pairs if s == cut]
    need = k - len(above)
    total = sum(above) + need * (sum(tied) / len(tied))
    return total / k


def share_at_or_above(scores: Sequence[float], level: float) -> float:
    return sum(1 for s in scores if s >= level) / len(scores) if scores else 0.0


def cut_changes(old: Sequence[float], new: Sequence[float],
                cuts: Sequence[float] = ALERT_CUTS) -> Dict[str, int]:
    """Events that newly cross a cut upward (the registered bar), and that fall below one they met before (informational)."""
    up = down = 0
    for o, n in zip(old, new):
        for c in cuts:
            if n >= c > o:
                up += 1
            elif o >= c > n:
                down += 1
    return {"newly_above_cut": up, "newly_below_cut": down}


def counts_above_cut(old: Sequence[float], new: Sequence[float], cuts: Sequence[float] = ALERT_CUTS) -> Dict[float, Tuple[int, int]]:
    """{cut: (events at or above under the old score, under the new)}."""
    return {c: (sum(1 for o in old if o >= c), sum(1 for n in new if n >= c)) for c in cuts}


def distribution(scores: Sequence[float], levels: Sequence[float] = (0.5, 0.8, 0.9)) -> Dict[str, object]:
    """n, distinct values (rounded to 3 places), quantiles, mode and its share, and the share at or above each level."""
    s = sorted(round(x, 3) for x in scores)
    n = len(s)
    if n == 0:
        return {"n": 0}
    counts: Dict[float, int] = {}
    for x in s:
        counts[x] = counts.get(x, 0) + 1
    mode, mode_n = max(counts.items(), key=lambda kv: kv[1])

    def q(p: float) -> float:
        return s[min(n - 1, int(p * n))]

    return {
        "n": n, "distinct": len(counts), "min": s[0], "p10": q(0.10), "p50": q(0.50), "p90": q(0.90), "max": s[-1],
        "mode": mode, "mode_share": mode_n / n, **{f"share_ge_{lv}": share_at_or_above(s, lv) for lv in levels},
    }


def five_session_abs_move(days: Sequence[date], closes: Sequence[float], d: date, sessions: int = 5) -> Optional[float]:
    """|close `sessions` bars after the last close on or before d / that close - 1|; None when either bar is missing.
    `days` is ascending. d itself need not be a session (a weekend event starts from the Friday close)."""
    i = bisect.bisect_right(days, d) - 1
    if i < 0 or i + sessions >= len(days) or (d - days[i]).days > 5 or closes[i] <= 0:
        return None
    return abs(closes[i + sessions] / closes[i] - 1.0)


def short_volume_verdicts(old: Sequence[float], new: Sequence[float], moves: Sequence[Optional[float]],
                          cap: float = 0.95) -> List[Tuple[str, str, bool]]:
    """The four registered bars for finra_short_volume as (bar, measured value, passed)."""
    rho = spearman(old, new)
    cap_share = sum(1 for n in new if n >= cap - 1e-9) / len(new) if new else 1.0
    old_cap = sum(1 for o in old if o >= cap - 1e-9) / len(old) if old else 1.0
    top_new, top_old = top_share_mean(new, moves), top_share_mean(old, moves)
    ch = cut_changes(old, new)
    return [
        ("share on the 0.95 cap < 5%", f"{cap_share:.1%} (old {old_cap:.1%})", cap_share < 0.05),
        ("rank correlation with old >= 0.9", "n/a" if rho is None else f"{rho:.3f}", rho is not None and rho >= 0.9),
        ("top 1% by new keeps old top 1% mean |5-session move|",
         "n/a" if top_new is None or top_old is None else f"new {top_new:.4f} vs old {top_old:.4f}",
         top_new is not None and top_old is not None and top_new >= top_old),
        ("no event newly above a cut", f"{ch['newly_above_cut']} newly above, {ch['newly_below_cut']} newly below (informational)",
         ch["newly_above_cut"] == 0),
    ]


def news_verdicts(old: Sequence[float], new: Sequence[float], min_events: int = 1000,
                  cuts: Sequence[float] = ALERT_CUTS) -> List[Tuple[str, str, bool]]:
    """The two registered bars for alpaca_news: >= 10 distinct values over 1,000 events; no count above a cut changes."""
    distinct = len({round(x, 3) for x in new})
    moved = {c: v for c, v in counts_above_cut(old, new, cuts).items() if v[0] != v[1]}
    return [
        (f">= 10 distinct values over {min_events} events", f"{distinct} distinct over {len(new)} events",
         distinct >= 10 and len(new) >= min_events),
        ("no count above any cut changes", "none" if not moved else ", ".join(f">={c}: {a}->{b}" for c, (a, b) in sorted(moved.items())),
         not moved),
    ]


_SPEED_RE = re.compile(r"implies ([0-9.]+) kn")


def spoof_speed_knots(headline: str) -> Optional[float]:
    """The implied speed in a transit-spoof headline ("... implies 756.4 kn over 27.4 nm")."""
    m = _SPEED_RE.search(headline or "")
    return float(m.group(1)) if m else None


def vessel_spoof_score(speed_knots: Optional[float], base_knots: float = 50.0) -> float:
    """Proposal for the transit spoof (constant 0.82 today): 0.62 at the plausibility bound, rising by a decade of implied
    speed to 0.72 and by two decades to 0.82, which stays the ceiling so no cut is crossed upward. Old score without a speed."""
    if speed_knots is None or speed_knots <= 0 or not math.isfinite(speed_knots):
        return 0.82
    return round(0.62 + 0.20 * min(1.0, max(0.0, math.log10(speed_knots / base_knots)) / 2.0), 3)


def extended_hours_score(old: float, notional_usd: Optional[float], floor_usd: float = 250_000.0) -> float:
    """Proposal for extended-hours blocks (61% at 0.8 or more): the larger of nothing and the block's size on a log scale,
    0.50 at the floor to 0.95 at 100x the floor, capped at the old score so no cut is crossed upward."""
    if notional_usd is None or notional_usd <= 0 or not math.isfinite(notional_usd):
        return old
    size = 0.5 + 0.45 * min(1.0, max(0.0, math.log10(notional_usd / floor_usd)) / 2.0)
    return min(old, round(size, 3))        # never rounds an old score upward across a cut


def share_below(scores: Sequence[float], level: float) -> float:
    return sum(1 for x in scores if x < level) / len(scores) if scores else 0.0


def outcome_verdicts(old: Sequence[float], new: Sequence[float], moves: Sequence[Optional[float]],
                     min_events: int = 300, cap: float = 0.95) -> List[Tuple[str, str, bool]]:
    """Revised registered bars (13 Nov): Spearman of the score with the 5-session absolute move, new >= old, on at least
    `min_events` events with an outcome; then the cap, top-1% and no-newly-above-cut bars of short_volume_verdicts."""
    pairs = [(o, n, m) for o, n, m in zip(old, new, moves) if m is not None]
    rho_old = spearman([p[0] for p in pairs], [p[2] for p in pairs])
    rho_new = spearman([p[1] for p in pairs], [p[2] for p in pairs])
    ok = len(pairs) >= min_events and rho_old is not None and rho_new is not None and rho_new >= rho_old
    first = ("outcome Spearman new >= old on >= %d events" % min_events,
             f"n {len(pairs)}; new {'n/a' if rho_new is None else format(rho_new, '.4f')} vs old "
             f"{'n/a' if rho_old is None else format(rho_old, '.4f')}", ok)
    rest = [v for v in short_volume_verdicts(old, new, moves, cap) if not v[0].startswith("rank correlation")]
    return [first] + rest
