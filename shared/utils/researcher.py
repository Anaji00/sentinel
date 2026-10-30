"""
shared/utils/researcher.py

The researcher's arithmetic, kept pure (services/agents/researcher.py does the
I/O). Each week it screens candidate stock signals -- the stored features in
shared/utils/stock_features.REGISTRY and combinations of them that the judgment
model proposes -- and queues the few that survive for a pre-registered trial.
It never changes the live model: adoption stays with the trial harness.

  candidate   a stored feature, or one of a fixed grammar of combinations of
              two (product, difference, one gated by the other). The model can
              only choose names and operators; nothing it writes is executed.
  screen      monthly cross-sectional rank IC against the next month's return,
              and the incremental IC after taking out 12-1 momentum and 1-month
              reversal (what a new signal must add to the obvious ones).
  honesty     every test ever run counts: Benjamini-Hochberg over the whole
              history, and a t of at least 3 (Harvey, Liu & Zhu 2016) before a
              candidate is queued.
  bandit      which family the model proposes into next is Thompson-sampled
              from each family's record of producing a candidate that clears a
              looser bar (|t| >= 2), so effort follows what has paid.
"""
from __future__ import annotations

import math
import random
from typing import Dict, List, Literal, Mapping, Optional, Sequence, Tuple

import numpy as np
from pydantic import BaseModel, Field

MIN_NAMES = 30                  # a month with fewer names has no IC
MIN_MONTHS = 24
QUEUE_T = 3.0                   # Harvey, Liu & Zhu's bar for a new factor
QUEUE_Q = 0.10
BANDIT_T = 2.0                  # a family "paid" when a proposal clears this
NW_LAGS = 2
OPS = ("raw", "product", "diff", "gate")


class Idea(BaseModel):
    op: Literal["product", "diff", "gate"]
    a: str
    b: str
    rationale: str = Field(default="", max_length=300)


class Ideas(BaseModel):
    ideas: List[Idea] = Field(default_factory=list, max_length=3)


def key(op: str, a: str, b: Optional[str] = None) -> str:
    return a if op == "raw" else f"{op}({a},{b})"


def valid(idea: Mapping[str, object], allowed: Sequence[str]) -> Optional[Tuple[str, str, str]]:
    """(op, a, b) when the idea names two different known features and a known
    operator, else None."""
    op, a, b = str(idea.get("op") or ""), str(idea.get("a") or ""), str(idea.get("b") or "")
    if op not in OPS[1:] or a not in allowed or b not in allowed or a == b:
        return None
    return op, a, b


def cs_rank(x: np.ndarray) -> np.ndarray:
    """Each row's values as centred ranks in [-0.5, 0.5]; NaN stays NaN. Ties
    share their average rank: an 8-K flag is zero for almost every stock, and
    breaking those ties by column order ranked the zeros alphabetically."""
    out = np.full(x.shape, np.nan)
    for i, row in enumerate(x):
        ok = np.isfinite(row)
        n = int(ok.sum())
        if n >= 2:
            v = row[ok]
            r = np.empty(n)
            r[np.argsort(v, kind="stable")] = np.arange(n, dtype=float)
            _, inv = np.unique(v, return_inverse=True)
            r = (np.bincount(inv, r) / np.bincount(inv))[inv]
            out[i, ok] = r / (n - 1) - 0.5
    return out


def build(op: str, a: np.ndarray, b: Optional[np.ndarray] = None) -> np.ndarray:
    """The candidate's panel from its parts' panels (dates x tickers)."""
    ra = cs_rank(a)
    if op == "raw":
        return ra
    assert b is not None
    rb = cs_rank(b)
    if op == "product":
        return ra * rb
    if op == "diff":
        return ra - rb
    if op == "gate":
        return np.where(np.isfinite(b), np.where(b > 0, ra, 0.0), np.nan)
    raise ValueError(op)


def _corr(x: np.ndarray, y: np.ndarray) -> float:
    xc, yc = x - x.mean(), y - y.mean()
    d = math.sqrt(float((xc * xc).sum() * (yc * yc).sum()))
    return float((xc * yc).sum() / d) if d > 0 else float("nan")


def ic_series(signal: np.ndarray, fwd: np.ndarray, controls: Sequence[np.ndarray] = ()) -> List[float]:
    """Per month, the rank correlation of the signal with the next month's
    return; with controls, of the signal's residual after regressing its rank
    on theirs (the incremental IC). Months with too few names are skipped."""
    s, f = cs_rank(signal), cs_rank(fwd)
    cs = [cs_rank(c) for c in controls]
    out = []
    for i in range(s.shape[0]):
        ok = np.isfinite(s[i]) & np.isfinite(f[i])
        for c in cs:
            ok &= np.isfinite(c[i])
        if ok.sum() < MIN_NAMES or np.nanstd(s[i][ok]) == 0:
            continue
        x = s[i][ok]
        if cs:
            X = np.column_stack([np.ones(int(ok.sum()))] + [c[i][ok] for c in cs])
            beta, *_ = np.linalg.lstsq(X, x, rcond=None)
            x = x - X @ beta
        v = _corr(x, f[i][ok])
        if math.isfinite(v):
            out.append(v)
    return out


def nw_t(xs: Sequence[float], lags: int = NW_LAGS) -> float:
    """Newey-West t of the mean (Bartlett weights)."""
    n = len(xs)
    if n < 3:
        return 0.0
    x = np.asarray(xs, dtype=float)
    e = x - x.mean()
    var = float(e @ e) / n
    for k in range(1, min(lags, n - 1) + 1):
        var += 2 * (1 - k / (lags + 1)) * float(e[k:] @ e[:-k]) / n
    return float(x.mean() / math.sqrt(var / n)) if var > 0 else 0.0


def p_two_sided(t: float) -> float:
    return math.erfc(abs(t) / math.sqrt(2))


def summarize(ics: Sequence[float], inc: Sequence[float]) -> Dict[str, float]:
    return {"months": len(ics), "ic": round(float(np.mean(ics)), 4) if ics else 0.0,
            "t": round(nw_t(ics), 2), "inc_ic": round(float(np.mean(inc)), 4) if inc else 0.0,
            "inc_t": round(nw_t(inc), 2), "inc_months": len(inc)}


def judge(board: Dict[str, Dict[str, object]], tests_run: int) -> None:
    """Adds q (over every test ever run, not just this week's) and a verdict
    to each row, in place."""
    from shared.utils.reaction_study import benjamini_hochberg

    rows = [r for r in board.values() if int(r.get("inc_months") or 0) >= MIN_MONTHS]
    ps = [p_two_sided(float(r["inc_t"])) for r in rows]
    # Tests run before and since dropped off the board still count against it:
    # pad with p = 1, which costs the survivors without being one of them.
    qs = benjamini_hochberg(ps + [1.0] * max(0, tests_run - len(ps)))[:len(ps)]
    for r, q in zip(rows, qs):
        r["q"] = round(q, 4)
        r["verdict"] = ("queue for a trial" if q <= QUEUE_Q and abs(float(r["inc_t"])) >= QUEUE_T
                        else "promising" if abs(float(r["inc_t"])) >= BANDIT_T else "nothing new")
    for r in board.values():
        r.setdefault("q", None)
        r.setdefault("verdict", "too few months")


def pick_family(record: Mapping[str, Tuple[int, int]], families: Sequence[str], rng: random.Random) -> str:
    """Thompson sampling on Beta(1 + wins, 1 + losses) per family."""
    best, draw = families[0], -1.0
    for fam in families:
        w, l = record.get(fam, (0, 0))
        d = rng.betavariate(1 + w, 1 + l)
        if d > draw:
            best, draw = fam, d
    return best


def prompt(family: str, features: Mapping[str, str], board: Sequence[Mapping[str, object]]) -> Tuple[str, str]:
    """The proposal request: the features (with what they measure) and what has
    been tried, so the model proposes something new."""
    system = ("You are a quantitative researcher. Propose up to three new stock signals, each a combination of "
              "two of the listed features with one operator: product (both high), diff (one high, the other low), "
              "or gate (feature a, only where feature b is positive). Prefer combinations with an economic reason, "
              f"at least one feature from the {family} family, and nothing already tried. Answer in JSON only.")
    feats = "\n".join(f"- {k}: {v}" for k, v in features.items())
    tried = "\n".join(f"- {r['candidate']}: incremental t {r['inc_t']}" for r in board[:30]) or "- none yet"
    user = (f"Features:\n{feats}\n\nAlready tried:\n{tried}\n\n"
            '{"ideas": [{"op": "product"|"diff"|"gate", "a": "<feature>", "b": "<feature>", "rationale": "..."}]}')
    return system, user


__all__ = ["BANDIT_T", "Idea", "Ideas", "MIN_MONTHS", "QUEUE_Q", "QUEUE_T", "build", "cs_rank", "ic_series",
           "judge", "key", "nw_t", "p_two_sided", "pick_family", "prompt", "summarize", "valid"]
