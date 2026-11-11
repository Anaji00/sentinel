"""
shared/utils/champion_gate.py

When an automated refit may replace the model that is live.

Two refits overwrote the live news models on weaker evidence than the models they replaced:
`services/correlation/news_model_job.py` stored every walk-forward report in the live key, including an unvalidated one
that switched live scoring off; `services/agents/news_model_refit.py` replaced the weights whenever the new fit cleared an
absolute bar (rank IC 0.02, t 2), however far below the incumbent it was.

A challenger replaces a champion only when it is validated, and its out-of-sample rank IC is not distinguishably worse:
at least the champion's less one standard error of the champion's own estimate (IC over its t). That is the same
reasoning as the 0.10 switch margin on the stock model (shared/utils/model_choice.py): two models inside each other's
noise are the same model, so the live one stays unless the newer is at least as good. A champion with no recorded IC is
replaced by any validated challenger. Brier skill (shared/utils/forecast_skill.py) applies to probability forecasts; these
models forecast returns, so rank IC is the comparison.

Pure: numbers in, a verdict and a reason out.
"""
from __future__ import annotations

import math
from typing import Optional, Tuple


def _ok(x: Optional[float]) -> bool:
    return x is not None and isinstance(x, (int, float)) and math.isfinite(x)


def challenger_may_replace(champion_ic: Optional[float], champion_t: Optional[float],
                           challenger_ic: Optional[float], challenger_validated: bool) -> Tuple[bool, str]:
    """(replace, reason)."""
    if not challenger_validated:
        return False, "challenger did not pass its own gate"
    if not _ok(challenger_ic):
        return False, "challenger has no out-of-sample IC"
    if not _ok(champion_ic):
        return True, "no champion IC on record"
    # Without a usable t the champion's error is unknown, so no tolerance is given: the challenger must match it.
    se = abs(champion_ic) / champion_t if _ok(champion_t) and champion_t and champion_t > 0 else 0.0
    if challenger_ic >= champion_ic - se:
        return True, f"challenger IC {challenger_ic:.4f} is within one standard error ({se:.4f}) of the champion's {champion_ic:.4f} or better"
    return False, f"challenger IC {challenger_ic:.4f} is more than one standard error ({se:.4f}) below the champion's {champion_ic:.4f}"


__all__ = ["challenger_may_replace"]
