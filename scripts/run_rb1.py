"""RB1 (MODEL_TRIALS.md, registered 6 Nov 2026, amendment RB1a 11 Nov): a plain momentum-minus-volatility rule through the B1 walk-forward
book on eight calendars. Read-only. Run in its own capped container, never inside factor-model, and not between 22:00 and 06:00 UTC.

    docker run --rm --memory 9g --network sentinel-network --env-file .env -v <repo>/shared:/app/shared:ro \
        -v <repo>/services:/app/services:ro -v <repo>/scripts:/app/scripts:ro -w /app -e PYTHONPATH=/app \
        sentinel-factor-model python scripts/run_rb1.py
"""
import asyncio
import json
import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from shared.db import get_redis, get_timescale  # noqa: E402
from shared.utils import factor_model as fm  # noqa: E402

USED = (0, 5, 10, 15)
FRESH = (2, 7, 12, 17)
B1_MEAN, B1_USED_MEAN, B1_FRESH = 0.40, 0.47, {2: 0.34, 7: 0.49, 12: 0.19, 17: 0.28}
SHORT_FEE_BPS = 44.0


def fee_adjusted_sharpe(net_bps: float, sr_period: float, horizon: int = 21, fee_bps: float = SHORT_FEE_BPS):
    """Net Sharpe after the short fee, from the report's net mean (bps a period) and per-period Sharpe; None if either is flat."""
    if not sr_period or sr_period == 0:
        return None
    sd = net_bps / sr_period
    if sd <= 0:
        return None
    return (net_bps - fee_bps) / sd * math.sqrt(252 / horizon)


async def main() -> None:
    from services.agents.factor_engine import FactorEngine

    db, redis = await get_timescale(), await get_redis()
    panel = await FactorEngine(db, redis)._panel()
    if panel is None:
        print("no panel")
        return
    panel = fm.tradable_subset(panel)
    feats = fm.features(panel)
    spec = fm.HORIZON_SPECS["monthly"]
    rows = {}
    for off in USED + FRESH:
        rep = fm.walk_forward(panel, "rb1_rule", spec=spec, feats=feats, offset=off)
        sharpe = fee_adjusted_sharpe(rep.ls_net_bps or 0.0, rep.sr_period or 0.0, spec.horizon)
        rows[off] = {"periods": rep.periods, "ic": rep.mean_ic, "net_bps": rep.ls_net_bps, "sharpe_no_fee": rep.net_sharpe,
                     "sharpe_after_fee": None if sharpe is None else round(sharpe, 2)}
        print(off, json.dumps(rows[off]), flush=True)
    vals = [r["sharpe_after_fee"] for r in rows.values() if r["sharpe_after_fee"] is not None]
    mean = sum(vals) / len(vals) if vals else None
    fresh = {o: rows[o]["sharpe_after_fee"] for o in FRESH}
    b1_leads_fresh = sum(1 for o in FRESH if fresh[o] is not None and B1_FRESH[o] - fresh[o] >= 0.15)
    print(json.dumps({"rb1_mean_sharpe": None if mean is None else round(mean, 2), "b1_mean": B1_MEAN,
                      "gap_b1_minus_rb1": None if mean is None else round(B1_MEAN - mean, 2),
                      "rb1_used_mean": round(sum(rows[o]["sharpe_after_fee"] or 0 for o in USED) / 4, 2),
                      "rb1_fresh": fresh, "b1_leads_by_0.15_on_fresh_calendars": b1_leads_fresh,
                      "reading": ("within 0.15: the model has not been shown to beat a rule" if mean is not None and B1_MEAN - mean < 0.15
                                  else "B1 leads by 0.15 or more on the mean" if mean is not None else "no result")}, indent=1))


if __name__ == "__main__":
    asyncio.run(main())
