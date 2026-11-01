"""
shared/utils/exposure_lens.py

What the traded book is betting on without meaning to: its net sensitivity
to each macro instrument (from the measured exposures, shared/utils/
macro_exposure.py) and to the market, each stated as the book's move for a
standard shock, against a limit. B1 is built market-neutral from stock ranks;
nothing watched whether its names together leaned on rates or oil. On 30 Oct
2026 they barely did (a 25 bp move in the 10-year ~0.4% of the book). Pure.
"""
from __future__ import annotations

from typing import Dict, List, Mapping, Sequence

# A standard shock per instrument, in the units its exposure was measured in:
# basis points for yields, log returns for everything else.
SHOCKS: Dict[str, float] = {"US2Y": 25.0, "US10Y": 25.0, "US30Y": 25.0, "DXY": 0.02, "VIX": 0.20}
DEFAULT_SHOCK = 0.10                    # a 10% move in a commodity future
SHOCK_LABEL: Dict[str, str] = {"US2Y": "+25 bp", "US10Y": "+25 bp", "US30Y": "+25 bp", "DXY": "+2%", "VIX": "+20%"}
LIMIT = 0.01                            # a book move over 1% for one standard shock is flagged
MARKET_SHOCK = 0.05                     # SPY -5%
MARKET_LIMIT = 0.01


def net_exposures(weights: Mapping[str, float], betas: Sequence[Mapping[str, object]]) -> Dict[str, float]:
    """{instrument: sum of weight x beta} over the book's names with a measured exposure."""
    out: Dict[str, float] = {}
    for b in betas:
        w = weights.get(str(b["ticker"]).upper())
        if w:
            m = str(b["macro"])
            out[m] = out.get(m, 0.0) + w * float(b["beta"])        # type: ignore[arg-type]
    return out


def lens(net: Mapping[str, float], market_beta: float, coverage: Mapping[str, int]) -> List[Dict[str, object]]:
    """Rows: instrument, shock, the book's move for it, names measured, flagged."""
    rows: List[Dict[str, object]] = [{"instrument": "SPY (market)", "shock": f"-{MARKET_SHOCK:.0%}",
                                      "book_move": round(-MARKET_SHOCK * market_beta, 5), "names": None,
                                      "flag": abs(MARKET_SHOCK * market_beta) > MARKET_LIMIT}]
    for m, n in sorted(net.items(), key=lambda kv: -abs(kv[1] * SHOCKS.get(kv[0], DEFAULT_SHOCK))):
        move = n * SHOCKS.get(m, DEFAULT_SHOCK)
        rows.append({"instrument": m, "shock": SHOCK_LABEL.get(m, f"+{DEFAULT_SHOCK:.0%}"), "book_move": round(move, 5),
                     "names": coverage.get(m, 0), "flag": abs(move) > LIMIT})
    return rows


def book_weights(longs: Sequence[str], shorts: Sequence[str]) -> Dict[str, float]:
    """Equal weight within each leg, +1 long and -1 short in total (B1's construction)."""
    w: Dict[str, float] = {}
    for t in longs:
        w[t.upper()] = w.get(t.upper(), 0.0) + 1.0 / len(longs)
    for t in shorts:
        w[t.upper()] = w.get(t.upper(), 0.0) - 1.0 / len(shorts)
    return w


__all__ = ["LIMIT", "SHOCKS", "book_weights", "lens", "net_exposures"]
