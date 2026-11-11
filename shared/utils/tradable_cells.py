"""
shared/utils/tradable_cells.py

Which proven reaction-study cells may reach the live idea layer (G4, approved 11 Nov 2026).

G4 re-scored the proven cells on liquid, borrowable names (reports/Event backtest scope.md, 7 Nov 2026). Five survived
with the registered bars (at least 20 independent units, |t| 2 or more, hit rate 53% or more, same sign): delisting
notices 30d, dilution 30d, insider opportunistic sells 30d, put sweeps 1d and 1h. The rest of the proven cells (two that
held only on liquid names, eight that failed, one that could not be tested, and every cell the deeper study added after
G4 ran, such as officer changes 1h) are SHADOW: still measured and recorded, never raised as a signal or counted as
evidence. An untested cell is shadow, not tradable: missing evidence is not a pass.

A surviving cell is raised only on an instrument that is itself liquid (trailing dollar volume at least LIQUID_MIN_USD,
the level of the 1,000th most liquid name when G4 ran), and a short only on one Alpaca marks shortable and easy to
borrow. Re-derive the list with scripts/audit_tradable_signals.py when the study changes.

Pure: names and numbers in, a verdict and a reason out.
"""
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

TRADABLE_CELLS = frozenset({
    ("8k_delisting_notice", "30d"),
    ("8k_dilution", "30d"),
    ("insider_opportunistic_sell", "30d"),
    ("options_put_sweep", "1d"),
    ("options_put_sweep", "1h"),
})
LIQUID_MIN_USD = 140.8e6
SHADOW = "shadow"


def is_tradable_cell(family: Optional[str], horizon: Optional[str]) -> bool:
    return (str(family), str(horizon)) in TRADABLE_CELLS


def gate_cell(cell: Mapping[str, Any]) -> Dict[str, Any]:
    """The cell as the live layer may see it: a proven cell outside the tradable list becomes verdict 'shadow'."""
    out = dict(cell)
    if out.get("verdict") == "proven" and not is_tradable_cell(out.get("family"), out.get("horizon")):
        out["shadow_of"] = "proven"
        out["verdict"] = SHADOW
    return out


def gate_cells(cells: Iterable[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    return [gate_cell(c) for c in cells]


def instrument_ok(avg_dollar_volume: Optional[float], direction: int,
                  asset: Optional[Sequence[Any]]) -> Tuple[bool, str]:
    """May a surviving cell be raised on this instrument? `asset` is asset_universe's [kind, shortable, easy_to_borrow, name]."""
    if avg_dollar_volume is None or avg_dollar_volume != avg_dollar_volume:
        return False, "no dollar-volume history"
    if avg_dollar_volume < LIQUID_MIN_USD:
        return False, f"trailing dollar volume ${avg_dollar_volume / 1e6:.0f}M is under ${LIQUID_MIN_USD / 1e6:.1f}M"
    if direction < 0:
        if not asset or len(asset) < 3:
            return False, "no borrow data for a short"
        if not (asset[1] and asset[2]):
            return False, "not shortable and easy to borrow"
    return True, "liquid" if direction >= 0 else "liquid and easy to borrow"


__all__ = ["LIQUID_MIN_USD", "SHADOW", "TRADABLE_CELLS", "gate_cell", "gate_cells", "instrument_ok", "is_tradable_cell"]
