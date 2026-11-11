"""
shared/utils/exposure_gate.py

A macro-themed alert on a stock needs a measured exposure to that theme.

"Oil Price Spike Leads to Energy Stock Selloff: NVDA" and 980 other stocks were published because the rule's trigger
could not name what it was about (reports/Divergence alert.md). The `macro_exposures` table holds, for 440 stocks, the
instruments whose daily change enters the stock's return with |t| >= 3 after controlling for the market
(shared/utils/macro_exposure.py). A stock with no row for the theme has no measured exposure to it: the alert passes only
with a row whose partial correlation is at least MIN_PARTIAL_CORR. Instruments themselves (futures, yields, FX, crypto)
and unnamed themes pass untouched.

Pure: a table in, a verdict and a reason out.
"""
from __future__ import annotations

from typing import Mapping, Optional, Sequence, Tuple

MIN_PARTIAL_CORR = 0.25

THEME_MACROS = {
    "yield": ("US2Y", "US10Y", "US30Y"), "yields": ("US2Y", "US10Y", "US30Y"), "treasury": ("US2Y", "US10Y", "US30Y"),
    "treasuries": ("US2Y", "US10Y", "US30Y"), "rates": ("US2Y", "US10Y", "US30Y"),
    "oil": ("CL=F", "BZ=F"), "crude": ("CL=F", "BZ=F"), "brent": ("BZ=F",), "wti": ("CL=F",),
    "energy": ("CL=F", "BZ=F", "NG=F"), "fuel": ("CL=F", "BZ=F"),
    "gold": ("GC=F",), "dollar": ("DXY",), "usd": ("DXY",), "natgas": ("NG=F",),
    "volatility": ("VIX",), "vix": ("VIX",),
}

# table: ticker -> {macro: partial correlation}
ExposureTable = Mapping[str, Mapping[str, float]]


def theme_of(text: Optional[str]) -> Optional[str]:
    words = {w.strip(".,:;()&'-").lower() for w in (text or "").replace("‑", " ").replace("-", " ").split()}
    for theme in THEME_MACROS:
        if theme in words:
            return theme
    return None


def is_instrument(entity_id: Optional[str]) -> bool:
    """Futures, yields, FX and crypto pairs are the macro series themselves, not stocks exposed to them."""
    e = str(entity_id or "").upper()
    return (not e or "=" in e or e in {"DXY", "VIX", "US2Y", "US10Y", "US30Y", "US02Y", "UNKNOWN", "HAWKES"}
            or e.endswith(("USDT", "USD", "PERP")))


def passes(headline_or_rule: Optional[str], entity_id: Optional[str], table: ExposureTable,
           min_pc: float = MIN_PARTIAL_CORR) -> Tuple[bool, str]:
    theme = theme_of(headline_or_rule)
    if theme is None:
        return True, "no macro theme named"
    if is_instrument(entity_id):
        return True, "the entity is a macro instrument"
    row = table.get(str(entity_id).upper(), {})
    best = max((abs(row[m]) for m in THEME_MACROS[theme] if m in row and row[m] == row[m]), default=0.0)
    if best >= min_pc:
        return True, f"measured exposure to {theme}: |partial correlation| {best:.2f}"
    return False, f"no measured exposure of {entity_id} to {theme} (best |partial correlation| {best:.2f}, bar {min_pc:.2f})"


def build_table(rows: Sequence[Mapping[str, object]]) -> dict:
    """Rows of (ticker, macro, partial_corr) into the lookup table; unusable rows are left out."""
    out: dict = {}
    for r in rows:
        try:
            pc = float(r["partial_corr"])  # type: ignore[arg-type]
        except (KeyError, TypeError, ValueError):
            continue
        if pc != pc:
            continue
        out.setdefault(str(r["ticker"]).upper(), {})[str(r["macro"])] = pc
    return out


__all__ = ["MIN_PARTIAL_CORR", "build_table", "passes", "theme_of"]
