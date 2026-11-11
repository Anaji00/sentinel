"""Symbols that may become an instrument in the signal study, and ones that may not.

G3 (reports/Unmeasurable signals.md): of the instruments the reaction study could not price, most were not
securities at all -- filings without a listing ("NONE", "N/A"), several tickers joined in one field
("HEI, HEI.A", "BCDA;BCDAW") and crypto pairs on company-news tags ("BTCUSD"). Pure functions; no I/O.
"""
from __future__ import annotations

import re
from typing import Iterable, List

from shared.utils.form4 import listed_symbol

_SEPARATORS = re.compile(r"[,;/\s]+")
# A spot crypto pair as news vendors write it: BTCUSD, ETHUSDT. Needs two or more letters before the quote
# currency, so the three-letter equity ticker USD (ProShares Ultra Semiconductors) is untouched.
_CRYPTO_PAIR = re.compile(r"^[A-Z0-9]{2,10}(USD|USDT|USDC)$")


def is_crypto_pair(symbol: str) -> bool:
    """True for a crypto quote pair, which the equity study cannot price."""
    return bool(_CRYPTO_PAIR.match(symbol.strip().upper()))


def clean_symbols(raw: str) -> List[str]:
    """The tradable symbols named by one free-text field, in order and without repeats.

    Splits on commas, semicolons, slashes and spaces; drops placeholders for "no listing" and crypto pairs.
    Futures (`CL=F`) and share classes (`BRK.B`) pass unchanged.
    """
    out: List[str] = []
    if not listed_symbol(str(raw or "")):       # the whole field is a placeholder ("N/A" must not split into N and A)
        return out
    for part in _SEPARATORS.split(str(raw or "")):
        sym = listed_symbol(part)
        if sym and not is_crypto_pair(sym) and sym not in out:
            out.append(sym)
    return out


def clean_all(values: Iterable[str]) -> List[str]:
    """clean_symbols over several fields, keeping order and dropping repeats."""
    out: List[str] = []
    for v in values:
        for sym in clean_symbols(v):
            if sym not in out:
                out.append(sym)
    return out


__all__ = ["clean_all", "clean_symbols", "is_crypto_pair"]
