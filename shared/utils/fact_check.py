"""
shared/utils/fact_check.py

Does what an agent wrote stand on what it was given?

Agents that answer without outside feedback drift, and their memories keep
their own mistakes (Round 19's research). The analyst already refuses an answer
whose cited fact numbers do not exist (shared/utils/analyst.py `verified`).
This is the same check for agents that write free text: every number in the
answer must appear in the prompt it was given, within a tolerance, and every
ticker it names must appear there or be a listed equity the caller knows.

  numbers   pulled from the answer, with $, % and thousands separators
            removed. A number counts as supported when some number in the
            source is within REL_TOL of it, or equals it as a percent and a
            fraction (12.5% vs 0.125). Small counts (0-10) and years are not
            claims and are skipped.
  tickers   the caller passes the symbols the answer names; each must appear
            in the source as a word, or be in `known`.

Pure: text in, report out.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Iterable, List, Optional, Sequence, Set

REL_TOL = 0.02
MIN_NUMBERS = 2              # fewer numbers than this cannot fail the number test
MAX_UNSUPPORTED_SHARE = 0.5

_NUM = re.compile(r"(?<![A-Za-z0-9_.])[-+]?\$?\d{1,3}(?:,\d{3})+(?:\.\d+)?%?|(?<![A-Za-z0-9_.])[-+]?\$?\d+(?:\.\d+)?%?")
_WORD = re.compile(r"[A-Z][A-Z0-9.\-]{0,9}")


@dataclass
class Report:
    numbers_checked: int = 0
    unsupported_numbers: List[float] = field(default_factory=list)
    unknown_tickers: List[str] = field(default_factory=list)

    @property
    def numbers_ok(self) -> bool:
        if self.numbers_checked < MIN_NUMBERS:
            return True
        return len(self.unsupported_numbers) / self.numbers_checked <= MAX_UNSUPPORTED_SHARE

    @property
    def ok(self) -> bool:
        return self.numbers_ok and not self.unknown_tickers

    def summary(self) -> str:
        parts = []
        if self.unsupported_numbers:
            parts.append(f"{len(self.unsupported_numbers)} of {self.numbers_checked} numbers not in the data")
        if self.unknown_tickers:
            parts.append("unknown: " + ", ".join(self.unknown_tickers[:5]))
        return "; ".join(parts) or "all cited numbers and tickers found in the data"


def numbers(text: str) -> List[float]:
    out = []
    for m in _NUM.finditer(text or ""):
        s = m.group(0).replace("$", "").replace(",", "")
        pct = s.endswith("%")
        try:
            v = float(s.rstrip("%"))
        except ValueError:
            continue
        out.append(v / 100.0 if pct else v)
    return out


def _trivial(x: float) -> bool:
    return (float(x).is_integer() and 0 <= x <= 10) or (float(x).is_integer() and 1900 <= x <= 2100)


def supported(x: float, pool: Sequence[float], rel: float = REL_TOL) -> bool:
    for p in pool:
        for a, b in ((x, p), (x * 100.0, p), (x, p * 100.0), (abs(x), abs(p))):
            if abs(a - b) <= rel * max(abs(b), 1e-9) or abs(a - b) < 1e-9:
                return True
    return False


def check(answer: str, source: str, tickers: Iterable[str] = (), known: Optional[Set[str]] = None) -> Report:
    """Numbers in `answer` and the `tickers` it names, against `source`."""
    pool = numbers(source)
    claims = [x for x in numbers(answer) if not _trivial(x)]
    words = set(_WORD.findall(source or ""))
    known = known or set()
    rep = Report(numbers_checked=len(claims))
    rep.unsupported_numbers = [x for x in claims if not supported(x, pool)]
    for t in tickers:
        s = str(t or "").strip().upper()
        if s and s not in words and s not in known:
            rep.unknown_tickers.append(s)
    return rep


__all__ = ["MAX_UNSUPPORTED_SHARE", "MIN_NUMBERS", "REL_TOL", "Report", "check", "numbers", "supported"]
