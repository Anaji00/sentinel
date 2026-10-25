"""
shared/utils/corporate_actions.py

Which stocks' stored history went stale because of a corporate action. Pure;
the tradfi collector fetches Alpaca's corporate-actions feed and re-downloads
the adjusted history of each symbol named.

Bars are stored adjusted (`adjustment=all`) *as of the day they were loaded*.
A split, spin-off or dividend afterwards re-adjusts every earlier bar at the
vendor, but the daily top-up only rewrites the last ten days -- so DXJ's 3:1
split on 9 Oct 2026 left 178.22 -> 60.18 in the stored series, a -66% day that
never happened, while the vendor's own series reads 59.41 -> 60.18. A cash
dividend does the same on a smaller scale: each ex-date after loading shows
the dividend as a loss.
"""
from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any, Dict, Mapping, Optional

# Actions that change the adjusted price of every earlier bar.
SPLIT_TYPES = ("forward_split", "reverse_split", "unit_split", "stock_dividend", "spin_off")
DIVIDEND_TYPES = ("cash_dividend",)
PRICE_ACTION_TYPES = SPLIT_TYPES + DIVIDEND_TYPES

# The response groups by plural type name; these are the keys that carry a
# change to the adjusted price, and whether it is a split-like event (which
# also re-scales minute history) or a dividend (daily history only).
_GROUPS = {
    "forward_splits": True, "reverse_splits": True, "unit_splits": True,
    "stock_dividends": True, "spin_offs": True, "cash_dividends": False,
}
_TICKER = re.compile(r"^[A-Z][A-Z.]{0,5}$")     # not a CUSIP


def _d(x: Any) -> Optional[date]:
    try:
        return datetime.fromisoformat(str(x)[:10]).date() if x else None
    except ValueError:
        return None


def affected(body: Mapping[str, Any], today: Optional[date] = None) -> Dict[str, Dict[str, Any]]:
    """{symbol: {"ex_date": earliest ex-date, "split": bool}} for every ticker an
    action re-adjusts. A spin-off names its parent (`source_symbol`); splits and
    dividends name the stock (`symbol`, or `old_symbol` across a renaming
    reverse split). Actions dated in the future are not yet in any bar."""
    today = today or date.today()
    out: Dict[str, Dict[str, Any]] = {}
    for group, is_split in _GROUPS.items():
        for a in (body.get("corporate_actions") or {}).get(group) or []:
            ex = _d(a.get("ex_date"))
            if ex is None or ex > today:
                continue
            for key in ("symbol", "source_symbol", "old_symbol", "new_symbol"):
                sym = str(a.get(key) or "").upper()
                if not _TICKER.match(sym):
                    continue
                e = out.setdefault(sym, {"ex_date": ex, "split": False})
                e["ex_date"] = min(e["ex_date"], ex)
                e["split"] = e["split"] or is_split
    return out


__all__ = ["DIVIDEND_TYPES", "PRICE_ACTION_TYPES", "SPLIT_TYPES", "affected"]
