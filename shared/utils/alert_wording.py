"""Wording of alerts whose family measures the size of a move, not its sign (layer audit 18 Nov, finding 1; wording only, tier and delivery unchanged). Pure."""
from __future__ import annotations

from typing import Optional

# Families graded "moves more": the date-clustered tests found a size-of-move effect and no direction.
UNDIRECTED_FAMILIES = ("peer cointegration",)
NOTE = "Direction is not predicted: this alert flags an unusually large move in the stock, not whether it goes up or down."


def direction_note(rule_name: object) -> Optional[str]:
    """The one-line note for an undirected-family alert, else None."""
    name = str(rule_name or "").lower()
    return NOTE if any(f in name for f in UNDIRECTED_FAMILIES) else None


__all__ = ["NOTE", "UNDIRECTED_FAMILIES", "direction_note"]
