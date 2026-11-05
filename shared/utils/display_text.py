"""
shared/utils/display_text.py

Headlines as a person reads them. About 25 places in the enrichers open a headline
with an emoji, and block trades also end in "| Anomaly: 0.91", which the feed
already shows as a badge. Stripped here, once, rather than at each of them.
"""
from __future__ import annotations

import re

# Emoji and pictographs, the bitcoin sign, and the joiners that compose them.
_LEADING_EMOJI = re.compile("^[\U0001F300-\U0001FAFF☀-➿₿️‍\\s]+")
_SCORE_SUFFIX = re.compile(r"\s*\|\s*Anomaly:\s*\d+(?:\.\d+)?", re.IGNORECASE)


def plain_headline(text: str) -> str:
    """The headline without a leading emoji or a repeated score. Empty stays empty."""
    return _SCORE_SUFFIX.sub("", _LEADING_EMOJI.sub("", text or "")).strip()


__all__ = ["plain_headline"]
