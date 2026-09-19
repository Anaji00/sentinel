"""Redis keys the filings path writes, named once.

`sentinel:13f:prominent_ciks` is the set the filings collector builds as it
decides which institutions are worth following. It was written and read by
nothing until `/agents/conclusions` gave it a reader -- and giving it one
immediately created the defect the key-convention ratchet exists to catch: the
same literal typed out in two files, which is how this platform previously lost
a watchlist lookup to a missing `.strip()`.
"""
from __future__ import annotations

PROMINENT_CIKS_KEY = "sentinel:13f:prominent_ciks"

__all__ = ["PROMINENT_CIKS_KEY"]
