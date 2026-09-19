"""Keys holding what the agent tier has concluded, named once.

Each of these was written by exactly one service and read by nothing -- they
appeared a single time in the repository, at the line that wrote them. Giving
them a reader (`/agents/conclusions`) immediately created the defect the
key-convention ratchet exists to catch: the same literal typed out in two
files, which is how this platform previously lost a watchlist lookup.

So the reader and the writer import the same name. That is the whole point of
the convention, and adding a reader is exactly the moment it starts to matter.
"""
from __future__ import annotations

# The macro engine's persisted conclusions.
MACRO_RATES_REGIME_KEY = "sentinel:macro:latest_rates_regime"
MACRO_SPREAD_2Y10Y_KEY = "sentinel:macro:spread_2y10y"
MACRO_INVERSE_CORRELATION_PREFIX = "sentinel:macro:inverse_correlation:"

# The cross-domain excitation matrix the correlation service learns.
HAWKES_BRANCHING_RATIOS_KEY = "sentinel:hawkes:branching_ratios"

# What the historical backfill last fetched, and how much of it.
TRADFI_BACKFILL_REPORT_KEY = "sentinel:backfill:tradfi:last"

# Per-agent correlation analysis written by the reasoning service.
AGENT_CORRELATION_ANALYSIS_PREFIX = "sentinel:agents:correlation_analysis:"

__all__ = [
    "MACRO_RATES_REGIME_KEY",
    "MACRO_SPREAD_2Y10Y_KEY",
    "MACRO_INVERSE_CORRELATION_PREFIX",
    "HAWKES_BRANCHING_RATIOS_KEY",
    "TRADFI_BACKFILL_REPORT_KEY",
    "AGENT_CORRELATION_ANALYSIS_PREFIX",
]
