"""Where trading signals and their study live in Redis.

Written by services/agents/signal_engine.py and read by the gateway's
/signals routes; declared once here so the two cannot drift apart.
"""

STUDY_KEY = "sentinel:signals:study"
ACTIVE_KEY = "sentinel:signals:active"
EVIDENCE_KEY = "sentinel:signals:evidence:{instrument}:{horizon}"
OUTCOMES_KEY = "sentinel:signals:outcomes"
CURSOR_KEY = "sentinel:signals:cursor"
# Tickers whose daily_bars history has been loaded years deep; the rest get a
# deep load before their routine top-up.
DAILY_DEEP_KEY = "sentinel:daily_bars:deep"
# The nightly cross-sectional model: its validation report and today's ranks.
FACTOR_REPORT_KEY = "sentinel:factor_model:report"
FACTOR_SCORES_KEY = "sentinel:factor_model:scores"
RECOMMENDATIONS_KEY = "sentinel:recommendations:latest"


def factor_scores_key(horizon: str) -> str:
    """The cross-sectional ranks for one horizon ("daily", "weekly", "monthly")."""
    return f"{FACTOR_SCORES_KEY}:{horizon}"
# Tickers whose 8-K history has been rebuilt from EDGAR into the archive.
EDGAR_8K_HISTORY_DONE_KEY = "sentinel:history:edgar8k:done"
# Cached evidence-convergence results, keyed by window; short-lived.
EVIDENCE_CACHE_PREFIX = "sentinel:signals:evidence:"
# Instruments the study could not measure for want of price bars, written by
# the study and read by collector-tradfi's backfill loop; and those already
# backfilled, so each is loaded in full only once.
NEEDS_BARS_KEY = "sentinel:signals:needs_bars"
BARS_BACKFILLED_KEY = "sentinel:signals:bars_backfilled"
MAX_NEEDS_BARS = 300

__all__ = [
    "ACTIVE_KEY", "BARS_BACKFILLED_KEY", "CURSOR_KEY", "DAILY_DEEP_KEY", "EDGAR_8K_HISTORY_DONE_KEY", "FACTOR_REPORT_KEY", "FACTOR_SCORES_KEY", "factor_scores_key", "RECOMMENDATIONS_KEY", "EVIDENCE_CACHE_PREFIX", "EVIDENCE_KEY", "MAX_NEEDS_BARS",
    "NEEDS_BARS_KEY", "OUTCOMES_KEY", "STUDY_KEY",
]
