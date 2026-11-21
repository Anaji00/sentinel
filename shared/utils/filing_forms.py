"""Which EDGAR forms carry no information the platform can use.

Pure and import-free so the collector, its tests and the replay script share one
definition. The per-CIK poller has skipped 424B2 since the 765-of-1,189-filings
week; the firehose did not, and in the week of 11-18 Nov 1,310 of its 3,081
events were 424B2 structured-note pricing supplements at a constant 0.20.
"""

# 424B3 (merger and resale prospectuses), 424B4 (IPO and follow-on pricing) and
# 424B5 (shelf takedowns, often equity and so a dilution signal) are kept.
LOW_VALUE_FORM_PREFIXES = ("424B2",)


def is_low_value_form(form: str) -> bool:
    """True for a form that is routine paperwork with no event content."""
    return str(form or "").strip().upper().startswith(LOW_VALUE_FORM_PREFIXES)
