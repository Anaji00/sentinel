"""Text shortening that admits it happened.

Eleven hard slices across the agent layer land on model-generated prose, and
eight carried no marker at all. The live cost was readable in the one active
bulletin, which ended "...an aviation alert with a specific aircraft ID and
position details along with an" -- an `[:80]` cut mid-clause, with nothing to
say a cut had occurred.

A reader cannot tell a truncated summary from a model that stopped early, and
neither can the consensus engine that fuses these bulletins: one is a display
artefact and the other is a failed inference, and they were being rendered
identically. The marker is the whole point.
"""

from typing import Final, Optional

# What a cut looks like. A single character, so it costs almost nothing against
# the budget it is protecting, and is unambiguous in a log, a prompt or a UI.
ELLIPSIS: Final[str] = "…"


def clip(text: Optional[str], limit: int, marker: str = ELLIPSIS) -> str:
    """`text` shortened to `limit` characters, marked when anything was removed.

    The marker is counted inside the limit rather than appended past it, so the
    result never exceeds what the caller budgeted -- the reason several of these
    sites were slicing bare in the first place was to respect a hard ceiling.
    Text that already fits is returned unchanged and unmarked.
    """
    if text is None:
        return ""
    value = str(text)
    if limit <= 0:
        return ""
    if len(value) <= limit:
        return value
    if limit <= len(marker):
        return value[:limit]
    return value[: limit - len(marker)].rstrip() + marker
