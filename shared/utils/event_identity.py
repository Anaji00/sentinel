"""
shared/utils/event_identity.py

One observation, one event_id.

An enricher that turns one raw message into one normalized event should inherit
`raw.event_id` -- that inheritance is what lets a stored event be traced back to
the message it came from, and most of the enrichers in this tree do it
correctly.

Three do it inside a loop that emits several events. Measured on the live table
over seven days: `coinbase_candles` / `market_anomaly` held 20,302 rows under
12,246 event_ids, because one batch of OHLCV bars fans out into one event per
bar and every one of them inherited the batch's id. The rows are genuinely
different observations -- different `occurred_at`, different anomaly scores --
sharing an identity:

    5d254dba-...  03:05:09  score 0.4343
    5d254dba-...  03:09:09  score 0.2951
    5d254dba-...  03:15:08  score 0.2156     ... six in all

The composite primary key `(event_id, occurred_at)` holds, so the database is
not corrupted. What breaks is everything that treats `event_id` as a handle:

  - The vector index keys points on it. Measured: 40 shared ids covering 93
    database rows resolve to 14 points, so the other observations have no
    vector of their own and cannot be retrieved as precedents.
  - `/events/detail/{event_id}` can only return one of them.
  - A correlation's `trigger_event_id` and `supporting_event_ids` name a set
    rather than an observation.

`derive_event_id` gives each observation its own id, derived rather than random
so that reprocessing the same bar produces the same id -- which is what keeps
the pipeline idempotent on replay, and the dead-letter replay path now depends
on that.
"""

from __future__ import annotations

import uuid
from typing import Any

# A fixed namespace, so an id derived today and the same id derived after a
# redeploy are equal. Generated once and written down rather than computed, for
# exactly the reason every other constant in this tree is.
SENTINEL_EVENT_NAMESPACE = uuid.UUID("6f3d2b1a-9c47-5e08-b1d2-3a7c4e5f6081")


def derive_event_id(parent_id: Any, *discriminators: Any) -> str:
    """A stable id for one observation fanned out of `parent_id`.

    The discriminators must together distinguish the observations in a batch --
    for a candle that is the instrument and the bar's timestamp. Passing none is
    an error rather than a silent return of the parent, because the caller that
    forgets is exactly the caller this exists for.
    """
    if not discriminators:
        raise ValueError(
            "derive_event_id needs at least one discriminator; without one it "
            "would hand every observation in the batch the same id, which is "
            "the defect it exists to fix"
        )
    parts = [str(parent_id or "")] + [str(d) for d in discriminators]
    return str(uuid.uuid5(SENTINEL_EVENT_NAMESPACE, "|".join(parts)))
