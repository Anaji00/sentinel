"""
tests/test_db_write_batch_integrity.py

One malformed identifier destroying an entire write batch.

Measured live, with the market open and the collector plainly emitting blocks:

    FATAL DB WRITE ERROR: invalid input for query argument $29 ...
    invalid UUID 'granger:AAPL:MSFT:lag2': length must be between 32..36
    characters. Routing batch to DLQ to prevent data loss.

    HEARTBEAT | processed=4838 errors=2130

A 44% loss rate, and equity_block events had vanished from the database
entirely while `docker logs` showed QQQ, TSLA and GOOG blocks arriving second by
second.

The chain: statistical_discovery mints readable identifiers -- "granger:
AAPL:MSFT:lag2", "corr:stat:NVDA:TSM" -- and caches them in Redis; the tradfi
enricher attaches them to every event for a ticker that has one; db_writer
passes them to events.correlation_ids, which is uuid[]. asyncpg rejects the
whole executemany batch on the first bad value, so one discovered correlation
made every event batched beside it unwritable.

The shape worth remembering: the failure was not in the events that were wrong.
It was in the events that were fine and were thrown away for sharing a batch
with one that wasn't.
"""

import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pytest  # noqa: E402

from services.enrichment.db_writer import _as_uuid  # noqa: E402


# -- the identifiers that broke it --------------------------------------------

@pytest.mark.parametrize(
    "semantic",
    [
        "granger:AAPL:MSFT:lag2",
        "corr:stat:NVDA:TSM",
        "granger:NVDA:AMD:lag1",
        "",
        "not-a-uuid",
    ],
)
def test_a_semantic_identifier_becomes_a_valid_uuid(semantic):
    uuid.UUID(_as_uuid(semantic))          # raises if it is not one


def test_a_real_uuid_passes_through_untouched():
    """Coercion must not rewrite identifiers that were already correct."""
    real = str(uuid.uuid4())
    assert _as_uuid(real) == real


def test_the_mapping_is_stable():
    """Deterministic, so any join on the identifier still holds. A random UUID
    would satisfy the column and silently destroy the linkage."""
    assert _as_uuid("granger:AAPL:MSFT:lag2") == _as_uuid("granger:AAPL:MSFT:lag2")


def test_distinct_identifiers_stay_distinct():
    """Collapsing two correlations onto one UUID would merge unrelated links."""
    assert _as_uuid("corr:stat:A:B") != _as_uuid("corr:stat:A:C")
    assert _as_uuid("granger:A:B:lag1") != _as_uuid("granger:A:B:lag2")


@pytest.mark.parametrize("junk", [None, 0, 12.5, [], {}])
def test_coercion_never_raises(junk):
    """A writer that throws while preparing a row loses the batch it was
    preparing -- which is the failure being fixed, reintroduced one layer up."""
    uuid.UUID(_as_uuid(junk))


# -- the batch must survive one bad row ---------------------------------------

def test_correlation_ids_are_coerced_before_the_write():
    source = (ROOT / "services/enrichment/db_writer.py").read_text(encoding="utf-8")
    assert "[_as_uuid(c) for c in (getattr(e, 'correlation_ids', None) or [])]" in source


def test_the_event_id_uses_the_same_rule():
    """Two coercion paths would drift; event_id had this treatment already and
    correlation_ids did not, which is exactly how the gap arose."""
    source = (ROOT / "services/enrichment/db_writer.py").read_text(encoding="utf-8")
    assert "event_id = _as_uuid(e.event_id)" in source


def test_a_none_correlation_list_does_not_crash():
    """getattr default of [] is not enough: the attribute can exist and be None."""
    class _E:
        correlation_ids = None

    assert [_as_uuid(c) for c in (getattr(_E(), "correlation_ids", None) or [])] == []
