"""init.sql and the migrations must not declare different retention.

`add_retention_policy(..., if_not_exists => TRUE)` keeps whichever policy got
there first and says nothing about the one it ignored. Migration 0020 asked for
400 days on `events`, init.sql had already asked for 90, and the deployment ran
90 for the life of the migration while the migration's own comment described it
as deliberately generous. Verified against
`timescaledb_information.jobs` on the running database: {"drop_after": "90 days"}.

A disagreement here is invisible at every layer -- the SQL succeeds, the
migration records as applied, and the only way to see it is to ask the database
what policy it actually holds.
"""
import pathlib
import re

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
INIT = (ROOT / "shared" / "db" / "init.sql").read_text(encoding="utf-8")
MIGRATE = (ROOT / "shared" / "db" / "migrate.py").read_text(encoding="utf-8")

POLICY_RE = re.compile(
    r"add_(retention|compression)_policy\(\s*'(\w+)'\s*,\s*INTERVAL\s*'([^']+)'"
)


def _policies(text):
    out = {}
    for kind, table, interval in POLICY_RE.findall(text):
        out.setdefault((kind, table), set()).add(interval)
    return out


def test_no_table_is_given_two_different_retention_intervals():
    merged = {}
    for src in (INIT, MIGRATE):
        for key, intervals in _policies(src).items():
            merged.setdefault(key, set()).update(intervals)
    conflicts = {k: sorted(v) for k, v in merged.items() if len(v) > 1}
    assert not conflicts, (
        f"the same policy is declared with different intervals: {conflicts}. "
        "if_not_exists => TRUE keeps the first and ignores the rest silently, "
        "so the database runs one of these and the file promises another."
    )


def test_events_retention_is_stated_once_and_is_ninety_days():
    intervals = _policies(INIT).get(("retention", "events"), set())
    intervals |= _policies(MIGRATE).get(("retention", "events"), set())
    assert intervals == {"90 days"}, intervals


def test_compression_is_always_shorter_than_retention():
    """Compressing after retention has dropped a chunk does nothing."""
    merged = {}
    for src in (INIT, MIGRATE):
        for (kind, table), intervals in _policies(src).items():
            merged.setdefault(table, {})[kind] = sorted(intervals)[0]

    def days(interval):
        n, unit = interval.split()
        return int(n) * {"days": 1, "day": 1}[unit]

    for table, kinds in merged.items():
        if "compression" in kinds and "retention" in kinds:
            assert days(kinds["compression"]) < days(kinds["retention"]), (
                f"{table}: compresses after {kinds['compression']} but is dropped "
                f"after {kinds['retention']}"
            )
