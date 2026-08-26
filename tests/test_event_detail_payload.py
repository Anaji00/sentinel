"""
tests/test_event_detail_payload.py

"DEEP DATABASE JSON PAYLOAD (LIVE): No structured detail available"

Reported as common across the console. Two independent causes, and either alone
was enough to empty the panel:

  1. The inspector fetched `/events/{event_id}`. That matches the
     `/events/{domain}` route, and an unrecognised "domain" fell through to the
     all-events branch -- so it returned a *list of fifty unrelated events*
     instead of the one requested. DataGrid flattens objects and returns nothing
     for an array (isPlainObject excludes them), so every inspector rendered the
     empty state. The route answered a question nobody asked rather than saying
     the caller had the wrong URL.

  2. The all-events query aliased `summary as domain_data` -- a text column in
     the field every consumer treats as the structured payload. A string
     flattens to nothing too, so panels reading `domain_data` were empty even
     when the row's real payload sat in the very columns being skipped. That
     branch also returned no `headline`.

Fixing the routing exposed that the sub-type interleaving ranked every matching
row: 131,937 for crypto. Bounded to a candidate pool and backed by partial
indexes, tradfi went from 15.56s to 0.10s and cyber from 18.23s to 0.28s.
"""

import re
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

ROUTE = ROOT / "services/api_gateway/routes/events.py"
FEED = ROOT / "frontend/src/components/IntelligenceFeed.tsx"
MIGRATE = ROOT / "shared/db/migrate.py"


def _code(path: Path) -> str:
    """Source with commentary stripped, but not string literals.

    These files document the defects by name, so matching raw text would assert
    against the explanation. Triple-quoted strings are deliberately kept for the
    Python route: the SQL lives in one, and stripping it removed everything the
    query assertions are about.
    """
    text = path.read_text(encoding="utf-8")
    if path.suffix == ".py":
        text = re.sub(r"^\s*#.*$", "", text, flags=re.M)
        # SQL comments live inside the kept string literals and explain the
        # very expressions these assertions forbid.
        return re.sub(r"^\s*--.*$", "", text, flags=re.M)
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.S)
    return re.sub(r"^\s*//.*$", "", text, flags=re.M)


# ── the payload must be the payload ──────────────────────────────────────────

def test_domain_data_is_never_a_summary_string():
    """`summary as domain_data` put prose in the structured-payload field."""
    code = _code(ROUTE)
    assert "summary as domain_data" not in code


def test_the_all_branch_returns_the_real_payload():
    code = _code(ROUTE)
    # The payload COALESCE, not the coordinate one that appears earlier.
    marker = "COALESCE(" + chr(10) + " " * 27 + "crypto_data"
    assert marker in code, "the all branch does not project a payload"
    block = code[code.index(marker):]
    for column in ("crypto_data", "prediction_market_data", "vessel_data",
                   "flight_data", "security_data", "financial_data"):
        assert column in block[:400], column


def test_both_branches_return_the_headline():
    """The all branch returned none, leaving the feed to reconstruct one."""
    code = _code(ROUTE)
    assert code.count("headline,") >= 2
    assert code.count("summary,") >= 2


# ── an id is not a domain ────────────────────────────────────────────────────

def test_an_event_id_is_refused_as_a_domain():
    code = _code(ROUTE)
    assert "_UUID_RE.match(domain)" in code
    assert "status_code=404" in code


def test_the_refusal_names_the_right_route():
    code = _code(ROUTE)
    block = code[code.index("_UUID_RE.match(domain)"):][:400]
    assert "/events/detail/" in block, "an error that does not say where to go is half an error"


def test_a_deliberate_4xx_is_not_reported_as_a_database_failure():
    code = _code(ROUTE)
    assert "except HTTPException:" in code
    assert code.index("except HTTPException:") < code.index('detail="Database query failed"')


def test_the_client_calls_the_detail_route():
    code = _code(FEED)
    assert "/events/detail/${encodeURIComponent(event.event_id)}" in code
    assert "`/events/${encodeURIComponent(event.event_id)}`" not in code


# ── the query must stay bounded ──────────────────────────────────────────────

def test_the_interleaving_runs_over_a_bounded_pool():
    """Ranking all 131,937 crypto rows to return fifty timed out at 90s."""
    code = _code(ROUTE)
    assert "candidate_pool" in code
    assert "WITH recent AS (" in code
    block = code[code.index("WITH recent AS ("):]
    assert "ORDER BY occurred_at DESC" in block[:300]
    assert "LIMIT {candidate_pool}" in block[:300]


def test_the_pool_is_large_enough_to_interleave():
    code = _code(ROUTE)
    assert "max(2000, limit * 40)" in code


def test_every_domain_payload_column_is_indexed():
    """Without these, finding the newest matching rows scans the hypertable."""
    code = MIGRATE.read_text(encoding="utf-8")
    block = code[code.index("0008_domain_payload_indexes"):]
    for column in ("financial_data", "crypto_data", "security_data",
                   "prediction_market_data", "vessel_data", "flight_data"):
        assert f"WHERE {column} IS NOT NULL" in block, column


def test_the_indexes_are_ordered_for_the_access_pattern():
    """The query wants "newest N carrying this payload"."""
    code = MIGRATE.read_text(encoding="utf-8")
    block = code[code.index("0008_domain_payload_indexes"):]
    assert block.count("(occurred_at DESC)") >= 6


# ── the client must tolerate what it is given ────────────────────────────────

def test_the_grid_renders_nothing_for_a_non_object():
    """An array or a string must not be mistaken for a payload."""
    grid = (ROOT / "frontend/src/components/ui/DataGrid.tsx").read_text(encoding="utf-8")
    assert "!Array.isArray(v)" in grid, "an array would flatten as an object"
