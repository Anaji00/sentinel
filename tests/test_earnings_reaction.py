"""Post-earnings drift becomes an event family, known only once the reaction has closed."""

import pathlib
import sys
from datetime import date, datetime, timedelta, timezone

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.agents.earnings_reaction import reactions  # noqa: E402
from shared.utils.signal_families import observations  # noqa: E402

DAYS = [date(2026, 9, 1) + timedelta(days=k) for k in range(10)]


def test_reaction_is_market_adjusted_and_stamped_at_the_second_close() -> None:
    bars = {"AAA": (DAYS, [100.0] * 4 + [110.0] * 6)}
    spy = (DAYS, [100.0] * 4 + [101.0] * 6)
    after_close = datetime(2026, 9, 3, 20, 5, tzinfo=timezone.utc)      # after day 2's close -> base day 2
    out = reactions([("AAA", after_close)], bars, spy)
    assert len(out) == 1
    r = out[0]
    assert abs(r["ear"] - (0.10 - 0.01)) < 1e-12
    assert r["known_at"].date() == DAYS[4] and r["known_at"].hour == 20      # 16:00 New York, EDT


def test_small_reactions_are_not_events_and_duplicates_collapse() -> None:
    bars = {"AAA": (DAYS, [100.0] * 4 + [100.5] * 6)}
    spy = (DAYS, [100.0] * 10)
    at = datetime(2026, 9, 3, 20, 5, tzinfo=timezone.utc)
    assert reactions([("AAA", at)], bars, spy) == []
    bars2 = {"AAA": (DAYS, [100.0] * 4 + [95.0] * 6)}
    out = reactions([("AAA", at), ("AAA", at + timedelta(minutes=5))], bars2, spy)
    assert len(out) == 1 and out[0]["ear"] < 0


def test_the_study_reads_it_as_drift_in_the_reactions_direction() -> None:
    row = {"type": "earnings_reaction", "event_id": "x", "created_at": datetime(2026, 9, 5, 20, tzinfo=timezone.utc),
           "primary_entity_id": "AAA", "financial_data": {"ticker": "AAA", "ear": -0.05}, "tags": []}
    obs = observations(row)
    assert [(o.family, o.direction) for o in obs] == [("earnings_reaction_down", -1)]


def test_a_reaction_whose_close_has_not_happened_is_not_an_event() -> None:
    """The daily table holds today's bar mid-session: its 'close' is not final."""
    bars = {"AAA": (DAYS, [100.0] * 4 + [110.0] * 6)}
    spy = (DAYS, [100.0] * 4 + [101.0] * 6)
    after_close = datetime(2026, 9, 3, 20, 5, tzinfo=timezone.utc)
    midday = datetime(2026, 9, 5, 16, 0, tzinfo=timezone.utc)           # day 4, before its 16:00 NY close
    assert reactions([("AAA", after_close)], bars, spy, as_of=midday) == []
    assert len(reactions([("AAA", after_close)], bars, spy, as_of=midday + timedelta(hours=5))) == 1
