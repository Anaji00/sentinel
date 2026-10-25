"""Daily company news: parsing, point-in-time features, events and families."""

import pathlib
import sys
from datetime import date, datetime, timezone

import numpy as np

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from shared.utils import signal_families as sf  # noqa: E402
from shared.utils import stock_news as sn  # noqa: E402


def test_an_alpaca_item_parses_to_a_row() -> None:
    row = sn.parse_item({"id": 51234, "headline": " Micron falls on weak guide ", "created_at": "2026-10-15T14:00:00Z",
                         "symbols": ["MU", "brk/b"], "source": "benzinga", "summary": "x", "url": "u"})
    assert row["id"] == 51234 and row["symbols"] == ["BRK.B", "MU"] and row["headline"] == "Micron falls on weak guide"
    assert row["created_at"] == datetime(2026, 10, 15, 14, tzinfo=timezone.utc)
    assert sn.parse_item({"id": "x", "headline": "h", "created_at": "2026-10-15T14:00:00Z"}) is None


def test_daily_features_count_news_at_the_next_session_and_average_tone() -> None:
    sessions = [date(2026, 10, 9), date(2026, 10, 12), date(2026, 10, 13)]
    # Saturday's news rolls to Monday the 12th; a roundup about many stocks is left out upstream (SQL).
    rows = [(date(2026, 10, 9), "AAA", 0.8, 1), (date(2026, 10, 10), "AAA", -0.4, 2), (date(2026, 10, 13), "BBB", 1.0, 1)]
    f = sn.daily_features(sessions, ["AAA", "BBB"], rows)
    assert np.isclose(f["news_tone_5"][1, 0], (0.8 - 0.4) / 3)          # Friday and Saturday by Monday
    assert np.isnan(f["news_tone_5"][0, 1]) and np.isclose(f["news_tone_5"][2, 1], 1.0)
    assert np.isclose(f["news_count_21"][2, 0], np.log1p(3))


def test_a_strong_single_stock_article_becomes_a_family_observation() -> None:
    ev = sn.event_row({"symbols": ["MU"]}, 0.03, 0.95)
    assert ev["side"] == "negative" and "exposes:MU" in ev["tags"]
    assert sn.event_row({"symbols": ["MU"]}, 0.4, 0.3) is None                     # not strong enough
    assert sn.event_row({"symbols": ["A", "B", "C"]}, 0.95, 0.0) is None           # a roundup
    row = {"type": "headline", "source": sn.EVENT_SOURCE, "event_id": "x", "headline": "h",
           "created_at": datetime(2026, 10, 15, 14, tzinfo=timezone.utc), "tags": ev["tags"], "sentiment": ev["tone"]}
    assert [(o.family, o.instrument, o.direction) for o in sf.observations(row)] == [("stock_news_negative", "MU", -1)]


def test_the_news_fade_counts_only_positive_articles_from_its_registration() -> None:
    ev = sn.event_row({"symbols": ["MU"]}, 0.97, 0.01)
    assert ev["side"] == "positive"

    def fams(when):
        row = {"type": "headline", "source": sn.EVENT_SOURCE, "event_id": "x", "headline": "h",
               "created_at": when, "tags": ev["tags"], "sentiment": ev["tone"]}
        return [(o.family, o.direction) for o in sf.observations(row)]

    assert fams(datetime(2026, 10, 15, 14, tzinfo=timezone.utc)) == [("stock_news_positive", 1)]   # history: no vote
    assert fams(sf.NEWS_FADE_REGISTERED) == [("stock_news_positive", 1), ("stock_news_fade", -1)]
    assert "stock_news_fade" in sf.FAMILY_DESCRIPTIONS
