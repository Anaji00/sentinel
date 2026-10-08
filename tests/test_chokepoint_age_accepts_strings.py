"""The chokepoint panel 500'd when the pooled client returned an ISO string."""

import pathlib
import sys
from datetime import datetime, timedelta, timezone

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.api_gateway.routes.chokepoints import _age_seconds  # noqa: E402


def test_iso_strings_are_aged_like_datetimes() -> None:
    ts = datetime.now(timezone.utc) - timedelta(hours=2)
    for value in (ts, ts.isoformat(), ts.isoformat().replace("+00:00", "Z")):
        age = _age_seconds(value)
        assert age is not None and 7100 < age < 7300


def test_unparseable_strings_have_no_age() -> None:
    assert _age_seconds("not a time") is None
    assert _age_seconds(None) is None
