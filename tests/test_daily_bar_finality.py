"""Today's daily bar is not stored until the session is over."""

import importlib.util
import pathlib
import sys
from datetime import date, datetime, timezone

ROOT = pathlib.Path(__file__).resolve().parents[1]
for p in (ROOT, ROOT / "services" / "collector-tradfi"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))


def _mod():
    spec = importlib.util.spec_from_file_location(
        "historical_backfill", ROOT / "services" / "collector-tradfi" / "historical_backfill.py")
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def test_a_bar_for_a_session_still_open_is_not_final() -> None:
    m = _mod()
    morning = datetime(2026, 10, 19, 13, 39, tzinfo=timezone.utc)          # 09:39 New York
    evening = datetime(2026, 10, 19, 20, 35, tzinfo=timezone.utc)          # 16:35 New York
    assert m.is_final(date(2026, 10, 16), morning)                          # Friday's bar: complete
    assert not m.is_final(date(2026, 10, 19), morning)                      # today's, nine minutes in
    assert m.is_final(date(2026, 10, 19), evening)
