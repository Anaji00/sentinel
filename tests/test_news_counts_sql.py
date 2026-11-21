"""The 6-hour and 20-day article counts failed on every event (110 swallowed errors in half a day, 13 Nov 2026): `$2 - INTERVAL` left the parameter's type to be inferred
as an interval, so the comparison with created_at did not exist. Every use of $2 must be cast."""
import re

from services.correlation import stock_news_scorer as sns


def test_every_use_of_the_time_parameter_is_cast_to_timestamptz():
    uses = re.findall(r"\$2(?!\w)(::\w+)?", sns._COUNTS_SQL)
    assert uses, "the query no longer uses $2"
    assert all(u == "::timestamptz" for u in uses), uses
