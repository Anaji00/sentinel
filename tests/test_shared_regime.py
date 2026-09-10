"""Gap 9: statistics computed across regimes the platform does not know changed.

The machinery existed and did not work. `SentinelAgent.current_regime()` read
`regime`, `rates_regime` or `state` out of the cached rates brief and fell back
to "unknown". `RatesRegimeBrief` defines none of those three -- its keys are
curve_state, yield_spread_2y10y_bps, breakeven_inflation_bps, tips_yield,
credit_spread_widening_signal, regime_summary, macro_risk_level and
recommended_hedging.

Verified against the live payload before the fix: all three lookups returned
None, so **the reader returned "unknown" on every call since it was written**
and the regime-partitioned scorecards behind it -- the ones its own docstring
says exist because "a hit rate earned in a bull-steepening regime says little
about the same strategy under inversion, and Kelly sizing treats whatever it is
handed as the true win probability" -- never partitioned anything.

A reader and a writer built against different schemas: the same defect as the
telemetry worker reading five keys the wargamer never sent, this time in the
path that decides position size.
"""
import json

import pytest

from shared.utils.regime import (
    FLAT_BPS,
    INVERTED_BPS,
    UNKNOWN,
    current_regime,
    regime_from_brief,
    regime_from_spread,
    stamp,
)

# The payload as it was read out of Redis when this was found.
LIVE_BRIEF = {
    "curve_state": "2Y Yield: 4.390% | 10Y Yield: 4.800%",
    "yield_spread_2y10y_bps": 41.0,
    "breakeven_inflation_bps": 237.0,
    "tips_yield": 2.43,
    "credit_spread_widening_signal": "crypto",
    "regime_summary": "...",
    "macro_risk_level": "ELEVATED",
}


def test_the_old_lookup_could_never_have_worked():
    """Not a regression guard -- the record of why this was silent."""
    for key in ("regime", "rates_regime", "state"):
        assert key not in LIVE_BRIEF


def test_the_live_payload_now_resolves_to_a_regime():
    assert regime_from_brief(LIVE_BRIEF) == "normal_steepening"


@pytest.mark.parametrize(
    "bps,expected",
    [
        (-30.0, "inverted"),
        (-0.1, "inverted"),
        (0.0, "flat"),
        (12.0, "flat"),
        (41.0, "normal_steepening"),
        (149.0, "normal_steepening"),
        (200.0, "steep"),
    ],
)
def test_the_curve_is_read_from_the_number(bps, expected):
    assert regime_from_spread(bps) == expected


@pytest.mark.parametrize("missing", [None, float("nan"), "", "not a number"])
def test_an_unmeasured_spread_is_unknown_not_a_guess(missing):
    """Unknown falls back to the unpartitioned scorecard, which is honest.

    No history for this regime is a different statement from history saying the
    regime does not matter.
    """
    assert regime_from_spread(missing) == UNKNOWN


def test_the_boundaries_are_wide_enough_not_to_flip_daily():
    """A regime that flips on a one-basis-point move repartitions everything."""
    assert FLAT_BPS - INVERTED_BPS >= 10.0


def test_prose_is_never_parsed_for_the_regime():
    """A direction signal was already derived by searching a paragraph once."""
    misleading = dict(LIVE_BRIEF)
    misleading["curve_state"] = "the curve is no longer inverted"
    assert regime_from_brief(misleading) == "normal_steepening"


def test_an_explicit_stamp_wins_over_the_derivation():
    stamped = dict(LIVE_BRIEF, regime="Inverted")
    assert regime_from_brief(stamped) == "inverted"


def test_stamp_adds_what_every_reader_was_looking_for():
    out = stamp(LIVE_BRIEF)
    assert out["regime"] == "normal_steepening"
    assert LIVE_BRIEF.get("regime") is None, "stamp must not mutate its input"


@pytest.mark.anyio
async def test_current_regime_reads_the_cache():
    class _Raw:
        async def get(self, key):
            return json.dumps(stamp(LIVE_BRIEF))

    class _Client:
        raw = _Raw()

    assert await current_regime(_Client()) == "normal_steepening"


@pytest.mark.anyio
async def test_an_absent_cache_is_unknown_not_an_error():
    class _Raw:
        async def get(self, key):
            return None

    class _Client:
        raw = _Raw()

    assert await current_regime(_Client()) == UNKNOWN
    assert await current_regime(None) == UNKNOWN


def test_the_enum_fields_are_constrained_so_the_decoder_enforces_them():
    """Both held free text live: a rendered sentence, and "crypto"."""
    import pydantic

    from services.agents.macro_intelligence_engine import RatesRegimeBrief

    good = dict(
        curve_state="Normal Steepening",
        yield_spread_2y10y_bps=41.0,
        breakeven_inflation_bps=237.0,
        tips_yield=2.43,
        credit_spread_widening_signal="Stable",
        regime_summary="x",
        macro_risk_level="LOW",
    )
    RatesRegimeBrief(**good)

    for field, bad in (
        ("curve_state", "2Y Yield: 4.390% | 10Y Yield: 4.800%"),
        ("credit_spread_widening_signal", "crypto"),
    ):
        with pytest.raises(pydantic.ValidationError):
            RatesRegimeBrief(**{**good, field: bad})


def test_the_agent_reads_the_one_shared_definition():
    """Four components deriving a domain four ways is already in this audit."""
    import pathlib

    src = (pathlib.Path(__file__).resolve().parents[1] / "services" / "agents" / "base.py").read_text(
        encoding="utf-8"
    )
    assert "shared_current_regime(self.redis)" in src
    assert 'brief.get("rates_regime")' not in src
