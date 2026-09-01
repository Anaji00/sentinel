"""
tests/test_candle_feature_integrity.py

A $3.2M bar published as "on $-0.0M vol".

score_crypto_candle normalised features[2] in place, on the list its caller had
passed. Index 2 therefore stopped being a notional in dollars the instant a
score was taken and became a z-score -- while every reader downstream still
believed it was money. The structural-anomaly headlines in both the crypto and
tradfi enrichers render it as `${notional/1e6:.1f}M vol`, so a z-score of about
-0.03 divided by a million printed as `$-0.0M`.

The volume itself was never wrong. The Redis block for that bar holds 1318.42
ETH at $2454.97, its running notional mean sits at $3.22M, and the figure
reaches the stored event's size_tokens intact. Only the headline's copy was
overwritten, by the scoring call that sits between the two reads of the same
list.

What these pin is the aliasing, not the arithmetic: taking a score must not
change the thing being scored.
"""

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.enrichment.anomaly_scorer import DynamicAnomalyScorer  # noqa: E402


class _StubRedis:
    """Enough Redis for the scorer's constructor. Never actually reached."""
    raw = None


def _scorer(monkeypatch, normalized=-0.0312):
    scorer = DynamicAnomalyScorer(_StubRedis())

    async def _fake_normalize(ticker, feature_name, raw_value):
        # Stands in for the EMA z-score Lua: returns something small and
        # signed, which is exactly the shape that made the bug invisible.
        return normalized

    async def _fake_score_event(event_type, entity, features):
        return {"score": 0.71}

    monkeypatch.setattr(scorer, "_dynamic_normalize", _fake_normalize)
    monkeypatch.setattr(scorer, "score_event", _fake_score_event)
    return scorer


# One 30-minute ETHUSDT bar, taken from the live Redis block.
ETH_CLOSE = 2454.97
ETH_VOLUME = 1318.4217256700001
ETH_NOTIONAL = ETH_CLOSE * ETH_VOLUME          # about $3.24M


@pytest.mark.anyio
async def test_scoring_a_crypto_candle_leaves_the_notional_alone(monkeypatch):
    """The regression, stated directly."""
    scorer = _scorer(monkeypatch)
    features = [0.0011, 0.0016, ETH_NOTIONAL, 0.5, 0.0]

    await scorer.score_crypto_candle("ETHUSDT", features)

    assert features[2] == ETH_NOTIONAL, "the scorer overwrote its caller's notional"


@pytest.mark.anyio
async def test_the_headline_figure_survives_scoring(monkeypatch):
    """What the operator actually sees.

    The enrichers read features[2] *after* scoring and divide by 1e6. Before
    this fix that produced "-0.0"; a $3.24M bar has to render as "$3.2M".
    """
    scorer = _scorer(monkeypatch)
    features = [0.0011, 0.0016, ETH_NOTIONAL, 0.5, 0.0]

    await scorer.score_crypto_candle("ETHUSDT", features)

    assert f"${features[2] / 1e6:.1f}M" == "$3.2M"


@pytest.mark.anyio
async def test_no_feature_is_disturbed_by_scoring(monkeypatch):
    """Not just index 2 -- taking a score changes nothing about the input."""
    scorer = _scorer(monkeypatch)
    features = [0.0011, 0.0016, ETH_NOTIONAL, 0.5, -0.004]
    before = list(features)

    await scorer.score_crypto_candle("ETHUSDT", features)

    assert features == before


@pytest.mark.anyio
async def test_the_tradfi_candle_path_has_the_same_guarantee(monkeypatch):
    """tradfi.py reads features[2] the same way, so it needs the same promise."""
    scorer = _scorer(monkeypatch)
    features = [0.004, 0.006, 8_400_000.0, 0.5, 0.0]
    before = list(features)

    await scorer.score_market_candle("tradfi", "NVDA", features)

    assert features == before


@pytest.mark.anyio
async def test_the_normalized_value_still_reaches_the_model(monkeypatch):
    """The copy must not quietly disable the normalisation.

    Preserving the caller's list is only correct if score_event still receives
    the z-score it has always been given -- otherwise this "fix" would change
    every score in the system.
    """
    scorer = _scorer(monkeypatch, normalized=-0.0312)
    seen = {}

    async def _capture(event_type, entity, features):
        seen["features"] = list(features)
        return {"score": 0.71}

    monkeypatch.setattr(scorer, "score_event", _capture)

    await scorer.score_crypto_candle("ETHUSDT", [0.0011, 0.0016, ETH_NOTIONAL, 0.5, 0.0])

    assert seen["features"][2] == -0.0312, "the model was handed a raw notional"
    # And the untouched features are passed through unchanged.
    assert seen["features"][0] == 0.0011
    assert seen["features"][1] == 0.0016


@pytest.mark.anyio
async def test_a_short_feature_list_is_not_padded_into_the_caller(monkeypatch):
    """score_event pads to five. That padding must not land on the caller."""
    scorer = _scorer(monkeypatch)
    features = [0.0011, 0.0016]

    await scorer.score_crypto_candle("ETHUSDT", features)

    assert features == [0.0011, 0.0016]
