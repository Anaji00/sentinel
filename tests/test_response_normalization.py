"""
tests/test_response_normalization.py

Asserts the API boundary emits clean, predictable payloads: bounded float
precision, ISO timestamps, no storage/transport bookkeeping, and an explicit
field contract that does not widen when a table gains a column.
"""

import json
from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

from shared.utils.serialization import (
    INTERNAL_FIELDS,
    PRECISION_RATE,
    PRECISION_SCORE,
    PRECISION_STANDARD,
    normalize_floats,
    normalize_temporal,
    score_dto,
    strip_internal,
    to_dto,
)


# ── FLOAT PRECISION ──────────────────────────────────────────────────────────

def test_binary_float_noise_never_reaches_the_wire():
    """The concrete defect: `0.6 * 4.5` serializes as 2.6999999999999997.

    Values like this rendered verbatim in the dashboard because rounding was
    left to whichever consumer remembered to do it.
    """
    raw = 0.6 * 4.5
    assert repr(raw) == "2.6999999999999997"  # sanity: the noise is real

    cleaned = normalize_floats({"z_score": raw})
    assert cleaned["z_score"] == 2.7
    assert json.dumps(cleaned) == '{"z_score": 2.7}'


@pytest.mark.parametrize("score", [0.6, 0.7, 0.85, 0.23, 0.91])
def test_scaled_scores_serialize_cleanly(score):
    payload = score_dto({"anomaly_score": score, "z_score": score * 4.5})
    for value in payload.values():
        # At most PRECISION_SCORE decimals once serialized.
        text = json.dumps(value)
        if "." in text:
            assert len(text.split(".")[1]) <= PRECISION_SCORE, text


def test_precision_is_bounded_at_every_depth():
    nested = {
        "outer": 1.23456789,
        "inner": {"deep": [2.3456789, {"deeper": 3.456789}]},
    }
    out = normalize_floats(nested, PRECISION_STANDARD)
    assert out["outer"] == 1.23
    assert out["inner"]["deep"][0] == 2.35
    assert out["inner"]["deep"][1]["deeper"] == 3.46


def test_booleans_and_ints_survive_rounding():
    """A bool is an int subclass; naive float handling turns True into 1.0."""
    out = normalize_floats({"enabled": True, "disabled": False, "count": 7, "name": "NVDA"})
    assert out["enabled"] is True
    assert out["disabled"] is False
    assert out["count"] == 7 and isinstance(out["count"], int)
    assert out["name"] == "NVDA"


def test_negative_zero_never_renders():
    """-0.0 formats as '-0.00', which reads as a signed value that is not one."""
    assert normalize_floats({"delta": -0.0001}, 2)["delta"] == 0.0
    assert json.dumps(normalize_floats({"delta": -0.0001}, 2)) == '{"delta": 0.0}'


def test_decimal_is_coerced():
    """Postgres NUMERIC arrives as Decimal, which json.dumps cannot serialize."""
    out = normalize_floats({"price": Decimal("1234.56789")}, 2)
    assert out["price"] == 1234.57
    json.dumps(out)  # must not raise


def test_none_is_preserved_not_coerced_to_zero():
    """Absence must stay absent -- a zero is a measurement."""
    out = normalize_floats({"sharpe": None, "measured": 0.0})
    assert out["sharpe"] is None
    assert out["measured"] == 0.0


def test_precision_classes_differ_by_semantics():
    assert PRECISION_SCORE > PRECISION_STANDARD
    assert PRECISION_RATE > PRECISION_SCORE

    rate = 0.000123456
    assert normalize_floats(rate, PRECISION_STANDARD) == 0.0
    assert normalize_floats(rate, PRECISION_RATE) == 0.000123


# ── TEMPORAL ─────────────────────────────────────────────────────────────────

def test_datetimes_become_iso_strings():
    dt = datetime(2026, 8, 21, 14, 32, 5, tzinfo=timezone.utc)
    out = normalize_temporal({"occurred_at": dt, "day": date(2026, 8, 21)})
    assert out["occurred_at"] == "2026-08-21T14:32:05+00:00"
    assert out["day"] == "2026-08-21"
    json.dumps(out)  # must not raise


def test_nested_datetimes_are_converted():
    dt = datetime(2026, 8, 21, tzinfo=timezone.utc)
    out = normalize_temporal({"items": [{"at": dt}]})
    assert isinstance(out["items"][0]["at"], str)


# ── INTERNAL FIELD REMOVAL ───────────────────────────────────────────────────

def test_transport_metadata_is_stripped():
    row = {
        "ticker": "NVDA",
        "raw_payload": {"kafka": "blob"},
        "kafka_offset": 918273,
        "kafka_partition": 3,
        "debug_trace": "...",
        "anomaly_score": 0.8,
    }
    out = strip_internal(row)
    assert set(out) == {"ticker", "anomaly_score"}


def test_internal_fields_are_stripped_at_depth():
    out = strip_internal({"outer": {"inner": {"raw_payload": 1, "keep": 2}}})
    assert out["outer"]["inner"] == {"keep": 2}


def test_every_declared_internal_field_is_actually_dropped():
    row = {f: "x" for f in INTERNAL_FIELDS}
    row["legitimate"] = "y"
    assert strip_internal(row) == {"legitimate": "y"}


# ── DTO CONTRACT ─────────────────────────────────────────────────────────────

def test_allowlist_fails_closed_when_the_schema_grows():
    """A new database column must not silently start reaching clients."""
    row = {
        "correlation_id": "c-1",
        "rule_name": "hawkes",
        "detected_at": datetime(2026, 8, 21, tzinfo=timezone.utc),
        # A column added later by a migration:
        "internal_scoring_weights": [0.1, 0.2],
    }
    out = to_dto(row, fields=("correlation_id", "rule_name", "detected_at"))
    assert set(out) == {"correlation_id", "rule_name", "detected_at"}
    assert "internal_scoring_weights" not in out


def test_allowlist_yields_a_stable_shape_for_missing_columns():
    """Requested-but-absent fields render as None, not as a missing key."""
    out = to_dto({"a": 1}, fields=("a", "b"))
    assert out == {"a": 1, "b": None}


def test_per_field_precision_overrides():
    out = to_dto(
        {"price": 1234.56789, "confidence": 0.123456789},
        precision=PRECISION_STANDARD,
        field_precision={"confidence": PRECISION_SCORE},
    )
    assert out["price"] == 1234.57
    assert out["confidence"] == 0.1235


def test_lists_of_records_normalize_elementwise():
    rows = [{"score": 0.6 * 4.5, "raw_payload": "x"} for _ in range(3)]
    out = to_dto(rows, precision=PRECISION_SCORE)
    assert len(out) == 3
    for rec in out:
        assert rec["score"] == 2.7
        assert "raw_payload" not in rec


def test_dto_output_is_json_serializable():
    """The whole point: a route can return this directly."""
    row = {
        "correlation_id": "c-1",
        "detected_at": datetime.now(timezone.utc),
        "weight": Decimal("0.87654321"),
        "nested": {"scores": [0.1 * 3, 0.2 * 3]},
        "raw_payload": object(),  # unserializable, and must be dropped
    }
    out = to_dto(row, precision=PRECISION_SCORE)
    json.dumps(out)  # must not raise


# ── ROUTE-LEVEL WIRING ───────────────────────────────────────────────────────

def test_radar_route_normalizes_its_scores():
    """The radar endpoint must not hand-build unrounded float payloads."""
    from pathlib import Path
    src = (Path(__file__).resolve().parents[1]
           / "services" / "api_gateway" / "routes" / "radar.py").read_text(encoding="utf-8")

    assert "score_dto(" in src, "radar anomalies are not normalized"
    assert 'float(r["anomaly_score"] or 0.0) * 4.5' not in src, (
        "raw unrounded z_score computation still present"
    )
    assert "Z_SCORE_SCALE" in src, "magic scaling factor is still inlined"


def test_scenarios_route_uses_an_explicit_field_contract():
    from pathlib import Path
    src = (Path(__file__).resolve().parents[1]
           / "services" / "api_gateway" / "routes" / "scenarios.py").read_text(encoding="utf-8")

    assert "CORRELATION_FIELDS" in src, "correlation response has no field allowlist"
    assert "to_dto(item, fields=CORRELATION_FIELDS)" in src, (
        "correlation rows are still spread into the response"
    )


# ── UNIT NAMING CONVENTION ───────────────────────────────────────────────────

from shared.utils.serialization import (  # noqa: E402
    UNIT_CANONICAL_NAMES,
    annotate_units,
    assert_units_declared,
    canonical_unit_name,
    declares_unit,
)


def test_fields_that_already_declare_a_unit_are_left_alone():
    """var_95_pct, market_value_usd and half_life_bars are already unambiguous."""
    for name in ("var_95_pct", "market_value_usd", "slippage_est_bps",
                 "half_life_bars", "processing_latency_ms", "bar_count"):
        assert declares_unit(name), f"{name} should be recognized as unit-bearing"
        assert canonical_unit_name(name) is None


def test_ambiguous_ratio_fields_get_a_canonical_alias():
    """These are exactly the fields where ratio-vs-percent caused real defects."""
    for name in ("confidence", "weight", "conviction", "probability", "score"):
        canonical = canonical_unit_name(name)
        assert canonical is not None, f"{name} has no canonical form"
        assert canonical.endswith(("_ratio", "_r")), (
            f"{name} -> {canonical} does not declare a ratio unit"
        )


def test_annotation_is_additive_so_existing_consumers_keep_working():
    """Renaming 139 sites at once would break every client."""
    out = annotate_units({"confidence": 0.87, "price": 12.5})
    assert out["confidence"] == 0.87       # original retained
    assert out["confidence_ratio"] == 0.87  # canonical added
    assert out["price_usd"] == 12.5


def test_annotation_ignores_non_numeric_values():
    """A `value` holding a string is not a quantity and needs no unit."""
    out = annotate_units({"value": "PENDING", "amount": 5.0})
    assert "value_usd" not in out
    assert out["amount_usd"] == 5.0


def test_annotation_never_overwrites_an_existing_canonical_field():
    out = annotate_units({"confidence": 0.5, "confidence_ratio": 0.9})
    assert out["confidence_ratio"] == 0.9


def test_booleans_are_not_treated_as_quantities():
    """A bool is an int subclass; aliasing it would invent a unit for a flag."""
    out = annotate_units({"score": True})
    assert "score_ratio" not in out


def test_to_dto_emits_canonical_names_on_request():
    out = to_dto({"confidence": 0.876543, "var_95_pct": 2.4},
                 precision=PRECISION_SCORE, units=True)
    assert out["confidence_ratio"] == 0.8765
    assert "var_95_pct" in out


def test_units_flag_is_off_by_default():
    """Opt-in, so no existing response shape changes silently."""
    out = to_dto({"confidence": 0.5})
    assert "confidence_ratio" not in out


def test_lint_helper_identifies_ambiguous_fields():
    """Used by tests to stop the problem growing."""
    offenders = assert_units_declared({"confidence": 0.5, "var_95_pct": 2.4, "ticker": "NVDA"})
    assert offenders == ["confidence"]


def test_every_canonical_name_declares_a_unit():
    """A mapping whose target is itself ambiguous would be pointless."""
    for ambiguous, canonical in UNIT_CANONICAL_NAMES.items():
        assert declares_unit(canonical), (
            f"{ambiguous} -> {canonical}, which still declares no unit"
        )
