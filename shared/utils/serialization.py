"""
shared/utils/serialization.py

Response normalization at the API boundary.

Two problems this solves, both of which are invisible until they reach a screen:

  1. FLOAT NOISE. A value computed as `0.6 * 4.5` serializes as
     2.6999999999999997 and renders verbatim in a browser. Rounding at the point
     of display means every consumer has to remember to do it; rounding here
     means the wire format is already clean.

  2. INTERNAL LEAKAGE. Spreading a database row into a response ships whatever
     the schema happens to contain -- surrogate keys, ingestion bookkeeping,
     Kafka offsets -- and couples the client to the storage layer. Responses
     should carry the fields a client needs, chosen deliberately.

Precision is a presentation concern, so it is applied on the way out rather than
in the computation: the stored value keeps full precision, the transmitted one
is legible.
"""

from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

# Field names that describe how a record was stored or transported rather than
# what it means. Never useful to a dashboard, and a standing invitation to
# couple the client to internals.
INTERNAL_FIELDS = frozenset({
    "raw_payload",
    "kafka_offset",
    "kafka_partition",
    "kafka_topic",
    "_id",
    "__v",
    "ctid",
    "xmin",
    "internal_notes",
    "debug_trace",
    "stack_trace",
})

# Precision by semantic class. Probabilities and scores need more resolution
# than currency; rates need more than either.
PRECISION_SCORE = 4      # probabilities, confidences, anomaly scores, weights
PRECISION_STANDARD = 2   # currency, percentages, ratios
PRECISION_RATE = 6       # funding rates, basis points, yields


# ── UNIT NAMING CONVENTION ───────────────────────────────────────────────────
# A numeric field with a unit must say so in its name. The codebase already does
# this where it matters most -- `var_95_pct`, `market_value_usd`,
# `slippage_est_bps`, `half_life_bars` cannot be misread -- but `confidence`,
# `weight` and `score` carry no unit, and those are precisely the fields where
# the ratio-versus-percent ambiguity produced real defects: a 0-1 ratio rendered
# through a percent formatter is wrong by 100x and still looks plausible.
#
# Renaming 139 emission sites at once would break every consumer, so this is an
# additive migration: `annotate_units=True` emits the canonical name alongside
# the original. Consumers move at their own pace, and `assert_units_declared()`
# stops new ambiguous fields being introduced meanwhile.

# Ambiguous name -> canonical name carrying its unit.
UNIT_CANONICAL_NAMES = {
    # 0-1 ratios. The single largest source of formatting defects.
    "confidence": "confidence_ratio",
    "conviction": "conviction_ratio",
    "conviction_score": "conviction_ratio",
    "probability": "probability_ratio",
    "score": "score_ratio",
    "anomaly": "anomaly_score_ratio",
    "anomaly_score": "anomaly_score_ratio",
    "weight": "weight_ratio",
    "model_weight": "weight_ratio",
    "similarity": "similarity_ratio",
    "coefficient": "coefficient_r",       # Pearson r, bounded [-1, 1]
    "correlation": "coefficient_r",
    # Monetary
    "price": "price_usd",
    "value": "value_usd",
    "amount": "amount_usd",
    "notional": "notional_usd",
    # Counts and sizes
    "volume": "volume_units",
    "size": "size_units",
    "count": "count_n",
    # Rates
    "rate": "rate_ratio",
    "threshold": "threshold_value",
}

# Suffixes that already declare a unit. A field ending in one of these is
# unambiguous and is left alone.
DECLARED_UNIT_SUFFIXES = (
    "_pct", "_usd", "_bps", "_ratio", "_r", "_bars", "_sec", "_ms",
    "_epoch", "_count", "_n", "_total", "_units", "_id", "_at", "_date",
    "_name", "_type", "_state", "_status", "_value", "_hash", "_url", "_key",
)


def declares_unit(field_name: str) -> bool:
    """True when *field_name* already states its unit or is non-numeric."""
    lower = field_name.lower()
    return lower.endswith(DECLARED_UNIT_SUFFIXES)


def canonical_unit_name(field_name: str) -> Optional[str]:
    """Canonical unit-bearing name for an ambiguous field, or None."""
    if declares_unit(field_name):
        return None
    return UNIT_CANONICAL_NAMES.get(field_name.lower())


def annotate_units(record: Mapping[str, Any]) -> Dict[str, Any]:
    """Adds a unit-declaring alias beside every ambiguous numeric field.

    The original key is retained so existing consumers keep working. Only
    numeric values are aliased: a `value` holding a string is not a quantity and
    needs no unit.
    """
    out: Dict[str, Any] = dict(record)
    for key, value in record.items():
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            continue
        canonical = canonical_unit_name(key)
        if canonical and canonical not in out:
            out[canonical] = value
    return out


def assert_units_declared(record: Mapping[str, Any], context: str = "") -> List[str]:
    """Returns ambiguous numeric field names in *record*.

    Used by tests to stop the problem growing: an empty list means every
    numeric field either declares its unit or has no unit to declare.
    """
    offenders: List[str] = []
    for key, value in record.items():
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            continue
        if key.lower() in UNIT_CANONICAL_NAMES and not declares_unit(key):
            offenders.append(f"{context}{key}" if context else key)
    return offenders


def _round_scalar(value: float, places: int) -> float:
    """Rounds a float, collapsing -0.0 to 0.0 so it never renders as '-0.00'."""
    rounded = round(value, places)
    return 0.0 if rounded == 0 else rounded


def normalize_floats(obj: Any, places: int = PRECISION_STANDARD) -> Any:
    """Recursively round every float in *obj* to *places* decimals.

    Traverses dicts, lists and tuples. Leaves ints, bools, strings and None
    untouched -- a bool is an int subclass in Python, so it is checked first to
    avoid turning True into 1.0.
    """
    if isinstance(obj, bool) or obj is None:
        return obj
    if isinstance(obj, float):
        return _round_scalar(obj, places)
    if isinstance(obj, Decimal):
        return _round_scalar(float(obj), places)
    if isinstance(obj, Mapping):
        return {k: normalize_floats(v, places) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [normalize_floats(v, places) for v in obj]
    return obj


def normalize_temporal(obj: Any) -> Any:
    """Recursively convert datetimes and dates to ISO-8601 strings.

    A dashboard cannot render a datetime object, and each serializer that
    stringifies one its own way produces a different format on a different panel.
    """
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, date):
        return obj.isoformat()
    if isinstance(obj, Mapping):
        return {k: normalize_temporal(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [normalize_temporal(v) for v in obj]
    return obj


def strip_internal(obj: Any, extra: Optional[Iterable[str]] = None) -> Any:
    """Recursively drop transport/storage bookkeeping fields."""
    drop = INTERNAL_FIELDS | set(extra or ())
    if isinstance(obj, Mapping):
        return {k: strip_internal(v, extra) for k, v in obj.items() if k not in drop}
    if isinstance(obj, (list, tuple)):
        return [strip_internal(v, extra) for v in obj]
    return obj


def to_dto(
    obj: Any,
    *,
    fields: Optional[Sequence[str]] = None,
    precision: int = PRECISION_STANDARD,
    field_precision: Optional[Mapping[str, int]] = None,
    drop: Optional[Iterable[str]] = None,
    units: bool = False,
) -> Any:
    """Normalize a record (or list of records) into a clean response payload.

    Applies, in order: field selection, internal-field removal, temporal
    stringification, and float rounding.

    Args:
        obj: A mapping, or a sequence of mappings.
        fields: If given, an allowlist -- only these keys survive. Preferred over
            `drop` for client-facing payloads, because it fails closed when the
            underlying schema grows a new column.
        precision: Default decimal places for floats.
        field_precision: Per-field overrides, e.g. {"confidence": 4}. Applied to
            matching keys at any depth.
        drop: Additional field names to remove.

    Returns:
        A plain dict or list of dicts, safe to return directly from a route.
    """
    if isinstance(obj, (list, tuple)):
        return [to_dto(o, fields=fields, precision=precision,
                       field_precision=field_precision, drop=drop, units=units) for o in obj]

    if not isinstance(obj, Mapping):
        return normalize_floats(normalize_temporal(obj), precision)

    record: Dict[str, Any] = dict(obj)
    if fields is not None:
        record = {k: record.get(k) for k in fields}

    record = strip_internal(record, drop)
    record = normalize_temporal(record)
    if units:
        record = annotate_units(record)

    overrides = dict(field_precision or {})
    out: Dict[str, Any] = {}
    for key, value in record.items():
        places = overrides.get(key, precision)
        out[key] = normalize_floats(value, places)
    return out


def score_dto(obj: Any, **kwargs: Any) -> Any:
    """`to_dto` tuned for probability/score payloads (4 decimal places)."""
    kwargs.setdefault("precision", PRECISION_SCORE)
    return to_dto(obj, **kwargs)
