"""Edge confidence has to be earned against the ticker's own base rate.

The validator scored an edge on how often the target ticker had any anomaly
above 0.5 within 24 hours of a source event -- with nothing to compare that to.
A liquid ticker clears that bar most days, so a completely spurious edge
produced a hit rate near 1.0. The sweep runs every five minutes over the same
fixed 30-day window and stepped an EWMA toward that rate each time, so within
hours every exposure edge on an active ticker sat at confidence 1.0 and had
published a `quant_discovery` describing itself as empirically validated.

`validation_samples` compounded the same way: it was `previous + len(rows)` over
an unchanged body of evidence, so it grew by up to 50 every five minutes and
the MIN_SAMPLES_BEFORE_TRUST gate stopped meaning anything within minutes of
startup.

These tests pin the statistic, not the plumbing.
"""
import pytest

from services.agents.edge_validator import (
    MAX_TESTABLE_BASE_RATE,
    _base_rate,
    _evidence,
    _word_pattern,
)


class _DB:
    """A query double that answers by matching on the SQL, not by position.

    It did not, in fact, match on anything: one row was returned for every
    query regardless of what was asked. That was adequate while `_base_rate`
    issued a single statement and stopped being so when it grew three -- a
    qualifying count, an observed-at-all count, and a span -- because a double
    that answers `n = 0` to all of them describes an entity the platform has
    never recorded, which is now a different verdict from one that reacts
    rarely. The docstring described the behaviour this class did not have.
    """

    def __init__(self, n, span_sec, observed=None):
        self._n, self._span = n, span_sec
        # Observed by default. Every test written before the untestable-target
        # guard is about an entity the platform records and which simply does
        # not react often, so that is the case they should keep exercising.
        self._observed = float(observed) if observed is not None else max(float(n), 1.0)
        self.calls = []

    async def query(self, sql, *args):
        self.calls.append((sql, args))
        if "span_sec" in sql:
            return [{"span_sec": self._span}]
        if "anomaly_score" in sql:
            return [{"n": self._n}]
        return [{"n": self._observed}]


# ── the statistic ────────────────────────────────────────────────────────────

def test_beating_a_low_base_rate_is_evidence():
    assert _evidence(hits=18, trials=20, base_rate=0.2) > 0.99


def test_matching_a_high_base_rate_is_not():
    """18 of 20 sounds impressive until the ticker does it 9 days in 10."""
    assert _evidence(hits=18, trials=20, base_rate=0.9) < 0.5


def test_the_old_hit_rate_and_the_new_evidence_disagree_where_it_matters():
    """This is the whole defect, in one comparison."""
    hits, trials = 18, 20
    old_hit_rate = hits / trials
    assert old_hit_rate == 0.9  # promoted, under the old scoring
    assert _evidence(hits, trials, base_rate=0.9) < old_hit_rate


def test_exactly_the_base_rate_lands_near_a_coin_flip():
    assert 0.3 < _evidence(hits=10, trials=20, base_rate=0.5) < 0.7


def test_no_trials_is_no_evidence_not_certainty():
    assert _evidence(hits=0, trials=0, base_rate=0.1) == 0.0


def test_evidence_is_bounded():
    for hits, trials, p in ((0, 10, 0.5), (10, 10, 0.01), (5, 10, 0.5)):
        assert 0.0 <= _evidence(hits, trials, p) <= 1.0


# ── the null ─────────────────────────────────────────────────────────────────

@pytest.mark.anyio
async def test_base_rate_rises_with_arrival_rate():
    """Twice the reactions in the same span means a higher 24h probability."""
    quiet = await _base_rate(_DB(n=3, span_sec=30 * 86400), "AAPL")
    busy = await _base_rate(_DB(n=60, span_sec=30 * 86400), "AAPL")
    assert quiet < busy


@pytest.mark.anyio
async def test_a_ticker_that_reacts_daily_is_untestable():
    """One reaction a day puts P(24h window) past the discrimination limit."""
    p0 = await _base_rate(_DB(n=30 * 4, span_sec=30 * 86400), "SPY")
    assert p0 > MAX_TESTABLE_BASE_RATE


@pytest.mark.anyio
async def test_no_history_does_not_become_a_zero_base_rate():
    """A zero null makes a single hit infinitely significant.

    The floor is half an event across the lookback -- the usual continuity
    correction -- which is ~1.7% over 24 hours. Small enough that real evidence
    still registers, large enough that one coincidence does not read as proof.
    """
    p0 = await _base_rate(_DB(n=0, span_sec=0), "ZZNEVER")
    assert p0 is not None and 0.0 < p0 < 0.05
    # One hit in five trials against that floor must not be near-certainty.
    assert _evidence(hits=1, trials=5, base_rate=p0) < 0.95


@pytest.mark.anyio
async def test_base_rate_query_failure_is_not_a_base_rate_of_zero():
    class _Broken:
        async def query(self, *a):
            raise RuntimeError("timescale down")

    assert await _base_rate(_Broken(), "AAPL") is None


# ── the matching ─────────────────────────────────────────────────────────────

def test_headline_matching_is_word_bounded():
    """`ILIKE '%BP%'` matched abrupt, subpoena and BPO."""
    import re as _re

    # Postgres \m and \M are word boundaries; Python's equivalent is \b.
    py = _word_pattern("BP").replace(r"\m", r"\b").replace(r"\M", r"\b")
    assert _re.search(py, "BP cuts output", _re.I)
    assert not _re.search(py, "an abrupt reversal", _re.I)
    assert not _re.search(py, "BPO earnings", _re.I)


def test_ticker_punctuation_is_escaped_not_interpreted():
    """`BRK.B` as a regex would match BRKXB."""
    import re as _re

    py = _word_pattern("BRK.B").replace(r"\m", r"\b").replace(r"\M", r"\b")
    assert _re.search(py, "BRK.B rallied", _re.I)
    assert not _re.search(py, "BRKXB rallied", _re.I)


# ── an entity the platform has never recorded ────────────────────────────────


@pytest.mark.asyncio
async def test_an_unobserved_target_is_untestable_not_refuted():
    """Zero events is absence of measurement, not absence of reaction.

    The first sweep that ever completed decayed all seven of its edges on
    "0/50 hits vs base 1.7%". 1.7% is the no-qualifying-events floor, and the
    targets -- US10Y, GC=F, BTCUSD, VOLATILE_1M_CANDLE -- carried zero rows in
    the events table at all. Each sweep would have taken another 5% off a
    confidence nothing had measured.
    """
    calls = []

    class _DB:
        async def query(self, sql, *args):
            calls.append(sql)
            # Both the qualifying count and the observed count come back zero.
            return [{"n": 0.0}]

    assert await _base_rate(_DB(), "US10Y") is None
    assert len(calls) == 2, "the observed-at-all count must be asked for"


@pytest.mark.asyncio
async def test_an_observed_but_quiet_target_keeps_the_floor():
    """Rarely reacting is a real signal and must still be gradeable."""
    seen = {"i": 0}

    class _DB:
        async def query(self, sql, *args):
            seen["i"] += 1
            if seen["i"] == 1:
                return [{"n": 0.0}]        # none clearing the anomaly threshold
            if seen["i"] == 2:
                return [{"n": 412.0}]      # but the entity is observed
            return [{"span_sec": 30 * 86400.0}]

    rate = await _base_rate(_DB(), "PBF")
    assert rate is not None
    assert 0.0 < rate < 0.05, rate
