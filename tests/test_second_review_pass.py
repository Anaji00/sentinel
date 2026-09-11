"""The nine findings of the second review pass, as tests.

Each was reproduced against the running deployment before it was repaired, and
the numbers in these docstrings are the measurements taken then, not estimates.
The tests drive the repaired code rather than asserting about its source
wherever driving it is possible.
"""
import ast
import pathlib
import re

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]


def _code_only(src: str) -> str:
    """Source with its comments removed.

    These assertions are about what the code does, and every one of these files
    now carries a comment quoting the expression that was replaced -- which is
    the point of the comment and would otherwise fail the test describing it.
    """
    out = []
    for line in src.splitlines():
        stripped = line.lstrip()
        if stripped.startswith(("#", "//", "*", "/*")):
            continue
        out.append(re.sub(r"\s+(#|//)\s.*$", "", line))
    return chr(10).join(out)


# ── 240: one "/health" anywhere and the gateway answers as the operator ───────

PROXY = (ROOT / "frontend" / "src" / "app" / "api" / "proxy" / "[...path]" / "route.ts").read_text(
    encoding="utf-8"
)


def test_the_proxy_no_longer_exempts_on_a_substring():
    """`/api/v1/events/health` returned 500 events where `/events/crypto` returned 401."""
    assert "pathname.includes('/health')" not in _code_only(PROXY), (
        "a substring test standing in for a route test: every path containing "
        "those seven characters inherited the liveness-probe exemption"
    )


def test_only_liveness_and_readiness_are_exempt():
    assert "const PROBE_PATHS" in PROXY
    for allowed in ("api/v1/health/liveness", "api/v1/health/readiness"):
        assert allowed in PROXY
    # The two that describe the deployment rather than whether it is up.
    i = PROXY.index("const PROBE_PATHS")
    j = PROXY.index("\n", PROXY.index("]", i))
    listed = PROXY[i:j]
    assert "secrets" not in listed
    assert "health/data" not in listed


def test_a_probe_is_forwarded_without_the_operator_key():
    """The escalation was the key, not the skipped check."""
    assert "hasSession || publicPath || probePath" in PROXY


def test_the_gateway_allowlists_rather_than_denylists_health():
    """A denylist only protects the paths somebody remembered; /secrets was not on it."""
    deps = (ROOT / "services" / "api_gateway" / "dependencies.py").read_text(encoding="utf-8")
    assert "_AUTHENTICATED_HEALTH_PATHS" not in deps
    assert "_OPEN_HEALTH_PATHS" in deps
    assert 'path.startswith("/api/v1/health")' not in deps

    ns: dict = {}
    i = deps.index("_OPEN_HEALTH_PATHS = frozenset({")
    exec(compile(deps[i:deps.index("})", i) + 2], "deps", "exec"), ns)
    allowed = ns["_OPEN_HEALTH_PATHS"]
    assert allowed == {"/api/v1/health/liveness", "/api/v1/health/readiness"}
    assert "/api/v1/health/secrets" not in allowed


# ── 241: the model is handed 9% of the context window it has ─────────────────


def test_the_context_window_is_not_a_literal_chosen_from_the_model_name():
    """`num_ctx` and the prompt budget were two literals sixty lines apart.

    One `3072 if ... else 4096` survives, inside `_heuristic_ctx`, and is meant
    to: it is the answer before /api/show has replied, and it is what the
    platform used for every request before this existed. What must not survive
    is a second copy at a call site, because that is how a budget and the window
    it is a budget for come to disagree.
    """
    src = (ROOT / "shared" / "utils" / "ollama.py").read_text(encoding="utf-8")
    tree = ast.parse(src)
    guesses = [
        n for n in ast.walk(tree)
        if isinstance(n, ast.IfExp)
        and "3072" in ast.unparse(n) and "4096" in ast.unparse(n)
    ]
    assert len(guesses) == 1, [ast.unparse(g) for g in guesses]

    heuristic = next(
        n for n in ast.walk(tree)
        if isinstance(n, ast.FunctionDef) and n.name == "_heuristic_ctx"
    )
    assert guesses[0] in ast.walk(heuristic), (
        "the guess belongs in the documented fallback, not at a call site"
    )

    # Both consumers read the one resolver.
    assert src.count("context_window_for(") >= 3


def test_a_declared_window_is_used_and_capped():
    from shared.utils import ollama as o

    o._MODEL_CTX_CACHE.clear()
    try:
        # Nothing learned yet: the old heuristic, so a cold start is unchanged.
        assert o.context_window_for("qwen2.5:1.5b") == min(3072, o.OLLAMA_MAX_CTX)
        # What /api/show actually reports for this model.
        o._MODEL_CTX_CACHE["qwen2.5:1.5b"] = 32768
        assert o.context_window_for("qwen2.5:1.5b") == o.OLLAMA_MAX_CTX
        assert o.OLLAMA_MAX_CTX >= 8192, (
            "the budget has to clear the 8,033-character prompts this platform "
            "builds; at 3,072 tokens it was 4,064 characters"
        )
    finally:
        o._MODEL_CTX_CACHE.clear()


def test_the_prompt_budget_now_fits_the_prompts_this_platform_builds():
    """8,033 characters was the largest live scenario prompt; 4,064 was the budget."""
    from shared.utils import ollama as o

    o._MODEL_CTX_CACHE.clear()
    o._MODEL_CTX_CACHE["qwen2.5:1.5b"] = 32768
    try:
        budget = o.deliverable_prompt_chars("qwen2.5:1.5b", num_predict=1024)
        assert budget > 8033, budget
    finally:
        o._MODEL_CTX_CACHE.clear()


# ── 242/243/245: every confidence was one of two constants ───────────────────


def test_a_cascade_confidence_varies_with_its_flashpoint_index():
    """963 clusters in 24 hours carried exactly one value: the Pydantic default."""
    from services.correlation.cascade import _cascade_confidence, CASCADE_CONF_CEILING

    values = {_cascade_confidence(f, True) for f in (35.0, 52.3, 72.3, 88.1, 100.0)}
    assert len(values) == 5, values
    assert max(values) <= CASCADE_CONF_CEILING
    # A single-domain storm is not a cascade and cannot claim a cascade's number.
    assert _cascade_confidence(72.3, False) < _cascade_confidence(72.3, True)


def test_a_cascade_never_publishes_the_model_default():
    src = (ROOT / "services" / "correlation" / "cascade.py").read_text(encoding="utf-8")
    assert "confidence_score=_cascade_confidence(" in src


def test_the_flashpoint_index_is_rankable_not_only_readable():
    src = (ROOT / "services" / "correlation" / "cascade.py").read_text(encoding="utf-8")
    assert '"flashpoint_index": flashpoint_index' in src, (
        "it was written only into the prose of `description`, where nothing "
        "downstream could rank on it"
    )


def test_a_forecast_is_never_published_as_certain():
    """19 of 19 Hawkes clusters in 24 hours carried confidence exactly 1.000."""
    from services.correlation.main import _hawkes_confidence, RULE_CONF_CEILING

    for multiplier in (2.0, 3.0, 5.0, 10.0, 20.0, 50.0, 1000.0):
        conf = _hawkes_confidence(multiplier)
        assert conf < 1.0, multiplier
        assert conf <= RULE_CONF_CEILING, multiplier

    # And the ordering survives the top of the range instead of collapsing.
    assert _hawkes_confidence(5.0) < _hawkes_confidence(10.0) < _hawkes_confidence(20.0)


def test_the_semantic_path_scores_itself_on_its_evidence():
    """1,201 of 1,243 semantic clusters carried the literal 0.7999999999999999."""
    src = (ROOT / "services" / "correlation" / "main.py").read_text(encoding="utf-8")
    assert "(0.35 + (0.15 * distinct_subjects))" not in _code_only(src), (
        "no breadth term, no independence term, no cross-domain term -- and "
        "`kept` is capped at three, so almost every cluster landed on the same "
        "float"
    )
    assert "_rule_confidence(event, kept, semantic_domains)" in src


# ── 244: every cascade filed as geopolitical ─────────────────────────────────


def test_a_wallet_cluster_is_not_filed_as_a_geopolitical_event():
    src = (ROOT / "services" / "correlation" / "main.py").read_text(encoding="utf-8")
    assert 'cascade_cluster.primary_domain = "geopolitical"' not in _code_only(src)
    cascade = (ROOT / "services" / "correlation" / "cascade.py").read_text(encoding="utf-8")
    assert "primary_domain=(" in cascade


def test_the_cascade_counts_domains_not_event_types():
    """vessel_position + vessel_dark read as two domains and cleared the gate."""
    src = (ROOT / "services" / "correlation" / "cascade.py").read_text(encoding="utf-8")
    assert "domain = resolve_event_domain(event)" in src
    assert '"event_type": event_type' in src


def test_the_world_domains_are_domains():
    from services.correlation.cascade import _WORLD_DOMAINS

    # These were event types, plus three names that are neither and could never
    # match anything.
    for stale in ("vessel_position", "flight_position", "bgp_anomaly", "osint", "sanctions"):
        assert stale not in _WORLD_DOMAINS, stale
    assert {"news", "maritime", "aviation", "cyber"} <= _WORLD_DOMAINS


# ── 246: a dark aircraft carries no aircraft data and no position ────────────


def test_a_dark_aircraft_carries_its_last_position_and_payload():
    """vessel_dark 199/199 with payload and position; flight_dark 0/530."""
    src = (ROOT / "services" / "enrichment" / "aviation_gap_detector.py").read_text(encoding="utf-8")
    assert 'latitude=val.get("lat")' in src, (
        "the coordinates were in the dict the detector was already reading -- "
        "the vessel detector, on the identical record shape, carries them"
    )
    assert 'longitude=val.get("lon")' in src
    assert "flight_data=FlightData(" in src, (
        "/events/aviation filters on `flight_data IS NOT NULL`, so without this "
        "the aviation panel cannot return a single dark aircraft"
    )


def test_both_gap_detectors_read_the_same_record_the_same_way():
    aviation = (ROOT / "services" / "enrichment" / "aviation_gap_detector.py").read_text(encoding="utf-8")
    vessel = (ROOT / "services" / "enrichment" / "gap_detector.py").read_text(encoding="utf-8")
    for src in (aviation, vessel):
        assert 'val.get("lat")' in src
        assert 'val.get("lon")' in src


# ── 247: the news feed returns no news ───────────────────────────────────────

EVENTS = (ROOT / "services" / "api_gateway" / "routes" / "events.py").read_text(encoding="utf-8")


def test_news_is_filtered_rather_than_routed_past_the_filter():
    """The newest 50 rows were 26 crypto transfers, 22 vessel positions, 0 headlines."""
    assert 'or domain == "news"' not in _code_only(EVENTS), (
        "the one domain whose DOMAIN_TO_COLUMN entry names a real column was "
        "routed into the branch that applies no domain predicate at all"
    )
    assert "NEWS_PREDICATE" in EVENTS


def test_the_news_predicate_is_the_endpoints_own_domain_case():
    """The filter and the label have to be the same statement."""
    from services.api_gateway.routes.events import NEWS_PREDICATE

    for column in (
        "crypto_data", "prediction_market_data", "vessel_data",
        "flight_data", "security_data", "financial_data",
    ):
        assert f"{column} IS NULL" in NEWS_PREDICATE, column


def test_news_projects_no_payload_rather_than_its_headline_text():
    """`headline as domain_data` is text in the field consumers read as an object."""
    assert 'domain_data_sql = "NULL::jsonb"' in EVENTS
    assert "{target_column} as domain_data" not in _code_only(EVENTS)


def test_an_unknown_domain_is_a_404_not_the_whole_table():
    assert "is not a domain. Known domains" in EVENTS


# ── 248: the live feed guesses the domain from a substring ───────────────────

HOOK = (ROOT / "frontend" / "src" / "lib" / "useLiveEvents.ts").read_text(encoding="utf-8")
DOMAIN_TS = (ROOT / "frontend" / "src" / "lib" / "domain.ts").read_text(encoding="utf-8")
FEED = (ROOT / "frontend" / "src" / "components" / "IntelligenceFeed.tsx").read_text(encoding="utf-8")


def test_the_tab_filter_reads_the_domain_the_gateway_sends():
    """`prediction_market_trade` contains "market": badged PREDICTION, listed under TRADFI."""
    assert "resolveEventDomain(e)" in HOOK
    assert "if (resolved) return resolved === selectedDomain;" in HOOK


def test_the_badge_and_the_filter_share_one_resolver():
    assert "resolveEventDomain" in FEED
    assert "from './types'" in DOMAIN_TS


def test_the_resolver_prefers_the_declared_domain_then_the_payload():
    """Order matters: crypto leads, for the candle case the server field exists for."""
    i_declared = DOMAIN_TS.index("const declared")
    i_crypto = DOMAIN_TS.index("if (e.crypto_data)")
    i_financial = DOMAIN_TS.index("if (e.financial_data)")
    assert i_declared < i_crypto < i_financial


# ── 239: the interaction record the consequence gap asked for ────────────────


def test_score_bands_are_the_unit_the_calibration_works_in():
    from services.api_gateway.routes.feedback import _score_band

    assert _score_band(0.0) == "0.0-0.1"
    assert _score_band(0.94) == "0.9-1.0"
    assert _score_band(1.0) == "0.9-1.0"
    assert _score_band(None) == "unknown"


def test_surfaced_is_recorded_so_the_rates_have_a_denominator():
    from services.api_gateway.routes.feedback import INTERACTIONS

    assert "surfaced" in INTERACTIONS, (
        "40 of 50 and 40 of 40,000 are opposite findings; without the "
        "denominator an open count says nothing"
    )
    assert {"opened", "dismissed", "acted_on"} <= set(INTERACTIONS)


def test_the_frontend_records_both_halves():
    interactions = (ROOT / "frontend" / "src" / "lib" / "interactions.ts").read_text(encoding="utf-8")
    assert "surfaced" in interactions and "opened" in interactions
    assert "emitted.has(key)" in interactions, (
        "without de-duplication `surfaced` fires on every re-render and the "
        "denominator becomes a count of React renders"
    )
    assert "recordInteraction('surfaced'" in FEED
    assert "recordInteraction('opened'" in FEED


# ── 249: four scoring keys had no detector, so every score was 0.5 ───────────


def _scorer():
    """The scorer, with a Redis stub: none of this touches Redis."""
    from services.enrichment.anomaly_scorer import DynamicAnomalyScorer

    class _Raw:
        def __getattr__(self, _name):
            raise AssertionError("these paths must not touch Redis")

    class _Client:
        raw = _Raw()

    return DynamicAnomalyScorer(redis_client=_Client())


SCORING_KEYS_IN_USE = (
    "crypto_candle",
    "crypto_perp_funding",
    "crypto_trade",
    "cyber_anomaly",
    "prediction_market_trade",
    "tradfi_candle",
    "tradfi_trade",
)


@pytest.mark.parametrize("key", SCORING_KEYS_IN_USE)
def test_every_scoring_key_in_use_resolves_to_a_detector(key):
    """Four of these seven resolved to OTHER, and there is no OTHER detector.

    `score_event_batch` returns its uninitialised 0.5 when `_detector_for`
    returns None, and `evaluate_multi_timeframe` admits a frame only at
    `anomaly >= 0.6`. A constant 0.5 makes the whole multi-timeframe candle
    detector arithmetically incapable of emitting anything, in both domains.
    Measured on the running deployment before this: 90 live Coinbase bars, 0
    clearing 0.6, max score exactly 0.5000, coverage basis None on every one.
    """
    s = _scorer()
    domain = s._get_domain(key)
    assert domain != "other", f"{key} resolves to OTHER, which has no detector"
    assert s._detector_for(key) is not None, key


def test_the_candle_keys_belong_to_the_domain_they_name():
    s = _scorer()
    assert s._get_domain("crypto_candle") == "crypto"
    assert s._get_domain("tradfi_candle") == "tradfi"
    assert s._get_domain("tradfi_trade") == "tradfi"
    assert s._get_domain("cyber_anomaly") == "cyber"


def test_a_candle_is_still_scored_apart_from_a_trade():
    """Pooling incommensurable feature spaces is the other way to get this wrong."""
    s = _scorer()
    assert s._detector_key("crypto_candle") != s._detector_key("crypto_trade")


def test_a_real_event_type_is_untouched_by_the_mapping():
    """The map answers for scoring keys only; events still resolve as events."""
    s = _scorer()
    assert s._get_domain("vessel_position") == "maritime"
    assert s._get_domain("prediction_market_trade") == "prediction"
    assert s._get_domain("not_a_real_type_at_all") == "other"


def test_the_mapping_is_written_out_rather_than_split_on_underscore():
    src = (ROOT / "services" / "enrichment" / "anomaly_scorer.py").read_text(encoding="utf-8")
    body = src[src.index("_SCORING_KEY_DOMAIN = {"):]
    body = body[:body.index("}") + 1]
    assert 'split("_")' not in body, (
        "deriving a domain from a prefix is the defect _get_domain's own "
        "docstring records, and the one the Hawkes tracker had"
    )
