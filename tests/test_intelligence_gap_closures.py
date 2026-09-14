"""The seventeen intelligence gaps from the three deep scans, as tests.

Every measurement quoted was taken against the running deployment before the
repair. Where the repair is data rather than code, the test pins the code that
stops it recurring.
"""
import math
import pathlib
import time

import numpy as np
import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]


# ── 250/264: the semantic store's payload, and its stale domains ─────────────

SOFT = (ROOT / "services" / "correlation" / "soft_correlator.py").read_text(encoding="utf-8")
CORR = (ROOT / "services" / "correlation" / "main.py").read_text(encoding="utf-8")


@pytest.mark.parametrize("field", ["headline", "summary", "entity_name", "entity_id", "source"])
def test_the_payload_carries_every_field_the_consumer_reads(field):
    """Six keys were stored; the semantic path read back eleven."""
    body = SOFT[SOFT.index('"payload": {'): SOFT.index("except Exception as e:", SOFT.index('"payload": {'))]
    assert f'"{field}"' in body, field


def test_a_stale_pseudo_domain_is_normalised_on_read():
    """32.5% of 12,000 sampled points carried a `.split("_")[0]` prefix."""
    from services.correlation.soft_correlator import _canonical_domain_name as f

    # The nine pseudo-domains observed live, and what each one is.
    assert f("flight") == "aviation"
    assert f("vessel") == "maritime"
    assert f("equity") == "tradfi"
    assert f("options") == "tradfi"
    assert f("filing") == "tradfi"
    assert f("earnings") == "tradfi"
    assert f("bgp") == "cyber"
    assert f("ransomware") == "cyber"
    assert f("headline") == "news"
    # Canonical values pass through untouched.
    for d in ("aviation", "maritime", "tradfi", "cyber", "news", "crypto", "prediction"):
        assert f(d) == d
    # Absent stays absent: a missing domain is not evidence of one.
    assert f(None) is None
    assert f("") is None
    assert f("not-a-domain-at-all") is None


@pytest.mark.parametrize("pair", [
    ("aviation", "flight"), ("market", "tradfi"), ("maritime", "vessel"),
    ("cyber", "ransomware"), ("options", "tradfi"),
    ("filing", "tradfi"), ("equity", "tradfi"),
])
def test_the_false_cross_domain_pairs_collapse_to_one_domain(pair):
    """Every pair 812 of 812 clusters declared in 24 hours, and all were false."""
    from services.correlation.soft_correlator import _canonical_domain_name as f

    resolved = {f(d) for d in pair if f(d)}
    assert len(resolved) == 1, f"{pair} still reads as {resolved}"


def test_the_ambiguous_prefix_resolves_to_nothing_rather_than_a_guess():
    """`market` is the prefix of the one type two domains both emit.

    `["crypto","market"]` was one of the eight false pairs, and unlike the
    others it cannot be collapsed by a lookup: market_anomaly comes from the
    crypto candle path and the equity candle path alike, which is precisely what
    AMBIGUOUS_EVENT_TYPES records. Unresolved means uncounted, so the cluster
    reads as the single domain it actually has.
    """
    from services.correlation.soft_correlator import _canonical_domain_name as f

    assert f("market") is None
    assert {d for d in (f("crypto"), f("market")) if d} == {"crypto"}


def test_find_similar_normalises_before_returning():
    assert 'payload["domain"] = _canonical_domain_name(payload.get("domain"))' in SOFT


def test_an_unnamed_supporting_event_is_not_a_distinct_subject():
    """`or idx` made distinct_subjects always len(kept), which is capped at 3."""
    assert "or e.get(\"entity_id\") or idx" not in CORR
    assert "_named_subjects" in CORR and "unnamed_subjects" in CORR


def test_an_undescribed_event_is_named_rather_than_called_unknown():
    assert "no description stored" in CORR


# ── 251: fabricated graph statistics ────────────────────────────────────────

SUP = (ROOT / "services" / "agents" / "supervisor.py").read_text(encoding="utf-8")


def test_an_unmeasured_statistic_is_absent_not_zero():
    """392,810 of 399,070 edges carried p_value = 0.0 with no method recorded."""
    from services.agents.supervisor import _edge_stats as stats

    empty = stats({})
    assert empty["p_value"] is None, "a p-value nobody measured must not read as 0.0000"
    assert empty["coefficient"] is None
    assert empty["f_stat"] is None
    assert empty["lag"] is None

    measured = stats({"p_value": 0.0014, "coefficient": 0.66, "lag": 2})
    assert measured["p_value"] == pytest.approx(0.0014)
    assert measured["coefficient"] == pytest.approx(0.66)
    assert measured["lag"] == 2
    # A genuine zero coefficient is still a measurement.
    assert stats({"coefficient": 0.0})["coefficient"] == 0.0


def test_the_zero_fill_is_gone_from_both_write_sites():
    code = "\n".join(l for l in SUP.splitlines() if not l.lstrip().startswith("#"))
    assert 'props.get("p_value", 0.0)' not in code
    assert 'props.get("coefficient", 0.0)' not in code


def test_an_unrated_edge_is_left_unrated():
    """The readers coalesce onto UNRATED_EDGE_CONFIDENCE; a 1.0 default hid it."""
    from services.agents.supervisor import _as_unit_interval as f

    assert f(None, default=None) is None
    assert f(0.9, default=None) == pytest.approx(0.9)
    # Percentages still rescale, and out-of-range values still clamp.
    assert f(95.0, default=None) == pytest.approx(0.95)

    # The link writer specifically. Node proposals elsewhere in that file keep
    # their 1.0 on purpose: "this vessel exists" is a claim the AIS feed does
    # make with certainty, and the finding was about edges.
    writer = (ROOT / "services" / "enrichment" / "graph_writer.py").read_text(encoding="utf-8")
    link = writer[writer.index("normalized_rel = normalize_predicate("):]
    link = link[: link.index("async def add_tags")]
    assert 'properties.get("confidence", 1.0)' not in link
    assert 'properties.get("confidence") is not None else None' in link


# ── 252: the correlation window's prune ─────────────────────────────────────


def test_the_prune_counter_exists_before_it_is_incremented():
    """`+= 1` raised AttributeError on every write: 7,400 counted failures."""
    from services.correlation.event_store import EventStore, PRUNE_EVERY_N_WRITES

    store = EventStore(redis_client=None, db_client=None)
    assert hasattr(store, "_writes_since_prune")
    # At the threshold, so the first write of a new process evicts the backlog
    # the previous one left rather than waiting 250 more.
    assert store._writes_since_prune == PRUNE_EVERY_N_WRITES


# ── 253: the fallback estimator's state ─────────────────────────────────────


def test_the_fallback_estimator_exists_on_the_rrcf_path():
    """`_insert_rrcf` falls back when every tree throws; 2,250 events were lost."""
    from shared.utils.streaming_detectors import RRCFDetector

    d = RRCFDetector(num_trees=4, window_size=32, shingle_size=1)
    assert hasattr(d, "_ema_mean")
    assert hasattr(d, "_ema_var")
    assert hasattr(d, "_ema_alpha")


def test_the_fallback_runs_when_the_forest_throws():
    """The path that raised AttributeError and dead-lettered the whole batch."""
    from shared.utils.streaming_detectors import RRCFDetector

    d = RRCFDetector(num_trees=2, window_size=16, shingle_size=1)
    for i in range(20):
        d.insert(np.array([float(i)]))

    class _Exploding:
        def __getattr__(self, _name):
            raise RuntimeError("tree is corrupt")

    d._forest = [_Exploding(), _Exploding()]
    score = d.insert(np.array([99.0]))          # must not raise
    assert 0.0 <= score <= 1.0


# ── 258: candle timestamps ──────────────────────────────────────────────────


def test_a_bar_still_open_is_never_stamped_in_the_future():
    """366 of 895 market_anomaly events were dated up to 14,398s ahead."""
    from datetime import datetime, timedelta, timezone

    from shared.utils.candles import candle_observation_ts

    # The clamp is to the function's own `now`, which is necessarily later than
    # one taken before the call. Comparing against the earlier reading made this
    # a race the test lost by microseconds -- intermittently, in CI, for a
    # reason having nothing to do with the code under test. The bound is taken
    # after the call instead.
    started = datetime.now(timezone.utc)
    for tf in (1, 5, 15, 30, 60, 240):
        block = {"start_ts": (started - timedelta(seconds=30)).isoformat()}
        stamped = candle_observation_ts(block, tf)
        now = datetime.now(timezone.utc)
        assert stamped <= now, f"{tf}m frame stamped {(stamped - now).total_seconds()}s ahead"


def test_a_completed_bar_is_stamped_at_its_close():
    from datetime import datetime, timedelta, timezone

    from shared.utils.candles import candle_observation_ts

    now = datetime.now(timezone.utc)
    block = {"start_ts": (now - timedelta(minutes=300)).isoformat()}
    stamped = candle_observation_ts(block, 240)
    assert abs((stamped - (now - timedelta(minutes=60))).total_seconds()) < 2


def test_both_candle_paths_use_the_one_rule():
    crypto = (ROOT / "services" / "enrichment" / "enrichers" / "crypto.py").read_text(encoding="utf-8")
    tradfi = (ROOT / "services" / "enrichment" / "enrichers" / "tradfi.py").read_text(encoding="utf-8")
    assert "candle_observation_ts(block, tf)" in crypto
    assert "candle_observation_ts(block, tf)" in tradfi
    assert 'occurred_at=datetime.fromisoformat(block["start_ts"])' not in tradfi


# ── 260: sentiment saturation ───────────────────────────────────────────────


def test_one_keyword_no_longer_reaches_the_top_of_the_scale():
    """995 of 1,605 headlines sat at 0.00, 351 at -1.00 and 222 at +1.00."""
    from services.enrichment.enrichers.news import _sentiment

    single = _sentiment("Oil prices surge")
    assert 0.0 < abs(single) < 0.5, single

    richer = _sentiment(
        "Missile strike kills dozens amid collapse of ceasefire and default fears"
    )
    assert abs(richer) > abs(single), (richer, single)
    assert abs(richer) < 1.0, "no amount of evidence should reach certainty"

    assert _sentiment("Quiet session for markets") == 0.0


def test_sentiment_ordering_survives_the_shrinkage():
    from services.enrichment.enrichers.news import _sentiment

    weak = abs(_sentiment("Stocks climb"))
    strong = abs(_sentiment("War escalates as sanctions widen and default looms amid collapse"))
    assert weak < strong


# ── 261/263: corroboration, keyed on publication rather than arrival ────────


def test_two_outlets_corroborate_across_an_arrival_lag():
    """Headlines arrive a mean of 2h50m late and up to 11.5 hours."""
    from shared.utils.corroboration import CorroborationTracker

    t = CorroborationTracker()
    now = time.time()
    published = now - 8 * 3600           # both filed eight hours ago...
    t.observe("Iran seizes tanker in Strait of Hormuz amid rising tensions",
              source="reuters", reliability=0.9, now=now, published_at=published)
    second = t.observe("Tanker seized by Iran in Strait of Hormuz as tensions rise",
                       source="ap", reliability=0.9, now=now, published_at=published + 1800)

    assert second.is_single_sourced is False
    assert second.independent_sources == 2
    assert second.corroboration_score > 0.5


def test_stories_genuinely_far_apart_still_do_not_corroborate():
    from shared.utils.corroboration import CorroborationTracker

    t = CorroborationTracker()
    now = time.time()
    t.observe("Iran seizes tanker in Strait of Hormuz amid rising tensions",
              source="reuters", reliability=0.9, now=now, published_at=now - 30 * 3600)
    second = t.observe("Tanker seized by Iran in Strait of Hormuz as tensions rise",
                       source="ap", reliability=0.9, now=now, published_at=now - 2 * 3600)
    assert second.is_single_sourced is True


def test_a_future_dateline_cannot_hold_a_claim_open_forever():
    from shared.utils.corroboration import CorroborationTracker

    t = CorroborationTracker()
    now = time.time()
    a = t.observe("Something happens somewhere important today",
                  source="wire", reliability=0.5, now=now, published_at=now + 90 * 86400)
    assert a.is_single_sourced is True


def test_the_news_enricher_passes_publication_time():
    news = (ROOT / "services" / "enrichment" / "enrichers" / "news.py").read_text(encoding="utf-8")
    assert "published_at=(" in news


# ── 262: the two stores nothing wrote ───────────────────────────────────────


def test_the_entity_sentiment_the_scorer_reads_now_has_a_writer():
    news = (ROOT / "services" / "enrichment" / "enrichers" / "news.py").read_text(encoding="utf-8")
    scorer = (ROOT / "services" / "enrichment" / "anomaly_scorer.py").read_text(encoding="utf-8")
    assert "sentinel:semantic_sentiment:" in scorer, "the reader"
    assert "sentinel:semantic_sentiment:" in news, "and now the writer"
    assert "_record_entity_sentiment" in news


def test_the_vessel_watchlist_the_scorer_reads_now_has_a_writer():
    """Reader and writer, checked by the key they share rather than its spelling.

    This asserted the literal `sentinel:watched:vessels` in both files, which
    is the thing that was wrong: two files spelling a key out is how
    `sentinel:watched:equities` came to be read under a name nobody wrote. Both
    now import it from `shared.utils.watchlists`, so the property to check is
    that they resolve to the same constant -- not that they contain the same
    string.
    """
    from shared.utils.watchlists import WATCHED_VESSELS_KEY

    maritime = (ROOT / "services" / "enrichment" / "enrichers" / "maritime.py").read_text(encoding="utf-8")
    scorer = (ROOT / "services" / "enrichment" / "anomaly_scorer.py").read_text(encoding="utf-8")

    assert WATCHED_VESSELS_KEY == "sentinel:watched:vessels"
    assert "WATCHED_VESSELS_KEY" in scorer, "the reader"
    assert "WATCHED_VESSELS_KEY" in maritime, "and the writer"
    for source in (maritime, scorer):
        assert f'"{WATCHED_VESSELS_KEY}"' not in source, (
            "the key is spelled out again rather than imported"
        )
    assert "_watch_vessel" in maritime


# ── 259: the discovery engine's reach ───────────────────────────────────────

DISC = (ROOT / "services" / "correlation" / "statistical_discovery.py").read_text(encoding="utf-8")


def test_candidates_come_from_the_data_as_well_as_the_hand_list():
    """17 pairs were evaluated against 1,181 tickers and 2,335,815 bars."""
    assert "_tickers_with_history" in DISC
    assert "liquid = await self._tickers_with_history()" in DISC


def test_symbols_with_no_history_are_named_rather_than_dropped_silently():
    """TNX, DXY, BTC-USD, ETH-USD, EURUSD and XLI had zero rows, for months."""
    assert "no usable price history" in DISC


# ── 257: identifiers that embed a mutable value ─────────────────────────────


@pytest.mark.parametrize("decorated,clean", [
    ("SI=F ($59.06)", "SI=F"),
    ("MU ($932.93)", "MU"),
    ("CPB [21.53]", "CPB"),
    ("ES=F (12.5%)", "ES=F"),
])
def test_a_price_is_stripped_from_an_identifier(decorated, clean):
    from shared.models.events import graph_node_id

    assert graph_node_id(decorated, "Entity") == clean


@pytest.mark.parametrize("kept", ["NVDA (Class A)", "TSM (finorion)"])
def test_a_distinguishing_parenthetical_is_left_alone(kept):
    from shared.models.events import graph_node_id

    assert graph_node_id(kept, "Entity") == kept


# ── 265: the radar's statistic ──────────────────────────────────────────────


def test_the_radar_statistic_has_somewhere_to_live():
    from shared.models.events import FinancialData

    assert "z_score" in FinancialData.model_fields
    fd = FinancialData(ticker="SGOV", z_score=4.2, underlying_price=100.5)
    assert fd.z_score == pytest.approx(4.2)


def test_the_radar_reads_the_key_its_collector_sends():
    """`close_price` is sent; `price` was read, so underlying_price was 0.0."""
    tradfi = (ROOT / "services" / "enrichment" / "enrichers" / "tradfi.py").read_text(encoding="utf-8")
    assert 'p.get("close_price") or p.get("price")' in tradfi
    assert "z_score=round(z_score, 4)" in tradfi

    collector = (ROOT / "services" / "collector-radar" / "main.py").read_text(encoding="utf-8")
    assert '"close_price": close_price' in collector


# ── 266: the event-to-cluster back-link ─────────────────────────────────────


def test_a_persisted_correlation_links_back_to_its_events():
    """20,256 of 20,271 scored events carried no correlation_ids."""
    store = (ROOT / "services" / "correlation" / "event_store.py").read_text(encoding="utf-8")
    assert "_link_events_to_correlation" in store
    assert "array_append" in store
    # Idempotent: a replayed cluster must not accumulate duplicates.
    assert "@> ARRAY[$1::uuid]" in store


# ── 254: predictions, and therefore scorecards ──────────────────────────────

BASE = (ROOT / "services" / "agents" / "base.py").read_text(encoding="utf-8")


def test_a_directional_bulletin_is_recorded_as_a_prediction():
    """Zero scorecards existed; the only six predictions were retired unscored.

    This asserted the literal source line `expected_direction in ("up","down")`,
    which pinned one spelling of the rule rather than the rule. The gate is now
    the shared vocabulary -- the same one the resolver and the Subjective Logic
    mapper read -- so the behaviour is asserted instead of the text, and
    "bearish" is no longer fused by consensus while being invisible to the
    recorder.
    """
    from services.agents.base import is_scoreable_direction

    assert "_record_bulletin_prediction" in BASE
    assert "is_scoreable_direction(expected_direction)" in BASE

    # A directional claim is recorded, whichever word states it.
    for word in ("up", "down", "bullish", "bearish", "long", "short", "SELL"):
        assert is_scoreable_direction(word), word

    # Anything that is not a directional claim is not invented into one.
    for word in ("neutral", "uncertain", "", None, "sideways"):
        assert not is_scoreable_direction(word), word


def test_a_bulletin_without_a_price_is_skipped_rather_than_stored_unresolvable():
    """entry_price=0.0 is exactly how the six that existed came to be retired."""
    body = BASE[BASE.index("async def _record_bulletin_prediction"):]
    body = body[: body.index("\n    async def publish_bulletin")]
    assert "entry <= 0" in body
    assert "agent_prediction_unpriced_total" in body


# ── 255: the swarm's shared focus ───────────────────────────────────────────


def test_more_than_one_agent_consults_the_focus_set():
    """277 agent pairs at Jaccard 0.00; nine agents offered, one read."""
    offered = "offer_focus(" in BASE
    assert offered, "publish_bulletin offers every subject"

    readers = [
        p for p in (ROOT / "services" / "agents").glob("*.py")
        if "prioritise(" in p.read_text(encoding="utf-8")
        and "def prioritise" not in p.read_text(encoding="utf-8")
    ]
    assert len(readers) >= 2, [p.name for p in readers]


def test_the_focus_order_is_additive_not_restrictive():
    """A candidate an agent chose must never be dropped by the focus set."""
    import asyncio

    from shared.utils.focus import prioritise

    class _Raw:
        async def zrevrange(self, *a, **k):
            return [b"NVDA"]

        async def zrange(self, *a, **k):
            return [b"NVDA"]

    class _Client:
        raw = _Raw()

    candidates = ["AAPL", "NVDA", "MSFT"]
    out = asyncio.run(prioritise(_Client(), candidates))
    assert sorted(out) == sorted(candidates)


# ── 254/255 completion: the scorecard chain, and the swarm's shared focus ────


def test_the_scorecard_chain_is_complete_end_to_end():
    """Driven on the running stack: 2 predictions -> resolve -> a real scorecard.

    `sentinel:agents:scorecard*` held zero keys, so every agent carried the
    unproven weight and the Subjective Logic fusion was uniform by construction.
    Recording a prediction was the half that could be shown immediately; the
    scorecard needs a resolution, and the horizon is 24 hours. Driven with the
    deadline backdated, `resolve_due_predictions` produced
    `predictions_made: 2, predictions_correct: 1, brier_score: 0.2,
    consensus_weight: 0.8` -- a weight that is not the unproven default, which
    is the whole point of it existing.

    This pins the wiring the drive exercised.
    """
    assert "_resolve_predictions_loop" in BASE
    assert "await self.resolve_due_predictions()" in BASE
    assert "await self.update_scorecard(" in BASE


def test_the_focus_set_is_fed_from_the_abundant_signal():
    """Four bulletins across nine agents cannot fill a twelve-slot focus set.

    `offer_focus` was called from `publish_bulletin` and the scenario tracker
    only. Every agent tier consumes Topics.CORRELATIONS, and a cluster the
    engine graded ELEVATED or above is exactly the shape of thing worth a second
    opinion -- so the set is now fed from the signal there are thousands of a
    day rather than the one there are four of.
    """
    corr = (ROOT / "services" / "correlation" / "main.py").read_text(encoding="utf-8")
    assert "offer_focus(" in corr
    assert "_FOCUS_WORTHY_TIERS" in corr

    from services.correlation.main import _FOCUS_WORTHY_TIERS

    assert _FOCUS_WORTHY_TIERS == {"ELEVATED", "INTELLIGENCE", "CRITICAL"}
    # WATCH and MONITOR are the engine saying "noted", which is not a reason to
    # spend a second agent's inference.
    assert "WATCH" not in _FOCUS_WORTHY_TIERS
    assert "MONITOR" not in _FOCUS_WORTHY_TIERS


def test_three_agents_now_consult_the_focus_set():
    """One did. The measure that matters is two opinions on one entity."""
    agents = ROOT / "services" / "agents"
    readers = sorted(
        p.name for p in agents.glob("*.py")
        if "prioritise(self.redis" in p.read_text(encoding="utf-8")
    )
    assert len(readers) >= 3, readers
    for expected in ("quant_trading_engine.py", "radar_agent.py", "stock_correlation_agent.py"):
        assert expected in readers, (expected, readers)
