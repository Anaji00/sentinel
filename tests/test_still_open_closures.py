"""Closures for items that had been standing on the still-open list.

Each of these was found by scanning rather than by a failure: a function with no
call site, a Redis list with no writer, an event type no rule referenced, a
column the insert omitted. None of them raised an error, and none would have.
"""

import ast
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

CORR = (ROOT / "services/correlation/main.py").read_text(encoding="utf-8")
RULE_AGENT = (ROOT / "services/agents/rule_agent.py").read_text(encoding="utf-8")
SCORER = (ROOT / "services/enrichment/anomaly_scorer.py").read_text(encoding="utf-8")
DRIFT = (ROOT / "services/telemetry-worker/drift_scheduler.py").read_text(encoding="utf-8")
GEN = (ROOT / "services/reasoning/scenario_generator.py").read_text(encoding="utf-8")
SAVE = (ROOT / "services/reasoning/main.py").read_text(encoding="utf-8")


# ── Financial signals that no rule could see ──────────────────────────────────

def _shipped_rules():
    import importlib.util
    spec = importlib.util.spec_from_file_location("corr_main_rules", ROOT / "services/correlation/main.py")
    m = importlib.util.module_from_spec(spec)
    sys.modules["corr_main_rules"] = m
    spec.loader.exec_module(m)
    return m.SHIPPED_RULES, m.RULE_DEFINITION_VERSION


def test_the_orphan_financial_types_now_have_rules():
    """83 earnings_surprise, 98 filing and 10 thirteen_f events in 48 hours,
    and not one rule referenced any of them."""
    rules, _ = _shipped_rules()
    covered = set()
    for r in rules:
        t = r["trigger_event_type"]
        covered |= set(t) if isinstance(t, list) else {t}
        for c in r.get("correlations", []):
            covered |= set(c.get("event_types") or [])
    for t in ("earnings_surprise", "filing", "thirteen_f", "insider_trade"):
        assert t in covered, t


def test_every_shipped_rule_names_only_real_event_types():
    """The failure mode the synthesised rules demonstrated."""
    from shared.models.events import EventType
    valid = {e.value for e in EventType}
    rules, _ = _shipped_rules()
    for r in rules:
        t = r["trigger_event_type"]
        named = set(t) if isinstance(t, list) else {t}
        for c in r.get("correlations", []):
            named |= set(c.get("event_types") or [])
        assert named <= valid, (r["rule_id"], named - valid)


def test_the_new_single_name_rules_ask_for_same_entity():
    rules, _ = _shipped_rules()
    for rid in ("rule_earnings_surprise_flow", "rule_insider_filing_flow",
                "rule_institutional_position_shift"):
        rule = next(r for r in rules if r["rule_id"] == rid)
        assert all(c.get("same_entity") for c in rule["correlations"]), rid


def test_the_version_was_bumped_so_the_rules_can_reach_production():
    """Without this the new definitions sit in code and never reconcile."""
    _, version = _shipped_rules()
    assert version >= 3


# ── Rule pruning ──────────────────────────────────────────────────────────────

def test_the_prune_engine_is_now_invoked():
    """A complete LLM engine for retiring stale rules that nothing called, so
    the rule set could only grow."""
    calls = [
        l for l in RULE_AGENT.splitlines()
        if "_maybe_prune_rules(" in l and not l.strip().startswith(("#", "async def", "def "))
    ]
    assert calls, "_maybe_prune_rules is never called"
    assert "_evaluate_and_prune_rules(decoded, current_context)" in RULE_AGENT


def test_pruning_is_rate_limited():
    """It costs a full inference on a host managing ~35 an hour."""
    assert "PRUNE_COOLDOWN_SEC" in RULE_AGENT
    assert "is_recently_processed(\"rule_prune_pass\"" in RULE_AGENT


# ── Model drift ───────────────────────────────────────────────────────────────

def test_the_scorer_feeds_the_drift_monitor():
    """Both its input lists were written by nothing."""
    assert "_record_score_sample(scores)" in SCORER
    assert "sentinel:ml:current_scores" in SCORER


def test_the_drift_sample_write_cannot_break_scoring():
    """It sits on the hot enrichment path."""
    tree = ast.parse(SCORER)
    fn = next(n for n in ast.walk(tree)
              if isinstance(n, ast.AsyncFunctionDef) and n.name == "_record_score_sample")
    assert any(isinstance(n, ast.Try) for n in ast.walk(fn))


def test_unmeasured_drift_is_not_reported_as_no_drift():
    """It returned drift_detected=False every hour while measuring nothing."""
    assert '"drift_detected": False, "psi": 0.0, "status": "initializing"' not in DRIFT
    assert '"drift_detected": None' in DRIFT
    assert "insufficient_samples" in DRIFT


def test_the_baseline_seeds_itself():
    assert "baseline_seeded" in DRIFT
    assert "sentinel:ml:baseline_scores" in DRIFT


# ── Scenario provenance ───────────────────────────────────────────────────────

def test_the_scenario_carries_its_evidence():
    """628 of 628 scenarios had this column empty."""
    assert "supporting_event_ids=[" in GEN
    assert "cluster.supporting_event_ids" in GEN


def test_the_insert_writes_the_column():
    """The insert named nine columns; the table defines thirteen."""
    insert = SAVE[SAVE.index("INSERT INTO scenarios"):]
    insert = insert[:insert.index("logger.info")]
    assert "supporting_event_ids" in insert
    assert "$10::uuid[]" in insert


def test_the_placeholders_match_the_columns():
    """An off-by-one here fails at runtime inside a caught handler, which is
    how the table stayed empty once before."""
    insert = SAVE[SAVE.index("INSERT INTO scenarios"):]
    header = insert[:insert.index("VALUES")]
    columns = [c.strip() for c in header[header.index("(") + 1:header.rindex(")")].split(",")]
    values = insert[insert.index("VALUES"):]
    values = values[:values.index(")")]
    placeholders = [v for v in values.split(",") if "$" in v]
    assert len(columns) == len(placeholders), (len(columns), len(placeholders))


# ── Redis key names ───────────────────────────────────────────────────────────

QUANT = (ROOT / "services/agents/quant_trading_engine.py").read_text(encoding="utf-8")
BASE = (ROOT / "services/agents/base.py").read_text(encoding="utf-8")
LIB = (ROOT / "services/reasoning/pattern_library.py").read_text(encoding="utf-8")


def test_the_covered_call_scoping_reads_the_key_that_exists():
    """sentinel:watched:equities is a 44-member zset the same file writes.
    One read site said :watchlist: instead, always came back empty, and the
    scoping check is skipped entirely when the set is None -- so the overlay
    ran for every ticker instead of the watchlist. It failed open."""
    assert "sentinel:watchlist:equities" not in QUANT
    assert QUANT.count('"sentinel:watched:equities"') >= 3


def test_the_watchlist_key_is_consistent_across_the_tree():
    for path in (ROOT / "services").rglob("*.py"):
        text = path.read_text(encoding="utf-8", errors="ignore")
        assert "sentinel:watchlist:equities" not in text, path


# ── Swarm memory ──────────────────────────────────────────────────────────────

def test_publishing_a_bulletin_records_a_memory():
    """write_agent_memory had no call site, so the shared memory held zero
    entries -- while read_agent_memories runs on every relevant macro dispatch
    and therefore returned nothing every time."""
    publish = BASE[BASE.index("async def publish_bulletin"):]
    publish = publish[:publish.index("\n    async def ", 10)]
    assert "write_agent_memory(" in publish


def test_the_memory_write_cannot_break_publication():
    fn = BASE[BASE.index("async def write_agent_memory"):]
    fn = fn[:fn.index("\n    async def ", 10)]
    assert "try:" in fn and "except" in fn


# ── Precedent balance ─────────────────────────────────────────────────────────

def test_precedents_reserve_room_for_the_minority_outcome():
    """216 confirmed and 0 denied, ordered by recency alone, so every precedent
    ever shown to the model was a scenario that came true."""
    assert "_balance_outcomes(" in LIB


def test_a_single_outcome_corpus_is_returned_unchanged():
    import importlib.util
    spec = importlib.util.spec_from_file_location("patlib", ROOT / "services/reasoning/pattern_library.py")
    m = importlib.util.module_from_spec(spec)
    sys.modules["patlib"] = m
    spec.loader.exec_module(m)
    cls = next(c for c in vars(m).values() if isinstance(c, type) and hasattr(c, "_balance_outcomes"))
    rows = [{"status": "confirmed", "created_at": i} for i in range(6)]
    assert len(cls._balance_outcomes(rows, 5)) == 5


def test_both_outcomes_appear_when_both_exist():
    import importlib.util, sys as _s
    m = _s.modules["patlib"]
    cls = next(c for c in vars(m).values() if isinstance(c, type) and hasattr(c, "_balance_outcomes"))
    rows = [{"status": "confirmed", "created_at": i} for i in range(20)]
    rows += [{"status": "denied", "created_at": 100 + i} for i in range(2)]
    out = cls._balance_outcomes(rows, 6)
    assert any(r["status"] == "denied" for r in out)
    assert any(r["status"] == "confirmed" for r in out)
    assert len(out) == 6


# ── The two memory keys ───────────────────────────────────────────────────────

def test_the_context_reads_the_memory_key_that_is_written():
    """fetch_global_context read sentinel:memory:shared, which appears exactly
    once in the tree -- that read -- and is written by nothing. So the SHARED
    SWARM MEMORIES section of the context every agent fetches was always empty,
    while write_agent_memory wrote to a different key nothing in that path read.
    Two halves of one mechanism, each pointed at a name the other did not use."""
    assert '"sentinel:memory:shared"' not in BASE
    fn = BASE[BASE.index("async def fetch_global_context"):]
    fn = fn[:fn.index("\n    @property")]
    assert '"sentinel:agents:episodic_memory"' in fn


def test_no_service_still_references_the_orphan_key():
    """Parsed, not grepped: the comment explaining the removal names it."""
    for path in (ROOT / "services").rglob("*.py"):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8", errors="ignore"))
        except SyntaxError:
            continue
        strings = [n.value for n in ast.walk(tree)
                   if isinstance(n, ast.Constant) and isinstance(n.value, str)]
        assert not any("sentinel:memory:shared" in v for v in strings), path


def test_cross_agent_bulletins_reach_the_prompt():
    """get_bulletins_for_prompt builds a prompt-ready block and had no call
    site, so no agent had ever seen another agent's bulletins."""
    tree = ast.parse(BASE)
    fn = next(n for n in ast.walk(tree)
              if isinstance(n, ast.AsyncFunctionDef) and n.name == "fetch_global_context")
    calls = [ast.unparse(n.func) for n in ast.walk(fn) if isinstance(n, ast.Call)]
    assert any("get_bulletins_for_prompt" in c for c in calls)


def test_the_bulletin_accessor_does_not_call_itself():
    """An earlier attempt inserted the call into the accessor's own return."""
    tree = ast.parse(BASE)
    fn = next(n for n in ast.walk(tree)
              if isinstance(n, ast.AsyncFunctionDef) and n.name == "get_bulletins_for_prompt")
    calls = [ast.unparse(n.func) for n in ast.walk(fn) if isinstance(n, ast.Call)]
    assert not any("get_bulletins_for_prompt" in c for c in calls)


def test_neither_context_addition_can_break_the_context():
    """Both sit inside the path four agents call on every dispatch."""
    fn = BASE[BASE.index("async def fetch_global_context"):]
    fn = fn[:fn.index("\n    @property")]
    assert fn.count("except Exception") >= 3


# ── The Form 4 feed ───────────────────────────────────────────────────────────

TRADFI = (ROOT / "services/collector-tradfi/main.py").read_text(encoding="utf-8")


def test_the_form4_poller_sends_an_acceptable_user_agent():
    """It sent "SENTINEL/1.0", which carries no contact address. Verified
    directly against SEC: that UA returns HTTP 403 while one with a contact
    address returns HTTP 200 and 22,643 bytes. The poller has therefore been
    blocked for the life of the deployment -- which is why there are zero
    insider_trade events, and why the insider clustering, the insider
    correlation rule and the Form 4 enricher have all sat idle."""
    poller = TRADFI[TRADFI.index("async def poll_form4"):]
    poller = poller[:poller.index("\nasync def ", 10)]
    assert '"User-Agent": "SENTINEL/1.0"' not in poller
    assert "SEC_USER_AGENT" in poller


def test_a_rejected_form4_fetch_is_logged():
    """It returned silently, so a 403 looked exactly like a quiet filing day."""
    poller = TRADFI[TRADFI.index("async def poll_form4"):]
    poller = poller[:poller.index("\nasync def ", 10)]
    block = poller[poller.index("if resp.status != 200:"):]
    block = block[:block.index("return") + 6]
    assert "logger.warning" in block


def test_both_sec_callers_share_one_setting():
    filings = (ROOT / "services/collector-filings/main.py").read_text(encoding="utf-8")
    assert 'os.getenv(' in filings and "SEC_USER_AGENT" in filings
    assert "SEC_USER_AGENT" in TRADFI


def test_the_form4_source_matches_what_the_enricher_routes():
    """source="sec_form4" is what dispatches to _enrich_insider, which produces
    EventType.INSIDER_TRADE. The chain was correct; only the fetch was blocked."""
    poller = TRADFI[TRADFI.index("async def poll_form4"):]
    poller = poller[:poller.index("\nasync def ", 10)]
    assert 'source="sec_form4"' in poller
    enricher = (ROOT / "services/enrichment/enrichers/tradfi.py").read_text(encoding="utf-8")
    assert 'source == "sec_form4"' in enricher
    assert "_enrich_insider(raw, p)" in enricher
