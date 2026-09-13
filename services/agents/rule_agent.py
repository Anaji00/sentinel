import asyncio
import json
import re
import logging
from typing import List, Dict, Optional, Any, Union
from datetime import datetime, timezone

from pydantic import BaseModel, ConfigDict, Field
from services.agents.base import SentinelAgent
from shared.kafka import Topics
from shared.utils.text import clip
from shared.utils.quiet_failures import swallowed
from shared.utils.entity_resolution import is_plausible_entity_name
from shared.utils.rule_feedback import firing_counts, rule_performance
from shared.utils.quiet_failures import dropped
from shared.models.correlation_rules import (
    CLAUSE_KEYS,
    is_seed_rule,
    clause_event_types,
    declares_a_join,
    trigger_event_types,
)

logger = logging.getLogger("agent.rule_synthesizer")

# How long the observed-vocabulary snapshot is reused before being re-measured.
_VOCAB_CACHE_SEC = 900
_vocab_cache: dict = {"at": 0.0, "types": [], "rules": []}


async def _observed_vocabulary(agent) -> tuple:
    """The event types this platform actually produces, and the rules it holds.

    The prompt previously named nine event types in prose, out of the
    forty-three the platform defines, and constrained only the trigger -- it
    said nothing at all about what may appear inside a correlation clause.
    That is where "location" and "vessel" came from: the model was asked to
    write in a DSL whose vocabulary it had to guess for the field that matters
    most.

    Measured types are used rather than the whole enum, because a type the
    platform has never emitted is not a useful thing to write a rule about, and
    because forty-three names is a large fraction of a 7,664-character budget.
    Falls back to the enum when the database cannot be reached, so the model is
    never left guessing.
    """
    import time as _time
    now = _time.monotonic()
    if _vocab_cache["types"] and (now - _vocab_cache["at"]) < _VOCAB_CACHE_SEC:
        return _vocab_cache["types"], _vocab_cache["rules"]

    types = []
    try:
        rows = await agent.db.query(
            """
            SELECT type, count(*) AS n
            FROM events
            WHERE occurred_at > now() - interval '24 hours'
            GROUP BY type
            HAVING count(*) > 0
            ORDER BY n DESC
            LIMIT 20
            """
        )
        types = [r["type"] for r in (rows or []) if r.get("type")]
    except Exception as e:
        agent.logger.debug("Could not measure event vocabulary: %s", e)

    if not types:
        try:
            from shared.models.events import EventType
            types = sorted({e.value for e in EventType})[:20]
        except Exception:
            types = []

    existing = []
    try:
        stored = await agent.redis.raw.hgetall("sentinel:correlation:dynamic_rules")
        for raw in (stored or {}).values():
            try:
                rule = json.loads(raw if isinstance(raw, str) else raw.decode("utf-8"))
            except (ValueError, AttributeError):
                continue
            name = rule.get("rule_name") or rule.get("rule_id")
            if name:
                existing.append(str(name))
    except Exception as e:
        agent.logger.debug("Could not read existing rules: %s", e)

    _vocab_cache.update({"at": now, "types": types, "rules": existing[:12]})
    return types, _vocab_cache["rules"]


def _unknown_event_types(rule) -> set:
    """Event types a synthesised rule names that the platform does not define.

    Covers the trigger and every correlation clause. Returns an empty set for a
    rule that names none at all -- an over-broad rule is a different problem
    from one that cannot fire, and is not this check's to reject.
    """
    try:
        from shared.models.events import EventType
        known = {e.value for e in EventType}
    except Exception:
        return set()

    named = set()
    trigger = getattr(rule, "trigger_event_type", None)
    if isinstance(trigger, str):
        named.add(trigger)
    elif isinstance(trigger, (list, tuple)):
        named.update(t for t in trigger if isinstance(t, str))

    for clause in (getattr(rule, "correlations", None) or []):
        types = clause.get("event_types") if isinstance(clause, dict) else getattr(clause, "event_types", None)
        if isinstance(types, str):
            named.add(types)
        elif isinstance(types, (list, tuple)):
            named.update(t for t in types if isinstance(t, str))

    return {t for t in named if t and t not in known}


class CorrelationDef(BaseModel):
    """One clause of a rule, in the DSL the correlation engine actually reads.

    Every field below is a key `evaluate_dynamic_rules` looks up. That
    equivalence is the point, and it did not hold: this model declared five
    fields while the evaluator read ten, and Pydantic's default is to *drop*
    what it was not told about.

    The consequence was specific and total. The synthesiser's prompt tells the
    model, in as many words, to set `"same_entity": true` for a rule about one
    company, and its worked example includes it -- and every rule the agent has
    ever produced arrived at the evaluator without it, because validation threw
    it away before storage. A join the prompt asked for, the model supplied and
    the engine supports was silently removed in between, so every synthetic
    rule correlated a block trade in one name with activity in another. That is
    the same defect the shipped rules were repaired for, reintroduced on the
    path that writes most of the rules.

    `extra="forbid"` so the next key added to the engine fails loudly here
    rather than being quietly discarded, and the accompanying test asserts the
    two vocabularies are identical.
    """

    model_config = ConfigDict(extra="forbid")

    event_types: List[str]
    hours: int
    min_anomaly: float
    tags: Optional[List[str]] = None
    # The joins. `region` takes a place name, or `true` meaning "wherever the
    # trigger is"; the others are flags.
    region: Optional[Union[str, bool]] = None
    same_entity: Optional[bool] = None
    shared_tags: Optional[bool] = None
    proximity_km: Optional[float] = None
    # Ordering. A rule that says its evidence came *before* the trigger is
    # making a different and stronger claim than one that says both happened
    # this week, and the engine has supported it since the informed-trading
    # sequence was written.
    precedes_trigger: Optional[bool] = None
    follows_trigger: Optional[bool] = None
    within_minutes: Optional[int] = None


# Checked at import, not only in a test.
#
# This model is the gate every synthesised rule passes through, and the way it
# failed was by declaring fewer fields than the evaluator reads -- silently, for
# the life of the deployment. A test catches that on the next run; this catches
# it before the service starts, which is where a contract between two services
# should be enforced.
_declared = set(CorrelationDef.model_fields)
if _declared != set(CLAUSE_KEYS):
    raise RuntimeError(
        "CorrelationDef has drifted from the correlation rule DSL. "
        f"only here: {sorted(_declared - set(CLAUSE_KEYS))}; "
        f"only in the DSL: {sorted(set(CLAUSE_KEYS) - _declared)}. "
        "A key the evaluator reads and this model does not declare is deleted "
        "in validation without a word."
    )
del _declared


class DynamicRule(BaseModel):
    # Deliberately not `forbid` here, unlike the clause above. A stray
    # top-level key -- a "rationale" the model volunteered -- is harmless and
    # costs nothing to drop; discarding the whole rule for it would be a worse
    # trade. Every key that changes what a rule *means* lives on the clause.

    rule_id: str
    rule_name: str
    # A list, as the shipped rules use. Declared as a bare string, a synthetic
    # rule could not express "any of these three types triggers this", which is
    # how every cyber and every sequence rule in the shipped set is written.
    trigger_event_type: Union[str, List[str]]
    conditions: Dict[str, Any] = Field(default_factory=dict)
    correlations: List[CorrelationDef]
    alert_tier: str
    tags: List[str]
    version: int = 1
    expires_at: int = Field(default_factory=lambda: int(datetime.now(timezone.utc).timestamp()) + 7 * 86400)

# A rule id has to be a stable, reusable identifier. The synthesiser was
# minting one per news story -- "KavinskyDeathAlert" fired 504 times in ten
# hours, one Gaza headline became RI-001/2/3 firing 160 times each, and
# "guardian_world_alert" is a newspaper section promoted to a detector. 2,587
# distinct rule ids exist, 1,206 of which fired exactly once, and the namespace
# grows by 200-400 a day against a prune cycle that runs every six hours.
#
# A rule is a reusable hypothesis about how event *types* co-occur. The patterns
# below are what a one-off looks like when it is written as a rule.
_EVENT_SHAPED_RULE_ID = re.compile(
    r"(death|died|dead|killed|obituary"        # a person, once
    r"|alert$"                                 # a headline wearing a rule's clothes
    r"|(19|20)\d{2}"                           # a year: a rule is not about one
    r"|[0-9a-f]{6,}"                           # a transponder hex or contract address
    r"|usdt$|usd$|btc$|eth$)",                 # one instrument
    re.IGNORECASE,
)

# Verbs that describe a single occurrence rather than a recurring pattern.
_ONE_OFF_NAME_TOKENS = {
    "found", "dead", "died", "killed", "announces", "announced", "approves",
    "approved", "wins", "won", "resigns", "resigned", "arrested", "indicted",
}

# The active synthetic set is held at this size. See _enforce_rule_ceiling.
MAX_ACTIVE_SYNTHETIC_RULES = 40

_FALLBACK_REGIONS = (
    "strait of hormuz", "strait of malacca", "turkish straits", "black sea",
    "red sea", "taiwan strait", "south china sea", "suez canal",
    "bab-el-mandeb", "gulf of guinea", "panama canal",
)


def _region_in_name(rule_name):
    """The chokepoint a rule name claims, if any."""
    try:
        from shared.utils.regions import get_all_region_names
        known = list(get_all_region_names())
    except Exception:
        known = list(_FALLBACK_REGIONS)
    low = (rule_name or "").lower()
    for r in known:
        if r and str(r).lower() in low:
            return str(r).lower()
    return None


def _is_reusable_rule(rule):
    """Return a rejection reason, or None if the rule describes a pattern.

    A rule named for a story cannot stop being true, so it never expires on its
    own merits -- it keeps matching whatever else is happening and carries the
    story's name into every alert it produces.
    """
    rid = str(getattr(rule, "rule_id", "") or "")
    rname = str(getattr(rule, "rule_name", "") or "")

    if _EVENT_SHAPED_RULE_ID.search(rid):
        return "rule_id " + repr(rid) + " names a single event rather than a pattern"

    name_tokens = {t.strip(".,:;-").lower() for t in rname.split()}
    hit = name_tokens & _ONE_OFF_NAME_TOKENS
    if hit:
        return "rule_name " + repr(rname) + " describes one occurrence (" + ", ".join(sorted(hit)) + ")"

    # A rule whose clauses filter on nothing matches the whole stream.
    if not getattr(rule, "correlations", None):
        return "no correlation clauses: the rule constrains nothing"

    # A rule named for a place must filter on that place. syn_3 is named
    # "Turkish Straits Traffic Drop" and carries region=null on both clauses,
    # so corrected event types would have matched vessel activity anywhere on
    # earth and reported it under that name.
    named_region = _region_in_name(rname)
    if named_region and not any(
        str(getattr(c, "region", "") or "").strip().lower() == named_region
        for c in rule.correlations
    ):
        return (
            "rule_name claims region " + repr(named_region) +
            " but no clause filters on it; it would match everywhere"
        )

    # A cross-domain clause that declares no join.
    #
    # The engine falls back to matching on a shared subject when a rule spans
    # domains and says nothing about what connects its sides, and counts every
    # time it has to. That fallback is a safety net, not a design: a rule
    # stored without a join is one whose author never decided what it means,
    # and it will sit in the rule set asserting co-occurrence under a name that
    # promises a relationship. This is the shape that produced 69% of the
    # correlation layer's output on nothing but a shared 48 hours.
    #
    # Rejected here rather than repaired, because the join a rule needs depends
    # on what the rule is claiming, and that is the model's decision to make --
    # the prompt now lists all six and gives the case for each.
    trigger_domains = _trigger_domains(rule)
    for clause in (getattr(rule, "correlations", None) or []):
        types = clause_event_types(clause)
        if _domains_of(types) - trigger_domains and not declares_a_join(clause):
            return (
                "clause " + repr(sorted(types)) + " spans domains and declares no "
                "join; it would correlate on a shared time window alone"
            )

    return None


def _domains_of(types) -> set:
    """The domains a list of event types belongs to, skipping what it cannot name."""
    from shared.models.events import event_domain
    out = set()
    for t in types:
        try:
            out.add(event_domain(t))
        except Exception:
            continue
    return out - {""}


def _trigger_domains(rule) -> set:
    """Every domain the rule can be triggered from.

    All of them, not the first. A rule triggering on ["ransomware",
    "price_anomaly"] already spans cyber and tradfi, so judging its clauses
    against whichever trigger happened to be written first would demand a join
    from a clause that is in the same domain as one of them.
    """
    return _domains_of(trigger_event_types(rule))


class RuleList(BaseModel):
    rules: List[DynamicRule]

class RuleSynthesizerAgent(SentinelAgent):
    """
    Subscribes to macro intelligence briefs and synthesizes/updates
    JSON DSL rules in Redis based on geopolitical and market conditions.
    """
    
    @property
    def output_topic(self) -> str:
        return Topics.RULES_SYNTHESIZED

    async def handle(self, message: dict) -> None:
        """
        Triggered when the Macro Strategist publishes a new brief OR when a rule fails.
        """
        # Feedback Loop: Handle Rule Failure
        if message.get("type") == "rule_failure":
            rule_id = message.get("rule_id")
            if rule_id:
                self.logger.warning(f"Deprecating failed rule: {rule_id}")
                # Remove from HASH
                await self.redis.raw.hdel("sentinel:correlation:dynamic_rules", rule_id)
                # Publish tombstone for hot-reloading
                tombstone = json.dumps({"rule_id": rule_id, "deprecated": True})
                await self.redis.raw.publish("sentinel:correlation:rule_updates", tombstone)
                # Emit Telemetry
                if self._producer:
                    await self._producer.send(Topics.TELEMETRY, {
                        "agent": "rule_synthesizer",
                        "event": "rule_deprecated",
                        "rule_id": rule_id,
                        "timestamp": datetime.now(timezone.utc).isoformat()
                    })
            return

        summary = ""
        entities = []
        prompt_context = ""

        # An analyst's verdict on a rule, which used to fall off the end.
        #
        # /feedback publishes to RULES_FEEDBACK and this agent subscribes to it,
        # so the topic-contract check reports the pairing as healthy -- a
        # producer and a consumer, correctly wired. What the check cannot see is
        # that the message reached the `else` below, found no
        # `brief.headline_summary`, and returned. Every analyst verdict the
        # platform has ever collected arrived here and was dropped on the line
        # after it arrived.
        #
        # A consumer that discards every message of a type still counts as a
        # consumer, which is why the wiring check passed for as long as this
        # did. The verdict is not a synthesis trigger -- it says an existing
        # rule is wrong, not that a new one is needed -- so it goes to the
        # curator that decides which rules survive.
        if message.get("source") == "analyst" and message.get("rule_id"):
            rule_id = str(message["rule_id"])
            record = await rule_performance(self.redis, rule_id)
            self.logger.info(
                "Analyst verdict on %s: %s (%s negative of %s, review=%s)",
                rule_id, message.get("verdict"), record["negative"],
                record["total"], record["needs_review"],
            )
            if record["needs_review"]:
                # Enough negative judgement to be worth the inference. The
                # cooldown inside still applies, so a run of complaints cannot
                # turn every verdict into a prune pass.
                await self._maybe_prune_rules(
                    f"An analyst marked rule {rule_id} as {message.get('verdict')}. "
                    f"{record['negative']} of {record['total']} verdicts on it are negative."
                )
            return

        # Branch based on message structure
        if "scenario_id" in message:
            summary = message.get("headline", "")
            hypotheses = message.get("hypotheses", [])
            sig = message.get("significance", "")
            self.logger.debug(f"Synthesizing rules based on Reasoning Scenario: {summary}")
            prompt_context = f"A new AI-generated reasoning scenario has been generated:\nSUMMARY: {summary}\nSIGNIFICANCE: {sig}\nHYPOTHESES: {hypotheses}"
        elif message.get("type") == "quant_discovery":
            summary = message.get("description", "")
            entities = message.get("correlated_assets", [])
            self.logger.debug(f"Synthesizing rules based on Quant Discovery: {summary}")
            prompt_context = f"A new quantitative peer relationship has been discovered:\nSUMMARY: {summary}\nASSETS: {entities}"
        else:
            brief = message.get("brief", {})
            summary = brief.get("headline_summary", "")
            entities = brief.get("entities", [])
            if not summary:
                # The end of the dispatch chain, and it was a bare return.
                #
                # Counted rather than dropped, the way the enrichment tier ends
                # its own chains: one unmatched shape is a new producer being
                # wired up, the same shape unmatched ten thousand times is a
                # feed being thrown away, and only the count tells them apart.
                dropped(
                    "agents.rule_synthesizer.unrouted_message",
                    "no branch matched this message shape",
                    self.logger,
                    detail=f"keys={sorted(message)[:8]}",
                )
                return
            self.logger.debug(f"Synthesizing rules based on macro shift: {summary}")
            prompt_context = f"A new macro intelligence brief has been issued:\nSUMMARY: {summary}\nENTITIES: {entities}"

        observed_types, existing_rules = await _observed_vocabulary(self)
        vocabulary = ", ".join(observed_types) if observed_types else "none measured"
        _nl = chr(10)
        already = (
            _nl + "RULES THAT ALREADY EXIST (do not restate these):" + _nl
            + "- " + (_nl + "- ").join(existing_rules)
            if existing_rules else ""
        )

        prompt = f"""=== SYNTHETIC RULE GENERATION ===
{prompt_context}

EVENT TYPES THIS PLATFORM EMITS. Every trigger_event_type and every entry in a
correlation's event_types MUST be one of these exactly. A rule naming anything
else can never match and will be rejected:
{vocabulary}
{already}

DIRECTIVES:
Synthesize up to 3 correlation rules. A rule earns its place by describing a
convergence that the listed event types can actually express.
- conditions.min_anomaly is required and must be above 0. A rule without one
  fires on every matching event and is noise.
- correlations must not be empty. An empty list means the rule corroborates
  nothing and will trigger on the bare event.
- hours is the lookback for a clause. Prefer the shortest window that could
  still capture the relationship; 48 hours of options flow will always find
  something.
- EVERY clause must say what connects its evidence to the trigger. A clause
  that declares no join asserts that falling in the same time window is itself
  the relationship, which for a rule spanning domains is not a finding. Use
  exactly one of:
    "same_entity": true   one company, ticker, vessel or wallet. "Block trade
                          and options activity converge" means in the SAME name.
    "region": true        wherever the trigger is -- for chokepoints, theatres,
                          and anything geographic. A literal place name also works.
    "shared_tags": true   the two sides name the same subject, when they cannot
                          share an identifier: a headline about a company and
                          that company's stock, a CPI print and what repriced.
    "proximity_km": 50    within this distance, for events carrying coordinates.
    "follows_trigger": true, "within_minutes": 240
                          the evidence came AFTER the trigger, inside the window
                          -- contagion and spillover, where the two sides are
                          genuinely different subjects.
    "precedes_trigger": true, "within_minutes": 4320
                          the evidence came BEFORE it. Positioning ahead of a
                          catalyst; the order is the whole signal.
  These are the only join keys. A clause naming anything else is rejected.
- Alert tiers: WATCH | ALERT | ELEVATED | INTELLIGENCE | CRITICAL. Reserve
  CRITICAL for convergences that would change a decision today.

Return raw JSON matching the RuleList schema. One complete rule, for shape:
{{"rules": [{{"rule_id": "syn_1",
  "rule_name": "Insider Sale Into Options Accumulation",
  "trigger_event_type": ["price_anomaly", "equity_block"],
  "conditions": {{"min_anomaly": 0.4}},
  "correlations": [{{"event_types": ["options_flow", "equity_block"],
                    "hours": 24, "min_anomaly": 0.5, "same_entity": true,
                    "precedes_trigger": true, "within_minutes": 2880}}],
  "alert_tier": "ELEVATED", "tags": ["equity", "insider"]}}]}}"""
        
        try:
            response = await self._execute_with_telemetry(
                message=message,
                system_prompt="You are SENTINEL Rule Architect. Synthesize precise JSON DSL correlation rules for real-time anomaly detection. Return ONLY raw JSON.",
                user_prompt=prompt,
                schema=RuleList,
                temperature=0.2
            )
            
            if hasattr(response, "rules") and response.rules:
                for rule in response.rules:
                    # A rule naming event types that do not exist cannot fire.
                    #
                    # The correlation engine matches trigger_event_type exactly
                    # against EventType, and three synthesised rules are sitting
                    # in production right now referencing "location" and
                    # "vessel" -- neither of which is among the 43 real types.
                    # They last produced a cluster on 2026-08-31 and have been
                    # inert since, with nothing raised and nothing logged: the
                    # engine simply never matched them.
                    #
                    # The model is being asked to write in a DSL whose
                    # vocabulary it has to guess. Checking its output against
                    # the real enum is the least that can be done, and it turns
                    # a silent dead rule into a visible rejection.
                    invalid = _unknown_event_types(rule)
                    if invalid:
                        self.logger.warning(
                            "Rejecting synthesised rule %s: names event types "
                            "that do not exist (%s). It could never fire.",
                            rule.rule_id, ", ".join(sorted(invalid)),
                        )
                        continue

                    # A rule has to be about a recurring pattern, not a story.
                    reason = _is_reusable_rule(rule)
                    if reason:
                        self.logger.warning(
                            "Rejecting synthesised rule %s: %s. A rule describing one "
                            "event outlives its subject and keeps matching whatever "
                            "else is happening.",
                            rule.rule_id, reason,
                        )
                        continue

                    # The namespace has no ceiling of its own: rule ids are
                    # minted faster than the prune cycle can retire them.
                    await self._enforce_rule_ceiling()

                    rule_json = json.dumps(rule.model_dump())
                    
                    # Store in HASH mapping rule_id -> rule_json (7-day auto-expiry timestamp included)
                    await self.redis.raw.hset("sentinel:correlation:dynamic_rules", rule.rule_id, rule_json)
                    
                    # Publish for hot-reloading in correlation engine
                    await self.redis.raw.publish("sentinel:correlation:rule_updates", rule_json)
                    
                    self.logger.info(f"Deployed new synthetic rule: {rule.rule_id} - {rule.rule_name}")
                    
                    if self._producer:
                        # 1. Publish to RULES_SYNTHESIZED
                        await self._producer.send(Topics.RULES_SYNTHESIZED, rule.model_dump(), key=rule.rule_id)

                        # 2. Emit Telemetry
                        await self._producer.send(Topics.TELEMETRY, {
                            "agent": "rule_synthesizer",
                            "event": "rule_created",
                            "rule_id": rule.rule_id,
                            "rule_name": rule.rule_name,
                            "timestamp": datetime.now(timezone.utc).isoformat()
                        })

                        # 3. Route discovered entity correlations through governed ONTOLOGY_PROPOSALS (§3.7)
                        # A tag is not an entity.
                        #
                        # This chained a synthesised rule's tags pairwise into
                        # MACRO_CORRELATED edges, both endpoints labelled
                        # `Company`, with no check that either string names a
                        # company. That is how "0.80" -- a confidence score the
                        # model printed into its own tag list -- became a node in
                        # the live graph with ten correlation edges to real
                        # issuers, and how "GLOBAL CONTEXT", a prompt heading,
                        # became another. Both then carried degree, and
                        # centrality is degree.
                        #
                        # The length ceiling was the only filter and it is the
                        # wrong axis: "0.80" is four characters.
                        rule_tags = [
                            t.upper() for t in (rule.tags or [])
                            if len(t) <= 10 and is_plausible_entity_name(t)
                        ]
                        if len(rule_tags) >= 2:
                            for i in range(len(rule_tags) - 1):
                                prop = {
                                    "entity_id": rule_tags[i],
                                    "action": "LINK_ENTITY",
                                    "data": {
                                        "target_id": rule_tags[i+1],
                                        "source_label": "Company",
                                        "target_label": "Company",
                                        "relation_type": "MACRO_CORRELATED",
                                        "weight": 0.85,
                                        "confidence": 0.80,
                                        "properties": {
                                            "rule_id": rule.rule_id,
                                            "rule_name": rule.rule_name,
                                            "alert_tier": rule.alert_tier,
                                        }
                                    }
                                }
                                await self._producer.send(Topics.ONTOLOGY_PROPOSALS, prop, key=rule_tags[i])
            # Prune after creating, so the rule set can shrink as well as grow.
            #
            # _evaluate_and_prune_rules is a complete engine for retiring
            # obsolete, duplicate and stale rules, and nothing called it. The
            # rule set could therefore only grow: three synthesised rules
            # naming event types the platform does not define had sat in Redis
            # since 2026-08-31, incapable of firing and never removed.
            #
            # Gated on a cooldown because it costs an inference on a host that
            # manages roughly thirty-five an hour, and the rule set does not
            # change fast enough to justify one per macro brief.
            await self._maybe_prune_rules(prompt_context)

        except Exception as e:
            self.logger.error(f"Failed to synthesize rules: {e}")

    # How long between prune passes. The rule set changes slowly and each pass
    # costs a full inference.
    PRUNE_COOLDOWN_SEC = 6 * 3600

    async def _enforce_rule_ceiling(self) -> None:
        """Keep the active synthetic rule set bounded by retiring the oldest.

        The prune engine runs behind a six-hour cooldown because it costs a full
        inference on a host managing about thirty-five an hour. That reasoning
        assumed the rule set does not change faster than that; it changes 200 to
        400 times a day. This is the cheap half -- no inference, oldest first --
        so the expensive judgement is not the only thing standing between the
        namespace and unbounded growth.
        """
        try:
            active = await self.redis.raw.hgetall("sentinel:correlation:dynamic_rules")
            if not active:
                return

            # Seed rules share this hash and are not the agent's to retire.
            #
            # The budget below is named for synthetic rules and was counting
            # both, so every rule added to the build quietly took one slot from
            # the synthesiser: seventeen shipped rules turned a budget of forty
            # into a budget of twenty-three, and nothing said so. Counting only
            # what this agent wrote is what keeps the platform's floor and its
            # capacity to discover independent of each other.
            aged = []
            for rid, rjson in active.items():
                rid_s = rid.decode() if isinstance(rid, bytes) else str(rid)
                try:
                    obj = json.loads(rjson.decode() if isinstance(rjson, bytes) else rjson)
                except Exception:
                    # A rule that cannot be parsed cannot be evaluated either,
                    # and cannot claim to be a seed rule.
                    aged.append((0, rid_s))
                    continue
                if is_seed_rule(obj):
                    continue
                aged.append((int(obj.get("expires_at") or 0), rid_s))

            if len(aged) <= MAX_ACTIVE_SYNTHETIC_RULES:
                return

            aged.sort()
            surplus = len(aged) - MAX_ACTIVE_SYNTHETIC_RULES
            for _, rid_s in aged[:surplus]:
                await self.redis.raw.hdel("sentinel:correlation:dynamic_rules", rid_s)
                await self.redis.raw.publish(
                    "sentinel:correlation:rule_updates",
                    json.dumps({"rule_id": rid_s, "deprecated": True}),
                )
            self.logger.info(
                "Retired %d synthetic rule(s) to hold the active set at %d.",
                surplus, MAX_ACTIVE_SYNTHETIC_RULES,
            )
        except Exception as e:
            self.logger.warning("Rule ceiling enforcement failed (set may grow): %s", e)

    async def _maybe_prune_rules(self, current_context: str) -> None:
        """Runs a prune pass when one is due.

        Reads the live rule set rather than taking it from the caller, so a
        prune reflects everything in Redis including rules other instances
        wrote.
        """
        try:
            if await self.is_recently_processed("rule_prune_pass", window_seconds=self.PRUNE_COOLDOWN_SEC):
                return
            active = await self.redis.raw.hgetall("sentinel:correlation:dynamic_rules")
            if not active:
                return
            decoded = {}
            for k, v in active.items():
                rid = k if isinstance(k, str) else k.decode("utf-8")
                rjson = v if isinstance(v, str) else v.decode("utf-8")
                # A seed rule is never a prune candidate.
                #
                # This handed the whole hash to a model asked to "identify any
                # rules that are obsolete, contradictory, or duplicate" and
                # deleted whatever ids it named. Nothing distinguished a rule
                # the build ships from one the agent invented last Tuesday, so
                # one prune pass could remove the only rule that lets this
                # platform see a tanker go dark in a chokepoint -- and nothing
                # would restore it until the correlation service next restarted
                # and reconciled the shipped set.
                try:
                    if is_seed_rule(json.loads(rjson)):
                        continue
                except (ValueError, TypeError) as _exc:
                    # Unparseable, so it cannot claim to be a seed rule -- and
                    # a rule the evaluator cannot read is a good prune
                    # candidate rather than a reason to stop. Counted, because
                    # a rising number here means something is writing junk into
                    # the rule hash.
                    swallowed("agents.rule_agent.unparseable_stored_rule", _exc, self.logger)
                decoded[rid] = rjson
            if not decoded:
                return
            await self.mark_processed("rule_prune_pass", window_seconds=self.PRUNE_COOLDOWN_SEC)
            self.logger.info("Evaluating %s active rules for pruning.", len(decoded))
            await self._evaluate_and_prune_rules(decoded, current_context)
        except Exception as e:
            self.logger.warning("Rule prune pass skipped: %s", e)

    async def _evaluate_and_prune_rules(self, active_rules: Dict[str, str], current_context: str) -> None:
        """
        LLM Reasoning Engine for Rule Pruning:
        Evaluates active correlation rules against current market context, how
        often each has fired, and what analysts said about it when it did.

        The last two are new here, and the docstring claimed one of them
        already: it said "current market context and hit rates" while the
        summaries carried a rule name, a trigger type and an expiry. The model
        was being asked which rules are obsolete from their names.

        Both inputs existed. `correlation_rule_fired:{rule_id}` is incremented
        on every firing and published to the metrics hash. `/feedback` records
        an analyst's verdict per rule and flags `needs_review` -- and that flag
        was read by nothing at all: no component fetched the endpoint, no
        service read the key, and the counters expired on a TTL. An analyst
        could mark the same rule wrong every day for a week and the curator
        deciding its fate would never hear about it.
        """
        if not active_rules:
            return

        fired = await firing_counts(self.redis)

        rule_summaries = []
        for r_id, r_json in active_rules.items():
            try:
                r_obj = json.loads(r_json)
            except Exception as _exc:
                swallowed("agents.rule_agent.prune_summary_parse", _exc, self.logger)
                continue
            record = await rule_performance(self.redis, r_id, fired)
            rule_summaries.append({
                "rule_id": r_id,
                "rule_name": r_obj.get("rule_name"),
                "trigger_event": r_obj.get("trigger_event_type"),
                "expires_at": r_obj.get("expires_at"),
                "times_fired": record["times_fired"],
                "analyst_verdicts": record["total"],
                "analyst_negative": record["negative"],
                "flagged_by_analysts": record["needs_review"],
            })

        if not rule_summaries:
            return

        prune_prompt = f"""
        You are the Sentinel Rule Engine Pruning Curator.
        CURRENT MARKET CONTEXT:
        {current_context}

        ACTIVE DYNAMIC CORRELATION RULES:
        {json.dumps(rule_summaries, separators=(',', ':'))}

        Each rule carries its record:
          - times_fired: how often the correlation engine has matched it.
          - analyst_verdicts / analyst_negative: how many times an analyst
            judged a finding from this rule, and how many of those were
            negative.
          - flagged_by_analysts: true when there is enough feedback to judge
            and the negative share is above the review threshold.

        Weigh that record above the rule's name. A rule analysts have
        repeatedly marked wrong is a prune candidate whatever it is called; a
        rule that has never fired is untested rather than useless, and firing
        often with no negative verdicts is the opposite of obsolete.

        Analyze each rule's relevance to current market conditions. Identify any rules that are obsolete, contradictory, or duplicate.
        Return a JSON list of rule_ids that should be PRUNED and REMOVED from active correlation evaluation.
        """

        try:
            class PruneDecision(BaseModel):
                prune_rule_ids: List[str] = Field(default_factory=list)
                reasoning: str = ""

            decision: PruneDecision = await self._execute_with_telemetry(
                message={"type": "prune_eval"},
                system_prompt="You evaluate and prune obsolete correlation rules.",
                user_prompt=prune_prompt,
                schema=PruneDecision,
                temperature=0.1
            )

            for target_id in decision.prune_rule_ids:
                if target_id in active_rules:
                    self.logger.info(f"🧠 LLM Pruned obsolete rule: {target_id} | Rationale: {clip(decision.reasoning, 100)}")
                    await self.redis.raw.hdel("sentinel:correlation:dynamic_rules", target_id)
                    tombstone = json.dumps({"rule_id": target_id, "deprecated": True})
                    await self.redis.raw.publish("sentinel:correlation:rule_updates", tombstone)
        except Exception as e:
            self.logger.warning(f"Failed to execute LLM rule pruning: {e}")
