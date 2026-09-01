"""
services/reasoning/scenario_generator.py

SENTINEL SCENARIO GENERATOR — OLLAMA EDITION
=============================================
Replaces the Gemini-based implementation entirely.
All inference now runs against local Llama3 via Ollama.

Why this works without Gemini:
  Gemini's primary advantage here was native JSON schema enforcement via
  types.Schema. Our OllamaClient provides equivalent enforcement through a
  Pydantic retry loop. The output quality difference on a well-prompted
  scenario synthesis task is minimal — Llama3 70B is competitive with
  Gemini Flash for structured analytical text generation.

  Additional benefits of going full-Ollama:
    - No API cost or rate limits
    - No external network dependency in the critical synthesis path
    - Consistent inference behavior (same model as agents)
    - Single LLM stack to monitor and debug

Architecture:
  ScenarioGenerator.generate() is called by reasoning/main.py.
  Interface is identical to the Gemini version — callers don't change.
  Internal implementation uses OllamaClient from shared/utils/ollama.py,
  which is the same client the agent swarm uses.

Prompt design for Llama3:
  Scenario synthesis requires broader narrative reasoning than the agent
  tasks (peer discovery, entity classification). The system prompt is
  longer and more structured to compensate for the fact that we're asking
  for a multi-hypothesis intelligence brief, not a simple classification.
  Temperature is set higher than agents (0.25 vs 0.1) to get more diverse
  hypotheses while still maintaining JSON structure.
"""

import asyncio
import json
import re
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Literal

import aiohttp
from pydantic import BaseModel, Field
from shared.models.events import ResolutionSignal

from shared.models import CorrelationCluster, Scenario, ScenarioStatus
from shared.utils.ollama import deliverable_prompt_chars, OllamaClient, SchemaViolationError

logger = logging.getLogger("reasoning.generator")

# ── SYSTEM PROMPT ─────────────────────────────────────────────────────────────
# More detailed than agent prompts because scenario synthesis requires broader
# narrative reasoning across multiple domains simultaneously.

SCENARIO_SYSTEM_PROMPT = """You are SENTINEL, an elite multi-domain intelligence analyst across geopolitics, maritime security, financial microstructure, cyber infrastructure, and military movements.

You are synthesizing a CONFIRMED ANOMALY CLUSTER — multi-domain correlated signals detected by SENTINEL.

ANALYTICAL DIRECTIVES:
- Treat signals as probabilistic indicators; separate causal drivers from background noise.
- Formulate 3 distinct hypotheses spanning baseline, alternative, and high-impact tail risks.
- Prioritize cross-domain convergence (e.g. maritime AIS dark + insider options sweep + cyber CVE).
- Ensure watch/deny signals specify concrete, observable events (entities, tickers, MMSI, locations, thresholds).
- Calibrate prior probabilities to sum to 100%.

OUTPUT RULES:
1. Return ONLY valid JSON matching the schema. No markdown wrappers or preamble.
2. Every schema field is REQUIRED.
3. "hypotheses" MUST contain exactly 3 hypotheses.
4. "confidence_overall" must be an integer [0-100].
5. "recommended_monitoring" must be an array of actionable indicator strings.
6. Every hypothesis MUST have at least one watch_signal. A hypothesis with an
   empty watch_signals list can never be confirmed or denied, and is discarded.
7. Every signal's "entity" must be a specific name taken from the SIGNAL section
   -- a ticker, wallet, vessel or country. Never a category: "the market",
   "shipping" and "cryptocurrency exchanges" are all rejected.
   Example: {"entity": "ETHUSDT", "observable": "spot volume",
             "comparator": "above", "threshold": 20.0, "unit": "%"}

OUTPUT SCHEMA:
ANGLE BRACKETS ARE SLOTS TO FILL, NEVER TEXT TO COPY. A response that repeats
any <...> phrasing, or describes what a field should contain instead of
containing it, is rejected and re-requested.

{
  "headline": "<the judgment itself, max 150 chars>",
  "significance": "<2-3 sentences: what is at stake strategically, financially, operationally>",
  "hypotheses": [
    {
      "label": "<name this hypothesis>",
      "probability": 45,
      "mechanism": "<how the signals above cause one another. Name at least one signal from the SIGNAL section by its actual value.>",
      "beneficiaries": ["<who gains if this is true>"],
      "watch_signals": [{"entity": "<the ticker/wallet/vessel this is about>", "observable": "<what would be seen>", "comparator": "above|below|occurs|absent", "threshold": 5.0, "unit": "%"}],
      "deny_signals": [{"entity": "<name it>", "observable": "<what would refute this>", "comparator": "occurs", "threshold": null, "unit": null}],
      "time_horizon": "immediate | 24h | 72h | 1week | 1month"
    },
    {
      "label": "<a genuinely different explanation, not a restatement>",
      "probability": 35,
      "mechanism": "<why the same signals could mean this instead>",
      "beneficiaries": ["<who gains under this reading>"],
      "watch_signals": [{"entity": "<name it>", "observable": "<must differ from the first hypothesis>", "comparator": "occurs", "threshold": null, "unit": null}],
      "deny_signals": [{"entity": "<name it>", "observable": "<must differ from the first hypothesis>", "comparator": "occurs", "threshold": null, "unit": null}],
      "time_horizon": "24h"
    },
    {
      "label": "<the low-probability, high-impact case>",
      "probability": 20,
      "mechanism": "<what would have to be true for this>",
      "beneficiaries": ["<who gains under this reading>"],
      "watch_signals": [{"entity": "<name it>", "observable": "<must differ from the others>", "comparator": "occurs", "threshold": null, "unit": null}],
      "deny_signals": [{"entity": "<name it>", "observable": "<must differ from the others>", "comparator": "occurs", "threshold": null, "unit": null}],
      "time_horizon": "72h"
    }
  ],
  "recommended_monitoring": [
    "Specific track, ticker, wallet, or sensor feed to monitor"
  ],
  "confidence_overall": 62,
  "confidence_rationale": "Key evidence vs intelligence gaps driving confidence level"
}

CRITICAL: The 3 hypothesis probabilities MUST sum to 100."""


# ── OUTPUT SCHEMA ─────────────────────────────────────────────────────────────

class GeneratedSignal(ResolutionSignal):
    """What the model is required to emit, as opposed to what the table holds.

    Two jobs, deliberately separated. ResolutionSignal in shared/models must
    stay permissive: hundreds of stored scenarios have bare-sentence signals and
    a required `entity` would stop them loading at all. This subclass is the one
    that becomes the JSON schema handed to Ollama as `format`, so here the
    entity is mandatory and the comparator is an enum -- constraints that reach
    the decoder as a grammar rather than the prompt as a request.

    Strict on the way in from the model, permissive on the way out of the
    database.
    """
    entity: str = Field(min_length=1, description="Ticker, wallet, vessel or country. Never a category.")
    observable: str = Field(min_length=1, description="What would be seen, in a few words.")
    comparator: Literal["above", "below", "occurs", "absent"] = "occurs"


class HypothesisOutput(BaseModel):
    label:          str
    probability:    int
    mechanism:      str
    beneficiaries:  List[str]           = Field(default_factory=list)
    # Two, not an open list, and measured rather than assumed.
    #
    # A structured signal costs several times what a sentence did, and this
    # schema was already at the token ceiling -- "Ollama truncated at the
    # token limit (900 tokens produced)", which arrives as a missing required
    # field rather than as anything mentioning length.
    #
    # The bound turned out to be the single largest throughput change in this
    # deployment. Held against max_length=8 with everything else identical:
    #
    #     max_length=8   mean 132.4s   (n=20)
    #     max_length=2   mean  90.2s   (n=36)
    #
    # a 32% cut, on a host where inference is the bottleneck for every
    # product surface. Generation is linear in tokens; asking for less is the
    # lever that works here, and giving the model server more cores was
    # measured and made the whole system slower.
    #
    # Raising this needs the same A/B, not an argument about richer output.
    watch_signals:  List[GeneratedSignal] = Field(default_factory=list, max_length=2)
    deny_signals:   List[GeneratedSignal] = Field(default_factory=list, max_length=2)
    time_horizon:   str                 = "unknown"


class ScenarioOutput(BaseModel):
    """
    What we ask Llama3 to produce.
    Separate from the DB Scenario model — maps to it after validation.
    """
    headline:               str
    significance:           str
    hypotheses:             List[HypothesisOutput]
    recommended_monitoring: List[str]
    confidence_overall:     int
    confidence_rationale:   str


# ── GENERATOR ─────────────────────────────────────────────────────────────────

# A draft is challenged when challenging it could change the answer. These are
# the conditions under which a red-team pass historically earns its cost.
# A complete ScenarioOutput measured past 1,500 tokens: three nested
# hypotheses of seven fields each, plus five top-level fields. Asking for
# 1024 returned 4,056 characters with the JSON still open, and an
# unterminated object fails every branch of the extractor.
# What kind of thing a cluster is about, stated plainly for the model.
#
# A 1.5B model will not infer that "ADAUSDT" is a perpetual futures pair or that
# "0x28c6c062..." is a wallet, and the prompt previously offered no help: it led
# with the detector's name and a section headed "RECENT GEOPOLITICAL HEADLINES".
# The result was assessments like "Geopolitical Cascade Alert in 'Adausdt'" --
# a crypto ticker read as a place.
_DOMAIN_SUBJECTS = {
    "crypto": "crypto assets and on-chain addresses",
    "tradfi": "publicly traded equities",
    "financial": "publicly traded equities",
    "maritime": "commercial vessels",
    "aviation": "aircraft",
    "cyber": "network infrastructure and disclosed vulnerabilities",
    "prediction": "prediction-market contracts",
    "news": "reported events",
}

_PAYLOAD_DOMAIN = (
    ("crypto_data", "crypto"),
    ("financial_data", "tradfi"),
    ("vessel_data", "maritime"),
    ("flight_data", "aviation"),
    ("cyber_data", "cyber"),
)


def _subject_line(cluster, raw_events) -> str:
    """One line naming what this cluster is about, and of what kind."""
    domains = []
    for event in (raw_events or [])[:12]:
        if not isinstance(event, dict):
            continue
        for column, domain in _PAYLOAD_DOMAIN:
            if event.get(column) and domain not in domains:
                domains.append(domain)

    entities = []
    for source in (getattr(cluster, "entity_names", None), getattr(cluster, "entity_ids", None)):
        for value in (source or []):
            text = str(value).strip()
            if text and text not in entities:
                entities.append(text)
        if entities:
            break

    kinds = [_DOMAIN_SUBJECTS[d] for d in domains if d in _DOMAIN_SUBJECTS]
    kind_text = " and ".join(kinds) if kinds else "signals of unstated type"
    entity_text = ", ".join(entities[:6]) if entities else "unnamed entities"
    # The caution about geography applies to instruments and addresses. A vessel
    # or an aircraft genuinely has a position, and telling the model otherwise
    # would suppress the most useful thing it can say about one.
    caution = ""
    if any(d in ("crypto", "tradfi", "financial", "prediction") for d in domains):
        caution = (
            " A ticker or address identifies an instrument or an account; it is "
            "not a country, city, or region, and no geography follows from it."
        )
    return f"Entities: {entity_text}" + chr(10) + f"These are {kind_text}.{caution}"


SCENARIO_TOKEN_BUDGET = 1800

# Room set aside for the prose statement of the JSON schema.
#
# Zero on the attempt this budget is sized for. The first attempt sends the
# schema as a decoding grammar and no longer restates it in prose, which is
# 2,715 characters of a 7,664-character window returned to the evidence.
#
# Retries do carry it, and will truncate sooner as a result. That is the right
# way round: attempt 0 is the one worth sizing for, and a retry is already a
# degraded path carrying correction text of its own.
_SCHEMA_RESERVE_CHARS = 0

CRITIQUE_CONFIDENCE_CEILING = 75      # at or above this the draft has committed
CLEAR_LEAD_MARGIN = 15                # points between the top two hypotheses
MIN_ARGUED_RATIONALE_CHARS = 80       # shorter than this is a claim, not an argument


def _draft_needs_critique(output) -> bool:
    """Whether a second inference on this draft is worth its cost.

    The policy is deliberately conservative: only a draft that is strong on
    every structural axis skips the red team. Everything else is challenged,
    because a critique that was not needed costs time while a critique that was
    skipped costs correctness.

    "Strong" means it committed to an answer (high confidence), ranked its
    hypotheses clearly rather than splitting them evenly, offered more than one
    to weigh, and argued the confidence rather than asserting it.
    """
    if output is None:
        return False

    hypotheses = getattr(output, "hypotheses", None) or []
    if len(hypotheses) < 2:
        # Nothing to weigh against anything. Either malformed or unconsidered.
        return True

    confidence = getattr(output, "confidence_overall", 0) or 0
    rationale = str(getattr(output, "confidence_rationale", "") or "")
    probabilities = sorted(
        (getattr(h, "probability", 0) or 0 for h in hypotheses), reverse=True
    )
    leading_margin = probabilities[0] - probabilities[1]

    is_strong = (
        confidence >= CRITIQUE_CONFIDENCE_CEILING
        and leading_margin >= CLEAR_LEAD_MARGIN
        and len(rationale) >= MIN_ARGUED_RATIONALE_CHARS
    )
    return not is_strong


# Phrases that mean the model described the field instead of filling it.
#
# A 1.5B model copies a filled-in-looking template rather than reasoning into
# it. Observed in production: a scenario passed every schema check, normalised
# its probabilities to 64/27/9, carried three hypotheses -- and every mechanism
# was the prompt's own placeholder ("Causal mechanism explaining signal
# convergence"), every deny_signal was identical across all three, and the
# beneficiaries were "MARINA ARIEL" and "INSIDE", the second being a fragment of
# the word "Insider". Structurally perfect, analytically empty.
#
# That is worse than no scenario. An empty table says the system found nothing;
# a well-formed scenario that contains no analysis says it found something, and
# an analyst has no way to tell the difference from the outside.
_TEMPLATE_ECHOES = (
    "causal mechanism explaining",
    "alternative causal explanation",
    "tail-risk / high-impact alternative",
    "concrete observable",
    "confirming indicator",
    "refuting indicator",
    "key_actor",
    "distinct hypothesis title",
    "second hypothesis title",
    "third hypothesis title",
    "intelligence judgment (max",
    "sentences on strategic",
)


def _cluster_entities(cluster) -> List[str]:
    """The named subjects this cluster is about, best source first."""
    out = []
    for source in (getattr(cluster, "entity_names", None), getattr(cluster, "entity_ids", None)):
        for value in (source or []):
            text = str(value).strip()
            if text and text not in out:
                out.append(text)
        if out:
            break
    return out


# How the assembled context is divided when it will not all fit.
#
# The builder's own docstring budgets ~2,500 tokens and it was assembling five
# times that: a live prompt reached 49,115 characters against a send budget of
# 7,664. Its caps count items -- five events, ten relationships, three patterns
# -- and never measure them, so five events carrying full JSON payloads blow the
# window on their own and the consensus block had no cap at all.
#
# Truncation then chose what survived by position rather than by worth. Sizing
# the sections here means the choice is made by something that knows what each
# one is for.
#
# Shares of the dynamic half of the budget. Events come first because they are
# the evidence the judgment rests on; precedent and graph context inform it;
# headlines and agent commentary are corroboration and go first when space is
# short.
_SECTION_SHARES = {
    "events": 0.40,
    "graph": 0.15,
    "patterns": 0.15,
    "headlines": 0.10,
    "agent": 0.10,
    "consensus": 0.10,
}

# The static half -- rules, the JSON schema, the task -- is roughly this much of
# the window and is not negotiable, so the dynamic sections share the rest.
_DYNAMIC_BUDGET_SHARE = 0.55


def _clip(text: str, budget: int, label: str) -> str:
    """One section, cut to its share, saying so where it was cut.

    The marker matters: a model handed a JSON object that stops mid-key cannot
    tell truncation from malformed input, and will reason about the fragment as
    though it were the whole.
    """
    text = str(text or "")
    if budget <= 0 or len(text) <= budget:
        return text
    marker = chr(10) + "...[" + label + " truncated to fit the context window]"
    room = max(0, budget - len(marker))
    return text[:room] + marker


def _scenario_body_text(output) -> str:
    """Everything the model actually wrote, for checking what it reasoned about."""
    parts = [str(getattr(output, "significance", "") or "")]
    for hypothesis in getattr(output, "hypotheses", None) or []:
        for field in ("label", "mechanism", "beneficiaries"):
            value = getattr(hypothesis, field, None)
            if isinstance(value, (list, tuple)):
                parts.extend(str(v) for v in value)
            elif value:
                parts.append(str(value))
    return " ".join(parts)


# Shortest entity name that can be matched in prose at all.
#
# Substring matching defeated the relevance check on short names: a cluster
# whose entity was "AS" produced "AS: Potential Maritime Security Threat",
# because "as" occurs in "increased", "unauthorized" and simply "as". Two
# characters cannot be evidence that a scenario is about something.
_MIN_MATCHABLE_ENTITY = 3


def _names(haystack: str, entity: str) -> bool:
    """Whether `haystack` actually refers to `entity`.

    Word-boundary matching, because tickers and identifiers are words. "AS" in
    "increased" is a coincidence of spelling, and a coincidence is what put an
    autonomous-system number in front of a maritime headline.
    """
    token = str(entity or "").strip().lower()
    if len(token) < _MIN_MATCHABLE_ENTITY:
        return False
    return re.search(rf"(?<![0-9a-z]){re.escape(token)}(?![0-9a-z])", haystack) is not None


def _ground_headline(headline: str, entities: List[str], body: str = "") -> str:
    """Ensures the headline names the thing the scenario is actually about.

    Measured over 24 hours, 10 of 83 scenario headlines carried a concrete
    identifier. The rest read "Suspicious Crypto Transfer Activity" -- true of
    thousands of events and therefore about none of them.

    The first version of this took entities[0] and put it in front. On live
    multi-domain clusters that produced headlines that were worse than vague,
    they were wrong:

        VRH6823: Cryptocurrency Market Shift...     (an aircraft callsign)
        QQQ: Potential Cybersecurity Threat...      (an ETF)

    A cluster spanning crypto, aviation and cyber holds entities from all three,
    and the first one is not the subject -- it is just first. An ungrounded
    headline is imprecise; a wrongly grounded one asserts something false, and
    that is the worse failure.

    So the entity has to earn its place: it is attached only if the scenario's
    own body reasoned about it. That makes this a repair of an omission -- the
    model discussed the entity and left it out of the headline -- rather than a
    guess at what the headline meant. Where nothing qualifies, the headline
    stands as written.
    """
    text = str(headline or "").strip()
    if not text or not entities:
        return text

    lowered = text.lower()
    if any(_names(lowered, e) for e in entities if e):
        return text

    # Only an entity the scenario actually discusses can be its subject.
    body_lower = str(body or "").lower()
    subject = next(
        (e for e in entities if _names(body_lower, e)),
        None,
    )
    if not subject:
        return text

    # Addresses and long identifiers are unreadable in full and the headline has
    # a length budget; the prefix is enough to tell two clusters apart.
    if len(subject) > 18:
        subject = f"{subject[:10]}…"
    return f"{subject}: {text}"[:300]


def _echoes_the_template(output) -> bool:
    """True when the draft describes its fields instead of filling them."""
    if output is None:
        return False

    def _looks_copied(value) -> bool:
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered.startswith("<") and lowered.endswith(">"):
                return True
            return any(marker in lowered for marker in _TEMPLATE_ECHOES)
        if isinstance(value, (list, tuple)):
            return any(_looks_copied(v) for v in value)
        if isinstance(value, BaseModel):
            # Signals stopped being strings when they became ResolutionSignal,
            # and this guard only recognised str and list -- so a model that
            # copied "<the ticker/wallet/vessel this is about>" straight into
            # `entity` satisfied min_length=1 and sailed past the check written
            # to reject exactly that.
            return any(_looks_copied(v) for v in value.model_dump().values())
        if isinstance(value, dict):
            return any(_looks_copied(v) for v in value.values())
        if value is not None and not isinstance(value, (int, float, bool)):
            # The failure mode this guard has already suffered once: signals
            # became ResolutionSignal objects, the walk recognised only str and
            # list, and the check kept passing while inspecting nothing. A
            # silent fall-through is what made that invisible for 35 minutes and
            # 22 wasted inferences, so an unrecognised type now says so.
            logger.warning(
                "Template-echo guard cannot inspect a %s; the field is passing "
                "unchecked. Extend _looks_copied for this type.",
                type(value).__name__,
            )
        return False

    if _looks_copied(getattr(output, "headline", "")) or _looks_copied(
        getattr(output, "significance", "")
    ):
        return True

    for hypothesis in getattr(output, "hypotheses", None) or []:
        for field in ("label", "mechanism", "beneficiaries", "watch_signals", "deny_signals"):
            if _looks_copied(getattr(hypothesis, field, None)):
                return True
    return False


def _signal_signature(signal) -> str:
    """What a signal is *about*, for telling hypotheses apart.

    str() on a ResolutionSignal renders every field, so two hypotheses whose
    signals name the same entity and the same observable but differ in a
    threshold -- 5.0 against 6.0 -- produced different signatures and passed a
    check whose whole purpose is that observation can separate them. Only the
    entity and the observable decide what an observer would go and look at.
    """
    entity = getattr(signal, "entity", None)
    if entity is None and isinstance(signal, dict):
        entity = signal.get("entity")
        observable = signal.get("observable", "")
    elif entity is not None:
        observable = getattr(signal, "observable", "") or ""
    else:
        # str() renders every field, so a threshold change alone produced a
        # different signature and two indistinguishable hypotheses passed a
        # check whose entire purpose is that observation separates them.
        # Reaching this branch means the signal is neither an object with
        # `entity` nor a dict, which is the shape change that disarms it.
        logger.warning(
            "Signal signature falling back to str() for a %s; hypotheses "
            "carrying this type cannot be told apart reliably.",
            type(signal).__name__,
        )
        return str(signal).strip().lower()
    return f"{str(entity).strip().lower()}|{str(observable).strip().lower()}"


def _discriminates_between_hypotheses(output) -> bool:
    """Whether the hypotheses can actually be told apart by observation.

    Identical deny_signals across every hypothesis is the failure this catches:
    a refuting observable that refutes all three refutes none of them, and the
    whole point of competing hypotheses is that evidence can separate them.
    """
    hypotheses = getattr(output, "hypotheses", None) or []
    if len(hypotheses) < 2:
        return True
    signatures = set()
    for hypothesis in hypotheses:
        deny = tuple(sorted(_signal_signature(s) for s in (hypothesis.deny_signals or [])))
        watch = tuple(sorted(_signal_signature(s) for s in (hypothesis.watch_signals or [])))
        signatures.add((deny, watch))
    return len(signatures) > 1


# Ceilings a draft's own structure places on how confident it may claim to be.
#
# confidence_overall was taken from the model and clamped to [0,100], which
# trusts a self-report that is not grounded in anything. Measured across 78
# scenarios: eight distinct values, with 85 on nearly half. A confidence that
# takes one value regardless of the evidence is not a confidence.
#
# These do not invent a number and never inflate one -- they cap the model's
# claim at what the draft can support. A scenario whose hypotheses cannot be
# told apart is not 85% confident of anything, whatever it says.
UNSEPARATED_CEILING = 65      # the top two hypotheses are within CLEAR_LEAD_MARGIN
UNARGUED_CEILING = 60         # the rationale asserts rather than argues
SINGLE_HYPOTHESIS_CEILING = 50  # nothing was weighed against anything
# No watch signal on any hypothesis: nothing about this scenario can ever be
# checked. Measured -- one scenario in nine came back with three hypotheses and
# not a single signal between them, which the tracker will sweep forever
# without ever being able to confirm or deny. Lower than the other ceilings
# because the others describe a weak argument and this one describes a claim
# that cannot be wrong.
UNFALSIFIABLE_CEILING = 35


def _supported_confidence(output) -> int:
    """The model's confidence, capped by what its own draft supports.

    Each cap states a reason and is visible in the log line, so a low
    confidence can be explained rather than merely observed. Nothing here
    raises a number the model did not claim.
    """
    claimed = int(max(0, min(100, getattr(output, "confidence_overall", 0) or 0)))
    hypotheses = getattr(output, "hypotheses", None) or []
    rationale = str(getattr(output, "confidence_rationale", "") or "")

    ceiling = 100
    if len(hypotheses) < 2:
        ceiling = min(ceiling, SINGLE_HYPOTHESIS_CEILING)
    else:
        probabilities = sorted(
            (getattr(h, "probability", 0) or 0 for h in hypotheses), reverse=True
        )
        if (probabilities[0] - probabilities[1]) < CLEAR_LEAD_MARGIN:
            ceiling = min(ceiling, UNSEPARATED_CEILING)

    if len(rationale) < MIN_ARGUED_RATIONALE_CHARS:
        ceiling = min(ceiling, UNARGUED_CEILING)

    # A scenario nothing could refute is not a forecast. Capped rather than
    # rejected, following the rule the other ceilings follow: the analysis may
    # still be worth reading, and discarding it would lose the hypotheses too.
    # What it must not do is carry the confidence of something checkable.
    if not any(getattr(h, "watch_signals", None) for h in hypotheses):
        ceiling = min(ceiling, UNFALSIFIABLE_CEILING)

    return min(claimed, ceiling)


def _is_at_least_as_complete(candidate, incumbent) -> bool:
    """Whether a critique's output may replace the draft it reviewed.

    Structural only -- this cannot judge whether the analysis got better, but it
    can refuse the cases where it plainly got worse. A red team that deletes the
    hypotheses has not improved anything.
    """
    if candidate is None:
        return False
    if not str(getattr(candidate, "headline", "") or "").strip():
        return False
    new_hypotheses = getattr(candidate, "hypotheses", None) or []
    old_hypotheses = getattr(incumbent, "hypotheses", None) or []
    if not new_hypotheses:
        return False
    # Losing a hypothesis is legitimate -- pruning a weak one is what a critique
    # is for -- but losing most of them is a failure, not an edit.
    return len(new_hypotheses) >= max(1, len(old_hypotheses) - 1)


class ScenarioGenerator:
    """
    Synthesizes correlation clusters into intelligence scenarios using Llama3.
    Drop-in replacement for the Gemini-based generator — same public interface.
    """

    def __init__(self, db_client, redis_client=None):
        # Store the database connection and redis client
        self.db    = db_client
        self.redis = redis_client
        self.model = os.getenv("AGENT_MODEL", "llama3")
        
        # Concurrency limit: one synthesis at a time per process.
        # The OllamaClient also acquires the global semaphore, but we keep this
        # as a generator-level guard for clarity and to prevent scenario tasks
        # from stacking up if the reasoning loop is processing fast bursts.
        # If 5 anomaly clusters arrive at the same time,
        # only 1 gets to enter the `generate()` method. The other 4 wait their turn.
        # This prevents our local Llama3 instance from running out of memory.
        self._limiter = asyncio.Semaphore(1)
        
        # HTTP session created lazily on first use
        self._session: Optional[aiohttp.ClientSession] = None

    def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            from shared.utils.ollama import OLLAMA_TIMEOUT
            connector = aiohttp.TCPConnector(limit=3, ttl_dns_cache=300)
            self._session = aiohttp.ClientSession(connector=connector, timeout=OLLAMA_TIMEOUT)
        return self._session

    async def generate(
        self,
        cluster:  CorrelationCluster,
        context:  dict,
        patterns: list,
    ) -> Optional[Scenario]:
        """
        Main entry point. Called by reasoning/main.py.
        Returns a Scenario object ready for DB insertion, or None on failure.
        """
        # Enter the semaphore lock. No other task can pass this line until 
        # `_synthesize` is completely finished.
        async with self._limiter:
            return await self._synthesize(cluster, context, patterns)

    async def _synthesize(
        self,
        cluster:  CorrelationCluster,
        context:  dict,
        patterns: list,
    ) -> Optional[Scenario]:
        """
        The core pipeline: 
        1. Fetch details -> 2. Build Prompt -> 3. Call AI -> 4. Fix Math -> 5. Save Data.
        We separate this from `generate()` so the semaphore only wraps the execution logic,
        keeping the code clean.
        """

        # ── 1. HYDRATE RAW EVENTS FROM DB ─────────────────────────────────────
        # The `cluster` only has event IDs. We need the actual text and data 
        # to feed the LLM, so we pull them from the database.
        event_ids = list(filter(None, [
            cluster.trigger_event_id, *cluster.supporting_event_ids
        ]))
        raw_events = await self._fetch_events(event_ids)

        # ── 2. BUILD USER PROMPT ───────────────────────────────────────────────
        user_prompt = self._build_user_prompt(cluster, context, patterns, raw_events)

        # ── 3. CALL LLAMA3 PASS 1 (GENERATION) ─────────────────────────────────
        logger.info(
            "🧠 Synthesizing [%s] %s via Llama3 (Pass 1: Generation)...",
            cluster.alert_tier.name,
            cluster.rule_name,
        )

        client = OllamaClient(self._get_session(), self.model, redis_client=self.redis)
        max_retries = 2
        retry_delay = 5.0

        output: Optional[ScenarioOutput] = None
        for attempt in range(max_retries):
            try:
                output = await client.infer(
                    system_prompt=SCENARIO_SYSTEM_PROMPT,
                    user_prompt=user_prompt,
                    schema=ScenarioOutput,
                    temperature=0.25,
                    max_retries=3,
                    num_predict=SCENARIO_TOKEN_BUDGET,
                )
                break
            except (SchemaViolationError, Exception) as e:
                logger.error("Pass 1 generation error for %s: %s", cluster.correlation_id[:8], e)
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 2
                    continue
                return None

        if not output:
            return None

        # A draft that describes its fields instead of filling them is not a
        # scenario, however well-formed it is. Discarded rather than published:
        # an empty scenarios table says the system found nothing, while a
        # structurally perfect scenario containing no analysis says it found
        # something, and nobody downstream can tell the two apart.
        if _echoes_the_template(output):
            logger.warning(
                "Discarding scenario for %s: the draft echoed the prompt template "
                "instead of analysing the cluster.",
                cluster.correlation_id[:8],
            )
            return None

        if not _discriminates_between_hypotheses(output):
            logger.warning(
                "Discarding scenario for %s: every hypothesis carries the same "
                "watch and deny signals, so no observation could tell them apart.",
                cluster.correlation_id[:8],
            )
            return None

        # ── 3.5 PASS 2 (DEVIL'S ADVOCATE CRITIQUE & POLISH) ───────────────────
        #
        # The critique is a second full inference -- on this host roughly eight
        # minutes -- so running it unconditionally halves how many scenarios can
        # be produced at all. It earns that cost on a draft that is uncertain or
        # internally inconsistent; on one that is already confident and coherent
        # it mostly rephrases. Spending the swarm's scarcest resource on the
        # drafts that need challenging is the point of having a red team.
        if not _draft_needs_critique(output):
            logger.info(
                "Pass 2 skipped for %s: draft is confident and internally consistent",
                cluster.correlation_id[:8],
            )
            output = self._normalize_probabilities(output)
            return self._to_scenario(cluster, output)

        try:
            logger.info("😈 Running Pass 2 Critique (Devil's Advocate) for %s...", cluster.correlation_id[:8])
            critique_prompt = f"""
            ORIGINAL SCENARIO DRAFT:
            {output.model_dump_json() if hasattr(output, 'model_dump_json') else json.dumps(output.dict())}

            ORIGINAL CORRELATION CONTEXT:
            {user_prompt[:1500]}

            Review this draft ruthlessly. Correct any logical leaps, adjust hypothesis probabilities to sum to 100, and ensure all watch/deny signals are concrete and observable.
            """

            critique_system = """You are SENTINEL Red Team / Devil's Advocate.
Review the intelligence scenario draft, challenge weak assumptions, refine confidence ratings, and return a polished final ScenarioOutput JSON. Respond with raw JSON matching the schema exactly."""

            polished_output: ScenarioOutput = await client.infer(
                system_prompt=critique_system,
                user_prompt=critique_prompt,
                schema=ScenarioOutput,
                temperature=0.15,
                max_retries=2,
                num_predict=SCENARIO_TOKEN_BUDGET,
            )
            # The critique replaces the draft only if it is at least as usable.
            # It was accepted unconditionally, so a pass that dropped the
            # hypotheses or returned an empty headline silently destroyed a
            # perfectly good scenario -- and the failure looked like a success.
            if _is_at_least_as_complete(polished_output, output) and not _echoes_the_template(
                polished_output
            ):
                output = polished_output
            else:
                logger.warning(
                    "Pass 2 critique for %s returned a weaker draft; keeping Pass 1",
                    cluster.correlation_id[:8],
                )
        except Exception as e:
            logger.warning(f"Pass 2 critique skipped (fallback to Pass 1 draft): {e}")

        # ── 4. VALIDATE HYPOTHESIS PROBABILITIES ──────────────────────────────
        output = self._normalize_probabilities(output)

        # ── 5. MAP TO DB SCENARIO MODEL ────────────────────────────────────────
        return self._to_scenario(cluster, output)

    def _to_scenario(self, cluster, output) -> Scenario:
        """Maps a validated model output onto the database Scenario.

        Shared by both exits from synthesis -- the critiqued path and the
        skip-critique path -- so the two cannot drift apart.
        """
        scenario = Scenario(
            # A real UUID, matching both the model's own default and the
            # scenarios.scenario_id column type. The short "scn_xxxxxxxx" form
            # was rejected by asyncpg on every insert -- "invalid UUID
            # 'scn_a2754df3': length must be between 32..36 characters" -- and
            # the exception was caught, logged and swallowed, so the pipeline
            # reported success while the table stayed empty.
            scenario_id=str(uuid.uuid4()),
            correlation_id=cluster.correlation_id,
            status=ScenarioStatus.HYPOTHESIS,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
            headline=_ground_headline(
                output.headline,
                _cluster_entities(cluster),
                _scenario_body_text(output),
            ),
            significance=output.significance,
            hypotheses=[h.dict() for h in output.hypotheses],
            recommended_monitoring=output.recommended_monitoring,
            confidence_overall=_supported_confidence(output),
            confidence_rationale=output.confidence_rationale,
        )

        logger.info(
            "✅ Scenario synthesized: %s (confidence=%d%%)",
            scenario.headline[:80], scenario.confidence_overall,
        )
        return scenario

    def _build_user_prompt(
        self,
        cluster:    CorrelationCluster,
        context:    dict,
        patterns:   list,
        raw_events: list,
    ) -> str:
        """
        Assembles the intelligence package for Llama3.

        Context window budget (~4096 tokens for Llama3 8B):
          - Correlation header:    ~200 tokens
          - Raw events:            ~800 tokens (capped at 5 events)
          - Graph context:         ~600 tokens (capped at 10 relationships)
          - Historical patterns:   ~400 tokens (capped at 3 patterns)
          - Recent headlines:      ~300 tokens (capped at 5)
          - Instruction:           ~200 tokens
          Total:                   ~2500 tokens — leaves room for hypothesis generation
        """
        # Hierarchical Context Compression: Use pre-summarized structured event table
        events_section = context.get("compressed_events_table")
        if not events_section:
            events_section = json.dumps(raw_events[:5], separators=(',', ':'), default=str)

        # Compact graph representation
        graph_items = context.get("entity_graph", [])[:10]
        if graph_items:
            graph_lines = []
            for g in graph_items:
                if isinstance(g, dict):
                    entity_id = g.get("entity_id", "Entity")
                    rels = g.get("relationships", [])
                    flags = g.get("flags", [])
                    if rels:
                        for r in rels[:3]:
                            rel_type = r.get("rel", "CONNECTED_TO")
                            target = r.get("connected", "Entity")
                            graph_lines.append(f"• {entity_id} --[{rel_type}]--> {target}")
                    if flags:
                        graph_lines.append(f"• {entity_id} FLAGGED_AS: {', '.join(flags)}")
                else:
                    graph_lines.append(f"• {str(g)[:100]}")
            graph_section = "\n".join(graph_lines) if graph_lines else "None"
        else:
            graph_section = "None"

        patterns_section = json.dumps(patterns[:3], separators=(',', ':'), default=str)

        headlines = context.get("recent_headlines", [])[:5]
        headlines_section = "\n".join(f"• {h}" for h in headlines) if headlines else "None available"

        agent_intel = context.get("agent_intel_briefs", [])
        agent_section = ""
        if agent_intel:
            agent_section = f"""
=== AGENT INTELLIGENCE BRIEFS ===
Pre-analyzed intelligence from the SENTINEL Intel Agent:
{json.dumps(agent_intel[:2], separators=(',', ':'), default=str)}
"""

        bulletins = context.get("active_bulletins", [])
        consensus = context.get("consensus_analysis", {})
        consensus_section = ""
        if bulletins or consensus:
            consensus_section = f"""
=== AGENT SWARM BULLETINS & CONSENSUS ANALYSIS ===
Pre-computed swarm consensus and agent bulletins:
{json.dumps({'consensus': consensus, 'bulletins': bulletins[:5]}, separators=(',', ':'), default=str)}
"""

        # Size the assembled context to what will actually be sent.
        #
        # Without this the builder overshoots by five times and the send path
        # decides what survives by position. Each section is cut to its share
        # here instead, so the choice is made by something that knows what the
        # sections are for.
        try:
            budget = int(deliverable_prompt_chars(self.model, SCENARIO_TOKEN_BUDGET))
        except Exception:
            budget = 7664

        # The system prompt is part of the same window and this builder does not
        # emit it, so sizing without subtracting it overshoots by its whole
        # length. Measured live: a user prompt sized to 7,664 arrived as 13,555
        # combined, because SCENARIO_SYSTEM_PROMPT is 4,049 characters and the
        # schema another 2,715.
        #
        # What that arithmetic exposes is worth stating plainly rather than
        # papering over: fixed instructions occupy the large majority of this
        # host's window before a single event is added. Sizing correctly stops
        # the rules being cut; it does not create room that is not there.
        budget = max(1024, budget - len(SCENARIO_SYSTEM_PROMPT) - _SCHEMA_RESERVE_CHARS)
        dynamic = max(1024, int(budget * _DYNAMIC_BUDGET_SHARE))

        def _apply(allowance: int) -> dict:
            return {
                "events": _clip(events_section, int(allowance * _SECTION_SHARES["events"]), "signal data"),
                "graph": _clip(graph_section, int(allowance * _SECTION_SHARES["graph"]), "graph context"),
                "patterns": _clip(patterns_section, int(allowance * _SECTION_SHARES["patterns"]), "precedents"),
                "headlines": _clip(headlines_section, int(allowance * _SECTION_SHARES["headlines"]), "news"),
                "agent": _clip(agent_section, int(allowance * _SECTION_SHARES["agent"]), "agent briefs"),
                "consensus": _clip(consensus_section, int(allowance * _SECTION_SHARES["consensus"]), "consensus"),
            }

        sized = _apply(dynamic)
        events_section = sized["events"]
        graph_section = sized["graph"]
        patterns_section = sized["patterns"]
        headlines_section = sized["headlines"]
        agent_section = sized["agent"]
        consensus_section = sized["consensus"]

        def _render(sections: dict) -> str:
            events_section = sections["events"]
            graph_section = sections["graph"]
            patterns_section = sections["patterns"]
            headlines_section = sections["headlines"]
            agent_section = sections["agent"]
            consensus_section = sections["consensus"]
            return f"""=== SUBJECT ===
    {_subject_line(cluster, raw_events)}
    
    === ANOMALY CLUSTER ===
    Detector That Fired: {cluster.rule_name}
      (This is the name of a pattern detector, not a conclusion. The detector
       matches on signal shape, so its name may not describe this subject at all --
       a rule called "Geopolitical Cascade" fires on correlated movement, including
       between crypto pairs. Judge the subject from the SUBJECT and SIGNAL sections,
       never from the detector's name.)
    Alert Tier: {cluster.alert_tier.name}
    Description: {cluster.description}
    Tags: {', '.join(cluster.tags)}
    Detected At: {cluster.detected_at.isoformat()}
    
    === RAW SIGNAL DATA ===
    {events_section}
    
    === ENTITY GRAPH CONTEXT ===
    Known relationships for involved entities:
    {graph_section}
    
    === HISTORICAL PRECEDENTS ===
    Similar confirmed/denied scenarios from the past 90 days:
    {patterns_section}
    
    === RECENT NEWS CONTEXT ===
    (Background only. Do not assume the subject above is connected to these unless
     an entity is named in both.)
    {headlines_section}
    {agent_section}
    {consensus_section}
    === TASK ===
    Synthesize the signals above into a structured intelligence assessment.
    
    Rules for a defensible assessment:
    - Write about the subject named in the SUBJECT section. An identifier such as
      ADAUSDT, BTC-USDT-SWAP or 0x28c6c062 is an instrument or an address. It is
      not a place, a country, or an organisation, and no geography may be inferred
      from it.
    - Every mechanism must quote a number or a name that appears above. Write what
      connects the signals, not what a mechanism is. "A 6-hour AIS gap preceded a
      4x spike in call volume" is a mechanism; "causal mechanism explaining signal
      convergence" is a description of the field and will be rejected.
    - If the evidence does not support three distinct explanations, make the weaker
      ones explicitly low-probability rather than inventing detail.
    - beneficiaries are PARTIES who would gain: a named company, state, exchange,
      counterparty or wallet. Never a word taken from your own hypothesis label, and
      never the subject of the threat itself -- a vessel does not benefit from an
      attack on that vessel. If no party can be identified from the evidence, return
      an empty list.
    - watch_signals and deny_signals must be observations that separate THIS
      hypothesis from the others. If a signal would confirm or refute every
      hypothesis equally, it belongs in none of them: an indicator that cannot
      discriminate is not an indicator.
    - The headline must name the subject and what is unusual about it. Do not open
      with the detector's name.
    - Keep prose tight: significance in 2-3 sentences, each mechanism in one or two.
    
    Produce exactly 3 hypotheses whose probabilities sum to 100.
    Return the JSON assessment now:"""

        prompt = _render(sized)

        # A second pass, because the static half is not a guess worth making.
        #
        # The rules, the JSON schema and the task turned out to be larger than
        # the share reserved for them, so a fixed split still overshot: 12,230
        # characters against 7,664. Measuring the rendered prompt and re-sizing
        # against what the template actually left is the only way to be sure the
        # sections fit, and it costs two string builds rather than an inference.
        if len(prompt) > budget:
            static_chars = len(prompt) - sum(len(v) for v in sized.values())
            remaining = max(512, budget - static_chars)
            prompt = _render(_apply(remaining))
        return prompt

    async def _fetch_events(self, event_ids: List[str]) -> List[Dict]:
        """Fetch raw event details from TimescaleDB for the prompt."""
        if not event_ids:
            return []
        try:
            rows = await self.db.query(
                """
                SELECT type, source, tags, anomaly_score, occurred_at,
                       financial_data, vessel_data, flight_data,
                       crypto_data, cyber_data, headline
                FROM events
                WHERE event_id::text = ANY($1::text[])
                ORDER BY anomaly_score DESC
                """,
                event_ids
            )
            
            # Datetime objects break `json.dumps()` later on.
            # We loop through the results and convert any datetimes to ISO 8601 strings (e.g. "2023-10-24T12:00:00Z").
            cleaned = []
            for row in rows:
                cleaned_row = {}
                for k, v in row.items():
                    if isinstance(v, datetime):
                        cleaned_row[k] = v.isoformat()
                    elif v is not None:
                        cleaned_row[k] = v
                cleaned.append(cleaned_row)
            return cleaned
        except Exception as e:
            logger.error("Failed to hydrate events: %s", e)
            return []

    @staticmethod
    def _normalize_probabilities(output: ScenarioOutput) -> ScenarioOutput:
        """
        Ensure hypothesis probabilities sum to 100.
        Llama3 occasionally produces [45, 35, 25] or [40, 40, 40].
        We rescale proportionally rather than rejecting valid scenarios.
        
        CONCEPT: LLM Math Limitations
        LLMs do not "calculate" numbers; they predict the next likely word.
        Therefore, they are notoriously bad at ensuring numbers sum to exactly 100.
        Instead of failing the pipeline, we fix the AI's math programmatically.
        """
        if not output.hypotheses:
            return output

        total = sum(h.probability for h in output.hypotheses)
        if total == 0 or total == 100:
            return output

        # Proportional rescaling: If the AI output 40, 40, 40 (sum 120),
        # (40/120) * 100 = ~33.
        for h in output.hypotheses:
            h.probability = round((h.probability / total) * 100)

        # Fix rounding error on the first hypothesis to guarantee sum=100
        # Sometimes rounding makes the sum 99 or 101. We dump the remainder onto the first hypothesis.
        diff = 100 - sum(h.probability for h in output.hypotheses)
        output.hypotheses[0].probability += diff

        logger.debug(
            "Normalized hypothesis probabilities from sum=%d to 100", total
        )
        return output

    async def close(self):
        """
        Clean up HTTP session on shutdown.
        Always close aiohttp sessions to prevent "Unclosed client session" memory leak warnings.
        """
        if self._session and not self._session.closed:
            await self._session.close()