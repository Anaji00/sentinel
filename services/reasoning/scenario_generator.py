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
from shared.utils.untrusted_text import quote_untrusted_block
from shared.models.events import ResolutionSignal

from shared.models import CorrelationCluster, Scenario, ScenarioStatus
from shared.utils.ollama import deliverable_prompt_chars, DEFAULT_MODEL, OllamaClient, SchemaViolationError

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

OUTPUT SHAPE:
ANGLE BRACKETS ARE SLOTS TO FILL, NEVER TEXT TO COPY. A response that repeats
any <...> phrasing, or describes what a field should contain instead of
containing it, is rejected and re-requested.

One hypothesis, shown in full. Produce exactly 3 in the "hypotheses" array.

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
    }
  ],
  "recommended_monitoring": ["<specific track, ticker, wallet, or sensor feed>"],
  "confidence_overall": 62,
  "confidence_rationale": "<key evidence against the intelligence gaps>"
}

THE OTHER TWO HYPOTHESES take the same shape and must differ in substance:
- The second is a genuinely different explanation of the same signals, not a
  restatement of the first in other words.
- The third is the low-probability, high-impact case: what would have to be true.
- Their watch and deny observables must differ from the first and from each
  other. An observable that appears under every hypothesis discriminates
  between none of them and is discarded.
- Probabilities descend and the three MUST sum to 100."""


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

# The critique's scaffolding: headers, the instruction, and indentation.
#
# Kept as a template so its fixed cost is measured rather than estimated. A
# hardcoded number goes stale the first time someone rewords the instruction,
# and it goes stale silently -- the prompt simply starts overflowing again,
# which is the failure this budget exists to prevent.
_CRITIQUE_TEMPLATE = """
            ORIGINAL SCENARIO DRAFT:
            {draft}

            ORIGINAL CORRELATION CONTEXT:
            {context}

            Review this draft ruthlessly. Correct any logical leaps, adjust hypothesis probabilities to sum to 100, and ensure all watch/deny signals are concrete and observable.
            """
_CRITIQUE_FIXED_CHARS = len(_CRITIQUE_TEMPLATE.format(draft="", context=""))


def _critique_context_room(draft_json: str, system_prompt: str, model: str) -> int:
    """Characters left for the original context once the draft is seated.

    Pure sizing, kept out of the inference path so it can be tested without a
    model. Raises _CritiqueNotAffordable when the draft alone will not fit,
    because a critique of a truncated draft is worse than no critique: the
    reviewer corrects the half it was shown and drops the rest without saying
    so, and the caller cannot tell that from a real review.
    """
    try:
        budget = int(deliverable_prompt_chars(model, SCENARIO_TOKEN_BUDGET))
    except Exception:
        # The same floor _build_user_prompt falls back to.
        budget = 4064
    room = max(0, budget - len(system_prompt) - _CRITIQUE_FIXED_CHARS)
    if len(draft_json) > room:
        raise _CritiqueNotAffordable(
            f"draft is {len(draft_json)} chars against {room} of room"
        )
    return room - len(draft_json)


class _CritiqueNotAffordable(Exception):
    """The draft alone exceeds the window, so a faithful review is impossible."""


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


def _known_entity_tokens(cluster, raw_events) -> set:
    """Every identifier this cluster actually contains, lowercased.

    Drawn from the cluster's own entity lists and from the events behind it, so
    a signal naming something the cluster never mentioned can be recognised.
    """
    tokens = set()
    for source in (
        getattr(cluster, "entity_ids", None),
        getattr(cluster, "entity_names", None),
    ):
        for value in (source or []):
            token = str(value or "").strip().lower()
            if len(token) >= _MIN_MATCHABLE_ENTITY:
                tokens.add(token)
    for event in (raw_events or []):
        if not isinstance(event, dict):
            continue
        for key in ("entity_id", "entity_name", "primary_entity_id", "primary_entity_name"):
            token = str(event.get(key) or "").strip().lower()
            if len(token) >= _MIN_MATCHABLE_ENTITY:
                tokens.add(token)
    return tokens


def _prune_unresolvable_signals(output, known: set) -> int:
    """Drops watch and deny signals naming entities the cluster never contained.

    Measured across 48 hours of scenarios: 353 distinct signal entities, and
    201 of them -- 57% -- named something this platform has never observed.
    They fall into a few shapes, all of them the model writing rather than
    reading: invented placeholders ("XYZ Corp", "Exchange A", "Vessel X", "JKL
    Wallet"), categories the prompt explicitly forbids ("INSIDER", "CYBER
    THREAT", "Stablecoin Usage"), mangled tickers ("PIP R" for PIPR, "ADBES"),
    and observables written into the entity field ("AIS call volume").

    The tracker resolves signals by indexed entity lookup, so each of these is
    a sweep that can never match. The scenario carrying them cannot be
    confirmed or denied through them, which is the same unfalsifiability the
    confidence ceilings exist to price -- and those ceilings still apply, since
    a hypothesis left with no watch signal is caught by UNFALSIFIABLE_CEILING.

    The prompt already states this rule and is ignored 57% of the time. A rule
    worth stating to the model is worth enforcing on its output.
    """
    if not known:
        return 0

    removed = 0
    for hypothesis in (getattr(output, "hypotheses", None) or []):
        for field in ("watch_signals", "deny_signals"):
            signals = getattr(hypothesis, field, None)
            if not signals:
                continue
            kept = []
            for signal in signals:
                entity = str(getattr(signal, "entity", "") or "").strip().lower()
                if entity and any(_names(candidate, entity) or entity == candidate
                                  for candidate in known):
                    kept.append(signal)
                else:
                    removed += 1
            setattr(hypothesis, field, kept)
    return removed


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

# No deny signal on any hypothesis: the scenario can be confirmed and never
# refuted. Measured across the full corpus once it became readable -- 148 of 676
# scenarios, 21.9%, carry no deny signal anywhere. That is the other half of why
# 672 scenarios produced 213 confirmations and not one denial: the confidence
# arithmetic made the deny branch unreachable, and for a fifth of the corpus
# there was nothing to feed it either. Set below the tracker's CONFIRM_THRESHOLD
# of 65, because a claim that only has evidence pointing one way should not be
# able to assert itself as strongly as one that could have gone the other.
UNREFUTABLE_CEILING = 55

# Every hypothesis watching for the same thing. The tracker applies watch hits
# per hypothesis, so when all three watch the same observable a hit raises all
# three together and separates none of them -- the update is uninformative
# whatever fires. Measured: 117 of 676 scenarios, 17.3%.
#
# Capped rather than discarded. The fully degenerate case, where the deny
# signals match as well, is still rejected outright by
# _discriminates_between_hypotheses; this is the weaker form where the
# hypotheses differ but nothing observable tells them apart.
INDISCRIMINATE_WATCH_CEILING = 50


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

    # The refuting direction, which the check above does not cover: a draft can
    # carry watch signals on every hypothesis and no deny signal anywhere, and
    # such a scenario is confirmable but not refutable.
    if hypotheses and not any(getattr(h, "deny_signals", None) for h in hypotheses):
        ceiling = min(ceiling, UNREFUTABLE_CEILING)

    if _shares_one_watch_set(hypotheses):
        ceiling = min(ceiling, INDISCRIMINATE_WATCH_CEILING)

    return min(claimed, ceiling)


def _shares_one_watch_set(hypotheses) -> bool:
    """Whether every hypothesis watches for exactly the same things.

    Separate from _discriminates_between_hypotheses, which compares the deny and
    watch sets as a single signature and so only rejects when both match. A
    scenario whose hypotheses share their watch signals but differ in their deny
    signals passes that guard, and 117 of 676 did.
    """
    if len(hypotheses) < 2:
        return False
    signatures = {
        tuple(sorted(_signal_signature(s) for s in (getattr(h, "watch_signals", None) or [])))
        for h in hypotheses
    }
    # An empty watch set on every hypothesis is the unfalsifiable case above,
    # which carries a lower ceiling already; do not claim it here as well.
    if signatures == {()}:
        return False
    return len(signatures) == 1


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


def _hypotheses_are_discriminable(hypotheses) -> bool:
    """Whether any observation could tell these hypotheses apart.

    Hypotheses that carry identical watch and deny signals are not competing
    explanations -- they are one explanation written several ways, and no
    evidence can ever move probability between them. The tracker already
    refuses to store such a scenario; this catches it before the second model
    pass, which on this host costs roughly five minutes.
    """
    if not hypotheses or len(hypotheses) < 2:
        return True

    signatures = set()
    for h in hypotheses:
        watch = getattr(h, "watch_signals", None) or []
        deny = getattr(h, "deny_signals", None) or []
        signatures.add((
            tuple(sorted(str(x) for x in watch)),
            tuple(sorted(str(x) for x in deny)),
        ))
    return len(signatures) > 1


def _subject_is_in_evidence(scenario, cluster) -> bool:
    """Whether the scenario's subject appears anywhere in the evidence.

    Live on 4 September a crypto liquidation cluster was published as
    "Crypto Liquidation & Equity Spillover Linked to US President Trump" with a
    leading hypothesis of "Trump's Economic Policy Influence (61%)" at overall
    confidence 85. Nothing in the cluster named him. The arithmetic was sound
    and the subject was invented.

    Deliberately permissive: it asks only that *some* entity the cluster
    actually carries is named in the headline. A scenario is allowed to reason
    beyond its evidence; it is not allowed to be about something the evidence
    never mentioned.
    """
    headline = str(getattr(scenario, "headline", "") or "").lower()
    if not headline:
        return True

    known = []
    for attr in ("entity_names", "entity_ids"):
        known.extend(str(x) for x in (getattr(cluster, attr, None) or []))
    for attr in ("primary_entity_name", "primary_entity_id"):
        v = getattr(cluster, attr, None)
        if v:
            known.append(str(v))
    known = [k.strip().lower() for k in known if k and str(k).strip().lower() not in ("", "unknown")]
    if not known:
        # Nothing to check against; not evidence of invention.
        return True

    if any(k in headline for k in known):
        return True

    # A headline that names nothing is not a headline that names the wrong thing.
    #
    # This originally required a cluster entity to appear in the headline, and
    # discarded one scenario in three on the live stream -- including
    # "Market Impact Convergence Alert", which invents no subject at all. The
    # failure this guard exists for was different in kind: a crypto liquidation
    # published as "Linked to US President Trump", naming a specific person the
    # evidence never mentioned.
    #
    # So the test is narrowed to that case. A headline is refused only when it
    # names something entity-shaped -- a capitalised proper noun of two or more
    # words, or a ticker-like token -- and none of what it names is in the
    # cluster. A generic summary passes, because generic is not false.
    named = _named_subjects(str(getattr(scenario, "headline", "") or ""))
    if not named:
        return True
    return any(any(k in n or n in k for k in known) for n in named)


# Tokens that look like a subject rather than a description.
_PROPER_NOUN = re.compile(r"\b(?:[A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\b")
_TICKERISH_TOKEN = re.compile(r"\b[A-Z]{2,5}(?:[.\-][A-Z]{1,2})?\b")

# Words that are capitalised in a headline without naming anyone.
_NOT_SUBJECTS = frozenset({
    # The vocabulary alerts are written in. A headline assembled entirely from
    # these names no one, which is a different thing from naming the wrong one.
    "MARKET", "IMPACT", "ALERT", "CONVERGENCE", "CRITICAL", "SIGNAL", "RISK",
    "CROSS", "DOMAIN", "SEMANTIC", "CASCADE", "IMMINENT", "WATCH", "FLOW",
    "SURGE", "SPIKE", "ANOMALY", "IMPLICATIONS", "IMPLICATION", "OUTLOOK",
    "ANALYSIS", "REPORT", "UPDATE", "WARNING", "ELEVATED", "INTELLIGENCE",
    "GEOPOLITICAL", "STRUCTURAL", "LIQUIDATION", "SPILLOVER", "SWEEP",
    "EARNINGS", "PRECEDES", "OPTIONS", "EQUITY", "CRYPTO", "VOLUME",
    "DISRUPTION", "EXCITATION", "RESEMBLANCE", "SENTIMENT", "PRESSURE",
    "CONFIRMED", "DETECTED", "OBSERVED", "POTENTIAL", "POSSIBLE",
    "BEARISH", "BULLISH", "NEUTRAL", "DARK", "POOL", "BLOCK", "TRADE",
    "TRANSFER", "WHALE", "FUNDING", "RATE", "OPEN", "INTEREST", "CANDLE",
})


def _named_subjects(headline: str) -> list:
    """Entity-shaped tokens in a headline: proper nouns and ticker-like symbols.

    Deliberately conservative about what counts as naming a subject, because
    every false positive here discards a scenario that cost minutes of model
    time to produce.
    """
    if not headline:
        return []
    out = []
    for m in _PROPER_NOUN.findall(headline):
        # A phrase every word of which is a headline word names nobody.
        # "Market Impact Convergence Alert" is title case and not a subject;
        # "US President Trump" is title case and is one. The distinction is
        # whether any word survives the vocabulary of alert-writing.
        words = [w for w in m.split() if w.upper() not in _NOT_SUBJECTS]
        if words:
            out.append(m.strip().lower())
    for m in _TICKERISH_TOKEN.findall(headline):
        if m.upper() not in _NOT_SUBJECTS and len(m) >= 2:
            out.append(m.strip().lower())
    return out


class ScenarioGenerator:
    """
    Synthesizes correlation clusters into intelligence scenarios using Llama3.
    Drop-in replacement for the Gemini-based generator — same public interface.
    """

    def __init__(self, db_client, redis_client=None):
        # Store the database connection and redis client
        self.db    = db_client
        self.redis = redis_client
        self.model = os.getenv("AGENT_MODEL", DEFAULT_MODEL)
        
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

        # ── 3. PASS 1 (GENERATION) ─────────────────────────────────────────────
        #
        # The line named Llama3 while qwen2.5:1.5b did the work, which is how
        # the model mismatch stayed invisible: the log agreed with the stale
        # default rather than with the model that ran. It reports self.model now.
        logger.info(
            "🧠 Synthesizing [%s] %s via %s (Pass 1: Generation)...",
            cluster.alert_tier.name,
            cluster.rule_name,
            self.model,
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

        # Signals naming entities this cluster never contained are dropped
        # before anything downstream is asked to resolve them.
        pruned = _prune_unresolvable_signals(
            output, _known_entity_tokens(cluster, raw_events)
        )
        if pruned:
            logger.info(
                "Dropped %s unresolvable signal(s) from %s: named entities the "
                "cluster does not contain.",
                pruned, cluster.correlation_id[:8],
            )

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
        # Refuse a draft whose hypotheses no observation could separate, before
        # spending the second pass on it.
        #
        # The tracker already discards these, and on 4 September it did so after
        # roughly ten minutes of model time had been spent producing one. The
        # check is the same; doing it here means the cost is one pass instead of
        # two on a host that manages a few dozen inferences an hour.
        if not _hypotheses_are_discriminable(getattr(output, "hypotheses", None)):
            logger.warning(
                "Discarding draft for %s before Pass 2: every hypothesis carries the "
                "same watch and deny signals, so no observation could tell them apart.",
                cluster.correlation_id[:8],
            )
            return None

        if not _draft_needs_critique(output):
            logger.info(
                "Pass 2 skipped for %s: draft is confident and internally consistent",
                cluster.correlation_id[:8],
            )
            output = self._normalize_probabilities(output)
            scenario = self._to_scenario(cluster, output)
            return scenario if self._grounded(scenario, cluster) else None

        try:
            logger.info("😈 Running Pass 2 Critique (Devil's Advocate) for %s...", cluster.correlation_id[:8])
            critique_system = """You are SENTINEL Red Team / Devil's Advocate.
Review the intelligence scenario draft, challenge weak assumptions, refine confidence ratings, and return a polished final ScenarioOutput JSON. Respond with raw JSON matching the schema exactly."""

            # The critique gets the same window as the draft did, and was the
            # only prompt on this path never sized against it.
            #
            # It inlined the entire draft -- three hypotheses with mechanisms,
            # signals, monitoring and rationale -- plus 1,500 characters of
            # context, unbounded. Those are the 8,608 and 11,173 character
            # prompts in the truncation log, against a 7,664 ceiling.
            #
            # Truncation now cuts the middle to preserve the task, which on this
            # prompt means cutting the draft the critique exists to review. A
            # reviewer handed half a draft will correct the half it can see and
            # silently drop the rest, and that costs a full inference on a host
            # that completes about fifty-eight an hour.
            draft_json = (
                output.model_dump_json() if hasattr(output, "model_dump_json")
                else json.dumps(output.dict())
            )
            # Whatever the draft leaves over goes to the original context, which
            # is corroboration here rather than the subject under review.
            context_room = _critique_context_room(draft_json, critique_system, self.model)
            critique_prompt = _CRITIQUE_TEMPLATE.format(
                draft=draft_json, context=user_prompt[:context_room],
            )

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
        except _CritiqueNotAffordable as e:
            # The draft stands unreviewed, which is the honest outcome rather
            # than a review of a fragment. Logged at info: this is a designed
            # path, not a fault.
            logger.info(
                "Pass 2 critique skipped for %s: %s.", cluster.correlation_id[:8], e,
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
            # The events this scenario was actually built from.
            #
            # The column exists on the model and on the table and was left
            # empty on all 628 scenarios, so a scenario could only be traced to
            # its evidence indirectly, by following its correlation_id to the
            # cluster and reading that cluster's list. Carrying it directly
            # costs nothing and survives the cluster being pruned.
            supporting_event_ids=[
                str(e) for e in ([cluster.trigger_event_id] if cluster.trigger_event_id else [])
                + list(cluster.supporting_event_ids or [])
                if e
            ],
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

        # The rate, stated, not left to be inferred from three examples.
        #
        # A model shown three precedents infers a base rate from those three.
        # The library balances outcomes deliberately, so the sample it sees is
        # closer to 50/50 than the corpus is -- and before denial was reachable
        # the corpus was 216 confirmed against 0 denied, so any inference from
        # the examples was wrong in one direction or the other.
        #
        # Stated as counts as well as a rate: "8 of 11" and "0.73" warrant
        # different confidence and the model should see which it has.
        base_rate = context.get("rule_base_rate") or {}
        base_rate_section = ""
        if base_rate.get("sufficient"):
            base_rate_section = (
                "\n    Base rate for this rule: "
                f"{base_rate.get('confirmed')} of {base_rate.get('resolved')} "
                f"resolved scenarios were confirmed "
                f"({base_rate.get('confirmation_rate'):.0%}). "
                "The precedents above are a balanced sample and do not reflect "
                "this rate; weight your confidence against the rate, not the sample."
            )
        elif base_rate.get("resolved") is not None:
            base_rate_section = (
                "\n    Base rate for this rule: unknown -- "
                f"{base_rate.get('resolved')} resolved scenario(s), too few to state one. "
                "Do not infer a success rate from the precedents above."
            )

        headlines = context.get("recent_headlines", [])[:5]
        # Fenced and marked untrusted, not interpolated.
        #
        # This joined the raw strings straight into the prompt -- text from 51
        # external feeds, with no delimiting and nothing telling the model it
        # was quoted evidence rather than direction. Anyone able to place a
        # line in a syndicated feed was writing into the context of the model
        # that produces scenarios, whose output moves discovery_confidence and
        # reaches an advisory path.
        headlines_section = quote_untrusted_block(
            headlines, label="RECENT HEADLINES", max_items=5
        )

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
    
    === RECENT NEWS CONTEXT ===
    (Background only. Do not assume the subject above is connected to these unless
     an entity is named in both.)
    {headlines_section}
    {agent_section}
    {consensus_section}
    
    === HISTORICAL PRECEDENTS ===
    Similar confirmed/denied scenarios from the past 90 days:
    {patterns_section}
    {base_rate_section}
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
    - EVERY hypothesis must carry at least one deny_signal. A hypothesis with
      nothing that would refute it is not a hypothesis, it is an assertion, and
      it will be capped below the threshold at which anything is ever confirmed.
      State what you would have to see to abandon this explanation.
    - No two hypotheses may share the same set of watch_signals. If you find
      yourself writing the same observable under two hypotheses, at least one of
      them is not distinct enough to be worth listing separately -- change the
      observable, or change the hypothesis.
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

    def _grounded(self, scenario, cluster) -> bool:
        """Whether the scenario is about something the evidence actually names.

        Permissive by design -- it asks only that some entity the cluster
        carries appears in the headline. A scenario may reason beyond its
        evidence; it may not be *about* a subject the evidence never mentioned.
        """
        if scenario is None:
            return False
        if _subject_is_in_evidence(scenario, cluster):
            return True
        logger.warning(
            "Discarding scenario for %s: headline names a subject the cluster does "
            "not contain (%r).",
            getattr(cluster, "correlation_id", "?")[:8],
            str(getattr(scenario, "headline", ""))[:90],
        )
        return False

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