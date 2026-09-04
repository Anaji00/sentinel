import json
import os
import time
from typing import Any, Dict, Optional, List
from pydantic import BaseModel
from services.agents.base import InferenceBatcher, SentinelAgent
from shared.kafka import Topics
from shared.utils.equities import BROAD_MARKET_ETFS, is_valid_primary_equity

class RadarDecision(BaseModel):
    investigate: bool
    rationale: str


class RadarCandidateDecision(BaseModel):
    """One ticker's verdict inside a batched answer."""
    ticker: str
    investigate: bool
    rationale: str


class RadarBatchDecision(BaseModel):
    """Many verdicts from one inference.

    The scarce resource is the call, not the token: a budget slot opens every
    600s and an inference runs three to six minutes, so deciding one ticker per
    call capped this agent near twenty decisions an hour against a stream of
    roughly a hundred and fifty thousand events. Asking about ten candidates at
    once costs one extra prompt line each and returns ten verdicts.
    """
    decisions: List[RadarCandidateDecision]

class RadarCandidateDecision(BaseModel):
    """One ticker's verdict inside a batched answer."""
    ticker: str
    investigate: bool
    rationale: str


class RadarBatchDecision(BaseModel):
    """Many verdicts from one inference.

    The unit of scarcity here is the call, not the token: a slot opens every
    600s and an inference runs three to six minutes, so deciding one ticker per
    call capped the agent at roughly twenty decisions an hour. Asking about ten
    at once costs one extra line of prompt each and returns ten verdicts.
    """
    decisions: List[RadarCandidateDecision]


class WatchlistPruneDecision(BaseModel):
    evict_tickers: List[str]
    rationale: str

# An earnings surprise this large is worth a look regardless of the flow behind
# it. Below it, a beat is noise: consensus is set to be beaten by a little.
MIN_EARNINGS_SURPRISE_PCT = 10.0


def _earnings_surprise_pct(message: Dict[str, Any]) -> float:
    """EPS surprise on an event, or 0.0 when it is not an earnings event."""
    for container in (
        message.get("financial_data"),
        message.get("raw_payload"),
        message.get("trigger"),
        message,
    ):
        if not isinstance(container, dict):
            continue
        value = container.get("eps_surprise_pct")
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return 0.0


# How many tickers one inference decides, and how long a lone candidate waits
# for company. Twenty seconds is negligible against a 600s budget slot and a
# multi-minute inference, so batching costs a candidate almost no latency and
# buys the agent an order of magnitude of coverage.
RADAR_BATCH_SIZE = int(os.getenv("RADAR_BATCH_SIZE", "10"))
RADAR_BATCH_WAIT_SEC = float(os.getenv("RADAR_BATCH_WAIT_SEC", "20"))


class RadarAgent(SentinelAgent):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cooldown_seconds = 86400
        self._decision_batcher = InferenceBatcher(
            name="radar_decisions",
            flush_fn=self._decide_batch,
            max_items=RADAR_BATCH_SIZE,
            max_wait_sec=RADAR_BATCH_WAIT_SEC,
            logger=self.logger,
            max_waiters=self.dispatch_concurrency,
        )

    async def _decide_batch(self, items: List[tuple]) -> Dict[str, "RadarCandidateDecision"]:
        """Judges every queued candidate in a single inference.

        Returns {ticker: decision}. A ticker the model does not answer for is
        simply absent, and the batcher resolves it to None -- no verdict rather
        than an invented one. That distinction matters: an unanswered ticker
        must never be recorded as "decided not to investigate", because nothing
        decided it.
        """
        if not items:
            return {}

        lines = []
        for _, c in items:
            line = (f"- {c['ticker']}: z-score {c['z_score']:.2f}, "
                    f"1-minute flow ${c['notional_usd'] / 1e6:.2f}M, regime: {c['regime']}")
            if c.get("cross_context"):
                line += f" | prior agent context: {c['cross_context']}"
            lines.append(line)
        candidates = "\n".join(lines)
        tickers = [c["ticker"] for _, c in items]

        prompt = (
            f"A background radar has flagged institutional volume anomalies on "
            f"{len(items)} primary US-listed instruments.\n\n"
            f"CANDIDATES\n{candidates}\n\n"
            "For EACH candidate decide whether the flow warrants active high-frequency "
            "tracking. Look for 'smart money' sweeps: size that is large relative to that "
            "instrument's own regime, not merely large in dollars. Judge each on its own "
            "metrics -- these are unrelated instruments and a verdict on one says nothing "
            "about another.\n\n"
            "Return one decision per candidate, for every ticker listed and no others.\n"
            "Return ONLY valid JSON:\n"
            '{"decisions": [{"ticker": "<exactly one of: ' + ", ".join(tickers) + '>", '
            "\"investigate\": true, \"rationale\": \"<one sentence citing that candidate's own numbers>\"}]}"
        )

        # One budget slot, one telemetry record, one model call, for the whole
        # batch. The first candidate's message carries the trace.
        batch = await self._execute_with_telemetry(
            message=items[0][1].get("message", {}),
            system_prompt="You are a quantitative trading systems engineer judging institutional order flow.",
            user_prompt=prompt,
            schema=RadarBatchDecision,
            temperature=0.1,
            # ~48 tokens of verdict per candidate plus JSON scaffolding.
            num_predict=min(1024, 96 + 48 * len(items)),
        )

        wanted = {t.upper() for t in tickers}
        out: Dict[str, RadarCandidateDecision] = {}
        for d in (getattr(batch, "decisions", None) or []):
            name = str(getattr(d, "ticker", "")).strip().upper()
            # Only tickers actually asked about: a model that invents a symbol
            # must not have that verdict acted on.
            if name in wanted and name not in out:
                out[name] = d
        return out
    
    @property
    def output_topic(self) -> str:
        return Topics.RADAR_DECISIONS
    
    def _extract_radar_params(self, message: Dict[str, Any]) -> tuple[Optional[str], float, float]:
        """Extracts ticker, z_score, and notional_usd cleanly from raw, discovery, or enriched payloads."""
        ticker = None
        z_score = 0.0
        notional_usd = 0.0

        def _safe_float(val, default=0.0) -> float:
            if val is None:
                return default
            try:
                return float(val)
            except (ValueError, TypeError):
                return default

        def _notional_from(d: dict) -> float:
            """Dollar flow behind an event, however the producer expressed it.

            Three vocabularies reach this agent for one quantity and none of
            them is present on the events that matter most:

              - collector-radar sent {ticker, volume, close_price, z_score}. It
                computed volume * price, refused to emit below $150k, and then
                dropped the number -- so `notional_usd` was absent and read as
                0.0, and every anomaly it raised died at the $50k floor below.
                Measured: a real CGCP anomaly carried $341,415 of flow and
                arrived here as $0.
              - enriched equity anomalies carry `premium_usd`, an options field,
                fixed at 0.0 for equities, with close_price null.
              - options flow genuinely uses premium_usd.

            So notional is taken from whichever the producer supplied, and
            derived from volume * price when only those exist.
            """
            for key in ("notional_usd", "notional", "premium_usd", "value_usd"):
                value = _safe_float(d.get(key))
                if value > 0:
                    return value
            volume = _safe_float(d.get("volume") or d.get("size") or d.get("shares"))
            price = _safe_float(
                d.get("close_price") or d.get("price") or d.get("last_price")
            )
            return volume * price

        if "raw_payload" in message and isinstance(message["raw_payload"], dict):
            p = message["raw_payload"]
            ticker = p.get("ticker")
            z_score = _safe_float(p.get("z_score"))
            notional_usd = _notional_from(p)
        elif "financial_data" in message and isinstance(message["financial_data"], dict):
            fd = message["financial_data"]
            ticker = fd.get("ticker")
            z_score = _safe_float(message.get("anomaly_score")) * 5.0
            notional_usd = _notional_from(fd)
        elif "trigger" in message and isinstance(message["trigger"], dict):
            trig = message["trigger"]
            ticker = trig.get("ticker")
            z_score = _safe_float(trig.get("anomaly_score"))
            notional_usd = _notional_from(trig)

        if ticker:
            ticker = str(ticker).upper().strip()

        return ticker, z_score, notional_usd

    async def prune_watchlist_if_needed(self):
        """
        Agentic Reasoning Watchlist Pruning:
        Enforces Finnhub WebSocket free-tier limit of 50 tickers.
        When sentinel:watched:equities size >= 45, passes candidate tickers to LLM to reason 
        which lower conviction/stale tickers to remove from surveillance.
        """
        try:
            equities_key = "sentinel:watched:equities"
            total_count = await self.redis.raw.zcard(equities_key)
            if total_count < 45:
                return

            raw_items = await self.redis.raw.zrange(equities_key, 0, -1, withscores=True)
            if not raw_items:
                return

            tickers = [t.decode('utf-8') if isinstance(t, bytes) else str(t) for t, _ in raw_items]
            
            prompt = f"""
            You are a quantitative portfolio manager and surveillance systems engineer.
            The surveillance watchlist currently tracks {total_count} equities, approaching the Finnhub API limit of 50.
            
            Active Tickers in Watchlist:
            {', '.join(tickers)}
            
            Evaluate this watchlist and select 5 to 10 tickers that are lowest-conviction or lowest strategic priority to remove from surveillance.
            Return ONLY valid JSON.
            Schema: {{"evict_tickers": ["TICKER1", "TICKER2"], "rationale": "string"}}
            """

            try:
                decision = await self._execute_with_telemetry(
                    message={"system": "watchlist_prune"},
                    system_prompt="You are a quantitative portfolio manager.",
                    user_prompt=prompt,
                    schema=WatchlistPruneDecision,
                    temperature=0.1,
                    num_predict=256,
                )
                evict_tickers = [t.upper().strip() for t in decision.evict_tickers if t.upper().strip() in tickers]
                if evict_tickers:
                    await self.redis.raw.zrem(equities_key, *evict_tickers)
                    self.logger.info(
                        f"🧠 AGENT REASONING PRUNING: Evicted {len(evict_tickers)} tickers ({', '.join(evict_tickers)}) "
                        f"from sentinel:watched:equities. Rationale: {decision.rationale} (Size: {total_count} -> {total_count - len(evict_tickers)})."
                    )
                    return
            except Exception as e:
                self.logger.warning(f"LLM pruning reasoning unavailable ({e}). Fallback to deterministic oldest score pruning.")

            # Fallback: Evict 5 oldest if LLM reasoning unavailable
            oldest_items = await self.redis.raw.zrange(equities_key, 0, 5, withscores=False)
            fallback_evict = [t.decode('utf-8') if isinstance(t, bytes) else str(t) for t in oldest_items]
            if fallback_evict:
                await self.redis.raw.zrem(equities_key, *fallback_evict)
                self.logger.info(f"⚡ DETERMINISTIC PRUNING: Evicted {fallback_evict} from watchlist.")
        except Exception as e:
            self.logger.warning(f"Error during watchlist pruning: {e}")

    # A reserved inference slot.
    #
    # Radar batches its decisions, so one slot is worth up to RADAR_BATCH_SIZE
    # verdicts rather than one -- which is exactly the argument for giving it a
    # slot it can rely on. Measured while sharing the common budget: 26
    # candidates queued over ten minutes and not one batch reached the model,
    # because knowledge_graph_engine, rule_synthesizer and
    # stock_correlation_agent kept winning the shared key first. Batching
    # multiplied the value of a slot radar was not getting.
    INFERENCE_LANE = "radar"

    # Bars needed before a regime estimate says anything. Below this, Hurst and
    # GARCH are fitting noise.
    MIN_REGIME_BARS = 20

    async def _fetch_close_history(self, ticker: str, limit: int = 120) -> tuple:
        """Hourly closes for a ticker, oldest first. Redis first, then the bars.

        The Redis list is a hot cache appended once per hour-bucket rollover, so
        it holds one entry per hour of continuous uptime for that ticker.
        Measured across live equities it held one or two -- against the twenty
        required before Hurst or GARCH would be computed. So the regime line
        read "Hurst/GARCH Regime: Baseline Initialization" for every ticker on
        every evaluation, and the model was told nothing about the instrument's
        behaviour while tradfi_bars_1h held 68 bars for those same names. The
        history was collected, aggregated and stored correctly, and simply never
        consulted.

        Returned oldest-first, which is also a correction: lrange returns
        newest-first and simple_returns() reads consecutive pairs as
        (previous, current), so every return computed from the old path had its
        sign inverted.
        """
        closes: List[float] = []
        try:
            raw_candles = await self.redis.raw.lrange(f"sentinel:candles:1h:{ticker}", 0, limit - 1)
            for c in raw_candles or []:
                # Per entry, not per list. A single unparseable bar used to
                # abort the loop and discard every valid one after it, so one
                # bad write cost the whole series -- and the series going quiet
                # is indistinguishable from a ticker with no history.
                try:
                    item = json.loads(c if isinstance(c, str) else c.decode("utf-8"))
                    if isinstance(item, dict) and item.get("close") is not None:
                        closes.append(float(item["close"]))
                except (ValueError, TypeError, AttributeError, UnicodeDecodeError):
                    continue
            closes.reverse()          # lrange is newest-first; returns need chronological
        except Exception as e:
            self.logger.debug("Redis candle history unavailable for %s: %s", ticker, e)

        if len(closes) >= self.MIN_REGIME_BARS or self.db is None:
            return closes, "1h"

        # The durable series, coarsest first.
        #
        # An hourly regime is the one worth having, but the hourly aggregate
        # only holds what the collector has been up to accumulate: measured,
        # 8 bars for MSFT and META against the 20 a regime estimate needs, while
        # the 15-minute aggregate held 21 and the 5-minute 40. Falling back to a
        # finer bucket is the difference between a regime and "Baseline
        # Initialization" forever.
        #
        # The timeframe is returned with the series because it changes what the
        # number means: volatility over 5-minute bars is not volatility over
        # hourly ones, and telling a model "GARCH volatility 3%" without saying
        # over what is how a figure gets read as something it is not.
        for table, label in (("tradfi_bars_1h", "1h"),
                             ("tradfi_bars_15m", "15m"),
                             ("tradfi_bars_5m", "5m")):
            try:
                rows = await self.db.query(
                    f"""
                    SELECT close FROM (
                        SELECT close, bucket_time FROM {table}
                        WHERE ticker = $1 AND close IS NOT NULL
                        ORDER BY bucket_time DESC LIMIT $2
                    ) recent ORDER BY bucket_time ASC
                    """,
                    ticker, limit,
                )
                durable = [float(r["close"]) for r in (rows or []) if r.get("close") is not None]
                if len(durable) >= self.MIN_REGIME_BARS:
                    return durable, label
                if len(durable) > len(closes):
                    closes = durable
            except Exception as e:
                self.logger.debug("Candle history unavailable for %s from %s: %s", ticker, table, e)

        return closes, "1h"

    # Radar acts on equities and their derivatives. It subscribes to
    # ENRICHED_EVENTS for those, and that topic is 90% flight_position,
    # crypto_transfer and vessel_position -- none of which reaches past the
    # first filter in handle() below. Declaring the types here drops them
    # before they take a dispatch slot, which is what the 68,937-message
    # backlog was made of.
    INTERESTED_EVENT_TYPES = frozenset({
        "equity_block", "market_anomaly", "options_flow", "volume_anomaly",
        "earnings_report", "earnings_surprise", "insider_trade",
        "filing", "thirteen_f", "headline",
    })

    async def handle(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        ticker, z_score, notional_usd = self._extract_radar_params(message)

        # 1. PRIMARY EQUITY & DERIVATIVE FILTER (No NVDY, options, derivatives, or crypto)
        #
        # Index and sector funds pass too. is_valid_primary_equity answers "is
        # this a company", and it now correctly says no to SPY, QQQ and the rest
        # of BROAD_MARKET_ETFS -- but that is not the question here. The
        # collector subscribes SPY and QQQ as core anchors and enrichment scores
        # their blocks, so gating on the company test alone meant a $200M SPY
        # block was collected, enriched, and then dropped on this line. Leveraged
        # and inverse products are still excluded: they are not in this set.
        if not ticker or not (
            is_valid_primary_equity(ticker) or ticker.upper() in BROAD_MARKET_ETFS
        ):
            return None

        # 2. HEIGHTENED ANOMALY FLOW GATEKEEPER ($50k notional minimum)
        #
        # Flow is not the only way an equity becomes interesting. An earnings
        # surprise has no notional at all -- there is no trade behind it -- so a
        # dollar floor silently excluded the entire category: 501 earnings
        # events over three days, every one with premium_usd null, every one
        # dropped here. Those are judged on the size of the surprise instead,
        # which is the quantity that actually carries the signal.
        surprise_pct = abs(_earnings_surprise_pct(message))
        if surprise_pct < MIN_EARNINGS_SURPRISE_PCT and notional_usd < 50_000:
            return None
        
        # Idempotency: Do not re-evaluate a ticker we already escalated today
        if await self.is_recently_processed(ticker, self.cooldown_seconds):
            self.logger.info(f"Idempotency: Skipped evaluating ticker '{ticker}' (already evaluated recently).")
            return None
        
        # ─── AGENTIC REASONING ───
        # ─── CROSS-DOMAIN QUANT REGIME METRICS ───
        from shared.utils import quant_calc
        import numpy as np

        closes, tf_label = await self._fetch_close_history(ticker)

        if len(closes) >= self.MIN_REGIME_BARS:
            returns = quant_calc.simple_returns(closes)
            hurst_val = quant_calc.hurst_exponent(closes)
            garch_vol = quant_calc.garch_volatility(returns, annualize=True)
            # The timeframe is stated. A GARCH figure means something different
            # over 5-minute bars than over hourly ones, and an unqualified
            # percentage invites the reader to assume the wrong one.
            regime_str = (
                f"Hurst Exponent: {hurst_val:.3f} "
                f"({'Trending' if hurst_val > 0.5 else 'Mean-Reverting'}) | "
                f"GARCH(1,1) Volatility: {garch_vol:.2%} "
                f"[over {len(closes)} {tf_label} bars]"
            )
        else:
            regime_str = (
                f"Hurst/GARCH Regime: insufficient history "
                f"({len(closes)} bars, {self.MIN_REGIME_BARS} needed)"
            )

        self.logger.info(f"🔍 Evaluating anomaly for {ticker} | Z-Score: {z_score:.2f} | Flow: ${notional_usd / 1e6:.2f}M | {regime_str}")
        entity_context = await self.fetch_entity_context(ticker)
        cross_context = await self.get_cross_agent_context(ticker=ticker, limit=2)
        cross_block = f"\nCross-Agent Intelligence:\n{cross_context}\n" if cross_context else ""
        
        try:
            # Queued rather than dispatched. The batcher answers this ticker
            # together with whatever else arrives in the same short window, so
            # one budget slot covers up to RADAR_BATCH_SIZE candidates instead
            # of one. Awaiting a single ticker's verdict reads exactly as the
            # direct call did.
            decision = await self._decision_batcher.submit(
                ticker,
                {
                    "ticker": ticker,
                    "z_score": z_score,
                    "notional_usd": notional_usd,
                    "regime": regime_str,
                    "entity_context": entity_context,
                    "cross_context": cross_context,
                    "message": message,
                },
            )

            # None means no verdict was reached -- shed, timed out or
            # unparseable. Distinct from a verdict of False, and treated as
            # "do not escalate" without recording a decision that was never made.
            if decision is None:
                self.logger.debug("No radar verdict reached for %s", ticker)
                return None

            if decision.investigate:
                self.logger.info(f"🧠 AGENT ESCALATION: {ticker} -> Primary Surveillance. Rationale: {decision.rationale}")

                # ─── AGENTIC REASONING WATCHLIST PRUNING BEFORE INJECTION ───
                await self.prune_watchlist_if_needed()

                async with self.redis.raw.pipeline(transaction=True) as pipe:
                    pipe.zadd("sentinel:watched:equities", mapping={ticker: time.time()})
                    pipe.zremrangebyrank("sentinel:watched:equities", 0, -46)
                    await pipe.execute()
                
                # Publish the thesis, so the escalation reaches the swarm.
                #
                # RadarAgent appeared in neither the publish_bulletin nor the
                # record_prediction call graph -- and it is the one agent that
                # reliably wins an inference slot, since it holds a reserved
                # lane. The agents that do publish are the ones that rarely get
                # a slot at all, so "no bulletin has ever completed end to end"
                # had two causes and only one of them was capacity. This is the
                # other: the working agent was never wired to the output.
                #
                # Deliberately NOT a prediction. RadarDecision is
                # {investigate, rationale} -- it says this instrument deserves
                # attention, not which way it will move. Recording a direction
                # here would invent a claim the agent never made and then score
                # a track record against it, which is the failure this codebase
                # has spent its time removing.
                await self.publish_bulletin(
                    bulletin_type="thesis",
                    summary=f"{ticker} escalated to primary surveillance: {decision.rationale}",
                    ticker=ticker,
                    conviction=min(1.0, max(0.0, z_score / 10.0)),
                    expected_direction="neutral",
                    payload={
                        "z_score": round(z_score, 2),
                        "notional_usd": round(notional_usd, 2),
                        "regime": regime_str,
                        "rationale": decision.rationale,
                    },
                    ttl_seconds=6 * 3600,
                )

                await self.mark_processed(ticker, self.cooldown_seconds)

                return {
                    "event_type": "dynamic_allocation",
                    "ticker": ticker,
                    "agent_rationale": decision.rationale,
                    "z_score_trigger": z_score
                }
            else:
                self.logger.info(f"🧠 AGENT BYPASS: {ticker} not escalated. Rationale: {decision.rationale}")
        except Exception as e:
            self.logger.warning(f"Agent LLM reasoning unavailable for {ticker}: {e}. Engaging Deterministic Quant Rules.")
            if z_score >= 3.5 and notional_usd >= 50_000:
                decision = RadarDecision(
                    investigate=True, 
                    rationale=f"Deterministic Quant Fallback: Volume Z-score {z_score:.2f} >= 3.5 with ${notional_usd/1e6:.2f}M flow."
                )
                self.logger.info(f"⚡ DETERMINISTIC ESCALATION: {ticker} -> Primary Surveillance.")
                
                await self.prune_watchlist_if_needed()

                async with self.redis.raw.pipeline(transaction=True) as pipe:
                    pipe.zadd("sentinel:watched:equities", mapping={ticker: time.time()})
                    pipe.zremrangebyrank("sentinel:watched:equities", 0, -46)
                    await pipe.execute()
                await self.mark_processed(ticker, self.cooldown_seconds)
                return {
                    "event_type": "dynamic_allocation",
                    "ticker": ticker,
                    "agent_rationale": decision.rationale,
                    "z_score_trigger": z_score
                }
        
        return None
