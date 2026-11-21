"""Read-only. Programmatic part of the LLM-output rubric (written before it was run; see the report's section B).

Rubric (each item is pass/fail per output; the report states the fixed thresholds):
  R1 numbers      >= 2 numeric facts in the free text (percentages, prices, sigma, counts), not counting probabilities
  R2 falsifiable  a claim a later price/event could contradict: scenarios a watch/deny signal with a numeric threshold and a comparator;
                  analyst views a p_beat at least 0.03 from 0.5; correlation-agent calls an expected_direction that is not "neutral"
  R3 evidence     cited ids/events exist: scenario supporting_event_ids resolve in events or signal_event_archive (>= 80%)
  R4 not templated probabilities are not one of the triples/values that repeat across outputs (a triple used by >= 3 scenarios, a p_beat equal
                  to another output's for the same ticker the same hour)
  R5 consistent   named entities are the ones in the packet (scenario signal entities in the correlation's entity_ids or in the narrative)
  R6 length/cost  characters of output and tokens (usage meter) per output
  R7 consumer     recorded separately (flow map)
Run: docker exec -i -w /app sentinel-api_gateway python - < scripts/research_llm_rubric.py
"""
import asyncio
import json
import re
import sys
from collections import Counter

sys.path.insert(0, "/app")
from shared.db import get_timescale  # noqa: E402

NUM = re.compile(r"(?<![A-Za-z])[-+]?\$?\d[\d,]*\.?\d*\s?(%|x|sigma|bps|M|B|k|K)?")


def nums(text):
    return [m.group(0) for m in NUM.finditer(text or "")]


def pct(a, b):
    return f"{a}/{b} ({(100 * a / b):.0f}%)" if b else "0/0"


async def main():
    db = await get_timescale()
    # ---- scenarios
    rows = await db.query("""SELECT scenario_id, correlation_id, status, narrative_summary, hypotheses, supporting_event_ids, confidence_overall,
                             primary_entity_id FROM scenarios WHERE created_at > NOW() - INTERVAL '14 days' ORDER BY created_at DESC""") or []
    n = len(rows)
    r1 = r2 = r3 = r5 = 0
    triples = Counter()
    lens = []
    allp = []
    for s in rows:
        hyp = s["hypotheses"] if isinstance(s["hypotheses"], list) else json.loads(s["hypotheses"] or "[]")
        text = (s["narrative_summary"] or "") + " ".join(str(h.get("mechanism") or "") for h in hyp)
        lens.append(len(text))
        probs = tuple(int(h.get("probability") or 0) for h in hyp)
        triples[probs] += 1
        allp += list(probs)
        if len(nums(text)) >= 2:
            r1 += 1
        sig_ok = any(sg.get("threshold") is not None and sg.get("comparator") in ("above", "below")
                     for h in hyp for sg in (h.get("watch_signals") or []) + (h.get("deny_signals") or []))
        r2 += sig_ok
        ids = list(s["supporting_event_ids"] or [])
        if ids:
            ex = await db.query("SELECT count(*) AS n FROM events WHERE event_id::text = ANY($1::text[])", ids)
            ex2 = await db.query("SELECT count(*) AS n FROM signal_event_archive WHERE event_id = ANY($1::text[])", ids)
            found = max(int(ex[0]["n"]), int(ex2[0]["n"]))
            r3 += (found / len(ids) >= 0.8)
        c = await db.query_one("SELECT entity_ids FROM correlations WHERE correlation_id = $1", s["correlation_id"])
        ents = {str(e).upper() for e in (c["entity_ids"] if c else []) or []}
        sig_ents = {str(sg.get("entity") or "").upper() for h in hyp for sg in (h.get("watch_signals") or []) + (h.get("deny_signals") or [])}
        sig_ents.discard("")
        r5 += (not sig_ents) or all((e in ents) or (e in text.upper()) for e in sig_ents)
    templ = sum(c for t, c in triples.items() if c >= 3)
    print(f"SCENARIOS 14d n={n}")
    print("  R1 numbers>=2      ", pct(r1, n))
    print("  R2 falsifiable     ", pct(r2, n))
    print("  R3 evidence exists ", pct(r3, n))
    print("  R4 templated triple", pct(templ, n), "| distinct triples", len(triples), "| top", triples.most_common(4))
    print("  R5 entities in packet", pct(r5, n))
    print("  R6 chars median", sorted(lens)[len(lens) // 2] if lens else None, "| status", Counter(s["status"] for s in rows))
    print("  probability values", Counter(allp).most_common(8))
    # ---- analysts / agents from telemetry
    for name in ("analyst_agent", "analyst_shadow", "stock_correlation_agent", "adversarial_wargamer_v2_hosted", "rule_synthesizer"):
        tel = await db.query("""SELECT output_payload, latency_ms, occurred_at FROM agent_telemetry WHERE agent_name = $1 AND status = 'COMPLETE'
                                AND occurred_at > NOW() - INTERVAL '7 days' ORDER BY occurred_at DESC""", name) or []
        outs = []
        for t in tel:
            p = t["output_payload"]
            try:
                p = json.loads(p) if isinstance(p, str) else p
            except ValueError:
                p = None
            outs.append(p)
        N = len(outs)
        keys = Counter(json.dumps(o, sort_keys=True)[:400] for o in outs if o)
        dup = sum(c - 1 for c in keys.values() if c > 1)
        print(f"{name.upper()} 7d n={N}, duplicate payloads {dup}, median latency ms {sorted(int(t['latency_ms'] or 0) for t in tel)[N // 2] if N else None}")
        if name in ("analyst_agent", "analyst_shadow"):
            ps = []
            numeric = 0
            for o in outs:
                views = o.get("views") if isinstance(o, dict) and "views" in o else [o]
                for v in views or []:
                    if isinstance(v, dict) and v.get("p_beat") is not None:
                        ps.append(float(v["p_beat"]))
                        numeric += len(nums(str(v.get("rationale")))) >= 2
            print("  views", len(ps), "| p_beat within 0.47-0.53:", pct(sum(1 for p in ps if abs(p - 0.5) < 0.03), len(ps)), "| rationale with >=2 numbers:", pct(numeric, len(ps)),
                  "| distinct p_beat", len(set(round(p, 2) for p in ps)))
        if name == "stock_correlation_agent":
            dirs = Counter(str((o or {}).get("expected_direction")).lower() for o in outs)
            conv = Counter(round(float((o or {}).get("conviction") or 0), 1) for o in outs)
            ev = sum(1 for o in outs if o and o.get("evidence_for"))
            print("  expected_direction", dict(dirs), "| conviction", dict(conv), "| evidence_for non-empty", pct(ev, N),
                  "| sympathy movers per output", round(sum(len((o or {}).get("sympathy_movers") or []) for o in outs) / max(N, 1), 1))
        if name == "adversarial_wargamer_v2_hosted":
            dp = Counter()
            moves = 0
            for o in outs:
                for m in (o or {}).get("moves", []):
                    dp[m.get("disruption_potential_percent")] += 1
                    moves += 1
            print("  moves", moves, "| disruption_potential_percent values", dict(dp))
        if name == "rule_synthesizer":
            rules = [r for o in outs for r in ((o or {}).get("rules") or [])]
            loose = sum(1 for r in rules if (r.get("conditions") or {}).get("min_anomaly", 1) <= 0.4)
            print("  rules proposed", len(rules), "| distinct names", len({r.get("rule_name") for r in rules}), "| loose min_anomaly<=0.4:", pct(loose, len(rules)),
                  "| tiers", dict(Counter(r.get("alert_tier") for r in rules)))
    # ---- stored agent predictions (macro, quant, analyst variants)
    for name in ("macro_intelligence_engine", "quant_trading_engine", "stock_correlation_agent.baseline"):
        pr = await db.query("""SELECT confidence, direction, horizon_hours, ticker, predicted_target FROM agent_predictions WHERE agent_name = $1
                               AND occurred_at > NOW() - INTERVAL '14 days'""", name) or []
        print(f"{name} 14d n={len(pr)} | distinct confidence {len({round(float(p['confidence'] or 0), 2) for p in pr})} | directions {dict(Counter(p['direction'] for p in pr))}"
              f" | horizons {dict(Counter(p['horizon_hours'] for p in pr).most_common(4))} | distinct targets {len({p['predicted_target'] for p in pr})}")
    # ---- llm_news
    ln = await db.query("""SELECT score, event_type, model FROM stock_news_llm WHERE created_at > NOW() - INTERVAL '7 days'""") or []
    sc = Counter(round(float(r["score"] or 0), 1) for r in ln)
    print(f"LLM_NEWS 7d n={len(ln)} | score {dict(sorted(sc.items()))} | event types {dict(Counter(r['event_type'] for r in ln).most_common(6))} | models {dict(Counter(r['model'] for r in ln))}")
    # ---- divergence briefs
    dv = await db.query("""SELECT metrics_summary, description FROM correlations WHERE 'divergence' = ANY(tags) AND detected_at > NOW() - INTERVAL '14 days'""") or []
    lab = Counter()
    for r in dv:
        ms = r["metrics_summary"]
        ms = json.loads(ms) if isinstance(ms, str) else (ms or {})
        lab[((ms.get("regime") or {}).get("label"))] += 1
    print(f"DIVERGENCE briefs 14d n={len(dv)} | regime labels {dict(lab)} | description chars median {sorted(len(r['description'] or '') for r in dv)[len(dv) // 2] if dv else None}")


asyncio.run(main())
