# Signals and agents: what the user can see (frontend extension, 18 Nov 2026)

Extends `reports/Signals, AI output and agent structure research - 13 Nov.md` (called **SR** below) from the interface side. Read-only on code; nothing here changes sizing, caps, the kill switch, execution, the five-cell G4 gate, B1 construction, market-cap floors, any rule family or agent, anything with cyber events, or adds a model call. Items that touch those are tagged **OWNER-ONLY** and are shown, never acted on.

All numbers come from SR, `reports/Grading and score replays - 13 Nov.md` (**GR**) or `reports/System improvement research - 13 Nov.md` (**SI**). I did not re-run any of them.

## 0. Scope, and a duplicate to disclose

- **Duplicate.** Before your message arrived I wrote `reports/signals-and-agents-research.md` (13 Nov), a parallel version of SR. It is superseded: SR's 14 Nov corrections (quant engine not shown worse than chance; agents re-graded by date; macro edge partly beta) are right and mine still uses the withdrawn quant figure. I have put a notice at its top. Please do not build from it.
- **What I read in the frontend (working tree, 18 Nov):** `IntelligenceFeed.tsx` (score badge, tier badge, scenario confidence), `ModelQuota.tsx`, `TrackRecord.tsx`, `ui/ConvictionMeter.tsx`, `lib/heldBack.ts` (quota types), `ScenarioPaths.tsx` (probability), and the gateway routes `GET /predictions/producer-grades` and `GET /health/model-quota`. **I did not read** `ThesisPanel`, `AgentConclusions`, `CallsWorkspace` or the wargamer panel in full, so rows marked "unverified" below need a quick look by the build session. The frontend is in motion (many modified files), so recheck line numbers.
- **Evidence strength.** The research behind the wording choices is thin. Where I cite a convention it is from the earlier reports (`reports/frontend-research.md`, `frontend-overhaul-ia.md`); the new wording itself is untested with real users. Say so in the release note and test it with two or three people.

## 1. What users cannot see today

### 1.1 Findings from the code

| Finding | Where | Why it misleads |
|---|---|---|
| **Nothing in the app reads `clustered`.** A search of `frontend/src` finds no use of `producer-grades` or `clustered`. The route has served it since 13 Nov. | whole frontend | The only honest evidence for a family (SR A1) is invisible. Users see the old stored `t` values only if some page shows them (not found) |
| **Rule alerts carry a pulsing amber dot and "TIER n" whatever their record.** | `IntelligenceFeed.tsx` ~415-425 (`animate-ping`, `TIER {c.alert_tier}`) | Urgency styling on 13 of 14 families that SR finds uninformative or below control. "TIER 1" is the WATCH tier (SR A3: 74% of correlations, never sent as alerts) but reads as "first priority" |
| **The "How unusual" badge colours by `anomaly_score`** (red at 0.9+, amber at 0.7+). | `IntelligenceFeed.tsx` 221-241 | Scores that are flat or saturated drive the colour: `vessel_spoof` is 0.82 on all 2,335 transit events (amber on every one); `finra_short_volume` has 21% of events on 0.95 (red) and its old score has a **negative** rank correlation with the next move, -0.23 (GR "Revised-bar outcome test"); `alpaca_extended_hours` 19% at 0.95 or more and no rank skill (GR). `alpaca_news` is flat 0.50 (grey, so harmless) |
| **Scenario "CONFIDENCE: N%" is printed as a plain number.** | `IntelligenceFeed.tsx` ~807, ~1032 | SR B2: scenario probabilities were templated until 12 Nov and are ungraded; 22% state an "Overall confidence" in the text that differs from the stored field |
| **Bull and bear and scenario paths show probabilities** (`ScenarioPaths.tsx` line 28 clamps `h.probability` to 0 to 100 for display). | `ScenarioPaths.tsx` | Same: ungraded, repeated round triples (60/30/10 x8, 65/25/10 x7, 60/25/15 x5, SR B2) |
| **`ConvictionMeter` is the right pattern for a graded probability** (centred on 50%, words such as "about a coin flip") **but it is fed model numbers for anything.** | `ui/ConvictionMeter.tsx` | The analyst's `p_beat` lies in 0.47 to 0.53 in 91% of cases (SR B2); the meter would say "coin flip", which is honest, but a call with p 0.53 would show a thin bar of "a bit better than even" on a producer graded at chance |
| **The analyst's views list has no reader.** `sentinel:analyst:views` is commented "for the dashboard", but no route or component reads it (SR B2). | gateway, frontend | Not a UI bug; it means the UI must not claim to show "analyst views" |
| **`ModelQuota` shows requests-per-day only, in words.** | `ModelQuota.tsx`, `heldBack.ts` `quotaLine` | It does not show which agent spent the tokens, tokens per minute (the Groq limit that binds: 8,000 TPM, SI R7/R13), or what the output was read by |

### 1.2 Coverage table: does the UI show each voice's evidence status?

"Shown today" is what I could confirm in code. "Unverified" means I did not open the file.

| Voice | Evidence status that exists (SR) | Shown today? |
|---|---|---|
| 13 correlation families | clustered verdict, dates, units, top-3-date share (`producer-grades.clustered`) | **No** |
| Scenarios | status, confidence (ungraded) | Percent shown, no grade state |
| macro_intelligence_engine | +84 bps raw, t 2.03 over 24 dates; beta-adjusted +47 bps, t 1.58 (SR "Further check") | Unverified |
| stock_correlation_agent (4 variants) | 4 to 8 dates each: "not judged"; `.baseline` falls from t 6.3 to 1.87, then to 0.44 beta-adjusted | Unverified |
| analyst (local) and shadow | 5 to 6 dates, "not judged"; Brier 0.251 | Not shown (no reader) |
| quant_trading_engine | not shown worse than chance; "no demonstrated skill, 29 dates" (SR correction 14 Nov) | Unverified |
| wargamer v2 | entity-claim skill -0.152 against placebo; templated disruption percentages (SR B2) | Unverified (one route feeds one panel) |
| rule_synthesizer rules | probation, capped at WATCH | Unverified |
| Factor model rank | live rank-correlation vs promised (`/predictions/model-health`) | Partly (ModelHealth component exists; not checked) |
| Idea forecast | "flat top" flagged `calibration_flat` | Frontend session already added honest wording; not rechecked here |
| Score sources (anomaly) | constant or saturated for 4 sources (GR, SI R6) | **No** |

## 2. Recommendation U1: a quiet "Alert quality" badge on every rule alert

**Question it answers:** "Has this kind of alert ever told us anything?" It is about the *alert type's record*, not about this stock.

**Data:** `GET /predictions/producer-grades` → `clustered`. Each row, per `shared/utils/grades_v2.py`: `family, k (1 or 5), units, dates, mean_ratio_by_date, excess_over_control, t_over_dates, top3_date_share, verdict`. The verdict is one of four server-written phrases. Families with fewer than 8 dates are **absent** from the list, so absence means "too few dates or not graded", not "good". The wrapper shape of the stored key I did not read.

**Gap (ask backend, local_9831f41a):** a correlation row carries `rule_name`, but the grade is keyed by `family` from `producer_grades.rule_family`. The frontend cannot import that Python. Ask for `family` on each `/correlations` row, or a `rules → family` map inside `clustered`.

**Wording (layperson, one line, plus a longer tooltip).** The measure is *size of move*, not direction, so the copy must never suggest "will go up".

| State | Badge text | Tooltip |
|---|---|---|
| `moves more than the same-day control` | "Past record: bigger moves than usual" | "After this kind of alert, stocks moved more than a random stock on the same day (N days of history). This says nothing about whether they went up or down." |
| `no difference from the same-day control` | "Past record: no clear signal" | "After this kind of alert, stocks moved about as much as a random stock that day (N days)." |
| `moves less than the same-day control` | "Past record: smaller moves than usual" | "After this kind of alert, stocks moved less than a random stock that day (N days). Part of this may be that the stock had already moved when the alert fired." |
| `too few dates` or absent | "Not enough history to judge" | "Needs at least 8 different days of alerts; this type has N." |
| no entry at all | "Not graded yet" | "We have not measured this alert type." |

**Rules:**
- Grey and neutral for every state. Never the green or red reserved for price direction. One distinguishing shape only (filled, half, empty circle, dash).
- Show **days**, not alerts: "11 days", never "2,935 alerts". If `top3_date_share` is 0.5 or more, add "mostly on 3 days". For peer decoupling (SR A1: 11 dates, 54% of units on 3 days) that line is the honest caption.
- Never print `t`, `q` or "significant". The longer tooltip may show "excess +0.43 (11 days)" for the owner's view only (section 6).
- Replace the pulsing dot and "TIER n" on rule cards: show "Watch" or "Alert" in words, with no animation, and let the quality badge carry the record. Only tier changes the sort, not the colour.
- **I want to flag a possible disagreement with SR's reading of "below control".** My earlier reading of the code (not run) suggests the control may be unfair to stocks that just moved, because the denominator (60-session typical move) includes the alert-day spike. SR may have tested this; if not, the "smaller moves" text should keep its caution clause until the backend has a matched-control result. Either way the UI must not call such a family "bad" or "worse".

**Acceptance checks (vitest + Testing Library):**
1. A fixture with `verdict: "too few dates"` renders "Not enough history to judge" and **no numeric t, excess or percentage**.
2. A card whose family is absent from `clustered` renders "Not graded yet", never "no clear signal".
3. No element inside the badge has class `text-positive`, `text-negative`, `bg-positive` or `bg-negative` (a test over the rendered DOM).
4. The badge shows `dates`, not `units`; with `top3_date_share >= 0.5` it contains "mostly on 3 days".
5. A rule card renders no element with `animate-ping`, and no text matching `/TIER \d/`.
6. Snapshot of the four states at 320 px width: no text overflow, and the state is readable without colour (assert the icon shape differs per state).

## 3. Recommendation U2: flat or saturated scores must not drive colour or order

**Data needed:** a per-source `score_quality` flag: `ok`, `flat` (fewer than 10 distinct values per 500 events), `saturated` (more than 40% above 0.8), or `not predictive` (outcome rank correlation not above zero). The measurements exist as scripts (GR `measure_score_distributions.py`, `outcome_test_scores.py`; SI R6). No route serves them. **Ask system-research or backend** for `GET /health/score-quality` (read-only, from the same stored measurements).

**Interim (build session):** a small constant list in `lib/` naming the four sources from GR and SI: `vessel_spoof`, `finra_short_volume`, `alpaca_extended_hours`, `alpaca_news`, with a comment citing GR and a test that fails when the backend flag arrives and disagrees. Remove it when the route exists.

**Behaviour for a flagged source:** the "How unusual" badge becomes neutral grey with the text "Score not informative for this source" in the tooltip; no amber or red; the feed's default sort ignores the score for those rows (sort by time within the source); and any "top anomalies" list excludes them. The event still appears.

**Acceptance checks:**
1. An event from `vessel_spoof` with score 0.82 renders no `text-caution` or `text-negative` class and the tooltip says the score is not informative.
2. An event from an unflagged source at 0.82 renders the amber badge as today.
3. A list of ten mixed events with equal timestamps: flagged rows keep their time order and are not ranked above lower-scored unflagged rows.
4. When `score_quality` is unavailable (route error), the badges render in grey with "score quality unknown", not as if ok (fail toward neutral).

## 4. Recommendation U3: how model-written text appears

Applies to alert text, scenarios, bull and bear case, briefs and any agent paragraph. Builds on SR B4 (schema validation, numbers-first packets, thin-evidence marking) and does not add a model call.

### 4.1 One block, five parts, in this order

1. **Claim, one sentence.** Plain words; no number unless it is a stored fact.
2. **Because.** Two to four evidence rows. Each row is a stored fact with a chip showing its id and kind ("Filing, 11 Nov", "Move, 5 sessions"). A chip opens the evidence drawer from the stored row (`/events/detail/{event_id}` exists; a fact id from `case_file` needs a route, see requests). A sentence with no chip is not rendered as a claim; it appears under "Context" in grey, or not at all.
3. **What would prove it wrong.** The scenario's deny signals as a checklist with live state: "Not yet", "Happened (date)", "Expired". These are checkable against the data and are the one thing the UI can grade by itself.
4. **Record.** "This kind of text has been graded on N cases over D days: …" or "Not graded yet". Same states as U1.
5. **Provenance line.** "Written by <model>, <date>; the facts were as of <time>". Matches what the case file stores (SR section C: models and cutoff are recorded).

**Shared event ids.** `event_identity.py` documents ids shared by several rows (40 ids covering 93 rows). A chip for such an id must say "1 of 3 observations" and open a list, not guess.

### 4.2 Abstention states (never hide them)

| State | When | What the card says |
|---|---|---|
| **Thin evidence** | fewer than the minimum cited facts, or the packet is missing a source | "Not enough evidence for a written view." plus the list of what is missing ("No filings, no recent headlines") |
| **No view** | analyst `p_beat` within 0.03 of 0.5 (SR B4.1) | "No view: the model sees no clear edge." No bar |
| **Not graded** | any producer with fewer than 8 dates (SR A2) | "No record yet." |
| **Withdrawn** | a validator rejected the output (range check, confidence mismatch, repeated template) | "Output withdrawn after a check. See why." (owner view only: reason) |

### 4.3 What must never be shown as a forecast

- A model-written probability or percentage on a scenario or hypothesis. Show hypotheses **unranked** (or in a deterministic order), each with its signals. **OWNER-ONLY** to reverse.
- "Overall confidence N%" lines inside narrative text (22% differ from the stored field, SR B2). Strip with a pattern and show nothing in its place.
- Analyst `p_beat`, `ConvictionMeter` or "chance it goes our way" for any producer in the "not judged" state. Use "No view" or the empty state.
- Agent `conviction` to two decimals (macro has 13 distinct values; quant has four, SR B2); show a word only (see 4.4), or nothing.
- The wargamer's "disruption potential %" (85 x31, 70 x26, 40 x17: templated, SR B2). Label the panel "Role-play scenario, not a forecast" (**OWNER-ONLY** to hide or remove the panel; that decision is SR #2).
- The idea forecast's single number above the last supported knot (`calibration_flat`): already treated by the frontend session.

### 4.4 Uncertainty in words

Words come from the **graded record**, never from the model's own number. A ladder, from the producer's status:

| Status | Words |
|---|---|
| Not graded or not judged | "Unproven" |
| Graded, no difference | "No track record of being right" |
| Graded, moves more | "Has preceded bigger moves, direction unknown" |
| Withdrawn | "Withdrawn" |

Add the existing `oddsWords` ladder only where a probability is graded (the factor model's idea list).

### 4.5 Owner's evaluation view of model text (read-only)

A "Text quality" tab on the Trust page (see `frontend-overhaul-ia.md`). Per producer (scenario, analyst, shadow, correlation agent, wargamer, rule_synthesizer, macro, quant, llm_news, divergence brief), per week:
- the SR rubric rates: R1 numbers, R2 falsifiable, R3 evidence exists, R4 not templated, R5 entities in packet, R6 length and tokens, R7 consumer exists;
- validator rejections by reason (SR B4.1);
- abstention share (thin evidence, no view);
- falsifier outcomes: how many deny or watch signals fired, and by when;
- cost: tokens per output and per *useful* output (SR B3: 4.4k per scenario, 34k per confirmed one).

**Data needed (ask system-research):** the rubric job currently runs as a one-off script (`scripts/research_llm_rubric.py`). A daily read-only job that stores its rates under one key, and validator counters under another, would feed this tab. No new model calls: the rubric is programmatic. **A weekly LLM-judge sample would be new model calls; OWNER-ONLY, with trigger, cap, admission check and consumer stated first.**

**Acceptance checks:**
1. A block with a sentence lacking a chip renders that sentence in the "Context" grey group, not in "Because".
2. A scenario with `probability` fields renders **no `%` character** in the hypotheses list; the deny signals render as a checklist.
3. Text containing "Overall confidence 70%" renders without that fragment.
4. Analyst view with `p_beat = 0.51` renders "No view" and no meter element.
5. A fact id shared by three rows renders "1 of 3 observations".
6. The evaluation tab renders "No data yet" (not zeros) when the rubric key is empty.

## 5. Recommendation U4: one situation card instead of many alerts

SR A2 numbers: 3,810 ticker-days carry 11,748 correlations (3.1 each; 20% have two or more rules); 161 agent ticker-days have two or more agents, with four `stock_correlation_agent` variants emitting 825, 816, 194 and 85 rows; across layers the overlap is small (30%), so the layers rarely agree.

**Design: one card per stock per New York session** (the same key `alert_dedup` uses for alerts, and the same idea as `situation.py` for divergences):
- Header: ticker, one plain sentence ("Moving against its peers since Tuesday"), the Alert-quality badge of the *best-graded* source behind it.
- **"Seen by"** line: "3 rules and 2 agents. Counted as 2 independent sources." Expands to a list, each source with its own badge and time. "Independent" follows `evidence_fusion` (families sharing a source count once). Never print the raw count as if it were strength.
- **Agreement and disagreement as information.** If two producers point opposite ways, show a **"Mixed signals"** row with both views side by side and the line "These do not average out; treat as unclear." Do not combine them into one score. If they agree, say "Same direction" and still show only the independent count.
- The four correlation-agent variants appear as one "Correlation agent" entry with a "4 versions" expander, since SR marks merging them as a pending backend decision (class ii). Until then group by agent name in the UI.
- Cross-layer: if a recommendation idea or scenario exists for the same ticker-day, link it as a separate row ("Also on the call list", "A scenario was written").

**Data:** the cleanest source is a backend "situation per stock-session" (SR C3 "evidence packet builder"; not built). **Interim:** group client-side by `(ticker, NY session date)` from `/correlations`, `/agents/conclusions` and recommendations. The `/correlations?limit=200` window means counts can be undercounted on busy days: say "at least" when the list is at its limit.

**Acceptance checks:**
1. Two correlations with the same ticker and session, different rules, render as **one** card with a "Seen by 2" line; different sessions render two cards.
2. Four agent-variant rows for one ticker-day render one "Correlation agent" entry with "4 versions".
3. Opposite directions render the text "Mixed signals" and both views, and the card contains no combined score or colour.
4. Independent count never exceeds the raw count, and equals 1 when all sources share a source group (fixture).
5. When the list length equals the request limit, the card says "at least".

## 6. Recommendation U5: operator view of the agent swarm and the inference budget

**Audience:** the owner (admin role). Read-only; **no buttons that retire, merge or change anything**, because every such action is OWNER-ONLY. The view's job is to make the pending decisions legible.

**Place:** Trust → "Agents and inference" (sits with Operations and Audit, admin only, per `frontend-overhaul-ia.md`).

### 6.1 Top strip: the budget in one line each

From `GET /health/model-quota` (exists): per hosted model, state, requests today of limit. **Add the limit that binds.** The route's `share_of_limit_used` already picks the strictest limit; show *which* one ("tokens a minute: 8,000", SI R7 Groq gpt-oss-120b). Pair with local CPU time (SR B3: 13,001 s in 7 days, of which analyst_agent 11,733 s). Today the component shows requests per day only. The route's other fields were not read in full; check that tokens per minute are in the payload before building.

### 6.2 The table: one row per producer

| Column | Source |
|---|---|
| Producer and what it writes | static copy |
| **Who reads it** (consumer) | SR "Consumers" tables; static until a route exists |
| Calls per day versus cap | needs data (see requests) |
| Tokens (7 days) and share of total | SR B3: llm_news 976k (45%), comment_letter 276k, stock_correlation_agent 256k, scenario_generator 240k, analyst_shadow 136k, rule_synthesizer 124k, 13D 88k, wargamer 59k |
| Local model seconds | SR B3 |
| Graded calls, **dates**, and the by-date result | SR correction addendum table (one observation per date), plus beta-adjusted where available |
| Verdict in the same words as U1 | "Not enough dates" for most |
| Cost per useful output | SR B3 |

**Reading aids:** a bar of share of hosted tokens that has a **named reader** versus **stored feature only** (SR: at least 57% of hosted tokens feed no live model input). This is the single most informative visual for the owner's "which agents earn their cost".

### 6.3 "Decisions waiting" panel (static, evidence summary, no actions)

Each item: the decision, who reads the output today (SR consumer table, "what breaks if it goes"), the evidence in words, and the status "Owner decision".

| Item (**all OWNER-ONLY**) | Evidence to show | What breaks if removed (SR) |
|---|---|---|
| analyst local path | 90% of local model time for about 86 distinct outputs; 91% of `p_beat` in 0.47 to 0.53; 45 calls on 6 dates, t -1.31 (not judged); registered shadow test pending | grading only; the hosted shadow continues |
| wargamer v2 | entity-claim skill -0.152 against placebo (171 claims); templated percentages; 59k tokens | one dashboard panel; no alert or model input |
| quant_trading_engine alert role | **no demonstrated skill; not worse than chance** (SR correction 14 Nov; 24 h calls +8.6 bps, t 0.36, 27 dates); no alert path exists | nothing to demote today; removing the agent would starve 7 consumer groups |

State the correction visibly: SR #8 supersedes the earlier "worse than chance" wording, and the owner's earlier decision list may still say so. Show "Superseded 14 Nov" on any old text.

**Two more things this view should refuse to do:** show a "savings" figure for removing an agent (SR gives no cost in money; the free tiers are quota not price), and recommend a decision (the owner decides).

### 6.4 Acceptance checks
1. A model with `status: "cooling down"` shows the cooldown seconds and which limit bound; a model with no key shows "not in use".
2. The table renders "Needs data" for calls-per-day (not `0`) until the backend field exists.
3. A snapshot of the "Decisions waiting" panel contains no `button` or `a` with a mutation verb (retire, remove, disable); an automated grep of the rendered DOM for those verbs passes.
4. The quant row never contains the words "worse than chance" and contains the lower-bound statement.
5. A non-admin user sees none of this page and a direct URL returns the existing 403 view (reuse `sidebar_roles` test pattern).
6. The token-share bar's segments sum to 100% of the figure shown and each segment has a text label (not colour only).

## 7. Cross-cutting rules the build session can adopt now

1. **Absence is not good news.** Every "no data", "not graded" or "too few days" state is a labelled grey state, never a blank and never a green tick.
2. **Counts are of days, then of independent sources, then of alerts.** In that order of prominence.
3. **Colour reserved.** Green and red only mean a price moved up or down. Quality and record use shape and words.
4. **Fail neutral.** If a grade, flag or quota route errors, render the neutral state with "unavailable", not the favourable one.
5. **Wording is data.** Keep the four verdict phrases and the abstention phrases in one `lib/` file with tests, so a reviewer can read them all at once.

## 8. Data and decisions to request

| Ask | From | Why | Class |
|---|---|---|---|
| `family` on `/correlations` rows, or a rule-to-family map in `clustered` | backend (local_9831f41a) | U1 cannot match a card to a grade without it | additive read |
| `GET /health/score-quality` per source (distinct values, share at cap, outcome rank correlation) | system-research (local_6eae883b) or backend | U2; removes the hard-coded list | read-only, from stored measurements |
| Daily rubric and validator-reject counters stored under keys the gateway can read | system-research | U3 evaluation tab; no model call | read-only job |
| Fact-id lookup route (`/evidence/fact/{id}` or similar) that returns the stored fact and its source | backend | U3 chips for `case_file` facts, not just event ids | additive read |
| Calls per day per producer and per-consumer tokens (7-day), joined with the SR consumer table | system-research | U5 columns "calls versus cap" and "who reads it" | read-only; any new model call is **OWNER-ONLY** |
| Matched-control version of the clustered grade (same-sector, same recent move and volatility) | system-research | confirms or removes the "moves less" caution in U1 | read-only |
| Beta-adjusted, date-clustered agent grade stored beside `excess_spy` (SR: `excess_beta`) | system-research | so U5 shows agents on the figure SR says to judge | additive column |
| Situation per stock-session (SR C3 evidence packet) | backend | U4 without client-side grouping | additive, shadow first |

**OWNER-ONLY, listed once:** retiring or demoting any rule family or agent; hiding or removing the wargamer panel; showing hypotheses unranked (a product decision about what users see; reversible); a weekly LLM-judge sample or any new model call; anything touching sizing, caps, kill switch, execution, G4, B1 or market-cap floors; cyber events (this report touches none).

## 9. Not checked

- The text of `ThesisPanel`, `AgentConclusions`, `CallsWorkspace` and the wargamer panel (unverified rows in section 1.2).
- The wrapper shape of the stored `clustered` key and whether `/health/model-quota` returns tokens-per-minute fields.
- Whether any current page already shows the factor model's idea record in the way section 1.2 suggests (the frontend session has been editing `IdeaCard` and `TrackRecord`).
- User testing of any wording above. None has been done.
