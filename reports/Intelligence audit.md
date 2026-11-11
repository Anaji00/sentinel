# Intelligence audit: where is the system not smart yet? (7 November 2026)

The stress test (6 Nov) asked whether the system breaks. This audit asks whether each layer is **right and useful**, with each test's rule written before
its result. Free data only; nothing here changes trading.

## Layers and the question for each

| Layer | Question | Test | Status |
|---|---|---|---|
| Connections (graph, peers, links) | Do the pairs we call connected move together afterwards, and does one lead the other, beyond what the same industry already gives? | C1, C2 below | Run first |
| Collection | Are we early or late, and which sources never fire first? | Lag of our ingest time against each source's own timestamp, by source | Next |
| Enrichment | Are the entities and tickers attached to events right? | 100 events, a hosted model and a second model each judge the tags; disagreement is reviewed by hand | Next |
| Correlation | Do the alerts we raise precede moves that a random day does not? | Alert families against matched no-alert days (already graded on direction; add magnitude) | Next |
| Agents / predictions | Do trade ideas beat the chance rate on independent periods? | Track record chance rate | Waits for 20 independent periods |
| Stock model | Does it beat a plain rule? | RB1 (registered in MODEL_TRIALS.md) | After the lab run |

## Registered before running: connection tests (7 Nov 2026)

Data: the last 420 sessions of `daily_bars` (adjusted closes) and the stored connection tables. Returns are daily log returns net of SPY's.
Window T is the last 120 sessions; a link or peer set is judged only on returns after it was fixed.

**Connection types:** (a) sympathy peers, rebuilt on sessions T-240 to T-120 with the live function and judged on T-120 to T, so they are out of sample;
(b) text peers (10-K similarity; stored set, judged on the last 120 sessions, since the text does not use returns); (c) customer links (a supplier and its
named customer's ticker, only links filed before T-120); (d) the three combined where a pair appears in more than one.

**C1, co-movement beyond industry.** For each linked pair, its correlation over T. Baseline: for each linked pair, 5 random pairs that share the first ticker
and the same 2-digit SIC. Lift = mean linked correlation minus mean baseline correlation. Bootstrap over source tickers, 2,000 draws.
Pass: lift at least 0.03 and the 95% interval above zero.

**C2, lead and lag.** Over T, regress each stock's excess return on day t+1 on its own day-t excess return and the mean day-t excess return of its linked peers,
one pooled regression per connection type, standard errors clustered by date. Pass: the peer coefficient has |t| at least 2. Report its size as basis points of
next-day move per 1% peer move, and the same coefficient for same-industry random peers. A connection that only co-moves (C1) but does not lead (C2) is
a description, not a signal.

**Reading:** a type that fails C1 is not measuring a connection. A type that passes C1 and fails C2 is real but not tradable as a lead. Both pass: candidate input
for the stock model, to be registered as its own trial. All four types are reported whichever way they fall. This adds no model trial.

**C3, do big peer moves lead? (registered 7 Nov 2026, after C1 and C2 had run and before C3 did).** C2 averages every day, most of which are noise. C3 keeps only the
largest linked-peer days: the top 2% of the type's absolute mean peer excess return. The same regression is run on those observations, with the stock's next-day
and next-5-day cumulative excess return as outcomes. Pass: peer coefficient t at least 2 on either horizon, same sign as the peer's move. This adds one
more test to the four types, so a single pass among eight (4 types x 2 horizons) is weak evidence; a pass needs |t| at least 3 to count.

## Results: connections (7 Nov 2026; `scripts/audit_connections.py`)

Test window 19 May to 6 Nov 2026 (120 sessions), 1,242 names. Sympathy peers were rebuilt on 24 Nov to 18 May and judged only after, so they are out of sample.

| Connection type | Source stocks | C1: linked corr / same-industry random / lift (95% interval) | C2: next-day peer coefficient (t) | C3: largest peer days, next day / next 5 days (t) |
|---|---|---|---|---|
| Sympathy peers (measured) | 927 | 0.55 / 0.26 / **+0.29** (0.27 to 0.30) | −0.01 (−0.3) | 0.007 (0.2) / −0.012 (−0.2) |
| Text peers (10-K similarity) | 579 | 0.37 / 0.26 / **+0.11** (0.09 to 0.13) | 0.003 (0.1) | 0.03 (0.5) / −0.21 (−2.1) |
| Customer links, supplier to customer | 76 | 0.30 / 0.31 / **−0.01** (−0.08 to 0.05) | 0.014 (0.7) | 0.05 (1.5) / 0.12 (1.6) |
| Customer links, customer to supplier | 61 | 0.32 / 0.29 / +0.03 (−0.04 to 0.10) | −0.016 (−1.0) | −0.06 (−2.5) / −0.23 (−2.1) |

**Verdicts under the registered rules**
- Sympathy and text peers **pass C1**: they find stocks that really move together, well beyond the same industry. Sympathy's lift is partly selection (it keeps the most correlated of 1,200), but it holds out of sample.
- Customer links **fail C1**: the 267 supplier and customer pairs from 10-K filings co-move no more than random same-industry stocks. Either the links are not economically tight enough to show in daily returns, or the name-to-ticker matching is poor. The data cannot say which.
- **Nothing leads (C2 and C3 all fail).** No peer move predicts the next day or the next five days. The three C3 readings near |t| 2 to 2.5 are negative (reversal, not a lead) and below the |t| 3 bar set for eight tests.

**What this means.** Connections are good at saying who is exposed to whom, and no good yet at saying who moves next. The stock correlation agent's "sympathy move" ideas
have no support at a daily horizon in these peers. Use connections for risk (shared exposure, concentration, "what else does this event touch") rather than as a signal.

## Strategies to test next, in order (each with the test that must come before building it)

| Layer | Strategy | Test first |
|---|---|---|
| Connections | **Event-conditioned links:** a connection matters when one name has a company-specific shock, not on every day. | C4: after a stock's own news or filing shock (from `stock_news_llm`/events), do its linked peers drift beyond their beta over 1 and 5 days? Compare linked with same-industry random peers. |
| Connections | **Precision of customer links:** weight by share of revenue, drop weak name matches, check a sample against the filing text. | C1 on links with a stated share of 10% or more; a hosted model judges 50 sampled links against the filing sentence. |
| Connections | **Several methods agreeing:** pairs found by two or more of sympathy, text, customer links, graph edges. | C1 lift for the overlap against each method alone. |
| Connections | **Partial correlation graph** (already in `partial_correlation.py`) in place of pairwise correlation. | C1 and C2 on its edges, same window. |
| Connections | **Intraday lead-lag:** daily bars are too coarse to see a lead that lasts minutes. | C2 on 5-minute bars for the 200 most liquid linked pairs. |
| Collection | Race the sources: which feed first reports each kind of event, and by how much. | Lag of our ingest time against each source's own timestamp, by source and event type. |
| Enrichment | Check the entities and tickers attached to events. | 100 sampled events judged by two different hosted models; review the disagreements by hand. |
| Correlation | Do alert families precede bigger moves than matched no-alert days (magnitude, not only direction)? | Matched-day comparison per family, with independent-day counts. |
| Stock model | Does the model beat a plain rule? | RB1 (registered). |

## C4: after a company-specific shock, do linked peers drift? (registered 7 Nov 2026, before the news was exported)

**Shock:** stock A on session d where (1) at least one news item names A alone (news after 16:00 New York time counts for the next session), (2) A's return net of SPY
that session is 4% or more in size, and (3) its linked peers' mean excess return that session is under 1% in size, so the news has not already reached them. A shock
is one stock-session. **Outcome:** the peers' mean excess return over session d+1 (h=1) and d+1 to d+5 (h=5), multiplied by the sign of A's move, so a positive number is
"peers follow the shock". **Baseline:** the same shocks, with 5 random same-2-digit-SIC stocks per source stock in place of its linked peers. **Statistic:** mean of
(linked outcome minus baseline outcome) over shocks; t from a bootstrap over sessions (2,000 draws), so shocks on the same day count once.
**Pass:** t of 3 or more, positive, on either horizon. Eight tests (four connection types x two horizons), so 3 and not 2.
**Windows:** sympathy peers over the last 120 sessions only (out of sample); text peers and customer links over all 424 sessions exported.
**Reading:** a pass means the connection carries news forward and is a candidate input for the idea layer, to be registered as its own trial. A fail means the
connections describe exposure and do not carry shocks at daily speed. Whichever way it falls, it is reported.

### C4 result (7 Nov 2026)

Peer drift after a company-specific shock, signed by the shock's direction, in basis points (linked peers / random same-industry peers / difference; t from a bootstrap over sessions).

| Connection type | Shocks (sessions) | Next day | Next 5 days |
|---|---|---|---|
| Sympathy peers (out of sample) | 784 (113) | +2 / +8 / −6 (t −0.7) | −27 / +9 / −36 (t −1.7) |
| Text peers | 2,788 (417) | +4 / 0 / +4 (t 1.0) | −4 / −15 / +11 (t 1.1) |
| Customer links, supplier to customer | 377 (236) | −12 / +6 / −17 (t −1.2) | −12 / +4 / −16 (t −0.6) |
| Customer links, customer to supplier | 169 (135) | −1 / −3 / +3 (t 0.1) | +10 / +34 / −24 (t −0.5) |

**Verdict: no connection type passes** (the bar was t of 3 or more, positive). Even when a stock makes a 4% company-specific move on its own news and its peers have not yet moved, the peers do not follow in
the next day or week. If anything the sympathy peers fade (−36 bps over five days, t −1.7, below the bar and the wrong sign for a lead). The sample is 120 sessions for sympathy and 424 for the rest, and
the customer-link samples (169 to 377 shocks) are too small to rule out a modest effect; for sympathy and text peers the estimates are tight enough to say any daily-speed drift is small.

**Conclusion for the connection layer.** Across four tests (C1 to C4) the connections we find describe who is exposed to whom, and none carries news forward at a daily horizon. Using them as a
signal is not supported. What remains untested is speed faster than a day (C5, intraday), and whether the customer links are wrong or merely weak (the precision check).

## Customer-link precision (registered 7 Nov 2026, before the clean subset was tested)

The 267 supplier-to-customer links with a ticker came from 104 distinct customer-name and ticker pairs. I reviewed all 104 against one rule: a link is **wrong** if the name
does not denote the ticker's company or names several companies; **unusable** if the company is right but the ticker has fewer than 200 sessions of price data here (OTC or foreign lines);
**clean** otherwise. By the supplier's own table I also review the doubtful short names. The clean subset also needs a stated revenue share of 10% or more.

- **P1 precision:** the share of links that are clean, wrong and unusable.
- **P2 co-movement on the clean subset:** C1, C2 and C4 as registered above, supplier to customer and customer to supplier. Same bars (C1: lift 0.03 with the interval above zero; C2: t of 2; C4: t of 3).
- **P3 dose and response:** among clean links, the Spearman rank correlation between stated share and the link's excess correlation over its same-industry random baseline. Pass: positive with p under 0.05.

**Reading:** if the clean subset passes C1, the weakness was extraction, and the fix is cleaning, not abandoning filings as a source. If it still fails, a customer named in a filing does not show up in daily returns, and the layer's
value is in the case file (who depends on whom), not in prices.

### Customer-link precision result (7 Nov 2026)

**P1 precision.** Of the 267 links with a ticker: **199 clean (75%)**, **44 wrong (16%)**, **24 unusable (9%)**. 185 of the clean ones state a revenue share of 10% or more.
- Wrong: a phrase read as a company ("On" became On Holding for 12 links from AMGN, REGN, FCFS, LII and RPM, three of them with a stated share of 74% to 79%; "People", "Outdoor", "Freedom", "Senior Notes", "Emerging Markets"),
  several companies read as one ("Target, Walmart, Amazon" became TGT; "Abbott, Boston Scientific" became ABT), and a wrong ticker for a right name (Evernorth Health Services became XRPN).
- Unusable: the right company on an OTC or foreign line that has no price data here (Shell RYDAF, Sanofi SNYNF, Nokia NOKBF, Enbridge ENBNF, SK Hynix HXSCL, MTN Group MTNOY, Greif GEF-B, Aumovio AMVIF).
- Judgment calls kept as right: Pool (for Pentair), Walt Disney World (for Host Hotels). Reviewed by me against the written rule; the reviewing was not blind to the earlier C1 result.

**P2 co-movement on the clean subset (right company, price data, share of 10% or more).**

| Direction | Source stocks | C1 lift (95% interval) | C2 t | C4 next day / 5 days (t) |
|---|---|---|---|---|
| Supplier to customer | 64 | +0.013 (−0.067 to +0.093) | 1.3 | 0.3 / 0.1 |
| Customer to supplier | 48 | +0.031 (−0.052 to +0.115) | −1.0 | −0.8 / −1.1 |

**P3 dose and response.** Across 173 clean links the stated share and the link's excess correlation are not positively related (Spearman −0.13, p 0.10): suppliers more dependent on a customer do not move more with it.

**Verdict.** Both problems are real. A quarter of the table is wrong or unusable, and wrong rows reach the case files and the stock features (`case_file.py`, `stock_features.py`, the ownership route, `caseFacts.ts`).
But cleaning does not rescue the signal: the clean subset still fails C1, C2 and C4, and share of revenue does not predict co-movement. With 64 and 48 source stocks the intervals are wide (about ±0.08), so a modest
effect cannot be ruled out; a large one can. Customer links are a fact about dependence for the case file, not a price signal.

**Fix worth doing (not yet done).** In `shared/utils/customer_links.py`: reject names under four characters or on a generic-word list, reject a name containing a comma or "and" (several companies) rather than mapping it to the first,
require the name to resemble the ticker's company name, and map OTC and foreign lines to their US listing or drop them. Then re-run this audit on the cleaned table.

### Customer-link fix applied (7 Nov 2026)

`shared/utils/customer_links.py` now rejects generic phrases and headings ("On", "People", "Senior Notes", names ending in Region, Markets or Notes) and comma lists of several companies, and strips "and its affiliates" tails;
the model path uses the same gate. 5 tests in `tests/test_customer_links.py`; the customer, case-file, stock-feature, ownership and prompt suites pass (86). The gate rejected 44 of the 303 stored rows (the 44 found by hand, a junk fragment
the review missed, and not the one "Fox" row at 4%). Those 44 rows and 14 graph `SUPPLIER_TO` edges that only they supported were deleted; 259 rows remain, 224 with a ticker. The removed rows are saved with their filing accession numbers.
Not fixed: OTC and foreign lines for real customers (they carry no prices here and are harmless to the features), and two alias-table collisions (Evernorth, Southern Minnesota Energy) removed by name only. The running
jobs pick the new gate up when their containers restart. The audit's clean-subset result stands as the verdict on prices; no re-audit was run on the pruned table.
