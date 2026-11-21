# Frontend overhaul: the agreed plan in one page (12 Nov 2026)

This merges two research reports and the backend's answers so far. Read the reports for evidence and detail:

- `reports/frontend-overhaul-ia.md`: the screens, what happens to each current page, five information ideas, first-run states.
- `reports/frontend-overhaul-design.md`: type, colour, density, components, five signature elements, motion, migration.
- Earlier: `reports/frontend-research.md` (23 usability findings) and `reports/style-research.md` (styling techniques).

Status: a proposal. Nothing in it is built. Three decisions need the owner (end of this page). The backend session has not yet replied to the question about data for the visuals; where this plan depends on it, it says so.

## What the overhaul is for

Today the product has 16 sidebar links and four ways to reach a stock. A trader asks four questions: what is happening, which stock, what is the evidence, how am I doing. The overhaul gives each question one screen, puts evidence before opinion, and gives the interface a recognisable look built from numbers, not decoration.

## The six screens

| Screen | One job | Replaces |
|---|---|---|
| Today | What should I know in two minutes? Session, market weather, what changed since last visit, paper account, watchlist, top five calls | Home, Desk overview |
| Stock | Should I act, and what is the evidence? Header, candle chart, key facts, evidence beside opinion, related, order | Desk middle pane, stock drawer, Calls detail |
| Scan | Which stocks should I look at? Sector map with breadth, sortable table, unusual moves | Markets, Calls list, Desk list |
| My Book | How am I doing, and is the model doing what it claims? Account, positions, orders, honest record | Performance, order ticket |
| Research | What is behind the price? News and events, filings and funds, connections, rates | Intel, Filings, Connections, Money, Watch |
| Trust | Can I rely on this? How it works, analysts, data feeds, audit and operations (admin) | How It Works, AI Analysts, Operations, Audit |

The sidebar falls from 16 links to six, plus a profile menu. Old addresses redirect. Sizing, caps, the kill switch and execution logic are not touched.

## The look

- Numbers are the product: every figure in JetBrains Mono, tabular and slashed-zero, signed with a real minus and an arrow, unit small and figure large. A missing value is a dash, never zero.
- Three colour languages that never mix: periwinkle means "you can act", a blue-teal and orange pair means up and down (safe for red-green colour blindness, with a red/green setting for people who prefer it), and the existing green, amber and red mean system status, always in a pill with a word.
- Hairlines and surface steps instead of shadows; two densities (comfortable, compact).
- Five signature elements that each carry data: range-in-context bar (price against its 52-week range), sector heat map with breadth, session clock ring, confidence ribbon (hit rate with its sample size), market pulse strip.

## Build order

Each step ships on its own, behind passing tests, and nothing waits on the whole plan.

| # | Step | Needs from the backend | Status |
|---|---|---|---|
| 1 | Design tokens: direction colours, density, number styles; theme and colour tests updated on purpose | nothing | can start |
| 2 | Primitives: stat tile, price header, evidence row, empty/loading/stale states | nothing | can start |
| 3 | Session strip and clock ring | quotes (live) | can start |
| 4 | Navigation shell: six entries, redirects, profile menu | nothing | can start |
| 5 | Stock page as the single place for a ticker; range-in-context bar; evidence beside opinion | snapshot (coming), range candles (coming) | first pass can start with current data |
| 6 | My Book: summary, positions, honest record with the confidence ribbon | portfolio summary and track record (live) | can start |
| 7 | Today: weather sentence, watchlist, calls, account card | screener for breadth (coming) | partly blocked |
| 8 | Scan: sortable table, filters | screener (coming) | blocked |
| 9 | Sector heat map with breadth | screener with sector and breadth (coming) | blocked |
| 10 | "What changed since you last looked" digest, first-run states | per-user last-seen and one events feed (not built; asked of the backend) | blocked |

## Honest limits

- The research found little published work on retail stock-page layout. Most conventions rest on practitioner sources; each report says where the evidence is weak. The layout choices are hypotheses to check with two or three real users.
- The weather sentence is our own composite. Its rules need writing down in advance and showing in a tooltip.
- The digest is the idea most likely to encourage trading on attention. It ranks by relevance to what you hold or follow, never by excitement.
- Colour numbers in the design report are one researcher's calculations, a first check only. They must be checked in a colour-blindness simulator on the real screens.
- Holidays are not tracked, so the session clock can say "open" on a market holiday.

## Decisions for the owner

1. Navigation: go from 16 links to six, with the old pages moved as in the table? Or keep the current sidebar and only restyle?
2. Direction colours: switch up and down to blue-teal and orange by default, with a red/green setting? Or keep red/green as the default?
3. Options, Crypto and Charts pages: keep, or drop once the Stock page covers them? (Nobody has checked how much they are used.)

Also open: whether Follow the Money and Surveillance Watch stay under Research or move to a separate area outside the trading flow, and whether the sector map covers only the stocks the system already tracks.
