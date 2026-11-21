# Anti-markdown, anti-slop interface design for Sentinel (18 Nov 2026)

Read-only research. No code, tests or configs were changed. This report follows `reports/style-research.md`, `reports/frontend-overhaul-design.md` and the slop list in `reports/frontend-research.md` (item 8 and item 19 there).

**Source quality key.** **(official)** = the organisation's own page or the originator's own essay. **(secondary)** = a practitioner write-up or news summary. **(weak)** = vendor blog, aggregator, or an AI "skill" file. Where a source did not come back from my search, I say so instead of filling in from memory. The "rendered markdown" diagnosis in section 1 is my own synthesis: no source defines it, so treat it as an argued opinion, not a finding.

---

## 1. The problem, stated precisely

### 1.1 "Reads as rendered markdown"

A page reads as rendered documentation when its structure comes from the document outline, not from the data. The tells, with examples from our own files:

| Tell | What it looks like | Where we have it today |
|---|---|---|
| **Outline layout** | Stacked `H2`, then a paragraph, then a bullet list, repeated down the page | `TodayOverview.tsx` renders four `Card`s, each `h2` + body + a "See every …" link; `SignalsCard` is a `<ul>` of rows |
| **Uniform card grid** | Equal-sized boxes, same radius, same padding, same minimum height, no dominant one | `TodayOverview` `Card` is `rounded-md border bg-raised p-4 min-h-[15rem]` for every card; `ScanBoard.tsx:167` uses `grid-cols-[repeat(auto-fill,minmax(13rem,1fr))]` |
| **Label-over-value tiles everywhere** | A small grey label above a number, in every tile, in the same style | `MetricCard`, `StatTile`, the portfolio card (`text-lead` figure over `text-micro` caption) |
| **Every section a bordered box** | A rule or border around each region, nested | `.panel` plus `.panel-header` plus inner `rounded-md border` pieces (14 `rounded` hits in the five screens I counted) |
| **Bold-lead sentences** | "**Fast:** it is quick. **Safe:** it is safe." | Mostly in copy and tooltips; check by reading each page aloud |
| **Icon or emoji bullets** | A symbol at the start of every list item | Not found in the files I read (earlier report said none), keep it that way |
| **Pills for everything** | Rounded-full coloured chips used for any state | `SignalsCard` "Leans up / Leans down" is a `rounded-full bg-positive/10` pill |
| **Same radius and padding everywhere** | One `rounded-md p-4` on all containers | The `Card` above |
| **Prose that fills space** | Sentences that could sit on any site | Section 8 of `frontend-research.md` lists examples |

The cause is simple: markdown renders a document outline, and a dashboard built from an outline inherits its rhythm (heading, text, list, repeat). A designed page instead has **one dominant element, a few supporting ones, and quiet fine print**, and its grouping comes from space and alignment, not boxes.

### 1.2 "Reads as AI slop" (added evidence)

Our earlier list (emoji icons, purple/indigo gradients and glows, three-card feature rows, generic buttons, slogan headlines, vague lists, invented statistics) is repeated in several design blogs, all **weak**: https://www.925studios.co/blog/ai-slop-design-tells , https://www.utsubo.com/blog/built-with-ai-trust-signals-2026 . New material:

- **The one-sided thick border on a rounded card** is the single most cited tell, in tool vendors' catalogues (weak): https://impeccable.style/gallery . We use none; check `IdeaCard` and `TrackRecord` before adding any accent edge.
- **Identical card grids and uniform large radii**: a decorative grid "adds lines without helping people use the page"; scale the radius to the card, and keep grids for canvases and tasks that need measurement (same vendor, weak): https://noqta.tn/blog/ai-design-slop-overused-ui-patterns-fix-2026 . Three-equal-card rows and mirrored sections are described as default model output (weak): https://instagit.com/Nutlope/hallmark/what-are-the-common-ai-design-patterns-hallmark-aims-to-avoid/
- **Root cause given:** models return "the median of every example in their training data", so unconstrained output converges on one look (weak, repeated by several of those vendors). Plausible, not measured.
- **Writing tells:** a detector vendor (Pangram) reports double-asterisk bold at about 43 times the human rate (265 vs 5 per 10,000 words), em dashes at about 17 vs 2, and "not just X but Y" at about 3 times: https://www.pangram.com/signs-of-ai-writing . It sells a detector, so the figures are its own measurements; the same sources warn that no single tell is conclusive and that human writers also use all of them. I found no source that isolates bold-lead-in bullets specifically, so that rule rests on the general bold finding and on our own judgement.

**Working definition for this project.** An element is slop if it is *there because it is a default*, not because of the data it carries. Test: delete it and ask if the reader loses information. If not, it is decoration.

---

## 2. What strong practitioners do instead

Each entry says what to copy. Sources are what I could confirm; where I could not reach the page I say so.

| Reference | What to learn | Evidence |
|---|---|---|
| **Edward Tufte** (data-ink, sparklines, small multiples, direct labelling) | Maximise the share of ink that carries non-redundant data; sparklines are "word-sized graphics" inside text and tables; repeat the same small chart across many items so comparison is free; label lines in place, not in a legend | Tufte's own sparkline note (official): https://www.edwardtufte.com/notebook/sparkline-theory-and-practice-edward-tufte/ ; summaries of the book's principles (secondary/weak, not the book): https://faculty.cc.gatech.edu/~stasko/4460/Notes/tufte.pdf |
| **Financial Times graphics (Chart Doctor, Visual Vocabulary, John Burn-Murdoch)** | Chart type chosen by the question; labels sit beside lines, in the line's colour; a strong title that states the finding; annotations placed where the reader's eye is. A summary reports a reader study in which well-annotated charts with narrative titles were recalled best | FT Visual Vocabulary repository (official): https://github.com/Financial-Times/chart-doctor/blob/main/visual-vocabulary/README.md ; direct labelling (secondary): https://data.europa.eu/apps/data-visualisation-guide/labels-and-marks ; talk summary (secondary): https://lab.imedd.org/en/from-data-to-storytelling-concept-and-design-tips-from-the-financial-times-john-burn-murdoch/ . I did not reach FT's internal style guide, and I found nothing specific on Bloomberg Graphics data pages |
| **Bloomberg Terminal** | Density is a product feature; one signature colour scheme (amber on black) used consistently; standards documented and taught; changes rolled out slowly because even a button move disrupts users | Bloomberg UX blog (official): https://www.bloomberg.com/ux/2020/08/11/consistency-more-than-just-a-buzzword/ ; no official page gives exact hex or spacing rules |
| **The Pudding** | A story is a few top-line findings walked through step by step, then the full data opened for exploration; writers cut prose so visuals carry the point; annotations on charts (they use `d3-annotation`, `scrollama`) | The Pudding's own resources and FAQ (official): https://pudding.cool/resources ; process summaries (secondary): https://www.storybench.org/?p=8613 , https://flowingdata.com/2020/09/08/telling-stories-in-visual-data-driven-essays/ |
| **Observable Plot / Observable stories** | Charts built by layering marks; titles, subtitles and annotations "tell the reader what is interesting"; a `tip` mark gives static annotations; text marks carry multi-line notes | Observable docs (official): https://observablehq.com/plot/marks/tip ; https://observablehq.com/blog/five-techniques-to-improve-chart-annotations |
| **Bret Victor, Explorable Explanations** | Active reading: the reader changes an input and sees the consequence; attention is guided to a phenomenon, not left to a widget | Victor's essay (official): https://worrydream.com/ExplorableExplanations/ |
| **Linear** | Reduce noise with fewer separators and subtle borders; keep density high without overwhelm | Linear's own post (official): https://linear.app/blog/behind-the-latest-design-refresh ; https://linear.app/now/how-we-redesigned-the-linear-ui |
| **Stripe** | Restraint as a functional requirement: writing, code examples, grouping and spacing carry meaning, not decoration | Single-sample commentary (weak): https://anthonyhobday.com/blog/20220810 ; Stripe's own design posts did not come back in my search |
| **Koyfin and TradingView** | Customisable watchlist columns, saved screens, dense tables | Koyfin's feature pages (official): https://www.koyfin.com/features/stock-screener/ . **I could not confirm that Koyfin shows sparklines in its screener or watchlists**; do not cite it for that. TradingView I did not source |
| **Swiss / International Typographic Style** | A strict grid, asymmetric layouts, flush-left ragged-right, hierarchy from large size differences on the grid rather than colour or decoration | Textbook overviews (secondary): https://courses.lumenlearning.com/suny-graphicdesign/chapter/1-6-international-typographic-style/ ; https://www.printmag.com/?p=1387 |
| **Brutalist web design** | Not covered by my searches, so no claim here. Treat as unsourced inspiration only |

### 2.1 The layout moves that follow from these references

1. **Asymmetric columns on a grid** (for example 8 + 4, or 5 + 3 + 2), never n equal columns.
2. **One dominant element per screen,** about two to three times the visual weight of anything else.
3. **The table is the design:** align, rule with hairlines, add inline sparklines and tiny bars, and drop the box around it.
4. **Marginalia and annotations on charts:** event markers and one-line notes at the point they refer to.
5. **Direct labels, not legends:** series named at the line's end, in ink, with the colour only on a small mark.
6. **Inline sparklines in prose and tables,** sized to the text line.
7. **Big numerals, small units,** set on a baseline, with signs and arrows doing the direction work.
8. **Hairline rules, not boxes:** one rule between regions; boxes only for things that float or can be dragged.
9. **Scale contrast in type:** three to four sizes only, with big jumps (about 1.5 to 2 times) so hierarchy is obvious without bold or colour.
10. **Whitespace as grouping:** a gap twice as big between groups as inside them.

**Constraint kept throughout:** colour carries meaning only (direction, status, action). Nothing below uses colour for decoration, and no sketch introduces a raw Tailwind hue class (guard test `color_tokens.test.ts`).

---

## 3. Proposals for our screens

Data notes first. Routes I checked in the gateway: `/stocks/{ticker}/evidence` (sections quote, snapshot, held_back, regime, producer_grade, positioning, model, each with a status), `/quotes`, `/market/breadth`, `/market/heatmap`, `/stocks/screener` (rows: ticker, price, change_pct, volume_vs_20d, model_percentile, next_event {kind, date, estimated}, market_cap, sector), `/predictions/track-record/slice` (calls, share right, net results in bps, `live.n`, `live.since`, `outcome_spread`, `n_dates`), `/portfolio/summary`, `/changes`, `/calendar/earnings` (rows: ticker, date, hour, estimates, market_cap). **Two items are not in your route list but exist and are needed:** `/predictions/closes/{ticker}` (daily closes, up to 400 days, already used by `TickerHero`) and `/radar/candles/{ticker}`.

Direction colour in the sketches is written `var(--up)` / `var(--down)` (the pair from `frontend-overhaul-design.md`). If those tokens are not in the stylesheet yet, use `--positive` / `--negative` for now. Status colours are not used below.

### 3.1 Today / Home: "one dominant element"

- **Remove:** the four equal `Card`s with `min-h-[15rem]`, the repeated "See every holding" link under each, and the rounded-full "Leans up" pills.
- **Replace with:** a three-region layout. (1) A dominant **book strip**: the paper book's return against the S&P in 56 px numerals with the day's change and a 90-day spark line that goes the full width, no border. (2) A **"since you last looked"** column of changes (`/changes`), written as an editorial timeline (see 3.6). (3) A narrow **today's calendar** margin (`/calendar/earnings`, next 3 days) as a plain list with dates in a left gutter. The old "strongest evidence" list becomes the screener-style table below, not a card.
- **Data:** `/portfolio/summary` (book), `/changes`, `/calendar/earnings?days=3&scope=traded`, `/market/breadth` for a one-line market context. (`TodayOverview.tsx` currently uses `/predictions/model-portfolio` and `/signals/matrix`; both exist, so the change is layout first, data second.)
- **Sketch (CSS, 8 lines):**
```css
.today { display:grid; gap:2rem 3rem; grid-template-columns: minmax(0,8fr) minmax(16rem,4fr); }
.today-lead { grid-column:1; }            /* the dominant element */
.today-lead .figure { font-size:var(--text-hero); line-height:1; letter-spacing:-.02em; }
.today-lead .figure small { font-size:.4em; color:var(--text-muted); margin-inline-start:.25em; }
.today-side { grid-column:2; border-inline-start:1px solid var(--border-subtle); padding-inline-start:1.5rem; }
.today-rule { border:0; border-top:1px solid var(--border-subtle); margin-block:1.5rem; }
@container (width < 40rem) { .today { grid-template-columns:1fr; } .today-side { border:0; padding:0; } }
```
- **Accessibility:** single `h1` and one `h2` per region; the figure has a text equivalent in its label ("Paper book up 3.2% against the S&P's 2.1% since 4 Nov, 14 trading days"); order in the DOM matches reading order; the sparkline is `aria-hidden` when the figure is already read out.
- **Effort:** M.

### 3.2 Stock page: an annotated price chart

- **Remove:** the chart's separate legend line and the stack of equal sub-panels below it.
- **Replace with:** the candle chart with **event markers on the time axis** (earnings dates, 8-K and other filings, divergence alerts), each a small tick on a rail under the candles, with a one-line label that appears on hover or focus and, for the most recent two, always shown. Direct labels for the 20 and 50 averages at the line ends. The evidence sections sit in a right-hand margin column as hairline-separated notes aligned to the dates they refer to.
- **Data:** `/radar/candles/{t}` (bars), `/stocks/{t}/evidence` (held_back, regime, model), `/calendar/earnings?tickers=T` for upcoming and past report dates. Filing dates come from the filings route already used by `FilingsStream.tsx` (not on your list; confirm the shape before relying on it).
- **Sketch (SVG, 12 lines), inside `CandleChart.tsx`'s `<svg>`:**
```tsx
{events.map((e) => (
  <g key={e.id} transform={`translate(${x(e.i)} ${PLOT_H + 4})`} tabIndex={0} role="img"
     aria-label={`${e.kind} on ${e.date}: ${e.note}`} className="evt">
    <line y2={-PLOT_H} stroke="var(--border-subtle)" strokeDasharray="1 3" />   {/* faint guide up through candles */}
    <path d="M0 0 l4 6 h-8z" fill="var(--text-secondary)" />                    {/* neutral marker: not colour-coded */}
    <text y="18" textAnchor="middle" fontSize="9" fill="var(--text-muted)">{e.short}</text>  {/* "Q3", "8-K" */}
  </g>
))}
```
Markers are neutral ink because an earnings date is not up or down; only the *reaction* (next-day move, printed in the label as "+4.1%") uses direction colour.
- **Accessibility:** each marker is focusable with a full-sentence label; the existing table view lists events as extra rows; hover is not required (touch shows the label on tap); markers use shape plus text, not colour.
- **Effort:** M (S if events are already joined server-side; the join of filings to a ticker is the unknown).

### 3.3 Scan: the table is the design, with inline sparklines

- **Remove:** the auto-fill grid of sector cards (`ScanBoard.tsx:167`) as a separate block, and the grey header row styling that differs from every other table.
- **Replace with:** one dominant **sector strip** (the heat map as a single horizontal band: width = number of stocks, shade = move, direct-labelled), then the screener table with columns: ticker + name, price, day change with a 60-day **sparkline**, a thin bar for volume vs usual, a model-percentile **dot on a 0–100 track**, and "next event" as a short date. No row borders except hairlines; sort indicator is a small arrow.
- **Data:** `/market/heatmap?by=sector`, `/market/breadth`, `/stocks/screener`. **Gap:** the screener rows carry no price history. Sparklines need `/predictions/closes/{t}` (one request per ticker) or a new batch route. Do not fire 100 requests: fetch only the first 25 visible rows, with `IntersectionObserver`, cache per ticker in SWR, and render a flat dash while loading. A batch endpoint (`/stocks/screener?with=spark`) is the right fix and a backend task, flagged here and not done.
- **Sketch (SVG sparkline component, 9 lines):**
```tsx
export function Spark({ v, w = 72, h = 18 }: { v: number[]; w?: number; h?: number }) {
  if (v.length < 2) return <span className="text-ink-mute">—</span>;
  const lo = Math.min(...v), hi = Math.max(...v), up = v[v.length - 1] >= v[0];
  const pts = v.map((y, i) => `${(i / (v.length - 1)) * w},${h - 1 - ((y - lo) / (hi - lo || 1)) * (h - 2)}`).join(' ');
  return (<svg width={w} height={h} viewBox={`0 0 ${w} ${h}`} aria-hidden>
    <polyline points={pts} fill="none" strokeWidth="1.25" stroke={up ? 'var(--up)' : 'var(--down)'} />
    <circle cx={w} cy={h - 1 - ((v[v.length - 1] - lo) / (hi - lo || 1)) * (h - 2)} r="1.8" fill="currentColor" />
  </svg>);
}
```
- **Accessibility:** the sparkline is decorative (`aria-hidden`) because the day-change cell already states the move in text; the model dot has a visually hidden "model rank 82 of 100"; table keeps `<th scope>` and is sortable by keyboard (`aria-sort`).
- **Effort:** M (sparkline S; the data fetch is the work).

### 3.4 Calls: a typographic scale instead of boxes

- **Remove:** idea cards of equal size with pill badges for every state.
- **Replace with:** a **ledger list**, one call per row, set on a three-level scale: ticker at 20 px mono, the call in one plain sentence at 14 px, and fine print (horizon, evidence strength, track record n) at 11 px muted. A call's strength is shown by a 2 px **left rule's length** and a number, not a chip colour. The track-record line shares the row ("past calls like this: 41, 58% right") with a small interval ribbon (see `frontend-overhaul-design.md` §3.4).
- **Data:** `/stocks/{t}/evidence` per row on open, `/predictions/track-record/slice` for the record line (`live.n`, share right, `outcome_spread`).
- **Sketch (CSS, 8 lines):**
```css
.call { display:grid; grid-template-columns:7rem 1fr auto; column-gap:1.5rem; padding-block:.9rem;
        border-top:1px solid var(--border-subtle); }
.call .sym { font:600 1.25rem/1 var(--font-mono); }
.call .say { font-size:.875rem; max-width:62ch; text-wrap:pretty; }
.call .fine { font-size:var(--text-micro); color:var(--text-muted); font-variant-numeric:tabular-nums; }
.call:has(:focus-visible) { background:var(--bg-overlay); }
.call:first-child { border-top:0; }
```
- **Accessibility:** the whole row is one link target with a single name; strength is the written word ("Higher confidence") plus the number, never only the rule length; keep a minimum 44 px target height on touch.
- **Effort:** M.

### 3.5 Performance: an annotated equity line, direct labels, and honest small numbers

- **Remove:** the tile row of four equal KPI cards and the legend.
- **Replace with:** one wide equity curve (book vs S&P) with **direct labels at the line ends**, a single annotated note at the biggest drawdown or best week, and below it a two-column table of horizons (daily, weekly, monthly) with n, share right and an interval ribbon. Where `n` is below the floor, the ribbon is hatched and the row says "too few to judge".
- **Data:** `/portfolio/summary`, `/predictions/track-record/slice` (once per horizon), existing model-portfolio NAV series used by the current Today card.
- **Sketch (SVG, 7 lines):**
```tsx
<path d={bookPath} fill="none" stroke="var(--text-primary)" strokeWidth="1.5" />
<path d={spyPath}  fill="none" stroke="var(--text-muted)"   strokeWidth="1.25" strokeDasharray="3 2" />
<text x={W - 4} y={yBook}  textAnchor="end" className="fill-ink"      fontSize="11">Book +3.2%</text>
<text x={W - 4} y={ySpy + 14} textAnchor="end" className="fill-ink-mute" fontSize="11">S&amp;P +2.1%</text>
<line x1={xDD} x2={xDD} y1={0} y2={H} stroke="var(--border-strong)" strokeDasharray="1 3" />
<text x={xDD + 4} y={12} className="fill-ink-dim" fontSize="10">Worst week: −2.4%</text>
```
Two lines are told apart by **weight and dash**, not hue, so no colour is spent on decoration; direction colour is reserved for the signed figures.
- **Accessibility:** a text summary above the chart states both returns and the period; labels are real text; dash pattern differs for colour-blind readers; a data table toggle exists.
- **Effort:** M.

### 3.6 Editorial "what happened" timeline instead of a bullet list

- **Remove:** the bulleted "what changed" list (and bold lead-ins inside it).
- **Replace with:** a vertical **time-gutter timeline**: a left gutter with the time in mono, a hairline, then one sentence per event; the most important event of the day is set larger and runs two lines; older events step down in size and ink. Events of the same hour group under one gutter label. A small inline spark or a "+4.1% by close" figure sits inside the sentence.
- **Data:** `/changes` (what moved since last visit), `/calendar/earnings`, filings route for filings. The wording is built by existing pure helpers (`lib/plainEvent.ts`, `lib/plainFiling.ts`), so no new copy engine.
- **Sketch (HTML + CSS, 10 lines):**
```html
<ol class="tl">
  <li><time>14:32</time><p class="lead">NVDA filed an 8-K naming a new CFO. <span class="num">+2.1%</span> since.</p></li>
  <li><time>11:05</time><p>XOM's volume ran at <span class="num">2.4×</span> its usual by noon.</p></li>
</ol>
```
```css
.tl { display:grid; row-gap:1.1rem; } .tl li { display:grid; grid-template-columns:3.5rem 1fr; column-gap:1rem; }
.tl time { font:var(--text-micro) var(--font-mono); color:var(--text-muted); padding-top:.2em; }
.tl li + li { border-top:1px solid var(--border-subtle); padding-top:1.1rem; }
.tl .lead { font-size:var(--text-head); line-height:1.3; text-wrap:balance; } .tl p { font-size:.875rem; }
```
- **Accessibility:** a real ordered list with `<time datetime>`; the order is newest first and says so in the heading; no reliance on colour; links inside the sentence have underlines on focus.
- **Effort:** S.

### 3.7 Type scale that uses contrast instead of boxes

- **Remove:** panel headers with background fills and bordered captions as the main way to show hierarchy.
- **Replace with:** a four-step scale applied by **role**, not by container: *figure* (hero, 48 px mono), *statement* (18 px sans, the sentence that says what it means), *body* (14 px), *fine* (11 px, muted). A region needs at most one figure and one statement. Separate regions by a hairline and a 2× gap.
- **Sketch (CSS, 6 lines):**
```css
.t-figure { font:600 clamp(2rem,1.4rem + 2vw,3rem)/1 var(--font-mono); letter-spacing:-.02em; }
.t-say    { font:500 var(--text-head)/1.3 var(--font-sans); text-wrap:balance; max-width:40ch; }
.t-body   { font:400 .875rem/1.5 var(--font-sans); max-width:68ch; text-wrap:pretty; }
.t-fine   { font:400 var(--text-micro)/1.35 var(--font-sans); color:var(--text-muted); }
.region + .region { margin-block-start:2.5rem; border-top:1px solid var(--border-subtle); padding-top:1.5rem; }
```
- **Accessibility:** sizes in `rem`, so browser zoom and text-size settings work; contrast comes from size and weight, not colour; heading levels stay in order independent of size.
- **Effort:** S for the classes, M to apply across screens.

---

## 4. One-page style guide, and which rules can be tests

Rules a contributor can check by eye. "Test" = can be a guard test. Existing guard tests are named in section 5.

| # | Rule | Test? |
|---|---|---|
| 1 | **One bordered container per region.** A region is a screen area with one `h2`. No nested bordered boxes. | Partial: a scan that fails if a `.panel` or `rounded … border` element contains another within a component file is brittle; use a Playwright check on rendered pages (count of nested elements with a border and a background) |
| 2 | **One dominant element per screen** (the largest figure is at least 2× the next). | No (needs eyes). A Playwright check of the largest font-size on each page is possible but weak |
| 3 | **Every number gets the unit style:** figures in `.num`, units in `<small>`; missing values are `—`, never 0. | Yes: a unit test over `lib/format.ts` helpers (returns the `—` for null); a lint-style scan for raw `.toFixed(` in `.tsx` outside the formatter |
| 4 | **No bold-lead sentences.** No `<strong>`/`font-semibold` at the start of a list item or sentence followed by a colon. | Yes, partially: a regex over `.tsx` string literals for `<strong>…</strong>:` or `<b>` at the start of `<li>` |
| 5 | **No emoji and no emoji-like symbols as icons or bullets** (▲▼ for direction are allowed). | Yes: a regex over `.tsx` and `lib/*.ts` for emoji ranges, with an allow-list |
| 6 | **Colour means direction, status or action only.** Direction tokens only in figures, candles, sparklines, heat cells; status tokens only in pills, banners and notices; accent only for interactive or selected. | Yes: extend `color_tokens.test.ts` with a class allow-list per role |
| 7 | **Radius scale:** 0 for tables and rules, 4 px for chips and inputs, 6 px for floating panels, full only for pills. No other radius values. | Yes: scan for `rounded-(lg|xl|2xl|3xl)` in `.tsx` (the existing `ui.ts` uses `rounded-lg`/`rounded-xl` in a few recipes, so this needs a deliberate update) |
| 8 | **No one-sided accent border on a rounded container** (`border-l-4` with `rounded`). | Yes: regex |
| 9 | **No equal-card grid of three or more** for content (`grid-cols-3` with identical children). Grids are for charts, maps and tiled tasks. | Weak: regex flags `grid-cols-3`/`auto-fill` for review, not for fail |
| 10 | **Direct labels over legends:** a chart with two or more series labels them at the line end. | Partial: Playwright check that a chart `<svg>` has `<text>` labels equal to its series count |
| 11 | **Annotate, don't explain:** if a chart has an event worth a sentence, the sentence is on the chart. | No |
| 12 | **Hairlines, not boxes:** separate with `border-top` at `--border-subtle`; shadows only on things that float. | Partial: scan for `shadow-` utilities outside overlay components |
| 13 | **Whitespace groups:** the gap between groups is at least 2× the gap within a group. | No (review) |
| 14 | **Motion marks change only** (tick tint, place change, loading), and stops under reduced motion. | Existing CSS tests cover the reduced-motion block; a scan for `animate-` classes outside an allow-list is possible |
| 15 | **Plain headings that say what the thing is or what happened,** not slogans; no "unlock", "supercharge", "seamless". | Yes, cheap: a banned-word list over UI strings (weak but useful) |
| 16 | **Em dash and "not just … but …" habits:** limit to one em dash per paragraph of UI copy; no "not just X but Y". | Partial: regex for "not just" in UI strings; the em-dash limit is a review rule (the evidence is that these are statistically more common in model text, which does not make them wrong, so keep this one advisory) |

---

## 5. Respecting the existing guard tests

| Test | Constraint | Impact of this plan |
|---|---|---|
| `frontend/src/__tests__/color_tokens.test.ts` | No raw Tailwind hue classes; no `text/bg/border-white` in `.tsx` | All sketches use CSS variables or role-token classes. New direction classes must exist in `@theme inline` first |
| `frontend/src/__tests__/theme.test.ts` | Dark tokens must exist in the light block | Any new token (for example `--up`, `--down`, spacing) goes in both |
| `tests/test_frontend_shell_and_swarm_api.py` | `.glass-panel` slice, `.glow-cyan` block, `prefers-reduced-motion`, `:focus-visible`, `tabular-nums` | Do not edit or reorder those blocks; new classes go in a new `@layer components` section after them |
| `tests/test_frontend_enterprise_surface.py` | Locale and time-zone explicit in formatting | The timeline's `<time>` and every figure use `lib/format.ts` and the zone context |
| `tests/test_frontend_icon_vocabulary.py` | Icon vocabulary; no empty button | Event markers are SVG text and shapes, not icons; no change needed |
| Component and render tests (`component_rendering`, `ui_states`, `datagrid`, `idea_cards`, `trade_ideas_plain`, `first_look`, `today_overview`) | Markup and wording | I did not read them all. Replacing cards with the ledger and timeline will change expected strings; update those assertions deliberately, with a comment that the redesign changed them, and run them after each screen |

---

## 6. Ranking: what to do first

Five highest-impact changes (value for the data, then effort):

1. **Replace the equal-card Today grid with one dominant book figure plus a time-gutter changes timeline** (3.1 + 3.6). It is the first screen and the strongest "documentation" tell.
2. **Put events on the price chart** (earnings, filings, alerts) with direct labels (3.2). This is the clearest way for the product to show evidence at the point it applies.
3. **Turn the screener into the design: inline sparklines, volume bars and the model-percentile dot, no card grid** (3.3), after deciding between lazy per-row closes and a batch endpoint.
4. **Adopt the four-step type scale with a hairline-and-gap rhythm, and delete the container boxes it replaces** (3.7, rules 1, 7, 8, 12).
5. **Add the small guard tests: no emoji, no bold-lead lists, no one-sided accent borders, no banned slogan words, a role-based colour allow-list** (rules 4, 5, 6, 8, 15), so the look does not drift back.

**Evidence gaps to close before building.**
- I found no authoritative source for Bloomberg Graphics data pages, Stripe's own design posts, Koyfin screener sparklines, or Brutalist web design; none of them is used as proof above.
- All "AI slop" design-tell sources are vendor or agency blogs (weak); the writing-tell numbers come from a detector vendor. The reasoning stands on first principles (does the element carry data?), not on those counts.
- Before building 3.3, check how many rows the screener shows and whether a batch history route is acceptable (a backend change, outside this report).
- Run the colour pair and the hatched-ribbon pattern through a colour-blindness simulator on real screenshots.
