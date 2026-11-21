# Sentinel frontend overhaul: design system, signature elements, motion and migration (12 Nov 2026)

Read-only research and design. No code, tests or configs were changed. Builds on `reports/style-research.md` (the safe items from it are already applied: mono face loaded as `--font-jb`, accent scale derived from one token, hollow candles, `text-wrap`, `prefers-contrast`).

Evidence policy: each external claim has a URL. Weak sources are labelled **(weak)**. Colour numbers marked **measured** are my own calculation in this session (WCAG contrast and CIE Lab distance, with the Machado colour-blindness matrices); treat them as a first check, not a replacement for testing in a simulator on the real UI.

---

## 1. Design system proposal (Tailwind 4 tokens)

Principle: **numbers are the product, colour is a language with three words, and everything else is quiet.** The three colour words never share a hue family: *direction* (up/down), *status* (good/watch/bad), and *action* (accent). Today direction and status share green and red, which is the main reason the interface cannot yet say "price is down but the system is healthy" without confusion.

### 1.1 Typefaces and roles

| Role | Face | Where | Notes |
|---|---|---|---|
| Voice (sentences, labels, headings) | Inter (already loaded, `--font-inter`) | Everything that is read as language | Keep `font-optical-sizing: auto`; headings at weight 600, `letter-spacing: -0.01em`; labels 500. Inter ships with proportional digits by default and has `tnum` and `zero` features: https://rsms.me/inter/ |
| Figures (instrument) | JetBrains Mono (already loaded, `--font-jb`) | Prices, changes, percentages, tickers, timestamps, IDs, table cells | Open Font License is my understanding; not verified in this session. Tabular by nature. Only two weights used: 450 for body figures, 650 for hero figures. |

Rule of thumb: **if you could sum it, sort it or copy it, it is mono; if you read it, it is Inter.** A sentence that contains a number ("rose 4.2% after the filing") stays Inter, with only the number wrapped in a `.num` span.

### 1.2 Type scale

Keep the five existing steps and add two, so nothing in use breaks. Sizes in px at 100% zoom; line heights unitless.

| Token (Tailwind class) | Size / line | Role |
|---|---|---|
| `text-micro` (exists) | 11 / 1.35 | Dense table cells, unit suffixes, timestamps |
| `text-xs` | 12 / 1.4 | Labels, secondary figures |
| `text-sm` | 14 / 1.5 | Body, primary figures in tables |
| `text-head` (exists) | 18 / 1.3 | Panel and page titles |
| `text-lead` (exists) | 24 / 1.2 | The number a panel exists to show |
| `text-display` (exists) | 34 / 1.1 | One hero figure per screen |
| **`text-hero` (new)** | clamp(2.25rem, 1.6rem + 2vw, 3.25rem) / 1.05 | The price on a stock page only |
| **`text-whisper` (new)** | 10 / 1.3, uppercase, 0.08em tracking | Axis units and legends only; never for sentences |

Fluid `clamp()` is used for one token only. Everything else stays fixed because dense UIs need predictable row heights.

### 1.3 Number styling rules (the recognisable part)

1. All figures use `font-variant-numeric: tabular-nums slashed-zero lining-nums` (tabular numbers are widely available since 2022: https://web-platform-dx.github.io/web-features-explorer/features/font-variant-numeric).
2. **Right-align** every numeric column; align on the decimal by giving every row in a column the same decimals.
3. **Unit small, figure large.** `$` and `%` and suffixes (`K`, `M`, `x`) render at 70% size, muted, baseline-aligned: `<span class="num">184.32<small>USD</small></span>`.
4. **Sign is explicit and typographic:** `+` and a true minus `−` (U+2212), never a hyphen. Zero is `0.00` in muted ink, never coloured (already true in `directionColor()`).
5. **Direction is carried by sign plus glyph plus colour,** never colour alone (see 1.4).
6. **Missing is a dash, not zero:** `—` in muted ink, with a `title`/`aria-label` of "not available" (matches the repo rule that missing data is not zero).
7. Never `toLocaleString()` without a locale; format through `lib/format.ts` (a repo guard test enforces this).
8. Large numbers compact only above 10,000, and the full value stays in the tooltip.

### 1.4 Colour roles

Three families, kept apart.

**Surfaces and ink (neutral slate, unchanged).** `--bg-base/raised/overlay/inset`, `--text-primary/secondary/muted`. Keep.

**Action (one accent).** `--accent` periwinkle (`#8b9dff` dark, `#4152c9` light). Means "you can act on this" or "selected". The scale is already derived from it.

**Direction (new, colour-blind safe).** Replace red/green as the up/down pair with a blue-teal and orange pair. Datawrapper's guidance: blue with orange or red is the safest pairing; avoid green with orange or red: https://www.datawrapper.de/blog/colorblindness-part2 . Okabe-Ito is the standard colour-blind-safe categorical set (blogs; **weak** for exact claims): https://kindatechnical.com/data-visualization/colorblind-friendly-color-palettes-for-data-visualization.html

| Token | Dark | Light | Meaning |
|---|---|---|---|
| `--up` | `#2fb8d6` | `#0a6f8a` | Price/value rose, buy-side, bar closed above open |
| `--down` | `#f97316` | `#c2410c` | Price/value fell, sell-side, bar closed below open |
| `--flat` | `--text-muted` | `--text-muted` | Zero or unchanged |
| `--up-dim` / `--down-dim` | 14% alpha of each | 10% alpha | Tints behind flashes, heat cells, ribbons |

Measured (my calculation): on `#111722`, `--up` has contrast 7.65:1 and `--down` 6.41:1; on light `#ffffff` the light pair gives 5.75:1 and 5.18:1 (all above 3:1 for graphics and 4.5:1 for text: https://w3.org/WAI/WCAG22/Understanding/non-text-contrast). Perceptual distance between up and down (Lab ΔE, higher is better): about 105 normal, 98 under deuteranopia simulation, 89 under protanopia, 90 under tritanopia. The old red/green pair (`#34d399`/`#f87171`) drops to about 18 under deuteranopia and 24 under protanopia, which is why it fails. The accent is also about 51 ΔE from `--up`, close enough that **accent never appears inside a chart series as a direction;** it is used for selection and the moving averages only (as now).

Offer a **setting**: "Colour pair: Blue/orange (default) or Red/green". Many traders expect red/green and a toggle costs one `data-palette` attribute. The classic option maps `--up: var(--positive)` and `--down: var(--negative)`.

**Status (existing, unchanged).** `--positive #34d399`, `--caution #fbbf24`, `--negative #f87171`, `--info`, `--civic`. Used only for system and signal state (data fresh, gate open, limit hit, error). To keep status from being read as direction, status always travels with an icon or word ("Healthy", "Delayed", "Blocked") and sits in a pill, never as a bare coloured number. Residual risk: `--down` orange is only about 31 ΔE from caution amber at `#fb923c` and about 43 at `#f97316`; that is why I picked `#f97316` and why status uses pills and words.

**Rule (testable):** direction tokens appear only in `.num`, candle, sparkline, heat cell, ribbon and flash classes; status tokens only in `.pill`, banners and notices. A new guard test (section 5) can scan for misuse.

### 1.5 Elevation and borders

- Four steps, by surface value, not shadow: `page → raised → overlay → inset`. A raised panel has a 1px `--border-subtle` and a 1px inner top highlight (`inset 0 1px 0` at 4% ink). Overlays (popover, drawer, palette) add `--shadow-overlay` only: they are the only things that float.
- Borders: `subtle` for layout, `strong` for hover/inputs, `accent` for selected. Rules between table rows are `--border-subtle` at half alpha, 1px, with no zebra stripes (hover and selected rows do the work).
- Radius: 6px panels (exists), 4px chips/inputs, 999px for pills only.
- No gradients on surfaces, no blur, no glow (guarded by tests).

### 1.6 Spacing and density

4px base grid. Two modes via `<html data-density="comfortable|compact">`, default **comfortable** for ordinary users, **compact** for a desk:

| Token | Comfortable | Compact |
|---|---|---|
| `--pad` (panel body) | 16px | 10px |
| `--pad-y-head` (panel header) | 12px | 6px |
| `--row-h` (table row) | 40px | 28px |
| `--gap` (grid gap) | 16px | 10px |
| Type base | 14px | 13px |

Density is stored like the theme (`THEME_KEY` pattern) and set by the boot script before hydration to avoid a flash. Each token also needs a Tailwind alias (`@theme inline { --spacing-pad: var(--pad); }`).

### 1.7 Light and dark

Both themes are full redefinitions (as now). Rule: every token added to dark is added to light in the same commit (the theme test enforces it). Light-theme direction tokens above are chosen for ≥5:1 on white. Under `prefers-contrast: more`, borders go to 45% and 70% alpha (already applied). `light-dark()` is not recommended now (supported since Chrome 123, Firefox 120, Safari 17.5 per a mirror: https://docs.w3cub.com/css/color_value/light-dark) because the theme test reads two separate blocks.

---

## 2. Component kit and the one craft technique each

Support figures come from the sources cited once at the end of this section, or in `style-research.md`. "Baseline" below means usable in all current engines.

**Shared states every component must define:** default, hover, focus-visible, selected/pressed, disabled, loading, empty, error, stale (data older than its freshness limit), and reduced-motion. The table lists what is *specific* to each.

| Component | Specific states | The one craft technique | Support |
|---|---|---|---|
| **Stat tile** | value, delta, delta direction (up/down/flat/NA), stale, compact variant | **Container query:** tile re-lays out from stacked to side-by-side by its own width | Container queries: Chrome 105, Firefox 110, baseline widely available since 2025-08-14: https://web-platform-dx.github.io/web-features-explorer/features/container-queries |
| **Price header** (`TickerHero`) | live, delayed, market closed, halted, tick up/down | **`@property` flash alpha:** register `--tick-a` as `<number>` so one keyframe fades the tint smoothly and the glyph fades with it | `@property` Baseline 2024: https://web.dev/blog/at-property-baseline |
| **Candle chart** | frame (1d/1w/…), hover bar, hollow-up/filled-down, table view, no data, gap | **CSS variables inside SVG + `paint-order: stroke`** so the last-price tag stays legible over candles, and `vector-effect: non-scaling-stroke` keeps hairlines 1px on any width (plain SVG/CSS, baseline) | Widely supported (MDN `vector-effect`, `paint-order`; not fetched, **unverified here**) |
| **Sparkline** | up/down/flat, last-point dot, no history | **Registered custom property for stroke colour** driven by `--dir` so one rule colours line, dot and area | `@property` as above |
| **Heat-map cell** | value, breadth (see 3.2), no data, selected | **`:has()`:** highlight the whole row/column when any cell inside is focused, no JS | `:has()` in all modern engines since 2023: https://web.dev/articles/5-css-snippets-every-front-end-developer-should-know-in-2024 |
| **Evidence row** (a filing, a print, a headline behind an alert) | source, time, weight, expanded, superseded | **Subgrid:** source / time / text / weight columns align across all rows even when text wraps | Subgrid: Chrome 117, Firefox 71, Safari 16 (secondary): https://web-platform-dx.github.io/web-features-explorer/features/subgrid |
| **Track-record line** ("this kind of alert, n=… hit rate …") | n small, n adequate, no history | **Confidence ribbon** (3.4); technique: `clip-path`/`mask` to fade the ribbon edges to show uncertainty | `clip-path`, `mask`: baseline (**not verified here**) |
| **Gate / limit notice** (a gate closed, a cap reached, kill switch, paper account only) | open, closing soon, closed, overridden | **`:has()` on the panel** to add a coloured top rule and dim the controls when a notice is present | `:has()` as above |
| **Empty / loading / error** | skeleton, first-load, refreshing, partial, failed, retry | **`@starting-style` + `transition-behavior: allow-discrete`** for fade-in of content replacing a skeleton; skeleton keeps the existing pulse | `@starting-style` Baseline 2024 (**not verified in this session**; check MDN before use) |
| **Command palette** | empty query, results, no match, loading | **Native `<dialog>` + `::backdrop`** instead of a custom overlay: free focus trap and Escape, top-layer rendering | `<dialog>` baseline since 2022 (**not verified here**); the repo already has `useDialog.ts`, so keep and test it |
| **Drawer vs popover** | see rule below | **View transitions** for the drawer (a pick morphs into the drawer header); **popover + anchor positioning** for tooltips and glossary terms | View transitions same-document Baseline newly available (Firefox 144): https://web.dev/blog/same-document-view-transitions-are-now-baseline-newly-available ; anchor positioning in all three engines by Firefox 147 (secondary): https://www.youngju.dev/transcribe/culture/2026-05-16-web-standards-2026-container-queries-view-transitions-popover-anchor-positioning-css-nesting-deep-dive.en |

**Drawer vs popover rule.** Popover: one fact, read-only, closes on outside click (glossary term, a number's definition, a candle's detail on touch). Drawer: anything with its own heading, more than one block, or an action (stock detail, an alert's evidence). Dialog (modal): only for a decision that blocks the page (confirm, settings, palette). Pick the smallest.

**Hazards for the kit.** `.panel` has `contain: layout style paint`, which clips anything that is not in the top layer, so popovers and dialogs must use the top layer (`popover`, `<dialog>`), not absolute positioning inside a panel. `body { overflow: hidden }` also clips.

---

## 3. Five signature elements

These are chosen because each encodes something the product already knows, needs no new paid data, and reads at a glance. Ranked by value for effort.

Data names below refer to what the repo already has where I could find it (`lib/marketSession.ts` returns `SessionState = 'pre' | 'open' | 'after' | 'closed'`; candles come from `/radar/candles/{ticker}`; `daily_bars` holds daily closes per the project memory). Anything else is flagged "needs backend".

### 3.1 Range in context bar (price vs 52-week range)

- **Encodes:** where today's price sits between the 52-week low and high, plus where it was 1 day and 1 month ago, so "up 2%" is read against "near the yearly top".
- **Data:** current price, 52-week low/high, optional prior closes. All derivable from daily closes (`daily_bars`, not `tradfi_bars_1d`). Needs a small endpoint or a client calculation over 252 bars. If fewer than 200 bars exist, show "short history" instead of a bar.
- **Accessibility:** a text label always accompanies it ("62% of the way up its 52-week range"); marker shape is a diamond, not a colour; the track uses neutral ink, direction colour only on the 1-day tick.
- **Reduced motion:** the marker moves with a 160 ms transform only if motion is allowed; otherwise it jumps.
- **Sketch (SVG, 14 lines):**
```html
<svg viewBox="0 0 200 22" role="img" aria-label="Price 184.32 is 62% of the way up its 52-week range 121.10 to 199.80">
  <line x1="4" x2="196" y1="11" y2="11" stroke="var(--border-strong)" stroke-width="2" stroke-linecap="round"/>
  <!-- the filled part: low to now -->
  <line x1="4" x2="123" y1="11" y2="11" stroke="var(--text-secondary)" stroke-width="2" stroke-linecap="round"/>
  <!-- 1-month-ago tick -->
  <line x1="108" x2="108" y1="6" y2="16" stroke="var(--text-muted)" stroke-width="1"/>
  <!-- now: diamond -->
  <path d="M123 5 l6 6 -6 6 -6 -6z" fill="var(--up)" stroke="var(--bg-raised)" stroke-width="1.5"/>
  <text x="4" y="21" font-size="8" fill="var(--text-muted)" class="num">121.10</text>
  <text x="196" y="21" font-size="8" text-anchor="end" fill="var(--text-muted)" class="num">199.80</text>
</svg>
```
x is `4 + 192 × (price − low) / (high − low)`. Choose `--up` or `--down` for the diamond by the day's change.

### 3.2 Sector heat map with breadth

- **Encodes:** how each sector moved today (cell colour = direction and size of move) and **how many of its members agree** (breadth, as a small bar inside the cell). A sector up 1% on 20% of members is a different story from 1% on 90%.
- **Data:** per-sector average change and share of members up. Needs sector membership (likely exists in the correlation or universe tables; check) and daily changes; breadth needs member-level changes. If missing, ship the cell without the breadth bar and mark it "needs backend".
- **Accessibility:** colour is **never** the only channel: each cell prints the signed value; size of move is a luminance step (3 steps), not hue alone; the direction colours are the blue/orange pair. Sequential scale advice: perceptually uniform maps such as viridis are colour-blind safe for ordered data (secondary, **weak**): https://www.cs.purdue.edu/CS49000-VIZ/slides/12.Colors.pdf ; diverging data is better served by a ColorBrewer-style diverging scheme (same source family). I have no authoritative page on Finviz's own design; do not copy it from memory.
- **Reduced motion:** cells re-colour with a 120 ms opacity cross-fade at most; under reduced motion no transition.
- **Sketch (CSS grid + one cell, 12 lines):**
```css
.heat { display:grid; grid-template-columns:repeat(auto-fill,minmax(7.5rem,1fr)); gap:2px; }
.heat-cell { --m: 0;               /* move, set inline from -3..+3 clamp */
  --dir: var(--up); position:relative; padding:.5rem; min-height:4rem;
  background: color-mix(in oklch, var(--dir) calc(abs(var(--m)) * 22%), var(--bg-raised)); }
.heat-cell[data-dir="down"] { --dir: var(--down); }
.heat-cell:has(:focus-visible) { outline:2px solid var(--accent); }
.heat-cell .breadth { position:absolute; inset:auto .5rem .4rem .5rem; height:3px;
  background: linear-gradient(90deg, var(--text-primary) calc(var(--b) * 1%), var(--border-subtle) 0); }
```
Compute the percentage in JS if `abs()` in `calc()` is not available (CSS `abs()` is newer; **check support before relying on it**, I did not verify it).

### 3.3 Session clock ring

- **Encodes:** where the US market day is: pre-market, open (with how much of the 6.5 hours have passed), after-hours, closed, plus time to next change. It explains stale prices and why the numbers are not moving.
- **Data:** `marketSession(now)` exists and returns the state; it needs time to the next boundary and the open/close times (derive from the same module; holidays need a calendar, flag that as not covered).
- **Accessibility:** the ring has a text twin ("Market open, 2 h 14 min left") in an `aria-live="polite"` region updated at most once a minute; the state is also a word and an icon, not just an arc colour. Arc colour uses status tokens (`--positive` open, `--caution` extended hours, `--text-muted` closed).
- **Reduced motion:** no sweeping animation at all; the arc is static and redrawn once a minute. A radar-sweep style animation is deliberately not used (it is decoration).
- **Sketch (SVG, 12 lines):**
```html
<svg viewBox="0 0 36 36" width="28" height="28" role="img" aria-label="Market open, 2 hours 14 minutes left">
  <circle cx="18" cy="18" r="15" fill="none" stroke="var(--border-strong)" stroke-width="3"/>
  <!-- day arc: pre 4:00-9:30, regular 9:30-16:00, after 16:00-20:00; one circle per segment -->
  <circle cx="18" cy="18" r="15" fill="none" stroke="var(--positive)" stroke-width="3"
          pathLength="100" stroke-dasharray="41 59" stroke-dashoffset="-21" transform="rotate(-90 18 18)"/>
  <!-- now marker, angle = 360 * (minutes since 04:00 / 960) -->
  <line x1="18" y1="1.5" x2="18" y2="6" stroke="var(--text-primary)" stroke-width="2" transform="rotate(137 18 18)"/>
</svg>
```
`pathLength="100"` makes dash values percentages of the day (960 minutes from 04:00 to 20:00 Eastern); regular session is 6.5/16 ≈ 41% starting at 5.5/16 ≈ 34% in, so the dash offset above is illustrative; compute it from `marketSession()` rather than hard-coding.

### 3.4 Confidence ribbon that shows sample size

- **Encodes:** a track-record number ("hit rate 58%") together with **how much evidence stands behind it**. The ribbon is the plausible range; its width shrinks as n grows. Prevents a 3-of-4 record from looking like 75% certainty.
- **Data:** hits and total `n` per alert type or strategy. Compute an interval client-side with a Wilson interval (pure function in `shared/utils`-style `lib/`), no new backend. Show "too few to judge" below a floor (the repo's pre-registration rules should set it; use their number, not mine).
- **Why this is honest:** research on uncertainty displays finds that designs emphasising the mean make readers overlook the uncertainty and treat distance as effect size (Kale, Kay, Hullman): https://mucollective.northwestern.edu/files/2020-Visualizing-Inferential-Uncertainty.pdf ; and that interval width shrinks with n while the data's spread does not, which readers misjudge (Hofman, Goldstein, Hullman, 2020; same lab page: https://www.hullmanlab.northwestern.edu/paper/2020/06/01/vis-effect-size-judgements.html). Practical consequence: print n next to the interval ("n=41") and do not let the point estimate dominate. Evidence is from lab studies of effect sizes, not trading UIs; applying it here is my inference.
- **Accessibility:** text always reads "58% (range 44–71%, 41 cases)"; the ribbon has a pattern (hatched fill) when n is below the floor, so it is not colour-only; contrast ≥3:1 against the track.
- **Reduced motion:** none needed; no animation.
- **Sketch (SVG, 11 lines):**
```html
<svg viewBox="0 0 200 18" role="img" aria-label="Hit rate 58 percent, plausible range 44 to 71 percent, 41 cases">
  <defs><pattern id="thin" width="4" height="4" patternUnits="userSpaceOnUse" patternTransform="rotate(45)">
    <line x1="0" y1="0" x2="0" y2="4" stroke="var(--text-muted)" stroke-width="1"/></pattern></defs>
  <rect x="2" y="7" width="196" height="4" rx="2" fill="var(--border-subtle)"/>          <!-- 0..100% -->
  <rect x="90" y="5" width="54" height="8" rx="2" fill="var(--accent-dim)" stroke="var(--border-accent)"/>
  <!-- use fill="url(#thin)" instead when n is below the floor -->
  <line x1="119" x2="119" y1="3" y2="15" stroke="var(--text-primary)" stroke-width="2"/>  <!-- point estimate -->
  <line x1="100" x2="100" y1="2" y2="16" stroke="var(--text-muted)" stroke-dasharray="2 2"/> <!-- 50% reference -->
  <text x="198" y="17" text-anchor="end" font-size="8" fill="var(--text-muted)" class="num">n=41</text>
</svg>
```
x positions come from `2 + 196 × rate`. Use the accent for the ribbon (it is not direction and not status).

### 3.5 Pulse strip for market mood

- **Encodes:** a single row of small bars, one per recent interval (for example the last 60 minutes or the last 20 sessions), height = breadth of advancing vs declining, colour = direction pair. It shows whether the day is broadening or fading, not only the index level.
- **Data:** advancing/declining counts per interval. Not obviously in the repo (a `MarketMood.tsx` component exists in `components/ui/`; read it first to see what it uses). If only a single reading exists, show a history-less gauge and mark the strip "needs backend history".
- **Accessibility:** the latest value is printed ("62% of stocks up now"); bars use height as the primary channel, colour second; a visually hidden table lists the values; a `title` per bar.
- **Reduced motion:** new bars append without sliding; the strip redraws once per interval.
- **Sketch (CSS, 8 lines):**
```css
.pulse { display:flex; align-items:end; gap:2px; height:24px; }
.pulse > i { flex:1; min-width:2px; height:calc(var(--h) * 1%);        /* 0..100 */
  background: color-mix(in oklch, var(--dir) 70%, var(--bg-raised)); border-radius:1px; }
.pulse > i[data-dir="up"]   { --dir: var(--up); }
.pulse > i[data-dir="down"] { --dir: var(--down); }
.pulse > i:last-child { outline:1px solid var(--text-primary); outline-offset:1px; }
```

**Not recommended (to avoid gimmicks):** animated radar sweeps, glowing gauges, 3D charts, particle backgrounds, and a "neon" dark mode. They add no data and the repo has already retired them.

---

## 4. Motion policy

Motion is allowed only when it **names a change in the data or in where you are**. Otherwise things are still.

| Class | Allowed | Duration / easing | Reduced motion |
|---|---|---|---|
| Tick (live figure changed) | Tint behind the figure, direction glyph fades in | 600 ms, ease-out, max one per figure per 500 ms | Tint stays as a 150 ms opacity fade or just the glyph; no movement |
| Place change (select a stock, open drawer) | View transition or 160–220 ms slide/fade | 160–220 ms, `cubic-bezier(0.2,0.8,0.2,1)` (existing) | No animation; instant swap |
| Loading | Skeleton pulse (existing) | 1.6 s loop, opacity only | Static skeleton, no pulse |
| Hover/focus | Colour and border only | 120–150 ms | Instant |
| Data arriving (new bar, new row) | Fade in only, no slide | 120 ms | Instant |
| Forbidden | Parallax, infinite decorative loops, bouncing, spinning radar, auto-scrolling tickers | n/a | n/a |

Duration bands vary by source and none is official: small elements 100–150 ms, medium 200–300 ms, large 300–500 ms **(weak, community and design-system sites)**: https://designhubs.pages.dev/motion/mo04-duration . Guidance from the same family of sources: reduced motion means "less", not necessarily "none"; remove spatial movement and keep short fades; avoid a blanket `*` rule that also kills spinners and progress bars. The repo's current global rule collapses *all* animations to 0.01 ms, which also kills the skeleton pulse and loading cue; consider scoping it so loading indicators fall back to a static state rather than disappearing (a design choice, not an accessibility requirement I can cite). Price-tick tints are not "flashing" in the seizure sense at this duration and rate; the three-flashes rule (WCAG 2.3.1) was not fetched here, so check it before shipping anything faster. WCAG 2.3.3 (interaction animation) is AAA and concerns motion triggered by the user: https://w3c.github.io/wcag/understanding/animation-from-interactions

Implementation rule: one set of tokens: `--dur-fast: 120ms; --dur-base: 180ms; --dur-slow: 240ms; --ease-out: cubic-bezier(.2,.8,.2,1);` and a single `@media (prefers-reduced-motion: reduce)` block that sets them to `0ms`, with spinners and skeletons explicitly exempted to a static look.

---

## 5. Migration plan (guard-test-safe)

### 5.1 Guard tests and the edit each needs

| Test | What it enforces | Overhaul impact | Deliberate edit |
|---|---|---|---|
| `frontend/src/__tests__/color_tokens.test.ts` | No raw Tailwind hue classes or `text/bg/border-white` in any `.tsx` | New classes are role tokens (`text-up`, `bg-down-dim`) | None, but add `--color-up`, `--color-down`, `--color-up-dim`, `--color-down-dim` to `@theme inline` first so the classes exist |
| `frontend/src/__tests__/theme.test.ts` | Every token in the first dark `:root { /* Surfaces` block exists in the light block; boot script parses | New tokens (`--up`, `--down`, `--pad`, `--row-h`, `--gap`, `--dur-*`) must be in **both** blocks | Add each to both in the same commit. Density tokens that do not change by theme still need declaring in both blocks or be defined in a **third** `:root` block that the regex does not read (preferred: put density and motion tokens in a separate `:root` block *after* the light block; the test only parses the two named blocks) |
| `tests/test_frontend_shell_and_swarm_api.py` (`.glass-panel` slice, `.glow-cyan,` block, `prefers-reduced-motion`, `:focus-visible`+`outline`, `tabular-nums`) | Retired blur/glow; motion, focus and tabular numerals present | Do not merge `.panel` into `.glass-panel`; keep the retired classes in place | None. Never reorder `.glass-panel` before `.glass-panel-hover` |
| `tests/test_frontend_enterprise_surface.py` | No zone-less date/time formatting; locale on `toLocaleString` | Any new number or clock helper must pass a locale and zone | None; route new formatting through `lib/format.ts` and the time-zone context |
| `tests/test_frontend_icon_vocabulary.py` | Icon vocabulary; no empty `<button>` | New icon-only buttons need labels; new glyph icons (▲▼ are text, not icons) | If a new icon is added to `icons.tsx`, extend the vocabulary list deliberately with a comment |
| `button_input.test.tsx`, `enterprise_surface.test.tsx` | Button/Input recipes and states | Changing `lib/ui.ts` recipes (radius, density) can break snapshots | Update expected class strings, with a comment saying the overhaul changed them |
| `sidebar_roles.test.tsx`, `component_rendering.test.tsx`, `ui_states.test.tsx`, `datagrid.test.tsx`, `flash.test.ts`, `desk_figures.test.ts`, `candles.test.ts`, `heroPrice.test.ts`, `audit_trail.test.ts` | Markup roles, rendering, states, flash, desk figures, candle maths | Not read in full by me | Run them after each step; if a class or role string changes, update the assertion deliberately |

New guard tests to add (so the overhaul cannot regress): (a) direction tokens only in a whitelist of classes; (b) every `--up/--down` pair has ≥3:1 against `--bg-raised` in both themes (a pure function over the CSS values); (c) every colour-bearing SVG element has a text equivalent (extend the existing a11y tests).

### 5.2 Safe order

1. **Tokens only (no visible change):** add `--up/--down/--*-dim`, density and motion tokens, `@theme inline` aliases, `PALETTE_HEX`/`CSS_VAR` entries in `palette.ts`. Add the contrast test. Run vitest and the two Python guard files.
2. **Primitives:** `.num`, `.num small`, `.pill`, `.panel` container query, `Button`/`Input` density, `Disclosure`, `EmptyState`/`Skeleton`/`PanelError` states, `Term` popover. One PR each, with the guard tests green before the next.
3. **Chart kit:** candles to `--up/--down` (hollow up already done), sparkline, heat cell, ribbon, range bar as shared SVG components in `components/ui/`, each with a pure-function test of its geometry in `lib/`.
4. **Signature elements:** range bar on the price header, ribbon on the track-record line, session ring in the header, then the heat map and pulse strip (the last two may need backend data).
5. **Screens:** Desk first (it already composes the pieces), then Today, Trade ideas, stock drawer, then the remaining panels. Density toggle ships last.
6. **Clean-up:** remove unused compatibility classes only after a repo-wide grep proves zero users; the guard test pins `.glass-panel` and `.glow-*`, so leave them.

Use a feature flag (`data-palette`, `data-density` attributes) so each step can ship dark-launched and be reverted by changing an attribute.

---

## 6. Risks and performance budget

**Risks**
- **Palette change surprises users.** Traders expect red/green. Mitigation: the setting, and a one-time note on first load. Evidence for blue/orange is design guidance (Datawrapper), not a trading usability study.
- **Direction orange vs status amber confusion.** Measured ΔE is about 43 between `#f97316` and `#fbbf24`. Mitigation: status always in pills with words; direction always with sign and glyph.
- **Browser gaps.** Anchor positioning and `field-sizing` are newly available (Firefox 147 and 152 respectively per the sources above); view transitions need Firefox 144. All must degrade to an instant swap or a plain positioned element. `text-wrap: pretty` has no Firefox support: https://chromium.googlesource.com/external/github.com/mdn/browser-compat-data/+/HEAD/css/properties/text-wrap.json
- **Tailwind 4 theming.** `@theme inline` with `data-theme` works today; community posts warn of issues with multiple themes, which are **weak** sources: https://skills.cat/skills/baotoq/agent-skills/tailwind-patterns . Do not restructure it while adding tokens.
- **Clipping.** `contain: layout style paint` on `.panel` and `overflow: hidden` on `body` clip non-top-layer overlays.
- **Data dependence.** Heat-map breadth and the pulse strip need member-level and interval history that I could not confirm exists. Ship the element without breadth or history first, rather than inventing numbers.
- **Over-design.** Five signature elements on one screen is a gimmick. Budget: at most two per screen.

**Performance budget (targets I propose; no external source)**
- Fonts: two families, three files total, `display: swap`, latin subset; under 100 KB transferred.
- CSS: global stylesheet growth under 8 KB gzipped for this overhaul.
- JS: no new runtime dependency (free-data and no-new-packages rules); sparkline, heat map, ribbon, ring are hand-written SVG, each under 3 KB.
- Rendering: under 400 SVG nodes per chart; heat map under 150 cells; ticks batched to one paint per 250 ms; `container-type` on panels measured on the Desk page before rollout; View Transitions only on user picks, never on the live tape.
- Animation: only `opacity`, `transform` and a registered colour property; no layout-affecting animation; at most 10 simultaneous tick flashes.
- Check with the browser pane's performance trace on the Desk page before and after steps 2, 3 and 5; revert any step that regresses interaction latency noticeably.

**Evidence gaps to close before building**
1. Confirm what `MarketMood.tsx`, `lib/marketSession.ts` and the sector/universe tables actually provide (I read only export names).
2. Check MDN for `abs()` in CSS `calc()`, `@starting-style`, `<dialog>`, `vector-effect`, `paint-order` (not verified here).
3. Run the new up/down pair through a real colour-blindness simulator on screenshots of the actual UI, including the heat map at three intensity steps.
4. Read the four unread frontend tests listed in 5.1 before editing anything they cover.
