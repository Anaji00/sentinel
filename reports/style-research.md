# Sentinel dashboard: visual style research (12 Nov 2026)

Scope: a ranked plan to make the dashboard look distinctive and professionally designed using modern CSS that fits our stack (Next.js 16, React 19, Tailwind 4, SVG charts, dark and light themes). Read-only research; no code, tests or configs were changed. Companion to `reports/frontend-research.md` (usability).

How to read the sources: every claim has a URL. Where the search returned a mirror or a secondary write-up instead of MDN itself, I say so. Browser versions below are as reported by those sources and should be re-checked on MDN or Can I Use before relying on an exact number.

---

## 1. What we already have (audit)

**Already good, keep:**
- `globals.css` has a real token system: surfaces by elevation (`--bg-base/raised/overlay/inset`), three border strengths, one accent, severity colours, light theme as a full redefinition under `:root[data-theme="light"]`. Glow and backdrop blur are already retired (guarded by tests).
- `palette.ts` hands charts `var(--token, hex)` so SVG follows the theme. `ui.ts` centralises Button and Input recipes. Focus ring, reduced-motion, tabular figures, `.flash-up/.flash-down` (via `color-mix`) and `@container` (in `TickerHero`, `TradeIdeas`) already exist.
- `CandleChart.tsx` uses hairline gridlines (0.5px), a dashed last-price line, a price tag on the axis, and a crosshair. Colours come from `text-positive/negative` through `currentColor`.

**Inconsistent or unused:**
1. Only one typeface is loaded (`Inter` via `next/font`, `layout.tsx:9`). Mono is a system stack (`ui-monospace, "SF Mono", "JetBrains Mono"...`), so figures look different on every machine; JetBrains Mono is named but never loaded.
2. `--shadow-panel` is defined and aliased but `.panel` does not use it; most panels are flat. Fine, but it is two systems where one would do.
3. `.panel` and `.glass-panel` are near-duplicates (the second exists only as a compatibility alias, and a test pins it).
4. The `--color-cyan-*` scale is hard-coded hex (`globals.css:57-66`) and does not change in the light theme, so the 1,200 "cyan" class names stay dark-theme periwinkle in light mode. This is the biggest visual inconsistency I found.
5. Up/down colour is the same pair as severity (`--positive/--negative`). A rising price and a "good" status share one meaning slot. Candles are colour-only (filled either way), which fails colour-blind use.
6. Two hexes live in two places (`PALETTE_HEX` in `palette.ts` mirrors `:root`). Acceptable as a fallback, but any new token must be added in both.
7. Almost no modern CSS in use: grep finds `@container` twice, no `:has()`, `@property`, `oklch`, `light-dark`, `view-transition`, `text-wrap`, `popover`, `anchor-name`.
8. `body { overflow: hidden }` plus `font-feature-settings: "cv11", "ss01"` (single-storey a and open digits) is set globally; this fights with `tabular-nums` in some digit styles and is not documented.
9. No density control; text steps are fixed (11/12/14/18/24/34 px).

**Guard tests that constrain styling (recommendations below respect them):**
- `frontend/src/__tests__/color_tokens.test.ts`: no raw Tailwind hue class (`text-emerald-400`, `bg-sky-500/20`...) and no `text-white/bg-white/border-white` in any `.tsx`. New classes must be role tokens.
- `frontend/src/__tests__/theme.test.ts`: parses the dark block (`:root { /* Surfaces` up to the first `}`) and the `:root[data-theme="light"]` block; every `--token:` in dark must also be in light (except `--radius`, `--font-mono`). The boot script and `THEME_KEY` are tested.
- `tests/test_frontend_shell_and_swarm_api.py:301-328`: `.glass-panel {` must precede `.glass-panel-hover` with no `backdrop-filter` between; `.glow-cyan,` block must contain `box-shadow: none` within 220 characters; file must contain `prefers-reduced-motion`, `:focus-visible` plus `outline`, and `tabular-nums`.
- `tests/test_frontend_enterprise_surface.py`: no zone-less `toLocaleTimeString()`/`toLocaleDateString()`, no `toLocaleString()` without an explicit locale (affects any number-format change).
- `tests/test_frontend_icon_vocabulary.py`: empty `<button>` ban, icon vocabulary. `button_input.test.tsx` and `enterprise_surface.test.tsx` cover Button/Input (raw `<button>` use).
- Not read in full (check before the build): `component_rendering.test.tsx`, `sidebar_roles.test.tsx`, `flash.test.ts`, `ui_states.test.tsx`.

---

## 2. Ranked plan (top ten first)

Effort: S under half a day, M about a day, L several days. "Support" is as reported by the cited source.

| # | Technique | Why it helps this product | Evidence | Support | Where to apply | Effort |
|---|---|---|---|---|---|---|
| 1 | **Typeface pairing + figure system**: load a real mono for figures and tickers, keep Inter for prose, turn on `tabular-nums slashed-zero` for all numerics | Numbers are the product. Right-aligned, equal-width digits make columns comparable and give an instantly recognisable look. Today mono is a system fallback, so figures differ per machine | Decimal alignment is "the most important typographic rule for trading data" (a community design file, weak source): https://claudskills.com/skills/trading-design/SKILL.md . `tabular-nums` widely available since 2022: https://web-platform-dx.github.io/web-features-explorer/features/font-variant-numeric . Inter defaults to proportional digits, has `tnum`, `zero`: https://rsms.me/inter/ | Baseline widely available | `layout.tsx:9` add a second `next/font/google` family (JetBrains Mono or IBM Plex Mono, both OFL; licence not verified in my search) as `--font-jb`; `globals.css:165` `--font-mono`; `.stat-value`, `.symbol` | M |
| 2 | **Directional colours separate from severity, colour-blind safe candles**: new `--up`/`--down` tokens, hollow-vs-filled bodies as a second cue, optional blue/orange palette switch | Today "price up" and "healthy" share green. Red/green alone fails for colour-blind users and fails WCAG 1.4.1 (colour as sole signal) | Blue with orange is the safest pair; avoid green with orange or same-lightness blue: https://www.datawrapper.de/blog/colorblindness-part2 . WCAG 1.4.11 applies to chart elements, 3:1: https://w3.org/WAI/WCAG22/Understanding/non-text-contrast . TradingView community blue/orange themes exist (weak, user scripts): https://my.tradingview.com/script/WZDZUkz4-Candlestick-Themes-NYSE-Pro-GPXalgo | CSS only; no support issue | `globals.css` add `--up/--down` to dark AND light blocks (theme test); `palette.ts` add keys and `directionColor()`; `CandleChart.tsx:173-176` (fill hollow when close<open) | M |
| 3 | **Derive the accent scale with `color-mix()` / OKLCH instead of 10 hard-coded hexes** | Fixes the light-theme "cyan" scale that never changes, and means one accent edit recolours everything. Makes the single signature accent real | `color-mix()` Chrome 111, Firefox 113, Safari 16.2 (secondary): https://developer.chrome.com/blog/css-color-mix ; MDN: https://developer.mozilla.org/docs/Web/CSS/color_value/color-mix . Linear derives whole themes from base, accent and contrast in LCH rather than 98 hand-set variables: https://linear.app/now/how-we-redesigned-the-linear-ui | `color-mix`, `oklch()` baseline since 2023 | `globals.css:57-66` | S |
| 4 | **Hairlines and surface steps, not shadows**: one rule for elevation (border + one surface step), drop `--shadow-panel` from panels, add a 1px inner top highlight on raised surfaces | Gives a precise, instrument-like look and removes a second elevation system. Already the stated intent in the file header | Linear's redesign reduced visual noise and tightened alignment: https://linear.app/now/how-we-redesigned-the-linear-ui ; trading guide: colour reserved for meaning so nothing competes (secondary): https://adminlte.io/blog/trading-dashboard-design-examples/ | Plain CSS | `.panel`, `.panel-header`, `globals.css:237-262` | S |
| 5 | **Container queries for panel layout** (panels respond to their own width, not the screen) | The desk has three panes whose widths change independently; media queries cannot express that | Baseline widely available since 2025-08-14, Chrome 105 / Firefox 110: https://web-platform-dx.github.io/web-features-explorer/features/container-queries . Tailwind 4 already ships `@container` variants and we use them in `TickerHero`, `TradeIdeas` | Widely available | `Desk.tsx` panes, `.panel` (add `container-type: inline-size`), `MetricCard`, `StatTile` | M |
| 6 | **Price-tick motion with meaning**: keep the existing flash, add a direction glyph (▲/▼) so it is not colour-only, shorten to ~600 ms, cap per second, stop under reduced motion | Motion that carries information is the one allowed motion; the glyph makes it accessible | WCAG 2.3.3 is AAA and about interaction-triggered motion; a server price push likely falls outside it (my inference, flagged in source search): https://w3c.github.io/wcag/understanding/animation-from-interactions . Reduced motion is the standard control. Flashing limits are 2.3.1 (not fetched) | Plain CSS | `globals.css:443-452`, `useFlash.ts`, `LiveTape.tsx`, `MarketCharts.tsx` | S |
| 7 | **`text-wrap: balance` on titles, `pretty` on prose** | No one-word last lines on idea cards, headlines and alert text; costs nothing | `balance` Chrome 114 / Firefox 121; `pretty` Chrome 117, no Firefox (secondary): https://chromium.googlesource.com/external/github.com/mdn/browser-compat-data/+/HEAD/css/properties/text-wrap.json . Treat `pretty` as enhancement: https://savvy.co.il/en/blog/css/css-text-wrap-pretty-balance/ (weak) | `balance` broad; `pretty` partial | `globals.css` base: `h1,h2,h3,.panel-title{...}`; `Headline.tsx`, `IdeaCard.tsx` | S |
| 8 | **Density control** (compact / comfortable) through custom properties on `<html data-density>` | Traders differ; ordinary users want air, desks want rows. Also removes ad-hoc padding | Density as a design identity in trading UIs (secondary): https://claudskills.com/skills/trading-design/SKILL.md . Our own text-step system already centralises sizes (`globals.css:143-161`) | Plain CSS | `globals.css` `--pad`, `--row-h`; `DataGrid.tsx`, `.panel-header`; toggle beside `ThemeToggle.tsx` | M |
| 9 | **`prefers-contrast: more` and `forced-colors`** overrides for borders, chart lines and the accent | The subtle border is 13% alpha; low-vision users lose panel edges. Chart gridlines and hover crosshair need 3:1 | `prefers-contrast` baseline widely available since 2024-11-30: https://web-platform-dx.github.io/web-features-explorer/features/prefers-contrast . MDN: https://developer.mozilla.org/en-US/docs/Web/CSS/@media/prefers-contrast . Caveat: it overlaps `forced-colors` on Windows: https://kilianvalkhof.com/2023/css-html/i-no-longer-understand-prefers-contrast/ | Widely available | end of `globals.css`; both theme blocks | S |
| 10 | **View transitions for the desk pane and drawer** (`view-transition-name` on the picked stock, `startViewTransition` in the click handler) | A place that arrives, not a jump; polish that reads as deliberately designed | Same-document baseline newly available; Firefox 144: https://web.dev/blog/same-document-view-transitions-are-now-baseline-newly-available . Guide: https://developer.chrome.com/docs/web-platform/view-transitions/same-document | Baseline newly available (Oct 2025) | `Desk.tsx` selection; `StockDrawer` (replaces `.drawer-in`); wrap in a feature check | M |

### Further items (below the cut)

| # | Technique | Note | Evidence | Support | Where | Effort |
|---|---|---|---|---|---|---|
| 11 | `light-dark()` for token pairs | Halves the dark/light duplication, but conflicts with `theme.test.ts` (it parses two separate blocks) and with `@theme inline`. Do only with a deliberate test update | https://developer.mozilla.org/docs/Web/CSS/color_value/light-dark (needs `color-scheme: light dark`, per https://www.stefanjudis.com/blog/light-dark-isnt-the-same-as-prefers-color-scheme) | Chrome 123, Firefox 120, Safari 17.5 (mirror) | `:root` blocks | M |
| 12 | `@property` for an animatable flash alpha or number tween | Lets one keyframe animate `--flash-a` smoothly; otherwise not needed | https://web.dev/blog/at-property-baseline | Baseline 2024 | `globals.css:443-452` | S |
| 13 | `:has()` for parent-aware states (row highlight when a checkbox or pressed child exists; panel with error shows red top rule) | Removes JS state classes | https://web.dev/articles/5-css-snippets-every-front-end-developer-should-know-in-2024 | All modern engines since 2023 | `DataGrid.tsx`, `PanelError` | S |
| 14 | Subgrid for aligned card rows (idea cards with a varying number of lines) | Aligns label/figure baselines across a row | https://web-platform-dx.github.io/web-features-explorer/features/subgrid | Chrome 117, Firefox 71, Safari 16 | `TradeIdeas.tsx`, `IdeaCard.tsx` | M |
| 15 | Popover + anchor positioning for chart tooltips and glossary terms (`Term.tsx`) | Top-layer rendering avoids clipping; but `.panel` has `contain: layout style paint`, which clips anything not in the top layer. Chart crosshair readout is SVG and does not need it | Popover Baseline 2024; anchor positioning in all three engines by Firefox 147, Jan 2026 (secondary): https://www.youngju.dev/transcribe/culture/2026-05-16-web-standards-2026-container-queries-view-transitions-popover-anchor-positioning-css-nesting-deep-dive.en | Newly available | `Term.tsx`, `CommandPalette`, dialogs (`useDialog.ts`) | L |
| 16 | Cascade layers | Already in use (`@layer base/components/utilities`). Keep; do not add more. A source warns that `:root` inside `@layer base` can bite in Tailwind 4 (community, weak): https://skills.cat/skills/baotoq/agent-skills/tailwind-patterns | Baseline since 2022 (mirror): https://docs.w3cub.com/css/@layer | Widely available | n/a | n/a |
| 17 | `field-sizing: content` for the ask-a-question box and note fields | Nice but low value; Firefox only in 152, one source disagrees | https://web.dev/blog/web-platform-06-2026 | Newly available | `Input.tsx` | S |
| 18 | Scroll-driven animations | Skip: Firefox not enabled by default (sources disagree), and our UI needs no scroll-linked effects | https://developer.chrome.com/docs/css-ui/scroll-driven-animations | Partial | none | n/a |

---

## 3. Code sketches for the top ten

**1. Fonts and figures** (`layout.tsx`, `globals.css`)
```css
:root { --font-mono: var(--font-jb), ui-monospace, "SF Mono", Consolas, monospace; }
.stat-value, .symbol, .font-mono, table, [data-numeric] {
  font-variant-numeric: tabular-nums slashed-zero lining-nums;
  font-feature-settings: "zero" 1, "tnum" 1; /* Inter-specific; cv11 and ss01 stay for prose only */
}
```
Drop `cv11`/`ss01` from `body` and keep them on headings only; "open digits" (ss01) inside tables are less distinct from `8`/`0` at 11px. Right-align numeric columns in `DataGrid`.

**2. Direction tokens and hollow candles**
```css
:root { --up: #2dd4bf; --down: #f59e0b; }            /* blue-teal / amber: safe for red-green deficiency */
:root[data-theme="light"] { --up: #0f766e; --down: #b45309; }
[data-palette="classic"] { --up: var(--positive); --down: var(--negative); }
```
In `CandleChart.tsx` render `fill={isUp(b) ? 'var(--bg-raised)' : 'currentColor'}` with a 1px stroke so up bars are hollow. Check each pair at 3:1 against `--bg-raised` (WCAG 1.4.11). Keep `--positive/--negative` for status.

**3. Accent scale from one value**
```css
@theme inline {
  --color-cyan-300: color-mix(in oklch, var(--accent) 70%, white);
  --color-cyan-500: var(--accent);
  --color-cyan-700: color-mix(in oklch, var(--accent) 65%, black);
  --color-cyan-900: color-mix(in oklch, var(--accent) 35%, var(--bg-base));
}
```
Note: `white` and `black` here are CSS keywords inside `color-mix`, not Tailwind classes, so the colour-token test (which scans `.tsx`) is unaffected.

**4. Hairline elevation**
```css
.panel { border: 1px solid var(--border-subtle);
  box-shadow: inset 0 1px 0 color-mix(in srgb, var(--text-primary) 4%, transparent); }
.panel-header { background: color-mix(in srgb, var(--bg-inset) 70%, var(--bg-raised)); }
```

**5. Panels that size themselves**
```css
.panel { container-type: inline-size; } /* keep contain: layout style paint */
@container (width < 420px) { .stat-grid { grid-template-columns: 1fr 1fr; } .panel-subtitle { display: none; } }
```
`container-type: inline-size` already implies layout containment; check it does not change child sizing in `DataGrid` before rolling out everywhere.

**6. Tick flash with a glyph**
```css
.tick-up::before   { content: "▲"; font-size: .7em; margin-inline-end: .25em; color: var(--up); }
.tick-down::before { content: "▼"; font-size: .7em; margin-inline-end: .25em; color: var(--down); }
.flash-up { animation: flash-up 600ms ease-out; }
```
Tick: add the class in `useFlash`; drop it after the animation ends so the glyph is not permanent unless wanted.

**7. Wrapping**
```css
h1, h2, h3, .panel-title { text-wrap: balance; }
.prose p, .brief p, [data-prose] { text-wrap: pretty; }
```

**8. Density**
```css
:root { --pad-y: .75rem; --row-h: 2.25rem; }
:root[data-density="compact"] { --pad-y: .375rem; --row-h: 1.75rem; }
.panel-header { padding-block: var(--pad-y); }
```
Persist the choice like `THEME_KEY`; the boot script must set it before hydration to avoid a flash.

**9. Contrast and forced colours**
```css
@media (prefers-contrast: more) {
  :root { --border-subtle: rgba(160,174,200,.45); --border-strong: rgba(160,174,200,.7); --text-muted: var(--text-secondary); }
}
@media (forced-colors: active) { .panel { border: 1px solid CanvasText; } .badge { forced-color-adjust: none; } }
```
Add the same override inside the light selector so it is not lost on specificity.

**10. View transition on desk pick**
```ts
const go = (t: string) => (document as any).startViewTransition ? document.startViewTransition(() => setPicked(t)) : setPicked(t);
```
```css
::view-transition-old(root), ::view-transition-new(root) { animation-duration: 160ms; }
@media (prefers-reduced-motion: reduce) { ::view-transition-group(*) { animation: none; } }
```
Use `flushSync` inside the callback so React commits before the snapshot. Typing note: avoid `any`, declare a minimal type.

---

## 4. Chart styling notes (SVG)

- **Candle colours:** change up/down to a colour-blind-safe pair and add a second cue (hollow up bars). Source: https://www.datawrapper.de/blog/colorblindness-part2 and WCAG 1.4.1/1.4.11 at https://w3.org/WAI/WCAG22/Understanding/non-text-contrast . Offer the classic red/green behind a setting, since many traders expect it.
- **Gridlines:** already 0.5px on `--border-subtle` (about 13% alpha). That is intentionally quiet but sits below 3:1; gridlines are decorative so exempt, but axis labels and the crosshair are not: the crosshair uses `stroke-ink-dim`, fine. Under `prefers-contrast: more` raise gridline opacity.
- **Crosshair and tooltip:** the chart already pairs a vertical line, a horizontal line with a price tag, and an `aria-live` readout. Next step: put the readout in a fixed strip above the plot (already done) and snap the price tag to the bar's close when hovering. No new CSS needed.
- **Last-price tag:** already present; give it `paint-order: stroke` with a page-colour stroke so text stays readable over candles.
- **Table fallback:** the chart already has an "as table" mode (`asTable`), which satisfies a text alternative.

---

## 5. Signature look (proposal)

Sentinel should read as a **quiet instrument panel with one voice**: Inter for sentences and a true monospace (JetBrains Mono, loaded, not a system fallback) for every figure, ticker and timestamp, so numbers are always the sharpest thing on the screen. All numbers are tabular, lining and slashed-zero, right-aligned in columns, with the unit set small and muted next to the figure. The palette stays near-monochrome slate surfaces separated by 1px hairlines instead of shadows, with a single periwinkle accent meaning only "you can act on this or it is selected"; direction (up/down) gets its own colour-blind-safe pair that is never reused for health or severity, and severity keeps its own three. Motion is limited to three things that carry meaning: a 600 ms tint and arrow when a live figure ticks, a 160 ms view transition when you change the stock you are reading, and a pulse while data is loading; everything else is still, and all of it stops under reduced motion. A compact/comfortable density switch lets the same layout serve a desk and a first-time user.

---

## 6. Risks

**Performance**
- Extra web font: keep one mono weight pair via `next/font` with `display: swap` and `subsets: ['latin']`; use the variable file if it exists. Expect a small LCP cost.
- `color-mix()` in many rules is cheap; but `container-type` on every `.panel` adds layout containment work across ~16 panels. Measure on the desk page before applying to all.
- View transitions snapshot the page; avoid on high-frequency updates, trigger only on user picks.
- Tick flashes: cap per element (for example one per 500 ms) so a fast tape does not strobe (also relevant to WCAG 2.3.1 three flashes; not fetched here).

**Support gaps**
- `text-wrap: pretty` has no Firefox support (source above); `balance` is fine.
- View transitions only landed in Firefox 144 (Oct 2025); scroll-driven animations are not default in Firefox; `field-sizing` is Firefox 152; anchor positioning is "newly available". Use as progressive enhancement or skip.
- Sources disagree on some exact versions (relative colour syntax Firefox 120 vs 128; `field-sizing`; `pretty` in Safari). Verify on MDN and Can I Use before depending on one.
- `Tailwind 4`: the `@theme inline` and multi-theme interplay is reported to cause trouble with data-attribute theming in community posts (weak sources). We already use `@theme inline` with `data-theme` and it works; do not restructure it while changing tokens.

**Tests that need care or a deliberate update**
- `theme.test.ts`: any new token in the dark `:root` block (for example `--up`, `--down`, `--pad-y`) must also be in the light block or the test fails. The regex also relies on the `/* Surfaces` comment in the first `:root` block, and reads up to the first `}`. Do not add nested rules or a `}` early in that block. Using `light-dark()` would break the test's premise: update it on purpose, with a comment.
- `test_frontend_shell_and_swarm_api.py`: keep `.glass-panel {` before `.glass-panel-hover` with no `backdrop-filter`; keep `.glow-cyan,` block with `box-shadow: none` in the first 220 characters; keep the strings `prefers-reduced-motion`, `:focus-visible` plus `outline`, and `tabular-nums`. Merging `.panel` and `.glass-panel` would fail these.
- `color_tokens.test.ts`: no new raw hue classes; use role tokens (`text-up` after adding `--color-up` in `@theme inline`).
- `color_tokens`/`palette`: any new token must be added to `PALETTE_HEX`, `CSS_VAR` and the light block, or charts diverge from CSS.
- Locale guard: any number formatting added for aligned figures must pass an explicit locale (`format.ts`), never `toLocaleString()` bare.
- `flash.test.ts`, `component_rendering.test.tsx`, `sidebar_roles.test.tsx`: not read; check them if tick, panel or sidebar markup changes.
- Existing `body { overflow: hidden }` and `.panel { contain: layout style paint }` will clip any popover or tooltip that is not in the top layer (item 15).

**Order I would build in:** 3, 4, 7, 9 (all S, mostly CSS only), then 1, 2 (visible identity), then 6, 5, 8, then 10.
