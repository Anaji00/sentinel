/**
 * Plain wording for a stock's snapshot (`/stocks/{ticker}/snapshot`): the facts row under the price and the last-twelve-
 * months business figures from SEC filings. A missing figure is left out or marked "not meaningful", never shown as 0.
 */

import { formatNumber } from './format';

export interface Snapshot {
  ticker: string;
  market_cap: number | null;
  market_cap_as_of: string | null;
  range_52w: { low: number | null; high: number | null; from_date: string | null } | null;
  next_earnings: { date: string | null; estimated: boolean | null } | null;
  pe_trailing: { value: number | null; basis: string | null; as_of: string | null } | null;
  metrics: {
    revenue: number | null;
    revenue_growth_pct: number | null;
    gross_margin_pct: number | null;
    operating_margin_pct: number | null;
    net_margin_pct: number | null;
    cash_from_operations_margin_pct: number | null;
    return_on_equity_pct: number | null;
    price_to_sales: number | null;
    price_to_book: number | null;
    basis: string | null;
    as_of: string | null;
  } | null;
}

export interface Fact {
  label: string;
  value: string;
  title?: string;
}

const num = (v: unknown): v is number => typeof v === 'number' && Number.isFinite(v);

/** $5.17T, $480B, $12.4M: a company size or a revenue in words a person reads. */
export function bigMoney(v: number | null | undefined): string | null {
  if (!num(v) || v <= 0) return null;
  const units: [number, string][] = [[1e12, 'T'], [1e9, 'B'], [1e6, 'M']];
  for (const [size, suffix] of units) {
    if (v >= size) return `$${(v / size).toFixed(v / size >= 100 ? 0 : 2)}${suffix}`;
  }
  return `$${formatNumber(Math.round(v), { decimals: 0 })}`;
}

const signedPct = (v: number, digits = 1) => `${v >= 0 ? '+' : '−'}${Math.abs(v).toFixed(digits)}%`;

/** Where a price sits in its 52-week range, 0 at the low and 1 at the high; null when the range or price is unusable. */
export function rangePosition(price: number | null | undefined, low: number | null | undefined, high: number | null | undefined): number | null {
  if (!num(price) || !num(low) || !num(high) || high <= low) return null;
  return Math.min(1, Math.max(0, (price - low) / (high - low)));
}

/** The facts row: size, price to earnings, next earnings date. `day` turns an ISO date into words. */
export function factsRow(s: Snapshot | null | undefined, day: (iso: string) => string): Fact[] {
  if (!s) return [];
  const out: Fact[] = [];
  const cap = bigMoney(s.market_cap);
  if (cap) out.push({ label: 'Company size', value: cap, title: s.market_cap_as_of ? `Market value as of ${day(s.market_cap_as_of)}.` : undefined });
  const pe = s.pe_trailing?.value;
  out.push(
    num(pe) && pe > 0
      ? { label: 'Price to earnings', value: `${pe.toFixed(1)}`, title: `${s.pe_trailing?.basis ?? 'Trailing 12 months'}${s.pe_trailing?.as_of ? `, as of ${day(s.pe_trailing.as_of)}` : ''}.` }
      : { label: 'Price to earnings', value: 'not meaningful', title: 'Shown only when the company made a profit over the last 12 months.' },
  );
  const e = s.next_earnings;
  if (e?.date) out.push({ label: e.estimated ? 'Earnings expected' : 'Next earnings', value: day(e.date), title: e.estimated ? 'An estimate: one quarter after the last results filing, not a listed date.' : 'The date the company listed.' });
  return out;
}

/** A run of text in a sentence; `fig` marks a figure, `title` is its hover explanation. */
export interface SentencePart {
  text: string;
  fig?: boolean;
  title?: string;
}

/**
 * The facts row as one sentence: "A $2.6B company, priced at 28.1 times last year's profit. Next reports Nov 20."
 * A price to earnings that cannot be worked out is said in words, never shown as a number.
 */
export function factsSentence(facts: Fact[]): SentencePart[] {
  const by = (label: string): Fact | undefined => facts.find((f) => f.label === label);
  const size = by('Company size');
  const pe = by('Price to earnings');
  const next = by('Next earnings') ?? by('Earnings expected');
  const out: SentencePart[] = [];
  if (size) out.push({ text: 'A ' }, { text: size.value, fig: true, title: size.title }, { text: ' company' });
  if (pe) {
    const lead = size ? ', ' : '';
    if (pe.value === 'not meaningful') out.push({ text: `${lead}not profitable over the last 12 months`, title: pe.title });
    else if (Number(pe.value) > 200) out.push({ text: `${lead}priced at ` }, { text: 'over 200', fig: true, title: pe.title }, { text: " times last year's profit, which was tiny next to the price" });
    else out.push({ text: `${lead}priced at ` }, { text: pe.value, fig: true, title: pe.title }, { text: " times last year's profit" });
  }
  if (out.length > 0) out.push({ text: '.' });
  if (next) out.push({ text: `${out.length > 0 ? ' ' : ''}${next.label === 'Next earnings' ? 'Next reports ' : 'Expected to report '}` }, { text: next.value, fig: true, title: next.title }, { text: '.' });
  return out;
}

/** The business figures for the last 12 months, in plain labels; a figure that cannot be computed is left out. */
export function businessFacts(s: Snapshot | null | undefined): Fact[] {
  const m = s?.metrics;
  if (!m) return [];
  const out: Fact[] = [];
  const rev = bigMoney(m.revenue);
  if (rev) out.push({ label: 'Sales, last 12 months', value: rev });
  if (num(m.revenue_growth_pct)) out.push({ label: 'Sales growth', value: signedPct(m.revenue_growth_pct), title: 'Against the 12 months before.' });
  if (num(m.gross_margin_pct)) out.push({ label: 'Gross margin', value: `${m.gross_margin_pct.toFixed(1)}%`, title: 'Share of sales left after the direct cost of making the product.' });
  if (num(m.operating_margin_pct)) out.push({ label: 'Operating margin', value: signedPct(m.operating_margin_pct).replace('+', ''), title: 'Share of sales left after running costs, before interest and tax.' });
  if (num(m.net_margin_pct)) out.push({ label: 'Profit margin', value: signedPct(m.net_margin_pct).replace('+', ''), title: 'Share of sales kept as profit.' });
  out.push(num(m.return_on_equity_pct) ? { label: 'Return on equity', value: `${m.return_on_equity_pct.toFixed(1)}%`, title: 'Profit as a share of what shareholders have put in.' } : { label: 'Return on equity', value: 'not meaningful', title: 'Not shown when equity is zero or negative.' });
  if (num(m.price_to_sales)) out.push({ label: 'Price to sales', value: m.price_to_sales.toFixed(1), title: 'Market value divided by 12 months of sales.' });
  out.push(num(m.price_to_book) ? { label: 'Price to book', value: m.price_to_book.toFixed(1), title: 'Market value divided by net assets on the balance sheet.' } : { label: 'Price to book', value: 'not meaningful', title: 'Not shown when net assets are zero or negative.' });
  return out;
}
