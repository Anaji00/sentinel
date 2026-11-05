/**
 * What changed on one stock this week (GET /stocks/{ticker}/changes).
 *
 * Every part arrives with a status. "no_data" means nothing is stored for the stock;
 * "unavailable" means the read failed. Neither is "nothing changed", and the page says
 * which it is. Large-trade sides are inferred and often unknown, so the money with no
 * known side is always shown beside buyer and seller, and no buyer-to-seller ratio is
 * ever computed from the known part alone.
 *
 * Pure: the response in, plain sentences out.
 */

import { formatCompact, formatCurrency, formatNumber } from './format';

export type SectionStatus = 'ok' | 'no_data' | 'unavailable';

export interface PriceChange {
  as_of: string;
  close: number | null;
  change_1d_pct: number | null;
  change_5d_pct: number | null;
}
export interface RankChange {
  as_of: string;
  percentile: number | null;
  percentile_before: number | null;
  note: string;
}
export interface OptionsChange {
  as_of: string;
  atm_iv: number | null;
  atm_iv_before: number | null;
  put_call_ratio: number | null;
}
export interface ShortChange {
  as_of: string;
  short_shares: number | null;
  change_pct: number | null;
  days_to_cover: number | null;
}
export interface FlowSide {
  kind: string;
  buyer_usd: number;
  seller_usd: number;
  unknown_usd: number;
  buyer_trades: number;
  seller_trades: number;
  unknown_trades: number;
  note: string;
}
export interface MacroLink {
  symbol: string;
  name: string;
  direction: 'with' | 'against';
  t_stat: number | null;
  sessions: number | null;
  as_of: string | null;
}
export interface StockEvent {
  kind: string;
  headline: string | null;
  at: string;
  url: string | null;
}
export interface StockChanges {
  ticker: string;
  window_days: number;
  sections: Record<string, SectionStatus>;
  price: PriceChange | null;
  rank: RankChange | null;
  options: OptionsChange | null;
  short_interest: ShortChange | null;
  flow: FlowSide[];
  macro_links: MacroLink[];
  events: StockEvent[];
}

export const changesKey = (ticker: string): string => `/stocks/${encodeURIComponent(ticker)}/changes`;

/** The line for a part that has nothing to show, which says why. Null when the part is fine. */
export function sectionNote(label: string, status: SectionStatus | undefined): { text: string; tone: 'muted' | 'caution' } | null {
  if (status === 'unavailable') return { text: `Could not read ${label} right now.`, tone: 'caution' };
  if (status === 'no_data' || status === undefined) return { text: `No ${label} stored for this stock.`, tone: 'muted' };
  return null;
}

const pct = (n: number, decimals = 1): string => formatNumber(Math.abs(n), { decimals });
const verb = (n: number): string => (n >= 0 ? 'rose' : 'fell');

export function priceLine(p: PriceChange): string {
  const parts: string[] = [];
  if (p.change_1d_pct != null) parts.push(`Last session: ${verb(p.change_1d_pct)} ${pct(p.change_1d_pct)}%.`);
  if (p.change_5d_pct != null) parts.push(`Last 5 sessions: ${verb(p.change_5d_pct)} ${pct(p.change_5d_pct)}%.`);
  if (p.close != null) parts.push(`Closed at ${formatCurrency(p.close, { decimals: 2 })}.`);
  return parts.join(' ');
}

/** The model's rank in everyday words, or null when there is none. The note about its testing is shown beside it. */
export function rankLine(r: RankChange): string | null {
  if (r.percentile == null) return null;
  const now = Math.round(r.percentile * 100);
  let line = `The model ranks it stronger than ${now}% of stocks`;
  if (r.percentile_before != null) {
    const before = Math.round(r.percentile_before * 100);
    line += now > before ? `, up from ${before}% five days earlier` : now < before ? `, down from ${before}% five days earlier` : ', the same as five days earlier';
  }
  return `${line}.`;
}

export function optionsLine(o: OptionsChange): string | null {
  const parts: string[] = [];
  if (o.atm_iv != null) {
    const now = Math.round(o.atm_iv * 100);
    let swing = `Options are pricing a yearly swing of about ${now}%`;
    if (o.atm_iv_before != null) {
      const before = Math.round(o.atm_iv_before * 100);
      swing += now > before ? `, up from ${before}%` : now < before ? `, down from ${before}%` : ', unchanged';
    }
    parts.push(`${swing}.`);
  }
  if (o.put_call_ratio != null) {
    const r = o.put_call_ratio;
    const mix = r < 0.7 ? 'more bets on a rise than a fall' : r > 1 ? 'more bets on a fall than a rise' : 'a fairly even mix';
    parts.push(`${formatNumber(r, { decimals: 2 })} puts traded for every call: ${mix}.`);
  }
  return parts.length ? parts.join(' ') : null;
}

export function shortLine(s: ShortChange): string | null {
  if (s.short_shares == null) return null;
  let line = `Short sellers held ${formatCompact(s.short_shares, { decimals: 0 })} shares`;
  if (s.change_pct != null) line += `, ${pct(s.change_pct)}% ${s.change_pct >= 0 ? 'more' : 'fewer'} than the report before`;
  line += '.';
  if (s.days_to_cover != null) line += ` Buying them back would take about ${formatNumber(s.days_to_cover, { decimals: 1 })} days of trading.`;
  return line;
}

export interface FlowView {
  title: string;
  parts: { label: string; usd: string; trades: number }[];
  /** A sentence when most of the money has no known side; null otherwise. */
  caution: string | null;
  note: string;
}

const FLOW_TITLE: Record<string, string> = { equity_block: 'Large stock trades', options_flow: 'Large option trades' };

/** Buyer, seller and unknown, always all three. The share with no known side is the whole money, not the trade count. */
export function flowView(f: FlowSide): FlowView {
  const total = f.buyer_usd + f.seller_usd + f.unknown_usd;
  const unknownShare = total > 0 ? f.unknown_usd / total : 0;
  return {
    title: FLOW_TITLE[f.kind] ?? 'Large trades',
    parts: [
      { label: 'Buyer-started', usd: formatCurrency(f.buyer_usd), trades: f.buyer_trades },
      { label: 'Seller-started', usd: formatCurrency(f.seller_usd), trades: f.seller_trades },
      { label: 'Side not known', usd: formatCurrency(f.unknown_usd), trades: f.unknown_trades },
    ],
    caution:
      unknownShare >= 0.5
        ? `Most of the money (${Math.round(unknownShare * 100)}%) has no known side, so this does not show which way large traders leaned.`
        : null,
    note: f.note,
  };
}

export function macroLine(m: MacroLink): string {
  const t = Math.abs(m.t_stat ?? 0);
  const strength = t >= 8 ? 'a strong link' : t >= 5 ? 'a clear link' : 'a weaker link';
  return `Tends to move ${m.direction === 'against' ? 'against' : 'with'} ${m.name} (${strength})`;
}

const EVENT_LABEL: Record<string, string> = {
  filing: 'SEC filing',
  insider_trade: 'Insider trade',
  earnings_report: 'Earnings',
  headline: 'News',
};

export function eventKindLabel(kind: string): string {
  if (EVENT_LABEL[kind]) return EVENT_LABEL[kind];
  const words = kind.replace(/_/g, ' ').trim();
  return words ? words.charAt(0).toUpperCase() + words.slice(1) : 'Item';
}

/** A link only when it is a web address: a stored url is data, not something to follow blindly. */
export function safeUrl(url: string | null | undefined): string | null {
  if (!url) return null;
  try {
    const u = new URL(url);
    return u.protocol === 'https:' || u.protocol === 'http:' ? u.href : null;
  } catch {
    return null;
  }
}

/** "Nov 5" for a stored calendar day, in the style the rest of the app uses. Empty for a bad one. */
export function dayLabel(day: string | null | undefined): string {
  if (!day) return '';
  const d = new Date(`${day.slice(0, 10)}T00:00:00Z`);
  if (Number.isNaN(d.getTime())) return '';
  return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', timeZone: 'UTC' });
}
