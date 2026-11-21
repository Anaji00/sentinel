/**
 * Plain wording for three backend figures: a stock's quote (`/quotes`), the track record of past trade ideas
 * (`/predictions/track-record/slice`) and the paper account (`/portfolio/summary`). A missing figure stays missing:
 * nothing here turns null into 0.
 */

export interface Quote {
  ticker: string;
  price: number | null;
  as_of: string | null;
  age_sec: number | null;
  source: 'minute_bar' | 'daily_close' | null;
  session: 'regular' | 'pre_market' | 'after_hours' | 'closed' | 'always_open' | null;
  delayed: boolean | null;
  change_pct: number | null;
  prev_close: number | null;
  change_base_date: string | null;
}

const SESSION_WORDS: Record<string, string> = {
  regular: 'Market open',
  pre_market: 'Pre-market',
  after_hours: 'After hours',
  closed: 'Market closed',
  always_open: 'Trades around the clock',
};

export function sessionWords(s: Quote['session']): string {
  return s ? (SESSION_WORDS[s] ?? '') : '';
}

/** "22:55 New York time", from an ISO instant. */
export function clockNy(iso: string | null): string {
  if (!iso) return '';
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return '';
  return `${d.toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit', timeZone: 'America/New_York' })} New York time`;
}

/** The sentence under the price: what it is, where the change is measured from, when, and whether it is late. */
export function quoteCaption(q: Quote, baseLabel: string | null): string {
  const what = q.source === 'minute_bar' ? 'latest trade' : 'last close';
  const parts = [baseLabel ? `${what}, change from the ${baseLabel} close` : what];
  const session = sessionWords(q.session);
  if (session) parts.push(session);
  const when = clockNy(q.as_of);
  if (when) parts.push(`as of ${when}${q.delayed ? ', delayed' : ''}`);
  return parts.join(' · ');
}

export interface TrackSlice {
  horizon: string;
  direction?: string | null;
  n_calls: number | null;
  n_closed: number | null;
  stopped: number | null;
  /** Open ideas with no price bar yet: counted in the total, but not in the share right. */
  n_open_unmarked?: number | null;
  share_right: number | null;
  mean_net_bps_after_costs: number | null;
  median_net_bps: number | null;
  live?: { n: number | null; since: string | null } | null;
  /** How many different days the ideas were logged on, and the first and last of them. */
  n_dates?: number | null;
  first_date?: string | null;
  last_date?: string | null;
  backtest?: { months: number | null; ic: number | null; net_sharpe_after_fee: number | null; validated: boolean | null } | null;
  outcome_spread?: { p10_bps: number | null; p50_bps: number | null; p90_bps: number | null; n: number | null } | null;
}

/** The track-record horizon for an idea held for `days` trading days. */
export function sliceHorizon(days: number): 'daily' | 'weekly' | 'monthly' {
  return days <= 2 ? 'daily' : days <= 8 ? 'weekly' : 'monthly';
}

const pctOf = (bps: number) => `${bps >= 0 ? '+' : '−'}${Math.abs(bps / 100).toFixed(1)}%`;

/** One honest line about past ideas, with the sample size and start date always beside any rate; null when there is none to show. */
export function trackLine(s: TrackSlice | null | undefined, since: (iso: string) => string): { headline: string; detail: string } | null {
  const n = s?.n_calls;
  if (!s || n == null || n <= 0) return null;
  // A record from only a handful of days is one episode, not a track record: say so with the dates.
  const few = s.n_dates != null && s.n_dates <= 12 && s.first_date && s.last_date;
  const from = few ? `, from only ${s.n_dates} days (${since(s.first_date as string)} to ${since(s.last_date as string)})` : s.live?.since ? ` since ${since(s.live.since)}` : '';
  const small = n < 30;
  const right = s.share_right == null || small ? `too few to judge (${n} so far${from})` : `right ${Math.round(s.share_right * 100)}% of the time over ${n} ideas${from}`;
  const spread = s.outcome_spread;
  const range =
    spread && spread.p10_bps != null && spread.p90_bps != null && spread.p50_bps != null
      ? `Typical result ${pctOf(spread.p50_bps)}, with the middle 80% between ${pctOf(spread.p10_bps)} and ${pctOf(spread.p90_bps)}.`
      : '';
  const stopped = s.stopped != null && s.n_closed ? ` ${s.stopped} of ${s.n_closed} finished ideas were stopped out.` : '';
  const unmarked = s.n_open_unmarked ? ` ${s.n_open_unmarked} open idea${s.n_open_unmarked === 1 ? ' has' : 's have'} no price yet and ${s.n_open_unmarked === 1 ? 'is' : 'are'} not in the share right.` : '';
  return {
    headline: `Past ${s.horizon} ideas${s.direction ? ` (${s.direction})` : ''}: ${right}`,
    detail: `${range}${stopped}${unmarked} Paper trades after costs; ideas still open are counted at the latest close.`.trim(),
  };
}

export interface AccountSummary {
  equity: number | null;
  day_change: number | null;
  day_change_pct: number | null;
  base_date: string | null;
  positions_count: number | null;
  as_of: string | null;
}

/** The paper account for the header: its value and its change since the last record, with the date of that record. */
export function accountChip(a: AccountSummary | null | undefined, money: (v: number) => string, since: (iso: string) => string): { text: string; tone: 'positive' | 'negative' | 'neutral'; title: string } | null {
  if (!a || a.equity == null) return null;
  const pct = a.day_change_pct;
  const tone = pct == null || pct === 0 ? 'neutral' : pct > 0 ? 'positive' : 'negative';
  const change = pct == null ? '' : ` ${pct >= 0 ? '+' : '−'}${Math.abs(pct).toFixed(2)}%`;
  const base = a.base_date ? ` Change is since the ${since(a.base_date)} record.` : '';
  const pos = a.positions_count != null ? ` ${a.positions_count} positions.` : '';
  return { text: `${money(a.equity)}${change}`, tone, title: `Paper account: practice money only.${pos}${base}` };
}
