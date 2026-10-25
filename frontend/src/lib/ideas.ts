/**
 * Trade ideas: the payload of /predictions/recommendations
 * (services/agents/recommendation_engine.py) and the plain-English helpers the
 * ideas board, the home page's top calls and the stock page all share.
 */
import { ABSENT, formatNumber } from './format';

export interface OptionLeg {
  symbol: string;
  right: 'C' | 'P';
  side: 'buy' | 'sell';
  strike: number;
  delta: number;
  iv: number | null;
  mid: number;
}

export interface OptionIdea {
  structure: string;
  expiry: string;
  dte: number;
  legs: OptionLeg[];
  net: number;
  max_loss: number;
  max_gain: number | null;
  breakeven: number;
  prob_profit_by_delta: number;
  /** Priced under the model's forecast (shared/utils/option_ev.py). */
  priced_by?: string;
  ev?: number;
  ev_per_risk?: number;
  prob_profit?: number;
  kelly_share?: number;
  iv_regime: string;
  atm_iv: number | null;
  realized_vol: number | null;
}

export interface Idea {
  ticker: string;
  direction: 1 | -1;
  /** Rank across all names: what the forecast is calibrated on. */
  percentile: number | null;
  /** Rank within the stock's industry, which chose it (as the headline book does). */
  industry_percentile?: number | null;
  action: string;
  entry: number;
  stop: number | null;
  exit: string;
  expected_excess_bps: number | null;
  realized_vol: number | null;
  cp_iv_spread: number | null;
  option: OptionIdea | null;
  /** The last quarter of daily closes, for the card's sparkline. */
  spark?: number[];
  /** The live price and its move since the last completed close (%). */
  price?: number | null;
  change_pct?: number | null;
  /** This stock's own forecast (shared/utils/expected_returns.py). */
  forecast?: {
    expected_excess_bps: number | null;
    vol_annual: number | null;
    prob_beats_spy: number | null;
    stock_weight: number;
    edge_after_costs: boolean;
    /** Round trip at the stated size of a $1M account: spread, impact and borrow. */
    cost_bps?: number;
    spread_bps?: number;
    /** Position at which costs take the whole expected edge. */
    capacity_usd?: number;
    /** Today's company news read (trial M), bps over a week against other news names. */
    news_bps?: number | null;
    target: number | null;
  } | null;
  /** Event-led ideas: the insider, block-trade and filing evidence behind them. */
  evidence?: string[];
  /** The model's top reasons, from its exact per-feature contributions. */
  reasons?: string[];
  earnings?: { next_estimated: string | null; last: string | null; inside_hold: boolean } | null;
  /** The Signal Board's view and the event evidence each way. */
  board?: {
    headline: string | null;
    confidence: string | null;
    for: string[];
    against: string[];
  } | null;
  /** What a $10,000 position risks: loss at the stop, cost a side, size for 1% daily risk. */
  risk?: {
    loss_at_stop_usd: number | null;
    cost_bps: number | null;
    vol_sized_usd: number | null;
  } | null;
}

/** One stock and direction across horizons: a name more than one model wants. */
export interface Consensus {
  ticker: string;
  direction: 1 | -1;
  horizons: string[];
  reasons: string[];
}

export interface Book {
  book_usd: number;
  gross: number;
  net: number;
  net_beta: number;
  spy_hedge_usd: number;
  ex_ante_vol: number | null;
  top_industry: string;
  top_industry_share: number;
  cost_to_enter_usd: number;
  cost_to_enter_bps: number;
  max_days_to_trade: number;
  participation_cap: number;
}

export interface HorizonIdeas {
  book?: Book | null;
  withheld?: string | null;
  /** What a buy at this horizon has meant out of sample, against the average stock and the S&P 500. */
  long_note?: string | null;
  horizon_days: number;
  validated: boolean;
  /** Independent bets on each side: correlated ideas count once (shared/utils/recommendations.py). */
  effective_bets?: {
    long: number | null;
    short: number | null;
    long_ideas: number;
    short_ideas: number;
  } | null;
  model: string | null;
  net_sharpe: number | null;
  deflated_sharpe: number | null;
  expected_excess_bps: number | null;
  ideas: Idea[];
}

export interface IntradaySignal {
  id: string;
  instrument: string;
  horizon: string;
  direction: string;
  probability: number;
  expected_bps: number | null;
  entry_price: number | null;
  families?: string[];
}

export interface Recommendations {
  generated_at?: string;
  as_of?: string;
  primary_horizon?: string;
  horizons: Record<string, HorizonIdeas>;
  intraday: IntradaySignal[];
  consensus?: Consensus[];
  note?: string;
  /** The day's backdrop: the regime gauge, the sizing dial ideas carry
   * (momentum-volatility scaling), and SPY's move today. */
  pulse?: {
    regime: string | null;
    sizing_scale: number;
    momentum_vol_scale: number | null;
    spy: number | null;
    spy_change_pct: number | null;
  } | null;
  /** The headline model portfolio's independent bets, each side. */
  book_effective_bets?: {
    long: number | null;
    short: number | null;
    long_names: number;
    short_names: number;
  } | null;
}

// Named for what each one bets on, not by its trading-desk name: "bull call
// spread (debit)" means nothing to most readers; "bet on a rise" does.
export const STRUCTURE: Record<string, string> = {
  long_call: 'Buy a call',
  long_put: 'Buy a put',
  bull_call_debit: 'Call spread: bet on a rise',
  bear_put_debit: 'Put spread: bet on a fall',
  bull_put_credit: 'Get paid if it holds up',
  bear_call_credit: 'Get paid if it stays down',
};

export const ORDER = ['daily', 'weekly', 'monthly', 'events'];
export const LABEL: Record<string, string> = {
  daily: 'Next day',
  weekly: 'Next week',
  monthly: 'Next month',
  events: 'After big events (insider buying, large trades, filings)',
};

export const money = (v: number | null | undefined) =>
  v == null ? ABSENT : `$${formatNumber(Math.abs(v), { decimals: 0 })}`;

export const usd = (v: number) => `$${formatNumber(Math.abs(v), { decimals: 0 })}`;
export const legName = (l: OptionLeg) =>
  `$${formatNumber(l.strike, { decimals: 2 })} ${l.right === 'C' ? 'call' : 'put'}`;

/** The option position in a sentence a non-specialist can act on. */
export function plainOption(ticker: string, o: OptionIdea): string {
  const [a, b] = o.legs;
  const when = `expiring ${o.expiry}`;
  switch (o.structure) {
    case 'long_call':
      return `Buy one ${legName(a)} ${when} for about ${usd(o.net)}. The most you can lose is that ${usd(o.net)}; it pays off if ${ticker} is above $${formatNumber(o.breakeven, { decimals: 2 })} at expiry.`;
    case 'long_put':
      return `Buy one ${legName(a)} ${when} for about ${usd(o.net)}. The most you can lose is that ${usd(o.net)}; it pays off if ${ticker} is below $${formatNumber(o.breakeven, { decimals: 2 })} at expiry.`;
    case 'bull_call_debit':
    case 'bear_put_debit':
      return `Buy the ${legName(a)} and sell the ${legName(b)}, ${when}, for about ${usd(o.net)} net. Most you can lose: ${usd(o.max_loss)}; most you can make: ${o.max_gain != null ? usd(o.max_gain) : 'open'}. Breakeven at $${formatNumber(o.breakeven, { decimals: 2 })}.`;
    case 'bull_put_credit':
    case 'bear_call_credit':
      return `Sell the ${legName(a)} and buy the ${legName(b)}, ${when}, collecting about ${usd(o.net)}. You keep it all if ${ticker} stays ${o.structure === 'bull_put_credit' ? 'above' : 'below'} $${formatNumber(a.strike, { decimals: 2 })}; most you can lose: ${usd(o.max_loss)}.`;
    default:
      return '';
  }
}

/** What the stock idea means, without overstating it. */
export function plainIdea(
  i: Idea,
  days: number,
  expectedBps: number | null,
  validated: boolean,
): string {
  if (i.evidence && i.evidence.length) {
    const dir = i.direction > 0 ? 'up' : 'down';
    const stopTxt =
      i.stop != null ? ` or if it reaches $${formatNumber(i.stop, { decimals: 2 })}` : '';
    return `Fresh evidence on ${i.ticker} points ${dir}: ${i.evidence.join('; ')}. The model is not behind this one; it is graded on its own in the paper ledger. Exit after ${days} trading days${stopTxt}.`;
  }
  // Buys are judged against the average stock (the model's picks beat it out of
  // sample while a few megacaps carried the S&P 500); shorts against the market.
  const side = i.direction > 0 ? 'beat the average stock' : 'lag the market';
  const size =
    expectedBps != null
      ? ` by about ${formatNumber(Math.abs(expectedBps) / 100, { decimals: 2 })}%`
      : '';
  const evidence = validated
    ? 'our model, which has passed every test we set it'
    : 'our model, which did well on years of data it never saw but is not yet fully validated';
  const stop = i.stop != null ? ` or if it reaches $${formatNumber(i.stop, { decimals: 2 })}` : '';
  return `${evidence[0].toUpperCase()}${evidence.slice(1)} expects ${i.ticker} to ${side}${size} over ${days} trading day${days === 1 ? '' : 's'} on average. That is a small edge that shows up across many stocks, not a promise for this one. Exit after ${days} day${days === 1 ? '' : 's'}${stop}.`;
}

/** The card's supporting lines: why, what could upset it, and what it risks. */
export function ideaDetails(i: Idea): { label: string; text: string; tone?: 'warn' }[] {
  const out: { label: string; text: string; tone?: 'warn' }[] = [];
  const f = i.forecast;
  if (f && f.expected_excess_bps != null) {
    const bits = [
      `expected to do ${formatNumber(Math.abs(f.expected_excess_bps) / 100, { decimals: 2 })}% ${f.expected_excess_bps >= 0 ? 'better' : 'worse'} than the S&P 500`,
    ];
    if (f.prob_beats_spy != null) {
      const p = i.direction > 0 ? f.prob_beats_spy : 1 - f.prob_beats_spy;
      bits.push(
        `a ${formatNumber(p * 100, { decimals: 0 })}% chance it goes the way we expect`,
      );
    }
    if (f.target != null) bits.push(`price target $${formatNumber(f.target, { decimals: 2 })}`);
    bits.push(
      f.stock_weight > 0
        ? `put no more than ${formatNumber(f.stock_weight * 100, { decimals: 1 })}% of your account in it`
        : 'not worth buying on its own: trading costs would eat the expected gain',
    );
    if (f.cost_bps != null) {
      const spread =
        f.spread_bps != null
          ? ` (${formatNumber(f.spread_bps / 100, { decimals: 2 })}% of that is the gap between buy and sell prices)`
          : '';
      bits.push(
        `buying and later selling costs about ${formatNumber(f.cost_bps / 100, { decimals: 2 })}%${spread}`,
      );
    }
    if (f.news_bps != null) {
      bits.push(
        `today's news about it moves the forecast ${f.news_bps >= 0 ? 'up' : 'down'} by ${formatNumber(Math.abs(f.news_bps) / 100, { decimals: 2 })}% (already included)`,
      );
    }
    if (f.capacity_usd != null && f.capacity_usd > 0) {
      bits.push(`above about ${usd(f.capacity_usd)}, trading costs would eat the whole gain`);
    }
    out.push({
      label: 'What we expect',
      text: `${bits.join('; ')}.`,
      tone: f.edge_after_costs ? undefined : 'warn',
    });
  }
  if (i.reasons && i.reasons.length) {
    out.push({ label: 'Why', text: `${i.ticker} ${i.reasons.join('; ')}.` });
  }
  if (i.earnings?.inside_hold) {
    out.push({
      label: 'Earnings',
      text: `Results are likely due around ${i.earnings.next_estimated ?? 'this window'}, inside the holding period: the stock can gap either way on the day, past the stop.`,
      tone: 'warn',
    });
  }
  const b = i.board;
  if (b && (b.for.length || b.against.length)) {
    const parts: string[] = [];
    if (b.for.length) parts.push(`for: ${b.for.join('; ')}`);
    if (b.against.length) parts.push(`against: ${b.against.join('; ')}`);
    out.push({
      label: 'Other signals',
      text: parts.join(' · '),
      tone: b.against.length > b.for.length ? 'warn' : undefined,
    });
  }
  const r = i.risk;
  if (r && (r.loss_at_stop_usd != null || r.vol_sized_usd != null)) {
    const bits: string[] = [];
    if (r.loss_at_stop_usd != null)
      bits.push(
        `$10,000 here loses about $${formatNumber(r.loss_at_stop_usd, { decimals: 0 })} if the stop is hit`,
      );
    if (r.cost_bps != null)
      bits.push(`trading costs about ${formatNumber(r.cost_bps / 100, { decimals: 2 })}% each way`);
    if (r.vol_sized_usd != null)
      bits.push(
        `to risk about $100 (1%) on a typical day, hold about $${formatNumber(r.vol_sized_usd, { decimals: 0 })} of it`,
      );
    out.push({ label: 'Risk', text: `${bits.join('; ')}.` });
  }
  return out;
}

/** The chance the idea goes the right way: the model's probability the stock
 * beats SPY for a buy, and that it lags SPY for a short. */
export function rightWayOdds(i: Idea): number | null {
  const p = i.forecast?.prob_beats_spy;
  if (p == null) return null;
  return i.direction > 0 ? p : 1 - p;
}

/** The idea's expected move against SPY, as a fraction: its own forecast, or
 * the horizon's average when it has none. */
export function expectedMove(i: Idea, fallbackBps: number | null = null): number | null {
  const bps = i.forecast?.expected_excess_bps ?? fallbackBps;
  return bps == null ? null : bps / 100;
}

export interface RankedCall {
  idea: Idea;
  horizon: string;
  days: number;
  validated: boolean;
}

/** A rank percentile in words: 0.99 reads "Top 1%", 0.03 reads "Bottom 3%". */
export function rankWords(p: number): string {
  return p >= 0.5
    ? `Top ${Math.max(1, Math.round((1 - p) * 100))}%`
    : `Bottom ${Math.max(1, Math.round(p * 100))}%`;
}

/** The day's strongest calls across the stock horizons for the home page:
 * sized ideas first, by expected move times size, one card per stock, buys and
 * shorts taken in turn so the hero shows both sides while both have calls. */
export function pickTopCalls(rec: Recommendations | undefined, n = 6): RankedCall[] {
  if (!rec?.horizons) return [];
  const all: RankedCall[] = [];
  for (const h of ['monthly', 'weekly', 'daily']) {
    const hz = rec.horizons[h];
    if (!hz) continue;
    for (const idea of hz.ideas ?? []) {
      all.push({ idea, horizon: h, days: hz.horizon_days, validated: hz.validated });
    }
  }
  const score = (c: RankedCall) =>
    Math.abs(c.idea.forecast?.expected_excess_bps ?? 0) *
    Math.max(c.idea.forecast?.stock_weight ?? 0, 0.001);
  const seen = new Set<string>();
  const sides: RankedCall[][] = [[], []];
  for (const c of all.sort((a, b) => score(b) - score(a))) {
    if (seen.has(c.idea.ticker)) continue;
    seen.add(c.idea.ticker);
    sides[c.idea.direction > 0 ? 0 : 1].push(c);
  }
  const [longs, shorts] = sides;
  const out: RankedCall[] = [];
  let next = longs.length && shorts.length && score(shorts[0]) > score(longs[0]) ? 1 : 0;
  while (out.length < n && (longs.length || shorts.length)) {
    const side = sides[next].length ? sides[next] : sides[1 - next];
    out.push(side.shift()!);
    next = 1 - next;
  }
  return out;
}
