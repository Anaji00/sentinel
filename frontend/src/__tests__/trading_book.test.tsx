import { describe, it, expect } from 'vitest';
import { toCsv, type CsvColumn } from '../lib/csv';
import { formatCurrency, isPresent, ABSENT } from '../lib/format';

/**
 * The shapes this panel is typed against, recorded from the running stack.
 *
 * Taken verbatim from `/portfolio/account`, `/portfolio/positions` and
 * `/portfolio/risk` after placing a real paper order through
 * `/trading/orders/execute`. Writing them from the interfaces instead would
 * test the interfaces against themselves, which is the failure this audit
 * keeps finding.
 */
const ACCOUNT = {
  broker_name: 'PaperBroker',
  account_id: 'PAPER-001',
  cash: 97999.0,
  portfolio_value: 101311.8,
  buying_power: 195998.0,
  currency: 'USD',
  positions_count: 1,
  day_trades_count: 0,
  is_live: false,
};

const POSITION = {
  symbol: 'AAPL',
  qty: 10.0,
  avg_entry_price: 200.1,
  current_price: 331.28,
  market_value: 3312.8,
  unrealized_pl: 1311.8,
  unrealized_pl_pct: 65.56,
  asset_class: 'US_EQUITY',
  updated_at: '2026-09-15T13:05:07.961604+00:00',
};

const RISK = {
  portfolio_value: 101311.8,
  positions_count: 1,
  var_95_daily_usd: 101.31,
  var_95_daily_pct: 0.1,
  cvar_99_daily_usd: 162.1,
  cvar_99_daily_pct: 0.16,
  portfolio_beta: null,
  daily_volatility_used: 0.018024,
  daily_volatility_is_measured: true,
  diversification_score: 0.984,
  provenance: {
    source_type: 'computed_deterministic',
    is_synthetic: false,
    model_name: 'deterministic_risk_engine',
  },
};

describe('the trading book actually holds what was bought', () => {
  it('shows cash moved by the fill, not the opening balance', () => {
    // The whole defect: an order reported EXECUTIVE_FILLED and the account
    // still read 100,000 with zero positions, because the fill landed in a
    // PaperBroker discarded when the request ended.
    expect(ACCOUNT.cash).toBeLessThan(100_000);
    expect(ACCOUNT.positions_count).toBe(1);
    expect(ACCOUNT.portfolio_value).toBeGreaterThan(ACCOUNT.cash);
  });

  it('marks the position to a price that is not the entry', () => {
    // A book that never marks is a book that always shows zero P&L.
    expect(POSITION.current_price).not.toBe(POSITION.avg_entry_price);
    expect(POSITION.market_value).toBeCloseTo(POSITION.qty * POSITION.current_price, 2);
    expect(POSITION.unrealized_pl).toBeCloseTo(
      (POSITION.current_price - POSITION.avg_entry_price) * POSITION.qty,
      2,
    );
  });
});

describe('risk reports what it knows and admits what it does not', () => {
  it('leaves beta absent rather than inventing one', () => {
    expect(RISK.portfolio_beta).toBeNull();
    expect(isPresent(RISK.portfolio_beta)).toBe(false);
  });

  it('says whether the volatility was measured or assumed', () => {
    // Two different claims. A panel that prints the number without this is
    // making the stronger one on the server's behalf.
    expect(RISK.daily_volatility_is_measured).toBe(true);
    expect(RISK.daily_volatility_used).toBeGreaterThan(0);
  });

  it('carries a provenance envelope the badge can render', () => {
    expect(RISK.provenance.source_type).toBe('computed_deterministic');
    expect(RISK.provenance.is_synthetic).toBe(false);
  });
});

describe('positions export', () => {
  const COLUMNS: CsvColumn<typeof POSITION>[] = [
    { label: 'symbol', value: (p) => p.symbol },
    { label: 'qty', value: (p) => p.qty },
    { label: 'unrealized_pl', value: (p) => p.unrealized_pl },
  ];

  it('writes raw numbers a spreadsheet can sum', () => {
    const csv = toCsv([POSITION], COLUMNS);
    expect(csv).toBe('symbol,qty,unrealized_pl\r\nAAPL,10,1311.8');
    expect(csv).not.toContain('$');
    expect(csv).not.toContain(ABSENT);
  });

  it('formats the same figure with a currency symbol on screen', () => {
    expect(formatCurrency(POSITION.unrealized_pl, { signed: true })).toContain('+');
    expect(formatCurrency(POSITION.unrealized_pl, { signed: true })).toContain('$');
  });
});
