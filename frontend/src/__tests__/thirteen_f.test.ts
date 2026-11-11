import { describe, expect, it } from 'vitest';
import { reportPositions, reportValueUsd } from '../lib/thirteenF';

describe('13F report totals', () => {
  it('reads the detail route names and the list names', () => {
    expect(reportValueUsd({ total_portfolio_value_usd: 2.5e11 })).toBe(2.5e11);
    expect(reportValueUsd({ total_value_usd: 1e9 })).toBe(1e9);
    expect(reportPositions({ total_positions_count: 41 })).toBe(41);
    expect(reportPositions({ holdings_count: 7 })).toBe(7);
  });

  it('prefers the detail name and keeps a real zero', () => {
    expect(reportValueUsd({ total_portfolio_value_usd: 5, total_value_usd: 9 })).toBe(5);
    expect(reportPositions({ total_positions_count: 0, holdings_count: 7 })).toBe(0);
  });

  it('is absent, not NaN or zero, when the report carries neither', () => {
    expect(reportValueUsd({})).toBeNull();
    expect(reportValueUsd(null)).toBeNull();
    expect(reportPositions({ total_positions_count: Number.NaN })).toBeNull();
    expect(reportPositions(undefined)).toBeNull();
  });
});
