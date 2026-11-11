/**
 * Reading a 13F report. The portfolio detail route names its totals `total_portfolio_value_usd` and
 * `total_positions_count`, while the filer list names them `total_value_usd` and `holdings_count`. The panel read
 * only the list's names from the detail report, so the headline tiles showed "$NaNB" and "positions" with no number.
 * Both spellings are accepted here, and a total that is in neither is absent, never zero or NaN.
 */

export interface ReportTotals {
  total_portfolio_value_usd?: number | null;
  total_value_usd?: number | null;
  total_positions_count?: number | null;
  holdings_count?: number | null;
}

const num = (v: unknown): number | null => (typeof v === 'number' && Number.isFinite(v) ? v : null);

export function reportValueUsd(r: ReportTotals | null | undefined): number | null {
  return num(r?.total_portfolio_value_usd) ?? num(r?.total_value_usd);
}

export function reportPositions(r: ReportTotals | null | undefined): number | null {
  return num(r?.total_positions_count) ?? num(r?.holdings_count);
}
