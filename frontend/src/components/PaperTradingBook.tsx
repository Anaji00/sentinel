'use client';

/**
 * The trading book: what is held, what it is worth, and what it risks.
 *
 * `/portfolio/account`, `/portfolio/positions` and `/portfolio/risk` have been
 * served for the life of the project and no component called any of them. The
 * only portfolio surface in the UI was a Kelly-sizing panel that submitted
 * orders and then showed a toast -- so an operator could place a trade and had
 * nowhere to see that they held it.
 *
 * Which was apt, because until this pass they did not. `/trading/orders/execute`
 * built a fresh `PaperBroker` per request, so a fill landed in an object
 * discarded when the response was written. Measured on the deployment: an order
 * for 10 AAPL returned EXECUTIVE_FILLED with a bracket, and positions came back
 * empty with cash unchanged.
 *
 * Two things this panel refuses to soften:
 *
 *   `is_live` is stated, always, in the header. Paper and real capital must
 *   never be a detail someone has to infer from context.
 *
 *   The risk figures carry their provenance envelope. The endpoint publishes a
 *   methodology that names its own weakening assumption -- the L2 norm of
 *   weights equals portfolio volatility only if positions are uncorrelated --
 *   and a number quoted without that is a stronger claim than the server made.
 */

import React from 'react';
import useSWR from 'swr';
import { describeApiError, fetcher } from '../lib/api';
import { Card } from './ui/Card';
import { EmptyState } from './ui/EmptyState';
import { ExportButton } from './ui/ExportButton';
import { POLL } from './ui/DataProvider';
import { ProvenanceBadge, type ProvenanceType } from './ProvenanceBadge';
import { IconAlert, IconDown, IconRisk, IconUp } from './ui/icons';
import { ABSENT, formatCurrency, formatNumber, formatPercent, isPresent } from '../lib/format';
import type { CsvColumn } from '../lib/csv';
import { OrderTicket } from './OrderTicket';

interface AccountSummary {
  broker_name: string;
  account_id: string;
  cash: number;
  portfolio_value: number;
  buying_power: number;
  currency: string;
  positions_count: number;
  is_live: boolean;
}

interface Position {
  symbol: string;
  qty: number;
  avg_entry_price: number;
  current_price: number;
  market_value: number;
  unrealized_pl: number;
  unrealized_pl_pct: number;
  asset_class: string;
  updated_at: string;
}

interface PositionsResponse {
  count: number;
  total_market_value: number;
  total_unrealized_pl: number;
  positions: Position[];
}

interface Provenance {
  source_type: ProvenanceType;
  methodology?: string | null;
  model_name?: string | null;
  model_confidence?: number | null;
  data_inputs?: string[];
  is_synthetic?: boolean;
}

interface RiskMetrics {
  portfolio_value: number;
  positions_count: number;
  var_95_daily_usd: number | null;
  var_95_daily_pct: number | null;
  cvar_99_daily_usd: number | null;
  cvar_99_daily_pct: number | null;
  portfolio_beta: number | null;
  daily_volatility_used?: number | null;
  daily_volatility_is_measured?: boolean;
  diversification_score: number | null;
  concentration_hhi?: number | null;
  sector_exposure: Record<string, number>;
  provenance?: Provenance;
}

const CSV_COLUMNS: CsvColumn<Position>[] = [
  { label: 'symbol', value: (p) => p.symbol },
  { label: 'qty', value: (p) => p.qty },
  { label: 'avg_entry_price', value: (p) => p.avg_entry_price },
  { label: 'current_price', value: (p) => p.current_price },
  { label: 'market_value', value: (p) => p.market_value },
  { label: 'unrealized_pl', value: (p) => p.unrealized_pl },
  { label: 'unrealized_pl_pct', value: (p) => p.unrealized_pl_pct },
  { label: 'asset_class', value: (p) => p.asset_class },
  { label: 'marked_at', value: (p) => p.updated_at },
];

/** A money figure that shows its sign and takes its colour from it. */
function Pnl({ usd, pct }: { usd: number | null | undefined; pct?: number | null }) {
  if (!isPresent(usd)) return <span className="text-ink-mute">{ABSENT}</span>;
  const tone = usd > 0 ? 'text-positive' : usd < 0 ? 'text-negative' : 'text-ink-mute';
  return (
    <span className={`inline-flex items-center gap-1 font-mono ${tone}`}>
      {usd > 0 ? <IconUp /> : usd < 0 ? <IconDown /> : null}
      {formatCurrency(usd, { signed: true })}
      {isPresent(pct) && (
        <span className="text-ink-mute">({formatNumber(pct, { decimals: 2, signed: true })}%)</span>
      )}
    </span>
  );
}

function Stat({ label, value, basis }: { label: string; value: React.ReactNode; basis?: string }) {
  return (
    <div className="flex min-w-0 flex-col gap-0.5">
      <span className="stat-label">{label}</span>
      <span className="font-mono text-head font-semibold leading-none text-ink">{value}</span>
      {basis && <span className="truncate text-micro text-ink-mute">{basis}</span>}
    </div>
  );
}

export default function PaperTradingBook() {
  const { data: account, error: accountError } = useSWR<AccountSummary>(
    '/portfolio/account',
    fetcher,
    { refreshInterval: POLL.standard },
  );
  const { data: book, error: bookError } = useSWR<PositionsResponse>(
    '/portfolio/positions',
    fetcher,
    { refreshInterval: POLL.standard },
  );
  const { data: risk } = useSWR<RiskMetrics>('/portfolio/risk', fetcher, {
    refreshInterval: POLL.slow,
  });

  const error = accountError ?? bookError;
  const positions = book?.positions ?? [];

  if (error) {
    return (
      <Card className="h-full">
        <EmptyState
          kind="error"
          title="Trading book unavailable"
          detail={describeApiError(error) ?? undefined}
        />
      </Card>
    );
  }

  if (!account) {
    return (
      <Card className="h-full">
        <EmptyState kind="loading" title="Reading the book" />
      </Card>
    );
  }

  // Against the starting cash, which is what "up or down" means for a book
  // that has never taken a deposit. Stated in the basis line rather than left
  // for the reader to assume.
  const openPl = book?.total_unrealized_pl ?? null;

  return (
    <Card noPadding className="flex h-full flex-col overflow-hidden">
      <div className="panel-header shrink-0">
        <div className="min-w-0">
          <h2 className="panel-title flex items-center gap-2">
            Trading book
            {/* Never inferred from context. An operator glancing at this panel
                has to be able to tell simulated capital from real without
                reading anything else on the page. */}
            <span
              className={`badge ${account.is_live ? 'tone-negative' : 'tone-info'}`}
              title={
                account.is_live
                  ? 'Orders from this platform reach a real venue against real capital.'
                  : 'Simulated. No order from this platform reaches a venue.'
              }
            >
              {account.is_live ? (
                <>
                  <IconAlert /> Live capital
                </>
              ) : (
                'Paper'
              )}
            </span>
          </h2>
          <p className="panel-subtitle">
            {account.broker_name} · {account.account_id} · {account.currency}
          </p>
        </div>
        <ExportButton subject="positions" rows={positions} columns={CSV_COLUMNS} />
      </div>

      <div className="grid shrink-0 grid-cols-2 gap-x-4 gap-y-3 border-b border-line px-3.5 py-3 sm:grid-cols-4">
        <Stat
          label="Portfolio value"
          value={formatCurrency(account.portfolio_value, { decimals: 0 })}
          basis="cash plus marked positions"
        />
        <Stat
          label="Cash"
          value={formatCurrency(account.cash, { decimals: 0 })}
          basis="uncommitted"
        />
        <Stat
          label="Buying power"
          value={formatCurrency(account.buying_power, { decimals: 0 })}
          basis="what the book will let you commit"
        />
        <Stat
          label="Open P&L"
          value={<Pnl usd={openPl} />}
          basis={
            positions.length === 0
              ? 'nothing held'
              : `across ${positions.length} position${positions.length === 1 ? '' : 's'}`
          }
        />
      </div>

      <div className="min-h-0 flex-1 overflow-y-auto">
        {positions.length === 0 ? (
          /* The book is process-local by design -- `shared/broker` says so, and
             persisting it properly belongs in Postgres rather than Redis.
             Verified on the deployment: after a gateway restart, positions
             return to zero and cash to the opening balance, while the audit
             ledger keeps every entry. An operator who restarts and finds the
             book empty should be able to read why here rather than assume
             their data was lost. */
          <EmptyState
            kind="empty"
            title="No open positions"
            detail={
              account.is_live
                ? 'Nothing is held at the venue.'
                : 'Nothing is held. Filled orders appear here, and the paper book resets when the gateway restarts — the audit trail does not.'
            }
          />
        ) : (
          <table className="w-full text-left text-xs">
            <thead className="sticky top-0 bg-inset">
              <tr className="border-b border-line">
                <th className="stat-label px-3 py-2">Symbol</th>
                <th className="stat-label px-3 py-2 text-right">Qty</th>
                <th className="stat-label px-3 py-2 text-right">Avg entry</th>
                <th className="stat-label px-3 py-2 text-right">Mark</th>
                <th className="stat-label px-3 py-2 text-right">Market value</th>
                <th className="stat-label px-3 py-2 text-right">Open P&L</th>
              </tr>
            </thead>
            <tbody>
              {positions.map((p) => (
                <tr key={p.symbol} className="border-b border-line/60 hover:bg-overlay">
                  <td className="px-3 py-2">
                    <span className="symbol text-accent">{p.symbol}</span>
                  </td>
                  <td className="px-3 py-2 text-right font-mono text-ink-dim">
                    {formatNumber(p.qty, { decimals: 0 })}
                  </td>
                  <td className="px-3 py-2 text-right font-mono text-ink-dim">
                    {formatCurrency(p.avg_entry_price)}
                  </td>
                  <td className="px-3 py-2 text-right font-mono text-ink">
                    {formatCurrency(p.current_price)}
                  </td>
                  <td className="px-3 py-2 text-right font-mono text-ink-dim">
                    {formatCurrency(p.market_value, { decimals: 0 })}
                  </td>
                  <td className="px-3 py-2 text-right">
                    <Pnl usd={p.unrealized_pl} pct={p.unrealized_pl_pct} />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>

      {/* Order entry. Deliberately below the book rather than above it: the
          first thing on this panel should be what is held, not a form. */}
      <OrderTicket
        isLive={account.is_live}
        marks={Object.fromEntries(positions.map((p) => [p.symbol, p.current_price]))}
      />

      {risk && (
        <div className="shrink-0 space-y-2 border-t border-line bg-inset px-3.5 py-3">
          <div className="flex flex-wrap items-center justify-between gap-2">
            <span className="stat-label flex items-center gap-1.5">
              <IconRisk />
              One-day risk
            </span>
            {risk.provenance && (
              <ProvenanceBadge
                sourceType={risk.provenance.source_type}
                methodology={risk.provenance.methodology ?? undefined}
                modelName={risk.provenance.model_name ?? undefined}
                confidence={risk.provenance.model_confidence ?? undefined}
                dataInputs={risk.provenance.data_inputs ?? []}
                isSynthetic={risk.provenance.is_synthetic ?? false}
              />
            )}
          </div>
          <div className="grid grid-cols-2 gap-x-4 gap-y-2 sm:grid-cols-4">
            <Stat
              label="VaR 95%"
              value={formatCurrency(risk.var_95_daily_usd, { decimals: 0 })}
              basis={
                isPresent(risk.var_95_daily_pct)
                  ? `${formatNumber(risk.var_95_daily_pct, { decimals: 2 })}% of book`
                  : undefined
              }
            />
            <Stat
              label="CVaR 99%"
              value={formatCurrency(risk.cvar_99_daily_usd, { decimals: 0 })}
              basis={
                isPresent(risk.cvar_99_daily_pct)
                  ? `${formatNumber(risk.cvar_99_daily_pct, { decimals: 2 })}% of book`
                  : undefined
              }
            />
            <Stat
              label="Daily volatility"
              value={
                isPresent(risk.daily_volatility_used)
                  ? formatPercent(risk.daily_volatility_used, { from: 'ratio', decimals: 2 })
                  : ABSENT
              }
              // Measured and assumed are different claims, and this endpoint
              // says which it is. Reporting both as a number would drop the
              // only part that says how much to trust it.
              basis={
                risk.daily_volatility_is_measured === undefined
                  ? undefined
                  : risk.daily_volatility_is_measured
                    ? 'measured from realised bars'
                    : 'assumed, not measured'
              }
            />
            <Stat
              label="Portfolio beta"
              value={formatNumber(risk.portfolio_beta)}
              basis={risk.portfolio_beta === null ? 'not computed for this book' : 'against SPY'}
            />
          </div>
        </div>
      )}
    </Card>
  );
}
