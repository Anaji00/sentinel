'use client';

/**
 * Placing and cancelling an order from the book.
 *
 * `POST /portfolio/orders` and `DELETE /portfolio/orders/{id}` are the generic
 * order path: symbol, quantity, side, type. They check the kill switch, record
 * intent in the audit ledger before the venue is reached, and require ADMIN.
 * Neither had a caller -- the only way to place an order from the product was
 * the advisor's Kelly-sized bracket, which is a different thing: that one
 * acts on a signal the platform generated, and this one lets a person act on
 * their own judgement.
 *
 * This is the one surface in the pass where building it adds risk rather than
 * only value, so it is deliberate about three things:
 *
 *   It states whether the book is paper or live, in the ticket, every time.
 *   The same panel shows both, and an operator who has been reading a paper
 *   book all week must not be able to submit into a real one by habit.
 *
 *   It names the whole order back before submitting -- symbol, side, quantity,
 *   type, and the notional if that can be computed -- because an order is not
 *   reversible and a confirmation that says "Are you sure?" is not a
 *   confirmation.
 *
 *   It refuses before the wire on anything the server would refuse anyway, so
 *   a mistake is a message rather than a 422.
 */

import React from 'react';
import useSWR, { mutate as globalMutate } from 'swr';
import { apiClient, describeApiError, fetcher } from '../lib/api';
import { useFeedback } from './ui/Feedback';
import { POLL } from './ui/DataProvider';
import { IconAlert, IconBlocked } from './ui/icons';
import { formatCurrency, formatNumber, isPresent } from '../lib/format';

const SIDES = ['BUY', 'SELL'] as const;
/** `OrderType` in shared/broker/base.py. The two a person places by hand. */
const TYPES = ['MARKET', 'LIMIT'] as const;

type Side = (typeof SIDES)[number];
type OrderType = (typeof TYPES)[number];

interface FlagState {
  signals?: Record<string, { enabled: boolean; kill_switched: boolean; reason?: string }>;
  master_kill_switch?: { active: boolean; reason: string };
}

interface OrderTicketProps {
  /** Whether this book reaches a real venue. Never inferred here. */
  isLive: boolean;
  /** Marks, for the notional estimate. Symbol to last price. */
  marks?: Record<string, number>;
  onPlaced?: () => void;
}

export function OrderTicket({ isLive, marks = {}, onPlaced }: OrderTicketProps) {
  const { toast, confirm } = useFeedback();
  const [symbol, setSymbol] = React.useState('');
  const [qty, setQty] = React.useState('');
  const [side, setSide] = React.useState<Side>('BUY');
  const [type, setType] = React.useState<OrderType>('MARKET');
  const [limit, setLimit] = React.useState('');
  const [busy, setBusy] = React.useState(false);

  // The kill switch, read rather than assumed. The gateway refuses with 423
  // when execution is halted; showing that here means an operator learns it
  // before typing an order rather than after submitting one.
  const { data: flags } = useSWR<FlagState>('/flags', fetcher, { refreshInterval: POLL.slow });
  const execution = flags?.signals?.order_execution;
  const halted =
    flags?.master_kill_switch?.active === true ||
    (execution !== undefined && (execution.enabled === false || execution.kill_switched === true));

  const cleanSymbol = symbol.trim().toUpperCase();
  const quantity = Number(qty);
  const limitPrice = Number(limit);

  const problems: string[] = [];
  if (cleanSymbol.length === 0) problems.push('a symbol');
  if (cleanSymbol.length > 10) problems.push('a symbol of ten characters or fewer');
  if (!Number.isFinite(quantity) || quantity <= 0) problems.push('a positive quantity');
  if (type === 'LIMIT' && (!Number.isFinite(limitPrice) || limitPrice <= 0))
    problems.push('a positive limit price');

  const mark = marks[cleanSymbol];
  const reference = type === 'LIMIT' && Number.isFinite(limitPrice) ? limitPrice : mark;
  const notional =
    isPresent(reference) && Number.isFinite(quantity) && quantity > 0 ? reference * quantity : null;

  const submit = async () => {
    if (problems.length > 0 || halted) return;

    // The whole order, named back. "Are you sure?" is not a confirmation.
    const ok = await confirm({
      title: isLive ? 'Place this order against real capital?' : 'Place this paper order?',
      body:
        `${side} ${formatNumber(quantity, { decimals: 0 })} ${cleanSymbol} as a ` +
        `${type.toLowerCase()} order` +
        (type === 'LIMIT' ? ` at ${formatCurrency(limitPrice)}` : '') +
        (notional !== null ? `, about ${formatCurrency(notional, { decimals: 0 })}` : '') +
        `. ${
          isLive
            ? 'This book reaches a real venue. The order cannot be recalled once filled.'
            : 'This is the simulated book; no order reaches a venue.'
        }`,
      confirmLabel: `${side} ${cleanSymbol}`,
    });
    if (!ok) return;

    setBusy(true);
    try {
      await apiClient.post('/portfolio/orders', {
        symbol: cleanSymbol,
        qty: quantity,
        side,
        order_type: type,
        ...(type === 'LIMIT' ? { limit_price: limitPrice } : {}),
      });
      toast('success', `${side} ${cleanSymbol} submitted`, 'Recorded in the audit trail.');
      setSymbol('');
      setQty('');
      setLimit('');
      await Promise.all([
        globalMutate('/portfolio/positions'),
        globalMutate('/portfolio/account'),
        globalMutate('/portfolio/risk'),
      ]);
      onPlaced?.();
    } catch (err) {
      // 423 is the kill switch and 503 is the ledger refusing to record; both
      // mean no order was placed, and both are worth reading in full.
      toast('error', 'No order was placed.', describeApiError(err) ?? undefined);
    }
    setBusy(false);
  };

  const field =
    'rounded-md border border-line bg-page px-2 py-1 text-micro text-ink outline-none focus:border-line-accent';

  return (
    <div className="space-y-2 border-t border-line px-3.5 py-3">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <span className="stat-label">Place an order</span>
        {halted ? (
          <span className="badge tone-negative">
            <IconBlocked />
            Execution halted
          </span>
        ) : isLive ? (
          <span className="badge tone-negative">
            <IconAlert />
            Real capital
          </span>
        ) : (
          <span className="badge tone-info">Paper</span>
        )}
      </div>

      {halted && (
        <p className="text-micro tone-caution">
          {/* The flag's own reason, not a paraphrase. */}
          {flags?.master_kill_switch?.active
            ? flags.master_kill_switch.reason || 'The platform kill switch is active.'
            : execution?.reason || 'Order execution is disabled at the gateway.'}
        </p>
      )}

      <div className="flex flex-wrap items-end gap-1.5">
        <label className="flex flex-col gap-0.5">
          <span className="stat-label">Symbol</span>
          <input
            value={symbol}
            onChange={(e) => setSymbol(e.target.value)}
            maxLength={10}
            placeholder="AAPL"
            className={`${field} w-24 font-mono uppercase`}
          />
        </label>

        <label className="flex flex-col gap-0.5">
          <span className="stat-label">Quantity</span>
          <input
            value={qty}
            onChange={(e) => setQty(e.target.value)}
            inputMode="decimal"
            placeholder="10"
            className={`${field} w-20 text-right font-mono`}
          />
        </label>

        <div className="flex flex-col gap-0.5">
          <span className="stat-label">Side</span>
          <div className="flex gap-1">
            {SIDES.map((s) => (
              <button
                key={s}
                onClick={() => setSide(s)}
                aria-pressed={side === s}
                className={`cursor-pointer rounded-md border px-2 py-1 text-micro font-medium transition-colors ${
                  side === s
                    ? s === 'BUY'
                      ? 'border-positive/50 bg-positive/10 text-positive'
                      : 'border-negative/50 bg-negative/10 text-negative'
                    : 'border-line text-ink-mute hover:text-ink-dim'
                }`}
              >
                {s}
              </button>
            ))}
          </div>
        </div>

        <div className="flex flex-col gap-0.5">
          <span className="stat-label">Type</span>
          <div className="flex gap-1">
            {TYPES.map((t) => (
              <button
                key={t}
                onClick={() => setType(t)}
                aria-pressed={type === t}
                className={`cursor-pointer rounded-md border px-2 py-1 text-micro font-medium transition-colors ${
                  type === t
                    ? 'border-line-accent bg-accent-dim text-accent'
                    : 'border-line text-ink-mute hover:text-ink-dim'
                }`}
              >
                {t === 'MARKET' ? 'Market' : 'Limit'}
              </button>
            ))}
          </div>
        </div>

        {type === 'LIMIT' && (
          <label className="flex flex-col gap-0.5">
            <span className="stat-label">Limit</span>
            <input
              value={limit}
              onChange={(e) => setLimit(e.target.value)}
              inputMode="decimal"
              placeholder="200.00"
              className={`${field} w-24 text-right font-mono`}
            />
          </label>
        )}

        <button
          onClick={submit}
          disabled={problems.length > 0 || halted || busy}
          className="rounded-md border border-line-accent bg-accent-dim px-3 py-1 text-micro font-semibold text-accent transition-colors enabled:cursor-pointer enabled:hover:border-accent disabled:opacity-40"
        >
          {busy ? 'Submitting…' : 'Review'}
        </button>
      </div>

      <p className="text-micro text-ink-mute">
        {/* What is missing, or what this will cost. Both are more useful than
            a disabled button with no explanation. */}
        {problems.length > 0
          ? `Needs ${problems.join(', ')}.`
          : notional !== null
            ? `About ${formatCurrency(notional, { decimals: 0 })} at ${
                type === 'LIMIT' ? 'the limit' : 'the last mark'
              }. Order entry requires an administrator.`
            : 'No mark for that symbol yet, so the notional cannot be estimated here. Order entry requires an administrator.'}
      </p>
    </div>
  );
}

export default OrderTicket;
