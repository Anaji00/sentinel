'use client';

import React, { useCallback, useEffect, useState } from 'react';
import { formatDateTime, formatNumber } from '../lib/format';
import { useTimeZone } from './ui/TimeZoneContext';
import useSWR from 'swr';
import { fetcher } from '../lib/api';
import { POLL } from './ui/DataProvider';

/** What the gateway reports about the signed-in account's entitlement. */
interface BillingStatus {
  email: string;
  tier: string;
  has_pro: boolean;
  subscription_status: string;
  subscription_ends_at: string | null;
  pro_features: string[];
  billing_enabled: boolean;
  manageable: boolean;
}

export default function SubscriptionPanel() {
  const [status, setStatus] = useState<BillingStatus | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [joined, setJoined] = useState(false);

  // Re-read after joining, so the number the operator just changed is the
  // number they see.
  const { data: waitlist } = useSWR<{ waiting: number }>('/billing/waitlist/count', fetcher, {
    refreshInterval: POLL.slow,
  });
  const waitingCount = typeof waitlist?.waiting === 'number' ? waitlist.waiting : null;
  const { zone } = useTimeZone();

  const load = useCallback(async () => {
    try {
      const res = await fetch('/api/proxy/api/v1/billing/status', { cache: 'no-store' });
      if (!res.ok)
        throw new Error(
          res.status === 401 ? 'Sign in to view your plan.' : 'Could not load your plan.',
        );
      setStatus(await res.json());
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Could not load your plan.');
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  // Both actions hand off to a Stripe-hosted page. No card details are ever
  // entered into this application, which is what keeps it out of PCI scope.
  const go = async (path: string) => {
    setBusy(true);
    setError(null);
    try {
      const res = await fetch(`/api/proxy/api/v1/billing/${path}`, { method: 'POST' });
      const body = await res.json().catch(() => ({}));
      if (!res.ok || !body?.url) {
        throw new Error(body?.detail || 'Stripe is not reachable right now. Nothing was charged.');
      }
      window.location.href = body.url;
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Something went wrong. Nothing was charged.');
      setBusy(false);
    }
  };

  const joinWaitlist = async () => {
    if (!status) return;
    setBusy(true);
    setError(null);
    try {
      const res = await fetch('/api/proxy/api/v1/billing/waitlist', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email: status.email }),
      });
      if (!res.ok) throw new Error('Could not add you to the list. Try again.');
      setJoined(true);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Could not add you to the list.');
    } finally {
      setBusy(false);
    }
  };

  if (error && !status) {
    return <p className="text-sm text-rose-400">{error}</p>;
  }
  if (!status) {
    return <p className="text-sm text-ink-dim">Loading your plan…</p>;
  }

  // A renewal date is a date in someone's calendar, so it renders in the zone
  // the operator chose. Near a month boundary the browser's zone and theirs
  // disagree about which day it is, and a billing date is a bad place for that.
  const renews = status.subscription_ends_at
    ? formatDateTime(status.subscription_ends_at, zone, { seconds: false, withZone: false }).slice(
        0,
        10,
      )
    : null;

  return (
    <div className="max-w-2xl space-y-6">
      <header className="space-y-1">
        <h1 className="text-xl font-bold text-ink">Your plan</h1>
        <p className="text-sm text-ink-dim">{status.email}</p>
      </header>

      <div className="rounded-lg border border-line-strong bg-raised/60 p-5 space-y-3">
        <div className="flex items-center gap-3">
          <span
            className={`text-xs font-mono font-bold uppercase tracking-widest px-2 py-1 rounded ${
              status.has_pro ? 'bg-emerald-500/15 text-emerald-400' : 'bg-slate-600/30 text-ink-dim'
            }`}
          >
            {status.has_pro ? 'Pro' : 'Free'}
          </span>
          {status.has_pro && renews && (
            <span className="text-xs text-ink-dim font-mono">Renews {renews}</span>
          )}
          {!status.has_pro && status.subscription_status === 'past_due' && (
            <span className="text-xs text-amber-400 font-mono">Payment retrying</span>
          )}
        </div>

        <p className="text-sm text-ink-dim leading-relaxed">
          {status.has_pro
            ? 'You have the full platform, including the reasoning tier.'
            : 'The analyst platform is yours in full — every domain and all dashboards. Pro adds the reasoning tier.'}
        </p>

        {!status.has_pro && (
          <ul className="text-sm text-ink-dim space-y-1 pl-4 list-disc">
            <li>Agent swarm reasoning over live events</li>
            <li>Generated scenario briefs</li>
            <li>Agent-authored analysis</li>
            <li>Unlimited backtest runs</li>
          </ul>
        )}

        {error && <p className="text-sm text-rose-400">{error}</p>}

        <div className="flex flex-wrap gap-3 pt-1 items-center">
          {!status.has_pro && status.billing_enabled && (
            <button
              onClick={() => go('checkout')}
              disabled={busy}
              className="px-4 py-2 rounded bg-emerald-500 text-slate-950 text-sm font-bold hover:bg-emerald-400 disabled:opacity-50"
            >
              {busy ? 'Opening Stripe…' : 'Upgrade to Pro'}
            </button>
          )}

          {/* Payments are switched off until there are enough people waiting to
              justify a host that can carry them. Capturing the interest is the
              point: it is the number that decides when to turn billing on. */}
          {!status.has_pro &&
            !status.billing_enabled &&
            (joined ? (
              <p className="text-sm tone-positive">
                You are on the list. We will email you when Pro opens.
                {waitingCount !== null && (
                  <span className="text-ink-dim">
                    {' '}
                    {waitingCount === 1
                      ? 'You are the first.'
                      : `${formatNumber(waitingCount, { decimals: 0 })} people are waiting.`}
                  </span>
                )}
              </p>
            ) : (
              <div className="flex flex-wrap items-center gap-2">
                <button
                  onClick={joinWaitlist}
                  disabled={busy}
                  className="rounded bg-accent px-4 py-2 text-sm font-bold text-inset hover:opacity-90 disabled:opacity-50"
                >
                  {busy ? 'Adding…' : 'Notify me when Pro opens'}
                </button>
                {/* The count the comment above calls "the number that decides
                    when to turn billing on" -- served by
                    `/billing/waitlist/count` and, until now, read by nothing.
                    A waitlist whose size nobody can see cannot inform the
                    decision it exists for. */}
                {waitingCount !== null && (
                  <span className="text-micro text-ink-mute">
                    {waitingCount === 0
                      ? 'Nobody is waiting yet.'
                      : `${formatNumber(waitingCount, { decimals: 0 })} ${
                          waitingCount === 1 ? 'person is' : 'people are'
                        } waiting.`}
                  </span>
                )}
              </div>
            ))}

          {status.manageable && (
            <button
              onClick={() => go('portal')}
              disabled={busy}
              className="px-4 py-2 rounded border border-line-strong text-ink text-sm font-semibold hover:bg-overlay disabled:opacity-50"
            >
              Manage subscription
            </button>
          )}
        </div>
      </div>
    </div>
  );
}
