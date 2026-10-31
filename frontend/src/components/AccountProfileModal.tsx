'use client';

import React, { useState, useEffect, useCallback } from 'react';
import IntegrationsPanel from './IntegrationsPanel';
import { ABSENT, formatNumber, formatTimestamp, formatAge, formatSymbol } from '../lib/format';
import { IconClose } from './ui/icons';
import { useDialog } from './ui/useDialog';
import { useSession } from './ui/SessionContext';

interface AccountProfileModalProps {
  isOpen: boolean;
  onClose: () => void;
}

/** Authenticated identity, from the session cookie the BFF issued. */
interface SessionIdentity {
  email: string | null;
  role: string | null;
}

/** Live platform facts. Null means "not reported", never zero. */
interface PlatformSnapshot {
  activeAgents: number | null;
  reportingTiers: Record<string, string>;
  watchlistCount: number | null;
  systemStatus: string | null;
  healthyComponents: number | null;
  scoredComponents: number | null;
  lastUpdated: string | null;
}

/**
 * Rate limiter constants mirrored from services/api_gateway/dependencies.py.
 * Stated here because the operator needs to know the ceiling they are working
 * against; kept adjacent to a test that asserts they match the backend.
 */
const RATE_LIMIT_BURST = 120;
const RATE_LIMIT_PER_SEC = 10;

const Field: React.FC<{ label: string; value: React.ReactNode; tone?: string; hint?: string }> = ({
  label,
  value,
  tone = 'text-ink',
  hint,
}) => (
  <div className="p-3 bg-raised/60 rounded-xl border border-line">
    <span className="text-micro text-ink-dim uppercase tracking-wider font-semibold">{label}</span>
    <p className={`text-xs font-bold mt-1 ${tone}`}>{value}</p>
    {hint && <p className="text-micro text-ink-mute mt-0.5 font-normal">{hint}</p>}
  </div>
);

type TabKey = 'overview' | 'access' | 'integrations' | 'telemetry';

const TABS: ReadonlyArray<{ key: TabKey; label: string }> = [
  { key: 'overview', label: 'Overview' },
  { key: 'access', label: 'Access & limits' },
  { key: 'integrations', label: 'Data sources' },
  { key: 'telemetry', label: 'Platform status' },
];

export const AccountProfileModal: React.FC<AccountProfileModalProps> = ({ isOpen, onClose }) => {
  // Escape, focus trap, focus restore, backdrop dismiss. This overlay had
  // none of them: a keyboard user could tab out of it into the page behind,
  // which is still focusable and now invisible under the backdrop.
  const dialog = useDialog(isOpen, onClose, 'Account');
  const session = useSession();
  const identity: SessionIdentity = { email: session.email, role: session.role };
  const [platform, setPlatform] = useState<PlatformSnapshot | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState<TabKey>('overview');
  // The session endpoint used to report every user as 'admin'; it now returns
  // the role carried in the signed token, so this is a real check.
  const isOperator = (identity.role || '').toUpperCase() === 'ADMIN';

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [sessionRes, agentsRes, watchlistRes, healthRes] = await Promise.allSettled([
        Promise.resolve(null),
        fetch('/api/proxy/api/v1/agents/processes'),
        fetch('/api/proxy/api/v1/watchlists/equities'),
        fetch('/api/proxy/api/v1/health/data'),
      ]);

      // Identity is not re-derived here. This used to fetch the session a
      // third time, alongside the header and the live-events hook, and the
      // three could disagree; it now reads the one the provider holds.
      void sessionRes;

      const snapshot: PlatformSnapshot = {
        activeAgents: null,
        reportingTiers: {},
        watchlistCount: null,
        systemStatus: null,
        healthyComponents: null,
        scoredComponents: null,
        lastUpdated: new Date().toISOString(),
      };

      if (agentsRes.status === 'fulfilled' && agentsRes.value.ok) {
        const a = await agentsRes.value.json();
        snapshot.activeAgents =
          typeof a?.active_agents_count === 'number' ? a.active_agents_count : null;
        snapshot.reportingTiers = a?.reporting_tiers ?? {};
      }

      if (watchlistRes.status === 'fulfilled' && watchlistRes.value.ok) {
        const w = await watchlistRes.value.json();
        const list = Array.isArray(w) ? w : (w?.watchlist ?? w?.equities);
        snapshot.watchlistCount = Array.isArray(list) ? list.length : null;
      }

      if (healthRes.status === 'fulfilled' && healthRes.value.ok) {
        const h = await healthRes.value.json();
        snapshot.systemStatus = h?.system_status ?? null;
        snapshot.healthyComponents = typeof h?.healthy_count === 'number' ? h.healthy_count : null;
        snapshot.scoredComponents =
          typeof h?.scored_components_count === 'number'
            ? h.scored_components_count
            : typeof h?.components_count === 'number'
              ? h.components_count
              : null;
      }

      setPlatform(snapshot);
    } catch (e) {
      setError(e instanceof Error ? e.message : 'Unable to load account details');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    if (isOpen) load();
  }, [isOpen, load]);

  if (!isOpen) return null;

  const email = identity.email ?? null;
  const initials = email
    ? email
        .replace(/@.*/, '')
        .split(/[._-]/)
        .map((p) => p[0]?.toUpperCase() ?? '')
        .join('')
        .slice(0, 2) || '?'
    : '?';

  const statusTone =
    platform?.systemStatus === 'OPERATIONAL'
      ? 'text-positive'
      : platform?.systemStatus === 'DEGRADED'
        ? 'text-caution'
        : platform?.systemStatus
          ? 'text-negative'
          : 'text-ink-dim';

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/80 p-4 animate-in fade-in duration-200"
      {...dialog.overlayProps}
    >
      <div
        className="relative w-full max-w-2xl bg-raised border border-accent/40 rounded-2xl overflow-hidden text-ink"
        {...dialog.panelProps}
      >
        {/* Header */}
        <div className="flex items-center justify-between px-6 py-4 bg-page/80 border-b border-line">
          <div className="flex items-center gap-3">
            <div className="h-10 w-10 rounded-xl bg-accent-dim border border-line-accent flex items-center justify-center text-accent font-bold text-lg">
              {initials}
            </div>
            <div>
              <h3 className="text-sm font-bold text-ink">{email ?? 'Not signed in'}</h3>
              <p className="text-micro text-ink-dim">
                {identity.role ? `Role: ${formatSymbol(identity.role)}` : 'Role unavailable'}
              </p>
            </div>
          </div>
          <button
            onClick={onClose}
            aria-label="Close account panel"
            className="text-ink-dim hover:text-ink transition-colors text-lg px-2 rounded focus:outline-none focus:ring-2 focus:ring-accent"
          >
            <IconClose />
          </button>
        </div>

        {/* Tabs */}
        <div className="flex border-b border-line bg-page/50">
          {/* Data sources is operator configuration -- which upstreams are set up
              and which environment variables are missing. That is reconnaissance
              about the deployment rather than a product feature, so the tab is
              not offered to ordinary accounts. The gateway enforces this too;
              hiding it here just avoids showing a tab that would 403. */}
          {TABS.filter((t) => t.key !== 'integrations' || isOperator).map(({ key, label }) => (
            <button
              key={key}
              onClick={() => setActiveTab(key)}
              className={`px-4 py-2.5 text-micro font-bold tracking-wider transition-colors focus:outline-none focus:ring-2 focus:ring-inset focus:ring-accent ${
                activeTab === key
                  ? 'text-accent border-b-2 border-line-accent bg-raised/40'
                  : 'text-ink-mute hover:text-ink-dim'
              }`}
            >
              {label}
            </button>
          ))}

          {/* Sign out lived nowhere. `/api/auth/logout` existed, clears the
              cookie correctly, and had no caller anywhere in the codebase --
              so the only way out of a session was to wait 24 hours for it to
              expire or to clear cookies by hand. */}
          <button
            onClick={() => {
              onClose();
              void session.signOut();
            }}
            className="ml-auto mr-3 cursor-pointer self-center rounded-md border border-line px-2.5 py-1 text-micro font-medium text-ink-dim transition-colors hover:border-negative/50 hover:text-negative"
          >
            Sign out
          </button>
        </div>

        <div className="p-6 space-y-6 max-h-[60vh] overflow-y-auto">
          {error && (
            <div className="p-3 bg-negative/10 border border-negative/30 rounded-xl text-micro text-negative">
              {error} —{' '}
              <button onClick={load} className="underline hover:text-negative">
                retry
              </button>
            </div>
          )}

          {activeTab === 'overview' && (
            <div className="space-y-4">
              <div className="grid grid-cols-2 gap-4">
                <Field label="Signed in as" value={email ?? ABSENT} tone="text-ink" />
                <Field
                  label="Authorization role"
                  value={identity.role ? formatSymbol(identity.role) : ABSENT}
                  tone="text-info"
                  hint="Determines which endpoints this session may call"
                />
                <Field
                  label="Active swarm agents"
                  value={loading ? '…' : formatNumber(platform?.activeAgents, { decimals: 0 })}
                  tone="text-positive"
                  hint={
                    platform && Object.keys(platform.reportingTiers).length > 0
                      ? Object.entries(platform.reportingTiers)
                          .map(
                            ([tier, status]) =>
                              `${tier.replace('agents-', '')}: ${status.toLowerCase()}`,
                          )
                          .join('·')
                      : 'Tier status unavailable'
                  }
                />
                <Field
                  label="Monitored equities"
                  value={loading ? '…' : formatNumber(platform?.watchlistCount, { decimals: 0 })}
                  tone="text-accent"
                  hint="Active watchlist size"
                />
              </div>

              <div className="p-4 bg-raised/50 border border-line rounded-xl">
                <h4 className="text-xs font-bold text-ink-dim">DEPLOYMENT</h4>
                <p className="text-micro text-ink-dim mt-1 leading-relaxed">
                  Self-hosted single-node deployment. All inference runs locally; no telemetry
                  leaves this host. Agent tiers run behind an opt-in profile, so a tier reporting as
                  not deployed reflects the current run mode rather than a fault.
                </p>
              </div>
            </div>
          )}

          {activeTab === 'access' && (
            <div className="space-y-4">
              <div className="p-3 bg-raised/60 rounded-xl border border-line space-y-2">
                <span className="text-micro font-bold text-ink-dim">Session authentication</span>
                <ul className="text-micro text-ink-dim space-y-1 list-disc list-inside leading-relaxed">
                  <li>
                    Browser requests authenticate with an HttpOnly, SameSite=Strict session cookie
                  </li>
                  <li>
                    The cookie is HMAC SHA-256 signed and verified independently by the gateway
                  </li>
                  <li>
                    REST calls are proxied server-side; the gateway API key is never sent to the
                    browser
                  </li>
                  <li>WebSocket handshakes are authenticated before the connection is accepted</li>
                </ul>
              </div>

              <div className="p-3 bg-raised/60 rounded-xl border border-line space-y-2">
                <span className="text-micro font-bold text-ink-dim">Rate limit</span>
                <div className="grid grid-cols-2 gap-3 text-micro mt-1">
                  <div>
                    <span className="text-ink-mute">Burst capacity</span>
                    <p className="text-accent font-bold">
                      {formatNumber(RATE_LIMIT_BURST, { decimals: 0 })} requests
                    </p>
                  </div>
                  <div>
                    <span className="text-ink-mute">Sustained refill</span>
                    <p className="text-accent font-bold">
                      {formatNumber(RATE_LIMIT_PER_SEC, { decimals: 0 })} req/sec
                    </p>
                  </div>
                </div>
                <p className="text-micro text-ink-mute leading-relaxed">
                  Token bucket, applied per authenticated identity. Sustained throughput is{''}
                  {formatNumber(RATE_LIMIT_PER_SEC * 60, { decimals: 0 })} requests per minute;
                  exceeding it returns HTTP 429.
                </p>
              </div>

              <div className="p-3 bg-caution/10 border border-caution/30 rounded-xl">
                <span className="text-micro font-bold text-caution">API key</span>
                <p className="text-micro text-ink-dim mt-1 leading-relaxed">
                  The gateway API key is held server-side and is deliberately not exposed to this
                  interface. To issue direct REST or WebSocket calls, read{''}
                  <code className="text-caution">API_GATEWAY_KEY</code> from the deployment
                  environment and send it in the <code className="text-caution">X-API-KEY</code>{' '}
                  header.
                </p>
              </div>
            </div>
          )}

          {activeTab === 'access' && (
            /* The plan page was built and linked from nowhere, so nobody could
               reach their own subscription. It belongs beside access and limits
               rather than in the main navigation, which is for intelligence
               surfaces rather than account settings. */
            <a
              href="/account"
              className="mt-4 inline-flex items-center gap-2 text-micro font-bold uppercase tracking-widest text-accent hover:text-accent"
            >
              Manage plan &amp; billing →
            </a>
          )}

          {activeTab === 'integrations' && isOperator && (
            /* Which upstreams are configured, and what each missing key costs.
               Lives here rather than on a dashboard route because it is
               operator configuration, not intelligence. */
            <IntegrationsPanel />
          )}

          {activeTab === 'telemetry' && (
            <div className="space-y-4">
              <div className="grid grid-cols-2 gap-4">
                <Field
                  label="System status"
                  value={loading ? '…' : (platform?.systemStatus ?? ABSENT)}
                  tone={statusTone}
                />
                <Field
                  label="Healthy components"
                  value={
                    loading
                      ? '…'
                      : platform?.healthyComponents !== null && platform?.scoredComponents
                        ? `${formatNumber(platform.healthyComponents, { decimals: 0 })} / ${formatNumber(
                            platform.scoredComponents,
                            { decimals: 0 },
                          )}`
                        : ABSENT
                  }
                  tone="text-accent"
                  hint="Components not deployed in this run mode are excluded"
                />
              </div>

              <div className="p-3 bg-raised/60 rounded-xl border border-line">
                <span className="text-micro font-bold text-ink-dim">Agent tiers</span>
                {platform && Object.keys(platform.reportingTiers).length > 0 ? (
                  <div className="mt-2 space-y-1">
                    {Object.entries(platform.reportingTiers).map(([tier, status]) => (
                      <div key={tier} className="flex justify-between text-micro">
                        <span className="text-ink-dim">{tier}</span>
                        <span
                          className={
                            status === 'HEALTHY'
                              ? 'text-positive'
                              : status === 'DEGRADED'
                                ? 'text-caution'
                                : status === 'NOT_DEPLOYED'
                                  ? 'text-ink-mute'
                                  : 'text-negative'
                          }
                        >
                          {status}
                        </span>
                      </div>
                    ))}
                  </div>
                ) : (
                  <p className="text-micro text-ink-mute mt-1">Tier status unavailable</p>
                )}
              </div>

              <div className="flex items-center justify-between text-micro text-ink-mute">
                <span>
                  Snapshot taken {platform?.lastUpdated ? formatAge(platform.lastUpdated) : ABSENT}{' '}
                  ago
                  {platform?.lastUpdated ? `· ${formatTimestamp(platform.lastUpdated)}` : ''}
                </span>
                <button
                  onClick={load}
                  disabled={loading}
                  className="px-2 py-1 border border-line-strong rounded hover:text-ink hover:border-line-strong transition-colors disabled:opacity-50 focus:outline-none focus:ring-2 focus:ring-accent"
                >
                  {loading ? 'REFRESHING…' : 'REFRESH'}
                </button>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};
