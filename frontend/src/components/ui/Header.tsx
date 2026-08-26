'use client';

/**
 * Application header.
 *
 * Rebuilt because most of what it displayed was not true. The account button
 * showed "A. VANCE / INSTITUTIONAL" with the initials "AV" as literals, so
 * every user who signed in saw a fictional person's name instead of their own.
 * The telemetry strip asserted "AIS TANKERS: ACTIVE", "ADS-B FLIGHTS: TRACKING"
 * and "AGENT SWARM: ACTIVE (8)" as fixed text -- the deployment runs ten agents,
 * and none of those three statuses was read from anything. The version badge
 * "v2.4 EDA ACTIVE" was likewise hardcoded.
 *
 * What remains is measured: the stream state from the telemetry store, gateway
 * latency from a timed request, the live agent count from /agents/processes, and
 * the signed-in identity from the session. The visual treatment follows the same
 * rules as the panels -- no neon halo, no backdrop blur, cyan reserved for what
 * is interactive.
 */

import React, { useState, useEffect } from 'react';
import useSWR from 'swr';
import { Command } from 'lucide-react';
import SystemHealthHUD from '../SystemHealthHUD';
import { apiClient, fetcher } from '../../lib/api';
import { useTelemetryStore } from '../../lib/store';
import { AccountProfileModal } from '../AccountProfileModal';

const TIMEZONES: Array<[string, string]> = [
  ['America/New_York', 'US EST'],
  ['UTC', 'UTC'],
  ['America/Chicago', 'US CST'],
  ['America/Denver', 'US MST'],
  ['America/Los_Angeles', 'US PST'],
  ['Europe/London', 'GMT'],
  ['Europe/Paris', 'CET'],
  ['Asia/Tokyo', 'JST'],
];

/** Initials for the avatar, from whatever the session actually gives us. */
function initialsFor(email?: string | null): string {
  if (!email) return '··';
  const local = email.split('@')[0] || '';
  const parts = local.split(/[._-]+/).filter(Boolean);
  if (parts.length >= 2) return (parts[0][0] + parts[1][0]).toUpperCase();
  return local.slice(0, 2).toUpperCase() || '··';
}

export const Header: React.FC = () => {
  const [time, setTime] = useState<string>('');
  const [timezone, setTimezone] = useState<string>('America/New_York');
  const [latency, setLatency] = useState<number | null>(null);
  const [isProfileOpen, setIsProfileOpen] = useState<boolean>(false);
  const [isMac, setIsMac] = useState<boolean>(false);

  const isConnected = useTelemetryStore((state) => state.isConnected);
  // Distinct from "connecting": the feed was refused for lack of a session.
  const authRequired = useTelemetryStore((state) => state.authRequired);

  const { data: session } = useSWR<{ authenticated: boolean; user?: { email: string; role: string } }>(
    '/api/auth/session',
    (url: string) => fetch(url).then((r) => (r.ok ? r.json() : { authenticated: false })),
    { refreshInterval: 60000 },
  );

  const { data: processes } = useSWR<{ active_agents_count: number }>(
    '/agents/processes',
    fetcher,
    { refreshInterval: 15000 },
  );

  useEffect(() => {
    setIsMac(/Mac|iPhone|iPad/.test(navigator.platform || navigator.userAgent));
  }, []);

  useEffect(() => {
    const updateClock = () => {
      const now = new Date();
      try {
        const parts = new Intl.DateTimeFormat('en-US', {
          timeZone: timezone,
          hour: '2-digit',
          minute: '2-digit',
          second: '2-digit',
          hourCycle: 'h23',
          timeZoneName: 'short',
        }).formatToParts(now);
        const p: Record<string, string> = {};
        parts.forEach((part) => {
          p[part.type] = part.value;
        });
        setTime(`${p.hour}:${p.minute}:${p.second} ${p.timeZoneName || ''}`);
      } catch {
        setTime(now.toISOString().substring(11, 19) + ' UTC');
      }
    };
    updateClock();
    const interval = setInterval(updateClock, 1000);
    return () => clearInterval(interval);
  }, [timezone]);

  useEffect(() => {
    const measureLatency = async () => {
      const start = Date.now();
      try {
        await apiClient.get('/health');
        setLatency(Date.now() - start);
      } catch {
        // A failed probe is not a latency reading. Reporting the elapsed time of
        // a request that never completed would present an outage as a number.
        setLatency(null);
      }
    };
    measureLatency();
    const timer = setInterval(measureLatency, 10000);
    return () => clearInterval(timer);
  }, []);

  const email = session?.user?.email;
  const role = session?.user?.role;
  const agentCount = processes?.active_agents_count;

  const streamTone = isConnected ? 'tone-positive' : authRequired ? 'tone-negative' : 'tone-caution';
  const streamDot = isConnected ? 'bg-emerald-400' : authRequired ? 'bg-rose-400' : 'bg-amber-400';

  return (
    <header
      className="h-14 min-h-[56px] w-full shrink-0 z-40 flex items-center justify-between gap-4
                 px-4 sm:px-6 bg-[var(--bg-inset)] border-b border-[var(--border-subtle)]"
    >
      {/* Identity */}
      <div className="flex items-center gap-3 shrink-0 min-w-0">
        <div className="h-8 w-8 rounded-lg bg-[var(--accent-dim)] border border-[var(--border-accent)]
                        flex items-center justify-center shrink-0">
          <span className="text-[var(--accent)] font-semibold text-sm">S</span>
        </div>
        <div className="min-w-0">
          <h1 className="text-sm font-semibold text-slate-100 leading-tight truncate">Sentinel</h1>
          <p className="text-[10px] text-slate-500 leading-tight truncate hidden sm:block">
            Multi-domain intelligence
          </p>
        </div>
      </div>

      {/* Measured state */}
      <div className="hidden lg:flex items-center gap-4 text-[11px] min-w-0">
        <span className="flex items-center gap-1.5 whitespace-nowrap">
          <span className={`h-1.5 w-1.5 rounded-full ${streamDot}`} />
          <span className="text-slate-500">Stream</span>
          {authRequired ? (
            <a href="/login" className="text-rose-400 hover:text-rose-300 underline underline-offset-2">
              sign in
            </a>
          ) : (
            <span className={streamTone}>{isConnected ? 'live' : 'connecting'}</span>
          )}
        </span>

        <span className="text-slate-700">·</span>

        <span className="flex items-center gap-1.5 whitespace-nowrap">
          <span className="text-slate-500">Agents</span>
          <span className="text-slate-300 tabular">
            {typeof agentCount === 'number' ? agentCount : '—'}
          </span>
        </span>

        <span className="text-slate-700">·</span>

        <span className="flex items-center gap-1.5 whitespace-nowrap">
          <span className="text-slate-500">Gateway</span>
          <span className={latency === null ? 'tone-negative' : 'text-slate-300 tabular'}>
            {latency === null ? 'unreachable' : `${latency}ms`}
          </span>
        </span>
      </div>

      {/* Controls */}
      <div className="flex items-center gap-2 sm:gap-3 shrink-0">
        <span
          className="hidden xl:flex items-center gap-1 text-[10px] text-slate-500 border border-[var(--border-subtle)]
                     rounded px-1.5 py-1"
          title="Open the command palette"
        >
          {isMac ? <Command className="h-3 w-3" /> : <span className="font-medium">Ctrl</span>}
          <span className="font-medium">K</span>
        </span>

        <div className="hidden xl:flex items-center gap-2">
          <span className="text-[11px] text-slate-300 tabular whitespace-nowrap" suppressHydrationWarning>
            {time}
          </span>
          <select
            value={timezone}
            onChange={(e) => setTimezone(e.target.value)}
            className="bg-[var(--bg-raised)] text-[10px] text-slate-400 border border-[var(--border-subtle)]
                       rounded px-1.5 py-1 outline-none cursor-pointer hover:text-slate-200 transition-colors"
            aria-label="Clock timezone"
          >
            {TIMEZONES.map(([tz, label]) => (
              <option key={tz} value={tz}>
                {label}
              </option>
            ))}
          </select>
        </div>

        <SystemHealthHUD />

        <button
          onClick={() => setIsProfileOpen(true)}
          className="flex items-center gap-2 pl-1.5 pr-2.5 py-1.5 rounded-lg border border-[var(--border-subtle)]
                     hover:border-[var(--border-strong)] hover:bg-[var(--bg-raised)] transition-colors"
          title="Account"
        >
          <span className="h-6 w-6 rounded bg-[var(--accent-dim)] border border-[var(--border-accent)]
                           flex items-center justify-center text-[10px] font-semibold text-[var(--accent)]">
            {initialsFor(email)}
          </span>
          <span className="hidden sm:flex flex-col text-left min-w-0">
            <span className="text-[11px] text-slate-200 leading-tight truncate max-w-[160px]">
              {email || 'Not signed in'}
            </span>
            {role && (
              <span className="text-[9px] text-slate-500 leading-tight uppercase tracking-wide">{role}</span>
            )}
          </span>
        </button>
      </div>

      <AccountProfileModal isOpen={isProfileOpen} onClose={() => setIsProfileOpen(false)} />
    </header>
  );
};
